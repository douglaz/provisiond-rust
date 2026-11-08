#!/usr/bin/env python3
"""STO-7/STO-54: isolated two-node physical replication and archive drill.

All servers are supervised children of this foreground process in its capped cgroup.
No component is started on a promoted or restored history.
"""
import argparse
import hashlib
import json
import os
from pathlib import Path
import secrets
import shutil
import signal
import socket
import subprocess
import sys
import tempfile
import time
import traceback
import tomllib

SPEC = 'ac37c7cae5d3ff746515137774a4cdd5a3531375'
# The caller's capped unit and its private, owned run directory (outside git and /tmp).
ROOT = Path(os.environ['PROVISIOND_HARNESS_ROOT'])
UNIT = os.environ['PROVISIOND_HARNESS_UNIT']
assert ROOT.is_absolute() and not str(ROOT).startswith('/tmp/') and UNIT.endswith('.service')


def cap_check():
    assert Path('/proc/self/cgroup').read_text().strip().endswith('/' + UNIT)
    r = subprocess.run(['systemctl', '--user', 'show', UNIT, '--property=MemoryMax',
                        '--property=MemorySwapMax', '--property=OOMPolicy'],
                       capture_output=True, text=True, check=True)
    assert set(r.stdout.splitlines()) == {'MemoryMax=17179869184', 'MemorySwapMax=0',
                                        'OOMPolicy=continue'}, r.stdout


def free_port(port):
    assert 55500 <= port <= 55559
    with socket.socket() as s:
        s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        s.bind(('127.0.0.1', port))


def allocation():
    """Required private ten-port allocation, validated before starting any child."""
    raw = os.environ.get('PROVISIOND_HARNESS_PORT_BASE', '')
    if not raw.isascii() or not raw.isdecimal():
        raise ValueError('PROVISIOND_HARNESS_PORT_BASE must be an integer')
    base = int(raw)
    if base not in range(55500, 55560, 10):
        raise ValueError('ten harness ports require a slot base in 55500..55550')
    return list(range(base, base + 10))


def preflight_ports():
    ports = allocation()
    for port in ports:
        free_port(port)
    return ports


def eventually(check, seconds=20):
    end = time.monotonic() + seconds
    last = None
    while time.monotonic() < end:
        try:
            last = check()
            if last:
                return last
        except (subprocess.SubprocessError, OSError):
            pass
        time.sleep(.05)
    raise AssertionError(f'condition did not activate within {seconds}s; last={last!r}')


class Harness:
    def __init__(self, phase, seed):
        cap_check()
        self.ports = preflight_ports()
        os.umask(0o077)
        ROOT.mkdir(parents=True, exist_ok=True)
        self.root = Path(tempfile.mkdtemp(prefix=f'{phase}-', dir=ROOT))
        self.phase, self.seed = phase, seed
        self.events, self.commands, self.results, self.nodes = [], [], [], {}
        self.started = time.monotonic()
        self.passwords = {r: secrets.token_hex(24) for r in
                          ['replicator', 'administrator', 'migrator', 'app', 'observer', 'denied']}
        self.pgpass = self.root / 'pgpass'
        self.pgpass.write_text(''.join(f'127.0.0.1:*:*:{r}:{p}\n' for r, p in self.passwords.items()))
        self.pgpass.chmod(0o600)
        self.target = self.root/'target'
        self.env = dict(os.environ, PGPASSFILE=str(self.pgpass), PGCONNECT_TIMEOUT='2', CARGO_TARGET_DIR=str(self.target))
        self.config = dict(synchronous_commit='on', synchronous_standby_names='FIRST 1 (provisiond_sync)',
                           max_connections=24, shared_buffers='32MB', wal_level='replica',
                           archive_mode='on', restart_after_crash='on', ports=self.ports[:3],bootstrap_readiness_seconds=60,postgres_stop_wait_seconds=60,bootstrap_command_seconds=180,
                           authentication='scram-sha-256 loopback only', standby_role='replicator')
        if phase in ('checks','commits','regressions','corrections','core','workflow','default-workflow'):
            self.config['store'] = dict(read_pool=1,write_pool=1,statement_ms=1000,lock_ms=500,
                idle_transaction_ms=1000,call_ms=2000,check_ms=3000,checkout_ms=250,
                keepalive_idle_seconds=2,keepalive_interval_seconds=1,keepalive_count=2,
                isolation='read committed',search_path='pg_catalog,fixture',stop_fixture_ms=1000,
                transport='direct TCP; loopback proxy for faults; sslmode=disable')
            self.config['ports'].append(self.ports[3])
        if phase in ('core','workflow','default-workflow'):
            self.config['engine']=dict(workers=3,write_pool=3,compatibility_read_pool=1,
                startup_lock_outside_pool=True,startup_wait_ms=500,store_retry_ms=2000,stop_record_ms=1000,
                startup_lock_keepalives=dict(idle_seconds=2,interval_seconds=1,count=2,profile_assignment="shared with pooled local values"),
                runtime_build=dict(features=["store-test-hooks"],target_directory=str(self.target)),
                migration_lock_ms=500,migration_lock_retries=2,schema_version=2,semantic_version=2,notice_cap=16)
        self.archive = self.root / 'archive'
        self.archive.mkdir()
        self.event('invocation', seed=seed, specification=SPEC, phase=phase,
                   configuration=json.loads(json.dumps(self.config)),
                   configuration_sha256=hashlib.sha256(json.dumps(self.config, sort_keys=True).encode()).hexdigest())
        self.source = self.run(['git', 'rev-parse', 'HEAD']).stdout.strip()
        self.dirty = bool(self.run(['git','status','--porcelain','--','.',':(exclude)evidence/**']).stdout.strip())
        # Exact content identity, including untracked source, without hashing credentials/artifacts.
        paths = subprocess.check_output(['git','ls-files','--cached','--others','--exclude-standard'], text=True).splitlines()
        hashes = {p: hashlib.sha256(Path(p).read_bytes()).hexdigest() for p in paths
                  if Path(p).is_file() and not p.startswith('evidence/')}
        self.tree = hashlib.sha256(json.dumps(hashes, sort_keys=True).encode()).hexdigest()
        self.source_files = hashes
        self.versions = {}
        for name, cmd in {
            'postgres': ['postgres','--version'], 'pg_ctl':['pg_ctl','--version'],
            'pg_basebackup':['pg_basebackup','--version'], 'psql':['psql','--version'],
            'pg_verifybackup':['pg_verifybackup','--version'], 'initdb':['initdb','--version'],
            'nix':['nix','--version'], 'rustc':['rustc','--version'], 'cargo':['cargo','--version'],
            'python':['python3','--version'], 'os':['uname','-sm'],
        }.items():
            self.versions[name] = self.run(cmd).stdout.strip()
        self.versions['crates'] = {p['name']:p['version'] for p in tomllib.loads(Path('Cargo.lock').read_text())['package']}
        self.versions['postgres_path'] = str(Path(shutil.which('postgres')).resolve())
        self.versions['nixpkgs'] = 'aff8a0b28396750446e5537a96461bc4facdb287:postgresql_18'
        # Keep currently unused server slots out of this run's outgoing client
        # source-port selection. No socket listens or impersonates PostgreSQL.
        self.port_reservations={}
        try:
            for port in self.ports:
                self.reserve_port(port)
        except BaseException:
            for port in list(self.port_reservations):
                self.release_port(port)
            raise
        self.event('allocation_reserved',ports=self.ports)

    def reserve_port(self,port):
        assert port in self.ports
        if port in self.port_reservations:
            return
        reservation=socket.socket()
        try:
            reservation.setsockopt(socket.SOL_SOCKET,socket.SO_REUSEADDR,1)
            reservation.bind(('127.0.0.1',port))
        except BaseException:
            reservation.close()
            raise
        self.port_reservations[port]=reservation

    def release_port(self,port):
        reservation=self.port_reservations.pop(port,None)
        if reservation:
            reservation.close()

    def event(self, kind, **data):
        e = dict(event=kind, timestamp=time.time(), elapsed=time.monotonic()-self.started, **data)
        self.events.append(e)
        with (self.root / 'events.jsonl').open('a') as f:
            f.write(json.dumps(e, sort_keys=True)+'\n')

    def run(self, cmd, *, sql=None, ok=True, timeout=45):
        # SQL/passwords are intentionally omitted from command manifests.
        try:
            r = subprocess.run(cmd, input=sql, env=self.env, capture_output=True, text=True, timeout=timeout)
        except subprocess.TimeoutExpired:
            self.commands.append(dict(command=cmd, exit_status=None, result='inconclusive',reason='supervised command timed out; subprocess.run killed and waited for its child'))
            raise
        self.commands.append(dict(command=cmd, exit_status=r.returncode))
        with (self.root / 'commands.log').open('a') as f:
            f.write(json.dumps(cmd)+'\n'+r.stdout+r.stderr+f'\nstatus={r.returncode}\n')
        if ok:
            assert r.returncode == 0, f'{cmd[0]} exited {r.returncode}: {r.stderr}'
        return r

    def sql(self, sql, node='primary', role='administrator', ok=True):
        port = self.nodes[node]['port']
        return self.run(['psql','-XAt','-v','ON_ERROR_STOP=1','-h','127.0.0.1',
                         '-p',str(port),'-U',role,'-d','postgres'], sql=sql, ok=ok, timeout=12).stdout.strip()

    def init(self):
        data = self.root / 'primary'
        pw = self.root / 'init-password'
        pw.write_text(self.passwords['administrator'])
        self.run(['initdb','-D',str(data),'-U','administrator','--pwfile='+str(pw),
                  '--auth=scram-sha-256','--no-locale','--encoding=UTF8'],timeout=self.config['bootstrap_command_seconds'])
        # Offline cluster initialization, before any server accepts connections.
        # Like initdb's catalog initialization, this creates only the replication
        # identity. synchronous_commit stays on; no component or migration exists.
        self.run(['postgres','--single','-D',str(data),'postgres'],
                 sql=f"CREATE ROLE replicator LOGIN REPLICATION PASSWORD '{self.passwords['replicator']}';\n",timeout=self.config['bootstrap_command_seconds'])
        (data / 'pg_hba.conf').write_text(
            'host replication replicator 127.0.0.1/32 scram-sha-256\n'
            'host all all 127.0.0.1/32 scram-sha-256\n')
        self.configure(data, self.ports[0])
        self.nodes['primary'] = dict(data=data, port=self.ports[0])
        self.start('primary')
        self.backup('standby', standby=True)
        self.nodes['standby'] = dict(data=self.root/'standby', port=self.ports[1])
        self.configure(self.root/'standby', self.ports[1])
        self.start('standby')
        eventually(lambda: self.sql("SELECT count(*)=1 FROM pg_stat_replication WHERE application_name='provisiond_sync' AND sync_state='sync' AND state='streaming'")=='t')
        setup = '\n'.join(f"CREATE ROLE {r} LOGIN NOSUPERUSER PASSWORD '{self.passwords[r]}';" for r in self.passwords if r not in ('replicator','administrator'))
        self.sql(setup)
        self.sql('GRANT pg_read_all_stats TO observer;')
        self.identity()
        names=['synchronous_commit','synchronous_standby_names','max_connections','superuser_reserved_connections',
               'wal_level','archive_mode','archive_command','restart_after_crash','listen_addresses','port','shared_buffers',
               'tcp_keepalives_idle','tcp_keepalives_interval','tcp_keepalives_count','lc_messages']
        literals=','.join("'"+name+"'" for name in names)
        observed=json.loads(self.sql(f'SELECT json_object_agg(name,setting)::text FROM pg_settings WHERE name IN ({literals})'))
        observed['archive_command']=observed['archive_command'].replace(str(self.root),'<owned-run>')
        assert observed['synchronous_commit']=='on' and observed['synchronous_standby_names']=='FIRST 1 (provisiond_sync)'
        assert observed['archive_mode']=='on' and observed['restart_after_crash']=='on'
        self.config['effective_primary']=observed
        self.event('effective_configuration_observed',configuration=observed,
                   sha256=hashlib.sha256(json.dumps(observed,sort_keys=True).encode()).hexdigest())
        self.sql('CREATE SCHEMA fixture AUTHORIZATION migrator; GRANT USAGE ON SCHEMA fixture TO app;')
        self.sql('CREATE TABLE fixture.destinations(id integer PRIMARY KEY, destination text NOT NULL); GRANT SELECT, INSERT, UPDATE ON fixture.destinations TO app;',role='migrator')
        self.event('bootstrap_complete', maintenance_mode_exception=False,
                   bootstrap_role='initdb administrator; offline replication identity only',
                   backup_name='bootstrap_backup', designated_name='provisiond_sync')

    def configure(self, data, port):
        (data / 'postgresql.conf').write_text(f"""
listen_addresses = '127.0.0.1'
port = {port}
unix_socket_directories = ''
shared_buffers = '32MB'
max_connections = 24
max_wal_senders = 5
wal_level = replica
wal_keep_size = '128MB'
synchronous_commit = on
synchronous_standby_names = 'FIRST 1 (provisiond_sync)'
restart_after_crash = on
archive_mode = on
archive_command = 'test ! -f {self.archive}/%f && cp %p {self.archive}/%f'
logging_collector = off
log_line_prefix = '%m [%p] '
lc_messages = 'C'
wal_receiver_status_interval = '100ms'
tcp_keepalives_idle = 2
tcp_keepalives_interval = 1
tcp_keepalives_count = 2
""")

    def start(self, name, role='administrator'):
        cap_check()
        n = self.nodes[name]
        self.release_port(n['port'])
        free_port(n['port'])
        log = (self.root / f'{name}.log').open('ab', buffering=0)
        readiness_started=time.monotonic()
        n['process'] = subprocess.Popen(['postgres','-D',str(n['data'])], env=self.env,
                                         stdout=log, stderr=log)
        n['command'] = dict(command=['postgres','-D',str(n['data'])],exit_status=None)
        self.commands.append(n['command'])
        log.close()
        eventually(lambda: self.sql('SELECT 1',node=name,role=role,ok=False)=='1',seconds=self.config['bootstrap_readiness_seconds'])
        self.event('server_started', node=name, pid=n['process'].pid, port=n['port'],bootstrap_readiness_seconds=self.config['bootstrap_readiness_seconds'],readiness_elapsed_seconds=time.monotonic()-readiness_started)

    def stop(self, name, mode='fast'):
        n = self.nodes[name]
        if n.get('process') and n['process'].poll() is None:
            self.run(['pg_ctl','-D',str(n['data']),'stop','-m',mode,'-w','-t',str(self.config['postgres_stop_wait_seconds'])],timeout=75)
            status=n['process'].wait(timeout=5)
            n['command']['exit_status']=status
            self.event('server_stopped', node=name, mode=mode,exit_status=status)
        elif n.get('process'):
            status=n['process'].returncode
            n['command']['exit_status']=status
            assert status==0, f'owned server {name} exited unexpectedly: {status}'
        if not any(other['port']==n['port'] and other.get('process') and other['process'].poll() is None for other in self.nodes.values()):
            self.reserve_port(n['port'])

    def crash_backend(self, pid):
        # Harness-only deliberate crash, validated against this owned postmaster.
        parent = int(Path(f'/proc/{pid}/stat').read_text().split(') ',1)[1].split()[1])
        assert parent == self.nodes['primary']['process'].pid
        assert self.sql(f'SELECT count(*) FROM pg_stat_activity WHERE pid={pid}') == '1'
        self.event('fault_requested', fault='owned_backend_crash', pid=pid)
        os.kill(pid, signal.SIGKILL)
        eventually(lambda: self.sql('SELECT 1',ok=False)=='1')
        self.event('fault_activated', fault='owned_backend_crash', pid=pid,
                   postmaster_pid=self.nodes['primary']['process'].pid)

    def backup(self, name, standby=False):
        dest = self.root / name
        started=time.monotonic()
        self.event('backup_requested', destination=name, wal_mode='stream', application_name='bootstrap_backup',command_bound_seconds=self.config['bootstrap_command_seconds'])
        self.run(['pg_basebackup','-d',f'host=127.0.0.1 port={self.ports[0]} user=replicator dbname=postgres application_name=bootstrap_backup',
                  '-D',str(dest),'-X','stream','--checkpoint=fast','--no-password'],timeout=self.config['bootstrap_command_seconds'])
        self.run(['pg_verifybackup',str(dest)],timeout=self.config['bootstrap_command_seconds'])
        provenance=self.root/(name+'-backup-manifest.json')
        provenance.write_bytes((dest/'backup_manifest').read_bytes())
        self.event('backup_provenance_retained',path=str(provenance),sha256=hashlib.sha256(provenance.read_bytes()).hexdigest())
        if standby:
            (dest/'standby.signal').touch()
            # Set the final application name and authenticated role before startup.
            (dest/'postgresql.auto.conf').write_text(
                f"primary_conninfo = 'host=127.0.0.1 port={self.ports[0]} user=replicator application_name=provisiond_sync passfile={self.pgpass}'\n")
        self.event('backup_complete', destination=name, verified=True,elapsed_seconds=time.monotonic()-started)

    def identity(self):
        target = self.sql('SELECT pg_current_wal_flush_lsn()')
        eventually(lambda: self.sql(f"SELECT count(*)=1 FROM pg_stat_replication WHERE application_name='provisiond_sync' AND usename='replicator' AND client_addr='127.0.0.1' AND sync_state='sync' AND flush_lsn >= '{target}'::pg_lsn")=='t')
        p = self.sql('SELECT system_identifier FROM pg_control_system()')
        s = self.sql('SELECT system_identifier FROM pg_control_system()',node='standby')
        assert p == s
        assert self.sql('SELECT pg_is_in_recovery()',node='standby') == 't'
        self.event('identity_catchup', system_identifier=p, sampled_primary_flush=target,
                   standby_system_identifier=s, authenticated_role='replicator',
                   primary_checkpoint=self.sql('SELECT timeline_id, redo_lsn FROM pg_control_checkpoint()'),
                   standby_checkpoint=self.sql('SELECT timeline_id, redo_lsn FROM pg_control_checkpoint()',node='standby'),
                   provenance='verified pg_basebackup from this primary; exclusive run-owned data and credentials')

    def drill(self):
        self.init()
        self.sql("INSERT INTO fixture.destinations VALUES (1,'archive-before')", role='app')
        eventually(lambda: self.sql('SELECT count(*) FROM fixture.destinations',node='standby')=='1')
        self.stop('standby')
        # Rebuild with no acknowledgement partner and without an ordinary SQL commit.
        self.backup('rebuilt', standby=True)
        self.nodes['retired'] = self.nodes.pop('standby')
        self.nodes['standby'] = dict(data=self.root/'rebuilt',port=self.ports[1])
        self.configure(self.root/'rebuilt',self.ports[1])
        self.start('standby')
        self.identity()
        self.results.append(dict(test='W1-bootstrap-rebuild-replication',result='pass',
                                 establishes='fresh/rebuilt physical standby, authenticated identity/provenance and sampled prefix'))
        self.backup('restore-backup')
        self.sql("INSERT INTO fixture.destinations VALUES (2,'archive-after-unique-fixture')",role='app')
        # This row is outside the base backup: recovery must fetch the archived segment.
        self.sql('SELECT pg_switch_wal()')
        eventually(lambda: self.sql('SELECT archived_count > 0 FROM pg_stat_archiver')=='t')
        self.stop('standby')
        self.stop('primary')
        restore = self.root/'restored'
        shutil.copytree(self.root/'restore-backup',restore)
        # Force archive replay for WAL produced after backup; do not use streaming.
        self.configure(restore,self.ports[2])
        (restore/'postgresql.auto.conf').write_text(f"restore_command = 'cp {self.archive}/%f %p'\n")
        (restore/'standby.signal').touch()
        self.nodes['restored'] = dict(data=restore,port=self.ports[2])
        self.start('restored')
        eventually(lambda: self.sql("SELECT destination FROM fixture.destinations WHERE id=2",node='restored')=='archive-after-unique-fixture')
        self.event('archive_restore_observed', fixture='id=2 created after base backup',
                   replay_lsn=self.sql('SELECT pg_last_wal_replay_lsn()',node='restored'),
                   components='stopped; no incident or product startup claimed')
        self.run(['pg_ctl','-D',str(restore),'promote','-w','-t','15'])
        assert self.sql('SELECT pg_is_in_recovery()',node='restored')=='f'
        self.event('operator_promotion_complete', components='stopped')
        self.results.append(dict(test='W1-archive-restore-operator-promotion',result='pass',
                                 establishes='post-backup fixture recovered from archived WAL; explicit infrastructure-only promotion'))
        import threading
        sys.path.insert(0,'tests/support')
        from pause import Simulator
        for kind in ['provider','rail']:
            simulator=Simulator()
            replies=[]
            worker=threading.Thread(target=lambda:replies.append(simulator.call()))
            worker.start()
            try:
                assert simulator.acceptance.reached.wait(3) and not simulator.accepted
                self.event('simulator_pause_observed',simulator=kind,point='acceptance')
                simulator.acceptance.release.set()
                assert simulator.reply.reached.wait(3) and simulator.accepted and not replies
                self.event('simulator_pause_observed',simulator=kind,point='reply')
            finally:
                simulator.acceptance.release.set()
                simulator.reply.release.set()
                worker.join(timeout=3)
            assert not worker.is_alive() and replies==['simulated reply']
        self.results.append(dict(test='simulator-acceptance-reply-pauses',result='pass',
                                 establishes='deterministic local provider/rail hooks only; no external effects'))


    def cleanup(self):
        errors=[]
        if hasattr(self,'probes'):
            from scenarios import cleanup_clients
            errors.extend(cleanup_clients(self))
        for name in list(self.nodes):
            try:
                self.stop(name)
            except Exception as e:
                errors.append(f'{name}: {e}')
        # Remove only explicitly registered data owned by this invocation, after assertions.
        paths = {n['data'] for n in self.nodes.values()}
        paths.update(self.root/n for n in ['primary','restored','restore-backup','standby','rebuilt'])
        paths.update(getattr(self,'owned_extra_paths',[]))
        paths.add(self.target)  # invocation-owned build output, after every child is reaped
        for path in sorted(paths):
            if path.exists() and not errors:
                shutil.rmtree(path)
                self.event('cleanup_removed', path=str(path))
        startup_binary=self.config.get('workflow',{}).get('builds',{}).get('default_startup_api')
        if startup_binary:
            startup=Path(startup_binary['executable']).parent.parent
            used=any(e['event']=='workflow_binary_started' and e.get('executable')==str(startup/'debug/api') and e.get('features')==[] for e in self.events)
            removed=not startup.exists() and not self.target.exists()
            self.event('startup_build_cleanup_observed',default_binary_used=used,
                       normal_target_removed=not self.target.exists(),startup_target_removed=not startup.exists())
            if not used or not removed:
                errors.append('isolated startup build output must be used and removed after child completion')
        for path in [self.pgpass,self.root/'init-password']:
            if path.exists():
                path.unlink()
        for port in list(self.port_reservations):
            self.release_port(port)
        self.event('allocation_released',ports=self.ports)
        self.event('cleanup_complete', result='fail' if errors else 'pass', errors=errors,
                   raw_evidence=str(self.root))
        return not errors

    def manifest(self, status):
        # Raw protocol/application streams stay under the private owned root.
        # The repository receives consequential, credential-redacted observations.
        observations=[]
        for event in self.events:
            if event['event'] in ('send_requested','bytes_transmitted'):
                continue
            value=dict(event)
            if event['event']=='protocol_response':
                if 'store COMMIT' not in event.get('query',''):
                    continue
            if 'query' in value:
                value['statement_kind']='COMMIT' if 'store COMMIT' in value['query'] else 'read-only check' if 'store ' in value['query'] else 'fixture statement'
                del value['query']
            observations.append(value)
        raw=self.root/'events.jsonl'
        manifest=dict(result=status, specification_commit=SPEC, implementation_commit=self.source,
                    tested_source_sha256=self.tree, source_files=self.source_files, source_dirty=self.dirty,
                    seed=self.seed, versions=self.versions, configuration=self.config,
                    configuration_sha256=hashlib.sha256(json.dumps(self.config,sort_keys=True).encode()).hexdigest(),
                    command=['nix','develop','--command','python3',*sys.argv], commands=self.commands,
                    scenarios=self.results, events=observations, raw_evidence=str(self.root),
                    trace=dict(path=str(raw),sha256=hashlib.sha256(raw.read_bytes()).hexdigest()),
                    external_provider_rail_evidence='simulator only; no real effects' if self.phase in ('core','workflow','default-workflow') else 'not applicable: no external effects',
                    limitations=['local processes only; no separate-host durability or fencing proof',
                                 'no product recovery incidents, automatic provider reconciliation, full meter or production alarms',
                                 'DB-10 full saturation: not run (W8)'])
        encoded=json.dumps(manifest)
        for password in self.passwords.values():
            encoded=encoded.replace(password,'<redacted>')
        # Repository evidence carries no host-local paths.
        encoded=encoded.replace(str(ROOT),'<run-dir>').replace(str(Path.cwd()),'<repo>')
        return json.loads(encoded)


def main():
    ap=argparse.ArgumentParser()
    ap.add_argument('--phase',choices=['w1','checks','commits','regressions','corrections','core','controls','workflow','default-workflow','all'],default='all')
    ap.add_argument('--case',choices=['session','transaction','runtime','cancelled','execute_cancel','checkout','caller_sql','commit_setting','notices','notice_sticky','verify_cancel','admission_latency','migrations','money','claims','generation','stops','external','claim_cancel','defer_cancel','licensing_stop','outcome_stop','replacement_outcome','deadline','lock_observers','terminal_zero','terminal_capacity','loss_stop','queued_claim','queued_claim_pool','displaced_record','surface','predispatch','startup','w4_atomic','w4_ack','w4_overload','w4_search_error','w4_query','w4_record','w4_default','w4_redaction','w4_halt','w4_forward','w4_live_stop','w4_price','w4_empty_data','w4_spend','w4_abandon','w4_unauth','w4_pacing','w4_token_pacing','w4_settlement','w4_guards','w4_suspended_attach','w4_no_rate','w4_no_rate_absent','w4_stop_cost','w4_regressions','w4_reserve_record','w4_rate_record','w4_conversion_record','w4_offer_record','w4_underpayment','w4_overpayment','w4_operator_hiding','w4_api_startup','w4_api_idle','w4_api_checkout','w4_api_commit','w4_observation','w4_preflight','w4_restart_error','w4_stop_error'],help='one focused regression; default runs all')
    ap.add_argument('--defect', choices=['execute_cancel','checkout','caller_sql','commit_setting','notices','notice_sticky','verify_cancel','generation','claim_guard','payment_atomic','claim_cancel','defer_cancel','licensing_stop','outcome_stop','replacement_outcome','deadline','lock_observers','surface','predispatch','startup_order','terminal_zero','terminal_capacity','loss_stop','queued_claim','queued_claim_pool','displaced_record','lock_record_reset','w4_atomic','w4_ack','w4_overload'])
    ap.add_argument('--seed',type=int,default=20261105)
    ap.add_argument('--evidence',type=Path)
    args=ap.parse_args()
    preflight_ports()
    cases={
        'workflow':{'w4_atomic','w4_ack','w4_overload','w4_search_error','w4_query','w4_record','w4_default','w4_redaction','w4_halt','w4_forward','w4_live_stop','w4_price','w4_empty_data','w4_spend','w4_abandon','w4_unauth','w4_pacing','w4_token_pacing','w4_settlement','w4_guards','w4_suspended_attach','w4_no_rate','w4_no_rate_absent','w4_stop_cost','w4_regressions','w4_reserve_record','w4_rate_record','w4_conversion_record','w4_offer_record','w4_underpayment','w4_overpayment','w4_operator_hiding','w4_api_startup','w4_api_idle','w4_api_checkout','w4_api_commit','w4_observation','w4_preflight','w4_restart_error','w4_stop_error'},
        'regressions':{'session','transaction','runtime','cancelled'},
        'corrections':{'execute_cancel','checkout','caller_sql','commit_setting','notices','notice_sticky','verify_cancel','admission_latency'},
        'core':{'migrations','money','claims','generation','stops','external','claim_cancel','defer_cancel','licensing_stop','outcome_stop','replacement_outcome','deadline','lock_observers','terminal_zero','terminal_capacity','loss_stop','queued_claim','queued_claim_pool','displaced_record','surface','predispatch','startup'},
    }
    if args.case and args.case not in cases.get(args.phase,set()):
        ap.error('--case is incompatible with the selected phase')
    defect_cases={
        **{d:('workflow',d) for d in ('w4_atomic','w4_ack','w4_overload')},
        **{d:('corrections',d) for d in ('execute_cancel','checkout','caller_sql','commit_setting','notices','notice_sticky','verify_cancel')},
        'generation':('core','generation'),'claim_guard':('core','claims'),'payment_atomic':('core','money'),
        **{d:('core',d) for d in ('claim_cancel','defer_cancel','licensing_stop','outcome_stop','replacement_outcome','deadline','lock_observers','surface','predispatch')},
        'startup_order':('core','startup'),
        **{d:('core',d) for d in ('terminal_zero','terminal_capacity','loss_stop')},
        'queued_claim':('core','queued_claim'),
        'queued_claim_pool':('core','queued_claim_pool'),
        'displaced_record':('core','displaced_record'),
        'lock_record_reset':('core','lock_observers'),
    }
    if args.defect and (args.phase,args.case)!=defect_cases[args.defect]:
        ap.error('--defect requires its exact compatible phase and case')
    if args.phase=='controls':
        from controls import controls
        return controls(args.seed,args.evidence)
    if args.phase=='all':
        cap_check()
        records=[]
        for phase in ['w1','checks','commits','regressions','corrections','core','workflow','default-workflow','controls']:
            command=['python3','tests/postgres/harness.py','--phase',phase,'--seed',str(args.seed)]
            # Explicit evidence names; every child command remains foreground.
            if args.evidence:
                path=args.evidence.with_name(args.evidence.stem+'-'+phase+'.json')
                command.extend(['--evidence',str(path)])
            result=subprocess.run(command,check=False)
            records.append(dict(phase=phase,command=['nix','develop','--command',*command],exit_status=result.returncode,
                                result='pass' if result.returncode==0 else 'fail'))
            if result.returncode!=0:
                break
        status=0 if len(records)==9 and all(r['exit_status']==0 for r in records) else 1
        if args.evidence:
            args.evidence.write_text(json.dumps(dict(result='pass' if status==0 else 'fail',phases=records),indent=2)+'\n')
        return status
    h=Harness(args.phase,args.seed)
    status='fail'
    try:
        if args.phase=='w1':
            h.drill()
        elif args.phase=='checks':
            from scenarios import check_scenarios
            h.run(['cargo','build','--example','store-probe','--features','store-test-hooks','--locked'],timeout=120)
            check_scenarios(h)
        elif args.phase=='regressions':
            from scenarios import regression_scenarios
            h.run(['cargo','build','--example','store-probe','--features','store-test-hooks','--locked'],timeout=120)
            regression_scenarios(h,args.case)
        elif args.phase=='corrections':
            from corrections import corrections
            h.run(['cargo','build','--example','store-probe','--features','store-test-hooks','--locked'],timeout=120)
            corrections(h,args.case,args.defect)
        elif args.phase=='core':
            from core import core_scenarios
            h.run(['cargo','build','--bins','--examples','--features','store-test-hooks','--locked'],timeout=120)
            core_scenarios(h,args.case,args.defect)
        elif args.phase=='default-workflow':
            from workflow import workflow
            workflow(h,'w4_default')
        elif args.phase=='workflow':
            from workflow import workflow
            workflow(h,args.case,args.defect)
        elif args.phase=='commits':
            from scenarios import commit_scenarios
            h.run(['cargo','build','--example','store-probe','--features','store-test-hooks','--locked'],timeout=120)
            commit_scenarios(h)
        else:
            raise ValueError('phase not implemented')
        status='pass'
    except BaseException as e:
        h.event('failure',type=type(e).__name__,message=str(e))
        h.results.append(dict(test=args.case or args.phase,result='fail',reason=str(e)))
        traceback.print_exc()
    finally:
        if not h.cleanup():
            status='fail'
        manifest=h.manifest(status)
        (h.root/'manifest.json').write_text(json.dumps(manifest,indent=2)+'\n')
        if args.evidence:
            args.evidence.write_text(json.dumps(manifest,indent=2)+'\n')
        print(json.dumps(dict(result=status,raw_evidence=str(h.root),scenarios=h.results)),flush=True)
    return 0 if status=='pass' else 1

if __name__=='__main__':
    sys.exit(main())
