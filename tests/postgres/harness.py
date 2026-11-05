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

SPEC = 'ac37c7cae5d3ff746515137774a4cdd5a3531375'
ROOT = Path('/var/tmp/rloop-units/provisiond-rust-w1w2-1105T2104/harness')
UNIT = 'rloop-provisiond-rust-w1w2-1105T2104.service'


def cap_check():
    assert Path('/proc/self/cgroup').read_text().strip().endswith('/' + UNIT)
    r = subprocess.run(['systemctl', '--user', 'show', UNIT, '--property=MemoryMax',
                        '--property=MemorySwapMax', '--property=OOMPolicy'],
                       capture_output=True, text=True, check=True)
    assert set(r.stdout.splitlines()) == {'MemoryMax=17179869184', 'MemorySwapMax=0',
                                        'OOMPolicy=continue'}, r.stdout


def free_port(port):
    assert 55410 <= port <= 55429
    with socket.socket() as s:
        s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        s.bind(('127.0.0.1', port))


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
        self.env = dict(os.environ, PGPASSFILE=str(self.pgpass), PGCONNECT_TIMEOUT='2')
        self.config = dict(synchronous_commit='on', synchronous_standby_names='FIRST 1 (provisiond_sync)',
                           max_connections=24, shared_buffers='32MB', wal_level='replica',
                           archive_mode='on', restart_after_crash='on', ports=[55410,55411,55412],
                           authentication='scram-sha-256 loopback only', standby_role='replicator')
        self.archive = self.root / 'archive'
        self.archive.mkdir()
        self.event('invocation', seed=seed, specification=SPEC, phase=phase,
                   configuration=self.config,
                   configuration_sha256=hashlib.sha256(json.dumps(self.config, sort_keys=True).encode()).hexdigest())
        self.source = self.run(['git', 'rev-parse', 'HEAD']).stdout.strip()
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
        self.versions['postgres_path'] = str(Path(shutil.which('postgres')).resolve())
        self.versions['nixpkgs'] = 'aff8a0b28396750446e5537a96461bc4facdb287:postgresql_18'

    def event(self, kind, **data):
        e = dict(event=kind, timestamp=time.time(), elapsed=time.monotonic()-self.started, **data)
        self.events.append(e)
        with (self.root / 'events.jsonl').open('a') as f:
            f.write(json.dumps(e, sort_keys=True)+'\n')

    def run(self, cmd, *, sql=None, ok=True, timeout=45):
        # SQL/passwords are intentionally omitted from command manifests.
        r = subprocess.run(cmd, input=sql, env=self.env, capture_output=True, text=True, timeout=timeout)
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
                  '--auth=scram-sha-256','--no-locale','--encoding=UTF8'])
        # Offline cluster initialization, before any server accepts connections.
        # Like initdb's catalog initialization, this creates only the replication
        # identity. synchronous_commit stays on; no component or migration exists.
        self.run(['postgres','--single','-D',str(data),'postgres'],
                 sql=f"CREATE ROLE replicator LOGIN REPLICATION PASSWORD '{self.passwords['replicator']}';\n")
        (data / 'pg_hba.conf').write_text(
            'host replication replicator 127.0.0.1/32 scram-sha-256\n'
            'host all all 127.0.0.1/32 scram-sha-256\n')
        self.configure(data, 55410)
        self.nodes['primary'] = dict(data=data, port=55410)
        self.start('primary')
        self.backup('standby', standby=True)
        self.nodes['standby'] = dict(data=self.root/'standby', port=55411)
        self.configure(self.root/'standby', 55411)
        self.start('standby')
        eventually(lambda: self.sql("SELECT count(*)=1 FROM pg_stat_replication WHERE application_name='provisiond_sync' AND sync_state='sync' AND state='streaming'")=='t')
        setup = '\n'.join(f"CREATE ROLE {r} LOGIN NOSUPERUSER PASSWORD '{self.passwords[r]}';" for r in self.passwords if r not in ('replicator','administrator'))
        self.sql(setup)
        self.sql('GRANT pg_read_all_stats TO observer;')
        self.identity()
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
        free_port(n['port'])
        log = (self.root / f'{name}.log').open('ab', buffering=0)
        n['process'] = subprocess.Popen(['postgres','-D',str(n['data'])], env=self.env,
                                         stdout=log, stderr=log)
        log.close()
        eventually(lambda: self.sql('SELECT 1',node=name,role=role,ok=False)=='1')
        self.event('server_started', node=name, pid=n['process'].pid, port=n['port'])

    def stop(self, name, mode='fast'):
        n = self.nodes[name]
        if n.get('process') and n['process'].poll() is None:
            self.run(['pg_ctl','-D',str(n['data']),'stop','-m',mode,'-w','-t','15'])
            n['process'].wait(timeout=5)
            self.event('server_stopped', node=name, mode=mode)

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
        self.event('backup_requested', destination=name, wal_mode='stream', application_name='bootstrap_backup')
        self.run(['pg_basebackup','-d','host=127.0.0.1 port=55410 user=replicator dbname=postgres application_name=bootstrap_backup',
                  '-D',str(dest),'-X','stream','--checkpoint=fast','--no-password'])
        self.run(['pg_verifybackup',str(dest)])
        if standby:
            (dest/'standby.signal').touch()
            # Set the final application name and authenticated role before startup.
            (dest/'postgresql.auto.conf').write_text(
                f"primary_conninfo = 'host=127.0.0.1 port=55410 user=replicator application_name=provisiond_sync passfile={self.pgpass}'\n")
        self.event('backup_complete', destination=name, verified=True)

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
        self.nodes['standby'] = dict(data=self.root/'rebuilt',port=55411)
        self.configure(self.root/'rebuilt',55411)
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
        self.configure(restore,55412)
        (restore/'postgresql.auto.conf').write_text(f"restore_command = 'cp {self.archive}/%f %p'\n")
        (restore/'standby.signal').touch()
        self.nodes['restored'] = dict(data=restore,port=55412)
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
        paths.update(self.root/n for n in ['restore-backup','standby','rebuilt'])
        paths.update(getattr(self,'owned_extra_paths',[]))
        for path in sorted(paths):
            if path.exists() and not errors:
                shutil.rmtree(path)
                self.event('cleanup_removed', path=str(path))
        for path in [self.pgpass,self.root/'init-password']:
            if path.exists():
                path.unlink()
        self.event('cleanup_complete', result='fail' if errors else 'pass', errors=errors,
                   raw_evidence=str(self.root))
        return not errors

    def manifest(self, status):
        return dict(result=status, specification_commit=SPEC, implementation_commit=self.source,
                    tested_source_sha256=self.tree, source_files=self.source_files,
                    seed=self.seed, versions=self.versions, configuration=self.config,
                    configuration_sha256=hashlib.sha256(json.dumps(self.config,sort_keys=True).encode()).hexdigest(),
                    command=['nix','develop','--command','python3',*sys.argv], commands=self.commands,
                    scenarios=self.results, events=self.events, raw_evidence=str(self.root),
                    external_provider_rail_evidence='not applicable: no external effects',
                    limitations=['local processes only; no separate-host durability or fencing proof',
                                 'no product recovery incident, engine, ledger or production alarm implementation',
                                 'DB-10 full saturation: not run (W8)'])


def main():
    ap=argparse.ArgumentParser()
    ap.add_argument('--phase',choices=['w1','checks','commits'],default='w1')
    ap.add_argument('--seed',type=int,default=20261105)
    ap.add_argument('--evidence',type=Path)
    args=ap.parse_args()
    h=Harness(args.phase,args.seed)
    status='fail'
    try:
        if args.phase=='w1':
            h.drill()
        elif args.phase=='checks':
            from scenarios import check_scenarios
            h.run(['cargo','build','--example','store-probe','--features','store-test-hooks','--locked'],timeout=120)
            check_scenarios(h)
        else:
            raise ValueError('phase not implemented')
        status='pass'
    except BaseException as e:
        h.event('failure',type=type(e).__name__,message=str(e))
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
