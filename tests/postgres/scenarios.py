"""Real-client STO-7/CNF-56/CNF-365 scenarios. No product business transitions."""
import json
import os
from pathlib import Path
import queue
import signal
import subprocess
import threading
import time
from harness import eventually
from proxy import Proxy


class Probe:
    def __init__(self,h,role='app',password=None):
        self.h=h
        self.events=[]
        self.queue=queue.Queue()
        self.index=len(getattr(h,'probes',[]))
        env=dict(h.env,STORE_CONNECTION=f"host=127.0.0.1 port=55413 user={role} password={password or h.passwords[role]} dbname=postgres sslmode=disable")
        stderr=(h.root/f'client-{self.index}.stderr').open('w')
        self.p=subprocess.Popen(['target/debug/examples/store-probe'],stdin=subprocess.PIPE,
                                stdout=subprocess.PIPE,stderr=stderr,text=True,env=env,bufsize=1)
        stderr.close()
        self.thread=threading.Thread(target=self.read)
        self.thread.start()
        h.probes.append(self)
        h.event('client_process_started',pid=self.p.pid,probe=self.index)

    def read(self):
        for line in self.p.stdout:
            try:
                e=json.loads(line)
            except ValueError:
                e=dict(event='invalid_client_output',line=line)
            self.events.append(e)
            self.h.event('client_observation',probe=self.index,message=e)
            self.queue.put(e)

    def send(self,command):
        self.p.stdin.write(command+'\n')
        self.p.stdin.flush()

    def result(self,command,ok=True):
        end=time.monotonic()+12
        while time.monotonic()<end:
            e=self.queue.get(timeout=max(.01,end-time.monotonic()))
            if e.get('event')=='result' and e['command']==command:
                assert e['ok']==ok,e
                return e
        raise AssertionError('missing client result: '+command)

    def command(self,command,ok=True):
        self.send(command)
        return self.result(command.split()[0],ok)

    def finish(self):
        if self.p.poll() is None:
            self.send('exit')
            self.p.stdin.close()
            status=self.p.wait(timeout=8)
        else:
            status=self.p.returncode
        self.thread.join(timeout=3)
        self.h.commands.append(dict(command=['target/debug/examples/store-probe'],exit_status=status))
        assert status==0,(status,(self.h.root/f'client-{self.index}.stderr').read_text())


def prepare(h):
    h.init()
    h.sql(Path('tests/postgres/privileges.sql').read_text())
    h.probes=[]
    h.paused=[]
    h.extra=[]
    h.proxy=Proxy(h)


def pause_standby(h):
    pid=int(h.sql('SELECT pid FROM pg_stat_wal_receiver',node='standby'))
    parent=int(Path(f'/proc/{pid}/stat').read_text().split(') ',1)[1].split()[1])
    assert parent==h.nodes['standby']['process'].pid
    h.event('fault_requested',fault='standby_receiver_pause',pid=pid)
    os.kill(pid,signal.SIGSTOP)
    eventually(lambda: Path(f'/proc/{pid}/stat').read_text().split(') ',1)[1].startswith('T '))
    h.paused.append(pid)
    h.event('fault_activated',fault='standby_receiver_pause',pid=pid)
    return pid


def resume_standby(h):
    for pid in h.paused:
        os.kill(pid,signal.SIGCONT)
        h.event('fault_released',fault='standby_receiver_pause',pid=pid)
    h.paused=[]
    h.identity()


def syncrep(h):
    value=h.sql("SELECT pid FROM pg_stat_activity WHERE wait_event='SyncRep' AND usename IN ('app','administrator') ORDER BY pid")
    return [int(v) for v in value.splitlines() if v]


def fixture_write_wait(h,id):
    log=(h.root/f'fixture-{id}.log').open('w')
    p=subprocess.Popen(['psql','-XAt','-v','ON_ERROR_STOP=1','-h','127.0.0.1','-p','55410','-U','administrator','-d','postgres'],
                       stdin=subprocess.PIPE,stdout=log,stderr=log,text=True,env=h.env)
    log.close()
    h.extra.append(p)
    p.stdin.write(f"BEGIN; INSERT INTO fixture.destinations VALUES ({id},'destination-{id}'); COMMIT;\n")
    p.stdin.close()
    pid=eventually(lambda: next(iter(syncrep(h)),None))
    h.event('server_commit_received',session=pid,wait_event='SyncRep',fixture=id)
    return p,pid


def negative_cancel(h,p,pid):
    h.event('fault_requested',fault='deliberate_contract_violation_backend_cancel',session=pid)
    assert h.sql(f'SELECT pg_cancel_backend({pid})')=='t'
    status=p.wait(timeout=10)
    assert status==0
    h.event('fault_activated',fault='deliberate_contract_violation_backend_cancel',session=pid,
            independently_visible=True)
    h.commands.append(dict(command=['psql','isolated fixture transaction'],exit_status=status))


def check_scenarios(h):
    prepare(h)
    p=Probe(h)
    admitted=p.command('admit')['value']['session']
    settings=p.command('settings')['value']['settings']
    expected=['statement_timeout=1s','lock_timeout=500ms','idle_in_transaction_session_timeout=1s',
              'default_transaction_isolation=read committed','search_path=pg_catalog, fixture',
              'synchronous_commit=on','tcp_keepalives_idle=2','tcp_keepalives_interval=1',
              'tcp_keepalives_count=2','synchronous_standby_names=FIRST 1 (provisiond_sync)']
    assert settings==expected,settings
    p.command('ping')
    h.results.append(dict(test='fresh-pooled-settings-and-idle-check',result='pass',session=admitted,settings=settings))
    p.command('abandon')
    assert h.sql('SELECT count(*) FROM fixture.destinations WHERE id=700')=='0'
    p.command('failed')
    gate=h.proxy.arm('SELECT pg_sleep(30)','send')
    p.command('pre_start')
    old=gate.wait()['pid']
    replacement=p.command('pre_cancel')['value']['new_session']
    assert replacement!=old
    gate.release()
    h.results.append(dict(test='precommit-abandon-failure-cancellation',result='pass',
                         establishes='uncommitted insert absent; aborted/cancelled sessions closed, never recycled dirty'))

    # Least access: app sees only names in the raw stats view; function is narrow.
    assert h.sql("SELECT flush_lsn IS NULL FROM pg_stat_replication WHERE application_name='provisiond_sync'",role='app')=='t'
    for sql in ["SELECT pg_read_file('postgresql.conf')",'SELECT pg_reload_conf()',
                'SET ROLE stats_reader','CREATE TABLE store_guard.forbidden(id int)']:
        port=h.nodes['primary']['port']
        r=h.run(['psql','-XAt','-v','ON_ERROR_STOP=1','-h','127.0.0.1','-p',str(port),'-U','app','-d','postgres'],sql=sql,ok=False)
        assert r.returncode==3 and 'permission denied' in r.stderr,r.stderr
    d=Probe(h,role='denied')
    d.command('admit',ok=False)
    d.finish()
    h.results.append(dict(test='DB-05-denied-and-privilege-boundaries',result='pass',
                         grants='app: USAGE store_guard; EXECUTE store_guard.sample(); no pg_read_all_stats or owner membership'))
    # A real visibility failure, not an invented NULL adapter response.
    h.sql('ALTER FUNCTION store_guard.sample() SECURITY INVOKER')
    n=Probe(h)
    failure=n.command('admit',ok=False)
    assert 'NULL' in failure['error'],failure
    n.finish()
    h.sql('ALTER FUNCTION store_guard.sample() SECURITY DEFINER')
    bad=Probe(h,password='deliberately-wrong-run-local-password')
    bad.command('admit',ok=False)
    bad.finish()
    h.results.append(dict(test='DB-05-NULL-and-authentication',result='pass',
                         injection='real server SECURITY INVOKER hides stats; separate wrong password'))
    # Fixed admission target completes while independent acknowledged writes advance WAL.
    active=threading.Event()
    errors=[]
    def writer():
        try:
            i=100
            while not active.is_set():
                h.sql(f"INSERT INTO fixture.destinations VALUES ({i},'active')",role='app')
                i+=1
        except Exception as e:
            errors.append(str(e))
    t=threading.Thread(target=writer)
    h.background=[(active,t)]
    t.start()
    a=Probe(h)
    a.command('admit')
    a.finish()
    active.set()
    t.join(timeout=12)
    assert not t.is_alive() and not errors,errors
    h.results.append(dict(test='active-primary-fixed-target',result='pass'))
    # DB-06: a deliberate prohibited cancellation makes a later destination visible
    # on the primary while its sole intended standby is below that commit.
    pause_standby(h)
    writer,pid=fixture_write_wait(h,6)
    negative_cancel(h,writer,pid)
    assert h.sql('SELECT destination FROM fixture.destinations WHERE id=6')=='destination-6'
    post=h.proxy.arm('store post_read target','send')
    p.send('read 6')
    c=post.wait()
    assert c['pid']==admitted, 'reader must preserve its uninterrupted physical session'
    assert any(e.get('event')=='destination_read' for e in p.events)
    post.release()
    eventually(lambda: any(e.get('event')=='check_observation' and e.get('kind')=='post_read' for e in p.events))
    observations=[e for e in p.events if e.get('event')=='check_observation' and e.get('kind')=='post_read']
    assert observations and not any(e.get('event')=='destination_exposed' for e in p.events)
    def lsn(value):
        hi,lo=value.split('/')
        return (int(hi,16)<<32)|int(lo,16)
    old_target=next(e['target'] for e in p.events if e.get('event')=='check_target' and e['kind']=='admission')
    assert lsn(observations[0]['target'])>lsn(old_target)
    assert lsn(observations[0]['observed'][6])<lsn(observations[0]['target'])
    assert len({e['target'] for e in observations})==1
    resume_standby(h)
    assert p.result('read')['value']['value']=='destination-6'
    h.results.append(dict(test='DB-06',result='pass',reader_session=admitted,
                         fixture='deliberate contract violation: cancel isolated SyncRep backend; no sync settings lowered',
                         establishes='post-read fixed target blocks exposure on the original physical session until catch-up'))
    # DB-07: break after SELECT by blocking post-read target transmission.
    gate=h.proxy.arm('store post_read target','send')
    q=Probe(h)
    q.send('read 6')
    c=gate.wait()
    first=c['pid']
    h.proxy.close_connection(c)
    gate.release()
    assert q.result('read')['value']['value']=='destination-6'
    reads=[e for e in q.events if e.get('event')=='destination_read']
    assert len(reads)==2 and reads[0]['session']==first and reads[1]['session']!=first,reads
    assert [e['attempt'] for e in q.events if e.get('event')=='destination_exposed']==[2]
    q.finish()
    h.results.append(dict(test='DB-07',result='pass',attempts=reads,establishes='failed value discarded; SELECT/check repeated on new admitted session'))
    # DB-09: actual target reply is withheld, through the read call deadline.
    gate=h.proxy.arm('store admission target','reply')
    q=Probe(h)
    before=len(h.events)
    q.send('admit')
    c=gate.wait()
    assert h.sql("SELECT count(*) FROM pg_stat_activity WHERE wait_event='SyncRep'")=='0'
    started=time.monotonic()
    failure=q.result('admit',ok=False)
    assert 'deadline' in failure['error'],failure
    assert time.monotonic()-started < 4
    assert not any(e.get('event')=='session_admitted' for e in q.events)
    eventually(lambda: any(e['event']=='transport_closed' and e.get('session')==c['pid'] for e in h.events[before:]))
    gate.release()
    q.finish()
    h.results.append(dict(test='DB-09',result='pass',session=c['pid'],establishes='reply withheld and unused transport closed at deadline; no check writes/SyncRep waiter'))
    p.finish()
    # Real zero designated senders.
    h.stop('standby')
    q=Probe(h)
    failure=q.command('admit',ok=False)
    assert 'got 0' in failure['error'],failure
    q.finish()
    h.start('standby')
    h.identity()
    h.results.append(dict(test='DB-05-zero',result='pass',injection='sole standby stopped'))
    replication_negatives(h)
    assert h.proxy.cancel_requests==0
    h.results.append(dict(test='checks-no-client-cancel',result='pass',observed_cancel_packets=0))


def replication_negatives(h):
    # Isolated replication-protocol stream, explicitly not a second physical standby.
    def receiver(name,role='replicator'):
        path=h.root/f'wal-stream-{len(h.extra)}'
        path.mkdir()
        h.owned_extra_paths=getattr(h,'owned_extra_paths',[])+[path]
        log=(h.root/f'wal-stream-{len(h.extra)}.log').open('w')
        p=subprocess.Popen(['pg_receivewal','--no-loop','--synchronous','-D',str(path),
                            '-d',f'host=127.0.0.1 port=55410 user={role} application_name={name}'],
                           env=h.env,stdout=log,stderr=log)
        log.close()
        h.extra.append(p)
        return p
    duplicate=receiver('provisiond_sync')
    eventually(lambda: h.sql("SELECT count(*) FROM pg_stat_replication WHERE application_name='provisiond_sync'")=='2')
    q=Probe(h)
    failure=q.command('admit',ok=False)
    assert 'got 2' in failure['error'],failure
    q.finish()
    duplicate.terminate()
    duplicate.wait(timeout=5)
    h.event('replication_protocol_negative_complete',case='duplicate name',physical_standbys=1)
    h.results.append(dict(test='DB-05-duplicate',result='pass',injection='isolated pg_receivewal protocol connection; not another physical standby'))
    pause_standby(h)
    wrong=receiver('bootstrap_backup')
    eventually(lambda: h.sql("SELECT count(*) FROM pg_stat_replication WHERE application_name='bootstrap_backup' AND flush_lsn IS NOT NULL")=='1')
    writer,pid=fixture_write_wait(h,56)
    negative_cancel(h,writer,pid)
    target=h.sql('SELECT pg_current_wal_flush_lsn()')
    eventually(lambda: h.sql(f"SELECT flush_lsn >= '{target}'::pg_lsn FROM pg_stat_replication WHERE application_name='bootstrap_backup'")=='t')
    # Measure pg_basebackup's own streaming sender too, rather than treating a
    # different replication utility as pg_basebackup evidence.
    backup_path=h.root/'identity-backup'
    h.owned_extra_paths.append(backup_path)
    backup_log=(h.root/'identity-backup.log').open('w')
    backup=subprocess.Popen(['pg_basebackup','-d','host=127.0.0.1 port=55410 user=replicator application_name=bootstrap_probe',
                             '-D',str(backup_path),'-X','stream','--checkpoint=fast','--max-rate=1M','--no-password'],
                            env=h.env,stdout=backup_log,stderr=backup_log)
    backup_log.close()
    h.extra.append(backup)
    eventually(lambda: h.sql("SELECT count(*) FROM pg_stat_replication WHERE application_name='bootstrap_probe' AND state='streaming'")=='1')
    h.event('backup_sender_observed',observation=h.sql("SELECT application_name,state,coalesce(flush_lsn::text,'NULL') FROM pg_stat_replication WHERE application_name='bootstrap_probe'"),
            note='NULL is reported as unknown; only the independent pg_receivewal stream asserts a later durable flush')
    q=Probe(h)
    failure=q.command('admit',ok=False)
    assert 'deadline' in failure['error'],failure
    assert not any(e.get('event')=='session_admitted' for e in q.events)
    q.finish()
    wrong.terminate()
    wrong.wait(timeout=5)
    resume_standby(h)
    status=backup.wait(timeout=45)
    h.commands.append(dict(command=['pg_basebackup','-X','stream','--max-rate=1M','application_name=bootstrap_probe'],exit_status=status))
    assert status==0,(h.root/'identity-backup.log').read_text()
    h.run(['pg_verifybackup',str(backup_path)])
    h.results.append(dict(test='DB-05-non-designated',result='pass',
                         injection='caught-up pg_receivewal backup-named protocol stream; designated physical receiver paused',
                         establishes='non-designated later flush never substitutes for designated peer'))
    # Wrong authenticated identity with the right name, using a distinct replication role.
    h.sql('ALTER ROLE observer REPLICATION')
    h.stop('standby')
    # HBA grants replication only to replicator: authentication/role spoof fails.
    fake=receiver('provisiond_sync','observer')
    status=fake.wait(timeout=5)
    assert status!=0
    q=Probe(h)
    q.command('admit',ok=False)
    q.finish()
    h.start('standby')
    h.identity()
    h.sql('ALTER ROLE observer NOREPLICATION')
    # Focused server adapter for identity predicate (not a topology proof).
    body=Path('tests/postgres/privileges.sql').read_text()
    definition=body[body.index('CREATE FUNCTION'):body.index('ALTER FUNCTION')]
    definition=definition.replace('CREATE FUNCTION','CREATE OR REPLACE FUNCTION').replace('SELECT pid, usename,','SELECT pid, \'observer\'::name,')
    h.sql(definition)
    q=Probe(h)
    failure=q.command('admit',ok=False)
    assert 'wrong designated identity' in failure['error'],failure
    q.finish()
    original=body[body.index('CREATE FUNCTION'):body.index('ALTER FUNCTION')].replace('CREATE FUNCTION','CREATE OR REPLACE FUNCTION')
    h.sql(original)
    h.results.append(dict(test='DB-05-wrong-identity',result='pass',
                         injection='real replication HBA rejection; separate SQL function adapter tests wrong-role predicate'))


def cleanup_clients(h):
    errors=[]
    for stop,thread in getattr(h,'background',[]):
        stop.set()
        thread.join(timeout=15)
        if thread.is_alive():
            errors.append('writer thread did not stop')
    for pid in getattr(h,'paused',[]):
        try:
            os.kill(pid,signal.SIGCONT)
        except ProcessLookupError:
            pass
    h.paused=[]
    for p in getattr(h,'probes',[]):
        try:
            p.finish()
        except Exception as e:
            errors.append(str(e))
            if p.p.poll() is None:
                p.p.kill()
                p.p.wait(timeout=5)
                p.thread.join(timeout=3)
    if hasattr(h,'proxy'):
        try:
            h.proxy.close()
        except Exception as e:
            errors.append(str(e))
    for p in getattr(h,'extra',[]):
        if p.poll() is None:
            p.terminate()
            try:
                p.wait(timeout=5)
            except subprocess.TimeoutExpired:
                p.kill()
                p.wait(timeout=5)
    return errors
