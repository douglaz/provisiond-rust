"""Real-client STO-7/CNF-56/CNF-365 scenarios. No product business transitions."""
import json
import os
from pathlib import Path
import queue
import signal
import subprocess
import threading
import time
from harness import eventually, cap_check
from proxy import Proxy


class Probe:
    def __init__(self,h,role='app',password=None,defect=None):
        cap_check()
        self.expected_status=0
        self.finished=False
        self.h=h
        self.events=[]
        self.queue=queue.Queue()
        self.index=len(getattr(h,'probes',[]))
        env=dict(h.env,STORE_CONNECTION=f"host=127.0.0.1 port=55413 user={role} password={password or h.passwords[role]} dbname=postgres sslmode=disable")
        if defect:
            env['STORE_TEST_DEFECT']=defect
        self.defect=defect
        stderr=(h.root/f'client-{self.index}.stderr').open('w')
        self.p=subprocess.Popen(['target/debug/examples/store-probe'],stdin=subprocess.PIPE,
                                stdout=subprocess.PIPE,stderr=stderr,text=True,env=env,bufsize=1)
        stderr.close()
        self.thread=threading.Thread(target=self.read)
        self.thread.start()
        h.probes.append(self)
        h.event('client_process_started',pid=self.p.pid,probe=self.index,test_defect=defect)

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
                assert ok is None or e['ok']==ok,e
                return e
        raise AssertionError('missing client result: '+command)

    def command(self,command,ok=True):
        self.send(command)
        return self.result(command.split()[0],ok)

    def finish(self):
        if self.finished:
            return
        if self.p.poll() is None:
            self.send('exit')
            self.p.stdin.close()
            status=self.p.wait(timeout=8)
        else:
            status=self.p.returncode
        self.thread.join(timeout=3)
        self.h.commands.append(dict(command=['target/debug/examples/store-probe'],test_defect=self.defect,exit_status=status))
        self.finished=True
        assert status==self.expected_status,(status,(self.h.root/f'client-{self.index}.stderr').read_text())


def prepare(h):
    h.init()
    h.sql(Path('tests/postgres/privileges.sql').read_text())
    h.sql('CREATE TABLE fixture.deferred(value integer UNIQUE DEFERRABLE INITIALLY DEFERRED); GRANT INSERT ON fixture.deferred TO app;',role='migrator')
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
    h.paused.append((pid,parent,Path(f'/proc/{pid}/stat').read_text().split(') ',1)[1].split()[19]))
    h.event('fault_activated',fault='standby_receiver_pause',pid=pid)
    return pid


def resume_standby(h):
    for pid,parent,start in h.paused:
        fields=Path(f'/proc/{pid}/stat').read_text().split(') ',1)[1].split()
        assert int(fields[1])==parent and fields[19]==start,'paused process identity changed'
        os.kill(pid,signal.SIGCONT)
        h.event('fault_released',fault='standby_receiver_pause',pid=pid)
    h.paused=[]
    h.identity()


def syncrep(h):
    value=h.sql("SELECT pid FROM pg_stat_activity WHERE wait_event='SyncRep' AND usename IN ('app','administrator') ORDER BY pid")
    return [int(v) for v in value.splitlines() if v]


def fixture_write_wait(h,id):
    log=(h.root/f'fixture-{id}.log').open('w')
    cap_check()
    p=subprocess.Popen(['psql','-XAt','-v','ON_ERROR_STOP=1','-h','127.0.0.1','-p','55410','-U','administrator','-d','postgres'],
                       stdin=subprocess.PIPE,stdout=log,stderr=log,text=True,env=h.env)
    log.close()
    h.extra.append(p)
    p.recorded_command=dict(command=p.args,exit_status=None)
    h.commands.append(p.recorded_command)
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
        cap_check()
        p=subprocess.Popen(['pg_receivewal','--no-loop','--synchronous','-D',str(path),
                            '-d',f'host=127.0.0.1 port=55410 user={role} application_name={name}'],
                           env=h.env,stdout=log,stderr=log)
        log.close()
        h.extra.append(p)
        p.recorded_command=dict(command=p.args,exit_status=None)
        h.commands.append(p.recorded_command)
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
    cap_check()
    backup=subprocess.Popen(['pg_basebackup','-d','host=127.0.0.1 port=55410 user=replicator application_name=bootstrap_probe',
                             '-D',str(backup_path),'-X','stream','--checkpoint=fast','--max-rate=1M','--status-interval=1','--no-password'],
                            env=h.env,stdout=backup_log,stderr=backup_log)
    backup_log.close()
    h.extra.append(backup)
    backup.recorded_command=dict(command=backup.args,exit_status=None)
    h.commands.append(backup.recorded_command)
    eventually(lambda: h.sql("SELECT count(*) FROM pg_stat_replication WHERE application_name='bootstrap_probe' AND state='streaming'")=='1')
    h.event('backup_sender_observed',observation=h.sql("SELECT application_name,state,coalesce(flush_lsn::text,'NULL') FROM pg_stat_replication WHERE application_name='bootstrap_probe'"),
            note='backup-command NULL is unknown; streaming sender is separately checked through the exact admission target')
    q=Probe(h)
    q.send('admit')
    eventually(lambda:any(e.get('event')=='check_target' for e in q.events))
    exact=next(e['target'] for e in q.events if e.get('event')=='check_target')
    # Physical-backup infrastructure: finish the current WAL segment so the
    # backup's streaming sender can report a durable prefix beyond this fixed
    # target. No component row/transaction or replication setting changes.
    h.event('backup_wal_segment_release_requested',target=exact)
    switched=h.sql('SELECT pg_switch_wal()')
    h.event('backup_wal_segment_release_observed',target=exact,switch_lsn=switched)
    eventually(lambda:h.sql(f"SELECT flush_lsn >= '{exact}'::pg_lsn FROM pg_stat_replication WHERE application_name='bootstrap_probe' AND state='streaming'")=='t',seconds=2)
    assert not any(e.get('event')=='result' for e in q.events),'attempt ended before the non-designated backup reached its target'
    h.event('non_designated_backup_caught_up',target=exact,
            senders=h.sql("SELECT application_name,state,coalesce(flush_lsn::text,'NULL') FROM pg_stat_replication ORDER BY application_name,state"),
            attempted_session=next(e['session'] for e in q.events if e.get('event')=='check_target'))
    failure=q.result('admit',ok=False)
    assert 'deadline' in failure['error'],failure
    assert not any(e.get('event')=='session_admitted' for e in q.events)
    q.finish()
    wrong.terminate()
    wrong.wait(timeout=5)
    resume_standby(h)
    status=backup.wait(timeout=45)
    h.commands.append(dict(command=['pg_basebackup','-X','stream','--max-rate=1M','--status-interval=1','application_name=bootstrap_probe'],exit_status=status))
    assert status==0,(h.root/'identity-backup.log').read_text()
    h.run(['pg_verifybackup',str(backup_path)])
    h.results.append(dict(test='DB-05-non-designated',result='pass',
                         injection='pg_basebackup streaming sender reached the exact admission target while its connection check remained pending; extra pg_receivewal stream; designated receiver paused',
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
    for pid,parent,start in getattr(h,'paused',[]):
        try:
            fields=Path(f'/proc/{pid}/stat').read_text().split(') ',1)[1].split()
            if int(fields[1])==parent and fields[19]==start:
                os.kill(pid,signal.SIGCONT)
        except (ProcessLookupError,FileNotFoundError):
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
        if hasattr(p,'recorded_command'):
            p.recorded_command['exit_status']=p.returncode
    return errors


def wait_commit(h,p,pid):
    eventually(lambda: pid in syncrep(h))
    h.event('server_commit_received',probe=p.index,session=pid,wait_event='SyncRep')
    assert any(e.get('event')=='owner_accepted' and e['session']==pid for e in p.events)
    assert not any(e.get('event')=='commit_classified' and e['session']==pid for e in p.events)


def classified(p,pid=None):
    return [e for e in p.events if e.get('event')=='commit_classified' and (pid is None or e['session']==pid)]


def commit_scenarios(h):
    prepare(h)
    # A clean complete COMMIT and a proven pre-COMMIT rejection are separate.
    p=Probe(h)
    pid=p.command('prepare 1001')['value']['session']
    p.command('commit')
    assert p.command('await')['value']['outcome']=='acknowledged'
    assert classified(p)[-1]['command_tag']=='COMMIT'
    same=p.command('prepare 1002')['value']['session']
    assert same==pid, 'successful terminal transaction should allow clean reuse'
    p.command('rollback')
    p.command('reject_failed')
    p.command('deferred')
    assert p.command('await')['value']['outcome']=='rejected'
    assert h.sql('SELECT count(*) FROM fixture.deferred')=='0'
    p.finish()
    h.results.append(dict(test='commit-command-and-rejection-semantics',result='pass',
                         establishes='COMMIT plus complete idle reply acknowledges; failed pre-COMMIT and deferred constraint rejection do not'))
    # Retention defect frees the original checkout only after real receipt and cancellation.
    p=Probe(h,defect='retention')
    pid=p.command('prepare 1250')['value']['session']
    pause_standby(h)
    p.command('commit')
    wait_commit(h,p,pid)
    p.command('drop_request')
    eventually(lambda:any(e.get('event')=='defect_checkout_released' for e in p.events))
    assert pid in syncrep(h)
    p.expected_status=42
    p.send('assert_retained')
    status=p.p.wait(timeout=8)
    p.thread.join(timeout=2)
    assert status==42 and any(e.get('assertion')=='original write checkout retained' for e in p.events)
    resume_standby(h)
    p.finish()
    h.results.append(dict(test='retention-defect-control',result='expected negative demonstration',exit_status=42,
                         assertion='original write checkout retained',establishes='exact same green retention assertion detects original-checkout loss after observed SyncRep'))
    # Two independent ordinary client pools exercise the engine/API database arms
    # without starting product components or implementing their caller policies.
    engine=Probe(h)
    api=Probe(h)
    api.command('admit')
    ep=engine.command('prepare 1103')['value']['session']
    ap=api.command('prepare 1104')['value']['session']
    pause_standby(h)
    engine.command('commit')
    api.command('commit')
    wait_commit(h,engine,ep)
    wait_commit(h,api,ap)
    api.command('ping')
    assert ep in syncrep(h) and ap in syncrep(h)
    resume_standby(h)
    assert engine.command('await')['value']['outcome']=='acknowledged'
    assert api.command('await')['value']['outcome']=='acknowledged'
    engine.finish()
    api.finish()
    h.results.append(dict(test='DB-01-independent-client-pools',result='pass',engine_fixture_backend=ep,api_fixture_backend=ap,
                         establishes='both state-changing fixture commits simultaneously wait for the sole designated standby; pre-admitted read remains usable',
                         not_run='product engine/API business workflows and alarms'))
    # DB-01/02: retain an uninterrupted healthy read session and separate budget.
    for number,cancellation in enumerate(['timeout_request','drop_request','cancel_worker'],start=1100):
        p=Probe(h)
        reader=p.command('admit')['value']['session']
        pid=p.command(f'prepare {number}')['value']['session']
        pause_standby(h)
        p.command('commit')
        wait_commit(h,p,pid)
        if cancellation=='timeout_request':
            assert p.command('past_deadline')['value']['checked_out']==1
            assert pid in syncrep(h) and not classified(p,pid)
            p.command('ping')
            # A new physical session needs a new check and must fail while behind.
            new=Probe(h)
            new.command('admit',ok=False)
            assert not any(e.get('event')=='session_admitted' for e in new.events)
            new.finish()
        p.command(cancellation)
        p.command('recycle')
        p.command('assert_retained')
        assert pid in syncrep(h) and not classified(p,pid)
        assert any(c['pid']==pid and not c['closed'] for c in h.proxy.connections), 'original transport must remain live'
        p.command('ping')
        assert [e['session'] for e in p.events if e.get('event')=='session_admitted' and e['session']==reader]==[reader]
        resume_standby(h)
        eventually(lambda: classified(p,pid))
        assert classified(p,pid)[-1]['outcome']=='acknowledged'
        assert h.sql(f'SELECT destination FROM fixture.destinations WHERE id={number}')==f'client-{number}'
        p.finish()
        h.results.append(dict(test='DB-02-'+cancellation,result='pass',session=pid,read_session=reader,
                             establishes='real SyncRep preceded cancellation; original checkout retained, extra write refused, same read session usable; clean terminal result after release'))
    h.results.append(dict(test='DB-01',result='pass',
                         establishes='state-changing real client COMMIT waits beyond server/client call bounds; healthy read session still usable; fresh checks fail closed',
                         not_run='engine retry, funding/settlement and production alarm arms'))
    # DB-08 green and exact failing warning control against the same assertion.
    for defect,number in [('warning',1201),(None,1200)]:
        p=Probe(h,defect=defect)
        pid=p.command(f'prepare {number}')['value']['session']
        pause_standby(h)
        p.command('commit')
        wait_commit(h,p,pid)
        h.event('fault_requested',fault='deliberate_contract_violation_backend_cancel',session=pid)
        assert h.sql(f'SELECT pg_cancel_backend({pid})')=='t'
        if defect:
            p.expected_status=43
            p.send('assert_unacknowledged')
            status=p.p.wait(timeout=8)
            p.thread.join(timeout=2)
            assert status==43 and any(e.get('assertion')=='replication warning must not acknowledge' for e in p.events)
            result='expected negative demonstration'
        else:
            assert p.command('assert_unacknowledged')['value']['outcome']=='unresolved'
            result='pass'
        warnings=[e for e in p.events if e.get('event')=='driver_notice' and e.get('severity')=='WARNING']
        assert warnings and warnings[-1]['detail']=='The transaction has already committed locally, but might not have been replicated to the standby.',warnings
        assert classified(p,pid)[-1]['command_tag']=='COMMIT'
        assert h.sql(f'SELECT destination FROM fixture.destinations WHERE id={number}')==f'client-{number}'
        h.event('fault_activated',fault='real_replication_warning',session=pid,
                diagnostic=warnings[-1],apparent_completion='COMMIT',control=defect)
        resume_standby(h)
        p.finish()
        h.results.append(dict(test='DB-08' if not defect else 'warning-defect-control',result=result,
                             exit_status=0 if not defect else 43,assertion='replication warning must not acknowledge',
                             diagnostic=warnings[-1],establishes='actual driver warning plus COMMIT completion; intentional defect fails exact assertion'))
    # Lost terminal reply: database effect is independently visible, client is unresolved.
    p=Probe(h)
    pid=p.command('prepare 1300')['value']['session']
    gate=h.proxy.arm('COMMIT /* store COMMIT */','commit_reply')
    p.command('commit')
    c=gate.wait()
    assert c['pid']==pid
    assert h.sql('SELECT destination FROM fixture.destinations WHERE id=1300')=='client-1300'
    assert not classified(p,pid)
    h.proxy.close_connection(c)
    gate.release()
    assert p.command('await')['value']['outcome']=='unresolved'
    new=p.command('prepare 1301')['value']['session']
    assert new!=pid
    p.command('rollback')
    p.finish()
    h.results.append(dict(test='lost-COMMIT-reply',result='pass',session=pid,
                         establishes='COMMIT reached server and fixture committed; withheld terminal reply then socket death yields unresolved, never rollback; replacement checked'))
    residual_and_stop(h)
    crash_and_restart(h)
    assert h.proxy.cancel_requests==0
    h.results.append(dict(test='commit-lifecycle-no-client-cancel',result='pass',observed_cancel_packets=0))
    h.results.append(dict(test='DB-10',result='not run',reason='full capacity/saturation/retry/recovery is W8'))


def residual_and_stop(h):
    # Pre-establish uninterrupted observation access before exiting the client.
    observer=Probe(h)
    observer.command('admit')
    p=Probe(h)
    pid=p.command('prepare 1400')['value']['session']
    pause_standby(h)
    p.command('commit')
    wait_commit(h,p,pid)
    p.expected_status=74
    p.send('process_exit')
    assert p.p.wait(timeout=5)==74
    assert observer.command(f'observe {pid}')['value']['observed']==f'{pid}:SyncRep'
    replacement=Probe(h)
    replacement.command('admit',ok=False)
    replacement.finish()
    assert observer.command(f'observe {pid}')['value']['observed']==f'{pid}:SyncRep'
    h.event('residual_backend_observed',session=pid,client_exit_status=74,live_new_pool_usable_sessions=0,
            observation='same pre-established physical observer; no global occupancy bound inferred')
    resume_standby(h)
    p.finish()
    observer.finish()
    h.results.append(dict(test='DB-03',result='pass',old_backend=pid,
                         establishes='backend outlives process during SyncRep, outside replacement pool accounting; replacement check remains enabled'))
    # Stop begins while owner remains pending; process exit, not abandonment, ends the obligation.
    observer=Probe(h)
    observer.command('admit')
    p=Probe(h)
    pid=p.command('prepare 1450')['value']['session']
    pause_standby(h)
    p.command('commit')
    wait_commit(h,p,pid)
    p.expected_status=75
    start=time.monotonic()
    p.send('stop')
    eventually(lambda:any(e.get('event')=='stop_started' for e in p.events))
    assert p.p.poll() is None
    assert observer.command(f'observe {pid}')['value']['observed']==f'{pid}:SyncRep'
    while p.p.poll() is None and time.monotonic()-start<5:
        if not any(e.get('event')=='stop_bound_expired' for e in p.events):
            assert any(c['pid']==pid and not c['closed'] for c in h.proxy.connections), 'shutdown initiation released original transport'
        time.sleep(.02)
    assert p.p.returncode==75
    elapsed=time.monotonic()-start
    p.thread.join(timeout=2)
    assert .9<=elapsed<3 and any(e.get('event')=='stop_bound_expired' and e['unresolved'] for e in p.events)
    assert not classified(p,pid)
    resume_standby(h)
    p.finish()
    observer.finish()
    h.results.append(dict(test='bounded-process-stop',result='pass',exit_status=75,stop_bound_ms=1000,elapsed_seconds=elapsed,
                         establishes='original sent COMMIT owned while process survives; stop bound exits whole fixture with unresolved report',
                         not_run='product stop-record/ENG-07 implementation'))


def crash_and_restart(h):
    for crash,id in [('backend',1500),('primary',1501)]:
        p=Probe(h)
        # Same pool object survives both the failed attempt and recovery.
        p.command('admit')
        pid=p.command(f'prepare {id}')['value']['session']
        pause_standby(h)
        p.command('commit')
        wait_commit(h,p,pid)
        if crash=='backend':
            postmaster=h.nodes['primary']['process'].pid
            h.crash_backend(pid)
            assert h.nodes['primary']['process'].pid==postmaster
            eventually(lambda:'all server processes terminated; reinitializing' in (h.root/'primary.log').read_text())
        else:
            h.stop('primary',mode='immediate')
            h.start('primary')
        assert p.command('await')['value']['outcome']=='unresolved'
        # Waiting locally committed write is visible after PostgreSQL recovery,
        # but that fact does not authorize an unchecked new physical session.
        assert h.sql(f'SELECT destination FROM fixture.destinations WHERE id={id}')==f'client-{id}'
        p.command('release')
        p.command('admit',ok=False)
        before=len(p.events)
        resume_standby(h)
        p.command('admit')
        new_events=p.events[before:]
        assert any(e.get('event')=='check_target' and e.get('kind')=='admission' for e in new_events)
        assert any(e.get('event')=='session_admitted' for e in new_events)
        p.finish()
        h.results.append(dict(test='DB-04-'+crash,result='pass',old_backend=pid,
                             establishes='real waiting COMMIT becomes unresolved; same pool cannot expose new session until read-only admission passes after recovery'))


def regression_scenarios(h,case=None):
    """STO-7: deadlines close retained objects; sent COMMIT outlives its reactor caller."""
    prepare(h)
    for kind in ['session','transaction']:
        if case and case!=kind:
            continue
        p=Probe(h)
        pid=p.command('admit' if kind=='session' else 'tx_begin')['value']['session']
        pattern='SELECT 1::text' if kind=='session' else "INSERT INTO fixture.destinations VALUES (1800,"
        gate=h.proxy.arm(pattern,'reply' if kind=='session' else 'execute_reply')
        command='ping' if kind=='session' else 'tx_execute'
        start=time.monotonic()
        p.send(command)
        c=gate.wait()
        assert c['pid']==pid and not c['closed']
        before=h.sql(f"SELECT state FROM pg_stat_activity WHERE pid={pid}")
        assert before==('idle' if kind=='session' else 'idle in transaction'),before
        assert time.monotonic()-start<1, 'fault was not observed before client deadline'
        h.event('deadline_fault_observed',call_kind=kind,session=pid,backend_state=before,object_retained=True)
        failure=p.result(command,ok=False)
        assert 'pre-COMMIT' in failure['error'] and 'deadline' in failure['error'],failure
        # Front-side EOF is observed independently of withheld server replies.
        # Transaction idle timeout may already have killed the server backend;
        # that cannot close the client side of this deliberately gated proxy.
        end=time.monotonic()+.5
        while not c['closed'] and time.monotonic()<end:
            time.sleep(.01)
        backend=h.sql(f"SELECT count(*) FROM pg_stat_activity WHERE pid={pid}")
        h.event('deadline_closure_observed',call_kind=kind,session=pid,transport_closed=c['closed'],
                backend_count=backend,closure_source=c.get('close_source'),object_retained=True,elapsed_seconds=time.monotonic()-start,
                server_timeout='idle_in_transaction_session_timeout=1000ms; independent of local closure')
        assert c['closed'] and c.get('close_source')=='client_eof', 'expired pre-COMMIT call must close transport while object retained'
        # Still retained: repeat must fail locally without transmitting work.
        again=p.command(command,ok=False)
        expected_error='unusable session' if kind=='session' else 'failed/cancelled'
        assert expected_error in again['error'],again
        p.command('release' if kind=='session' else 'tx_drop')
        gate.release()
        new=p.command('admit' if kind=='session' else 'write_admit')['value']['session']
        assert new!=pid
        assert any(e.get('event')=='session_admitted' and e['session']==new for e in p.events)
        eventually(lambda:h.sql(f'SELECT count(*) FROM pg_stat_activity WHERE pid={pid}')=='0')
        if kind=='transaction':
            assert h.sql('SELECT count(*) FROM fixture.destinations WHERE id=1800')=='0'
        p.finish()
        h.results.append(dict(test='precommit-deadline-'+kind,result='pass',session=pid,new_session=new,
                              establishes='activated withheld reply on admitted session; local transport closed while object retained; no dirty reuse; server fate separately observed'))
    if case in (None,'runtime'):
        p=Probe(h)
        pid=p.command('prepare 1801')['value']['session']
        c=next(c for c in h.proxy.connections if c['pid']==pid)
        pause_standby(h)
        p.command('commit')
        wait_commit(h,p,pid)
        p.command('drop_runtime')
        assert any(e.get('event')=='request_runtime_dropped' for e in p.events)
        checked_out=p.command('past_deadline')['value']['checked_out']
        observations=dict(session=pid,transport_open=not c['closed'],checked_out=checked_out,
                          terminal_outcomes=classified(p,pid),backend_in_syncrep=pid in syncrep(h))
        h.event('runtime_drop_observed',**observations)
        assert not c['closed'] and not classified(p,pid), 'caller runtime teardown must preserve original pending COMMIT transport and owner'
        p.command('assert_retained')
        assert pid in syncrep(h)
        h.event('runtime_drop_retention',session=pid,checked_out=1,replacement_refused=True)
        resume_standby(h)
        assert p.command('await')['value']['outcome']=='acknowledged'
        assert classified(p,pid)[-1]['session']==pid
        assert not c['closed']
        assert h.sql('SELECT destination FROM fixture.destinations WHERE id=1801')=='client-1801'
        h.event('runtime_drop_reply_consumed',session=pid,outcome='acknowledged',original_transport=True)
        p.finish()
        h.results.append(dict(test='sent-commit-caller-runtime-drop',result='pass',session=pid,
                              establishes='SyncRep observed before initiating reactor destroyed; original transport/checkout retained; replacement refused; real reply consumed on standby restoration'))
    if case in (None,'cancelled'):
        cancelled_calls(h)
    if case is None:
        construction_cancellation(h)
        construction_deadline(h)
    assert h.proxy.cancel_requests==0
    h.results.append(dict(test='regression-no-client-cancel',result='pass',observed_cancel_packets=0))



def cancelled_calls(h):
    """STO-7: a caller that drops an in-flight read call, or a whole destination
    attempt, must not return that physical session to the pool."""
    p=Probe(h)
    reused=[]
    for kind,command,pattern in [('query','cancel_query',"'cancelled query probe'"),
                                 ('destination','cancel_read 1810','destination SELECT')]:
        gate=h.proxy.arm(pattern,'reply')
        p.send(command)
        c=gate.wait()
        h.event('cancelled_call_fault_observed',call_kind=kind,session=c['pid'])
        result=p.result(command.split()[0],ok=None)
        gate.release()
        h.event('cancelled_call_observed',call_kind=kind,session=c['pid'],ok=result['ok'],
                error=result.get('error'),value=result.get('value'))
        if not result['ok']:
            reused.append(kind)
            continue
        assert result['value']['old_session']==c['pid']!=result['value']['new_session']
        eventually(lambda:c['closed'],seconds=2)
        assert c.get('close_source')=='client_eof',c.get('close_source')
        eventually(lambda:h.sql(f"SELECT count(*) FROM pg_stat_activity WHERE pid={c['pid']}")=='0')
        h.results.append(dict(test='cancelled-'+kind+'-not-reused',result='pass',session=c['pid'],
                              new_session=result['value']['new_session'],
                              establishes='reply withheld; caller drops the in-flight call; client closes that transport; next checkout is a new backend'))
    p.finish()
    assert not reused,f'cancelled pre-COMMIT read must not return its physical session: {reused}'


def construction_cancellation(h):
    # STO-7: exercise the new supervisor construction handoff both before
    # authentication completes and after the driver exists, during admission.
    for stage,pattern in [('authentication',''),('admission','store admission target')]:
        p=Probe(h)
        gate=h.proxy.arm(pattern,'reply')
        p.command('pre_start')
        c=gate.wait()
        assert not c['closed']
        result=p.command('pre_cancel')
        eventually(lambda:c['closed'],seconds=.5)
        assert c['close_source']=='client_eof'
        new=result['value']['new_session']
        assert any(e.get('event')=='session_admitted' and e['session']==new for e in p.events)
        assert c['pid']!=new
        h.event('construction_cancel_observed',stage=stage,session=c['pid'],new_session=new,
                transport_closed=True,closure_source=c['close_source'])
        gate.release()
        p.finish()
        h.results.append(dict(test='construction-cancel-'+stage,result='pass',
                              establishes='observed constructor reply withholding; cancelled future closes original transport; replacement passes settings/admission'))



def construction_deadline(h):
    # STO-7: the process-owned connect/authentication future keeps its bound.
    p=Probe(h)
    gate=h.proxy.arm('','reply')
    start=time.monotonic()
    p.send('admit')
    c=gate.wait()
    assert not c['closed'] and time.monotonic()-start<1
    result=p.result('admit',ok=False)
    assert 'connect deadline' in result['error'],result
    eventually(lambda:c['closed'],seconds=.5)
    assert c['close_source']=='client_eof'
    assert not any(e.get('event')=='session_admitted' for e in p.events)
    h.event('construction_deadline_observed',transport_closed=True,closure_source=c['close_source'],
            elapsed_seconds=time.monotonic()-start)
    gate.release()
    p.command('admit')
    p.finish()
    h.results.append(dict(test='construction-authentication-deadline',result='pass',
                          establishes='supervisor-side authentication deadline closes unused physical transport; subsequent session passes admission'))
