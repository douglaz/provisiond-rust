"""OPS-49/STO-7: real COMMIT wait attribution, completion and stop precedence."""
import time
from w5 import admit, events, external, healthy_standby, orders, pause_dispatch, publish_control, reaped, snapshot, successor, trace


def observer_contract(w,p):
    """OPS-15/49, STO-55: actual startup order, role and separate capacity."""
    rows=events(p);started=events(p,'commit_observer_started')
    assert len(started)==1
    observer=started[0];pid=observer['session']
    lock=events(p,'startup_lock_acquired')[0]
    generation=events(p,'generation_advanced')[0]
    assert rows.index(lock)<rows.index(observer)<rows.index(generation)
    assert any(e['event']=='session_admitted' and e['session']==pid for e in rows[:rows.index(observer)])
    assert pid!=lock['session'] and observer['startup_sessions']==7 and observer['ordinary_capacity']==21
    assert not events(w.api,'commit_observer_started')
    statements=[e for e in w.h.events if e['event']=='bytes_transmitted' and e.get('session')==pid]
    assert statements and not any(e.get('query','').startswith(('BEGIN','UPDATE','INSERT','COMMIT')) for e in statements)
    assert any('default_transaction_read_only=on' in e.get('query','') for e in statements)
    w.h.event('w5_observer_contract',scenario=w.case,startup=observer,lock_session=lock['session'],api_observers=0,read_only=True)


def attribution(w,p,owner):
    samples=[e for e in events(p,'commit_observer_sample') if e['owner']==owner]
    completed=[e for e in events(p,'commit_accounting_completed') if e['owner']==owner]
    assert len(completed)==1 and samples, 'OPS-49 real observer evidence missing'
    exempt=0;first=None;last=None
    for e in samples:
        assert e['received_us']>=e['sent_us']
        if e['matching']:
            assert e['identity_matches'] and e['state']=='active' and e['wait_event']=='SyncRep' and e['store_commit']
            if first is None:first=e['received_us']
            last=e['sent_us']
        else:
            if first is not None:exempt+=max(0,last-first)
            first=last=None
    if first is not None:exempt+=max(0,last-first)
    # Microsecond presentation truncates each instant independently.
    assert abs(exempt-completed[0]['exempt_us'])<=len(samples)
    w.h.event('w5_accounting_attribution',scenario=w.case,owner=owner,samples=samples,completion=completed[0],recomputed_exempt_us=exempt)
    return completed[0]['exempt_us']


def accounting(w, mode):
    from harness import eventually
    from scenarios import pause_standby,resume_standby
    h=w.h;t,body,op=admit(w,'accounting-'+mode)
    healthy_standby(h,'engine-settlement')
    statement=h.proxy.arm('SET status=$3,result=','send');reply=None;paused=False
    try:
        p=w.start_engine();connection=statement.wait();pid=connection['pid']
        if mode!='reply':pause_dispatch(w,p)
        reply=h.proxy.arm('store COMMIT','commit_reply',session=pid)
        if mode in ('syncrep','mixed','stop'):
            # Pause only the designated standby; ordinary observer queries remain
            # autocommit read-only and need no synchronous acknowledgement.
            from w5 import publish_control
            commit=h.proxy.arm('store COMMIT','send',session=pid)
            statement.release();commit.wait();pause_standby(h);paused=True;commit.release()
            eventually(lambda:h.sql(f"SELECT count(*) FROM pg_stat_activity WHERE pid={pid} AND state='active' AND wait_event='SyncRep'")=='1')
            owner=[e for e in events(p,'commit_possible_dispatch') if e['session']==pid][-1]['owner']
            eventually(lambda:any(e.get('owner')==owner and e.get('matching') for e in events(p,'commit_observer_sample')))
            started=time.monotonic()
            if mode=='stop':
                publish_control(w.control/'stop-operation',op)
                reaped(w,p)
                assert time.monotonic()-started<1.6
                assert not any(e.get('owner')==owner for e in events(p,'commit_owner_completed'))
                assert any('stop bound expired' in e.get('reason','') for e in events(p,'engine_fatal'))
                h.event('w5_accounting_activated',scenario=w.case,mode=mode,activated=True,session=pid,owner=owner,stop_won=True,sent_commit_outstanding=True,engine_exit=p.returncode,samples=[e for e in events(p,'commit_observer_sample') if e['owner']==owner])
                resume_standby(h);paused=False;reply.release()
                eventually(lambda:h.sql(f"SELECT count(*) FROM pg_stat_activity WHERE pid={pid} AND wait_event='SyncRep'")=='0')
                assert snapshot(w,op)['operation']['status']=='succeeded'
                successor(w,op,'succeeded')
                trace(w,op,'original stop bound precedes completion-only accounting',mode=mode)
                return
            # A sent COMMIT remains owned beyond the Store-retry bound.
            while time.monotonic()-started<2.25:
                assert p.poll() is None
                assert h.sql(f"SELECT wait_event FROM pg_stat_activity WHERE pid={pid}")=='SyncRep'
                time.sleep(.08)
            assert not any(e.get('owner')==owner for e in events(p,'commit_accounting_completed'))
            resume_standby(h);paused=False
        else:statement.release()
        reply.wait()
        owner=[e for e in events(p,'commit_possible_dispatch') if e['session']==pid][-1]['owner']
        stored=snapshot(w,op);provider=external(w,op)
        assert stored['operation']['status']=='succeeded' and len(orders(w,op))==1
        if mode in ('reply','mixed','unavailable','mismatch'):
            observer_gate=None
            if mode=='unavailable':
                observer_gate=h.proxy.arm('OPS-49 observer','reply')
                observer_gate.wait()
            if mode=='mismatch':
                h.proxy.observer_identity_mismatch=True
            started=time.monotonic();server=[]
            while time.monotonic()-started<2.2:
                value=h.sql(f"SELECT state||':'||wait_event FROM pg_stat_activity WHERE pid={pid}")
                assert value=='idle:ClientRead';server.append(value)
                assert p.poll() is None, 'OPS-49 accounting must wait for COMMIT completion'
                time.sleep(.08)
            if observer_gate:observer_gate.release()
            h.proxy.observer_identity_mismatch=False
            h.proxy.close_connection(connection,'w5_nonstandby_commit_reply_delay');reply.release()
            eventually(lambda:p.poll() is not None or any(e.get('operation')==op for e in events(p,'settlement_finished')))
            retries=events(p,'whole_transaction_retry')
            h.event('w5_accounting_activated',scenario=w.case,mode=mode,activated=True,operation=op,session=pid,owner=owner,held_ms=round((time.monotonic()-started)*1000),backend_samples=server,retries=retries,engine_exit=p.poll(),provider_orders=len(orders(w,op)),durable_rows_unchanged=snapshot(w,op)==stored)
            # This assertion is also the unchanged-runtime regression driver.
            assert p.poll() is not None and not retries, 'OPS-49 non-standby COMMIT reply delay must consume retry budget'
            p.wait(timeout=1)
            observer_contract(w,p)
            exempt=attribution(w,p,owner)
            if mode in ('reply','unavailable','mismatch'):assert exempt==0
            if mode=='mixed':assert exempt>2_000_000
            if mode in ('unavailable','mismatch'):assert events(p,'commit_observer_failed')
            if mode=='mismatch':assert any(not s['identity_matches'] for s in events(p,'commit_observer_sample') if s['owner']==owner)
        else:
            # Real long SyncRep, then lost reply: only measured wait is exempt;
            # the whole database repeat succeeds without repeating provider I/O.
            h.proxy.close_connection(connection,'w5_syncrep_ack_lost');reply.release()
            eventually(lambda:any(e.get('operation')==op for e in events(p,'settlement_finished')))
            assert p.poll() is None and events(p,'whole_transaction_retry')
            observer_contract(w,p)
            assert attribution(w,p,owner)>2_000_000
            h.event('w5_accounting_activated',scenario=w.case,mode=mode,activated=True,session=pid,owner=owner,real_syncrep=True,retries=events(p,'whole_transaction_retry'),completion_only=True)
            w.stop_engine()
        assert snapshot(w,op)==stored and external(w,op)==provider
        successor(w,op,'succeeded')
        assert snapshot(w,op)==stored and external(w,op)==provider
        trace(w,op,'COMMIT accounting at receipt completion',mode=mode,one_purchase=True,rows_unchanged=True)
    finally:
        h.proxy.observer_identity_mismatch=False
        if paused:resume_standby(h)
        statement.release()
        if reply:reply.release()
        for name in ('hold-dispatch','stop-operation'):
            path=w.control/name
            if path.exists():path.unlink()


def reconnect(w):
    """Loss is alarmed; a new checked observer attributes a later attempt."""
    from harness import eventually
    t,body,op=admit(w,'accounting-reconnect');h=w.h
    hold=w.control/'hold-step-9';hold.touch()
    try:
        p=w.start_engine()
        eventually(lambda:events(p,'dispatch_step_held'))
        observer=events(p,'commit_observer_started')[-1]['session']
        assert h.sql(f"SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE pid={observer} AND state='idle' AND wait_event IS DISTINCT FROM 'SyncRep'")=='t'
        eventually(lambda:events(p,'commit_observer_reestablished'))
        fresh=events(p,'commit_observer_reestablished')[-1]['session']
        assert fresh!=observer and events(p,'commit_observer_failed')
        assert any(e.get('session')==fresh for e in events(p,'session_admitted'))
        # Freeze the later settlement, after the new checked observer exists.
        gate=h.proxy.arm('SET status=$3,result=','send');hold.unlink();connection=gate.wait()
        from scenarios import pause_standby,resume_standby
        commit=h.proxy.arm('store COMMIT','send',session=connection['pid']);gate.release();commit.wait()
        pause_standby(h)
        try:
            commit.release()
            eventually(lambda:any(e.get('observer_session')==fresh and e.get('session')==connection['pid'] and e.get('matching') for e in events(p,'commit_observer_sample')))
        finally:resume_standby(h);commit.release();gate.release()
        w.wait_status(op,'succeeded')
        eventually(lambda:any(e.get('operation')==op for e in events(p,'settlement_finished')))
        h.event('w5_accounting_activated',scenario=w.case,activated=True,observer_lost=observer,checked_replacement=fresh,later_attempt=connection['pid'],failure=events(p,'commit_observer_failed'))
        w.stop_engine();successor(w,op,'succeeded')
        assert len(orders(w,op))==1
        trace(w,op,'observer re-established before a later real COMMIT')
    finally:
        if hold.exists():hold.unlink()


CASES={**{'w5_accounting_'+mode:(lambda w,mode=mode:accounting(w,mode)) for mode in ('reply','syncrep','mixed','unavailable','mismatch','stop')},'w5_accounting_reconnect':reconnect}
