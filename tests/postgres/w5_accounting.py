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


def terminate_idle_observer(h,observer):
    """Wait for the recorded observer's idle gap without targeting a replacement."""
    from harness import eventually
    try:
        eventually(lambda:h.sql(f"SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE pid={observer} AND state='idle' AND wait_event IS DISTINCT FROM 'SyncRep'")=='t')
    except AssertionError as error:
        raise AssertionError(f'observer {observer} idle termination timed out: {error}') from error


def reconnect(w):
    """Loss is alarmed; a new checked observer attributes a later attempt."""
    from harness import eventually
    t,body,op=admit(w,'accounting-reconnect');h=w.h
    hold=w.control/'hold-step-9';hold.touch()
    try:
        p=w.start_engine()
        eventually(lambda:events(p,'dispatch_step_held'))
        observer=events(p,'commit_observer_started')[-1]['session']
        terminate_idle_observer(h,observer)
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


def failure_run(w,saturation=False):
    """OPS-49/STO-7/55: observer exhaustion retains the sent COMMIT owner.

    Closure is an internal invariant observation, tested by skipping end_run.
    Completion arithmetic independently proves charging on this server history;
    no checked reconnect during the stalled standby is assumed.
    """
    import json
    from harness import eventually
    from scenarios import pause_standby,resume_standby
    h=w.h;scenario_start=len(h.events)
    t,body,op=admit(w,'accounting-failure-run')
    healthy_standby(h,'engine-observer-failure')
    statement=h.proxy.arm('SET status=$3,result=','send');commit=reply=None;paused=False;capacity=[]
    try:
        p=w.start_engine();connection=statement.wait();pid=connection['pid']
        pause_dispatch(w,p)
        reply=h.proxy.arm('store COMMIT','commit_reply',session=pid)
        commit=h.proxy.arm('store COMMIT','send',session=pid)
        statement.release();commit.wait();pause_standby(h);paused=True;commit.release()
        owner=[e for e in events(p,'commit_possible_dispatch') if e['session']==pid][-1]['owner']
        def matching():
            return [e for e in events(p,'commit_observer_sample') if e['owner']==owner and e['matching']]
        eventually(lambda:len(matching())>=4 and matching()[-1]['sent_us']-matching()[0]['received_us']>40_000)
        samples=matching();observer=samples[-1]['observer_session']
        state=json.loads(h.sql(f"SELECT jsonb_build_object('pid',pid,'backend_start',backend_start::text,'state',state,'wait_event_type',wait_event_type,'wait_event',wait_event,'query',query) FROM pg_stat_activity WHERE pid={pid}"))
        assert state['state']=='active' and state['wait_event']=='SyncRep' and state['query']=='COMMIT /* store COMMIT */'
        assert state['backend_start']==samples[-1]['backend_start']
        h.event('w5_observer_failure_prerequisite',operation=op,owner=owner,settlement=state,observer_session=observer,matching_samples=samples)
        if saturation:
            from w8 import saturate
            capacity=saturate(h)
            # Fixed owned clients have no supervisor or connection retry. Stop
            # the API out of band; engine dispatch is already quiesced, with
            # its sent settlement retained. No fresh observer drives recovery.
            w.stop_api()
            h.event('w8_saturation_quiesced',replacement_processes=0,api_exit=w.api.returncode,dispatch_paused=True,sent_commit_retained=True)
        # Kill precisely the original observer, independently excluding SyncRep.
        # Its periodic read can be active at the first probe, especially while
        # opening saturation clients. Wait for idle without relaxing the guard.
        terminate_idle_observer(h,observer)
        eventually(lambda:any(e['owner']==owner for e in events(p,'test_observer_run_closed')))
        closure=next(e for e in events(p,'test_observer_run_closed') if e['owner']==owner)
        before=closure['before'];after=closure['after']
        assert closure['session']==pid and closure['backend_start']==state['backend_start']
        assert before['first_received_us']==samples[0]['received_us'] and before['last_sent_us']>=samples[-1]['sent_us']
        assert before['last_sent_us']>before['first_received_us'] and before['exempt_us']==0
        h.event('w5_observer_run_closure_observed',operation=op,owner=owner,observer_session=observer,closure=closure,real_syncrep=True,established_run=True,run_still_open=after['first_received_us'] is not None or after['last_sent_us'] is not None)
        assert after['first_received_us'] is None and after['last_sent_us'] is None, 'OPS-49 observer failure must close an established SyncRep sample run'
        measured=before['last_sent_us']-before['first_received_us']
        assert abs(after['exempt_us']-measured)<=1
        eventually(lambda:events(p,'commit_observer_failed'))
        failed_at=time.monotonic()
        # Independent server observations bound a genuinely unobserved wait.
        server=[]
        while time.monotonic()-failed_at<2.2:
            assert p.poll() is None and not connection['closed'], 'STO-7 observer exhaustion must retain alive process and owned sent COMMIT'
            value=h.sql(f"SELECT state||':'||wait_event FROM pg_stat_activity WHERE pid={pid}")
            assert value=='active:SyncRep';server.append(dict(elapsed=time.monotonic()-failed_at,state=value))
            assert not any(e['owner']==owner for e in events(p,'commit_accounting_completed'))
            time.sleep(.08)
        publish_control(w.control/'observe-commit-owner',str(owner))
        eventually(lambda:any(e.get('owner')==owner for e in events(p,'test_commit_checkout')))
        ownership=next(e for e in events(p,'test_commit_checkout') if e['owner']==owner)
        assert ownership['session']==pid and ownership['pooled_checkout_held'] and not ownership['transport_dead']
        alarms=[json.loads(line) for line in p.error.read_text().splitlines() if line.startswith('{')]
        assert any(e.get('event')=='alarm' and e.get('threshold_ms')==0 and 'observer reconnect failed' in e.get('message','') for e in alarms), 'STO-55 bounded observer reconnect must actually exhaust'
        attempts=len(h.proxy.connections)
        time.sleep(.15)
        assert len(h.proxy.connections)==attempts, 'STO-55 exhausted observer must stop reconnecting'
        assert not events(p,'engine_fatal') and not events(p,'commit_observer_reestablished')
        h.event('w5_observer_exhaustion_retained',owner=owner,ownership=ownership,alarms=alarms,connections=attempts,unobserved_server_samples=server)
        # STO-55 step 3: signal the previously identified owned receiver; the
        # saturation arm needs no SQL connection for this recovery action.
        resume_standby(h,verify=not saturation);paused=False;reply.wait()
        stored=snapshot(w,op);provider=external(w,op)
        assert stored['operation']['status']=='succeeded' and len(orders(w,op))==1
        assert p.poll() is None and not connection['closed'], 'STO-7 observer exhaustion must retain alive process and owned sent COMMIT'
        assert not events(p,'engine_fatal') and not any(e['owner']==owner for e in events(p,'commit_accounting_completed'))
        assert not events(p,'commit_observer_reestablished'), 'STO-55 exhausted observer cannot reconnect after standby returns'
        h.proxy.close_connection(connection,'w5_observer_failure_commit_reply_lost');reply.release()
        reaped(w,p)
        observer_contract(w,p)
        exempt=attribution(w,p,owner)
        assert abs(exempt-measured)<=2, 'OPS-49 unobserved gap must remain charged at completion'
        completed=next(e for e in events(p,'commit_accounting_completed') if e['owner']==owner)
        assert completed['completion_us']-closure['failed_us']>2_000_000
        # Other engine components may unwind an unsent transaction after the
        # final receipt. The exhausted observer must admit no later COMMIT.
        rows=events(p)
        assert not any(e['event']=='commit_possible_dispatch' for e in rows[rows.index(completed)+1:])
        # W5 shares proxy history. This invocation claims only this operation,
        # pauses dispatch at its settlement, then reaps before any successor.
        # Count every transport in that scenario window: filtering by the first
        # PID/owner would hide a whole-transaction repeat on a fresh connection.
        assert {e['operation'] for e in rows if e['event']=='claim_registered'}=={op}
        settlements=[e for e in h.events[scenario_start:] if e['event']=='bytes_transmitted' and 'SET status=$3,result=' in e.get('query','')]
        h.event('w5_owned_settlement_count',scenario=w.case,operation=op,owner=owner,transmissions=len(settlements),sessions=[e['session'] for e in settlements],scope='scenario entry through engine exit, all transports, sole claimed operation')
        assert len(settlements)==1, 'OPS-49 observer exhaustion must not repeat the owned settlement'
        fatal=next(e for e in events(p,'engine_fatal') if 'observer reconnect failed' in e.get('reason',''))
        assert events(p).index(completed)<events(p).index(fatal), 'STO-7 completion must precede deferred observer fatal'
        h.event('w5_accounting_activated',scenario=w.case,activated=True,operation=op,owner=owner,session=pid,observer_failure=closure,observer_exhausted=True,deferred_fatal=fatal,ownership=ownership,unobserved_server_samples=server,completion=completed,exempt_us=exempt,engine_exit=p.returncode,proof='run closure and independent completion charging; exhaustion retains owner until receipt resolves')
        if capacity:
            from w8 import release_capacity
            release_capacity(capacity);capacity=[]
            h.identity()
            h.event('w8_saturation_recovered',recovery_controls='out-of-band API stop, dispatch pause, receiver resume',fresh_sql_required_for_recovery=False,primary_shutdown='not run; standby restoration completed wait',same_history=True)
        successor(w,op,'succeeded')
        assert snapshot(w,op)==stored and external(w,op)==provider and len(orders(w,op))==1
        trace(w,op,'observer fails inside established SyncRep run; unobserved gap charged',run_closed=True,one_purchase=True,rows_unchanged=True)
    finally:
        if capacity:
            from w8 import release_capacity
            release_capacity(capacity)
        if paused:resume_standby(h)
        statement.release()
        if commit:commit.release()
        if reply:reply.release()
        path=w.control/'hold-dispatch'
        if path.exists():path.unlink()


CASES={**{'w5_accounting_'+mode:(lambda w,mode=mode:accounting(w,mode)) for mode in ('reply','syncrep','mixed','unavailable','mismatch','stop')},'w5_accounting_reconnect':reconnect,'w5_accounting_failure_run':failure_run}
CASES['w5_accounting_saturation']=lambda w:failure_run(w,True)
