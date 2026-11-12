"""W5: causal interruption traces through real binaries, PG and simulators.

OPS-11/15/21/43/47/49/50, STO-3/7/23/55. Every selector must activate its
named fault; the complete phase refuses missing or duplicate scenario results.
"""

CASES = [
    'w5_admission_before', 'w5_admission_reply', 'w5_claim_reply',
    'w5_step8', 'w5_accepted_lost', 'w5_settlement_blocked',
    'w5_settlement_reply', 'w5_stop_record', 'w5_lock_loss',
    'w5_retained_checkouts', 'w5_process_death', 'w5_connection_check',
    'w5_contention', 'w5_remote_completion', 'w5_generation_record',
]


def w5(h, case=None, defect=None):
    from workflow import Workflow
    h.config['w5']=dict(required_scenarios=CASES,selected_case=case,defect=defect,automatic_reconciliation='not run (W6)',deployment_saturation='not run (W8)',recovery_incidents='not run (W7)')
    w = Workflow(h, defect='generation' if defect == 'w5_generation' else defect)
    try:
        for name in ([case] if case else CASES):
            w.case = name
            w.stop_engine()
            before=len(h.events)
            SCENARIOS[name](w)
            assert any(e['event']=='w5_causal_trace' or e['event']=='w5_connection_check_activated' for e in h.events[before:]), 'W5 scenario missing causal trace'
            h.results.append(dict(test=name,result='pass'))
        expected=[case] if case else CASES
        observed=[r['test'] for r in h.results if r['test'] in CASES]
        assert observed==expected, 'W5 required scenario inventory incomplete or duplicated'
        h.event('w5_inventory_complete',required=expected,observed=observed)
    finally:
        w.close()


def events(p, kind=None):
    from workflow import trace_events
    rows=trace_events(p.log)
    return [e for e in rows if kind is None or e.get('event')==kind]


def publish_control(path, value):
    """Publish complete payloads: existence is the executable's request signal."""
    import os
    pending=path.with_name(path.name+'.pending')
    # This invocation owns the private control directory and this exact path.
    with pending.open('x') as stream:stream.write(value)
    os.replace(pending,path)


def snapshot(w, op):
    """STO-3/23/28: compare complete durable status, machine and money rows."""
    import json
    return json.loads(w.h.sql(f"""SELECT jsonb_build_object(
      'operation',to_jsonb(o)-'request', 'commitment',to_jsonb(c),
      'machines',COALESCE((SELECT jsonb_agg(to_jsonb(m) ORDER BY m.id) FROM provisiond.machines m WHERE m.id=o.machine_id),'[]'),
      'ledger',COALESCE((SELECT jsonb_agg(to_jsonb(l) ORDER BY l.seq) FROM provisiond.ledger_entries l WHERE l.operation_id=o.id),'[]'),
      'meter_totals',COALESCE((SELECT jsonb_agg(to_jsonb(mt)) FROM provisiond.meter_totals mt WHERE mt.subject_id=o.machine_id),'[]'),
      'exclusions',COALESCE((SELECT jsonb_agg(to_jsonb(x)) FROM provisiond.resource_exclusions x WHERE x.operation_id=o.id),'[]'))
      FROM provisiond.operations o JOIN provisiond.commitments c ON c.id=o.commitment_id WHERE o.id='{op}'"""))


def external(w, op):
    return [e for e in w.external()['events'] if e.get('command')==op]


def orders(w, op):
    return [e for e in external(w,op) if e['path']=='/order' and e['stage']=='request_received']


def admit(w, suffix, resource=False):
    from w4_regressions import funded
    t=funded(w,suffix)
    if resource:
        import json
        original=json.loads(w.h.sql(f"SELECT descriptor FROM provisiond.provider_accounts WHERE account='{t['account']}'"))
        t['_original_descriptor']=original
        descriptor=dict(original,offer_is_resource=True)
        w.h.sql("UPDATE provisiond.provider_accounts SET descriptor='%s' WHERE account='%s'"%(json.dumps(descriptor),t['account']))
        w.restart_api()
    body=w.create_body(t)
    op=w.request('POST','/v1/machines',body,t,suffix,status=202)['id']
    if resource:assert len(snapshot(w,op)['exclusions'])==1
    return t,body,op


def restore_resource(w,t):
    import json
    original=t.pop('_original_descriptor',None)
    if original is not None:
        w.h.sql("UPDATE provisiond.provider_accounts SET descriptor='%s' WHERE account='%s'"%(json.dumps(original),t['account']))
        w.restart_api()


def reaped(w, p):
    from harness import eventually
    eventually(lambda:p.poll() is not None,seconds=5)
    p.wait(timeout=1)
    assert p.returncode!=0
    w.h.event('w5_process_exit',component=p.name,exit_status=p.returncode,harness_killed=False,
              fatal_events=events(p,'engine_fatal'))


def successor(w, op, expected):
    w.stop_engine()
    p=w.start_engine()
    assert w.status(op)==expected
    w.h.event('w5_successor',operation=op,status=expected,generation=w.h.sql('SELECT generation FROM provisiond.startup_generation'),
              startup=events(p,'restart_pass_complete'),snapshot=snapshot(w,op),orders=len(orders(w,op)))
    w.stop_engine()


def trace(w, op, boundary, **facts):
    w.h.event('w5_causal_trace',scenario=w.case,operation=op,boundary=boundary,
              snapshot=snapshot(w,op),provider=external(w,op),**facts)


def socket_request(w,t,body,key,correlation):
    import json
    import socket
    data=json.dumps(body).encode()
    conn=socket.create_connection(('127.0.0.1',w.h.ports[4]),timeout=8)
    header=(f'POST /v1/machines HTTP/1.1\r\nHost: localhost\r\nAuthorization: Bearer {t["spending_token"]}\r\n'
            f'Idempotency-Key: {key}\r\nX-Correlation-Id: {correlation}\r\nContent-Type: application/json\r\nContent-Length: {len(data)}\r\n\r\n')
    conn.sendall(header.encode()+data)
    return conn


def healthy_standby(h, component):
    """STO-7/CNF-365: establish identity and catch-up BEFORE arming a fault."""
    import json
    h.identity()
    target=h.sql('SELECT pg_current_wal_flush_lsn()')
    rows=json.loads(h.sql("SELECT jsonb_agg(jsonb_build_object('pid',pid,'role',usename,'name',application_name,'address',host(client_addr),'state',state,'sync_state',sync_state,'flush',flush_lsn::text)) FROM pg_stat_replication WHERE application_name='provisiond_sync'"))
    assert len(rows)==1 and rows[0]['role']=='replicator' and rows[0]['address']=='127.0.0.1' and rows[0]['state']=='streaming' and rows[0]['sync_state']=='sync'
    assert h.sql(f"SELECT flush_lsn >= '{target}'::pg_lsn FROM pg_stat_replication WHERE pid={rows[0]['pid']}")=='t'
    h.event('w5_healthy_before_fault',component=component,target=target,designated=rows)
    return dict(target=target,designated=rows)


def original_receipt(w,t,key):
    """API-11/STO-35: bind the ORIGINAL durable admission before replay."""
    import json
    rows=json.loads(w.h.sql(f"SELECT COALESCE(jsonb_agg(jsonb_build_object('operation',o.id,'claim',o.claim_number,'status',o.status,'receipt',to_jsonb(r)-'response_body','commitment',c.id)),'[]') FROM provisiond.idempotency_records r JOIN provisiond.operations o ON o.id=r.resource_id::uuid JOIN provisiond.commitments c ON c.id=o.commitment_id AND c.operation_id=o.id WHERE r.scope_id='{t['tenant_id']}' AND r.key='{key}'"))
    assert len(rows)==1 and rows[0]['status']=='queued' and rows[0]['claim']==0, 'API-11 original admission missing before replay'
    assert not external(w,rows[0]['operation'])
    w.h.event('w5_original_admission_identified',original=rows[0],before_replay=True)
    return rows[0]['operation']


def failed_attempt_release(w,p,pid,release=None):
    """OPS-49: application objects AND attributed server transaction end."""
    rows=events(p)
    release=release or next(e for e in rows if e.get('event')=='transaction_resources_released' and e.get('session')==pid)
    transaction=release['transaction']
    assert release['component_available']==1
    # This fixture has one worker and paused dispatch: spare capacity cannot
    # establish that the original failed checkout was actually returned.
    assert release['pool_checked_out']==0, 'OPS-49 failed attempt must release its original write checkout before backoff'
    released=rows.index(release)
    assert any(e.get('event')=='whole_transaction_retry' and e.get('transaction')==transaction for e in rows[released+1:]), 'OPS-49 release must precede retry/backoff'
    rolled_back=[e for e in rows[:released] if e.get('event')=='transaction_rollback_acknowledged' and e.get('transaction')==transaction]
    if rolled_back:
        assert rolled_back[-1]['ready_status']==ord('I')
        server=next(e for e in w.h.events if e['event']=='protocol_response' and e.get('session')==pid and e.get('command_tag')=='ROLLBACK' and e.get('query')==f'ROLLBACK /* store transaction {transaction} */')
    else:
        assert release['discarded']
        from harness import eventually
        eventually(lambda:w.h.sql(f'SELECT count(*) FROM pg_stat_activity WHERE pid={pid}')=='0')
        server=dict(session=pid,backend_absent=True)
    w.h.event('w5_failed_attempt_release',session=pid,application_release=release,rollback=rolled_back[-1] if rolled_back else None,server_end=server,release_before_backoff=True)


def admission_before(w):
    """API-11/STO-23/35: fail before COMMIT, no admission, canonical resend."""
    from concurrent.futures import ThreadPoolExecutor
    from w4_regressions import funded
    from workflow import http
    h=w.h;t=funded(w,'w5-admission-before');body=w.create_body(t);key='w5-before-commit'
    def state():
        return h.sql(f"""SELECT jsonb_build_object(
          'operations',(SELECT jsonb_agg(to_jsonb(o)-'request') FROM provisiond.operations o WHERE tenant_id='{t['tenant_id']}'),
          'commitments',(SELECT jsonb_agg(to_jsonb(c)) FROM provisiond.commitments c WHERE tenant_id='{t['tenant_id']}'),
          'receipts',(SELECT jsonb_agg(to_jsonb(r)) FROM provisiond.idempotency_records r WHERE scope_id='{t['tenant_id']}'),
          'ceilings',(SELECT jsonb_agg(to_jsonb(c)) FROM provisiond.ceiling_counters c WHERE principal='{t['tenant_id']}'))""")
    before=state();hold=w.control/'hold-funding';hold.touch()
    gate=h.proxy.arm('SHOW synchronous_commit /* precommit verify */','reply')
    marker=len(h.events)
    try:
        with ThreadPoolExecutor(max_workers=1) as pool:
            pending=pool.submit(http,h.ports[4],'POST','/v1/machines',body,t['spending_token'],key)
            connection=gate.wait()
            assert state()==before
            assert not any(e['event']=='bytes_transmitted' and e.get('session')==connection['pid'] and 'store COMMIT' in e.get('query','') for e in h.events[marker:])
            h.event('w5_before_admission_activated',session=connection['pid'],receipt_operation_commitment_ceilings_unchanged=True)
            h.proxy.close_connection(connection,'w5_before_admission_commit');gate.release()
            code,value,_=pending.result(timeout=8)
            assert code==500 and value['error']['kind']=='internal'
        assert state()==before
    finally:
        gate.release();hold.unlink()
    op=w.request('POST','/v1/machines',body,t,key,status=202)['id']
    w.start_engine();w.wait_status(op,'succeeded');w.stop_engine()
    assert len(orders(w,op))==1
    trace(w,op,'admission before COMMIT',first_http_status=code,no_admission_before_resend=True)


def admission_reply(w):
    """API-11/12: acknowledged admission, actual HTTP loss, same operation."""
    from w4_regressions import funded
    from harness import eventually
    t=funded(w,'w5-admission-reply');body=w.create_body(t);key='w5-http-lost'
    hold=w.control/'hold-http-response';hold.touch();correlation='w5-admission-reply'
    gate=w.h.proxy.arm('store COMMIT','send')
    conn=socket_request(w,t,body,key,correlation)
    try:
        connection=gate.wait();pid=connection['pid']
        owner=[e for e in events(w.api,'commit_possible_dispatch') if e['session']==pid][-1]['owner']
        gate.release()
        eventually(lambda:any(e.get('correlation_id')==correlation and e.get('status')==202 for e in events(w.api,'http_response_held')))
        assert any(e.get('owner')==owner and e.get('outcome')=='acknowledged' for e in events(w.api,'commit_classified'))
        op=w.h.sql(f"SELECT resource_id FROM provisiond.idempotency_records WHERE scope_id='{t['tenant_id']}' AND key='{key}'")
        admitted=snapshot(w,op);assert admitted['operation']['status']=='queued' and admitted['commitment']['reserved_sats']==43
        conn.close()
        eventually(lambda:any(e.get('correlation_id')==correlation for e in events(w.api,'http_reply_lost')))
        w.h.event('w5_http_reply_loss_activated',operation=op,session=pid,owner=owner,acknowledged=True,delivered=False,correlation_id=correlation)
    finally:
        conn.close();gate.release();hold.unlink()
    assert w.request('POST','/v1/machines',body,t,key,status=202)['id']==op
    changed=dict(body,hostname='different')
    assert w.request('POST','/v1/machines',changed,t,key,status=409)['error']['kind']=='conflict'
    assert snapshot(w,op)==admitted
    w.start_engine();w.wait_status(op,'succeeded');w.stop_engine()
    assert len(orders(w,op))==1
    trace(w,op,'admission acknowledged; HTTP reply discarded',canonical_replay=True,changed_fingerprint_conflicts=True)


def claim_reply(w):
    """ENG-03/OPS-49/15: learn nothing from lost claim acknowledgement."""
    from harness import eventually
    t,body,op=admit(w,'w5-claim-reply');h=w.h
    claim=h.proxy.arm("SET status='running',claim_number=claim_number+1",'send')
    reply=None
    try:
        p=w.start_engine();connection=claim.wait();pid=connection['pid']
        reply=h.proxy.arm('store COMMIT','commit_reply',session=pid);claim.release();reply.wait()
        stored=snapshot(w,op)
        assert stored['operation']['status']=='running' and stored['operation']['claim_number']==1
        assert stored['operation']['about_to_order_at'] is None and not external(w,op)
        owner=[e for e in events(p,'commit_possible_dispatch') if e['session']==pid][-1]
        h.event('w5_claim_reply_loss_activated',operation=op,claim=1,session=pid,owner=owner['owner'],committed=True,worker_registered=False)
        attempts=len(events(p,'claim_attempt_started'))
        h.proxy.close_connection(connection,'w5_claim_reply_lost');reply.release();reaped(w,p)
        assert len(events(p,'claim_attempt_started'))==attempts
        assert not any(e.get('operation')==op for e in events(p,'claim_registered'))
        assert 'no blind claim retry' in p.log.read_text() and not orders(w,op)
    finally:
        claim.release()
        if reply:reply.release()
    successor(w,op,'failed')
    after=snapshot(w,op);assert after['commitment']['state']=='closed' and after['commitment']['reserved_sats']==0 and not after['ledger']
    trace(w,op,'claim COMMIT reply lost',unknown_claim_fatal=True,null_marker_restart_exception=True)


def step8(w):
    """OPS-11/15/43: live position differs from crash's persisted marker."""
    from harness import eventually
    for arm in ('failure','stop','crash'):
        t,body,op=admit(w,'w5-step8-'+arm,resource=arm!='crash')
        hold=w.control/'hold-step-9';hold.touch();fail=w.control/'fail-step-9'
        try:
            p=w.start_engine()
            eventually(lambda:any(e.get('operation')==op and e.get('step')==9 for e in events(p,'dispatch_step_held')))
            licensed=snapshot(w,op)
            assert licensed['operation']['about_to_order_at'] is not None and not orders(w,op)
            assert any(e.get('operation')==op for e in events(p,'dispatch_license_acknowledged'))
            w.h.event('w5_step8_activated',operation=op,arm=arm,acknowledged=True,marker_set=True,step9_invoked=False)
            if arm=='failure':
                fail.touch();hold.unlink();w.wait_status(op,'failed');w.stop_engine()
            elif arm=='stop':
                publish_control(w.control/'stop-operation',op);reaped(w,p)
            else:
                p.kill();p.wait(timeout=5)
                w.h.event('w5_engine_crash',operation=op,exit_status=p.returncode,recorded=False)
        finally:
            for path in (hold,fail):
                if path.exists():path.unlink()
        expected='needs_reconciliation' if arm=='crash' else 'failed'
        successor(w,op,expected)
        after=snapshot(w,op)
        assert not after['ledger'] and not after['machines'] and not orders(w,op)
        assert after['commitment']['reserved_sats']==(43 if arm=='crash' else 0)
        if arm!='crash':
            assert after['commitment']['state']=='closed' and not after['exclusions']
            w.operation_reads(t,op,'network' if arm=='failure' else 'conflict',True)
        else:w.operation_reads(t,op,'internal',False)
        trace(w,op,'step 8 acknowledged before step 9',arm=arm)
        restore_resource(w,t)
    # Existing real-binary repeat and account/tenant refusal guard probes remain
    # part of this W5 selector as well as the historical workflow inventory.
    from workflow import licensing, lost_ack_refusals
    from w4_regressions import funded
    licensing(w,funded(w,'w5-step8-repeat'))
    lost_ack_refusals(w)


def accepted_lost(w):
    """ENG-05 interruption arm only: independent effect, unknown acceptance."""
    from harness import eventually
    t,body,op=admit(w,'w5-accepted-lost');w.provider_control(mode='drop_reply')
    try:
        p=w.start_engine();w.wait_status(op,'needs_reconciliation');w.stop_engine()
        before=snapshot(w,op);evidence=external(w,op)
        accepted=[e for e in evidence if e['stage']=='accepted'];created=[e for e in evidence if e['stage']=='resource_created']
        assert accepted and created and any(e['stage']=='reply_dropped' for e in evidence)
        count=len(orders(w,op))
        w.h.event('w5_blind_retry_observed',operation=op,orders=count,resources=len(created),accepted=len(accepted),retry_activated=bool(events(p,'defect_provider_retry')),reply_dropped=True)
        assert count==1 and len(created)==1, 'OPS-12 lost reply must not dispatch a second purchase'
        assert before['operation']['error']['details']['accepted'] is None
        assert before['commitment']['state']=='open' and before['commitment']['reserved_sats']==43
        assert not before['machines'] and not before['ledger']
        view=w.operation_reads(t,op,before['operation']['error']['kind'],False)
        successor(w,op,'needs_reconciliation')
        assert w.request('POST','/v1/machines',body,t,'w5-accepted-lost',status=202)['id']==op
        assert snapshot(w,op)==before and len(orders(w,op))==1
        trace(w,op,'provider accepted and created; reply lost',unknown_acceptance=True,automatic_reconciliation='not run (W6)')
    finally:w.provider_control(mode='normal')


def settlement_blocked(w):
    """ENG-06/OPS-49: retain final result and confirming-read instant on retry."""
    from harness import eventually
    t,body,op=admit(w,'w5-settlement-blocked');h=w.h
    gate=h.proxy.arm('SET status=$3,result=','send');again=None
    try:
        p=w.start_engine();connection=gate.wait()
        assert any(e.get('operation')==op for e in events(p,'execute_returned'))
        pause_dispatch(w,p)
        verified=[e for e in events(p,'provider_verified') if e.get('operation')==op];assert len(verified)==1
        evidence=external(w,op)
        again=h.proxy.arm('SET status=$3,result=','send')
        h.proxy.close_connection(connection,'w5_settlement_precommit_error');gate.release()
        repeat=again.wait()
        assert repeat['pid']!=connection['pid']
        assert events(p,'whole_transaction_retry') and external(w,op)==evidence
        failed_attempt_release(w,p,connection['pid'])
        h.event('w5_settlement_retry_activated',operation=op,failed_session=connection['pid'],repeat_session=repeat['pid'],final_outcome=verified[0],attempts=2,release_evidence='w5_failed_attempt_release')
        again.release();w.wait_status(op,'succeeded')
        eventually(lambda:any(e.get('operation')==op for e in events(p,'settlement_finished')))
        w.stop_engine();after=snapshot(w,op)
        assert external(w,op)==evidence and len(orders(w,op))==1
        from datetime import datetime
        assert datetime.fromisoformat(after['machines'][0]['state_observed_at'])==datetime.fromisoformat(verified[0]['state_observed_at'])
        assert len(after['ledger'])==2
        successor(w,op,'succeeded')
        assert snapshot(w,op)==after and external(w,op)==evidence and len(orders(w,op))==1
        trace(w,op,'known outcome; settlement transport fails before COMMIT',whole_transaction_repeat=True,no_provider_or_verification_repeat=True)
    finally:
        gate.release()
        if again:again.release()
        path=w.control/'hold-dispatch'
        if path.exists():path.unlink()


def settlement_reply(w):
    """ENG-06/STO-3: exact repeats preserve every status/machine/money field."""
    from harness import eventually
    for arm in ('success','release'):
        t,body,op=admit(w,'w5-settlement-reply-'+arm,resource=arm=='release');h=w.h
        fail=w.control/'fail-step-9'
        if arm=='release':fail.touch()
        gate=h.proxy.arm('SET status=$3,result=','send');reply=None
        try:
            p=w.start_engine();connection=gate.wait()
            reply=h.proxy.arm('store COMMIT','commit_reply',session=connection['pid']);gate.release();reply.wait()
            before=snapshot(w,op);provider=external(w,op)
            assert before['operation']['status']==('succeeded' if arm=='success' else 'failed')
            h.event('w5_settlement_reply_loss_activated',operation=op,arm=arm,session=connection['pid'],locally_visible=True,command_complete_received=True,original=before)
            h.proxy.close_connection(connection,'w5_settlement_ack_lost');reply.release()
            eventually(lambda:any(e.get('operation')==op for e in events(p,'settlement_finished')))
            assert events(p,'whole_transaction_retry')
            w.stop_engine();assert snapshot(w,op)==before and external(w,op)==provider
            successor(w,op,before['operation']['status'])
            assert snapshot(w,op)==before and external(w,op)==provider
            if arm=='release':assert before['commitment']['reserved_sats']==0 and before['commitment']['state']=='closed' and not before['ledger'] and not before['exclusions']
            trace(w,op,'settlement COMMIT complete; reply lost',arm=arm,full_rows_identical=True)
        finally:
            gate.release()
            if reply:reply.release()
            if fail.exists():fail.unlink()
            restore_resource(w,t)


def pause_dispatch(w,p):
    """Keep fixture stops separate from unrelated empty-queue claim COMMITs."""
    from harness import eventually
    hold=w.control/'hold-dispatch';hold.touch()
    eventually(lambda:events(p,'test_dispatch_paused'))
    return hold


def stop_record(w):
    """OPS-21: later stop at an actual record boundary, before/after outcome."""
    from harness import eventually
    for arm in ('cut_off','returned','returned_land','cut_off_commit','returned_commit'):
        t,body,op=admit(w,'w5-record-'+arm);h=w.h
        hold=w.control/'hold-step-9'
        if arm.startswith('cut_off'):hold.touch()
        gate=h.proxy.arm('SET status=$3,result=','send') if not arm.startswith('cut_off') else None
        pause=None;reply=None
        try:
            p=w.start_engine()
            if arm.startswith('cut_off'):
                eventually(lambda:any(e.get('operation')==op for e in events(p,'dispatch_step_held')))
                pause=pause_dispatch(w,p)
                gate=h.proxy.arm('SET status=$3,result=','send')
                publish_control(w.control/'stop-operation',op)
            else:
                gate.wait()
                assert any(e.get('operation')==op for e in events(p,'execute_returned'))
                pause=pause_dispatch(w,p)
                publish_control(w.control/'stop-operation',op)
                eventually(lambda:any(e.get('operation')==op for e in events(p,'stop_record_started')))
            connection=gate.wait()
            start=[e for e in events(p,'stop_record_started') if e.get('operation')==op];assert len(start)==1 and start[0]['bound_ms']==1000
            eventually(lambda:events(p,'test_record_snapshot'))
            original=events(p,'test_record_snapshot')[-1]['record']
            assert original['deadline'] is not None
            if arm.endswith('_commit'):
                reply=h.proxy.arm('store COMMIT','commit_reply',session=connection['pid'])
                gate.release();reply.wait()
                committed=snapshot(w,op)
                assert committed['operation']['status']==('failed' if arm.startswith('cut_off') else 'succeeded')
                h.event('w5_stop_record_committed_unobserved',operation=op,arm=arm,session=connection['pid'],state=committed,record=original,command_complete_received=True,application_acknowledgement=False)
            if arm=='returned_land':
                gate.release();reaped(w,p)
                assert w.status(op)=='succeeded'
                assert snapshot(w,op)['operation']['error'] is None
                assert any(e.get('operation')==op and e.get('landed') for e in events(p,'settlement_finished'))
                h.event('w5_first_stop_after_return',operation=op,final_outcome='succeeded',cut_off=False,record=original,engine_exit=p.returncode)
            else:
                marker=len(h.events);started=len(events(p,'stop_record_started'))
                (w.control/'later-stop').touch()
                reaped(w,p)
                later=events(p,'test_later_stop_arbitrated')
                assert later, 'later stop arbitration never activated'
                h.event('w5_later_stop_arbitration_observed',operation=op,arm=arm,original=original,after=later[-1])
                assert later[-1]['record']==original and later[-1]['fatal'] and 'later stop' in (later[-1]['error'] or ''), 'OPS-21 later stop arbitration must preserve original absolute deadline'
                assert len(events(p,'stop_record_started'))==started
                assert not any(e['event']=='bytes_transmitted' and e.get('query','').startswith('BEGIN') for e in h.events[marker:])
                h.event('w5_later_stop_activated',operation=op,arm=arm,session=connection['pid'],original_record=start[0],record_state=original,record_count=started,no_transaction_after_later_stop=True,engine_exit=p.returncode)
        finally:
            if gate:gate.release()
            if reply:reply.release()
            for path in (hold,w.control/'later-stop',w.control/'stop-operation',w.control/'hold-dispatch'):
                if path.exists():path.unlink()
        before=snapshot(w,op)['operation']['status']
        successor(w,op,'needs_reconciliation' if before=='running' else before)
        trace(w,op,'later stop during stop record',arm=arm,server_outcome_before_restart=before)
        if arm.endswith('_commit'):assert snapshot(w,op)==committed


def generation_record(w):
    """ENG-01 selected record path: supersession is immediate, never retried."""
    from harness import eventually
    t,body,op=admit(w,'w5-generation-record');h=w.h
    hold=w.control/'hold-step-9';hold.touch();gate=None
    try:
        p=w.start_engine()
        eventually(lambda:any(e.get('operation')==op and e.get('step')==9 for e in events(p,'dispatch_step_held')))
        pause_dispatch(w,p)
        gate=h.proxy.arm('FOR SHARE /* OPS-50 first statement */','send')
        publish_control(w.control/'stop-operation',op)
        connection=gate.wait();before=snapshot(w,op)
        old=h.sql('SELECT generation FROM provisiond.startup_generation')
        new=h.sql('UPDATE provisiond.startup_generation SET generation=gen_random_uuid() RETURNING generation')
        assert old!=new
        marker=len(h.events);gate.release();reaped(w,p)
        superseded=events(p,'engine_superseded');after=snapshot(w,op)
        refused=bool(superseded) and before==after
        h.event('w5_generation_observed',operation=op,session=connection['pid'],old_generation=old,new_generation=new,refused=refused,
                unequal_checks=[e for e in events(p,'generation_checked') if e.get('equal') is False],unsafe_record=after['operation']['status']!='running',engine_exit=p.returncode)
        assert refused, 'OPS-50 stale generation record must refuse without retry'
        assert not events(p,'whole_transaction_retry')
        # No business entry follows the refused first statement; ROLLBACK is allowed.
        assert not any(e['event']=='bytes_transmitted' and e.get('query','').startswith('BEGIN') for e in h.events[marker:])
    finally:
        if gate:gate.release()
        for path in (hold,w.control/'hold-dispatch',w.control/'stop-operation'):
            if path.exists():path.unlink()
    successor(w,op,'needs_reconciliation')
    trace(w,op,'generation refusal during stop record',no_retry=True)
    if w.defect is None:generation_after_lost_record(w)


def lock_loss(w):
    """ENG-02/07: create execute, older record and real SyncRep expiry arms."""
    from harness import eventually
    from scenarios import pause_standby,resume_standby
    from w4_publication import verification_stop
    verification_stop(w,'search')
    for arm in ('older_record','returned_retry','freeze'):
        t,body,op=admit(w,'w5-lock-'+arm);h=w.h
        hold=w.control/'hold-step-9';hold.touch();gate=None;paused=False
        try:
            p=w.start_engine()
            eventually(lambda:any(e.get('operation')==op and e.get('step')==9 for e in events(p,'dispatch_step_held')))
            pause_dispatch(w,p)
            generation=h.sql('SELECT generation FROM provisiond.startup_generation')
            lock=events(p,'startup_lock_acquired')[-1]['session']
            gate=h.proxy.arm('SET status=$3,result=','send')
            if arm=='returned_retry':hold.unlink()
            else:publish_control(w.control/'stop-operation',op)
            connection=gate.wait()
            if arm!='freeze':
                first=connection
                repeat=h.proxy.arm('SET status=$3,result=','send')
                h.proxy.close_connection(first,'w5_record_first_store_error');gate.release()
                connection=repeat.wait();gate=repeat
                assert events(p,'whole_transaction_retry')
                h.event('w5_lock_record_retry',operation=op,failed_session=first['pid'],repeat_session=connection['pid'],returned_outcome=arm=='returned_retry')
            original=[e for e in events(p,'stop_record_started') if e.get('operation')==op]
            if arm!='returned_retry':assert len(original)==1
            commit=None
            if arm=='freeze':
                commit=h.proxy.arm('store COMMIT','send',session=connection['pid'])
                gate.release();commit.wait();pause_standby(h);paused=True;commit.release()
                eventually(lambda:h.sql(f"SELECT count(*) FROM pg_stat_activity WHERE pid={connection['pid']} AND wait_event='SyncRep'")=='1')
            assert h.sql(f"SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE pid={lock} AND state='idle' AND wait_event IS DISTINCT FROM 'SyncRep'")=='t'
            eventually(lambda:events(p,'startup_lock_lost'))
            detection=len(h.events)
            if arm!='freeze':
                if arm=='older_record':
                    initial=events(p,'test_record_snapshot')[-1]['record']
                    (w.control/'snapshot-record').touch()
                    eventually(lambda:len(events(p,'test_record_snapshot'))>=2)
                    assert events(p,'test_record_snapshot')[-1]['record']==initial
                else:
                    original=[e for e in events(p,'stop_record_started') if e.get('operation')==op]
                    assert len(original)==1 and original[0].get('existing_settlement') is True
                gate.release()
            reaped(w,p)
            assert h.sql('SELECT generation FROM provisiond.startup_generation')==generation
            assert len([e for e in events(p,'stop_record_started') if e.get('operation')==op])==1
            loss=next(i for i,e in enumerate(events(p)) if e.get('event')=='startup_lock_lost')
            assert not any(e.get('event')=='claim_attempt_started' for e in events(p)[loss+1:])
            if arm=='freeze':
                assert any('stop bound expired' in e.get('reason','') and op in e['reason'] for e in events(p,'engine_fatal'))
                assert h.sql(f"SELECT count(*) FROM pg_stat_activity WHERE pid={connection['pid']} AND wait_event='SyncRep'")=='1'
            else:
                finished=events(p,'settlement_finished' if arm=='returned_retry' else 'stop_record_finished')
                assert any(e.get('operation')==op and e.get('landed') for e in finished)
                assert w.status(op)==('succeeded' if arm=='returned_retry' else 'failed')
            h.event('w5_lock_loss_activated',operation=op,arm=arm,lock_session=lock,record_session=connection['pid'],generation_unchanged=True,original_record=original[0],engine_exit=p.returncode,record_expired=arm=='freeze',residual_owner=connection['pid'] if arm=='freeze' else None)
        finally:
            if paused:resume_standby(h)
            if gate:gate.release()
            for path in (hold,w.control/'hold-dispatch',w.control/'stop-operation'):
                if path.exists():path.unlink()
        before=w.status(op)
        successor(w,op,'needs_reconciliation' if before=='running' else before)
        trace(w,op,'lock loss while recording',arm=arm,predecessor_status=before)


def retained_checkouts(w):
    """DB-02/CNF-296/299: actual API cancellation retains original checkout."""
    from harness import eventually
    from scenarios import pause_standby,resume_standby
    from w4_regressions import funded
    from workflow import http
    h=w.h;t=funded(w,'w5-retained');body=w.create_body(t);key='w5-retained-key';correlation='w5-retained'
    # The actual local API has one write and one independently budgeted read slot.
    read_marker=len(h.events)
    w.request('GET','/v1/balance',tenant=t)
    read_session=[e['session'] for e in h.events[read_marker:] if e['event']=='bytes_transmitted' and e.get('query','').startswith("SELECT jsonb_build_object('balance_sats'")][-1]
    assert any(e.get('session')==read_session for e in events(w.api,'session_admitted'))
    hold=w.control/'hold-funding';hold.touch()
    cancel=w.control/'cancel-disconnected';cancel.touch()
    gate=h.proxy.arm('store COMMIT','send');conn=None;paused=False
    try:
        conn=socket_request(w,t,body,key,correlation)
        connection=gate.wait();pid=connection['pid']
        owner=[e for e in events(w.api,'commit_possible_dispatch') if e['session']==pid][-1]['owner']
        pause_standby(h);paused=True;gate.release()
        eventually(lambda:h.sql(f"SELECT count(*) FROM pg_stat_activity WHERE pid={pid} AND wait_event='SyncRep'")=='1')
        conn.close()
        eventually(lambda:any(e.get('correlation_id')==correlation for e in events(w.api,'http_handler_cancelled')))
        # Let the isolated defect's closed-receiver branch run before checking.
        if w.defect=='w5_checkout':
            eventually(lambda:any(e.get('owner')==owner for e in events(w.api,'defect_checkout_released')))
        publish_control(w.control/'observe-commit-owner',str(owner))
        eventually(lambda:any(e.get('owner')==owner for e in events(w.api,'test_commit_checkout')))
        ownership=[e for e in events(w.api,'test_commit_checkout') if e.get('owner')==owner][-1]
        assert ownership['session']==pid and ownership['transport_dead'] is False
        retained=ownership['pooled_checkout_held']
        h.event('w5_checkout_retention_observed',session=pid,owner=owner,cancelled=True,syncrep=True,retained=retained,ownership=ownership,api_alive=w.api.poll() is None,configured_write_checkouts=1)
        assert retained, 'STO-7 HTTP cancellation must retain original sent-COMMIT checkout'
        assert not connection['closed'] and w.api.poll() is None
        assert not any(e.get('owner')==owner for e in events(w.api,'commit_owner_completed'))
        start=len(events(w.api,'http_business_begin'));fresh='w5-retained-fresh'
        code,value,_=http(h.ports[4],'POST','/v1/machines',body,t['spending_token'],fresh)
        assert code==503 and value['error']['kind']=='overloaded' and value['error']['details']['retry_after_ms']==250
        assert len(events(w.api,'http_business_begin'))==start
        assert h.sql(f"SELECT count(*) FROM provisiond.idempotency_records WHERE scope_id='{t['tenant_id']}' AND key='{fresh}'")=='0'
        read_marker=len(h.events)
        w.request('GET','/v1/balance',tenant=t)
        actual=[e['session'] for e in h.events[read_marker:] if e['event']=='bytes_transmitted' and e.get('query','').startswith("SELECT jsonb_build_object('balance_sats'")]
        assert actual==[read_session], 'CNF-296 read must retain established checked physical session'
        assert h.sql(f"SELECT count(*) FROM pg_stat_activity WHERE pid={pid} AND wait_event='SyncRep'")=='1'
        h.event('w5_retained_overload',session=pid,fresh_key_business_begin=False,receipt=False,read_succeeded=True)
    finally:
        if conn:conn.close()
        if paused:resume_standby(h)
        gate.release()
        for path in (cancel,hold):
            if path.exists():path.unlink()
    eventually(lambda:any(e.get('owner')==owner for e in events(w.api,'commit_owner_completed')))
    classified=[e for e in events(w.api,'commit_classified') if e.get('owner')==owner]
    assert len(classified)==1 and classified[0]['outcome']=='acknowledged', 'STO-7 original cancelled admission must be acknowledged after standby recovery'
    op=original_receipt(w,t,key)
    assert w.request('POST','/v1/machines',body,t,key,status=202)['id']==op, 'API-11 replay must return original acknowledged admission'
    w.start_engine();w.wait_status(op,'succeeded');w.stop_engine()
    assert len(orders(w,op))==1
    from scenarios import Probe
    h.run(['cargo','build','--example','store-probe','--features','store-test-hooks','--locked'],timeout=120)
    executable=h.target/'debug/examples/store-probe'
    h.event('workflow_binary_started',component='store-probe',case=w.case,**w.build_identity(executable,['store-test-hooks']))
    probe=Probe(h)
    probe.command('admit')
    settings=probe.command('settings')['value']['settings']
    assert 'synchronous_commit=on' in settings
    probe.finish()
    trace(w,op,'all API write checkouts in SyncRep after HTTP cancellation',original_owner=owner,original_session=pid,established_read_session=read_session,fresh_checked_settings=settings)


def connection_check(w):
    """DB-09/CNF-365: healthy replication, attempted session closed unused."""
    from harness import eventually
    h=w.h
    for component in ('api','engine'):
        if component=='api':
            w.api.terminate();w.api.wait(timeout=5)
        healthy=healthy_standby(h,component)
        gate=h.proxy.arm('store admission target','reply')
        try:
            marker=len(h.events)
            if component=='api':
                p=w.start('api',dict(PROVISIOND_API_LISTEN=f'127.0.0.1:{h.ports[4]}',PROVISIOND_RAIL_SIMULATOR=f'127.0.0.1:{h.ports[6]}',PROVISIOND_LOCAL_PARAMETERS=str(w.parameters),PROVISIOND_TEST_CONTROL=str(w.control)))
                w.api=p
            else:p=w.start('engine',dict(PROVISIOND_TEST_CONTROL=str(w.control)));w.engine=p
            connection=gate.wait();pid=connection['pid'];reaped(w,p)
            eventually(lambda:any(e['event']=='transport_closed' and e.get('session')==pid and e.get('source')=='client_eof' for e in h.events[marker:]))
            sent=[e.get('query','') for e in h.events[marker:] if e['event']=='bytes_transmitted' and e.get('session')==pid]
            assert not any(q.startswith(('BEGIN','UPDATE','INSERT','COMMIT')) for q in sent)
            assert not any(e.get('session')==pid for e in events(p,'session_admitted'))
            assert h.sql(f"SELECT count(*) FROM pg_stat_activity WHERE pid={pid} AND wait_event='SyncRep'")=='0'
            h.event('w5_connection_check_activated',component=component,session=pid,closed_unused=True,no_check_write=True,no_fallback=True,exit_status=p.returncode,healthy_replication_before_fault=healthy)
        finally:gate.release()
        if component=='api':w.launch_api()
        else:w.start_engine();w.stop_engine()


def process_death(w):
    """DB-03/CNF-373 bounded residual/replacement/unchanged-history subset."""
    import time
    from harness import eventually
    from scenarios import pause_standby,resume_standby
    from w4_regressions import funded
    t=funded(w,'w5-process-death');h=w.h;body=w.create_body(t);key='w5-process-death-key'
    hold=w.control/'hold-funding';hold.touch()
    gate=h.proxy.arm('store COMMIT','send');conn=None;paused=False
    try:
        conn=socket_request(w,t,body,key,'w5-process-death')
        connection=gate.wait();pid=connection['pid']
        pause_standby(h);paused=True;gate.release()
        eventually(lambda:h.sql(f"SELECT count(*) FROM pg_stat_activity WHERE pid={pid} AND wait_event='SyncRep'")=='1')
        w.api.kill();w.api.wait(timeout=5);conn.close()
        assert h.sql(f"SELECT count(*) FROM pg_stat_activity WHERE pid={pid} AND wait_event='SyncRep'")=='1'
        attempts=[]
        for attempt in (1,2):
            time.sleep(.1*attempt)
            check=h.proxy.arm('store admission target','reply')
            try:
                p=w.start('api',dict(PROVISIOND_API_LISTEN=f'127.0.0.1:{h.ports[4]}',PROVISIOND_RAIL_SIMULATOR=f'127.0.0.1:{h.ports[6]}',PROVISIOND_LOCAL_PARAMETERS=str(w.parameters)))
                w.api=p;c=check.wait();reaped(w,p)
                assert not events(p,'api_listening') and not events(p,'http_business_begin')
                assert h.sql(f"SELECT count(*) FROM pg_stat_activity WHERE pid={pid} AND wait_event='SyncRep'")=='1'
                attempts.append(dict(attempt=attempt,backoff_ms=100*attempt,exit_status=p.returncode,session=c['pid'],connection_check_passed=False))
            finally:check.release()
        h.event('w5_process_death_activated',old_backend=pid,old_process_dead=True,residual_syncrep=True,replacements=attempts,
                replacement_limit_reached=True,sustained_failure=True,pool_occupancy_excludes_old_backend=True)
        # STO-55 steps 1/2: all replacements stopped; no component work remains.
        assert all(p.poll() is not None for p in w.children if getattr(p,'name',None) in ('api','engine'))
        h.event('w5_operator_recovery',step='stop replacements and quiesce',complete=True)
        resume_standby(h);paused=False
        eventually(lambda:h.sql(f"SELECT count(*) FROM pg_stat_activity WHERE pid={pid} AND wait_event='SyncRep'")=='0')
        op=original_receipt(w,t,key)
        h.event('w5_operator_recovery',step='restore designated standby on unchanged history',complete=True,individual_syncrep_cancelled=False,primary_shutdown_required=False,original_operation=op,application_acknowledgement='not observed: process killed',database_outcome='original admission persisted')
    finally:
        if conn:conn.close()
        if paused:resume_standby(h)
        gate.release()
        if hold.exists():hold.unlink()
    w.launch_api()
    assert w.request('POST','/v1/machines',body,t,key,status=202)['id']==op, 'API-11 replay after process death must return original persisted admission'
    w.start_engine();w.wait_status(op,'succeeded');w.stop_engine()
    assert len(orders(w,op))==1
    trace(w,op,'process killed with residual SyncRep backend',replacement_attempts=attempts,operator_recovery_completed=True,
          deployment_saturation='not run (W8)',promotion_incident='not run (W7)',automatic_reconciliation='not run (W6)')


def contention(w):
    """CNF-296 contending engine write/OPS-49: real row locks exhaust retry."""
    import subprocess
    from harness import eventually
    from scenarios import pause_standby,resume_standby
    t,body,op=admit(w,'w5-contention');h=w.h
    hold=w.control/'hold-step-9';hold.touch();funding=w.control/'hold-funding';funding.touch()
    gate=None;locker=None;paused=False
    try:
        p=w.start_engine()
        eventually(lambda:any(e.get('operation')==op and e.get('step')==9 for e in events(p,'dispatch_step_held')))
        pause_dispatch(w,p)
        gate=h.proxy.arm('SET status=$3,result=','send');hold.unlink();connection=gate.wait()
        provider=external(w,op);assert any(e.get('operation')==op for e in events(p,'execute_returned'))
        output=h.root/'w5-locker.stdout';error=h.root/'w5-locker.stderr'
        with output.open('w') as out,error.open('w') as err:
            locker=subprocess.Popen(['psql','-XAt','-v','ON_ERROR_STOP=1','-h','127.0.0.1','-p',str(h.ports[0]),'-U','app','-d','postgres'],
                stdin=subprocess.PIPE,stdout=out,stderr=err,text=True,env=h.env)
        h.extra.append(locker)
        locker.stdin.write(f"BEGIN; SELECT pg_backend_pid(); SELECT id FROM provisiond.commitments WHERE operation_id='{op}' FOR UPDATE;\n");locker.stdin.flush()
        eventually(lambda:len(output.read_text().splitlines())>=3)
        pid=int(output.read_text().splitlines()[1])
        pause_standby(h);paused=True;gate.release()
        eventually(lambda:h.sql(f"SELECT count(*) FROM pg_stat_activity WHERE pid<>{pid} AND {pid}=ANY(pg_blocking_pids(pid))")!='0')
        reaped(w,p)
        retries=events(p,'whole_transaction_retry');assert len(retries)>=3
        failed=[e for e in events(p,'transaction_resources_released') if e.get('component_available')==1]
        retried={e.get('transaction') for e in retries}
        attempted=[e for e in failed if e['transaction'] in retried]
        assert len(attempted)==len(retries)
        for released in attempted:
            failed_attempt_release(w,p,released['session'],released)
        assert any('store retry exhausted' in e.get('reason','') for e in events(p,'engine_fatal'))
        assert w.status(op)=='running' and external(w,op)==provider
        h.event('w5_contention_activated',operation=op,locker_backend=pid,engine_session=connection['pid'],standby_paused=True,retries=retries,engine_exit=p.returncode,final_outcome_retained_until_exit=True,no_provider_repeat=True)
    finally:
        if locker and locker.poll() is None:
            locker.stdin.write('ROLLBACK;\n\\q\n');locker.stdin.flush();locker.wait(timeout=5)
        if paused:resume_standby(h)
        if gate:gate.release()
        for path in (hold,funding,w.control/'hold-dispatch'):
            if path.exists():path.unlink()
    successor(w,op,'needs_reconciliation')
    assert len(orders(w,op))==1
    trace(w,op,'business row contention during replication stall',automatic_reconciliation='not run (W6)')


def remote_completion(w):
    """OPS-47/50: generation/lock fencing cannot undo a remote effect."""
    from harness import eventually
    t,body,op=admit(w,'w5-remote-completion');h=w.h
    hold=h.root/'provider/hold-before-effect';hold.touch()
    try:
        p=w.start_engine()
        eventually(lambda:any(e['stage']=='effect_held' for e in external(w,op)))
        assert not any(e['stage']=='resource_created' for e in external(w,op))
        lock=events(p,'startup_lock_acquired')[-1]['session']
        generation=h.sql('SELECT generation FROM provisiond.startup_generation')
        assert h.sql(f"SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE pid={lock} AND state='idle' AND wait_event IS DISTINCT FROM 'SyncRep'")=='t'
        reaped(w,p)
        assert events(p,'startup_lock_lost') and h.sql('SELECT generation FROM provisiond.startup_generation')==generation
        before=snapshot(w,op);assert before['operation']['status']=='needs_reconciliation'
        assert not any(e['stage']=='resource_created' for e in external(w,op))
        h.event('w5_remote_pending_after_exit',operation=op,lock_session=lock,engine_exit=p.returncode,accepted=True,effect_completed=False,record=before)
        hold.unlink()
        eventually(lambda:any(e['stage']=='resource_created' for e in external(w,op)))
        created=[e for e in external(w,op) if e['stage']=='resource_created'];assert len(created)==1
        assert created[0]['resource'] in w.external()['resources']
        successor(w,op,'needs_reconciliation')
        assert w.request('POST','/v1/machines',body,t,'w5-remote-completion',status=202)['id']==op
        assert snapshot(w,op)==before and len(orders(w,op))==1
        trace(w,op,'remote effect held until after lock-loss record and engine exit',remote_effect_completed_after_exit=True,automatic_reconciliation='not run (W6)')
    finally:
        if hold.exists():hold.unlink()


SCENARIOS=dict(zip(CASES,[admission_before,admission_reply,claim_reply,step8,accepted_lost,
    settlement_blocked,settlement_reply,stop_record,lock_loss,retained_checkouts,
    process_death,connection_check,contention,remote_completion,generation_record],strict=True))


def default_hooks(w):
    """OPS-12/STO-7: environment/files cannot arm faults in ordinary binaries."""
    from w4_regressions import funded
    h=w.h;w.api.terminate();w.api.wait(timeout=5)
    original_target=h.target;original_env=h.env;original_builds=w.builds;original_defect=w.defect
    target=h.root/'w5-default-target'
    h.owned_extra_paths=getattr(h,'owned_extra_paths',[])+[target]
    paths=[]
    try:
        h.target=target;h.env=dict(h.env,CARGO_TARGET_DIR=str(target))
        h.run(['cargo','build','--bins','--locked'],timeout=120)
        w.builds={name:w.build_identity(target/'debug'/name,[]) for name in ('api','engine','migrate')}
        h.config['workflow']['w5_default_builds']=w.builds
        w.defect='w5_blind_retry';w.launch_api()
        t=funded(w,'w5-default-hooks');body=w.create_body(t)
        for name in ('hold-http-response','cancel-disconnected','hold-step-9','later-stop','hold-dispatch'):
            path=w.control/name;path.touch();paths.append(path)
        op=w.request('POST','/v1/machines',body,t,'w5-default-success',status=202)['id']
        stop=w.control/'stop-operation';publish_control(stop,op);paths.append(stop)
        p=w.start_engine();w.wait_status(op,'succeeded');w.stop_engine()
        assert not any(events(p,kind) for kind in ('test_stop_requested','test_dispatch_paused','dispatch_step_held','defect_provider_retry'))
        assert not events(w.api,'http_response_held') and not events(w.api,'http_handler_cancelled')
        assert stop.exists() and len(orders(w,op))==1
        w.provider_control(mode='drop_reply')
        other=w.request('POST','/v1/machines',body,t,'w5-default-unknown',status=202)['id']
        w.defect='w5_stop_reset'
        p=w.start_engine();w.wait_status(other,'needs_reconciliation');w.stop_engine()
        assert not events(p,'test_later_stop_arbitrated')
        assert len(orders(w,other))==1 and not events(p,'defect_provider_retry')
        h.event('w5_default_observed',features=[],fault_files_ignored=True,defect_environment_ignored=True,
                stopped_file_unconsumed=True,success_operation=op,unresolved_operation=other,orders_per_operation=1,defect_environments=['w5_blind_retry','w5_stop_reset'])
        trace(w,other,'default build with fault files and blind-retry environment',faults_activated=False,expected_inert=True)
    finally:
        w.stop_engine()
        if w.api.poll() is None:w.api.terminate();w.api.wait(timeout=5)
        for path in paths:
            if path.exists():path.unlink()
        h.target=original_target;h.env=original_env;w.builds=original_builds;w.defect=original_defect
        w.provider_control(mode='normal');w.launch_api()


CASES.append('w5_default_hooks')
SCENARIOS['w5_default_hooks']=default_hooks


def inventory_complete(manifest):
    """The aggregate admits only a complete W5 run, never a focused subset."""
    expected=CASES
    actual=[r.get('test') for r in manifest.get('scenarios',[]) if r.get('test') in expected and r.get('result')=='pass']
    inventory=[e for e in manifest.get('events',[]) if e.get('event')=='w5_inventory_complete']
    cleanup=[e for e in manifest.get('events',[]) if e.get('event')=='cleanup_complete']
    return (manifest.get('result')=='pass' and actual==expected
        and len(inventory)==1 and inventory[0].get('required')==expected and inventory[0].get('observed')==expected
        and all(any(e.get('event')=='w5_accounting_activated' and e.get('scenario')==name and e.get('activated') is True for e in manifest.get('events',[])) for name in ACCOUNTING_CASES)
        and bool(cleanup) and cleanup[-1].get('result')=='pass')


def claim_precommit(w):
    """OPS-49: a known pre-dispatch claim failure is ordinary store failure."""
    from harness import eventually
    t,body,op=admit(w,'w5-claim-precommit');h=w.h
    claim=h.proxy.arm("SET status='running',claim_number=claim_number+1",'send');verify=None
    try:
        p=w.start_engine();connection=claim.wait();pid=connection['pid']
        marker=len(h.events)
        verify=h.proxy.arm('SHOW synchronous_commit /* precommit verify */','reply',session=pid)
        claim.release();verify.wait()
        assert not any(e['event']=='bytes_transmitted' and e.get('session')==pid and 'store COMMIT' in e.get('query','') for e in h.events[marker:])
        assert w.status(op)=='queued' and not orders(w,op)
        h.proxy.close_connection(connection,'w5_claim_known_precommit_failure');verify.release()
        eventually(lambda:p.poll() is not None or w.status(op)=='succeeded')
        h.event('w5_claim_precommit_observed',operation=op,session=pid,commit_dispatched=False,
                engine_exit=p.poll(),status=w.status(op),retry_events=events(p,'claim_precommit_retry'))
        assert p.poll() is None and w.status(op)=='succeeded', 'OPS-49 known pre-COMMIT claim failure must retry the whole transaction'
        learned=next(i for i,e in enumerate(events(p)) if e.get('event')=='claim_registered' and e.get('operation')==op)
        prefix=events(p)[:learned]
        retries=[e for e in prefix if e.get('event')=='claim_precommit_retry']
        assert len(retries)==1 and retries[0]['spent_ms']<2000
        assert sum(e.get('event')=='claim_attempt_started' for e in prefix)==2
        w.stop_engine();assert len(orders(w,op))==1
        successor(w,op,'succeeded')
        trace(w,op,'claim failed definitely before COMMIT dispatch',whole_claim_retried=True)
    finally:
        claim.release()
        if verify:verify.release()


CASES.append('w5_claim_precommit')
SCENARIOS['w5_claim_precommit']=claim_precommit


def backoff_budget(w):
    """OPS-49 supplemental real-runtime probe: backoff cannot buy another attempt."""
    import json
    import time
    from core import core_probe
    h=w.h;t,body,op=admit(w,'w5-backoff-budget')
    h.run(['cargo','build','--example','core-probe','--features','store-test-hooks','--locked'],timeout=120)
    executable=h.target/'debug/examples/core-probe'
    h.event('workflow_binary_started',component='core-probe',case=w.case,**w.build_identity(executable,['store-test-hooks']))
    p=core_probe(h);p.command('engine_start');claim=p.command('claim')['value']
    assert claim['id']==op
    gate=h.proxy.arm('BEGIN READ WRITE','send')
    try:
        marker=len(h.events)
        p.send('defer '+json.dumps(claim))
        connection=gate.wait()
        # Hold an idle physical session before BEGIN, so the independent idle-
        # transaction timeout cannot supply this failure. The actual retry trace
        # below must prove activation in the final backoff window, or fail.
        time.sleep(1.993)
        h.proxy.close_connection(connection,'w5_backoff_last_window');gate.release()
        from harness import eventually
        eventually(lambda:p.p.poll() is not None or any(e.get('event')=='result' and e.get('command')=='defer' for e in p.events))
        result=next((e for e in p.events if e.get('event')=='result' and e.get('command')=='defer'),None)
        fatal=any(e.get('event')=='engine_fatal' and 'store retry exhausted' in e.get('reason','') for e in p.events)
        p.expected_status=71 if fatal else 0;p.finish()
        retries=[e for e in p.events if e.get('event')=='whole_transaction_retry']
        assert len(retries)==1 and 1990<=retries[0]['spent_ms']<2000, 'backoff exhaustion boundary did not activate'
        begins=[e for e in h.events[marker:] if e['event']=='bytes_transmitted' and e.get('query')=='BEGIN READ WRITE']
        h.event('w5_backoff_budget_observed',operation=op,spent_before_backoff_ms=retries[0]['spent_ms'],backoff_ms=10,
                retry_bound_ms=2000,new_begin_after_backoff=len(begins),result=result,probe_exit=p.p.returncode,fatal_exhaustion=fatal)
        assert not begins and fatal and p.p.returncode==71, 'OPS-49 backoff exhaustion must forbid another transaction'
    finally:
        gate.release()
    successor(w,op,'failed')
    assert not orders(w,op)
    trace(w,op,'backoff crosses original retry bound',supplemental_runtime_probe=True)


CASES.append('w5_backoff_budget')
SCENARIOS['w5_backoff_budget']=backoff_budget



def generation_after_lost_record(w):
    """OPS-50: a refused repeat cannot undo an earlier unobserved COMMIT."""
    from harness import eventually
    t,body,op=admit(w,'w5-generation-after-record');h=w.h
    hold=w.control/'hold-step-9';hold.touch();statement=None;reply=None;repeat=None
    try:
        p=w.start_engine()
        eventually(lambda:any(e.get('operation')==op and e.get('step')==9 for e in events(p,'dispatch_step_held')))
        pause_dispatch(w,p)
        statement=h.proxy.arm('SET status=$3,result=','send')
        publish_control(w.control/'stop-operation',op)
        connection=statement.wait();pid=connection['pid']
        reply=h.proxy.arm('store COMMIT','commit_reply',session=pid)
        statement.release();reply.wait()
        committed=snapshot(w,op)
        assert committed['operation']['status']=='failed' and committed['commitment']['state']=='closed'
        repeat=h.proxy.arm('FOR SHARE /* OPS-50 first statement */','send')
        h.proxy.close_connection(connection,'w5_record_commit_lost_before_supersession');reply.release()
        refused=repeat.wait();assert len(events(p,'whole_transaction_retry'))==1
        old=h.sql('SELECT generation FROM provisiond.startup_generation')
        new=h.sql('UPDATE provisiond.startup_generation SET generation=gen_random_uuid() RETURNING generation')
        assert new!=old
        marker=len(h.events);repeat.release();reaped(w,p)
        assert events(p,'engine_superseded') and snapshot(w,op)==committed
        assert not any(e['event']=='bytes_transmitted' and e.get('query','').startswith('BEGIN') for e in h.events[marker:])
        h.event('w5_generation_after_lost_commit',operation=op,committed_session=pid,refused_session=refused['pid'],old_generation=old,new_generation=new,
                prior_record_committed=True,prior_application_acknowledgement=False,whole_retries=1,engine_exit=p.returncode,unchanged_rows=committed)
    finally:
        for gate in (statement,reply,repeat):
            if gate:gate.release()
        for path in (hold,w.control/'hold-dispatch',w.control/'stop-operation'):
            if path.exists():path.unlink()
    successor(w,op,'failed')
    assert snapshot(w,op)==committed and not orders(w,op)
    trace(w,op,'generation refusal after earlier record COMMIT reply lost',earlier_commit_preserved=True)


from w5_accounting import CASES as ACCOUNTING_CASES
CASES.extend(ACCOUNTING_CASES)
SCENARIOS.update(ACCOUNTING_CASES)
