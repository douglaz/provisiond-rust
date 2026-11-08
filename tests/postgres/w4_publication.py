"""OPS-47/21/13: executable dispatch starvation and retained provider evidence."""
import json
import time

from harness import eventually
from scenarios import pause_standby, resume_standby
from w4_regressions import funded


def state(w,op):
    return json.loads(w.h.sql(f"""SELECT jsonb_build_object(
        'status',o.status,'claim',o.claim_number,'error',o.error,'result',o.result,
        'machine',o.machine_id,'request_purged',o.request IS NULL,
        'summary',o.request_summary,'correlator',o.correlator_value,
        'reserved',c.reserved_sats,'commitment_state',c.state,
        'debits',(SELECT count(*) FROM provisiond.ledger_entries WHERE operation_id=o.id))
        FROM provisiond.operations o JOIN provisiond.commitments c ON c.id=o.commitment_id
        WHERE o.id='{op}'"""))


def operation_events(w,op):
    return [e for e in w.external()['events'] if e.get('command')==op]


def lock_dispatch(w):
    """CNF-366 extension: a real claim COMMIT in SyncRep cannot starve observation."""
    for mode in ('syncrep','late_claim','empty_claim'):
        lock_dispatch_arm(w,mode)


def lock_dispatch_arm(w,mode):
    h=w.h;w.stop_engine()
    a=funded(w,'lock-active-'+mode);b=funded(w,'lock-pending-'+mode)
    active=w.request('POST','/v1/machines',w.create_body(a),a,'lock-active',status=202)['id']
    hold=w.control/'hold-step-9';hold.touch()
    gates=[]
    try:
        p=w.start_engine()
        def current(event=None):
            rows=[json.loads(line) for line in p.log.read_text().splitlines() if line.startswith('{')]
            return [e for e in rows if event is None or e.get('event')==event]
        eventually(lambda:any(e.get('operation')==active and e.get('step')==9 for e in current('dispatch_step_held')))
        lock_pid=current('startup_lock_acquired')[-1]['session']
        generation=h.sql('SELECT generation FROM provisiond.startup_generation')
        # This is a normal dispatcher iteration, held before selecting B. A has
        # an acknowledged claim and license, with its execution still represented.
        selection=h.proxy.arm("SELECT id::text FROM provisiond.operations WHERE status='queued'",'send');gates.append(selection)
        connection=selection.wait();pid=connection['pid']
        assert pid!=lock_pid and p.poll() is None
        pending=None if mode=='empty_claim' else w.request('POST','/v1/machines',w.create_body(b),b,'lock-pending-'+mode,status=202)['id']
        if pending:eventually(lambda:h.sql(f"SELECT clock_timestamp()>=available_at FROM provisiond.operations WHERE id='{pending}'")=='t')
        commit=h.proxy.arm('COMMIT /* store COMMIT */','send',session=pid);gates.append(commit)
        selection.release();commit.wait()
        owners=[e for e in current('commit_possible_dispatch') if e['session']==pid]
        owner=owners[-1]['owner']
        assert state(w,active)['status']=='running'
        assert not operation_events(w,active) or not any(e['stage']=='accepted' for e in operation_events(w,active))
        reply=None;record=None
        if mode=='syncrep':
            pause_standby(h);commit.release()
            eventually(lambda:h.sql(f"SELECT count(*) FROM pg_stat_activity WHERE pid={pid} AND wait_event='SyncRep' AND query='COMMIT /* store COMMIT */'")=='1')
        else:
            reply=h.proxy.arm('COMMIT /* store COMMIT */','commit_reply',session=pid);gates.append(reply)
            record=h.proxy.arm('UPDATE provisiond.operations SET status=$3,result=','send');gates.append(record)
            commit.release();reply.wait()
        if pending:
            before=state(w,pending)
            assert before['status']==('queued' if mode=='syncrep' else 'running')
            assert before['claim']==(0 if mode=='syncrep' else 1)
            assert any(e['event']=='bytes_transmitted' and e.get('session')==pid and "SET status='running',claim_number=claim_number+1" in e.get('query','') for e in h.events)
        assert not any(e.get('operation')==pending for e in current('claim_registered'))
        h.event('dispatch_claim_syncrep_armed',active_operation=active,pending_operation=pending,
                claim_number=1 if pending else None,session=pid,commit_owner=owner,lock_session=lock_pid,arm=mode,
                generation=generation,represented_workers=1,pending_claims=1,configured_workers=3,
                wait_event='SyncRep' if mode=='syncrep' else 'reply gate only',claim_registered=False,setup_readiness_completed=True)
        # Attribute the otherwise session-less driver event: exactly one new
        # driver death, a forwarded 57P01 on this lock, and the held claim alive.
        deaths=len(current('driver_dead'))
        assert h.sql(f"SELECT state FROM pg_stat_activity WHERE pid={lock_pid}")=='idle'
        assert h.sql(f'SELECT pg_terminate_backend({lock_pid})')=='t'
        eventually(lambda:any(e['event']=='server_error_forwarded' and e['session']==lock_pid and e['sqlstate']=='57P01' for e in h.events))
        eventually(lambda:len(current('driver_dead'))==deaths+1)
        died=time.monotonic()
        h.event('lock_driver_death_attributed',lock_session=lock_pid,new_driver_dead_events=1,
                forwarded_sqlstate='57P01',claim_session=pid,claim_transport_closed=connection['closed'])
        assert not connection['closed'] and p.poll() is None
        if reply:
            record.wait()
            assert any(e.get('operation')==active for e in current('stop_record_started'))
            reply.release()
            eventually(lambda:any(e.get('owner')==owner for e in current('commit_owner_completed')))
            if pending:
                eventually(lambda:any(e.get('operation')==pending for e in current('stop_record_started')))
            time.sleep(.15)
            h.event('dispatch_reply_after_loss',arm=mode,pending_operation=pending,
                    engine_alive=p.poll() is None,original_record_still_pending=True,
                    claim_acknowledged=True,late_claim_stopped=bool(pending))
            assert p.poll() is None, 'OPS-47 dispatch completion must not bypass the original pending record drain'
        # Observation allowance, independent of the application's unchanged 1s
        # absolute record bound. An old executable remains alive with no fan-out.
        end=died+2.5
        while p.poll() is None and time.monotonic()<end:time.sleep(.01)
        events=current()
        loss=[e for e in events if e.get('event')=='startup_lock_lost' and e.get('session')==lock_pid]
        stops=[e for e in events if e.get('event')=='stop_record_started' and e.get('operation')==active]
        completed=[e for e in events if e.get('event')=='commit_owner_completed' and e.get('owner')==owner]
        h.event('dispatch_lock_handling_observed',engine_exit=p.poll(),loss_published=bool(loss),
                represented_worker_stops=stops,claim_owner_completed=bool(completed),
                claim_still_syncrep=h.sql(f"SELECT count(*) FROM pg_stat_activity WHERE pid={pid} AND wait_event='SyncRep'")=='1',
                elapsed_from_driver_death_ms=round((time.monotonic()-died)*1000),
                harness_terminated_engine=False)
        assert loss and stops and p.poll() not in (None,0), 'OPS-47 blocked dispatch must observe lock loss, stop represented workers and exit nonzero'
        assert len(loss)==1 and len(stops)==1 and stops[0]['bound_ms']==1000
        if mode=='syncrep':
            assert not completed,'sent claim COMMIT must retain its owner until process exit'
            assert not any(e.get('operation')==pending for e in events if e.get('event')=='claim_registered')
        else:
            assert completed and time.monotonic()-died>=.8
            if pending:
                assert len([e for e in events if e.get('event')=='claim_registered' and e.get('operation')==pending])==1
        loss_index=next(i for i,e in enumerate(events) if e.get('event')=='startup_lock_lost')
        assert not any(e.get('event')=='claim_attempt_started' for e in events[loss_index+1:])
        assert h.sql('SELECT generation FROM provisiond.startup_generation')==generation
        assert not any(e['stage']=='accepted' for op in [active,pending] if op for e in operation_events(w,op))
        h.event('dispatch_lock_invariants_checked',exit_status=p.returncode,unchanged_generation=True,
                no_new_claim_after_detection=True,no_early_claim_owner_release=True,
                pending_record_bound_ms=1000,periodic_business='not implemented in this profile',
                earlier_record_identity_and_verified_outcome_matrix='existing core regressions')
        h.results.append(dict(test='W4-lock-loss-during-dispatch-'+mode,result='pass'))
    finally:
        # Never cancel/terminate a SyncRep backend. End the application first,
        # restore only this owned receiver, then release transport fixture gates.
        w.stop_engine()
        if h.paused:resume_standby(h)
        for gate in gates:gate.release()
        hold.unlink()
        h.event('dispatch_lock_fixture_recovered',owned_replication_restored=True,arm=mode)


def accepted_identifiers(w):
    """OPS-13/27, PRV-5, WIR-9b/10b, LDG-39/67: evidence is no debit authority."""
    h=w.h;w.stop_engine();w.provider_control(mode='normal')
    # A real, separately admitted order supplies a distinct observed identifier
    # for deliberately wrong/ambiguous verification, never a made-up expected ID.
    seed=funded(w,'identifier-seed')
    seed_op=w.request('POST','/v1/machines',w.create_body(seed),seed,'identifier-seed',status=202)['id']
    w.start_engine();w.wait_status(seed_op,'succeeded');w.stop_engine()
    seed_id=next(e['resource'] for e in operation_events(w,seed_op) if e['stage']=='resource_created')
    matrix=[('search_throttle','rate_limited'),('search_network','network'),
            ('search_non_authoritative','provider'),('search_many','provider'),
            ('get_error','not_found'),('get_absent','provider'),('search_malformed','internal')]
    try:
        for mode,kind in matrix:
            t=funded(w,'identifiers-'+mode)
            w.provider_control(mode=mode,verification_resource=seed_id)
            op=w.request('POST','/v1/machines',w.create_body(t),t,'identifiers-'+mode,status=202)['id']
            admitted=state(w,op)
            w.start_engine();w.wait_status(op,'needs_reconciliation')
            eventually(lambda:any(e.get('operation')==op and e.get('landed') for e in w.events('engine','settlement_finished')))
            w.stop_engine()
            records=operation_events(w,op)
            order=[e for e in records if e.get('path')=='/order']
            assert [e['stage'] for e in order]==['request_received','accepted','resource_created','response_returned']
            created=order[2]['resource'];assert order[3]['resource']==created and created!=seed_id
            faults=[e for e in records if e['stage']=='verification_fault_returned']
            assert len(faults)==1 and faults[0]['mode']==mode
            reads=[e for e in records if e['stage']=='verification_identifiers_returned']
            expected={'order_reply':[created]}
            for read in reads:
                expected.setdefault(read['source'],[]).extend(read['identifiers'])
            expected={k:sorted(set(v)) for k,v in expected.items()}
            stored=state(w,op)
            view=w.operation_reads(t,op,kind,False)
            h.event('accepted_identifier_handoff_observed',operation=op,mode=mode,
                    created_external_id=created,independent_order_stages=[e['stage'] for e in order],
                    activated_fault=faults[0],observed_identifiers=expected,stored_error=stored['error'],
                    detail_list_equal=True,acknowledged_settlement=True)
            assert stored['error']['details'].get('provider_identifiers')==expected, 'OPS-13 unresolved verification must preserve every learned provider identifier in stored error.details'
            assert stored['error']==view['error'] and stored['error']['details']['accepted'] is True
            assert stored['error']['details']['attempted']=='create_machine'
            if mode=='search_throttle':assert stored['error']['details']['retry_after_ms']==1000
            if mode=='get_error':assert stored['error']['details']['status']==404
            assert stored['summary']==dict(admitted['summary'],correlator={'kind':'operation_uuid','value':op})
            assert stored['correlator']==op
            assert stored['summary']['offer']['offer_id']=='test-small'
            assert stored['summary']['setup_fee'] and stored['summary']['first_unit']
            assert stored['request_purged'] and stored['result'] is None and stored['machine'] is None
            assert stored['reserved']==43 and stored['commitment_state']=='open' and stored['debits']==0
            assert 'request' not in view and 'TEST_ONLY_SECRET_MARKER' not in json.dumps(view)
            count=h.sql('SELECT count(*) FROM provisiond.operations')
            w.provider_control(mode='normal');w.start_engine()
            assert state(w,op)==stored
            assert w.operation_reads(t,op,kind,False)==view
            w.stop_engine()
            assert operation_events(w,op)==records and h.sql('SELECT count(*) FROM provisiond.operations')==count
            assert created in w.external()['resources'] and seed_id in w.external()['resources']
            h.event('accepted_identifiers_survive_restart',operation=op,mode=mode,state=stored,
                    unchanged_operation_count=int(count),orders=1,created_resources=1,second_purchase=False)
            h.results.append(dict(test='W4-accepted-verification-error' if mode=='search_throttle' else 'W4-provider-identifiers-'+mode,result='pass'))
    finally:w.stop_engine();w.provider_control(mode='normal')
