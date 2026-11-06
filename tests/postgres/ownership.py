"""OPS-21/47/49/50, STO-13: concrete claim and stop ownership boundaries."""
import json
import subprocess
import time
from core import core_probe, enqueue, empty_queue, command, environment, binary, tenant_holder
from harness import eventually


def held(p,name):
    eventually(lambda:any(e.get('event')=='execution_boundary_held' and e['boundary']==name for e in p.events))


def accepted(p,op):
    return [e for e in p.events if e.get('event')=='simulator_accepted' and e.get('operation')==op]


def terminal_zero(h,defect=None):
    # OPS-22/STO-3: an acknowledged transaction can refuse the stale guard.
    for newer in (False,True):
        empty_queue(h);op=enqueue(h,'terminal-zero-'+str(newer),'terminal-zero-'+str(newer),resource=True,offer='terminal-zero-'+str(newer))
        p=core_probe(h,defect);p.command('engine_start');a=p.command('claim')['value']
        generation=h.sql('SELECT generation FROM provisiond.startup_generation')
        h.sql(f"UPDATE provisiond.operations SET claim_number=claim_number+1,status='queued' WHERE id='{op}'")
        def snapshot():
            return h.sql(f"SELECT jsonb_build_object('operation',(SELECT to_jsonb(o) FROM provisiond.operations o WHERE id='{op}'),'commitment',(SELECT to_jsonb(c) FROM provisiond.commitments c WHERE operation_id='{op}'),'exclusion',(SELECT to_jsonb(e) FROM provisiond.resource_exclusions e WHERE operation_id='{op}'),'ledger',(SELECT count(*) FROM provisiond.ledger_entries WHERE operation_id='{op}'),'machines',(SELECT count(*) FROM provisiond.machines))")
        before=snapshot()
        gate=h.proxy.arm('store COMMIT','commit_reply');p.command('record_stop '+op);gate.wait()
        assert snapshot()==before
        assert p.command('owners')['value']==dict(workers=1,available_slots=2)
        if newer:
            p.command('wait_start');eventually(lambda:any(e.get('event')=='waiter_polled' for e in p.events))
            b=p.command('claim')['value'];assert b['id']==op and b['number']>a['number']
            assert p.command('owners')['value']==dict(workers=1,available_slots=1), 'pending stale record retains its own permit alongside newer worker'
            assert p.command('wait_pending')['value']['pending'], 'existing observer must retain stale record until completion'
            before=snapshot()
        gate.release();p.command('wait_records')
        if newer:p.command('wait_join')
        eventually(lambda:any(e.get('event')=='stop_record_finished' and e.get('operation')==op for e in p.events))
        finished=next(e for e in p.events if e.get('event')=='stop_record_finished' and e.get('operation')==op)
        zero=[e for e in p.events if e.get('event')=='worker_zero_row' and e.get('operation')==op]
        owners=p.command('owners')['value']
        unchanged=snapshot()==before
        h.event('terminal_zero_observed',operation=op,newer_worker=newer,finished=finished,zero_row_diagnostics=zero,owners=owners,row_money_exclusion_unchanged=unchanged,generation_unchanged=h.sql('SELECT generation FROM provisiond.startup_generation')==generation)
        # Drain the stale fixture before its expected red assertion.
        p.finish()
        assert finished.get('landed') is False and zero, 'zero-row stop must report refusal and end stale execution'
        assert finished['affected'] is False and unchanged
        assert h.sql('SELECT generation FROM provisiond.startup_generation')==generation
        assert owners==dict(workers=int(newer),available_slots=3-int(newer)), 'late stale stop must preserve newer worker capacity'
        assert not any(e.get('event')=='whole_transaction_retry' for e in p.events)
        if newer:
            # The newer row is left running for the next ordinary restart fixture.
            assert h.sql(f"SELECT status||':'||claim_number FROM provisiond.operations WHERE id='{op}'")==f"running:{b['number']}"
    h.results.append(dict(test='terminal-stop-zero-row',result='pass',arms='acknowledged guard refusal, unchanged row/money/exclusion, stale-only retirement including newer worker'))



def queued_claim(h,defect=None,only=None):
    # OPS-47: a call queued before detection has not yet dispatched a transaction.
    for window in ([only] if only else ('entry','pool','admitted')):
        empty_queue(h)
        ids=[enqueue(h,'queued-loss-'+window+str(i),'queued-loss-'+window+str(i)) for i in range(2)]
        p=core_probe(h,defect,boundary='lock_registration')
        startup=p.command('engine_start')['value'];active=p.command('claim')['value']['id']
        queued=next(i for i in ids if i!=active)
        if window=='entry':
            blocker=h.proxy.arm('OPS-50 first statement','execute_reply')
            p.command('begin_start');blocker.wait()
        elif window=='pool':p.command('pool_fill')
        else:blocker=h.proxy.arm('store COMMIT','commit_reply')
        marker=len(h.events);client_marker=len(p.events)
        p.command('claim_start')
        if window=='admitted':blocker.wait()
        else:
            waiting='component_entry_waiting' if window=='entry' else 'pool_checkout_waiting'
            eventually(lambda:any(e.get('event')==waiting for e in p.events[client_marker:]))
            assert not any(e.get('event')=='send_requested' and e.get('query')=='BEGIN READ WRITE' for e in h.events[marker:])
            assert h.sql(f"SELECT status||':'||claim_number FROM provisiond.operations WHERE id='{queued}'")=='queued:0'
        h.sql(f"SELECT pg_terminate_backend({startup['lock_pid']})")
        p.command('observe_pair');held(p,'lock_registration')
        assert any(e.get('event')=='startup_lock_lost' for e in p.events)
        pending_at_release=p.command('claim_pending')['value']['pending']
        wire_marker=len(h.events)
        if window=='entry':
            blocker.release();p.command('begin_join');p.command('rollback')
        elif window=='pool':p.command('pool_release')
        else:blocker.release()
        reply=p.command('claim_join',ok=None)
        state=h.sql(f"SELECT status||':'||claim_number FROM provisiond.operations WHERE id='{queued}'")
        begins=[e for e in h.events[wire_marker:] if e.get('event')=='send_requested' and e.get('query')=='BEGIN READ WRITE']
        registered=[e for e in p.events[client_marker:] if e.get('event')=='claim_registered']
        target_events=p.events[client_marker:]
        retries=[e for e in target_events if e.get('event') in ('claim_precommit_retry','whole_transaction_retry')]
        attempts=sum(e.get('event')=='claim_attempt_started' for e in target_events)
        checked_refusals=sum(e.get('event')=='checked_new_work_refused' for e in target_events)
        forbidden=(bool(begins) or bool(registered) or state!='queued:0' or bool(accepted(p,queued))) if window!='admitted' else False
        # Capture before fan-out: its stop-record BEGINs must not be charged to the claim.
        h.event('queued_claim_observed',window=window,operation=queued,detected_before_release=True,reply=reply,new_begin_count=len(begins),claim_registered=len(registered),durable_state=state,external_entries=len(accepted(p,queued)),forbidden_entry=forbidden,pending_at_release=pending_at_release,attempts=attempts,retries=retries,checked_refusals=checked_refusals)
        p.command('boundary_release');p.command('observe_join',ok=False);p.expected_status=71;p.finish()
        assert not forbidden, 'queued claim must not dispatch BEGIN after detected lock loss'
        assert pending_at_release and attempts==1 and not retries, 'queued claim requires its original pending attempt without precommit retry'
        if window=='admitted':
            assert reply['ok'] and state in ('running:1','failed:1') and len(registered)==1
            assert h.sql(f"SELECT status FROM provisiond.operations WHERE id='{queued}'")=='failed', 'already dispatched claim must retain bounded stop ownership'
        else:
            assert checked_refusals==1, 'queued claim requires refusal at checked entry'
            assert not reply['ok'] and 'no claims or periodic passes' in reply.get('error','')
            assert h.sql(f"SELECT status||':'||claim_number FROM provisiond.operations WHERE id='{queued}'")=='queued:0'
        assert h.sql(f"SELECT status FROM provisiond.operations WHERE id='{active}'")=='failed'
    h.results.append(dict(test='queued-claim-lock-loss',result='pass',arms='entry mutex and pool waits refused before BEGIN; already-dispatched claim retains stop ownership'))


def displaced_record(h,defect=None):
    # OPS-21: A remains pending after B replaces its operation-id worker entry.
    for mode in ('expiry','lock_loss'):
        empty_queue(h);op=enqueue(h,'displaced-record-'+mode,'displaced-record-'+mode)
        p=core_probe(h,defect,boundary='lock_registration');startup=p.command('engine_start')['value'];a=p.command('claim')['value']
        h.sql(f"UPDATE provisiond.operations SET claim_number=claim_number+1,status='queued' WHERE id='{op}'")
        gate=h.proxy.arm('store COMMIT','commit_reply');started=time.monotonic()
        p.command('record_stop '+op);gate.wait()
        original=p.command('record_state '+op)['value']
        assert original['result'] is None
        b=p.command('claim')['value'];assert b['id']==op and b['number']>a['number']
        assert p.command('owners')['value']==dict(workers=1,available_slots=1)
        p.command('wait_start');eventually(lambda:any(e.get('event')=='waiter_polled' for e in p.events))
        if mode=='lock_loss':
            h.sql(f"SELECT pg_terminate_backend({startup['lock_pid']})")
            p.command('observe_pair');held(p,'lock_registration');p.command('boundary_release')
            eventually(lambda:any(e.get('event')=='lock_registration_complete' for e in p.events))
            eventually(lambda:any(e.get('event')=='stop_record_finished' and e.get('operation')==op for e in p.events))
        observer_pending=p.command('wait_pending')['value']['pending']
        status=p.p.wait(timeout=max(.01,1.15-(time.monotonic()-started)))
        elapsed=time.monotonic()-started;p.expected_status=71;p.finish();gate.release()
        reasons=[e['reason'] for e in p.events if e.get('event')=='engine_fatal' and 'records not observed landed' in e.get('reason','')]
        h.event('displaced_record_expiry_observed',mode=mode,operation=op,original_claim=a['number'],replacement_claim=b['number'],original_record=original,exit_status=status,elapsed_seconds=elapsed,unresolved_logs=reasons,later_observer_pending=observer_pending,reply_withheld_through_exit=True)
        assert reasons and op in reasons[-1], 'displaced pending record expiry must name its operation'
        assert f"claim={a['number']}" in reasons[-1], 'expiry must identify the unobserved claim'
        assert observer_pending, 'later observer must retain displaced pending record'
        assert status==71 and elapsed<1.15
        assert not any(e.get('event')=='stop_record_finished' and e.get('affected') is False for e in p.events), 'withheld stale record must not invent acknowledgment'
        assert not any(e.get('event')=='whole_transaction_retry' for e in p.events)
        assert h.sql(f"SELECT status||':'||claim_number FROM provisiond.operations WHERE id='{op}'")==f"{'failed' if mode=='lock_loss' else 'running'}:{b['number']}"
    h.results.append(dict(test='displaced-record-expiry',result='pass',arms='real withheld COMMIT, replacement worker, later observer and lock loss, original deadline/claim identity'))


def terminal_capacity(h,defect=None):
    # OPS-21/STO-55: pending COMMIT retains capacity; terminal records release it.
    empty_queue(h);ids=[enqueue(h,'terminal-capacity-'+str(i),'terminal-capacity-'+str(i)) for i in range(4)]
    p=core_probe(h,defect);p.command('engine_start')
    for _ in ids:
        c=p.command('claim')['value'];assert c['id'] in ids
        gate=h.proxy.arm('store COMMIT','commit_reply');p.command('record_stop '+c['id']);gate.wait()
        held_owners=p.command('owners')['value'];assert held_owners==dict(workers=1,available_slots=2)
        assert p.command('record_state '+c['id'])['value']['result'] is None
        gate.release();p.command('wait_records')
        h.proxy.gates.remove(gate)
        eventually(lambda:any(e.get('event')=='stop_record_finished' and e.get('operation')==c['id'] for e in p.events))
        owners=p.command('owners')['value']
        h.event('terminal_capacity_observed',operation=c['id'],pending_owners=held_owners,completed_owners=owners,acknowledged=True)
        if owners!=dict(workers=0,available_slots=3):p.finish()
        assert owners==dict(workers=0,available_slots=3), 'completed stop must retire worker capacity'
        finished=next(e for e in p.events if e.get('event')=='stop_record_finished' and e.get('operation')==c['id'])
        assert finished['landed'] is True and finished['affected'] is True
    p.finish();h.results.append(dict(test='terminal-stop-capacity-reuse',result='pass',arms='four claims using three slots; original capacity held until acknowledgment'))


def loss_stop(h,defect=None):
    # OPS-21/47: user stops after observed loss are later even before fan-out.
    for window in ('unregistered','sent_record'):
        empty_queue(h);op=enqueue(h,'loss-user-'+window,'loss-user-'+window)
        p=core_probe(h,defect,boundary='lock_registration');startup=p.command('engine_start')['value'];p.command('claim')
        gate=h.proxy.arm('store COMMIT','commit_reply')
        original=None
        if window=='sent_record':
            p.command('record_stop '+op);gate.wait();original=p.command('record_state '+op)['value']
        generation=h.sql('SELECT generation FROM provisiond.startup_generation')
        h.sql(f"SELECT pg_terminate_backend({startup['lock_pid']})")
        p.command('observe_pair');held(p,'lock_registration')
        assert any(e.get('event')=='startup_lock_lost' for e in p.events)
        assert h.sql('SELECT generation FROM provisiond.startup_generation')==generation
        p.command('new_work',ok=False)
        p.command('claim',ok=False)
        if original:assert p.command('record_state '+op)['value']==original
        marker=len(h.events);started=time.monotonic()
        r=p.command('record_stop '+op,ok=None)
        later=not r['ok'] and 'later stop' in r.get('error','')
        if later:
            p.expected_status=71;p.finish()
        else:
            # Fault observed: the user stop was absorbed. Let the faulty owner
            # finish before asserting, so teardown is not the negative signal.
            if window=='unregistered':gate.wait()
            gate.release();p.command('boundary_release');p.command('observe_join',ok=False);p.expected_status=71;p.finish()
        gate.release()
        sends=[e for e in h.events[marker:] if e.get('event')=='send_requested' and 'store COMMIT' in e.get('query','')]
        starts=[e for e in p.events if e.get('event')=='stop_record_started']
        state=h.sql(f"SELECT status FROM provisiond.operations WHERE id='{op}'")
        h.event('user_stop_after_loss_observed',window=window,later_stop=later,exit_status=p.p.returncode,elapsed_seconds=time.monotonic()-started,stop_records_started=len(starts),record_commits_after_user_stop=len(sends),durable_status=state,original_record=original,generation_unchanged=h.sql('SELECT generation FROM provisiond.startup_generation')==generation)
        assert later, 'user stop after detected loss must take later-stop exit'
        assert time.monotonic()-started<.4, 'later stop must not wait for lock-loss fanout or a record'
        assert p.p.returncode==71 and len(starts)==int(window=='sent_record') and not sends
        assert state in ('running','failed')  # A sent transaction's fate is the server's.
        assert not any(e.get('event')=='whole_transaction_retry' for e in p.events)
    h.results.append(dict(test='user-stop-after-lock-loss',result='pass',arms='detection before fanout; earlier sent record; prompt nonzero later-stop exit without repeat'))


def claim_cancel(h,defect=None):
    for window in ('commit','claim_registration'):
        empty_queue(h)
        ids=[enqueue(h,f'cancel-{window}-{n}',f'cancel-{window}-{n}') for n in range(4)]
        p=core_probe(h,defect,boundary=window if window!='commit' else None)
        p.command('engine_start')
        gate=h.proxy.arm('store COMMIT','commit_reply') if window=='commit' else None
        p.command('claim_start')
        if gate: gate.wait()
        else: held(p,window)
        running=h.sql("SELECT id FROM provisiond.operations WHERE status='running'")
        assert running in ids
        p.command('claim_cancel')
        # The caller alone disappears; the owning process survives.
        assert p.p.poll() is None
        retained_slots=p.command('owners')['value']['available_slots']
        if gate: gate.release()
        else: p.command('boundary_release')
        time.sleep(.08)
        p.command('record_stop '+running,ok=None)
        r=p.command('wait_records',ok=None)
        owned=h.sql(f"SELECT status FROM provisiond.operations WHERE id='{running}'")=='failed'
        h.event('claim_cancellation_observed',window=window,operation=running,registered_owner=owned,available_slots_while_held=retained_slots,process_survived=p.p.poll() is None)
        assert owned and r['ok'] and retained_slots==2, 'cancelled claim must retain a registered worker owner'
        owners=p.command('owners')['value']
        assert owners['workers']==0 and owners['available_slots']==3, 'completed stop must retire worker capacity'
        assert not accepted(p,running)
        p.finish()
    h.results.append(dict(test='claim-caller-cancellation',result='pass',arms='sent COMMIT reply and completion/registration window'))


def licensing_stop(h,defect=None,only=None):
    for window in ([only] if only else ['reply','sql','retry','licensed_entry']):
        empty_queue(h);op=enqueue(h,'license-'+window,'license-'+window,resource=True,offer='license-'+window)
        p=core_probe(h,defect,boundary='licensing_retry' if window=='retry' else (window if window=='licensed_entry' else None))
        p.command('engine_start');p.command('claim')
        holder=None
        if window in ('sql','retry'):
            holder=tenant_holder(h,['license-'+window],'license-'+window)
            # Licensing itself locks the operation row, not the tenant primitive.
            holder.stdin.write(f"SELECT id FROM provisiond.operations WHERE id='{op}' FOR UPDATE;\n");holder.stdin.flush()
            eventually(lambda:h.sql("SELECT count(*) FROM pg_stat_activity WHERE usename='administrator' AND state='idle in transaction' AND query LIKE '%FOR UPDATE%'")=='1')
        gate=h.proxy.arm('store COMMIT','commit_reply') if window=='reply' else None
        p.command('execute_start '+op)
        if gate:
            connection=gate.wait()
            assert h.sql(f"SELECT about_to_order_at IS NOT NULL FROM provisiond.operations WHERE id='{op}'")=='t'
        elif window=='licensed_entry': held(p,window)
        elif window=='retry': held(p,'licensing_retry')
        else: eventually(lambda:h.sql("SELECT count(*) FROM pg_stat_activity WHERE usename='app' AND wait_event_type='Lock' AND query LIKE '%about_to_order_at%'")=='1')
        p.command('record_stop '+op)
        marker=len(p.events)
        wire_marker=len(h.events)
        if holder: holder.stdin.close();assert holder.wait(timeout=5)==0
        if gate:
            # The original sent checkout remains owned while stopped.
            assert not any(e.get('event')=='commit_owner_completed' and e.get('session')==connection['pid'] for e in p.events[marker:])
            gate.release()
        if window in ('licensed_entry','retry'):p.command('boundary_release')
        p.command('execute_join',ok=None)
        time.sleep(.04)
        h.event('licensing_stop_observed',window=window,operation=op,external_entries=len(accepted(p,op)))
        if accepted(p,op):
            # The forbidden entry is already independently observed. End this
            # owned faulty process with an explicit later stop before asserting,
            # retaining any sent transaction through that whole-process boundary.
            p.command('later_stop',ok=False);p.expected_status=71;p.finish()
        assert not accepted(p,op), 'stopped licensing must not enter provider execution'
        p.command('wait_records')
        assert not any(e.get('event')=='send_requested' and e.get('query','').startswith('UPDATE provisiond.operations SET about_to_order_at=') for e in h.events[wire_marker:]), 'stopped licensing must not repeat'
        state=h.sql(f"SELECT status||':'||(error->'details'->>'reason')||':'||(error->>'retryable') FROM provisiond.operations WHERE id='{op}'")
        assert state=='failed:cut_off:true', 'live pre-dispatch create must fail despite licensing marker'
        assert h.sql(f"SELECT reserved_sats||':'||state FROM provisiond.commitments WHERE operation_id='{op}'")=='0:closed'
        assert h.sql(f"SELECT count(*) FROM provisiond.resource_exclusions WHERE operation_id='{op}'")=='0'
        assert h.sql(f"SELECT count(*) FROM provisiond.ledger_entries WHERE operation_id='{op}'")=='0'
        p.finish()
    h.results.append(dict(test='licensing-stop',result='pass',arms='SQL, retry, sent COMMIT, licensed before dispatch; full release without debit'))


def outcome_stop(h,defect=None):
    empty_queue(h);op=enqueue(h,'outcome-window','outcome-window',resource=True,offer='outcome-window')
    p=core_probe(h,defect,boundary='outcome_publication');p.command('engine_start');p.command('claim')
    p.command('execute_return '+op);held(p,'outcome_publication')
    assert len(accepted(p,op))==1
    p.command('record_stop '+op);p.command('boundary_release');p.command('wait_records')
    state=h.sql(f"SELECT status||':'||COALESCE(error->'details'->>'reason','none') FROM provisiond.operations WHERE id='{op}'")
    h.event('outcome_stop_observed',operation=op,state=state,returned_before_stop=True,publication_after_stop=True)
    assert state=='succeeded:none', 'returned outcome must win over stale cancellation snapshot'
    assert h.sql(f"SELECT count(*)||':'||sum(-amount_sats) FROM provisiond.ledger_entries WHERE operation_id='{op}'")=='2:15'
    assert h.sql(f"SELECT reserved_sats||':'||version FROM provisiond.commitments WHERE operation_id='{op}'")=='45:2'
    assert h.sql(f"SELECT count(*) FROM provisiond.machines WHERE id=(SELECT machine_id FROM provisiond.operations WHERE id='{op}')")=='1'
    p.finish()
    h.results.append(dict(test='outcome-stop-publication',result='pass',arms='returned verified outcome, atomic machine/debits/commitment; cancellation counterpart in ENG-02'))


def replacement_outcome(h,defect=None):
    # STO-3/OPS-6/21/22: a returned result belongs to its captured execution.
    empty_queue(h);op=enqueue(h,'replacement-outcome','replacement-outcome',resource=True,offer='replacement-outcome')
    p=core_probe(h,defect,boundary='outcome_publication');p.command('engine_start')
    a=p.command('claim')['value'];assert a['id']==op
    external_a='stale-'+op;external_b='replacement-'+op
    p.command('execute_return '+op+' '+external_a);held(p,'outcome_publication')
    assert len(accepted(p,op))==1
    h.sql(f"UPDATE provisiond.operations SET claim_number=claim_number+1,status='queued' WHERE id='{op}'")
    p.command('record_stop '+op)
    b=p.command('claim')['value'];assert b['id']==op and b['number']>a['number']
    assert p.command('owners')['value']==dict(workers=1,available_slots=1)
    def snapshot():
        return h.sql(f"SELECT jsonb_build_object('operation',(SELECT to_jsonb(o) FROM provisiond.operations o WHERE id='{op}'),'commitment',(SELECT to_jsonb(c) FROM provisiond.commitments c WHERE operation_id='{op}'),'exclusion',(SELECT to_jsonb(e) FROM provisiond.resource_exclusions e WHERE operation_id='{op}'),'ledger',(SELECT jsonb_agg(to_jsonb(l) ORDER BY seq) FROM provisiond.ledger_entries l WHERE tenant_id='{b['tenant']}'),'machines',(SELECT jsonb_agg(to_jsonb(m) ORDER BY id) FROM provisiond.machines m))")
    before=snapshot()
    p.command('boundary_release');p.command('wait_records')
    eventually(lambda:any(e.get('event')=='stop_record_finished' and e.get('operation')==op for e in p.events))
    finished=next(e for e in p.events if e.get('event')=='stop_record_finished' and e.get('operation')==op)
    after_a=snapshot();owners=p.command('owners')['value']
    reply=p.command('start_settlement '+op,ok=None)
    if reply['ok']:p.command('wait_records')
    after_attempt=snapshot()
    h.event('replacement_outcome_publication_observed',operation=op,original_claim=a['number'],replacement_claim=b['number'],original_external_id=external_a,replacement_external_id=external_b,simulator_calls_before_release=len(accepted(p,op)),finished=finished,owners_after_a=owners,unchanged_after_a=after_a==before,replacement_settlement_reply=reply,unchanged_after_attempt=after_attempt==before)
    if reply['ok']:p.finish()
    assert not reply['ok'] and 'execute has not returned' in reply.get('error',''), 'replacement claim must refuse settlement without its own outcome'
    assert after_a==before and after_attempt==before
    stale=dict(a,external_id=external_a)
    refused=command(p,'outcome',stale,ok=False)
    assert 'worker claim replaced' in refused['error'] and snapshot()==before
    assert finished['affected'] is False and finished['landed'] is False
    assert owners==dict(workers=1,available_slots=2), 'stale outcome record must retain replacement worker and slot'
    captured=[e for e in p.events if e.get('event')=='stop_record_outcome' and e.get('operation')==op]
    assert len(captured)==1 and captured[0]['claim_number']==a['number'] and captured[0]['external_id']==external_a, 'stale record must consume its own verified outcome'
    zero=[e for e in p.events if e.get('event')=='worker_zero_row' and e.get('operation')==op]
    assert len(zero)==1 and zero[0]['claim_number']==a['number']
    p.command('execute_return '+op+' '+external_b)
    eventually(lambda:sum(e.get('event')=='execute_returned' and e.get('operation')==op for e in p.events)==2)
    calls=accepted(p,op)
    assert [e['claim_number'] for e in calls]==[a['number'],b['number']]
    p.command('start_settlement '+op);p.command('wait_records')
    assert h.sql(f"SELECT status||':'||claim_number||':'||(result->>'external_id') FROM provisiond.operations WHERE id='{op}'")==f"succeeded:{b['number']}:{external_b}"
    machine=h.sql(f"SELECT machine_id FROM provisiond.operations WHERE id='{op}'")
    assert h.sql(f"SELECT external_id FROM provisiond.machines WHERE id='{machine}'")==external_b
    assert h.sql(f"SELECT count(*) FROM provisiond.machines WHERE external_id='{external_a}'")=='0'
    assert h.sql(f"SELECT count(*)||':'||sum(-amount_sats)||':'||bool_and(machine_id='{machine}' AND commitment_id=(SELECT id FROM provisiond.commitments WHERE operation_id='{op}')) FROM provisiond.ledger_entries WHERE operation_id='{op}'")=='2:15:true'
    assert h.sql(f"SELECT count(*) FROM provisiond.ledger_entries WHERE tenant_id='{b['tenant']}' AND amount_sats>0")=='1'
    assert h.sql(f"SELECT reserved_sats||':'||version||':'||machine_id FROM provisiond.commitments WHERE operation_id='{op}'")==f'45:2:{machine}'
    assert h.sql(f"SELECT count(*) FROM provisiond.resource_exclusions WHERE operation_id='{op}'")=='0'
    settled=snapshot();b['external_id']=external_b
    assert command(p,'verified',b)['value']['affected']
    assert snapshot()==settled, 'replacement result must repeat all effects exactly once'
    assert p.command('owners')['value']==dict(workers=0,available_slots=3)
    assert len(accepted(p,op))==2
    h.event('replacement_own_outcome_observed',operation=op,original_record_outcome=captured[0],simulator_calls=calls,machine=machine,external_id=external_b,exact_repeat_unchanged=True)
    p.finish();h.results.append(dict(test='replacement-outcome-publication',result='pass',arms='held verified A return, independent advance and B claim, retained A zero-row record, B refuses absent outcome then executes distinct result exactly once'))


def deadline(h,defect=None):
    for mode in ('no_waiter','cancelled_waiter','settlement'):
        empty_queue(h);op=enqueue(h,'deadline-'+mode,'deadline-'+mode)
        p=core_probe(h,defect);p.command('engine_start');c=p.command('claim')['value']
        if mode=='settlement':
            c['external_id']='deadline-settlement';command(p,'outcome',c)
        gate=h.proxy.arm('store COMMIT','commit_reply')
        if mode=='settlement':p.command('start_settlement '+op);gate.wait()
        started=time.monotonic();p.command('record_stop '+op)
        if mode!='settlement':gate.wait()
        durable=h.sql(f"SELECT status FROM provisiond.operations WHERE id='{op}'")
        assert durable in ('failed','succeeded')
        if mode=='cancelled_waiter':
            p.command('wait_start');eventually(lambda:any(e.get('event')=='waiter_polled' for e in p.events));p.command('wait_cancel')
        try: status=p.p.wait(timeout=max(.01,1.15-(time.monotonic()-started)))
        except subprocess.TimeoutExpired:status=None
        elapsed=time.monotonic()-started
        h.event('record_deadline_observed',mode=mode,bound_ms=1000,tolerance_ms=150,elapsed_seconds=elapsed,exit_status=status,durable_status=durable,reply_withheld=True)
        if status is None:
            # The faulty process survived the original deadline. Release only
            # after observing that failure, then drain its late record so cleanup
            # cannot turn an intended red assertion into a setup/teardown error.
            gate.release()
            eventually(lambda:any(e.get('event') in ('stop_record_finished','settlement_finished') for e in p.events))
            h.event('late_record_completion_observed',mode=mode,process_survived=True,original_deadline_exceeded=True)
            p.finish()
        assert status==71, 'record deadline must exit without a polled waiter'
        p.expected_status=71;p.finish();gate.release()
        assert any(e.get('event')=='engine_fatal' and 'records not observed landed' in e.get('reason','') for e in p.events)
        assert not any(e.get('event')=='whole_transaction_retry' for e in p.events)
        assert h.sql(f"SELECT status FROM provisiond.operations WHERE id='{op}'")==durable
    h.results.append(dict(test='autonomous-stop-deadline',result='pass',arms='unpolled/cancelled waiter and existing settlement; actual nonzero exit; late reply cannot acknowledge'))


def lock_observers(h,defect=None):
    empty_queue(h);ids=[enqueue(h,'lock-pair-'+str(i),'lock-pair-'+str(i)) for i in range(3)]
    p=core_probe(h,defect,boundary='lock_registration');startup=p.command('engine_start')['value']
    claims=[p.command('claim')['value'] for _ in ids]
    p.command('execute_pending '+claims[0]['id']);eventually(lambda:accepted(p,claims[0]['id']))
    p.command('execute_return '+claims[1]['id']);eventually(lambda:any(e.get('event')=='execute_returned' for e in p.events))
    holder=tenant_holder(h,[claims[2]['tenant']],'lock-pair')
    p.command('record_stop '+claims[2]['id'])
    original=p.command('record_state '+claims[2]['id'])['value']
    assert original['deadline'] is not None and original['result'] is None
    generation=h.sql('SELECT generation FROM provisiond.startup_generation')
    h.sql(f"SELECT pg_terminate_backend({startup['lock_pid']})")
    eventually(lambda:any(e.get('event')=='driver_dead' for e in p.events))
    p.command('observe_pair');held(p,'lock_registration')
    after=p.command('record_state '+claims[2]['id'])['value']
    assert after==original, 'lock loss must retain original record identity and absolute deadline'
    p.command('new_work',ok=False)
    p.command('claim',ok=False)
    assert not any(e.get('event')=='claim_registered' for e in p.events[p.events.index(next(e for e in p.events if e.get('event')=='startup_lock_lost')):])
    p.command('boundary_release')
    # The earlier tenant holder stays locked until fan-out actually ran. The
    # second observer is serialized behind the first until it finishes/fails.
    eventually(lambda:any(e.get('event') in ('lock_registration_complete','engine_fatal') for e in p.events))
    if any(e.get('event')=='lock_registration_complete' for e in p.events):
        after_registration=p.command('record_state '+claims[2]['id'])['value']
    elif defect=='lock_observers':
        after_registration=after  # Only this explicit control exits before registration.
    else:after_registration=None
    holder.stdin.close();assert holder.wait(timeout=5)==0
    p.command('observe_join',ok=False);p.expected_status=71;p.finish()
    states=h.sql("SELECT string_agg(status,',' ORDER BY id) FROM provisiond.operations WHERE id IN ("+','.join("'"+i+"'" for i in ids)+")")
    starts=[e for e in p.events if e.get('event')=='stop_record_started']
    h.event('concurrent_lock_observers_observed',states=states,starts=len(starts),registration_windows=sum(e.get('event')=='execution_boundary_held' for e in p.events),original_record=original,after_detection=after,after_registration=after_registration,post_registration_read=any(e.get('event')=='lock_registration_complete' for e in p.events),new_work_refused=True,claim_refused=True)
    assert after_registration is not None, 'lock observer must complete registration before comparison'
    assert after_registration==original, 'lock-loss fanout must retain original record identity and absolute deadline'
    assert all(h.sql(f"SELECT status FROM provisiond.operations WHERE id='{c['id']}'")!='running' for c in claims), 'one lock loss must land records before exit'
    assert len(starts)==3 and len(set(e['operation'] for e in starts))==3
    assert h.sql('SELECT generation FROM provisiond.startup_generation')==generation
    assert h.sql(f"SELECT status FROM provisiond.operations WHERE id='{claims[1]['id']}'")=='succeeded'
    h.results.append(dict(test='concurrent-lock-loss-observers',result='pass',arms='single fanout, older bound retained, verified outcome, healthy store records before nonzero exit'))


def surface(h,defect=None):
    p=core_probe(h,defect)
    for method in ('claim','defer','settle','license','verified','reacquire'):
        r=p.command('unfenced '+method)['value']['refused']
        h.event('unfenced_engine_method_observed',method=method,refused=r)
        assert r, 'unfenced transaction must refuse engine-only business methods'
    p.finish();h.results.append(dict(test='engine-only-public-boundary',result='pass'))


def predispatch(h,defect=None):
    # Same durable licensing marker, three different ownership/position meanings.
    for mode in ('live','dispatched','restart'):
        empty_queue(h);op=enqueue(h,'position-'+mode,'position-'+mode,resource=True,offer='position-'+mode)
        p=core_probe(h,defect);p.command('engine_start');c=p.command('claim')['value'];assert c['id']==op
        command(p,'license',c)
        assert h.sql(f"SELECT about_to_order_at IS NOT NULL FROM provisiond.operations WHERE id='{op}'")=='t'
        if mode=='dispatched':p.command('execute_pending '+op);eventually(lambda:accepted(p,op))
        if mode=='restart':
            p.finish();p=core_probe(h);p.command('engine_start')
        else:
            p.command('record_stop '+op);p.command('wait_records')
        state=h.sql(f"SELECT status FROM provisiond.operations WHERE id='{op}'")
        h.event('live_create_position_observed',mode=mode,state=state,external_entries=len(accepted(p,op)),about_to_order_set=True)
        assert state==('failed' if mode=='live' else 'needs_reconciliation'), 'live pre-dispatch create must fail despite licensing marker'
        assert h.sql(f"SELECT count(*) FROM provisiond.ledger_entries WHERE operation_id='{op}'")=='0'
        assert h.sql(f"SELECT reserved_sats||':'||state FROM provisiond.commitments WHERE operation_id='{op}'")==('0:closed' if mode=='live' else '60:open')
        assert h.sql(f"SELECT count(*) FROM provisiond.resource_exclusions WHERE operation_id='{op}'")==('0' if mode=='live' else '1')
        p.finish()
    h.results.append(dict(test='live-create-position-versus-restart',result='pass',arms='licensed zero-dispatch fails/releases; dispatched cutoff and ordinary restart retain ambiguity'))


def startup(h,defect=None):
    empty_queue(h)
    for mode in ('newer','older','semantics','uninitialized','missing_history','missing_schema'):
        if mode in ('newer','older','semantics'):
            version,semantics={'newer':(2,1),'older':(0,1),'semantics':(1,2)}[mode]
            h.sql(f'UPDATE provisiond.schema_migrations SET version={version},semantics={semantics}')
        elif mode=='uninitialized':h.sql('DELETE FROM provisiond.schema_migrations')
        elif mode=='missing_history':h.sql('ALTER TABLE provisiond.schema_migrations RENAME TO saved_migrations')
        else:h.sql('ALTER SCHEMA provisiond RENAME TO missing_schema_fixture')
        schema='missing_schema_fixture' if mode=='missing_schema' else 'provisiond'
        generation=h.sql(f'SELECT generation FROM {schema}.startup_generation')
        r=binary(h,'engine',ok=False,defect=defect)
        protected=any(e in r.stdout for e in ('startup_lock_acquired','generation_advanced','claim_registered','simulator_accepted'))
        h.event('real_startup_schema_observed',mode=mode,exit_status=r.returncode,protected_entry=protected,check_only=False,error=r.stderr)
        assert not protected, 'unsupported real engine startup must precede protected entry'
        assert r.returncode==1 and ('unsupported schema/semantic' in r.stderr or 'missing/uninitialized schema' in r.stderr)
        assert h.sql(f'SELECT generation FROM {schema}.startup_generation')==generation
        if mode=='missing_schema':h.sql('ALTER SCHEMA missing_schema_fixture RENAME TO provisiond')
        elif mode=='missing_history':h.sql('ALTER TABLE provisiond.saved_migrations RENAME TO schema_migrations')
        elif mode=='uninitialized':h.sql('INSERT INTO provisiond.schema_migrations(version,semantics) VALUES(1,1)')
        else:h.sql('UPDATE provisiond.schema_migrations SET version=1,semantics=1')
    # Ordinary supported startup, no --check-schema: terminate only its lock session.
    log=h.root/'supported-engine-startup.log'
    with log.open('w') as output:
        p=subprocess.Popen(['target/debug/engine'],env=dict(h.env,**environment(h)),stdout=output,stderr=output,text=True)
    h.extra.append(p)
    eventually(lambda:'restart_pass_complete' in log.read_text())
    events=[json.loads(line) for line in log.read_text().splitlines() if line.startswith('{')]
    lock=next(e['session'] for e in events if e['event']=='startup_lock_acquired')
    observed=next(e for e in events if e['event']=='startup_lock_keepalives_observed')
    assert observed['session']==lock and observed['server']=='2:1:2' and observed['client_getters_verified']
    h.event('actual_lock_keepalives_observed',session=lock,idle_seconds=2,interval_seconds=1,count=2,server=observed['server'],client_getters_verified=True)
    h.sql(f'SELECT pg_terminate_backend({lock})')
    assert p.wait(timeout=3)==1
    h.event('supported_real_startup_observed',exit_after_lock_loss=1,features='store-test-hooks',check_only=False)
    h.results.append(dict(test='REL-02-real-engine-startup',result='pass',arms='missing schema/history, uninitialized, older/newer/incompatible semantics before lock; supported advance and lock keepalives'))
