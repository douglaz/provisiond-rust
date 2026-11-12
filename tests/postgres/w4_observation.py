"""WIR-11/STO-48: the confirming read dates state; LDG-38 dates the seed separately."""
from datetime import datetime
import json
import time

from harness import eventually
from w4_regressions import funded


def observation(w):
    from core import core_probe
    h=w.h;w.stop_engine()
    # Each arm uses actual HTTP funding/admission and the independent driver.
    for mode in ['reply_loss','constraint_timeout','stop_after_read']:
        t=funded(w,'observation-'+mode);account=t['account']
        # PRV-13b: retain the old effective read allowance explicitly so the
        # observation/stop hold, rather than an early timeout, remains the fault.
        allowance=3 if mode=='constraint_timeout' else 5
        descriptor=dict(w.descriptor,account=account,call_seconds=allowance,read_seconds=allowance)
        h.sql("UPDATE provisiond.provider_accounts SET descriptor='%s' WHERE account='%s'"%(json.dumps(descriptor),account))
        op=w.request('POST','/v1/machines',w.create_body(t),t,'observation-'+mode,status=202)['id']
        hold=h.root/'provider/hold-constraints';hold.touch()
        statement=h.proxy.arm('SET status=$3,result=','send') if mode=='reply_loss' else None
        probe=None
        try:
            if mode=='stop_after_read':
                h.run(['cargo','build','--example','core-probe','--features','store-test-hooks','--locked'],timeout=120)
                executable=h.target/'debug/examples/core-probe'
                h.event('workflow_binary_started',component='core-probe',case=w.case,**w.build_identity(executable,['store-test-hooks']))
                probe=core_probe(h);probe.command('engine_start');probe.command('dispatch_provider')
            else:w.start_engine()
            def calls():return [e for e in w.external()['events'] if e.get('command')==op]
            # A held constraint request for THIS operation proves the successful
            # authoritative /get has returned to the engine, before settlement.
            eventually(lambda:any(e['path']=='/constraints' and e['stage']=='request_held' for e in calls()))
            events=calls()
            read=next(e for e in events if e['path']=='/get' and e['stage']=='request_received')
            constraint=next(e for e in events if e['path']=='/constraints' and e['stage']=='request_received')
            assert any(e['path']=='/get' and e['stage']=='read_response_returned' for e in events)
            assert read['sequence']<constraint['sequence'] and w.status(op)=='running'
            barrier=time.time()
            h.event('provider_observation_delay_activated',operation=op,case=mode,
                    get_received_at=read['timestamp'],constraint_received_at=constraint['timestamp'],
                    get_sequence=read['sequence'],constraint_sequence=constraint['sequence'],
                    constraint_held=True,settlement_started=False,call_seconds=descriptor['call_seconds'],read_seconds=descriptor['read_seconds'])
            if mode=='constraint_timeout':
                w.wait_status(op,'succeeded')  # the bounded failed read leaves success/null
            else:
                # Time alone is not activation: the exact request hold above is.
                # Keep it active until public second precision distinguishes it.
                eventually(lambda:time.time()>=barrier+2.1,seconds=4)
                assert w.status(op)=='running'
                assert not any(e['path']=='/constraints' and e['stage']=='read_response_returned' for e in calls())
                if probe:
                    probe.command('record_stop '+op);probe.command('wait_records')
                else:hold.unlink()
            if statement:
                connection=statement.wait()
                reply=h.proxy.arm('store COMMIT','commit_reply',session=connection['pid'])
                statement.release();reply.wait()
                assert w.status(op)=='succeeded'
                before=snapshot(w,op);before_calls=calls()
                retry_count=len(w.events('engine','whole_transaction_retry'))
                h.proxy.close_connection(connection,'lost_observation_settlement_reply');reply.release()
                eventually(lambda:any(e.get('operation')==op for e in w.events('engine','settlement_finished')))
                assert len(w.events('engine','whole_transaction_retry'))>retry_count
                assert before==snapshot(w,op) and before_calls==calls(),'settlement repeat must retain observation, seed and all effects'
                h.event('provider_observation_repeat_observed',operation=op,whole_transaction_retry=True,
                        state_seed_money_revision_unchanged=True,provider_calls_repeated=False)
            w.wait_status(op,'succeeded')
            state=snapshot(w,op)
            machine=w.request('GET','/v1/machines/'+state['machine'],tenant=t)
            public=datetime.fromisoformat(machine['state_observed_at']).timestamp()
            h.event('provider_observation_settled',operation=op,case=mode,
                    get_received_at=read['timestamp'],constraint_received_at=constraint['timestamp'],
                    public_observed_at=machine['state_observed_at'],state=state)
            # This assertion must fail on the unfixed runtime, before seed checks.
            assert read['timestamp']<=state['observed_epoch']<=constraint['timestamp'] and int(read['timestamp'])<=public<=int(constraint['timestamp']), 'WIR-11 state observation must date the confirming provider read before delayed constraints'
            assert public==int(state['observed_epoch']) and machine['state']=='running'
            assert state['created_epoch']-constraint['timestamp']>=1
            assert state['paid'] and state['seconds']==3600 and state['seed_from_creation'] and state['high_water_matches_seed']
            assert state['ledger_rows']==2 and state['debits']==31 and state['remaining']==12 and state['machine_count']==1
            assert (state['constraint_date'] is None)==(mode!='reply_loss')
            requests=[e['path'] for e in calls() if e['stage']=='request_received']
            assert requests==['/offers','/order','/search','/get','/constraints'],requests
            assert w.request('POST','/v1/machines',w.create_body(t),t,'observation-'+mode,status=202)['id']==op
            assert state==snapshot(w,op)
            h.results.append(dict(test='W4-provider-observation-'+mode,result='pass'))
        finally:
            if statement:statement.release()
            if hold.exists():hold.unlink()
            if probe:probe.finish()
            w.stop_engine()
            h.sql("UPDATE provisiond.provider_accounts SET descriptor='%s' WHERE account='%s'"%(json.dumps(dict(w.descriptor,account=account)),account))


def snapshot(w,op):
    return json.loads(w.h.sql(f"""SELECT jsonb_build_object(
        'machine',m.id,'observed_epoch',extract(epoch FROM m.state_observed_at),
        'created_epoch',extract(epoch FROM m.created_at),'revision',o.revision,
        'seed',m.meter_seed_at,'seed_from_creation',m.meter_seed_at=m.created_at+interval '3600 seconds',
        'paid',m.first_unit_debit_id IS NOT NULL,'seconds',m.billing_unit_seconds,
        'high_water_matches_seed',(SELECT high_water_mark=m.meter_seed_at FROM provisiond.meter_totals WHERE subject_kind='machine' AND subject_id=m.id),
        'constraint_date',m.earliest_cancellation_date,'remaining',c.reserved_sats,
        'ledger_rows',(SELECT count(*) FROM provisiond.ledger_entries WHERE operation_id=o.id),
        'debits',(SELECT sum(-amount_sats) FROM provisiond.ledger_entries WHERE operation_id=o.id),
        'machine_count',(SELECT count(*) FROM provisiond.machines WHERE provider_account=m.provider_account AND external_id=m.external_id))
        FROM provisiond.operations o JOIN provisiond.machines m ON m.id=o.machine_id
        JOIN provisiond.commitments c ON c.id=o.commitment_id WHERE o.id='{op}'"""))
