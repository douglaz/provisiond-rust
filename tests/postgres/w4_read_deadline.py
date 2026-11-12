"""PRV-13b/5/36, OVR-19, OPS-11/12/27: priced sequential reply waits."""
import json

from core import environment
from harness import eventually
from w4_publication import state, operation_events
from w4_regressions import funded


READ_SECONDS=1
CALL_SECONDS=5
DELAY_SECONDS=3
TOLERANCE_SECONDS=.4
ASSERTION='PRV-13b confirming read wait must not exceed read_seconds plus scheduling tolerance'


def delay_result(w,op,path,since=0):
    def finished():
        return next((e for e in operation_events(w,op)
                     if e['sequence']>since and e['path']==path and e['stage']=='deadline_delay_finished'),None)
    return eventually(finished,seconds=8)


def assert_read_bound(w,op,path,descriptor,since=0):
    measured=delay_result(w,op,path,since)
    records=[e for e in operation_events(w,op) if e['sequence']>since]
    starts=[e for e in records if e['path']==path and e['stage']=='deadline_delay_started']
    assert len(starts)==1 and starts[0]['seconds']==DELAY_SECONDS
    assert len([e for e in records if e['path']==path and e['stage']=='request_received'])==1
    wait=measured['wait_seconds']
    received=next(e for e in records if e['path']==path and e['stage']=='request_received')
    closed=next((e for e in records if e['path']==path and e['stage']=='deadline_peer_closed'),None)
    receipt_wait=((closed['monotonic_ns'] if closed else measured['monotonic_ns'])-received['monotonic_ns'])/1e9
    w.h.event('provider_read_deadline_observed',operation=op,path=path,
              read_seconds=descriptor['read_seconds'],call_seconds=descriptor['call_seconds'],
              intended_delay_seconds=DELAY_SECONDS,tolerance_seconds=TOLERANCE_SECONDS,
              monotonic_wait_seconds=wait,receipt_to_wait_end_seconds=receipt_wait,measurement=measured['measurement'],
              peer_closed=measured['peer_closed'],delay_finished=True,
              readiness_before_dispatch=True,records=records)
    # Mandatory original-runtime red precedes all differing outcome assertions.
    assert wait<=descriptor['read_seconds']+TOLERANCE_SECONDS,ASSERTION
    assert receipt_wait<=descriptor['read_seconds']+TOLERANCE_SECONDS,ASSERTION
    assert wait>=descriptor['read_seconds']-TOLERANCE_SECONDS and measured['peer_closed']
    assert len([e for e in records if e['path']==path and e['stage']=='deadline_peer_closed'])==1
    return records


def descriptor(w,account):
    d=dict(w.descriptor,account=account,call_seconds=CALL_SECONDS,read_seconds=READ_SECONDS)
    w.h.sql("UPDATE provisiond.provider_accounts SET descriptor='%s' WHERE account='%s'"%(json.dumps(d),account))
    w.restart_api()  # immutable catalog: admission must freeze these exact terms
    return d


def read_deadline(w):
    h=w.h;w.stop_engine();w.provider_control(mode='normal')
    resolution_hold=w.control/'hold-reconciliation-record';resolution_hold.touch()
    originals={a:json.loads(h.sql(f"SELECT descriptor FROM provisiond.provider_accounts WHERE account='{a}'"))
               for a in ('sim-a','sim-b')}
    try:
        # Get is first so baseline reaches the precise wait assertion after an
        # independently accepted create, before checking settlement differences.
        for path in ('/get','/search','/constraints','/offers'):
            t=funded(w,'read-deadline-'+path[1:]);d=descriptor(w,t['account'])
            op=w.request('POST','/v1/machines',w.create_body(t),t,'read-deadline-'+path[1:],status=202)['id']
            # Admission and fault setup precede dispatch. The measurement starts
            # at actual provider receipt, after the engine's restart readiness.
            assert w.status(op)=='queued','read-deadline fixture must arm before dispatch'
            admitted=state(w,op)
            commitment=h.sql(f"SELECT to_jsonb(c) FROM provisiond.commitments c WHERE operation_id='{op}'")
            assert admitted['reserved']==43
            terms=admitted['summary']['offer']['accepted_terms']['pricing_descriptor']
            declared={k:v for k,v in terms.items() if k!='effective_visibility_seconds'}
            assert declared==d and sum(declared[k] for k in ('meter_seconds','call_seconds','visibility_seconds','resolution_seconds','read_seconds','hold_seconds','margin_seconds'))==11
            effective=terms.get('effective_visibility_seconds',terms['visibility_seconds'])
            assert effective>=d['visibility_seconds']
            priced_wind=11+effective-d['visibility_seconds']
            w.provider_control(deadline_delay=dict(operation=op,path=path,seconds=DELAY_SECONDS))
            p=w.start_engine()
            h.event('provider_deadline_fixture_ready',operation=op,path=path,engine_ready=p.poll() is None,
                    mutation_seconds=CALL_SECONDS,read_seconds=READ_SECONDS,priced_wind_seconds=priced_wind,immutable_declaration=d,effective_visibility_seconds=effective)
            records=assert_read_bound(w,op,path,d)
            expected='succeeded' if path=='/constraints' else 'failed' if path=='/offers' else 'needs_reconciliation'
            w.wait_status(op,expected);w.stop_engine();stored=state(w,op)
            orders=[e for e in records if e['path']=='/order' and e['stage']=='accepted']
            assert len(orders)==(0 if path=='/offers' else 1)
            if path in ('/get','/search'):
                assert stored['error']['kind']=='timeout' and stored['error']['retryable'] is False
                details=stored['error']['details'];external=orders[0]['sequence']
                created=next(e['resource'] for e in records if e['stage']=='resource_created')
                assert created=='SIMULATOR-RESOURCE-'+str(external)
                identifiers={'order_reply':[created]}
                if path=='/get':identifiers['search']=[created]
                assert details==dict(accepted=True,attempted='create_machine',provider_identifiers=identifiers)
                view=w.operation_reads(t,op,'timeout',False,details)
                assert stored['summary']==dict(admitted['summary'],correlator={'kind':'operation_uuid','value':op})
                assert stored['request_purged'] and stored['result'] is None and stored['machine'] is None
                assert stored['reserved']==43 and stored['commitment_state']=='open' and stored['debits']==0
                assert h.sql(f"SELECT to_jsonb(c) FROM provisiond.commitments c WHERE operation_id='{op}'")==commitment
                assert h.sql(f"SELECT count(*) FROM provisiond.machines WHERE provider_account='{t['account']}' AND external_id='{created}'")=='0'
                count=h.sql('SELECT count(*) FROM provisiond.operations')
                effects=w.external()
                w.start_engine();assert state(w,op)==stored and w.operation_reads(t,op,'timeout',False)==view
                w.stop_engine()
                assert w.external()==effects and h.sql('SELECT count(*) FROM provisiond.operations')==count
            elif path=='/constraints':
                # PRV-13c: failed read after confirmation is absorbed, not retried.
                assert stored['error'] is None and stored['debits']==2 and stored['reserved']==12
                assert h.sql(f"SELECT earliest_cancellation_date IS NULL FROM provisiond.machines WHERE id='{stored['machine']}'")=='t'
                w.operation_reads(t,op)
            else:
                assert stored['error']['kind']=='timeout' and stored['error']['details']['accepted'] is None
                assert stored['error']['retryable'] is True and stored['reserved']==0 and stored['commitment_state']=='closed'
                assert stored['machine'] is None and stored['debits']==0
            h.event('provider_deadline_durable_outcome',operation=op,path=path,state=stored,orders=len(orders),second_mutation=False)
            h.results.append(dict(test='W4-read-deadline-'+path[1:],result='pass'))

        # A longer mutation wait remains separate from the priced read term.
        for name,delayed in (('long-mutation',True),('all-one-prompt',False)):
            w.stop_engine();t=funded(w,'read-deadline-'+name)
            d=descriptor(w,t['account']) if delayed else dict(w.descriptor,account=t['account'])
            h.sql("UPDATE provisiond.provider_accounts SET descriptor='%s' WHERE account='%s'"%(json.dumps(d),t['account']))
            if not delayed:w.restart_api()
            op=w.request('POST','/v1/machines',w.create_body(t),t,'read-deadline-'+name,status=202)['id']
            if delayed:w.provider_control(deadline_delay=dict(operation=op,path='/order',seconds=DELAY_SECONDS))
            w.start_engine();w.wait_status(op,'succeeded');w.stop_engine()
            records=operation_events(w,op)
            if delayed:
                measured=delay_result(w,op,'/order')
                received=next(e for e in records if e['path']=='/order' and e['stage']=='request_received')
                returned=next(e for e in records if e['path']=='/order' and e['stage']=='response_returned')
                elapsed=(returned['monotonic_ns']-received['monotonic_ns'])/1e9
                assert not measured['peer_closed'] and READ_SECONDS+TOLERANCE_SECONDS<elapsed<CALL_SECONDS-TOLERANCE_SECONDS
                h.event('provider_mutation_deadline_observed',operation=op,monotonic_elapsed_seconds=elapsed,
                        read_seconds=READ_SECONDS,call_seconds=CALL_SECONDS,intended_delay_seconds=DELAY_SECONDS,tolerance_seconds=TOLERANCE_SECONDS,records=records)
            assert [e['path'] for e in records if e['stage']=='request_received']==['/offers','/order','/search','/get','/constraints']
            stored=state(w,op)
            assert stored['error'] is None and stored['debits']==2 and stored['reserved']==12 and stored['machine']
            w.operation_reads(t,op)
            h.results.append(dict(test='W4-read-deadline-'+name,result='pass'))

        # This separate adapter /get bypasses ordinary request dispatch.
        stop_read(w)
    finally:
        w.stop_engine();resolution_hold.unlink();w.provider_control(deadline_delay=None)
        for account,d in originals.items():
            h.sql("UPDATE provisiond.provider_accounts SET descriptor='%s' WHERE account='%s'"%(json.dumps(d),account))
        w.restart_api()


def stop_read(w):
    h=w.h;d=descriptor(w,'sim-a')
    resource=dict(external_id='TEST-READ-DEADLINE-STOP',account='sim-a',channel='orders',correlator='test-fixture',name='stop-cost-fixture',region='test',state='running',public_ips=[],present=True)
    w.provider_control(fixture_resource=resource,evidence={'operation':'stop-cost-fixture','delete':'acceptance_applied'})
    since=w.external()['events'][-1]['sequence']
    h.run(['cargo','build','--example','provider-probe','--features','store-test-hooks','--locked'],timeout=120)
    executable=h.target/'debug/examples/provider-probe'
    h.event('workflow_binary_started',component='provider-probe',case=w.case,**w.build_identity(executable,['store-test-hooks']))
    before=h.sql("SELECT jsonb_build_object('operations',(SELECT count(*) FROM provisiond.operations),'machines',(SELECT count(*) FROM provisiond.machines),'commitments',(SELECT jsonb_agg(to_jsonb(c) ORDER BY id) FROM provisiond.commitments c),'ledger',(SELECT count(*) FROM provisiond.ledger_entries))")
    w.provider_control(deadline_delay=dict(operation='stop-cost-fixture',path='/get',seconds=DELAY_SECONDS))
    old=h.env;h.env=dict(old,**environment(h))
    try:r=h.run([str(executable),'sim-a',resource['external_id'],'--report'],timeout=12)
    finally:h.env=old
    records=assert_read_bound(w,'stop-cost-fixture','/get',d,since)
    value=next(json.loads(line)['value'] for line in r.stdout.splitlines() if line.startswith('{') and json.loads(line).get('event')=='stop_cost_result')
    assert value['confirmed'] is False and value['error']['kind']=='network' and value['error']['retryable'] is False
    assert value['error']['details']=={'accepted':True}
    assert [e['path'] for e in records if e['stage']=='request_received']==['/delete','/get']
    assert len([e for e in records if e['stage']=='resource_deleted' and e['resource']==resource['external_id']])==1
    assert resource['external_id'] not in w.external()['resources']  # independent simulator fact only
    after=h.sql("SELECT jsonb_build_object('operations',(SELECT count(*) FROM provisiond.operations),'machines',(SELECT count(*) FROM provisiond.machines),'commitments',(SELECT jsonb_agg(to_jsonb(c) ORDER BY id) FROM provisiond.commitments c),'ledger',(SELECT count(*) FROM provisiond.ledger_entries))")
    assert before==after
    h.event('provider_delete_read_deadline_observed',result=value,accepted_delete=True,absence_confirmed=False,production_delete_workflow='not run',database_unchanged=True)
    h.results.append(dict(test='W4-read-deadline-delete-confirmation',result='pass'))
