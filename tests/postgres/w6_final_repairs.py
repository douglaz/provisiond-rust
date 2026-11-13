"""F1/F2: opening-rate provenance and permanent operator replay decisions."""
from datetime import datetime
from fractions import Fraction
import json
import math
import uuid
from harness import eventually
from workflow import http
from w5 import external, events

RATE_ASSERTIONS={
    'w6_f1_null':'F1 LDG-66 clamp opened with no rate must retain null opening rate',
    'w6_f1_current':'F1 LDG-66 clamp must carry current opening rate independently of historical price',
}
REPLAY_ASSERTION='F2 API-11 identical expired operator replay stays gone after actual retirement'


def opening_rate(w,available):
    """LDG-31/38/66/72, STO-37: real exit, historical price, current evidence."""
    from w6 import create,deletion,operation_state,one_purchase
    t,created,machine=create(w,'opening-rate-'+str(available))
    saved=json.loads(w.h.sql("SELECT value FROM provisiond.rates WHERE currency='USD'"))
    current=dict(saved,rate_num=20)
    w.h.sql(f"UPDATE provisiond.meter_totals SET high_water_mark=clock_timestamp()-interval '2 hours' WHERE subject_id='{machine}'")
    w.h.sql("UPDATE provisiond.fixture_rate_history SET accepted_at=clock_timestamp()-interval '3 hours' WHERE currency='USD'")
    # The existing trigger retains this real fixture acceptance boundary. The old
    # interval remains at 10/1; only the small subsequent tail sees the new state.
    w.h.sql("UPDATE provisiond.rates SET available=%s,value='%s' WHERE currency='USD'"%('true' if available else 'false',json.dumps(current)))
    boundary=w.h.sql("SELECT max(accepted_at) FROM provisiond.fixture_rate_history WHERE currency='USD'")
    op=deletion(w,t,machine,'opening-rate-delete-'+str(available))
    before=operation_state(w,op)
    enumeration=w.h.root/'provider/hold-enumerate';enumeration.touch()
    statement=w.h.proxy.arm("SET state='deleted',state_observed_at=",'send')
    w.start_engine();connection=statement.wait()
    gate=w.h.proxy.arm('store COMMIT','commit_reply',session=connection['pid']);statement.release()
    try:
        gate.wait();w.wait_status(op,'succeeded');state=operation_state(w,op)
        usage=[e for e in state['ledger'] if e['kind']=='usage_debit']
        clamps=[d for d in state['deficiencies'] if d['cause']=='clamp_overflow']
        assert len(usage)==len(clamps)==1 and -usage[0]['amount_sats']==12
        elapsed=datetime.fromisoformat(boundary)-datetime.fromisoformat(before['meter'][0]['high_water_mark'])
        micros=(elapsed.days*86400+elapsed.seconds)*1000000+elapsed.microseconds
        computed=math.ceil(Fraction(11*micros,3600*1000000))
        native=math.ceil(Fraction(micros,3600*1000000))
        assert computed==23 and clamps[0]['clamped_sats']==computed-12==11
        assert usage[0]['native_minor']==native*12//computed
        assert clamps[0]['native_minor']==native-usage[0]['native_minor']==2
        assert usage[0]['rate_num']==10 and usage[0]['rate_den']==1
        assert usage[0]['rate_source']==saved['source'] and usage[0]['haircut_bps']==saved['haircut_bps']
        assert datetime.fromisoformat(usage[0]['rate_observed_at'])==datetime.fromisoformat(saved['observed_at'])
        assert datetime.fromisoformat(usage[0]['increment_end'])==datetime.fromisoformat(boundary)
        assert clamps[0]['absorbed_seconds']==0 and clamps[0]['absorbed_from'] is None
        assert clamps[0]['currency']=='USD' and clamps[0]['subject_id']==machine
        assert clamps[0]['provider_account']==t['account']
        assert clamps[0]['idempotency_key']==usage[0]['idempotency_key']
        assert state['meter'][0]['high_water_mark']==state['machine']['meter_stopped_at']
        tail=datetime.fromisoformat(state['machine']['meter_stopped_at'])-datetime.fromisoformat(boundary)
        tail_micros=(tail.days*86400+tail.seconds)*1000000+tail.microseconds
        exact=Fraction(11*micros,3600*1000000)+(Fraction(22*tail_micros,3600*1000000) if available else 0)
        assert Fraction(int(state['meter'][0]['credit_num']),int(state['meter'][0]['credit_den']))==computed-exact
        condition=json.loads(w.h.sql("SELECT jsonb_build_object('available',available,'value',value) FROM provisiond.rates WHERE currency='USD'"))
        assert condition==dict(available=available,value=current)

        assert all(c['state']=='closed' and c['reserved_sats']==0 for c in state['commitments'])
        outages=[d for d in state['deficiencies'] if d['cause']=='rate_outage']
        assert len(outages)==(0 if available else 1)
        assert all(d['rate_num'] is None and d['rate_den'] is None and d['clamped_sats'] is None and d['absorbed_seconds']>0 for d in outages)
        rate_rows=json.loads(w.h.sql("SELECT jsonb_agg(to_jsonb(r) ORDER BY accepted_at,sequence) FROM provisiond.fixture_rate_history r WHERE currency='USD'"))
        w.h.event('w6_opening_rate',operation=op,created=created,machine=machine,
            current_available=available,current_rate=current,current_condition=condition,historical_rate=saved,
            boundary=boundary,elapsed_microseconds=micros,computed_sats=computed,native_minor=native,
            before=before,state=state,history=rate_rows,provider=external(w,op),
            assertion=RATE_ASSERTIONS[w.case],commit_reply_held=True)
        assert (clamps[0]['rate_num'],clamps[0]['rate_den'])==((20,1) if available else (None,None)),RATE_ASSERTIONS[w.case]
        # Return a rate before losing the already-landed settlement receipt. A
        # whole transaction repeat and ordinary successor must never backfill null.
        w.h.sql("UPDATE provisiond.rates SET available=true,value='%s' WHERE currency='USD'"%json.dumps(saved))
        w.h.proxy.close_connection(connection,'opening_rate_commit_reply_lost');gate.release()
        eventually(lambda:any(e['event']=='settlement_finished' and e.get('operation')==op for e in events(w.engine)))
        assert any(e['event']=='whole_transaction_retry' and e['external_call_repeated'] is False for e in events(w.engine))
        w.stop_engine();assert operation_state(w,op)==state
        w.start_engine();w.stop_engine();assert operation_state(w,op)==state
        replay=w.request('POST',f'/v1/machines/{machine}/actions/delete',{'acknowledge_destruction':True},t,'opening-rate-delete-'+str(available),status=202)
        assert replay['id']==op and operation_state(w,op)==state
        one_purchase(w,created)
        assert sum(e['stage']=='resource_deleted' for e in external(w,op))==1
        w.h.event('w6_opening_rate_repeat',operation=op,state=state,provider=external(w,op),
            rate_returned=True,landed_commit_reply_lost=True,whole_transaction_repeated=True,
            ordinary_successor=True,customer_replay=replay)
    finally:
        statement.release();gate.release();enumeration.unlink(missing_ok=True)
        w.h.sql("UPDATE provisiond.rates SET available=true,value='%s' WHERE currency='USD'"%json.dumps(saved))


def retained_state(w):
    # Excludes only diagnostic observations and transient HTTP traffic budgets.
    tables=['operations','retired_operations','idempotency_records','ledger_entries',
            'commitments','operator_deficiencies','machines','episodes','ceiling_counters','resource_exclusions']
    return {name:json.loads(w.h.sql(f"SELECT COALESCE(jsonb_agg(v ORDER BY v::text),'[]') FROM (SELECT to_jsonb(t) v FROM provisiond.{name} t) s")) for name in tables}


def retained_replay(w):
    """API-7/10/11/12/38, WIR-3/35, STO-14/25/33/35, DOM-21."""
    from w6 import lost,operator,one_purchase,eligible
    t,_,op,hold=lost(w,'retained-operator',search='incomplete')
    # Prior canonical scenarios can legitimately widen this account's observed
    # visibility. Use the existing disclosed per-operation age control before
    # requiring a real automatic attempt; never shorten its runtime window.
    eligible(w,op)
    w.start_engine();hold.unlink()
    eventually(lambda:any(e.get('event')=='resolution_search' and e.get('operation')==op for e in events(w.engine)))
    w.stop_engine()
    path=f'/v1/operations/{op}/actions/resolve'
    body=dict(resolution='abandoned',operator_ref='retained-operator')
    changed=dict(body,operator_ref='retained-changed')
    key='retained-operator-key'
    responses=[]
    def request(label,status,body=body,token=None,headers=None,key=key,target=path,raw=None,reason=None):
        code,value,hs=http(w.h.ports[7],'POST',target,body,token or w.operator_tokens[0],key,headers=headers,raw=raw)
        audit=eventually(lambda:[e for e in w.events('api','operator_decision') if e.get('correlation_id')==hs['X-Correlation-Id']]) if token!='invalid-operator-token' else []
        responses.append(dict(label=label,status=code,body=value,events=audit))
        if label=='physically_retired_identical':
            w.h.event('w6_retired_operator_replay',operation=op,responses=responses,
                retained=retained_state(w),provider=external(w,op),assertion=REPLAY_ASSERTION)
            assert code==status,REPLAY_ASSERTION
        else:assert code==status,(label,code,value)
        assert not any(k.startswith('Access-Control-') for k in hs)
        if reason:assert value['error']['details']['reason']==reason
        if status==200:assert audit[-1]['outcome'] in ('acknowledged','replayed')
        elif audit:assert audit[-1]['outcome']=='refused' and not any(e['outcome']=='acknowledged' for e in audit)
        return value
    original=request('resolution',200)
    assert request('unexpired_identical',200)==original
    request('unexpired_mismatch',409,body=changed,reason='idempotency_mismatch')
    # A second principal uses the same key on a separate real unresolved target.
    # It cannot observe this principal's receipt or acquire this target's effects.
    other_t,_,other,other_hold=lost(w,'retained-other-principal',search='incomplete')
    w.start_engine();other_hold.unlink();w.stop_engine()
    other_reply=operator(w,f'/v1/operations/{other}/actions/resolve',dict(resolution='absent',operator_ref='retained-other'),key,token=w.operator_tokens[1])
    assert other_reply['id']==other
    request('principal_collision',409,token=w.operator_tokens[1],reason='idempotency_mismatch')
    # Disclosed fixture deadline with components stopped, followed by real
    # housekeeping. Observe physical deletion before issuing any replay; no
    # held writer, checkout race or manual operation deletion stands in for it.
    w.api.terminate();w.api.wait(timeout=5)
    w.h.sql(f"UPDATE provisiond.idempotency_records SET expires_at=clock_timestamp()-interval '1 second' WHERE resource_kind='operation' AND resource_id='{op}'")
    until=w.h.sql(f"SELECT provisiond.wire_time(max(expires_at)) FROM provisiond.idempotency_records WHERE resource_kind='operation' AND resource_id='{op}'")
    w.restart_api()
    eventually(lambda:w.h.sql(f"SELECT count(*) FROM provisiond.operations WHERE id='{op}'")=='0')
    before=retained_state(w);provider=w.external()
    assert any(r['id']==op and r['tenant_id']==t['tenant_id'] for r in before['retired_operations'])
    expired=request('physically_retired_identical',410)
    assert expired['error']['details']['retained_until']==until
    for label,headers,token in [('global',{},None),('effective_tenant',{'X-Provisiond-Tenant':t['tenant_id']},None),('nonadmin_override_ignored',{'X-Provisiond-Tenant':other_t['tenant_id']},w.operator_tokens[1])]:
        if token:
            # The other principal never resolved this target; its key remains
            # independent even though its override has no scoping authority.
            request(label,409,token=token,headers=headers,reason='idempotency_mismatch')
        else:
            assert request(label,410,headers=headers)['error']['details']['retained_until']==until
            request(label+'_mismatch',409,body=changed,headers=headers,reason='idempotency_mismatch')
    request('wrong_tenant',404,headers={'X-Provisiond-Tenant':other_t['tenant_id']},raw=b'{invalid')
    request('unknown_identity',404,target=f'/v1/operations/{uuid.uuid4()}/actions/resolve')
    request('unknown_key',404,key='retained-brand-new-key')
    request('invalid_body',400,body=dict(body,unexpected=True))
    request('invalid_key',400,key='bad key')
    request('auth_before_parse',401,token='invalid-operator-token',raw=b'{invalid')
    canonical=b'{  "operator_ref":"retained-operator", "resolution":"abandoned" }'
    assert request('canonical_expired_replay',410,raw=canonical)['error']['details']['retained_until']==until
    detail=operator(w,f'/v1/operations/{op}',method='GET',status=410)
    assert detail['error']['details']['retained_until']==until
    assert retained_state(w)==before and w.external()==provider
    one_purchase(w,op);one_purchase(w,other)
    w.h.event('w6_retired_operator_replay_complete',operation=op,retained_until=until,
        responses=responses,before=before,after=retained_state(w),provider=external(w,op),
        physical_retirement=True,unchanged_receipts_fingerprints_deadlines=True)
    # The other operation has two genuine resolving receipts. Expiring only the
    # operator receipt cannot shorten the customer's still-live common horizon.
    w.h.sql(f"UPDATE provisiond.idempotency_records SET expires_at=clock_timestamp()-interval '1 second' WHERE scope_kind='operator' AND scope_id='operator-b' AND key='{key}'")
    maximum=w.h.sql(f"SELECT provisiond.wire_time(max(expires_at)) FROM provisiond.idempotency_records WHERE resource_kind='operation' AND resource_id='{other}'")
    live=retained_state(w)
    other_body=dict(resolution='absent',operator_ref='retained-other')
    assert operator(w,f'/v1/operations/{other}/actions/resolve',other_body,key,token=w.operator_tokens[1],headers={'X-Provisiond-Tenant':t['tenant_id']})==other_reply
    assert retained_state(w)==live and w.external()==provider
    w.h.event('w6_operator_maximum_horizon',operation=other,retained_until=maximum,
        operator_receipt_expired=True,customer_receipt_live=True,state=live,
        reply=other_reply,nonadmin_override_ignored=True)



FINAL_REPAIRS={'w6_f1_null':lambda w:opening_rate(w,False),
               'w6_f1_current':lambda w:opening_rate(w,True),'w6_f2':retained_replay}
