"""API-7/11: retained target authorization cannot depend on operator receipts."""
import json
import uuid
from harness import eventually
from workflow import http
from w5 import external, events
from w6_final_repairs import retained_state

ASSERTIONS={kind:f'G1 API-11 {kind} cross-target mismatch survives actual retirement'
            for kind in ('create','delete')}


def retained_target(w,kind):
    """API-7/10/11/12/38, WIR-3/34/35, STO-14/25/33/35; CNF-21–24/158 subsets."""
    from w6 import lost,eligible,operator,create,deletion,one_purchase
    # A genuine decision on a different target supplies this principal's key.
    # Neither the target below nor the other principal has an operator receipt.
    origin_t,_,origin,hold=lost(w,'target-origin-'+kind,search='incomplete')
    eligible(w,origin);w.start_engine();hold.unlink()
    eventually(lambda:any(e.get('operation')==origin for e in events(w.engine,'resolution_search')))
    w.stop_engine()
    key='retained-target-key-'+kind
    origin_path=f'/v1/operations/{origin}/actions/resolve'
    origin_body=dict(resolution='abandoned',operator_ref='target-origin-'+kind)
    original=operator(w,origin_path,origin_body,key)
    assert operator(w,origin_path,origin_body,key)==original
    t,created,machine=create(w,'customer-target-'+kind)
    op=created
    if kind=='delete':
        op=deletion(w,t,machine,'customer-target-delete-completion')
        w.start_engine();w.wait_status(op,'succeeded');w.stop_engine()
    live=json.loads(w.h.sql(f"SELECT to_jsonb(o) FROM provisiond.operations o WHERE id='{op}'"))
    assert live['kind']==kind and live['status']=='succeeded' and live['resolution'] is None
    path=f'/v1/operations/{op}/actions/resolve'
    body=dict(resolution='abandoned',operator_ref='cross-target-'+kind)
    responses=[]

    def request(label,status,body=body,token=None,headers=None,key=key,target=path,raw=None,reason=None):
        code,value,hs=http(w.h.ports[7],'POST',target,body,token or w.operator_tokens[0],key,headers=headers,raw=raw)
        audit=eventually(lambda:[e for e in w.events('api','operator_decision') if e.get('correlation_id')==hs['X-Correlation-Id']]) if token!='invalid-operator-token' else []
        responses.append(dict(label=label,status=code,body=value,events=audit))
        if label=='retired_cross_target':
            w.h.event('w6_retired_target',operation=op,operation_kind=kind,live=live,
                origin=origin,receipt_scopes=scopes,retained=retained,retained_until=until,
                before_status=responses[0]['status'],after_status=code,responses=responses,
                before=before,after=retained_state(w),provider_before=provider,provider_after=w.external(),
                physical_retirement=True,assertion=ASSERTIONS[kind])
            assert code==status,ASSERTIONS[kind]
        else:assert code==status,(label,code,value)
        assert not any(k.startswith('Access-Control-') for k in hs)
        if reason:assert value['error']['details']['reason']==reason
        if audit:
            assert audit[-1]['outcome']=='refused' and not any(e['outcome']=='acknowledged' for e in audit)
            if status!=404:
                assert audit[-1]['operation_kind']==kind and audit[-1]['machine_id']==machine
                assert audit[-1]['tenant']==t['tenant_id']
        return value

    def scopes_now():
        return json.loads(w.h.sql(f"SELECT jsonb_object_agg(scope_kind,n) FROM (SELECT scope_kind,count(*) n FROM provisiond.idempotency_records WHERE resource_kind='operation' AND resource_id='{op}' GROUP BY scope_kind) s"))

    scopes=scopes_now();assert scopes=={'tenant':1}
    before_live=retained_state(w);provider=w.external()
    request('live_cross_target',409,reason='idempotency_mismatch')
    request('live_wrong_kind',400,body=dict(body,resolution='applied' if kind=='create' else 'absent'))
    assert retained_state(w)==before_live and w.external()==provider
    # The only clock control is the disclosed receipt deadline, with components
    # stopped. Real housekeeping must remove the operation and retain identity.
    w.api.terminate();w.api.wait(timeout=5)
    w.h.sql(f"UPDATE provisiond.idempotency_records SET expires_at=clock_timestamp()-interval '1 second' WHERE resource_kind='operation' AND resource_id='{op}'")
    until=w.h.sql(f"SELECT provisiond.wire_time(max(expires_at)) FROM provisiond.idempotency_records WHERE resource_kind='operation' AND resource_id='{op}'")
    due=retained_state(w)
    w.restart_api()
    eventually(lambda:w.h.sql(f"SELECT count(*) FROM provisiond.operations WHERE id='{op}'")=='0')
    before=retained_state(w);provider=w.external()
    retained=next(r for r in before['retired_operations'] if r['id']==op)
    assert retained['tenant_id']==t['tenant_id'] and retained['created_at']==live['created_at']
    assert scopes_now()==scopes
    assert [o for o in due['operations'] if o['id']!=op]==before['operations']
    assert [r for r in before['retired_operations'] if r['id']!=op]==due['retired_operations']
    for table in due.keys()-{'operations','retired_operations'}:assert due[table]==before[table],table
    detail=operator(w,f'/v1/operations/{op}',method='GET',status=410)
    assert detail['error']['details']['retained_until']==until
    request('retired_cross_target',409,reason='idempotency_mismatch')
    # New metadata assertions follow the behavioral assertion so protected old
    # schemas fail at the response defect, never at a missing-column/setup error.
    assert retained['kind']==kind and retained['machine_id']==machine
    assert retained['retained_until']==next(r['expires_at'] for r in before['idempotency_records'] if r['resource_id']==op)
    request('effective_tenant',409,headers={'X-Provisiond-Tenant':t['tenant_id']},reason='idempotency_mismatch')
    canonical=json.dumps(dict(operator_ref=body['operator_ref'],resolution='abandoned'),indent=2).encode()
    request('canonical_cross_target',409,raw=canonical,reason='idempotency_mismatch')
    request('wrong_tenant_before_parse',404,headers={'X-Provisiond-Tenant':origin_t['tenant_id']},raw=b'{invalid')
    request('unknown_before_parse',404,target=f'/v1/operations/{uuid.uuid4()}/actions/resolve',raw=b'{invalid')
    request('auth_before_parse',401,token='invalid-operator-token',raw=b'{invalid')
    request('retired_wrong_kind',400,body=dict(body,resolution='applied' if kind=='create' else 'absent'))
    request('retired_unknown_field',400,body=dict(body,unexpected=True))
    if kind=='delete':
        request('retired_delete_external_id',400,body=dict(body,resolution='applied',external_id='unexpected'))
        request('retired_delete_applied',409,body=dict(body,resolution='applied'),reason='idempotency_mismatch')
        request('retired_delete_not_applied',409,body=dict(body,resolution='not_applied'),reason='idempotency_mismatch')
    request('fresh_key',404,key='retained-target-fresh-'+kind)
    request('other_principal_same_key',404,token=w.operator_tokens[1])
    request('nonadmin_override_ignored',404,token=w.operator_tokens[1],headers={'X-Provisiond-Tenant':origin_t['tenant_id']})
    assert operator(w,origin_path,origin_body,key)==original
    assert retained_state(w)==before and w.external()==provider
    one_purchase(w,origin);one_purchase(w,created)
    target_effects=external(w,op)
    if kind=='delete':assert sum(e['stage']=='resource_deleted' for e in target_effects)==1
    w.h.event('w6_retired_target_complete',operation=op,operation_kind=kind,origin=origin,
        retained=retained,retained_until=until,receipt_scopes=scopes,responses=responses,
        before=before,after=retained_state(w),provider=provider,
        independent_origin=external(w,origin),independent_create=external(w,created),
        independent_target=target_effects,physical_retirement=True,
        requirement_subset='API-7/10/11/12/38 WIR-3/34/35 STO-14/25/33/35 DOM-21 CNF-21–24/158')


RETAINED_TARGETS={f'w6_g1_{kind}':lambda w,kind=kind:retained_target(w,kind) for kind in ASSERTIONS}
