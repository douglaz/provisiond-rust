"""C1–C5: independently activated blocking continuation regressions."""
import json
import time
import uuid
from pathlib import Path
from harness import eventually
from workflow import http, PARAMETERS
from w5 import admit, snapshot, external, events
from w6_repairs import trace, marked


def dedicated_account(w,account):
    """Keep a scenario's observation clock independent of earlier long samples."""
    saved=json.loads(w.h.sql("SELECT jsonb_object_agg(account,assignable) FROM provisiond.provider_accounts"))
    descriptor=dict(w.descriptor,account=account)
    w.h.sql("INSERT INTO provisiond.provider_accounts VALUES('%s',true,'%s'); INSERT INTO provisiond.offer_listings VALUES('%s','%s',clock_timestamp()); UPDATE provisiond.provider_accounts SET assignable=(account='%s')"%(account,json.dumps(descriptor),account,json.dumps([w.offer]),account))
    accounts=json.loads(w.h.sql("SELECT jsonb_agg(account ORDER BY account) FROM provisiond.provider_accounts"))
    w.provider_control(offers={name:[w.offer] for name in accounts})
    w.restart_api()
    return saved


def restore_accounts(w,saved):
    w.h.sql("UPDATE provisiond.provider_accounts SET assignable=false")
    for account,assignable in saved.items():
        w.h.sql("UPDATE provisiond.provider_accounts SET assignable=%s WHERE account='%s'"%('true' if assignable else 'false',account))


def single_match(w):
    """OPS-27/33, LDG-32/39, STO-58: confirmation failure is not absence."""
    from w6 import lost, proof, eligible, one_purchase, operation_state
    saved=dedicated_account(w,'sim-positive')
    t,_,op,hold=lost(w,'single-confirmation',resource=True,search='normal',direct='failed')
    eligible(w,op);p=w.start_engine();hold.unlink()
    eventually(lambda:any(e['stage']=='resolution_direct_activated' and e['direct']=='failed' for e in external(w,op)))
    eventually(lambda:snapshot(w,op)['operation']['error']['details'].get('reconciliation',{}).get('matches')==1)
    before=snapshot(w,op);w.stop_engine()
    assert before['commitment']['reserved_sats']==43
    w.h.sql(f"UPDATE provisiond.operations SET mutation_wait_ended_at=clock_timestamp()-interval '61 seconds' WHERE id='{op}'")
    p=w.start_engine();eventually(lambda:events(p,'account_pass_complete'));w.stop_engine()
    after=snapshot(w,op)
    trace(w,op,'C1 exact single match, failed direct read, successor past negative window',before=before,after=after,fixture_age_seconds=61)
    assert after['commitment']['state']=='open' and after['commitment']['reserved_sats']==43,'C1 OPS-27 single exact match preserves original funding after failed confirmation'
    assert after['operation']['status']=='needs_reconciliation' and len(after['exclusions'])==1
    # Partial or failed later searches cannot retract the retained positive fact.
    for search in ('incomplete','failed'):
        proof(w,op,search=search,direct='failed');p=w.start_engine()
        eventually(lambda:events(p,'resolution_search'));w.stop_engine()
        assert snapshot(w,op)['commitment']['reserved_sats']==43
    proof(w,op,search='normal',direct='normal');w.start_engine();w.wait_status(op,'succeeded');w.stop_engine()
    final=operation_state(w,op);one_purchase(w,op)
    assert final['operation']['resolution']=='observed'
    assert sorted(int(l['amount_sats']) for l in final['ledger'])==[-20,-11]
    assert not final['deficiencies'] and not final['episodes']
    assert final['machine']['state']!='gone' and final['machine']['first_unit_debit_id'] is not None
    assert snapshot(w,op)['commitment']['reserved_sats']==12 and not snapshot(w,op)['exclusions']
    assert not any(e['stage']=='delete_accepted' for e in w.external()['events'] if e.get('account')==t['account'] and e.get('resource')==final['machine']['external_id'])
    trace(w,op,'C1 ordinary funded observed attachment after read recovery',final=final)
    from w5 import restore_resource
    restore_resource(w,t)
    restore_accounts(w,saved)


def schema_two(w,status,count=1):
    """Exact supported schema-2 rows, isolated while API/engine are stopped."""
    from core import binary
    w.stop_engine();w.api.terminate();w.api.wait(timeout=5)
    w.h.sql('DROP SCHEMA provisiond CASCADE; CREATE SCHEMA provisiond AUTHORIZATION migrator; GRANT USAGE ON SCHEMA provisiond TO app')
    for name in ('0001-core.sql','0002-local-service.sql'):
        w.h.sql(Path('migrations',name).read_text(),role='migrator')
    w.h.sql('CREATE TABLE provisiond.schema_migrations(version bigint PRIMARY KEY,semantics bigint NOT NULL,completed_at timestamptz NOT NULL DEFAULT transaction_timestamp()); INSERT INTO provisiond.schema_migrations(version,semantics) VALUES(1,1),(2,2); GRANT SELECT ON provisiond.schema_migrations TO app',role='migrator')
    descriptor=dict(w.descriptor,account='sim-a')
    w.h.sql("INSERT INTO provisiond.provider_accounts VALUES('sim-a',true,'%s'); INSERT INTO provisiond.tenant_identities(id) VALUES('legacy-tenant'); INSERT INTO provisiond.tenants(id,credential_digest,recovery_digest,status) VALUES('legacy-tenant','fixture','fixture','suspended')"%json.dumps(descriptor))
    rows=[]
    for i in range(count):
        machine,episode,op=[str(uuid.uuid4()) for _ in range(3)];external_id='legacy-'+machine
        w.h.sql("INSERT INTO provisiond.machines(id,tenant_id,provider_account,external_id,offer_id,kind,state,billing_unit_seconds) VALUES('%s','legacy-tenant','sim-a','%s','test-small','virtual','running',3600); INSERT INTO provisiond.episodes(id,machine_id,key,reasons,state) VALUES('%s','%s','delete','[\"tenant_suspended\"]','open'); INSERT INTO provisiond.operations(id,tenant_id,idempotency_key,kind,status,machine_id,provider_account,request_summary,correlation_id,requested_by,system_reason,episode_id) VALUES('%s','legacy-tenant','%s','delete','%s','%s','sim-a','{}','legacy-cleanup','system','tenant_suspended','%s')"%(machine,external_id,episode,machine,op,op,status,machine,episode))
        if status=='failed':
            error=dict(kind='unsupported',message='operation outside local dispatcher',retryable=False,details=dict(provider_account=None,capability='delete'))
        else:
            error=dict(kind='internal',message='engine restarted while operation was running',retryable=False,details={}) if status=='needs_reconciliation' else None
        if error is not None:w.h.sql("UPDATE provisiond.operations SET error='%s' WHERE id='%s'"%(json.dumps(error),op))
        w.provider_control(fixture_resource=dict(external_id=external_id,account='sim-a',present=True,state='running',correlator=None,channel='orders'))
        before=json.loads(w.h.sql(f"SELECT jsonb_build_object('operation',to_jsonb(o),'episode',(SELECT to_jsonb(e) FROM provisiond.episodes e WHERE id='{episode}')) FROM provisiond.operations o WHERE id='{op}'"))
        rows.append((machine,episode,op,before))
    binary(w.h,'migrate',role='migrator');w.launch_api()
    return rows


def legacy_failed(w):
    """OPS-48/STO-52/API-64/68: failed is stalled, never revived."""
    from w6 import operation_state, operator
    rows=schema_two(w,'failed',2)
    for machine,episode,op,before in rows:
        after=operation_state(w,op)
        trace(w,op,'C2 migrated base failed cleanup',before=before,after=after)
        assert after['episodes'][0]['state']=='stalled','C2 OPS-48 legacy failed current attempt migrates stalled'
        assert after['operation']['status']=='failed' and after['machine']['destroy_committed'] is None
        assert after['operation']['request_summary']['external_id']=='legacy-'+machine
        assert any(e['id']==episode for e in operator(w,'/v1/episodes?state=stalled,uncertain',method='GET')['episodes'])
    p=w.start_engine();eventually(lambda:events(p,'account_pass_complete'));w.stop_engine()
    assert all(not external(w,op) for _,_,op,_ in rows),'failed legacy attempts make no provider call'
    machine,episode,op,_=rows[0]
    retry=operator(w,f'/v1/episodes/{episode}/actions/retry',{'reason':'legacy supported retry'},'legacy-failed-retry')['current_operation_id']
    assert retry!=op and w.status(retry)=='queued' and w.status(op)=='failed'
    w.start_engine();w.wait_status(retry,'succeeded');w.stop_engine()
    assert len([e for e in external(w,retry) if e['stage']=='delete_accepted'])==1
    assert operation_state(w,retry)['episodes'][0]['close_reason']=='resource_gone'
    _,episode,op,_=rows[1];before=operation_state(w,op)
    operator(w,f'/v1/episodes/{episode}/actions/keep',{'operator_ref':'legacy-kept'},'legacy-failed-keep')
    after=operation_state(w,op)
    assert after['episodes'][0]['close_reason']=='kept' and after['ledger']==before['ledger'] and not external(w,op)
    trace(w,op,'C2 operator keep on migrated failed cleanup',before=before,after=after)


def legacy_unresolved(w):
    """OPS-23/45/48: interrupted legacy target retained without repeat."""
    from w6 import operation_state, operator, proof
    machine,episode,op,before=schema_two(w,'needs_reconciliation')[0]
    after=operation_state(w,op)
    trace(w,op,'C2 migrated base unresolved cleanup',before=before,after=after)
    assert after['operation']['request_summary'].get('external_id')=='legacy-'+machine,'C2 OPS-45 legacy unresolved attempt retains target identity'
    assert after['episodes'][0]['state']=='uncertain' and after['machine']['destroy_committed'] is None
    proof(w,op,direct='failed');w.h.sql(f"UPDATE provisiond.operations SET mutation_wait_ended_at=clock_timestamp()-interval '5 seconds' WHERE id='{op}'")
    p=w.start_engine();eventually(lambda:any(e['stage']=='resolution_direct_activated' for e in external(w,op)))
    eventually(lambda:events(p,'account_pass_complete'));assert p.poll() is None;w.stop_engine()
    assert w.status(op)=='needs_reconciliation' and operation_state(w,op)['episodes'][0]['state']=='uncertain'
    operator(w,f'/v1/operations/{op}/actions/resolve',{'resolution':'not_applied','operator_ref':'legacy-present'},'legacy-unresolved-resolution')
    assert operation_state(w,op)['episodes'][0]['state']=='stalled'
    operator(w,f'/v1/episodes/{episode}/actions/keep',{'operator_ref':'legacy-unresolved-kept'},'legacy-unresolved-keep')
    assert not any(e['stage']=='delete_accepted' for e in external(w,op))
    trace(w,op,'C2 safe evidence and operator exit, zero mutation repeats',trace=events(p))
    # The other supported base shape remains running until the ordinary startup
    # pass; migration supplies identity without inventing worker classification.
    machine,episode,op,before=schema_two(w,'running')[0]
    assert operation_state(w,op)['operation']['request_summary']['external_id']=='legacy-'+machine
    proof(w,op,direct='failed');p=w.start_engine()
    after=operation_state(w,op)
    assert after['operation']['status']=='needs_reconciliation' and after['episodes'][0]['state']=='uncertain'
    eventually(lambda:any(e['stage']=='resolution_direct_activated' for e in external(w,op)))
    assert p.poll() is None;w.stop_engine()
    assert not any(e['stage']=='delete_accepted' for e in external(w,op))
    trace(w,op,'C2 migrated running cleanup classified by actual ordinary startup',before=before,after=after,trace=events(p))


def receiptless(w,retry=False):
    """WIR-27/51, STO-14/33: actual retirement preserves scoped gone identity."""
    from core import core_probe
    from w6 import create, operator, proof, operation_state
    w.h.run(['cargo','build','--example','core-probe','--features','store-test-hooks','--locked'],timeout=120)
    t,_,machine=create(w,'receiptless-'+str(retry))
    w.h.sql(f"UPDATE provisiond.tenants SET status='suspended' WHERE id='{t['tenant_id']}'")
    p=core_probe(w.h);p.command('engine_start');episode=p.command('join_cleanup '+machine)['value']['episodes'][0];p.finish()
    op=w.h.sql(f"SELECT current_operation_id FROM provisiond.episodes WHERE id='{episode}'")
    if retry:
        proof(w,op,delete='reject');w.start_engine();w.wait_status(op,'failed');w.stop_engine()
        op=operator(w,f'/v1/episodes/{episode}/actions/retry',{'reason':'receiptless retry'},'receiptless-retry')['current_operation_id']
    w.start_engine();w.wait_status(op,'succeeded');w.stop_engine()
    assert w.h.sql(f"SELECT count(*) FROM provisiond.idempotency_records WHERE resource_kind='operation' AND resource_id='{op}'")=='0'
    before=operation_state(w,op)
    # The real job uses its ordinary age fallback; only this settled due-time is advanced.
    gate=w.h.proxy.arm('DELETE FROM provisiond.operations o','send')
    try:
        w.h.sql(f"UPDATE provisiond.operations SET updated_at=clock_timestamp()-interval '86401 seconds' WHERE id='{op}'")
        until=w.h.sql(f"SELECT provisiond.wire_time(updated_at+interval '86400 seconds') FROM provisiond.operations WHERE id='{op}'")
        gate.wait()
        logical=http(w.h.ports[7],'GET',f'/v1/operations/{op}',token=w.operator_tokens[0])
        assert w.h.sql(f"SELECT count(*) FROM provisiond.operations WHERE id='{op}'")=='1'
    finally:gate.release()
    eventually(lambda:w.h.sql(f"SELECT count(*) FROM provisiond.operations WHERE id='{op}'")=='0')
    code,body,_=http(w.h.ports[7],'GET',f'/v1/operations/{op}',token=w.operator_tokens[0])
    w.h.event('w6_receiptless_retirement',scenario=w.case,operation=op,episode=episode,before=before,retained_until=until,http_status=code,body=body,physical_row_removed=True,provider=external(w,op))
    assert code==410,'C3 WIR-51 retired receiptless attempt returns gone'
    assert logical[0]==410 and logical[1]['error']['details']['retained_until']==until
    assert body['error']['details']['retained_until']==until
    assert operator(w,f'/v1/episodes/{episode}',method='GET')['current_operation_id']==op
    assert operator(w,f'/v1/operations/{op}',method='GET',status=410,headers={'X-Provisiond-Tenant':t['tenant_id']})['error']['details']['retained_until']==until
    operator(w,f'/v1/operations/{op}',method='GET',status=404,headers={'X-Provisiond-Tenant':str(uuid.uuid4())})
    operator(w,f'/v1/operations/{uuid.uuid4()}',method='GET',status=404)
    w.request('GET',f'/v1/operations/{op}',tenant=t,status=410)
    w.request('POST',f'/v1/episodes/{episode}/actions/retry',{},t,'receiptless-hidden',status=404)


def ordinary_auth(w,site):
    """PRV-41/13c, OPS-11/13: exact ordinary successful-reply read site."""
    from w6 import one_purchase
    account='sim-ordinary-'+site.strip('/')
    saved=dedicated_account(w,account)
    t,_,op=admit(w,'ordinary-auth-'+site.strip('/'))
    w.provider_control(mode='normal',authentication_fault=dict(account=account,path=site,operation=op))
    original=snapshot(w,op)
    before=len(w.external()['events']);p=w.start_engine()
    eventually(lambda:any(e['stage']=='authentication_rejected' and e['command']==op and e['path']==site for e in w.external()['events'][before:]))
    eventually(lambda:w.status(op) in ('succeeded','needs_reconciliation'))
    time.sleep(2.3);w.stop_engine()
    observed=w.external()['events'][before:];failed=[e for e in observed if e['stage']=='authentication_rejected' and e['account']==account]
    state=w.h.sql(f"SELECT status FROM provisiond.provider_account_status WHERE provider_account='{account}'")
    trace(w,op,'C4 ordinary accepted-reply verification credential rejection',site=site,credential_status=state,observed_provider=observed,trace=events(p))
    assert failed and not failed[0].get('resolution') and failed[0]['command']==op
    assert state=='credentials_rejected' and len(failed)==1,'C4 PRV-41 ordinary '+site+' rejects credential durably'
    index=observed.index(failed[0])
    assert not any(e['stage']=='request_received' and e['account']==account for e in observed[index+1:])
    assert any(e['stage']=='request_received' and e['account']=='sim-b' for e in observed[index+1:])
    final=snapshot(w,op);one_purchase(w,op)
    assert final['operation']['request_summary']==original['operation']['request_summary']
    if site=='/constraints':
        assert w.status(op)=='succeeded' and final['machines'][0]['earliest_cancellation_date'] is None
        assert final['commitment']['reserved_sats']==12 and len(final['ledger'])==2
    else:
        assert w.status(op)=='needs_reconciliation' and final['commitment']['reserved_sats']==43
        assert final['operation']['error']['kind']=='authentication' and final['operation']['error']['details']['accepted'] is True
        assert not final['machines'] and not final['ledger']
        assert 'order_reply' in final['operation']['error']['details']['provider_identifiers']
        if site=='/get':assert 'search' in final['operation']['error']['details']['provider_identifiers']
    w.provider_control(authentication_fault=None)
    restore_accounts(w,saved)


def early_refusals(w):
    """SEC-32/39: authenticated early refusals never acquire decision effects."""
    from w6 import create, operator, operation_state
    t,_,machine=create(w,'refusal-target');operation_t,op=marked(w,'refusal-operation');w.start_engine();w.stop_engine()
    from core import core_probe
    w.h.run(['cargo','build','--example','core-probe','--features','store-test-hooks','--locked'],timeout=120)
    p=core_probe(w.h);p.command('engine_start');episode=p.command('join_cleanup '+machine)['value']['episodes'][0];p.finish()
    sentinel='SENTINEL-REFUSAL-SECRET'
    w.private.append(sentinel)
    paths=[f'/v1/operations/{op}/actions/resolve',f'/v1/episodes/{episode}/actions/retry',f'/v1/episodes/{episode}/actions/keep']
    before=[operation_state(w,x) for x in (op,w.h.sql(f"SELECT current_operation_id FROM provisiond.episodes WHERE id='{episode}'"))]
    provider=w.external();counts=w.h.sql('SELECT count(*) FROM provisiond.idempotency_records');ceilings=w.h.sql('SELECT count(*) FROM provisiond.ceiling_counters')
    cases=[]
    for path in paths:
        cases.extend([(path,{},None,{},400),(path,{},'bad key '+sentinel,{},400),(path.replace(op if '/operations/' in path else episode,str(uuid.uuid4())),{},'refusal-missing',{},404),(path,{},'refusal-scope',{'X-Provisiond-Tenant':str(uuid.uuid4())},404),(path,{},'refusal-body',{},400),(path,{'unknown':sentinel},'refusal-unknown',{},400),(path,{},'refusal-override',{'X-Provisiond-Tenant':sentinel},400)])
    responses=[]
    for i,(path,body,key,headers,expected) in enumerate(cases):
        code,reply,hs=http(w.h.ports[7],'POST',path,body,w.operator_tokens[0],key,headers=headers)
        assert code==expected,(code,reply)
        correlation=hs['X-Correlation-Id']
        audit=[e for e in w.events('api','operator_decision') if e.get('correlation_id')==correlation]
        responses.append(dict(status=code,correlation_id=correlation,events=audit))
        w.h.event('w6_early_operator_refusal',case=i,status=code,correlation_id=correlation,events=audit)
        assert any(e.get('outcome')=='refused' and e.get('principal')=='operator-a' and e.get('target')==path.split('/')[3] and e.get('reason') for e in audit),'C5 SEC-39 authenticated validation refusal emits safe decision event'
        assert all(e.get('outcome')!='acknowledged' for e in audit)
    for i,path in enumerate(paths):
        code,reply,hs=http(w.h.ports[7],'POST',path,token=w.operator_tokens[0],key='refusal-malformed-'+str(i),raw=('{"reason":"'+sentinel).encode())
        assert code==400
        audit=[e for e in w.events('api','operator_decision') if e.get('correlation_id')==hs['X-Correlation-Id']]
        assert len(audit)==1 and audit[0]['outcome']=='refused' and audit[0]['principal']=='operator-a'
        assert audit[0]['tenant']==(operation_t['tenant_id'] if i==0 else t['tenant_id'])
        assert audit[0]['target']==(op if i==0 else episode)
        w.h.event('w6_malformed_operator_body_refused',status=code,events=audit)
    assert [operation_state(w,x['operation']['id']) for x in before]==before
    assert w.external()==provider and w.h.sql('SELECT count(*) FROM provisiond.idempotency_records')==counts and w.h.sql('SELECT count(*) FROM provisiond.ceiling_counters')==ceilings
    # Rate refusal precedes the previous intent boundary too. The first validly
    # authenticated request consumes the fresh bucket; the second is refused.
    w.restart_api(dict(PARAMETERS,write_limit=1))
    first=http(w.h.ports[7],'POST',paths[0],{},w.operator_tokens[0])
    rate=http(w.h.ports[7],'POST',paths[0],{},w.operator_tokens[0])
    assert first[0]==400 and rate[0]==429
    audit=[e for e in w.events('api','operator_decision') if e.get('correlation_id')==rate[2]['X-Correlation-Id']]
    assert any(e.get('outcome')=='refused' and e.get('principal')=='operator-a' and e.get('target')==op for e in audit)
    w.h.event('w6_early_rate_refusal',status=rate[0],events=audit)
    w.restart_api()
    assert sentinel not in json.dumps(w.events('api'))
    w.h.event('w6_early_refusals_complete',responses=responses,state=before)


CONTINUATION={'w6_c1':single_match,'w6_c3_system':receiptless,'w6_c3_retry':lambda w:receiptless(w,True),
    'w6_c4_search':lambda w:ordinary_auth(w,'/search'),'w6_c4_get':lambda w:ordinary_auth(w,'/get'),
    'w6_c4_constraints':lambda w:ordinary_auth(w,'/constraints'),'w6_c5':early_refusals,
    'w6_c2_failed':legacy_failed,'w6_c2_unresolved':legacy_unresolved}
