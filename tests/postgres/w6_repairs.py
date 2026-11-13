"""R1–R9: activated W6 regressions on real API/engine/PG and independent effects."""
import concurrent.futures
import json
import time
import uuid
from pathlib import Path
from harness import eventually
from workflow import PARAMETERS, http
from w5 import admit, snapshot, external, orders, events


def marked(w, suffix, resource=False):
    """A real interrupted step-8 write, without invoking the purchase."""
    from w6 import proof
    w.stop_engine();t,_,op=admit(w,suffix,resource=resource)
    hold=w.control/'hold-step-9';hold.touch()
    p=w.start_engine()
    eventually(lambda:any(e.get('event')=='dispatch_step_held' and e.get('operation')==op for e in events(p)))
    w.stop_engine();hold.unlink()
    proof(w,op,search='incomplete')
    return t,op


def trace(w,op,boundary,**facts):
    from w6 import operation_state
    w.h.event('w6_repair_trace',scenario=w.case,operation=op,boundary=boundary,
              state=operation_state(w,op),provider=external(w,op),**facts)


def stable_window(w):
    """OPS-15/33, STO-58: evidence writes and successors cannot renew expiry."""
    from w6 import proof
    t,op=marked(w,'repair-stable',resource=True)
    w.start_engine()
    initial=snapshot(w,op)
    origin=time.monotonic();samples=[]
    # Reuse the reviewed probe's real marked-create/alternating-evidence prefix.
    # Stop before expiry to leave nonauthoritative evidence as the final mode.
    for i in range(12):
        proof(w,op,search='empty' if i%2==0 else 'incomplete')
        time.sleep(4)
        samples.append(snapshot(w,op))
        if i==5:w.stop_engine();w.start_engine()
    proof(w,op,search='incomplete')
    while time.monotonic()-origin<64:time.sleep(.1)
    state=snapshot(w,op);w.stop_engine()
    assert len(w.events('engine','resolution_search'))>=8,'R1 alternating evidence must actually be read'
    assert {s['operation']['error']['details']['reconciliation']['authoritative'] for s in samples}=={True,False}
    trace(w,op,'original real 60-second window after ordinary restart and alternating evidence',samples=samples,window_age_seconds=time.monotonic()-origin)
    assert state['commitment']['state']=='closed' and state['commitment']['reserved_sats']==0,'R1 OPS-33 original startup window releases reservation'
    assert state['operation']['status']=='needs_reconciliation' and state['operation']['resolution'] is None
    assert len(state['exclusions'])==1 and not orders(w,op)
    assert state['operation']['mutation_wait_ended_at'] is not None
    assert {s['operation']['mutation_wait_ended_at'] for s in samples}=={state['operation']['mutation_wait_ended_at']}
    assert initial['commitment']['reserved_sats']==43
    # Competing admission cannot consume the independent unresolved exclusion.
    body=w.create_body(t);w.request('POST','/v1/machines',body,t,'repair-stable-compete',status=409)
    from w6 import operator
    from w5 import restore_resource
    operator(w,f'/v1/operations/{op}/actions/resolve',{'resolution':'abandoned','operator_ref':'stable-window-finish'},'stable-window-finish')
    restore_resource(w,t)


def visibility_money(w):
    """OPS-33 release is local; PRV-36 continues to forbid early reads."""
    from w6 import lost,proof,one_purchase,eligible
    t,_,op,hold=lost(w,'repair-visibility',resource=True,search='incomplete')
    account=t['account']
    # A retained ordinary sample, rather than changing the declared bound.
    w.h.sql(f"INSERT INTO provisiond.provider_observations VALUES('{account}','{uuid.uuid4()}',clock_timestamp()-interval '120 seconds',clock_timestamp())")
    effective=float(w.h.sql(f"SELECT greatest(1,COALESCE(max(extract(epoch FROM observed_at-dispatched_at)),0)) FROM provisiond.provider_observations WHERE provider_account='{account}'"))
    assert effective>=120
    w.h.sql(f"UPDATE provisiond.operations SET mutation_wait_ended_at=clock_timestamp()-interval '54 seconds' WHERE id='{op}'")
    w.start_engine();hold.unlink();time.sleep(1.3)
    before=snapshot(w,op);assert before['commitment']['reserved_sats']==43
    w.stop_engine()
    w.h.sql(f"UPDATE provisiond.operations SET mutation_wait_ended_at=clock_timestamp()-interval '61 seconds' WHERE id='{op}'")
    received=len(external(w,op));p=w.start_engine()
    # A completed account pass follows local bookkeeping in both the unchanged
    # runtime and the fix. Observe it instead of cutting off a busy phase after
    # an assumed 1.4 seconds, before its first bookkeeping transaction finishes.
    eventually(lambda:events(p,'account_pass_complete'));w.stop_engine()
    after=snapshot(w,op)
    trace(w,op,'healthy expiry before effective provider-read window',before=before,after=after,effective_seconds=effective,injected_sample_seconds=120)
    assert after['commitment']['state']=='closed' and after['commitment']['reserved_sats']==0,'R2 OPS-33 healthy expiry independent of visibility'
    assert after['operation']['status']=='needs_reconciliation' and len(after['exclusions'])==1
    assert not any(e.get('resolution') for e in external(w,op)[received:] if e['stage']=='request_received')
    # Positive matched-in-progress knowledge survives local expiry while unhealthy.
    # This control uses a separate real lost purchase; do not reopen a commitment.
    t2,_,op2,barrier=lost(w,'repair-progress',search='in_progress')
    eligible(w,op2);w.start_engine();barrier.unlink()
    eventually(lambda:any(e.get('evidence',{}).get('in_progress') for e in w.events('engine','resolution_search') if e['operation']==op2))
    w.stop_engine();w.h.sql(f"UPDATE provisiond.operations SET mutation_wait_ended_at=clock_timestamp()-interval '130 seconds' WHERE id='{op2}'; UPDATE provisiond.provider_account_status SET status='account_unreachable' WHERE provider_account='{t2['account']}'")
    w.start_engine();time.sleep(1.2);w.stop_engine()
    assert snapshot(w,op2)['commitment']['reserved_sats']==43
    w.h.sql(f"UPDATE provisiond.provider_account_status SET status='healthy' WHERE provider_account='{t2['account']}'")
    proof(w,op2,search='normal');eligible(w,op2);w.start_engine();w.wait_status(op2,'succeeded');w.stop_engine();one_purchase(w,op2)
    # The released original attaches later under OPS-36, with exclusion settled.
    proof(w,op,search='normal');eligible(w,op);w.start_engine();w.wait_status(op,'succeeded');w.stop_engine()
    assert not snapshot(w,op)['exclusions'];one_purchase(w,op)
    from w5 import restore_resource
    restore_resource(w,t)


def cleanup_restart(w):
    """OPS-48: startup updates the current open episode in the same write."""
    from w6 import create,operation_state,proof
    from core import core_probe
    w.h.run(['cargo','build','--example','core-probe','--features','store-test-hooks','--locked'],timeout=120)
    for step in (101,103):
        t,_,machine=create(w,'repair-episode-'+str(step))
        w.h.sql(f"UPDATE provisiond.tenants SET status='suspended' WHERE id='{t['tenant_id']}'")
        p=core_probe(w.h);p.command('engine_start');episode=p.command('join_cleanup '+machine)['value']['episodes'][0];p.finish()
        op=w.h.sql(f"SELECT current_operation_id FROM provisiond.episodes WHERE id='{episode}'")
        hold=w.control/f'hold-step-{step}';hold.touch();p=w.start_engine()
        eventually(lambda:any(e.get('event')=='dispatch_step_held' and e.get('operation')==op for e in events(p)))
        before=operation_state(w,op);w.stop_engine();hold.unlink()
        proof(w,op,direct='failed');barrier=w.h.root/'provider/hold-reconciliation';barrier.touch()
        w.start_engine();after=operation_state(w,op);w.stop_engine();barrier.unlink()
        trace(w,op,'episode-backed ordinary restart before/after marker',step=step,before=before,after=after)
        assert after['operation']['status']=='needs_reconciliation'
        assert after['episodes'][0]['state']=='uncertain','R3 OPS-48 interrupted current episode becomes uncertain'
        assert after['machine']['destroy_committed']==before['machine']['destroy_committed']
        assert bool(after['operation']['write_started_at'])==(step==103)
        # Permanent close and ownership guard survive a later interruption.
        w.h.sql(f"UPDATE provisiond.episodes SET state='closed',closed_at=clock_timestamp(),close_reason='abandoned' WHERE id='{episode}'; UPDATE provisiond.machines SET destroy_committed=NULL WHERE id='{machine}'; UPDATE provisiond.operations SET status='running' WHERE id='{op}'")
        w.start_engine();closed=operation_state(w,op);w.stop_engine()
        assert closed['episodes'][0]['state']=='closed' and closed['episodes'][0]['close_reason']=='abandoned'


def resolution_ceiling(w):
    """API-7/SEC-39: replay first; one durable principal budget across tenants."""
    from w6 import operator
    authority=w.h.root/'operator-authority.json';original=authority.read_text()
    principals=json.loads(original)
    for entry in principals:entry['principal']='resolution-'+entry['principal']
    authority.write_text(json.dumps(principals))
    w.restart_api(dict(PARAMETERS,operator_resolution_limit=2,ceiling_seconds=86400))
    pairs=[marked(w,'repair-ceiling-'+str(i)) for i in range(4)]
    w.start_engine();w.stop_engine()
    body={'resolution':'abandoned','operator_ref':'repair-ceiling'}
    path=lambda op:f'/v1/operations/{op}/actions/resolve'
    first=operator(w,path(pairs[0][1]),body,'repair-ceiling-first')
    assert operator(w,path(pairs[0][1]),body,'repair-ceiling-first')==first
    operator(w,path(pairs[0][1]),dict(body,operator_ref='changed'),'repair-ceiling-first',status=409)
    before={op:snapshot(w,op) for _,op in pairs[1:]}
    def call(pair):return pair[1],http(w.h.ports[7],'POST',path(pair[1]),body,w.operator_tokens[0],'repair-ceiling-'+pair[1])
    with concurrent.futures.ThreadPoolExecutor(max_workers=2) as pool:results=list(pool.map(call,pairs[1:3]))
    w.h.event('w6_resolution_ceiling_activated',responses=results,before=before)
    assert sorted(r[1][0] for r in results)==[200,429],'R4 SEC-39 concurrent resolutions obey durable principal ceiling'
    denied=next(op for op,r in results if r[0]==429)
    assert snapshot(w,denied)==before[denied]
    assert int(w.h.sql("SELECT sum(used) FROM provisiond.ceiling_counters WHERE principal='operator:resolution-operator-a' AND ceiling='operator_resolutions'"))==2
    assert w.h.sql(f"SELECT count(*) FROM provisiond.idempotency_records WHERE key='repair-ceiling-{denied}'")=='0'
    # Second principal has independent allowance, and non-admin ignores override.
    operator(w,path(denied),body,'repair-ceiling-second',token=w.operator_tokens[1],headers={'X-Provisiond-Tenant':str(uuid.uuid4())})
    operator(w,path(pairs[3][1]),body,'repair-ceiling-exhausted',status=429)
    assert snapshot(w,pairs[3][1])==before[pairs[3][1]]
    # The exhausted allowance survives an actual API successor.
    w.restart_api(dict(PARAMETERS,operator_resolution_limit=2,ceiling_seconds=86400))
    assert operator(w,path(pairs[0][1]),body,'repair-ceiling-first')==first
    operator(w,path(pairs[3][1]),body,'repair-ceiling-exhausted',status=429)
    authority.write_text(original)
    w.restart_api()


def auth_stop(w):
    """PRV-41: each reached read site stops the credential, preserving others."""
    from w6 import create,lost,proof,eligible,deletion
    # This credential has its own retained observation history. Other scenarios
    # deliberately grow sim-a's visibility beyond the negative window; that
    # would legitimately release money before this case's pre-expiry check.
    account='sim-auth';other='sim-b'
    descriptor=dict(w.descriptor,account=account)
    w.h.sql("INSERT INTO provisiond.provider_accounts VALUES('%s',true,'%s'); INSERT INTO provisiond.offer_listings VALUES('%s','%s',clock_timestamp())"%(account,json.dumps(descriptor),account,json.dumps([w.offer])))
    w.provider_control(offers={name:[w.offer] for name in ('sim-a','sim-b',account)})
    w.restart_api()
    for site in ('/search','/get','/constraints','/enumerate','/keys/list','/keys/delete','account-get','delete-get'):
        w.stop_engine();w.h.sql("UPDATE provisiond.provider_account_status SET status='healthy'")
        # Force assignment to the account under test without changing runtime policy.
        w.h.sql(f"UPDATE provisiond.provider_accounts SET assignable=(account='{account}')")
        op=None
        if site in ('/search','/get','/constraints'):
            t,_,op,hold=lost(w,'repair-auth-'+site.strip('/'),search='normal');eligible(w,op);hold.unlink()
        else:
            t,created,machine=create(w,'repair-auth-'+site.replace('/',''))
            if site=='delete-get':
                op=deletion(w,t,machine,'repair-auth-delete');proof(w,op,delete='acceptance_not_applied')
                hold=w.h.root/'provider/hold-reconciliation';hold.touch();w.start_engine();w.wait_status(op,'needs_reconciliation');w.stop_engine();eligible(w,op);hold.unlink()
            elif site=='account-get':
                external_id=w.h.sql(f"SELECT external_id FROM provisiond.machines WHERE id='{machine}'")
                w.provider_control(remove_resource=external_id)
                eligible(w,created)
            elif site.startswith('/keys'):
                w.h.sql(f"UPDATE provisiond.provider_accounts SET descriptor=jsonb_set(descriptor,'{{temporary_keys}}','true') WHERE account='{account}'")
                # Eligible settled key, already owned by this test's real operation.
                http(w.h.ports[5],'POST','/keys',{'account':account,'operation':created})
        endpoint='/get' if site in ('account-get','delete-get') else site
        fault={'account':account,'path':endpoint}
        if site in ('/search','/get','/constraints','delete-get'):fault['operation']=op
        elif site=='account-get':fault['operation']=None
        before=len(w.external()['events']);w.provider_control(authentication_fault=fault)
        p=w.start_engine()
        eventually(lambda:any(e['stage']=='authentication_rejected' and e['account']==account and e['path']==endpoint for e in w.external()['events'][before:]))
        if site=='/search':
            original=snapshot(w,op);assert original['commitment']['reserved_sats']==43
            w.h.sql(f"UPDATE provisiond.operations SET mutation_wait_ended_at=clock_timestamp()-interval '61 seconds' WHERE id='{op}'")
            eventually(lambda:snapshot(w,op)['commitment']['state']=='closed')
            assert snapshot(w,op)['operation']['status']=='needs_reconciliation'
        time.sleep(2.4);w.stop_engine();observed=w.external()['events'][before:]
        failed=[e for e in observed if e['stage']=='authentication_rejected' and e['account']==account]
        state=w.h.sql(f"SELECT status FROM provisiond.provider_account_status WHERE provider_account='{account}'")
        w.h.event('w6_authentication_stop_activated',site=site,fault=fault,credential_status=state,provider=observed,trace=events(p))
        assert len(failed)==1 and state=='credentials_rejected','R5 PRV-41 one terminal authentication failure per credential'
        assert any(e.get('event')=='provider_credentials_rejected' and e.get('provider_account')==account and e.get('path')==endpoint
                   or e.get('event')=='resolution_read_refused' and e.get('kind')=='authentication' for e in events(p))
        if 'operation' in fault:assert failed[0]['command']==fault['operation'],'authentication fault must hit the selected read site'
        index=observed.index(failed[0])
        assert not any(e['stage']=='request_received' and e['account']==account for e in observed[index+1:]),'R5 PRV-41 no remaining or scheduled credential calls'
        assert any(e['stage']=='request_received' and e['account']==other for e in observed[index+1:]),'healthy account continues after credential stop'
        if site=='/constraints':
            assert w.status(op)=='succeeded' and snapshot(w,op)['machines'][0]['earliest_cancellation_date'] is None
        w.provider_control(authentication_fault=None)
        w.h.sql(f"UPDATE provisiond.provider_account_status SET status='healthy' WHERE provider_account='{account}'; UPDATE provisiond.provider_accounts SET descriptor=jsonb_set(descriptor,'{{temporary_keys}}','false') WHERE account='{account}'")
        if op and w.status(op)=='needs_reconciliation':
            from w6 import operator
            operator(w,f'/v1/operations/{op}/actions/resolve',{'resolution':'abandoned','operator_ref':'auth-test-cleanup'},'auth-cleanup-'+op)
    w.h.sql(f"UPDATE provisiond.provider_accounts SET assignable=(account<>'{account}')")


def operator_retention(w):
    """WIR-27/51: logical expiry and receipt-only operator identities agree."""
    from w6 import create,operator
    t,op,_=create(w,'repair-retention')
    retirement=w.h.proxy.arm('DELETE FROM provisiond.operations o','send')
    try:
        w.h.sql(f"UPDATE provisiond.idempotency_records SET expires_at=clock_timestamp()-interval '1 second' WHERE resource_id='{op}'")
        until=w.h.sql(f"SELECT provisiond.wire_time(max(expires_at)) FROM provisiond.idempotency_records WHERE resource_id='{op}'")
        retirement.wait()
        assert w.h.sql(f"SELECT count(*) FROM provisiond.operations WHERE id='{op}'")=='1'
        code,body,_=http(w.h.ports[7],'GET',f'/v1/operations/{op}',token=w.operator_tokens[0])
        w.h.event('w6_operator_retention_activated',operation=op,retained_until=until,http_status=code,body=body,physical_row_retained=True)
        assert code==410 and body['error']['details']['retained_until']==until,'R6 WIR-27 operator logical expiry returns retained horizon'
        assert w.h.sql(f"SELECT count(*) FROM provisiond.operations WHERE id='{op}'")=='1'
    finally:retirement.release()
    eventually(lambda:w.h.sql(f"SELECT count(*) FROM provisiond.operations WHERE id='{op}'")=='0')
    operator(w,f'/v1/operations/{op}',method='GET',status=410)
    operator(w,f'/v1/operations/{op}',method='GET',status=410,headers={'X-Provisiond-Tenant':t['tenant_id']})
    operator(w,f'/v1/operations/{op}',method='GET',status=404,headers={'X-Provisiond-Tenant':str(uuid.uuid4())})
    operator(w,f'/v1/operations/{op}',method='GET',status=410,token=w.operator_tokens[1],headers={'X-Provisiond-Tenant':str(uuid.uuid4())})
    operator(w,f'/v1/operations/{uuid.uuid4()}',method='GET',status=404)
    w.request('GET',f'/v1/operations/{op}',tenant=t,status=410)
    w.request('POST',f'/v1/operations/{op}/actions/resolve',{},t,'retention-hidden-key',status=404)
    # WIR-51's retained current attempt can have only an operator resolution
    # receipt. Its permanent episode supplies effective-tenant authorization.
    from core import core_probe
    from w6 import proof
    w.h.run(['cargo','build','--example','core-probe','--features','store-test-hooks','--locked'],timeout=120)
    t,_,machine=create(w,'retention-episode')
    w.h.sql(f"UPDATE provisiond.tenants SET status='suspended' WHERE id='{t['tenant_id']}'")
    p=core_probe(w.h);p.command('engine_start');episode=p.command('join_cleanup '+machine)['value']['episodes'][0];p.finish()
    current=w.h.sql(f"SELECT current_operation_id FROM provisiond.episodes WHERE id='{episode}'")
    hold=w.h.root/'provider/hold-reconciliation';hold.touch();proof(w,current,delete='acceptance_not_applied')
    w.start_engine();w.wait_status(current,'needs_reconciliation');w.stop_engine();hold.unlink()
    operator(w,f'/v1/operations/{current}/actions/resolve',{'resolution':'not_applied','operator_ref':'retention-episode'},'retention-episode-resolve')
    w.api.terminate();w.api.wait(timeout=5)
    w.h.sql(f"UPDATE provisiond.idempotency_records SET expires_at=clock_timestamp()-interval '1 second' WHERE resource_id='{current}'; DELETE FROM provisiond.operations WHERE id='{current}'")
    w.launch_api()
    assert operator(w,f'/v1/episodes/{episode}',method='GET')['current_operation_id']==current
    operator(w,f'/v1/operations/{current}',method='GET',status=410,headers={'X-Provisiond-Tenant':t['tenant_id']})
    operator(w,f'/v1/operations/{current}',method='GET',status=404,headers={'X-Provisiond-Tenant':str(uuid.uuid4())})
    _,unresolved=marked(w,'retention-unresolved');w.start_engine();w.stop_engine()
    w.h.sql(f"UPDATE provisiond.idempotency_records SET expires_at=clock_timestamp()-interval '1 second' WHERE resource_id='{unresolved}'")
    assert operator(w,f'/v1/operations/{unresolved}',method='GET')['status']=='needs_reconciliation'
    w.h.event('w6_episode_retention_identity',episode=episode,current_operation_id=current,unresolved=unresolved)


def legacy_cleanup(w):
    """STO-12/OPS-23/35/45: exact schema-2 placeholder upgrades offline."""
    from core import binary
    from w6 import proof,operation_state
    w.stop_engine();w.api.terminate();w.api.wait(timeout=5)
    # This case is last: replace only this invocation's schema while all runtime
    # components are stopped. Provider simulator state remains independently held.
    w.h.sql('DROP SCHEMA provisiond CASCADE; CREATE SCHEMA provisiond AUTHORIZATION migrator; GRANT USAGE ON SCHEMA provisiond TO app')
    for name in ('0001-core.sql','0002-local-service.sql'):
        w.h.sql(Path('migrations',name).read_text(),role='migrator')
    w.h.sql('CREATE TABLE provisiond.schema_migrations(version bigint PRIMARY KEY,semantics bigint NOT NULL,completed_at timestamptz NOT NULL DEFAULT transaction_timestamp()); INSERT INTO provisiond.schema_migrations(version,semantics) VALUES(1,1),(2,2); GRANT SELECT ON provisiond.schema_migrations TO app',role='migrator')
    machine,episode,op=[str(uuid.uuid4()) for _ in range(3)];external_id='legacy-'+machine
    descriptor=dict(w.descriptor,account='sim-a')
    w.h.sql("INSERT INTO provisiond.provider_accounts VALUES('sim-a',true,'%s'); INSERT INTO provisiond.tenant_identities(id) VALUES('legacy-tenant'); INSERT INTO provisiond.tenants(id,credential_digest,recovery_digest,status) VALUES('legacy-tenant','fixture','fixture','suspended'); INSERT INTO provisiond.machines(id,tenant_id,provider_account,external_id,offer_id,kind,state,billing_unit_seconds) VALUES('%s','legacy-tenant','sim-a','%s','test-small','virtual','running',3600); INSERT INTO provisiond.episodes(id,machine_id,key,reasons,state) VALUES('%s','%s','delete','[\"tenant_suspended\"]','open'); INSERT INTO provisiond.operations(id,tenant_id,idempotency_key,kind,status,machine_id,provider_account,request_summary,correlation_id,requested_by,system_reason,episode_id) VALUES('%s','legacy-tenant','legacy-cleanup','delete','queued','%s','sim-a','{}','legacy-cleanup','system','tenant_suspended','%s')"%(json.dumps(descriptor),machine,external_id,episode,machine,op,machine,episode))
    invalid=[]
    for origin,target,account in [('caller',machine,'sim-a'),('system',None,'sim-a'),('system',machine,'unrelated-account')]:
        bad=str(uuid.uuid4());invalid.append(bad)
        w.h.sql("INSERT INTO provisiond.operations(id,tenant_id,idempotency_key,kind,status,machine_id,provider_account,request,request_summary,correlation_id,requested_by,system_reason,episode_id) VALUES('%s','legacy-tenant','%s','delete','queued',%s,'%s','{\"acknowledge_destruction\":true}','{}','legacy-invalid','%s','tenant_suspended',NULL)"%(bad,bad,"'%s'"%target if target else 'NULL',account,origin))
    w.provider_control(fixture_resource=dict(external_id=external_id,account='sim-a',present=True,state='running',correlator=None,channel='orders'))
    for bad in invalid:w.h.sql(f"UPDATE provisiond.operations SET available_at=clock_timestamp()+interval '1 hour' WHERE id='{bad}'")
    binary(w.h,'migrate',role='migrator')
    proof(w,op,delete='acceptance_applied');hold=w.h.root/'provider/hold-reconciliation';hold.touch()
    w.start_engine();w.wait_status(op,'needs_reconciliation');w.stop_engine();hold.unlink()
    # Set a controlled eligible age without changing the production window.
    w.h.sql(f"UPDATE provisiond.operations SET mutation_wait_ended_at=clock_timestamp()-interval '5 seconds' WHERE id='{op}'")
    p=w.start_engine();time.sleep(1.5)
    trace(w,op,'actual offline schema-2 placeholder through acceptance-only delete',trace=events(p),engine_exit=p.poll())
    assert p.poll() is None and w.status(op)=='succeeded','R7 retained schema-2 cleanup identity resolves without engine exit'
    w.stop_engine();state=operation_state(w,op)
    assert state['operation']['episode_id']==episode and state['operation']['machine_id']==machine
    assert state['episodes'][0]['id']==episode and state['episodes'][0]['close_reason']=='resource_gone'
    assert len([e for e in external(w,op) if e['stage']=='delete_accepted'])==1
    for bad in invalid:w.h.sql(f"UPDATE provisiond.operations SET available_at=clock_timestamp() WHERE id='{bad}'")
    w.start_engine()
    for bad in invalid:w.wait_status(bad,'failed')
    w.stop_engine()
    for bad in invalid:
        state=operation_state(w,bad)
        assert state['operation']['status']=='failed' and state['operation']['error']['kind']=='conflict'
        assert state['operation']['error']['details']['reason']=='incomplete_record'
        assert state['operation']['write_started_at'] is None and not external(w,bad)


def operator_events(w):
    """SEC-32/39: intent, acknowledgment and replay are separate observations."""
    from w6 import operator,create,operation_state
    _,op=marked(w,'repair-events');w.start_engine();w.stop_engine()
    body={'resolution':'abandoned','operator_ref':'safe-evidence-reference'}
    operator(w,f'/v1/operations/{op}/actions/resolve',body,'repair-events-key')
    operator(w,f'/v1/operations/{op}/actions/resolve',body,'repair-events-key')
    observed=[e for e in w.events('api') if e.get('event')=='operator_decision' and e.get('target')==op]
    w.h.event('w6_operator_events_activated',operation=op,events=observed,state=operation_state(w,op))
    assert any(e.get('outcome')=='acknowledged' for e in observed),'R8 SEC-39 monitorable authenticated operator decision'
    assert any(e.get('outcome')=='replayed' for e in observed)
    assert all(e['principal']=='operator-a' and e['correlation_id'] and e['reason'] for e in observed)
    # A landed COMMIT with its response actually lost is uncertainty, followed
    # by an authenticated same-key replay, never a fabricated acknowledgment.
    _,lost_op=marked(w,'repair-event-lost');w.start_engine();w.stop_engine()
    statement=w.h.proxy.arm('resolution=$4','send')
    with concurrent.futures.ThreadPoolExecutor(max_workers=1) as pool:
        future=pool.submit(http,w.h.ports[7],'POST',f'/v1/operations/{lost_op}/actions/resolve',body,w.operator_tokens[0],'repair-event-lost-key')
        connection=statement.wait();gate=w.h.proxy.arm('store COMMIT','commit_reply',session=connection['pid']);statement.release()
        try:
            gate.wait();assert w.status(lost_op)=='failed'
            before=operation_state(w,lost_op)
            w.h.proxy.close_connection(connection,'operator_commit_reply_lost');gate.release()
            response=future.result(timeout=8);assert response[0]==500
        finally:gate.release();statement.release()
    operator(w,f'/v1/operations/{lost_op}/actions/resolve',body,'repair-event-lost-key')
    observed=[e for e in w.events('api','operator_decision') if e['target']==lost_op]
    assert [e['outcome'] for e in observed]==['intent','uncertain','intent','replayed']
    assert operation_state(w,lost_op)==before
    w.h.event('w6_operator_commit_reply_lost',operation=lost_op,events=observed,state=before)
    # Existing episode lifecycle drives both verbs; verify their real events.
    from w6 import episodes
    episodes(w,'-audit')
    verbs={e.get('verb') for e in w.events('api','operator_decision') if e.get('outcome')=='acknowledged'}
    assert {'resolve','retry','keep'}<=verbs
    for e in w.events('api','operator_decision'):
        if e['verb']=='retry' and e['outcome']=='acknowledged':
            assert w.h.sql(f"SELECT requested_by='operator' AND episode_id='{e['target']}' FROM provisiond.operations WHERE id='{e['operation_id']}'")=='t'


def delete_validation(w):
    """OPS-2/23, WIR-22: retain and repeat caller acknowledgment validation."""
    from w6 import create,deletion,operation_state
    for i,invalid in enumerate(({}, {'acknowledge_destruction':False}, {'acknowledge_destruction':True,'unknown':True})):
        t,_,machine=create(w,'repair-delete-validation-'+str(i));op=deletion(w,t,machine,'repair-delete-invalid-'+str(i))
        retained=json.loads(w.h.sql(f"SELECT request FROM provisiond.operations WHERE id='{op}'"))
        w.h.event('w6_delete_payload_retained',operation=op,request=retained)
        # Do not fail before activation: the controlled invalid body must reach
        # the worker so the red tests runtime behavior, not merely persistence.
        w.h.sql("UPDATE provisiond.operations SET request='%s' WHERE id='%s'"%(json.dumps(invalid),op))
        before=operation_state(w,op);w.start_engine();eventually(lambda:w.status(op) in ('failed','succeeded'));w.stop_engine();after=operation_state(w,op)
        trace(w,op,'real API delete admitted, caller field invalidated before worker',retained=retained,before=before,after=after)
        assert after['operation']['status']=='failed' and after['operation']['error']['kind']=='invalid_request','R9 OPS-23 invalid stored caller delete fails before mutation'
        assert after['operation']['error']['details']['field']==('/unknown' if 'unknown' in invalid else '/acknowledge_destruction')
        assert after['operation']['error']['retryable'] is False
        assert retained=={'acknowledge_destruction':True}
        assert w.h.sql(f"SELECT request IS NULL FROM provisiond.operations WHERE id='{op}'")=='t'
        assert after['operation']['write_started_at'] is None and not external(w,op)
        assert after['commitments']==before['commitments'] and after['ledger']==before['ledger'] and after['machine']==before['machine']
        normal=deletion(w,t,machine,'repair-delete-normal-'+str(i));w.start_engine();w.wait_status(normal,'succeeded');w.stop_engine()
    t,_,machine=create(w,'repair-delete-api-field');op=deletion(w,t,machine,'repair-delete-api-field-invalid')
    w.h.sql(f"UPDATE provisiond.operations SET request_summary=request_summary-'external_id' WHERE id='{op}'")
    before=operation_state(w,op);w.start_engine();w.wait_status(op,'failed');w.stop_engine();after=operation_state(w,op)
    assert after['operation']['error']['kind']=='conflict' and after['operation']['error']['details']['reason']=='incomplete_record'
    assert after['operation']['error']['retryable'] is True and after['operation']['write_started_at'] is None
    assert before['commitments']==after['commitments'] and before['ledger']==after['ledger'] and not external(w,op)


REPAIRS={'w6_r1':stable_window,'w6_r2':visibility_money,'w6_r3':cleanup_restart,
         'w6_r4':resolution_ceiling,'w6_r5':auth_stop,'w6_r6':operator_retention,
         'w6_r8':operator_events,'w6_r9':delete_validation,'w6_r7':legacy_cleanup}
