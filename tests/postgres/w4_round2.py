"""LDG-47, WIR-34, STO-7/55: HTTP regressions at the real adapter/owner boundaries."""
import concurrent.futures
import hashlib
from http.client import HTTPException
import json
import socket
import subprocess
import time

from core import environment
from harness import eventually
from workflow import http


def actual_credit(w, over=False):
    # CNF-116/117: intent stays 120, while the trusted final value differs.
    t=w.enrol();key='observed-over' if over else 'observed-under'
    d=w.request('POST','/v1/deposits',{'amount_sats':120},t,key)
    amount=121 if over else 41
    chain_amount=3 if over else 66
    payment_hash=w.h.sql(f"SELECT payment_hash FROM provisiond.deposits WHERE id='{d['id']}'")
    ln=dict(rail='lightning',destination=d['lightning']['invoice'],payment_hash=payment_hash,
            amount_sats=amount,settled=True,preimage_known=True,
            settled_at=time.strftime('%Y-%m-%dT%H:%M:%SZ',time.gmtime()))
    before=len(w.events('api','rail_credit_retry_pending'))
    w.settlement(ln)
    eventually(lambda:w.h.sql(f"SELECT count(*) FROM provisiond.payments WHERE deposit_id='{d['id']}'")=='1'
               or len(w.events('api','rail_credit_retry_pending'))>before)
    detail=w.request('GET','/v1/deposits/'+d['id'],tenant=t)
    w.h.event('actual_lightning_value_observed',requested_sats=120,settled_sats=amount,
              credited_sats=detail['credited_sats'],deposit=d['id'])
    assert detail['credited_sats']==amount,'LDG-47 must credit the trusted Lightning '+('overpayment' if over else 'underpayment')
    w.processed(ln,True)
    assert detail['requested_sats']==120
    assert w.request('GET','/v1/enrol/'+t['handle'])=={'status':'active' if over else 'pending'}
    if not over:
        assert w.h.sql(f"SELECT count(*) FROM provisiond.tenant_provider_accounts WHERE tenant_id='{t['tenant_id']}'")=='0'
    chain=dict(rail='onchain',destination=d['onchain']['address'],txid=hashlib.sha256(key.encode()).hexdigest(),
               vout=0,confirmations=2,amount_sats=chain_amount,settled_at=ln['settled_at'])
    w.settlement(chain);w.processed(chain,True)
    assert w.request('GET','/v1/enrol/'+t['handle'])=={'status':'active'}
    providers=w.request('GET','/v1/providers',tenant=t)['providers']
    assert len(providers)==1
    assert w.h.sql(f"SELECT s.status FROM provisiond.tenant_provider_accounts a JOIN provisiond.provider_account_status s USING(provider_account) WHERE a.tenant_id='{t['tenant_id']}'")=='healthy'
    # Concurrent adapter deliveries, not a customer balance-setting endpoint.
    replays=[dict(v,event_id=key+'-'+str(i)) for i,v in enumerate([ln,chain,ln,chain])]
    with concurrent.futures.ThreadPoolExecutor(max_workers=4) as pool:list(pool.map(w.settlement,replays))
    for v in replays:w.processed(v,True)
    total=amount+chain_amount
    assert w.request('GET','/v1/deposits/'+d['id'],tenant=t)['credited_sats']==total
    assert w.request('POST','/v1/deposits',{'amount_sats':120},t,key)==d
    # STO-30/46: the two payment rows reference exactly their matching credits.
    state=w.h.sql(f"SELECT count(*)||':'||sum(p.amount_sats)||':'||bool_and(p.amount_sats=l.amount_sats AND l.deposit_id=p.deposit_id AND l.tenant_id=p.credited_tenant_id) FROM provisiond.payments p JOIN provisiond.ledger_entries l ON l.id=p.ledger_entry_id WHERE p.deposit_id='{d['id']}'")
    assert state==f'2:{total}:true'
    assert w.h.sql(f"SELECT count(*) FROM provisiond.ledger_entries WHERE deposit_id='{d['id']}'")=='2'
    w.h.event('actual_payment_atomic_replay_observed',case=key,deposit=d['id'],requested_sats=120,
              lightning_sats=amount,onchain_sats=chain_amount,payments=2,credits=2,
              credited_sats=total,healthy_assignments=1,concurrent_replays=4)
    http(w.h.ports[6],'POST','/control',{'clear':True})
    w.h.results.append(dict(test='W4-'+key+'-credit',result='pass'))


def underpayment(w):actual_credit(w)
def overpayment(w):actual_credit(w,True)


def operator_hiding(w):
    from w4_regressions import funded
    active=funded(w,'operator-hiding');pending=w.enrol()
    routes=[('GET','/v1/provider-accounts'),('GET','/v1/tenants/id'),
        ('GET','/v1/episodes'),('GET','/v1/episodes/id'),
        ('POST','/v1/episodes/id/actions/retry'),('POST','/v1/episodes/id/actions/keep'),
        ('GET','/v1/address-resolution'),('GET','/v1/machines/id/attachments'),
        ('POST','/v1/machines/id/attachments/attachment/actions/release'),
        ('POST','/v1/abuse-cases'),('POST','/v1/abuse-cases/id/actions/close'),
        ('POST','/v1/abuse-cases/id/actions/record-transmission'),
        ('POST','/v1/abuse-cases/id/actions/revise-deadline'),
        ('POST','/v1/machines/id/actions/record-network-restriction'),
        ('POST','/v1/operations/id/actions/resolve'),('POST','/v1/deposits/id/actions/attribute'),
        ('POST','/v1/tenants/id/actions/suspend'),('POST','/v1/tenants/id/actions/resume'),
        ('POST','/v1/tenants/id/actions/assign-provider-account'),
        ('POST','/v1/provider-accounts/sim-a/actions/record-status'),
        ('POST','/v1/provider-accounts/sim-a/actions/record-billing'),
        ('GET','/v1/provider-accounts/sim-a/billing')]
    counts="SELECT (SELECT count(*) FROM provisiond.operations)||':'||(SELECT count(*) FROM provisiond.idempotency_records)||':'||(SELECT count(*) FROM provisiond.ledger_entries)"
    before=w.h.sql(counts);begins=len(w.events('api','http_business_begin'));effects=len(w.external()['events'])
    for t in [active,pending]:
        for method,path in routes:
            for query in ['', '?state=stalled&address=127.0.0.1']:
                code,value,_=http(w.h.ports[4],method,path+query,token=t['spending_token'],raw=b'{invalid')
                w.h.event('operator_route_hidden_observed',method=method,path=path,query_qualified=bool(query),principal_status=t['status'],status=code)
                assert code==404 and value['error']['kind']=='not_found','WIR-34 operator routes must return exact 404 to active and pending customers'
    assert w.h.sql(counts)==before and len(w.events('api','http_business_begin'))==begins
    assert len(w.external()['events'])==effects
    for path in ['/v1/providers','/v1/providers/'+active['account']+'/offers','/v1/balance','/v1/machines','/v1/operations']:
        w.request('GET',path,tenant=active)
    # Shared customer verbs remain unsupported, rather than hidden as operator verbs.
    w.request('GET','/v1/abuse-cases',tenant=active,status=501)
    w.request('POST','/v1/machines/id/actions/power',{},active,'unsupported-power',status=501)
    w.request('GET','/v1/abuse-cases',tenant=pending,status=403)
    for method,path in routes:w.request('OPTIONS',path)
    w.h.results.append(dict(test='W4-complete-operator-hiding',result='pass',customer_requests=len(routes)*4))


def api_events(p):
    return [json.loads(line) for line in p.log.read_text().splitlines() if line.startswith('{')]


def startup(w):
    # Default binary, no retry for negative startup. Schema-only needs no service values.
    w.api.terminate();w.api.wait(timeout=5)
    target=w.h.root/'default-startup-target'
    w.h.run(['cargo','build','--bin','api','--locked','--target-dir',str(target)],timeout=120)
    executable=target/'debug/api'
    identity=w.build_identity(executable,[])
    w.h.config['workflow']['builds']['default_startup_api']=identity
    base=dict(w.h.env,**environment(w.h),PROVISIOND_LOCAL_PARAMETERS=str(w.parameters),
              PROVISIOND_API_LISTEN=f'127.0.0.1:{w.h.ports[4]}',
              PROVISIOND_RAIL_SIMULATOR=f'127.0.0.1:{w.h.ports[6]}')
    def run(name,env,args=()):
        log=w.h.root/(name+'.stdout');err=w.h.root/(name+'.stderr')
        with log.open('w') as out,err.open('w') as errors:
            p=subprocess.Popen([str(executable),*args],env=env,stdout=out,stderr=errors,text=True)
        p.name='api';p.log=log;p.error=err;w.children.append(p);w.h.extra.append(p)
        w.h.event('workflow_binary_started',component='api',case=name,**identity)
        eventually(lambda:p.poll() is not None or any(e['event']=='api_listening' for e in api_events(p)))
        return p
    for var in ['PROVISIOND_RAIL_SIMULATOR','PROVISIOND_API_LISTEN']:
        for label,value in [('missing',None),('malformed','not-an-address'),('nonloopback','192.0.2.1:1234'),('zero-port','127.0.0.1:0')]:
            env=dict(base)
            if value is None:env.pop(var,None)
            else:env[var]=value
            p=run('startup-'+var+'-'+label,env)
            ready=any(e['event']=='api_listening' for e in api_events(p))
            w.h.event('required_configuration_startup_observed',parameter=var,case=label,exit_status=p.poll(),api_listening=ready)
            assert not ready and p.poll() not in (None,0),'required local configuration must refuse startup before api_listening'
            p.wait(timeout=5)
            assert p.returncode==1 and var in p.error.read_text()
            assert not any(e['event'] in ['external_entry','http_business_begin','rail_credit_acknowledged'] for e in api_events(p))
            with socket.socket() as s:assert s.connect_ex(('127.0.0.1',w.h.ports[4]))!=0
    env=dict(base)
    for var in ['PROVISIOND_LOCAL_PARAMETERS','PROVISIOND_API_LISTEN','PROVISIOND_RAIL_SIMULATOR']:env.pop(var,None)
    p=run('schema-only-without-service',env,['--check-schema']);assert p.wait(timeout=5)==0
    assert w.build_identity(w.h.target/'debug/api',w.builds['api']['features'])==w.builds['api']
    w.launch_api();w.request('OPTIONS','/v1/machines')
    w.h.results.append(dict(test='W4-required-startup-configuration',result='pass',default_binary=True,negative_cases=8))


def clean_failure(w,p,assertion):
    # No extra customer connection is made to wake the accept loop.
    eventually(lambda:p.poll() is not None or any(e['event']=='api_stopping' for e in api_events(p)),seconds=4)
    if p.poll() is None:eventually(lambda:p.poll() is not None,seconds=4)
    w.h.event('api_lifecycle_exit_observed',exit_status=p.returncode,
              tasks_finished=any(e['event']=='api_tasks_finished' for e in api_events(p)),
              panic='panicked' in p.error.read_text(),stderr_sha256=hashlib.sha256(p.error.read_bytes()).hexdigest())
    assert p.returncode==1 and 'panicked' not in p.error.read_text(),assertion
    assert any(e['event']=='api_tasks_finished' for e in api_events(p))
    with socket.socket() as s:assert s.connect_ex(('127.0.0.1',w.h.ports[4]))!=0


def idle_failure(w):
    # A malformed trusted adapter response ends the actual watcher, with no HTTP caller.
    p=w.api
    http(w.h.ports[6],'POST','/control',{'mode':'invalid_settlements'})
    try:
        # Original serve never observes completion until accept. Keep this a named
        # assertion, not an eventual-condition timeout masquerading as the regression.
        time.sleep(.6)
        stopped=p.poll() is not None or any(e['event']=='api_stopping' for e in api_events(p))
        w.h.event('idle_background_failure_observed',customer_connections=0,observed_stop=stopped,exit_status=p.poll())
        assert stopped,'background completion must stop readiness without customer traffic'
        clean_failure(w,p,'background failure must exit nonzero without panic or SIGABRT')
        assert 'funding watcher failed' in p.error.read_text() and 'rail observation array' in p.error.read_text()
    finally:http(w.h.ports[6],'POST','/control',{'mode':'normal'})
    w.launch_api()
    w.h.results.append(dict(test='W4-idle-background-task-failure',result='pass'))


def checkout_failure(w):
    from w4_regressions import funded
    t=funded(w,'housekeeping-deadline');p=w.api
    # Begin just after a completed housekeeping read, giving the held HTTP
    # request time to complete within the unchanged 1 s request drain bound.
    tick=w.h.proxy.arm('SELECT to_json(EXISTS(SELECT 1 FROM provisiond.enrolments','reply')
    tick.wait();tick.release()
    gate=w.h.proxy.arm("SELECT jsonb_build_object('balance_sats'",'reply')
    with concurrent.futures.ThreadPoolExecutor(max_workers=1) as pool:
        held=pool.submit(http,w.h.ports[4],'GET','/v1/balance',None,t['spending_token'])
        gate.wait();start=time.monotonic()
        try:
            # 1.6 s < 2 s call bound, > one housekeeping tick + actual 250 ms checkout.
            time.sleep(1.6)
        finally:gate.release()
        response=held.result(timeout=5)
    w.h.event('housekeeping_checkout_failure_observed',read_reply_held_seconds=time.monotonic()-start,
              held_response_status=response[0],checkout_ms=250,call_ms=2000,session=gate.connection['pid'])
    assert response[0]==200,'held real read must complete during bounded API draining'
    # On original source, the next connection wakes the stale-ready process and
    # demonstrates its panic. Corrected source already stopped without it.
    if not any(e['event']=='api_stopping' for e in api_events(p)) and p.poll() is None:
        try:http(w.h.ports[4],'OPTIONS','/v1/machines')
        except (OSError, HTTPException):pass
    clean_failure(w,p,'housekeeping checkout failure must exit nonzero without panic or SIGABRT')
    assert 'housekeeping failed' in p.error.read_text() and 'pool checkout' in p.error.read_text()
    w.launch_api()
    w.h.results.append(dict(test='W4-housekeeping-checkout-lifecycle',result='pass'))


def sent_commit_failure(w):
    t=w.enrol();p=w.api;events_before=len(api_events(p))
    gate=w.h.proxy.arm('COMMIT /* store COMMIT */','commit_reply')
    # A partial HTTP reader is another owned request task that cannot finish by itself.
    partial=socket.create_connection(('127.0.0.1',w.h.ports[4]));partial.sendall(b'GET /v1/')
    with concurrent.futures.ThreadPoolExecutor(max_workers=1) as pool:
        pending=pool.submit(http,w.h.ports[4],'POST','/v1/deposits',{'amount_sats':120},t['spending_token'],'lifecycle-held-commit')
        gate.wait();session=gate.connection['pid']
        http(w.h.ports[6],'POST','/control',{'mode':'invalid_settlements'})
        try:
            time.sleep(.6)
            assert p.poll() is None,'sent-COMMIT owner must remain alive until drain/exit boundary'
            assert not any(e.get('event')=='commit_owner_completed' and e.get('session')==session for e in api_events(p)[events_before:])
            assert not any(e.get('event')=='transport_closed' and e.get('session')==session for e in w.h.events)
            w.h.event('api_sent_commit_retained_observed',session=session,process_alive=True,
                      request_pending=not pending.done(),partial_request=True,commit_reply_withheld=True)
            if not any(e['event']=='api_stopping' for e in api_events(p)):
                try:http(w.h.ports[4],'OPTIONS','/v1/machines')
                except (OSError, HTTPException):pass
            eventually(lambda:p.poll() is not None or any(e['event']=='api_tasks_finished' for e in api_events(p)),seconds=4)
            if p.poll() is None:
                assert any(e['event']=='api_tasks_finished' and e['requests_drained'] is False for e in api_events(p))
                assert not any(e.get('event')=='commit_owner_completed' and e.get('session')==session for e in api_events(p)[events_before:])
                assert not any(e.get('event')=='transport_closed' and e.get('session')==session for e in w.h.events)
                w.h.event('api_sent_commit_retained_after_cancel',session=session,process_alive=True,
                          handlers_finished=True,commit_reply_withheld=True,checkout_retained=True)
            clean_failure(w,p,'sent-COMMIT lifecycle failure must exit nonzero without panic or SIGABRT')
            assert 'unresolved store work at process stop bound' in p.error.read_text()
            assert not any(e.get('event')=='commit_owner_completed' and e.get('session')==session for e in api_events(p)[events_before:])
        finally:
            gate.release();partial.close()
            http(w.h.ports[6],'POST','/control',{'mode':'normal'})
        try:pending.result(timeout=5)
        except (OSError, HTTPException):pass
    # The commit was durable with a withheld reply. Recovery is same-key HTTP replay.
    assert w.h.sql(f"SELECT count(*) FROM provisiond.deposits WHERE tenant_id='{t['tenant_id']}'")=='1'
    w.launch_api()
    d=w.request('POST','/v1/deposits',{'amount_sats':120},t,'lifecycle-held-commit')
    assert d['requested_sats']==120
    assert w.h.sql(f"SELECT count(*) FROM provisiond.deposits WHERE tenant_id='{t['tenant_id']}'")=='1'
    w.h.results.append(dict(test='W4-sent-COMMIT-and-pending-request-lifecycle',result='pass'))


CASES=dict(w4_underpayment=underpayment,w4_overpayment=overpayment,w4_operator_hiding=operator_hiding,
           w4_api_startup=startup,w4_api_idle=idle_failure,w4_api_checkout=checkout_failure,
           w4_api_commit=sent_commit_failure)
