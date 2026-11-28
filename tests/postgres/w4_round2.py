"""LDG-47, WIR-34, STO-7/55: HTTP regressions at the real adapter/owner boundaries."""
import concurrent.futures
from datetime import datetime, timezone
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
    for method,path in routes:
        code,_,headers=http(w.h.ports[4],'OPTIONS',path)
        assert code==404 and not any(k.startswith('Access-Control-') for k in headers)
    w.h.results.append(dict(test='W4-complete-operator-hiding',result='pass',customer_requests=len(routes)*4))


def api_events(p):
    return [json.loads(line) for line in p.log.read_text().splitlines() if line.startswith('{')]


def startup(w):
    # Default binary, no retry for negative startup. Schema-only needs no service values.
    w.stop_api()
    target=w.h.root/'default-startup-target'
    assert not target.exists(),'startup target must belong to this invocation'
    w.h.owned_extra_paths=getattr(w.h,'owned_extra_paths',[])+[target]
    w.h.run(['cargo','build','--bin','api','--locked','--target-dir',str(target)],timeout=120)
    executable=target/'debug/api'
    identity=w.build_identity(executable,[])
    w.h.config['workflow']['builds']['default_startup_api']=identity
    base=dict(w.h.env,**environment(w.h),PROVISIOND_LOCAL_PARAMETERS=str(w.parameters),
              PROVISIOND_API_LISTEN=f'127.0.0.1:{w.h.ports[4]}',
              PROVISIOND_RAIL_SIMULATOR=f'127.0.0.1:{w.h.ports[6]}')
    def run(name,env,args=()):
        w.h.release_port(w.h.ports[4])
        if w.operator_env:w.h.release_port(w.h.ports[7])
        log=w.h.root/(name+'.stdout');err=w.h.root/(name+'.stderr')
        with log.open('w') as out,err.open('w') as errors:
            p=subprocess.Popen([str(executable),*args],env=env,stdout=out,stderr=errors,text=True)
        p.name='api';p.log=log;p.error=err;w.children.append(p);w.h.extra.append(p)
        w.h.event('workflow_binary_started',component='api',case=name,**identity)
        eventually(lambda:p.poll() is not None or any(e['event']=='api_listening' for e in api_events(p)))
        if p.poll() is not None:
            if p.returncode!=0:
                from workflow import deployment_exit
                deployment_exit(w.h,'api',p,case=name,retry_eligible=False)
            w.reserve_api_ports()
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
    # SEC-3, pvr-r7ov: a token setting that is not valid UTF-8 is named and its value appears in
    # no diagnostic, also when token_env names a setting the API reads first. The word is
    # invented; it is no credential.
    authority=w.h.root/'startup-operator-authority.json'
    word=b'inventedstartupword'
    for label,setting,value in [('before','TEST_STARTUP_OPERATOR',b'\xff\xfe'+word),
            ('after','TEST_STARTUP_OPERATOR',word+b'\xff\xfe'),('both','TEST_STARTUP_OPERATOR',b'\xff'+word+b'\xfe'),
            ('shared-name','PROVISIOND_RAIL_SIMULATOR',b'\xff'+word+b'\xfe')]:
        authority.write_text(json.dumps([dict(principal='operator-a',token_env=setting,admin=True)]))
        env=dict(base,PROVISIOND_OPERATOR_LISTEN=f'127.0.0.1:{w.h.ports[7]}',
                 PROVISIOND_OPERATOR_AUTHORITY=str(authority),**{setting:value})
        p=run('startup-operator-token-'+label,env);p.wait(timeout=5)
        streams=p.log.read_bytes()+p.error.read_bytes()
        exposed=word in streams
        w.h.event('operator_token_setting_startup_observed',case=label,setting=setting,exit_status=p.returncode,value_exposed=exposed)
        assert p.returncode==1 and not exposed,'SEC-3 operator token setting value must not appear in a startup diagnostic'
        assert any(line.startswith(b'provisiond api: ') and line.endswith(setting.encode()+b' is not valid UTF-8 text')
                   for line in p.error.read_bytes().splitlines())
    env=dict(base)
    for var in ['PROVISIOND_LOCAL_PARAMETERS','PROVISIOND_API_LISTEN','PROVISIOND_RAIL_SIMULATOR']:env.pop(var,None)
    p=run('schema-only-without-service',env,['--check-schema']);assert p.wait(timeout=5)==0
    assert w.build_identity(w.h.target/'debug/api',w.builds['api']['features'])==w.builds['api']
    w.launch_api();w.request('OPTIONS','/v1/machines')
    w.h.results.append(dict(test='W4-required-startup-configuration',result='pass',default_binary=True,negative_cases=8,operator_token_cases=4))


def clean_failure(w,p,assertion):
    # No extra customer connection is made to wake the accept loop.
    eventually(lambda:p.poll() is not None or any(e['event']=='api_stopping' for e in api_events(p)),seconds=4)
    if p.poll() is None:eventually(lambda:p.poll() is not None,seconds=4)
    from workflow import exit_evidence
    w.h.event('api_lifecycle_exit_observed',**exit_evidence(p),
              tasks_finished=any(e['event']=='api_tasks_finished' for e in api_events(p)),
              panic='panicked' in p.error.read_text())
    w.reserve_api_ports()
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
    from w4_probe import completed_probe
    from test_w4_probe import probe_completion_regressions
    w.h.event('housekeeping_probe_checker_regressions',tests=probe_completion_regressions(),result='pass')
    t=funded(w,'housekeeping-deadline');p=w.api
    # STO-55, STO-7, OVR-18/19; D-70: a pre-transaction checkout failure
    # skips one tick. It does not change sent-COMMIT or fatal-task ownership.
    probe='SELECT to_json(EXISTS(SELECT 1 FROM provisiond.enrolments'
    tick=w.h.proxy.arm(probe,'reply')
    try:tick.wait()
    finally:tick.release()
    before=len(api_events(p))
    gate=w.h.proxy.arm("SELECT jsonb_build_object('balance_sats'",'reply')
    with concurrent.futures.ThreadPoolExecutor(max_workers=1) as pool:
        held=pool.submit(http,w.h.ports[4],'GET','/v1/balance',None,t['spending_token'])
        try:
            gate.wait();start=time.monotonic()
            try:
                skip=eventually(lambda:next((e for e in api_events(p)[before:]
                    if e.get('event')=='housekeeping_tick_skipped'
                    and e.get('stage')=='freeze_read'
                    and any('whole pool checkout deadline' in c or
                            'pool checkout/create/check: Timeout occurred while waiting for a slot' in c
                            for c in e.get('error_contexts',[]))),None),seconds=1.7)
            except AssertionError as error:
                raise AssertionError('held read must cause an actual housekeeping freeze_read checkout failure and skipped tick') from error
            elapsed=time.monotonic()-start
            assert .25<=elapsed<2,'checkout fault must occur within the held read call bound'
            assert not held.done() and not gate.released.is_set(),'customer read must still own the checkout at the skipped tick'
            assert any(e.get('event')=='pool_checkout_waiting' for e in api_events(p)[before:])
            assert p.poll() is None and w.api is p,'checkout failure must preserve the same API process'
            w.h.event('housekeeping_checkout_failure_observed',read_reply_held_seconds=elapsed,
                      checkout_ms=250,call_ms=2000,session=gate.connection['pid'],
                      api_pid=p.pid,process_alive=True,request_pending=True,skip=skip)
        finally:gate.release()
        response=held.result(timeout=5)
    assert response[0]==200,'held real read must complete after read capacity is restored'
    # STO-55: observe two actual later housekeeping reads on the restored
    # checkout, including their completed protocol replies, without restarting.
    resumed=[];cursor=len(w.h.events)
    for _ in range(2):
        # Own a new send before releasing it; a reply gate can catch preparation
        # or a trailing Z from the previous execution. Never reuse its cursor.
        marker=max(cursor,len(w.h.events))
        tick=w.h.proxy.arm(probe,'send')
        try:
            connection=tick.wait()
            session=connection['pid'];query=connection['query']
        finally:tick.release()
        completion=eventually(lambda:completed_probe(w.h.events,marker,session,query),seconds=2)
        cursor=completion['next_cursor']
        resumed.append(completion)
    healthy=http(w.h.ports[4],'GET','/v1/balance',None,t['spending_token'])
    assert healthy[0]==200 and healthy[1]==response[1]
    assert p.poll() is None and w.api is p,'housekeeping and customer reads must resume in the same API process'
    assert not any(e.get('event') in ('api_stopping','api_tasks_finished','api_background_task_failed','api_request_task_failed')
                   for e in api_events(p)[before:]),'skipped tick must not enter the fatal lifecycle'
    errors=p.error.read_text()
    assert not any(word in errors for word in ('panicked','housekeeping failed','fatal'))
    assert not any(e.get('event')=='alarm' and e.get('threshold_ms')==0
                   for e in (json.loads(line) for line in errors.splitlines() if line.startswith('{')))
    w.h.event('housekeeping_checkout_recovery_observed',api_pid=p.pid,process_alive=True,
              held_response_status=response[0],healthy_response_status=healthy[0],
              resumed_probe_sessions=[e['session'] for e in resumed],resumed_probe_executions=resumed,
              api_restarted=False,fatal_lifecycle=False)
    w.h.results.append(dict(test='W4-housekeeping-checkout-lifecycle',result='pass'))


def enrolment_retry(w):
    # STO-55/DOM-17, API-33/40, WIR-12: only a pre-BEGIN refusal leaves
    # this still-valid code usable. Successful enrolment has no replay receipt.
    w.stop_engine();w.restart_api();p=w.api
    h=w.h
    counts="SELECT (SELECT count(*) FROM provisiond.tenant_identities)||':'||(SELECT count(*) FROM provisiond.tenants)||':'||(SELECT count(*) FROM provisiond.enrolments)||':'||(SELECT count(*) FROM provisiond.idempotency_records)"
    before=[int(n) for n in h.sql(counts).split(':')]
    provider_before=w.external()['events']
    token=w.request('POST','/v1/enrol/token',{})
    htoken=token['admission_token'];w.private.append(htoken)
    expiry=datetime.fromisoformat(token['expires_at'].replace('Z','+00:00')).timestamp()
    # Deliberate whitespace and an ignored header survive the resend unchanged.
    payload=(' { "admission_token" : '+json.dumps(htoken)+' }\n').encode()
    key='ignored-enrolment-retry-header'
    invalid=w.request('POST','/v1/enrol',{'admission_token':'invalid','unexpected':1},status=400)
    assert invalid['error']['kind']=='invalid_request' and invalid['error']['details']=={}
    def begins():
        return sum(e['event']=='bytes_transmitted' and e.get('query','').startswith('BEGIN') for e in h.events)
    begin_before=begins();marker=len(h.events)
    # The restarted API has a warm read pool and a cold write pool. Withhold
    # its real admission-check reply, outside any transaction, past checkout.
    gate=h.proxy.arm('/* store admission target */','reply')
    with concurrent.futures.ThreadPoolExecutor(max_workers=1) as pool:
        pending=pool.submit(http,h.ports[4],'POST','/v1/enrol',None,None,key,payload)
        try:
            connection=gate.wait();session=connection['pid']
            code,value,headers=pending.result(timeout=5)
            h.event('enrolment_busy_reply_observed',status=code,error=value.get('error'),
                    session=session,api_pid=p.pid,body_sha256=hashlib.sha256(payload).hexdigest(),
                    begin_before=begin_before,begin_after=begins(),gate_activated=True)
            assert code==503,'STO-55 regression requires an actual pre-BEGIN enrolment busy 503'
            assert value['error']['kind']=='overloaded' and value['error']['retryable'] is True
            assert value['error']['details']=={'retry_after_ms':250}
            assert set(value)=={'error','correlation_id'} and set(value['error'])=={'kind','message','retryable','details'}
            assert headers['X-Correlation-Id']==value['correlation_id']
            assert headers['Retry-After']=='1'
            assert not gate.released.is_set() and begins()==begin_before,'busy enrolment must transmit no BEGIN'
            assert any(e['event']=='bytes_transmitted' and e.get('session')==session
                       and '/* store admission target */' in e.get('query','') for e in h.events[marker:])
            assert not any(e.get('event')=='session_admitted' and e.get('session')==session for e in api_events(p))
            assert [int(n) for n in h.sql(counts).split(':')]==before,'busy enrolment must create no account or receipt'
            assert w.external()['events']==provider_before,'busy enrolment must initiate no provider mutation'
            assert p.poll() is None and w.api is p
            refused_at=time.monotonic()
        finally:gate.release()
    delay=max(value['error']['details']['retry_after_ms'],int(headers['Retry-After'])*1000)/1000
    assert datetime.now(timezone.utc).timestamp()+delay<expiry,'regression requires a still-valid admission token'
    time.sleep(delay)
    elapsed=time.monotonic()-refused_at
    code,tenant,_=http(h.ports[4],'POST','/v1/enrol',key=key,raw=payload)
    h.event('enrolment_same_body_resend_observed',status=code,error=tenant.get('error'),
            wait_seconds=elapsed,minimum_wait_seconds=delay,token_unexpired=datetime.now(timezone.utc).timestamp()<expiry,
            body_sha256=hashlib.sha256(payload).hexdigest(),api_pid=p.pid)
    assert code==200,'STO-55/DOM-17: paced identical-body enrolment resend after pre-BEGIN busy must return 200'
    w.private.extend([tenant['spending_token'],tenant['recovery_credential']])
    assert elapsed>=delay and datetime.now(timezone.utc).timestamp()<expiry
    assert tenant['status']=='pending' and tenant['handle'] and tenant['tenant_id']
    assert tenant['spending_token'].startswith('pvd_s_') and tenant['recovery_credential'].startswith('pvd_r_')
    after=[before[0]+1,before[1]+1,before[2]+1,before[3]]
    assert [int(n) for n in h.sql(counts).split(':')]==after,'resend must create exactly one account without an idempotency receipt'
    assert h.sql(f"SELECT count(*) FROM provisiond.enrolments WHERE tenant_id='{tenant['tenant_id']}' AND handle='{tenant['handle']}'")=='1'
    for raw in [payload,json.dumps({'admission_token':htoken,'unexpected':1}).encode()]:
        code,reused,_=http(h.ports[4],'POST','/v1/enrol',key=key,raw=raw)
        assert code==400 and reused['error']==invalid['error'],'API-33/40, WIR-12: spent token must remain a generic 400 before field validation'
    assert [int(n) for n in h.sql(counts).split(':')]==after,'spent-code reuse must create no second account'
    assert w.external()['events']==provider_before and p.poll() is None and w.api is p
    h.event('enrolment_busy_recovery_observed',api_pid=p.pid,api_restarted=False,
            accounts_created=1,receipt_created=False,successful_code_reuse_status=400,
            invalid_token_precedes_unknown_field=True,counts_before=before,counts_after=after)
    h.results.append(dict(test='W4-enrolment-pre-BEGIN-busy-retry',result='pass'))


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
           w4_enrolment_retry=enrolment_retry,w4_api_commit=sent_commit_failure)
