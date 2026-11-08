"""WIR/API/MONEY/OPS acceptance via real HTTP, PG, and independent simulators."""
import concurrent.futures
import hashlib
from http.client import HTTPConnection
import json
from pathlib import Path
import socket
import subprocess
import time
from core import environment, binary
from scenarios import prepare
from harness import eventually

PARAMETERS=dict(activation_sats=100,deposit_seconds=3600,finality_seconds=600,tenant_seconds=10800,
 pending_limit=64,admission_delay_ms=50,admission_validity_seconds=60,admission_capacity=256,
 source_concurrency=2,rate_interval_ms=1000,read_limit=1000,write_limit=1000,created_limit=100,
 spend_limit=1000000,ceiling_seconds=3600,retention_seconds=3600,receipt_seconds=86400,
 listing_age_seconds=3600,lightning_floor_sats=1,onchain_floor_sats=20,confirmation_depth=2)


def http(port,method,path,body=None,token=None,key=None,raw=None,headers=None):
    conn=HTTPConnection('127.0.0.1',port,timeout=8)
    hs={'Content-Type':'application/json'}
    if token:hs['Authorization']='Bearer '+token
    if key:hs['Idempotency-Key']=key
    hs.update(headers or {})
    data=raw if raw is not None else json.dumps(body).encode() if body is not None else b''
    conn.request(method,path,body=data,headers=hs)
    response=conn.getresponse()
    result=(response.status,json.loads(response.read()),dict(response.getheaders()))
    conn.close()
    return result


class Workflow:
    def __init__(self,h,defect=None,default_build=False):
        self.defect=defect
        self.default_build=default_build
        self.h=h;self.children=[];self.engine=None;self.private=[]
        prepare(h)
        h.run(['cargo','build','--bins',*(['--features','store-test-hooks'] if not default_build else []),'--locked'],timeout=120)
        binary(h,'migrate',role='migrator')
        self.parameters=h.root/'local-parameters.json';self.parameters.write_text(json.dumps(PARAMETERS))
        self.control=h.root/'worker-control';self.control.mkdir()
        self.descriptor=dict(account='sim-a',kind='local_simulator',capabilities=['provision_virtual','list_offers','delete_machine'],
            channel='orders',channels=['orders'],offer_is_resource=False,temporary_keys=False,
            currency='USD',stop_cost=True,allow_orders=True,correlator='operation_id',margin_bps=1000,order_budget={'limit':20,'per_seconds':86400},cancellation_bound=time.strftime('%Y-%m-%d',time.gmtime()),
            meter_seconds=1,call_seconds=1,visibility_seconds=1,resolution_seconds=1,read_seconds=1,hold_seconds=1,margin_seconds=1,
            max_correlator_bytes=128,max_rate_outage_seconds=600)
        self.offer=dict(id='test-small',name='Nonpayable simulator virtual machine',kind='virtual',regions=['test'],
            recurring_price='0.01',setup_fee='0.02',currency='USD',period_seconds=3600,
            billing_unit=dict(seconds=3600,native_floor=None),install_strategies=[],cancellation_bound=None,
            traffic_bound='unlimited',attachments=[],sellable=True)
        self.rate=dict(currency='USD',minor_scale=2,rate_num=10,rate_den=1,margin_bps=1000,haircut_bps=0,
            source='simulator-fixed-rational',observed_at=time.strftime('%Y-%m-%dT%H:%M:%SZ',time.gmtime()))
        for account in ['sim-a','sim-b']:
            d=dict(self.descriptor,account=account)
            h.sql("INSERT INTO provisiond.provider_accounts(account,assignable,descriptor) VALUES('%s',true,'%s')"%(account,json.dumps(d)))
            h.sql("INSERT INTO provisiond.offer_listings VALUES('%s','%s',clock_timestamp())"%(account,json.dumps([self.offer])))
        h.sql("INSERT INTO provisiond.rates VALUES('USD','%s',true)"%json.dumps(self.rate))
        self.provider=self.simulator('provider',5)
        self.rail=self.simulator('rail',6)
        self.provider_control(offers={'sim-a':[self.offer],'sim-b':[self.offer]})
        h.config['workflow']=dict(parameters=PARAMETERS,descriptor=self.descriptor,offer=self.offer,rate=self.rate,listeners=dict(api=h.ports[4],provider=h.ports[5],rail=h.ports[6]),test_build_features=[] if default_build else ['store-test-hooks'])
        if default_build:h.config['engine']['runtime_build']['features']=[]
        h.release_port(h.ports[4])
        self.launch_api()
    def simulator(self,kind,offset):
        h=self.h;h.release_port(h.ports[offset]);root=h.root/kind;root.mkdir()
        log=(h.root/(kind+'.stdout')).open('w');err=(h.root/(kind+'.stderr')).open('w')
        p=subprocess.Popen(['python3','tests/postgres/simulator.py','--port',str(h.ports[offset]),'--root',str(root),'--kind',kind],env=h.env,stdout=log,stderr=err,text=True)
        log.close();err.close();h.extra.append(p);self.children.append(p)
        eventually(lambda:(h.root/(kind+'.stdout')).read_text())
        return p
    def start(self,name,extra=None):
        h=self.h
        if self.defect:extra=dict(extra or {},STORE_TEST_DEFECT=self.defect)
        count=sum(1 for c in self.children if getattr(c,'name',None)==name)
        log=h.root/f'{name}-{count}.stdout';err=h.root/f'{name}-{count}.stderr'
        with log.open('w') as out,err.open('w') as errors:
            p=subprocess.Popen([str(h.target/'debug'/name)],env=dict(h.env,**environment(h),**(extra or {})),stdout=out,stderr=errors,text=True)
        p.name=name;p.log=log;p.error=err;self.children.append(p);h.extra.append(p)
        return p
    def restart_api(self,parameters=None):
        if self.api.poll() is None:self.api.terminate();self.api.wait(timeout=5)
        self.parameters.write_text(json.dumps(parameters or PARAMETERS))
        self.h.event('api_parameters_applied',parameters=parameters or PARAMETERS)
        self.launch_api()
    def launch_api(self):
        # Each binary immediately fails a cold checkout refusal. Only fixture
        # deployment may make another recorded, read-only pre-listener attempt.
        for attempt in range(1,4):
            self.api=self.start('api',dict(PROVISIOND_API_LISTEN=f'127.0.0.1:{self.h.ports[4]}',PROVISIOND_RAIL_SIMULATOR=f'127.0.0.1:{self.h.ports[6]}',PROVISIOND_LOCAL_PARAMETERS=str(self.parameters),PROVISIOND_TEST_CONTROL=str(self.control)))
            eventually(lambda:'api_listening' in self.api.log.read_text() or self.api.poll() is not None)
            if self.api.poll() is None:return
            events=[json.loads(line) for line in self.api.log.read_text().splitlines() if line.startswith('{')]
            forbidden=any(e.get('event') in ['api_listening','external_entry'] or e.get('event','').startswith(('http_','rail_','funding_','invoice_')) for e in events)
            armed=any(not g.released.is_set() for g in self.h.proxy.gates)
            checkout_refusal='whole pool checkout deadline' in self.api.error.read_text()
            eligible=(self.api.returncode!=0 and checkout_refusal and not forbidden and not armed and self.defect is None)
            self.h.event('api_fixture_deployment_exit',attempt=attempt,exit_status=self.api.returncode,result='fail',pre_listener=not any(e.get('event')=='api_listening' for e in events),business_begin_count=sum(e.get('event')=='http_business_begin' for e in events),funding_or_external_events=sum(e.get('event')=='external_entry' or e.get('event','').startswith(('rail_','funding_','invoice_')) for e in events),armed_transport_fault=armed,checkout_refusal=checkout_refusal,retry_eligible=eligible,stdout_sha256=hashlib.sha256(self.api.log.read_bytes()).hexdigest(),stderr_sha256=hashlib.sha256(self.api.error.read_bytes()).hexdigest())
            assert eligible and attempt<3,'current API startup failed before readiness: '+self.api.error.read_text()
            self.h.event('api_fixture_deployment_repeated',failed_attempt=attempt,previous_process_exited=True,read_only_before_listener=True,checkout_ms=250)
    def start_engine(self):
        self.engine=self.start('engine',dict(PROVISIOND_TEST_CONTROL=str(self.control)))
        eventually(lambda:'restart_pass_complete' in self.engine.log.read_text() or self.engine.poll() is not None)
        assert self.engine.poll() is None,'current engine startup failed before readiness: '+self.engine.error.read_text()
        return self.engine
    def stop_engine(self):
        if self.engine and self.engine.poll() is None:self.engine.terminate();self.engine.wait(timeout=5)
        self.engine=None
    def events(self,name,event=None):
        result=[]
        for p in self.children:
            if getattr(p,'name',None)!=name:continue
            for line in p.log.read_text().splitlines():
                if line.startswith('{'):
                    e=json.loads(line)
                    if event is None or e.get('event')==event:result.append(e)
        return result
    def provider_control(self,**value):return http(self.h.ports[5],'POST','/control',value)[1]
    def external(self):return http(self.h.ports[5],'GET','/inspect')[1]
    def request(self,method,path,body=None,tenant=None,key=None,raw=None,status=200,headers=None):
        token=tenant['spending_token'] if isinstance(tenant,dict) else tenant
        code,value,hs=http(self.h.ports[4],method,path,body,token,key,raw,headers)
        assert code==status,(method,path,code,value)
        for k in ['X-Correlation-Id','Access-Control-Allow-Origin','Access-Control-Expose-Headers','Vary']:assert k in hs,(k,hs)
        if code>=400:assert set(value)=={'error','correlation_id'} and set(value['error'])=={'kind','message','retryable','details'}
        return value
    def enrol(self):
        token=self.request('POST','/v1/enrol/token',{})
        tenant=self.request('POST','/v1/enrol',{'admission_token':token['admission_token']},key='ignored-header')
        self.private.extend([token['admission_token'],tenant['spending_token'],tenant['recovery_credential']])
        return tenant
    def fund(self,tenant,suffix,assignment=True):
        deposit=self.request('POST','/v1/deposits',{'amount_sats':60},tenant,'deposit-'+suffix)
        assert deposit['lightning']['invoice'].startswith('TEST/SIMULATOR/NONPAYABLE/')
        assert deposit['onchain']['address'].startswith('TEST/SIMULATOR/NONPAYABLE/')
        payment_hash=self.h.sql(f"SELECT payment_hash FROM provisiond.deposits WHERE id='{deposit['id']}'")
        lightning=dict(rail='lightning',destination=deposit['lightning']['invoice'],payment_hash=payment_hash,amount_sats=60,settled=False,preimage_known=False,settled_at=time.strftime('%Y-%m-%dT%H:%M:%SZ',time.gmtime()))
        self.settlement(lightning)
        # Attribute a completed ingestion decision to this exact nonfinal value.
        self.processed(lightning,False)
        assert self.h.sql(f"SELECT count(*) FROM provisiond.payments WHERE deposit_id='{deposit['id']}'")=='0'
        assert self.h.sql(f"SELECT count(*) FROM provisiond.ledger_entries WHERE deposit_id='{deposit['id']}'")=='0'
        assert self.request('GET','/v1/enrol/'+tenant['handle'])=={'status':'pending'}
        lightning.update(settled=True,preimage_known=True);self.settlement(lightning);self.processed(lightning,True)
        eventually(lambda:self.request('GET','/v1/deposits/'+deposit['id'],tenant=tenant)['credited_sats']==60)
        assert self.request('GET','/v1/enrol/'+tenant['handle'])=={'status':'pending'}
        onchain=dict(rail='onchain',destination=deposit['onchain']['address'],txid=hashlib.sha256(suffix.encode()).hexdigest(),vout=0,confirmations=1,amount_sats=55,settled_at=lightning['settled_at'])
        self.settlement(onchain)
        self.processed(onchain,False)
        assert self.h.sql(f"SELECT count(*) FROM provisiond.payments WHERE deposit_id='{deposit['id']}'")=='1'
        assert self.h.sql(f"SELECT count(*) FROM provisiond.ledger_entries WHERE deposit_id='{deposit['id']}'")=='1'
        assert self.request('GET','/v1/enrol/'+tenant['handle'])=={'status':'pending'}
        assert self.h.sql(f"SELECT count(*) FROM provisiond.tenant_provider_accounts WHERE tenant_id='{tenant['tenant_id']}'")=='0'
        onchain['confirmations']=2;self.settlement(onchain);self.processed(onchain,True)
        eventually(lambda:self.request('GET','/v1/enrol/'+tenant['handle'])=={'status':'active'})
        assert self.request('GET','/v1/deposits/'+deposit['id'],tenant=tenant)['credited_sats']==115
        assert self.request('POST','/v1/deposits',{'amount_sats':60},tenant,'deposit-'+suffix)==deposit
        # Distinct concurrent deliveries of each same payment identity all reach
        # the common adapter, and still leave exactly one credit per identity.
        replays=[dict(value,event_id='replay-'+str(i)) for i,value in enumerate([lightning,lightning,onchain,onchain])]
        with concurrent.futures.ThreadPoolExecutor(max_workers=4) as pool:
            list(pool.map(self.settlement,replays))
        for value in replays:self.processed(value,True)
        assert self.h.sql(f"SELECT count(*) FROM provisiond.payments WHERE deposit_id='{deposit['id']}'")=='2'
        assert self.h.sql(f"SELECT count(*) FROM provisiond.ledger_entries WHERE deposit_id='{deposit['id']}'")=='2'
        providers=self.request('GET','/v1/providers',tenant=tenant)['providers']
        if assignment:tenant['account']=providers[0]['account']
        else:assert providers==[]
        # All exact ingestion decisions/replays above completed. Stop resending
        # this fixture's settled history during unrelated later fault windows.
        http(self.h.ports[6],'POST','/control',{'clear':True})
        return deposit,lightning,onchain
    def settlement(self,v):return http(self.h.ports[6],'POST','/control',{'settlement':v})
    def processed(self,v,credit_path):
        digest=hashlib.sha256(json.dumps(v,sort_keys=True,separators=(',',':'),ensure_ascii=False).encode()).hexdigest()
        eventually(lambda:any(e.get('observation_digest')==digest and e.get('credit_path')==credit_path for e in self.events('api','rail_observation_processed')))
        self.h.event('exact_rail_decision_observed',observation_digest=digest,rail=v['rail'],credit_path=credit_path)

    def create_body(self,t,**changes):
        return dict(provider_account=t['account'],offer='test-small',hostname='worker-test',region='test',image=None,
            ssh_public_keys=['ssh-ed25519 AAAAC3NzaC1lZDI1NTE5AAAAIAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA'],
            user_data=None,labels={},provider_options={},runway_seconds=3600,max_commitment_sats=100,acknowledge_purchase=True,**changes)
    def status(self,id):return self.h.sql(f"SELECT status FROM provisiond.operations WHERE id='{id}'")
    def wait_status(self,id,value):eventually(lambda:self.status(id)==value)
    def close(self):
        for p in reversed(self.children):
            if p.poll() is None:p.terminate()
            p.wait(timeout=5)
        assert all(p.poll() is not None for p in self.children)
        selected={'api_listening','http_business_begin','http_response','dispatch_step','dispatch_step_held','dispatch_license_acknowledged','provider_verified','execute_returned','settlement_finished','external_entry','rail_credit_acknowledged','rail_observation_processed','activation_without_healthy_assignment','whole_transaction_retry','generation_checked','worker_zero_row','destination_read','destination_exposed','token_request_held','token_issued','token_request_abandoned','funding_held'}
        for child in self.children:
            if getattr(child,'name',None) in ['api','engine']:
                trace=[json.loads(line) for line in child.log.read_text().splitlines() if line.startswith('{')]
                self.h.event('application_trace',component=child.name,path=str(child.log),sha256=hashlib.sha256(child.log.read_bytes()).hexdigest(),events=[e for e in trace if e.get('event') in selected])
        for kind in ['provider','rail']:
            path=self.h.root/kind/'effects.jsonl'
            self.h.event('independent_simulator_evidence',simulator_kind=kind,sha256=hashlib.sha256(path.read_bytes()).hexdigest() if path.exists() else None,records=[json.loads(s) for s in path.read_text().splitlines()] if path.exists() else [])
        self.h.event('workflow_children_reaped',count=len(self.children))


def workflow(h,case=None,defect=None):
    w=Workflow(h,defect='w4_ack' if case=='w4_default' else 'payment_atomic' if defect=='w4_atomic' else defect,default_build=case=='w4_default')
    try:
        from w4_regressions import CASES
        if case=='w4_regressions':
            for regression in CASES.values():regression(w)
            return
        if case in CASES:
            CASES[case](w)
            return
        if case=='w4_default':
            t=w.enrol();w.fund(t,'default-feature-tenant');licensing(w,t)
            assert not w.events('engine','defect_ack_boundary_bypassed')
            h.results.append(dict(test='W4-default-binaries-refuse-defect-environment',result='pass'))
            return
        if case=='w4_live_stop':
            live_provider_stop(w)
            return
        if case=='w4_forward':
            forwarded_validation(w)
            return
        if case=='w4_halt':
            halt_invoice(w)
            return
        if case=='w4_redaction':
            error_redaction(w)
            return
        if case=='w4_record':
            admission_field_repetition(w)
            return
        if case=='w4_query':
            query_leniency(w)
            return
        if case=='w4_search_error':
            accepted_verification_error(w)
            return
        if case=='w4_atomic':
            payment_atomic(w)
            return
        if case in ['w4_ack','w4_overload']:
            t=w.enrol();w.fund(t,'control-tenant')
            (licensing if case=='w4_ack' else overload)(w,t)
            return
        t=w.enrol();other=w.enrol()
        assert w.request('GET','/v1/balance',tenant=t,status=403)['error']['kind']=='not_activated'
        deposit,ln,chain=w.fund(t,'tenant-one');w.fund(other,'tenant-two')
        assert t['account']!=other['account']
        body=w.create_body(t)
        admitted=w.request('POST','/v1/machines',body,t,'workflow-create-1',status=202)
        op=admitted['id'];assert admitted['status']=='queued' and admitted['kind']=='create_machine'
        balance=w.request('GET','/v1/balance?commitments=true',tenant=t)
        assert balance['committed_sats']==43 and balance['available_sats']==72
        assert balance['commitments'][0]['machine_id'] is None and balance['commitments'][0]['operation_id']==op
        assert h.sql(f"SELECT count(*) FROM provisiond.idempotency_records WHERE resource_id='{op}'")=='1'
        w.start_engine();w.wait_status(op,'succeeded')
        view=w.request('GET','/v1/operations/'+op,tenant=t);machine=w.request('GET','/v1/machines/'+view['machine_id'],tenant=t)
        assert view['terminal'] and not view['retryable'] and 'request' not in view and view['result']=={'machine_id':machine['id']}
        assert machine['install_strategies']==[] and 'external_id' not in machine and 'metadata' not in machine
        assert machine['state_observed_at'] and machine['runway_until']
        assert h.sql(f"SELECT billing_unit_seconds||':'||(first_unit_debit_id IS NOT NULL) FROM provisiond.machines WHERE id='{machine['id']}'")=='3600:true'
        balance=w.request('GET','/v1/balance',tenant=t)
        assert balance['balance_sats']==84 and balance['committed_sats']==12 and balance['available_sats']==72
        canonical=json.dumps(dict(reversed(list(body.items()))),indent=3).encode()
        replay=w.request('POST','/v1/machines',tenant=t,key='workflow-create-1',raw=canonical,status=202)
        assert replay['id']==op and h.sql(f"SELECT request IS NULL FROM provisiond.operations WHERE id='{op}'")=='t'
        assert len([e for e in w.external()['events'] if e['stage']=='accepted'])==1
        assert h.sql(f"SELECT count(*) FROM provisiond.payments WHERE deposit_id='{deposit['id']}'")=='2'
        for route in ['/v1/machines/'+machine['id'],'/v1/operations/'+op,'/v1/deposits/'+deposit['id']]:assert w.request('GET',route,tenant=other,status=404)['error']['kind']=='not_found'
        steps=[e['step'] for e in w.events('engine','dispatch_step') if e['operation']==op]
        assert steps==list(range(1,10)),steps
        stages=[e['stage'] for e in w.external()['events'] if e.get('command')==op and e.get('path')=='/order']
        assert stages==['request_received','accepted','resource_created','response_returned'],stages
        h.event('fresh_http_workflow_observed',tenant_count=2,payments=2,credited_sats=115,operation=op,machine=machine['id'],commitment_sats=43,setup_debit=20,unit_debit=11,remaining_commitment=12,steps=steps,external_order_stages=stages)
        h.results.append(dict(test='W4-fresh-HTTP-workflow',result='pass'))
        wire_auth(w,t,other,op,body)
        dropped_reply(w,t)
        overload(w,t)
        worker_refusals(w,other)
        page=w.request('GET','/v1/operations?limit=1',tenant=other)
        assert page['next_cursor']
        assert w.request('GET','/v1/operations?limit=1&cursor='+page['next_cursor'],tenant=t,status=400)['error']['kind']=='invalid_request'
        # Fresh funding retains the acceptance rule while providing independent test budgets.
        extra=w.enrol();w.fund(extra,'licensing-tenant');w.provider_control(offers={'sim-a':[w.offer],'sim-b':[w.offer]});licensing(w,extra)
        settlement_loss(w,extra)
        destinations(w,t,deposit)
        payment_atomic(w)
        accepted_verification_error(w)
        query_leniency(w)
        admission_field_repetition(w)
        guards=w.enrol();w.fund(guards,'guard-tenant');licensing_guards_and_crash(w,guards)
        tail=w.enrol();w.fund(tail,'tail-tenant');admission_refusals(w,tail)
        temporary_keys(w)
        concurrency_and_resource(w)
        unavailable_activation(w)
        lost_ack_refusals(w)
        ceiling_restart(w)
        token_limits_and_expiry(w)
        error_redaction(w)
        halt_invoice(w)
        forwarded_validation(w)
        live_provider_stop(w)
        for regression in CASES.values():regression(w)
    finally:w.close()


def wire_auth(w,t,other,op,body):
    # API-7: auth and pending precede even malformed JSON/key validation.
    assert w.request('POST','/v1/machines',raw=b'{',status=401)['error']['kind']=='authentication'
    assert w.request('GET','/v1/balance',tenant=t['recovery_credential'],status=401)['error']['kind']=='authentication'
    bad=w.request('POST','/v1/enrol',{'admission_token':'invalid','unexpected':1},status=400)
    assert bad['error']['details']=={}
    for raw in [b'{"x":{"n":1,"n":2}}',b'{"amount_sats":1.0}',b'{"amount_sats":1e1}',b'{"amount_sats":9007199254740992}',b'\xff']:
        w.request('POST','/v1/deposits',tenant=t,key='strict-json-test',raw=raw,status=400)
    value=dict(body,acknowledge_purchase=False,z_unknown=True)
    assert w.request('POST','/v1/machines',value,t,'least-pointer-test',status=400)['error']['details']['field']=='/acknowledge_purchase'
    w.request('OPTIONS','/v1/machines',status=200)
    first=w.request('POST','/v1/deposits',{'amount_sats':5},t,'principal-collision-key');second=w.request('POST','/v1/deposits',{'amount_sats':6},other,'principal-collision-key')
    assert first['id']!=second['id']
    # Ignore customer tenant override. Assignment remains the bearer principal's.
    assert w.request('GET','/v1/providers',tenant=t,headers={'X-Provisiond-Tenant':other['tenant_id']})['providers'][0]['account']==t['account']
    w.h.sql(f"UPDATE provisiond.tenants SET status='suspended' WHERE id='{t['tenant_id']}'")
    assert w.request('POST','/v1/machines',body,t,'workflow-create-1',status=202)['id']==op
    assert w.request('POST','/v1/machines',body,t,'suspended-new-key',status=403)['error']['kind']=='suspended'
    assert w.request('POST','/v1/machines',value,t,'suspended-invalid',status=400)['error']['kind']=='invalid_request'
    w.h.sql(f"UPDATE provisiond.tenants SET status='active' WHERE id='{t['tenant_id']}'")
    assert w.request('POST','/v1/machines?changed=1',body,t,'workflow-create-1',status=409)['error']['details']['reason']=='idempotency_mismatch'
    value=dict(body,hostname='different')
    assert w.request('POST','/v1/machines',value,t,'workflow-create-1',status=409)['error']['details']['reason']=='idempotency_mismatch'
    hashes=w.h.sql('SELECT credential_digest||recovery_digest FROM provisiond.tenants')
    assert t['spending_token'] not in hashes and t['recovery_credential'] not in hashes
    w.h.event('wire_auth_observed',authentication_before_body=True,suspended_replay=True,strict_nested_json=True,override_ignored=True,changed_target_and_body_conflict=True,secrets_hashed_only=True)
    w.h.results.append(dict(test='W4-wire-auth',result='pass'))


def dropped_reply(w,t):
    w.provider_control(mode='drop_reply')
    op=w.request('POST','/v1/machines',w.create_body(t),t,'dropped-reply-create',status=202)['id']
    w.wait_status(op,'needs_reconciliation')
    view=w.request('GET','/v1/operations/'+op,tenant=t)
    assert view['retryable'] is False and view['error']['retryable'] is False
    assert w.h.sql(f"SELECT reserved_sats||':'||state FROM provisiond.commitments WHERE operation_id='{op}'")=='43:open'
    assert w.h.sql(f"SELECT count(*) FROM provisiond.ledger_entries WHERE operation_id='{op}'")=='0'
    before=[e for e in w.external()['events'] if e.get('command')==op]
    assert [e['stage'] for e in before if e.get('path')=='/order']==['request_received','accepted','resource_created','reply_dropped']
    w.stop_engine();w.start_engine()
    replay=w.request('POST','/v1/machines',w.create_body(t),t,'dropped-reply-create',status=202)
    assert replay['id']==op and replay['status']=='needs_reconciliation'
    assert len([e for e in w.external()['events'] if e.get('command')==op and e['stage']=='accepted'])==1
    w.provider_control(mode='normal')
    w.h.event('dropped_reply_observed',operation=op,external_acceptance=True,external_resource=True,response_returned=False,effects_survived_restart=True,second_purchase=False,automatic_reconciliation='not run')
    w.h.results.append(dict(test='W4-dropped-reply',result='pass',limits='automatic reconciliation not run'))


def overload(w,t):
    # CNF-299: a new key plus lost first reply -> refused old-key resend -> same operation.
    from w4_regressions import funded
    w.stop_engine();t=funded(w,'overload-isolated');body=w.create_body(t)
    http(w.h.ports[6],'POST','/control',{'clear':True})
    hold=w.control/'hold-funding';hold.touch();eventually(lambda:w.events('api','funding_held'))
    gate=w.h.proxy.arm('store COMMIT','commit_reply')
    with concurrent.futures.ThreadPoolExecutor(max_workers=2) as pool:
        pending=pool.submit(http,w.h.ports[4],'POST','/v1/machines',body,t['spending_token'],'overload-old-key')
        connection=gate.wait()
        op=w.h.sql(f"SELECT resource_id FROM provisiond.idempotency_records WHERE scope_id='{t['tenant_id']}' AND key='overload-old-key'")
        assert op and w.status(op)=='queued'
        marker='overload-never-used';digest=hashlib.sha256(marker.encode()).hexdigest()
        before=len([e for e in w.events('api','http_business_begin') if e.get('key_digest')==digest])
        mutations=len([e for e in w.external()['events'] if e['stage']=='accepted'])
        transmitted_begins=lambda:sum(e['event']=='bytes_transmitted' and e.get('query','').startswith('BEGIN') for e in w.h.events)
        new_begin_before=transmitted_begins()
        refusal=w.request('POST','/v1/deposits',{'amount_sats':62},t,marker,status=503)
        assert refusal['error']['kind']=='overloaded' and refusal['error']['retryable'] and refusal['error']['details']['retry_after_ms']==250
        after=len([e for e in w.events('api','http_business_begin') if e.get('key_digest')==digest])
        new_begin_after=transmitted_begins()
        w.h.event('defect_overload_entry_observed',business_entries=after-before)
        assert before==after==0 and new_begin_before==new_begin_after,'overload must precede request business BEGIN'
        assert w.h.sql(f"SELECT count(*) FROM provisiond.idempotency_records WHERE scope_id='{t['tenant_id']}' AND key='{marker}'")=='0'
        w.request('GET','/v1/balance',tenant=t) # separately unexhausted read budget
        w.h.proxy.close_connection(connection,'lost_http_commit_reply');gate.release()
        failed_status,failed,_=pending.result(timeout=8)
        assert failed_status==500 and failed['error']['kind']=='internal'
        w.h.event('old_key_first_reply_lost',operation=op,observed_http_status=failed_status)
        # Saturate the read checkout used by authentication/receipt discovery only
        # after the first caller has observed its lost acknowledgement.
        read_gate=w.h.proxy.arm("SELECT jsonb_build_object('balance_sats'",'reply')
        reading=pool.submit(http,w.h.ports[4],'GET','/v1/balance',None,t['spending_token'])
        read_gate.wait()
        snapshot_sql=f"SELECT jsonb_build_object('operations',(SELECT count(*) FROM provisiond.operations WHERE tenant_id='{t['tenant_id']}'),'commitments',(SELECT count(*) FROM provisiond.commitments WHERE tenant_id='{t['tenant_id']}'),'receipts',(SELECT count(*) FROM provisiond.idempotency_records WHERE scope_id='{t['tenant_id']}'),'reserved',(SELECT sum(reserved_sats) FROM provisiond.commitments WHERE tenant_id='{t['tenant_id']}'),'money',(SELECT sum(amount_sats) FROM provisiond.ledger_entries WHERE tenant_id='{t['tenant_id']}'))::text"
        snapshot=w.h.sql(snapshot_sql);begins=len(w.events('api','http_business_begin'));old_begin_before=transmitted_begins()
        refusal=w.request('POST','/v1/machines',body,t,'overload-old-key',status=503)
        assert refusal['error']['kind']=='overloaded' and refusal['error']['details']['retry_after_ms']==250
        old_begin_after=transmitted_begins()
        assert len(w.events('api','http_business_begin'))==begins and w.h.sql(snapshot_sql)==snapshot and old_begin_after==old_begin_before
        assert len([e for e in w.external()['events'] if e['stage']=='accepted'])==mutations
        read_gate.release();assert reading.result(timeout=8)[0]==200
    old=w.request('POST','/v1/machines',body,t,'overload-old-key',status=202)
    assert old['id']==op
    w.request('POST','/v1/deposits',{'amount_sats':62},t,marker)
    assert w.h.sql(f"SELECT count(*) FROM provisiond.operations WHERE tenant_id='{t['tenant_id']}'")=='1'
    assert w.h.sql(f"SELECT count(*) FROM provisiond.idempotency_records WHERE scope_id='{t['tenant_id']}' AND key='overload-old-key'")=='1'
    hold.unlink()
    w.h.event('http_overload_observed',actual_sent_commit_held=True,new_key_business_begin=False,new_receipt=False,separate_read_budget=True,old_first_reply_lost_before_refusal=True,old_refused_before_business_begin=True,old_receipt_recovered=True,operation=op,provider_mutations=0,new_key_actual_begin_counts=[new_begin_before,new_begin_after],old_key_actual_begin_counts=[old_begin_before,old_begin_after])
    w.h.results.append(dict(test='W4-overload-new-and-lost-reply-old-key',result='pass'))


def worker_refusals(w,t):
    # OPS-43: first-refusal order is observed at real driver entries, not a mock return.
    h=w.h;w.stop_engine();account=t['account'];original=dict(w.descriptor,account=account)
    cases=[('account_first',1,'conflict','state'),('tenant',1,'conflict','tenant_suspended'),
        ('channel',2,'rate_limited',None),('caller',3,'invalid_request',None),('record',3,'conflict','incomplete_record'),
        ('ordering_capability',3,'unsupported',None),('listing_capability',4,'unsupported',None),
        ('absent',5,'invalid_request','offer_not_sellable'),('kind',5,'invalid_request','offer_not_sellable'),
        ('unsellable',5,'invalid_request','offer_not_sellable'),('native_price',6,'conflict','price_moved'),
        ('driver_caller',7,'invalid_request',None),('driver_system',7,'internal',None)]
    for name,last,kind,reason in cases:
        body=w.create_body(t)
        if name=='driver_caller':body['provider_options']={'location':'bad'}
        op=w.request('POST','/v1/machines',body,t,'worker-refusal-'+name,status=202)['id']
        descriptor=dict(original);current=dict(w.offer)
        if name=='account_first':h.sql(f"UPDATE provisiond.provider_account_status SET status='account_unreachable' WHERE provider_account='{account}'")
        if name in ['account_first','tenant']:h.sql(f"UPDATE provisiond.tenants SET status='suspended' WHERE id='{t['tenant_id']}'")
        if name=='channel':h.sql(f"INSERT INTO provisiond.channel_closures VALUES('{account}','orders',clock_timestamp()+interval '1 hour')");current['sellable']=False
        if name=='caller':h.sql(f"UPDATE provisiond.operations SET request=jsonb_set(request,'{{acknowledge_purchase}}','false') WHERE id='{op}'")
        if name=='record':h.sql(f"UPDATE provisiond.operations SET request_summary=request_summary #- '{{offer,machine_kind}}' WHERE id='{op}'")
        if name=='ordering_capability':descriptor['capabilities']=['list_offers','delete_machine']
        if name=='listing_capability':descriptor['capabilities']=['provision_virtual','delete_machine']
        if name=='kind':current['kind']='bare_metal'
        if name=='unsellable':current['sellable']=False
        if name=='native_price':current['setup_fee']='0.03'
        if name=='driver_system':descriptor['max_correlator_bytes']=1
        h.sql("UPDATE provisiond.provider_accounts SET descriptor='%s' WHERE account='%s'"%(json.dumps(descriptor),account))
        w.provider_control(offers={account:[] if name=='absent' else [current]})
        w.start_engine();w.wait_status(op,'failed');w.stop_engine()
        view=w.request('GET','/v1/operations/'+op,tenant=t)
        assert view['error']['kind']==kind,(name,view)
        if reason:assert view['error']['details']['reason']==reason,(name,view)
        assert 'refusals' not in view['error']['details']
        assert h.sql(f"SELECT reserved_sats||':'||state FROM provisiond.commitments WHERE operation_id='{op}'")=='0:closed'
        assert h.sql(f"SELECT count(*) FROM provisiond.ledger_entries WHERE operation_id='{op}'")=='0'
        steps=[e['step'] for e in w.events('engine','dispatch_step') if e['operation']==op]
        assert steps==list(range(1,last+1)),(name,steps)
        requests=[e for e in w.external()['events'] if e.get('command')==op and e['stage']=='request_received']
        assert [e['path'] for e in requests]==([] if last<5 else ['/offers']),(name,requests)
        h.event('worker_first_refusal_observed',case=name,steps=steps,error_kind=kind,external_requests=[e['path'] for e in requests])
        h.sql(f"UPDATE provisiond.provider_account_status SET status='healthy' WHERE provider_account='{account}'")
        h.sql(f"UPDATE provisiond.tenants SET status='active' WHERE id='{t['tenant_id']}'")
        h.sql('DELETE FROM provisiond.channel_closures')
        h.sql("UPDATE provisiond.provider_accounts SET descriptor='%s' WHERE account='%s'"%(json.dumps(original),account))
        w.provider_control(offers={account:[w.offer]})
    # An exchange-only move changes future runway, never the accepted native price.
    op=w.request('POST','/v1/machines',w.create_body(t),t,'rate-only-create',status=202)['id']
    moved=dict(w.rate,rate_num=100);h.sql("UPDATE provisiond.rates SET value='%s' WHERE currency='USD'"%json.dumps(moved))
    w.start_engine();w.wait_status(op,'succeeded');w.stop_engine()
    assert h.sql(f"SELECT sum(-amount_sats) FROM provisiond.ledger_entries WHERE operation_id='{op}'")=='31'
    h.sql("UPDATE provisiond.rates SET value='%s' WHERE currency='USD'"%json.dumps(w.rate))
    # Zero first-unit obligation still copies positive billing seconds and seeds at now.
    zero=dict(w.offer,recurring_price='0.00')
    h.sql("UPDATE provisiond.offer_listings SET offers='%s',read_at=clock_timestamp() WHERE provider_account='%s'"%(json.dumps([zero]),account))
    w.provider_control(offers={account:[zero]})
    op=w.request('POST','/v1/machines',w.create_body(t),t,'zero-unit-create',status=202)['id']
    w.start_engine();w.wait_status(op,'succeeded');w.stop_engine()
    copied=h.sql(f"SELECT m.billing_unit_seconds||':'||(m.first_unit_debit_id IS NULL)||':'||(m.meter_seed_at=m.state_observed_at) FROM provisiond.machines m JOIN provisiond.operations o ON o.machine_id=m.id WHERE o.id='{op}'")
    assert copied=='3600:true:true','zero first unit must retain billing duration without a debit'
    h.event('zero_unit_observed',billing_seconds=3600,unit_debit=False,seed_at_observation=True)
    h.sql("UPDATE provisiond.offer_listings SET offers='%s',read_at=clock_timestamp() WHERE provider_account='%s'"%(json.dumps([w.offer]),account))
    w.provider_control(offers={account:[w.offer]})
    h.results.append(dict(test='W4-worker-first-refusals-and-zero-unit',result='pass',cases=len(cases)))


def licensing(w,t):
    h=w.h;w.stop_engine()
    op=w.request('POST','/v1/machines',w.create_body(t),t,'licensing-ack-test',status=202)['id']
    gate=h.proxy.arm('SET correlator_kind=COALESCE','send')
    w.start_engine();connection=gate.wait()
    reply=h.proxy.arm('store COMMIT','commit_reply',session=connection['pid']);gate.release();reply.wait()
    assert h.sql(f"SELECT about_to_order_at IS NOT NULL FROM provisiond.operations WHERE id='{op}'")=='t'
    marker=h.sql(f"SELECT correlator_value||':'||about_to_order_at||':'||revision FROM provisiond.operations WHERE id='{op}'")
    h.event('defect_ack_boundary_observed',operation=op,mutation_before_ack=bool([e for e in w.external()['events'] if e.get('command')==op and e.get('path') in ['/keys','/order']]))
    assert not [e for e in w.external()['events'] if e.get('command')==op and e.get('path') in ['/keys','/order']],'step 8 must be acknowledged before provider mutation'
    # Lose only acknowledgement. Whole-transaction repeat must preserve the committed marker.
    h.proxy.close_connection(connection,'lost_step8_acknowledgement');reply.release()
    w.wait_status(op,'succeeded');w.stop_engine()
    assert h.sql(f"SELECT correlator_value||':'||about_to_order_at||':'||(revision-1) FROM provisiond.operations WHERE id='{op}'")==marker
    assert len([e for e in w.external()['events'] if e.get('command')==op and e['stage']=='accepted'])==1
    assert w.events('engine','whole_transaction_retry')
    h.event('step8_acknowledgement_observed',operation=op,markers_durable_before_ack=True,mutation_before_ack=False,whole_repeat_preserved_marker=True,orders=1)
    h.results.append(dict(test='W4-step8-acknowledgement',result='pass'))


def settlement_loss(w,t):
    h=w.h;w.stop_engine();w.provider_control(offers={'sim-a':[w.offer],'sim-b':[w.offer]})
    op=w.request('POST','/v1/machines',w.create_body(t),t,'settlement-reply-loss',status=202)['id']
    statement=h.proxy.arm('SET status=$3,result=','send');w.start_engine();connection=statement.wait()
    reply=h.proxy.arm('store COMMIT','commit_reply',session=connection['pid']);statement.release();reply.wait()
    assert w.status(op)=='succeeded'
    before=h.sql(f"SELECT o.revision||':'||o.machine_id||':'||c.reserved_sats||':'||(SELECT count(*) FROM provisiond.ledger_entries WHERE operation_id=o.id) FROM provisiond.operations o JOIN provisiond.commitments c ON c.id=o.commitment_id WHERE o.id='{op}'")
    calls=len([e for e in w.external()['events'] if e.get('command')==op])
    h.proxy.close_connection(connection,'lost_verified_settlement_reply');reply.release()
    eventually(lambda:any(e.get('operation')==op for e in w.events('engine','settlement_finished')))
    w.stop_engine()
    after=h.sql(f"SELECT o.revision||':'||o.machine_id||':'||c.reserved_sats||':'||(SELECT count(*) FROM provisiond.ledger_entries WHERE operation_id=o.id) FROM provisiond.operations o JOIN provisiond.commitments c ON c.id=o.commitment_id WHERE o.id='{op}'")
    assert before==after and calls==len([e for e in w.external()['events'] if e.get('command')==op])
    h.event('verified_settlement_reply_loss_observed',operation=op,machine_money_revision_stable=True,provider_calls_repeated=False)
    h.results.append(dict(test='W4-verified-settlement-reply-loss',result='pass'))


def destinations(w,t,deposit):
    h=w.h;hold=w.control/'hold-funding';hold.touch();eventually(lambda:w.events('api','funding_held'))
    for suffix in ['new','read','replay']:
        # Every selected destination is read before sampling a new flush target.
        gate=h.proxy.arm('store post_read target','reply')
        with concurrent.futures.ThreadPoolExecutor(max_workers=1) as pool:
            if suffix=='read':future=pool.submit(w.request,'GET','/v1/deposits/'+deposit['id'],None,t)
            else:future=pool.submit(w.request,'POST','/v1/deposits',{'amount_sats':63},t,'postread-key-test')
            c=gate.wait();pid=c['pid']
            assert any(e.get('session')==pid for e in w.events('api','destination_read'))
            assert not future.done()
            # Discard the selected value on loss of this exact physical session.
            h.proxy.close_connection(c,'lost_http_postread_session');gate.release()
            result=future.result(timeout=8)
            assert result['id']
            assert any(e.get('attempt')==2 and e.get('session')!=pid for e in w.events('api','destination_exposed'))
        h.event('http_destination_check_observed',path=suffix,held_after_read=True,response_withheld=True,lost_session_repeated_entire_read=True)
    hold.unlink();h.results.append(dict(test='W4-HTTP-destination-checks',result='pass',arms=['new','read','replay']))


def payment_atomic(w):
    t=w.enrol();deposit=w.request('POST','/v1/deposits',{'amount_sats':60},t,'atomic-rail-credit')
    payment_hash=w.h.sql(f"SELECT payment_hash FROM provisiond.deposits WHERE id='{deposit['id']}'")
    w.settlement(dict(rail='lightning',destination=deposit['lightning']['invoice'],payment_hash=payment_hash,amount_sats=60,settled=True,preimage_known=True,settled_at=time.strftime('%Y-%m-%dT%H:%M:%SZ',time.gmtime())))
    eventually(lambda:any(e.get('deposit')==deposit['id'] for e in w.events('api','rail_credit_acknowledged')))
    counts=w.h.sql(f"SELECT (SELECT count(*) FROM provisiond.payments WHERE deposit_id='{deposit['id']}')||':'||(SELECT count(*) FROM provisiond.ledger_entries WHERE deposit_id='{deposit['id']}')")
    w.h.event('w4_payment_atomicity_observed',counts=counts,adapter_acknowledged=True)
    assert counts=='1:1','rail payment and credit must be atomic'
    w.h.results.append(dict(test='W4-payment-atomicity',result='pass'))


def licensing_guards_and_crash(w,t):
    h=w.h;account=t['account'];w.stop_engine();w.provider_control(offers={'sim-a':[w.offer],'sim-b':[w.offer]})
    for target in ['account','tenant']:
        op=w.request('POST','/v1/machines',w.create_body(t),t,'contended-guard-'+target,status=202)['id']
        hold=w.control/'hold-step-8';hold.touch();w.start_engine()
        eventually(lambda:any(e.get('operation')==op and e['step']==8 for e in w.events('engine','dispatch_step_held')))
        log=(h.root/('row-holder-'+target+'.stdout')).open('w');err=(h.root/('row-holder-'+target+'.stderr')).open('w')
        p=subprocess.Popen(['psql','-XAt','-v','ON_ERROR_STOP=1','-h','127.0.0.1','-p',str(h.ports[0]),'-U','administrator','-d','postgres'],stdin=subprocess.PIPE,stdout=log,stderr=err,text=True,env=h.env)
        log.close();err.close();h.extra.append(p)
        update=f"UPDATE provisiond.provider_account_status SET status='account_unreachable' WHERE provider_account='{account}'" if target=='account' else f"UPDATE provisiond.tenants SET status='suspended' WHERE id='{t['tenant_id']}'"
        p.stdin.write('BEGIN; '+update+'; SELECT 9812;\n');p.stdin.flush()
        eventually(lambda:'9812' in (h.root/('row-holder-'+target+'.stdout')).read_text())
        hold.unlink()
        eventually(lambda:int(h.sql("SELECT count(*) FROM pg_stat_activity WHERE wait_event_type='Lock' AND (query LIKE 'SELECT status FROM provisiond.provider_account_status%' OR query LIKE 'UPDATE provisiond.tenants SET status=status%')"))>=1)
        assert not [e for e in w.external()['events'] if e.get('command')==op and e.get('path') in ['/keys','/order']]
        p.stdin.write('COMMIT;\n');p.stdin.close();assert p.wait(timeout=5)==0
        w.wait_status(op,'failed');w.stop_engine()
        assert h.sql(f"SELECT about_to_order_at IS NULL AND correlator_value IS NULL FROM provisiond.operations WHERE id='{op}'")=='t'
        assert h.sql(f"SELECT reserved_sats||':'||state FROM provisiond.commitments WHERE operation_id='{op}'")=='0:closed'
        h.sql(f"UPDATE provisiond.provider_account_status SET status='healthy' WHERE provider_account='{account}'")
        h.sql(f"UPDATE provisiond.tenants SET status='active' WHERE id='{t['tenant_id']}'")
        h.event('step8_contended_guard_observed',target_type=target,wait_event='Lock',marker_rolled_back=True,mutation=False)
    for mode,step in [('worker_failure',9),('crash_null',8),('crash_set',9)]:
        op=w.request('POST','/v1/machines',w.create_body(t),t,'worker-crash-'+mode,status=202)['id']
        if mode=='worker_failure':
            failure=w.control/'fail-step-9';failure.touch();w.start_engine();w.wait_status(op,'failed');w.stop_engine();failure.unlink()
            assert h.sql(f"SELECT about_to_order_at IS NOT NULL FROM provisiond.operations WHERE id='{op}'")=='t'
        else:
            hold=w.control/f'hold-step-{step}';hold.touch();w.start_engine()
            eventually(lambda:any(e.get('operation')==op and e['step']==step for e in w.events('engine','dispatch_step_held')))
            w.stop_engine();hold.unlink();w.start_engine();w.wait_status(op,'failed' if mode=='crash_null' else 'needs_reconciliation');w.stop_engine()
        assert h.sql(f"SELECT count(*) FROM provisiond.ledger_entries WHERE operation_id='{op}'")=='0'
        assert not [e for e in w.external()['events'] if e.get('command')==op and e.get('path') in ['/keys','/order']]
        h.event('worker_crash_distinction_observed',mode=mode,state=w.status(op),step=step,provider_mutation=False)
    h.results.append(dict(test='W4-step8-row-contention-and-crash',result='pass'))


def admission_refusals(w,t):
    h=w.h;w.stop_engine();account=t['account'];key='collected-tail-key';body=w.create_body(t)
    h.sql(f"UPDATE provisiond.provider_account_status SET status='account_unreachable' WHERE provider_account='{account}'")
    unsellable=dict(w.offer,sellable=False)
    h.sql("UPDATE provisiond.offer_listings SET offers='%s' WHERE provider_account='%s'"%(json.dumps([unsellable]),account))
    h.sql(f"INSERT INTO provisiond.channel_closures VALUES('{account}','orders',clock_timestamp()+interval '1 hour')")
    h.sql('UPDATE provisiond.funding_health SET solvency_halt=true');h.sql('UPDATE provisiond.rates SET available=false')
    refused=w.request('POST','/v1/machines',body,t,key,status=409)['error']
    assert [e['kind'] for e in refused['details']['refusals']]==['conflict','invalid_request','rate_limited','halted','halted']
    assert refused['retryable'] is False and 'retry_after_ms' not in refused['details']
    assert refused['details']['not_checked']==[{'check':c,'because':b} for c in ['commitment_cap','balance'] for b in ['provider_account_not_healthy','offer_not_sellable','rate_unavailable']]
    assert all('required_sats' not in e['details'] for e in refused['details']['refusals'])
    assert h.sql(f"SELECT count(*) FROM provisiond.idempotency_records WHERE scope_id='{t['tenant_id']}' AND key='{key}'")=='0'
    h.sql(f"UPDATE provisiond.provider_account_status SET status='healthy' WHERE provider_account='{account}'")
    h.sql("UPDATE provisiond.offer_listings SET offers='%s',read_at=clock_timestamp()-interval '2 hours' WHERE provider_account='%s'"%(json.dumps([w.offer]),account))
    h.sql('DELETE FROM provisiond.channel_closures');h.sql('UPDATE provisiond.funding_health SET solvency_halt=false');h.sql('UPDATE provisiond.rates SET available=true')
    assert w.request('POST','/v1/machines',body,t,key,status=502)['error']['kind']=='provider'
    h.sql(f"UPDATE provisiond.offer_listings SET read_at=clock_timestamp() WHERE provider_account='{account}'")
    op=w.request('POST','/v1/machines',body,t,key,status=202)['id']
    assert h.sql(f"SELECT count(*) FROM provisiond.idempotency_records WHERE scope_id='{t['tenant_id']}' AND key='{key}'")=='1'
    w.provider_control(offers={'sim-a':[w.offer],'sim-b':[w.offer]});w.start_engine();w.wait_status(op,'succeeded');w.stop_engine()
    # Collections are principal scoped, and every supplied predicate intersects.
    assert w.request('GET','/v1/operations?terminal=false&status=succeeded',tenant=t)['operations']==[]
    rows=w.request('GET','/v1/operations?limit=0',tenant=t);assert len(rows['operations'])==1
    w.request('GET','/v1/operations?limit=nope',tenant=t,status=400)
    h.event('rich_admission_refusal_observed',ordered_refusals=5,not_checked_reasons=6,no_refusal_receipt=True,stale_listing_refused=True,same_key_after_clear=True)
    h.results.append(dict(test='W4-rich-admission-tail',result='pass'))


def accepted_verification_error(w):
    t=w.enrol();w.fund(t,'accepted-verification');w.provider_control(mode='search_throttle')
    op=w.request('POST','/v1/machines',w.create_body(t),t,'accepted-search-error',status=202)['id']
    w.start_engine();eventually(lambda:w.status(op) in ['failed','needs_reconciliation']);w.stop_engine()
    observed=w.status(op);w.h.event('accepted_verification_error_observed',operation=op,classification=observed,accepted_effect=True,verification_throttle=True)
    assert observed=='needs_reconciliation','accepted create verification failure must retain uncertainty'
    view=w.request('GET','/v1/operations/'+op,tenant=t)
    assert view['retryable'] is False and view['error']['retryable'] is False
    assert view['error']['details']['retry_after_ms']==1000 and view['error']['details']['accepted'] is True
    assert w.h.sql(f"SELECT reserved_sats||':'||state FROM provisiond.commitments WHERE operation_id='{op}'")=='43:open'
    assert w.h.sql(f"SELECT count(*) FROM provisiond.ledger_entries WHERE operation_id='{op}'")=='0'
    w.provider_control(mode='normal')
    w.h.results.append(dict(test='W4-accepted-verification-error',result='pass'))


def temporary_keys(w):
    w.stop_engine();t=w.enrol();w.fund(t,'temporary-keys');h=w.h;account=t['account']
    d=dict(w.descriptor,account=account,temporary_keys=True)
    h.sql("UPDATE provisiond.provider_accounts SET descriptor='%s' WHERE account='%s'"%(json.dumps(d),account))
    w.provider_control(offers={'sim-a':[w.offer],'sim-b':[w.offer]})
    for mode in ['reject_order','duplicate_key','normal','drop_key_reply']:
        w.provider_control(mode=mode)
        op=w.request('POST','/v1/machines',w.create_body(t),t,'temporary-key-'+mode,status=202)['id']
        w.start_engine();w.wait_status(op,'succeeded' if mode=='normal' else 'needs_reconciliation' if mode=='drop_key_reply' else 'failed');w.stop_engine()
        records=[e for e in w.external()['events'] if e.get('command')==op]
        paths=[e['path'] for e in records if e['stage']=='request_received']
        assert paths[0]=='/offers' and paths[1]=='/keys'
        if mode=='reject_order':assert paths==['/offers','/keys','/order','/keys/delete'] and op not in w.external()['keys']
        if mode=='duplicate_key':assert paths==['/offers','/keys'] and not [e for e in records if e['stage']=='key_deleted']
        if mode=='normal':assert paths[:3]==['/offers','/keys','/order'] and w.external()['keys'][op]['name']==op
        if mode=='drop_key_reply':assert paths==['/offers','/keys'] and op in w.external()['keys'] and not [e for e in records if e['stage']=='key_deleted']
        h.event('temporary_key_observed',mode=mode,operation=op,requests=paths,owns_only_acknowledged_registration=True,sweep_cleanup='not run')
    h.sql("UPDATE provisiond.provider_accounts SET descriptor='%s' WHERE account='%s'"%(json.dumps(dict(w.descriptor,account=account)),account))
    w.provider_control(mode='normal')
    h.results.append(dict(test='W4-temporary-key-ownership',result='pass',limits='settled-key sweep cleanup not run'))


def concurrency_and_resource(w):
    h=w.h;w.stop_engine();w.provider_control(offers={'sim-a':[w.offer],'sim-b':[w.offer]})
    t=w.enrol();w.fund(t,'concurrent-balance');body=w.create_body(t);body['runway_seconds']=16000
    with concurrent.futures.ThreadPoolExecutor(max_workers=2) as pool:
        requests=[pool.submit(http,h.ports[4],'POST','/v1/machines',body,t['spending_token'],'balance-race-'+str(i)) for i in range(2)]
        responses=[f.result(timeout=8) for f in requests]
    assert sorted(r[0] for r in responses)==[202,402],responses
    op=next(r[1]['id'] for r in responses if r[0]==202)
    assert h.sql(f"SELECT sum(reserved_sats) FROM provisiond.commitments WHERE tenant_id='{t['tenant_id']}' AND state='open'")=='80'
    w.start_engine();w.wait_status(op,'succeeded');w.stop_engine()
    # Configure exactly one healthy account, so both new assignments are automatic.
    h.sql("UPDATE provisiond.provider_account_status SET status='account_unreachable' WHERE provider_account='sim-b'")
    a=w.enrol();w.fund(a,'resource-race-a');b=w.enrol();w.fund(b,'resource-race-b')
    assert a['account']==b['account']=='sim-a'
    descriptor=dict(w.descriptor,account='sim-a',offer_is_resource=True)
    h.sql("UPDATE provisiond.provider_accounts SET descriptor='%s' WHERE account='sim-a'"%json.dumps(descriptor));w.restart_api()
    with concurrent.futures.ThreadPoolExecutor(max_workers=2) as pool:
        requests=[pool.submit(http,h.ports[4],'POST','/v1/machines',w.create_body(tenant),tenant['spending_token'],'resource-race-key') for tenant in [a,b]]
        responses=[f.result(timeout=8) for f in requests]
    assert sorted(r[0] for r in responses)==[202,409],responses
    loser=next(r[1]['error'] for r in responses if r[0]==409)
    assert loser['details']=={'reason':'state'} and loser['retryable'] is False
    assert h.sql("SELECT count(*) FROM provisiond.resource_exclusions WHERE provider_account='sim-a'")=='1'
    op=next(r[1]['id'] for r in responses if r[0]==202);w.start_engine();w.wait_status(op,'succeeded');w.stop_engine()
    assert h.sql("SELECT count(*) FROM provisiond.resource_exclusions WHERE provider_account='sim-a'")=='0'
    h.sql("UPDATE provisiond.provider_accounts SET descriptor='%s' WHERE account='sim-a'"%json.dumps(dict(w.descriptor,account='sim-a')))
    h.sql("UPDATE provisiond.provider_account_status SET status='healthy' WHERE provider_account='sim-b'");w.restart_api()
    # Cursor identity and every filter are part of the sealed principal-scoped cursor.
    owner=w.request('GET','/v1/operations?limit=1',tenant=a)
    assert all(v['tenant']==a['tenant_id'] for v in owner['operations'])
    h.event('http_concurrent_admission_observed',same_balance_admitted=1,same_balance_refused=1,reserved_sats=80,resource_offer_admitted=1,resource_offer_conflict=1,assignments_automatic=True)
    h.results.append(dict(test='W4-HTTP-concurrent-admission',result='pass'))


def unavailable_activation(w):
    h=w.h;h.sql("UPDATE provisiond.provider_account_status SET status='account_unreachable'")
    t=w.enrol();w.fund(t,'unavailable-activation',assignment=False)
    assert w.request('GET','/v1/enrol/'+t['handle'])=={'status':'active'}
    assert any(e.get('tenant')==t['tenant_id'] for e in w.events('api','activation_without_healthy_assignment'))
    h.sql("UPDATE provisiond.provider_account_status SET status='healthy'")
    assert h.sql(f"SELECT count(*) FROM provisiond.tenant_provider_accounts WHERE tenant_id='{t['tenant_id']}'")=='0'
    h.event('no_healthy_activation_observed',activated=True,assigned=False,monitorable_event=True,later_assignment_sweep='not run')
    h.results.append(dict(test='W4-no-healthy-activation',result='pass',limits='later assignment sweep not run'))


def lost_ack_refusals(w):
    h=w.h;w.stop_engine();t=w.enrol();w.fund(t,'lost-ack-refusal');account=t['account']
    w.provider_control(offers={'sim-a':[w.offer],'sim-b':[w.offer]})
    for target in ['account','tenant']:
        op=w.request('POST','/v1/machines',w.create_body(t),t,'lost-ack-refusal-'+target,status=202)['id']
        statement=h.proxy.arm('SET correlator_kind=COALESCE','send');w.start_engine();connection=statement.wait()
        reply=h.proxy.arm('store COMMIT','commit_reply',session=connection['pid']);statement.release();reply.wait()
        marker=h.sql(f"SELECT correlator_value||':'||about_to_order_at FROM provisiond.operations WHERE id='{op}'")
        if target=='account':h.sql(f"UPDATE provisiond.provider_account_status SET status='account_unreachable' WHERE provider_account='{account}'")
        else:h.sql(f"UPDATE provisiond.tenants SET status='suspended' WHERE id='{t['tenant_id']}'")
        h.proxy.close_connection(connection,'lost_step8_ack_then_guard_refusal');reply.release();w.wait_status(op,'failed');w.stop_engine()
        assert h.sql(f"SELECT correlator_value||':'||about_to_order_at FROM provisiond.operations WHERE id='{op}'")==marker
        assert h.sql(f"SELECT reserved_sats||':'||state FROM provisiond.commitments WHERE operation_id='{op}'")=='0:closed'
        assert not [e for e in w.external()['events'] if e.get('command')==op and e.get('path') in ['/keys','/order']]
        error=json.loads(h.sql(f"SELECT error::text FROM provisiond.operations WHERE id='{op}'"))
        assert error['details']['reason']==('state' if target=='account' else 'tenant_suspended')
        h.sql(f"UPDATE provisiond.provider_account_status SET status='healthy' WHERE provider_account='{account}'")
        h.sql(f"UPDATE provisiond.tenants SET status='active' WHERE id='{t['tenant_id']}'")
        h.event('lost_ack_guard_refusal_observed',target_type=target,operation=op,prior_marker_preserved=True,recorded_failure=True,mutation=False)
    h.results.append(dict(test='W4-lost-step8-ack-guard-refusal',result='pass'))


def ceiling_restart(w):
    h=w.h;t=w.enrol();w.fund(t,'ceiling-restart');w.stop_engine()
    parameters=dict(PARAMETERS,created_limit=1,ceiling_seconds=86400)
    w.restart_api(parameters)
    body=w.create_body(t);op=w.request('POST','/v1/machines',body,t,'ceiling-first-key',status=202)['id']
    assert w.request('POST','/v1/machines',body,t,'ceiling-first-key',status=202)['id']==op
    error=w.request('POST','/v1/machines',body,t,'ceiling-second-key',status=429)['error']
    assert error['kind']=='ceiling_exceeded' and error['details']['ceiling']=='machines_created'
    w.restart_api(parameters)
    assert w.request('POST','/v1/machines',body,t,'ceiling-second-key',status=429)['error']['kind']=='ceiling_exceeded'
    assert h.sql(f"SELECT used FROM provisiond.ceiling_counters WHERE principal='{t['tenant_id']}' AND ceiling='machines_created'")=='1'
    assert h.sql(f"SELECT count(*) FROM provisiond.idempotency_records WHERE scope_id='{t['tenant_id']}' AND key='ceiling-second-key'")=='0'
    w.provider_control(offers={'sim-a':[w.offer],'sim-b':[w.offer]});w.start_engine();w.wait_status(op,'succeeded');w.stop_engine();w.restart_api()
    h.event('durable_ceiling_observed',used=1,limit=1,interval_seconds=86400,replays_spend_slot=False,retained_across_restart=True)
    h.results.append(dict(test='W4-durable-ceiling-restart',result='pass'))


def query_leniency(w):
    from w4_regressions import funded
    w.stop_engine();t=funded(w,'query-leniency')
    op=w.request('POST','/v1/machines',w.create_body(t),t,'query-leniency-create',status=202)['id']
    view=w.request('GET','/v1/balance?commitments=true&terminal=false&requested_by=caller',tenant=t)
    w.h.event('query_leniency_observed',commitments=len(view['commitments']),expected_operation=op)
    assert len(view['commitments'])==1,'unknown balance query parameters must not filter commitments'
    assert view['commitments'][0]['operation_id']==op
    code,value,_=http(w.h.ports[4],'GET','/v1/operations?status=queued%2Crunning',token=t['spending_token'])
    w.h.event('decoded_query_observed',encoded_status=code)
    assert code==200,'encoded valid query values must be decoded'
    assert any(v['id']==op for v in value['operations'])
    assert w.request('GET','/v1/operations?%73tatus=succeeded%2Cfailed',tenant=t)['operations']==[]
    assert w.request('GET','/v1/balance?%63ommitments=%74rue',tenant=t)['commitments'][0]['operation_id']==op
    key='raw-target-fingerprint';body=w.create_body(t)
    # A raw target change remains a fingerprint change despite query equivalence.
    w.request('POST','/v1/deposits?ignored=%31',{'amount_sats':10},t,key)
    assert w.request('POST','/v1/deposits?ignored=1',{'amount_sats':10},t,key,status=409)['error']['details']['reason']=='idempotency_mismatch'
    w.h.results.append(dict(test='W4-decoded-query-and-raw-fingerprint',result='pass'))


def admission_field_repetition(w):
    t=w.enrol();w.fund(t,'admission-record-validation')
    op=w.request('POST','/v1/machines',w.create_body(t),t,'admission-record-validation',status=202)['id']
    w.h.sql(f"UPDATE provisiond.operations SET request_summary=jsonb_set(request_summary,'{{offer,accepted_terms,offer,currency}}','null') WHERE id='{op}'")
    w.start_engine();w.wait_status(op,'failed');w.stop_engine()
    error=w.request('GET','/v1/operations/'+op,tenant=t)['error']
    requests=[e['path'] for e in w.external()['events'] if e.get('command')==op and e['stage']=='request_received']
    w.h.event('admission_field_repetition_observed',reason=error['details'].get('reason'),provider_requests=requests)
    assert error['details'].get('reason')=='incomplete_record' and requests==[],'invalid admission fields must fail before List offers'
    w.h.results.append(dict(test='W4-admission-field-repetition',result='pass'))


def token_limits_and_expiry(w):
    h=w.h;w.stop_engine();parameters=dict(PARAMETERS,admission_delay_ms=500)
    w.restart_api(parameters)
    before=len(w.events('api','token_request_held'));issued=len(w.events('api','token_issued'));abandoned=len(w.events('api','token_request_abandoned'))
    count=h.sql('SELECT count(*) FROM provisiond.tenants')
    callers=[socket.create_connection(('127.0.0.1',h.ports[4])) for _ in range(2)]
    for caller in callers:caller.sendall(b'POST /v1/enrol/token HTTP/1.1\r\nHost: localhost\r\nContent-Type: application/json\r\nContent-Length: 2\r\n\r\n{}')
    eventually(lambda:len(w.events('api','token_request_held'))==before+2)
    refusal=w.request('POST','/v1/enrol/token',{},status=429)
    assert refusal['error']['kind']=='rate_limited'
    for caller in callers:caller.close()
    eventually(lambda:len(w.events('api','token_request_abandoned'))==abandoned+2)
    assert len(w.events('api','token_issued'))==issued and h.sql('SELECT count(*) FROM provisiond.tenants')==count
    pending=int(h.sql("SELECT count(*) FROM provisiond.tenants WHERE status='pending'"))
    w.restart_api(dict(PARAMETERS,pending_limit=pending+1))
    w.enrol();token=w.request('POST','/v1/enrol/token',{})
    assert w.request('POST','/v1/enrol',{'admission_token':token['admission_token']},status=429)['error']['kind']=='rate_limited'
    w.restart_api(dict(PARAMETERS,deposit_seconds=2,finality_seconds=3,tenant_seconds=6))
    t=w.enrol();deposit=w.request('POST','/v1/deposits',{'amount_sats':60},t,'expiry-deposit')
    payment_hash=h.sql(f"SELECT payment_hash FROM provisiond.deposits WHERE id='{deposit['id']}'")
    instant=time.strftime('%Y-%m-%dT%H:%M:%SZ',time.gmtime())
    w.settlement(dict(rail='lightning',destination=deposit['lightning']['invoice'],payment_hash=payment_hash,amount_sats=60,settled=True,preimage_known=True,settled_at=instant))
    eventually(lambda:h.sql(f"SELECT count(*) FROM provisiond.payments WHERE deposit_id='{deposit['id']}'")=='1')
    eventually(lambda:h.sql(f"SELECT expires_at-clock_timestamp()<interval '2.5 seconds' FROM provisiond.enrolments WHERE tenant_id='{t['tenant_id']}'")=='t')
    assert w.request('POST','/v1/deposits',{'amount_sats':60},t,'closed-signup-window',status=409)['error']['details']['reason']=='signup_window_closed'
    eventually(lambda:h.sql(f"SELECT count(*) FROM provisiond.tenants WHERE id='{t['tenant_id']}'")=='0')
    assert w.request('GET','/v1/enrol/'+t['handle'],status=404)['error']['kind']=='not_found'
    w.settlement(dict(rail='onchain',destination=deposit['onchain']['address'],txid=hashlib.sha256(b'orphan-credit').hexdigest(),vout=0,confirmations=2,amount_sats=55,settled_at=instant))
    eventually(lambda:h.sql(f"SELECT count(*) FROM provisiond.payments WHERE deposit_id='{deposit['id']}'")=='2')
    assert h.sql(f"SELECT balance_after FROM provisiond.ledger_entries WHERE tenant_id='{t['tenant_id']}' ORDER BY seq DESC LIMIT 1")=='115'
    w.restart_api()
    h.event('enrolment_limits_and_expiry_observed',held_requests_abandoned=2,tokens_left_after_abandonment=0,pending_ceiling_refused=True,closed_signup_refused=True,expired_credentials_removed=True,orphan_credit_retained=True)
    h.results.append(dict(test='W4-enrolment-limits-TTL-orphan',result='pass'))


def error_redaction(w):
    t=w.enrol();w.fund(t,'error-redaction');w.provider_control(mode='secret_error')
    op=w.request('POST','/v1/machines',w.create_body(t),t,'redacted-provider-error',status=202)['id']
    w.start_engine();w.wait_status(op,'failed');w.stop_engine()
    stored=w.h.sql(f"SELECT error::text FROM provisiond.operations WHERE id='{op}'")
    rendered=json.dumps(w.request('GET','/v1/operations/'+op,tenant=t))
    leaked='TEST_ONLY_SECRET_MARKER' in stored or 'TEST_ONLY_SECRET_MARKER' in rendered
    w.h.event('provider_error_redaction_observed',operation=op,controlled_marker_leaked=leaked)
    assert not leaked,'provider error secrets must be redacted before persistence'
    w.provider_control(mode='normal')
    w.h.results.append(dict(test='W4-provider-error-redaction',result='pass'))


def halt_invoice(w):
    t=w.enrol();deposit=w.request('POST','/v1/deposits',{'amount_sats':60},t,'halt-invoice-deposit')
    before=len(w.events('api','external_entry'))
    w.h.sql('UPDATE provisiond.funding_health SET solvency_halt=true')
    eventually(lambda:len(w.events('api','external_entry'))>=before+2)
    eventually(lambda:w.request('GET','/v1/deposits/'+deposit['id'],tenant=t)['lightning']['cancelled'])
    view=w.request('GET','/v1/deposits/'+deposit['id'],tenant=t)
    w.h.event('halt_invoice_cancellation_observed',gate=view['gate'],cancelled=view['lightning']['cancelled'])
    assert view['gate']=='solvency' and view['lightning']['cancelled'],'solvency halt must cancel an unsettled invoice'
    assert w.request('POST','/v1/deposits',{'amount_sats':60},t,'halt-refused-deposit',status=503)['error']['details']['gate']=='solvency'
    w.settlement(dict(rail='onchain',destination=deposit['onchain']['address'],txid=hashlib.sha256(b'halt-under-floor').hexdigest(),vout=0,confirmations=2,amount_sats=5,settled_at=time.strftime('%Y-%m-%dT%H:%M:%SZ',time.gmtime())))
    eventually(lambda:w.request('GET','/v1/deposits/'+deposit['id'],tenant=t)['credited_sats']==5)
    w.h.sql('UPDATE provisiond.funding_health SET solvency_halt=false')
    view=w.request('GET','/v1/deposits/'+deposit['id'],tenant=t)
    assert view['gate'] is None and view['lightning']['cancelled'] and not view['expired']
    w.h.results.append(dict(test='W4-halt-cancellation-and-under-floor-chain-credit',result='pass'))


def stop_cost_adapter(w):
    w.stop_engine();resource=dict(external_id='TEST-STOP-COST-PROBE',account='sim-a',channel='orders',correlator='test-fixture',name='stop-cost-fixture',region='test',state='running',public_ips=[],present=True)
    w.provider_control(fixture_resource=resource)
    w.h.run(['cargo','build','--example','provider-probe','--features','store-test-hooks','--locked'],timeout=120)
    previous_env=w.h.env
    w.h.env=dict(previous_env,**environment(w.h))
    try:result=w.h.run([str(w.h.target/'debug/examples/provider-probe'),'sim-a',resource['external_id']],timeout=12)
    finally:w.h.env=previous_env
    assert resource['external_id'] not in w.external()['resources']
    calls=[e['path'] for e in w.external()['events'] if e.get('command')=='stop-cost-fixture' and e['stage']=='request_received']
    assert calls==['/delete','/get']
    assert 'external_entry' in result.stdout
    w.h.event('stop_cost_adapter_observed',independent_fixture=True,requests=calls,absence_confirmed=True,production_cancellation_workflow='not run')
    w.h.results.append(dict(test='W4-stop-cost-driver-adapter',result='pass'))


def forwarded_validation(w):
    t=w.enrol();w.fund(t,'forwarded-validation');body=w.create_body(t);body['ssh_public_keys']=['ssh-ed25519 invalid-key-material']
    op=w.request('POST','/v1/machines',body,t,'forwarded-key-validation',status=202)['id']
    w.start_engine();eventually(lambda:w.status(op) in ['succeeded','failed','needs_reconciliation']);w.stop_engine()
    view=w.request('GET','/v1/operations/'+op,tenant=t)
    w.h.event('forwarded_value_validation_observed',operation=op,state=view['status'],driver_field=view['error']['details'].get('field') if view['error'] else None)
    assert view['status']=='failed' and view['error']['details'].get('field')=='/ssh_public_keys/0','invalid forwarded key must be rejected before provider mutation'
    assert not [e for e in w.external()['events'] if e.get('command')==op and e.get('path') in ['/keys','/order']]
    w.h.results.append(dict(test='W4-forwarded-key-validation',result='pass'))


def live_provider_stop(w):
    from core import core_probe
    h=w.h;w.stop_engine();t=w.enrol();w.fund(t,'live-provider-stop');account=t['account']
    descriptor=dict(w.descriptor,account=account,call_seconds=5)
    h.sql("UPDATE provisiond.provider_accounts SET descriptor='%s' WHERE account='%s'"%(json.dumps(descriptor),account))
    op=w.request('POST','/v1/machines',w.create_body(t),t,'live-provider-stop-create',status=202)['id']
    h.run(['cargo','build','--example','core-probe','--features','store-test-hooks','--locked'],timeout=120)
    hold=h.root/'provider/hold-after-create';hold.touch()
    p=core_probe(h);p.command('engine_start');p.command('dispatch_provider')
    eventually(lambda:any(e.get('command')==op and e['stage']=='resource_created' for e in w.external()['events']))
    p.command('record_stop '+op);p.send('wait_records')
    eventually(lambda:p.p.poll() is not None or any(e.get('event')=='stop_record_finished' and e.get('operation')==op for e in p.events),seconds=5)
    observed=w.status(op);exited=p.p.poll()
    h.event('live_provider_stop_observed',operation=op,state=observed,exit_status=exited,provider_effect_retained=True)
    if exited is not None:p.expected_status=exited
    p.finish();hold.unlink()
    assert exited is None and observed=='needs_reconciliation','live provider I/O must cancel before the stop-record bound'
    assert h.sql(f"SELECT reserved_sats||':'||state FROM provisiond.commitments WHERE operation_id='{op}'")=='43:open'
    assert h.sql(f"SELECT count(*) FROM provisiond.ledger_entries WHERE operation_id='{op}'")=='0'
    h.sql("UPDATE provisiond.provider_accounts SET descriptor='%s' WHERE account='%s'"%(json.dumps(dict(w.descriptor,account=account)),account))
    h.results.append(dict(test='W4-live-provider-stop',result='pass'))
