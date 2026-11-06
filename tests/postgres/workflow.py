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
    def __init__(self,h):
        self.h=h;self.children=[];self.engine=None;self.private=[]
        prepare(h)
        h.run(['cargo','build','--bins','--features','store-test-hooks','--locked'],timeout=120)
        binary(h,'migrate',role='migrator')
        self.parameters=h.root/'local-parameters.json';self.parameters.write_text(json.dumps(PARAMETERS))
        self.control=h.root/'worker-control';self.control.mkdir()
        self.descriptor=dict(account='sim-a',kind='local_simulator',capabilities=['provision_virtual','list_offers','delete_machine'],
            channel='orders',channels=['orders'],offer_is_resource=False,temporary_keys=False,
            currency='USD',stop_cost=True,cancellation_bound=time.strftime('%Y-%m-%d',time.gmtime()),
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
        h.release_port(h.ports[4])
        self.api=self.start('api',dict(PROVISIOND_API_LISTEN=f'127.0.0.1:{h.ports[4]}',PROVISIOND_RAIL_SIMULATOR=f'127.0.0.1:{h.ports[6]}',PROVISIOND_LOCAL_PARAMETERS=str(self.parameters)))
        eventually(lambda:self.events('api','api_listening'))
    def simulator(self,kind,offset):
        h=self.h;h.release_port(h.ports[offset]);root=h.root/kind;root.mkdir()
        log=(h.root/(kind+'.stdout')).open('w');err=(h.root/(kind+'.stderr')).open('w')
        p=subprocess.Popen(['python3','tests/postgres/simulator.py','--port',str(h.ports[offset]),'--root',str(root),'--kind',kind],env=h.env,stdout=log,stderr=err,text=True)
        log.close();err.close();h.extra.append(p);self.children.append(p)
        eventually(lambda:(h.root/(kind+'.stdout')).read_text())
        return p
    def start(self,name,extra=None):
        h=self.h
        count=sum(1 for c in self.children if getattr(c,'name',None)==name)
        log=h.root/f'{name}-{count}.stdout';err=h.root/f'{name}-{count}.stderr'
        with log.open('w') as out,err.open('w') as errors:
            p=subprocess.Popen([str(h.target/'debug'/name)],env=dict(h.env,**environment(h),**(extra or {})),stdout=out,stderr=errors,text=True)
        p.name=name;p.log=log;p.error=err;self.children.append(p);h.extra.append(p)
        return p
    def start_engine(self):
        self.engine=self.start('engine',dict(PROVISIOND_TEST_CONTROL=str(self.control)))
        eventually(lambda:self.events('engine','restart_pass_complete'))
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
    def fund(self,tenant,suffix):
        deposit=self.request('POST','/v1/deposits',{'amount_sats':60},tenant,'deposit-'+suffix)
        assert deposit['lightning']['invoice'].startswith('TEST/SIMULATOR/NONPAYABLE/')
        assert deposit['onchain']['address'].startswith('TEST/SIMULATOR/NONPAYABLE/')
        payment_hash=self.h.sql(f"SELECT payment_hash FROM provisiond.deposits WHERE id='{deposit['id']}'")
        lightning=dict(rail='lightning',destination=deposit['lightning']['invoice'],payment_hash=payment_hash,amount_sats=60,settled=False,preimage_known=False,settled_at=time.strftime('%Y-%m-%dT%H:%M:%SZ',time.gmtime()))
        self.settlement(lightning)
        # Poll observation entry, then independent SQL: a held HTLC credits nothing.
        eventually(lambda:self.events('api','external_entry'))
        assert self.h.sql(f"SELECT count(*) FROM provisiond.payments WHERE deposit_id='{deposit['id']}'")=='0'
        lightning.update(settled=True,preimage_known=True);self.settlement(lightning)
        eventually(lambda:self.request('GET','/v1/deposits/'+deposit['id'],tenant=tenant)['credited_sats']==60)
        assert self.request('GET','/v1/enrol/'+tenant['handle'])=={'status':'pending'}
        onchain=dict(rail='onchain',destination=deposit['onchain']['address'],txid=hashlib.sha256(suffix.encode()).hexdigest(),vout=0,confirmations=1,amount_sats=55,settled_at=lightning['settled_at'])
        self.settlement(onchain)
        assert self.h.sql(f"SELECT count(*) FROM provisiond.payments WHERE deposit_id='{deposit['id']}'")=='1'
        onchain['confirmations']=2;self.settlement(onchain)
        eventually(lambda:self.request('GET','/v1/enrol/'+tenant['handle'])=={'status':'active'})
        assert self.request('GET','/v1/deposits/'+deposit['id'],tenant=tenant)['credited_sats']==115
        assert self.request('POST','/v1/deposits',{'amount_sats':60},tenant,'deposit-'+suffix)==deposit
        self.settlement(lightning);self.settlement(onchain)
        account=self.request('GET','/v1/providers',tenant=tenant)['providers'][0]['account']
        tenant['account']=account
        return deposit,lightning,onchain
    def settlement(self,v):return http(self.h.ports[6],'POST','/control',{'settlement':v})
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
        self.h.event('workflow_children_reaped',count=len(self.children))


def workflow(h):
    w=Workflow(h)
    try:
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
    finally:w.close()
