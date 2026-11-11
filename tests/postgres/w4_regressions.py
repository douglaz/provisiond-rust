"""Focused API-7/13/29/49/50, OPS-43 and LDG-31 regression observations."""
import concurrent.futures
import hashlib
import json
import socket
import time
from workflow import PARAMETERS, http
from harness import eventually


def funded(w, suffix):
    """Real HTTP enrolment and trusted final adapter observations, no balance edits."""
    t=w.enrol();d=w.request('POST','/v1/deposits',{'amount_sats':115},t,'regression-fund-'+suffix)
    payment_hash=w.h.sql(f"SELECT payment_hash FROM provisiond.deposits WHERE id='{d['id']}'")
    w.settlement(dict(rail='lightning',destination=d['lightning']['invoice'],payment_hash=payment_hash,amount_sats=115,settled=True,preimage_known=True,settled_at=time.strftime('%Y-%m-%dT%H:%M:%SZ',time.gmtime())))
    eventually(lambda:w.request('GET','/v1/enrol/'+t['handle'])=={'status':'active'})
    t['account']=w.request('GET','/v1/providers',tenant=t)['providers'][0]['account']
    eventually(lambda:any(e.get('deposit')==d['id'] for e in w.events('api','rail_credit_acknowledged')))
    http(w.h.ports[6],'POST','/control',{'clear':True})
    return t


def price(w):
    # OPS-43: decreases offset increases; compare with admitted coverage at its basis.
    w.stop_engine();t=funded(w,'coverage');account=t['account']
    for name,recurring,setup,expected in [('offset','0.00','0.03','succeeded'),('uncovered','0.01','0.04','failed')]:
        current=dict(w.offer,recurring_price=recurring,setup_fee=setup)
        op=w.request('POST','/v1/machines',w.create_body(t),t,'native-coverage-'+name,status=202)['id']
        assert w.h.sql(f"SELECT admitted_sats FROM provisiond.operations WHERE id='{op}'")=='43'
        # Runtime descriptor/rate changes cannot replace the accepted price basis.
        w.provider_control(offers={account:[current]})
        if name=='offset':
            moved=dict(w.rate,rate_num=100)
            w.h.sql("UPDATE provisiond.rates SET value='%s' WHERE currency='USD'"%json.dumps(moved))
            w.h.sql("UPDATE provisiond.provider_accounts SET descriptor='%s' WHERE account='%s'"%(json.dumps(dict(w.descriptor,account=account,margin_bps=9000)),account))
        w.start_engine();eventually(lambda:w.status(op) in ['failed','succeeded','needs_reconciliation']);w.stop_engine()
        state=w.status(op);view=w.request('GET','/v1/operations/'+op,tenant=t)
        orders=[e for e in w.external()['events'] if e.get('command')==op and e['stage']=='accepted']
        w.h.event('native_coverage_observed',case=name,operation=op,accepted_reserve=43,current_recurring=recurring,current_setup=setup,state=state,orders=len(orders),error=view['error'])
        assert state==expected,'covered offsetting native move must not refuse' if name=='offset' else 'uncovered native rise must refuse'
        if name=='uncovered':
            assert view['error']['details']['reason']=='price_moved' and view['error']['details']['shortfall_native']==2
            assert view['retryable'] and view['error']['retryable'] and not orders
        else:assert len(orders)==1
        w.h.sql("UPDATE provisiond.rates SET value='%s' WHERE currency='USD'"%json.dumps(w.rate))
        w.h.sql("UPDATE provisiond.provider_accounts SET descriptor='%s' WHERE account='%s'"%(json.dumps(dict(w.descriptor,account=account)),account))
    w.provider_control(offers={'sim-a':[w.offer],'sim-b':[w.offer]})
    w.h.results.append(dict(test='W4-native-price-coverage',result='pass'))


def empty_data(w):
    # API-13/WIR-17/OPS-23: the same valid empty string reaches worker repetition.
    w.stop_engine();t=funded(w,'empty-data');body=w.create_body(t);body['user_data']=''
    code,value,_=http(w.h.ports[4],'POST','/v1/machines',body,t['spending_token'],'valid-empty-user-data')
    w.h.event('empty_user_data_observed',admission_status=code,error=value.get('error'))
    assert code==202,'empty user data must be admitted'
    op=value['id'];w.start_engine();w.wait_status(op,'succeeded');w.stop_engine()
    assert len([e for e in w.external()['events'] if e.get('command')==op and e['stage']=='accepted'])==1
    for value in [0,'x'*1048577]:
        body['user_data']=value
        assert w.request('POST','/v1/machines',body,t,'invalid-user-data',status=400)['error']['details']['field']=='/user_data'
    body.update(user_data=0,image='')
    assert w.request('POST','/v1/machines',body,t,'least-user-data',status=400)['error']['details']['field']=='/image'
    w.h.results.append(dict(test='W4-empty-user-data-admission-worker',result='pass'))


def spend(w):
    # API-7 step 5c wins over a simultaneous capability refusal; all counters durable.
    w.stop_engine();t=funded(w,'spend-ceiling');account=t['account']
    p=dict(PARAMETERS,spend_limit=43,ceiling_seconds=86400);w.restart_api(p)
    body=w.create_body(t);op=w.request('POST','/v1/machines',body,t,'exhaust-spend-first',status=202)['id']
    descriptor=dict(w.descriptor,account=account,capabilities=['list_offers','delete_machine'])
    w.h.sql("UPDATE provisiond.provider_accounts SET descriptor='%s' WHERE account='%s'"%(json.dumps(descriptor),account));w.restart_api(p)
    assert w.request('POST','/v1/machines',body,t,'exhaust-spend-first',status=202)['id']==op
    before=w.h.sql(f"SELECT (SELECT count(*) FROM provisiond.operations WHERE tenant_id='{t['tenant_id']}')||':'||(SELECT count(*) FROM provisiond.commitments WHERE tenant_id='{t['tenant_id']}')||':'||(SELECT sum(used) FROM provisiond.ceiling_counters WHERE principal='{t['tenant_id']}')")
    code,value,hs=http(w.h.ports[4],'POST','/v1/machines',body,t['spending_token'],'exhaust-spend-second')
    w.h.event('exhausted_spend_precedence_observed',status=code,error=value.get('error'),used=43,limit=43,capability_available=False)
    assert code==429 and value['error']['kind']=='ceiling_exceeded','exhausted spend ceiling must precede capability refusal'
    details=value['error']['details'];assert details['ceiling']=='spend' and details['limit']==43 and details['interval_seconds']==86400 and details['retry_after_ms']>0 and 'Retry-After' in hs
    assert w.h.sql(f"SELECT (SELECT count(*) FROM provisiond.operations WHERE tenant_id='{t['tenant_id']}')||':'||(SELECT count(*) FROM provisiond.commitments WHERE tenant_id='{t['tenant_id']}')||':'||(SELECT sum(used) FROM provisiond.ceiling_counters WHERE principal='{t['tenant_id']}')")==before
    assert w.h.sql(f"SELECT count(*) FROM provisiond.idempotency_records WHERE scope_id='{t['tenant_id']}' AND key='exhaust-spend-second'")=='0'
    w.h.sql(f"UPDATE provisiond.tenants SET status='suspended' WHERE id='{t['tenant_id']}'")
    assert w.request('POST','/v1/machines',body,t,'exhaust-spend-first',status=202)['id']==op
    assert w.request('POST','/v1/machines',body,t,'exhaust-spend-second',status=403)['error']['kind']=='suspended'
    w.h.sql(f"UPDATE provisiond.tenants SET status='active' WHERE id='{t['tenant_id']}'")
    w.h.sql("UPDATE provisiond.provider_accounts SET descriptor='%s' WHERE account='%s'"%(json.dumps(dict(w.descriptor,account=account)),account));w.restart_api()
    w.h.results.append(dict(test='W4-exhausted-spend-precedence',result='pass'))


def abandonment(w):
    # WIR-49: routing and disconnect semantics must agree on ignored queries.
    for path in ['/v1/enrol/token','/v1/enrol/token?ignored=1']:
        w.restart_api(dict(PARAMETERS,admission_delay_ms=1500,source_concurrency=1))
        held=len(w.events('api','token_request_held'));abandoned=len(w.events('api','token_request_abandoned'));issued=len(w.events('api','token_issued'))
        rows=w.h.sql("SELECT (SELECT count(*) FROM provisiond.tenants)||':'||(SELECT count(*) FROM provisiond.enrolments)")
        conn=socket.create_connection(('127.0.0.1',w.h.ports[4]));conn.sendall(('POST '+path+' HTTP/1.1\r\nHost: localhost\r\nContent-Type: application/json\r\nContent-Length: 2\r\n\r\n{}').encode())
        eventually(lambda:len(w.events('api','token_request_held'))==held+1);conn.close()
        # A completed cancellation trace is the processing barrier, not a sleep.
        try:eventually(lambda:len(w.events('api','token_request_abandoned'))==abandoned+1,seconds=3)
        except AssertionError:pass
        cancelled=len(w.events('api','token_request_abandoned'))==abandoned+1
        w.h.event('token_abandonment_observed',target=path,cancelled=cancelled,tokens_issued=len(w.events('api','token_issued'))-issued)
        assert cancelled and len(w.events('api','token_issued'))==issued,'query-qualified abandoned token request must cancel issuance'
        assert w.h.sql("SELECT (SELECT count(*) FROM provisiond.tenants)||':'||(SELECT count(*) FROM provisiond.enrolments)")==rows
        assert w.request('POST',path,{})['admission_token'] # same source slot reusable
    w.restart_api();w.h.results.append(dict(test='W4-plain-query-token-abandonment',result='pass'))


def unauthenticated(w):
    t=w.enrol()
    for token in [None,'invalid-repeated',t['recovery_credential']]:
        w.restart_api(dict(PARAMETERS,rate_interval_ms=10000,read_limit=30,write_limit=2))
        codes=[]
        for i in range(3):codes.append(http(w.h.ports[4],'POST','/v1/machines',token=token,key='unauth-rate-test',raw=b'{')[0])
        w.h.event('unauthenticated_pacing_observed',credential_kind='missing' if token is None else 'recovery' if token==t['recovery_credential'] else 'invalid',statuses=codes)
        assert codes==[401,401,429],'unauthenticated requests must be source limited'
        for i in range(3):assert http(w.h.ports[4],'OPTIONS','/v1/machines')[0]==200
        assert http(w.h.ports[4],'POST','/v1/machines',token='fresh-invalid-token',key='unauth-rate-test',raw=b'{')[0]==429
        assert http(w.h.ports[4],'GET','/v1/provider-accounts')[0]==404
        assert http(w.h.ports[4],'GET','/v1/balance',token='bad-read')[0]==401 # read/write separate
        assert w.h.sql("SELECT count(*) FROM provisiond.tenants WHERE id='127.0.0.1'")=='0'
    w.restart_api();w.h.results.append(dict(test='W4-unauthenticated-source-pacing',result='pass'))


def pacing(w):
    # API-50: accepted intervals must cover sustained three-read fleet cycles.
    t=funded(w,'poll-budget');other=funded(w,'poll-budget-other')
    invalid=dict(PARAMETERS,read_limit=3,rate_interval_ms=10000)
    w.api.terminate();w.api.wait(timeout=5);w.parameters.write_text(json.dumps(invalid))
    w.api=w.start('api',dict(PROVISIOND_API_LISTEN=f'127.0.0.1:{w.h.ports[4]}',PROVISIOND_RAIL_SIMULATOR=f'127.0.0.1:{w.h.ports[6]}',PROVISIOND_LOCAL_PARAMETERS=str(w.parameters),PROVISIOND_TEST_CONTROL=str(w.control)))
    eventually(lambda:w.api.poll() is not None or 'api_listening' in w.api.log.read_text())
    rejected=w.api.poll() is not None
    w.h.event('polling_budget_validation_observed',read_limit=3,rate_interval_ms=10000,rejected=rejected,exit_status=w.api.poll())
    assert rejected,'incoherent polling budget must be rejected'
    for interval,limit,cycles in [(1000,3,4),(1500,6,5)]:
        w.restart_api(dict(PARAMETERS,read_limit=limit,rate_interval_ms=interval))
        statuses=[]
        for cycle in range(cycles):
            for tenant in [t,other]:
                for path in ['/v1/operations?terminal=false','/v1/balance','/v1/machines']:
                    code,value,headers=http(w.h.ports[4],'GET',path,token=tenant['spending_token']);statuses.append(code)
                    assert code==200,'obedient fleet polling must not be throttled'
                    if 'poll_after_ms' in value:assert int(headers['Retry-After'])*1000==value['poll_after_ms']==1000
            # Wait only the explicitly advertised caller cadence, never as fault proof.
            time.sleep(1) # advertised 1000 ms measured after the completed responses
        w.h.event('sustained_polling_observed',rate_interval_ms=interval,read_limit=limit,cycles=cycles,statuses=statuses)
    w.restart_api();w.h.results.append(dict(test='W4-polling-budget-window-boundaries',result='pass'))


def token_pacing(w):
    for delay in [400,800]:
        w.restart_api(dict(PARAMETERS,admission_delay_ms=delay,source_concurrency=1))
        held=len(w.events('api','token_request_held'));abandoned=len(w.events('api','token_request_abandoned'))
        caller=socket.create_connection(('127.0.0.1',w.h.ports[4]));caller.sendall(b'POST /v1/enrol/token HTTP/1.1\r\nHost: localhost\r\nContent-Type: application/json\r\nContent-Length: 2\r\n\r\n{}')
        eventually(lambda:len(w.events('api','token_request_held'))==held+1)
        code,value,hs=http(w.h.ports[4],'POST','/v1/enrol/token',{})
        retry=value['error']['details']['retry_after_ms'];caller.close()
        w.h.event('token_pacing_observed',admission_delay_ms=delay,retry_after_ms=retry,status=code)
        assert code==429 and retry==1000 and hs['Retry-After']=='1','token concurrency pacing must be independent of admission delay'
        eventually(lambda:len(w.events('api','token_request_abandoned'))==abandoned+1)
        code,value,hs=http(w.h.ports[4],'POST','/v1/enrol/token',{})
        assert code==200 and 'Retry-After' not in hs and 'poll_after_ms' not in value
        tenant=w.request('POST','/v1/enrol',{'admission_token':value['admission_token']})
        code,value,hs=http(w.h.ports[4],'GET','/v1/enrol/'+tenant['handle'])
        assert code==200 and 'Retry-After' not in hs and value=={'status':'pending'}
    w.restart_api();w.h.results.append(dict(test='W4-enrolment-pacing-no-delay-oracle',result='pass'))


CASES={'w4_price':price,'w4_empty_data':empty_data,'w4_spend':spend,'w4_abandon':abandonment,'w4_unauth':unauthenticated,'w4_pacing':pacing,'w4_token_pacing':token_pacing}


def settlement(w):
    # PRV-13b/LDG-31/39/66/67: real admission formula and exact clamp partition.
    # The only SQL financial fixture reduces this create's *remaining commitment*
    # after acknowledged licensing; it never sets a tenant balance or admits a create.
    from fractions import Fraction
    w.stop_engine();original_rate=dict(w.rate)
    offer=dict(w.offer,setup_fee='0.07',billing_unit={'seconds':3600,'native_floor':'0.05'})
    rate=dict(w.rate,rate_num=3,rate_den=2,haircut_bps=2500)
    expected_reserve=Fraction(7)+Fraction(5)*Fraction(11,10)+Fraction(11,10)*Fraction(3607,3600)
    expected_reserve*=Fraction(3,2)*Fraction(5,4)
    expected_reserve=-(-expected_reserve.numerator//expected_reserve.denominator)
    assert expected_reserve==26
    for case,remaining,current_rate in [('setup_partial',5,(7,3)),('unit_partial',17,(7,3)),('zero_debit',0,None)]:
        w.h.sql("UPDATE provisiond.rates SET value='%s',available=true WHERE currency='USD'"%json.dumps(rate))
        w.h.sql("UPDATE provisiond.offer_listings SET offers='%s',read_at=clock_timestamp()"%json.dumps([offer]))
        w.provider_control(offers={'sim-a':[offer],'sim-b':[offer]})
        t=funded(w,'partition-'+case);body=w.create_body(t)
        op=w.request('POST','/v1/machines',body,t,'settlement-partition-'+case,status=202)['id']
        assert w.h.sql(f"SELECT admitted_sats FROM provisiond.operations WHERE id='{op}'")==str(expected_reserve)
        summary=json.loads(w.h.sql(f"SELECT request_summary::text FROM provisiond.operations WHERE id='{op}'"))
        assert summary['setup_fee']['sats']==14 and summary['setup_fee']['native_minor']==7
        assert summary['first_unit']['sats']==11 and summary['first_unit']['native_minor']==5
        hold=w.control/'hold-step-9';hold.touch();w.start_engine()
        eventually(lambda:any(e.get('operation')==op and e.get('step')==9 for e in w.events('engine','dispatch_step_held')))
        w.h.sql(f"UPDATE provisiond.commitments SET reserved_sats={remaining},version=version+1 WHERE operation_id='{op}'")
        if current_rate:
            opened=dict(rate,rate_num=current_rate[0],rate_den=current_rate[1]);w.h.sql("UPDATE provisiond.rates SET value='%s' WHERE currency='USD'"%json.dumps(opened))
        else:w.h.sql("UPDATE provisiond.rates SET available=false WHERE currency='USD'")
        available_before=w.request('GET','/v1/balance',tenant=t)['available_sats']
        statement=w.h.proxy.arm('SET status=$3,result=','send');hold.unlink();connection=statement.wait()
        reply=w.h.proxy.arm('store COMMIT','commit_reply',session=connection['pid']);statement.release();reply.wait()
        assert w.status(op)=='succeeded'
        def state():
            return json.loads(w.h.sql(f"SELECT jsonb_build_object('operation',to_jsonb(o),'machine',(SELECT to_jsonb(m) FROM provisiond.machines m WHERE m.id=o.machine_id),'commitment',(SELECT to_jsonb(c) FROM provisiond.commitments c WHERE c.id=o.commitment_id),'ledger',COALESCE((SELECT jsonb_agg(to_jsonb(l) ORDER BY seq) FROM provisiond.ledger_entries l WHERE operation_id=o.id),'[]'::jsonb),'deficiencies',COALESCE((SELECT jsonb_agg(to_jsonb(d) ORDER BY idempotency_key) FROM provisiond.operator_deficiencies d WHERE operation_id=o.id),'[]'::jsonb),'exclusions',(SELECT count(*) FROM provisiond.resource_exclusions WHERE operation_id=o.id))::text FROM provisiond.operations o WHERE o.id='{op}'"))
        before=state();calls=len([e for e in w.external()['events'] if e.get('command')==op])
        if current_rate is None:
            assert before['machine']['runway_until'] is None and before['machine']['customer_rate_num'] is None and before['machine']['protected_sats'] is None
            # The record opened without a rate must stay null after rate return
            # and the real lost-acknowledgement whole-transaction repeat.
            w.h.sql("UPDATE provisiond.rates SET available=true WHERE currency='USD'")
        w.h.proxy.close_connection(connection,'lost_clamped_settlement_reply');reply.release()
        eventually(lambda:any(e.get('operation')==op for e in w.events('engine','settlement_finished')));w.stop_engine()
        after=state();assert before==after,'clamped settlement repeat must preserve every effect'
        assert calls==len([e for e in w.external()['events'] if e.get('command')==op])
        assert w.request('GET','/v1/balance',tenant=t)['available_sats']==available_before
        debits={e['kind']:e for e in after['ledger']};deficiencies={d['idempotency_key'].split(':',1)[0]:d for d in after['deficiencies']}
        left=remaining;expected_entries=0
        for kind,native,computed in [('setup_fee_debit',7,14),('first_unit_debit',5,11)]:
            debit=min(left,computed);left-=debit;customer_native=native*debit//computed
            if debit:
                expected_entries+=1;entry=debits[kind]
                assert entry['amount_sats']==-debit and entry['native_minor']==customer_native
                assert entry['native_currency']=='USD' and entry['rate_num']==3 and entry['rate_den']==2 and entry['haircut_bps']==2500
                assert entry['rate_source']==rate['source'] and entry['rounding_rule_version']=='ceil-v1'
                assert entry['operation_id']==op and entry['machine_id']==after['machine']['id'] and entry['commitment_id']==after['commitment']['id']
            else:assert kind not in debits
            if debit<computed:
                deficiency=deficiencies[kind]
                w.h.event('deficiency_partition_observed',case=case,debit_kind=kind,computed_sats=computed,customer_sats=debit,native_total=native,customer_native=customer_native,operator_native=deficiency['native_minor'],opening_rate=[deficiency['rate_num'],deficiency['rate_den']],expected_opening_rate=current_rate)
                assert deficiency['rate_num']==(current_rate[0] if current_rate else None) and deficiency['rate_den']==(current_rate[1] if current_rate else None),'deficiency must retain its actual opening rate or null'
                assert deficiency['native_minor']==native-customer_native and deficiency['clamped_sats']==computed-debit
                assert deficiency['cause']=='clamp_overflow' and deficiency['provider_account']==t['account'] and deficiency['currency']=='USD'
                assert deficiency['subject_kind']=='machine' and deficiency['subject_id']==after['machine']['id'] and deficiency['absorbed_seconds']==0
            else:assert kind not in deficiencies
        assert len(after['ledger'])==expected_entries and after['commitment']['reserved_sats']==left==0
        assert bool(after['machine']['first_unit_debit_id'])==('first_unit_debit' in debits)
        if 'first_unit_debit' in debits:assert after['machine']['first_unit_debit_id']==debits['first_unit_debit']['id']
        assert after['machine']['billing_unit_seconds']==3600 and after['exclusions']==0
        w.h.event('clamped_settlement_observed',case=case,operation=op,admitted_sats=26,fixture_remaining=remaining,customer_debit=remaining,available_unchanged=True,first_unit_reference=bool(after['machine']['first_unit_debit_id']),all_effects_repeat_stable=True)
    w.h.sql("UPDATE provisiond.rates SET value='%s',available=true WHERE currency='USD'"%json.dumps(original_rate))
    w.h.sql("UPDATE provisiond.offer_listings SET offers='%s',read_at=clock_timestamp()"%json.dumps([w.offer]));w.provider_control(offers={'sim-a':[w.offer],'sim-b':[w.offer]})
    w.h.results.append(dict(test='W4-rational-floor-clamp-deficiencies',result='pass',cases=3))


CASES['w4_settlement']=settlement


def guards(w):
    # API-63: a status change after the coherent tail snapshot must win the
    # conditional write; the retryability comes from its locked refusing status.
    w.stop_engine();t=funded(w,'admission-guard');account=t['account']
    for status,retryable in [('account_unreachable',True),('terminated',False)]:
        key='admission-guard-'+status;body=w.create_body(t)
        statement=w.h.proxy.arm('SELECT status FROM provisiond.provider_account_status WHERE provider_account=$1 FOR UPDATE','send')
        before=w.h.sql(f"SELECT (SELECT count(*) FROM provisiond.operations WHERE tenant_id='{t['tenant_id']}')||':'||(SELECT count(*) FROM provisiond.commitments WHERE tenant_id='{t['tenant_id']}')")
        with concurrent.futures.ThreadPoolExecutor(max_workers=1) as pool:
            future=pool.submit(http,w.h.ports[4],'POST','/v1/machines',body,t['spending_token'],key)
            statement.wait();w.h.sql(f"UPDATE provisiond.provider_account_status SET status='{status}' WHERE provider_account='{account}'");statement.release()
            code,value,_=future.result(timeout=8)
        assert code==409 and value['error']['retryable']==retryable and value['error']['details']['reason']=='state'
        assert w.h.sql(f"SELECT count(*) FROM provisiond.idempotency_records WHERE scope_id='{t['tenant_id']}' AND key='{key}'")=='0'
        assert w.h.sql(f"SELECT (SELECT count(*) FROM provisiond.operations WHERE tenant_id='{t['tenant_id']}')||':'||(SELECT count(*) FROM provisiond.commitments WHERE tenant_id='{t['tenant_id']}')")==before
        w.h.sql(f"UPDATE provisiond.provider_account_status SET status='healthy' WHERE provider_account='{account}'")
        w.h.event('admission_account_guard_race_observed',refusing_status=status,retryable=retryable,admission_effects=0)
    # OPS-43 step 8's ordinary claim guard is observed on the actual dispatcher.
    op=w.request('POST','/v1/machines',w.create_body(t),t,'dispatch-stale-step8',status=202)['id']
    hold=w.control/'hold-step-8';hold.touch();w.start_engine()
    eventually(lambda:any(e.get('operation')==op and e.get('step')==8 for e in w.events('engine','dispatch_step_held')))
    w.h.sql(f"UPDATE provisiond.operations SET claim_number=claim_number+1 WHERE id='{op}'")
    snapshot=w.h.sql(f"SELECT o.status||':'||o.claim_number||':'||c.reserved_sats||':'||(o.about_to_order_at IS NULL)||':'||(o.correlator_value IS NULL) FROM provisiond.operations o JOIN provisiond.commitments c ON c.id=o.commitment_id WHERE o.id='{op}'")
    hold.unlink();eventually(lambda:any(e.get('operation')==op for e in w.events('engine','dispatch_license_refused')));w.stop_engine()
    assert w.h.sql(f"SELECT o.status||':'||o.claim_number||':'||c.reserved_sats||':'||(o.about_to_order_at IS NULL)||':'||(o.correlator_value IS NULL) FROM provisiond.operations o JOIN provisiond.commitments c ON c.id=o.commitment_id WHERE o.id='{op}'")==snapshot
    assert not [e for e in w.external()['events'] if e.get('command')==op and e.get('path') in ['/keys','/order']]
    assert w.h.sql(f"SELECT count(*) FROM provisiond.ledger_entries WHERE operation_id='{op}'")=='0'
    w.h.event('dispatch_step8_zero_row_observed',operation=op,claim_guard_refused=True,marker_unchanged=True,provider_mutations=0)
    w.h.results.append(dict(test='W4-admission-race-dispatch-zero-row',result='pass'))


def suspended_attach(w):
    # OPS-27: licensed orders may finish after suspension; attachment and the
    # cancellation enqueue commit together. Full cancellation execution is deferred.
    from core import core_probe
    w.stop_engine();t=funded(w,'suspended-attach')
    op=w.request('POST','/v1/machines',w.create_body(t),t,'accepted-then-suspended',status=202)['id']
    w.h.run(['cargo','build','--example','core-probe','--features','store-test-hooks','--locked'],timeout=120)
    hold=w.h.root/'provider/hold-after-create';hold.touch()
    p=core_probe(w.h);p.command('engine_start');p.command('dispatch_provider')
    eventually(lambda:any(e.get('command')==op and e['stage']=='resource_created' for e in w.external()['events']))
    w.h.sql(f"UPDATE provisiond.tenants SET status='suspended' WHERE id='{t['tenant_id']}'");hold.unlink();w.wait_status(op,'succeeded')
    snapshot=json.loads(w.h.sql(f"SELECT jsonb_build_object('parent',o.status,'machine',m.id,'unit_debit',m.first_unit_debit_id,'setup_count',(SELECT count(*) FROM provisiond.ledger_entries WHERE operation_id=o.id AND kind='setup_fee_debit'),'cancellations',(SELECT jsonb_agg(jsonb_build_object('kind',c.kind,'status',c.status,'requested_by',c.requested_by,'reason',c.system_reason,'episode_id',c.episode_id)) FROM provisiond.operations c WHERE c.machine_id=m.id AND c.kind='delete'))::text FROM provisiond.operations o JOIN provisiond.machines m ON m.id=o.machine_id WHERE o.id='{op}'"))
    assert snapshot['setup_count']==1 and snapshot['unit_debit']
    assert len(snapshot['cancellations'])==1
    cancellation=snapshot['cancellations'][0]
    assert cancellation['status']=='queued' and cancellation['requested_by']=='system' and cancellation['reason']=='tenant_suspended' and cancellation['episode_id']
    p.finish()
    w.h.event('accepted_suspended_attach_observed',operation=op,create_settled=True,machine_attached=True,setup_and_unit_debits=True,system_cancellation_enqueued=True,cancellation_execution='not run')
    w.h.results.append(dict(test='W4-accepted-suspended-attach-enqueue',result='pass',limits='cancellation execution not run'))


CASES.update(w4_guards=guards,w4_suspended_attach=suspended_attach)


def no_rate_projection(w):
    # LDG-33/40: an accepted effect still settles; only re-derivation halts.
    w.stop_engine();t=funded(w,'no-rate-projection')
    op=w.request('POST','/v1/machines',w.create_body(t),t,'no-rate-accepted-create',status=202)['id']
    hold=w.control/'hold-step-9';hold.touch();w.start_engine()
    eventually(lambda:any(e.get('operation')==op and e.get('step')==9 for e in w.events('engine','dispatch_step_held')))
    w.h.sql("UPDATE provisiond.rates SET available=false WHERE currency='USD'");hold.unlink()
    w.wait_status(op,'succeeded');w.stop_engine()
    view=json.loads(w.h.sql(f"SELECT jsonb_build_object('runway',m.runway_until,'customer_num',m.customer_rate_num,'customer_den',m.customer_rate_den,'protected',m.protected_sats,'first_unit_paid',m.first_unit_debit_id IS NOT NULL,'eligible_seed',m.meter_seed_at=m.created_at+interval '3600 seconds' AND m.state_observed_at<=m.created_at AND EXISTS(SELECT 1 FROM provisiond.meter_totals mt WHERE mt.subject_id=m.id AND mt.high_water_mark=m.meter_seed_at),'debits',(SELECT sum(-amount_sats) FROM provisiond.ledger_entries WHERE operation_id=o.id),'exhaustion_cancellations',(SELECT count(*) FROM provisiond.operations WHERE machine_id=m.id AND system_reason='exhausted'))::text FROM provisiond.operations o JOIN provisiond.machines m ON m.id=o.machine_id WHERE o.id='{op}'"))
    w.h.event('no_rate_settlement_projection_observed',operation=op,settled=True,**view)
    assert view['runway'] is None and view['customer_num'] is None and view['customer_den'] is None and view['protected'] is None,'unavailable current rate must halt runway projection'
    assert view['first_unit_paid'] and view['eligible_seed'] and view['debits']==31 and view['exhaustion_cancellations']==0
    w.h.sql("UPDATE provisiond.rates SET available=true WHERE currency='USD'")
    w.h.results.append(dict(test='W4-no-rate-accepted-settlement',result='pass',limits='later ordinary runway rederivation not run'))


CASES['w4_no_rate']=no_rate_projection


def absent_rate(w):
    # The no-current-rate case also includes no rates row, not only unavailable.
    w.stop_engine();t=funded(w,'absent-rate')
    op=w.request('POST','/v1/machines',w.create_body(t),t,'absent-rate-accepted-create',status=202)['id']
    hold=w.control/'hold-step-9';hold.touch();w.start_engine()
    eventually(lambda:any(e.get('operation')==op and e.get('step')==9 for e in w.events('engine','dispatch_step_held')))
    w.h.sql("DELETE FROM provisiond.rates WHERE currency='USD'");hold.unlink()
    eventually(lambda:w.status(op)=='succeeded' or 'index out of bounds' in w.engine.error.read_text())
    state=w.status(op);panic='index out of bounds' in w.engine.error.read_text()
    w.h.event('absent_rate_settlement_observed',operation=op,state=state,index_panic=panic)
    assert state=='succeeded' and not panic,'absent current rate must still settle accepted effect'
    w.stop_engine()
    view=json.loads(w.h.sql(f"SELECT jsonb_build_object('runway',m.runway_until,'customer_num',m.customer_rate_num,'paid',m.first_unit_debit_id IS NOT NULL) FROM provisiond.operations o JOIN provisiond.machines m ON m.id=o.machine_id WHERE o.id='{op}'"))
    assert view['runway'] is None and view['customer_num'] is None and view['paid']
    w.h.sql("INSERT INTO provisiond.rates VALUES('USD','%s',true)"%json.dumps(w.rate))
    w.h.results.append(dict(test='W4-absent-rate-accepted-settlement',result='pass'))


from workflow import stop_cost_adapter
CASES.update(w4_no_rate_absent=absent_rate,w4_stop_cost=stop_cost_adapter)


def admission_record_fields(w, category):
    # OPS-23/43: API-written accepted authority/conversion validation is step 3,
    # before even a read-only List offers request; edited records are untrusted.
    w.stop_engine();t=funded(w,'accepted-record-'+category)
    if category=='reserve':
        cases=[('missing',None,None),('zero',None,0),('negative',None,-1),('mismatch',None,42)]
    elif category=='rate':
        base=['offer','accepted_terms','rate']
        cases=[('missing_num',base+['rate_num'],None),('zero_num',base+['rate_num'],0),
               ('zero_den',base+['rate_den'],0),('negative_den',base+['rate_den'],-1),
               ('bad_haircut',base+['haircut_bps'],10001),('missing_haircut',base+['haircut_bps'],None),
               ('missing_source',base+['source'],None),('empty_source',base+['source'],''),
               ('bad_observation',base+['observed_at'],'invalid-time'),('missing_observation',base+['observed_at'],None)]
    elif category=='offer':
        cases=[('strategies',['offer','install_strategies'],['provider_catalogue']),
               ('missing_bound',['offer','declared_cancellation_bound'],None),
               ('null_bound',['offer','declared_cancellation_bound'],None),
               ('wrong_bound',['offer','declared_cancellation_bound'],'1970-01-01T00:00:00Z'),
               ('period',['offer','accepted_terms','offer','period_seconds'],7200)]
    else:
        cases=[('setup_sats',['setup_fee','sats'],1),('setup_rate',['setup_fee','rate_num'],11),
               ('setup_haircut',['setup_fee','haircut_bps'],1),('setup_source',['setup_fee','rate_source'],'different-source'),
               ('unit_sats',['first_unit','sats'],1),('unit_rounding',['first_unit','rounding_rule_version'],'floor-v1'),
               ('unit_seconds',['first_unit','seconds'],0)]
    assertion='accepted '+category+' fields must fail incomplete_record before List offers'
    for name,path,value in cases:
        op=w.request('POST','/v1/machines',w.create_body(t),t,'accepted-record-'+category+'-'+name,status=202)['id']
        if path is None:
            literal='NULL' if value is None else str(value)
            w.h.sql(f"UPDATE provisiond.operations SET admitted_sats={literal} WHERE id='{op}'")
        elif name=='missing_bound':
            w.h.sql(f"UPDATE provisiond.operations SET request_summary=request_summary #- '{{offer,declared_cancellation_bound}}' WHERE id='{op}'")
        else:
            pointer='{'+','.join(path)+'}'
            w.h.sql("UPDATE provisiond.operations SET request_summary=jsonb_set(request_summary,'%s','%s'::jsonb) WHERE id='%s'"%(pointer,json.dumps(value),op))
        w.start_engine();eventually(lambda:w.status(op) in ['failed','succeeded','needs_reconciliation']);w.stop_engine()
        view=w.request('GET','/v1/operations/'+op,tenant=t)
        requests=[e['path'] for e in w.external()['events'] if e.get('command')==op and e['stage']=='request_received']
        steps=[e['step'] for e in w.events('engine','dispatch_step') if e.get('operation')==op]
        error=view['error'] or {}
        w.h.event('accepted_record_validation_observed',category=category,corruption=name,operation=op,state=view['status'],reason=error.get('details',{}).get('reason'),provider_requests=requests,steps=steps)
        assert view['status']=='failed' and error.get('kind')=='conflict' and error.get('details',{}).get('reason')=='incomplete_record' and error.get('retryable') is True and view['retryable'] is True and steps==[1,2,3] and requests==[],assertion
        assert w.h.sql(f"SELECT reserved_sats||':'||state FROM provisiond.commitments WHERE operation_id='{op}'")=='0:closed'
        assert w.h.sql(f"SELECT count(*) FROM provisiond.ledger_entries WHERE operation_id='{op}'")=='0'
        assert w.h.sql(f"SELECT about_to_order_at IS NULL AND correlator_value IS NULL FROM provisiond.operations WHERE id='{op}'")=='t'
    w.h.results.append(dict(test='W4-accepted-'+category+'-record-validation',result='pass',cases=len(cases)))


def reserve_record(w):admission_record_fields(w,'reserve')
def rate_record(w):admission_record_fields(w,'rate')
def conversion_record(w):admission_record_fields(w,'conversion')
CASES.update(w4_reserve_record=reserve_record,w4_rate_record=rate_record,w4_conversion_record=conversion_record)


def offer_record(w):admission_record_fields(w,'offer')
CASES['w4_offer_record']=offer_record

from w4_round2 import CASES as ROUND2_CASES
CASES.update(ROUND2_CASES)

# Run after default startup/lifecycle checks to verify their build isolation too.
from w4_observation import observation
CASES['w4_observation']=observation

from w4_preflight import preflight
CASES['w4_preflight']=preflight


def provider_refusal(w):
    """PRV-5, OPS-11/12/43, LDG-32/35/39, STO-3/58: definitive step-9 rejection."""
    from w4_publication import state, operation_events
    h=w.h;w.stop_engine();t=funded(w,'provider-refusal');account=t['account']
    original=json.loads(h.sql(f"SELECT descriptor FROM provisiond.provider_accounts WHERE account='{account}'"))
    descriptor=dict(original,offer_is_resource=True,temporary_keys=True)
    h.sql("UPDATE provisiond.provider_accounts SET descriptor='%s' WHERE account='%s'"%(json.dumps(descriptor),account))
    w.restart_api();w.provider_control(mode='provider_400')
    body=w.create_body(t);key='definitive-provider-refusal'
    before=w.request('GET','/v1/balance',tenant=t)
    admission=w.request('POST','/v1/machines',body,t,key,status=202);op=admission['id']
    admitted=state(w,op)
    commitment=h.sql(f"SELECT commitment_id FROM provisiond.operations WHERE id='{op}'")
    balance=w.request('GET','/v1/balance?commitments=true',tenant=t)
    assert before['available_sats']==115 and balance['available_sats']==72
    assert admitted['reserved']==43 and admitted['commitment_state']=='open'
    assert any(c['operation_id']==op and c['reserved_sats']==43 for c in balance['commitments'])
    assert h.sql(f"SELECT count(*) FROM provisiond.resource_exclusions WHERE operation_id='{op}'")=='1'
    h.event('provider_refusal_admitted',operation=op,commitment=commitment,state=admitted,
            available_before=before['available_sats'],available_admitted=balance['available_sats'],exclusions=1)
    # Hold the ordinary settlement statement, after the real client returned its
    # retained error, while the original admitted money/exclusion still exist.
    gate=h.proxy.arm('SET status=$3,result=','send')
    try:
        w.start_engine();gate.wait()
        eventually(lambda:any(e['stage']=='response_returned' for e in operation_events(w,op) if e['path']=='/order'))
        records=operation_events(w,op)
        order=[e for e in records if e['path']=='/order']
        assert [e['stage'] for e in order]==['request_received','order_error_selected','response_returned']
        assert order[-1]['error']['details']=={'status':400,'accepted':False}
        assert any(e.get('operation')==op for e in w.events('engine','dispatch_license_acknowledged'))
        assert [e['step'] for e in w.events('engine','dispatch_step') if e.get('operation')==op]==list(range(1,10))
        assert state(w,op)['reserved']==43 and state(w,op)['status']=='running'
        assert w.request('GET','/v1/balance',tenant=t)['available_sats']==72
        assert op not in w.external()['keys'] and any(e['stage']=='key_deleted' for e in records)
        assert not any(e['stage'] in ('accepted','resource_created') for e in records)
    finally:gate.release()
    eventually(lambda:any(e.get('operation')==op for e in w.events('engine','settlement_finished')))
    w.stop_engine();stored=state(w,op)
    # Read both endpoints before the intended red: baseline must first establish
    # the real client's canonical provider/400/accepted-false error, not a hook.
    detail=w.request('GET','/v1/operations/'+op,tenant=t)
    listed=next(v for v in w.request('GET','/v1/operations?limit=100',tenant=t)['operations'] if v['id']==op)
    after=w.request('GET','/v1/balance',tenant=t)
    h.event('provider_refusal_settlement_observed',operation=op,commitment=commitment,
            state=stored,detail=detail,listed=listed,available_after=after['available_sats'],order=order)
    assert stored['error']['kind']=='provider' and stored['error']['details']['status']==400 and stored['error']['details']['accepted'] is False
    assert stored['status']=='failed', 'OPS-11 provider/400/accepted-false order must settle failed and release original commitment'
    expected={'kind':'provider','message':'simulator request refused','retryable':True,'details':{'status':400,'accepted':False}}
    assert stored['error']==expected and detail==listed
    assert w.operation_reads(t,op,'provider',True,expected['details'])==detail
    assert detail['status']=='failed' and detail['terminal'] is True
    assert stored['reserved']==0 and stored['commitment_state']=='closed'
    assert stored['machine'] is None and stored['debits']==0 and stored['result'] is None
    assert h.sql(f"SELECT count(*) FROM provisiond.machines WHERE tenant_id='{t['tenant_id']}'")=='0'
    assert h.sql(f"SELECT count(*) FROM provisiond.resource_exclusions WHERE operation_id='{op}'")=='0'
    assert after==before and after['available_sats']==115
    snapshot=state(w,op);events=operation_events(w,op)
    closed_commitment=h.sql(f"SELECT to_jsonb(c) FROM provisiond.commitments c WHERE id='{commitment}'")
    assert h.sql(f"SELECT count(*) FROM provisiond.idempotency_records WHERE resource_id='{op}'")=='1'
    for restart in (False,True):
        if restart:w.restart_api();w.start_engine();w.stop_engine()
        assert w.request('POST','/v1/machines',body,t,key,status=202)==detail
        assert state(w,op)==snapshot and operation_events(w,op)==events
        assert h.sql(f"SELECT to_jsonb(c) FROM provisiond.commitments c WHERE id='{commitment}'")==closed_commitment
        assert w.request('GET','/v1/balance',tenant=t)==before
        assert w.operation_reads(t,op,'provider',True)==detail
        assert h.sql(f"SELECT commitment_id FROM provisiond.operations WHERE id='{op}'")==commitment
    # Reuse the freed exclusion on a distinct, successful ordinary create.
    w.provider_control(mode='normal')
    successor=w.request('POST','/v1/machines',body,t,'after-definitive-refusal',status=202)['id']
    w.start_engine();w.wait_status(successor,'succeeded');w.stop_engine()
    assert state(w,successor)['debits']==2
    assert [e['stage'] for e in operation_events(w,successor) if e['path']=='/order']==['request_received','accepted','resource_created','response_returned']
    assert state(w,op)==snapshot and operation_events(w,op)==events
    h.event('provider_refusal_replay_restart_observed',operation=op,commitment=commitment,
            unchanged_state=snapshot,closed_commitment=json.loads(closed_commitment),order_requests=1,
            commitment_unchanged_on_replay=True,restored_available_sats=115,successor=successor)
    h.sql("UPDATE provisiond.provider_accounts SET descriptor='%s' WHERE account='%s'"%(json.dumps(original),account))
    w.restart_api();h.results.append(dict(test='W4-definitive-provider-refusal',result='pass'))


def provider_ambiguity(w):
    """PRV-5, OPS-11/12/13/16, LDG-39/67: dispatched ambiguity never frees money."""
    from w4_publication import state, operation_events
    h=w.h;w.stop_engine()
    for mode in ('provider_500','provider_no_status','search_provider_400'):
        t=funded(w,'ambiguity-'+mode);w.provider_control(mode=mode)
        body=w.create_body(t);key='ambiguity-'+mode
        admission=w.request('POST','/v1/machines',body,t,key,status=202);op=admission['id']
        admitted=state(w,op)
        w.start_engine();eventually(lambda:any(e.get('operation')==op for e in w.events('engine','settlement_finished')));w.stop_engine()
        stored=state(w,op);view=w.operation_reads(t,op,'provider',False)
        assert stored['status']=='needs_reconciliation' and stored['error']==view['error']
        assert stored['reserved']==admitted['reserved']==43 and stored['commitment_state']=='open'
        assert stored['debits']==0 and stored['machine'] is None
        assert w.request('GET','/v1/balance',tenant=t)['available_sats']==72
        details=stored['error']['details'];assert details['attempted']=='create_machine'
        records=operation_events(w,op);order=[e for e in records if e['path']=='/order']
        if mode=='search_provider_400':
            assert details['accepted'] is True and details['status']==400
            assert [e['stage'] for e in order]==['request_received','accepted','resource_created','response_returned']
            assert details['provider_identifiers']['order_reply']==[order[-1]['resource']]
            assert any(e['stage']=='verification_fault_returned' and e['error']['details']['accepted'] is False for e in records)
        else:
            assert details['accepted'] is None
            assert details.get('status')==(500 if mode=='provider_500' else None)
            assert [e['stage'] for e in order]==['request_received','order_error_selected','response_returned']
        assert any(e.get('operation')==op for e in w.events('engine','dispatch_license_acknowledged'))
        assert w.request('POST','/v1/machines',body,t,key,status=202)==view
        w.start_engine();w.stop_engine()
        assert state(w,op)==stored and operation_events(w,op)==records
        assert w.operation_reads(t,op,'provider',False)==view
        h.event('provider_ambiguity_guard_observed',mode=mode,operation=op,state=stored,
                available_sats=72,order=order,restart_unchanged=True,second_order=False)
    w.provider_control(mode='normal');h.results.append(dict(test='W4-dispatched-provider-ambiguity',result='pass'))


def mixed_pacing(w):
    """API-3/7/29/50, WIR-4/5/9a: one source, independent valid caller budgets."""
    h=w.h;w.stop_engine();a=funded(w,'mixed-a');b=funded(w,'mixed-b')
    pending=w.enrol();suspended=w.enrol()
    h.sql(f"UPDATE provisiond.tenants SET status='suspended' WHERE id='{suspended['tenant_id']}'")
    interval=10000;limit=30;writes=2
    w.restart_api(dict(PARAMETERS,rate_interval_ms=interval,read_limit=limit,write_limit=writes))
    started=time.monotonic();observations=[]
    def call(label,method,path,token=None,body=None,key=None,raw=None):
        code,value,headers=http(h.ports[4],method,path,body,token,key,raw)
        assert value.get('correlation_id',headers['X-Correlation-Id'])==headers['X-Correlation-Id']
        assert 'Retry-After' in headers['Access-Control-Expose-Headers']
        if code>=400:assert set(value)=={'error','correlation_id'} and set(value['error'])=={'kind','message','retryable','details'}
        if code==429:
            hint=value['error']['details']['retry_after_ms']
            assert value['error']['kind']=='rate_limited' and value['error']['retryable'] is True
            assert 0<hint<=interval and headers['Retry-After']==str((hint+999)//1000)
        observation=dict(label=label,method=method,status=code,elapsed_ms=round((time.monotonic()-started)*1000),
                         error=value.get('error'),retry_after=headers.get('Retry-After'))
        observations.append(observation);h.event('mixed_pacing_response',**observation)
        return code,value
    for i,token in enumerate((None,'unknown-one',a['recovery_credential'],'rotated-unknown')):
        code,value=call('invalid-write-'+str(i),'POST','/v1/deposits',token,key='mixed-invalid',raw=b'{')
        assert code==(401 if i<writes else 429)
    h.event('mixed_source_exhausted',budget='write',limit=writes,statuses=[v['status'] for v in observations],interval_ms=interval)
    # The baseline red must be exactly this valid request after proven exhaustion.
    code,deposit=call('valid-a-first-write','POST','/v1/deposits',a['spending_token'],{'amount_sats':1},'mixed-a-first')
    assert code==200, 'API-50 valid caller must reach independent write budget after failed-auth source exhaustion'
    assert deposit['requested_sats']==1
    for name,t in [('a',a),('b',b)]:
        for i in range(1 if name=='a' else 0,writes):
            code,value=call('valid-'+name+'-write-'+str(i),'POST','/v1/deposits',t['spending_token'],{'amount_sats':1},'mixed-'+name+'-'+str(i))
            assert code==200, 'unexpected 429 or refusal before valid principal write allowance exhausted'
            assert value['requested_sats']==1 and value['id']!=deposit['id']
        assert call('exhausted-'+name+'-write','POST','/v1/deposits',t['spending_token'],{'amount_sats':1},'mixed-'+name+'-exhausted')[0]==429
    # Source reads have their own full allowance despite exhausted source writes.
    for i in range(limit+2):
        token=(None,'unknown-read-'+str(i),a['recovery_credential'])[i%3]
        assert call('invalid-read-'+str(i),'GET','/v1/balance',token)[0]==(401 if i<limit else 429)
    h.event('mixed_source_exhausted',budget='read',limit=limit,interval_ms=interval)
    assert call('valid-pending','POST','/v1/machines',pending['spending_token'],raw=b'{')[1]['error']['kind']=='not_activated'
    assert call('valid-suspended','POST','/v1/deposits',suspended['spending_token'],{'amount_sats':1},'mixed-suspended')[1]['error']['kind']=='suspended'
    paths=['/v1/operations?terminal=false','/v1/balance','/v1/machines']
    for name,t in [('a',a),('b',b)]:
        # Deliberate burst to exhaust this principal, not obedient polling. The
        # separate pacing case verifies sustained advertised-cadence cycles.
        for i in range(limit):
            code,value=call('valid-'+name+'-read-'+str(i),'GET',paths[i%3],t['spending_token'])
            assert code==200, 'unexpected 429 or refusal before valid principal read allowance exhausted'
            if i%3==0:assert value['operations']==[] and value['poll_after_ms']==1000
            elif i%3==1:assert value['available_sats']==115
            else:assert value['machines']==[]
        assert call('exhausted-'+name+'-read','GET','/v1/balance',t['spending_token'])[0]==429
    assert call('still-invalid-write','POST','/v1/deposits','new-invalid-spelling',raw=b'{')[0]==429
    assert call('still-invalid-read','GET','/v1/balance','new-invalid-read')[0]==429
    elapsed=round((time.monotonic()-started)*1000)
    assert elapsed<interval, 'mixed traffic must complete in one source window; no accidental allowance reset'
    h.event('mixed_pacing_observed',interval_ms=interval,read_limit=limit,write_limit=writes,
            advertised_cadence_ms=1000,required_reads_per_window=30,elapsed_ms=elapsed,principals=2,
            valid_reads_each=limit,valid_writes_each=writes,observations=observations)
    w.restart_api();h.results.append(dict(test='W4-mixed-source-principal-pacing',result='pass'))


CASES.update({'w4_provider_refusal':provider_refusal,'w4_provider_ambiguity':provider_ambiguity,'w4_mixed_pacing':mixed_pacing})
