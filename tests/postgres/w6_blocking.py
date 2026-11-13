"""E1–E3: bounded retirement, account-wide credential stop and STO-37 fields."""
import json
import time
from harness import eventually
from workflow import http
from w5 import admit, external, events, snapshot
from w6_continuation import dedicated_account, restore_accounts

RETIRE_ASSERTION='E1 STO-14 actual retirement must stay live within statement bound'
FIELD_ASSERTION='E3 STO-37 non-clamp deficiency must have null clamped_sats'


def deficiency_fields(w,op,state):
    rows=state['deficiencies']
    w.h.event('w6_deficiency_fields',scenario=w.case,operation=op,rows=rows,
              assertion=FIELD_ASSERTION,producer_observed=bool(rows))
    assert all(d['clamped_sats'] is None for d in rows if d['cause']!='clamp_overflow'),FIELD_ASSERTION
    assert all(d['clamped_sats'] is not None and d['clamped_sats']>0 for d in rows if d['cause']=='clamp_overflow')
    for d in rows:
        if d['cause'] in ('unrecoverable_setup_fee','unrecoverable_first_unit'):
            charge=state['operation']['request_summary']['setup_fee' if d['cause']=='unrecoverable_setup_fee' else 'first_unit']
            assert d['native_minor']==charge['native_minor'] and d['currency']==charge['currency']
        assert d['native_minor']>0 and d['provider_account']
        if d['cause']!='rate_outage':assert d['absorbed_seconds']==0


def retirement(w):
    """STO-14/24/25/33, WIR-27: activate the real due transaction first."""
    from w6 import create
    from w4_retention_scale import sql,snapshot,completions
    saved=dedicated_account(w,'sim-retirement')
    t,_,_=create(w,'retirement-genuine')
    w.api.terminate();w.api.wait(timeout=5)
    # Fixed extra history: no planner overrides, runtime or timeout changes.
    sql(w,"""
      INSERT INTO provisiond.operations(id,tenant_id,idempotency_key,kind,status,request_summary,correlation_id)
      SELECT md5('retire-'||i)::uuid,'retire-tenant-'||(i%100),'retire-'||i,'create','succeeded','{}','retire-scale'
      FROM generate_series(1,20000) i;
      INSERT INTO provisiond.idempotency_records(scope_kind,scope_id,key,fingerprint,resource_kind,resource_id,status,response_body,expires_at)
      SELECT 'tenant','retire-history','retire-'||i,'fixture','operation',md5('retire-'||i)::uuid,202,'{}',
        CASE WHEN i<=990 THEN statement_timestamp()-interval '1 second' ELSE statement_timestamp()+interval '1 day' END
      FROM generate_series(1,200000) i;
      ANALYZE provisiond.operations; ANALYZE provisiond.idempotency_records;
    """)
    before=snapshot(w);start=len(w.h.events)
    began=time.monotonic();w.restart_api()
    def remaining():return int(sql(w,"SELECT count(*) FROM provisiond.operations WHERE correlation_id='retire-scale'"))
    eventually(lambda:w.api.poll() is not None or remaining()==19010,seconds=15)
    if w.api.poll() is not None:w.api.wait(timeout=5)
    after=snapshot(w);left=remaining();diagnostic=w.api.error.read_text()
    statements=[e for e in w.h.events[start:] if 'DELETE FROM provisiond.operations' in e.get('query','')]
    failures=[e for e in statements if e['event']=='protocol_response' and e.get('tag')=='E']
    sqlstates=[e['sqlstate'] for e in w.h.events[start:] if e['event']=='server_error_forwarded']
    retired=json.loads(sql(w,"SELECT COALESCE(jsonb_agg(to_jsonb(r) ORDER BY id),'[]') FROM provisiond.retired_operations r WHERE id IN (SELECT md5('retire-'||i)::uuid FROM generate_series(1,990) i)"))
    w.h.event('w6_retirement_bound',assertion=RETIRE_ASSERTION,api_exit=w.api.poll(),
        elapsed_seconds=time.monotonic()-began,expected_removed=990,remaining=left,
        due_query_sent=any(e['event']=='bytes_transmitted' for e in statements),
        due_query_errors=failures,sqlstates=sqlstates,statement_ms=1000,
        housekeeping_failure='API housekeeping failed' in diagnostic,
        statement_timeout='canceling statement due to statement timeout' in diagnostic,
        state_rolled_back=(left==20000 and not retired),retired=retired,
        financial_history_unchanged=before==after)
    assert w.api.poll() is None and left==19010,RETIRE_ASSERTION
    assert not failures and len(retired)==990 and before==after
    assert sql(w,"SELECT count(*) FROM provisiond.retired_operations o JOIN provisiond.idempotency_records r ON r.resource_kind='operation' AND r.resource_id=o.id WHERE r.scope_id='retire-history' AND o.retained_until<>r.expires_at")=='0'
    # After the actual acknowledged retirement, both customer and job stay live.
    assert http(w.h.ports[4],'GET','/v1/balance',token=t['spending_token'])[0]==200
    count=len(completions(w.h,0,'SELECT to_json(EXISTS(SELECT 1 FROM provisiond.enrolments'))
    eventually(lambda:len(completions(w.h,0,'SELECT to_json(EXISTS(SELECT 1 FROM provisiond.enrolments'))>count)
    restore_accounts(w,saved)


def credential(w,site):
    """PRV-41/5/7/9, OPS-11/12/43/45: independently select each call."""
    from w6 import create,deletion,operation_state,proof
    account='sim-terminal-'+site.strip('/').replace('/','-')
    saved=dedicated_account(w,account)
    if site=='/delete':
        t,created,machine=create(w,'terminal-delete-machine')
        # A system cleanup exercises the actual stalled episode and its fence.
        from core import core_probe
        w.h.run(['cargo','build','--example','core-probe','--features','store-test-hooks','--locked'],timeout=120)
        w.h.sql(f"UPDATE provisiond.tenants SET status='suspended' WHERE id='{t['tenant_id']}'")
        p=core_probe(w.h);p.command('engine_start');episode=p.command('join_cleanup '+machine)['value']['episodes'][0];p.finish()
        op=w.h.sql(f"SELECT current_operation_id FROM provisiond.episodes WHERE id='{episode}'")
    else:
        t,_,op=admit(w,'terminal-'+site.strip('/').replace('/','-'))
    if site in ('/keys','/keys/delete'):
        w.h.sql(f"UPDATE provisiond.provider_accounts SET descriptor=jsonb_set(descriptor,'{{temporary_keys}}','true') WHERE account='{account}'")
    if site=='/keys/delete':w.provider_control(mode='provider_400')
    # Another real, already queued create must be refused before provider I/O.
    queued_t,_,queued=admit(w,'terminal-queued-'+op)
    w.provider_control(authentication_fault=dict(account=account,path=site,operation=op))
    before=len(w.external()['events'])
    # One worker makes the pending-work ordering deterministic; no runtime bound changes.
    p=w.start('engine',dict(PROVISIOND_TEST_CONTROL=str(w.control),PROVISIOND_WORKERS='1'));w.engine=p
    eventually(lambda:'restart_pass_complete' in p.log.read_text() or p.poll() is not None)
    assert p.poll() is None,p.error.read_text()
    eventually(lambda:any(e['stage']=='authentication_rejected' and e['command']==op and e['path']==site for e in w.external()['events'][before:]))
    w.wait_status(op,'failed');eventually(lambda:w.status(queued) in ('failed','succeeded','needs_reconciliation'));time.sleep(2.3)
    state=operation_state(w,op)
    observed=w.external()['events'][before:]
    rejected=[e for e in observed if e['stage']=='authentication_rejected' and e['account']==account]
    status=w.h.sql(f"SELECT status FROM provisiond.provider_account_status WHERE provider_account='{account}'")
    assertion='E2 PRV-41 '+site+' rejects credential durably'
    w.h.event('w6_terminal_credential',site=site,operation=op,queued=queued,assertion=assertion,
        credential_status=status,provider=observed,state=state,queued_state=operation_state(w,queued),trace=events(p))
    assert rejected and rejected[0]['command']==op
    assert status=='credentials_rejected' and len(rejected)==1,assertion
    assert not any(e['stage']=='request_received' and e['account']==account for e in observed[observed.index(rejected[0])+1:])
    assert not external(w,queued)
    assert any(e.get('event')=='provider_credentials_rejected' and e.get('path')==site and e.get('provider_account')==account for e in events(p))
    assert not state['ledger'] if site!='/delete' else state['machine']['meter_stopped_at'] is None
    if site=='/delete':
        assert state['operation']['write_started_at'] is not None
        assert state['episodes'][-1]['state']=='stalled' and state['machine']['destroy_committed']==episode
    else:
        assert not any(e['stage']=='accepted' for e in external(w,op))
        assert all(c['state']=='closed' and c['reserved_sats']==0 for c in state['commitments'])
        assert (state['operation']['about_to_order_at'] is None)==(site=='/offers')
        assert not state['machine'] and not snapshot(w,op)['exclusions']
    if site=='/keys':assert op not in w.external()['keys']
    if site in ('/order','/keys/delete'):
        assert not any(e['stage']=='key_deleted' for e in external(w,op))
    if site=='/keys/delete':assert op in w.external()['keys'] and any(e['stage']=='key_registered' for e in external(w,op))
    # Rejected credentials stay stopped after ordinary successor and fault removal.
    w.stop_engine();w.provider_control(mode='normal',authentication_fault=None)
    code,refusal,_=http(w.h.ports[4],'POST','/v1/machines',w.create_body(queued_t),queued_t['spending_token'],'terminal-subsequent-'+op)
    assert code==409 and refusal['error']['details']['reason']=='state'
    w.h.sql("UPDATE provisiond.provider_accounts SET assignable=(account='sim-b')")
    w.restart_api();healthy,_,healthy_op=admit(w,'terminal-healthy-'+op)
    w.start_engine();w.wait_status(healthy_op,'succeeded');time.sleep(1.2);w.stop_engine()
    later=w.external()['events'][before:]
    assert not any(e['stage']=='request_received' and e['account']==account for e in later[later.index(rejected[0])+1:])
    assert healthy['account']=='sim-b' and sum(e['stage']=='accepted' for e in external(w,healthy_op))==1
    w.h.event('w6_terminal_credential_complete',site=site,operation=op,healthy_operation=healthy_op,
        subsequent_admission=code,provider=later,credential_status=status,
        rejected_purchases=sum(e['stage']=='accepted' for e in external(w,op)))
    restore_accounts(w,saved)

def deficiency_reply(w):
    """OPS-27/49, LDG-39/66, STO-37: charged absence survives lost COMMIT."""
    from w6 import lost,proof,eligible,operation_state,one_purchase
    _,_,op,hold=lost(w,'deficiency-reply',search='normal',direct='absent')
    resource=next(e['value'] for e in external(w,op) if e['stage']=='resource_created')
    w.provider_control(remove_resource=resource['external_id'])
    proof(w,op,search='normal',direct='absent',resources=[dict(resource,setup_fee_charged=True,first_unit_charged=True)])
    eligible(w,op)
    enumeration=w.h.root/'provider/hold-enumerate';enumeration.touch()
    statement=w.h.proxy.arm('resolution=$4','send')
    w.start_engine();hold.unlink();connection=statement.wait()
    gate=w.h.proxy.arm('store COMMIT','commit_reply',session=connection['pid']);statement.release()
    try:
        gate.wait();w.wait_status(op,'failed');before=operation_state(w,op)
        deficiency_fields(w,op,before)
        assert len(before['deficiencies'])==2 and not before['ledger']
        w.h.proxy.close_connection(connection,'deficiency_resolution_reply_lost');gate.release()
        eventually(lambda:any(e['event']=='resolution_recorded' and e['operation']==op for e in events(w.engine)))
        w.stop_engine();after=operation_state(w,op)
        assert after==before,'STO-37 lost-resolution repeat must preserve exact deficiency rows'
        w.start_engine();w.stop_engine();assert operation_state(w,op)==after
        one_purchase(w,op)
        w.h.event('w6_deficiency_reply_repeat',operation=op,before=before,after=after,
            landed_commit_reply_lost=True,whole_transaction_repeated=True)
    finally:gate.release();enumeration.unlink(missing_ok=True)

BLOCKING={
    'w6_e2_offers':lambda w:credential(w,'/offers'),
    'w6_e2_keys':lambda w:credential(w,'/keys'),
    'w6_e2_order':lambda w:credential(w,'/order'),
    'w6_e2_key_cleanup':lambda w:credential(w,'/keys/delete'),
    'w6_e2_delete':lambda w:credential(w,'/delete'),
    'w6_e3_reply':deficiency_reply,
    'w6_e1':retirement,
}
