"""STO-14/24/25/33/35, API-11, DOM-21, CNF-158: keyed retention.

Real API/engine/PG and loopback effects. The identical fixture also runs on the
unchanged baseline; its only config exception is retention_seconds=2 to satisfy
that binary's older unequal-horizon startup check. No compiled probe overlay.
"""
import hashlib
import json
import os
import time

from harness import eventually
from workflow import PARAMETERS, http

HORIZON = 30
ASSERTION = 'SAME-KEY REPLAY EXPECTS EXISTING OPERATION'
WORK = 'SELECT to_json(EXISTS(SELECT 1 FROM provisiond.enrolments'
DELETE = 'DELETE FROM provisiond.operations'


def parameters():
    # Baseline and repaired generated configs differ only in this parameter VALUE.
    return dict(PARAMETERS, receipt_seconds=HORIZON,
                retention_seconds=int(os.environ.get('PROVISIOND_TEST_RETENTION_SECONDS', HORIZON)))


def receipt(w, op):
    return json.loads(w.h.sql(f"""SELECT jsonb_build_object(
      'created',min(created_at)::text,'expires',max(expires_at)::text,
      'wire',provisiond.wire_time(max(expires_at)),
      'now',statement_timestamp()::text,
      'remaining',extract(epoch FROM max(expires_at)-statement_timestamp()),
      'count',count(*),'admission_seconds',extract(epoch FROM min(expires_at)-min(created_at)))
      FROM provisiond.idempotency_records WHERE resource_kind='operation' AND resource_id='{op}'"""))


def protected(w, t):
    tenant=t['tenant_id']
    # Complete financial/history rows and counters, not only net totals.
    values={table:json.loads(w.h.sql(f"SELECT COALESCE(jsonb_agg(to_jsonb(r) ORDER BY id),'[]') FROM provisiond.{table} r WHERE tenant_id='{tenant}'"))
            for table in ['ledger_entries','commitments','machines']}
    values['counters']=json.loads(w.h.sql(f"SELECT COALESCE(jsonb_agg(to_jsonb(r) ORDER BY ceiling,interval_start),'[]') FROM provisiond.ceiling_counters r WHERE principal='{tenant}'"))
    values['episodes']=json.loads(w.h.sql("SELECT COALESCE(jsonb_agg(to_jsonb(r) ORDER BY id),'[]') FROM provisiond.episodes r"))
    values['receipts']=json.loads(w.h.sql(f"SELECT COALESCE(jsonb_agg(jsonb_build_object('scope_kind',scope_kind,'scope_id',scope_id,'key',key,'resource_id',resource_id,'fingerprint',fingerprint,'created_at',created_at,'expires_at',expires_at) ORDER BY scope_kind,scope_id,key),'[]') FROM provisiond.idempotency_records WHERE scope_id='{tenant}' OR resource_id IN (SELECT resource_id FROM provisiond.idempotency_records WHERE scope_id='{tenant}')"))
    values['external']=w.external()
    return values


def visible(w,t,key,body,op,status):
    detail=w.request('GET','/v1/operations/'+op,tenant=t)
    replay=w.request('POST','/v1/machines',body,t,key,status=202)
    listed=w.request('GET','/v1/operations?limit=100',tenant=t)['operations']
    assert detail==replay and detail['id']==op and detail['status']==status
    assert [v for v in listed if v['id']==op]==[detail]
    return detail


def gone(w,t,key,body,op,deadline):
    views=[w.request('GET','/v1/operations/'+op,tenant=t,status=410),
           w.request('POST','/v1/machines',body,t,key,status=410)]
    for value in views:
        assert value['error']['kind']=='gone' and value['error']['retryable'] is False
        assert value['error']['details']=={'retained_until':deadline}
    assert not any(v['id']==op for v in w.request('GET','/v1/operations?limit=100',tenant=t)['operations'])
    assert w.request('POST','/v1/machines',dict(body,hostname='conflicting-body'),t,key,status=409)['error']['details']['reason']=='idempotency_mismatch'
    return views


def retention(w):
    h=w.h;started=time.monotonic();w.stop_engine()
    p=parameters();w.restart_api(p)
    config=w.parameters.read_bytes()
    h.event('retention_parameters',parameters=p,sha256=hashlib.sha256(config).hexdigest(),horizon_seconds=HORIZON)
    t=w.enrol();w.fund(t,'retention-settled')
    u=w.enrol();w.fund(u,'retention-late')
    body=w.create_body(t);late_body=w.create_body(u)
    # Actual ambiguous provider acceptance; keep its reserve and error forever
    # until supported resolution, even after the stored receipt expires.
    w.provider_control(mode='drop_reply')
    b=w.request('POST','/v1/machines',body,t,'retention-ambiguous',status=202)['id']
    w.start_engine();w.wait_status(b,'needs_reconciliation')
    ambiguous=visible(w,t,'retention-ambiguous',body,b,'needs_reconciliation')
    w.provider_control(mode='normal')
    a=w.request('POST','/v1/machines',body,t,'retention-settled',status=202)['id']
    w.wait_status(a,'succeeded');w.stop_engine()
    current=w.request('GET','/v1/operations/'+a,tenant=t)
    admitted=receipt(w,a)
    assert admitted['admission_seconds']==HORIZON
    before=protected(w,t)
    ids=h.sql(f"SELECT id::text FROM provisiond.operations WHERE tenant_id='{t['tenant_id']}' ORDER BY id")
    # Observe at least four housekeeping probes after terminal settlement. With
    # baseline retention=2 this includes actual physical deletion, not a sleep
    # that merely assumes the sweep ran. All waits are identical in red/green.
    probe_start=len([e for e in h.events if e.get('event')=='protocol_response' and e.get('tag')=='C' and WORK in e.get('query','')])
    terminal=time.monotonic()
    eventually(lambda: h.sql(f"SELECT statement_timestamp()>updated_at+interval '4 seconds' FROM provisiond.operations WHERE id='{a}'")=='t'
               or h.sql(f"SELECT count(*) FROM provisiond.operations WHERE id='{a}'")=='0',seconds=10)
    # The baseline can delete at 2 s; still allow > two observed probe periods.
    while time.monotonic()-terminal<4.2:time.sleep(.05)
    probe_count=len([e for e in h.events if e.get('event')=='protocol_response' and e.get('tag')=='C' and WORK in e.get('query','')])-probe_start
    assert probe_count>=3,'retention housekeeping observation inconclusive'
    live=receipt(w,a)
    row_count=int(h.sql(f"SELECT count(*) FROM provisiond.operations WHERE id='{a}'"))
    if live['remaining']<10:
        h.event('retention_window_missed',result='inconclusive',receipt=live)
        raise AssertionError('retention receipt window inconclusive')
    response=http(h.ports[4],'POST','/v1/machines',body,t['spending_token'],'retention-settled')
    h.event('retention_decisive_replay',assertion=ASSERTION,status=response[0],body=response[1],
            current_operation=current,receipt=live,physical_rows=row_count,
            terminal_elapsed_seconds=time.monotonic()-terminal,monotonic_elapsed_seconds=time.monotonic()-started,
            legacy_retention_seconds=2,minimum_wait_seconds=4.2,window_tolerance_seconds=1,observed_housekeeping_probes=probe_count,
            protected_before=before)
    # First decisive regression assertion: baseline must fail HERE with 410.
    assert response[0]==202 and response[1]==current,ASSERTION
    assert visible(w,t,'retention-settled',body,a,'succeeded')==current
    assert protected(w,t)==before
    assert h.sql(f"SELECT id::text FROM provisiond.operations WHERE tenant_id='{t['tenant_id']}' ORDER BY id")==ids
    h.results.append(dict(test='W4-retention-live-receipt-replay',result='pass'))

    # Explicit historical/multiple-receipt fixture. Other scope and tenant alias
    # both resolve to A; an expired key must use the latest deadline, too.
    h.sql(f"""INSERT INTO provisiond.idempotency_records(scope_kind,scope_id,key,fingerprint,resource_kind,resource_id,status,response_body,expires_at)
      SELECT 'operator','retention-fixture','retention-other-scope',fingerprint,resource_kind,resource_id,status,response_body,expires_at+interval '6 seconds'
      FROM provisiond.idempotency_records WHERE scope_id='{t['tenant_id']}' AND key='retention-settled';
      INSERT INTO provisiond.idempotency_records(scope_kind,scope_id,key,fingerprint,resource_kind,resource_id,status,response_body,expires_at)
      SELECT scope_kind,scope_id,'retention-alias',fingerprint,resource_kind,resource_id,status,response_body,expires_at-interval '1 second'
      FROM provisiond.idempotency_records WHERE scope_id='{t['tenant_id']}' AND key='retention-settled'""")
    effective=receipt(w,a)
    h.event('retention_multiple_receipt_fixture',operation=a,original=admitted,effective=effective,synthetic_fixture=True)
    # Configuration changes do not rewrite stored receipt deadlines.
    changed=dict(p,receipt_seconds=HORIZON+10,retention_seconds=(2 if p['retention_seconds']==2 else HORIZON+10))
    w.restart_api(changed)
    assert receipt(w,a)['expires']==effective['expires']
    # A genuine queued create crosses E before being dispatched. Its descriptor
    # prices a bounded delayed mutation, without changing store or stop bounds.
    descriptor=dict(w.descriptor,account=u['account'],call_seconds=60)
    h.sql("UPDATE provisiond.provider_accounts SET descriptor='%s' WHERE account='%s'"%(json.dumps(descriptor),u['account']))
    w.restart_api(p)
    c=w.request('POST','/v1/machines',late_body,u,'retention-late',status=202)['id']
    c_receipt=receipt(w,c)
    # Prove A survives its ORIGINAL E while a different scope's receipt protects it.
    eventually(lambda:receipt(w,a)['remaining']<4.5,seconds=HORIZON+10)
    assert receipt(w,a)['remaining']>1,'multiple-receipt window inconclusive'
    before=protected(w,t)
    assert visible(w,t,'retention-settled',body,a,'succeeded')==current
    assert visible(w,t,'retention-alias',body,a,'succeeded')==current
    assert protected(w,t)==before
    h.event('retention_resolving_receipt_protected',operation=a,receipt=receipt(w,a),original_deadline_elapsed=True,physical_rows=1)
    eventually(lambda:receipt(w,c)['remaining']<-3 and receipt(w,a)['remaining']<-3,seconds=HORIZON+10)
    before=protected(w,t)
    assert visible(w,t,'retention-ambiguous',body,b,'needs_reconciliation')==ambiguous
    assert visible(w,u,'retention-late',late_body,c,'queued')['terminal'] is False
    assert protected(w,t)==before
    a_gone=gone(w,t,'retention-settled',body,a,effective['wire'])
    gone(w,t,'retention-alias',body,a,effective['wire'])
    eventually(lambda:h.sql(f"SELECT count(*) FROM provisiond.operations WHERE id='{a}'")=='0')
    assert protected(w,t)==before
    assert h.sql(f"SELECT count(*) FROM provisiond.idempotency_records WHERE resource_id='{a}'")=='3'
    assert h.sql(f"SELECT reserved_sats||':'||state FROM provisiond.commitments WHERE operation_id='{b}'")=='43:open'
    h.event('retention_settled_gone',operation=a,receipt=receipt(w,a),responses=a_gone,physical_rows=0,protected_unchanged=True)
    h.results.append(dict(test='W4-retention-max-stored-deadline-and-permanent-refusal',result='pass'))

    hold=h.root/'provider/hold-after-create';hold.touch()
    try:
        w.start_engine()
        eventually(lambda:any(e.get('command')==c and e.get('stage')=='resource_created' for e in w.external()['events']))
        # Restart really occurred after expiry; B preserves its uncertainty.
        assert visible(w,t,'retention-ambiguous',body,b,'needs_reconciliation')==ambiguous
        running_before=protected(w,u)
        visible(w,u,'retention-late',late_body,c,'running')
        time.sleep(3.2)
        visible(w,u,'retention-late',late_body,c,'running')
        assert protected(w,u)==running_before
        h.event('retention_unsettled_after_expiry',queued_and_running=c,needs_reconciliation=b,
                late_receipt=c_receipt,current_receipt=receipt(w,c),housekeeping_window_seconds=3.2,
                ambiguity_unchanged=ambiguous==w.request('GET','/v1/operations/'+b,tenant=t),restart=True)
    finally:hold.unlink()
    eventually(lambda:w.status(c) in ('succeeded',''),seconds=10)
    # Empty means housekeeping already reclaimed a settled row; prove success by
    # its authoritative machine and debits, not by assuming gone means success.
    assert h.sql(f"SELECT count(*) FROM provisiond.machines WHERE id IN (SELECT machine_id FROM provisiond.ledger_entries WHERE operation_id='{c}')")=='1'
    late_before=protected(w,u)
    late_gone=gone(w,u,'retention-late',late_body,c,c_receipt['wire'])
    eventually(lambda:h.sql(f"SELECT count(*) FROM provisiond.operations WHERE id='{c}'")=='0')
    assert protected(w,u)==late_before
    assert len([e for e in w.external()['events'] if e.get('command')==c and e.get('stage')=='accepted'])==1
    h.event('retention_late_settlement',operation=c,responses=late_gone,receipt=receipt(w,c),machine_and_debits=True,new_horizon=False)
    h.results.append(dict(test='W4-retention-unsettled-restart-and-late-settlement',result='pass'))

    # Force a historical deadline at a database statement instant, then hold
    # the DELETE before dispatch. The sole read connection stays available and
    # customer reads finish inside the unchanged idle-transaction/call bounds.
    # Only fixture time is changed, never runtime/bounds or settlement state.
    v=w.enrol();w.fund(v,'retention-logical')
    logical_body=w.create_body(v)
    d=w.request('POST','/v1/machines',logical_body,v,'retention-logical',status=202)['id']
    w.wait_status(d,'succeeded');w.stop_engine()
    gate=h.proxy.arm(DELETE,'send')
    try:
        h.sql(f"UPDATE provisiond.idempotency_records SET expires_at=date_trunc('second',statement_timestamp())+interval '3.95 seconds' WHERE resource_kind='operation' AND resource_id='{d}'")
        # The full fractional receipt promise survives even after its rendered
        # wire second has passed. Rounding is presentation only, never deletion.
        eventually(lambda:h.sql(f"SELECT statement_timestamp()>=date_trunc('second',expires_at) AND statement_timestamp()<expires_at FROM provisiond.idempotency_records WHERE resource_id='{d}'")=='t',seconds=5)
        fractional=receipt(w,d)
        visible(w,v,'retention-logical',logical_body,d,'succeeded')
        assert receipt(w,d)['remaining']>0,'fractional receipt window inconclusive'
        h.event('retention_fractional_receipt_protected',operation=d,receipt=fractional,wire_second_elapsed=True)
        activation=gate.wait();boundary=time.monotonic()
        logical_receipt=receipt(w,d);before=protected(w,v)
        logical=gone(w,v,'retention-logical',logical_body,d,logical_receipt['wire'])
        row=h.sql(f"SELECT status FROM provisiond.operations WHERE id='{d}'")
        assert row=='succeeded','logical cutoff must be observed before physical sweep'
        assert protected(w,v)==before
        elapsed=time.monotonic()-boundary
        assert elapsed<.75,'logical cutoff probe must finish inside unchanged idle transaction bound'
        h.event('retention_logical_cutoff',operation=d,receipt=logical_receipt,responses=logical,physical_status=row,
                elapsed_seconds=elapsed,store_call_seconds=2,idle_transaction_seconds=1,activation_session=activation['pid'],synthetic_deadline_fixture=True)
    finally:gate.release()
    eventually(lambda:h.sql(f"SELECT count(*) FROM provisiond.operations WHERE id='{d}'")=='0')
    visible(w,t,'retention-ambiguous',body,b,'needs_reconciliation')
    assert h.sql(f"SELECT count(*) FROM provisiond.operations WHERE tenant_id='{t['tenant_id']}'")=='1'
    # Unknown/foreign identity stays not_found; receipt refusal never leaks it.
    w.request('GET','/v1/operations/'+b,tenant=u,status=404)
    w.request('GET','/v1/operations/00000000-0000-0000-0000-000000000000',tenant=t,status=404)
    assert len([e for e in w.external()['events'] if e.get('command')==b and e.get('stage')=='accepted'])==1
    w.restart_api()
    h.results.append(dict(test='W4-retention-logical-cutoff-detail-list-replay',result='pass'))
