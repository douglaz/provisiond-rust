"""STO-7/14/24/25/33, OPS-49: real housekeeping under retained history.

The same fixture on unchanged runtime must fail the explicit liveness assertion
after a real retention-probe timeout. No defect hooks or planner overrides.
"""
import hashlib
import json
import os
from pathlib import Path
import re
import time

from harness import eventually
from workflow import http
from w4_retention import WORK, DELETE

ASSERTION = 'RETENTION HOUSEKEEPING MUST STAY LIVE WITHIN STATEMENT BOUND'
READ_ASSERTION = 'RETAINED HISTORY READS MUST MEET STATEMENT AND HTTP BOUNDS'
SHAPES = [(20000, 200000), (20000, 1000000), (50000, 1000000)]


def sql(w, text):
    # Bulk fixture setup/snapshots have a separate supervisor bound. Application
    # statements and measured SQL retain statement_timeout=1000ms throughout.
    h = w.h
    return h.run(['psql', '-XAt', '-v', 'ON_ERROR_STOP=1', '-h', '127.0.0.1',
                  '-p', str(h.ports[0]), '-U', 'administrator', '-d', 'postgres'],
                 sql=text, timeout=180).stdout.strip()


def literal(value):
    return "'" + str(value).replace("'", "''") + "'"


def plan(w, label, query, types, values, analyze=True):
    args = ','.join(literal(v) for v in values)
    text = ("SET statement_timeout='1000ms'; BEGIN; PREPARE measured(" +
            ','.join(types) + ') AS ' + query + '; EXPLAIN (' +
            ('ANALYZE, BUFFERS, ' if analyze else '') +
            'FORMAT JSON) EXECUTE measured(' + args + '); ROLLBACK;')
    started = time.monotonic()
    try:
        output = sql(w, text)
    except AssertionError as error:
        w.h.event('retention_scale_plan_failure',label=label,query=query,
                  exit_status=w.h.commands[-1]['exit_status'],diagnostic=str(error),
                  complete_psql_seconds=time.monotonic()-started,statement_ms=1000)
        return None
    data = json.loads(output[output.index('['):output.rindex(']')+1])
    path = w.h.root / (label + '-plan.json')
    path.write_text(json.dumps(data, indent=2) + '\n')
    w.h.event('retention_scale_plan', label=label, query=query, types=types,
              values=values, analyzed=analyze, statement_ms=1000,
              execution_ms=data[0].get('Execution Time'),
              planning_ms=data[0].get('Planning Time'),
              complete_psql_seconds=time.monotonic()-started, exit_status=0,
              path=str(path), sha256=hashlib.sha256(path.read_bytes()).hexdigest(),plan=data)
    return data


def completions(h, start, prefix):
    return [e for e in h.events[start:] if e['event']=='protocol_response'
            and e.get('tag')=='C' and e.get('query', '').startswith(prefix)]


def live(w, shape):
    h = w.h
    start = len(h.events)
    began = time.monotonic()
    w.restart_api()
    eventually(lambda: w.api.poll() is not None or len(completions(h, start, WORK))>=5,
               seconds=12)
    elapsed = time.monotonic()-began
    probes = completions(h, start, WORK)
    errors = [e for e in h.events[start:] if e['event']=='protocol_response'
              and e.get('tag')=='E' and e.get('query', '').startswith(WORK)]
    server_errors = [e for e in h.events[start:] if e['event']=='server_error_forwarded']
    trace = w.api.log.read_text()
    diagnostic = w.api.error.read_text()
    h.event('retention_scale_liveness', shape=shape, assertion=ASSERTION,
            api_exit=w.api.poll(), elapsed_seconds=elapsed,
            successful_probes=len(probes), probe_error_observed=bool(errors),
            statement_timeout_observed='canceling statement due to statement timeout' in diagnostic,
            housekeeping_failure_observed='API housekeeping failed' in diagnostic,
            server_sqlstates=[e.get('sqlstate') for e in server_errors],
            api_stopping='api_stopping' in trace,
            stdout=str(w.api.log), stderr=str(w.api.error),
            stdout_sha256=hashlib.sha256(w.api.log.read_bytes()).hexdigest(),
            stderr_sha256=hashlib.sha256(w.api.error.read_bytes()).hexdigest())
    assert w.api.poll() is None and len(probes)>=5, ASSERTION
    return probes[-1]['query']


def seed(w, tenant, operations, receipts, status='succeeded'):
    # One genuine settled order/receipt plus synthetic rows distributed across
    # 100 tenants. Receipt-only rows model permanent refusal history, not live work.
    sql(w, f"""
      INSERT INTO provisiond.operations(id,tenant_id,idempotency_key,kind,status,request_summary,correlation_id)
      SELECT md5('scale-'||i)::uuid,CASE WHEN i%100=0 THEN {literal(tenant)} ELSE 'scale-tenant-'||(i%100) END,
        'scale-'||i,'create',{literal(status)},'{{}}','scale-history' FROM generate_series(1,{operations-1}) i;
      INSERT INTO provisiond.idempotency_records(scope_kind,scope_id,key,fingerprint,resource_kind,resource_id,status,response_body,expires_at)
      SELECT 'tenant','scale-history','scale-'||i,'fixture','operation',md5('scale-'||i)::uuid,202,'{{}}',
        CASE WHEN i<{operations} THEN statement_timestamp()+interval '1 day' ELSE statement_timestamp()-interval '1 day' END
      FROM generate_series(1,{receipts-1}) i;
      ANALYZE provisiond.operations; ANALYZE provisiond.idempotency_records;
    """)
    counts = json.loads(sql(w, """SELECT jsonb_build_object('operations',(SELECT count(*) FROM provisiond.operations),
      'operation_receipts',(SELECT count(*) FROM provisiond.idempotency_records WHERE resource_kind='operation'),
      'other_receipts',(SELECT count(*) FROM provisiond.idempotency_records WHERE resource_kind<>'operation'))"""))
    assert counts['operations']==operations and counts['operation_receipts']==receipts
    w.h.event('retention_scale_seed', counts=counts, synthetic=True,
              distribution='100 tenant identities; 1 genuine simulator operation; permanent receipt-only history',
              operation_status=status,
              indexes=sql(w, "SELECT indexdef FROM pg_indexes WHERE schemaname='provisiond' AND tablename='idempotency_records'"))


def history(w, shape, t, op, body, deposit, operation_status='succeeded'):
    h = w.h
    page = None
    failures=[]
    for name in ['detail', 'operation-replay', 'list', 'filtered-list', 'cursor', 'deposit-replay']:
        if name=='detail':method,target,data,key,expected='GET','/v1/operations/'+op,None,None,200
        elif name=='operation-replay':method,target,data,key,expected='POST','/v1/machines',body,'scale-genuine',202
        elif name=='deposit-replay':method,target,data,key,expected='POST','/v1/deposits',{'amount_sats':60},'deposit-scale',200
        else:
            method,data,key,expected='GET',None,None,200
            target='/v1/operations?limit=2'
            if name in ('filtered-list','cursor'):
                target+='&status='+operation_status+'&requested_by=caller&terminal='+('true' if operation_status=='succeeded' else 'false')
            if name=='cursor':
                if page is None or not page.get('next_cursor'):
                    h.event('retention_scale_read_not_run',shape=shape,path=name,reason='filtered page did not return a cursor')
                    failures.append(name)
                    continue
                target+='&cursor='+page['next_cursor']
        # Begin just after an ordinary completed housekeeping probe. This
        # leaves its full next period for the read's server-side deadline and
        # prevents pool-checkout failure from preempting the read regression.
        probe_count=len(completions(h,0,WORK))
        eventually(lambda:len(completions(h,0,WORK))>probe_count,seconds=5)
        start = len(h.events)
        began = time.monotonic()
        status,value,_ = http(h.ports[4],method,target,data,t['spending_token'],key)
        elapsed = time.monotonic()-began
        events = h.events[start:]
        queries = [e['query'] for e in events if e['event']=='bytes_transmitted'
                   and 'FULL JOIN' in e.get('query', '')]
        # Capture exact runtime SQL and protocol interval separately from HTTP.
        intervals=[]
        for query in dict.fromkeys(queries):
            sent=next(e for e in events if e['event']=='bytes_transmitted' and e.get('query')==query)
            done=[e for e in events if e['event']=='protocol_response' and e.get('tag')=='C'
                  and e.get('session')==sent['session'] and e.get('query')==query]
            if done:intervals.append(done[-1]['elapsed']-sent['elapsed'])
        h.event('retention_scale_http', shape=shape, path=name, status=status,
                http_seconds=elapsed, protocol_statement_seconds=intervals,
                statement_ms=1000, store_call_ms=2000, priced_read_seconds=1,
                direct_store_call_timing='not separately instrumented')
        good=status==expected and elapsed<1
        if not good:failures.append(name)
        assert w.api.poll() is None, 'read regression preempted by API process failure'
        if status!=expected:
            h.event('retention_scale_read_refusal',shape=shape,path=name,response=value)
            if queries:plan(w,shape+'-'+name,queries[-1],['text','text'] if name in ('detail','operation-replay','deposit-replay') else ['text']*7+['bigint'],
                            [t['tenant_id'],op if name=='detail' else key] if name in ('detail','operation-replay','deposit-replay') else [t['tenant_id'],operation_status if name!='list' else '', 'caller' if name!='list' else '', '', ('true' if operation_status=='succeeded' else 'false') if name!='list' else '', '', '', 3],analyze=False)
            continue
        if name=='deposit-replay':assert value==deposit
        elif name in ('detail','operation-replay'):assert value['id']==op and value['status']==operation_status
        else:
            assert len(value['operations'])==2
            if name=='cursor':assert not {v['id'] for v in value['operations']} & {v['id'] for v in page['operations']}
            page=value
        query=queries[-1]
        if name=='detail':types,values=['text','text'],[t['tenant_id'],op]
        elif name in ('operation-replay','deposit-replay'):types,values=['text','text'],[t['tenant_id'],key]
        else:
            # Cursor contents are signed JSON in the actual API; use the rows'
            # full database timestamp/id to measure the equivalent final SQL.
            types=['text']*7+['bigint']
            values=[t['tenant_id'],operation_status if name!='list' else '',
                    'caller' if name!='list' else '', '', ('true' if operation_status=='succeeded' else 'false') if name!='list' else '', '', '', 3]
            if name=='cursor':
                previous=last_filtered['operations'][-1]['id']
                values[5]=sql(w,f"SELECT created_at::text FROM provisiond.operations WHERE id={literal(previous)}")
                values[6]=previous
            if name=='filtered-list':last_filtered=value
        if plan(w,shape+'-'+name,query,types,values) is None:failures.append(name+'-plan')
    h.event('retention_scale_read_bound',shape=shape,assertion=READ_ASSERTION,failures=failures,
            api_exit=w.api.poll(),housekeeping_preemption=False)
    assert not failures, READ_ASSERTION


def snapshot(w):
    values={table:json.loads(sql(w, f"SELECT COALESCE(jsonb_agg(to_jsonb(r) ORDER BY to_jsonb(r)::text),'[]') FROM provisiond.{table} r"))
            for table in ['ledger_entries','commitments','episodes','machines','ceiling_counters']}
    values['receipts']=sql(w,"SELECT count(*)||':'||md5(string_agg(md5(to_jsonb(r)::text),'' ORDER BY scope_kind,scope_id,key)) FROM provisiond.idempotency_records r")
    values['external']=w.external()
    return values


def mixed(w, shape, probe, purge, total):
    h=w.h
    w.api.terminate();w.api.wait(timeout=5)
    sql(w,"""
      UPDATE provisiond.idempotency_records SET expires_at=statement_timestamp()-interval '1 second'
        WHERE scope_id='scale-history' AND key IN (SELECT 'scale-'||i FROM generate_series(1,1000) i);
      INSERT INTO provisiond.idempotency_records(scope_kind,scope_id,key,fingerprint,resource_kind,resource_id,status,response_body,expires_at)
        SELECT 'operator','scale-other',key,fingerprint,resource_kind,resource_id,status,response_body,statement_timestamp()+interval '1 day'
        FROM provisiond.idempotency_records WHERE scope_id='scale-history' AND key IN (SELECT 'scale-'||i FROM generate_series(1,10) i);
      INSERT INTO provisiond.operations(id,tenant_id,idempotency_key,kind,status,request_summary,correlation_id,updated_at)
        SELECT md5('scale-extra-'||i)::uuid,'scale-history','scale-extra-'||i,'create',
          CASE i WHEN 1 THEN 'failed' WHEN 2 THEN 'succeeded' WHEN 3 THEN 'queued' WHEN 4 THEN 'running' ELSE 'needs_reconciliation' END,
          '{}','scale-history',CASE WHEN i=2 THEN statement_timestamp() ELSE statement_timestamp()-interval '2 days' END
        FROM generate_series(1,5) i;
      INSERT INTO provisiond.idempotency_records(scope_kind,scope_id,key,fingerprint,resource_kind,resource_id,status,response_body,expires_at)
        SELECT 'tenant','scale-history',idempotency_key,'fixture','operation',id,202,'{}',statement_timestamp()-interval '1 second'
        FROM provisiond.operations WHERE idempotency_key IN ('scale-extra-3','scale-extra-4','scale-extra-5');
      ANALYZE provisiond.operations; ANALYZE provisiond.idempotency_records;
    """)
    before=snapshot(w)
    retired_before=sql(w,"SELECT count(*)||':'||COALESCE(md5(string_agg(to_jsonb(r)::text,'' ORDER BY id)),'') FROM provisiond.retired_operations r")
    expected={hashlib.md5(('scale-'+str(i)).encode()).hexdigest() for i in range(11,1001)}
    expected.add(hashlib.md5(b'scale-extra-1').hexdigest())
    result=sql(w,'SET statement_timeout=1000; PREPARE probe(bigint) AS '+probe+'; EXECUTE probe(86400);')
    assert result.splitlines()[-1]=='true'
    assert plan(w,shape+'-mixed-probe',probe,['bigint'],[86400]) is not None
    assert plan(w,shape+'-mixed-delete',purge,['bigint'],[86400]) is not None
    # Independent returned-ID comparison rolls back; actual deletion authority
    # is evaluated again by the real housekeeping transaction after restart.
    output=sql(w,'SET statement_timeout=1000; BEGIN; PREPARE purge(bigint) AS '+purge+'; EXECUTE purge(86400); ROLLBACK;')
    returned={line.replace('-','') for line in output.splitlines() if re.fullmatch(r'[0-9a-f-]{36}',line)}
    assert returned==expected
    assert snapshot(w)==before
    assert sql(w,"SELECT count(*)||':'||COALESCE(md5(string_agg(to_jsonb(r)::text,'' ORDER BY id)),'') FROM provisiond.retired_operations r")==retired_before,'rolled-back deletion must leave no retired identity'
    start=len(h.events)
    w.restart_api()
    eventually(lambda:int(sql(w,'SELECT count(*) FROM provisiond.operations'))==total+5-len(expected),seconds=12)
    eventually(lambda:len(completions(h,start,WORK))>=3,seconds=12)
    deletes=completions(h,start,purge.split('(',1)[0])
    assert deletes and any(e.get('command_tag') in ('DELETE '+str(len(expected)), 'INSERT 0 '+str(len(expected))) for e in deletes)
    assert snapshot(w)==before
    retired=json.loads(sql(w,"SELECT jsonb_agg(to_jsonb(r) ORDER BY id) FROM provisiond.retired_operations r WHERE id IN (SELECT md5('scale-'||i)::uuid FROM generate_series(11,1000) i UNION ALL SELECT md5('scale-extra-1')::uuid)"))
    assert {r['id'].replace('-','') for r in retired}==expected
    assert sql(w,"SELECT count(*) FROM provisiond.retired_operations o JOIN provisiond.idempotency_records r ON r.resource_kind='operation' AND r.resource_id=o.id WHERE o.retained_until<>r.expires_at")== '0'
    survivors=sql(w,"SELECT idempotency_key||':'||status FROM provisiond.operations WHERE idempotency_key LIKE 'scale-extra-%' ORDER BY idempotency_key")
    assert survivors.splitlines()==['scale-extra-2:succeeded','scale-extra-3:queued','scale-extra-4:running','scale-extra-5:needs_reconciliation']
    assert sql(w,"SELECT count(*) FROM provisiond.operations WHERE id IN (SELECT md5('scale-'||i)::uuid FROM generate_series(1,10) i)")=='10'
    assert sql(w,"SELECT count(*) FROM provisiond.idempotency_records WHERE scope_id='scale-history' AND key IN (SELECT 'scale-'||i FROM generate_series(11,1000) i)")=='990'
    h.event('retention_scale_mixed',shape=shape,deleted_ids=sorted(expected),actual_delete_command_tags=[e['command_tag'] for e in deletes],
            expected_deleted=len(expected),retired=retired,rollback_preserved_identities=True,protected_aliases=10,unsettled_statuses=3,receiptless_old_deleted=True,
            receiptless_recent_retained=True,financial_history_receipts_and_external_unchanged=True,
            before_sha256=hashlib.sha256(json.dumps(before,sort_keys=True).encode()).hexdigest())


def retention_scale(w):
    h=w.h
    t=w.enrol();deposit,_,_=w.fund(t,'scale')
    body=w.create_body(t)
    op=w.request('POST','/v1/machines',body,t,'scale-genuine',status=202)['id']
    w.start_engine();w.wait_status(op,'succeeded');w.stop_engine()
    source=Path('src/api/mod.rs').read_text()
    relation=re.search(r'const SETTLED_RETENTION: &str = "([^"]+)";',source).group(1)
    purge_template=re.search(r'let purge = format!\(\s*"([^"\n]+)"\s*\)',source).group(1)
    purge=purge_template.replace('{SETTLED_RETENTION}',relation)
    shape=tuple(map(int,os.environ.get('PROVISIOND_TEST_RETENTION_SCALE','20000:200000').split(':')))
    assert shape in SHAPES
    for operations,receipts in [shape]:
        shape=f'{operations}-operations-{receipts}-receipts'
        w.api.terminate();w.api.wait(timeout=5)
        seed(w,t['tenant_id'],operations,receipts)
        probe=live(w,shape)
        assert sql(w,'SET statement_timeout=1000; PREPARE probe(bigint) AS '+probe+'; EXECUTE probe(86400);').splitlines()[-1]=='false'
        assert plan(w,shape+'-no-work-probe',probe,['bigint'],[86400]) is not None
        assert plan(w,shape+'-no-work-delete',purge,['bigint'],[86400]) is not None
        history(w,shape,t,op,body,deposit)
        mixed(w,shape,probe,purge,operations)
        h.results.append(dict(test='W4-retention-statement-bound-'+shape,result='pass'))


def retention_reads(w):
    # OPS-3/STO-33: protected unsettled rows keep the old housekeeping probe
    # cheap. Its genuine failure cannot preempt the independent read-bound red.
    h=w.h
    t=w.enrol();deposit,_,_=w.fund(t,'scale')
    body=w.create_body(t)
    op=w.request('POST','/v1/machines',body,t,'scale-genuine',status=202)['id']
    shape=tuple(map(int,os.environ.get('PROVISIOND_TEST_RETENTION_SCALE','20000:2000000').split(':')))
    assert shape in [(20000,200000),(20000,1000000),(50000,1000000),(20000,2000000)]
    w.api.terminate();w.api.wait(timeout=5)
    seed(w,t['tenant_id'],*shape,status='queued')
    live(w,'read-only-'+str(shape))
    start=len(h.events)
    try:
        history(w,'read-only-'+str(shape),t,op,body,deposit,operation_status='queued')
    finally:
        probes=completions(h,start,WORK)
        errors=[e for e in h.events[start:] if e['event']=='protocol_response'
                and e.get('tag')=='E' and e.get('query','').startswith(WORK)]
        h.event('retention_scale_read_isolation',api_exit=w.api.poll(),successful_probes=len(probes),probe_errors=len(errors))
        assert w.api.poll() is None and not errors, 'read regression preempted by housekeeping failure'
    h.results.append(dict(test='W4-retained-history-read-bound',result='pass'))
