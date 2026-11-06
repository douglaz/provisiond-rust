"""OPS-21/47/49/50, STO-13: concrete claim and stop ownership boundaries."""
import json
import subprocess
import time
from core import core_probe, enqueue, empty_queue, command, environment, binary, tenant_holder
from harness import eventually


def held(p,name):
    eventually(lambda:any(e.get('event')=='execution_boundary_held' and e['boundary']==name for e in p.events))


def accepted(p,op):
    return [e for e in p.events if e.get('event')=='simulator_accepted' and e.get('operation')==op]


def claim_cancel(h,defect=None):
    for window in ('commit','claim_registration'):
        empty_queue(h)
        ids=[enqueue(h,f'cancel-{window}-{n}',f'cancel-{window}-{n}') for n in range(4)]
        p=core_probe(h,defect,boundary=window if window!='commit' else None)
        p.command('engine_start')
        gate=h.proxy.arm('store COMMIT','commit_reply') if window=='commit' else None
        p.command('claim_start')
        if gate: gate.wait()
        else: held(p,window)
        running=h.sql("SELECT id FROM provisiond.operations WHERE status='running'")
        assert running in ids
        p.command('claim_cancel')
        # The caller alone disappears; the owning process survives.
        assert p.p.poll() is None
        retained_slots=p.command('owners')['value']['available_slots']
        if gate: gate.release()
        else: p.command('boundary_release')
        time.sleep(.08)
        p.command('record_stop '+running,ok=None)
        r=p.command('wait_records',ok=None)
        owned=h.sql(f"SELECT status FROM provisiond.operations WHERE id='{running}'")=='failed'
        h.event('claim_cancellation_observed',window=window,operation=running,registered_owner=owned,available_slots_while_held=retained_slots,process_survived=p.p.poll() is None)
        assert owned and r['ok'] and retained_slots==2, 'cancelled claim must retain a registered worker owner'
        owners=p.command('owners')['value']
        assert owners['workers']==1 and owners['available_slots']==2
        assert not accepted(p,running)
        # A stopped worker still owns a slot in this implementation until record retirement.
        p.finish()
    h.results.append(dict(test='claim-caller-cancellation',result='pass',arms='sent COMMIT reply and completion/registration window'))


def licensing_stop(h,defect=None,only=None):
    for window in ([only] if only else ['reply','sql','retry','licensed_entry']):
        empty_queue(h);op=enqueue(h,'license-'+window,'license-'+window,resource=True,offer='license-'+window)
        p=core_probe(h,defect,boundary='licensing_retry' if window=='retry' else (window if window=='licensed_entry' else None))
        p.command('engine_start');p.command('claim')
        holder=None
        if window in ('sql','retry'):
            holder=tenant_holder(h,['license-'+window],'license-'+window)
            # Licensing itself locks the operation row, not the tenant primitive.
            holder.stdin.write(f"SELECT id FROM provisiond.operations WHERE id='{op}' FOR UPDATE;\n");holder.stdin.flush()
            eventually(lambda:h.sql("SELECT count(*) FROM pg_stat_activity WHERE usename='administrator' AND state='idle in transaction' AND query LIKE '%FOR UPDATE%'")=='1')
        gate=h.proxy.arm('store COMMIT','commit_reply') if window=='reply' else None
        p.command('execute_start '+op)
        if gate:
            connection=gate.wait()
            assert h.sql(f"SELECT about_to_order_at IS NOT NULL FROM provisiond.operations WHERE id='{op}'")=='t'
        elif window=='licensed_entry': held(p,window)
        elif window=='retry': held(p,'licensing_retry')
        else: eventually(lambda:h.sql("SELECT count(*) FROM pg_stat_activity WHERE usename='app' AND wait_event_type='Lock' AND query LIKE '%about_to_order_at%'")=='1')
        p.command('record_stop '+op)
        marker=len(p.events)
        wire_marker=len(h.events)
        if holder: holder.stdin.close();assert holder.wait(timeout=5)==0
        if gate:
            # The original sent checkout remains owned while stopped.
            assert not any(e.get('event')=='commit_owner_completed' and e.get('session')==connection['pid'] for e in p.events[marker:])
            gate.release()
        if window in ('licensed_entry','retry'):p.command('boundary_release')
        p.command('execute_join',ok=None)
        time.sleep(.04)
        h.event('licensing_stop_observed',window=window,operation=op,external_entries=len(accepted(p,op)))
        if accepted(p,op):
            # The forbidden entry is already independently observed. End this
            # owned faulty process with an explicit later stop before asserting,
            # retaining any sent transaction through that whole-process boundary.
            p.command('later_stop',ok=False);p.expected_status=71;p.finish()
        assert not accepted(p,op), 'stopped licensing must not enter provider execution'
        p.command('wait_records')
        assert not any(e.get('event')=='send_requested' and e.get('query','').startswith('UPDATE provisiond.operations SET about_to_order_at=') for e in h.events[wire_marker:]), 'stopped licensing must not repeat'
        state=h.sql(f"SELECT status||':'||(error->'details'->>'reason')||':'||(error->>'retryable') FROM provisiond.operations WHERE id='{op}'")
        assert state=='failed:cut_off:true', 'live pre-dispatch create must fail despite licensing marker'
        assert h.sql(f"SELECT reserved_sats||':'||state FROM provisiond.commitments WHERE operation_id='{op}'")=='0:closed'
        assert h.sql(f"SELECT count(*) FROM provisiond.resource_exclusions WHERE operation_id='{op}'")=='0'
        assert h.sql(f"SELECT count(*) FROM provisiond.ledger_entries WHERE operation_id='{op}'")=='0'
        p.finish()
    h.results.append(dict(test='licensing-stop',result='pass',arms='SQL, retry, sent COMMIT, licensed before dispatch; full release without debit'))


def outcome_stop(h,defect=None):
    empty_queue(h);op=enqueue(h,'outcome-window','outcome-window',resource=True,offer='outcome-window')
    p=core_probe(h,defect,boundary='outcome_publication');p.command('engine_start');p.command('claim')
    p.command('execute_return '+op);held(p,'outcome_publication')
    assert len(accepted(p,op))==1
    p.command('record_stop '+op);p.command('boundary_release');p.command('wait_records')
    state=h.sql(f"SELECT status||':'||COALESCE(error->'details'->>'reason','none') FROM provisiond.operations WHERE id='{op}'")
    h.event('outcome_stop_observed',operation=op,state=state,returned_before_stop=True,publication_after_stop=True)
    assert state=='succeeded:none', 'returned outcome must win over stale cancellation snapshot'
    assert h.sql(f"SELECT count(*)||':'||sum(-amount_sats) FROM provisiond.ledger_entries WHERE operation_id='{op}'")=='2:15'
    assert h.sql(f"SELECT reserved_sats||':'||version FROM provisiond.commitments WHERE operation_id='{op}'")=='45:2'
    assert h.sql(f"SELECT count(*) FROM provisiond.machines WHERE id=(SELECT machine_id FROM provisiond.operations WHERE id='{op}')")=='1'
    p.finish()
    h.results.append(dict(test='outcome-stop-publication',result='pass',arms='returned verified outcome, atomic machine/debits/commitment; cancellation counterpart in ENG-02'))


def deadline(h,defect=None):
    for mode in ('no_waiter','cancelled_waiter','settlement'):
        empty_queue(h);op=enqueue(h,'deadline-'+mode,'deadline-'+mode)
        p=core_probe(h,defect);p.command('engine_start');c=p.command('claim')['value']
        if mode=='settlement':
            c['external_id']='deadline-settlement';command(p,'outcome',c)
        gate=h.proxy.arm('store COMMIT','commit_reply')
        if mode=='settlement':p.command('start_settlement '+op);gate.wait()
        started=time.monotonic();p.command('record_stop '+op)
        if mode!='settlement':gate.wait()
        durable=h.sql(f"SELECT status FROM provisiond.operations WHERE id='{op}'")
        assert durable in ('failed','succeeded')
        if mode=='cancelled_waiter':
            p.command('wait_start');eventually(lambda:any(e.get('event')=='waiter_polled' for e in p.events));p.command('wait_cancel')
        try: status=p.p.wait(timeout=max(.01,1.15-(time.monotonic()-started)))
        except subprocess.TimeoutExpired:status=None
        elapsed=time.monotonic()-started
        h.event('record_deadline_observed',mode=mode,bound_ms=1000,tolerance_ms=150,elapsed_seconds=elapsed,exit_status=status,durable_status=durable,reply_withheld=True)
        if status is None:
            # The faulty process survived the original deadline. Release only
            # after observing that failure, then drain its late record so cleanup
            # cannot turn an intended red assertion into a setup/teardown error.
            gate.release()
            eventually(lambda:any(e.get('event') in ('stop_record_finished','settlement_finished') for e in p.events))
            h.event('late_record_completion_observed',mode=mode,process_survived=True,original_deadline_exceeded=True)
            p.finish()
        assert status==71, 'record deadline must exit without a polled waiter'
        p.expected_status=71;p.finish();gate.release()
        assert any(e.get('event')=='engine_fatal' and 'records not observed landed' in e.get('reason','') for e in p.events)
        assert not any(e.get('event')=='whole_transaction_retry' for e in p.events)
        assert h.sql(f"SELECT status FROM provisiond.operations WHERE id='{op}'")==durable
    h.results.append(dict(test='autonomous-stop-deadline',result='pass',arms='unpolled/cancelled waiter and existing settlement; actual nonzero exit; late reply cannot acknowledge'))


def lock_observers(h,defect=None):
    empty_queue(h);ids=[enqueue(h,'lock-pair-'+str(i),'lock-pair-'+str(i)) for i in range(3)]
    p=core_probe(h,defect,boundary='lock_registration');startup=p.command('engine_start')['value']
    claims=[p.command('claim')['value'] for _ in ids]
    p.command('execute_pending '+claims[0]['id']);eventually(lambda:accepted(p,claims[0]['id']))
    p.command('execute_return '+claims[1]['id']);eventually(lambda:any(e.get('event')=='execute_returned' for e in p.events))
    holder=tenant_holder(h,[claims[2]['tenant']],'lock-pair')
    p.command('record_stop '+claims[2]['id'])
    generation=h.sql('SELECT generation FROM provisiond.startup_generation')
    h.sql(f"SELECT pg_terminate_backend({startup['lock_pid']})")
    eventually(lambda:any(e.get('event')=='driver_dead' for e in p.events))
    p.command('observe_pair');held(p,'lock_registration');time.sleep(.02)
    p.command('boundary_release')
    holder.stdin.close();assert holder.wait(timeout=5)==0
    p.command('observe_join',ok=False);p.expected_status=71;p.finish()
    states=h.sql("SELECT string_agg(status,',' ORDER BY id) FROM provisiond.operations WHERE id IN ("+','.join("'"+i+"'" for i in ids)+")")
    starts=[e for e in p.events if e.get('event')=='stop_record_started']
    h.event('concurrent_lock_observers_observed',states=states,starts=len(starts),registration_windows=sum(e.get('event')=='execution_boundary_held' for e in p.events))
    assert all(h.sql(f"SELECT status FROM provisiond.operations WHERE id='{c['id']}'")!='running' for c in claims), 'one lock loss must land records before exit'
    assert len(starts)==3 and len(set(e['operation'] for e in starts))==3
    assert h.sql('SELECT generation FROM provisiond.startup_generation')==generation
    assert h.sql(f"SELECT status FROM provisiond.operations WHERE id='{claims[1]['id']}'")=='succeeded'
    h.results.append(dict(test='concurrent-lock-loss-observers',result='pass',arms='single fanout, older bound retained, verified outcome, healthy store records before nonzero exit'))


def surface(h,defect=None):
    p=core_probe(h,defect)
    for method in ('claim','defer','settle','license','verified','reacquire'):
        r=p.command('unfenced '+method)['value']['refused']
        h.event('unfenced_engine_method_observed',method=method,refused=r)
        assert r, 'unfenced transaction must refuse engine-only business methods'
    p.finish();h.results.append(dict(test='engine-only-public-boundary',result='pass'))


def predispatch(h,defect=None):
    # Same durable licensing marker, three different ownership/position meanings.
    for mode in ('live','dispatched','restart'):
        empty_queue(h);op=enqueue(h,'position-'+mode,'position-'+mode,resource=True,offer='position-'+mode)
        p=core_probe(h,defect);p.command('engine_start');c=p.command('claim')['value'];assert c['id']==op
        command(p,'license',c)
        assert h.sql(f"SELECT about_to_order_at IS NOT NULL FROM provisiond.operations WHERE id='{op}'")=='t'
        if mode=='dispatched':p.command('execute_pending '+op);eventually(lambda:accepted(p,op))
        if mode=='restart':
            p.finish();p=core_probe(h);p.command('engine_start')
        else:
            p.command('record_stop '+op);p.command('wait_records')
        state=h.sql(f"SELECT status FROM provisiond.operations WHERE id='{op}'")
        h.event('live_create_position_observed',mode=mode,state=state,external_entries=len(accepted(p,op)),about_to_order_set=True)
        assert state==('failed' if mode=='live' else 'needs_reconciliation'), 'live pre-dispatch create must fail despite licensing marker'
        assert h.sql(f"SELECT count(*) FROM provisiond.ledger_entries WHERE operation_id='{op}'")=='0'
        assert h.sql(f"SELECT reserved_sats||':'||state FROM provisiond.commitments WHERE operation_id='{op}'")==('0:closed' if mode=='live' else '60:open')
        assert h.sql(f"SELECT count(*) FROM provisiond.resource_exclusions WHERE operation_id='{op}'")==('0' if mode=='live' else '1')
        p.finish()
    h.results.append(dict(test='live-create-position-versus-restart',result='pass',arms='licensed zero-dispatch fails/releases; dispatched cutoff and ordinary restart retain ambiguity'))


def startup(h,defect=None):
    empty_queue(h)
    for mode in ('newer','older','semantics','uninitialized','missing_history','missing_schema'):
        if mode in ('newer','older','semantics'):
            version,semantics={'newer':(2,1),'older':(0,1),'semantics':(1,2)}[mode]
            h.sql(f'UPDATE provisiond.schema_migrations SET version={version},semantics={semantics}')
        elif mode=='uninitialized':h.sql('DELETE FROM provisiond.schema_migrations')
        elif mode=='missing_history':h.sql('ALTER TABLE provisiond.schema_migrations RENAME TO saved_migrations')
        else:h.sql('ALTER SCHEMA provisiond RENAME TO missing_schema_fixture')
        schema='missing_schema_fixture' if mode=='missing_schema' else 'provisiond'
        generation=h.sql(f'SELECT generation FROM {schema}.startup_generation')
        r=binary(h,'engine',ok=False,defect=defect)
        protected=any(e in r.stdout for e in ('startup_lock_acquired','generation_advanced','claim_registered','simulator_accepted'))
        h.event('real_startup_schema_observed',mode=mode,exit_status=r.returncode,protected_entry=protected,check_only=False,error=r.stderr)
        assert not protected, 'unsupported real engine startup must precede protected entry'
        assert r.returncode==1 and ('unsupported schema/semantic' in r.stderr or 'missing/uninitialized schema' in r.stderr)
        assert h.sql(f'SELECT generation FROM {schema}.startup_generation')==generation
        if mode=='missing_schema':h.sql('ALTER SCHEMA missing_schema_fixture RENAME TO provisiond')
        elif mode=='missing_history':h.sql('ALTER TABLE provisiond.saved_migrations RENAME TO schema_migrations')
        elif mode=='uninitialized':h.sql('INSERT INTO provisiond.schema_migrations(version,semantics) VALUES(1,1)')
        else:h.sql('UPDATE provisiond.schema_migrations SET version=1,semantics=1')
    # Ordinary supported startup, no --check-schema: terminate only its lock session.
    log=h.root/'supported-engine-startup.log'
    with log.open('w') as output:
        p=subprocess.Popen(['target/debug/engine'],env=dict(h.env,**environment(h)),stdout=output,stderr=output,text=True)
    h.extra.append(p)
    eventually(lambda:'restart_pass_complete' in log.read_text())
    events=[json.loads(line) for line in log.read_text().splitlines() if line.startswith('{')]
    lock=next(e['session'] for e in events if e['event']=='startup_lock_acquired')
    observed=next(e for e in events if e['event']=='startup_lock_keepalives_observed')
    assert observed['session']==lock and observed['server']=='2:1:2' and observed['client_getters_verified']
    h.event('actual_lock_keepalives_observed',session=lock,idle_seconds=2,interval_seconds=1,count=2,server=observed['server'],client_getters_verified=True)
    h.sql(f'SELECT pg_terminate_backend({lock})')
    assert p.wait(timeout=3)==1
    h.event('supported_real_startup_observed',exit_after_lock_loss=1,features='store-test-hooks',check_only=False)
    h.results.append(dict(test='REL-02-real-engine-startup',result='pass',arms='missing schema/history, uninitialized, older/newer/incompatible semantics before lock; supported advance and lock keepalives'))
