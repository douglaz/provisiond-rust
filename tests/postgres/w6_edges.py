"""Reached OPS-41/48 and LDG-38/72 edges, using real lifecycle prefixes."""
import concurrent.futures
import json
import math
import time
from datetime import datetime,timezone,timedelta


def measurement(w):
    from w6 import create
    from w5 import external
    samples=[]
    for i in range(20):
        _,op,_=create(w,'allocation-sample-'+str(i))
        facts=external(w,op)
        accepted=next(e for e in facts if e['stage']=='accepted')
        allocated=next(e for e in facts if e['stage']=='resource_created')
        samples.append(dict(operation=op,account=accepted['account'],accepted_ns=accepted['monotonic_ns'],allocated_ns=allocated['monotonic_ns'],seconds=(allocated['monotonic_ns']-accepted['monotonic_ns'])/1e9))
    maximum=max(s['seconds'] for s in samples)
    negative=math.ceil(maximum)+59
    assert negative==60,'measured allocation plus disclosed conservative margin must fit the declared negative window'
    observed=json.loads(w.h.sql("SELECT jsonb_agg(to_jsonb(p)) FROM provisiond.provider_observations p"))
    assert len(observed)>=20
    w.h.event('w6_allocation_measurement',samples=samples,sample_size=20,worst_seconds=maximum,negative_seconds=negative,
        derivation='ceiling of worst ordinary local allocation plus 59 seconds conservative margin',
        visibility=dict(declared_seconds=1,status='unmeasured conservative bound; no re-declaration',ordinary_samples=observed),
        boundary='durably recorded independent acceptance to resource creation; request-to-observation retained separately')


def episode_order(w):
    from w6 import create,operator,operation_state,proof
    from core import core_probe
    from harness import eventually
    from w5 import external,events
    # A concurrent join invokes the production helper through two fenced store units.
    w.h.run(['cargo','build','--example','core-probe','--features','store-test-hooks','--locked'],timeout=120)
    t,_,machine=create(w,'episode-order')
    p=core_probe(w.h);p.command('engine_start')
    joined=p.command('join_cleanup '+machine)['value']['episodes'];p.finish()
    assert joined[0]==joined[1]
    episode=joined[0]
    before=json.loads(w.h.sql(f"SELECT to_jsonb(e) FROM provisiond.episodes e WHERE id='{episode}'"))
    assert set(before['reasons'])=={'late_attach_cleanup','tenant_suspended'}
    cleanup=before['current_operation_id']
    # A historical suspension reason is not current suspension. Step 3 finds the
    # real prepaid unit/funding and aborts, even though reason contains suspended.
    w.start_engine();w.wait_status(cleanup,'succeeded');w.stop_engine()
    funded=operation_state(w,cleanup)
    assert funded['episodes'][0]['close_reason']=='funded' and funded['machine']['destroy_committed'] is None
    assert funded['operation']['write_started_at'] is None and not external(w,cleanup)
    # A queued, never-fenced operator retry must re-derive funding, not use origin
    # alone as the step-2 exemption. API-64's fresh attempt remains same episode.
    p=core_probe(w.h);p.command('engine_start');episode=p.command('join_cleanup '+machine)['value']['episodes'][0];p.finish()
    op=w.h.sql(f"SELECT current_operation_id FROM provisiond.episodes WHERE id='{episode}'")
    fail=w.control/'fail-step-101';fail.touch();w.start_engine();w.wait_status(op,'failed');w.stop_engine();fail.unlink()
    assert operation_state(w,op)['machine']['destroy_committed'] is None
    retried=operator(w,f'/v1/episodes/{episode}/actions/retry',{'reason':'never-fenced test'},'never-fenced-retry')
    fresh=retried['current_operation_id'];w.start_engine();w.wait_status(fresh,'succeeded');w.stop_engine()
    assert operation_state(w,fresh)['episodes'][-1]['state']=='closed'
    assert not external(w,fresh),'never-fenced operator origin alone must not force destruction'
    # A currently suspended attempt reaches step 2, becomes uncertain, resolves
    # not_applied into stalled, then has a keep/retry race serialized by tenant.
    w.h.sql(f"UPDATE provisiond.tenants SET status='suspended' WHERE id='{t['tenant_id']}'")
    p=core_probe(w.h);p.command('engine_start');episode=p.command('join_cleanup '+machine)['value']['episodes'][0];p.finish()
    op=w.h.sql(f"SELECT current_operation_id FROM provisiond.episodes WHERE id='{episode}'")
    barrier=w.h.root/'provider/hold-reconciliation';barrier.touch()
    proof(w,op,delete='acceptance_not_applied');w.start_engine();w.wait_status(op,'needs_reconciliation')
    assert next(e for e in operation_state(w,op)['episodes'] if e['id']==episode)['state']=='uncertain'
    w.stop_engine();w.start_engine();barrier.unlink();w.wait_status(op,'failed');w.stop_engine()
    stalled=operation_state(w,op)
    assert next(e for e in stalled['episodes'] if e['id']==episode)['state']=='stalled'
    # No timer retry may issue another deletion while stalled.
    calls=len([e for e in external(w,op) if e['stage']=='delete_accepted'])
    w.start_engine();time.sleep(1.2);w.stop_engine()
    assert calls==len([e for e in external(w,op) if e['stage']=='delete_accepted'])==1
    from workflow import http
    def action(verb):
        body={'operator_ref':'race-keep'} if verb=='keep' else {'reason':'race retry'}
        return http(w.h.ports[7],'POST',f'/v1/episodes/{episode}/actions/{verb}',body,w.operator_tokens[0],'race-'+verb)
    with concurrent.futures.ThreadPoolExecutor(max_workers=2) as pool:
        results=list(pool.map(action,['keep','retry']))
    assert sorted(r[0] for r in results)==[200,409]
    state=operation_state(w,op)
    current=next(e for e in state['episodes'] if e['id']==episode)
    if current['state']!='closed':
        fresh=current['current_operation_id'];proof(w,fresh,delete='reject')
        w.start_engine();w.wait_status(fresh,'failed');w.stop_engine()
        operator(w,f'/v1/episodes/{episode}/actions/keep',{'operator_ref':'race-finish'},'race-finish-key')
    w.h.event('w6_causal_trace',scenario=w.case,operation=op,boundary='concurrent reason join; ordered funded abort; never-fenced retry; uncertain-not_applied-stalled; no timer retry; keep/retry race',snapshot=operation_state(w,op),race_statuses=[r[0] for r in results],provider=external(w,op))
    for verb in ('abandoned','applied'):
        p=core_probe(w.h);p.command('engine_start');episode=p.command('join_cleanup '+machine)['value']['episodes'][0];p.finish()
        op=w.h.sql(f"SELECT current_operation_id FROM provisiond.episodes WHERE id='{episode}'")
        barrier.touch();proof(w,op,delete='acceptance_applied' if verb=='applied' else 'unknown')
        w.start_engine();w.wait_status(op,'needs_reconciliation');w.stop_engine()
        operator(w,f'/v1/operations/{op}/actions/resolve',{'resolution':verb,'operator_ref':'episode-'+verb},'episode-resolve-'+verb)
        final=operation_state(w,op)
        closed=next(e for e in final['episodes'] if e['id']==episode)
        assert closed['state']=='closed' and closed['close_reason']==('resource_gone' if verb=='applied' else 'abandoned')
        assert final['machine']['destroy_committed'] is None
        for action,body in [('retry',{'reason':'late retry'}),('keep',{'operator_ref':'late-keep'})]:
            operator(w,f'/v1/episodes/{episode}/actions/{action}',body,'closed-'+verb+'-'+action,status=409)
        w.start_engine();barrier.unlink();time.sleep(1.2);w.stop_engine()
        assert next(e for e in operation_state(w,op)['episodes'] if e['id']==episode)==closed,'a permanent episode close must survive later attempts and account observations'
        w.h.event('w6_causal_trace',scenario=w.case,operation=op,boundary='uncertain episode '+verb+' closes permanently; late keep/retry refused',snapshot=operation_state(w,op),provider=external(w,op))



def meter_edges(w):
    from w6 import create,deletion,operation_state
    # A no-rate span crosses a UTC month and a duplicate unavailable verdict.
    # It must be one native-only deficiency, with each period's no-entry mark.
    t,_,machine=create(w,'meter-outage-month')
    start=datetime.now(timezone.utc).replace(day=1,hour=0,minute=0,second=0,microsecond=0)-timedelta(seconds=30)
    until=start+timedelta(seconds=60)
    rate=json.loads(w.h.sql("SELECT value FROM provisiond.rates WHERE currency='USD'"))
    w.h.sql(f"UPDATE provisiond.meter_totals SET high_water_mark='{start.isoformat()}',billing_period='{start:%Y-%m}' WHERE subject_id='{machine}'")
    w.h.sql("UPDATE provisiond.fixture_rate_history SET accepted_at='%s' WHERE currency='USD'"%((start-timedelta(hours=1)).isoformat()))
    for instant,available in [(start-timedelta(seconds=1),False),(start+timedelta(seconds=10),False),(until,True)]:
        w.h.sql("INSERT INTO provisiond.fixture_rate_history(currency,accepted_at,value,available) VALUES('USD','%s','%s',%s)"%(instant.isoformat(),json.dumps(rate),str(available).lower()))
    op=deletion(w,t,machine,'meter-month-key');w.start_engine();w.wait_status(op,'succeeded');w.stop_engine()
    state=operation_state(w,op)
    outages=[d for d in state['deficiencies'] if d['cause']=='rate_outage']
    assert len(outages)==1 and outages[0]['absorbed_seconds']==60
    from w6_blocking import deficiency_fields
    deficiency_fields(w,op,state)
    assert outages[0]['rate_num'] is None and outages[0]['rate_den'] is None and outages[0]['native_minor']>0
    assert len(state['meter'])==2
    assert all(0<=int(m['credit_num'])<int(m['credit_den']) for m in state['meter'])
    assert all(e['billing_period'] and e['increment_end'] for e in state['ledger'] if e['kind']=='usage_debit')
    w.h.event('w6_causal_trace',scenario=w.case,operation=op,boundary='exit split at UTC month and durable rate acceptance; one maximal native-only outage',snapshot=state,clock=dict(start=start.isoformat(),returned=until.isoformat()))
    # One-sided audit uses the entry key's end; it must detect a mark behind an
    # actual usage debit without reconstructing the credit from ledger totals.
    w.h.sql(f"UPDATE provisiond.meter_totals SET high_water_mark=(SELECT max(increment_end)-interval '1 second' FROM provisiond.ledger_entries WHERE machine_id='{machine}') WHERE subject_id='{machine}' AND billing_period=(SELECT max(billing_period) FROM provisiond.ledger_entries WHERE machine_id='{machine}' AND kind='usage_debit')")
    assert int(w.h.sql('SELECT provisiond.audit_machine_meters()'))>=1
    assert w.h.sql(f"SELECT meter_quarantined FROM provisiond.machines WHERE id='{machine}'")=='t'
    w.h.event('w6_meter_high_water_audit',machine=machine,usage_keys=[e['idempotency_key'] for e in state['ledger'] if e['kind']=='usage_debit'],alert=True)
    # Explicit post-migration/import audit runs outside posting, detects corrupt
    # credit, quarantines only this subject and still allows the real delete.
    t,_,machine=create(w,'meter-quarantine')
    w.h.sql(f"UPDATE provisiond.meter_totals SET credit_num=2,credit_den=1,high_water_mark=clock_timestamp()-interval '1 minute' WHERE subject_id='{machine}'")
    assert int(w.h.sql('SELECT provisiond.audit_machine_meters()'))>=1
    # Existing open outage evidence may close at exit, while quarantine keeps
    # every meter row and ledger entry untouched and opens no new deficiency.
    w.h.sql(f"INSERT INTO provisiond.operator_deficiencies(provider_account,machine_id,subject_kind,subject_id,operation_id,cause,native_minor,currency,clamped_sats,idempotency_key,absorbed_from) SELECT provider_account,id,'machine',id,gen_random_uuid(),'rate_outage',0,'USD',NULL,'quarantined-open-outage',clock_timestamp()-interval '30 seconds' FROM provisiond.machines WHERE id='{machine}'")
    op=deletion(w,t,machine,'quarantine-delete-key');before=operation_state(w,op)
    w.start_engine();w.wait_status(op,'succeeded');w.stop_engine();after=operation_state(w,op)
    assert before['meter']==after['meter'] and before['ledger']==after['ledger']
    assert after['machine']['meter_quarantined'] and after['machine']['state']=='deleted'
    assert len(before['deficiencies'])==len(after['deficiencies'])==1
    outage=after['deficiencies'][0]
    assert outage['absorbed_until']==after['machine']['meter_stopped_at'] and outage['absorbed_seconds']>=30
    assert outage['native_minor']>0 and outage['rate_num'] is None and outage['rate_den'] is None
    deficiency_fields(w,op,after)
    assert w.h.sql(f"SELECT count(*) FROM provisiond.meter_alerts WHERE machine_id='{machine}'")=='1'
    w.h.event('w6_causal_trace',scenario=w.case,operation=op,boundary='explicit range audit quarantines; deletion records stop with unchanged meter evidence',before=before,snapshot=after)


def default_hooks(w):
    """Ordinary executables exercise every new defect environment without arming it."""
    from w6 import create,lost,pending,proof,one_purchase
    from w5 import events,orders,admit
    from harness import eventually
    h=w.h;w.stop_engine();w.api.terminate();w.api.wait(timeout=5)
    original=(h.target,h.env,w.builds,w.defect)
    target=h.root/'w6-default-target'
    h.owned_extra_paths=getattr(h,'owned_extra_paths',[])+[target]
    hold=h.root/'provider/hold-reconciliation';paths=[];observed=[]
    try:
        h.target=target;h.env=dict(h.env,CARGO_TARGET_DIR=str(target))
        h.run(['cargo','build','--bins','--locked'],timeout=120)
        w.builds={name:w.build_identity(target/'debug'/name,[]) for name in ('api','engine','migrate')}
        h.config['workflow']['w6_default_builds']=dict(w.builds)
        w.launch_api()
        record_hold=w.control/'hold-reconciliation-record';record_hold.touch();paths.append(record_hold)
        # Real reply loss reaches both retry and premature-release control sites.
        for defect in ('w6_blind_repeat','w6_release_timeout'):
            w.defect=defect
            t,_,op,barrier=lost(w,'ordinary-'+defect,search='incomplete')
            state=pending(w,op);one_purchase(w,op)
            assert state['commitment']['reserved_sats']==43
            barrier.unlink();observed.append(dict(defect=defect,operation=op,orders=1,reserve=43))
        # Wrong exact correlator reaches resolution, but ordinary code never attaches.
        w.defect='w6_non_exact'
        _,_,op,barrier=lost(w,'ordinary-non-exact',search='wrong')
        w.start_engine();barrier.unlink()
        eventually(lambda:any(e['operation']==op for e in events(w.engine,'resolution_search')))
        assert not pending(w,op)['machines'];w.stop_engine();one_purchase(w,op)
        observed.append(dict(defect=w.defect,operation=op,attached=False))
        # A real interrupted running row plus closed incident marker reaches the
        # ordinary startup pass; no test-hook hold can stage it in these binaries.
        import uuid
        w.defect='w6_skip_startup';hold.touch()
        _,_,op=admit(w,'ordinary-startup')
        effect=h.root/'provider/hold-before-effect';effect.touch();paths.append(effect)
        w.provider_control(mode='normal');p=w.start_engine()
        eventually(lambda:any(e['stage']=='effect_held' for e in w.external()['events'] if e.get('command')==op))
        w.stop_engine();h.sql(f"INSERT INTO provisiond.recovery_incidents(id,closed_at) VALUES('{uuid.uuid4()}',clock_timestamp())")
        p=w.start_engine()
        assert events(p,'restart_pass_complete')[-1]['running_found']==1
        assert w.status(op)=='needs_reconciliation'
        w.stop_engine();effect.unlink();hold.unlink()
        # Completion barrier distinguishes acceptance and its later allocation.
        eventually(lambda:any(e['stage']=='resource_created' for e in w.external()['events'] if e.get('command')==op))
        observed.append(dict(defect=w.defect,operation=op,running_found=1))
        # The normal binary must also ignore the new delete hold/failure files.
        w.defect=None
        for name in ('hold-step-101','fail-step-102'):
            path=w.control/name;path.touch();paths.append(path)
        t,_,machine=create(w,'ordinary-delete')
        from w6 import deletion
        op=deletion(w,t,machine,'ordinary-delete-key');w.start_engine();w.wait_status(op,'succeeded');w.stop_engine()
        assert all(path.exists() for path in paths if path.name!='hold-before-effect')
        h.event('w6_default_observed',features=[],defect_environments=observed,delete_operation=op,control_files_ignored=True)
    finally:
        w.stop_engine()
        if w.api.poll() is None:w.api.terminate();w.api.wait(timeout=5)
        for path in paths:
            if path.exists():path.unlink()
        hold.unlink(missing_ok=True)
        h.target,h.env,w.builds,w.defect=original
        w.provider_control(mode='normal');w.launch_api()
