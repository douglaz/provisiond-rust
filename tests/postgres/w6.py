"""OPS-27/29/30/33: reconciliation through real executables and independent effects."""
import json
import time
from w5 import admit, snapshot, external, orders, events, restore_resource

CASES=['w6_measurement','w6_positive','w6_empty','w6_delayed','w6_conflicting','w6_multiple',
       'w6_startup','w6_operator','w6_delete','w6_delete_evidence','w6_episodes','w6_late_attach','w6_gone_reply','w6_exact',
       'w6_account','w6_delete_classification','w6_resolution_reply','w6_exit','w6_channels','w6_episode_order','w6_meter_edges','w6_default_hooks']

def inventory_complete(manifest):
    inventories=[e for e in manifest.get('events',[]) if e.get('event')=='w6_inventory_complete']
    return (manifest.get('result')=='pass' and len(inventories)==1
        and inventories[0]['required']==CASES and inventories[0]['observed']==CASES
        and [r['test'] for r in manifest.get('scenarios',[]) if r['test'] in CASES]==CASES
        and all(r['result']=='pass' for r in manifest.get('scenarios',[]) if r['test'] in CASES)
        and any(e.get('event')=='cleanup_complete' and e.get('result')=='pass' for e in manifest.get('events',[])))

def proof(w,op,**value):
    w.provider_control(evidence=dict(operation=op,**value))

def wait_search(w,op):
    return eventually(lambda:[e for e in w.events('engine','resolution_search') if e['operation']==op])[-1]

def checkpoint(w,op,boundary,**facts):
    remote=external(w,op)
    w.h.event('w6_causal_trace',scenario=w.case,operation=op,boundary=boundary,
        snapshot=snapshot(w,op),provider=remote,**facts)

def pending(w,op):
    state=snapshot(w,op)
    assert state['operation']['status']=='needs_reconciliation'
    assert state['operation']['resolution'] is None
    assert not state['machines'] and not state['ledger']
    return state

def lost(w,suffix,resource=False,**evidence):
    w.stop_engine()
    t,body,op=admit(w,suffix,resource=resource)
    proof(w,op,**evidence)
    hold=w.h.root/'provider'/'hold-reconciliation';hold.touch(exist_ok=True)
    w.provider_control(mode='drop_reply')
    p=w.start_engine()
    eventually(lambda:w.status(op)=='needs_reconciliation')
    remote=eventually(lambda:[e for e in external(w,op) if e['stage']=='reply_dropped'])
    state=pending(w,op)
    w.h.event('w6_lost_reply_activated',operation=op,purchase=remote[-1]['resource'],snapshot=state,
              accepted_count=sum(e['stage']=='accepted' for e in external(w,op)),reply_delivered=False)
    assert state['commitment']['state']=='open' and state['commitment']['reserved_sats']==43, 'original-reservation-preserved before negative window'
    assert len(orders(w,op))==1, 'one purchase/no repeated order'
    w.stop_engine()
    w.provider_control(mode='normal')
    return t,body,op,hold

def one_purchase(w,op):
    accepted=[e for e in external(w,op) if e['stage']=='accepted']
    assert len(accepted)==1 and len(orders(w,op))==1, 'one purchase/no repeated order'

def positive(w):
    t,body,op,hold=lost(w,'reconcile-positive',search='normal')
    w.start_engine();hold.unlink()
    eventually(lambda:w.status(op)=='succeeded')
    w.stop_engine()
    result=snapshot(w,op)
    assert result['operation']['resolution']=='observed'
    assert len(result['machines'])==1 and len(result['ledger'])==2
    assert result['commitment']['reserved_sats']==12
    assert result['machines'][0]['first_unit_debit_id'] is not None
    checkpoint(w,op,'independent exact search and direct present after real successor',resolution=result['operation']['resolution'])
    one_purchase(w,op)
    w.start_engine();w.stop_engine()
    assert snapshot(w,op)==result,'resolution settlement is immutable across successor'
    assert w.request('POST','/v1/machines',body,t,'reconcile-positive',status=202)['id']==op

def empty(w):
    t,_,op,hold=lost(w,'reconcile-empty',resource=True,search='empty')
    resource=next(e['resource'] for e in external(w,op) if e['stage']=='resource_created')
    w.provider_control(remove_resource=resource)
    w.start_engine();hold.unlink();wait_search(w,op)
    before=pending(w,op)
    assert before['commitment']['state']=='open' and before['commitment']['reserved_sats']==43,'original-reservation-preserved before negative window'
    assert len(before['exclusions'])==1
    w.stop_engine()
    w.h.sql(f"UPDATE provisiond.operations SET mutation_wait_ended_at=clock_timestamp()-interval '61 seconds' WHERE id='{op}'")
    w.h.event('w6_fixture_clock',operation=op,negative_seconds=60,elapsed_seconds=61,basis='fixture advances wait-end age without changing ordinary bounds')
    w.start_engine();eventually(lambda:w.status(op)=='failed');w.stop_engine()
    final=snapshot(w,op)
    assert final['operation']['resolution']=='absent' and final['commitment']['state']=='closed'
    assert not final['exclusions'] and not final['ledger'] and not final['machines']
    checkpoint(w,op,'authoritative empty before and after declared negative window',before=before)
    one_purchase(w,op);restore_resource(w,t)

def delayed(w):
    _,_,op,hold=lost(w,'reconcile-delayed',search='normal',direct='absent')
    # The first ordinary successor is inside the window: no read can prove absence.
    w.h.sql(f"UPDATE provisiond.operations SET mutation_wait_ended_at=clock_timestamp()+interval '3 seconds' WHERE id='{op}'")
    w.start_engine();hold.unlink()
    time.sleep(1.2)
    assert not [e for e in w.events('engine','resolution_search') if e['operation']==op]
    before=pending(w,op)
    proof(w,op,search='normal')
    eventually(lambda:w.status(op)=='succeeded');w.stop_engine()
    checkpoint(w,op,'visibility wait then exact direct presence',before=before)
    one_purchase(w,op)
    w.stop_engine();_,_,other=admit(w,'reconcile-in-progress')
    proof(w,other,search='in_progress')
    effect=w.h.root/'provider/hold-before-effect';effect.touch()
    w.provider_control(mode='drop_reply');w.start_engine()
    eventually(lambda:any(e['stage']=='effect_held' for e in external(w,other)))
    w.wait_status(other,'needs_reconciliation');w.stop_engine()
    assert not [e for e in external(w,other) if e['stage']=='resource_created']
    w.h.sql(f"UPDATE provisiond.operations SET mutation_wait_ended_at=clock_timestamp()-interval '61 seconds' WHERE id='{other}'")
    w.start_engine();wait_search(w,other)
    assert pending(w,other)['commitment']['state']=='open','matched in-progress keeps original commitment beyond negative window'
    checkpoint(w,other,'matched in-progress past negative window; actual remote allocation still held',negative_seconds=60)
    effect.unlink()
    eventually(lambda:any(e['stage']=='resource_created' for e in external(w,other)))
    proof(w,other,search='normal');w.wait_status(other,'succeeded')
    w.stop_engine();one_purchase(w,other);w.provider_control(mode='normal')

def conflicting(w):
    for name,config in [('wrong',dict(search='wrong')),('failed',dict(search='normal',direct='failed')),('account',dict(search='normal',direct='wrong_account'))]:
        _,_,op,hold=lost(w,'reconcile-conflict-'+name,**config)
        w.start_engine();hold.unlink();wait_search(w,op)
        time.sleep(.2)
        result=snapshot(w,op)
        w.h.event('w6_exact_observed',operation=op,search=name,attached=bool(result['machines']),provider=external(w,op),status=result['operation']['status'])
        assert not result['machines'],'exact-correlator/no-attachment'
        result=pending(w,op)
        assert result['commitment']['state']=='open'
        checkpoint(w,op,'conflicting '+name+' evidence remains unresolved')
        w.stop_engine();one_purchase(w,op)

def multiple(w):
    _,_,op,hold=lost(w,'reconcile-many',search='many')
    w.start_engine();hold.unlink();seen=wait_search(w,op)
    result=pending(w,op)
    assert seen['evidence']['matches']==2 and len(seen['evidence']['provider_identifiers'])==2
    assert result['commitment']['state']=='open'
    checkpoint(w,op,'one accepted purchase, two visible candidates, neither attached',candidates=seen['evidence'])
    w.stop_engine();one_purchase(w,op)

def operator(w,path,body=None,key=None,status=200,token=None,method='POST',headers=None):
    code,value,hs=http(w.h.ports[7],method,path,body,token or w.operator_tokens[0],key,headers=headers)
    assert not any(k.startswith('Access-Control-') for k in hs),'operator response must not expose customer CORS'
    assert code==status,(code,value)
    return value

def operation_state(w,op):
    return json.loads(w.h.sql(f"""SELECT jsonb_build_object('operation',to_jsonb(o)-'request',
      'machine',(SELECT to_jsonb(m) FROM provisiond.machines m WHERE m.id=o.machine_id),
      'commitments',COALESCE((SELECT jsonb_agg(to_jsonb(c) ORDER BY c.id) FROM provisiond.commitments c WHERE c.machine_id=o.machine_id OR c.operation_id=o.id),'[]'),
      'ledger',COALESCE((SELECT jsonb_agg(to_jsonb(l) ORDER BY seq) FROM provisiond.ledger_entries l WHERE l.machine_id=o.machine_id OR l.operation_id=o.id),'[]'),
      'episodes',COALESCE((SELECT jsonb_agg(to_jsonb(e)) FROM provisiond.episodes e WHERE e.machine_id=o.machine_id),'[]'),
      'deficiencies',COALESCE((SELECT jsonb_agg(to_jsonb(d) ORDER BY id) FROM provisiond.operator_deficiencies d WHERE d.machine_id=o.machine_id OR d.operation_id=o.id),'[]'),
      'meter',COALESCE((SELECT jsonb_agg(to_jsonb(m)) FROM provisiond.meter_totals m WHERE m.subject_id=o.machine_id),'[]')) FROM provisiond.operations o WHERE id='{op}'"""))

def create(w,suffix):
    w.stop_engine();t,body,op=admit(w,suffix)
    w.provider_control(mode='normal');w.start_engine();w.wait_status(op,'succeeded');w.stop_engine()
    return t,op,w.h.sql(f"SELECT machine_id FROM provisiond.operations WHERE id='{op}'")

def deletion(w,t,machine,suffix):
    return w.request('POST',f'/v1/machines/{machine}/actions/delete',{'acknowledge_destruction':True},t,suffix,status=202)['id']

def startup(w):
    import uuid
    for marked in (False,True):
        w.stop_engine();t,_,op=admit(w,'startup-'+str(marked),resource=True)
        hold=w.control/('hold-step-9' if marked else 'hold-step-1');hold.touch()
        first=w.start_engine();eventually(lambda:any(e['event']=='dispatch_step_held' and e['operation']==op for e in events(first)))
        w.h.event('w6_interrupted_worker',operation=op,marked=marked,claim=1)
        w.stop_engine();hold.unlink()
        w.h.sql(f"INSERT INTO provisiond.recovery_incidents(id,closed_at) VALUES('{uuid.uuid4()}',clock_timestamp())")
        barrier=w.h.root/'provider/hold-reconciliation';barrier.touch(exist_ok=True)
        second=w.start_engine()
        found=events(second,'restart_pass_complete')[-1]['running_found']
        w.h.event('w6_startup_observed',operation=op,found=found,status=w.status(op),closed_marker=True,trace=events(second))
        assert found==1 and w.status(op)==('needs_reconciliation' if marked else 'failed'),'interrupted-running-row classification/count'
        state=snapshot(w,op)
        assert state['commitment']['state']==('open' if marked else 'closed')
        assert bool(state['exclusions'])==marked
        assert not orders(w,op),'startup exception must make zero purchases'
        checkpoint(w,op,'ordinary running pass with retained closed marker',marked=marked,running_found=found)
        w.stop_engine();barrier.unlink()
        if marked:operator(w,f'/v1/operations/{op}/actions/resolve',dict(resolution='absent',operator_ref='opref-startup-zero-call'),'startup-absent-key')
        restore_resource(w,t)

def human(w):
    for verb in ['absent','abandoned','observed']:
        t,_,op,hold=lost(w,'human-'+verb,search='incomplete')
        w.start_engine();hold.unlink();wait_search(w,op);w.stop_engine()
        body=dict(resolution=verb,operator_ref='opref-resolution')
        if verb=='observed':body.update(external_id=next(e['resource'] for e in external(w,op) if e['stage']=='resource_created'))
        path=f'/v1/operations/{op}/actions/resolve'
        for bad in [dict(resolution='applied',operator_ref='opref-bad'),dict(body,unexpected=True),dict(body,operator_ref='name@example.invalid')]:
            operator(w,path,bad,'invalid-resolution',status=400)
        before=len(orders(w,op))
        first=operator(w,path,body,'operator-resolution-'+verb)
        assert operator(w,path,body,'operator-resolution-'+verb)==first,'same operator key must replay exact decision'
        operator(w,path,body,'operator-new-key',status=409)
        code,_,headers=http(w.h.ports[4],'POST',path+'?ignored=1',body,t['spending_token'],'customer-resolve')
        assert code==404 and not any(k.startswith('Access-Control-') for k in headers)
        operator(w,path,body,'customer-operator',token=t['spending_token'],status=404)
        state=snapshot(w,op)
        assert state['operation']['resolved_by']=='operator-a' and state['operation']['resolution_evidence']==body
        assert len(orders(w,op))==before
        if verb=='observed':assert state['machines'][0]['earliest_cancellation_date'] is None
        if verb=='abandoned':assert w.h.sql(f"SELECT count(*) FROM provisiond.operator_deficiencies WHERE operation_id='{op}'")=='2'
        checkpoint(w,op,'authenticated synchronous '+verb,operator_reply=first)

def caller_delete(w):
    t,create_op,machine=create(w,'caller-delete')
    path=f'/v1/machines/{machine}/actions/delete'
    w.request('POST',path,{},t,'missing-destruction',status=400)
    code,_,_=http(w.h.ports[4],'POST','/v1/machines/unknown/actions/delete',token=t['spending_token'],key='unknown-delete-key',raw=b'{invalid')
    assert code==404,'delete ownership authorization must precede body parsing'

    op=deletion(w,t,machine,'caller-delete-key')
    w.start_engine();w.wait_status(op,'succeeded');w.stop_engine()
    final=operation_state(w,op)
    assert final['machine']['state']=='deleted' and final['machine']['meter_stopped_at'] is not None
    assert all(c['state']=='closed' for c in final['commitments'])
    assert final['operation']['write_started_at'] is not None
    assert w.request('POST',path,{'acknowledge_destruction':True},t,'caller-delete-key',status=202)['id']==op
    w.h.event('w6_causal_trace',scenario=w.case,operation=op,boundary='gone reply at observation instant',snapshot=final,provider=external(w,op))
    assert len([e for e in external(w,op) if e['stage']=='delete_accepted'])==1
    # Already-gone has no mutation and no marker.
    other=deletion(w,t,machine,'already-gone-key');w.start_engine();w.wait_status(other,'succeeded');w.stop_engine()
    assert operation_state(w,other)['operation']['write_started_at'] is None
    assert not [e for e in external(w,other) if e['stage']=='delete_accepted']

def delete_evidence(w):
    for arm,expected in [('acceptance_applied','applied'),('acceptance_not_applied','not_applied'),('drop_reply','applied')]:
        t,_,machine=create(w,'delete-'+arm)
        op=deletion(w,t,machine,'delete-'+arm+'-key')
        proof(w,op,delete=arm)
        hold=w.h.root/'provider/hold-reconciliation';hold.touch()
        w.start_engine();w.wait_status(op,'needs_reconciliation')
        before=operation_state(w,op)
        assert before['machine']['meter_stopped_at'] is None and any(c['state']=='open' for c in before['commitments'])
        w.stop_engine();w.start_engine();hold.unlink()
        w.wait_status(op,'succeeded' if expected=='applied' else 'failed');w.stop_engine()
        final=operation_state(w,op)
        assert final['operation']['resolution']==expected
        assert len([e for e in external(w,op) if e['stage']=='delete_accepted'])==1,'delete reconciliation must not repeat mutation'
        assert (final['machine']['meter_stopped_at'] is not None)==(expected=='applied')
        w.h.event('w6_causal_trace',scenario=w.case,operation=op,boundary=arm,before=before,snapshot=final,provider=external(w,op))

def episodes(w):
    w.stop_engine();t,_,op=admit(w,'suspended-cleanup')
    hold=w.h.root/'provider/hold-after-create';hold.touch();cut=w.control/'hold-step-101';cut.touch()
    w.provider_control(mode='normal');w.start_engine()
    eventually(lambda:any(e['stage']=='resource_created' for e in external(w,op)))
    w.h.sql(f"UPDATE provisiond.tenants SET status='suspended' WHERE id='{t['tenant_id']}'")
    hold.unlink();w.wait_status(op,'succeeded')
    cleanup=eventually(lambda:w.h.sql(f"SELECT id FROM provisiond.operations WHERE kind='delete' AND tenant_id='{t['tenant_id']}'"))
    episode=w.h.sql(f"SELECT episode_id FROM provisiond.operations WHERE id='{cleanup}'")
    proof(w,cleanup,delete='reject');cut.unlink();w.wait_status(cleanup,'failed');w.stop_engine()
    stalled=operation_state(w,cleanup);assert stalled['episodes'][0]['state']=='stalled' and stalled['machine']['destroy_committed']==episode
    path=f'/v1/episodes/{episode}/actions/'
    retry=operator(w,path+'retry',{'reason':'opaque fixture decision'},'episode-retry-key')
    fresh=retry['current_operation_id'];assert fresh!=cleanup
    assert operation_state(w,fresh)['operation']['requested_by']=='operator'
    # Current suspension is now false. The inherited fence still licenses this retry.
    w.h.sql(f"UPDATE provisiond.tenants SET status='active' WHERE id='{t['tenant_id']}'")
    proof(w,fresh,delete='reject');w.start_engine();w.wait_status(fresh,'failed');w.stop_engine()
    before=operation_state(w,fresh)
    kept=operator(w,path+'keep',{'operator_ref':'opref-keep'},'episode-keep-key')
    assert kept['state']=='closed' and kept['close_reason']=='kept' and kept['kept_by']=='operator-a'
    assert operator(w,path+'keep',{'operator_ref':'opref-keep'},'episode-keep-key')==kept
    operator(w,path+'retry',{'reason':'new attempt'},'closed-retry-key',status=409)
    after=operation_state(w,fresh)
    assert before['commitments']==after['commitments'] and before['ledger']==after['ledger']
    assert after['machine']['destroy_committed'] is None
    views=operator(w,'/v1/episodes?state=closed&limit=1',method='GET')
    assert any(e['id']==episode for e in views['episodes'])
    w.h.event('w6_causal_trace',scenario=w.case,operation=fresh,boundary='suspension cleanup, fenced operator retry, keep',snapshot=after,prior=stalled,provider=external(w,fresh))

def late(w):
    for arm in ['covered','insufficient','no_rate']:
        t,_,op,hold=lost(w,'late-'+arm,resource=True,search='incomplete')
        w.h.sql(f"UPDATE provisiond.operations SET mutation_wait_ended_at=clock_timestamp()-interval '61 seconds' WHERE id='{op}'")
        w.start_engine();hold.unlink();wait_search(w,op)
        eventually(lambda:snapshot(w,op)['commitment']['state']=='closed');w.stop_engine()
        released=pending(w,op);assert released['exclusions'],'negative-window release must retain outcome exclusion'
        if arm=='insufficient':
            w.h.sql(f"INSERT INTO provisiond.ledger_entries(tenant_id,seq,kind,amount_sats,balance_after,idempotency_key) SELECT '{t['tenant_id']}',max(seq)+1,'correction',-115,0,'fixture-consumed-available' FROM provisiond.ledger_entries WHERE tenant_id='{t['tenant_id']}'")
        if arm=='no_rate':w.h.sql("UPDATE provisiond.rates SET available=false WHERE currency='USD'")
        cut=w.control/'hold-step-101';cut.touch();proof(w,op,search='normal')
        w.start_engine();w.wait_status(op,'succeeded')
        cleanup=eventually(lambda:w.h.sql(f"SELECT id FROM provisiond.operations WHERE kind='delete' AND tenant_id='{t['tenant_id']}'"))
        state=operation_state(w,op)
        assert state['machine']['first_unit_debit_id'] is None and not state['ledger']
        assert len([d for d in state['deficiencies'] if d['cause'].startswith('unrecoverable_')])==2
        assert sum(c['state']=='open' for c in state['commitments'])==(arm=='covered')
        assert sum(d['cause']=='late_attach_cleanup' for d in state['deficiencies'])==(arm!='covered')
        if arm=='no_rate':assert all(d['rate_num'] is None for d in state['deficiencies'])
        cut.unlink()
        if arm=='no_rate':
            p=w.engine;eventually(lambda:p.poll() is not None)
            assert any(e['event']=='delete_no_rate_stopped' for e in events(p))
            assert not [e for e in external(w,cleanup) if e['path']=='/delete']
            assert operation_state(w,cleanup)['machine']['destroy_committed'] is None
            w.h.results.append(dict(test='OPS-41-no-rate-steps-4-5',result='not run',reason='path stopped before fence/provider call; deferred follow-on owner'))
            w.h.sql("UPDATE provisiond.rates SET available=true WHERE currency='USD'")
        else:w.wait_status(cleanup,'succeeded')
        w.stop_engine();checkpoint(w,op,'late attach '+arm,late_state=state,cleanup=operation_state(w,cleanup),original_release=released)
        one_purchase(w,op);restore_resource(w,t)

def gone_reply(w):
    from w6_gone_reply import probe
    w.stop_engine();probe(w)

def worker_exact(w):
    from w6_exact import probe
    probe(w)

def account(w):
    w.stop_engine();t,_,op=admit(w,'account-sweep')
    original=json.loads(w.h.sql(f"SELECT descriptor FROM provisiond.provider_accounts WHERE account='{t['account']}'"))
    w.h.sql("UPDATE provisiond.provider_accounts SET descriptor='%s' WHERE account='%s'"%(json.dumps(dict(original,temporary_keys=True)),t['account']))
    w.provider_control(mode='normal');w.start_engine();w.wait_status(op,'succeeded');w.stop_engine()
    machine=w.h.sql(f"SELECT machine_id FROM provisiond.operations WHERE id='{op}'")
    actual=next(e['value'] for e in external(w,op) if e['stage']=='resource_created')
    w.provider_control(fixture_resource=dict(actual,state='stopped'))
    # Extra independent unrecorded resources force multiple local pages.
    for i in range(3):w.provider_control(fixture_resource=dict(actual,external_id='UNRECORDED-'+str(i),correlator=None))
    w.start_engine()
    eventually(lambda:operation_state(w,op)['machine']['state']=='stopped')
    eventually(lambda:any(e['external_id']=='UNRECORDED-0' for e in w.events('engine','unrecorded_provider_resource')))
    eventually(lambda:any(e['stage']=='key_deleted' for e in external(w,op)))
    assert w.h.sql("SELECT count(*) FROM provisiond.machines WHERE external_id LIKE 'UNRECORDED-%'")=='0'
    w.stop_engine();w.provider_control(remove_resource=actual['external_id']);proof(w,None,enumeration='incomplete')
    w.start_engine();eventually(lambda:any(e['complete'] is False for e in events(w.engine,'account_pass_complete')))
    assert operation_state(w,op)['machine']['meter_stopped_at'] is None
    w.stop_engine();proof(w,None,direct='failed')
    w.start_engine();eventually(lambda:any(e['stage']=='resolution_direct_activated' for e in w.external()['events'] if e.get('command') is None))
    assert operation_state(w,op)['machine']['meter_stopped_at'] is None
    w.stop_engine();proof(w,None);w.start_engine()
    eventually(lambda:operation_state(w,op)['machine']['state']=='deleted');w.stop_engine()
    w.h.event('w6_causal_trace',scenario=w.case,operation=op,boundary='paginated complete account pass and direct confirmation',snapshot=operation_state(w,op),provider=w.external())
    w.h.sql("UPDATE provisiond.provider_accounts SET descriptor='%s' WHERE account='%s'"%(json.dumps(original),t['account']))

def delete_classification(w):
    t,_,machine=create(w,'delete-classification')
    barrier=w.h.root/'provider/hold-reconciliation';barrier.touch()
    for arm,step,crash in [('before',102,False),('after',103,False),('restart-null',102,True),('restart-set',103,True)]:
        op=deletion(w,t,machine,'delete-classification-'+arm)
        control=w.control/(('hold-step-' if crash else 'fail-step-')+str(step));control.touch()
        p=w.start_engine()
        if crash:
            eventually(lambda:any(e['event']=='dispatch_step_held' and e['operation']==op for e in events(p)))
            w.stop_engine();control.unlink();w.start_engine();assert w.status(op)=='needs_reconciliation'
        else:
            w.wait_status(op,'failed' if arm=='before' else 'needs_reconciliation');control.unlink()
        state=operation_state(w,op)
        assert (state['operation']['write_started_at'] is not None)==(step==103)
        assert state['machine']['meter_stopped_at'] is None and any(c['state']=='open' for c in state['commitments'])
        assert not [e for e in external(w,op) if e['path']=='/delete'],'classification boundary must precede provider mutation'
        w.h.event('w6_causal_trace',scenario=w.case,operation=op,boundary=arm,snapshot=state,provider=external(w,op))
        w.stop_engine()
        if state['operation']['status']=='needs_reconciliation':
            body=dict(resolution='abandoned' if arm=='after' else 'not_applied',operator_ref='opref-delete')
            operator(w,f'/v1/operations/{op}/actions/resolve',body,'classify-resolution-'+arm)
            assert operation_state(w,op)['commitments']==state['commitments'],'delete abandonment must preserve running funding'
    barrier.unlink()

def resolution_reply(w):
    _,_,op,hold=lost(w,'resolution-reply',search='normal')
    enumeration=w.h.root/'provider/hold-enumerate';enumeration.touch()
    statement=w.h.proxy.arm('resolution=$4','send')
    w.start_engine();hold.unlink()
    connection=statement.wait()
    gate=w.h.proxy.arm('store COMMIT','commit_reply',session=connection['pid'])
    statement.release()
    try:
        gate.wait();w.wait_status(op,'succeeded');before=snapshot(w,op)
        w.h.proxy.close_connection(connection,'resolution_reply_lost');gate.release()
        eventually(lambda:any(e['event']=='resolution_recorded' and e['operation']==op for e in events(w.engine)))
        w.stop_engine();after=snapshot(w,op)
        assert before==after,'resolution whole-store repeat must preserve every landed effect'
        one_purchase(w,op)
        checkpoint(w,op,'landed resolution COMMIT reply lost and whole transaction repeated',before=before,after=after)
    finally:gate.release();enumeration.unlink(missing_ok=True)

def exit_accounting(w):
    t,op,machine=create(w,'exit-elapsed')
    # A controlled billing clock sets a real lifecycle's unclosed tail two hours
    # behind the reply. No production duration or retry bound is shortened.
    w.h.sql(f"UPDATE provisiond.meter_totals SET high_water_mark=clock_timestamp()-interval '2 hours' WHERE subject_id='{machine}'")
    w.h.sql("UPDATE provisiond.fixture_rate_history SET accepted_at=clock_timestamp()-interval '1 day' WHERE currency='USD'")
    delete=deletion(w,t,machine,'exit-elapsed-key')
    before=operation_state(w,delete)
    w.start_engine();w.wait_status(delete,'succeeded');w.stop_engine()
    final=operation_state(w,delete)
    usage=[e for e in final['ledger'] if e['kind']=='usage_debit']
    assert usage and sum(-e['amount_sats'] for e in usage)==12,'elapsed exit must debit remaining authority before release'
    assert any(d['cause']=='clamp_overflow' and d['clamped_sats']>=10 for d in final['deficiencies'])
    assert all(c['state']=='closed' and c['reserved_sats']==0 for c in final['commitments'])
    assert final['meter'][0]['high_water_mark']==final['machine']['meter_stopped_at']
    w.h.event('w6_causal_trace',scenario=w.case,operation=delete,boundary='elapsed two-hour exit, clamp, empty authority and atomic close',before=before,snapshot=final,provider=external(w,delete),clock='explicit fixture high-water/rate acceptance timestamps; real reply observation')

def channels(w):
    t,_,op,hold=lost(w,'all-channels',search='normal')
    original=json.loads(w.h.sql(f"SELECT descriptor FROM provisiond.provider_accounts WHERE account='{t['account']}'"))
    w.h.sql("UPDATE provisiond.provider_accounts SET descriptor='%s' WHERE account='%s'"%(json.dumps(dict(original,channels=['orders','alternate'])),t['account']))
    resource=next(e['value'] for e in external(w,op) if e['stage']=='resource_created')
    other=dict(resource,channel='alternate',external_id=resource['external_id']+'-independent-candidate')
    proof(w,op,channels={'orders':[resource],'alternate':[other]})
    w.start_engine();hold.unlink();seen=wait_search(w,op);w.stop_engine()
    assert seen['evidence']['matches']==2 and not pending(w,op)['machines']
    checkpoint(w,op,'union across both channels before cardinality; one purchase, independently seeded second candidate',evidence=seen['evidence'])
    proof(w,op,channels={'orders':[],'alternate':[]},incomplete_channel='alternate')
    w.h.sql(f"UPDATE provisiond.operations SET mutation_wait_ended_at=clock_timestamp()-interval '61 seconds' WHERE id='{op}'")
    w.start_engine();eventually(lambda:snapshot(w,op)['commitment']['state']=='closed');w.stop_engine()
    assert pending(w,op)['operation']['resolution'] is None,'incomplete channel cannot establish absence'
    proof(w,op,channels={'orders':[resource],'alternate':[dict(resource,channel='alternate')]})
    hold_step=w.control/'hold-step-101';hold_step.touch();w.start_engine();w.wait_status(op,'succeeded');w.stop_engine();hold_step.unlink()
    assert len(snapshot(w,op)['machines'])==1,'same external identity across channels deduplicates before cardinality'
    one_purchase(w,op)
    w.h.sql("UPDATE provisiond.provider_accounts SET descriptor='%s' WHERE account='%s'"%(json.dumps(original),t['account']))

from w6_edges import measurement,episode_order,meter_edges,default_hooks

SCENARIOS=dict(zip(CASES,[measurement,positive,empty,delayed,conflicting,multiple,startup,human,caller_delete,delete_evidence,episodes,late,gone_reply,worker_exact,account,delete_classification,resolution_reply,exit_accounting,channels,episode_order,meter_edges,default_hooks],strict=True))

def w6(h,case=None,defect=None):
    global eventually,http
    from harness import eventually
    from workflow import Workflow,http
    h.config['w6']=dict(required_scenarios=CASES,selected_case=case,defect=defect,
        negative_seconds=60,clock='real UTC; explicit per-operation fixture age controls recorded',
        recovery_incidents='not run (W7)')
    w=Workflow(h,defect=defect,operator=True)
    try:
        for name in ([case] if case else CASES):
            w.case=name;SCENARIOS[name](w)
            h.results.append(dict(test=name,result='pass'))
        expected=[case] if case else CASES
        observed=[r['test'] for r in h.results if r['test'] in CASES]
        assert observed==expected,'W6 required scenario inventory incomplete or duplicated'
        h.event('w6_inventory_complete',required=expected,observed=observed)
    finally:w.close()
