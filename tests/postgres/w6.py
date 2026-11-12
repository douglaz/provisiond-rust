"""OPS-27/29/30/33: reconciliation through real executables and independent effects."""
import json
import time
from harness import eventually
from workflow import Workflow
from w5 import admit, snapshot, external, orders, events, restore_resource

CASES=['w6_positive','w6_empty','w6_delayed','w6_conflicting','w6_multiple']

def inventory_complete(manifest):
    inventories=[e for e in manifest.get('events',[]) if e.get('event')=='w6_inventory_complete']
    return (manifest.get('result')=='pass' and len(inventories)==1
        and inventories[0]['required']==CASES and inventories[0]['observed']==CASES
        and [r['test'] for r in manifest.get('results',[]) if r['test'] in CASES]==CASES)

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
    assert state['commitment']['state']=='open' and state['commitment']['reserved_sats']==43, 'original-reservation-preserved before negative window'
    assert len(orders(w,op))==1, 'one purchase/no repeated order'
    w.h.event('w6_lost_reply_activated',operation=op,purchase=remote[-1]['resource'],snapshot=state,
              accepted_count=sum(e['stage']=='accepted' for e in external(w,op)),reply_delivered=False)
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
    _,_,other,hold=lost(w,'reconcile-in-progress',search='in_progress')
    w.h.sql(f"UPDATE provisiond.operations SET mutation_wait_ended_at=clock_timestamp()-interval '61 seconds' WHERE id='{other}'")
    w.start_engine();hold.unlink();wait_search(w,other)
    assert pending(w,other)['commitment']['state']=='open','matched in-progress keeps original commitment beyond negative window'
    checkpoint(w,other,'matched in-progress past negative window',negative_seconds=60)
    w.stop_engine();one_purchase(w,other)

def conflicting(w):
    for name,config in [('wrong',dict(search='wrong')),('failed',dict(search='normal',direct='failed')),('account',dict(search='normal',direct='wrong_account'))]:
        _,_,op,hold=lost(w,'reconcile-conflict-'+name,**config)
        w.start_engine();hold.unlink();wait_search(w,op)
        time.sleep(.2)
        result=pending(w,op)
        assert not result['machines'],'exact-correlator/no-attachment'
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

SCENARIOS=dict(zip(CASES,[positive,empty,delayed,conflicting,multiple]))

def w6(h,case=None,defect=None):
    h.config['w6']=dict(required_scenarios=CASES,selected_case=case,defect=defect,
        negative_seconds=60,clock='real UTC; explicit per-operation fixture age controls recorded',
        recovery_incidents='not run (W7)')
    w=Workflow(h,defect=defect)
    try:
        for name in ([case] if case else CASES):
            w.case=name;SCENARIOS[name](w)
            h.results.append(dict(test=name,result='pass'))
        expected=[case] if case else CASES
        observed=[r['test'] for r in h.results if r['test'] in CASES]
        assert observed==expected,'W6 required scenario inventory incomplete or duplicated'
        h.event('w6_inventory_complete',required=expected,observed=observed)
    finally:w.close()
