"""Required small defect controls; exact assertions and observed activation only."""
import json
import subprocess
from pathlib import Path
from harness import ROOT, cap_check
import tempfile

CONTROLS=[
    ('w4_atomic','workflow','w4_atomic','rail payment and credit must be atomic','w4_payment_atomicity_observed'),
    ('w4_ack','workflow','w4_ack','step 8 must be acknowledged before provider mutation','defect_ack_boundary_observed'),
    ('w4_overload','workflow','w4_overload','overload must precede request business BEGIN','defect_overload_entry_observed'),
    ('execute_cancel','corrections','execute_cancel','cancelled execute must close retained transaction transport','execute_cancellation_observed'),
    ('checkout','corrections','checkout','whole checkout exceeded 250ms plus 150ms scheduling tolerance','whole_checkout_observed'),
    ('caller_sql','corrections','caller_sql','caller SQL must not change protected settings','caller_sql_policy_observed'),
    ('commit_setting','corrections','commit_setting','lowered synchronous_commit must be rejected before COMMIT','precommit_corruption_observed'),
    ('notices','corrections','notices','notice retention must remain capped at 16','notice_retention_observed'),
    ('notice_sticky','corrections','notice_sticky','COMMIT warning after notice cap must remain unresolved','commit_warning_after_flood_observed'),
    ('verify_cancel','corrections','verify_cancel','cancelled verification must close before reply release','verification_cancellation_observed'),
    ('generation','core','generation','stale engine generation must refuse transaction','stale_generation_observed'),
    ('claim_guard','core','claims','stale claim must affect zero rows','stale_claim_guard_observed'),
    ('payment_atomic','core','money','payment and credit must be atomic','payment_split_intermediate_observed'),
    ('claim_cancel','core','claim_cancel','cancelled claim must retain a registered worker owner','claim_cancellation_observed'),
    ('defer_cancel','core','defer_cancel','acknowledged cancelled defer must retire original worker capacity','defer_cancellation_observed'),
    ('licensing_stop','core','licensing_stop','stopped licensing must not enter provider execution','licensing_stop_observed'),
    ('outcome_stop','core','outcome_stop','returned outcome must win over stale cancellation snapshot','outcome_stop_observed'),
    ('deadline','core','deadline','record deadline must exit without a polled waiter','record_deadline_observed'),
    ('lock_observers','core','lock_observers','one lock loss must land records before exit','concurrent_lock_observers_observed'),
    ('surface','core','surface','unfenced transaction must refuse engine-only business methods','unfenced_engine_method_observed'),
    ('predispatch','core','predispatch','live pre-dispatch create must fail despite licensing marker','live_create_position_observed'),
    ('startup_order','core','startup','unsupported real engine startup must precede protected entry','real_startup_schema_observed'),
    ('terminal_zero','core','terminal_zero','zero-row stop must report refusal and end stale execution','terminal_zero_observed'),
    ('terminal_capacity','core','terminal_capacity','completed stop must retire worker capacity','terminal_capacity_observed'),
    ('loss_stop','core','loss_stop','user stop after detected loss must take later-stop exit','user_stop_after_loss_observed'),
    ('queued_claim','core','queued_claim','queued claim must not dispatch BEGIN after detected lock loss','queued_claim_observed'),
    ('queued_claim_pool','core','queued_claim_pool','queued claim must not dispatch BEGIN after detected lock loss','queued_claim_observed'),
    ('replacement_outcome','core','replacement_outcome','replacement claim must refuse settlement without its own outcome','replacement_outcome_publication_observed'),
    ('displaced_record','core','displaced_record','displaced pending record expiry must name its operation','displaced_record_expiry_observed'),
    ('lock_record_reset','core','lock_observers','lock-loss fanout must retain original record identity and absolute deadline','concurrent_lock_observers_observed'),
]


def controls(seed,evidence):
    cap_check()
    root=Path(tempfile.mkdtemp(prefix='controls-',dir=ROOT))
    results=[]
    for defect,phase,case,assertion,observation in CONTROLS:
        raw=root/(defect+'.json')
        command=['python3','tests/postgres/harness.py','--phase',phase,'--case',case,
                 '--defect',defect,'--seed',str(seed),'--evidence',str(raw)]
        child=subprocess.run(command,check=False)
        manifest=json.loads(raw.read_text()) if raw.exists() else {}
        events=manifest.get('events',[])
        failures=[e for e in events if e['event']=='failure']
        cleanup=[e for e in events if e['event']=='cleanup_complete']
        observed=[e for e in events if e['event']==observation]
        valid=(child.returncode==1 and len(failures)==1 and failures[0].get('type')=='AssertionError'
            and failures[0].get('message')==assertion and cleanup and cleanup[-1]['result']=='pass' and observed)
        if defect=='caller_sql':valid=valid and observed[-1].get('observed_synchronous_commit')=='off'
        if defect=='payment_atomic':valid=valid and observed[-1].get('counts')=='0:1'
        if defect=='claim_guard':valid=valid and observed[-1].get('affected') is True
        if defect=='defer_cancel':
            activation=observed[-1] if observed else {}
            valid=valid and activation.get('cancelled') is True and activation.get('process_survived') is True and activation.get('commit_completion_observed') is True and activation.get('durable_state')=='queued:1' and activation.get('held_owners')==dict(workers=1,available_slots=2) and activation.get('after_cancel')==activation.get('held_owners') and activation.get('completed_owners')==activation.get('held_owners') and activation.get('no_stop_or_simulator_effect') is True
            valid=valid and any(e['event']=='fault_activated' and e.get('direction')=='commit_reply' and e.get('session')==activation.get('session') for e in events)
        if defect=='generation':valid=valid and observed[-1].get('refused') is False
        if defect=='queued_claim_pool':valid=valid and observed[-1].get('window')=='pool' and observed[-1].get('forbidden_entry') is True
        if defect=='replacement_outcome':valid=valid and observed[-1].get('simulator_calls_before_release')==1 and observed[-1].get('replacement_settlement_reply',{}).get('ok') is True and observed[-1].get('unchanged_after_attempt') is False
        result='expected negative demonstration' if valid else 'fail'
        results.append(dict(defect=defect,result=result,exit_status=child.returncode,intended_assertion=assertion))
        if valid:
            manifest.update(result=result,negative_command_exit_status=1,intended_assertion=assertion)
            for scenario in manifest.get('scenarios',[]):
                if scenario['result']=='fail':scenario['result']='expected negative demonstration'
            # The raw child retains its actual fail. The final red manifest records
            # why the parent recognizes that precise failure as the expected control.
            if evidence:
                path=evidence.parent/(('w4-red-' if defect.startswith('w4_') else 'w3-red-')+defect+'.json')
                manifest['command']=['nix','develop','--command',*command[:-1],'<run-dir>/control-manifest.json']
                path.write_text(json.dumps(manifest,indent=2)+'\n')
                results[-1]['evidence']=str(path)
        else:
            break
    status=0 if len(results)==len(CONTROLS) and all(r['result']=='expected negative demonstration' for r in results) else 1
    if evidence:
        evidence.write_text(json.dumps(dict(result='pass' if status==0 else 'fail',controls=results),indent=2)+'\n')
    print(json.dumps(dict(result='pass' if status==0 else 'fail',controls=results)),flush=True)
    return status
