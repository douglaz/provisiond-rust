"""STO-54/56, OPS-15/41: actual operator recovery on isolated physical histories."""
import hashlib
import json
from pathlib import Path
import subprocess
import shutil
import socket
import time
import uuid
from harness import eventually
from core import environment
from workflow import Workflow, http, trace_events
from w5 import admit

CASES=['w7_ordinary','w7_rebuild','w7_lossless','w7_continuation','w7_supersession','w7_lossy',
       'w7_markers','w7_grace','w7_paused','w7_close','w7_reporting','w7_default_hooks']
CASES.append('w7_interruptions')
CASES.append('w7_credentials')
CASES.append('w7_legacy_upgrade')
CASES.extend(['w7_parent_retirement','w7_caller_grace','w7_coherent_rates'])
CASES.append('w7_attachment_order')
CASES.extend(['w7_expired_replay','w7_reporting_progress'])

def record(w):
    return json.loads(w.h.sql("SELECT COALESCE((SELECT to_jsonb(r) FROM provisiond.recovery_incidents r WHERE closed_at IS NULL),'null'::jsonb)::text"))

def operator(w,*args,ok=True,extra=None):
    h=w.h
    old=h.env
    prefix=h.root/f'recovery-{len(h.commands)}'
    stdout_path=prefix.with_suffix('.stdout')
    stderr_path=prefix.with_suffix('.stderr')
    h.env=dict(old,**environment(h),PROVISIOND_RECOVERY_LOG=str(prefix.with_suffix('.jsonl')),
        PROVISIOND_COMPONENTS_STOPPED='yes',
        PROVISIOND_RECOVERY_INTERVAL_SECONDS='3',PROVISIOND_RECOVERY_DERIVATION_GAP='10000')
    h.env.update(extra or {})
    h.env={key:value for key,value in h.env.items() if value is not None}
    if w.defect and w.defect!='w7_no_ack':h.env['STORE_TEST_DEFECT']=w.defect
    try:
        command=[str(h.target/'debug/recovery'),*args]
        try:
            result=h.run(command,ok=False,timeout=12)
        except subprocess.TimeoutExpired as error:
            for path,stream in ((stdout_path,error.stdout),(stderr_path,error.stderr)):
                if isinstance(stream,str):
                    path.write_text(stream)
                else:
                    path.write_bytes(stream or b'')
            error.add_note(f'Recovery command streams saved to {stdout_path} and {stderr_path}')
            raise
        stdout_path.write_text(result.stdout)
        stderr_path.write_text(result.stderr)
        if ok:
            assert result.returncode == 0, f'{command[0]} exited {result.returncode}: {result.stderr}'
        # STO-54/56: internal traces stay in the requested diagnostic file.
        # OVR-18/STO-55: failed standalone calls can also emit entry::alarm's
        # recovery operator alarm on stderr, alongside plain operator notices.
        for line in result.stderr.splitlines():
            line=line.strip()
            try:alarm=json.loads(line)
            except json.JSONDecodeError:
                assert not line.startswith(('{','[')),'STO-54 malformed recovery JSON on stderr'
                continue
            assert (result.returncode!=0 and isinstance(alarm,dict)
                and set(alarm)=={'event','component','message','threshold_ms','operator_action_required'}
                and alarm['event']=='alarm' and alarm['component']=='recovery'
                and isinstance(alarm['message'],str) and alarm['message'].strip()
                and type(alarm['threshold_ms']) is int and alarm['threshold_ms']==0
                and alarm['operator_action_required'] is True),'STO-54 recovery trace or invalid operator alarm on stderr'
        row=None
        if result.returncode==0 and args[0]!='--check-schema':
            assert len(result.stdout.splitlines())==1,'STO-56 recovery stdout must contain one result row'
            row=json.loads(result.stdout)
            assert row is None or isinstance(row,dict) and 'event' not in row,'STO-56 recovery stdout contains trace instead of result'
            if args[0] in ('prepare','parent','close'):
                expected=(record(w) if args[0]=='prepare' else json.loads(h.sql(f"SELECT to_jsonb(r) FROM provisiond.recovery_incidents r WHERE id='{args[1]}'")))
                assert row==expected,'STO-56 recovery action must return its persisted result row'
        else:assert not result.stdout,'STO-54 failed recovery must not print a result'
        log=Path(h.env['PROVISIOND_RECOVERY_LOG']) if 'PROVISIOND_RECOVERY_LOG' in h.env else None
        events=trace_events(log) if log and log.exists() else []
        if log:assert events,'STO-54 requested recovery log must receive trace events'
        h.event('recovery_cli_contract_observed',action=args[0],row=row,log_requested=log is not None,
            trace_events=events,stdout_path=str(stdout_path),stderr_path=str(stderr_path))
        h.event('recovery_cli_completed',action=args[0],exit_status=result.returncode,
            stdout_sha256=hashlib.sha256(result.stdout.encode()).hexdigest(),
            stderr_sha256=hashlib.sha256(result.stderr.encode()).hexdigest())
        return result
    finally:h.env=old

def establish(w,id=None,predecessor=None,extra=None,evidence_input=None):
    id=id or str(uuid.uuid4())
    evidence=w.h.root/('history-'+id+'.json')
    contents=dict(incident_id=id,reference='local-history/'+id,
        lower_unknown_reason='no certified source clock error or retained complete-record inclusion proof',
        upper_unknown_reason='local fence observed; no certified event clock error bound')
    contents.update(evidence_input or {})
    evidence.write_text(json.dumps(contents))
    result=operator(w,'establish',id,predecessor or 'none',extra=dict(PROVISIOND_RECOVERY_EVIDENCE=str(evidence),**(extra or {})))
    w.h.event('incident_establishment_acknowledged',incident=id,record=record(w),components_stopped=all(p.poll() is not None for p in w.children if getattr(p,'name',None) in ('api','engine')))
    return id,result

def promote(w):
    h=w.h
    h.fence_authority('primary',w)
    old=h.nodes.pop('primary')
    old_name='retired-'+str(len(h.retired_authorities))
    h.nodes[old_name]=old
    h.retired_authorities.remove('primary');h.retired_authorities.add(old_name)
    promoted=h.nodes.pop('standby')
    h.nodes['primary']=promoted
    h.ports[0],h.ports[1]=promoted['port'],old['port']
    h.run(['pg_ctl','-D',str(promoted['data']),'promote','-w','-t','15'])
    assert h.sql('SELECT pg_is_in_recovery()')=='f'
    h.event('recovery_promotion_observed',operator_initiated=True,old_authority=old_name,
        system_identifier=h.sql('SELECT system_identifier FROM pg_control_system()'),
        checkpoint=h.sql('SELECT timeline_id,redo_lsn FROM pg_control_checkpoint()'),components_stopped=True)

def bootstrap(w):
    h=w.h
    name='replacement-'+str(len(h.retired_authorities))
    h.backup(name,standby=True,source='primary')
    h.nodes['standby']=dict(data=h.root/name,port=h.ports[1])
    h.configure(h.root/name,h.ports[1]);h.start('standby');h.identity()
    assert h.sql("SHOW synchronous_commit")=='on'
    assert h.sql("SHOW synchronous_standby_names")=='FIRST 1 (provisiond_sync)'
    h.event('replacement_standby_verified',maintenance_mode_exception=False,prior_incident_required=False)

def continue_recovery(w):
    operator(w,'prepare')
    w.quiesced=False
    w.start_engine()
    eventually(lambda:record(w)['steps_completed']==2)
    w.launch_api()
    eventually(lambda:record(w)['steps_completed']==3)

def reissue(w,tenant,key):
    response=w.request('POST','/v1/recovery/revoke',{},tenant['recovery_credential'],key)
    w.private.append(response['spending_token'])
    tenant['spending_token']=response['spending_token']
    return response

def lossless(w):
    h=w.h
    # Dispatched effect survives independently. Hold only automatic evidence;
    # a second running create is observed before its about-to-order marker.
    barrier=h.root/'provider/hold-reconciliation';barrier.touch()
    a,_,ordered=admit(w,'recovery-dispatched')
    w.provider_control(mode='drop_reply');w.start_engine();w.wait_status(ordered,'needs_reconciliation');w.stop_engine()
    b,_,running=admit(w,'recovery-running-null')
    c,_,queued=admit(w,'recovery-queued')
    h.sql(f"UPDATE provisiond.operations SET available_at=clock_timestamp()+interval '1 hour' WHERE id='{queued}'")
    hold=w.control/'hold-step-1';hold.touch();w.start_engine()
    eventually(lambda:any(e.get('event')=='dispatch_step_held' and e.get('operation')==running for e in trace_events(w.engine.log)))
    assert h.sql(f"SELECT status||':'||(about_to_order_at IS NULL) FROM provisiond.operations WHERE id='{running}'")=='running:true'
    generation=h.sql('SELECT generation FROM provisiond.startup_generation')
    before={id:json.loads(h.sql(f"SELECT to_jsonb(o)::text FROM provisiond.operations o WHERE id='{id}'")) for id in (ordered,running,queued)}
    independent=w.external()['events']
    assert any(e['stage']=='resource_created' and e.get('command')==ordered for e in independent)
    h.sql("UPDATE provisiond.ceiling_counters SET used=0")
    h.event('w7_counter_reseed_fixture',surviving_admissions=3,prior_counters_seeded_zero=True)
    promote(w)
    # REC-02: actually attempt establishment without the replacement. The
    # checked store refuses before transaction entry; this is not a SyncRep claim.
    retained_event=str(uuid.uuid4())
    missing=operator(w,'establish',retained_event,'none',ok=False)
    assert missing.returncode!=0 and 'expected exactly one designated walsender, got 0' in missing.stderr
    assert h.sql('SELECT count(*) FROM provisiond.recovery_incidents')=='0'
    launched=None;blocked=True;startup_work=False;sync_wait=False;launch_error=None
    try:
        launched=w.start('engine')
        # Only the test-build admission defect can reach this point while the
        # deployment is quiesced. Observe the real generation-only COMMIT wait.
        eventually(lambda:any(e.get('event')=='startup_lock_acquired' for e in trace_events(launched.log)) or launched.poll() is not None)
        startup_work=any(e.get('event')=='startup_lock_acquired' for e in trace_events(launched.log))
        if startup_work:
            eventually(lambda:h.sql("SELECT count(*) FROM pg_stat_activity WHERE wait_event='SyncRep' AND query LIKE 'COMMIT%'")!='0')
            sync_wait=True
        blocked=not startup_work
        launch_error=launched.error.read_text()
    except AssertionError as error:
        assert str(error)=='STO-54 component restart prevented by local deployment fence'
    finally:
        if launched is not None and launched.poll() is None:launched.terminate();launched.wait(timeout=5)
    h.event('w7_no_ack_observed',component_launch_blocked=blocked,launched_pid=launched.pid if launched else None,
        launched_exit=launched.returncode if launched else None,incident_acknowledged=False,replacement_standby_absent=True,
        startup_work=startup_work,generation_commit_waited_in_syncrep=sync_wait,launch_error=launch_error,
        establishment_exit=missing.returncode,establishment_refused_before_begin=True)
    assert blocked and not startup_work,'STO-54 no component may start before synchronous incident acknowledgment'
    bootstrap(w)
    assert {id:json.loads(h.sql(f"SELECT to_jsonb(o)::text FROM provisiond.operations o WHERE id='{id}'")) for id in before}==before
    id,_=establish(w,retained_event)
    initial=record(w)
    h.event('w7_lossless_incident_observed',record=initial,three_rows_survived=True)
    assert initial is not None and initial['steps_completed']==0 and initial['grace_ends_at'] is None,'STO-54 lossless promotion must establish incident before component startup'
    hold.unlink();w.provider_control(mode='normal')
    continue_recovery(w)
    w.stop_engine()
    assert h.sql('SELECT generation FROM provisiond.startup_generation')!=generation
    for op in (ordered,running,queued):assert w.status(op)=='needs_reconciliation'
    assert all(h.sql(f"SELECT claim_number FROM provisiond.operations WHERE id='{op}'")==str(before[op]['claim_number']) for op in (running,queued))
    for tenant in (a,b,c):
        assert h.sql(f"SELECT used FROM provisiond.ceiling_counters WHERE principal='{tenant['tenant_id']}' AND ceiling='machines_created' ORDER BY interval_start DESC LIMIT 1")=='1'
        w.request('GET','/v1/balance',tenant=tenant,status=401)
        reissue(w,tenant,'recovery-replace-'+tenant['tenant_id'])
        w.request('GET','/v1/balance',tenant=tenant)
    saved=record(w)
    w.restart_api();w.start_engine();w.stop_engine()
    assert record(w)==saved,'STO-54 marked recovery steps must not repeat on successor'
    w.request('GET','/v1/balance',tenant=a)
    h.event('w7_causal_trace',scenario=w.case,incident=id,before=before,after=record(w),
        provider=independent,lossless_rows_survived=True,credentials_invalidated=True,
        generation_only_advance_preserved=True,queued_quarantine=True)
    barrier.unlink()

def ordinary(w):
    h=w.h
    tenant,_,op=admit(w,'unchanged-restart')
    h.sql(f"UPDATE provisiond.operations SET available_at=clock_timestamp()+interval '1 hour' WHERE id='{op}'")
    generations=[]
    for iteration in range(2):
        p=w.start_engine();w.stop_engine()
        generations.append(h.sql('SELECT generation FROM provisiond.startup_generation'))
        assert record(w) is None and w.status(op)=='queued'
    assert generations[0]!=generations[1]
    w.stop_api()
    before=len(h.proxy.connections)
    h.stop('primary',mode='immediate');h.start('primary');h.identity()
    w.launch_api();p=w.start_engine();w.stop_engine()
    assert record(w) is None and w.status(op)=='queued'
    assert len(h.proxy.connections)>before
    h.event('w7_causal_trace',scenario=w.case,incident=None,generations=generations,
        new_sessions_after_primary_crash=len(h.proxy.connections)-before,
        queued_preserved=True,uncertain_commit_arm='prior canonical commits/W5 phases; not activated here')

def rebuild(w):
    h=w.h
    w.stop_engine();w.stop_api();w.quiesced=True
    h.stop('standby')
    retired=h.nodes.pop('standby');h.nodes['intact-retired-standby']=retired
    h.backup('intact-rebuild',standby=True)
    h.nodes['standby']=dict(data=h.root/'intact-rebuild',port=h.ports[1])
    h.configure(h.root/'intact-rebuild',h.ports[1]);h.start('standby');h.identity()
    assert record(w) is None
    w.quiesced=False;w.launch_api();w.start_engine();w.stop_engine()
    h.event('w7_causal_trace',scenario=w.case,incident=None,application_quiesced=True,
        backup_name='bootstrap_backup',maintenance_mode_exception=False,verified_source_and_catchup=True)

def continuation(w):
    h=w.h
    tenant=w.enrol();w.fund(tenant,'continuation-funding')
    predecessor=record(w)
    promote(w);bootstrap(w)
    id,_=establish(w,predecessor=predecessor['id'] if predecessor else None)
    operator(w,'prepare');w.quiesced=False
    w.start_engine();eventually(lambda:record(w)['steps_completed']==2);w.stop_engine()
    saved=record(w)
    id,_=establish(w,id,saved['supersedes_incident_id'],extra=dict(PROVISIOND_RECOVERY_EXPECTED_INSTANT=saved['restore_instant']))
    after=record(w)
    h.event('w7_progress_retry_observed',before=saved,after=after,nonzero_progress=2)
    assert after==saved,'STO-56 same-ID establishment must preserve committed steps=2'
    mismatch=operator(w,'establish',id,saved['supersedes_incident_id'] or 'none',ok=False,
        extra=dict(PROVISIOND_RECOVERY_EVIDENCE=str(h.root/('history-'+id+'.json')),PROVISIOND_RECOVERY_EXPECTED_INSTANT='2000-01-01T00:00:00Z'))
    assert mismatch.returncode!=0 and record(w)==saved
    mismatch=operator(w,'establish',id,str(uuid.uuid4()),ok=False,
        extra=dict(PROVISIOND_RECOVERY_EVIDENCE=str(h.root/('history-'+id+'.json'))))
    assert mismatch.returncode!=0 and record(w)==saved
    w.launch_api();assert record(w)['steps_completed']==3
    replacement=reissue(w,tenant,'continuation-replacement')
    saved=record(w);index=h.sql('SELECT last_value FROM provisiond.deposit_derivation_index')
    w.restart_api();w.start_engine();w.stop_engine()
    assert record(w)==saved and h.sql('SELECT last_value FROM provisiond.deposit_derivation_index')==index
    w.request('GET','/v1/balance',tenant=tenant)
    if not w.default_build:
        hold=w.control/'hold-step-1';hold.touch()
        body=w.create_body(tenant)
        running=w.request('POST','/v1/machines',body,tenant,'continued-running-null',status=202)['id']
        p=w.start_engine()
        eventually(lambda:any(e.get('event')=='dispatch_step_held' and e.get('operation')==running for e in trace_events(p.log)))
        pause=w.control/'hold-dispatch';pause.touch()
        eventually(lambda:any(e.get('event')=='test_dispatch_paused' for e in trace_events(p.log)))
        queued=w.request('POST','/v1/machines',body,tenant,'continued-new-queued',status=202)['id']
        w.stop_engine();hold.unlink()
        eventually(lambda:h.sql(f"SELECT grace_ends_at<=clock_timestamp() FROM provisiond.recovery_incidents WHERE id='{id}'")=='t')
        w.start_engine();w.stop_engine();pause.unlink()
        assert w.status(running)=='needs_reconciliation' and w.status(queued)=='queued'
        assert h.sql(f"SELECT claim_number FROM provisiond.operations WHERE id='{queued}'")=='0'
        assert record(w)==saved
        w.request('GET','/v1/balance',tenant=tenant)
        h.event('w7_causal_trace',scenario=w.case,incident=id,after_grace_end=True,
            fresh_running_pass=running,new_queued_not_requarantined=queued,null_create_exception_still_suppressed=True)
    h.event('w7_causal_trace',scenario=w.case,incident=id,step2=saved,after=record(w),
        index_unchanged=index,reissued_generation=replacement['credential_generation'])

def supersession(w):
    if record(w) is None:continuation(w)
    before=record(w);assert before['steps_completed']==3
    promote(w);bootstrap(w)
    id,_=establish(w,predecessor=before['id'])
    after=record(w)
    h=w.h
    h.event('w7_supersession_observed',before=before,after=after)
    assert after['id']!=before['id'] and after['supersedes_incident_id']==before['id'] and after['steps_completed']==0 and after['grace_ends_at'] is None,'STO-56 supersession must start fresh progress and null grace'
    assert h.sql(f"SELECT closed_at IS NOT NULL FROM provisiond.recovery_incidents WHERE id='{before['id']}'")=='t'
    establish(w,before['id'],before['supersedes_incident_id'],extra=dict(PROVISIOND_RECOVERY_EXPECTED_INSTANT=before['restore_instant']))
    assert record(w)==after,'STO-56 retry of predecessor must not supersede its successor'
    generations=h.sql("SELECT jsonb_object_agg(id,credential_generation)::text FROM provisiond.tenants")
    continue_recovery(w);w.stop_engine()
    current_generations=json.loads(h.sql("SELECT jsonb_object_agg(id,credential_generation)::text FROM provisiond.tenants"))
    assert all(current_generations[k]==v+1 for k,v in json.loads(generations).items())
    h.event('w7_causal_trace',scenario=w.case,incident=id,predecessor=before,after=record(w),credential_bump_repeated_for_new_event=True)

def lossy(w):
    h=w.h
    # The backup deliberately inherits a completed but still-open predecessor.
    # History replacement must supersede it, just as a second promotion does.
    new_incident(w);w.stop_engine()
    from w6 import create,deletion
    residual_tenant,_,residual_machine=create(w,'rollback-residual-machine')
    residual_before=w.request('GET','/v1/machines/'+residual_machine,tenant=residual_tenant)
    balance_before=w.request('GET','/v1/balance',tenant=residual_tenant)
    barrier=h.root/'provider/hold-reconciliation';barrier.touch(exist_ok=True)
    tenant,body,op=admit(w,'rollback-create')
    h.sql(f"UPDATE provisiond.operations SET available_at=clock_timestamp()+interval '1 hour' WHERE id='{op}'")
    known_tenant,_,known_op=admit(w,'rollback-retained-correlator')
    w.provider_control(mode='drop_reply');w.start_engine();w.wait_status(known_op,'needs_reconciliation');w.stop_engine()
    w.provider_control(mode='normal')
    assert h.sql(f"SELECT correlator_kind FROM provisiond.operations WHERE id='{known_op}'")=='operation_uuid'
    assert h.sql(f"SELECT correlator_kind IS NULL FROM provisiond.operations WHERE id='{op}'")=='t'
    destination=w.request('POST','/v1/deposits',{'amount_sats':77},tenant,'rollback-payment-deposit')
    expired=w.request('POST','/v1/deposits',{'amount_sats':5},tenant,'expired-watch-binding')
    h.sql(f"UPDATE provisiond.deposits SET expires_at=clock_timestamp()-interval '1 second' WHERE id='{expired['id']}'")
    predecessor=record(w)
    backup_max_index=int(h.sql('SELECT max(derivation_index) FROM provisiond.deposits'))
    h.backup('lossy-backup')
    h.sql(f"UPDATE provisiond.operations SET available_at=clock_timestamp() WHERE id='{op}'")
    # Seed the extension state because extension admission is deliberately absent.
    # A commitment is not a ledger entry and replayed payments cannot rebuild it.
    h.sql(f"BEGIN; UPDATE provisiond.commitments SET reserved_sats=70,version=version+1 WHERE machine_id='{residual_machine}' AND state='open'; UPDATE provisiond.machines SET runway_until=runway_until+interval '1 hour' WHERE id='{residual_machine}'; COMMIT;")
    extension_view=w.request('GET','/v1/machines/'+residual_machine,tenant=residual_tenant)
    assert extension_view['runway_until']!=residual_before['runway_until']
    lost_gone=deletion(w,residual_tenant,residual_machine,'rollback-gone-write')
    allocated=[]
    for i in range(3):
        deposit=w.request('POST','/v1/deposits',{'amount_sats':7},tenant,'rolled-allocation-'+str(i))
        allocated.append(dict(deposit=deposit['id'],address=deposit['onchain']['address'],index=int(h.sql(f"SELECT derivation_index FROM provisiond.deposits WHERE id='{deposit['id']}'"))))
    w.start_engine();w.wait_status(op,'succeeded');w.wait_status(lost_gone,'succeeded');w.stop_engine()
    assert h.sql(f"SELECT state FROM provisiond.machines WHERE id='{residual_machine}'")=='deleted'
    lost_machine=h.sql(f"SELECT machine_id FROM provisiond.operations WHERE id='{op}'")
    unrecorded=[]
    for n in range(2):
        orphan,_,orphan_op=admit(w,'rollback-unrecorded-'+str(n))
        w.start_engine();w.wait_status(orphan_op,'succeeded');w.stop_engine()
        external=h.sql(f"SELECT m.external_id FROM provisiond.machines m JOIN provisiond.operations o ON o.machine_id=m.id WHERE o.id='{orphan_op}'")
        unrecorded.append(dict(operation=orphan_op,external_id=external,account=orphan['account']))
    assert {r['account'] for r in unrecorded}=={'sim-a','sim-b'}
    allocated=json.loads(h.sql(f"SELECT jsonb_agg(jsonb_build_object('deposit',id,'address',address,'index',derivation_index) ORDER BY derivation_index)::text FROM provisiond.deposits WHERE derivation_index>{backup_max_index}"))
    reissue(w,tenant,'rollback-token-revocation')
    payment_hash=h.sql(f"SELECT payment_hash FROM provisiond.deposits WHERE id='{destination['id']}'")
    settled_at=time.strftime('%Y-%m-%dT%H:%M:%SZ',time.gmtime())
    observations=[dict(rail='lightning',destination=destination['lightning']['invoice'],payment_hash=payment_hash,amount_sats=31,settled=True,preimage_known=True,settled_at=settled_at),
        dict(rail='onchain',destination=destination['onchain']['address'],txid=hashlib.sha256(op.encode()).hexdigest(),vout=0,amount_sats=46,confirmations=2,settled_at=settled_at)]
    for observation in observations:w.settlement(observation);w.processed(observation,True)
    assert h.sql(f"SELECT count(*) FROM provisiond.payments WHERE deposit_id='{destination['id']}'")=='2'
    acknowledged=json.loads(h.sql(f"SELECT jsonb_build_object('operation',(SELECT to_jsonb(o) FROM provisiond.operations o WHERE id='{op}'),'balance',(SELECT balance_after FROM provisiond.ledger_entries WHERE tenant_id='{tenant['tenant_id']}' ORDER BY seq DESC LIMIT 1))::text"))
    provider=w.external()['events']
    h.fence_authority('primary',w);h.stop('standby')
    retired=h.nodes.pop('primary');h.nodes['lossy-retired-primary']=retired
    h.retired_authorities.remove('primary');h.retired_authorities.add('lossy-retired-primary')
    h.nodes['lossy-retired-standby']=h.nodes.pop('standby');h.retired_authorities.add('lossy-retired-standby')
    restored=h.root/'lossy-restored'
    shutil.copytree(h.root/'lossy-backup',restored)
    h.owned_extra_paths.append(restored)
    h.configure(restored,h.ports[0])
    (restored/'postgresql.auto.conf').write_text('')
    h.nodes['primary']=dict(data=restored,port=h.ports[0]);h.start('primary')
    assert w.status(op)=='queued'
    assert h.sql(f"SELECT state FROM provisiond.machines WHERE id='{residual_machine}'")!='deleted'
    assert h.sql(f"SELECT count(*) FROM provisiond.machines WHERE id='{lost_machine}'")=='0'
    assert h.sql(f"SELECT count(*) FROM provisiond.payments WHERE deposit_id='{destination['id']}'")=='0'
    assert all(h.sql(f"SELECT count(*) FROM provisiond.operations WHERE id='{r['operation']}'")=='0' for r in unrecorded)
    assert all(h.sql(f"SELECT count(*) FROM provisiond.deposits WHERE id='{d['deposit']}'")=='0' for d in allocated)
    h.event('w7_lossy_history_observed',operation=op,acknowledged_before=acknowledged,
        restored_status=w.status(op),rolled_back_allocations=allocated,provider_survived=provider,
        history_replacement=True,local_machine_removed=True,payments_removed=True)
    bootstrap(w)
    id,_=establish(w,predecessor=predecessor['id'] if predecessor else None)
    continue_recovery(w);w.stop_engine()
    watched=w.events('api','recovery_watch_set_derived')[-1]['deposits']
    assert destination['id'] in watched and expired['id'] not in watched
    assert w.status(op)=='needs_reconciliation'
    assert h.sql(f"SELECT count(*) FROM provisiond.payments WHERE deposit_id='{destination['id']}'")=='2'
    reissue(w,tenant,'post-rollback-replacement')
    reissue(w,residual_tenant,'post-rollback-residual-replacement')
    restored_view=w.request('GET','/v1/machines/'+residual_machine,tenant=residual_tenant)
    restored_balance=w.request('GET','/v1/balance',tenant=residual_tenant)
    assert restored_view['runway_until']==residual_before['runway_until'] and restored_view['runway_until']!=extension_view['runway_until']
    assert restored_balance['balance_sats']==balance_before['balance_sats'] and restored_balance['committed_sats']==balance_before['committed_sats']
    assert h.sql(f"SELECT count(*) FROM provisiond.meter_alerts WHERE machine_id='{residual_machine}'")=='0'
    next_deposit=w.request('POST','/v1/deposits',{'amount_sats':9},tenant,'post-rollback-allocation')
    next_index=int(h.sql(f"SELECT derivation_index FROM provisiond.deposits WHERE id='{next_deposit['id']}'"))
    assert next_index>max(d['index'] for d in allocated),'STO-54 finite-gap skip must exceed every rolled-back index'
    assert next_deposit['onchain']['address'] not in {d['address'] for d in allocated}
    saved=record(w);w.restart_api();w.start_engine();w.stop_engine()
    assert record(w)==saved
    assert h.sql(f"SELECT count(*) FROM provisiond.payments WHERE deposit_id='{destination['id']}'")=='2'
    h.event('w7_causal_trace',scenario=w.case,incident=id,after=record(w),allocated_before_restore=allocated,next_index=next_index,
        payment_replay_count=2,provider_survived=True,residuals=['lost commitment/extension information is not rebuilt from payments','consistent history does not certify absence of loss'],
        seeded_extension=True,runway_before_backup=residual_before['runway_until'],runway_in_lost_history=extension_view['runway_until'],restored_runway=restored_view['runway_until'],consistent_meter_audit_does_not_certify_no_loss=True,
        limit='A finite skip-forward test is not proof of address non-reuse under an arbitrarily large unbounded rollback.')
    assert h.sql(f"SELECT count(*) FROM provisiond.recovery_account_passes WHERE incident_id='{id}'")=='0','STO-54 no complete post-restore pass while reconciliation is held'
    before_absence=json.loads(h.sql(f"SELECT jsonb_build_object('state',state,'state_observed_at',state_observed_at,'meter_stopped_at',meter_stopped_at) FROM provisiond.machines WHERE id='{residual_machine}'"))
    barrier.unlink();w.start_engine()
    account=residual_before['provider_account']
    eventually(lambda:h.sql(f"SELECT count(*) FROM provisiond.recovery_account_passes WHERE incident_id='{id}' AND provider_account='{account}'")=='1')
    def account_observation():
        # STO-53/54, PRV-36: read the persisted max, including the real
        # restored correlator's dispatch-to-observation latency. Never age it.
        return json.loads(h.sql(f"""
            SELECT jsonb_build_object('incident',p.incident_id,'account',p.provider_account,
                'machine_id',m.id,'external_id',m.external_id,
                'machine',jsonb_build_object('state',m.state,
                    'state_observed_at',m.state_observed_at,'meter_stopped_at',m.meter_stopped_at),
                'first_completed_at',p.first_completed_at,'second_completed_at',p.second_completed_at,
                'now',transaction_timestamp(),'declared_seconds',(a.descriptor->>'visibility_seconds')::numeric,
                'max_sample',sample.value,'effective_seconds',v.seconds,
                'absence_not_before',p.first_completed_at+v.seconds*interval '1 second',
                'remaining_seconds',greatest(0,extract(epoch FROM
                    p.first_completed_at+v.seconds*interval '1 second'-transaction_timestamp())),
                'absence_after_window',m.state_observed_at>=p.first_completed_at+v.seconds*interval '1 second',
                'second_pass_after_window',p.second_completed_at>=p.first_completed_at+v.seconds*interval '1 second')
            FROM provisiond.recovery_account_passes p
            JOIN provisiond.provider_accounts a ON a.account=p.provider_account
            JOIN provisiond.machines m ON m.id='{residual_machine}' AND m.provider_account=p.provider_account
            LEFT JOIN LATERAL (SELECT to_jsonb(o) AS value,
                extract(epoch FROM o.observed_at-o.dispatched_at) AS seconds
                FROM provisiond.provider_observations o WHERE o.provider_account=p.provider_account
                ORDER BY o.observed_at-o.dispatched_at DESC LIMIT 1) sample ON true
            CROSS JOIN LATERAL (SELECT greatest((a.descriptor->>'visibility_seconds')::numeric,
                COALESCE(sample.seconds,0)) AS seconds) v
            WHERE p.incident_id='{id}' AND p.provider_account='{account}'
        """))
    first=account_observation()
    h.event('w7_lossy_account_window',phase='first_complete_pass',**first)
    assert first['machine']==before_absence and first['machine']['state']!='deleted','STO-54 first complete account pass must record no absence'
    assert first['second_completed_at'] is None,'STO-54 first complete pass must precede an eligible second pass'
    # Wait from this incident/account's first completed pass plus its actual
    # effective window, with 20 seconds for scheduling/completion. The absolute
    # 180-second fixture cap prevents a widening window or a stalled sweep from
    # making the scenario unbounded. Polling performs reads only.
    wait_seconds=min(180,float(first['remaining_seconds'])+20)
    def later_absence():
        observed=account_observation()
        if observed['machine']['state']!='deleted':return False
        assert observed['absence_after_window'],'STO-54 absence must be observed after the effective window'
        if observed['second_completed_at'] is None:return False
        assert observed['second_pass_after_window'],'STO-54 only a later eligible complete pass may record absence'
        return observed
    second=eventually(later_absence,seconds=wait_seconds)
    h.event('w7_lossy_account_window',phase='later_complete_pass',wait_seconds=wait_seconds,**second)
    w.wait_status(known_op,'succeeded');w.stop_engine()
    assert w.status(op)=='needs_reconciliation','OPS-27 missing restored correlator must remain operator-owned'
    assert len([e for e in w.external()['events'] if e.get('stage')=='accepted' and e.get('command')==op])==1
    reports=w.events('engine','unrecorded_provider_resource')
    for resource in unrecorded:
        assert any(e['provider_account']==resource['account'] and e['external_id']==resource['external_id'] and e['auto_attached'] is False for e in reports)
        assert h.sql(f"SELECT count(*) FROM provisiond.machines WHERE provider_account='{resource['account']}' AND external_id='{resource['external_id']}'")=='0'
    h.event('w7_causal_trace',scenario=w.case,incident=id,lost_gone_write=True,first_pass_no_absence=True,
        second_pass_recorded_absence=True,restored_create_resolved_observed=True,known_correlator_operation=known_op,
        missing_correlator_operation=op,missing_correlator_requires_operator=True,no_duplicate_purchase=True,unrecorded_per_account=unrecorded)

def new_incident(w,api=True):
    predecessor=record(w)
    promote(w);bootstrap(w)
    id,_=establish(w,predecessor=predecessor['id'] if predecessor else None)
    operator(w,'prepare');w.quiesced=False
    w.start_engine();eventually(lambda:record(w)['steps_completed']==2)
    if api:w.launch_api();eventually(lambda:record(w)['steps_completed']==3)
    return id

def seed_operation(w,tenant,kind,status='queued'):
    # Explicit historical metadata; no install/rescue/suspension admission claim.
    return w.h.sql("INSERT INTO provisiond.operations(tenant_id,idempotency_key,kind,status,request_summary,correlation_id,rescue_address,rescue_port,error) VALUES('%s','seed-%s','%s','%s','{}','seeded-recovery-metadata','simulator-rescue',22,'{\"kind\":\"internal\",\"message\":\"seeded interrupted metadata\",\"retryable\":false,\"details\":{}}') RETURNING id"%(tenant['tenant_id'],uuid.uuid4(),kind,status)).splitlines()[0]

def markers(w):
    h=w.h;tenant=w.enrol();w.fund(tenant,'markers-funded')
    old=[seed_operation(w,tenant,k,s) for k,s in [('install','queued'),('install','running'),('rescue_inventory','needs_reconciliation')]]
    parents=[seed_operation(w,tenant,'suspend_tenant',s) for s in ('running','queued')]
    h.sql(f"UPDATE provisiond.operations SET write_started_at=clock_timestamp(),rescue_exited_cleanly=true WHERE id='{old[1]}'")
    id=new_incident(w);w.stop_engine();reissue(w,tenant,'markers-new-token')
    for op in old:
        view=w.request('GET','/v1/operations/'+op,tenant=tenant)
        assert view['error']['details']['rescue_exit']=='unknown'
        if view['kind']=='install':assert view['error']['details']['disk_effect']=='unknown'
    assert h.sql(f"SELECT count(*) FROM provisiond.recovery_waiting_parents WHERE incident_id='{id}' AND disposition IS NULL")=='2'
    assert all(h.sql(f"SELECT claim_number FROM provisiond.operations WHERE id='{op}'")=='0' for op in parents)
    fresh=seed_operation(w,tenant,'install','needs_reconciliation')
    w.start_engine();w.stop_engine()
    assert h.sql(f"SELECT restore_markers_untrusted FROM provisiond.operations WHERE id='{fresh}'")=='f'
    operator(w,'parent',id,parents[0],'confirmed')
    operator(w,'parent',id,parents[1],'cancelled')
    eventually(lambda:h.sql(f"SELECT grace_ends_at<=clock_timestamp() FROM provisiond.recovery_incidents WHERE id='{id}'")=='t')
    h.sql(f"UPDATE provisiond.operations SET status='running' WHERE id='{parents[0]}'")
    w.start_engine();w.stop_engine()
    assert w.status(parents[0])=='queued'
    assert h.sql(f"SELECT disposition FROM provisiond.recovery_waiting_parents WHERE incident_id='{id}' AND operation_id='{parents[0]}'")=='confirmed'
    assert operator(w,'parent',id,parents[0],'cancelled',ok=False).returncode!=0
    promote(w);bootstrap(w);next_id,_=establish(w,predecessor=id)
    assert h.sql(f"SELECT restore_markers_untrusted FROM provisiond.operations WHERE id='{fresh}'")=='t'
    h.event('w7_causal_trace',scenario=w.case,incident=next_id,seeded_operations=old,waiting_parents=parents,
        fresh_provenance_changed_only_on_new_event=fresh,not_run='fresh install/rescue admission and suspension fan-out; deferred workers')

def grace(w):
    from w6 import create,deletion
    h=w.h;tenant,_,machine=create(w,'grace-machine')
    suspended,_,suspended_machine=create(w,'grace-suspended-machine')
    retrying,_,retry_machine=create(w,'grace-fenced-retry-machine')
    unfenced,_,unfenced_machine=create(w,'grace-unfenced-retry-machine')
    op,_=seed_cleanup(w,tenant,machine,'grace-system-delete')
    suspended_op,_=seed_cleanup(w,suspended,suspended_machine,'grace-suspension-cleanup')
    retry_op,retry_episode=seed_cleanup(w,retrying,retry_machine,'grace-inherited-retry')
    unfenced_op,_=seed_cleanup(w,unfenced,unfenced_machine,'grace-unfenced-retry')
    h.sql(f"UPDATE provisiond.tenants SET status='suspended' WHERE id='{suspended['tenant_id']}'; UPDATE provisiond.operations SET requested_by='operator',system_reason=NULL WHERE id IN ('{retry_op}','{unfenced_op}'); UPDATE provisiond.machines SET destroy_committed='{retry_episode}' WHERE id='{retry_machine}'; UPDATE provisiond.commitments SET reserved_sats=70,version=version+1 WHERE machine_id IN ('{retry_machine}','{unfenced_machine}') AND state='open'")
    pending=w.enrol()
    id=new_incident(w,api=False)
    eventually(lambda:any(e.get('event')=='delete_decision' and e.get('operation')==op and e.get('decision')=='deferred' for e in w.events('engine')))
    assert record(w)['grace_ends_at'] is None
    for candidate in (op,suspended_op,retry_op,unfenced_op):
        eventually(lambda:h.sql(f"SELECT status='queued' AND write_started_at IS NULL FROM provisiond.operations WHERE id='{candidate}'")=='t')
    h.sql(f"UPDATE provisiond.enrolments SET expires_at=clock_timestamp()-interval '1 second' WHERE tenant_id='{pending['tenant_id']}'")
    retired=seed_operation(w,tenant,'power','failed')
    h.sql(f"UPDATE provisiond.operations SET updated_at=clock_timestamp()-interval '365 days' WHERE id='{retired}'")
    w.launch_api();end=record(w)['grace_ends_at']
    for candidate in (op,suspended_op,retry_op,unfenced_op):
        eventually(lambda:h.sql(f"SELECT available_at=grace_ends_at FROM provisiond.operations,provisiond.recovery_incidents WHERE operations.id='{candidate}' AND recovery_incidents.id='{id}'")=='t')
    assert h.sql(f"SELECT destroy_committed IS NULL FROM provisiond.machines WHERE id='{machine}'")=='t'
    time.sleep(1.1)
    assert h.sql(f"SELECT count(*) FROM provisiond.tenants WHERE id='{pending['tenant_id']}'")=='1'
    assert h.sql(f"SELECT count(*) FROM provisiond.operations WHERE id='{retired}'")=='1'
    for candidate in (op,suspended_op,retry_op,unfenced_op):w.wait_status(candidate,'succeeded')
    w.stop_engine()
    assert h.sql(f"SELECT result->>'no_mutation_required' FROM provisiond.operations WHERE id='{unfenced_op}'")=='true'
    assert h.sql(f"SELECT state='deleted' FROM provisiond.machines WHERE id='{retry_machine}'")=='t'
    eventually(lambda:h.sql(f"SELECT count(*) FROM provisiond.tenants WHERE id='{pending['tenant_id']}'")=='0')
    eventually(lambda:h.sql(f"SELECT count(*) FROM provisiond.operations WHERE id='{retired}'")=='0')
    assert record(w)['closed_at'] is None and record(w)['grace_ends_at']==end
    h.event('w7_causal_trace',scenario=w.case,incident=id,operation=op,null_then_future_claim_deferral=True,
        original_grace_end=end,settled_while_incident_open=True,ttl_and_retention_frozen_then_lifted=True,
        current_suspension_and_both_retry_origins=True,seeded_cleanup_and_retry_states=True)

def seed_cleanup(w,tenant,machine,suffix):
    h=w.h
    episode=h.sql(f"INSERT INTO provisiond.episodes(machine_id,key,reasons,state) VALUES('{machine}','delete','[\"exhausted\"]','attempting') RETURNING id").splitlines()[0]
    op=h.sql(f"INSERT INTO provisiond.operations(tenant_id,idempotency_key,kind,status,machine_id,provider_account,requested_by,system_reason,episode_id,request_summary,correlation_id) SELECT tenant_id,'{suffix}','delete','queued',id,provider_account,'system','exhausted','{episode}',jsonb_build_object('kind','delete','machine_id',id,'provider_account',provider_account,'external_id',external_id),'seeded-cleanup' FROM provisiond.machines WHERE id='{machine}' RETURNING id").splitlines()[0]
    h.sql(f"UPDATE provisiond.episodes SET current_operation_id='{op}' WHERE id='{episode}'")
    return op,episode

def stamped(w,currency,rows):
    for order,observed,accepted,bound,yielding in rows:
        w.h.sql("INSERT INTO provisiond.rate_observations(currency,acceptance_order,observed_at,accepted_at,staleness_bound_seconds,window_rate_num,window_rate_den) VALUES('%s',%d,'%s','%s',%d,%s,%s)"%(currency,order,observed,accepted,bound,'10' if yielding else 'NULL','1' if yielding else 'NULL'))

def paused(w):
    # Exact effective-time fixture, with acceptance lag, an older late row,
    # equal acceptance timestamps and a bound belonging to the newest evidence.
    rows=[(1,'2026-01-01T00:00:00Z','2026-01-01T00:00:00Z',3,True),
          (2,'2026-01-01T00:00:04Z','2026-01-01T00:00:06Z',3,True),
          (3,'2026-01-01T00:00:02Z','2026-01-01T00:00:06Z',99,False),
          (4,'2026-01-01T00:00:08Z','2026-01-01T00:00:08Z',2,True),
          (5,'2026-01-01T00:00:12Z','2026-01-01T00:00:14Z',3,True)]
    stamped(w,'USD-PROBE',rows)
    stamped(w,'EUR-PROBE',[(1,'2026-01-01T00:00:00Z','2026-01-01T00:00:00Z',30,True)])
    values={c:w.h.sql(f"SELECT provisiond.recovery_rate_present('{c}','2026-01-01T00:00:00Z','2026-01-01T00:00:16Z')") for c in ('USD-PROBE','EUR-PROBE')}
    assert float(values['USD-PROBE'])==6 and float(values['EUR-PROBE'])==16,'OPS-41 paused grace must sum stamped effective-time spans per currency'
    w.restart_api();w.start_engine();w.stop_engine()
    assert all(w.h.sql(f"SELECT provisiond.recovery_rate_present('{c}','2026-01-01T00:00:00Z','2026-01-01T00:00:16Z')")==v for c,v in values.items())
    w.h.event('w7_causal_trace',scenario=w.case,stamped_rows=rows,rate_present_seconds=values,
        subject_outage_closure_independent=True,seeded_history=True)
    from w6 import create
    tenant,_,machine=create(w,'paused-live-machine')
    funded,_,funded_machine=create(w,'paused-funded-machine')
    w.h.sql(f"UPDATE provisiond.commitments SET reserved_sats=0,version=version+1 WHERE machine_id='{machine}' AND state='open'")
    w.h.sql(f"UPDATE provisiond.meter_totals SET high_water_mark=clock_timestamp()-interval '1 second' WHERE subject_id='{machine}'")
    op,episode=seed_cleanup(w,tenant,machine,'paused-funding-cancel')
    w.h.sql(f"UPDATE provisiond.commitments SET reserved_sats=70,version=version+1 WHERE machine_id='{funded_machine}' AND state='open'")
    funded_op,funded_episode=seed_cleanup(w,funded,funded_machine,'paused-funded-cancel')
    w.h.sql("UPDATE provisiond.rates SET available=false WHERE currency='USD'")
    # Keep these seeded historical attempts queued until the event boundary.
    # create() has stopped the engine; new_incident starts only after establish.
    id=new_incident(w)
    end=record(w)['grace_ends_at']
    paused_dispatch=w.control/'hold-dispatch';paused_dispatch.touch()
    eventually(lambda:any(e.get('event')=='test_dispatch_paused' for e in trace_events(w.engine.log)))
    eventually(lambda:all(w.status(candidate)=='queued' for candidate in (op,funded_op)))
    w.stop_engine()
    eventually(lambda:w.h.sql(f"SELECT grace_ends_at<=clock_timestamp() FROM provisiond.recovery_incidents WHERE id='{id}'")=='t')
    # The return is a current accepting boundary, never backdated to observation.
    w.h.sql("UPDATE provisiond.rates SET available=true WHERE currency='USD'")
    paused_dispatch.unlink();w.start_engine()
    w.wait_status(funded_op,'succeeded')
    eventually(lambda:any(e.get('operation')==op and e.get('decision')=='deferred' for e in w.events('engine','delete_decision')))
    paused_dispatch.touch()
    eventually(lambda:any(e.get('event')=='test_dispatch_paused' for e in trace_events(w.engine.log)))
    eventually(lambda:w.status(op)=='queued');w.stop_engine()
    assert w.status(op)=='queued' and w.h.sql(f"SELECT destroy_committed IS NULL FROM provisiond.machines WHERE id='{machine}'")=='t'
    assert w.h.sql(f"SELECT state FROM provisiond.episodes WHERE id='{episode}'")=='attempting'
    assert w.h.sql(f"SELECT close_reason FROM provisiond.episodes WHERE id='{funded_episode}'")=='funded'
    paused_dispatch.unlink();w.start_engine()
    w.wait_status(op,'succeeded');w.stop_engine()
    assert record(w)['grace_ends_at']==end and record(w)['closed_at'] is None
    w.h.event('w7_causal_trace',scenario=w.case,incident=id,operation=op,
        seeded_cleanup=True,paused_after_wall_end=True,no_new_fence_while_paused=True,funded_abort_before_paused_check=True,
        successor_preserved_grace=True,original_end=end,accepted_return_spent_remaining_grace=True)

def close_case(w):
    from w6 import create
    h=w.h;tenant,_,machine=create(w,'close-machine')
    other,_,other_machine=create(w,'close-eur-machine')
    h.sql(f"UPDATE provisiond.machines SET native_terms=jsonb_set(native_terms,'{{offer,currency}}','\"EUR\"') WHERE id='{other_machine}'")
    parent=seed_operation(w,tenant,'suspend_tenant')
    id=new_incident(w,api=False)
    assert operator(w,'close',id,ok=False).returncode!=0
    w.launch_api()
    assert operator(w,'close',id,ok=False).returncode!=0
    eventually(lambda: h.sql(f"SELECT grace_ends_at<=clock_timestamp() FROM provisiond.recovery_incidents WHERE id='{id}'")=='t')
    eventually(lambda: h.sql(f"SELECT count(*) FROM provisiond.recovery_account_passes WHERE incident_id='{id}' AND second_completed_at IS NOT NULL")=='2')
    assert operator(w,'close',id,ok=False).returncode!=0
    operator(w,'parent',id,parent,'cancelled')
    assert operator(w,'close',id,ok=False).returncode!=0
    w.stop_engine()
    # Machine inventory is seeded for the close guard, never an invented provider
    # observation or a claim of general exhaustion/fan-out implementation.
    episode=h.sql(f"INSERT INTO provisiond.episodes(machine_id,key,reasons,state) VALUES('{machine}','delete','[\"exhausted\"]','stalled') RETURNING id").splitlines()[0]
    h.sql(f"UPDATE provisiond.machines SET destroy_committed='{episode}' WHERE id='{machine}'")
    # USD is discharged, EUR must independently block the close.
    assert operator(w,'close',id,ok=False).returncode!=0
    eur_episode=h.sql(f"INSERT INTO provisiond.episodes(machine_id,key,reasons,state) VALUES('{other_machine}','delete','[\"exhausted\"]','stalled') RETURNING id").splitlines()[0]
    h.sql(f"UPDATE provisiond.machines SET destroy_committed='{eur_episode}' WHERE id='{other_machine}'")
    h.sql(f"UPDATE provisiond.machines SET destroy_committed=NULL WHERE id='{other_machine}'")
    assert operator(w,'close',id,ok=False).returncode!=0
    h.sql(f"UPDATE provisiond.machines SET destroy_committed='{eur_episode}' WHERE id='{other_machine}'")
    operator(w,'close',id)
    assert record(w) is None
    w.start_engine();w.stop_engine()
    h.event('w7_causal_trace',scenario=w.case,incident=id,guarded_close=True,close_reblocked_after_fence_clear=True,
        closed_incident_ordinary_restart=True,seeded_machine_disposition=True,independent_currencies=['USD','EUR'])

def wait_quarantine(w):
    # STO-54(2): wait for the owner to receive COMMIT, plus its persisted mark.
    # A prepared trace or a primary-visible SyncRep transaction is insufficient.
    id=record(w)['id']
    w.h.event('w7_quarantine_wait_started',incident=id)
    eventually(lambda:record(w)['steps_completed']==2 and any(e.get('event')=='recovery_step_acknowledged'
        and e.get('incident')==id and e.get('step')==2 for e in trace_events(w.engine.log)))
    w.h.event('w7_quarantine_acknowledgment_observed',incident=id,record=record(w))

def reporting_progress(w):
    """STO-54/56: gate both preparation and delivery to the reporting waiter."""
    import concurrent.futures
    h=w.h;promote(w);bootstrap(w);id,_=establish(w)
    w.quiesced=False
    prepared=h.proxy.arm('SET steps_completed=2','send')
    delivery=None
    try:
        w.start_engine();connection=prepared.wait()
        engine_trace=trace_events(w.engine.log)
        assert any(e.get('event')=='recovery_loss_report' for e in engine_trace),'STO-54 supervised preparation must publish loss report in engine log'
        assert any(e.get('event')=='recovery_step_acknowledged' and e.get('step')==1 for e in engine_trace),'STO-54 supervised preparation must publish step-one acknowledgment in engine log'
        assert all('event' in e for e in engine_trace),'STO-56 supervised preparation must suppress CLI result rows'
        assert not any(line.startswith('{') for line in w.engine.error.read_text().splitlines()),'STO-54 supervised recovery events must stay in engine log'
        assert record(w)['steps_completed']==1
        # The old start/stop fixture would already terminate at this point.
        assert any(e.get('event')=='restart_pass_complete' for e in trace_events(w.engine.log))
        with concurrent.futures.ThreadPoolExecutor(max_workers=1) as pool:
            waiting=pool.submit(wait_quarantine,w)
            try:
                eventually(lambda:any(e.get('event')=='w7_quarantine_wait_started' for e in h.events))
                assert not waiting.done() and w.engine.poll() is None
                delivery=h.proxy.arm('store COMMIT','commit_reply',session=connection['pid'])
                prepared.release();delivery.wait()
                # Mark is committed and visible, but its acknowledgment is held.
                assert record(w)['steps_completed']==2
                assert not any(e.get('event')=='recovery_step_acknowledged' and e.get('step')==2 for e in trace_events(w.engine.log))
                assert not waiting.done() and w.engine.poll() is None
                h.event('w7_reporting_progress_gate_observed',incident=id,primary_mark=2,
                    restart_pass_complete=True,commit_reply_held=True,waiter_completed=False,engine_running=True)
            finally:
                prepared.release()
                if delivery:delivery.release()
            waiting.result(timeout=20)
        wait_quarantine(w);w.stop_engine()
        assert record(w)['steps_completed']==2
        h.event('w7_causal_trace',scenario=w.case,incident=id,prepared_gate_activated=True,
            committed_reply_gate_activated=True,stop_after_acknowledgment=True,after=record(w))
    finally:
        prepared.release()
        if delivery:delivery.release()

def reporting(w):
    promote(w);bootstrap(w)
    id=str(uuid.uuid4());path=w.h.root/'malformed-report.json';path.write_text('{malformed')
    result=operator(w,'establish',id,'none',ok=False,extra=dict(PROVISIOND_RECOVERY_EVIDENCE=str(path)))
    w.h.event('w7_reporting_file_observed',exit_status=result.returncode,case='malformed initial')
    assert result.returncode==0,'STO-54 malformed reporting file must not hold establishment'
    assert record(w)['history_evidence']['reporting_input']=='malformed' and record(w)['history_evidence']['invalid_certificate_input'] is True
    assert 'reporting input malformed JSON' in result.stderr
    operator(w,'prepare');w.quiesced=False;w.start_engine()
    wait_quarantine(w);w.stop_engine()
    before=record(w);assert before['steps_completed']==2
    path.unlink()
    result=operator(w,'establish',id,'none',ok=False,extra=dict(PROVISIOND_RECOVERY_EVIDENCE=str(path),PROVISIOND_RECOVERY_EXPECTED_INSTANT=before['restore_instant']))
    assert result.returncode==0 and record(w)==before,'STO-54 missing reporting file must preserve nonzero-progress retry'
    continue_recovery(w);w.stop_engine()
    before=record(w)
    finite=before['lost_window_starts_at'] is not None and before['loss_end_at'] is not None
    w.h.event('w7_reporting_observed',record=before,finite_duration=finite)
    assert before['lost_window_starts_at'] is None and before['loss_end_at'] is None and before['history_evidence']['finite_duration_established'] is False,'STO-54 bare timestamp or LSN must not establish finite loss duration'
    w.stop_engine();w.stop_api()
    establish(w,id,before['supersedes_incident_id'])
    assert record(w)==before
    w.h.event('w7_reporting_observed',record=record(w),finite_duration=False)
    w.h.event('w7_causal_trace',scenario=w.case,incident=id,unknown_lower=True,unknown_upper=True,
        missing_preconditions=['certified source clock error','continuous matching-prefix inclusion certificate','certified event clock error'],known_endpoint_arm='not run; operator clock/provenance prerequisites unavailable')
    promote(w);bootstrap(w)
    missing_id=str(uuid.uuid4())
    missing=operator(w,'establish',missing_id,id,extra=dict(PROVISIOND_RECOVERY_EVIDENCE=str(path)))
    assert missing.returncode==0 and record(w)['history_evidence']['finite_duration_established'] is False
    assert record(w)['history_evidence']['reporting_input']=='absent' and record(w)['history_evidence']['invalid_certificate_input'] is False,'STO-54 missing reporting input is absent, not invalid'
    assert 'no reporting input supplied' in missing.stderr
    id=missing_id;continue_recovery(w);w.stop_engine()
    w.h.event('w7_causal_trace',scenario=w.case,incident=id,initial_missing_reporting_file=True,no_extra_reporting_hold=True)
    promote(w);bootstrap(w)
    absent_id=str(uuid.uuid4())
    absent=operator(w,'establish',absent_id,id,extra=dict(PROVISIOND_RECOVERY_EVIDENCE=None,PROVISIOND_RECOVERY_LOG=None))
    evidence=record(w)['history_evidence']
    assert evidence['reporting_input']=='absent' and evidence['invalid_certificate_input'] is False,'STO-54 unset reporting input is absent, not invalid'
    assert evidence['finite_duration_established'] is False and 'no reporting input supplied' in absent.stderr
    id=absent_id;continue_recovery(w);w.stop_engine()
    quiet=operator(w,'prepare',extra=dict(PROVISIOND_RECOVERY_LOG=None))
    assert json.loads(quiet.stdout)==record(w) and not quiet.stderr,'STO-54 quiet preparation must return only its result'
    w.h.event('w7_causal_trace',scenario=w.case,incident=id,initial_unset_reporting_input=True,quiet_default=True,no_extra_reporting_hold=True)
    promote(w);bootstrap(w)
    unreadable_id=str(uuid.uuid4())
    unreadable=operator(w,'establish',unreadable_id,id,extra=dict(PROVISIOND_RECOVERY_EVIDENCE=str(w.h.root)))
    evidence=record(w)['history_evidence']
    assert evidence['reporting_input']=='malformed' and evidence['invalid_certificate_input'] is True,'STO-54 unreadable reporting input remains invalid'
    assert 'reporting input unreadable' in unreadable.stderr
    id=unreadable_id;continue_recovery(w);w.stop_engine()
    w.h.event('w7_causal_trace',scenario=w.case,incident=id,unreadable_reporting_input=True,no_extra_reporting_hold=True)
    for field in ('lower','upper'):
        promote(w);bootstrap(w)
        id,_=establish(w,predecessor=id,evidence_input={field:{'timestamp':'2026-01-01T00:00:00Z','lsn':'0/1234'}})
        assert record(w)['lost_window_starts_at'] is None and record(w)['loss_end_at'] is None
        assert record(w)['history_evidence']['invalid_certificate_input'] is True
        continue_recovery(w);w.stop_engine()
        w.h.event('w7_causal_trace',scenario=w.case,incident=id,independent_invalid_endpoint=field,after=record(w),no_extra_reporting_hold=True)

def default_hooks(w):
    w.h.env['STORE_TEST_DEFECT']='w7_retry_reset'
    continuation(w)
    assert w.default_build
    for defect in ('w7_lossless_restart','w7_supersession_inherits','w7_finite_guess'):
        w.h.env['STORE_TEST_DEFECT']=defect
        supersession(w)
        assert record(w)['lost_window_starts_at'] is None and record(w)['loss_end_at'] is None
    w.h.env['STORE_TEST_DEFECT']='w7_no_ack'
    prior=record(w);promote(w);event=str(uuid.uuid4())
    refused=operator(w,'establish',event,prior['id'],ok=False)
    assert refused.returncode!=0 and 'expected exactly one designated walsender, got 0' in refused.stderr
    assert record(w)==prior
    bootstrap(w);establish(w,event,prior['id']);continue_recovery(w);w.stop_engine()
    w.quiesced=True
    try:w.start('engine')
    except AssertionError as error:assert str(error)=='STO-54 component restart prevented by local deployment fence'
    else:raise AssertionError('default recovery deployment guard must ignore defect environment')
    w.quiesced=False
    w.h.event('w7_causal_trace',scenario=w.case,default_features=True,defect_environment_ignored=True,
        controls=['w7_retry_reset','w7_lossless_restart','w7_supersession_inherits','w7_finite_guess','w7_no_ack'])

def credentials(w):
    import concurrent.futures
    h=w.h;tenant=w.enrol()
    token=tenant['spending_token'];recovery=tenant['recovery_credential']
    code,_,_=http(h.ports[4],'POST','/v1/recovery/revoke',token=token,key='bad-body-auth-first',raw=b'{broken')
    assert code==401,'API-56 spending credential cannot reach recovery validation'
    w.request('POST','/v1/recovery/revoke',{'unknown':True},recovery,'invalid-revoke-body',status=400)
    response=reissue(w,tenant,'pending-recovery-key')
    assert response['credential_generation']==1
    repeat=w.request('POST','/v1/recovery/revoke',{},recovery,'pending-recovery-key',status=409)
    assert repeat['error']['details']['reason']=='credential_already_replaced'
    w.fund(tenant,'recovery-maintenance-funded')
    before=h.sql(f"SELECT balance_after FROM provisiond.ledger_entries WHERE tenant_id='{tenant['tenant_id']}' ORDER BY seq DESC LIMIT 1")
    def revoke(key):return http(h.ports[4],'POST','/v1/recovery/revoke',{},recovery,key)
    with concurrent.futures.ThreadPoolExecutor(max_workers=2) as pool:
        responses=list(pool.map(revoke,['concurrent-recovery-a','concurrent-recovery-b']))
    assert all(r[0]==200 for r in responses)
    values=sorted([r[1] for r in responses],key=lambda v:v['credential_generation'])
    w.private.extend(v['spending_token'] for v in values)
    assert [v['credential_generation'] for v in values]==[2,3]
    w.request('GET','/v1/balance',tenant=values[0]['spending_token'],status=401)
    tenant['spending_token']=values[1]['spending_token'];w.request('GET','/v1/balance',tenant=tenant)
    h.sql(f"UPDATE provisiond.tenants SET status='suspended' WHERE id='{tenant['tenant_id']}'")
    suspended=reissue(w,tenant,'suspended-recovery-key');assert suspended['credential_generation']==4
    w.request('POST','/v1/deposits',{'amount_sats':9},tenant,'pending-recovery-key',status=409)
    assert h.sql(f"SELECT balance_after FROM provisiond.ledger_entries WHERE tenant_id='{tenant['tenant_id']}' ORDER BY seq DESC LIMIT 1")==before
    assert h.sql(f"SELECT count(*) FROM provisiond.credential_replacements WHERE tenant_id='{tenant['tenant_id']}'")=='4'
    from http.client import HTTPConnection
    hold=w.control/'hold-http-response';hold.touch()
    held_before=len(w.events('api','http_response_held'))
    connection=HTTPConnection('127.0.0.1',h.ports[4],timeout=8)
    connection.request('POST','/v1/recovery/revoke',body=b'{}',headers={'Content-Type':'application/json','Authorization':'Bearer '+recovery,'Idempotency-Key':'lost-replacement-reply'})
    held=eventually(lambda:w.events('api','http_response_held')[held_before:])[-1]
    assert held['status']==200
    connection.close()
    eventually(lambda:any(e.get('correlation_id')==held['correlation_id'] for e in w.events('api','http_reply_lost')))
    hold.unlink()
    replay=w.request('POST','/v1/recovery/revoke',{},recovery,'lost-replacement-reply',status=409)
    assert replay['error']['details']['reason']=='credential_already_replaced'
    assert h.sql(f"SELECT credential_generation FROM provisiond.tenants WHERE id='{tenant['tenant_id']}'")=='5'
    replacement=reissue(w,tenant,'replace-after-lost-reply');assert replacement['credential_generation']==6
    retained=json.loads(h.sql(f"SELECT jsonb_agg(to_jsonb(r)) FROM provisiond.credential_replacements r WHERE tenant_id='{tenant['tenant_id']}'"))
    assert len(retained)==6 and all(set(r)=={'tenant_id','key','fingerprint','created_at'} for r in retained)
    assert h.sql(f"SELECT balance_after FROM provisiond.ledger_entries WHERE tenant_id='{tenant['tenant_id']}' ORDER BY seq DESC LIMIT 1")==before
    h.event('w7_causal_trace',scenario=w.case,lost_acknowledged_replacement_reply=True,same_key_refused_without_second_replacement=True,
        fresh_recovery_key_generation=6,retained_replacement_columns=sorted(retained[0]),lost_response_correlation=held['correlation_id'])

    h.event('w7_causal_trace',scenario=w.case,authentication_before_body=True,pending_and_suspended_maintenance=True,
        concurrent_generations=[2,3],latest_generation=4,old_token_dead=True,balance_unchanged=True,bearer_response_retained=False)

def legacy_upgrade(w):
    from pathlib import Path
    from core import binary
    h=w.h;w.stop_engine();w.stop_api()
    h.sql('DROP SCHEMA provisiond CASCADE; CREATE SCHEMA provisiond AUTHORIZATION migrator;')
    programs='\n'.join(Path('migrations/'+name).read_text() for name in ('0001-core.sql','0002-local-service.sql','0003-reconciliation.sql'))
    h.sql("BEGIN; CREATE TABLE provisiond.schema_migrations(version bigint PRIMARY KEY,semantics bigint NOT NULL,completed_at timestamptz NOT NULL DEFAULT transaction_timestamp());"+programs+"INSERT INTO provisiond.schema_migrations(version,semantics) VALUES(1,1),(2,2),(3,3); COMMIT;",role='migrator')
    # Legacy metadata fixtures stand in for kinds this prototype never admitted.
    h.sql("ALTER TABLE provisiond.operations DROP CONSTRAINT operations_kind_check; ALTER TABLE provisiond.operations ADD CONSTRAINT operations_kind_check CHECK(kind IN ('create','refresh','power','delete','install','rescue_inventory')); ALTER TABLE provisiond.operations ADD COLUMN restore_markers_untrusted boolean;")
    ids=[]
    for known in (False,True):
        id=str(uuid.uuid4());ids.append(id)
        value='false' if known else 'NULL'
        h.sql(f"INSERT INTO provisiond.operations(id,tenant_id,idempotency_key,kind,status,request_summary,correlation_id,restore_markers_untrusted,error) VALUES('{id}','legacy-metadata','legacy-{id}','install','failed','{{}}','legacy-metadata',{value},'{{\"kind\":\"internal\",\"message\":\"retained legacy error\",\"retryable\":false,\"details\":{{\"rescue_address\":\"retained-rescue\",\"rescue_port\":22}}}}')")
    incident=str(uuid.uuid4());h.sql(f"INSERT INTO provisiond.recovery_incidents(id) VALUES('{incident}')")
    generation=h.sql('SELECT generation FROM provisiond.startup_generation')
    binary(h,'migrate',role='migrator')
    assert h.sql(f"SELECT restore_markers_untrusted FROM provisiond.operations WHERE id='{ids[0]}'")=='t'
    assert h.sql(f"SELECT restore_markers_untrusted FROM provisiond.operations WHERE id='{ids[1]}'")=='f'
    view=json.loads(h.sql(f"SELECT body FROM provisiond.operation_views WHERE id='{ids[0]}'"))
    assert view['error']['details']['disk_effect']=='unknown' and view['error']['details']['rescue_exit']=='unknown'
    assert view['error']['details']['rescue_address']=='retained-rescue' and view['error']['details']['rescue_port']==22
    for name in ('api','engine'):
        result=binary(h,name,ok=False)
        assert result.returncode!=0 and 'open recovery incident unsupported' in result.stderr
        assert 'generation_advanced' not in result.stdout and 'api_listening' not in result.stdout
    assert record(w)['legacy'] is True and record(w)['restore_instant'] is None
    assert h.sql('SELECT generation FROM provisiond.startup_generation')==generation
    h.event('w7_causal_trace',scenario=w.case,legacy_incident=incident,conservative_null_backfill=True,
        preexisting_nonnull_preserved=True,settled_error_unknown=True,retained_rescue_endpoint_preserved=True,
        unsupported_legacy_prerequisites_not_fabricated=True,seeded_metadata=True)

def interruptions(w):
    h=w.h
    tenant=w.enrol();w.fund(tenant,'phase-interruptions')
    predecessor=record(w)
    promote(w);bootstrap(w)
    id=str(uuid.uuid4())
    evidence=h.root/'interrupted-event-evidence.json'
    evidence.write_text(json.dumps(dict(incident_id=id,reference='local-history/'+id,
        lower_unknown_reason='source clock and prefix certificate unavailable',upper_unknown_reason='event clock error bound unavailable')))
    env=dict(h.env,**environment(h),PROVISIOND_COMPONENTS_STOPPED='yes',
        PROVISIOND_RECOVERY_INTERVAL_SECONDS='3',PROVISIOND_RECOVERY_DERIVATION_GAP='10000',PROVISIOND_RECOVERY_EVIDENCE=str(evidence))
    for committed in (False,True):
        gate=h.proxy.arm('store COMMIT','commit_reply' if committed else 'send')
        out=h.root/f'establish-{committed}.stdout';err=h.root/f'establish-{committed}.stderr'
        with out.open('w') as stdout,err.open('w') as stderr:
            p=subprocess.Popen([str(h.target/'debug/recovery'),'establish',id,predecessor['id'] if predecessor else 'none'],env=env,stdout=stdout,stderr=stderr)
        h.extra.append(p)
        connection=gate.wait()
        before=record(w)
        assert (before is not None)==committed
        p.terminate();p.wait(timeout=5)
        h.proxy.close_connection(connection,'interrupted-establishment');gate.release();h.proxy.gates.remove(gate)
        eventually(lambda:h.sql('SELECT count(*) FROM pg_stat_activity WHERE pid='+str(connection['pid']))=='0')
        h.event('w7_establishment_reply_lost',incident=id,committed=committed,exit_status=p.returncode,retained_before=before)
    establish(w,id,predecessor['id'] if predecessor else None)
    operator(w,'prepare');assert record(w)['steps_completed']==1
    w.quiesced=False
    gate=h.proxy.arm('SET steps_completed=2','send')
    p=w.start('engine',dict(PROVISIOND_TEST_CONTROL=str(w.control)));w.engine=p
    connection=gate.wait();assert record(w)['steps_completed']==1
    p.terminate();p.wait(timeout=5)
    h.proxy.close_connection(connection,'interrupted-phase-2');gate.release();h.proxy.gates.remove(gate)
    w.start_engine();eventually(lambda:record(w)['steps_completed']==2);w.stop_engine()
    saved=record(w);establish(w,id,saved['supersedes_incident_id']);assert record(w)==saved
    gate=h.proxy.arm('SET steps_completed=3','send')
    p=w.start('api',dict(**w.operator_env,PROVISIOND_API_LISTEN=f'127.0.0.1:{h.ports[4]}',
        PROVISIOND_RAIL_SIMULATOR=f'127.0.0.1:{h.ports[6]}',PROVISIOND_LOCAL_PARAMETERS=str(w.parameters),PROVISIOND_TEST_CONTROL=str(w.control)))
    w.api=p
    connection=gate.wait()
    with socket.socket() as probe:assert probe.connect_ex(('127.0.0.1',h.ports[4]))!=0,'STO-54 listener must remain down before phase 3 mark'
    assert record(w)['steps_completed']==2 and record(w)['grace_ends_at'] is None
    skipped=int(h.sql('SELECT last_value FROM provisiond.deposit_derivation_index'))
    p.terminate();p.wait(timeout=5);w.reserve_api_ports()
    h.proxy.close_connection(connection,'interrupted-phase-3');gate.release();h.proxy.gates.remove(gate)
    w.launch_api();assert record(w)['steps_completed']==3
    assert int(h.sql('SELECT last_value FROM provisiond.deposit_derivation_index'))>skipped
    reissue(w,tenant,'interrupted-replacement')
    saved=record(w);w.restart_api();assert record(w)==saved
    h.event('w7_causal_trace',scenario=w.case,incident=id,establishment_no_commit_and_committed=True,
        interrupted_step2=True,interrupted_step3=True,steps2_retry_preserved=True,listener_closed_before_mark=True,after=record(w))

def parent_retirement(w):
    # STO-14/25 and STO-54: historical waiting-parent metadata exercises real
    # operator cancellation and API housekeeping, without a fan-out worker.
    h=w.h;tenant=w.enrol();w.fund(tenant,'parent-retention-funding')
    parent=seed_operation(w,tenant,'suspend_tenant')
    id=new_incident(w);w.stop_engine();reissue(w,tenant,'parent-retention-token')
    operator(w,'parent',id,parent,'cancelled')
    key='historical-parent-key'
    h.sql(f"INSERT INTO provisiond.idempotency_records(scope_kind,scope_id,key,fingerprint,resource_kind,resource_id,status,response_body,expires_at) VALUES('tenant','{tenant['tenant_id']}','{key}','historical-parent','operation','{parent}',202,'{{}}',clock_timestamp()-interval '1 second')")
    eventually(lambda:record(w)['grace_ends_at'] is not None and h.sql(f"SELECT grace_ends_at<=clock_timestamp() FROM provisiond.recovery_incidents WHERE id='{id}'")=='t')
    # Poll the actual service and row, so a failed required background task is
    # observed at this assertion rather than as a manual DELETE surrogate.
    deadline=time.monotonic()+5
    while time.monotonic()<deadline and w.api.poll() is None and h.sql(f"SELECT count(*) FROM provisiond.operations WHERE id='{parent}'")!='0':time.sleep(.05)
    state=dict(api_exit=w.api.poll(),remaining=h.sql(f"SELECT count(*) FROM provisiond.operations WHERE id='{parent}'"),
        disposition=h.sql(f"SELECT disposition FROM provisiond.recovery_waiting_parents WHERE incident_id='{id}' AND operation_id='{parent}'"))
    h.event('w7_parent_retirement_observed',operation=parent,**state)
    assert state['api_exit'] is None and state['remaining']=='0','STO-14 cancelled recovery parent must retire without stopping API'
    assert state['disposition']=='cancelled'
    assert h.sql(f"SELECT kind FROM provisiond.retired_operations WHERE id='{parent}'")=='suspend_tenant'
    w.request('GET','/v1/operations/'+parent,tenant=tenant,status=410)
    reply=w.request('POST','/v1/machines',w.create_body(tenant),tenant,key,status=409)
    assert reply['error']['details']['reason']=='idempotency_mismatch'
    w.request('GET','/v1/balance',tenant=tenant)
    h.event('w7_causal_trace',scenario=w.case,incident=id,operation=parent,retired_identity_and_key_refusal=True,seeded_historical_parent=True)

def caller_grace(w):
    # OPS-39/41: caller-requested deletion remains usable at both real phase
    # boundaries; system-attempt deferral is exercised separately by w7_grace.
    from w6 import create,deletion
    h=w.h;tenant,_,machine=create(w,'caller-null-grace')
    future,_,future_machine=create(w,'caller-future-grace')
    op=deletion(w,tenant,machine,'caller-before-step3')
    id=new_incident(w,api=False)
    deadline=time.monotonic()+4
    while time.monotonic()<deadline and w.status(op)!='succeeded':time.sleep(.05)
    h.event('w7_caller_grace_observed',operation=op,status=w.status(op),incident=record(w))
    assert w.status(op)=='succeeded' and record(w)['grace_ends_at'] is None,'OPS-41 restore grace must permit caller delete before step3'
    w.stop_engine()
    # Seed only the original queued operation's eligibility: API deletion below
    # is admitted normally, then executes after step 3 with future wall grace.
    w.launch_api();reissue(w,future,'caller-future-token')
    op=deletion(w,future,future_machine,'caller-after-step3')
    assert h.sql(f"SELECT grace_ends_at>clock_timestamp() FROM provisiond.recovery_incidents WHERE id='{id}'")=='t'
    w.start_engine();w.wait_status(op,'succeeded');w.stop_engine()
    assert h.sql(f"SELECT state FROM provisiond.machines WHERE id='{future_machine}'")=='deleted'
    h.event('w7_causal_trace',scenario=w.case,incident=id,null_grace_caller_deleted=True,future_grace_caller_deleted=True,provider=w.external()['events'])

def coherent_rates(w):
    # STO-49/OPS-41: supported current fixture inputs must durably feed the
    # historical reader. No second hand-maintained USD history is supplied.
    h=w.h
    before=h.sql("SELECT count(*) FROM provisiond.rate_observations WHERE currency='USD'")
    h.sql("UPDATE provisiond.rates SET available=false WHERE currency='USD'")
    after=h.sql("SELECT count(*) FROM provisiond.rate_observations WHERE currency='USD'")
    h.event('w7_rate_input_observed',before=before,after=after)
    assert int(after)==int(before)+1,'STO-49 supported rate update must stamp shared recovery history'
    row=json.loads(h.sql("SELECT to_jsonb(r) FROM provisiond.rate_observations r WHERE currency='USD' ORDER BY acceptance_order DESC LIMIT 1"))
    assert row['window_rate_num'] is None
    boundary=row['accepted_at']
    assert float(h.sql(f"SELECT provisiond.recovery_rate_present('USD','{boundary}',clock_timestamp())"))==0
    h.sql("UPDATE provisiond.rates SET available=true WHERE currency='USD'")
    row=json.loads(h.sql("SELECT to_jsonb(r) FROM provisiond.rate_observations r WHERE currency='USD' ORDER BY acceptance_order DESC LIMIT 1"))
    assert row['window_rate_num']==w.rate['rate_num']
    assert h.sql("SELECT max(accepted_at)=(SELECT max(accepted_at) FROM provisiond.fixture_rate_history WHERE currency='USD') FROM provisiond.rate_observations WHERE currency='USD'")=='t'
    w.h.event('w7_causal_trace',scenario=w.case,fixture_current_and_history_share_acceptance=True,latest=row)

def attachment_order(w):
    # STO-56: actual supported operator attachments on both sides of close.
    # The historical fence below only supplies the close disposition; no
    # production exhaustion enqueuer or no-rate cancellation is invented.
    h=w.h
    def lost(suffix):
        tenant,_,op=admit(w,suffix)
        w.provider_control(evidence=dict(operation=op,search='incomplete'))
        hold=h.root/'provider/hold-reconciliation';hold.touch(exist_ok=True)
        w.provider_control(mode='drop_reply');w.start_engine();w.wait_status(op,'needs_reconciliation');w.stop_engine()
        assert len([e for e in w.external()['events'] if e.get('command')==op and e['stage']=='accepted'])==1
        w.provider_control(mode='normal')
        return tenant,op,hold
    first,before_op,hold=lost('attach-before-close')
    second,after_op,hold=lost('attach-after-close')
    h.sql("UPDATE provisiond.rates SET available=false WHERE currency='USD'")
    id=new_incident(w)
    hold.unlink()
    eventually(lambda:h.sql(f"SELECT grace_ends_at<=clock_timestamp() FROM provisiond.recovery_incidents WHERE id='{id}'")=='t')
    eventually(lambda:h.sql(f"SELECT count(*) FROM provisiond.recovery_account_passes WHERE incident_id='{id}' AND second_completed_at IS NOT NULL")=='2')
    w.stop_engine()
    effects=w.external()['events']
    def attach(op,key):
        external=next(e['resource'] for e in effects if e.get('command')==op and e['stage']=='resource_created')
        code,reply,_=http(h.ports[7],'POST',f'/v1/operations/{op}/actions/resolve',dict(resolution='observed',operator_ref='local-attachment-order',external_id=external),w.operator_tokens[0],key)
        assert code==200,(code,reply)
        return h.sql(f"SELECT machine_id FROM provisiond.operations WHERE id='{op}'")
    machine=attach(before_op,'attach-before-close-key')
    assert h.sql(f"SELECT state<>'deleted' AND destroy_committed IS NULL FROM provisiond.machines WHERE id='{machine}'")=='t'
    assert operator(w,'close',id,ok=False).returncode!=0,'STO-56 pre-close attachment must affect current currency disposition'
    episode=h.sql(f"SELECT id FROM provisiond.episodes WHERE machine_id='{machine}' AND state<>'closed'")
    # Original commitment can be enough to attach funded, in which case seed
    # only the episode/fence needed to discharge this currency at the close.
    if not episode:
        episode=h.sql(f"INSERT INTO provisiond.episodes(machine_id,key,reasons,state) VALUES('{machine}','delete','[\"exhausted\"]','stalled') RETURNING id").splitlines()[0]
    h.sql(f"UPDATE provisiond.machines SET destroy_committed='{episode}' WHERE id='{machine}'")
    operator(w,'close',id);assert record(w) is None
    later=attach(after_op,'attach-after-close-key')
    assert record(w) is None and h.sql(f"SELECT destroy_committed IS NULL FROM provisiond.machines WHERE id='{later}'")=='t'
    h.event('w7_causal_trace',scenario=w.case,incident=id,preclose_attachment=machine,postclose_attachment=later,
        actual_operator_attachments=True,preclose_attachment_refused_close=True,postclose_attachment_did_not_reopen=True,
        seeded_fence=True,provider=effects,no_rate_cancellation_arm='not run; D-46')

def expired_replay(w):
    """STO-54(3)/29/30/31/32: retained settled history is not an active watch."""
    h=w.h;tenant=w.enrol();w.fund(tenant,'expired-replay-baseline')
    expired=w.request('POST','/v1/deposits',{'amount_sats':31},tenant,'expired-history')
    live=w.request('POST','/v1/deposits',{'amount_sats':46},tenant,'live-history')
    w.stop_api()
    (w.control/'hold-funding').touch()
    # Historical expiry metadata only; payment/credit outcomes are never seeded.
    h.sql(f"UPDATE provisiond.deposits SET expires_at=clock_timestamp()+interval '2 seconds' WHERE id='{expired['id']}'")
    instant=time.strftime('%Y-%m-%dT%H:%M:%SZ',time.gmtime())
    observations=[]
    for deposit,amount in ((expired,31),(live,46)):
        payment_hash=h.sql(f"SELECT payment_hash FROM provisiond.deposits WHERE id='{deposit['id']}'")
        observations.append(dict(rail='lightning',destination=deposit['lightning']['invoice'],payment_hash=payment_hash,
            amount_sats=amount,settled=True,preimage_known=True,settled_at=instant))
    observations.append(dict(rail='onchain',destination=expired['onchain']['address'],
        txid=hashlib.sha256(b'expired-history-onchain').hexdigest(),vout=0,confirmations=2,amount_sats=23,settled_at=instant))
    for value in observations:
        code,reply,_=w.settlement(value);assert code==200 and reply=={'ok':True}
        code,reply,_=w.settlement(dict(value,event_id='replayed-history'));assert code==200 and reply=={'ok':True}
    assert h.sql(f"SELECT '{instant}'::timestamptz<expires_at FROM provisiond.deposits WHERE id='{expired['id']}'")=='t'
    eventually(lambda:h.sql(f"SELECT expires_at<=clock_timestamp() FROM provisiond.deposits WHERE id='{expired['id']}'")=='t')
    assert h.sql(f"SELECT count(*) FROM provisiond.payments WHERE deposit_id IN ('{expired['id']}','{live['id']}')")=='0'
    id=new_incident(w,api=False);w.stop_engine()
    def credits():
        return json.loads(h.sql(f"SELECT jsonb_build_object('expired_payments',(SELECT count(*) FROM provisiond.payments WHERE deposit_id='{expired['id']}'),'live_payments',(SELECT count(*) FROM provisiond.payments WHERE deposit_id='{live['id']}'),'entries',(SELECT count(*) FROM provisiond.ledger_entries WHERE deposit_id IN ('{expired['id']}','{live['id']}')),'linked',(SELECT count(*) FROM provisiond.payments p JOIN provisiond.ledger_entries l ON l.id=p.ledger_entry_id WHERE p.deposit_id IN ('{expired['id']}','{live['id']}') AND l.amount_sats=p.amount_sats AND l.deposit_id=p.deposit_id),'balance',(SELECT balance_after FROM provisiond.ledger_entries WHERE tenant_id='{tenant['tenant_id']}' ORDER BY seq DESC LIMIT 1))::text"))
    expected=dict(expired_payments=2,live_payments=1,entries=3,linked=3,balance=215)
    for attempt in range(2):
        gate=h.proxy.arm('SET steps_completed=3','send')
        p=w.start('api',dict(**w.operator_env,PROVISIOND_API_LISTEN=f'127.0.0.1:{h.ports[4]}',
            PROVISIOND_RAIL_SIMULATOR=f'127.0.0.1:{h.ports[6]}',PROVISIOND_LOCAL_PARAMETERS=str(w.parameters),PROVISIOND_TEST_CONTROL=str(w.control)))
        w.api=p
        try:
            connection=gate.wait()
            assert record(w)['steps_completed']==2 and record(w)['grace_ends_at'] is None
            with socket.socket() as probe:assert probe.connect_ex(('127.0.0.1',h.ports[4]))!=0
            derived=next(e for e in trace_events(p.log) if e.get('event')=='recovery_watch_set_derived')
            assert expired['id'] not in derived['deposits'] and live['id'] in derived['deposits']
            observed=credits()
            h.event('w7_expired_replay_observed',incident=id,attempt=attempt,step=record(w)['steps_completed'],
                listener_down=True,watch_deposits=derived['deposits'],expired=expired['id'],live=live['id'],credits=observed,
                ordinary_watcher_started=any(e.get('event')=='funding_held' for e in trace_events(p.log)))
            assert observed==expected,'STO-54 expired settled history must credit both rails before step-3 mark and listener'
            assert not any(e.get('event') in ('api_listening','funding_held') for e in trace_events(p.log))
            if attempt==0:
                # Interrupt the unmarked phase after acknowledged payment commits.
                p.terminate();p.wait(timeout=5);w.reserve_api_ports()
                h.proxy.close_connection(connection,'expired-replay-unmarked-successor')
        finally:
            gate.release()
        if attempt==0:
            eventually(lambda:h.sql('SELECT count(*) FROM pg_stat_activity WHERE pid='+str(connection['pid']))=='0')
        else:
            eventually(lambda:any(e.get('event')=='api_listening' for e in trace_events(p.log)))
    assert record(w)['steps_completed']==3 and credits()==expected
    eventually(lambda:any(e.get('event')=='funding_held' for e in trace_events(w.api.log)))
    reissue(w,tenant,'expired-replay-replacement')
    assert w.request('GET','/v1/balance',tenant=tenant)['balance_sats']==215
    saved=record(w);w.restart_api()
    assert record(w)==saved and credits()==expected
    assert w.request('GET','/v1/balance',tenant=tenant)['balance_sats']==215
    independent=http(h.ports[6],'GET','/inspect')[1]['events']
    assert sum(e.get('stage')=='rail_observation' and e.get('value',{}).get('destination') in
        (expired['lightning']['invoice'],expired['onchain']['address'],live['lightning']['invoice']) for e in independent)==6
    h.event('w7_causal_trace',scenario=w.case,incident=id,settled_before_expiry=True,expired_before_recovery=True,
        before_mark_credits=expected,unmarked_successor_replayed_once=True,marked_successor_unchanged=True,
        active_watch_excludes_expired=True,ordinary_watcher_held=True,rails=['lightning','onchain'])

def inventory_complete(manifest):
    inventory=[e for e in manifest.get('events',[]) if e.get('event')=='w7_inventory_complete']
    children=[e for e in manifest.get('events',[]) if e.get('event')=='w7_child_result']
    def activated(child,name):
        events=child.get('events',[])
        return (child.get('result')=='pass' and child.get('seed')==manifest.get('seed')
            and any(e.get('event')=='cleanup_complete' and e.get('result')=='pass' for e in events)
            and any(e.get('event')=='w7_causal_trace' and e.get('scenario')==name for e in events))
    return (manifest.get('result')=='pass' and len(inventory)==1 and inventory[0]['required']==CASES
        and inventory[0]['observed']==CASES
        and [r['test'] for r in manifest.get('scenarios',[]) if r['test'] in CASES]==CASES
        and all(r['result']=='pass' for r in manifest.get('scenarios',[]) if r['test'] in CASES)
        and any(e.get('event')=='cleanup_complete' and e.get('result')=='pass' for e in manifest.get('events',[]))
        and [e.get('scenario') for e in children]==CASES
        and all(e.get('exit_status')==0 and activated(e.get('manifest',{}),e['scenario']) for e in children))

def w7(h,case=None,defect=None):
    if case is None:
        # Independent physical histories and process inventories per selector.
        # Each foreground child owns its assertions and complete cleanup.
        for port in list(h.port_reservations):h.release_port(port)
        h.release_slot();h.release_slot=lambda:None
        from runner import children
        jobs=[]
        for name in CASES:
            path=h.root/(name+'.json')
            command=['python3','tests/postgres/harness.py','--phase','w7','--case',name,'--seed',str(h.seed),'--evidence',str(path)]
            jobs.append((command,path,h.env))
        failed=False
        for name,result in zip(CASES,children(jobs),strict=True):
            path=h.root/(name+'.json')
            manifest=json.loads(path.read_text()) if path.exists() else {}
            h.event('w7_child_result',scenario=name,exit_status=result['exit_status'],wall_seconds=result['wall_seconds'],path=str(path),manifest=manifest)
            passed=result['exit_status']==0 and manifest.get('result')=='pass'
            failed=failed or not passed
            h.results.append(dict(test=name,result='pass' if passed else 'fail'))
        assert not failed,'W7 child scenario or cleanup failed'
        h.event('w7_inventory_complete',required=CASES,observed=[r['test'] for r in h.results])
        return
    w=Workflow(h,defect=defect,operator=True,default_build=case=='w7_default_hooks')
    try:
        for name in ([case] if case else CASES):
            w.case=name
            {'w7_ordinary':ordinary,'w7_rebuild':rebuild,'w7_lossless':lossless,'w7_continuation':continuation,'w7_supersession':supersession,'w7_lossy':lossy,
             'w7_markers':markers,'w7_grace':grace,'w7_paused':paused,'w7_close':close_case,'w7_reporting':reporting,'w7_default_hooks':default_hooks,'w7_interruptions':interruptions,'w7_credentials':credentials,'w7_legacy_upgrade':legacy_upgrade,'w7_parent_retirement':parent_retirement,'w7_caller_grace':caller_grace,'w7_coherent_rates':coherent_rates,'w7_attachment_order':attachment_order,'w7_expired_replay':expired_replay,'w7_reporting_progress':reporting_progress}[name](w)
            h.results.append(dict(test=name,result='pass'))
        expected=[case] if case else CASES
        h.event('w7_inventory_complete',required=expected,observed=[r['test'] for r in h.results if r['test'] in CASES])
    finally:w.close()
