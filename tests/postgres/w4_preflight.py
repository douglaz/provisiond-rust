"""OPS-49/43/21/15: real preflight read failures on HTTP-admitted creates."""
import hashlib
import json
import time

from harness import eventually
from w4_regressions import funded


READS={'account':"'request',o.request,'summary',o.request_summary",
       'tenant':'SELECT COALESCE((SELECT to_jsonb(status)',
       'channel':'SELECT COALESCE((SELECT greatest(0,ceil(extract(epoch FROM closed_until'}


def snapshot(w,op):
    return json.loads(w.h.sql(f"""SELECT jsonb_build_object(
        'status',o.status,'claim',o.claim_number,'error',o.error,
        'marker',o.about_to_order_at,'machine',o.machine_id,
        'reserved',c.reserved_sats,'closed',c.closed_at,
        'debits',(SELECT count(*) FROM provisiond.ledger_entries WHERE operation_id=o.id))
        FROM provisiond.operations o JOIN provisiond.commitments c ON c.id=o.commitment_id
        WHERE o.id='{op}'"""))


def calls(w,op):
    return [e for e in w.external()['events'] if e.get('command')==op]


def preflight(w,only=None):
    from core import core_probe
    h=w.h;w.stop_engine()
    # No runtime hooks inject the fault: the independent PG proxy disconnects
    # only the selected SELECT's transport, leaving claims/records/API intact.
    for mode,site in [('recover',site) for site in READS]+[('exhaust','account'),('stop','tenant')]:
        if only is not None and mode!=only:continue
        key='preflight-'+mode+'-'+site
        t=funded(w,key)
        op=w.request('POST','/v1/machines',w.create_body(t),t,key,status=202)['id']
        gate=h.proxy.arm(READS[site],'send');probe=None
        start=len(h.events)
        def faults():return [e for e in h.events[start:] if e.get('fault')=='preflight_read_transport']
        try:
            if mode=='stop':
                h.run(['cargo','build','--example','core-probe','--features','store-test-hooks','--locked'],timeout=120)
                executable=h.target/'debug/examples/core-probe'
                h.event('workflow_binary_started',component='core-probe',case=key,**w.build_identity(executable,['store-test-hooks']))
                probe=core_probe(h);probe.command('engine_start');probe.command('dispatch_provider')
            else:w.start_engine()
            connection=gate.wait()
            before=snapshot(w,op)
            assert before['status']=='running' and before['claim']==1 and before['reserved']==43
            assert before['marker'] is None and before['debits']==0 and not calls(w,op)
            # Only this operation is queued. Its selected SELECT is now held on
            # this exact backend, before transmission, after earlier checks.
            h.event('preflight_fault_attributed',operation=op,claim_number=before['claim'],site=site,
                    case=mode,session=connection['pid'],before=before,provider_requests=0)
            h.proxy.fail_query=READS[site]
            activated=time.monotonic();gate.release()
            eventually(lambda:len(faults())>=2 or w.status(op)!='running',seconds=5)
            current=snapshot(w,op)
            h.event('preflight_retry_live_observed',operation=op,site=site,case=mode,
                    transport_failures=len(faults()),state=current,provider_requests=len(calls(w,op)))
            # Intended original-runtime red: a real first read failure must not
            # settle internal/release authority instead of repeating the read.
            assert current==before and len(faults())>=2 and not calls(w,op), 'OPS-49 preflight store failure must retain running claim and commitment while retrying'
            if mode=='recover':
                h.proxy.fail_query=None
                w.wait_status(op,'succeeded');w.stop_engine()
                steps=[e['step'] for e in w.events('engine','dispatch_step') if e.get('operation')==op]
                assert steps==list(range(1,10)),steps
                requests=[e['path'] for e in calls(w,op) if e['stage']=='request_received']
                assert requests==['/offers','/order','/search','/get','/constraints'],requests
                retries=[e for e in w.events('engine','preflight_read_retry') if e.get('operation')==op]
                assert len(retries)>=2 and all(e['site']==site and e['claim_number']==1 for e in retries)
                state=snapshot(w,op)
                assert state['claim']==1 and state['reserved']==12 and state['debits']==2 and state['machine']
                assert w.request('POST','/v1/machines',w.create_body(t),t,key,status=202)['id']==op
                assert state==snapshot(w,op)
                w.operation_reads(t,op)
                h.event('preflight_recovered',operation=op,site=site,state=state,steps=steps,provider_requests=requests,retries=len(retries))
            elif mode=='exhaust':
                p=w.engine
                # The runtime decides exhaustion. This deadline only observes it.
                eventually(lambda:p.poll() is not None,seconds=8)
                elapsed=time.monotonic()-activated
                diagnostic='preflight store retry exhausted; no worker classification'
                assert p.returncode==1 and diagnostic in p.log.read_text()
                exhausted=[e for e in w.events('engine','preflight_read_exhausted') if e.get('operation')==op]
                assert len(exhausted)==1 and exhausted[0]['spent_ms']>=exhausted[0]['bound_ms']==2000
                assert snapshot(w,op)==before and not calls(w,op)
                assert not any(e.get('operation')==op for e in w.events('engine','settlement_finished'))
                h.event('preflight_exhaustion_observed',operation=op,exit_status=p.returncode,
                        store_retry_ms=2000,elapsed_ms=round(elapsed*1000),transport_failures=len(faults()),
                        diagnostic=diagnostic,accounted_ms=exhausted[0]['spent_ms'],state=snapshot(w,op),provider_requests=0,
                        stdout_sha256=hashlib.sha256(p.log.read_bytes()).hexdigest(),stderr_sha256=hashlib.sha256(p.error.read_bytes()).hexdigest())
                h.proxy.fail_query=None;w.stop_engine();w.start_engine()
                w.wait_status(op,'failed');w.stop_engine()
                state=snapshot(w,op)
                assert state['error']['kind']=='internal' and state['error']['retryable'] is True
                assert 'restart' in state['error']['message'] and state['closed'] and state['reserved']==0
                assert state['debits']==0 and state['marker'] is None and state['machine'] is None and not calls(w,op)
                h.event('preflight_restart_classified',operation=op,state=state,provider_requests=0)
                w.operation_reads(t,op,'internal',True,{})
            else:
                probe.command('record_stop '+op);probe.command('wait_records')
                state=snapshot(w,op)
                assert state['status']=='failed' and state['error']['kind']=='conflict'
                assert state['error']['details']['reason']=='cut_off' and state['error']['retryable'] is True
                assert state['closed'] and state['reserved']==0 and state['debits']==0
                assert state['marker'] is None and state['machine'] is None and not calls(w,op)
                n=len(faults());time.sleep(.05)
                assert len(faults())==n and not calls(w,op)
                h.event('preflight_survived_stop_recorded',operation=op,state=state,provider_requests=0,
                        further_read_attempts=0,stop_record_ms=1000)
                w.operation_reads(t,op,'conflict',True,{'reason':'cut_off'})
            h.results.append(dict(test='W4-'+key,result='pass'))
        finally:
            h.proxy.fail_query=None;gate.release()
            if probe:probe.finish()
            w.stop_engine()
