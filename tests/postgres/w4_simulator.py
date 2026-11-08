"""W4 tooling regression: distinct OPS-27/PRV-26 effects over real HTTP."""
import concurrent.futures
import hashlib
from http.client import HTTPException
import json
from pathlib import Path
import subprocess
import uuid

from harness import eventually


def concurrent_orders(h, source=Path('tests/postgres/simulator.py')):
    # A separate invocation-owned provider leaves the application workflow's
    # resource counts intact. No PG writes or application provider retries.
    from workflow import http
    root=h.root/'concurrent-provider';root.mkdir()
    port=h.ports[7];h.release_port(port)
    children=[]
    bodies=[dict(operation=str(uuid.uuid4()),account=account,channel='orders',
                 request=dict(hostname='test-concurrent-'+str(i)))
            for i,account in enumerate(['sim-a','sim-b'])]
    for body in bodies:body['correlator']=body['operation']
    def start(barrier):
        command=['python3',str(source),'--port',str(port),'--root',str(root),'--kind','provider']
        if barrier:command.append('--concurrent-order-barrier')
        out=h.root/f'concurrent-provider-{len(children)}.stdout'
        err=h.root/f'concurrent-provider-{len(children)}.stderr'
        with out.open('w') as stdout,err.open('w') as stderr:
            child=subprocess.Popen(command,env=h.env,stdout=stdout,stderr=stderr,text=True)
        children.append(child)
        h.event('concurrent_simulator_started',command=command,pid=child.pid,
                source_sha256=hashlib.sha256(source.read_bytes()).hexdigest(),barrier=barrier)
        eventually(lambda:out.read_text())
        assert child.poll() is None
        return child
    def stop(child):
        if child.poll() is None:child.terminate()
        status=child.wait(timeout=5)
        h.event('concurrent_simulator_reaped',pid=child.pid,exit_status=status)
    def call(path,body=None):
        status,value,_=http(port,'POST',path,body)
        assert status==200
        return value
    def reads(snapshot, label, replies):
        observations=[]
        for body,reply in zip(bodies,replies):
            found=call('/search',body)
            direct=call('/get',dict(body,external_id=reply['external_id']))
            observations.append(dict(command=body['operation'],account=body['account'],
                correlator=body['correlator'],channel=body['channel'],
                returned_id=reply['external_id'],search=found,direct=direct))
        h.event('concurrent_simulator_reads',phase=label,resources=snapshot['resources'],reads=observations)
        return observations
    try:
        child=start(True)
        with concurrent.futures.ThreadPoolExecutor(max_workers=2) as pool:
            replies=list(pool.map(lambda body:call('/order',body),bodies))
        assert all(reply['accepted'] is True for reply in replies)
        def completed():
            snapshot=call('/inspect')
            return snapshot if sum(e['stage']=='response_returned' for e in snapshot['events'])==2 else None
        snapshot=eventually(completed)
        events=snapshot['events']
        assert [e['sequence'] for e in events]==list(range(1,len(events)+1))
        acceptance=next(e for e in events if e['stage']=='acceptance_overlap')
        allocation=next(e for e in events if e['stage']=='allocation_overlap')
        assert acceptance['parties']==allocation['parties']==2
        assert sum(e['stage']=='accepted' and e['sequence']<acceptance['sequence'] for e in events)==2
        assert acceptance['sequence']<allocation['sequence']
        assert all(e['sequence']>allocation['sequence'] for e in events if e['stage']=='resource_created')
        for body in bodies:
            command=[e for e in events if e.get('command')==body['operation']]
            assert [e['stage'] for e in command]==['request_received','accepted','resource_created','response_returned']
            assert all(e['account']==body['account'] and e['correlator']==body['correlator'] and e['path']=='/order' for e in command)
            created=command[2]
            assert all(created['value'][key]==body[key] for key in ['account','correlator','channel'])
            assert created['value']['name']==body['request']['hostname']
            assert created['resource']==created['value']['external_id']==command[3]['resource']
        h.event('concurrent_allocation_overlap_observed',acceptance=acceptance,allocation=allocation,
                commands=[b['operation'] for b in bodies],replies=replies,independent_events=events)
        live=reads(snapshot,'live',replies)
        stop(child)
        durable_before=(root/'effects.jsonl').read_bytes()
        child=start(False)
        replay=call('/inspect')
        assert replay['events']==[json.loads(line) for line in durable_before.splitlines()]
        assert replay['resources']==snapshot['resources']
        retained=reads(replay,'replayed',replies)
        h.event('concurrent_resources_replayed',same_store=True,live_count=len(snapshot['resources']),
                replayed_count=len(replay['resources']),replies=replies)
        # Keep this assertion after replay so the original-code red also records
        # the lost accepted resource in both live state and durable replay.
        assert len({r['external_id'] for r in replies})==len(snapshot['resources'])==len(replay['resources'])==2, \
            'distinct accepted simulator orders must retain two distinct durable resources'
        for observations in [live,retained]:
            for observation in observations:
                direct=observation['direct'];found=observation['search']
                assert found['authoritative'] and found['resources']==[direct]
                assert all(direct[key]==observation[key] for key in ['account','correlator','channel'])
                assert direct['external_id']==observation['returned_id']
        h.results.append(dict(test='W4-concurrent-simulator-resources',result='pass',
                              limits='tooling regression; no automatic application reconciliation'))
    finally:
        for child in children:
            if child.poll() is None:stop(child)
        events=root/'effects.jsonl'
        h.event('concurrent_simulator_evidence',source_sha256=hashlib.sha256(source.read_bytes()).hexdigest(),
                sha256=hashlib.sha256(events.read_bytes()).hexdigest() if events.exists() else None,
                records=[json.loads(line) for line in events.read_text().splitlines()] if events.exists() else [])
        h.reserve_port(port)
        h.event('concurrent_simulator_cleanup',result='pass',children_reaped=len(children),port_reserved=port)


def search_during_create(h, source=Path('tests/postgres/simulator.py')):
    """PRV-26/OPS-27 tooling: keep A's search iterator active while B creates."""
    from workflow import http
    root=h.root/'search-provider';root.mkdir()
    hold=root/'hold-search-iteration'
    port=h.ports[7];h.release_port(port)
    children=[]
    bodies=[dict(operation=str(uuid.uuid4()),account=account,channel='orders',
                 request=dict(hostname='test-search-'+str(i)))
            for i,account in enumerate(['sim-a','sim-b'])]
    for body in bodies:body['correlator']=body['operation']
    def start(barrier):
        command=['python3',str(source),'--port',str(port),'--root',str(root),'--kind','provider']
        if barrier:command.append('--search-read-barrier')
        out=h.root/f'search-provider-{len(children)}.stdout'
        err=h.root/f'search-provider-{len(children)}.stderr'
        with out.open('w') as stdout,err.open('w') as stderr:
            child=subprocess.Popen(command,env=h.env,stdout=stdout,stderr=stderr,text=True)
        children.append(child)
        h.event('search_simulator_started',command=command,pid=child.pid,
                source_sha256=hashlib.sha256(source.read_bytes()).hexdigest(),barrier=barrier)
        eventually(lambda:out.read_text())
        assert child.poll() is None
        return child
    def stop(child):
        if child.poll() is None:child.terminate()
        status=child.wait(timeout=5)
        h.event('search_simulator_reaped',pid=child.pid,exit_status=status)
    def call(path,body=None):
        status,value,_=http(port,'POST',path,body)
        assert status==200
        return value
    def inspect_stage(stage,command):
        return next((e for e in call('/inspect')['events']
                     if e['stage']==stage and e.get('command')==command),None)
    def reads(snapshot,label,replies):
        observations=[]
        assert len({r['external_id'] for r in replies})==len(snapshot['resources'])==2
        for body,reply in zip(bodies,replies):
            direct=call('/get',dict(body,external_id=reply['external_id']))
            found=call('/search',body)
            assert found==dict(authoritative=True,resources=[direct])
            assert direct==snapshot['resources'][reply['external_id']]
            assert all(direct[key]==body[key] for key in ['account','channel','correlator'])
            assert direct['name']==body['request']['hostname']
            for key in ['account','channel','correlator']:
                assert call('/search',dict(body,**{key:'unmatched-test-value'}))==dict(authoritative=True,resources=[])
            observations.append(dict(command=body['operation'],search=found,direct=direct))
        h.event('search_simulator_reads',phase=label,resources=snapshot['resources'],reads=observations)
    try:
        child=start(True)
        replies=[call('/order',bodies[0])]
        assert replies[0]['accepted'] is True
        eventually(lambda:inspect_stage('response_returned',bodies[0]['operation']))
        hold.touch()
        # Both HTTP calls use the existing finite socket timeout. The hold is
        # released before joining even if setup or the concurrent order fails.
        with concurrent.futures.ThreadPoolExecutor(max_workers=1) as pool:
            search=pool.submit(call,'/search',bodies[0])
            try:
                held=eventually(lambda:inspect_stage('search_iterator_held',bodies[0]['operation']),seconds=5)
                assert held['resource']==replies[0]['external_id']
                replies.append(call('/order',bodies[1]))
                assert replies[1]['accepted'] is True
                returned=eventually(lambda:inspect_stage('response_returned',bodies[1]['operation']),seconds=5)
                assert not search.done(), 'search iterator must remain held until concurrent creation completes'
                h.event('search_create_overlap_observed',held=held,writer_returned=returned,replies=replies)
            finally:
                hold.unlink(missing_ok=True)
            try:
                found=search.result(timeout=10)
                outcome=dict(result='pass',response=found)
            except (OSError,HTTPException) as error:
                outcome=dict(result='fail',error=type(error).__name__,message=str(error))
        resumed=eventually(lambda:inspect_stage('search_iterator_resumed',bodies[0]['operation']),seconds=5)
        snapshot=call('/inspect')
        assert held['sequence']<returned['sequence']<resumed['sequence']
        assert [e['sequence'] for e in snapshot['events']]==list(range(1,len(snapshot['events'])+1))
        for body,reply in zip(bodies,replies):
            stages=[e for e in snapshot['events'] if e.get('command')==body['operation'] and e.get('path')=='/order']
            assert [e['stage'] for e in stages]==['request_received','accepted','resource_created','response_returned']
            assert stages[2]['resource']==stages[3]['resource']==reply['external_id']
            assert all(e['account']==body['account'] and e['correlator']==body['correlator'] for e in stages)
        reads(snapshot,'live',replies)
        # Preserve the deliberately exercised throttle mode as an error reply.
        call('/control',dict(mode='search_throttle'))
        throttle=call('/search',bodies[0])
        assert throttle['error']['kind']=='rate_limited' and throttle['error']['details']['accepted'] is False
        call('/control',dict(mode='normal'))
        stop(child)
        stderr=(h.root/'search-provider-0.stderr').read_text()
        h.event('search_http_outcome',**outcome,resumed=resumed,handler_stderr=stderr,throttle=throttle)
        durable=(root/'effects.jsonl').read_bytes()
        child=start(False)
        replay=call('/inspect')
        assert replay['events']==[json.loads(line) for line in durable.splitlines()]
        assert replay['resources']==snapshot['resources']
        reads(replay,'replayed',replies)
        stop(child)
        assert not (h.root/'search-provider-1.stderr').read_text()
        h.event('search_resources_replayed',live_count=len(snapshot['resources']),
                replayed_count=len(replay['resources']),same_store=True)
        # The original live-dictionary implementation reaches this assertion
        # after real overlap, RemoteDisconnected, matching RuntimeError and replay.
        assert outcome['result']=='pass', 'concurrent simulator search must complete its HTTP response'
        assert not stderr, 'concurrent simulator search must not raise a handler exception'
        assert outcome['response']==dict(authoritative=True,resources=[snapshot['resources'][replies[0]['external_id']]])
        h.results.append(dict(test='W4-search-during-simulator-create',result='pass',
                              limits='tooling regression; no automatic application reconciliation'))
    finally:
        hold.unlink(missing_ok=True)
        for child in children:
            if child.poll() is None:stop(child)
        events=root/'effects.jsonl'
        h.event('search_simulator_evidence',source_sha256=hashlib.sha256(source.read_bytes()).hexdigest(),
                sha256=hashlib.sha256(events.read_bytes()).hexdigest() if events.exists() else None,
                records=[json.loads(line) for line in events.read_text().splitlines()] if events.exists() else [])
        h.reserve_port(port)
        h.event('search_simulator_cleanup',result='pass',children_reaped=len(children),port_reserved=port)
