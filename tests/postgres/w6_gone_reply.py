"""PRV-36 strongest-source regression; works with the unchanged pre-delete runtime."""
import json
from core import environment
from harness import Harness
from workflow import Workflow

def probe(w):
    h=w.h
    resource=dict(external_id='TEST-GONE-REPLY',account='sim-a',channel='orders',correlator='opaque-test',name='test',region=None,state='running',public_ips=[],present=True)
    w.provider_control(fixture_resource=resource,mode='delete_gone_stale')
    h.run(['cargo','build','--example','provider-probe','--features','store-test-hooks','--locked'],timeout=120)
    h.env.update(environment(h))
    result=h.run([str(h.target/'debug/examples/provider-probe'),'sim-a',resource['external_id'],'--report'],ok=False,timeout=15)
    proof=w.external()
    value=next(json.loads(line)['value'] for line in result.stdout.splitlines() if line.startswith('{') and json.loads(line).get('event')=='stop_cost_result')
    deleted=[e for e in proof['events'] if e['stage']=='resource_deleted' and e['resource']==resource['external_id']]
    assert deleted,'gone reply fault must activate real provider deletion'
    h.event('w6_gone_reply_activated',provider=proof,adapter=value,child_exit=result.returncode)
    assert value['confirmed'] is True and value['error'] is None,'PRV-36 gone reply must override stale resource read'
    h.results.append(dict(test='W6-PRV36-gone-reply',result='pass'))

def run(h):
    w=Workflow(h)
    try:probe(w)
    finally:w.close()

if __name__=='__main__':
    import sys,traceback
    from pathlib import Path
    h=Harness('workflow',20261112);status='fail'
    try:run(h);status='pass'
    except BaseException as error:
        h.event('failure',type=type(error).__name__,message=str(error));traceback.print_exc()
    finally:
        if not h.cleanup():status='fail'
        Path(sys.argv[1]).write_text(json.dumps(h.manifest(status),indent=2)+'\n')
    sys.exit(0 if status=='pass' else 1)
