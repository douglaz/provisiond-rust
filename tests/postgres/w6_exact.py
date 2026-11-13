"""OPS-29 baseline-compatible settle-time attribution regression."""
import json
from harness import eventually
from workflow import Workflow
from w4_regressions import funded

def probe(w):
    w.stop_engine();t=funded(w,'exact-attribution')
    hold=w.h.root/'provider/hold-reconciliation';hold.touch(exist_ok=True)
    w.provider_control(mode='search_wrong_exact')
    op=w.request('POST','/v1/machines',w.create_body(t),t,'exact-attribution-key',status=202)['id']
    w.start_engine();eventually(lambda:w.status(op) in ('succeeded','needs_reconciliation'))
    w.stop_engine()
    attached=int(w.h.sql(f"SELECT count(*) FROM provisiond.machines m JOIN provisiond.operations o ON o.machine_id=m.id WHERE o.id='{op}'"))
    provider=[e for e in w.external()['events'] if e.get('command')==op]
    assert any(e['stage']=='non_exact_candidate_returned' for e in provider),'exact attribution fault must activate'
    w.h.event('w6_worker_exact_activated',operation=op,attached=attached,status=w.status(op),provider=provider)
    assert attached==0,'OPS-29 settle-time non-exact candidate must not attach'
    assert w.status(op)=='needs_reconciliation'
    hold.unlink();w.provider_control(mode='normal')

if __name__=='__main__':
    import sys,traceback
    from pathlib import Path
    from harness import Harness
    h=Harness('workflow',20261112);status='fail';w=None
    try:w=Workflow(h);probe(w);status='pass'
    except BaseException as error:
        h.event('failure',type=type(error).__name__,message=str(error));traceback.print_exc()
    finally:
        if w:w.close()
        if not h.cleanup():status='fail'
        Path(sys.argv[1]).write_text(json.dumps(h.manifest(status),indent=2)+'\n')
    sys.exit(0 if status=='pass' else 1)
