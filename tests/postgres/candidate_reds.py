"""STO-14/49/54 and OPS-41: preserve the five W7 candidate regression reds."""
import ast
import json
import os
from pathlib import Path
import subprocess
import sys

BASE='8339aae5ecf20e6337f4d5d5e22ec4f4573efbfb'
FIXTURE='06b7636cfed13cd4d6049dbe316ed8b191c93fa2'
ASSERTIONS={
    'w7_reporting':'STO-54 malformed reporting file must not hold establishment',
    'w7_parent_retirement':'STO-14 cancelled recovery parent must retire without stopping API',
    'w7_caller_grace':'OPS-41 restore grace must permit caller delete before step3',
    'w7_coherent_rates':'STO-49 supported rate update must stamp shared recovery history',
    'w7_expired_replay':'STO-54 expired settled history must credit both rails before step-3 mark and listener',
}


def main():
    # Historical runtimes predate the CLI output change. Use the immutable,
    # original CLI fixture for those reds; current-runtime contract assertions
    # run independently in every ordinary W7 child.
    import w7
    from harness import main as harness_main
    source=subprocess.check_output(['git','show',FIXTURE+':tests/postgres/w7.py'],text=True)
    function=next(node for node in ast.parse(source).body if isinstance(node,ast.FunctionDef) and node.name=='operator')
    module=ast.Module(body=[function],type_ignores=[])
    exec(compile(module,'historical-cli-fixture','exec'),w7.__dict__)
    os.environ['PROVISIOND_HARNESS_BUILD_VARIANT']='candidate'
    return harness_main()


def controls(root,seed):
    from runner import children
    jobs=[]
    for case in ASSERTIONS:
        path=root/('candidate-'+case+'.json')
        command=[sys.executable,'tests/postgres/candidate_reds.py','--phase','w7','--case',case,
                 '--seed',str(seed),'--evidence',str(path)]
        jobs.append((command,path,None))
    records=[]
    for (case,assertion),record in zip(ASSERTIONS.items(),children(jobs),strict=True):
        path=Path(record['path'])
        manifest=json.loads(path.read_text()) if path.exists() else {}
        events=manifest.get('events',[])
        failures=[e for e in events if e['event']=='failure']
        cleanup=[e for e in events if e['event']=='cleanup_complete']
        valid=(record['exit_status']==1 and len(failures)==1 and failures[0].get('type')=='AssertionError'
               and failures[0].get('message')==assertion and cleanup and cleanup[-1]['result']=='pass')
        records.append(dict(case=case,baseline=BASE,intended_assertion=assertion,**record,
                            result='expected negative demonstration' if valid else 'fail'))
    return records


if __name__=='__main__':raise SystemExit(main())
