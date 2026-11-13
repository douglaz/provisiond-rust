#!/usr/bin/env python3
"""Protected ac83e17 runtime/migration/dependency reds; fixtures only overlay."""
import argparse
import hashlib
import io
import json
import os
from pathlib import Path
import shutil
import subprocess
import tarfile
import tempfile
from harness import ROOT,cap_check,preflight_ports
from w6_blocking import RETIRE_ASSERTION,FIELD_ASSERTION
from w6_final_repairs import RATE_ASSERTIONS,REPLAY_ASSERTION
from w6_retired_targets import ASSERTIONS as TARGET_ASSERTIONS

BASE='ac83e172dae1d3c0cc7934c9633a403cfca0a679'
ASSERTIONS={'w6_e1':RETIRE_ASSERTION,
    **{case:'E2 PRV-41 '+site+' rejects credential durably' for case,site in
       [('w6_e2_offers','/offers'),('w6_e2_keys','/keys'),('w6_e2_order','/order'),
        ('w6_e2_key_cleanup','/keys/delete'),('w6_e2_delete','/delete')]},
    **{case:FIELD_ASSERTION for case in ['w6_empty','w6_operator','w6_late_attach','w6_meter_edges','w6_e3_reply']}}

def digest(path):return hashlib.sha256(path.read_bytes()).hexdigest()

def main():
    ap=argparse.ArgumentParser();ap.add_argument('--evidence',type=Path,required=True)
    final_assertions={**RATE_ASSERTIONS,'w6_f2':REPLAY_ASSERTION}
    ap.add_argument('--final-repairs',action='store_true')
    target_assertions={f'w6_g1_{kind}':message for kind,message in TARGET_ASSERTIONS.items()}
    ap.add_argument('--retained-targets',action='store_true')
    ap.add_argument('--case',choices=[*ASSERTIONS,*final_assertions,*target_assertions],action='append');args=ap.parse_args()
    assert not (args.final_repairs and args.retained_targets),'select one protected baseline'
    assertions=target_assertions if args.retained_targets else final_assertions if args.final_repairs else ASSERTIONS
    base='ab014865b46f1f0c15c5ead33a85f24fdbaa3c81' if args.retained_targets else '96c6001999a50fb4b3247b77ba29e60500baf72f' if args.final_repairs else BASE
    cases=args.case or list(assertions)
    assert all(case in assertions for case in cases),'case does not belong to selected baseline'
    cap_check();preflight_ports()
    repo=Path.cwd();ROOT.mkdir(parents=True,exist_ok=True)
    root=Path(tempfile.mkdtemp(prefix='blocking-baseline-',dir=ROOT));export=root/'source';export.mkdir()
    def git(*cmd):return subprocess.check_output(['git',*cmd])
    with tarfile.open(fileobj=io.BytesIO(git('archive',base))) as tree:tree.extractall(export,filter='data')
    subprocess.run(['git','init','--bare',str(export/'.git')],check=True,capture_output=True)
    subprocess.run(['git','--git-dir',str(export/'.git'),'fetch','--no-tags','--no-write-fetch-head',str(repo),base],check=True,capture_output=True)
    (export/'.git/HEAD').write_text(base+'\n')
    subprocess.run(['git','-C',str(export),'config','core.bare','false'],check=True)
    subprocess.run(['git','-C',str(export),'read-tree',base],check=True)
    assert not subprocess.check_output(['git','-C',str(export),'for-each-ref'])
    paths=git('ls-tree','-r','--name-only',base).decode().splitlines()
    protected={p:hashlib.sha256(git('show',base+':'+p)).hexdigest() for p in paths
        if p.startswith(('src/','migrations/')) or p in ('Cargo.toml','Cargo.lock','flake.nix','flake.lock')}
    def protect():
        assert {p:digest(export/p) for p in protected}==protected
        assert sorted(p.relative_to(export).as_posix() for d in ('src','migrations') for p in (export/d).rglob('*') if p.is_file())==sorted(p for p in protected if p.startswith(('src/','migrations/')))
    protect();overlays={}
    for path in sorted((repo/'tests/postgres').iterdir()):
        if path.is_file():
            relative=path.relative_to(repo);(export/relative).write_bytes(path.read_bytes());overlays[str(relative)]=digest(path)
    proof=dict(baseline=base,protected_before=protected,fixture_overlays=overlays,branch_or_tag_created=False)
    results=[]
    try:
        for case in cases:
            raw=root/(case+'.json');out=root/(case+'.stdout');err=root/(case+'.stderr')
            command=['python3','tests/postgres/harness.py','--phase','w6','--case',case,'--seed','20261112','--evidence',str(raw)]
            with out.open('w') as stdout,err.open('w') as stderr:
                child=subprocess.run(command,cwd=export,env=dict(os.environ,PROVISIOND_HARNESS_ROOT=str(root)),stdout=stdout,stderr=stderr)
            (root/(case+'.status')).write_text(str(child.returncode)+'\n');protect()
            manifest=json.loads(raw.read_text()) if raw.exists() else {}
            failures=[e for e in manifest.get('events',[]) if e['event']=='failure']
            cleanup=[e for e in manifest.get('events',[]) if e['event']=='cleanup_complete']
            activation=[e for e in manifest.get('events',[]) if e['event']==('w6_retired_target' if case in target_assertions else 'w6_opening_rate' if case in RATE_ASSERTIONS else 'w6_retired_operator_replay' if case=='w6_f2' else 'w6_retirement_bound' if case=='w6_e1' else 'w6_terminal_credential' if case.startswith('w6_e2_') else 'w6_deficiency_fields')]
            valid=child.returncode==1 and len(failures)==1 and failures[0].get('type')=='AssertionError' and failures[0].get('message')==assertions[case] and cleanup and cleanup[-1]['result']=='pass' and bool(activation)
            if case in target_assertions:
                a=activation[-1] if activation else {}
                valid=valid and a.get('before_status')==409 and a.get('after_status')==404 and a.get('physical_retirement') and a.get('receipt_scopes')=={'tenant':1} and a.get('before')==a.get('after') and a.get('provider_before')==a.get('provider_after') and a.get('retained',{}).get('id')==a.get('operation')
            elif case in RATE_ASSERTIONS:
                valid=valid and activation[-1]['computed_sats']==23 and activation[-1]['state']['deficiencies'] and activation[-1]['commit_reply_held']
            elif case=='w6_f2':
                valid=valid and activation[-1]['responses'][-1]['status']==404 and any(r['id']==activation[-1]['operation'] for r in activation[-1]['retained']['retired_operations'])
            elif case=='w6_e1':
                valid=valid and activation[-1]['due_query_sent'] and activation[-1]['due_query_errors'] and '57014' in activation[-1]['sqlstates'] and activation[-1]['housekeeping_failure'] and activation[-1]['state_rolled_back']
            elif case.startswith('w6_e2_'):
                valid=valid and (activation[-1]['credential_status']=='healthy' or sum(e['stage']=='authentication_rejected' for e in activation[-1]['provider'])>1) and any(e['stage']=='authentication_rejected' and e['command']==activation[-1]['operation'] and e['path']==activation[-1]['site'] for e in activation[-1]['provider'])
            else:valid=valid and any(d['cause']!='clamp_overflow' and d['clamped_sats'] is not None for d in activation[-1]['rows'])
            result=dict(case=case,result='expected negative demonstration' if valid else 'fail',exit_status=child.returncode,intended_assertion=assertions[case],command=command,raw_manifest=str(raw),stdout_sha256=digest(out),stderr_sha256=digest(err),activation=activation)
            results.append(result);print(json.dumps(dict(case=case,result=result['result'],exit_status=child.returncode,raw_manifest=str(raw))),flush=True)
            if not valid:break
    finally:
        protect();proof['protected_after']={p:digest(export/p) for p in protected}
        shutil.rmtree(export);proof['export_removed']=not export.exists()
    (root/'source-proof.json').write_text(json.dumps(proof,indent=2)+'\n')
    status=0 if len(results)==len(cases) and all(r['result']=='expected negative demonstration' for r in results) else 1
    args.evidence.write_text(json.dumps(dict(result='pass' if status==0 else 'fail',results=results,proof=proof),indent=2)+'\n')
    return status

if __name__=='__main__':raise SystemExit(main())
