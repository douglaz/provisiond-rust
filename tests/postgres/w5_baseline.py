#!/usr/bin/env python3
"""OPS-49 unchanged-runtime reds with the canonical, byte-identical fixtures.

Run under nix develop, with the same private allocation as harness.py. This
exports objects and a detached HEAD without creating a branch or changing the
working repository. Only tests/postgres files overlay the pinned base.
"""
import argparse
import hashlib
import io
import json
import os
from pathlib import Path
import subprocess
import tarfile
import tempfile
from harness import ROOT, cap_check, preflight_ports

BASE='aceea4eea8382c8d3b6fa554706c54920999369c'
ASSERTIONS={
    'w5_claim_precommit':'OPS-49 known pre-COMMIT claim failure must retry the whole transaction',
    'w5_backoff_budget':'OPS-49 backoff exhaustion must forbid another transaction',
    'w5_accounting_reply':'OPS-49 non-standby COMMIT reply delay must consume retry budget',
}


def digest(path):return hashlib.sha256(path.read_bytes()).hexdigest()


def main():
    ap=argparse.ArgumentParser();ap.add_argument('--evidence',type=Path,required=True)
    args=ap.parse_args();cap_check();preflight_ports()
    repo=Path.cwd();ROOT.mkdir(parents=True,exist_ok=True)
    root=Path(tempfile.mkdtemp(prefix='baseline-',dir=ROOT));export=root/'source';export.mkdir()
    def git(*cmd):return subprocess.check_output(['git',*cmd])
    archive=git('archive',BASE)
    with tarfile.open(fileobj=io.BytesIO(archive)) as tree:tree.extractall(export,filter='data')
    # An empty object database and detached identity: no branch or tag is made.
    subprocess.run(['git','init','--bare',str(export/'.git')],check=True,capture_output=True)
    subprocess.run(['git','--git-dir',str(export/'.git'),'fetch','--no-tags','--no-write-fetch-head',str(repo),BASE],check=True,capture_output=True)
    (export/'.git/HEAD').write_text(BASE+'\n')
    subprocess.run(['git','-C',str(export),'config','core.bare','false'],check=True)
    subprocess.run(['git','-C',str(export),'read-tree',BASE],check=True)
    assert not subprocess.check_output(['git','-C',str(export),'for-each-ref'])
    paths=git('ls-tree','-r','--name-only',BASE).decode().splitlines()
    protected={p:hashlib.sha256(git('show',BASE+':'+p)).hexdigest() for p in paths
               if p.startswith(('src/','migrations/')) or p in ('Cargo.toml','Cargo.lock','flake.nix','flake.lock')}
    def protect():
        assert {p:digest(export/p) for p in protected}==protected
        assert sorted(p.relative_to(export).as_posix() for directory in ('src','migrations') for p in (export/directory).rglob('*') if p.is_file())==sorted(p for p in protected if p.startswith(('src/','migrations/')))
    protect()
    overlays={}
    for path in sorted((repo/'tests/postgres').iterdir()):
        if path.is_file():
            relative=path.relative_to(repo);(export/relative).write_bytes(path.read_bytes());overlays[str(relative)]=digest(path)
    drivers={p:overlays[p] for p in ('tests/postgres/w5.py','tests/postgres/w5_accounting.py')}
    proof=dict(baseline=BASE,protected_before=protected,fixture_overlays=overlays,assertion_drivers=drivers,branch_or_tag_created=False)
    results=[]
    for case,assertion in ASSERTIONS.items():
        raw=root/(case+'.json');out=root/(case+'.stdout');err=root/(case+'.stderr')
        command=['python3','tests/postgres/harness.py','--phase','w5','--case',case,'--seed','20261112','--evidence',str(raw)]
        with out.open('w') as stdout,err.open('w') as stderr:
            child=subprocess.run(command,cwd=export,env=dict(os.environ,PROVISIOND_HARNESS_ROOT=str(root)),stdout=stdout,stderr=stderr)
        (root/(case+'.status')).write_text(str(child.returncode)+'\n')
        protect()
        manifest=json.loads(raw.read_text()) if raw.exists() else {}
        failures=[e for e in manifest.get('events',[]) if e['event']=='failure']
        cleanup=[e for e in manifest.get('events',[]) if e['event']=='cleanup_complete']
        activation={'w5_claim_precommit':'w5_claim_precommit_observed','w5_backoff_budget':'w5_backoff_budget_observed','w5_accounting_reply':'w5_accounting_activated'}[case]
        observed=[e for e in manifest.get('events',[]) if e['event']==activation]
        valid=child.returncode==1 and len(failures)==1 and failures[0].get('type')=='AssertionError' and failures[0].get('message')==assertion and cleanup and cleanup[-1]['result']=='pass' and observed
        if case=='w5_claim_precommit':valid=valid and observed[-1]['commit_dispatched'] is False and observed[-1]['engine_exit'] is not None
        if case=='w5_backoff_budget':valid=valid and observed[-1]['new_begin_after_backoff']>0 and observed[-1]['fatal_exhaustion'] is False
        if case=='w5_accounting_reply':valid=valid and observed[-1]['held_ms']>2000 and observed[-1]['retries'] and observed[-1]['engine_exit'] is None and observed[-1]['provider_orders']==1 and observed[-1]['durable_rows_unchanged'] is True
        result=dict(case=case,result='expected negative demonstration' if valid else 'fail',exit_status=child.returncode,intended_assertion=assertion,command=command,raw_manifest=str(raw),stdout_sha256=digest(out),stderr_sha256=digest(err),assertion_drivers=drivers)
        results.append(result)
        print(json.dumps(result),flush=True)
        if not valid:break
    proof['protected_after']={p:digest(export/p) for p in protected}
    (root/'source-proof.json').write_text(json.dumps(proof,indent=2)+'\n')
    status=0 if len(results)==len(ASSERTIONS) and all(r['result']=='expected negative demonstration' for r in results) else 1
    args.evidence.write_text(json.dumps(dict(result='pass' if status==0 else 'fail',results=results,source_proof=str(root/'source-proof.json'),proof=proof),indent=2)+'\n')
    return status


if __name__=='__main__':raise SystemExit(main())
