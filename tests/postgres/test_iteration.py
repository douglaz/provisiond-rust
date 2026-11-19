"""Iteration selection and command failures retain the complete requested inventory."""
from contextlib import contextmanager
import io
import json
import os
from pathlib import Path
import shutil
import subprocess
import sys
import tempfile
import unittest
from unittest.mock import patch

with patch.dict(os.environ, PROVISIOND_HARNESS_ROOT=str(Path.cwd() / 'target' / 'iteration-test'),
                PROVISIOND_HARNESS_UNIT='iteration-test.service'):
    import harness
import runner


class IterationSelector(unittest.TestCase):
    def run_selector(self, selector, exits, complete=True):
        with tempfile.TemporaryDirectory() as directory:
            root=Path(directory)
            evidence=root/'parent.json'
            invocations=[]
            @contextmanager
            def invocation(path,jobs,candidates=False):
                invocations.append((path,jobs,candidates))
                yield
            scheduled=[]
            def children(jobs):
                scheduled.extend(jobs)
                records=[]
                for command,path,env in jobs:
                    phase=command[command.index('--phase')+1]
                    path.write_text(json.dumps(dict(result='pass')))
                    records.append(dict(command=command,path=str(path),exit_status=exits.get(phase,0),wall_seconds=1))
                return records
            with patch.object(harness,'ROOT',root),patch.object(harness,'cap_check'),patch.object(
                    runner,'invocation',invocation),patch.object(runner,'children',children),patch(
                    'w7.inventory_complete',return_value=complete),patch('w6.inventory_complete',return_value=True),patch(
                    'w5.inventory_complete',return_value=True),patch.dict(os.environ,PROVISIOND_HARNESS_JOBS='3'),patch.object(
                    sys,'argv',['harness.py','--phase',selector,'--jobs','3','--seed','20261113','--evidence',str(evidence)]),patch(
                    'sys.stdout',new_callable=io.StringIO),patch('sys.stderr',new_callable=io.StringIO):
                try:status=harness.main()
                except SystemExit as error:status=error.code
            expected_status=1 if any(exits.values()) or not complete else 0
            self.assertEqual(status,expected_status,'selector must return the aggregate phase verdict')
            self.assertTrue(evidence.is_file(),'selector must write one parent manifest')
            manifest=json.loads(evidence.read_text())
            phases=runner.PHASES if selector=='all' else ['w7','controls']
            self.assertEqual([command[command.index('--phase')+1] for command,_,_ in scheduled],phases)
            self.assertEqual(invocations,[(root,3,True)])
            self.assertEqual(manifest['jobs'],3)
            self.assertEqual(manifest['result'],'pass' if expected_status==0 else 'fail')
            self.assertEqual([record['phase'] for record in manifest['phases']],phases)
            self.assertEqual([record['exit_status'] for record in manifest['phases']],[exits.get(p,0) for p in phases])
            for command,path,env in scheduled:
                self.assertEqual(command[-4:],['--seed','20261113','--evidence',str(path)])
                self.assertEqual(env['PROVISIOND_HARNESS_JOBS'],'3')
            return manifest

    def test_iteration_schedules_exactly_w7_and_controls(self):
        self.run_selector('iteration',{})

    def test_iteration_fails_if_either_phase_fails(self):
        for exits in ({'w7':7},{'controls':9},{'w7':7,'controls':9}):
            with self.subTest(exits=exits):self.run_selector('iteration',exits)

    def test_iteration_rejects_incomplete_w7_inventory_despite_zero_exit(self):
        manifest=self.run_selector('iteration',{},complete=False)
        self.assertEqual(manifest['phases'][0]['result'],'fail')

    def test_all_retains_all_twelve_phases(self):
        self.assertEqual(len(runner.PHASES),12)
        self.run_selector('all',{})


class IterationScript(unittest.TestCase):
    def test_runs_every_command_in_order_and_reports_true_exits(self):
        script=Path('tools/iteration.sh')
        self.assertTrue(script.is_file(),'per-iteration command must exist')
        for exits in ((0,0,0),(7,0,0),(0,9,0),(0,0,11),(7,9,11)):
            with self.subTest(exits=exits),tempfile.TemporaryDirectory() as directory:
                root=Path(directory)
                (root/'tools').mkdir();(root/'bin').mkdir()
                shutil.copy2(script,root/'tools/iteration.sh')
                (root/'tools/check.sh').write_text('echo check >> "$ITERATION_CALLS"\nexit "$CHECK_EXIT"\n')
                python=root/'bin/python3'
                python.write_text('''#!/usr/bin/env bash
if [[ "$1" == -m ]]; then
    echo units >> "$ITERATION_CALLS"
    printf '%s\\n' "$@" > "$ITERATION_UNITS_ARGS"
    exit "$UNITS_EXIT"
fi
echo harness >> "$ITERATION_CALLS"
printf '%s\\n' "$@" > "$ITERATION_HARNESS_ARGS"
exit "$HARNESS_EXIT"
''')
                python.chmod(0o755)
                calls=root/'calls';units=root/'units';args=root/'args'
                result=subprocess.run(['bash',str(root/'tools/iteration.sh'),'--seed','20261113','--evidence','raw path.json'],
                    cwd=root/'bin',env=dict(os.environ,PATH=str(root/'bin')+os.pathsep+os.environ['PATH'],
                        ITERATION_CALLS=str(calls),ITERATION_UNITS_ARGS=str(units),ITERATION_HARNESS_ARGS=str(args),
                        CHECK_EXIT=str(exits[0]),UNITS_EXIT=str(exits[1]),HARNESS_EXIT=str(exits[2])),capture_output=True,text=True)
                self.assertEqual(result.returncode,next((code for code in exits if code),0),result.stderr)
                self.assertEqual(calls.read_text().splitlines(),['check','units','harness'])
                self.assertEqual(units.read_text().splitlines(),['-m','unittest','discover','-s','tests/postgres','-p','test_*.py'])
                self.assertEqual(args.read_text().splitlines(),['tests/postgres/harness.py','--phase','iteration',
                    '--seed','20261113','--evidence','raw path.json'])
                reports=[line for line in result.stdout.splitlines() if 'exit_status=' in line]
                self.assertEqual(len(reports),3,result.stdout)
                for report,code in zip(reports,exits,strict=True):
                    self.assertIn(f'exit_status={code} ',report)
                    self.assertRegex(report,r'wall_seconds=\d+$')


if __name__=='__main__':unittest.main()
