"""OVR-18, STO-54/55/56: operator alarms are distinct from recovery traces."""
import json
from pathlib import Path
from subprocess import CompletedProcess
import tempfile
from types import SimpleNamespace
import unittest
from unittest.mock import patch

from harness import ROOT
from w7 import operator


ALARM=dict(event='alarm',component='recovery',message='recovery unavailable: incident mismatch',
           threshold_ms=0,operator_action_required=True)


class RecoveryOperatorContract(unittest.TestCase):
    def invoke(self,stderr,status=1,stdout='',trace=True):
        ROOT.mkdir(parents=True,exist_ok=True)
        with tempfile.TemporaryDirectory(prefix='operator-contract-',dir=ROOT) as directory:
            h=SimpleNamespace(root=Path(directory),target=Path(directory),env={},commands=[])
            original=h.env
            def run(command,**kwargs):
                if trace:
                    Path(h.env['PROVISIOND_RECOVERY_LOG']).write_text('{"event":"recovery_loss_report"}\n')
                return CompletedProcess(command,status,stdout,stderr)
            h.run=run
            h.event=lambda *args,**kwargs:None
            w=SimpleNamespace(h=h,defect=None)
            try:
                with patch('w7.environment',return_value={}):
                    return operator(w,'report',ok=status==0)
            finally:self.assertIs(h.env,original)

    def test_failed_call_accepts_recovery_alarm_and_plain_error(self):
        result=self.invoke(json.dumps(ALARM)+'\nprovisiond recovery: incident mismatch\n')
        self.assertEqual(result.returncode,1)

    def test_rejects_trace_and_invalid_alarm_shapes(self):
        alternatives=[
            dict(event='recovery_loss_report',incident='retained'),
            dict(ALARM,component='engine'),dict(ALARM,event='recovery_loss_report'),
            dict(ALARM,operator_action_required=False),dict(ALARM,operator_action_required='true'),
            dict(ALARM,threshold_ms='0'),dict(ALARM,threshold_ms=False),
            dict(ALARM,threshold_ms=2000),dict(ALARM,message=''),dict(ALARM,message={}),
            dict(ALARM,trace={'event':'recovery_loss_report'}),
            {key:value for key,value in ALARM.items() if key!='operator_action_required'},
            [ALARM],None,True,'trace',42,
        ]
        for value in alternatives:
            with self.subTest(value=value),self.assertRaisesRegex(AssertionError,'^STO-54 recovery trace or invalid operator alarm on stderr$'):
                self.invoke('  '+json.dumps(value)+'\n')

    def test_rejects_malformed_json(self):
        for line in ('{"event":"alarm"','  {broken}', '[{"event":"alarm"}'):
            with self.subTest(line=line),self.assertRaisesRegex(AssertionError,'^STO-54 malformed recovery JSON on stderr$'):
                self.invoke(line+'\n')

    def test_success_rejects_alarm_and_trace(self):
        for value in (ALARM,dict(event='recovery_loss_report'),[],None,'trace',42):
            with self.subTest(value=value),self.assertRaisesRegex(AssertionError,'^STO-54 recovery trace or invalid operator alarm on stderr$'):
                self.invoke(json.dumps(value)+'\n',status=0,stdout='null\n')

    def test_success_keeps_plain_notice_and_one_result(self):
        self.assertEqual(self.invoke('STO-54: reporting input absent\n',status=0,stdout='null\n').stdout,'null\n')

    def test_result_and_trace_file_assertions_remain(self):
        alternatives=[
            (dict(stdout='null\n'),'STO-54 failed recovery must not print a result'),
            (dict(status=0,stdout='null\nnull\n'),'STO-56 recovery stdout must contain one result row'),
            (dict(status=0,stdout='{"event":"recovery_loss_report"}\n'),'STO-56 recovery stdout contains trace instead of result'),
            (dict(trace=False),'STO-54 requested recovery log must receive trace events'),
        ]
        for kwargs,assertion in alternatives:
            with self.subTest(kwargs=kwargs),self.assertRaisesRegex(AssertionError,'^'+assertion+'$'):
                self.invoke('',**kwargs)


if __name__=='__main__':unittest.main()
