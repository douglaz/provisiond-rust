"""Trace truncation is distinct from malformed completed evidence."""
import json
import os
from pathlib import Path
import unittest
from unittest.mock import Mock, patch

with patch.dict(os.environ, PROVISIOND_HARNESS_ROOT=str(Path.cwd()/'target'/'trace-test'),
                PROVISIOND_HARNESS_UNIT='trace-test.service'):
    from workflow import trace_events


class TraceRecords(unittest.TestCase):
    def test_exit_tail_is_not_completed_evidence(self):
        path=Mock()
        path.read_text.return_value='{"event":"engine_superseded"}\n{"event":'
        self.assertEqual(trace_events(path),[dict(event='engine_superseded')])
        path.read_text.return_value='{"event":"unfinished"}'
        self.assertEqual(trace_events(path),[])

    def test_malformed_complete_record_fails(self):
        path=Mock()
        path.read_text.return_value='{"event":\n'
        with self.assertRaises(json.JSONDecodeError):trace_events(path)


if __name__=='__main__':unittest.main()
