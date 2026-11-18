"""STO-54: a quiesced rebuild keeps its stopped listeners isolated."""
import os
from pathlib import Path
import socket
import tempfile
from types import SimpleNamespace
import unittest
from unittest.mock import Mock,patch
from runner import port_pool,worker_slot

with patch.dict(os.environ,PROVISIOND_HARNESS_ROOT=str(Path.cwd()/'target/recovery-port-test'),
                PROVISIOND_HARNESS_UNIT='recovery-port-test.service'):
    from harness import Harness
    from w7 import rebuild,ordinary,legacy_upgrade,expired_replay
    from workflow import Workflow


class RecoveryListenerIsolation(unittest.TestCase):
    def test_rebuild_reserves_stopped_listeners_before_backup(self):
        with tempfile.TemporaryDirectory() as directory,patch.dict(os.environ,
                PROVISIOND_HARNESS_INVOCATION=directory,PROVISIOND_HARNESS_JOBS='1',
                PROVISIOND_HARNESS_SLOT_BASE='25880',PROVISIOND_HARNESS_PORT_BASE='25880'):
            with port_pool(Path(directory),25880),worker_slot():
                h=Harness.__new__(Harness)
                base=int(os.environ['PROVISIOND_HARNESS_PORT_BASE'])
                h.ports=list(range(base,base+10));h.port_reservations={}
                reached=[]
                class BoundaryReached(Exception):pass
                def stop(name):
                    self.assertEqual(name,'standby')
                    # A real competing client must be unable to take either
                    # listener while PostgreSQL is being rebuilt. No database
                    # process is needed to reproduce the allocation gap.
                    for offset in (4,7):
                        with socket.socket() as contender:
                            with self.assertRaises(OSError):
                                contender.bind(('127.0.0.1',h.ports[offset]))
                    reached.append(name)
                    raise BoundaryReached
                h.stop=stop
                w=Workflow.__new__(Workflow);w.h=h;w.stop_engine=Mock();w.api=Mock()
                w.api.poll.return_value=None;w.operator_env={'configured':'yes'}
                try:
                    with self.assertRaises(BoundaryReached):rebuild(w)
                    self.assertEqual(reached,['standby'])
                    w.api.terminate.assert_called_once_with()
                    w.api.wait.assert_called_once_with(timeout=5)
                finally:
                    for port in list(h.port_reservations):h.release_port(port)


    def test_stopped_listeners_are_reserved_at_each_work_boundary(self):
        # STO-7/54: ordinary restarts, offline schema work and recovery SQL keep
        # both listeners unavailable to competing clients until component start.
        for history in (ordinary,legacy_upgrade,expired_replay):
            for operator in (False,True):
                with self.subTest(history=history.__name__,operator=operator),tempfile.TemporaryDirectory() as directory,patch.dict(
                        os.environ,PROVISIOND_HARNESS_INVOCATION=directory,PROVISIOND_HARNESS_JOBS='1',
                        PROVISIOND_HARNESS_SLOT_BASE='25880',PROVISIOND_HARNESS_PORT_BASE='25880'):
                    with port_pool(Path(directory),25880),worker_slot():
                        h=Harness.__new__(Harness);base=int(os.environ['PROVISIOND_HARNESS_PORT_BASE'])
                        h.ports=list(range(base,base+10));h.port_reservations={};h.proxy=SimpleNamespace(connections=[])
                        generations=iter(('generation-1','generation-2'))
                        h.sql=Mock(side_effect=lambda query:next(generations) if query.startswith('SELECT generation') else '')
                        w=Workflow.__new__(Workflow);w.h=h;w.operator_env={'configured':'yes'} if operator else {}
                        w.api=Mock();w.api.poll.return_value=None;w.stop_engine=Mock();w.start_engine=Mock()
                        w.status=Mock(return_value='queued');w.enrol=Mock(return_value={});w.fund=Mock()
                        w.request=Mock(return_value={'id':'deposit'});w.control=Path(directory)
                        reached=[]
                        class BoundaryReached(Exception):pass
                        def boundary(*args,**kwargs):
                            for offset in ((4,7) if operator else (4,)):
                                with socket.socket() as contender:
                                    with self.assertRaises(OSError,msg='stopped listener port must stay reserved'):
                                        contender.bind(('127.0.0.1',h.ports[offset]))
                            reached.append(history.__name__)
                            raise BoundaryReached
                        if history is ordinary:h.stop=boundary
                        else:h.sql=boundary
                        try:
                            with patch('w7.admit',return_value=({},None,'operation')),patch('w7.record',return_value=None):
                                with self.assertRaises(BoundaryReached):history(w)
                            self.assertEqual(reached,[history.__name__])
                            w.api.terminate.assert_called_once_with();w.api.wait.assert_called_once_with(timeout=5)
                        finally:
                            for port in list(h.port_reservations):h.release_port(port)


if __name__=='__main__':unittest.main()
