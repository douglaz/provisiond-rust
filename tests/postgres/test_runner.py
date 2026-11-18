"""STO-7/54: failed children retain their exits and cannot hide later children."""
import json
import os
from pathlib import Path
import sys
import tempfile
import unittest
from unittest.mock import patch
from runner import children,worker_slot,port_pool


class ForegroundWorkers(unittest.TestCase):
    def test_completed_worker_does_not_immediately_recycle_ports(self):
        with tempfile.TemporaryDirectory() as directory,patch.dict(os.environ,
                PROVISIOND_HARNESS_INVOCATION=directory,PROVISIOND_HARNESS_JOBS='1',
                PROVISIOND_HARNESS_SLOT_BASE='25880',PROVISIOND_HARNESS_PORT_BASE='25880'):
            with port_pool(Path(directory),25880):
                with worker_slot():first=int(os.environ['PROVISIOND_HARNESS_PORT_BASE'])
                with worker_slot():second=int(os.environ['PROVISIOND_HARNESS_PORT_BASE'])
            self.assertNotEqual(first,second)
            self.assertTrue(all(25500<=base<=25990 and base%10==0 for base in (first,second)))
            self.assertEqual(os.environ['PROVISIOND_HARNESS_PORT_BASE'],'25880')

    def test_bounded_children_keep_failure_and_complete_inventory(self):
        with tempfile.TemporaryDirectory() as directory:
            root=Path(directory)
            # An advisory lock serializes updates to the measured live count.
            # The failure is deliberate; all five later assertions must run.
            script='''import fcntl,json,sys,time
from pathlib import Path
root=Path(sys.argv[1]);index=int(sys.argv[2])
def update(delta):
 with (root/'lock').open('w') as lock:
  fcntl.flock(lock,fcntl.LOCK_EX)
  path=root/'counts'
  state=json.loads(path.read_text()) if path.exists() else dict(live=0,peak=0,done=[])
  state['live']+=delta;state['peak']=max(state['peak'],state['live'])
  if delta<0:state['done'].append(index)
  path.write_text(json.dumps(state))
update(1);time.sleep(.04);update(-1)
print(index);print('diagnostic',file=sys.stderr)
raise SystemExit(7 if index==0 else 0)
'''
            jobs=[([sys.executable,'-c',script,str(root),str(i)],root/f'child-{i}.json',None) for i in range(6)]
            with patch.dict(os.environ,PROVISIOND_HARNESS_JOBS='2'):
                results=children(jobs)
            self.assertEqual([r['exit_status'] for r in results],[7,0,0,0,0,0])
            state=json.loads((root/'counts').read_text())
            self.assertEqual(sorted(state['done']),list(range(6)))
            self.assertEqual(state['live'],0)
            self.assertLessEqual(state['peak'],2)
            for i in range(6):
                self.assertEqual((root/f'child-{i}.stdout').read_text(),f'{i}\n')
                self.assertEqual((root/f'child-{i}.stderr').read_text(),'diagnostic\n')


if __name__=='__main__':unittest.main()
