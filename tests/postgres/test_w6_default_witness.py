"""PRV-36/STO-53/OPS-27: falsify the actual default-binary witness predicate."""
from datetime import datetime,timezone
import unittest

from w6_edges import default_resolution_witness


class DefaultResolutionWitness(unittest.TestCase):
    def setUp(self):
        self.initial=dict(effective_seconds=10,eligible_at=self.instant(110),remaining_seconds=10)
        self.current=dict(self.initial,remaining_seconds=0)
        self.seen=[dict(operation='target',evidence=dict(matches=0,authoritative=False))]
        self.provider=[dict(command='target',stage='request_received',path='/search',resolution=True,timestamp=110.9),
            dict(command='target',stage='resolution_evidence_returned',search='wrong',resources=['other-correlator'])]

    @staticmethod
    def instant(seconds):
        return datetime.fromtimestamp(seconds,timezone.utc).isoformat()

    def witness(self,now=15,deadline=20):
        return default_resolution_witness(self.initial,self.current,self.seen,self.provider,'target',now,deadline,20)

    def test_stable_eligible_timely_search(self):
        self.assertEqual(self.witness(),(self.seen[-1],self.provider[:1]))

    def test_late_search_is_rejected(self):
        # A stable eligible search first observed by an overdue poll used to pass.
        for now in (20,20.1):
            self.provider[0]['timestamp']=100+now
            with self.subTest(now=now),self.assertRaisesRegex(AssertionError,'effective-window budget'):
                self.witness(now=now)

    def test_widened_before_request_observed_after_eligibility_is_rejected(self):
        # Window widens 10 -> 11 before request at age 10.9; poll is at age 11.1.
        self.current.update(effective_seconds=11,eligible_at=self.instant(111),remaining_seconds=0)
        with self.assertRaisesRegex(AssertionError,'visibility window changed'):
            self.witness(now=11.1)

    def test_missing_search_cannot_pass(self):
        self.seen=[]
        self.provider=[]
        self.assertIsNone(self.witness())
        with self.assertRaisesRegex(AssertionError,'effective-window budget'):
            self.witness(now=20)

    def test_independent_evidence_is_required(self):
        for missing in ('request','response','resources','wrong-correlator','eligible','exact','authoritative','premature'):
            with self.subTest(missing=missing):
                candidate=DefaultResolutionWitness()
                candidate.setUp()
                if missing=='request':candidate.provider.pop(0)
                if missing=='response':candidate.provider.pop()
                if missing=='resources':candidate.provider[-1]['resources']=[]
                if missing=='wrong-correlator':candidate.provider[-1]['search']='normal'
                if missing=='eligible':candidate.current['remaining_seconds']=.1
                if missing=='exact':candidate.seen[-1]['evidence']['matches']=1
                if missing=='authoritative':candidate.seen[-1]['evidence']['authoritative']=True
                if missing=='premature':candidate.provider[0]['timestamp']=109.9
                with self.assertRaises(AssertionError):candidate.witness()


if __name__=='__main__':unittest.main()
