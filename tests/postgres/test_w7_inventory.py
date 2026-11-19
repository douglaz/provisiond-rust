"""Recovery acceptance requires each activated child and its completed cleanup."""
import copy
import unittest
from w7 import CASES,inventory_complete

class Inventory(unittest.TestCase):
    def test_missing_or_unactivated_child_cannot_pass(self):
        children=[dict(event='w7_child_result',scenario=name,exit_status=0,manifest=dict(result='pass',seed=20261113,
            events=[dict(event='w7_causal_trace',scenario=name),dict(event='cleanup_complete',result='pass')])) for name in CASES]
        complete=dict(result='pass',seed=20261113,scenarios=[dict(test=name,result='pass') for name in CASES],
            events=[dict(event='w7_inventory_complete',required=CASES,observed=CASES),dict(event='cleanup_complete',result='pass'),*children])
        self.assertTrue(inventory_complete(complete))
        for defect in ('missing','duplicate','unactivated','unclean','failed','seed'):
            candidate=copy.deepcopy(complete)
            if defect=='missing':candidate['events'].pop()
            if defect=='duplicate':candidate['events'].append(candidate['events'][-1])
            if defect=='unactivated':candidate['events'][-1]['manifest']['events'].pop(0)
            if defect=='unclean':candidate['events'][-1]['manifest']['events'][-1]['result']='fail'
            if defect=='failed':candidate['events'][-1]['exit_status']=1
            if defect=='seed':candidate['events'][-1]['manifest']['seed']=0
            with self.subTest(defect=defect):self.assertFalse(inventory_complete(candidate))

if __name__=='__main__':unittest.main()
