"""W5 aggregation must refuse focused, missing, duplicate and unclean runs."""
import copy
import unittest
from w5 import CASES, inventory_complete


class Inventory(unittest.TestCase):
    def test_full_inventory_only(self):
        complete=dict(result='pass',scenarios=[dict(test=name,result='pass') for name in CASES],
            events=[dict(event='w5_inventory_complete',required=CASES,observed=CASES),dict(event='cleanup_complete',result='pass')])
        self.assertTrue(inventory_complete(complete))
        self.assertFalse(inventory_complete({}))
        for kind in ('missing','duplicate','failed','focused','cleanup'):
            manifest=copy.deepcopy(complete)
            if kind=='missing':manifest['scenarios'].pop()
            if kind=='duplicate':manifest['scenarios'].append(manifest['scenarios'][0])
            if kind=='failed':manifest['scenarios'][0]['result']='inconclusive'
            if kind=='focused':manifest['events'][0]['required']=CASES[:1]
            if kind=='cleanup':manifest['events'][-1]['result']='fail'
            with self.subTest(kind=kind):self.assertFalse(inventory_complete(manifest))


if __name__=='__main__':unittest.main()
