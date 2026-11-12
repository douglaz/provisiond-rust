"""A focused, failed or unclean W6 run cannot satisfy aggregate acceptance."""
import copy
import unittest
from w6 import CASES,inventory_complete

class Inventory(unittest.TestCase):
    def test_only_complete_clean_inventory(self):
        complete=dict(result='pass',scenarios=[dict(test=name,result='pass') for name in CASES],
            events=[dict(event='w6_inventory_complete',required=CASES,observed=CASES),dict(event='cleanup_complete',result='pass')])
        self.assertTrue(inventory_complete(complete))
        self.assertFalse(inventory_complete({}))
        for change in ('missing','duplicate','failed','focused','cleanup'):
            candidate=copy.deepcopy(complete)
            if change=='missing':candidate['scenarios'].pop()
            if change=='duplicate':candidate['scenarios'].append(candidate['scenarios'][0])
            if change=='failed':candidate['scenarios'][0]['result']='inconclusive'
            if change=='focused':candidate['events'][0]['required']=CASES[:1]
            if change=='cleanup':candidate['events'][1]['result']='fail'
            with self.subTest(change=change):self.assertFalse(inventory_complete(candidate))

if __name__=='__main__':unittest.main()
