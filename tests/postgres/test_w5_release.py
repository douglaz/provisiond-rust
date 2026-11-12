"""OPS-49: release evidence must distinguish retention from spare capacity."""
import unittest
from w5_release_control import ASSERTION, probe


class ReleaseEvidence(unittest.TestCase):
    def test_zero_outstanding_checkout_is_release(self):
        self.assertTrue(probe(0)[0]['release_before_backoff'])

    def test_retained_checkout_is_rejected(self):
        for count in (1,2,3):
            with self.subTest(checked_out=count),self.assertRaisesRegex(AssertionError,'^'+ASSERTION+'$'):
                probe(count)


if __name__=='__main__':unittest.main()
