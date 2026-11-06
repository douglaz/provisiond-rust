"""Port preflight tests; never launch PostgreSQL or any subprocess."""
import os
import socket
import unittest
from unittest.mock import patch

with patch.dict(os.environ, PROVISIOND_HARNESS_ROOT='/var/tmp/port-test',
                PROVISIOND_HARNESS_UNIT='port-test.service'):
    from harness import allocation, preflight_ports


class Ports(unittest.TestCase):
    def test_missing_or_invalid(self):
        for value in ['', '-1', '55410', '55429', '55477', '55480', '55430.0', 'x']:
            with self.subTest(value=value), patch.dict(os.environ, PROVISIOND_HARNESS_PORT_BASE=value):
                with self.assertRaises(ValueError):
                    allocation()

    def test_all_endpoints(self):
        for base in [55430, 55440, 55450, 55460, 55470, 55476]:
            with patch.dict(os.environ, PROVISIOND_HARNESS_PORT_BASE=str(base)):
                self.assertEqual(allocation(), list(range(base, base+4)))

    def test_busy_refused(self):
        ports = allocation()
        with socket.socket() as listener:
            listener.bind(('127.0.0.1', ports[-1]))
            listener.listen()
            with self.assertRaises(OSError):
                preflight_ports()


if __name__ == '__main__':
    unittest.main()
