"""Port preflight tests; never launch PostgreSQL or any subprocess."""
import os
from pathlib import Path
import socket
import unittest
from unittest.mock import patch

with patch.dict(os.environ, PROVISIOND_HARNESS_ROOT=str(Path.cwd() / 'target' / 'port-test'),
                PROVISIOND_HARNESS_UNIT='port-test.service'):
    from harness import allocation, preflight_ports


class Ports(unittest.TestCase):
    def test_missing_or_invalid(self):
        for value in ['', '-1', '55410', '55479', '55501', '55551', '55560', '55500.0', 'x']:
            with self.subTest(value=value), patch.dict(os.environ, PROVISIOND_HARNESS_PORT_BASE=value):
                with self.assertRaises(ValueError):
                    allocation()

    def test_all_endpoints(self):
        for base in [55500, 55510, 55520, 55530, 55540, 55550]:
            with patch.dict(os.environ, PROVISIOND_HARNESS_PORT_BASE=str(base)):
                self.assertEqual(allocation(), list(range(base, base+10)))

    def test_busy_refused(self):
        with patch.dict(os.environ, PROVISIOND_HARNESS_PORT_BASE='55500'):
            for port in allocation():
                with self.subTest(port=port), socket.socket() as listener:
                    listener.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
                    listener.bind(('127.0.0.1', port))
                    listener.listen()
                    with self.assertRaises(OSError):
                        preflight_ports()


if __name__ == '__main__':
    unittest.main()
