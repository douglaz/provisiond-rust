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
    def test_ephemeral_overlap_refused(self):
        # STO-7 fixture isolation: refuse an allocation before any child starts.
        base=int(os.environ['PROVISIOND_HARNESS_PORT_BASE'])
        for start,end in ((base,base+9),(base+9,base+20),(base-20,base)):
            with self.subTest(start=start,end=end),patch.object(Path,'read_text',return_value=f'{start} {end}\n'):
                with self.assertRaisesRegex(ValueError,'harness ports overlap kernel ephemeral range'):
                    allocation()

    def test_new_range_outside_ephemeral_accepted(self):
        with patch.dict(os.environ,PROVISIOND_HARNESS_PORT_BASE='25500'),patch.object(
                Path,'read_text',return_value='32768 60999\n'):
            self.assertEqual(allocation(),list(range(25500,25510)))

    def test_runner_refuses_overlap_before_build_or_reservation(self):
        # STO-7 fixture allocations include every block the runner may assign.
        from runner import invocation,port_pool
        base=int(os.environ['PROVISIOND_HARNESS_PORT_BASE'])
        with patch.object(Path,'read_text',return_value=f'{base+499} {base+600}\n'),patch.object(
                Path,'mkdir',side_effect=ValueError('directory reached before ephemeral guard')),patch(
                'runner.subprocess.run') as run,patch('runner.socket.socket',
                side_effect=ValueError('socket reached before ephemeral guard')) as sockets:
            for context in (invocation(Path('/unused'),1),port_pool(Path('/unused'),base)):
                with self.subTest(context=context),self.assertRaisesRegex(
                        ValueError,'harness ports overlap kernel ephemeral range'):
                    with context:pass
            run.assert_not_called();sockets.assert_not_called()

    def test_missing_or_invalid(self):
        for value in ['', '-1', '25410', '25479', '25501', '25551', '26000', '25500.0', 'x']:
            with self.subTest(value=value), patch.dict(os.environ, PROVISIOND_HARNESS_PORT_BASE=value):
                with self.assertRaises(ValueError):
                    allocation()

    def test_all_endpoints(self):
        for base in range(25500,26000,10):
            with patch.dict(os.environ, PROVISIOND_HARNESS_PORT_BASE=str(base)):
                self.assertEqual(allocation(), list(range(base, base+10)))

    def test_busy_refused(self):
        with patch.dict(os.environ, PROVISIOND_HARNESS_PORT_BASE='25500'):
            for port in allocation():
                with self.subTest(port=port), socket.socket() as listener:
                    listener.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
                    listener.bind(('127.0.0.1', port))
                    listener.listen()
                    with self.assertRaises(OSError):
                        preflight_ports()


if __name__ == '__main__':
    unittest.main()
