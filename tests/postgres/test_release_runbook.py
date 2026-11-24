"""STO-13: the documented offline migration must check every deployment login."""
import os
from pathlib import Path
import re
import subprocess
import tempfile
import textwrap
import unittest


RUNBOOK = Path(__file__).resolve().parents[2] / 'docs/release.md'


def shell_block(step):
    """Read a complete documented shell block without rewriting its commands."""
    section, = re.findall(rf'^{step}\. .*?(?=^{step + 1}\. )',
                          RUNBOOK.read_text(), re.M | re.S)
    block, = re.findall(r'^   ```sh\n(.*?)^   ```$', section, re.M | re.S)
    return textwrap.dedent(block)


class ReleaseRunbook(unittest.TestCase):
    def test_complete_inventory_reaches_migrate(self):
        # Only the external executable is stubbed: sourcing and environment
        # forwarding are the actual documented shell, including its subshell.
        # Also execute step 1: its single-role default must work, and preparing
        # private configuration must preserve an explicit multi-role inventory.
        cases = [(False, 'app'), (False, 'app,worker'),
                 (True, None), (True, 'app'), (True, 'app,worker')]
        for prepare, inventory in cases:
            for configured in (None, 'app', 'worker'):
                with self.subTest(prepare=prepare, inventory=inventory,
                                  migrator_roles=configured):
                    with tempfile.TemporaryDirectory(prefix='release-runbook-') as directory:
                        root = Path(directory)
                        (root / 'bin').mkdir()
                        runner = root / 'bin/migrate'
                        runner.write_text('#!/bin/sh\n'
                                          'printf "%s\\n" "$PROVISIOND_APPLICATION_ROLES" > "$ROLE_CAPTURE"\n')
                        runner.chmod(0o700)
                        config = root / 'migrator.env'
                        config.write_text('# Harmless fixture configuration.\n' + (
                            '' if configured is None else
                            f'PROVISIOND_APPLICATION_ROLES={configured}\n'))
                        capture = root / 'roles'
                        env = dict(PATH=os.environ['PATH'], HOME=str(root), APP_ROLE='app',
                                   MIGRATOR_ENV=str(config), NEW_RELEASE=str(root),
                                   ROLE_CAPTURE=str(capture), TMPDIR=str(root))
                        if inventory is not None:
                            env['PROVISIOND_APPLICATION_ROLES'] = inventory
                        if prepare:
                            env.update({name: str(root / name.lower()) for name in (
                                'APP_ENV', 'OPERATOR_DATABASE_URL', 'PRIMARY_DATA',
                                'STANDBY_DATA', 'PRIMARY_LOG', 'STANDBY_LOG')})
                            env.update(PRIMARY_SERVER_OPTIONS='', STANDBY_SERVER_OPTIONS='')
                        script = (shell_block(1) if prepare else '') + shell_block(5)
                        result = subprocess.run(['sh', '-c', script],
                                                cwd=root, env=env, capture_output=True,
                                                text=True, timeout=10)
                        self.assertEqual(result.returncode, 0, result.stderr)
                        self.assertTrue(capture.is_file(), 'migration runner was not invoked')
                        self.assertEqual(capture.read_text().strip(), inventory or 'app',
                                         'full selected deployment inventory must reach migrate')


if __name__ == '__main__':
    unittest.main()
