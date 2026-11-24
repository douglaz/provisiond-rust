"""STO-55: pure sensitivity checks also exercised by the healthy checkout case."""
import io
import unittest

from w4_probe import completed_probe


QUERY = 'SELECT housekeeping_work_probe'
SESSION = 717021


def sent(query=QUERY, session=SESSION):
    return dict(event='send_requested', protocol='P', session=session, query=query)


def reply(tag, query=QUERY, session=SESSION, **values):
    return dict(event='protocol_response', tag=tag, query=query, session=session, **values)


def success():
    # Recorded reply order in the restored session: prepare Z, close Z, C, Z, Z.
    return [sent(), reply('Z', ready_status='I'), reply('Z', ready_status='I'),
            reply('C', command_tag='SELECT 1'), reply('Z', ready_status='I'),
            reply('Z', ready_status='I')]


class ProbeCompletionTests(unittest.TestCase):
    def check(self, events, cursor=0):
        return completed_probe(events, cursor, SESSION, QUERY)

    def test_preparation_and_incomplete_execution(self):
        rows = success()
        for end in (2, 3, 4):
            with self.subTest(end=end):
                self.assertIsNone(self.check(rows[:end]))
        self.assertIsNone(self.check([sent(), reply('Z'), reply('C', command_tag='SELECT 1')]))

    def test_errors_cannot_become_success_on_ready(self):
        errors = [reply('E'), dict(event='server_error_forwarded', session=SESSION)]
        for error in errors:
            for prefix in (success()[:3], success()[:4]):
                with self.subTest(error=error, prefix=prefix):
                    self.assertIsNone(self.check(prefix + [error, reply('Z', ready_status='I')]))
                    self.assertIsNone(self.check(prefix + [error] + success()))

    def test_exact_query_and_session_own_the_completion(self):
        for foreign in (dict(query=QUERY + ' other'), dict(session=SESSION + 1)):
            with self.subTest(foreign=foreign):
                self.assertIsNone(self.check([sent(), reply('C', command_tag='SELECT 1', **foreign),
                                               reply('Z', ready_status='I')]))
                self.assertIsNone(self.check([sent(), reply('C', command_tag='SELECT 1'),
                                               reply('Z', ready_status='I', **foreign)]))
        self.assertIsNone(self.check(success()[1:]))
        self.assertIsNone(self.check([sent(), sent('other')] + success()[1:]))
        self.assertIsNone(self.check([sent(), reply('C', command_tag='SELECT 0'), reply('Z', ready_status='I')]))
        self.assertIsNone(self.check(success()[:4] + [reply('Z', ready_status='E')]))

    def test_two_distinct_executions_and_no_reuse(self):
        rows = success()
        first = self.check(rows)
        self.assertEqual(first, dict(session=SESSION, send_cursor=0, command_cursor=3,
                                     ready_cursor=4, next_cursor=5))
        self.assertIsNone(self.check(rows, first['next_cursor']))
        second = self.check(rows + success(), first['next_cursor'])
        self.assertEqual(second['send_cursor'], len(rows))
        self.assertGreater(second['command_cursor'], first['ready_cursor'])
        self.assertIsNone(self.check(rows + success(), second['next_cursor']))


def probe_completion_regressions():
    output = io.StringIO()
    result = unittest.TextTestRunner(stream=output).run(
        unittest.defaultTestLoader.loadTestsFromTestCase(ProbeCompletionTests))
    assert result.wasSuccessful(), output.getvalue()
    return result.testsRun


if __name__ == '__main__':
    unittest.main()
