"""STO-7/54: empty pre-listener replay is read-only fixture deployment work."""
import json
import os
from pathlib import Path
from types import SimpleNamespace
import unittest
from unittest.mock import Mock,patch

with patch.dict(os.environ,PROVISIOND_HARNESS_ROOT=str(Path.cwd()/'target/retry-test'),
                PROVISIOND_HARNESS_UNIT='retry-test.service'):
    from workflow import Workflow

EVENTS=[dict(event='session_admitted',session=17),
        dict(event='recovery_startup_decision',incident=dict(steps_completed=2)),
        dict(event='recovery_watch_set_derived',deposits=[]),
        dict(event='external_entry',kind='Rail',simulator=True),
        dict(event='call_cancelled_session_closed',session=0),dict(event='driver_closed_unused')]
# The failed API start in the 2026-11-25 w7_default_hooks record (api-6.stdout), sessions renumbered.
RECORDED=[dict(event='client_keepalives_observed'),dict(event='check_target',kind='admission',session=17),
          dict(event='check_observation',kind='admission',session=17),dict(EVENTS[0]),
          dict(event='schema_compatible'),dict(event='schema_compatible'),
          dict(event='recovery_startup_decision',incident=dict(steps_completed=2)),
          dict(event='recovery_watch_set_derived',deposits=['4b82fc29-b1f6-46ef-9d46-a84276c7bc66']),
          dict(event='external_entry',kind='Rail',simulator=True),dict(event='client_keepalives_observed'),
          dict(event='check_target',kind='admission',session=18),dict(event='check_observation',kind='admission',session=18),
          dict(event='call_cancelled_session_closed',session=18),dict(event='driver_closed_unused'),
          dict(event='driver_closed_unused'),dict(event='supervisor_drained')]


def process(events,status):
    p=Mock()
    p.log.read_text.return_value=''.join(json.dumps(e)+'\n' for e in events)
    p.log.read_bytes.return_value=p.log.read_text.return_value.encode()
    p.error.read_text.return_value='whole pool checkout deadline' if status else ''
    p.error.read_bytes.return_value=p.error.read_text.return_value.encode()
    p.poll.return_value=status;p.returncode=status
    return p


class StartupRetry(unittest.TestCase):
    def test_failed_api_start_survives_reservation_error(self):
        # STO-7: a secondary fixture error must preserve the actual failed start.
        import hashlib
        w=self.fixture([])
        failed=process([],73)
        failed.error.read_text.return_value='Address already in use (os error 98)'
        failed.error.read_bytes.return_value=failed.error.read_text.return_value.encode()
        w.start=Mock(return_value=failed)
        w.reserve_api_ports=lambda:Workflow.reserve_api_ports(w)
        w.h.reserve_port=Mock(side_effect=OSError(98,'Address already in use'))
        try:
            Workflow.launch_api(w)
        except (AssertionError,OSError) as error:
            failure=error
        else:
            self.fail('failed startup must propagate')
        self.assertEqual(w.start.call_count,1)
        calls=w.h.event.call_args_list
        exits=[c.kwargs for c in calls if c.args==('api_fixture_deployment_exit',)]
        self.assertEqual(len(exits),1)
        self.assertEqual((exits[0]['exit_status'],exits[0]['result'],exits[0]['retry_eligible']),(73,'fail',False))
        self.assertEqual(exits[0]['stderr_excerpt'],failed.error.read_text.return_value)
        self.assertEqual(exits[0]['stdout_sha256'],hashlib.sha256(failed.log.read_bytes()).hexdigest())
        self.assertEqual(exits[0]['stderr_sha256'],hashlib.sha256(failed.error.read_bytes()).hexdigest())
        self.assertIsInstance(failure,AssertionError)
        self.assertIn('current API startup failed before readiness: Address already in use',str(failure))
        errors=[c.kwargs for c in calls if c.args==('api_fixture_reservation_error',)]
        self.assertEqual(len(errors),1);self.assertEqual(errors[0]['errno'],98)
        self.assertLess(next(i for i,c in enumerate(calls) if c.args==('api_fixture_deployment_exit',)),
                        next(i for i,c in enumerate(calls) if c.args==('api_fixture_reservation_error',)))

    def fixture(self,events):
        h=SimpleNamespace(ports=list(range(25500,25510)),proxy=SimpleNamespace(gates=[]),event=Mock())
        w=SimpleNamespace(h=h,reserve_api_ports=Mock(),operator_env={},parameters='parameters',control='control',defect=None,case='w7_reporting',
            start=Mock(side_effect=[process(events,1),process([dict(event='api_listening')],None)]))
        return w

    def test_failed_engine_start_recorded_before_cleanup_reservation_error(self):
        # STO-7: engine deployment evidence survives a later listener cleanup error.
        import hashlib
        import tempfile
        with tempfile.TemporaryDirectory() as directory:
            w=self.engine_fixture([],status=71)
            failed=process([],71);failed.name='engine'
            failed.log=Path(directory)/'engine.stdout';failed.log.write_text('')
            failed.error=Path(directory)/'engine.stderr';failed.error.write_text('Address already in use (os error 98)')
            w.start=Mock(return_value=failed);w.children=[failed];w.h.root=Path(directory)
            w.reserve_api_ports=lambda:Workflow.reserve_api_ports(w)
            w.h.reserve_port=Mock(side_effect=OSError(98,'Address already in use'))
            with self.assertRaisesRegex(AssertionError,'current engine startup failed before readiness'):
                Workflow.start_engine(w)
            with self.assertRaises(OSError):Workflow.close(w)
            calls=w.h.event.call_args_list
            exits=[c.kwargs for c in calls if c.args==('engine_fixture_deployment_exit',)]
            self.assertEqual(len(exits),1);self.assertEqual(exits[0]['exit_status'],71)
            self.assertEqual(exits[0]['result'],'fail')
            self.assertEqual(exits[0].get('stderr_excerpt'),failed.error.read_text())
            self.assertEqual(exits[0]['stdout_sha256'],hashlib.sha256(failed.log.read_bytes()).hexdigest())
            self.assertEqual(exits[0]['stderr_sha256'],hashlib.sha256(failed.error.read_bytes()).hexdigest())
            errors=[c for c in calls if c.args==('api_fixture_reservation_error',)]
            self.assertEqual(len(errors),1)
            self.assertLess(next(i for i,c in enumerate(calls) if c.args==('engine_fixture_deployment_exit',)),calls.index(errors[0]))
            self.assertEqual(w.start.call_count,1)

    def test_empty_replay_can_repeat_before_write_checkout(self):
        w=self.fixture(EVENTS)
        with patch('workflow.http',return_value=(200,[],{})):
            Workflow.launch_api(w)
        self.assertEqual(w.start.call_count,2)

    def test_effects_or_admitted_writes_cannot_repeat(self):
        for extra in [dict(event='rail_credit_acknowledged'),dict(event='session_admitted',session=18),
                      dict(event='api_listening'),dict(event='http_business_begin'),
                      dict(event='recovery_step_acknowledged',step=3),dict(event='external_entry',kind='Provider')]:
            w=self.fixture([*EVENTS,extra])
            with self.subTest(extra=extra),patch('workflow.http',return_value=(200,[],{})):
                with self.assertRaisesRegex(AssertionError,'current API startup failed before readiness'):
                    Workflow.launch_api(w)
            self.assertEqual(w.start.call_count,1)
        w=self.fixture(EVENTS)
        with patch('workflow.http',return_value=(200,[dict(rail='lightning')],{})):
            with self.assertRaisesRegex(AssertionError,'current API startup failed before readiness'):
                Workflow.launch_api(w)
        self.assertEqual(w.start.call_count,1)

    def test_retry_bound_and_fault_controls_stay_in_force(self):
        w=self.fixture(EVENTS)
        w.start=Mock(side_effect=[process(EVENTS,1) for _ in range(3)])
        with patch('workflow.http',return_value=(200,[],{})):
            with self.assertRaisesRegex(AssertionError,'current API startup failed before readiness'):
                Workflow.launch_api(w)
        self.assertEqual(w.start.call_count,3)
        for defect,armed in [('w7_finite_guess',False),(None,True)]:
            w=self.fixture(EVENTS);w.defect=defect
            if armed:
                release=Mock();release.is_set.return_value=False
                w.h.proxy.gates=[SimpleNamespace(released=release)]
            with self.subTest(defect=defect,armed=armed),patch('workflow.http',return_value=(200,[],{})):
                with self.assertRaisesRegex(AssertionError,'current API startup failed before readiness'):
                    Workflow.launch_api(w)
            self.assertEqual(w.start.call_count,1)


    def test_empty_replay_eligibility_does_not_depend_on_case(self):
        for case in ('w7_continuation','w7_supersession','another-history'):
            w=self.fixture(EVENTS);w.case=case
            with self.subTest(case=case),patch('workflow.http',return_value=(200,[],{})):
                Workflow.launch_api(w)
            self.assertEqual(w.start.call_count,2)

    def test_nonempty_or_effectful_replay_is_ineligible_in_any_case(self):
        for case in ('w7_reporting','w7_continuation','another-history'):
            for extra,settlements in [([], [dict(rail='lightning')]),
                    ([dict(event='rail_credit_acknowledged')], []),
                    ([dict(event='recovery_step_acknowledged',step=3)], [])]:
                w=self.fixture([*EVENTS,*extra]);w.case=case
                with self.subTest(case=case,extra=extra,settlements=settlements),patch(
                        'workflow.http',return_value=(200,settlements,{})):
                    with self.assertRaisesRegex(AssertionError,'current API startup failed before readiness'):
                        Workflow.launch_api(w)
                self.assertEqual(w.start.call_count,1)

    def test_recorded_write_constructor_refusal_can_repeat(self):
        # D-68, pvr-nepn: the 2026-11-25 w7_default_hooks shape. The watch set
        # named a deposit, the replay was empty, and the write constructor was
        # cancelled while its admission check waited; it never reached BEGIN.
        # Either driver may also end with its stream (tokio::select! picks a ready branch).
        stream=list(RECORDED);stream[-3]=dict(event='driver_closed')
        streams=list(stream);streams[-2]=dict(event='driver_closed')
        for events in (RECORDED,stream,streams):
            w=self.fixture(events)
            with self.subTest(drivers=[e['event'] for e in events[-3:-1]]),patch('workflow.http',return_value=(200,[],{})):
                Workflow.launch_api(w)
                self.assertEqual(w.start.call_count,2)
                exits=[c.kwargs for c in w.h.event.call_args_list if c.args==('api_fixture_deployment_exit',)]
                self.assertEqual([(e['retry_eligible'],e['empty_read_only_recovery_replay']) for e in exits],[(True,True)])

    def test_recorded_shape_with_admission_effect_or_unknown_session_cannot_repeat(self):
        unknown=[dict(e,session=19) if e['event']=='call_cancelled_session_closed' else e for e in RECORDED]
        for events,settlements in [([*RECORDED,dict(EVENTS[0],session=18)],[]),
                                   (unknown,[]),(RECORDED,[dict(rail='lightning')]),
                                   ([*RECORDED,dict(event='rail_observation_processed')],[]),
                                   ([*RECORDED,dict(event='driver_dead',error='reset')],[])]:
            w=self.fixture(events)
            with self.subTest(events=events[-1],settlements=settlements),patch(
                    'workflow.http',return_value=(200,settlements,{})):
                with self.assertRaisesRegex(AssertionError,'current API startup failed before readiness'):
                    Workflow.launch_api(w)
                self.assertEqual(w.start.call_count,1)

    def engine_fixture(self,events,status=1):
        w=self.fixture(events)
        w.start=Mock(side_effect=[process(events,status),process([dict(event='restart_pass_complete')],None)])
        return w

    def test_engine_pre_admission_checkout_can_repeat_with_true_exit_and_hashes(self):
        # STO-7: a failed checked constructor closes unused; deployment retries
        # do not turn that invocation into a pass or change the 250 ms bound.
        events=[dict(event=e,kind='admission') for e in ('client_keepalives_observed','check_target',
            'call_cancelled_session_closed','driver_closed_unused','supervisor_drained')]
        self.assert_engine_retry_evidence(events)

    def test_engine_standby_observations_can_repeat_with_true_exit_and_hashes(self):
        # STO-7: the recorded engine-16 refusal waited on the fixed target ten
        # times, then closed unused. Observing the peer is still pre-admission.
        events=[dict(event='client_keepalives_observed',count=2,idle_seconds=2,interval_seconds=1),
            dict(event='check_target',kind='admission',session=1866503,target='0/322C000'),
            *[dict(event='check_observation',kind='admission',session=1866503,target='0/322C000',
                observed=['1859245','replicator','provisiond_sync','127.0.0.1','streaming','sync','0/322BFE8'])
                for _ in range(10)],
            dict(event='driver_closed_unused'),dict(event='supervisor_drained')]
        self.assert_engine_retry_evidence(events)

    def assert_engine_retry_evidence(self,events):
        w=self.engine_fixture(events,status=71)
        processes=list(w.start.side_effect);w.start.side_effect=processes
        failed=processes[0]
        import hashlib
        result=Workflow.start_engine(w)
        self.assertIs(result,w.engine);self.assertEqual(w.start.call_count,2)
        exits=[c.kwargs for c in w.h.event.call_args_list if c.args==('engine_fixture_deployment_exit',)]
        self.assertEqual(len(exits),1)
        self.assertEqual((exits[0]['attempt'],exits[0]['exit_status'],exits[0]['result']),(1,71,'fail'))
        self.assertEqual(exits[0]['stderr_excerpt'],failed.error.read_text())
        self.assertEqual(exits[0]['events'],[e['event'] for e in events])
        self.assertEqual(exits[0]['stdout_sha256'],hashlib.sha256(failed.log.read_bytes()).hexdigest())
        self.assertEqual(exits[0]['stderr_sha256'],hashlib.sha256(failed.error.read_bytes()).hexdigest())
        repeats=[c.kwargs for c in w.h.event.call_args_list if c.args==('engine_fixture_deployment_repeated',)]
        self.assertEqual(len(repeats),1);self.assertEqual(repeats[0]['checkout_ms'],250)
        calls=w.h.event.call_args_list
        self.assertLess(next(i for i,c in enumerate(calls) if c.args==('engine_fixture_deployment_exit',)),
                        next(i for i,c in enumerate(calls) if c.args==('engine_fixture_deployment_repeated',)))

    def test_engine_admission_or_unknown_work_cannot_repeat(self):
        for event in ('session_admitted','schema_compatible','recovery_startup_decision',
                'startup_lock_acquired','generation_advanced','restart_pass_complete','external_entry',
                'http_business_begin','unknown_event'):
            w=self.engine_fixture([dict(event='check_target',kind='admission'),
                dict(event='check_observation',kind='admission'),dict(event=event)])
            with self.subTest(event=event),self.assertRaisesRegex(AssertionError,'current engine startup failed before readiness'):
                Workflow.start_engine(w)
            self.assertEqual(w.start.call_count,1)

    def test_engine_only_admission_check_events_can_repeat(self):
        for event in ('check_target','check_observation'):
            for kind in (None,'post_read','test','unknown'):
                events=[dict(event='check_target',kind='admission'),dict(event=event,kind=kind)]
                w=self.engine_fixture(events)
                with self.subTest(event=event,kind=kind),self.assertRaisesRegex(
                        AssertionError,'current engine startup failed before readiness'):
                    Workflow.start_engine(w)
                self.assertEqual(w.start.call_count,1)

    def test_engine_retry_bound_fault_defect_and_non_checkout_exits(self):
        events=[dict(event='check_target',kind='admission'),dict(event='check_observation',kind='admission')]
        w=self.engine_fixture(events);w.start=Mock(side_effect=[process(events,1) for _ in range(3)])
        with self.assertRaisesRegex(AssertionError,'current engine startup failed before readiness'):
            Workflow.start_engine(w)
        self.assertEqual(w.start.call_count,3)
        exits=[c.kwargs for c in w.h.event.call_args_list if c.args==('engine_fixture_deployment_exit',)]
        self.assertEqual([e['attempt'] for e in exits],[1,2,3])
        self.assertTrue(all(e['result']=='fail' and e['exit_status']==1 for e in exits))
        for mode in ('fault','defect','other_error','zero_exit'):
            w=self.engine_fixture(events,status=0 if mode=='zero_exit' else 1)
            if mode=='fault':
                release=Mock();release.is_set.return_value=False
                w.h.proxy.gates=[SimpleNamespace(released=release)]
            if mode=='defect':w.defect='w7_no_ack'
            if mode=='other_error':
                processes=list(w.start.side_effect);processes[0].error.read_text.return_value='another startup error'
                w.start.side_effect=processes
            with self.subTest(mode=mode),self.assertRaisesRegex(AssertionError,'current engine startup failed before readiness'):
                Workflow.start_engine(w)
            self.assertEqual(w.start.call_count,1)


if __name__=='__main__':unittest.main()
