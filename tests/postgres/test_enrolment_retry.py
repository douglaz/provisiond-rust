"""STO-55/DOM-17, API-33/40: finite fixture resends preserve refusal checks."""
import hashlib
import json
import os
from pathlib import Path
from types import SimpleNamespace
import unittest
from unittest.mock import Mock, patch

with patch.dict(os.environ, PROVISIOND_HARNESS_ROOT=str(Path.cwd()/'target/retry-test'),
                PROVISIOND_HARNESS_UNIT='retry-test.service'):
    from workflow import Workflow


def response(code=503,kind='overloaded',retryable=True,hint=250,retry_after='1'):
    headers={'X-Correlation-Id':'test-request','Access-Control-Allow-Origin':'*',
             'Access-Control-Expose-Headers':'Retry-After, X-Correlation-Id','Vary':'Origin',
             'Retry-After':retry_after}
    value={'error':{'kind':kind,'message':'store checkout unavailable',
                   'retryable':retryable,'details':{'retry_after_ms':hint}},
           'correlation_id':'test-request'} if code>=400 else {'status':'pending'}
    return code,value,headers


class EnrolmentRetry(unittest.TestCase):
    def fixture(self,defect=None,armed=False):
        gate=SimpleNamespace(released=SimpleNamespace(is_set=lambda:not armed))
        return SimpleNamespace(defect=defect,h=SimpleNamespace(ports=[0,0,0,0,25964],
            proxy=SimpleNamespace(gates=[gate]),event=Mock()))

    def test_enrolment_resends_exact_raw_bytes_without_a_key(self):
        raw=b' { "admission_token" : "fixture-code" }\n'
        w=self.fixture()
        with patch('workflow.http',side_effect=[response(),response(200)]) as send,patch('workflow.time.sleep') as sleep:
            self.assertEqual(Workflow.request(w,'POST','/v1/enrol',raw=raw),{'status':'pending'})
        self.assertEqual(send.call_count,2)
        self.assertEqual(send.call_args_list[0],send.call_args_list[1])
        self.assertEqual(send.call_args.args[6],raw)
        sleep.assert_called_once_with(1)
        event=w.h.event.call_args.kwargs
        self.assertIsNone(event['key_sha256'])
        self.assertEqual(event['body_sha256'],hashlib.sha256(raw).hexdigest())
        self.assertEqual(event['wait_ms'],1000)

    def test_serialized_body_is_frozen_before_first_send(self):
        w=self.fixture();body={'admission_token':'fixture-code'}
        expected=json.dumps(body).encode()
        def first(*args):
            body['admission_token']='must-not-replace-original'
            return response()
        calls=[]
        def send(*args):
            calls.append(args)
            return first(*args) if len(calls)==1 else response(200)
        with patch('workflow.http',side_effect=send),patch('workflow.time.sleep'):
            Workflow.request(w,'POST','/v1/enrol',body,key='ignored-header')
        self.assertEqual([c[6] for c in calls],[expected,expected])
        self.assertEqual([c[5] for c in calls],['ignored-header','ignored-header'])

    def test_attempt_budget_exhaustion_is_still_failure(self):
        w=self.fixture()
        with patch('workflow.http',return_value=response()) as send,patch('workflow.time.sleep') as sleep:
            with self.assertRaises(AssertionError):Workflow.request(w,'POST','/v1/enrol',{})
        self.assertEqual(send.call_count,3);self.assertEqual(sleep.call_count,2)
        self.assertEqual(w.h.event.call_count,2)

    def test_fault_defect_and_expected_error_never_resend(self):
        for defect,armed,status in [('checkout',False,200),(None,True,200),(None,False,503),(None,False,400)]:
            w=self.fixture(defect,armed)
            with self.subTest(defect=defect,armed=armed,status=status),patch('workflow.http',return_value=response()) as send,patch('workflow.time.sleep') as sleep:
                if status==503:Workflow.request(w,'POST','/v1/enrol',{},status=status)
                else:
                    with self.assertRaises(AssertionError):Workflow.request(w,'POST','/v1/enrol',{},status=status)
            send.assert_called_once();sleep.assert_not_called();w.h.event.assert_not_called()

    def test_invalid_token_and_unrelated_routes_are_not_retryable(self):
        for method,path,tenant,key,reply,status in [
                ('POST','/v1/enrol',None,None,response(400,'invalid_request',False),400),
                ('POST','/v1/enrol',None,None,response(kind='internal'),200),
                ('POST','/v1/enrol',None,None,response(retryable=False),200),
                ('GET','/v1/enrol',None,None,response(),200),
                ('POST','/v1/enrol/token',None,None,response(),200),
                ('POST','/v1/deposits',None,'key',response(),200),
                ('POST','/v1/machines','token',None,response(),202)]:
            w=self.fixture()
            with self.subTest(method=method,path=path,reply=reply),patch('workflow.http',return_value=reply) as send,patch('workflow.time.sleep') as sleep:
                if reply[0]==status:Workflow.request(w,method,path,{},tenant,key,status=status)
                else:
                    with self.assertRaises(AssertionError):Workflow.request(w,method,path,{},tenant,key,status=status)
            send.assert_called_once();sleep.assert_not_called()

    def test_existing_authenticated_write_resend_keeps_key(self):
        for path,status in [('/v1/deposits',200),('/v1/machines',202)]:
            w=self.fixture()
            with self.subTest(path=path),patch('workflow.http',side_effect=[response(),response(status)]) as send,patch('workflow.time.sleep') as sleep:
                Workflow.request(w,'POST',path,{'amount_sats':120},'spending-token','same-key',status=status)
            self.assertEqual(send.call_args_list[0],send.call_args_list[1])
            sleep.assert_called_once_with(1)
            self.assertEqual(w.h.event.call_args.kwargs['key_sha256'],hashlib.sha256(b'same-key').hexdigest())

    def test_pacing_errors_and_long_wait_do_not_become_success(self):
        for hint,header in [(0,'0'),(250,'0'),(250,'bad'),(1500,'2')]:
            w=self.fixture()
            with self.subTest(hint=hint,header=header),patch('workflow.http',return_value=response(hint=hint,retry_after=header)) as send,patch('workflow.time.sleep') as sleep:
                with self.assertRaises(AssertionError):Workflow.request(w,'POST','/v1/enrol',{})
            send.assert_called_once();sleep.assert_not_called()


if __name__=='__main__':unittest.main()
