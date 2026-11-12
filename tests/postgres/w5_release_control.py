"""OPS-49: direct sensitivity of the real failed-attempt release assertion.

Synthetic ownership records, not a runtime resource leak. Each probe is its own
foreground child; the parent recognizes the exact assertion and true exit.
"""
import argparse
import json
from pathlib import Path
import subprocess
import sys
from types import SimpleNamespace
from unittest.mock import patch
import w5

ASSERTION='OPS-49 failed attempt must release its original write checkout before backoff'


def probe(checked_out, helper=w5.failed_attempt_release):
    transaction=17;pid=101
    rollback=dict(event='transaction_rollback_acknowledged',transaction=transaction,session=pid,ready_status=ord('I'))
    release=dict(event='transaction_resources_released',transaction=transaction,session=pid,component_available=1,pool_checked_out=checked_out,discarded=False)
    rows=[rollback,release,dict(event='whole_transaction_retry',transaction=transaction)]
    emitted=[]
    h=SimpleNamespace(events=[dict(event='protocol_response',session=pid,command_tag='ROLLBACK',query=f'ROLLBACK /* store transaction {transaction} */')],event=lambda kind,**data:emitted.append(dict(event=kind,**data)))
    w=SimpleNamespace(h=h)
    with patch.object(w5,'events',return_value=rows):helper(w,SimpleNamespace(),pid,release)
    assert len(emitted)==1 and emitted[0]['release_before_backoff'] is True
    return emitted


def sensitivity(root):
    records=[]
    for count in (0,1,2,3):
        path=root/f'release-checkout-{count}.json'
        command=[sys.executable,'-B','tests/postgres/w5_release_control.py','--checked-out',str(count),'--evidence',str(path)]
        child=subprocess.run(command,check=False,capture_output=True,text=True)
        record=json.loads(path.read_text()) if path.exists() else {}
        valid=(child.returncode==(0 if count==0 else 1) and record.get('result')==('pass' if count==0 else 'fail') and record.get('cleanup')=='pass' and record.get('checked_out')==count and record.get('assertion')==(None if count==0 else ASSERTION))
        records.append(dict(checked_out=count,exit_status=child.returncode,result='pass' if count==0 and valid else 'expected negative demonstration' if valid else 'fail',assertion=record.get('assertion'),application_release=record.get('application_release'),command=command))
    return dict(result='pass' if all(r['result']!='fail' for r in records) else 'fail',method='synthetic records passed to actual failed_attempt_release helper',probes=records)


def main():
    ap=argparse.ArgumentParser();ap.add_argument('--checked-out',type=int,choices=range(4),required=True);ap.add_argument('--evidence',type=Path,required=True)
    args=ap.parse_args()
    record=dict(checked_out=args.checked_out,cleanup='pass',assertion=None,application_release=None)
    try:
        record['application_release']=probe(args.checked_out)
        record['result']='pass';status=0
    except AssertionError as error:
        record.update(result='fail',assertion=str(error));status=1
    args.evidence.write_text(json.dumps(record,indent=2)+'\n')
    return status


if __name__=='__main__':raise SystemExit(main())
