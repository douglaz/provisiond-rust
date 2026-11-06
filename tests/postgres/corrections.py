"""STO-7/STO-55 regressions through actual Store methods, real PostgreSQL."""
import time
from scenarios import prepare, Probe
from harness import eventually


def corrections(h, case=None, defect=None):
    prepare(h)
    if case in (None, 'execute_cancel'):
        p=Probe(h, defect=defect)
        pid=p.command('tx_begin')['value']['session']
        gate=h.proxy.arm("INSERT INTO fixture.destinations VALUES (1802,", 'execute_reply')
        p.send('tx_cancel_execute')
        c=gate.wait()
        assert c['pid']==pid
        assert h.sql(f'SELECT state FROM pg_stat_activity WHERE pid={pid}')=='idle in transaction'
        p.result('tx_cancel_execute')
        # Observe before the independent 1000 ms idle timeout. Transaction retained.
        end=time.monotonic()+.2
        while not c['closed'] and time.monotonic()<end:
            time.sleep(.005)
        h.event('execute_cancellation_observed', session=pid, retained=True,
                transport_closed=c['closed'], source=c.get('close_source'))
        assert c['closed'] and c.get('close_source')=='client_eof', 'cancelled execute must close retained transaction transport'
        p.command('commit')
        assert p.command('await')['value']['outcome']=='rejected'
        assert not any(e['event']=='send_requested' and 'store COMMIT' in e.get('query','') and e.get('session')==pid for e in h.events)
        gate.release()
        assert p.command('write_admit')['value']['session']!=pid
        assert h.sql('SELECT count(*) FROM fixture.destinations WHERE id=1802')=='0'
        p.finish()
        h.results.append(dict(test='execute-cancellation', result='pass'))
    if case in (None, 'checkout'):
        for stage,pattern in [('authentication',''),('admission','store admission target'),('slot',None)]:
            p=Probe(h, defect=defect)
            if stage=='slot':
                p.command('tx_begin')
                gate=None
            else:
                gate=h.proxy.arm(pattern,'reply')
            start=time.monotonic()
            p.send('write_admit')
            c=gate.wait() if gate else None
            result=p.result('write_admit',ok=False)
            elapsed=time.monotonic()-start
            h.event('whole_checkout_observed',stage=stage,elapsed_seconds=elapsed,
                    budget_ms=250,scheduling_tolerance_ms=150,error=result['error'])
            assert elapsed <= .4, 'whole checkout exceeded 250ms plus 150ms scheduling tolerance'
            assert not any(e.get('event')=='send_requested' and e.get('query','').startswith('BEGIN') for e in h.events if c and e.get('session')==c['pid'])
            if c:
                eventually(lambda:c['closed'],seconds=.3)
                assert c.get('close_source')=='client_eof'
                gate.release()
            else:
                p.command('tx_drop')
            p.command('write_admit')
            p.finish()
        h.results.append(dict(test='whole-checkout-deadline',result='pass'))
    if case in (None, 'caller_sql'):
        p=Probe(h,defect=defect)
        for statement in ["SET LOCAL synchronous_commit=off", "SET synchronous_commit=off", "RESET ALL", "RESET statement_timeout", "SELECT set_config('synchronous_commit','off',false)", "SELECT set_config('search_path','public',false)"]:
            result=p.command('setting_attack '+statement)['value']
            h.event('caller_sql_policy_observed',statement=statement,refused=result['refused'])
            assert result['refused'], 'caller SQL must not change protected settings'
        p.command('admit')
        values=p.command('settings')['value']['settings']
        assert 'synchronous_commit=on' in values and 'statement_timeout=1s' in values
        p.finish()
        h.results.append(dict(test='protected-caller-sql',result='pass'))
    if case in (None, 'commit_setting'):
        p=Probe(h,defect=defect)
        outcome=p.command('corrupt_setting')['value']['outcome']
        h.event('precommit_corruption_observed',outcome=outcome,
                durable_rows=h.sql('SELECT count(*) FROM fixture.destinations WHERE id=1803'))
        assert outcome=='rejected', 'lowered synchronous_commit must be rejected before COMMIT'
        assert h.sql('SELECT count(*) FROM fixture.destinations WHERE id=1803')=='0'
        p.command('admit')
        assert 'synchronous_commit=on' in p.command('settings')['value']['settings']
        p.finish()
        h.results.append(dict(test='precommit-setting-verification',result='pass'))
    if case in (None, 'notices'):
        p=Probe(h,defect=defect)
        for _ in range(2):
            result=p.command('notice_flood')['value']
            h.event('notice_retention_observed',**result)
            assert result['retained']<=16, 'notice retention must remain capped at 16'
            assert result['initial']==0, 'notice retention must reset at BEGIN'
            assert result['outcome']=='acknowledged'
        p.finish()
        h.results.append(dict(test='bounded-notices',result='pass'))
    if case in (None, 'notice_sticky'):
        h.sql("CREATE TABLE fixture.warn (value integer); CREATE FUNCTION fixture.warn_commit() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RAISE WARNING 'deferred commit fixture'; RETURN NEW; END $$; CREATE CONSTRAINT TRIGGER warn_at_commit AFTER INSERT ON fixture.warn DEFERRABLE INITIALLY DEFERRED FOR EACH ROW EXECUTE FUNCTION fixture.warn_commit(); GRANT INSERT ON fixture.warn TO app;",role='migrator')
        p=Probe(h,defect=defect)
        result=p.command('notice_warning')['value']
        assert result['retained']==16
        assert any(e.get('event')=='driver_notice' and e.get('severity')=='WARNING' for e in p.events), 'warning fault must activate'
        h.event('commit_warning_after_flood_observed',**result)
        assert result['outcome']=='unresolved', 'COMMIT warning after notice cap must remain unresolved'
        p.finish()
        h.results.append(dict(test='sticky-commit-warning',result='pass'))
    assert h.proxy.cancel_requests==0
