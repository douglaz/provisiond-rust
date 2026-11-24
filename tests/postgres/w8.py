"""STO-12/13/55: offline release boundaries and ordinary-capacity saturation."""
import json
from pathlib import Path
import subprocess

from core import binary, environment, offline_migration
from harness import eventually


def child(h, name, program, sql=None, role='administrator'):
    log=(h.root/(name+'.log')).open('w')
    env=dict(h.env,**environment(h,'migrator'))
    if program=='psql':
        command=['psql','-XAt','-v','ON_ERROR_STOP=1','-h','127.0.0.1','-p',str(h.ports[0]),'-U',role,'-d','postgres']
    else:command=[str(h.target/'debug'/program)]
    p=subprocess.Popen(command,env=env,stdin=subprocess.PIPE,stdout=log,stderr=log,text=True)
    p.recorded_command=dict(command=command,exit_status=None)
    h.commands.append(p.recorded_command)
    log.close();h.extra.append(p)
    if sql:p.stdin.write(sql+'\n');p.stdin.flush()
    return p


def history(h):
    return h.sql("SELECT json_agg(json_build_array(version,semantics,completed_at) ORDER BY version) FROM provisiond.schema_migrations")


def saturate(h):
    """STO-55: occupy ordinary slots, then prove a fresh observer is refused."""
    clients=[]
    for index in range(h.config['max_connections']+1):
        p=child(h,'saturation-'+str(index),'psql','SELECT pg_backend_pid();',role='observer')
        clients.append(p)
        log=h.root/('saturation-'+str(index)+'.log')
        eventually(lambda:p.poll() is not None or log.stat().st_size>0)
        if p.poll() is not None:
            assert p.returncode!=0 and ('remaining connection slots' in log.read_text() or 'too many clients' in log.read_text()), 'STO-55 fresh observer refusal must be real capacity exhaustion'
            h.event('w8_fresh_observer_refused',occupied_observers=len(clients)-1,exit_status=p.returncode,ordinary_capacity_unavailable=True)
            return clients
    raise AssertionError('STO-55 ordinary capacity exhaustion never occurred')


def release_capacity(clients):
    for p in clients:
        p.stdin.close()
        p.wait(timeout=5)


def migration_boundaries(h):
    # STO-13: even a no-op runner refuses LOGIN and surviving old sessions.
    before=history(h)
    refused=binary(h,'migrate',role='migrator',ok=False,offline=False)
    assert refused.returncode==1 and 'offline prerequisite refused' in refused.stderr
    assert history(h)==before
    old=child(h,'migration-old-app','psql','SELECT 1;',role='app')
    try:
        eventually(lambda:h.sql("SELECT count(*) FROM pg_stat_activity WHERE usename='app'")=='1')
        h.sql('ALTER ROLE app NOLOGIN')
        refused=binary(h,'migrate',role='migrator',ok=False,offline=False)
        assert refused.returncode==1 and 'remaining backends=1' in refused.stderr
        assert history(h)==before
    finally:
        old.stdin.close();assert old.wait(timeout=5)==0
        h.sql('ALTER ROLE app LOGIN')
    h.event('w8_offline_gate_observed',login_refused=True,surviving_session_refused=True,completed_history_unchanged=True)

    # STO-12: a real held startup key must not obstruct the migration key.
    key=int('50524f5653544152',16)
    holder=child(h,'migration-startup-key','psql',f'BEGIN; SELECT pg_advisory_xact_lock({key});')
    try:
        eventually(lambda:h.sql(f"SELECT count(*) FROM pg_locks WHERE locktype='advisory' AND granted AND classid={key>>32} AND objid={key&0xffffffff}")=='1')
        binary(h,'migrate',role='migrator')
        assert history(h)==before
    finally:
        holder.stdin.write('ROLLBACK;\n');holder.stdin.close();assert holder.wait(timeout=5)==0
    h.event('w8_migration_startup_keys_distinct',startup_key=key,migration_succeeded=True)

    # STO-12/13: interrupt a real blocked DDL and the exact DDL/version gap.
    # The release-1 fixture intentionally retains its original schema/history.
    for boundary in ('ddl','version'):
        h.sql('DROP SCHEMA provisiond CASCADE; CREATE SCHEMA provisiond AUTHORIZATION migrator;')
        h.sql("BEGIN; CREATE TABLE provisiond.schema_migrations(version bigint PRIMARY KEY,semantics bigint NOT NULL,completed_at timestamptz NOT NULL DEFAULT transaction_timestamp());"+
              Path('migrations/0001-core.sql').read_text()+"INSERT INTO provisiond.schema_migrations(version,semantics) VALUES(1,1); COMMIT;",role='migrator')
        before=history(h);holder=gate=ddl=None
        if boundary=='ddl':
            holder=child(h,'migration-ddl-lock','psql','BEGIN; LOCK TABLE provisiond.operations IN ACCESS SHARE MODE;')
            eventually(lambda:h.sql("SELECT count(*) FROM pg_locks WHERE relation='provisiond.operations'::regclass AND mode='AccessShareLock' AND granted")=='1')
            ddl=h.proxy.arm('CREATE TABLE provisiond.enrolments','send')
        else:gate=h.proxy.arm('INSERT INTO provisiond.schema_migrations(version,semantics) VALUES','send')
        try:
            with offline_migration(h):
                p=child(h,'migration-interrupted-'+boundary,'migrate')
                if gate:
                    connection=gate.wait()
                    assert 'INSERT INTO' in connection['query']
                else:
                    # STO-12/13: bind the fault to the actual DDL send, before
                    # its 500 ms lock window starts, rather than a recent socket.
                    connection=ddl.wait();ddl.release()
                    def active_ddl():
                        row=h.sql(f"SELECT row_to_json(a) FROM (SELECT pid,backend_start,state,wait_event_type,wait_event,query,pg_blocking_pids(pid) AS blockers FROM pg_stat_activity WHERE pid={connection['pid']} AND usename='migrator' AND state='active' AND wait_event_type='Lock') a")
                        return json.loads(row) if row else None
                    state=eventually(active_ddl)
                    h.event('w8_owned_active_ddl',**state)
                    # pg_stat_activity truncates long batches; match its full
                    # reported prefix against the exact gated DDL on this PID.
                    assert 'CREATE TABLE provisiond.enrolments' in state['query'] and connection['query'].startswith(state['query'])
                    assert state['blockers'] and state['wait_event']!='SyncRep'
                    assert not connection['closed'] and p.poll() is None
                # End only the owned migration transport, never a SyncRep backend.
                assert h.sql(f"SELECT count(*) FROM pg_stat_activity WHERE pid={connection['pid']} AND wait_event IS DISTINCT FROM 'SyncRep'")=='1'
                h.proxy.close_connection(connection,'w8-interrupted-'+boundary)
                if gate:gate.release()
                if ddl:
                    assert connection['closed'] and connection.get('close_source')=='w8-interrupted-ddl', 'STO-12 planned DDL transport close must occur'
                    assert any(e['event']=='transport_closed' and e.get('session')==connection['pid'] and e.get('source')=='w8-interrupted-ddl' for e in h.events), 'STO-12 planned DDL transport close must be recorded'
                assert p.wait(timeout=12)==1
                if ddl:
                    diagnostic=(h.root/'migration-interrupted-ddl.log').read_text()
                    assert 'connection closed' in diagnostic.lower(), 'STO-12 interrupted DDL must exit on connection failure'
                    assert 'migration_lock_retry' not in diagnostic and '55P03' not in diagnostic and 'lock timeout' not in diagnostic.lower(), 'STO-12 lock timeout/retry exhaustion is not a DDL transport interruption'
                    assert not any(e['event']=='server_error_forwarded' and e.get('session')==connection['pid'] and e.get('sqlstate')=='55P03' for e in h.events)
                    h.event('w8_ddl_transport_failure_observed',session=connection['pid'],close_source=connection['close_source'],exit_status=p.returncode,connection_failure=True,lock_retries=0)
                eventually(lambda:h.sql("SELECT count(*) FROM pg_stat_activity WHERE usename='migrator'")=='0')
                assert history(h)==before, 'STO-12 interrupted migration must retain earlier completed versions'
                assert h.sql("SELECT to_regclass('provisiond.enrolments') IS NULL")=='t', 'STO-12 interrupted DDL must roll back with its unrecorded version'
        finally:
            if ddl:ddl.release()
            if gate:gate.release()
            if holder:
                holder.stdin.write('ROLLBACK;\n');holder.stdin.close();assert holder.wait(timeout=5)==0
        binary(h,'migrate',role='migrator')
        after=json.loads(history(h))
        assert after[0]==json.loads(before)[0] and [r[:2] for r in after]==[[v,v] for v in range(1,6)]
        h.event('w8_migration_interruption_observed',boundary=boundary,prior_completion_unchanged=True,partial_ddl_rolled_back=True,later_invocation_completed=True)

    # STO-13: old numeric schema, incompatible semantics and contract metadata
    # are rejected independently, before the engine lock or API listener.
    saved=h.sql('SELECT row_to_json(v) FROM provisiond.schema_migrations v WHERE version=5')
    h.sql('DELETE FROM provisiond.schema_migrations WHERE version=5')
    for name in ('api','engine'):
        result=binary(h,name,'--check-schema',ok=False)
        assert result.returncode==1 and 'schema range 5..=5' in result.stderr
        assert 'startup_lock_acquired' not in result.stdout and 'api_listening' not in result.stdout
    h.sql("INSERT INTO provisiond.schema_migrations SELECT * FROM json_populate_record(NULL::provisiond.schema_migrations,$json$"+saved+'$json$)')
    for expression in ("stop_order='start before migrate'",'contract_step=false','rollback_floor=4'):
        h.sql('UPDATE provisiond.schema_migrations SET '+expression+' WHERE version=5')
        for name in ('api','engine'):
            result=binary(h,name,'--check-schema',ok=False)
            assert result.returncode==1 and 'contract/rollback declarations' in result.stderr
            assert 'startup_lock_acquired' not in result.stdout and 'api_listening' not in result.stdout
        h.sql('DELETE FROM provisiond.schema_migrations WHERE version=5')
        h.sql("INSERT INTO provisiond.schema_migrations SELECT * FROM json_populate_record(NULL::provisiond.schema_migrations,$json$"+saved+'$json$)')
    h.event('w8_compatibility_observed',old_schema_refused=True,contract_fields_refused=3)
