"""STO/OPS/LDG transactional acceptance through Rust and concurrent PostgreSQL.

SQL below prepares isolated fixtures and inspects independent state; the business
mutations under test go through the actual Rust transaction entry and owner.
"""
import json
import subprocess
import time
from pathlib import Path
from scenarios import prepare, Probe
from harness import eventually


def environment(h,role='app',defect=None):
    env=dict(PROVISIOND_PROFILE='local-simulator',
        PROVISIOND_DATABASE_URL=f'host=127.0.0.1 port={h.ports[3]} user={role} password={h.passwords[role]} dbname=postgres sslmode=disable',
        PROVISIOND_STANDBY_NAME='provisiond_sync',PROVISIOND_STANDBY_ROLE='replicator',
        PROVISIOND_STANDBY_ADDRESS='127.0.0.1',PROVISIOND_STARTUP_WAIT_MS='500')
    if defect: env['STORE_TEST_DEFECT']=defect
    return env


def core_probe(h,defect=None,boundary=None):
    env=environment(h)
    if boundary: env["STORE_TEST_BOUNDARY"]=boundary
    return Probe(h,defect=defect,program='core-probe',extra_env=env)


def binary(h,name,*args,role='app',ok=True,defect=None):
    old=h.env
    h.env=dict(old,**environment(h,role,defect))
    try:
        return h.run(['target/debug/'+name,*args],ok=ok,timeout=12)
    finally:
        h.env=old


def charge(sats):
    return dict(sats=sats,native_minor=sats,currency='SIM',rate_num=1,rate_den=1,
        rate_source='simulator-fixed-rational',rate_observed_at='2026-11-06T00:00:00Z',
        haircut_bps=0,rounding_rule_version='ceil-v1')


def request(tenant,key,reserve=60,resource=False,offer='catalogue',unit=10):
    value=dict(tenant=tenant,key=key,reserve=reserve,resource=resource,offer=offer,
        setup=charge(5),snapshot=dict(offer_id=offer,machine_kind='virtual',install_strategies=[],
            recurring_price='1',currency='SIM',recurring_period='hour',declared_cancellation_bound=None))
    if unit:
        value['first_unit']=dict(charge(unit),seconds=3600)
    return value


def command(p,cmd,value,ok=True):
    return p.command(cmd+' '+json.dumps(value,separators=(',',':')),ok=ok)


def deposit(h,tenant,key):
    # A retained destination fixture, not a funding-request API implementation.
    return h.sql(f"INSERT INTO provisiond.deposits(tenant_id,requested_sats,payment_hash,address,derivation_index,idempotency_key,expires_at) VALUES('{tenant}',999,'hash-{key}','address-{key}',(SELECT COALESCE(max(derivation_index),0)+1 FROM provisiond.deposits),'{key}',clock_timestamp()+interval '1 hour') RETURNING id",role='migrator').splitlines()[0]


def fund(h,tenant,key,amount=100,defect=None):
    d=deposit(h,tenant,key)
    value=dict(deposit=d,rail='lightning',reference='hash-'+key,amount=amount)
    # Setup funding is an API replay, never an engine retry or a changed checkout
    # bound. Retain each refusal; only a cold-checkout refusal admits this replay.
    for attempt in range(3):
        p=core_probe(h,defect)
        result=command(p,'payment',value,ok=None)
        p.finish()
        if result['ok']:return d,value,result
        assert 'whole pool checkout deadline' in result['error'],result
        h.event('fixture_funding_checkout_refused',attempt=attempt+1,request_replay=True,checkout_ms=250)
    raise AssertionError('fixture funding never acknowledged after three API requests')


def migrations(h):
    for name in ('api','engine'):
        r=binary(h,name,'--check-schema',ok=False)
        assert r.returncode!=0 and 'schema_compatible' not in r.stdout
    binary(h,'migrate',role='migrator')
    assert h.sql('SELECT version||\'/\'||semantics FROM provisiond.schema_migrations')=='1/1'
    assert h.sql('SELECT count(*) FROM provisiond.startup_generation')=='1'
    binary(h,'migrate',role='migrator')
    # Genuine concurrent fresh runners, each supervised and waited before assertions.
    h.sql('DROP SCHEMA provisiond CASCADE; CREATE SCHEMA provisiond AUTHORIZATION migrator;')
    children=[]
    for n in range(2):
        log=(h.root/f'migrate-concurrent-{n}.log').open('w')
        p=subprocess.Popen(['target/debug/migrate'],env=dict(h.env,**environment(h,'migrator')),stdout=log,stderr=log,text=True)
        log.close();h.extra.append(p);children.append(p)
    statuses=[p.wait(timeout=12) for p in children]
    h.event('migration_concurrent_observed',statuses=statuses,history=h.sql('SELECT count(*) FROM provisiond.schema_migrations'))
    assert statuses==[0,0]
    assert h.sql('SELECT count(*) FROM provisiond.schema_migrations')=='1'
    # Failed DDL/history transaction cannot expose partial schema or completion.
    h.sql('DROP SCHEMA provisiond CASCADE; CREATE SCHEMA provisiond AUTHORIZATION migrator;')
    r=binary(h,'migrate',role='migrator',defect='migration_failure',ok=False)
    assert r.returncode==1
    assert h.sql("SELECT to_regclass('provisiond.operations') IS NULL AND to_regclass('provisiond.schema_migrations') IS NULL")=='t'
    h.event('migration_failure_atomicity_observed',exit_status=r.returncode,ddl_visible=False,history_visible=False)
    # COMMIT actually lands before its reply is lost. Next invocation inspects it.
    gate=h.proxy.arm('store COMMIT','commit_reply')
    log=(h.root/'migration-uncertain.log').open('w')
    p=subprocess.Popen(['target/debug/migrate'],env=dict(h.env,**environment(h,'migrator')),stdout=log,stderr=log,text=True)
    log.close();h.extra.append(p)
    c=gate.wait()
    assert h.sql('SELECT count(*) FROM provisiond.schema_migrations')=='1'
    h.proxy.close_connection(c,'lost_migration_reply')
    gate.release()
    assert p.wait(timeout=12)==1
    h.event('migration_uncertain_observed',durable_version=1,exit_status=1)
    binary(h,'migrate',role='migrator')
    for version,semantics in [(0,1),(2,1),(1,2)]:
        h.sql(f'UPDATE provisiond.schema_migrations SET version={version},semantics={semantics}')
        for name in ('api','engine'):
            r=binary(h,name,'--check-schema',ok=False)
            assert r.returncode==1 and 'unsupported schema/semantic' in r.stderr
            assert 'startup_lock_acquired' not in r.stdout and 'generation_advanced' not in r.stdout
    h.sql('UPDATE provisiond.schema_migrations SET version=1,semantics=1')
    for name in ('api','engine'):
        r=binary(h,name,'--check-schema')
        assert 'schema_compatible' in r.stdout
    r=binary(h,'api',ok=False)
    assert r.returncode==78 and 'schema supported; HTTP service not implemented' in r.stderr
    # Initial acquisition failure is one attempt with returned layer reason.
    h.proxy.refuse_after=len(h.proxy.connections)
    before=len(h.proxy.connections)
    r=binary(h,'migrate',role='migrator',ok=False)
    assert r.returncode==1 and 'initial acquisition failed' in r.stderr
    assert len(h.proxy.connections)==before+1
    h.proxy.refuse_after=None
    h.event('migration_connection_failure_observed',stage='initial',exit_status=r.returncode,reason=r.stderr)
    # Lock-timeout retry reacquires once; refusal of that reconnect must carry
    # its own stage and returned reason, with no further connection attempt.
    log=(h.root/'migration-lock-holder.log').open('w')
    holder=subprocess.Popen(['psql','-XAt','-v','ON_ERROR_STOP=1','-h','127.0.0.1','-p',str(h.ports[0]),'-U','administrator','-d','postgres'],stdin=subprocess.PIPE,stdout=log,stderr=log,text=True,env=h.env)
    log.close();h.extra.append(holder)
    holder.stdin.write('BEGIN; SELECT pg_advisory_xact_lock(5787775703206938450);\n');holder.stdin.flush()
    eventually(lambda:h.sql("SELECT count(*) FROM pg_locks WHERE locktype='advisory' AND granted")=='1')
    h.proxy.refuse_after=len(h.proxy.connections)+1
    before=len(h.proxy.connections)
    r=binary(h,'migrate',role='migrator',ok=False)
    holder.stdin.close();assert holder.wait(timeout=5)==0
    h.proxy.refuse_after=None
    h.event('migration_connection_failure_observed',stage='reconnect',exit_status=r.returncode,reason=r.stderr,connections=len(h.proxy.connections)-before)
    assert r.returncode==1 and 'reconnect acquisition failed' in r.stderr
    assert len(h.proxy.connections)==before+2
    h.results.append(dict(test='REL-02',result='pass',arms='real binary compatibility; fresh/rerun/concurrent/failed/uncertain migrations'))


def money(h,defect=None):
    # No live tenants row: LDG-35's primitive and retained payments must still work.
    d=deposit(h,'departed-money','money')
    value=dict(deposit=d,rail='lightning',reference='hash-money',amount=100)
    if defect=='payment_atomic':
        a=core_probe(h,defect)
        command(a,'payment',value,ok=False)
        a.finish()
        counts=h.sql("SELECT (SELECT count(*) FROM provisiond.payments WHERE payment_ref='hash-money')||':'||(SELECT count(*) FROM provisiond.ledger_entries WHERE tenant_id='departed-money')")
        h.event('payment_split_intermediate_observed',counts=counts,durable=True)
        assert counts=='1:1', 'payment and credit must be atomic'
        raise AssertionError('payment split fault failed to activate')
    a=core_probe(h,defect);b=core_probe(h)
    a.send('payment '+json.dumps(value));b.send('payment '+json.dumps(value))
    ra=a.result('payment',ok=None);rb=b.result('payment',ok=None)
    a.finish();b.finish()
    counts=h.sql("SELECT (SELECT count(*) FROM provisiond.payments WHERE payment_ref='hash-money')||':'||(SELECT count(*) FROM provisiond.ledger_entries WHERE tenant_id='departed-money')")
    h.event('payment_atomicity_observed',counts=counts,first_request_ok=ra['ok'],second_request_ok=rb['ok'])
    assert counts=='1:1', 'payment and credit must be atomic'
    assert ra['ok'] and rb['ok'], 'duplicate settlement must be a successful replay'
    p=core_probe(h)
    command(p,'payment',value)
    value.update(rail='onchain',reference='simulated-tx:0',amount=7)
    command(p,'payment',value)
    assert h.sql("SELECT string_agg(seq||':'||amount_sats||':'||balance_after,',' ORDER BY seq) FROM provisiond.ledger_entries WHERE tenant_id='departed-money'")=='1:100:100,2:7:107'
    p.command('audit departed-money')
    p.finish()
    fund(h,'admission-tenant','admission')
    # Actual concurrent Rust clients authorize against one available balance.
    a=core_probe(h);b=core_probe(h)
    a.send('admit '+json.dumps(request('admission-tenant','a')))
    b.send('admit '+json.dumps(request('admission-tenant','b')))
    results=[a.result('admit',ok=None),b.result('admit',ok=None)]
    a.finish();b.finish()
    assert sum(r['ok'] for r in results)==1
    assert h.sql("SELECT count(*)||':'||sum(reserved_sats) FROM provisiond.commitments WHERE tenant_id='admission-tenant'")=='1:60'
    assert h.sql("SELECT count(*) FROM provisiond.operations WHERE tenant_id='admission-tenant'")=='1'
    assert h.sql("SELECT count(*) FROM provisiond.idempotency_records WHERE scope_id='admission-tenant'")=='1'
    h.event('concurrent_admission_observed',admitted=1,refused=1,reserved=60,balance=100,live_tenant=False)
    # Cross-tenant shared resource exclusion, catalogue does not acquire one.
    fund(h,'resource-a','resource-a');fund(h,'resource-b','resource-b')
    a=core_probe(h);b=core_probe(h)
    command(a,'admit',request('resource-a','resource-a',resource=True,offer='one-resource'))
    command(b,'admit',request('resource-b','resource-b',resource=True,offer='one-resource'),ok=False)
    command(b,'admit',request('resource-b','catalogue-b',resource=False))
    assert h.sql("SELECT count(*) FROM provisiond.resource_exclusions WHERE offer_id='one-resource'")=='1'
    a.finish();b.finish()
    a=core_probe(h);b=core_probe(h)
    command(a,'pair_hold',['departed-z','departed-a'])
    b.send('pair_hold '+json.dumps(['departed-a','departed-z']))
    eventually(lambda:h.sql("SELECT count(*) FROM pg_stat_activity WHERE wait_event_type='Lock' AND query LIKE '%LDG-35%'")=='1',seconds=1)
    a.command('commit');a.command('await')
    b.result('pair_hold');b.command('rollback')
    a.finish();b.finish()
    h.event('tenant_pair_serialization_observed',input_orders=['z,a','a,z'],acquisition_order='a,z',live_tenant_rows=False)
    h.results.extend([dict(test='MONEY-01',result='pass'),dict(test='MONEY-03',result='pass'),dict(test='resource-offer-exclusion',result='pass')])


def core_scenarios(h,case=None,defect=None):
    prepare(h)
    if case in (None,'migrations'):
        migrations(h)
        schema_constraints(h)
    else: binary(h,'migrate',role='migrator')
    import ownership
    for name in ('claim_cancel','licensing_stop','outcome_stop','replacement_outcome','deadline','lock_observers','terminal_zero','terminal_capacity','loss_stop','queued_claim','displaced_record','surface','predispatch','startup'):
        if case in (None,name): getattr(ownership,name)(h,defect)
    if case=='queued_claim_pool': ownership.queued_claim(h,defect,only='pool')
    if case in (None,'money'): money(h,defect)
    if case in (None,'claims'):
        claims(h,defect)
        settlement_loss(h)
        claim_loss(h)
    if case in (None,'generation'):
        generation(h,defect)
        licensed_uncertainty(h)
        advance_warning(h)
        startup_contracts(h)
    if case in (None,'external'): external(h)
    if case in (None,'stops'):
        stops(h)
        retry_exhaustion(h)
    if case in (None,'money'): audit_fault(h)
    if case is None:
        h.results.extend([dict(test='ENG-05',result='not run',reason='automatic provider reconciliation belongs to later work'),dict(test='ENG-07',result='not run',reason='dedicated replication-freeze stop-record drill belongs to later work'),dict(test='DB-10',result='not run',reason='W8 deployment-wide saturation')])


def empty_queue(h):
    # Fixture isolation between scenarios without deleting ledger/deposit history.
    h.sql("UPDATE provisiond.operations SET status='failed',request=NULL WHERE status IN ('queued','running')")


def enqueue(h,tenant,key,**kwargs):
    fund(h,tenant,key)
    p=core_probe(h)
    op=command(p,'admit',request(tenant,key,**kwargs))['value']['id']
    p.finish()
    return op


def claims(h,defect=None):
    empty_queue(h)
    op=enqueue(h,'claims','claims')
    p=core_probe(h,defect)
    p.command('engine_start')
    a=p.command('claim')['value'];assert a['id']==op and a['number']==1
    assert command(p,'defer',a)['value']['affected']
    time.sleep(.06)
    b=p.command('claim')['value'];assert b['id']==op and b['number']==2
    stale=command(p,'defer',a)['value']['affected']
    h.event('stale_claim_guard_observed',old_number=1,new_number=2,affected=stale)
    assert not stale, 'stale claim must affect zero rows'
    assert not command(p,'settle',a)['value']['affected']
    assert h.sql(f"SELECT status||':'||claim_number FROM provisiond.operations WHERE id='{op}'")=='running:2'
    b['external_id']='verified-simulator-claims'
    assert command(p,'verified',b)['value']['affected']
    before=h.sql(f"SELECT revision||':'||updated_at FROM provisiond.operations WHERE id='{op}'")
    assert command(p,'verified',b)['value']['affected']
    assert h.sql(f"SELECT revision||':'||updated_at FROM provisiond.operations WHERE id='{op}'")==before
    # Same target/columns from older execution cannot pass the repeat branch.
    a['external_id']=b['external_id']
    assert not command(p,'verified',a)['value']['affected']
    machine=h.sql(f"SELECT machine_id FROM provisiond.operations WHERE id='{op}'")
    assert h.sql(f"SELECT count(*)||':'||sum(-amount_sats) FROM provisiond.ledger_entries WHERE operation_id='{op}'")=='2:15'
    assert h.sql(f"SELECT reserved_sats||':'||version FROM provisiond.commitments WHERE operation_id='{op}'")=='45:2'
    assert h.sql(f"SELECT first_unit_debit_id IS NOT NULL FROM provisiond.machines WHERE id='{machine}'")=='t'
    p.finish()
    h.results.append(dict(test='ENG-04',result='pass',arms='both claim guard branches, defer, exact multirow repeat'))
    # Per-machine uniqueness: two queued operations, one running winner. The losing
    # atomic claim rolls back its number and short-defers instead of failing it.
    h.sql(f"INSERT INTO provisiond.operations(tenant_id,idempotency_key,kind,status,machine_id,request_summary,correlation_id) VALUES('claims','power-1','power','queued','{machine}','{{\"kind\":\"power\"}}','power-1'),('claims','power-2','power','queued','{machine}','{{\"kind\":\"power\"}}','power-2')",role='migrator')
    h.sql("CREATE FUNCTION fixture.pause_machine_claim() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN IF OLD.kind='power' AND OLD.status='queued' AND NEW.status='running' THEN PERFORM pg_sleep(0.1); END IF; RETURN NEW; END $$; CREATE TRIGGER pause_claim BEFORE UPDATE OF status ON provisiond.operations FOR EACH ROW EXECUTE FUNCTION fixture.pause_machine_claim()",role='migrator')
    p=core_probe(h);p.command('engine_start')
    p.send('claim_race')
    eventually(lambda:h.sql("SELECT count(*) FROM pg_stat_activity WHERE usename='app' AND wait_event='PgSleep' AND query LIKE 'UPDATE provisiond.operations SET status=%'")=='2',seconds=1)
    h.event('concurrent_claims_observed',active_postgresql_claim_transactions=2,driver='actual Rust Engine::claim',pause_ms=100)
    raced=p.result('claim_race')['value']['claims']
    winners=[c for c in raced if c is not None]
    assert len(winners)==1 and sum(c is None for c in raced)==1
    holder=winners[0];assert holder['number']==1
    h.sql('DROP TRIGGER pause_claim ON provisiond.operations; DROP FUNCTION fixture.pause_machine_claim()',role='migrator')
    p.command('execute_pending '+holder['id'],ok=False)
    assert not any(e.get('event')=='simulator_accepted' and e.get('operation')==holder['id'] for e in p.events)
    assert p.command('claim')['value'] is None
    assert h.sql(f"SELECT count(*) FROM provisiond.operations WHERE machine_id='{machine}' AND status='queued' AND claim_number=0")=='1'
    h.sql(f"UPDATE provisiond.operations SET yielded_at=clock_timestamp() WHERE id='{holder['id']}'")
    time.sleep(.06)
    other=p.command('claim')['value'];assert other and other['id']!=holder['id']
    assert not command(p,'reacquire',holder)['value']['affected']
    assert h.sql(f"SELECT status||':'||claim_number FROM provisiond.operations WHERE id='{holder['id']}'")=='queued:1'
    assert h.sql(f"SELECT status FROM provisiond.operations WHERE id='{other['id']}'")=='running'
    p.finish()
    h.results.append(dict(test='atomic-claim-machine-index',result='pass',arms='two overlapping Rust/PostgreSQL claims and refused yielded reacquire'))
    # STO-55: reserve a worker before awaiting its claim transaction, including
    # calls still waiting for a checkout. Completed claims retain that capacity.
    empty_queue(h)
    for n in range(4):
        enqueue(h,f'worker-slot-{n}',f'worker-slot-{n}')
    p=core_probe(h);p.command('engine_start')
    raced=p.command('claim_slots')['value']['claims']
    admitted=[c for c in raced if c and 'id' in c]
    refused=[c for c in raced if c and 'error' in c]
    h.event('worker_capacity_observed',concurrent_calls=4,configured_workers=3,admitted=len(admitted),refused=len(refused),results=raced)
    assert len(admitted)==3 and len(refused)==1, 'four concurrent claims must reserve only three configured workers'
    assert 'all configured workers occupied' in refused[0]['error']
    assert h.sql("SELECT count(*) FROM provisiond.operations WHERE tenant_id LIKE 'worker-slot-%' AND status='running'")=='3'
    assert h.sql("SELECT count(*) FROM provisiond.operations WHERE tenant_id LIKE 'worker-slot-%' AND status='queued' AND claim_number=0")=='1'
    again=p.command('claim',ok=False)
    assert 'all configured workers occupied' in again['error']
    p.finish()
    h.results.append(dict(test='worker-capacity-concurrent-claims',result='pass',arms='three retained permits, fourth refused before transaction'))


def settlement_loss(h):
    empty_queue(h)
    op=enqueue(h,'settlement-loss','settlement-loss',resource=True,offer='settlement-resource')
    p=core_probe(h);p.command('engine_start')
    c=p.command('claim')['value'];assert c['id']==op
    assert command(p,'license',c)['value']['outcome']=='acknowledged'
    p.command('external provider')
    c['external_id']='simulator-landed-once'
    gate=h.proxy.arm('store COMMIT','commit_reply')
    p.send('verified '+json.dumps(c))
    connection=gate.wait()
    assert h.sql(f"SELECT status FROM provisiond.operations WHERE id='{op}'")=='succeeded'
    before=h.sql(f"SELECT revision||':'||updated_at FROM provisiond.operations WHERE id='{op}'")
    h.event('settlement_reply_loss_activated',operation=op,durable_status='succeeded',provider='simulator')
    h.proxy.close_connection(connection,'lost_settlement_reply')
    gate.release()
    assert p.result('verified')['value']['affected']
    assert h.sql(f"SELECT revision||':'||updated_at FROM provisiond.operations WHERE id='{op}'")==before
    assert h.sql(f"SELECT count(*)||':'||sum(-amount_sats) FROM provisiond.ledger_entries WHERE operation_id='{op}'")=='2:15'
    assert h.sql(f"SELECT reserved_sats||':'||version FROM provisiond.commitments WHERE operation_id='{op}'")=='45:2'
    assert h.sql(f"SELECT count(*) FROM provisiond.resource_exclusions WHERE operation_id='{op}'")=='0'
    assert h.sql("SELECT count(*) FROM provisiond.machines WHERE external_id='simulator-landed-once'")=='1'
    assert len([e for e in p.events if e.get('event')=='simulator_effect'])==1
    p.finish()
    h.results.append(dict(test='ENG-06',result='pass',arms='whole verified-create write set repeated; no external repeat'))


def claim_loss(h):
    empty_queue(h)
    op=enqueue(h,'claim-loss','claim-loss')
    p=core_probe(h);p.command('engine_start')
    gate=h.proxy.arm('store COMMIT','commit_reply')
    p.send('claim');c=gate.wait()
    assert h.sql(f"SELECT status||':'||claim_number FROM provisiond.operations WHERE id='{op}'")=='running:1'
    h.event('claim_reply_loss_activated',operation=op,durable_claim_number=1)
    h.proxy.close_connection(c,'lost_claim_reply');gate.release()
    p.result('claim',ok=False)
    p.expected_status=71;p.finish()
    assert not any(e.get('event')=='simulator_effect' for e in p.events)
    n=core_probe(h);n.command('engine_start')
    assert h.sql(f"SELECT status FROM provisiond.operations WHERE id='{op}'")=='failed'
    assert h.sql(f"SELECT reserved_sats||':'||state FROM provisiond.commitments WHERE operation_id='{op}'")=='0:closed'
    n.finish()
    h.results.append(dict(test='ENG-03',result='pass',arms='claim reply lost after durable claim; fatal exit; restart recovery'))
    # Generation advance reply loss is fatal before startup sweep/claims.
    p=core_probe(h)
    gate=h.proxy.arm('store COMMIT','commit_reply')
    p.send('engine_start');c=gate.wait()
    h.event('advance_reply_loss_activated',durable_generation=h.sql('SELECT generation FROM provisiond.startup_generation'))
    h.proxy.close_connection(c,'lost_advance_reply');gate.release()
    p.result('engine_start',ok=False);p.expected_status=71;p.finish()
    assert not any(e.get('event')=='restart_pass_complete' for e in p.events)
    h.results.append(dict(test='generation-advance-reply-loss',result='pass'))


def generation(h,defect=None):
    empty_queue(h)
    # Each exposed engine entry invokes the shared generation-first path. Suppress
    # only old lock-session death delivery so its generation check must refuse it.
    paths=['begin'] if defect else ['begin','admit','payment','claim','defer','settle','verified','license','record_stop']
    for index,path in enumerate(paths):
        active=enqueue(h,'stale-record','stale-record') if path=='record_stop' else None
        p=core_probe(h,defect);old=p.command('engine_start')['value']
        if active: assert p.command('claim')['value']['id']==active
        new=dict(generation=h.sql('UPDATE provisiond.startup_generation SET generation=gen_random_uuid() RETURNING generation').splitlines()[0])
        assert old['generation']!=new['generation']
        marker=len(h.events)
        c=dict(id='00000000-0000-0000-0000-000000000001',tenant='stale',kind='create',number=1)
        if path=='begin': r=p.command('begin',ok=None)
        elif path=='admit':r=command(p,'admit',request('stale','stale'),ok=None)
        elif path=='payment':r=command(p,'payment',dict(deposit=c['id'],rail='lightning',reference='stale-payment',amount=1),ok=None)
        elif path=='claim':r=p.command('claim',ok=None)
        elif path in ('defer','settle','verified','license'):
            c['external_id']='stale-external';r=command(p,path,c,ok=None)
        else:
            p.command('record_stop '+active)
            r=p.command('wait_records',ok=None)
        h.event('stale_generation_observed',path=path,refused=not r['ok'],old_generation=old['generation'],new_generation=new['generation'])
        assert not r['ok'], 'stale engine generation must refuse transaction'
        p.expected_status=71;p.finish()
        assert any(e.get('event')=='engine_superseded' for e in p.events)
        assert not any(e.get('event')=='whole_transaction_retry' for e in p.events)
        after=[e for e in h.events[marker:] if e.get('event')=='client_observation' and e.get('probe')==p.index]
        assert any(e['message'].get('event')=='fatal_process_exit' for e in after)
    # CNF-368/369: separate admitted Rust session outside the pool, with test-only
    # disabled statement/idle timeouts, holds a share lock and an uncommitted claim.
    for bound in (500,80):
        empty_queue(h);op=enqueue(h,'share-'+str(bound),'share-'+str(bound))
        old=h.sql('SELECT generation FROM provisiond.startup_generation')
        p=core_probe(h);held=p.command('generation_holder')['value'];assert held['id']==op
        n=core_probe(h);n.send('engine_start '+str(bound))
        if bound==500:
            eventually(lambda:h.sql("SELECT count(*) FROM pg_stat_activity WHERE usename='app' AND wait_event_type='Lock' AND query LIKE '%OPS-15 advance%'")=='1',seconds=1)
            assert h.sql('SELECT generation FROM provisiond.startup_generation')==old
            p.command('commit');assert p.command('await')['value']['outcome']=='acknowledged'
            n.result('engine_start')
            assert h.sql(f"SELECT status FROM provisiond.operations WHERE id='{op}'")=='failed'
            h.event('generation_share_wait_observed',blocked_before_release=True,advanced_after_release=True,claimed_row_swept=True)
            n.finish()
        else:
            r=n.result('engine_start',ok=False);n.expected_status=71;n.finish()
            assert h.sql('SELECT generation FROM provisiond.startup_generation')==old
            assert not any(e.get('event')=='generation_advanced' for e in n.events)
            p.command('commit');assert p.command('await')['value']['outcome']=='acknowledged'
            h.event('generation_share_timeout_observed',wait_ms=80,generation_unchanged=True,holder_commit_acknowledged=True,error=r['error'])
        p.finish()
    # Numeric validation is exercised by real startup entry attempts, before any
    # session is acquired; the minimum also receives actual server SET replies.
    for value in ('0','-1','NaN','inf','0.4','0.6','1.5','2147483648'):
        p=core_probe(h);before=len(h.proxy.connections)
        p.command('engine_start '+value,ok=False);p.expected_status=71;p.finish()
        assert len(h.proxy.connections)==before
    p=core_probe(h);minimum=p.command('engine_start 1',ok=None)
    applied=[e for e in p.events if e.get('event')=='advance_timeout_settings_applied']
    assert applied and applied[-1]['statement_ms']==applied[-1]['lock_ms']==1
    if not minimum['ok']:p.expected_status=71
    p.finish()
    # UUID choices stay fresh even after reinstalling an earlier value in this
    # isolated history fixture. This does not run a product recovery incident.
    values=[]
    for _ in range(3):
        p=core_probe(h);values.append(p.command('engine_start')['value']['generation']);p.finish()
    h.sql(f"UPDATE provisiond.startup_generation SET generation='{values[0]}'")
    p=core_probe(h);values.append(p.command('engine_start')['value']['generation']);p.finish()
    assert len(set(values))==4
    h.event('fresh_generation_choices_observed',values=values,isolated_restored_value_fixture=True)
    h.results.append(dict(test='ENG-01',result='pass',arms='generation entry paths; held share; wait timeout; fresh UUID after restored value'))


def external(h):
    p=core_probe(h);p.command('engine_start')
    p.command('begin')
    p.command('begin',ok=False)
    for interface in ('provider','rail','rescue'):
        p.command('external '+interface,ok=False)
    gate=h.proxy.arm('store COMMIT','commit_reply')
    marker=len(p.events)
    p.command('commit');c=gate.wait()
    p.command('drop_receipt')
    for interface in ('provider','rail','rescue'):
        p.command('external '+interface,ok=False)
    p.command('begin',ok=False)
    assert not any(e.get('event')=='simulator_effect' for e in p.events)
    gate.release()
    eventually(lambda:any(e.get('event')=='commit_owner_completed' and e.get('session')==c['pid'] for e in p.events[marker:]))
    for interface in ('provider','rail','rescue'):p.command('external '+interface)
    assert len([e for e in p.events if e.get('event')=='simulator_effect'])==3
    p.finish()
    h.results.append(dict(test='ENG-08',result='pass',arms='provider/rail/rescue and original sent-COMMIT component ownership'))


def stops(h):
    empty_queue(h)
    ids=[enqueue(h,'stop-'+str(n),'stop-'+str(n)) for n in range(3)]
    p=core_probe(h);startup=p.command('engine_start')['value']
    claims=[p.command('claim')['value'] for _ in range(3)]
    assert set(c['id'] for c in claims)==set(ids)
    p.command('execute_pending '+claims[0]['id'])
    eventually(lambda:any(e.get('event')=='simulator_accepted' for e in p.events))
    p.command('execute_return '+claims[1]['id'])
    eventually(lambda:any(e.get('event')=='execute_returned' and e.get('operation')==claims[1]['id'] for e in p.events))
    # Hold the two tenants' actual advisory primitives. The after-return
    # settlement and the earlier cut-off record both see real lock_timeout errors
    # and repeat whole transactions before lock loss; release only after detection.
    holder=tenant_holder(h,[claims[1]['tenant'],claims[2]['tenant']],'stop-records')
    p.command('start_settlement '+claims[1]['id'])
    p.command('record_stop '+claims[2]['id'])
    eventually(lambda:len([e for e in p.events if e.get('event')=='whole_transaction_retry'])>=2,seconds=1.5)
    started=next(e for e in p.events if e.get('event')=='stop_record_started' and e['operation']==claims[2]['id'])
    marker=len(p.events)
    h.sql(f"SELECT pg_terminate_backend({startup['lock_pid']})")
    eventually(lambda:any(e.get('event')=='driver_dead' for e in p.events[marker:]),seconds=.2)
    p.send('poll_lock')
    eventually(lambda:any(e.get('event')=='startup_lock_lost' for e in p.events))
    holder.stdin.close();assert holder.wait(timeout=5)==0
    h.event('fault_released',fault='tenant_lock_contention',label='stop-records')
    p.result('poll_lock',ok=False);p.expected_status=71;p.finish()
    states={row.split('|')[0]:row.split('|')[1:] for row in h.sql("SELECT id,status,error->>'kind',error->'details'->>'reason' FROM provisiond.operations WHERE id IN ("+','.join("'"+i+"'" for i in ids)+")").splitlines()}
    assert states[claims[0]['id']][:2]==['needs_reconciliation','conflict']
    assert states[claims[1]['id']][:2]==['succeeded','']
    assert h.sql(f"SELECT count(*)||':'||sum(-amount_sats) FROM provisiond.ledger_entries WHERE operation_id='{claims[1]['id']}'")=='2:15'
    assert states[claims[2]['id']][:2]==['failed','conflict']
    starts=[e for e in p.events if e.get('event')=='stop_record_started']
    assert len(starts)==3 and sum(e['operation']==claims[2]['id'] for e in starts)==1
    assert started['bound_ms']==1000
    h.event('lock_loss_records_observed',states=states,records=3,original_pending_bound_ms=1000,exit_status=71)
    h.results.append(dict(test='ENG-02',result='pass',arms='active execute cancellation; after-return outcome; pending record retains bound'))
    # Record timeout and later stop are exits. Neither invents an acknowledgment.
    for mode in ('expiry','later'):
        empty_queue(h);op=enqueue(h,'stop-'+mode,'stop-'+mode)
        p=core_probe(h);p.command('engine_start');p.command('claim')
        gate=h.proxy.arm('OPS-50 first statement','reply')
        p.command('record_stop '+op);gate.wait()
        start=time.monotonic()
        if mode=='expiry':
            p.send('wait_records');assert p.p.wait(timeout=1.5)==71
        else:p.command('later_stop',ok=False)
        p.expected_status=71;p.finish();elapsed=time.monotonic()-start
        if mode=='expiry': assert .8<=elapsed<1.5
        gate.release()
        h.event('stop_record_exit_observed',mode=mode,bound_ms=1000,elapsed_seconds=elapsed,exit_status=71,record_not_observed_landed=True)
    h.results.append(dict(test='stop-record-exits',result='pass',arms='bound expiry and later stop; replication-freeze arm deferred'))


def audit_fault(h):
    p=core_probe(h)
    # Append a corrupted balance fixture without disabling append-only protection.
    h.sql("INSERT INTO provisiond.ledger_entries(tenant_id,seq,kind,amount_sats,balance_after,idempotency_key) VALUES('corrupt-audit',1,'operation_fee_debit',-1,3,'corrupt-fixture')",role='migrator')
    p.command('audit corrupt-audit',ok=False)
    assert h.sql('SELECT ledger_valid FROM provisiond.spending_health')=='f'
    command(p,'admit',request('departed-money','audit-must-refuse'),ok=False)
    p.finish()
    h.results.append(dict(test='ledger-audit-fails-closed',result='pass'))


def licensed_uncertainty(h):
    empty_queue(h);op=enqueue(h,'licensed','licensed',resource=True,offer='licensed-resource')
    p=core_probe(h);p.command('engine_start');c=p.command('claim')['value'];assert c['id']==op
    command(p,'license',c)
    # A generation read licensed this simulator call before the later advance.
    # No new transaction is needed to dispatch what was already licensed.
    advanced=h.sql('UPDATE provisiond.startup_generation SET generation=gen_random_uuid() RETURNING generation').splitlines()[0]
    p.command('external provider')
    assert len([e for e in p.events if e.get('event')=='simulator_effect'])==1
    command(p,'verified',dict(c,external_id='licensed-simulator-resource'),ok=False)
    p.expected_status=71;p.finish()
    assert h.sql(f"SELECT status FROM provisiond.operations WHERE id='{op}'")=='running'
    n=core_probe(h);n.command('engine_start')
    assert h.sql(f"SELECT status FROM provisiond.operations WHERE id='{op}'")=='needs_reconciliation'
    assert h.sql(f"SELECT count(*) FROM provisiond.resource_exclusions WHERE operation_id='{op}'")=='1'
    n.finish()
    h.event('already_licensed_simulator_uncertainty_observed',operation=op,later_generation=advanced,simulated_calls=1,repeated_purchase=False,unresolved_exclusion_retained=True)
    h.results.append(dict(test='licensed-call-uncertainty',result='pass',arms='does not claim automatic reconciliation/ENG-05'))


def advance_warning(h):
    empty_queue(h)
    h.sql("CREATE FUNCTION provisiond.warn_generation_commit() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RAISE WARNING 'generation COMMIT warning fixture'; RETURN NEW; END $$; CREATE CONSTRAINT TRIGGER warn_generation AFTER UPDATE ON provisiond.startup_generation DEFERRABLE INITIALLY DEFERRED FOR EACH ROW EXECUTE FUNCTION provisiond.warn_generation_commit()",role='migrator')
    p=core_probe(h);p.command('engine_start',ok=False);p.expected_status=71;p.finish()
    assert any(e.get('event')=='driver_notice' and e.get('severity')=='WARNING' for e in p.events)
    assert not any(e.get('event') in ('generation_advanced','restart_pass_complete') for e in p.events)
    h.sql('DROP TRIGGER warn_generation ON provisiond.startup_generation; DROP FUNCTION provisiond.warn_generation_commit()',role='migrator')
    h.results.append(dict(test='generation-advance-warning-fatal',result='pass'))


def tenant_holder(h,tenants,label):
    log=(h.root/('tenant-holder-'+label+'.log')).open('w')
    p=subprocess.Popen(['psql','-XAt','-v','ON_ERROR_STOP=1','-h','127.0.0.1','-p',str(h.ports[0]),'-U','administrator','-d','postgres'],stdin=subprocess.PIPE,stdout=log,stderr=log,text=True,env=h.env)
    log.close();h.extra.append(p)
    sql='BEGIN;'+''.join(" SELECT pg_advisory_xact_lock(1347572814,hashtext('"+t+"'));" for t in sorted(tenants))+'\n'
    p.stdin.write(sql);p.stdin.flush()
    eventually(lambda:h.sql("SELECT count(*) FROM pg_locks WHERE locktype='advisory' AND classid=1347572814 AND granted")==str(len(tenants)))
    h.event('fault_activated',fault='tenant_lock_contention',label=label,tenants=tenants)
    return p


def retry_exhaustion(h):
    empty_queue(h);op=enqueue(h,'retry-bound','retry-bound')
    p=core_probe(h);p.command('engine_start');c=p.command('claim')['value']
    assert c['id']==op
    holder=tenant_holder(h,[c['tenant']],'retry-exhaustion')
    start=time.monotonic()
    command(p,'settle',c,ok=False);p.expected_status=71;p.finish()
    elapsed=time.monotonic()-start
    assert elapsed>=2 and elapsed<3.5
    assert any(e.get('event')=='whole_transaction_retry' for e in p.events)
    holder.stdin.close();assert holder.wait(timeout=5)==0
    assert h.sql(f"SELECT status FROM provisiond.operations WHERE id='{op}'")=='running'
    assert h.sql(f"SELECT count(*) FROM provisiond.ledger_entries WHERE operation_id='{op}'")=='0'
    h.event('store_retry_bound_observed',store_retry_ms=2000,elapsed_seconds=elapsed,exit_status=71,external_repeated=False)
    h.results.append(dict(test='store-retry-lock-timeout-exhaustion',result='pass'))


def startup_contracts(h):
    env=environment(h);env.pop('PROVISIOND_STARTUP_WAIT_MS')
    r=subprocess.run(['target/debug/engine'],env=dict(h.env,**env),capture_output=True,text=True,timeout=8)
    assert r.returncode==1 and 'generation_advanced' not in r.stdout
    h.commands.append(dict(command=['target/debug/engine'],configuration='missing startup wait bound',exit_status=r.returncode))
    r=subprocess.run(['target/debug/engine'],env=dict(h.env,**environment(h),PROVISIOND_WORKERS='25'),capture_output=True,text=True,timeout=8)
    assert r.returncode==1 and 'engine pools plus lock exceed ordinary PostgreSQL capacity' in r.stderr
    assert 'startup_lock_acquired' not in r.stdout
    h.commands.append(dict(command=['target/debug/engine'],configuration='oversized engine pool',exit_status=r.returncode))
    r=h.run(['psql','-XAt','-v','ON_ERROR_STOP=1','-h','127.0.0.1','-p',str(h.ports[0]),'-U','app','-d','postgres'],sql='BEGIN READ ONLY; SELECT generation FROM provisiond.startup_generation FOR SHARE;',ok=False)
    assert r.returncode!=0 and 'read-only transaction' in r.stderr
    h.event('provider_licensing_mode_observed',read_only_share_refused=True,engine_share_mode='BEGIN READ WRITE',app_has_generation_update_privilege=True)
    h.results.append(dict(test='startup-configuration-and-licensing-mode',result='pass'))


def schema_constraints(h):
    for sql,reason in [
        ('DELETE FROM provisiond.startup_generation','retained state cannot be removed'),
        ('UPDATE provisiond.ledger_entries SET amount_sats=1','retained state cannot be removed'),
        ('DELETE FROM provisiond.ledger_entries','retained state cannot be removed'),
        ('DELETE FROM provisiond.deposits','retained state cannot be removed'),
        ("UPDATE provisiond.tenant_identities SET id='reused'",'retained state cannot be removed'),
        ('INSERT INTO provisiond.startup_generation(singleton) VALUES(false)','check constraint'),
    ]:
        r=h.run(['psql','-XAt','-v','ON_ERROR_STOP=1','-h','127.0.0.1','-p',str(h.ports[0]),'-U','migrator','-d','postgres'],sql=sql,ok=False)
        assert r.returncode!=0 and reason in r.stderr
    assert h.sql('SELECT count(*) FROM provisiond.startup_generation')=='1'
    assert h.sql("SELECT count(*) FROM pg_constraint WHERE contype='f' AND conrelid IN ('provisiond.ledger_entries'::regclass,'provisiond.deposits'::regclass) AND confrelid='provisiond.tenants'::regclass")=='0'
    assert h.sql("SELECT pg_has_role('migrator','pg_read_all_stats','member') OR pg_has_role('migrator','stats_reader','member')")=='f'
    h.results.append(dict(test='schema-retention-constraints',result='pass'))
