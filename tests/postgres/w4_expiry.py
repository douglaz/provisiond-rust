"""API-3/7/34/42, LDG-35, WIR-4/5/9: authenticated pending expiry race.

No runtime hook: the first receipt SELECT is reachable only after authenticate
has compared the digest and returned. Hold its Parse before BEGIN; exercise the
housekeeping-owned deletion transaction with identical locks and eligibility SQL.
"""
import concurrent.futures
import hashlib
from http.client import HTTPException
import json
import socket
import time

from harness import eventually
from workflow import PARAMETERS, http


REFUSAL = 'expired authenticated deposit must receive HTTP 401 authentication refusal'
STATUS = 'SELECT status FROM provisiond.tenants WHERE id=$1'
RECEIPT = "SELECT COALESCE((SELECT jsonb_build_object('fingerprint'"
WORK = 'SELECT to_json(EXISTS(SELECT 1 FROM provisiond.enrolments'


def snapshot(w):
    # Counts and complete counter values distinguish deletion from request writes.
    tables = ['deposits', 'idempotency_records', 'payments', 'ledger_entries',
              'operations', 'machines', 'commitments', 'tenant_provider_accounts']
    counts = {name: int(w.h.sql('SELECT count(*) FROM provisiond.' + name)) for name in tables}
    counts['counters'] = w.h.sql("SELECT COALESCE(jsonb_agg(to_jsonb(c) ORDER BY principal,ceiling,interval_start),'[]') FROM provisiond.ceiling_counters c")
    counts['provider'] = w.external()
    counts['rail'] = http(w.h.ports[6], 'GET', '/inspect')[1]
    return counts


def absent(h, tenant):
    return h.sql(f"SELECT (SELECT count(*) FROM provisiond.tenants WHERE id='{tenant}')||':'||(SELECT count(*) FROM provisiond.enrolments WHERE tenant_id='{tenant}')") == '0:0'


def expiry(w):
    h = w.h
    w.stop_engine()
    t = w.enrol()
    other = w.enrol()
    # A live pending signup still mints and replays, before any activation.
    good = w.request('POST', '/v1/deposits', {'amount_sats': 60}, other, 'expiry-pending-valid')
    assert w.request('GET', '/v1/enrol/' + other['handle']) == {'status': 'pending'}
    assert w.request('POST', '/v1/deposits', {'amount_sats': 60}, other, 'expiry-pending-valid') == good
    w.fund(other, 'expiry-unrelated')
    # API-7: stored replay and fingerprint precede suspension, new writes refuse.
    h.sql(f"UPDATE provisiond.tenants SET status='suspended' WHERE id='{other['tenant_id']}'")
    assert w.request('POST', '/v1/deposits', {'amount_sats': 60}, other, 'expiry-pending-valid') == good
    assert w.request('POST', '/v1/deposits', {'amount_sats': 61}, other, 'expiry-pending-valid', status=409)['error']['details']['reason'] == 'idempotency_mismatch'
    assert w.request('POST', '/v1/deposits', {'amount_sats': 60}, other, 'expiry-suspended-new', status=403)['error']['kind'] == 'suspended'
    h.sql(f"UPDATE provisiond.tenants SET status='active' WHERE id='{other['tenant_id']}'")
    # An existing principal with an elapsed mint cap is a different API-34 refusal.
    capped = w.enrol()
    h.sql(f"UPDATE provisiond.enrolments SET created_at=clock_timestamp()-interval '10500 seconds',expires_at=clock_timestamp()+interval '300 seconds' WHERE tenant_id='{capped['tenant_id']}'")
    assert w.request('POST', '/v1/deposits', {'amount_sats': 60}, capped, 'expiry-cap-closed', status=409)['error']['details']['reason'] == 'signup_window_closed'
    tenant = t['tenant_id']
    key = 'expiry-never-used-deposit'
    correlation = 'pending-expiry-refusal'
    assert h.sql(f"SELECT count(*) FROM provisiond.idempotency_records WHERE scope_id='{tenant}' AND key='{key}'") == '0'
    before = snapshot(w)
    identities = h.sql('SELECT count(*) FROM provisiond.tenant_identities')
    principals = h.sql('SELECT count(*) FROM provisiond.tenants')
    enrolments = h.sql('SELECT count(*) FROM provisiond.enrolments')
    original = w.api
    listening = len(w.events('api', 'api_listening'))
    def trace():
        return [json.loads(line) for line in original.log.read_text().splitlines() if line.startswith('{')]
    receipt = h.proxy.arm(RECEIPT, 'send')
    boundary = h.proxy.arm(STATUS, 'execute_reply')
    response = None
    transport_error = None
    with concurrent.futures.ThreadPoolExecutor(max_workers=1) as pool:
        request = pool.submit(http, h.ports[4], 'POST', '/v1/deposits',
                              {'amount_sats': 60}, t['spending_token'], key,
                              headers={'X-Correlation-Id': correlation, 'Origin': 'https://expiry.invalid'})
        try:
            auth_session = receipt.wait()['pid']
            assert not any(e.get('correlation_id') == correlation for e in w.events('api', 'http_business_begin'))
            h.event('expiry_authentication_returned',tenant=tenant,session=auth_session,
                    boundary='first receipt Parse after successful authenticate return; before business BEGIN')
            # API-34/42, STO-7/55, LDG-35: age only this unfunded fixture;
            # preserve the configured TTL and every application deadline. Same
            # capacity lock, tenant lock, eligibility and deletion SQL as sweep.
            deletion = h.sql(f"""BEGIN;
SET LOCAL statement_timeout='1000ms';
SET LOCAL lock_timeout='500ms';
SET LOCAL idle_in_transaction_session_timeout='1000ms';
SELECT pg_advisory_xact_lock(1347572816,0);
SELECT pg_advisory_xact_lock(1347572814,hashtext('{tenant}')) /* LDG-35 */;
UPDATE provisiond.enrolments SET created_at=transaction_timestamp()-interval '10801 seconds',expires_at=transaction_timestamp()-interval '1 second' WHERE tenant_id='{tenant}';
SELECT e.tenant_id FROM provisiond.enrolments e JOIN provisiond.tenants t ON t.id=e.tenant_id WHERE t.status='pending' AND e.expires_at<transaction_timestamp() AND NOT EXISTS(SELECT 1 FROM provisiond.deposits d WHERE d.tenant_id=t.id AND d.expires_at+{PARAMETERS['finality_seconds']}::bigint*interval '1 second'>transaction_timestamp()) AND e.tenant_id='{tenant}';
DELETE FROM provisiond.tenants WHERE id='{tenant}' AND status='pending' RETURNING id;
DELETE FROM provisiond.enrolments e WHERE tenant_id='{tenant}' AND NOT EXISTS(SELECT 1 FROM provisiond.tenants t WHERE t.id=e.tenant_id) RETURNING handle;
COMMIT;""", role='app')
            assert deletion.splitlines().count(tenant) == 2 and t['handle'] in deletion.splitlines() and deletion.splitlines()[-1] == 'COMMIT'
            assert absent(h, tenant), 'acknowledged deletion must precede request release'
            h.event('expiry_deletion_acknowledged',tenant=tenant,tenant_absent=True,enrolment_absent=True,
                    mechanism='housekeeping-owned serialization/deletion transaction via app fixture session',
                    capacity_lock=1347572816,tenant_lock=1347572814,synchronous_commit='on',
                    deposits_inside_finality_window=0,application_deadlines_unchanged=True)
            receipt.release()
            session = boundary.wait()['pid']
            begins = [e for e in trace() if e.get('event')=='http_business_begin' and e.get('correlation_id')==correlation]
            assert len(begins)==1 and begins[0]['tenant']==tenant and begins[0]['key_digest']==hashlib.sha256(key.encode()).hexdigest()
            h.event('expiry_status_boundary_reached',tenant=tenant,session=session)
            boundary.release()
            try:
                response = request.result(timeout=8)
            except (HTTPException, OSError) as error:
                transport_error = error
            zero = eventually(lambda: next((e for e in h.events if e['event']=='protocol_response' and e.get('session')==session and e.get('query')==STATUS and e.get('command_tag')=='SELECT 0'), None))
            assert zero
            # Drop discards the session. Either proxy direction may observe the
            # close first; reverse EOF shuts down the front socket and can turn
            # its subsequent read into client_error instead of client_eof.
            # Require prompt physical closure after the actual SELECT completion,
            # before the 1000ms idle timeout could supply backend disappearance.
            closure=eventually(lambda: next((e for e in h.events if e['event']=='transport_closed'
                and e.get('session')==session and e.get('source') in
                ('client_eof','client_error','server_eof_or_error')),None))
            activated=next(e for e in h.events if e['event']=='fault_activated'
                and e.get('session')==session and e.get('direction')=='execute_reply' and e.get('query')==STATUS)
            assert 0<=closure['elapsed']-activated['elapsed']<.5, 'pre-COMMIT discard must close before idle timeout'
            eventually(lambda: h.sql(f'SELECT count(*) FROM pg_stat_activity WHERE pid={session}') == '0')
            assert any(e.get('event')=='precommit_session_discarded' and e.get('session')==session for e in trace())
            listener_after_exit = None
            if transport_error:
                eventually(lambda: original.poll() is not None)
                try:
                    with socket.create_connection(('127.0.0.1', h.ports[4]), timeout=1):
                        listener_after_exit = 'accepted'
                except OSError as error:
                    listener_after_exit = type(error).__name__
            h.event('expiry_response_observed',status=response[0] if response else None,
                    body=response[1] if response else None,headers=response[2] if response else None,
                    transport_error=repr(transport_error) if transport_error else None,
                    api_exit_status=original.poll(),api_stderr=original.error.read_text(),
                    api_stopping=[e for e in trace() if e.get('event')=='api_stopping'],
                    listener_after_exit=listener_after_exit,status_row_count=0,
                    transaction_session_closed=True,backend_absent=True,closure_source=closure['source'],
                    close_after_status_ms=round((closure['elapsed']-activated['elapsed'])*1000))
            if transport_error:
                raise AssertionError(REFUSAL) from transport_error
            code, body, headers = response
            assert code == 401, REFUSAL
            assert body == {'error': {'kind':'authentication','message':'authentication required',
                                     'retryable':False,'details':{'reason':'token'}},
                            'correlation_id':correlation}, REFUSAL
            assert headers['X-Correlation-Id'] == correlation
            assert headers['Access-Control-Allow-Origin'] == '*'
            assert headers['Access-Control-Expose-Headers'] == 'Retry-After, X-Correlation-Id'
            assert headers['Vary'] == 'Origin' and 'Access-Control-Allow-Credentials' not in headers
            assert 'Retry-After' not in headers
        finally:
            receipt.release()
            boundary.release()
    assert snapshot(w) == before, 'expiry refusal must invent no business/counter/provider/rail effects'
    assert absent(h, tenant)
    assert h.sql('SELECT count(*) FROM provisiond.tenant_identities') == identities
    assert int(h.sql('SELECT count(*) FROM provisiond.tenants')) == int(principals)-1
    assert int(h.sql('SELECT count(*) FROM provisiond.enrolments')) == int(enrolments)-1
    assert h.sql(f"SELECT count(*) FROM provisiond.deposits WHERE tenant_id='{tenant}'") == '0'
    assert h.sql(f"SELECT count(*) FROM provisiond.idempotency_records WHERE scope_id='{tenant}'") == '0'
    assert h.sql(f"SELECT count(*) FROM provisiond.ceiling_counters WHERE principal='{tenant}'") == '0'
    # Reacquire exactly the abandoned request's primitive with a bounded wait.
    h.sql(f"BEGIN; SET LOCAL lock_timeout='500ms'; SELECT pg_advisory_xact_lock(1347572814,hashtext('{tenant}')); ROLLBACK;",role='app')
    # Both periodic components must make progress after the refusal, with the
    # same API process. Observe a complete housekeeping query and rail entry.
    after = time.time()
    rail_entries = len(w.events('api', 'external_entry'))
    eventually(lambda: any(e['event']=='protocol_response' and e.get('tag')=='Z' and e.get('query','').startswith(WORK) and e['timestamp']>after for e in h.events))
    eventually(lambda: len(w.events('api','external_entry')) > rail_entries)
    balance = w.request('GET', '/v1/balance', tenant=other)
    assert balance['balance_sats'] == 115
    live = w.request('POST', '/v1/deposits', {'amount_sats':60}, other, 'expiry-after-refusal')
    assert w.request('POST', '/v1/deposits', {'amount_sats':60}, other, 'expiry-after-refusal') == live
    assert original is w.api and original.poll() is None
    assert len(w.events('api','api_listening')) == listening
    assert not any(e.get('event') in ('api_stopping','api_background_task_failed','api_request_task_failed') for e in trace())
    assert not original.error.read_text()
    assert absent(h, tenant)
    h.event('expiry_liveness_no_effects_observed',tenant=tenant,same_api_process=True,
            exact_401=True,unrelated_authenticated_balance=115,post_refusal_deposit_replayed=True,
            periodic_housekeeping_progress=True,periodic_rail_progress=True,
            business_counts_unchanged=before,tenant_and_enrolment_removed=1,
            retained_identity_unchanged=True,transaction_discarded=True,tenant_lock_reacquired=True,
            default_features=w.default_build,unused_key_sha256=hashlib.sha256(key.encode()).hexdigest())
    h.results.append(dict(test='W4-authenticated-pending-expiry',result='pass'))
