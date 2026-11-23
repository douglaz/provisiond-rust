# Local recovery procedure

This is the simulator-only STO-54/56 procedure exercised by
[tests/postgres/w7.py](../tests/postgres/w7.py), using PostgreSQL 18.6 and one primary/one
designated synchronous physical standby. [The report](../evidence/w7/report.md) records the exact
source, commands, results and untested arms. The final serial committed-source validation on
`f302686` recorded exit 0 for the fast gate, units and all twelve phases. This is not a production fencing or deployment package.

## Prerequisites and event classification

Use the required capped foreground unit, a fresh private harness root, fresh credentials and an
exclusive free ten-port slot, as described in [README](../README.md). Keep all raw logs, databases,
WAL, backups and builds under that invocation's root. Credentials never enter git. Only owned
process IDs and exact owned directories may be stopped or removed. Keep a complete component
inventory and exclusive control of their launch/data paths throughout the event.

| Event | Procedure |
|---|---|
| Process or primary restart on unchanged intact history, no open incident | Ordinary startup and new-connection checks |
| Process successor on the same history with an open incident | Continue that incident and its progress |
| Standby-only rebuild with intact authoritative primary | Quiesce, bootstrap and verify the peer; create no incident |
| Every promotion, including lossless promotion | Stop/fence, establish replacement peer, acknowledge a new incident |
| Replacement of authoritative history | The same new-incident procedure |
| Promotion/replacement with an open predecessor | Atomically supersede it with a distinct, fresh incident |

Do not infer an event class from an LSN comparison or from whether rows survived. A process crash
creates no new incident. Keep `synchronous_commit=on` and `FIRST 1 (provisiond_sync)` throughout;
there is no asynchronous fallback, automatic promotion, connection authority or transaction pooler.

## The nine steps

1. **Stop all components and fence the old authority.** `Harness.fence_authority` terminates and
   joins every owned API/engine process and prevents replacement launches. It stops the owned
   PostgreSQL process, quarantines its data directory and fences its launch identity. A retained
   old session must fail, a new component connection must fail, and a normal relaunch of that
   identity must be refused. An environment flag or a stopped API alone is insufficient. This
   mechanism relies on the fixture's exclusive launcher/data ownership. Use a separately approved
   deployment mechanism on any real installation.
2. **Perform the explicit event.** The promotion fixture runs `pg_ctl -D <owned-standby> promote
   -w -t 15`. The lossy fixture starts an owned copy of an earlier verified physical backup as
   authority, deliberately excluding later acknowledged rows. It retains independent simulator
   effects and records source/target ancestry, backup manifest, system identifier and timeline
   observations. The old primary cannot serve again as authority. It may return only after a
   verified rebuild as standby of the new authority.
3. **Establish the replacement synchronous peer.** Build it from the new authority, configure
   the final authenticated role `replicator` and application name `provisiond_sync` before start,
   and verify the run-owned backup chain, matching system identifier, recovery role and sampled
   flush catch-up. Equal positions alone do not prove ancestry. No application, incident or
   migration commit may be acknowledged without this peer. In the no-replacement case, actual
   incident establishment refuses during connection admission; the fixture claims no sent-COMMIT
   SyncRep wait for that refusal.
4. **Use the physical-backup bootstrap method.** The exercised command is `pg_basebackup -d
   <private-connection-with-application_name=bootstrap_backup> -D <fresh-owned-directory> -X
   stream --checkpoint=fast --no-password`, followed by `pg_verifybackup <owned-directory>`.
   Its replication connection is distinct from the designated peer. The original bootstrap and
   replacement fixtures run without an existing acknowledgment partner or a prerequisite
   application/incident commit. Never relabel the backup sender as the designated peer.
5. **Keep maintenance confined.** This backup method required no maintenance commit-mode
   exception. No instruction here authorizes lowering commit mode. No backup session performs
   provisiond, schema, financial, credential or incident writes or enters a component pool.
6. **Close backup sessions; migrate offline if necessary.** Wait for backup and verification to
   finish before component recovery. On the verified replacement topology, run the deployable
   `migrate` binary as the migrator, following [release.md](release.md), including disabled
   application login and zero old backends. This release requires completed history 1/1 through 5/5. The runner
   records one acknowledged migration per transaction and never runs in API/engine startup.
   Unsupported schema/semantic history and legacy incident prerequisites refuse startup. There
   is no down-migration or mixed-release service.
7. **Establish and await the new incident.** Retain one new UUID before the first attempt. Use
   `recovery establish EVENT_ID PREDECESSOR_ID_OR_none`, with the normal local store configuration,
   `PROVISIOND_COMPONENTS_STOPPED=yes`, `PROVISIOND_RECOVERY_INTERVAL_SECONDS` and
   `PROVISIOND_RECOVERY_DERIVATION_GAP`. The fixtures use 3 seconds and 10000 respectively; these
   are not production values. `PROVISIOND_RECOVERY_EVIDENCE` may name a reporting JSON file.
   An unset variable or nonexistent file records absent input and reports
   `STO-54: no reporting input supplied; loss endpoints remain unknown` on stderr. Unreadable
   files or malformed JSON are recorded as malformed, with a plain notice naming the case on
   stderr. Absent input is not an invalid certificate. Both cases retain unknown endpoints
   without an extra startup hold or waiving any real prerequisite (STO-54).
   Wait for exit 0 and the acknowledged transaction
   before allowing either component to start. A lost reply requires inspection and a retry with
   the same retained ID, not a new event. There is no application deadline on a sent COMMIT's
   standby wait.
8. **Continue the phases in order.** `recovery prepare` performs step 1 before the engine's
   startup lock: emit the persisted loss report as diagnostics, maintain the selective freeze and
   re-establish account status. Set `PROVISIOND_RECOVERY_LOG` when running it directly to retain
   the report. Engine startup can supervise this separate actor if the step is unmarked.
   After taking its lifetime lock, the engine's first write is still only its generation
   replacement. The acknowledged generation precedes its ordinary running pass and step-2
   quarantine. Every queued create/install/rescue inventory is quarantined without releasing
   money or licensing another mutation; goal-state work remains queued. Waiting historical
   suspension parents require a disposition. The API requires step 2, reconstructs unexpired
   watches and independently replays both rails through retained destination bindings, including
   deposits that expired after settlement. Expired deposits stay outside the active watch set;
   missing bindings remain unresolved and create no entitlement. It reseeds surviving counters,
   bumps credential generations and skips allocation forward, then atomically marks step 3 with
   its original grace end. Its listener stays down until acknowledgment. Before stopping an engine
   to retry after step 2, observe both its persisted mark and its post-COMMIT
   `recovery_step_acknowledged` event; `restart_pass_complete` precedes that boundary.
9. **Reconcile and expose only permitted behavior.** Use independent simulator histories,
   exact retained correlators and non-mutating confirmation. The first complete account pass
   records no absence; only a second pass separated by the current effective visibility window
   may do so. Report unrecorded resources per account, without auto-attachment. Missing
   correlators and unknown effects remain operator-owned. Both rail payment keys still credit
   exactly once; replay does not rebuild lost commitments or extensions.

Steps 3–5 describe the replacement bootstrap subprocedure; they never authorize an incident commit
before the replacement is verified. The standby-only rebuild uses this same backup method while
the intact primary remains authoritative and components are quiesced, then resumes without a new
incident.

## Retry, credentials and parent disposition

Every successful `recovery establish`, `prepare`, `parent` or `close` prints one incident JSON
row on stdout, showing the action's result; `close` includes the retained `closed_at`.
`recovery report` prints the open row, or `null` when none is open. Plain reporting-input notices
and errors go to stderr; internal JSON trace events are silent by default. Set
`PROVISIOND_RECOVERY_LOG=<path>` to append every trace event as unchanged JSON, one per line, to
that file; an unopenable path fails with a plain error before recovery work begins.
`recovery --check-schema` prints no result on stdout. When the engine supervises `recovery prepare`
with `PROVISIOND_RECOVERY_PARENT_PIPE=yes`, trace events go to the engine's stdout log, with no
result row; a configured diagnostic file also receives those events.

A matching establishment retry is an acknowledged guarded
no-op preserving original instant, predecessor, reporting, provenance, progress and grace. Optional
`PROVISIOND_RECOVERY_EXPECTED_INSTANT` checks the retained instant. Instant/predecessor mismatches
refuse while components remain stopped. Retrying a closed predecessor never replaces its successor.
A genuinely new event uses a new ID, closes the predecessor by supersession and starts at step 0
with null grace; predecessor marks discharge nothing. An interrupted unmarked step may repeat
safe forward writes. A marked step never repeats or invalidates a newly reissued credential.

After step 3, customers use their recovery credential with `POST /v1/recovery/revoke`, body `{}`
and an idempotency key. It returns one fresh spending token, generation and revocation instant.
Store that secret privately; a second revocation invalidates the first token. A lower-generation
delayed response is already dead. Repeating a used key returns `409 credential_already_replaced`,
including after a lost response. Use a fresh recovery key for another replacement. The spending
token cannot authorize replacement; pending and suspended tenants retain this maintenance access.

`recovery parent INCIDENT_ID OPERATION_ID confirmed|cancelled` records a held historical parent's
disposition. Cancellation settles it without fan-out; confirmation does not implement the deferred
parent worker. Conflicting dispositions refuse. Its incident history survives ordinary operation
retirement, as do the retired identity and expired-key refusal. No install/rescue/fan-out execution
is demonstrated by the seeded metadata fixtures.

## Grace and close

Three facts remain separate: an open incident, the stored wall-clock end and funding-only paused
grace. Null/future wall grace defers every reached **system** cancellation before funding,
suspension or inherited-fence retry checks. It writes no new fence and makes no provider call.
An ordinary caller delete can stop its machine during this interval. At wall end the implemented
TTL/operation-retention actors resume even while the incident remains open; waiting-parent
confirmation is an additional condition on that exception. Deferred sweeps remain unimplemented.
The null-create ordinary-restart exception stays disabled for the entire open incident.

After wall end, current gone/closed disposition and inherited-fence/current-suspension exemptions
retain their OPS-41 order. A rate-present funded machine aborts before paused grace. Only an
otherwise unfunded funding cancellation consumes rate-present time since step 3, computed from
retained acceptance-order/verdict/freshness stamps per currency. Outages pause without resetting
that sum. Neither the sum nor a moving grace end is persisted. No-rate steps 4–5 remain safely
refused under D-46; a fixture must not enable them to complete a rehearsal.

`recovery close INCIDENT_ID` refuses until step 3 is marked, wall grace has passed, every parent
has a disposition and every configured account has its second complete pass. Each currency must
also have spent its own rate-present interval, or every current machine in it must be gone or
fenced. An outage bound is no substitute. Close reads current machine disposition under a table
lock: a pre-close attachment/fence clear can block it again; a post-close attachment does not
reopen it or obtain restore grace. Unhealthy accounts can conservatively prevent the second-pass
close condition; do not invent a pass or skip that prerequisite.

## Reporting and operational limits

`restore_instant` is the inserting transaction's event time, not recovered-history time. The
local implementation accepts no certified finite endpoint input. Lower and upper bounds stay
null, with incident-keyed reasons and no finite loss duration. A bare timestamp, LSN, backup
filename or restart time supplies no clock/prefix certificate. Continuation does not require the
failed host's reporting file. Certified-known lower/upper and lagging-standby variants remain
operator-owned `not run`, pending independently bounded clocks and retained matching-prefix and
stopping/fencing evidence plus a supported certificate-ingestion path.

The lossy fixture demonstrates that restored runway can move backward, payment replay cannot
reconstruct extensions/commitments, disappeared metering/rate/gone history stays lost, and a
consistent meter audit proves no absence of rollback. Unknown external effects remain unknown.
Its finite skip test covers every removed index actually recorded by that fixture; deployments
still need a justified allocation-loss bound. It proves no non-reuse under arbitrary unbounded
rollback. Synthetic extension state is not an implemented extend-runway route.

Different-person and before-first-customer-payment rehearsal, production fencing, separate failure
domains, production recovery bounds/alarms and general billing/provider conformance remain `not
run`, owned by the deployment operator or deferred work. Never use this local automated rehearsal
as their discharge. W8 adds the saturation procedure below; W9–W11 remain outside this procedure.

## Saturation recovery on unchanged history

STO-55's five steps below are ordered. STO-7 forbids cancelling or terminating any individual
primary backend waiting in SyncRep, except by primary shutdown. Use the inventory and private
configuration variables from [release.md](release.md), with `CURRENT_RELEASE` naming the installed
current release's directory. Keep the primary data/history identity unchanged. This is an outage;
there is no fixed recovery duration or guaranteed observer connection. These commands are written
for the owned local profile. The [recorded W8 journey](../evidence/w8/live.json) exercised standby
restoration with unavailable observer capacity; primary shutdown fallback remains not run. [Completed validation](../evidence/w8/validation.json) preserves the canonical W5 fixture failure
and the separate unchanged-source W5 pass.

Prepare the private event directory before restarting either unit (the release preparation
uses the same commands for the `local` instance):

```sh
install -d -m 0700 "$HOME/.local/state/provisiond"
export ENGINE_EVENTS="$HOME/.local/state/provisiond/local-engine-events.jsonl"
export API_EVENTS="$HOME/.local/state/provisiond/local-api-events.jsonl"
```

For another instance, match both event filenames to the unit instance (`%i` in the templates).
OVR-18/19 and STO-55: high-volume events go to these append files to avoid journald's per-user
rate limit suppressing alarms and fatal/refusal diagnostics, which remain on stderr in the journal.

1. **Stop replacement processes and connection growth through deployment controls.** This needs
   no database connection. Freeze other launchers. Mask the exact application units first:

   ```sh
   systemctl --user mask --runtime "$ENGINE_UNIT" "$API_UNIT"
   # If an owned runaway-client unit is the identified connection-growth source:
   # systemctl --user mask --runtime "$RUNAWAY_UNIT"
   # systemctl --user stop "$RUNAWAY_UNIT"
   ```

   Stop every separately inventoried launcher/client that can open application connections,
   including a manually launched recovery actor. Only act on this deployment's recorded units
   and PIDs. A stopped client may leave a backend behind; pool counts prove no global occupancy
   bound. Do not keep attempting new psql connections or reset supervisor counters during this step.

2. **Quiesce application work and preserve uncertainty.** No SQL is needed:

   ```sh
   systemctl --user stop "$API_UNIT" "$ENGINE_UNIT"
   systemctl --user show "$ENGINE_UNIT" "$API_UNIT" -p ActiveState -p SubState -p MainPID
   tail -n 200 "$ENGINE_EVENTS" "$API_EVENTS"
   journalctl --user -u "$ENGINE_UNIT" -u "$API_UNIT" -n 200 --no-pager
   ps -ww --ppid "$(head -n 1 "$PRIMARY_DATA/postmaster.pid")" -o pid,ppid,args
   tail -n 200 "$PRIMARY_LOG"
   ```

   Require MainPID=0 and stopped/failed units, plus stopped independent actors. Retain failed
   request keys, operation IDs, provider identifiers and private raw logs, including both
   `$ENGINE_EVENTS` and `$API_EVENTS` for `stop_record_started`, `stop_record_outcome`,
   `stop_record_finished` and `api_stopping`. A fatal stop or signal death is no classification
   of its in-flight work. PostgreSQL process titles may show COMMIT
   waiting for replication; server logs may also report waits if replication-wait logging was enabled in advance. Absence of those text markers proves no
   absence of SyncRep waiters. With no observer connection, treat all residual backends as
   possibly waiting; never target one with kill, pg_cancel_backend or pg_terminate_backend.
   OPS-21's 1000 ms record bound remains unchanged; the templates allow 5 seconds before forced
   kill. A process that dies before a record is acknowledged leaves restart recovery to OPS-15.

3. **Restore the designated standby so existing waits can complete.** Use process controls on
   that existing authenticated peer, without fresh primary SQL:

   ```sh
   pg_ctl -D "$STANDBY_DATA" status
   # If stopped:
   pg_ctl -D "$STANDBY_DATA" -l "$STANDBY_LOG" -o "$STANDBY_SERVER_OPTIONS" start -w -t 60
   # If suspended, instead resume the recorded owned standby postmaster:
   # kill -CONT "$OWNED_STANDBY_PID"
   tail -n 200 "$STANDBY_LOG"
   tail -n 200 "$PRIMARY_LOG"
   ps -ww --ppid "$(head -n 1 "$PRIMARY_DATA/postmaster.pid")" -o pid,ppid,args
   ```

   pg_ctl status returns 0 for running, 3 for stopped; other statuses require inspection.
   Preserve the original postmaster command-line options (including ports and configuration paths)
   in the two SERVER_OPTIONS variables as release.md describes; start does not reload postmaster.opts.
   If SQL replay alone was explicitly paused, resume it using the separate standby administrator
   connection: `psql "$STANDBY_OPERATOR_DATABASE_URL" -X -v ON_ERROR_STOP=1 -c 'SELECT pg_wal_replay_resume()'`.
   This is not a new primary observer connection. Wait for existing replication waits and
   disconnected clients to finish where possible. Never change designated identity, disable
   synchronous replication or manufacture a rollback from a failed checkout/check.

4. **If still necessary, shut down and start the primary on its intact unchanged history.**
   If residual backends or exhaustion prevent progress, this is the allowed way to end SyncRep
   waits, and needs no SQL connection:

   ```sh
   pg_ctl -D "$PRIMARY_DATA" stop -m fast -w -t 60
   pg_ctl -D "$PRIMARY_DATA" -l "$PRIMARY_LOG" -o "$PRIMARY_SERVER_OPTIONS" start -w -t 60
   ```

   Check command statuses and logs before continuing. A timeout is unverified, not permission to
   target a backend. Never substitute promotion, restore, copying data or clearing replication
   configuration for this step. If history must change, stop this procedure and enter STO-54's
   incident procedure above, even for a lossless promotion. A locally committed transaction may
   survive this shutdown; its prior client failure did not establish rollback.

5. **Restart only after connection and read checks pass.** Confirm unchanged owned data paths,
   primary system identity/history and designated peer provenance against the deployment's
   retained inventory. With connection growth stopped, the administrator can inspect recovery:

   ```sh
   psql "$OPERATOR_DATABASE_URL" -X -v ON_ERROR_STOP=1 <<'SQL'
   SELECT pg_is_in_recovery(),system_identifier FROM pg_control_system();
   SHOW synchronous_commit;
   SHOW synchronous_standby_names;
   SELECT usename,application_name,state,sync_state,flush_lsn FROM pg_stat_replication;
   SELECT pid,usename,wait_event_type,wait_event FROM pg_stat_activity;
   SELECT version,semantics,contract_step,rollback_floor FROM provisiond.schema_migrations ORDER BY version;
   SQL
   (
     set -a
     . "$APP_ENV"
     set +a
     "$CURRENT_RELEASE/bin/engine" --check-schema &&
     "$CURRENT_RELEASE/bin/api" --check-schema &&
     "$CURRENT_RELEASE/bin/recovery" --check-schema &&
     "$CURRENT_RELEASE/bin/recovery" report
   )
   ```

   Require primary recovery=false, unchanged system identity and intact history, synchronous
   commit on, the configured FIRST 1 designated standby only, correct authenticated sender in
   sync/streaming state, and successful real application connection/read/flush checks (exit 0).
   No new recovery incident is created on unchanged history. If one was already open, continue
   its marks and prerequisites. Keep stopped on any failed check. If login was disabled during
   earlier release work, re-enable it as release.md step 6 describes only after that migration
   succeeded. When ready:

   ```sh
   systemctl --user unmask --runtime "$ENGINE_UNIT" "$API_UNIT"
   for u in "$ENGINE_UNIT" "$API_UNIT"; do
     if systemctl --user is-failed --quiet "$u"; then
       systemctl --user reset-failed "$u"
     fi
   done
   # Both units must still be stopped when recording their event-file offsets.
   ENGINE_FROM=$(( $(stat -c %s "$ENGINE_EVENTS" 2>/dev/null || echo 0) + 1 ))
   API_FROM=$(( $(stat -c %s "$API_EVENTS" 2>/dev/null || echo 0) + 1 ))
   STARTED=no
   systemctl --user start "$ENGINE_UNIT" &&
   timeout 120 sh -c 'until tail -c +"$1" "$2" 2>/dev/null | grep -m1 -F "\"event\":\"restart_pass_complete\""; do sleep 1; done' _ "$ENGINE_FROM" "$ENGINE_EVENTS" &&
   systemctl --user is-active "$ENGINE_UNIT" &&
   # If an incident is open, require recovery_step_acknowledged step 2 in:
   # tail -c +"$ENGINE_FROM" "$ENGINE_EVENTS"
   systemctl --user start "$API_UNIT" &&
   timeout 120 sh -c 'until tail -c +"$1" "$2" 2>/dev/null | grep -m1 -F "\"event\":\"api_listening\""; do sleep 1; done' _ "$API_FROM" "$API_EVENTS" &&
   systemctl --user is-active "$API_UNIT" &&
   STARTED=yes
   # Alarms and fatal/refusal diagnostics remain in the journal.
   journalctl --user -u "$ENGINE_UNIT" -u "$API_UNIT" -n 200 --no-pager
   [ "$STARTED" = yes ] || { echo 'Startup not confirmed: do not resume customer traffic; inspect the journal and event files' >&2; false; }
   ```

   Event files append across restarts. Record offsets while both units are stopped; the
   bounded waits read only bytes appended after those offsets, including markers that would
   have scrolled out of a line-count tail. Require exit 0 from each start, wait and active check.
   A timeout (exit 124), other nonzero result or inactive unit is a failed step: stop here,
   inspect the journal and event files, and do not start the API or resume customer requests.
   Reset only failed units because a cleanly stopped unit may have been collected and resetting
   an unloaded unit fails. Reconcile previously running creates through retained exact correlators
   and direct reads,
   never through another purchase. Rail replay credits retained payments once. Customers inspect
   operation/balance/payment/machine reads and resend identical bytes under the original key,
   obeying retryable 503 pacing with bounded attempts. Do not invent failure for a commit that
   landed, success for an unacknowledged reply, or a receipt for a checkout-refused request.

The templates use `Restart=on-failure`, initial delay 1 s, three exponential steps to 8 s,
`StartLimitIntervalSec=infinity`, `StartLimitBurst=4`, MemoryMax=16G and MemorySwapMax=0.
The fourth start is the last until operator recovery and reset-failed; keep the failed units loaded
and do not restart the user manager to bypass the bound. Systemd rate counters are not a durable
external admission authority. Runtime checkout unavailability alarms on stderr after a 2000 ms
failure streak independently for read/write pools; the next successful checkout on that pool clears
the streak. Fatal engine/API exits alarm immediately (threshold 0 ms) and successful checkouts never
reset a sent transaction's uncertainty. Journald retains JSON lines with event=alarm and a readable
message. Check the journal and unit failed state after the start bound is reached. Production alert
routing and independent liveness/standby monitoring remain deployment-owned.
