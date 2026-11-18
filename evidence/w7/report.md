# W7 recovery validation

The final serial pre-publication run on committed operational source
`f302686a210f3b77decd169699314c1490a9cacc` recorded **pass**: fast gate exit 0,
Python units exit 0 and the complete twelve-phase canonical command exit 0. The earlier
three-worker failure remains a failure; this serial invocation is its one recorded rerun.
Documentation and evidence were provisional during that run and finalized afterward; they
are not executable inputs. This is a simulator-only, arm-specific pre-conformance result,
not production acceptance or discharge of an independent operator rehearsal.

Specification pin: `ac37c7cae5d3ff746515137774a4cdd5a3531375`; review base:
`6568cba4693514e71a2e9dd3e0b508d65d78ae8b`. Operational history after that base:
`9a2c0e5`, `8ba6023`, `8339aae`, `11c9ae1`, `f5018aa`, `06b7636`, `c3b5cb5`,
`66cec39`, `fbbfc01`, `f302686`. The accepted 89-file operational map is
`2bb105312b7f310351952545af13cbe2e46f31e4a4720dca0d6c1a476e8de64e`, matching
`11c9ae1`. The final 96-file map is
`dcaa24c27ed739129fcf66bcbd4e4af706d69bd7de8643e26c49522b1c299060`.
[Source inventory](source-inventory.json) retains both complete maps and all 26 per-file
changes/additions, attributed only to `f5018aa`, `06b7636`, `66cec39`, `fbbfc01` and
`f302686`: recovery result/reporting and quiet diagnostics, isolated parallel fixtures/ports,
pre-admission standby-check retry eligibility, exclusive timing-sensitive histories and the
iteration command. Tracker-only history changes do not alter this operational map.

The raw manifests' complete source map has SHA-256
`5708ad939e8b78a11c550d12f0ad9109b9da4df976c583c2a0da9dece5225149` and includes
dirty provisional documentation. Its `source_dirty: true` is preserved. The private HEAD and
operational-clean checks before/after identify the same committed executable inputs, and
finalization rechecked them without a build or test rerun.

## Final commands, environment and coverage

Every command ran in the required capped foreground allocation (16 GiB, no swap,
OOMPolicy=continue), using fresh invocation-owned directories and credentials. Whole stdout
and stderr were saved separately, with actual statuses retained before reading. The gate used
a fresh private target directory. Harness base was 25500, build concurrency 2, seed 20261113
and canonical worker bound 1. Private evidence paths use `<private-evidence>/<file>`;
`published_as` records the separate publication filename in the JSON records.

| Command | Exit | Wall seconds | Result |
|---|---:|---:|---|
| `nix develop --command bash tools/check.sh` | 0 | 54.122 | pass |
| `nix develop --command python3 -m unittest discover -s tests/postgres -p 'test_*.py'` | 0 | 2.799 | pass; 35 units |
| `nix develop --command bash tools/postgres.sh --jobs 1 --seed 20261113 --evidence <private-evidence>/canonical.json` | 0 | 1744.391 | pass; all twelve phases |

Total measured wall time: 1801.614 seconds. Canonical sampled peak memory: 4186673152 bytes;
cumulative unit peak after canonical: 4187398144 bytes. [Validation](validation.json)
retains recorded argv with private evidence paths replaced by the stated placeholder, separate
whole-stream hashes, versions and actual measurements. Commands retain their recorded executable
(including immutable tool paths); publication filenames are separate from the paths used by the run.
PostgreSQL/pg_basebackup/pg_verifybackup 18.6, Rust 1.97.1, Cargo 1.97.0, Python 3.14.7,
Nix 2.34.9 and the exact crate inventory are recorded. Hook-enabled and default executable
identities/features remain in the full W7/control manifests; shared build variants are in
[phase/build metrics](phase-metrics.json).

The [canonical parent](canonical.json) records every phase exit 0. The
[coverage comparison](coverage.json) against the retained historical inventory records all
checks true: twelve phases; the original 278 scenario/result rows, including existing
`not run` rows; all 21 unique W7 children; 44 controls at exact activated assertions;
five candidate reds with actual raw failure results; release sensitivity; build-once and
the global concurrency bound. This is equivalence to retained evidence, not a newly executed
baseline. Phase-level pass does not turn a retained `not run` row into a demonstration.

The parent, W7 phase, all 21 child manifests, lossless/lossy bundles, all 44 controls and
five candidate-red manifests are published in full apart from identifying paths and two embedded
stderr excerpts replaced by size/hash/retention metadata: `controls/red-w5_generation.json`
(`events[74].stderr_excerpt`) and `controls/red-w5_stop_reset.json` (`events[83].stderr_excerpt`).
Validation stream hashes describe the actual private streams. Ten other phase files are summaries:
per-scenario/child result, exact exit where recorded, counts, cleanup and full-manifest SHA-256.
Four scenario rows record explicit exits (42, 43, 0, 75); the other 253 raw scenario rows have
no exit_status field and retain null with a row-specific absence note. Phase process exits remain
separate; full command inventories are omitted from the summaries. Their full raw manifests are
**retained privately, not published**, with hashes in the [manifest index](raw-manifest-index.json).
No stdout/stderr dump, PostgreSQL log, patch, database, WAL or backup is published.

## Suite organisation and validation history

The selected procedure is **"Split it (Recommended)"**: each iteration runs quick checks,
units, W7 and controls (~11 min); a known checkout-deadline / timing-window failure gets
one rerun, explicitly recorded; before the PR the full suite runs once with 2-3 workers or
serially, taking longer than 20 min. `tools/iteration.sh` implements the iteration set using
`--phase iteration`, while `tools/postgres.sh --jobs N` implements all twelve phases.
Neither command automatically reruns a failure. Builds are shared read-only; one global
worker bound and reserved ten-port slots isolate mutable resources. Bases 25500–25990 are
ten-aligned within 25500–25999; the ephemeral-range guard refuses overlap without changing
kernel settings. Selected timing-sensitive histories run alone (D-67–D-69).

The [iteration record](iteration-validation.json) retains its single exit-0 measurement:
475.484 seconds total (32 s quick gate, 2 s units, 440 s W7/controls, whole-second step
resolution), 21/21 W7, 44/44 controls, five candidate reds, peak unit memory 7731761152 bytes.
It was measured on `fbbfc01` plus the iteration command subsequently committed as `f302686`.
Both acceptance runs on `fbbfc01` passed quick checks, units, W7 and controls. The first
also passed all twelve phases in 811.067 seconds; the second full invocation failed W6
`w6_episode_order` on the known post-startup checkout-deadline class.

[Prior failures](prior-failure.json) preserve every failure rather than rewriting it as pass:

| Recorded invocation | Canonical exit | Failed children / class | Disposition |
|---|---:|---|---|
| Earlier provisional source | 1 | W6 `w6_f2`: API housekeeping checkout failure, then connection refusal | Failure retained on its original source; underlying cause inconclusive |
| 2026-11-14 committed-source timing run | 1 | W7 `w7_reporting`: cold checkout after empty pre-listener replay; 1744 s | Failure retained; later shape-based deployment eligibility is a fixture change |
| First full parallel run | 1 | 2: W7 rebuild port-reservation gap; supersession pre-admission cold checkout | Failure retained; fixture changes recorded separately |
| Second full parallel run | 1 | 1: W6 release-timeout setup address-in-use in the old ephemeral range | Failure retained; listener-range/record-before-reservation changes recorded separately |
| Third full parallel run | 0 | 0; canonical 801.8 s, all checks/units/canonical 851.1 s | Separate pass after fixture changes; auxiliary retry-report helper still failed with TypeError |
| Committed-source parallel run on `66cec39` | 1 | 3: W5 pre-admission standby-check timeout, W6 default-hook activation window, W6 non-exact control's early status assertion | Failure retained; 43/44 recognized controls, not a negative-control pass for the early assertion |
| First acceptance run on `fbbfc01` | 0 | 0 | Full pass, 811.067 s canonical |
| Second acceptance run on `fbbfc01` | 1 | 1: W6 episode-order post-startup checkout deadline | Full run failed; iteration subset passed separately |
| Three-worker pre-publication run on `f302686` | 1 | 3: W6 delete-classification engine post-startup checkout deadline; W7 interruptions and W6 skip-startup control enrolment 503, retryable "store checkout unavailable" | Failure retained, 755.081 s canonical |
| Its one recorded serial rerun on `f302686` | 0 | 0 | Separate final pass, 1744.391 s canonical |

Two earlier reader-reported focused setup runs also failed at the 250 ms cold checkout bound
before reaching their W7 assertions. Their underlying cause is **inconclusive**; later focused
passes do not establish resilience or host-load causation. Fixture/deployment investigation
owns that uncertainty. No product pool, checkout, stop, retry or capacity policy was changed.
The open W8 question is engine/API survival of a brief post-startup connection-check timeout;
the fixture customer client does not honour enrolment's retryable 503. These remain follow-up
work, not changes made during documentation finalization.

## Operator and customer live journey

The [live journey](live-journey.json) used deployed release binaries on `06b7636`, an
operator-side harness kept outside the repository, PostgreSQL's tools, the recovery CLI and
customer/operator HTTP surfaces. It exited 0 with all 16 steps passing in 55.689 seconds,
including build and cleanup. Its recorded pre-history-rewrite identity is retained alongside
comparisons proving identical runtime/migrations/dependencies to `06b7636` and the final tip;
those comparisons each exited 0. This was a separate live journey, not the final canonical run.

It bootstrapped/migrated the deployment, enrolled and funded a customer on both simulated
rails, created two machines, took a physical backup, fenced/promoted, rebuilt and verified the
standby, established/prepared an incident and resumed service. Old spending tokens returned
401; recovery revocation issued a fresh token and same-key replay returned 409. Caller deletion
completed inside the 20-second grace. A later lossy restore replayed retained payments before
step-3 acknowledgment/listener, once per key; the lost second machine/charge stayed lost.
That backup predates the first incident, so the restored authority has no predecessor: this
journey does not claim retained-predecessor supersession (the canonical case covers that arm).
Close and standby-only rebuild completed with no new incident.

All 13 balance reads satisfied committed=sum(reservations) and available=balance-committed;
observed balances were 115 → 84 → 161 → 130 → 163 → 194 → 223. Every recovery call printed
one result row; stderr was quiet except the plain STO-54 absent-input notices on two establishes.
The allowed exceptions were role bootstrap, simulators as provider/rail backends and wallet,
and deployment configuration by SQL before customers existed. No hidden test control, fault
hook or fixture startup retry substitutes for a journey action. Owned processes/data/WAL/backups
were cleaned and ports freed. No real providers, purchases or funds were used.

## Rehearsed procedure and D-7

The [nine-step runbook](../../docs/recovery.md) covers only the exercised local stop/fence, explicit
promotion/history replacement, verified replacement standby, physical backup/verification,
no-maintenance-exception path, offline migration ordering, synchronous incident acknowledgment,
phased startup and reconciliation. [Lossless](lossless-bundle.json) and [lossy](lossy-bundle.json)
bundles retain the actual application, database and independent effect chain.

The exercised nine-step procedure is reproduced from the runbook:

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
   `migrate` binary as the migrator. Schema/semantics must be exactly 1/1, 2/2, 3/3, 4/4. The runner
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

D-7 is settled only for this local mechanism: stop/join the complete owned component inventory,
stop PostgreSQL, quarantine owned data/launch identity, independently observe old-session and
new-connection refusal and normal relaunch refusal, then build a verified authenticated standby
from the new authority. The old primary is not restarted as authority. Exclusive local launcher
and data ownership is an operational precondition, not production fencing proof.

`pg_basebackup -X stream`/`pg_verifybackup` run with backup application name `bootstrap_backup`,
distinct from `provisiond_sync`, without requiring an additional application/incident commit or
lowering synchronous commit mode. [W1](canonical-w1.json) supplies the initial empty-history
bootstrap proof; standby rebuild preserves authority with no incident. No maintenance commit-mode
exception was used. The known P2 lack of duplicate simultaneous backup-sender/no-migration
observations remains; the report does not infer those observations from a constant summary.

## Per-scenario activation and arms

Every row below is `pass` with child exit 0 and cleanup `pass`, in the final serial run.
[The machine-readable inventory](scenario-inventory.json) retains activation, actual assertions,
independent evidence, uncertainty and cleanup separately for all 21 cases. A CNF pointer names
only the stated arm, never the whole item.

| Selector / evidence | Planned IDs / arms | Activation and actual assertions | Remaining uncertainty / exclusion |
|---|---|---|---|
| [w7_ordinary](scenario-w7_ordinary.json) | REC ordinary; CNF-365/370 unchanged-history restart arms | Two actual process starts and an intact-primary crash/restart; queued row unchanged, no incident, distinct UUIDs and fresh checked proxy sessions. | No promotion or saturation claim. |
| [w7_rebuild](scenario-w7_rebuild.json) | REC-03; CNF-365 standby-replacement/bootstrap subset | Quiesce, stop old peer, verified physical backup from intact authority, authenticated replacement/catch-up; resume with no incident. W1 separately proves initial bootstrap before schema/incident commits. | No simultaneous duplicate backup-sender exclusion/no-migration observations; known P2 gap. |
| [w7_lossless](scenario-w7_lossless.json) | REC-01/02, DB-01; CNF-365 lossless/no-replacement, selected CNF-295/302/370 | Independent accepted order plus running null-marker and queued creates survive real promotion. Old session/new connection/old launch identity are fenced. Actual establish with no peer refuses before BEGIN; same retained event acknowledges after verified replacement. Quarantine, token replacement and surviving counter reseed are asserted. | No-peer positive arm is admission refusal, not a sent-COMMIT SyncRep claim. No production fencing proof. |
| [w7_continuation](scenario-w7_continuation.json) | REC-04; CNF-365 continuation and CNF-302 nonzero retry subset | Terminate engine after committed step 2; same-ID/instant/predecessor retry preserves entire row. Mismatches refuse. Marked step 3 preserves reissued token/index/end; successor after wall end interrupts fresh running work without requarantining new queued work. | Use w7_progress_retry_observed for step-2 evidence; final causal field named step2 is a later snapshot. |
| [w7_supersession](scenario-w7_supersession.json) | REC-05; CNF-365/302 supersession subset | Actual second promotion with predecessor at step 3: distinct fresh record, retained closed predecessor, null grace/zero steps. Old-ID retry cannot replace successor; new step 3 bumps credentials again. | No process-crash supersession. |
| [w7_lossy](scenario-w7_lossy.json) | REC-07 and REC-05; CNF-297, selected CNF-295/365/370 | Verified backup inherits open predecessor. Later acknowledged deposit/payment/operation/machine/gone rows are actually removed by history replacement while independent effects survive. Fresh incident skips before API mint; next index exceeds every removed index/address. Both rails replay once, expired watch excluded; known correlator resolves once, missing correlator stays unresolved; first pass records no absence, second confirms gone; reports unrecorded resources on both accounts. | Extension/commitment/runway state is explicitly seeded, not an extend-runway route. No general metering, arbitrary rollback/non-reuse or reconstructed entitlement claim. |
| [w7_markers](scenario-w7_markers.json) | Recovery contents; selected CNF-295/302 | Seed old queued/running install and unresolved rescue metadata, plus running/queued parents. New event invalidates provenance; wire reads show unknown. Fresh false metadata survives successor and changes only on later real promotion. Real parent confirmation/cancellation; confirmed interrupted parent returns queued after freeze with no second disposition. | Historical metadata only; no install/rescue admission, disk write, rescue session or fan-out execution. |
| [w7_grace](scenario-w7_grace.json) | Grace; selected CNF-99/218/271/308/312 restore claim arms | Seed actual supported system delete attempts with current suspension and both retry origins. Null/future grace defers without new marker/fence, stores original end, then dynamically lifts while incident remains open. Funded unfenced retry aborts; inherited-fence retry deletes. Real TTL/settled retention freeze and resume. | System attempts are seeded; enqueuers and no-rate steps 4–5 remain excluded. Caller deletion is tested separately. |
| [w7_paused](scenario-w7_paused.json) | Paused grace; selected CNF-218/308/312 effective-time/returned-rate arms | Exact stamped history yields USD 6 and EUR 16 rate-present seconds through stale evidence, older late rows, equal acceptance timestamps and returns. Live unfunded/funded attempts use ordinary controlled rate outage/return updates; funded abort precedes pause, unfunded attempt stays unfenced until remaining time is spent, successor preserves original end. | Complete controlled fixture verdicts, not production quorum/window estimator; no extension/no-rate lifecycle claim. |
| [w7_close](scenario-w7_close.json) | Close; STO-56 and selected CNF-295/218 | Actual CLI refuses unmarked/null/future grace and unmet parent/currency obligations. Wait for both account second passes, cancel parent, independently discharge currencies via seeded current fences; clearing a fence blocks again. Only satisfied current disposition closes; subsequent engine startup is ordinary. | Fence/currency inventory is labelled seed metadata. Private diagnostics confirm the actual prerequisite refusals. |
| [w7_reporting](scenario-w7_reporting.json) | REC-06; selected CNF-295/365 absent/invalid/no-hold arms | Malformed initial file acknowledges unknowns; lose file after committed step 2 and retry same ID with unchanged report/progress. Missing file on a new event also acknowledges. Independent lower/upper timestamp/LSN objects stay unknown/invalid, procedure completes and no finite duration is invented. | Certified-known lower/upper/lagging-standby arms not run; certificate ingestion unavailable. |
| [w7_default_hooks](scenario-w7_default_hooks.json) | Ordinary-build negative environments | Default-feature binaries continue/retry and supersede while each defect environment is supplied. Real no-peer establishment still refuses despite no-ack defect environment; after bootstrap the retained new event completes normally. | Not merely a harness flag check; ordinary runtime admission is exercised. |
| [w7_interruptions](scenario-w7_interruptions.json) | REC-04; CNF-365 continuation and CNF-302 retry subset | Proxy observes establishment before COMMIT and after committed reply withholding; terminate real CLI in each window, retry retained ID. Terminate engine within unmarked step 2 and API before step-3 mark; successor completes. Listener is down before mark, sequence moves only forward, marked continuation keeps replacement token. | Interrupted diagnostics are not interpreted as rollback or proof of nonexecution. |
| [w7_credentials](scenario-w7_credentials.json) | API-56/WIR-38; selected CNF-290 recovery replacement arm | Real HTTP authenticates before malformed body, rejects spending authority, allows pending/suspended recovery. Concurrent revocations return generations 2/3 with only latest live. Hold acknowledged generation-5 reply then disconnect; same key refuses without replacement, fresh key returns generation 6. Inspect retained replacement columns and unchanged money. | No bearer replay body. Final old constant latest_generation=4 is stale after added lost-reply subcase; generation 6 is asserted and separately recorded. |
| [w7_legacy_upgrade](scenario-w7_legacy_upgrade.json) | STO-12/13 and CNF-302 provenance migration subset | Build schema-3 legacy metadata with nullable provenance; offline actual migrator backfills settled null true, preserves existing false and rescue endpoint. Both real components refuse legacy incident before advancing generation. | No production schema-2 installation or actual rescue execution. |
| [w7_parent_retirement](scenario-w7_parent_retirement.json) | STO-14/25 and incident disposition regression | Cancel held parent through recovery entry. Expire seeded historical receipt; actual API housekeeping removes operation after freeze, API keeps serving, incident disposition and retired kind survive. GET returns 410 and reused tenant key conflicts. | Historical parent/receipt seeds; no fan-out admission. |
| [w7_caller_grace](scenario-w7_caller_grace.json) | OPS-39/41 caller boundary | Real caller deletes succeed before step 3 while end is null, then after step 3 while its stored end is future. Independent provider history and actual deleted machine state retained. | Selective system gate is independently exercised by w7_grace. |
| [w7_coherent_rates](scenario-w7_coherent_rates.json) | STO-49/LDG-58/59 controlled-input coherence | Ordinary USD rate UPDATE appends the shared history, stamps null outage then yielding return; current/historical accepting boundary agrees. No independent manually fed USD recovery history. | General feed construction and production source freshness selection remain deferred. |
| [w7_attachment_order](scenario-w7_attachment_order.json) | STO-56 current-disposition ordering | Two independent lost-reply purchases survive. Actual authenticated operator attachment before close introduces live unfenced machine and refuses close; seeded current fence permits close. Actual attachment afterward leaves incident closed and receives no restore grace. | Serial before/after ordering with real surfaces; no-rate cancellation execution remains not run under D-46. |
| [w7_expired_replay](scenario-w7_expired_replay.json) | STO-54(3)/29/30/31/32, LDG-47/57; selected CNF-365 pre-request payment arm | Real rail accepts Lightning/on-chain settlements before the retained binding expires. Hold ordinary watching and step-3 mark; assert two expired payments, one live comparison, three linked ledger credits, exact 215 balance with listener down. Interrupt before mark; successor replays once. Marked successor preserves progress/token. | Only expiry metadata is seeded. Expired binding stays outside active watches; no lost binding is invented. |
| [w7_reporting_progress](scenario-w7_reporting_progress.json) | STO-54(2)/56; selected CNF-365 progress arm | Hold step-2 write, then hold its committed reply on the same session. The old restart-pass readiness point is reached; the fixed waiter keeps engine running even with primary mark visible, until acknowledgment reaches the engine. | Isolated scheduling/delivery proof; deliberate phase-interruption scenarios remain separate. |

The current canonical W7 phase requires and records all 21 unique children, activated causal traces and completed cleanup.

## Regressions and sensitivity

[Five inherited-candidate regressions](regressions.json) are distinct from sensitivity controls.
All five have final recorded red manifests with exact assertions, exit 1 and cleanup, paired
with the final 21-child greens. Historical protection summaries are distinct from those fresh
reproductions. [Expired replay proof](expired-replay-regression.json) records hashes of all 40
protected runtime/migration/dependency files on the earlier candidate. The original private
source archive is unavailable because the second Run's private raw proof was deleted before
finalization. A separate post-red filesystem hash was not measured before repair; no such
measurement is inferred. Red has
zero expired credits and balance 161; green has both expired credits and balance 215 before the
step-3 mark/listener. The expired_replay function is byte-identical; its containing module later
gained the separate progress test. Both runs finish assertions/cleanup with their recorded status.
These are introduced candidate defects, not old merged-runtime claims. No new unchanged-review-
base defect was established; absent W7 setup there is not a valid red.

The reporting race is covered by an isolated real-engine scheduling/delivery gate, not a claimed
old-code red. It observes `restart_pass_complete` before step 2, then a primary-visible mark while
the COMMIT reply is withheld. The fixed fixture waits for both persisted progress and the engine's
post-acknowledgment event before stopping. Existing deliberate interruptions remain independent.

The five required isolated test-build controls are distinct:

| Fault | Observed activation and intended failing assertion | Child result |
|---|---|---|
| [Retry resets progress](controls/red-w7_retry_reset.json) | steps 2 before / 0 after matching establishment; `STO-56 same-ID establishment must preserve committed steps=2` | expected negative demonstration, exit 1 |
| [Lossless skips incident](controls/red-w7_lossless_restart.json) | three rows survive, acknowledged no-op leaves incident absent; `STO-54 lossless promotion must establish incident before component startup` | expected negative demonstration, exit 1 |
| [Startup before acknowledgment](controls/red-w7_no_ack.json) | real engine takes startup lock and its generation COMMIT waits in SyncRep with no replacement/incident acknowledgment; `STO-54 no component may start before synchronous incident acknowledgment` | expected negative demonstration, exit 1 |
| [Supersession inherits marks](controls/red-w7_supersession_inherits.json) | predecessor/new record both step 3 with inherited non-null grace; `STO-56 supersession must start fresh progress and null grace` | expected negative demonstration, exit 1 |
| [Invent finite duration](controls/red-w7_finite_guess.json) | uncertified event-time bounds produce false finite claim; `STO-54 bare timestamp or LSN must not establish finite loss duration` | expected negative demonstration, exit 1 |

The parent accepted only exact assertion/exit/activation/cleanup, not setup failures, timeouts or
missing faults. Unchanged-source normal selector runs are the 21-child greens. Ordinary default
builds ignore the defect environments; the no-ack control is absent there. All previous controls
also produced their required recognized negatives in [the current controls run](controls.json).

## Unknowns, residuals and not-run owners

| Item | Result | Owner / reason |
|---|---|---|
| Certified-known lower/upper bounds, lagging standby with certified prefix, clock/WAL inversion/ancestry/hole certificate variants | not run | Deployment operator: independently bounded source/event clocks, retained matching continuous-prefix and stopping/fencing certificates unavailable; certificate ingestion is unimplemented |
| Different-person rehearsal before first customer payment (CNF-295) | not run | Deployment operator; automated self-rehearsal is not its discharge |
| Separate failure domains, production fencing, real provider/rail behavior, production recovery point/gap/capacity/alarms | not run | Deployment operator and later authorized work; only local owned processes/simulators were used |
| Actual install/rescue admission/execution, raw-disk effects and parent fan-out | not run | Deferred workers; seeded historical metadata tests only provenance/quarantine/disposition |
| Extend-runway and survival by extension | not run | D-44/deferred work; the residual extension state is a labelled seed |
| OPS-41 no-rate steps 4–5 and full outage-bound disposition traces | not run | D-46; reached entry stays safely refused; no-rate production lifecycle not invented |
| General metering, production rate estimator and full CNF-99/218/271/295/302/308/312/365/370 | not run | Deferred owners; only listed restore arms were exercised |
| W8 saturation/offline-release project and W9–W11 | not run | Outside authorized work |

Payment replay does not reconstruct a lost extension/commitment or all vanished financial/meter/
rate/gone history. Restored runway moves backward. A consistent meter audit proves no absence of
history loss. Missing correlators and unknown external effects stay unknown/operator-owned.
The finite gap of 10000 covers only removed indices observed in this fixture and is not a proof
of address non-reuse under arbitrary unbounded rollback. No production value or missing
precondition is invented.

## Evidence limits, tracker and cleanup

[Artifact policy](artifact-policy.json) identifies preserved inaccurate summary fields. Raw W7
no-product-incident/no-external-effects labels are false; constant causal summaries are not
independent measurements. Actual child assertions, database state, application traces and
independent simulator histories own the arm claims. Those existing lower-priority matters were
not repaired here.

[Historical touched/adjacent bead dispositions](touched-beads.json) retain their original IDs,
statuses, source hashes and reasons as historical records, not a newly executed tracker review.
The later tracker-only commit `c3b5cb5` filed 22 follow-ups. This finalization makes no tracker
write, claim or follow-up repair.

The second Run's private raw proof was deleted from the host before finalization. Its accepted
identity remains provable from the committed source map; historical hash summaries do not
restore the deleted logs/archive. This limit is separate from the final serial proof, retained
privately with stream/manifest hashes. Current simulator watch membership is **derived/trace
only**: the rail returns settlement history and implements no real per-address subscription.
An expired ID's exclusion demonstrates the query boundary, not production watch enforcement.

Final serial child manifests retain cleanup observations after assertions and owned-resource
removal. [The publication audit](publication-audit.json) records operational identity, path/secret/
log checks and local evidence-link checks. No finalization build or test ran. This remains a
local simulator-only rehearsal with no real provider accounts, purchases or funds.
