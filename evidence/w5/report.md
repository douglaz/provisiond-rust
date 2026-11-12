# W5 interruption and ambiguity evidence

The supported W5 paths are implemented and the final recorded commands exited 0.
This remains a reduced internal prototype. Independent acceptance is separate from execution;
no automatic reconciliation, production availability, incident recovery or real funds are claimed.

## Source and commands

Specification: `ac37c7cae5d3ff746515137774a4cdd5a3531375`.
Whole-change review/regression base: `aceea4eea8382c8d3b6fa554706c54920999369c`.
Tested runtime and fixtures: `c7e71c803f7c10158636ff908856507713a608cb`, clean in every phase.
The [source inventory](tested-source.json) records exact runtime, fixture, migration, dependency,
Nix and specification-lock hashes. Later documentation/evidence commits preserve those bytes.
[Implementation commits](implementation-commits.json) covers the entire W5 history from the base,
including the earlier interruption hooks, retry fixes and fixture corrections; history was preserved.
The observer runtime, canonical fixtures and capacity assertion are respectively `0e1348f`,
`8ac8737` and `d21297c`. Commit `c7e71c8` adds discriminating closure/release coverage and
the actual blocked-settlement successor. Its runtime addition is an explicit test-build
observation and isolated skip-closure control; no new production defect or policy change is claimed.
Nothing was pushed, merged or published.

[Validation](validation.json) records complete stdout/stderr hashes, true statuses saved before
reading either stream, effective environment/configuration and exact argv. Raw paths are private.

| Command | Observed exit | Result |
|---|---:|---|
| `nix develop --command bash tools/check.sh` | 0 | pass |
| `nix develop --command bash tools/postgres.sh --seed 20261112 --evidence <raw-final-manifest>` | 0 | pass; all ten phases |
| `nix develop --command python3 tests/postgres/w5_baseline.py --evidence <private-baseline-manifest>` | 0 | pass; recognizes three intended child exits 1 |
| `nix develop --command python3 -m unittest discover -s tests/postgres -p 'test_*.py'` | 0 | pass; eight fixture tests |

The [aggregate](final.json) includes w1, checks, commits, regressions, corrections, core, workflow,
default-workflow, w5 and controls, each exit 0. The first nine phases contain 203 scenario entries
(including reused sub-results); W5 has 26 required selectors plus three reused sub-results.
All 35 runtime sensitivity children exited 1 at their exact assertions after activation and cleanup.
Those children are expected negative demonstrations, not 35 implementation regressions.
The separate direct release-helper probe accepts zero checkouts (child exit 0) and rejects retained
counts one through three (each child exit 1 at the exact release assertion). These synthetic
records establish fixture sensitivity; the real PostgreSQL release observations remain required.
Focused runs cannot satisfy the complete W5 inventory, including its eight activated accounting cases.

Versions: stock PostgreSQL tools 18.6, Rust 1.97.1, Cargo 1.97.0, Python 3.14.7, Nix 2.34.9,
tokio-postgres 0.7.18, tokio 1.54.1, deadpool 0.12.3; Linux x86_64. The exact dependency inventory
and pinned nixpkgs revision are in the source inventory. Seed is `20261112`.

The fixture uses one primary and one authenticated designated synchronous physical standby,
`synchronous_commit=on`, one API read/write checkout and three engine write checkouts. Checkout is
250 ms, statement/idle transaction 1000 ms, lock 500 ms, store-call/retry 2000 ms, connection check
3000 ms, startup wait 500 ms and stop record 1000 ms. Sent COMMIT has no deadline. None was widened.
The reused verification-stop search fixture uses its disclosed five-second simulator call allowance,
restores it after assertions and leaves store/stop bounds intact. D-6 remains Open for production.

## Activated scenarios

Every row below has result **pass** for its stated arm. [Scenario assertions](scenario-assertions.json)
records each causal trace: complete operation/machine/ledger/commitment/exclusion/seed snapshot,
independent simulator events, original identities and actual successor state. [Final W5](final-w5.json)
adds application decisions, attributed protocol faults, configuration and cleanup. An activated gate
proves receipt at that boundary; application `Acknowledged` proves observed transaction acknowledgement;
SQL visibility proves durable state visible on the current history; simulator acceptance/resource
events prove independent external effect. These meanings are not interchangeable.

| Selector | Actual fault, decision and final restart state | Test ID / discharged arm |
|---|---|---|
| `w5_admission_before` | Hold before possible admission COMMIT and sever its transport. Original operation/receipt/commitment/counters stay absent; no success claim. Same-key resend follows stored outcome, then succeeds with one order. | API-11, STO-23/35; admission interruption |
| `w5_admission_reply` | Lose HTTP delivery after the original owner actually acknowledges admission. Independent original receipt identifies the operation; canonical replay equals it, changed fingerprint conflicts, one purchase succeeds. | API-11/12/38; admission replay |
| `w5_claim_reply` | Independently observe committed claim and lose its real reply. Fatal claim uncertainty, no blind claim/worker/provider. Actual successor fails the same null-marker create, releases reserve and posts no debit. | ENG-03, OPS-49/15; claim uncertainty |
| `w5_step8` | Acknowledged marker, zero step-9 mutation. Surviving failure/stop fails/releases by worker position; crash successor leaves marked create unresolved with reserve. Lost-ack account/tenant guard repeats remain exercised. | OPS-43/11/15; CNF-362 selected create and ENG-04 guard arms |
| `w5_accepted_lost` | Independent accepted order and actual resource, lost reply, unknown accepted knowledge retained. Operation/error/terms/reserve stay unresolved through restart and keyed replay; no second purchase, machine or debit. | ENG-05 interruption/no-repeat only; W6 resolution not run |
| `w5_settlement_blocked` | Retained verified outcome/read instant precede real pre-COMMIT transport failure. Original failed session disappears; zero outstanding worker checkouts and a released component permit precede retry. Whole repeat succeeds without purchase, verification or constraint I/O repeat; an actual successor for the same operation preserves succeeded status, the complete durable snapshot, provider evidence and one order. | ENG-06 pre-COMMIT retry; OPS-49 |
| `w5_settlement_reply` | Success and deterministic-release settlements commit, then acknowledgement is lost. Guarded whole repeats and successor preserve complete status, machine, money, exclusion and seed rows; debit/release occurs once. | ENG-06; STO-3/23/28, OPS-49 |
| `w5_stop_record` | Second stop enters real existing-deadline arbitration. Post-call record/deadline remain identical, fatal later-stop result is observed and no later BEGIN occurs. Cut-off/returned precommit and sent-record arms accept actual server fate; returned-land keeps final outcome. Actual successors classify only remaining running rows. | OPS-21/22/49; CNF-362 selected create/record arms |
| `w5_lock_loss` | Independently attributed idle lock backend is terminated. No claim after detection; older record keeps deadline, returned outcome survives. A real SyncRep freeze expires at original stop bound with unobserved work logged; successor follows durable state. | ENG-02/07; selected CNF-366 create/record and freeze arms |
| `w5_retained_checkouts` | All configured API write slots (one) wait in SyncRep; HTTP handler cancels while original live transport, pooled checkout and permit remain owned. Fresh key gets 503 before BEGIN/receipt/provider, established checked read works. Restore produces original actual Acknowledged classification; identify receipt/operation before exact-identity replay. Fresh checked SHOW remains on; one purchase succeeds. | DB-02, CNF-296 retained-write arm; CNF-299 named overload arm |
| `w5_process_death` | Precisely kill API with residual SyncRep backend. Two backed-off replacements fail closed and stop; ordinary pools exclude the old backend. Quiesce, restore the same standby, observe residual completion and original persisted receipt/operation before restart/resend. Replay equals original; one purchase succeeds. No posthumous client acknowledgement is claimed. | DB-03, CNF-373 local residual/replacement/unchanged-history subset |
| `w5_connection_check` | Independently establish healthy authenticated designated history/catch-up before withholding each API/engine check reply. Uncertain connection closes unused at existing bound; no fallback, check write, business entry or check-created SyncRep. Actual later startup succeeds. | DB-09; CNF-365 withheld-check arm only |
| `w5_contention` | Owned business-row blocker is observed in server blocking identities while standby is paused. Per retried attempt, attributed ROLLBACK/idle acknowledgement, zero outstanding worker checkouts and a released semaphore precede backoff. Bounded exhaustion exits with retained provider outcome, one order and no settlement; actual successor leaves marked create unresolved. | ENG-06, OPS-49; selected CNF-296 contention arm |
| `w5_remote_completion` | Simulator accepts before held effect; lock loss causes actual record/exit. Independent resource appears afterward. Generation fencing never claims cancellation; no machine/debit or repeat across unresolved successor. | OPS-47/50 limitation; ENG-05 no-repeat subset |
| `w5_generation_record` | Out-of-band unequal generation refuses a real record immediately, without retry/new transaction. Separate lost-record-reply arm preserves the earlier commit before refusal. Actual successors preserve the server's result; no provider order. | ENG-01 selected record path; CNF-367, OPS-50 |
| `w5_default_hooks` | Separately built ordinary binaries ignore fault files, blind-retry environment and deadline-reset environment. One create succeeds and one dropped-reply create remains unresolved, one order each. | Default-feature inertness; no runtime fault activation claimed |
| `w5_claim_precommit` | Definitely unsent claim fails verification, releases owner resources and repeats the complete claim within bound. One worker/order succeeds; successor remains succeeded. | OPS-49 unsent-claim exception; protected baseline red |
| `w5_backoff_budget` | Real defer primitive fails just before retry exhaustion; ordinary backoff crosses the bound. No subsequent BEGIN, fatal exit 71, no provider call; null-marker successor fails/releases. | OPS-49 pre-next-attempt exhaustion; protected baseline red |
| `w5_accounting_reply` | Healthy replication, completed settlement, repeated independent idle:ClientRead samples, delayed/lost COMMIT reply. No in-flight exit. At receipt death, delay charges the bound, exits 1 without retry, preserves one purchase/money and succeeded successor. | OPS-49 non-standby reply charging; protected baseline red |
| `w5_accounting_syncrep` | Actual long SyncRep exceeds retry bound with sent owner retained; no accounting-triggered exit. Measured wait is exempt; lost reply allows guarded database repeat within the measured charged budget. One purchase and full row equality through succeeded successor. | OPS-49/STO-7 actual standby exemption and completion-only accounting |
| `w5_accounting_mixed` | Actual long SyncRep followed by independently observed completed-COMMIT idle delay. Wait is exempt; later independently observed idle reply hold charges. Exit 1 without repeat after receipt death; succeeded successor and unchanged money. | OPS-49 mixed interval attribution |
| `w5_accounting_unavailable` | Withhold actual observer response through its existing call deadline while settlement reply is delayed. Loud observer timeout, zero exemption, charged delay and exit 1 on receipt death; money and succeeded successor unchanged. | OPS-49 failed/unavailable sample charging |
| `w5_accounting_mismatch` | Alter only backend_start in actual observer DataRows. Identity refusal/loud failure, zero exemption despite same pid, delayed reply charges and exits 1; succeeded successor with unchanged money. | OPS-49 exact backend identity |
| `w5_accounting_stop` | Sent settlement actually waits in SyncRep while first after-return stop starts its original bound. Original stop expiry exits 71 before COMMIT receipt; restore observes residual completion and succeeded successor. No fictitious acknowledgement. | OPS-21 original bound precedes accounting; STO-7 ownership |
| `w5_accounting_reconnect` | Precisely terminate attributed idle observer, alarm and open a checked replacement. Replacement actually attributes SyncRep on a later settlement; one purchase succeeds and stays succeeded across successor. | OPS-49 observer re-establishment for later attempts |
| `w5_accounting_failure_run` | Independently observe real settlement SyncRep and an established matching sample run before precisely terminating its idle observer. Test-only state shows closure; the skip-closure control observes the run still open and fails that invariant. In the ordinary run, independent server samples and completion arithmetic show over two seconds of unobserved wait charged while the original sent checkout stays retained. Restore the same standby, admit a checked replacement, lose the reply and observe ordinary exhaustion, unchanged money and one purchase through succeeded successor. | OPS-49 failed-run closure invariant plus separate actual charging; no reconnect during stall assumed |

All phase and control cleanup records are pass. The runner reaped its children, shut down only
its own database directories, removed registered build directories and released its ten-port
allocation after assertions. No individual SyncRep backend was cancelled or terminated.
The idle lock/observer terminations were independently attributed and excluded SyncRep.

## OPS-49 implementation inventory

| Site | Complete unit / verification and exceptions |
|---|---|
| `Engine::order_step` → `transaction_stoppable` | Whole step-8 account/tenant/correlator transaction, not its last statement. Step-9 dispatch requires acknowledgement. W5 step8 and existing workflow guard arms. |
| `Engine::settle`, `start_settlement`, `begin_settlement` → `transaction_record`/`transaction_stoppable` | Retained provider outcome and confirming-read instant stay outside the retry closure; whole guarded settlement repeats. W5 blocked/reply/contention/accounting arms and core settlement tests. |
| `Engine::register_stop` → `transaction_record` | Shielded cut-off/after-return record repeats within both original bounds. True later stop, expiry or supersession ends without another attempt; lock loss leaves an older deadline alone. W5 stop/generation/lock-loss and existing core record matrix. |
| `Engine::defer_owned` → `transaction` | Process-owned whole defer; claim-number guards preserve replacement execution. Core defer cancellation/claims and W5 backoff crossing. |
| `Engine::preflight_value` | Each complete autocommit account, tenant and channel SELECT, including acquisition/check/execution/backoff. Failed session releases before backoff; provider I/O remains outside. Final workflow preflight recovery/exhaustion/stop arms. |
| `Engine::claim_owned` | Known UNSENT errors may repeat the complete claim under the original budget. Sent/uncertain reply is immediately fatal, with no blind claim/worker/provider; successor handles actual row. W5 claim-reply/precommit and core claim guards. |
| `EngineDatabase::start`, `restart_pass` | Startup work does not acquire worker retry semantics. Generation advance is its own first write after lock, with required statement/lock timeouts; error/lost reply is fatal before claims. Observer reads precede it but add no write. |
| `Component::begin_entry` | Generation mismatch rolls back and exits superseded without retry/new transaction. Status/claim-number refusal remains distinct. Licensed remote calls are not retracted. |
| Constraint/serialization/lock errors | Constraint errors do not enter the store-error loop. Serialization repeats immediately without charging that retry budget. Lock timeout is charged, ends transaction and releases resources before backoff. Stop-record time counts every repeat and never resets. |
| API, credit ingestion, migrations | No engine decorator. API caller error/keyed replay and rail replay retain their own semantics; migrations keep their startup/connection-failure rules. |

Only represented create paths and core primitives are exercised. General periodic schedulers,
all other operation kinds, resolution, incidents and full metering remain outside this implementation.

## Observer arithmetic, capacity and limits

`EngineDatabase::start` opens exactly one checked lifetime read-only observer after startup-lock
acquisition and before generation advance, outside the pools and separate from compatibility and
lock sessions. API opens none. Engine steady capacity is six sessions; startup includes the transient
advance for seven. Combined API/engine startup share is nine of 21 ordinary slots in the local
24-connection fixture, before other phase-appropriate headroom. The core capacity negative includes
18 workers: 20 slots under the old formula fit, while the required 22 under the new formula refuse.
No pool bound, PostgreSQL patch, build dependency or privilege grant was added.

Before first polling COMMIT, `own_commit` registers owner, pid, backend_start and dispatch instant.
While registered, one autocommit activity query per fixed 20 ms period requires exact identity,
active state, IPC/SyncRep and store COMMIT text. Choose uninterrupted matching runs; failed/missing/
nonmatching samples or reconnect end a run. No span stays open through observer loss. Each run
exempts `max(0, last_sent - first_received)`. Complete attempt elapsed to receipt delivery minus
that exemption is charged once, including verification, handoff, idle reply delay and read-back;
ordinary backoff charges separately. [Accounting evidence](accounting.json) retains attributed
run endpoints, counts, round trips and exact arithmetic, with raw-sample hashes mapped privately.

The long SyncRep case exempts the measured matching span and permits its bounded database repeat.
The mixed case exempts standby wait but charges the later idle reply hold; healthy idle delay and
unavailable/mismatched idle observation exempt zero. The failed-run case additionally establishes
positive matching endpoints before observer death, closes that run, retains its measured exemption
and charges the independently observed gap at completion. [Accounting evidence](accounting.json)
retains registration, failure/closure, checked replacement and completion identities and arithmetic.
The isolated skip-closure control rejects an open run at failure, an internal invariant proof.
The ordinary completion trace supplies the separate end-to-end charging proof; no second SyncRep
wait or impossible checked reconnect during the stall is synthesized.
Application-role visibility and checked observer replacement are executed observations. Under the
one-contiguous-wait premise, normal edge error is only toward charging, approximately
`2 × (period + sample round trip)` per uninterrupted run; missing observation can charge much more.
The PostgreSQL 18.6 source check of one contiguous SyncRep wait per COMMIT is **unverified** here.
Real-server traces establish these selected histories, not that uninspected source premise.

Accounting is completion-only: it cannot trigger exit while a sent COMMIT remains outstanding.
Original OPS-21 bounds and process exit remain independent. Two accepted known limits must remain:

- Observer loss during a standby stall followed by lost COMMIT reply can cause one alarmed ordinary
  exhaustion exit without money loss. This triple-fault limit is accepted, not repaired by expansion.
- A live transport answering keepalives but withholding the COMMIT reply forever causes no accounting
  exit. There is no new in-flight exhaustion or sent-COMMIT deadline.

## Negative demonstrations and source protection

[Controls](final-controls.json) lists all 35 exact assertions, child exits and individual red evidence.
Recognition requires actual activation/unsafe observation, child exit 1 at the intended assertion
and cleanup pass; setup failure, inactive hook or timeout cannot count. The five W5 controls are:

| Defect | Actual unsafe observation / intended assertion |
|---|---|
| `w5_observer_run` | A genuinely established SyncRep sample run remains open after observer transport death when only `end_run` is skipped; `OPS-49 observer failure must close an established SyncRep sample run`. |
| `w5_stop_reset` | Real second-stop arbitration changes the original deadline and returns nonfatal success; `OPS-21 later stop arbitration must preserve original absolute deadline`. |
| `w5_generation` | Unequal generation permits a stale record; `OPS-50 stale generation record must refuse without retry`. |
| `w5_checkout` | Actual original pooled checkout is absent despite live cancelled-handler transport/process in SyncRep; `STO-7 HTTP cancellation must retain original sent-COMMIT checkout`. |
| `w5_blind_retry` | Two independently accepted orders and actual resources; `OPS-12 lost reply must not dispatch a second purchase`. |

The deadline-reset control is a test-driver sensitivity correction, not an old-runtime deadline-reset bug.
The run-closure control is also sensitivity coverage; the ordinary runtime already closed the run.
[Release-assertion evidence](release-assertion.json) records the original helper accepting a retained
checkout count of one and the corrected helper rejecting it while accepting zero. Its canonical
child exits are checked independently of the real blocked-settlement/contention greens. The
earlier complete-record parser correction remains tested by the final Python command, without
crediting another runtime regression. A first continuation full run stopped on the old capacity
diagnostic assertion; the final core fixture checks the changed budget and the complete rerun passes.
That interrupted run supplies no final acceptance evidence.

[Regressions](regressions.json) records three real old-runtime defects: known-unsent claim handling,
backoff exhaustion before another BEGIN, and non-standby COMMIT reply delay. All final drivers ran
against unchanged `aceea4eea8382c8d3b6fa554706c54920999369c` in an invocation-owned export.
Only fixture files were overlaid; no branch/tag, working checkout reset or runtime patch was made.
[Protection proof](source-baseline.json) compares every runtime/migration/dependency/Nix hash before
and after, and binds the exact final assertion-driver hashes to the final green. All three reds
exit 1 at the intended assertion after real activation and cleanup; recognizing parent exits 0.
The historical 2197 ms / 21 ms accounting reproducer remains identified in that final comparison.

## Deferred work, review limits and evidence handling

- W6 automatic resolution remains **not run**: accepted/lost and late remote effects remain unresolved,
  retain their reserve/evidence and produce no second purchase. This does not complete ENG-05.
- W7 incident establishment, promotion, standby replacement and operational fencing remain **not run**.
  The W5 recovery drill restores the same designated standby on unchanged intact history.
- W8 production parameters, supervisor/reconnect policy, full saturation including loss of fresh
  observer access and full offline-release recovery remain **not run**. D-6 stays Open.
- Broader CNF-296 funding/deposit/rail arms, CNF-365 promotion/backup/destination arms, unsupported
  CNF-366 kinds, every CNF-367–370 write/startup/restore arm and deployment-wide CNF-373 obligations
  are not discharged by the selected W5 rows. Existing W1–W4 tests ran, with their original scope.
- Full operation-kind coverage, rescue installation, customer UI, real drivers/funds and W9–W11
  remain outside this work. Deferred rail-history response delivery, descriptor startup deadlines
  and receive-WAL fixture hardening were not implemented.
- Source premise inspection and the live-transport-forever history remain **not run**. The
  failure-run scenario executes one observer-loss/stall/lost-reply history and observes the accepted
  conservative exhaustion limit without money loss. Neither this history nor the measured checked
  replacement supplies a production recovery-duration guarantee.

Review-followup: the raw harness's top-level W5 external-effects label predates the W5 selector and
says “not applicable” despite the nested independent simulator traces. Final sanitized summaries
explicitly identify simulator evidence; the raw label supplies no behavioral authority. This implementation report
does not supply an independent acceptance verdict. No specification conflict remains stopped.

Only final sanitized manifests, assertions, reds, source protection, this report and audit remain
under `evidence/w5/`. `<private-run>`, `<private-harness>`, `<private-allocation>`, `<supervised-unit>`,
`<repo>`, `<owned-run>`, `<dependency-path>` and `<python>` denote sanitized paths or environment
identities; `<credential>` and `<redacted>` replace private values. The private raw-path/hash mapping
retains exact identities. `<raw-final-manifest>` and `<private-baseline-manifest>` denote private
manifest output files. Raw logs, source
exports, status files, database/WAL, credentials and superseded attempts remain outside git.
Incomplete final diagnostic tails are **inconclusive** diagnostics only and cannot establish behavior.
The [audit](audit.json) records source equality, stream status/hash checks, cleanup and redaction.
