# W5 interruption evidence

The implemented local interruption inventory passes. **Full W5 acceptance remains open:**
OPS-49's non-standby sent-COMMIT accounting path is stopped with a reproduced failure and a
pending measurement/scope decision (D-36). A successful canonical command does not close that gap.
No automatic reconciliation, incident recovery, production deployment or real funds are claimed.

## Identity and executed commands

Specification: `ac37c7cae5d3ff746515137774a4cdd5a3531375`.
Unchanged-runtime regression base: `aceea4eea8382c8d3b6fa554706c54920999369c`.
Final tested runtime and fixtures: `7b26aed794272668f9960af56fbcb26a57d5d2d6`, clean when tested.
The [operational source inventory](tested-source.json) binds all operational file hashes;
later documentation/evidence commits preserve those bytes. Full phase source inventories,
configuration hashes and actual executable hashes/features/invocations remain in the manifests.
[Implementation commits and changed paths](implementation-commits.json) list the nine ordinary
implementation commits in order. Documentation/evidence changes are a subsequent ordinary commit.

[Validation](validation.json) records whole stdout/stderr hashes and true exits, saved before
reading either stream:

| Command | Observed exit | Result |
|---|---|---|
| `nix develop --command bash tools/check.sh` | 0 | pass |
| `nix develop --command bash tools/postgres.sh --seed 20261112 --evidence <raw-final-manifest>` | 0 | pass, all ten required phases |
| `nix develop --command python3 -m unittest discover -s tests/postgres -p 'test_*.py'` | 0 | pass, six inventory/trace tests; [record](python-units.json) |

The canonical [aggregate](final.json) requires W1, checks, commits, regressions, corrections,
core, workflow, default-workflow, W5 and controls. Each child exited 0. W5 contains all eighteen
required selectors, plus three reused W4 sub-results. Focused, missing, duplicated, failed,
inconclusive or unclean W5 inventories cannot satisfy the parent. The controls parent recognizes
all thirty-three exact negative demonstrations; each deliberate-defect child exited 1.

Versions: PostgreSQL 18.6, Rust 1.97.1, Cargo 1.97.0, Python 3.14.7, Nix 2.34.9,
tokio-postgres 0.7.18; Linux x86_64. The full dependency inventory is in `tested-source.json`.
Seed: `20261112`. Effective server and application configuration is in each final child manifest.
The fixture uses one primary and one designated synchronous physical standby; `synchronous_commit=on`;
ordinary bounded pools; one API read/write checkout, three engine worker write slots and a dedicated
startup-lock session. Application checkout is 250 ms, statement/idle transaction 1000 ms, lock
500 ms, store-call/retry 2000 ms, and stop-record 1000 ms. Sent COMMIT has no client deadline.
W5 changes none of these values. The reused verification-stop search fixture temporarily permits
five seconds for its simulator call, restores it afterward and leaves the stop/store bounds intact.
These are local fixture parameters, not selected production capacities (D-6 stays Open for W8).

Hooks require `store-test-hooks`. The default-feature binaries are built separately, with actual
hashes and invocations, and remain inert with fault files and the blind-retry environment present.
No real-PostgreSQL test was added to default `cargo test`.

## Activated scenarios and exact claims

All rows below have result **pass** for their named implemented arms in
[final W5](final-w5.json). `fault_requested` alone is insufficient: protocol gates have attributed
`fault_activated` events. `w5_causal_trace` records operation/claim, boundary, full local snapshot
and independent provider state. Application traces identify owner/backend and completed decisions;
`w5_process_exit` and `w5_successor` record actual exits and successor results. Independent rail
and provider events distinguish request receipt, acceptance, resource creation and response delivery.
Zero external orders are observed in pre-dispatch arms. SQL visibility alone is never described as
synchronous acknowledgement or remote-effect cancellation.

| Selector | Activated boundary, decision and final state | Planned test IDs / demonstrated arms |
|---|---|---|
| `w5_admission_before` | Hold admission before COMMIT, sever its attributed transport; first response cannot claim success. Receipt/operation/commitment/counters are absent together. Identical keyed resend admits once and completes one order. | API-11, STO-23/35; admission-before-COMMIT arm, distinct from sent uncertainty |
| `w5_admission_reply` | Hold HTTP response after actual transaction acknowledgement, close the client socket. Receipt/operation/commitment exist independently of delivery. Same canonical request/key replays the same operation; changed fingerprint conflicts; one order. | API-11, STO-3/23/35; acknowledged admission with lost HTTP reply |
| `w5_claim_reply` | Actual claim COMMIT completes, its reply transport dies. Engine exits 1 with claim-uncertainty fatal, registers no worker and sends no provider order. Ordinary successor fails the null-marker create and releases its reservation. | ENG-03, OPS-49/15; claim lost acknowledgement, null-marker exception |
| `w5_step8` | Real licensing acknowledgement and nonnull marker before any step-9 invocation. Survived failure/first stop fail by live position, fully release reserve and remove an actual exclusion with no debit/order. Crash retains marker and successor moves it unresolved with reserve intact. Reused licensing lost-ack repeats retain account/tenant refusal guards. | OPS-43/11/15, ENG-04 selected guard, CNF-362 named create arms |
| `w5_accepted_lost` | Independent simulator records one accepted order and resource; reply is lost. Caller observes unknown acceptance. Operation/error/terms/open reserve persist, no machine/debit is invented. Restart and keyed resend send no second order. | ENG-05 interruption/no-repeat only; OPS-11/12/13/27/43, PRV-5/11; W6 resolution not run |
| `w5_settlement_blocked` | Final verified outcome/read instant precedes an attributed settlement transport failure before COMMIT. Engine repeats the whole transaction inside its existing budget, retains that instant and outcome, and atomically succeeds without repeated purchase, verification or constraint read. | ENG-06 selected retry arm, OPS-49/21, PRV-13c; no claim about stopped sent-COMMIT accounting |
| `w5_settlement_reply` | Independent success or deterministic-release settlement is visible, then real COMMIT acknowledgement is lost. Whole-transaction repeat and restart preserve complete operation/revision/time, machine, seed, ledger, references, commitment amount/state/version and exclusions. Release/debit occurs once. | ENG-06 success and release idempotency; STO-3/23/28, OPS-49 |
| `w5_stop_record` | Five arms: cut-off/returned record statement holds; first stop after final return allowed to land; cut-off/returned COMMIT-complete holds. Real later-stop exits 1 without a new record transaction. Earlier server outcome is accepted and successor preserves it. First stop after return retains final outcome, not cut_off. | OPS-21/49; CNF-362 named create/record arms, including sent record |
| `w5_lock_loss` | Kill only the attributed idle dedicated lock backend. Reused search-stop arm covers no earlier record. Older cut-off and returned-record retries preserve actual identity and absolute deadline through loss. True SyncRep freeze expires the original 1000 ms record bound, logs the unobserved claim and exits 71; residual backend is observed, standby resumes, actual successor classifies the persisted result. | OPS-47/21, ENG-07 named create-record freeze; no later-stop/deadline reset |
| `w5_retained_checkouts` | Single configured API write checkout reaches SyncRep; actual HTTP handler cancellation occurs. Owner observation samples the original pooled checkout as held with live transport. Fresh-key overload is 503 before business BEGIN/receipt; an established checked read session still reads. Standby resume lets original admission/replay finish once, and a fresh checkout verifies `synchronous_commit=on`. | DB-01/03, CNF-299 new-key overload; STO-7/55 retained ownership, not global backend capacity |
| `w5_process_death` | Kill exact API child while its backend waits SyncRep; backend survives independently. Two replacement startups with recorded 100/200 ms backoffs exit 1 after failed checks, issue no business work/fallback, and reach a sustained fixture-failure diagnostic. Stop replacements/quiesce, restore the designated standby on unchanged history, observe residual completion and keyed restart with one purchase. | DB-03, CNF-373 local residual/replacement arm only; saturation/observer-loss/recovery W8 not run |
| `w5_connection_check` | Healthy synchronous replication, attributed read-only check response withheld in actual API and engine startup. Session closes unused at its existing bound; both exit 1 with no check write, business entry, SyncRep or fallback. Separate subsequent startup succeeds. | DB-09, CNF-365 withheld-check arm only |
| `w5_contention` | Owned application transaction holds the relevant commitment row, actual blocker PIDs identify the blocked money/FK path, and standby is paused. Whole settlement retries exhaust the unchanged bound, engine exits 1, no provider call repeats. After release/resume successor classifies the actual marked running row unresolved. | ENG-06 contention/exhaustion; OPS-49/STO-7, no unbounded retry |
| `w5_remote_completion` | Simulator durably accepts, holds before creating its resource. Dedicated lock loss causes a real cut-off record and engine exit 1. Releasing the independent hold creates the resource after exit. Local unresolved row retains original reserve/error with no fictional machine/debit; successor/keyed replay sends no second purchase. | OPS-50 remote-effect limitation, PRV-5/11; W6 reconciliation not run |

Additional required inventory entries also pass:

| Selector | Evidence |
|---|---|
| `w5_generation_record` | First-statement generation refusal while recording exits 1 without retry. A separate arm loses an earlier record COMMIT reply, advances generation, observes one repeat refuse and no third BEGIN; earlier failed/released rows remain byte-for-byte unchanged. ENG-01 selected record arm, OPS-50/CNF-367. |
| `w5_default_hooks` | Default-feature build ignores every supplied control file and blind-retry environment; files remain unconsumed. Normal success and unknown acceptance each dispatch one order; source/features/executable identities recorded. |
| `w5_claim_precommit` | Claim precommit SHOW reply is gated and transport severed before any COMMIT dispatch. Exactly one bounded `claim_precommit_retry` (55 ms charged), exactly two claim attempts before registration, then one successful purchase. OPS-49 ordinary-error exception. |
| `w5_backoff_budget` | Supplementary actual runtime/core-probe defer transaction spends 1993 ms before the unchanged 10 ms backoff. Crossing the 2000 ms budget yields exit 71 and zero new BEGINs. Driver fails closed unless the intended timing interval activates. |

## Retry inventory and changes

`Engine::transaction` owns whole-transaction retry, used through `transaction_stoppable` for
step-8 licensing, `transaction_record` for settle/cut-off records, and the process-owned
`defer_owned` path. `preflight_value` retries three complete worker autocommit SELECTs (account,
tenant and ordering-channel checks), never an individual statement inside a multi-statement transaction.
`run_order` retains its provider outcome and successful confirming-read instant outside settlement's
retry closure. The final manifest compares the retained instant with the stored one and observes
no repeated external mutation or verification/constraint read.

`claim_owned` distinguishes new `CommitOutcome::NotDispatched` from a sent uncertain claim.
Known pre-COMMIT errors consume complete elapsed time, release failed checkout/component before
publishing retry permission and repeat the whole claim. Lost/rejected sent claim acknowledgement
remains fatal. The variant preserves the existing external `rejected` label; this narrow transaction
interface delta is required by OPS-49's claim exception and its unchanged-runtime reproducer.
No startup ordering changed.

Both ordinary claim and worker loops now check exhaustion before each next attempt, including
charged backoff. Serialization repeats immediately without charging the Store-retry bound;
constraint handling remains separate. Failed transactions/sessions are released before backoff.
Stop-record identity/deadline is independent of retry accounting: true later stop exits without
repeating a sent record; first execute stop is shielded; first stop after return retains its outcome;
lock loss preserves an older record; generation mismatch exits immediately without another transaction.

Generation advance and ordinary startup recovery are startup work with fatal errors, not new
worker retry sites. `Dispatcher::load` is startup descriptor discovery, with its existing deadline
limitation. API request/credit paths do not acquire the engine loop. There is no implemented engine
periodic scheduler to retrofit. Reacquire is a guarded store/core fixture primitive; no production
create reacquire workflow was invented. Generic provider/database retry decorators were not added.

**Known exception to completeness:** ordinary sent-COMMIT retry accounting still exempts the whole
send-to-reply/death interval. The correction below is stopped, not hidden by these passing sites.

## Negative controls and real regressions

[All thirty-three controls](final-controls.json) retain exact child assertions, source/features,
unsafe observations and successful cleanup. The three added isolated test-build defects are:

| Defect | Unsafe observation | Intended assertion, child exit 1 |
|---|---|---|
| `w5_generation` | Unequal generations, stale recording write actually succeeds | `OPS-50 stale generation record must refuse without retry` |
| `w5_checkout` | Same live SyncRep transport/cancelled handler, actual owner pooled checkout is absent | `STO-7 HTTP cancellation must retain original sent-COMMIT checkout` |
| `w5_blind_retry` | Two independently received orders, acceptances and created resources for one operation | `OPS-12 lost reply must not dispatch a second purchase` |

Their red manifests are `red-control-<defect>.json`. All thirty earlier controls remain required.
A defect activation diagnostic alone cannot pass the recognizing parent; setup, unactivated fault,
wrong assertion or cleanup failure cannot substitute for the intended child failure.

[Runtime regression comparison](regressions.json) exited 0 recognizing two unchanged-runtime reds.
Both used the identical final assertion driver with fixture-only overlays in invocation-owned isolated
clones. Protected runtime, migrations, dependency and Nix hashes were verified against the base before
and after. Their source proofs and each failing manifest are retained separately:

- `red-w5_claim_precommit.json`: child exit 1 at `OPS-49 known pre-COMMIT claim failure must retry the whole transaction`.
  COMMIT was never dispatched, baseline engine exited 1, row stayed queued, zero claim retry events.
  Final green retries once and succeeds. This is a runtime defect, distinct from sensitivity controls.
- `red-w5_backoff_budget.json`: child exit 1 at `OPS-49 backoff exhaustion must forbid another transaction`.
  Baseline spent 1993 ms plus 10 ms backoff yet began one more transaction and deferred successfully
  (probe exit 0). Final green has zero new BEGINs and fatal exit 71. Claim-loop symmetry is source
  inspection; a separate claim-backoff red was not run.

[Parser comparison](parser-comparison.json) is a fixture regression, not an old-runtime defect.
A real file containing a complete `engine_superseded` line followed by incomplete UTF-8 bytes
failed the old helper from `7043a89` with `UnicodeDecodeError` and the intended assertion
`completed trace prefix must survive an incomplete UTF-8 tail` (exit 1). Fixed helper returns the
complete prefix (exit 0). Incomplete final tails are separately hashed/length-recorded
`inconclusive` diagnostics; no behavioral assertion uses them. Malformed complete JSON still fails.
No incomplete-tail diagnostics occurred in the final green phase manifests.

Two other corrections concern fixtures only: the expiry probe now accepts either attributed proxy
read direction closing promptly after the actual precommit verification gate, before its unchanged
idle-transaction deadline; stop controls publish a closed payload atomically so readers cannot
consume an empty UUID. These are not claimed as original-runtime defects. Superseded failed suite
attempts and their raw logs remain private; this bundle contains only final demonstrations and the
required regression/control reds.

## Stopped OPS-49 accounting path

Owning requirement: [OPS-49, pinned lines 1968–2040](https://github.com/douglaz/provisiond/blob/ac37c7cae5d3ff746515137774a4cdd5a3531375/03-operation-lifecycle.md#L1968),
especially 1976–1979: connection, execution, commit, read-back and backoff consume the bound,
excluding actual standby wait. STO-7 separately forbids abandoning a live sent COMMIT.

Minimal reproduction, using real engine settlement and healthy synchronous replication:

1. Gate its COMMIT CommandComplete reply after the server produced it.
2. Independently read succeeded operation/money rows and sample that exact backend repeatedly as
   `idle:ClientRead` for more than the 2000 ms retry bound.
3. Sever only that reply transport; observe the engine's retry charge and exit state.

The unchanged-runtime [failure](stopped-accounting.json) held 2197 ms, sampled `idle:ClientRead`
twenty-two times, and observed `whole_transaction_retry` charged only 21 ms. Engine remained alive;
provider orders stayed one and durable rows stayed unchanged. The command exited 1 at
`OPS-49 non-standby COMMIT reply delay must consume retry budget`; cleanup passed.
[Source proof](source-accounting.json) and the manifest's private-driver hash bind this observation.
The demonstrated impact is a retry-budget violation, not a demonstrated double debit or purchase.
The current accounting still has this defect; it was not retested as a fixed path.

A frontend send/reply timer alone cannot distinguish actual standby wait from an already-finished
backend whose reply is delayed. Charging both violates the exemption, while exempting both has
this defect. A timer after receipt of CommandComplete cannot measure a reply held before receipt.
The smallest proposed decision is permission for a narrow independently attributable COMMIT-wait
measurement design, covering precision, missing observations and ordinary pool accounting, or an
explicit transfer of this exact arm to the integration owner. No general runtime observability
service, policy waiver or specification change was made. D-36 remains Open pending that decision.

## Limits, cleanup and proof locations

- **Not run, W6:** automatic resolving-observed/reconciliation, late attachment and the complete
  ENG-05 purchase-reconciliation drill. Accepted-lost and remote-after-exit arms stop unresolved.
- **Not run, W7:** promotion/restoration incidents and operational fencing. Restoring the same
  designated standby without changing database history is not an incident rehearsal.
- **Not run, W8:** deployment-wide DB-10/CNF-373 saturation, loss of fresh observer access,
  production restart/backoff policy and alarms; D-6 production parameters remain Open.
- **Not run, owning future operation work:** unsupported kinds within complete CNF-366 and the
  wider CNF-365 matrix. Only the named create/record/withheld-check arms above are demonstrated.
- **Not run, existing follow-up:** descriptor-at-startup boundedness and bounded delivery of
  arbitrary retained rail history. No implementation or conformance claim is added here.
- **Inconclusive diagnostics:** incomplete final log fragments carry no behavioral authority.
  A clean review is not test execution. Full W5 remains unaccepted because D-36 is unresolved.

The pinned bodies control differences from broad plan labels: null-marker claimed creates fail
and release on ordinary restart; marked crashed creates remain unresolved. A survived pre-step-9
failure releases despite its acknowledged marker. Earlier lost-reply record COMMIT may already
have landed before a later stop or supersession, and a database fence cannot cancel a licensed
remote effect. One passing CNF arm discharges no other arm.

Every final phase and deliberate-negative child records successful cleanup after assertions:
registered children reaped, proxy closed, run-created PostgreSQL/data/WAL/build resources removed,
allocation released. No individual SyncRep backend was cancelled or terminated. Residual sessions
were observed until same-standby recovery. A known idle lock session alone was deliberately killed.
Local checkouts do not globally bound residual backends across process lifetimes.

[Audit](audit.json) binds raw and sanitized manifest hashes, cleanup, source identity and redaction.
Raw foreground gate/suite stdout and stderr, exit-status files, traces, source clones and fixture
proofs remain private. Sanitized raw proof paths in the manifests use `<run-dir>`; the retained
private `evidence-raw-map.json` resolves each public file to its exact raw source. Validation log
hashes identify the gate/suite streams; child `trace` hashes identify causal event files. The final
canonical raw root is represented as `<run-dir>/harness/final-q87v3ew5`; unchanged-runtime proofs
as `<run-dir>/harness/baseline-g8qu5tfo`, `baseline-wjih9vaj` and `baseline-5iszvpuf`; parser proof
as `<run-dir>/harness/parser-proof-bydyvf6f`. These placeholders disclose no host path or credential.
Only sanitized final manifests/summaries are committed; raw logs, patches, consultation records,
secret-bearing responses and superseded attempts are excluded. Earlier W1–W4 evidence is unchanged.
