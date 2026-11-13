# provisiond-rust

An early Rust implementation of [provisiond](https://github.com/douglaz/provisiond), a
multi-provider control plane that provisions and manages VPS and bare-metal machines.

The specification lives in its own repository and owns every behavioural rule. This repository
contains the runtime prototype, database client, executable migrations, simulator fixtures and
integration tests against real PostgreSQL. Deployment packaging and full provider and rail
adapters remain future work.

**Status: not a conforming service.** The local prototype has an admitted database client,
offline migrations, fenced engine transactions and atomic money primitives. The API checks schema
compatibility before serving a loopback customer HTTP subset. The engine dispatches local simulated
creates through acknowledged licensing and verified settlement, reconciles independent simulator evidence,
and executes immediate-destroy deletion. An isolated operator listener resolves uncertain outcomes and
retries or keeps stalled cleanup episodes. Reached exit metering is implemented; general billing and
recovery incidents remain unimplemented. No real provider accounts or funds may be used.

## Where to start

- `spec-lock.json`: the specification commit this repository is written against, with a hash of each
  source file it traces to.
- `docs/scope.md`: what is implemented, simulator-only, unsupported, and not yet attempted, and the
  accepted operating assumptions.
- `docs/conformance-map.md`: requirement, pinned source, implementation symbol, test, evidence and
  status for the first slice.
- `docs/decisions.md`: implementation choices the specification leaves open, and their basis.

The gate is `nix develop --command bash tools/check.sh`; CI runs `tools/check.sh` on every pull
request.

## Local PostgreSQL drill

Run inside a systemd unit capped with `MemoryMax=16G`, `MemorySwapMax=0` and
`OOMPolicy=continue`. Export `PROVISIOND_HARNESS_UNIT` with that unit's name and
`PROVISIOND_HARNESS_ROOT` with a private absolute directory outside `/tmp`; [the
harness](tests/postgres/harness.py) checks both before it starts anything. Export `TMPDIR` with
the path of an existing private directory outside `/tmp` as well. Choose a free port allocation and keep new evidence
under the private harness root so a drill does not overwrite the recorded acceptance manifests:

```sh
PROVISIOND_HARNESS_PORT_BASE=55500 nix develop --command python3 tests/postgres/harness.py --phase w1 --seed 20261105 --evidence "$PROVISIOND_HARNESS_ROOT/w1.json"
```

This single foreground command creates fresh private directories under the owned harness
root, checks its entire ten-port slot within 55500–55559 before starting any server, bootstraps one primary and one authenticated
synchronous standby, rebuilds the standby, verifies a physical backup, restores a post-backup
fixture from archived WAL, explicitly promotes with components stopped, and tears down.
Its exit status includes assertions and cleanup. Raw logs remain outside git; the optional
`--evidence` path receives a sanitized manifest, with the harness root and checkout written as
`<run-dir>` and `<repo>`. The cgroup, cap and required port-allocation checks refuse execution outside an
isolated test allocation. See [scope](docs/scope.md) for limits.

The store connection/destination spike is exercised with:

```sh
PROVISIOND_HARNESS_PORT_BASE=55500 nix develop --command python3 tests/postgres/harness.py --phase checks --seed 20261105 --evidence "$PROVISIOND_HARNESS_ROOT/checks.json"
```

It uses the actual Rust client and ordinary bounded pools. The opt-in `store-probe` example is
an orchestration fixture; the default gate compiles it without starting PostgreSQL. Credentials
are generated per invocation and stay outside git. [Recorded results](evidence/r2-final-checks.json)
separate real server faults from focused identity-predicate fixtures.

Run the complete database, transactional-core and defect-control inventory from fresh private directories:

```sh
PROVISIOND_HARNESS_PORT_BASE=55500 nix develop --command bash tools/postgres.sh --seed 20261106 --evidence "$PROVISIOND_HARNESS_ROOT/acceptance.json"
```

The aggregate manifest retains absolute paths in its child-command records. Redact identifying
paths before sharing or committing it.

It performs bootstrap, replication loss/rebuild, physical backup/archive restore, connection and
destination checks, sent-COMMIT cancellation/retention, warnings, lost replies, backend crash,
primary restart, residual-backend and bounded-process-stop fixtures, retained-object deadline
closure, caller-runtime shutdown, constructor cancellation/deadlines, the transactional core,
the complete HTTP funding/create workflow, a default-feature workflow, W5 interruption scenarios,
thirty-nine defect controls, and cleanup. The parent accepts a failing control only with its
exact exit code and intended assertion after observed fault activation. A scenario/setup/cleanup failure makes the command
non-zero. The aggregate manifest requires all eleven phases (W1, checks, commits, regressions, corrections,
core, workflow, default-workflow, W5, W6, controls);
each writes its own sanitized manifest and retains raw evidence under the private harness root.

The `commits` and `regressions` phases can also run alone with the same harness options.
`--phase w5` runs the complete interruption inventory; `--phase w5 --case w5_claim_reply`
selects one boundary with the same allocation options. `--phase controls --case w5_controls`
runs the five W5 sensitivity controls and the direct release-assertion probe. Focused manifests
do not satisfy the complete 26-selector W5 inventory. The [W5 report](evidence/w5/report.md)
records the final ten-phase run, protected
unchanged-runtime regressions, and completion-only COMMIT accounting. One checked, dedicated
read-only engine observer attributes actual SyncRep spans; missing evidence stays charged.
Observer loss can cause a conservative exhaustion exit, while a live transport withholding a
COMMIT reply forever causes no accounting exit. Original stop bounds remain independent.
This is selected historical W5 coverage, with the known limits in D-37–D-39.
[W6](evidence/w6/report.md) adds simulator reconciliation and deletion; deployment-wide saturation
and recovery remain deferred.
The protected unchanged-runtime comparisons are reproducible in the same private allocation with
`nix develop --command python3 tests/postgres/w5_baseline.py --evidence <private-baseline-manifest>`.
The runner exports the fixed base, overlays fixtures only and verifies runtime/dependency hashes
before and after; it creates no branch or tag and leaves this checkout untouched.
For a focused regression, use `--phase regressions --case session`, `--case transaction`,
`--case runtime`, or `--case cancelled` (a read call or destination attempt dropped mid-flight). [Fresh acceptance and red/green results](evidence/r2.md) distinguish the
historical suites from coverage of both corrected lifetime defects. This local TCP spike disables
TLS and runs no product components on a promoted/restored history. It demonstrates only the
implemented subset. DB-10 saturation remains not run (W8).


## Local prototype entry points

The API validates its required loopback API/rail addresses before readiness, owns its periodic and
request tasks, and exits nonzero on a failed required background component after bounded cleanup.
Sent COMMITs retain their existing process ownership through reply, transport death or process exit.
The [W4 evidence](evidence/w4/report.md) records actual-value funding, operator hiding, startup and
lifecycle regressions alongside the complete local workflow. Verified creates retain the confirming
provider read time across later constraint I/O and settlement retries; their billing seed still uses
the billable creation instant. Worker preflight reads retry store failures within the existing bound,
retain their running claim and reserve, and exit nonzero on exhaustion. W6 adds non-mutating
reconciliation of create/delete uncertainty and account-wide simulator observations.

The named `local-simulator` profile uses D-6's provisional settings. Configure
`PROVISIOND_PROFILE=local-simulator`, `PROVISIOND_DATABASE_URL` (private, never committed),
`PROVISIOND_STANDBY_NAME`, `PROVISIOND_STANDBY_ROLE` and `PROVISIOND_STANDBY_ADDRESS`.
The startup-lock keepalives explicitly share the local pooled values (idle 2 s, interval 1 s,
count 2), validated and observed on the dedicated session; D-6 remains Open for production.
Engine startup also requires `PROVISIOND_STARTUP_WAIT_MS`, an integer in 1..2147483647;
its local worker count is `PROVISIOND_WORKERS` (default 3). Startup budgets its pools, lock,
dedicated observer and transient generation session. These are local fixture values,
not a production deployment configuration.

Bootstrap roles and the narrow observer before migration. The isolated harness creates roles
with fresh credentials, then applies [privileges.sql](tests/postgres/privileges.sql) as its bootstrap
administrator. That file gives the migrator USAGE/EXECUTE on the observer and ownership of the empty
`provisiond` schema, without statistics/admin membership. The migration never needs an undocumented
preparatory database command. This bootstrap fixture is not a production deployment package.

For an offline release, first stop old components, prevent their restart, and establish that their
sessions cannot write. Run `nix develop --command cargo run --bin migrate`; only an acknowledged
migration permits new components to start. Schema/semantic history must be exactly 1/1, 2/2 and 3/3;
there is no down-migration. Lock-timeout retries are bounded; failed initial/reconnect acquisition
exits immediately. Uncertain completion requires a later invocation to inspect durable history.

`api --check-schema` and `engine --check-schema` perform the real compatibility check and exit 0
only for a supported database. Ordinary `api` also requires a local parameter file
(`PROVISIOND_LOCAL_PARAMETERS`), an explicit loopback listener (`PROVISIOND_API_LISTEN`) and
the independent rail simulator (`PROVISIOND_RAIL_SIMULATOR`). Ordinary `engine` requires the
independent provider simulator (`PROVISIOND_PROVIDER_SIMULATOR`), advances the generation,
performs the ordinary restart pass, dispatches queued creates/deletes, reconciles evidence and
monitors its lifetime lock.
Missing local configuration refuses startup. Only the isolated test profile is supported.

The customer subset supports enrolment token issuance, enrolment and status; deposit creation and
detail; assigned providers and offers; create and acknowledged delete; machine and operation list/detail;
balance and optional commitments; and customer OPTIONS. Recovery and other customer mutations remain
unsupported. Operator-only routes stay hidden on this listener, including preflight. Fleet callers should poll `GET /v1/operations?terminal=false`, plus balance and machines,
at the server's advertised cadence. Use the same idempotency key after a lost reply. A fresh key
creates a fresh operation and can buy a second machine; `needs_reconciliation` is never an invitation
to buy again. Reconciliation searches exact retained evidence and never repeats the purchase.

Deposits expose only literal `TEST/SIMULATOR/NONPAYABLE` destinations. They are not Bitcoin
addresses or Lightning invoices and must never receive real funds. Both simulated destinations can
credit one deposit, each by its own payment identity and actual settled value. Credits below the
cumulative activation threshold leave enrolment pending. There are no refunds; orphan recovery
requires an operator workflow outside this prototype. Lightning expiry/cancellation and the continued
payability of an on-chain destination are distinct; the local disclosures describe watching and halt
limits. Healthy account assignment occurs atomically at activation; later assignment sweeps are deferred.

The complete local parameter and descriptor fixtures live in
[workflow.py](tests/postgres/workflow.py). They disclose admission delay, expiry/finality, pending
capacity, traffic budgets, durable principal ceilings, retention, floors, confirmation depth, margin,
conversion and sequential wind-down inputs. Configuration validation admits the three fleet reads at
the finest advertised cadence. These fixture settings do not settle production capacities or D-6.

STO-33/API-11 operation replay and readability share one **86400-second** local fixture
horizon, measured from receipt admission's database transaction timestamp. Both configuration
names, `retention_seconds` and `receipt_seconds`, must express that same positive duration.
Each operation uses the latest stored expiry among all receipts resolving to it; changing
configuration or replaying a key never renews or shortens an existing receipt. Queued, running
and `needs_reconciliation` operations remain readable, listed and replayable beyond expiry.
Once settled and expired, detail and matching-key replay return non-retryable `410 gone`,
and collections omit the operation, independently of the physical sweep. A late settlement
receives no new horizon. `details.retained_until` renders the stored deadline at wire second
precision; comparisons use the full database timestamp, so formatting never cuts an unexpired
receipt short. Receipts and fingerprints refuse reuse permanently: a changed body conflicts,
and a matching post-horizon key cannot purchase again. Read machines and balance after `gone`.
Deposit receipts use the same configured duration, so the fixture's 86400 seconds keep their
one-day lifetime. This fixture chooses no production
horizon; historical bodies already deleted by the old sweep cannot be reconstructed.

The focused keyed-retention regression is `--phase workflow --case w4_retention`; its four
checks also run in the complete workflow phase. The [retention supplement](evidence/retention-horizon/report.md)
records the unchanged-baseline failure, fixed outcomes and tested limits.

Focused commands use `--phase corrections --case verify_cancel`, `--phase core --case migrations`,
`--case money`, `--case claims`, `--case generation`, `--case stops` or `--case external`.
The additional core cases are `claim_cancel`, `defer_cancel`, `licensing_stop`, `outcome_stop`, `replacement_outcome`, `deadline`,
`lock_observers`, `terminal_zero`, `terminal_capacity`, `loss_stop`, `queued_claim`, `displaced_record`, `surface`, `predispatch` and `startup`. They exercise caller cancellation,
licensing SQL/retry/COMMIT stops, returned outcomes, actual autonomous deadline exits, concurrent
lock observers, terminal guard refusals and capacity reuse, user stops after detected loss,
engine-only methods, live dispatch position and real engine startup. Queued-entry checks hold the
component mutex and pool separately across detection. Displaced-record checks withhold the
original COMMIT reply past its deadline, including a later observer and lock loss. The concurrent
observer check compares the earlier exact deadline/identity after fan-out registration.
The replacement-outcome case holds A's verified return across B's claim, verifies A's own
zero-row record and B's refusal without its own result, then settles B's distinct result once.
Queued waits require one pending attempt with no precommit retry and actual checked-entry
refusal. `--case queued_claim_pool --defect queued_claim_pool` isolates the pool defect;
the canonical controls include both queued entry and pool windows.
`--phase controls` runs all thirty-nine required defects. Each defect requires its exact phase/case;
unknown or mismatched selectors exit nonzero before setup. The canonical command includes every
control and accepts only its intended assertion failure after activation and successful cleanup.
ENG-05 simulator reconciliation is recorded in W6. DB-10 deployment-wide saturation remains
`not run` for W8. W5 demonstrates ENG-07's named create-record replication-freeze arm.
[Historical W3 acceptance](evidence/w3.md) retains its original tested source. The
[defer cancellation supplement](evidence/defer/report.md) records its seven-phase run,
27 negative controls, original-runtime reproduction and claim-specific capacity return.

[W4 local workflow evidence](evidence/w4/report.md) records the nine-phase acceptance run,
30 activated sensitivity controls, 32 original-runtime regression arms and three tooling regressions
(startup cleanup, concurrent simulator resource retention and search during creation). The focused preflight case is
`--phase workflow --case w4_preflight`; its
`w4_restart_error` and `w4_stop_error` selectors isolate the existing restart and survived-stop arms.
These cases check complete operation errors through authenticated detail and list HTTP reads.
The restart selector also interrupts a create after acknowledged licensing and checks that
its reconciliation-pending error and operation view both return `retryable: false`.
The `--phase workflow --case w4_simulator` selector forces two separate-process HTTP orders through
overlapping acceptance/allocation, checks their four effect stages, and retains both resources through
simulator restart and durable replay. It also holds an active search iterator while another order
creates its resource, then checks both HTTP responses and live/replayed search/get associations.
Search filters a snapshot copied under the existing simulator lock. The complete runner includes both cases.
This is a simulator-only pre-conformance subset; the report identifies the tested source and limits.

The focused `--phase workflow --case w4_expiry` selector uses default-feature binaries to
exercise deletion of a pending tenant after successful authentication and before request
serialization. It checks the exact 401 refusal, service and periodic-task liveness, session/lock
release, no invented business effects, valid deposits and replay/suspension ordering. The same
case runs in the complete workflow. [Expiry evidence](evidence/expiry/report.md) records the
original-runtime red and fixed-runtime green.

The focused `--phase workflow --case w4_lock_dispatch` selector runs the real executable with an
active worker and a claim held in PostgreSQL `SyncRep`, then loses only the lifetime-lock session.
It also checks late and empty claim replies during the original record drain. Lock observation
continues independently of dispatch, with sent transactions retained until their reply, transport
death or process exit. The `--case w4_search_error` selector checks seven accepted verification
failures, retained provider identifiers in stored errors and authenticated detail/list reads,
open reserves and unchanged evidence across ordinary restart. Identifiers remain evidence for an
unresolved outcome. Both selectors run in the complete canonical workflow.

The `--phase workflow --case w4_verify_stop` selector checks survived cancellation during an
accepted create's pending search and confirming get. Use `w4_search_stop` or `w4_get_stop` to
isolate either window. Only the exact idle lifetime-lock session is terminated; the provider
request stays held through acknowledged cut-off recording and autonomous engine exit. Stored
errors and authenticated detail/list reads retain learned identifiers, accepted knowledge,
frozen terms and the open reserve across ordinary successor startup, with no second purchase.
Both windows also run in the complete canonical workflow. These are additional checks of the
existing identifier correction, with no new runtime defect established.


## Local reconciliation and operator subset

`--phase w6` runs all 40 required groups; a focused `--case` cannot satisfy aggregate acceptance.
`--phase controls --case w6_controls` runs the four new exact-assertion controls. The
[W6 report](evidence/w6/report.md) records the final eleven-phase command, unchanged-base
regressions, source protection and explicit limits. Automatic create resolution searches every
configured ordering channel and directly confirms exact identity/account attribution. Unknown,
partial and duplicate evidence remains unresolved. The original commitment and resource-offer
exclusion have separate lifetimes; a late attach records deficiencies, a floor-or-deficiency and
queued cleanup atomically.

Only simulator immediate destroy with no surviving billable attachments is supported. A gone reply
settles at its observation instant; acceptance alone requires later non-mutating evidence. Caller,
late-attach and already-queued suspension cleanup share the worker. Ordered funding/fence decisions
precede capability/marker/provider dispatch. No-rate steps 4–5 stop before a fence/call; general
exhaustion, suspension fan-out, extend-runway, scheduled cancellation and open-incident startup
continuation are not implemented.

The optional separate loopback operator listener uses `PROVISIOND_OPERATOR_LISTEN` and
`PROVISIOND_OPERATOR_AUTHORITY`. The latter names a private JSON file such as
`[{"principal":"local-operator","token_env":"PROVISIOND_OPERATOR_TOKEN","admin":true}]`;
provide a distinct token of at least 24 bytes through that environment variable. Tokens are held as
hashes and cannot coincide with customer/recovery credentials. This local authority is not a
production credential-management design. Supported routes are operation list/detail/resolve, episode
list/detail, stalled-episode retry and keep. Resolver identity, evidence and principal-scoped
receipts commit together; exact-key replay repeats no effects. Retry creates a fresh delete attempt;
keep clears the stalled episode's fence without buying runway or moving money.

Demonstrating no duplicate purchase for the supported traces is not a proof of exactly-once execution by an arbitrary provider. External uncertainty remains part of the product model. [S5] [S10]

The W6 continuation adds nine activated regression groups. Startup persists a stable negative-window
origin; local money expiry runs independently of visibility and credential health. Interrupted
cleanup episodes become uncertain in the startup transaction. Operator resolutions consume a durable
principal allowance (local `operator_resolution_limit`, defaulting to `created_limit`, per
`ceiling_seconds`) after keyed replay; resolve/retry/keep emit safe correlated decision events.
Operator operation detail uses the shared retention horizon, including retained episode identities.
The offline schema-2 upgrade fills the supported old cleanup target, and the delete worker repeats
caller/API-field validation before fencing or dispatch. Production allowance selection remains open.

The remaining W6 repairs retain a single exact match's original commitment while its confirming
read fails, including incomplete rereads and ordinary restart. Schema-2 failed/unresolved cleanup
attempts become stalled/uncertain with retained targets. Automatically settled cleanup and retry
attempts retain scoped `410 gone` identities after housekeeping removes them, using the configured
horizon. Ordinary create search/get/constraint authentication failures stop that credential;
confirmed creates still settle with a null constraint date. Authenticated early operator refusals
emit safe correlated events before key, target, body, override or rate validation exits.

The final W6 repair also covers preflight offers, temporary-key registration, ordering, eligible
pre-acceptance key cleanup and deletion credential failures. Each rejection stops that credential
durably; queued work makes no later provider call and healthy accounts continue. Generic refusal
fixtures use provider/400 errors; the secret-redaction fixture keeps its authentication fault on an
independent account. Non-clamp deficiencies store SQL/JSON null in `clamped_sats`, retaining native
cost and opening-rate provenance; actual clamps retain their exact satoshi remainder.
Retirement carries the grouped maximum receipt horizon into the atomic delete/identity insert.
The canonical `w6_e1` activates a due backlog; the three `w4_retention_scale` shapes and default
`w4_retention_reads` remain separate supplemental checks. The [W6 report](evidence/w6/report.md)
records 50 groups, protected baseline reds and final validation. Minimal retired identities remain
permanent; eventual retired-history read scaling remains deferred.


Exit clamp deficiencies record the current opening rate independently of the historical rate
that prices elapsed usage. An opening without a current rate stays null after rates return.
Operator resolve receipts remain reachable after physical operation retirement: an identical
expired replay returns `410 gone` with the retained horizon; a valid changed fingerprint returns
`409 idempotency_mismatch`. Retained identities authorize receipt decisions only, never a new
resolution. See the [opening-rate and retention regressions](evidence/w6/opening-retention-regressions.json).
