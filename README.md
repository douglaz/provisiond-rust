# provisiond-rust

An implementation of [provisiond](https://github.com/douglaz/provisiond), in Rust.

The specification lives in its own repository and owns every behavioural rule. This repository
holds what that one excludes: the runtime, the database client, executable migrations, deployment
packaging, the provider and rail simulators, and the integration tests that run against real
PostgreSQL.

**Status: not a conforming service.** The local prototype has an admitted database client,
offline migrations, fenced engine transactions and atomic money primitives. The API checks schema
compatibility before serving a loopback customer HTTP subset. The engine dispatches local simulated
creates through acknowledged licensing and verified settlement. Provider execution is simulator-only; automatic reconciliation,
metering and recovery incidents remain unimplemented. No real provider accounts or funds may be used.

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
`OOMPolicy=continue`. Set `PROVISIOND_HARNESS_UNIT` to that unit's name and
`PROVISIOND_HARNESS_ROOT` to a private absolute directory outside `/tmp`; [the
harness](tests/postgres/harness.py) checks both before it starts anything:

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

It performs bootstrap, replication loss/rebuild, physical backup/archive restore, connection and
destination checks, sent-COMMIT cancellation/retention, warnings, lost replies, backend crash,
primary restart, residual-backend and bounded-process-stop fixtures, retained-object deadline
closure, caller-runtime shutdown, constructor cancellation/deadlines, the transactional core,
the complete HTTP funding/create workflow, a default-feature workflow, thirty defect controls, and cleanup. The parent accepts a failing control only with its
exact exit code and intended assertion after observed fault activation. A scenario/setup/cleanup failure makes the command
non-zero. The aggregate manifest requires all nine phases (W1, checks, commits, regressions, corrections,
core, workflow, default-workflow, controls);
each writes its own sanitized manifest and retains raw evidence under the private harness root.

The `commits` and `regressions` phases can also run alone with the same harness options.
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
retain their running claim and reserve, and exit nonzero on exhaustion. Automatic reconciliation
remains deferred.

The named `local-simulator` profile uses D-6's provisional settings. Configure
`PROVISIOND_PROFILE=local-simulator`, `PROVISIOND_DATABASE_URL` (private, never committed),
`PROVISIOND_STANDBY_NAME`, `PROVISIOND_STANDBY_ROLE` and `PROVISIOND_STANDBY_ADDRESS`.
The startup-lock keepalives explicitly share the local pooled values (idle 2 s, interval 1 s,
count 2), validated and observed on the dedicated session; D-6 remains Open for production.
Engine startup also requires `PROVISIOND_STARTUP_WAIT_MS`, an integer in 1..2147483647;
its local worker count is `PROVISIOND_WORKERS` (default 3). These are local fixture values,
not a production deployment configuration.

Bootstrap roles and the narrow observer before migration. The isolated harness creates roles
with fresh credentials, then applies [privileges.sql](tests/postgres/privileges.sql) as its bootstrap
administrator. That file gives the migrator USAGE/EXECUTE on the observer and ownership of the empty
`provisiond` schema, without statistics/admin membership. The migration never needs an undocumented
preparatory database command. This bootstrap fixture is not a production deployment package.

For an offline release, first stop old components, prevent their restart, and establish that their
sessions cannot write. Run `nix develop --command cargo run --bin migrate`; only an acknowledged
migration permits new components to start. Schema/semantic history must be exactly 1/1 followed by 2/2;
there is no down-migration. Lock-timeout retries are bounded; failed initial/reconnect acquisition
exits immediately. Uncertain completion requires a later invocation to inspect durable history.

`api --check-schema` and `engine --check-schema` perform the real compatibility check and exit 0
only for a supported database. Ordinary `api` also requires a local parameter file
(`PROVISIOND_LOCAL_PARAMETERS`), an explicit loopback listener (`PROVISIOND_API_LISTEN`) and
the independent rail simulator (`PROVISIOND_RAIL_SIMULATOR`). Ordinary `engine` requires the
independent provider simulator (`PROVISIOND_PROVIDER_SIMULATOR`), advances the generation,
performs the ordinary restart pass, dispatches queued creates and monitors its lifetime lock.
Missing local configuration refuses startup. Only the isolated test profile is supported.

The customer subset supports enrolment token issuance, enrolment and status; deposit creation and
detail; assigned providers and offers; create; machine and operation list/detail; balance and optional
commitments; and unauthenticated OPTIONS. All other mutations, recovery and operator endpoints are
unsupported. Fleet callers should poll `GET /v1/operations?terminal=false`, plus balance and machines,
at the server's advertised cadence. Use the same idempotency key after a lost reply. A fresh key
creates a fresh operation and can buy a second machine; `needs_reconciliation` is never an invitation
to buy again. Automatic reconciliation remains deferred.

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
`--phase controls` runs all thirty required defects. Each defect requires its exact phase/case;
unknown or mismatched selectors exit nonzero before setup. The canonical command includes every
control and accepts only its intended assertion failure after activation and successful cleanup.
ENG-05 automatic reconciliation, ENG-07's dedicated replication-freeze stop-record drill and
DB-10 saturation remain `not run` for their later work packages.
[Historical W3 acceptance](evidence/w3.md) retains its original tested source. The
[defer cancellation supplement](evidence/defer/report.md) records its seven-phase run,
27 negative controls, original-runtime reproduction and claim-specific capacity return.

[W4 local workflow evidence](evidence/w4/report.md) records the nine-phase acceptance run,
30 activated sensitivity controls, 28 original-runtime regressions and three tooling regressions
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
