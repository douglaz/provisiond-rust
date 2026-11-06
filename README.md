# provisiond-rust

An implementation of [provisiond](https://github.com/douglaz/provisiond), in Rust.

The specification lives in its own repository and owns every behavioural rule. This repository
holds what that one excludes: the runtime, the database client, executable migrations, deployment
packaging, the provider and rail simulators, and the integration tests that run against real
PostgreSQL.

**Status: not a conforming service.** The local prototype has an admitted database client,
offline migrations, fenced engine transactions and atomic money primitives. The API checks schema
compatibility but has no HTTP service. Provider execution is simulator-only; automatic reconciliation,
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
PROVISIOND_HARNESS_PORT_BASE=55430 nix develop --command python3 tests/postgres/harness.py --phase w1 --seed 20261105 --evidence evidence/r2-final-w1.json
```

This single foreground command creates fresh private directories under the owned harness
root, checks its four-port allocation within 55430–55479 before starting any server, bootstraps one primary and one authenticated
synchronous standby, rebuilds the standby, verifies a physical backup, restores a post-backup
fixture from archived WAL, explicitly promotes with components stopped, and tears down.
Its exit status includes assertions and cleanup. Raw logs remain outside git; the optional
`--evidence` path receives a sanitized manifest, with the harness root and checkout written as
`<run-dir>` and `<repo>`. The cgroup, cap and required port-allocation checks refuse execution outside an
isolated test allocation. See [scope](docs/scope.md) for limits.

The store connection/destination spike is exercised with:

```sh
PROVISIOND_HARNESS_PORT_BASE=55430 nix develop --command python3 tests/postgres/harness.py --phase checks --seed 20261105 --evidence evidence/r2-final-checks.json
```

It uses the actual Rust client and ordinary bounded pools. The opt-in `store-probe` example is
an orchestration fixture; the default gate compiles it without starting PostgreSQL. Credentials
are generated per invocation and stay outside git. [Recorded results](evidence/r2-final-checks.json)
separate real server faults from focused identity-predicate fixtures.

Run the complete database, transactional-core and defect-control inventory from fresh private directories:

```sh
PROVISIOND_HARNESS_PORT_BASE=55430 nix develop --command bash tools/postgres.sh --seed 20261106 --evidence evidence/w3-final.json
```

It performs bootstrap, replication loss/rebuild, physical backup/archive restore, connection and
destination checks, sent-COMMIT cancellation/retention, warnings, lost replies, backend crash,
primary restart, residual-backend and bounded-process-stop fixtures, retained-object deadline
closure, caller-runtime shutdown, constructor cancellation/deadlines, the transactional core,
twenty-six additional defect controls, and cleanup. The parent accepts a failing control only with its
exact exit code and intended assertion after observed fault activation. A scenario/setup/cleanup failure makes the command
non-zero. The aggregate manifest requires all seven phases (W1, checks, commits, regressions, corrections,
core, controls);
each writes its own sanitized manifest and retains raw evidence under the private harness root.

The `commits` and `regressions` phases can also run alone with the same harness options.
For a focused regression, use `--phase regressions --case session`, `--case transaction`,
`--case runtime`, or `--case cancelled` (a read call or destination attempt dropped mid-flight). [Fresh acceptance and red/green results](evidence/r2.md) distinguish the
historical suites from coverage of both corrected lifetime defects. This local TCP spike disables
TLS and runs no product components on a promoted/restored history. It demonstrates only the
implemented subset. DB-10 saturation remains not run (W8).


## Local prototype entry points

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
migration permits new components to start. Release/schema/semantic version 1/1 is supported exactly;
there is no down-migration. Lock-timeout retries are bounded; failed initial/reconnect acquisition
exits immediately. Uncertain completion requires a later invocation to inspect durable history.

`api --check-schema` and `engine --check-schema` perform the real compatibility check and exit 0
only for a supported database. Ordinary `api` exits 78 after a successful check and explicitly
reports that HTTP service is unimplemented. Ordinary `engine` advances the generation, performs
the ordinary restart pass and monitors its lifetime lock; it has no provider dispatcher or full
operation scheduler. The integration fixtures invoke the same library paths directly.

Focused commands use `--phase corrections --case verify_cancel`, `--phase core --case migrations`,
`--case money`, `--case claims`, `--case generation`, `--case stops` or `--case external`.
The additional core cases are `claim_cancel`, `licensing_stop`, `outcome_stop`, `replacement_outcome`, `deadline`,
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
`--phase controls` runs all twenty-six required defects. Each defect requires its exact phase/case;
unknown or mismatched selectors exit nonzero before setup. The canonical command includes every
control and accepts only its intended assertion failure after activation and successful cleanup.
ENG-05 automatic reconciliation, ENG-07's dedicated replication-freeze stop-record drill and
DB-10 saturation remain `not run` for their later work packages.
[Recorded W3 acceptance](evidence/w3.md) names the tested commit, exact command statuses,
phase manifests, each negative control and cleanup results.
