# provisiond-rust

An implementation of [provisiond](https://github.com/douglaz/provisiond), in Rust.

The specification lives in its own repository and owns every behavioural rule. This repository
holds what that one excludes: the runtime, the database client, executable migrations, deployment
packaging, the provider and rail simulators, and the integration tests that run against real
PostgreSQL.

**Status: not a conforming service.** A local database harness and store lifecycle spike exist.
Product entry points still refuse service use. Do not point real provider accounts or real funds
at any build from this repository.

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

Run in the capped local test unit described in [the harness](tests/postgres/harness.py):

```sh
nix develop --command python3 tests/postgres/harness.py --phase w1 --evidence evidence/w1.json
```

This single foreground command creates fresh private directories under the fixed owned harness
root, checks ports 55410–55412 before binding, bootstraps one primary and one authenticated
synchronous standby, rebuilds the standby, verifies a physical backup, restores a post-backup
fixture from archived WAL, explicitly promotes with components stopped, and tears down.
Its exit status includes assertions and cleanup. Raw logs remain outside git; the optional
`--evidence` path receives a sanitized manifest. The fixed cgroup/port/path checks deliberately
refuse execution outside this host's isolated test allocation. See [scope](docs/scope.md) for limits.

The store connection/destination spike is exercised with:

```sh
nix develop --command python3 tests/postgres/harness.py --phase checks --evidence evidence/w2-checks.json
```

It uses the actual Rust client and ordinary bounded pools. The opt-in `store-probe` example is
an orchestration fixture; the default gate compiles it without starting PostgreSQL. Credentials
are generated per invocation and stay outside git. [Recorded results](evidence/w2-checks.md)
separate real server faults from focused identity-predicate fixtures.

Run the complete W1 then W2 inventory from fresh private directories with one foreground command:

```sh
nix develop --command bash tools/postgres.sh --evidence evidence/r2-final.json
```

It performs bootstrap, replication loss/rebuild, physical backup/archive restore, connection and
destination checks, sent-COMMIT cancellation/retention, warnings, lost replies, backend crash,
primary restart, residual-backend and bounded-process-stop fixtures, both targeted defect controls,
retained-object deadline closure, caller-runtime shutdown, constructor cancellation/deadlines,
and cleanup. The parent accepts a failing control only with its exact exit code and intended
assertion after observed fault activation. A scenario/setup/cleanup failure makes the command
non-zero. The aggregate manifest links four phase invocations (W1, checks, commits, regressions);
each writes its own sanitized manifest and retains raw evidence under the private harness root.

The `commits` and `regressions` phases can also run alone with the same harness options.
For a focused regression, use `--phase regressions --case session`, `--case transaction`, or
`--case runtime`. [Fresh acceptance and red/green results](evidence/r2.md) distinguish the
historical suites from coverage of both corrected lifetime defects. This local TCP spike disables
TLS and runs no product components on a promoted/restored history. It implements no W3 schema,
engine, ledger, real providers or production alarms. DB-10 saturation remains not run (W8).
