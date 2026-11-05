# provisiond-rust

An implementation of [provisiond](https://github.com/douglaz/provisiond), in Rust.

The specification lives in its own repository and owns every behavioural rule. This repository
holds what that one excludes: the runtime, the database client, executable migrations, deployment
packaging, the provider and rail simulators, and the integration tests that run against real
PostgreSQL.

**Status: not a conforming service.** A local database harness exists; product entry points still refuse service use. Do not point real provider
accounts or real funds at any build from this repository.

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
