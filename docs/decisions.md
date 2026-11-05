# Implementation decisions

This file records implementation and operating choices the pinned specification
(`douglaz/provisiond@ac37c7cae5d3ff746515137774a4cdd5a3531375`) leaves open. A decision here never
creates or changes a provisiond requirement. If a choice turns out to need a requirement change, stop
the affected path and raise it against the specification instead.

Each entry names its basis. An evidence-based entry cites the commands, versions and recorded results
that settle it. An owner decision quotes the owner.

## Settled

### D-1. Implementation repository

**Decided 2026-11-05 by the owner.** Asked where the implementation code should live, the owner
answered **"provisiond-rust"**. The repository is `github.com/douglaz/provisiond-rust`, public, kept
separate from the specification repository. The owner named no visibility; public matches the
specification repository and can be changed by the owner. One bootstrap commit, then pull requests
only; merges need the owner.

Basis (implementation plan, section 9): "Keep specification and implementation ownership separate."

### D-2. Language and workspace layout

**Plan proposal, adopted for W0.** The specification is language-neutral. The implementation plan
proposes "Rust, with one small workspace", and describes that as "an engineering proposal, not a
requirement attributed to the repository". The workspace is a single Cargo package `provisiond` with
`api`, `engine` and `migrate` binaries. Module directories from the plan's proposed layout are added
when the work package that needs them starts, not as empty placeholders.

### D-3. PostgreSQL and physical bootstrap

**Measured 2026-11-05.** Use nixpkgs `postgresql_18` at
`aff8a0b28396750446e5537a96461bc4facdb287`: PostgreSQL 18.6,
`/nix/store/h9aczmrkpcmm9z47648m5n9sg67vi5gw-postgresql-18.6/bin/postgres`.
The complete tool/OS versions and command statuses are in [W1 evidence](../evidence/w1.json).
`nix develop --command python3 tests/postgres/harness.py --phase w1 --evidence evidence/w1.json`
exited 0: fresh bootstrap, loss and rebuild of the sole designated standby, verified
`pg_basebackup -X stream`, archived-WAL restore of a post-backup fixture, explicit promotion,
and cleanup all passed. Raw logs remain at the manifest's owned evidence path.

`initdb` creates the isolated administrator. A single-user, offline catalog initialization creates
only the SCRAM replication role before the primary first accepts connections; it leaves
`synchronous_commit=on`. No component schema, migration, financial state or incident exists at
that point. Every subsequent catalog and fixture write waits for `FIRST 1 (provisiond_sync)`.
The physical backup stream uses `bootstrap_backup`; the standby is explicitly configured as
`replicator` / `provisiond_sync` before it starts. No maintenance commit-mode exception is used.
An initial setup experiment attempting to demote the bootstrap superuser failed (PostgreSQL
refuses it); that approach is absent from the harness. Setup failures are not resilience passes.

Deployment must preserve exclusive credentials and data provenance, the designated role/name,
actual primary history, replication settings and exclusion of transaction pooling. The drill
retains the verified-backup chain, system identifiers, timeline observations and sampled flush
catch-up; equal positions or timeline labels alone do not establish record agreement. The harness
cannot prove these duties on another deployment. Promotion/restore is infrastructure-only, with
components stopped; STO-54 incident handling is not implemented.

### D-5. Narrow statistics function

**Measured 2026-11-05.** [The fixture grants](../tests/postgres/privileges.sql) use a
NOLOGIN `stats_reader` with `pg_read_all_stats`, owning only `store_guard.sample()`.
The VOLATILE SECURITY DEFINER function pins `search_path=pg_catalog`, has no arguments,
returns only the designated sender's seven necessary fields, and grants PUBLIC no execution.
`app` gets schema USAGE and function EXECUTE, with no owner membership or direct statistics grant.
Its destination table grants are separate. No function can perform an administrative mutation.
This is narrower than giving every application session `pg_read_all_stats`.

`nix develop --command python3 tests/postgres/harness.py --phase checks --evidence evidence/w2-checks.json`
exited 0. [Evidence](../evidence/w2-checks.json) records denied schema/function access, actual
NULL stats under SECURITY INVOKER, raw stats hidden from `app`, and refusals of file reading,
configuration reload, setting the owner role and creating an object in the protected schema.
A SQL adapter tests the wrong-role predicate separately from real topology faults; it is not
represented as a history or authentication proof. The real HBA rejects a different replication role.
The deployment still owns standby provenance and exclusive authentication; a name or numeric
flush comparison cannot establish history agreement.

### W2 client candidate and provisional parameters

The measured candidate is `tokio-postgres 0.7.18`, `deadpool 0.12.3`, `tokio 1.54.1`
(exact transitive dependencies in Cargo.lock). The checks command above demonstrated fresh
physical-session settings, cancellation/failed-transaction closure, fixed-target polling on idle
and active primaries, same-session post-read checks, whole-read repetition, and bounded unused
connection closure. D-4 remains open until sent-COMMIT retention and actual warning classification
are measured. Dependency inspection found that dropped query futures still drain replies;
`Session` explicitly closes its supervised driver on pre-COMMIT discard instead of relying on that.

D-6 remains **Open for W8**. Provisional OVR-19 store-timeout rows are statement 1000 ms,
lock 500 ms, idle transaction 1000 ms, client pre-COMMIT call/connect 2000 ms, read-only check
3000 ms, and checkout 250 ms. Pooled TCP keepalives are idle 2 s, interval 1 s, count 2 on
both client and server. Isolation is read committed; search path is `pg_catalog,fixture`;
`synchronous_commit=on`. Read and write pools each have one checkout. The local primary's
`max_connections=24` with three reserved slots accommodates two store sessions plus bootstrap,
fixture/observer and replication-test headroom; it is not a product capacity budget. No engine
worker/periodic pool, Store-retry policy, production alarm threshold or restart policy is selected.
The stop bound remains a later lifecycle fixture. These are test values, not outage guarantees.

## Open

| ID | Choice | Where resolved | Required basis (implementation plan, section 9) | Status |
|---|---|---|---|---|
| D-4 | Rust database client/pool | W2 | "Observed warning, cancellation, and physical-session behavior." | open |
| D-6 | Fixed pool, timeout, and retry values | W2/W8 | "OVR-19 constraints, measured behavior, and deployment phase." | open |
| D-7 | Promotion/fencing and standby replacement procedure | W7 | "Rehearsed operational mechanism, not a prose assurance." | open |
| D-8 | First real provider path and spending limit | W10/W11 | "Current provider evidence and explicit operator approval." | open; needs the owner |
| D-9 | Fleet/exposure limits | W11 | "Stated assumptions and measured recovery, not arbitrary defaults." | open |

When an open entry is settled, move it to "Settled" with the exact versions, commands, exit statuses
and evidence paths that meet its basis. Preference, scope and cost questions go to the owner.
