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

## Open

| ID | Choice | Where resolved | Required basis (implementation plan, section 9) | Status |
|---|---|---|---|---|
| D-4 | Rust database client/pool | W2 | "Observed warning, cancellation, and physical-session behavior." | open |
| D-5 | Replication-statistics privilege mechanism | W2 | "Least necessary access and fail-closed tests." | open |
| D-6 | Fixed pool, timeout, and retry values | W2/W8 | "OVR-19 constraints, measured behavior, and deployment phase." | open |
| D-7 | Promotion/fencing and standby replacement procedure | W7 | "Rehearsed operational mechanism, not a prose assurance." | open |
| D-8 | First real provider path and spending limit | W10/W11 | "Current provider evidence and explicit operator approval." | open; needs the owner |
| D-9 | Fleet/exposure limits | W11 | "Stated assumptions and measured recovery, not arbitrary defaults." | open |

When an open entry is settled, move it to "Settled" with the exact versions, commands, exit statuses
and evidence paths that meet its basis. Preference, scope and cost questions go to the owner.
