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
The complete tool/OS versions and command statuses are in [W1 evidence](../evidence/r2-final-w1.json).
`nix develop --command python3 tests/postgres/harness.py --phase w1 --seed 20261105 --evidence evidence/r2-final-w1.json`
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

`nix develop --command python3 tests/postgres/harness.py --phase checks --seed 20261105 --evidence evidence/r2-final-checks.json`
exited 0. [Evidence](../evidence/r2-final-checks.json) records denied schema/function access, actual
NULL stats under SECURITY INVOKER, raw stats hidden from `app`, and refusals of file reading,
configuration reload, setting the owner role and creating an object in the protected schema.
A SQL adapter tests the wrong-role predicate separately from real topology faults; it is not
represented as a history or authentication proof. The real HBA rejects a different replication role.
The deployment still owns standby provenance and exclusive authentication; a name or numeric
flush comparison cannot establish history agreement.

### D-4. Rust client, ordinary pools and process-owned COMMITs

**Measured 2026-11-05.** Select `tokio-postgres 0.7.18`, `deadpool 0.12.3`, `tokio 1.54.1`
and `socket2 0.6.5` (exact transitive dependencies in Cargo.lock).
The fresh follow-up command
`nix develop --command bash tools/postgres.sh --evidence evidence/r2-final.json` exited 0,
including the complete prior inventory, original controls (42/43), and the new lifetime regressions.
[Follow-up evidence](../evidence/r2.md) records three intended assertion failures (exit 1 each)
against unchanged `acc410c` runtime source in a disposable checkout, then the same assertions
passing against the corrected source. The older command below passed its recorded cases but did
not exercise retained-object timeout closure or destruction of the initiating I/O runtime.

`Session::close` now taints and signals the supervised driver immediately on ordinary query,
transaction-execute or read-only-check deadline expiry, even while the caller retains the object.
No cancel request is sent. `Manager::create` constructs and authenticates TCP sockets through
`Process::run` on the supervisor's own reactor before use; moving only the driver cannot transfer
Tokio's reactor registration. Bounded construction is cancelled when its result receiver vanishes;
undelivered results are dropped. Measured authentication/admission cancellation and authentication
deadline fixtures show unused sockets closing and replacements passing admission. This retains
ordinary pools and the existing per-COMMIT ownership structure.

The actual Store regression destroys the initiating runtime after observed SyncRep while the
process survives: the original socket and checkout remain held, another write checkout is refused,
and restoring the standby produces the real acknowledged reply on that same connection. The
retained timeout fixtures observe client EOF separately from server state; the transaction's
independent idle timeout does not stand in for local closure. D-6 stays provisional/Open for W8.

`nix develop --command bash tools/postgres.sh --evidence evidence/r2-final.json`
exited 0, including both intended failing controls. [Recorded lifecycle evidence](../evidence/r2-final-commits.json)
shows request timeout/drop/worker cancellation retaining the original checkout and physical
transport through actual SyncRep, bounded checkout refusal, clean completion after catch-up,
real driver warnings and lost replies, new-session checks after backend crash and primary restart,
residual backends after process exit, and bounded whole-process stop. These supplement the
[connection/destination evidence](../evidence/r2-final-checks.json). The final combined command is
`nix develop --command bash tools/postgres.sh --evidence evidence/r2-final.json`; its recorded
phase statuses are in [the combined manifest](../evidence/r2-final.json).

Inspection of the installed driver's `connection.rs` (`poll_read`, `poll_message`, `poll_write`)
and `simple_query.rs` showed notices are separate messages, complete simple queries end at
ReadyForQuery, and dropped query futures can leave response draining active. `Session` explicitly
closes its supervised driver on pre-COMMIT discard. A small bounded TCP frame observer preserves
CommandComplete tags and ReadyForQuery status, which the driver's row-count-only query API hides;
fragmentation tests run without PostgreSQL. It records no authentication or row payloads. The
prototype supports one TCP endpoint with `sslmode=disable`; this is a local spike, not a production
TLS/deployment implementation. Kernel keepalive getters and server SHOW values verify both ends.

`Store::commit` synchronously registers ownership before possible dispatch. The retained process
thread drives a task set containing drivers and per-COMMIT owners; it observes outcomes even after
a receiver disappears. Owners retain the original deadpool object, and have no deadline or retry.
Panic guards abort before unwinding can release their checkout; dropping a live process supervisor
also aborts. Shutdown closes admission and drains; a bound expiry requires whole-process exit,
without claiming rollback. This is process lifecycle support, not product OPS-21 stop records.

On this PostgreSQL 18.6/C locale, deliberate isolated backend cancellation emitted severity
`WARNING`, SQLSTATE `01000`, primary message `canceling wait for synchronous replication due to
user request`, the specified replication DETAIL, and then apparent `COMMIT` completion. Any WARNING
associated with COMMIT is conservatively unresolved, even with that completion. Such sessions close.
Only a clean complete COMMIT reply with idle status acknowledges; explicit rollback or known
constraint/serialization/deadlock rejection with a complete idle reply is rejected. Other errors,
missing replies and transport death are unresolved. No component cancel packet was observed.
Eligible OPS-49 whole-transaction retries belong to future engine callers, never this owner.

## Open

| ID | Choice | Where resolved | Required basis (implementation plan, section 9) | Status |
|---|---|---|---|---|
| D-6 | Fixed pool, timeout, and retry values | W2/W8 | "OVR-19 constraints, measured behavior, and deployment phase." | open |
| D-7 | Promotion/fencing and standby replacement procedure | W7 | "Rehearsed operational mechanism, not a prose assurance." | open |
| D-8 | First real provider path and spending limit | W10/W11 | "Current provider evidence and explicit operator approval." | open; needs the owner |
| D-9 | Fleet/exposure limits | W11 | "Stated assumptions and measured recovery, not arbitrary defaults." | open |

### D-6 provisional W2 parameters (Open for W8)

D-6 remains **Open for W8**. Provisional OVR-19 store-timeout rows are statement 1000 ms,
lock 500 ms, idle transaction 1000 ms, client pre-COMMIT call/connect 2000 ms, read-only check
3000 ms, and checkout 250 ms. Pooled TCP keepalives are idle 2 s, interval 1 s, count 2 on
both client and server. Isolation is read committed; search path is `pg_catalog,fixture`;
`synchronous_commit=on`. Read and write pools each have one checkout. The local primary's
`max_connections=24` with three reserved slots accommodates two store sessions plus bootstrap,
fixture/observer and replication-test headroom; it is not a product capacity budget. No engine
worker/periodic pool, Store-retry policy, production alarm threshold or restart policy is selected.
The lifecycle fixture uses a 1000 ms stop bound and exits unresolved at that bound; two complete
read/check attempts are allowed for a broken destination-read session. These are test values,
not outage guarantees.

| OVR-19 owning row | Requirement | Provisional local fixture value |
|---|---|---|
| Store timeouts | STO-7, STO-55 | statement 1000 ms; lock 500 ms; idle transaction 1000 ms |
| API read/write concurrency | STO-55 | one checkout in each ordinary pool |
| Pool checkout bound | STO-55 | 250 ms |
| Pooled-connection keepalives | STO-7 | client/server idle 2 s, interval 1 s, count 2 |
| Stop-record bound | OPS-21 | 1000 ms lifecycle fixture only; product record handling absent |
| Connection budget | STO-55 | local max 24, reserved 3; fixture pools/observer/replication headroom only |

Client call/connect 2000 ms and check 3000 ms are implementation bounds obeying STO-7/OPS-49's
server-timeout floor. The store owner has no commit deadline. D-6 selects no production retry,
reconnect or alarm policy; full capacity and recovery evidence belongs to W8.

When an open entry is settled, move it to "Settled" with the exact versions, commands, exit statuses
and evidence paths that meet its basis. Preference, scope and cost questions go to the owner.
