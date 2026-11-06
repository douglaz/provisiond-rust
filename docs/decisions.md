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
No cancel request is sent. A caller that drops a read call or a `Store::destination` attempt
mid-flight also closes that session; the `cancelled` regression withholds the reply, drops the
caller, and checks the next checkout is a new backend. Against the pre-fix `043d8ae` store source
it fails for both arms ([red evidence](../evidence/r2-red-cancelled.json)) and passes in the full
command above. `Manager::create` constructs and authenticates TCP sockets through
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
Eligible OPS-49 whole-transaction retries belong to the engine caller (D-12), never this owner.

### D-10. Store boundaries and local transport measurements

STO-7/STO-55 retain the existing driver, pools, protocol observer and process-owned COMMIT path.
Raw production caller SQL is absent; typed business statements and internal migration/advance
settings are the admitted surface. Pending setting verification observes receipt cancellation;
possible COMMIT dispatch begins an irrevocable owner that retains the original checkout and
component permit. The notice cap is 16 payloads per physical session, reset at BEGIN, with an
independent sticky warning bit scoped to the actual COMMIT attempt.

The provisional 250 ms whole-checkout bound remains unchanged, including cold construction,
authentication, settings and admission. A controlled cold-admission comparison changed only both
fixture proxy sockets' TCP_NODELAY values. The recorded first comparison retained all four samples
per mode: off, 252.59/252.51/252.65/250.80 ms (three refusals); on,
48.29/48.84/48.13/51.76 ms (all admitted). This supports enabling the option on the fixture sockets;
it does not establish Nagle/delayed ACK as the underlying mechanism. The command
`PROVISIOND_HARNESS_PORT_BASE=55430 nix develop --command python3 tests/postgres/harness.py --phase corrections --seed 20261106`
exited 0. Its raw record is `<run-dir>/corrections-4391fq37/manifest.json`; superseded experiments
stay outside git. The original verification-cancellation assertion ran against the pre-fix runtime
and exited 1 at the intended closure assertion (`<run-dir>/corrections-ln6f7bqj/manifest.json`),
then the same assertion passed after correction. The final committed-source run retained all four
samples in each mode: off, 214.30/213.83/252.32/251.34 ms (one refusal); on,
48.01/49.35/51.38/48.52 ms (all admitted). [Final corrections](../evidence/w3-final-corrections.json)
and [the twenty-four controls](../evidence/w3-final-controls.json) record the integrated green/red results.
The complete commands, statuses and source identity are below. D-6 remains Open for W8.

Measured environment: PostgreSQL 18.6, rustc 1.97.1, cargo 1.97.0, Python 3.14.7, Nix 2.34.9;
tokio-postgres 0.7.18, deadpool 0.12.3, tokio 1.54.1, socket2 0.6.5. Cargo.lock and final manifests
retain exact versions. No production timeout or outage bound is selected by this experiment.

### D-11. Offline schema and bootstrap representation

STO-12/STO-13 use one transactional migration, schema/semantic version 1/1, ordinary index builds,
no SQL splitting and no nontransactional migration. The fixed migration key is
`0x50524f564d494752`; the distinct startup key is `0x50524f5653544152`. Deployment bootstrap owns
the pre-migration observer boundary: `tests/postgres/privileges.sql` grants app and migrator only
schema USAGE/function EXECUTE, with the existing pinned SECURITY DEFINER search path, no owner
membership and no broad statistics grant. Bootstrap creates an empty schema owned by migrator.
DDL/history completion use the ordinary synchronous COMMIT owner. Reconnect after lock-timeout
retry is diagnosed separately from initial acquisition; a connection failure is never retried.

The first-slice schema uses UUID identities, bigint signed satoshis, check-constrained text states,
JSONB closed summaries and explicit reference columns. Partial unique indexes enforce one
non-yielded running operation and one open commitment per machine. Ledger and deposit history
have no tenant FK; retained triggers protect the generation, tenant identities, deposits and
append-only ledger. Payments link one nonnull unique credit per unique rail identity. Principal
scope alone owns request receipt uniqueness. This is not a claim of the entire logical schema.

[Final core evidence](../evidence/w3-final-core.json), REL-02, records fresh, no-op and concurrent
runners; atomic DDL failure; a lost reply after durable history completion; supported and
unsupported schema/semantics through both real binaries; and one-attempt initial/reconnect
refusal with distinct reasons. Complete integrated commands and statuses are below.

### D-12. Engine entry and local concurrency

OVR-9/OPS-15/47/50 and STO-55 use an engine handle that exposes only fenced components. Each
component owns one semaphore permit through transaction completion, including process-owned
COMMIT. A separate entry mutex serializes generation checks with fatal-latch observation. Every
ordinary engine transaction begins READ WRITE, then reads the singleton FOR SHARE first; this
also implements provider-licensing reads. PostgreSQL refuses FOR SHARE in READ ONLY mode; the
local app role has the required generation UPDATE privilege. API components omit the fence.

The generation advance has a dedicated admitted transaction containing the replacement and its
required SET LOCAL configuration, with a client floor covering the selected startup bound. Its
acknowledgment precedes the bounded, per-row ordinary restart pass. An open incident refuses the
unsupported recovery path. The local profile uses three worker write slots, one compatibility
read slot and a dedicated lock session; startup checks these limits against ordinary capacity.
Worker count can be stated through `PROVISIOND_WORKERS`. A worker semaphore reserves capacity
before claiming and retains it through execution and records; simultaneous callers cannot exceed
the declared count. The regression first observed four claims for three workers (exit 1 at the
intended assertion), then three claims and one pre-transaction refusal after the fix (exit 0).
[Final core evidence](../evidence/w3-final-core.json) includes that overlapping four-caller check.
The fixture startup wait is 500 ms,
Store-retry budget 2000 ms, Stop-record bound 1000 ms, migration lock timeout 500 ms and two
lock-timeout retries. These values extend the provisional fixture register; D-6 stays Open.

Worker outcomes and after-return settlement loops are process-owned. A first stop gives an
existing settlement its own absolute Stop-record deadline; loss does not reset an older record.
Store errors charge whole attempts and backoff; serialization failures retry immediately without
charging that budget. Sent-COMMIT standby waits are excluded from that budget. Stop-record expiry
requires process exit and cannot release a sent owner while keeping the engine alive. No claim
reply uncertainty is retried blind. Unsupported parent/install/rescue/recovery paths are not built.

### D-13. Monetary slice and simulator terms

LDG-35 uses transaction advisory locks on `(1347572814, hashtext(tenant_id))`, ordered by ascending
ASCII tenant identifier, without a live tenant row. Hash collisions can reduce concurrency; they
do not permit concurrent writes for the same tenant. LDG-70 reads indexed latest seq/balance_after;
a separate explicit audit recomputes prefixes and durably latches spending closed on mismatch.
Repair/re-enable is an offline operator responsibility, not a replication-drift controller.

STO-23/27 and LDG-11 compose balance authorization, conditional resource exclusion, commitment,
operation and principal receipt. Resource claims have no expiry; known settlement releases them.
Receipts in this local fixture use a one-day horizon, not a selected production retention value.
STO-30/31 use payment identity rather than deposit/event identity, actual settled values and one
atomic credit/payment transaction. No reservation entry is posted. Verified-create settlement
repeats status, machine, setup/nonzero-unit debits, versioned commitment decrements, retained
snapshots/references and exclusion changes as one unit. Clamp overflow records the operator's
native-cost remainder; late attach and full metering remain unsupported. Every failed business
method makes its transaction uncommittable.

Offer and conversion inputs are simulator terms, not a first real provider/product decision.
The retained offer/charge shapes are closed; setup/unit conversion evidence is copied into debits.
The same component gate instruments provider, rail and rescue entries. No real providers, purchases
or funds are involved. [Final core evidence](../evidence/w3-final-core.json) records concurrent
payments/admissions, reverse-order tenant-lock inputs, multirow settlement replay, resource
exclusion, retained constraints and the durable audit halt. D-7, D-8 and D-9 are unchanged.

### D-14. Claim and stop ownership

OPS-49 claims now run under the existing process supervisor from reservation through
acknowledgment and worker registration. The caller holds a receipt; losing it leaves exactly one
registered worker and its original capacity, or fatal handling for an uncertain claim. No provider
entry follows an unlearned result. OPS-21 execution is registered before licensing, and its owner
covers the current transaction, retry loop and provider handoff. A stop permits the current sent
COMMIT to finish, prevents another licensing attempt/dispatch, then records. Stop registration
and actual simulator entry arbitrate under the worker lock. Create cut-offs use that live position;
ordinary restart alone keeps OPS-15's conservative `about_to_order_at` rule.

An execution publishes its final outcome before acknowledging termination; the stop record reads
that outcome afterward. Record deadline and observed result share one mutex. A supervisor watchdog
owns the first absolute deadline independently of `wait_records`, and the transaction loop checks
it before another attempt. Late completion cannot become timely acknowledgment. Expiry logs every
record not observed landed and exits the whole process nonzero, retaining sent-COMMIT ownership
until that exit. The Store-retry budget remains independent. Lock-loss observation alone is
serialized through fan-out and record completion; repeated observers cannot invent a later stop,
and an existing record keeps its first deadline. A real later stop retains its exit.

OPS-22 terminal completion distinguishes acknowledgment of the guarded effect (`Ok(true)`)
from acknowledgment of a zero-row refusal (`Ok(false)`). Both end that execution; only the former
is reported landed. The latter emits the observed guard refusal without asserting a restart.
Completion publishes the result and retires only the matching claim under the worker/record
locks. Records retain their original capacity permit while pending, including when a newer claim
replaces the registry entry; captured waiters retain their record result. Errors retain ownership
and the existing retry/deadline/exit handling. Four stop completions can reuse three slots.

OPS-21/47 detection stores its first monotonic timestamp and emits the loss log under the same
short mutex used by user-stop registration. Fan-out uses that timestamp for new records. A user
stop after detection exits as a later stop even before fan-out registers that worker; it never
waits on the observation mutex or repeats a sent record. An earlier record keeps its deadline
and identity through concurrent observers. Actual claims and the represented periodic admission
boundary refuse new work after detection. [Core evidence](../evidence/w3-final-core.json) and
[controls](../evidence/w3-final-controls.json) exercise both temporal orders. The three new
assertions first exited 1 on the original faulty completion/ordering logic at `f026454`, with
observation hooks only, then passed on final source. The red manifests retain that original
source identity as well as each final-source defect reproduction.

OPS-50's generation-first entry installs a private transaction capability only after the check.
Public claim/defer/reacquire/licensing/settlement methods require it before business SQL. API
admission/payment transactions remain unfenced. The exceptional held-generation fixture calls an
internal primitive only in the opt-in hooks build.

OVR-19's **Startup-lock connection keepalives** row explicitly shares the local pooled assignment:
client/server idle 2 s, interval 1 s, count 2. `StartupKeepalives` validates positive whole seconds
within PostgreSQL's signed range and a supported positive count before constructing the dedicated
transport. The shared session factory verifies client socket getters; a separate lock-session
observation checks all three server settings against that assignment. These are local-profile
values, not independently tunable production parameters. D-6 remains Open for W8.

Focused PostgreSQL commands for claim cancellation, licensing stops, returned-outcome publication,
unpolled deadlines, concurrent lock observers, live position and unfenced methods first exited 1
at their intended assertions on the supplied runtime logic, then exited 0 after correction.
`nix develop --command bash tools/check.sh` exited 0. The final committed-source gate, full
seven-phase acceptance and twenty-four targeted controls are recorded in [W3 acceptance](../evidence/w3.md).
The final manifests replace earlier W3 manifests; earlier passing inventory did not check these
ownership boundaries. Test-hook runtimes are identified as such; default-feature build validation
uses a separate target directory. No production policy or timing bound changed.

Concrete acceptance windows use only opt-in holds at claim registration, licensing retry/entry,
returned-outcome publication and lock-loss registration. Simulator external identities include the
operation ID to avoid cross-scenario collisions. Setup funding checks acknowledgment and permits
at most three API requests for the same payment identity after a recorded cold-checkout refusal;
this is a fixture replay, not an engine retry or an increased checkout bound. Other setup errors
fail. Fault tests retain their own assertions and do not use that replay to hide a fault.

### D-15. Claim dispatch arbitration and active record identity

**Measured 2026-11-06.** OPS-47 checked claim entry shares the loss-publication mutex after the
component-entry wait and the whole 250 ms pool acquisition. `Store::begin_checked` holds that
short guard only for the initial poll of BEGIN, then releases it before awaiting a reply.
Inspection of the locked tokio-postgres 0.7.18 `simple_query.rs::simple_query` and
`client.rs::simple_query` shows submission precedes the first response wait. No extra checkout
or blocking lock across store I/O is introduced. OPS-50's first generation statement and fatal
mismatch remain enforced. Ordinary record/settlement transactions remain permitted after loss.
Claims already dispatched can finish uncertain work; registration after detection attaches a
stop using the original loss timestamp. Refused queued entry is returned without another claim
attempt. There is still no production periodic scheduler.

OPS-21 active records are keyed by `(operation id, claim number)` independently of the current
worker map. Their results and original permits/deadlines survive worker replacement. New observers
and expiry use that active registry; expiry names operation and claim. Terminal success/refusal
removes the active entry, with captured observers retaining the result. Errors remain visible until
the required exit. Lock order is worker registry, active records, record state; no new lock crosses
an asynchronous store wait. Existing user-stop arbitration also takes the loss mutex under the
worker registry, before record access.

The focused real-PG commands `nix develop --command python3 tests/postgres/harness.py --phase core
--case <case> --seed 20261106 --evidence <raw-manifest>` exited 0 for `queued_claim`,
`displaced_record`, `lock_observers`, `terminal_zero` and `claim_cancel` after correction
(raw manifests under `<run-dir>/pending-green-i53_n1v2`). The first two safety assertions exited 1
on the faulty 916a46f runtime paths with observation hooks only. A deliberate post-registration
deadline reset exited 1 at the new comparison; it is a check-sensitivity control, not a newly
found runtime reset defect. [W3 acceptance](../evidence/w3.md) and the final core/three red manifests
record source hashes, intended assertions and successful cleanup. The final committed-source gate
and complete seven-phase command each exited 0, with 24 controls each exiting 1 as intended.
Versions remain PostgreSQL 18.6, rustc 1.97.1, tokio-postgres 0.7.18 and deadpool 0.12.3;
full version identity is in the final manifests. D-6/7/8/9 remain Open.

### Evidence basis for D-10 through D-15

**Measured 2026-11-06.** Tested implementation `6a2511abf38d2b9cb2377f6772bb599c85b4bb46`,
source-bundle SHA-256 `e9ac95f7c9d3b175679c3683c7f45672f3673a6973469dfe1d99c0c9dec9da1f`,
against the unchanged specification pin. The manifests recorded no source dirt. Later edits only
attach documentation and sanitized evidence; they do not change the tested runtime or fixtures.

- `nix develop --command bash tools/check.sh` exited **0** (format, lint and all
  unit/entry tests; no PostgreSQL started). [Gate record](../evidence/w3-gate.json) retains full-log
  hashes, default-feature binary build, port unit tests and invalid-selector refusals.
- `PROVISIOND_HARNESS_PORT_BASE=55430 nix develop --command bash tools/postgres.sh --seed 20261106 --evidence evidence/w3-final.json`
  exited **0**, all seven phases. [Aggregate](../evidence/w3-final.json) and
  [report](../evidence/w3.md) link every phase and each of twenty-four intended failing controls (exit 1).
  Cleanup passed for every positive and negative run. No real providers or funds were used.

Fixture transport/framing corrections preserve the 250 ms acquisition budget and
150 ms scheduling tolerance. D-14 separately records the runtime ownership corrections. The non-designated-backup test now distinguishes an
already-admitted fixed-target check from a separate cold admission, which must still refuse at
250 ms even with the backup sender ahead. A trace barrier precedes release of DB-06's post-read
fault. The busy-port fixture uses the allocation's own port and the harness's socket reuse mode.
Unused server slots remain bound without listening until needed, preventing this run's clients
from selecting those ports as ephemeral endpoints. These are isolated fixture mechanisms, not
host networking changes or a production connection-admission authority.

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
`synchronous_commit=on`. The W2 store fixture has one read and one write checkout. The local primary's
`max_connections=24` with three reserved slots accommodates two store sessions plus bootstrap,
fixture/observer and replication-test headroom; it is not a product capacity budget. W3 adds
three engine worker write slots, one compatibility read slot and a dedicated startup-lock session;
no periodic scheduler is implemented. Its Store-retry bound is 2000 ms. Production capacity,
retry policy, alarm thresholds and restart policy remain unselected.
The lifecycle fixture uses a 1000 ms stop bound and exits unresolved at that bound; two complete
read/check attempts are allowed for a broken destination-read session. These are test values,
not outage guarantees.

| OVR-19 owning row | Requirement | Provisional local fixture value |
|---|---|---|
| Store timeouts | STO-7, STO-55 | statement 1000 ms; lock 500 ms; idle transaction 1000 ms |
| API read/write concurrency | STO-55 | one checkout in each ordinary pool |
| Engine worker concurrency | STO-55 | three retained worker permits and three write checkouts; one compatibility read checkout; startup lock outside pools |
| Startup wait / Store retry | OPS-15, OPS-49 | 500 ms / 2000 ms |
| Pool checkout bound | STO-55 | 250 ms |
| Startup-lock connection keepalives | OPS-47, OVR-19 | explicit local assignment sharing pooled values; client/server idle 2 s, interval 1 s, count 2; validated and observed on lock session |
| Pooled-connection keepalives | STO-7 | client/server idle 2 s, interval 1 s, count 2 |
| Stop-record bound | OPS-21 | 1000 ms in lifecycle and W3 worker record fixtures |
| Connection budget | STO-55 | local max 24, reserved 3; fixture pools/observer/replication headroom only |

Client call/connect 2000 ms and check 3000 ms are implementation bounds obeying STO-7/OPS-49's
server-timeout floor. The store owner has no commit deadline. D-6 selects no production retry,
reconnect or alarm policy; full capacity and recovery evidence belongs to W8.

When an open entry is settled, move it to "Settled" with the exact versions, commands, exit statuses
and evidence paths that meet its basis. Preference, scope and cost questions go to the owner.
