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
samples in each mode: off, 213.51/214.58/211.85/252.19 ms (one refusal); on,
48.40/48.14/49.26/48.46 ms (all admitted). [Final corrections](../evidence/w3-final-corrections.json)
and [the twenty-six controls](../evidence/w3-final-controls.json) record the integrated green/red results.
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
a separate explicit audit recomputes prefixes and durably blocks new create admission on mismatch.
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
seven-phase acceptance and twenty-six targeted controls are recorded in [W3 acceptance](../evidence/w3.md).
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

### Historical evidence basis for D-10 through D-15

**Measured 2026-11-06.** Tested implementation `6a2511abf38d2b9cb2377f6772bb599c85b4bb46`,
source-bundle SHA-256 `e9ac95f7c9d3b175679c3683c7f45672f3673a6973469dfe1d99c0c9dec9da1f`,
against the unchanged specification pin. The manifests recorded no source dirt. This historical run and its manifests remain in git
history. D-16 and the current acceptance report record the subsequent ownership correction and
fresh final validation; the current final manifests supersede those linked here.

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

### D-16. Returned outcome ownership

**Measured 2026-11-06.** OPS-6/21/22 and STO-3 use one shared
`Arc<Mutex<Option<Outcome>>>` per claimed worker. Execution registration captures its claim,
component, termination acknowledgment and outcome cell together under the worker registry lock.
Execution publishes to that captured cell before setting its done flag. Stop registration captures
the same cell before a later claim can replace the worker; after termination it consumes that
cell's actual result under the original claim guard. A replacement owns a fresh empty cell.
The manual outcome setter checks the supplied claim, direct settlement rechecks its claim at
record registration, and stale direct writes do not borrow the replacement's component.
Provider handoff checks the captured execution identity under the existing stop-arbitration lock.
No lock crosses store I/O; cells expire with their execution/worker/record owners, without a
persistent table, execution-history registry or new supervisor. Existing deadlines and permits
remain unchanged.

`nix develop --command python3 tests/postgres/harness.py --phase core --case replacement_outcome
--seed 20261106 --evidence <run-dir>/outcome-correction/original-red.json` exited **1** at
`replacement claim must refuse settlement without its own outcome` on the unchanged `e9aec96`
runtime. All 15 runtime/migration file hashes match that commit; only four test fixtures changed,
so source dirt is recorded honestly. No runtime overlay was needed. PostgreSQL showed claim 3
`succeeded` with claim 1's external result after one simulator acceptance. Cleanup passed.
[The final publication control](../evidence/w3-red-replacement_outcome.json) retains that original
reproduction and the committed-source operation-only publication control (same assertion, exit 1).

Tested implementation `4f1fe6d5c40d39540ce0020a662c46bb81bd0f97`, source-bundle SHA-256
`987fcb89a2bf454a173c08396c97e19b0c9f84a68a49d1369ea9e158e09bc680`, has no source dirt.
`nix develop --command bash tools/check.sh` exited **0**.
`PROVISIOND_HARNESS_PORT_BASE=55430 nix develop --command bash tools/postgres.sh --seed 20261106 --evidence evidence/w3-final.json`
exited **0**, all seven phases: 80 positive entries, 26 intended assertion-failing controls
(exit 1 each), and successful cleanup in all 32 phase/control runs. The unchanged W2 negative
observations and deferred not-run entries remain separate. [Acceptance](../evidence/w3.md),
[aggregate](../evidence/w3-final.json), [gate](../evidence/w3-gate.json) and
[core](../evidence/w3-final-core.json) retain exact invocations/statuses, source identity and log hashes.

The new regression observes A's verified result at the actual stop-record handoff and acknowledged
zero-row refusal, unchanged B state/money/exclusion and retained capacity. B refuses settlement
without its own outcome, then makes its own distinguishable simulator call and atomically records
its machine/debits/commitment exactly once. Queued entry/pool acceptance now requires one original
pending attempt, no `claim_precommit_retry` or replacement attempt, and actual checked refusal;
the isolated pool admission control exits 1 at the intended forbidden-BEGIN assertion. Normal
lock-observer and deadline-reset runs require completed fan-out and a real post-registration read.
Only the explicitly selected pre-registration control keeps its earlier-snapshot fallback; both
observer controls exit 1 at their intended assertions. No runtime timing bound or retry policy changed.

Versions: PostgreSQL 18.6, rustc 1.97.1, cargo 1.97.0, Python 3.14.7, Nix 2.34.9;
tokio-postgres 0.7.18, deadpool 0.12.3 and tokio 1.54.1. The separate default-feature build exited 0;
runtime evidence uses `store-test-hooks`. Later documentation/evidence-only commits do not alter
these tested runtime/fixture files. D-6/7/8/9 remain Open.

### D-17. Process-owned defer completion

**Measured 2026-11-06.** OPS-8/6, STO-3/7/55 and OPS-49 use the existing
`Process::submit` supervisor for `Engine::defer`'s full transaction loop, completion and
claim-specific retirement. The waiter receives a `Result<bool>`; cancellation only drops that
receipt. The task captures the matching component and an independent clone of its original
worker permit before submission. Replacement of the operation-id entry cannot release that
permit before acknowledgment. Acknowledged true and false both retire only the original claim;
false remains a normal defer guard refusal, without settled-write fatal interpretation.
No transaction/retry, true-stop or generation-fence policy changes.

OPS-49/50 fatal completion exits independently of result delivery. A successful oneshot send is
not proof its receiver observed the result; a cancelled or indefinitely unpolled receiver cannot
suppress the already-required exit. The generation regression therefore observes actual exit 71,
supersession and no retries instead of requiring an API error reply before process exit.
The first canonical attempt on `35f8616` exited 1 when the old fixture waited for that reply;
its cleanup also failed because the fixture had not set the expected child status. That record
remains outside git and is summarized in [validation](../evidence/defer/validation.json).

The [original-runtime red](../evidence/defer/w3-red-defer_cancel.json) used unchanged base
`8d7e89110898ef8ad7addfc291adaa5bee7e8625`, with a fixture-only overlay. All 17
runtime/migration/dependency hashes match the base. It built, held the attributed real COMMIT
reply, joined caller cancellation, released the reply and observed durable queued state, then
exited 1 at the named capacity assertion; cleanup passed. The final manifest retains its exact
patch hash, complete source inventory, dirty state and separate log hashes alongside the final
focused control. Raw patches, builds and logs stay outside git.

Tested implementation `966751005c34721fecefedff7a210a390d657cb8`, source-bundle SHA-256
`9b5410a0395126ab6fc9c747fd8d29c81c2aaeb5e83331fb0642bfabdd920ce2`, had no source dirt. `nix develop --command bash tools/check.sh`
exited 0. `PROVISIOND_HARNESS_PORT_BASE=55430 nix develop --command bash tools/postgres.sh
--seed 20261106 --evidence <run-dir>/defer-repair/final-3/w3-defer-final.json` exited 0,
all seven phases: 81 positive entries and 27 intended failing controls (exit 1 each), with
successful final cleanup. Four cancelled defers reused three slots; stale true/false completion
retained the old permit until acknowledgment and preserved the newer worker and state. Cancelled
and live unpolled supersession receipts produced actual exit 71 with one refusal and no retry.
[Report](../evidence/defer/report.md), [aggregate](../evidence/defer/w3-defer-final.json) and
[validation](../evidence/defer/validation.json) preserve commands, source and stdout/stderr hashes.

Versions remain PostgreSQL 18.6, rustc 1.97.1, cargo 1.97.0, Python 3.14.7, Nix 2.34.9;
tokio-postgres 0.7.18, deadpool 0.12.3, tokio 1.54.1 and socket2 0.6.5. Runtime evidence
uses `store-test-hooks`. Prior W3 evidence still demonstrates its original commit; this is a
separate final supplement. ENG-05, ENG-07 and DB-10 remain not run; D-6/7/8/9 remain Open.

### D-18. Local customer service and simulator boundaries

**Measured 2026-11-08.** The `local-simulator` profile serves the customer subset listed in
`docs/scope.md` after compatibility checks. Authentication, strict I-JSON/JCS fingerprinting,
principal receipts, suspension and durable ceilings precede the ordered purchase tail. Failed
ordinary authentications consume bounded source-memory traffic allowances; successful principals
retain separate read/write budgets. Supported read configurations cover three fleet reads per
1000 ms advertised cadence, including partial windows. Token concurrency pacing is a constant
1000 ms independent of admission delay. No caller address is persisted.

The funding side ingests trusted simulator observations through `funding::credit`; customers cannot
credit themselves. One retained deposit accepts both payment identities and credits actual final
amounts once. Activation and one healthy assignable account are committed with the credit. Assignment
uses least-assigned count with account-name tie order under serialization. Activation still succeeds
when no healthy account exists, with a diagnostic and empty assignment; later assignment sweeps remain
deferred. Literal nonpayable TEST/SIMULATOR destinations, floors, expiry/finality and orphan limits
are disclosed. Concurrent simulator deliveries are serially ingested by one watcher; separate-client
core tests exercise concurrent payment transactions.

One virtual hourly simulator product, unlimited traffic, no billable attachments and a current-day
cancellation bound narrow the profile. Descriptors are immutable local configuration and must contain
no credentials. The API has no provider client, the engine has no rail/customer credential access,
and ledger code makes no external calls. The independent provider/rail processes keep fsync'd effect
records outside PostgreSQL. Only fully processed fixture delivery history is cleared between unrelated
fault windows; durable records remain. Dropped-order replies demonstrate unresolved reservation and
no resend, not automatic reconciliation or a complete cancellation workflow.

Ordinary fixture clients make at most three HTTP attempts for the same authenticated keyed
POST deposit/create after retryable checkout overload, honoring body and HTTP pacing with at most
one second of waiting per resend. Every replayed refusal and unchanged key/body digest is recorded.
Expected errors, reads, enrolment,
armed transport faults and defect builds remain direct; no engine startup or business-error retry
is added. Six fixture-only transport checks verify these guards; PostgreSQL workflow/control
assertions remain independent.

Core fixture admission setup mirrors the existing funding setup: at most three API requests with
the same frozen request/key, and only after a complete checkout-deadline refusal. Each refusal is
recorded. This adds no Store-internal retry, engine startup retry or target-fault retry; business
errors stop immediately.

### D-19. Accepted native coverage and settlement evidence

**Measured 2026-11-08.** `request_summary.offer.accepted_terms` retains the accepted offer, rate and
credential-free pricing descriptor. The API also retains the admitted reserve, setup/unit authorization
and conversion evidence. Before any driver read, step 3 recomputes their coherence with the accepted
offer, rate and descriptor, and checks frozen strategy, cancellation and hourly copies (OPS-23).
Twenty-six corruptions across four groups are refused before external reads. The worker compares
current native requirements, including component offsets and billing-unit floor, against the admitted
reserve at that accepted conversion basis. A native
shortfall refuses with `price_moved`; exchange movement alone never grows or refuses the commitment.
Pricing uses checked rational arithmetic, margin on machine time, one haircut/conversion on the native
sum and upward commitment rounding.

Settlement retains the authorized setup/unit satoshis. A clamped customer debit pairs with the same
commitment decrement, carrying `floor(native * customer_debit / computed_debit)`; the deficiency carries
the exact native remainder and never seizes available balance. It records the machine subject, provider
account, zero absorbed seconds and the actual opening rate, permanently null if unavailable. A nonzero
first-unit customer debit alone supplies the retained debit reference and paid-unit seed. Accepted
settlement and seed still commit without a current rate; runway re-derivation stops with null new
projection fields rather than using a stale rate or inventing exhaustion. Later metering/re-derivation
remains outside this slice.

### D-20. Separate acknowledged offline migrations

**Measured 2026-11-08.** `Store::migrate` applies one complete SQL program and its history row per
acknowledged transaction, including a fresh database. A lost reply after release 1 prevents speculative
release-2 execution; a later invocation reads durable history and continues. Each migration uses the
existing fixed advisory key and bounded lock-timeout retries. Failed initial/reconnect acquisition still
exits immediately. Components support exactly history `1/1,2/2`, checked before listener/startup lock.
Release 2 preserves release-1 identities, money, destinations and completion time, and backfills existing
machine-clamp subject evidence while preserving the renamed opening timestamp. Offline stop-everything
and no-down-migration rules remain unchanged.

The recorded local checkout setup refusals have no established cause. Ordinary API fixture setup may
make at most three separate, recorded deployment attempts after the process exits on an exact read-only
checkout refusal before listener/business/funding/external activity. Fault-armed, defect and negative
startup cases, and all engine startups, remain single invocations. No runtime timeout, checkout policy,
startup retry or production capacity is selected by this fixture handling. D-6/7/8/9 remain Open.


Physical fixture commands have an explicit 180 s setup bound, 60 s server readiness and pg_ctl
shutdown wait, with a 75 s shutdown command bound. Backup verification and synchronous durability
remain enabled. Observed setup fsync exceeded 20 s, checkpoint shutdown took 21.907 s, and one base
backup exceeded 45 s. Their cause is not established; these bounds select no application timeout,
production retry policy or pg_receivewal hardening. Earlier failed attempts remain failures.

**Recorded basis for D-18–D-20:** runtime/fixtures `e4b7a3c8b2595cc8e8d9a1687d16fb2000e1cba8`, specification
`ac37c7cae5d3ff746515137774a4cdd5a3531375`; PostgreSQL tools 18.6, Rust 1.97.1, Cargo 1.97.0,
Python 3.14.7 and Nix 2.34.9, nixpkgs `aff8a0b28396750446e5537a96461bc4facdb287`.
`nix develop --command bash tools/check.sh` exited 0.
`PROVISIOND_HARNESS_PORT_BASE=55500 nix develop --command bash tools/postgres.sh --seed 20261106
--evidence <run-dir>/final.json` exited 0. [Gate](../evidence/w4/gate.json),
[aggregate](../evidence/w4/final.json), [workflow](../evidence/w4/final-workflow.json),
[core](../evidence/w4/final-core.json), [controls](../evidence/w4/final-controls.json),
[original-runtime regressions](../evidence/w4/regressions.json) and
[source inventory](../evidence/w4/tested-source.json) hold versions, commands, observed results,
source hashes and cleanup. The final commands used the default Nix daemon with NIX_REMOTE/NIX_CONFIG
unset, as recorded
in [build environment](../evidence/w4/build-environment.json). The earlier read-only mode observations
remain historical. No host configuration was changed and no cause for earlier checkout refusals
is established. The [report](../evidence/w4/report.md) narrows every claim.

### D-21. Trusted payment values and bounded API task ownership

**Measured 2026-11-08.** LDG-47 credits the trusted final received amount on both rails, including
Lightning values above or below the retained deposit intent. Positive safe-integer bounds, finality,
destination/hash/outpoint binding, atomic payment/credit and payment-identity deduplication remain.
Cumulative activation and healthy assignment share the credit transaction. WIR-34 hiding covers its
closed method-scoped list and the existing dedicated operator namespaces; shared customer providers,
offers and abuse-case reads retain their customer behavior, including explicit unsupported outcomes.

The API validates its parameter file and positive-port loopback API/rail addresses after schema
compatibility and before binding/readiness. `--check-schema` is independent of unused service values.
Required watcher/housekeeping completion is selected alongside accepts; task errors and panics are
fatal diagnostics, never a silent dead component. Failure drops the listener, cancels/joins periodic
tasks, drains request handlers for one second, then cancels/joins the rest before dropping State/Store.
Process shutdown retains its existing one-second bound and safety guards. A sent COMMIT is never
cancelled with its receipt: after handler cancellation it still owns its checkout until completion,
transport death or process exit (STO-7/55). Unresolved work at the process bound exits 1 without an
ownership panic, with uncertainty retained. API components do not inherit OPS-49's engine retry loop.
A separate restart after configuration/capacity repair uses ordinary keyed requests/rail replay;
continuous availability or automatic repair is not claimed. D-6/7/8/9 remain Open.

**Recorded basis:** committed runtime/fixtures `e4b7a3c8b2595cc8e8d9a1687d16fb2000e1cba8`, specification
`ac37c7cae5d3ff746515137774a4cdd5a3531375`; PostgreSQL tools 18.6, Rust 1.97.1, Cargo 1.97.0,
Python 3.14.7, Nix 2.34.9, nixpkgs `aff8a0b28396750446e5537a96461bc4facdb287`.
`nix develop --command bash tools/check.sh` exited 0.
`PROVISIOND_HARNESS_PORT_BASE=55500 nix develop --command bash tools/postgres.sh --seed 20261106
--evidence <run-dir>/final.json` exited 0 across all nine phases, including the seven API/funding
regressions and three provider-observation arms.
[Gate](../evidence/w4/gate.json), [workflow](../evidence/w4/final-workflow.json),
[original-runtime reds](../evidence/w4/regressions.json), [source inventory](../evidence/w4/tested-source.json)
and [validation](../evidence/w4/validation.json) record versions, commands, exact hashes and cleanup.
The final commands used the default Nix daemon with NIX_REMOTE/NIX_CONFIG unset. The final
verification tree was clean in every phase; all runtime/fixture/tool/dependency hashes match the
commit. Later documentation/evidence-only commits preserve those operational hashes. A failed
earlier canonical run on `6c8a1be` found missing namespace-root hiding; it is retained
as fail. The follow-up focused route/unauthenticated checks and final canonical run exited 0. This
settles neither the earlier cold-checkout cause nor production parameter selection.

### D-22. Provider observation time and fixture executable identity

**Measured 2026-11-08.** WIR-11/STO-48 date state at the successful authoritative machine read.
The engine captures a UTC instant immediately after that read, before PRV-13c's constraint I/O,
and carries it in `Outcome::VerifiedCreateDetails`. OPS-21's shielded record and OPS-49's whole
transaction repeats retain that outcome. The atomic landed-order transaction writes its instant
with state using an explicit UTC timestamp, preserving PostgreSQL microsecond precision. Its
existing idempotent guard leaves an already-attached observation unchanged. No extra provider read
or persistent history is introduced. LDG-38's local billable-creation seed remains independent:
creation instant plus copied seconds for an actual nonzero customer first-unit debit, otherwise
creation instant. Debit reference, seed and high-water checks retain LDG-39's eligibility rules.

The startup fixture builds its default API in a separate target directory. Recorded build and
invocation hashes identify default versus `store-test-hooks` modes; later lifecycle cases keep
the original hook-enabled binary. The observation regression uses an operation-attributed hold
in the independent simulator after the confirming read, with finite 3/5-second driver call bounds.
It checks the persisted instant and second-precision HTTP view, a lost settlement acknowledgement
and actual whole-transaction repeat, a bounded constraint timeout, and a survived stop during
constraints. Machine, debit, commitment, seed and external-call counts remain stable on repeat.
No new runtime fault hook is needed. The separate default-feature workflow still proves that
environment variables cannot enable opt-in defects.

**Recorded basis:** committed runtime/fixtures `e4b7a3c8b2595cc8e8d9a1687d16fb2000e1cba8`, unchanged specification
pin; PostgreSQL 18.6, Rust 1.97.1, Cargo 1.97.0, Python 3.14.7, Nix 2.34.9,
tokio-postgres 0.7.18 (exact dependencies in the manifests).
`nix develop --command bash tools/check.sh` exited 0.
`PROVISIOND_HARNESS_PORT_BASE=55500 nix develop --command bash tools/postgres.sh --seed 20261106
--evidence <run-dir>/final.json` exited 0, all nine phases, 134 pass results and 30 recognized
intended-negative controls; cleanup passed. The pre-commit focused observation and regression
sequence commands each exited 0. The original `62333c6`-equivalent runtime at
`d62e1f4` exited 1 at the intended timestamp assertion after the attributed hold and real settlement
repeat; all 30 runtime/migration/dependency hashes match that original source. Six fixture-only
overlays are identified in [the red manifest](../evidence/w4/red-provider-observation.json).
[Final workflow](../evidence/w4/final-workflow.json), [validation](../evidence/w4/validation.json),
[source](../evidence/w4/tested-source.json) and [report](../evidence/w4/report.md) retain effective
build modes, commands/statuses and source/log hashes. Source was clean throughout final verification.
Earlier evidence's mixed API modes are recorded as a historical metadata limitation in validation;
the earlier lifecycle results are not reclassified as application failures. No application timing,
pricing, runway or accepted deployment policy changed; D-6/7/8/9 remain Open.

### D-23. Worker preflight retries and startup build ownership

**Measured 2026-11-08.** OPS-49 applies to the account/request, tenant-status and channel-closure
reads in OPS-43 steps 1–2. `Engine::preflight_value` repeats each complete autocommit SELECT,
including acquisition and new-session admission, releasing the session before backoff. It uses the
existing 2000 ms Store-retry budget. Serialization retries immediately without charging that budget;
constraint and fatal-generation errors retain their existing exception paths. OPS-21 cancellation
closes an in-flight read and prevents another attempt or later dispatch; the existing shielded
record classifies the pre-step-9 cut-off. Exhaustion logs the operation, claim, site and accounted
time and exits nonzero without producing a settled worker outcome. Provider calls, API semantics,
uncertain claims and the transaction/sent-COMMIT owners are unchanged. The supported dispatch path
has no other direct worker store reads after these checks; descriptor loading is startup work.

The independent PostgreSQL proxy closes only transports preparing the selected SELECT. Five
HTTP-enrolled/funded/admitted cases demonstrate recovery at all three sites, real engine exhaustion
and ordinary null-marker restart classification, and a survived stop during retry. While faulted,
the original running claim and 43-sat reserve persist with no provider request. Recovery performs
steps 1–9 once, one order, atomic settlement and unchanged keyed replay. The final exhaustion arm
observed 34 failed read transports, 2023 ms accounted against the 2000 ms bound and process exit 1,
with no debit or worker classification. These are local measurements, not an outage guarantee.

The separate default startup target is registered through `owned_extra_paths` before Cargo runs.
After child completion cleanup verifies the default executable was used and both targets are gone.
The original fixture fails that check with its startup target still present; earlier cleanup passes
covered only registered paths. Only the newly created reproducer's exact leftover target and private
source copy were removed after assertions. Historical invocation data was not cleaned.

**Recorded basis:** runtime/fixtures `36c9142a6870c10bac055b64d41f423d3f39cc99`, unchanged specification
pin; PostgreSQL 18.6, Rust 1.97.1, Cargo 1.97.0, Python 3.14.7, Nix 2.34.9 and tokio-postgres 0.7.18.
`nix develop --command bash tools/check.sh` exited 0.
`PROVISIOND_HARNESS_PORT_BASE=55500 nix develop --command bash tools/postgres.sh --seed 20261106
--evidence <run-dir>/final.json` exited 0 across all nine phases, with 139 pass results and
30 recognized intended-negative controls; cleanup passed. The pre-commit focused commands
`nix develop --command python3 tests/postgres/harness.py --phase workflow --case w4_preflight
--seed 20261106 --evidence <run-dir>/focused-preflight-final.json` and the same command with
`--case w4_api_startup --evidence <run-dir>/focused-startup.json` each exited 0.
The same two cases on an unchanged private `ca36547` runtime with five fixture-only overlays
each exited 1 at its intended assertion. [Runtime red](../evidence/w4/red-preflight-store.json),
[tooling red](../evidence/w4/red-startup-cleanup.json), [workflow](../evidence/w4/final-workflow.json),
[source](../evidence/w4/tested-source.json) and [validation](../evidence/w4/validation.json)
record exact identities, versions, commands/statuses and cleanup. All 54 operational paths match
the clean tested commit. D-6/7/8/9 remain Open; no production timing or deployment policy changed.

### D-24. Complete stored operation errors

**Measured 2026-11-08.** WIR-9/10b and DOM-17 require the four-field inner error on operation
reads. Both ordinary-restart producers now supply `details: {}`; survived-stop recording uses
`wire::Error::new` to supply a human-readable message alongside the existing conflict kind,
cut-off reason and retryability. OPS-16 still preserves existing errors verbatim. Classifier order,
markers, claim/generation guards, money, stop/retry bounds and API projections are unchanged.

`Workflow.operation_reads` checks authenticated HTTP detail and list responses before comparing
exact error keys, field types, nonempty messages and the retained view-level correlation identity.
The existing preflight exhaustion/restart and survived-stop arms are independently selectable;
the canonical workflow also checks the marked-create restart, ambiguous stop, ordinary business
and accepted-provider errors, and null-error successes. Their existing money and external-effect
assertions remain. These checks add no new fault scenario or service surface.

**Recorded basis:** committed runtime/fixtures `3eeac6fd677f37a4e11e525ba9acb8f2fc63a39f`, unchanged
specification pin; PostgreSQL 18.6, Rust 1.97.1, Cargo 1.97.0, Python 3.14.7, Nix 2.34.9 and
tokio-postgres 0.7.18. `nix develop --command bash tools/check.sh` exited 0.
`PROVISIOND_HARNESS_PORT_BASE=55500 nix develop --command bash tools/postgres.sh --seed 20261106
--evidence <run-dir>/final-complete.json` exited 0 across all nine phases: 139 pass results,
30 recognized intended-negative controls and successful cleanup. All 54 operational hashes match
the clean tested commit. The pre-commit `--phase workflow --case w4_preflight`,
`--case w4_live_stop` and complete `--phase workflow` commands each exited 0. On unchanged
`758649f` runtime, `--case w4_restart_error` and `--case w4_stop_error` each exited 1 at the
intended HTTP shape assertion, with both endpoint responses observed and cleanup passing.
All 31 runtime/migration/dependency/pin hashes match that original commit; only three fixtures
changed. The first corrected-source canonical attempt had a wrong test expectation for an
existing controlled network failure; its exit 1 and cleanup pass remain recorded as failure.
[Restart red](../evidence/w4/red-operation-restart.json), [stop red](../evidence/w4/red-operation-stop.json),
[workflow](../evidence/w4/final-workflow.json), [validation](../evidence/w4/validation.json),
[source](../evidence/w4/tested-source.json) and [report](../evidence/w4/report.md) retain exact
identities and logs. Earlier D-18–D-23 measurements retain their historical tested commits;
the linked final bundle now tests this correction. D-6/7/8/9 remain Open.

### D-25. Consistent reconciliation-pending retryability

**Measured 2026-11-08.** WIR-9b/API-51 require a reconciliation-pending operation and its error
to advise `retryable: false`. `Engine::restart_pass` now generates false in the ordinary branch
that moves a marked create to `needs_reconciliation`; its read-only refresh exception retains true.
The separate null-marker failed-create branch retains true, as OPS-15 requires. Both producers
retain their complete inner error and COALESCE preservation under OPS-16. No stored-error rewrite,
state transition, marker, claim/generation guard, revision/result, dispatch, accounting or timing
policy changed. D-24's earlier broad retryability preservation did not verify this equality; this
entry corrects that generated advice against the pinned contract.

`Workflow.operation_reads` requires every non-null error's boolean retryability to equal the
authoritative operation view and the expected case value, with false for `needs_reconciliation`.
The existing marked-create scenario observes the acknowledged step-8 marker while step 9 is held,
real process termination with signal 15 and unchanged running claim, then the successor's completed
one-row restart pass. Both authenticated HTTP endpoints show the same error/correlation and false
advice, with the original 43-sat reserve, no machine/debit and no key/order call. Null-marker restart
and worker-recorded failure still demonstrate failed/full-release/true advice. The selector
`--phase workflow --case w4_restart_error` now includes these existing arms as well as preflight
exhaustion; no new runtime fault or service route was introduced.

**Recorded basis:** committed runtime/fixtures `0d8f775e9871a0f3f6e5b8bb49f55cb6f9e5f18f`, unchanged
specification pin; PostgreSQL 18.6, Rust 1.97.1, Cargo 1.97.0, Python 3.14.7, Nix 2.34.9 and
tokio-postgres 0.7.18. `nix develop --command bash tools/check.sh` exited 0.
`PROVISIOND_HARNESS_PORT_BASE=55500 nix develop --command bash tools/postgres.sh --seed 20261106
--evidence <run-dir>/final-complete.json` exited 0 across all nine phases: 139 pass results,
30 recognized intended-negative controls and successful cleanup. All 54 operational hashes match
the clean tested commit. The pre-commit focused restart selector exited 0 on identical operational
bytes. On unchanged `8104047` runtime with only that workflow fixture overlaid, it exited 1 at the
intended HTTP retryability equality assertion after both actual responses and crash/restart were
observed; cleanup passed. All 31 runtime/migration/dependency/pin hashes match the original, and
the fixture hash matches the final commit. [Original-runtime red](../evidence/w4/red-restart-retryability.json),
[workflow](../evidence/w4/final-workflow.json), [source](../evidence/w4/tested-source.json),
[validation](../evidence/w4/validation.json) and [report](../evidence/w4/report.md) retain exact
identities, commands, log hashes, fault observations and limits. Earlier red identities remain;
there are 28 runtime regressions and one separate tooling regression. D-6/7/8/9 remain Open.

### D-26. Distinct durable simulator purchases

**Measured 2026-11-08.** The independent provider simulator derives each resource identity from
that order's returned acceptance event, whose sequence was allocated and fsync'd under the existing
lock. Reading a later shared sequence could give distinct accepted orders one resource ID and
lose one resource from both live state and replay. Inspection now copies a coherent locked snapshot.
Production runtime, migrations, dependencies, provider retries and accounting are unchanged;
all 31 corresponding hashes match the preceding implementation.

The explicit test-only `--concurrent-order-barrier` option overlaps two acceptances before either
allocation, then both allocations before either creation. Ordinary invocations have no barrier.
`nix develop --command python3 tests/postgres/harness.py --phase workflow --case w4_simulator
--seed 20261106 --evidence <run-dir>/focused.json` exited 0 before commit on the final fixture bytes.
Against the unchanged `06986ddeae79435f6b5499e638da5805f3c959ab` simulator with only scheduling and
inspection hooks overlaid, the same assertion driver exited 1 at the intended distinct-resource
assertion: two accepted commands returned one ID and retained one resource live and after restart.
Both processes were reaped and cleanup passed. This is a tooling regression, separate from the
28 application-runtime reds and 30 sensitivity controls. Exact sources, fixture/overlay hashes,
commands, independent events and statuses are in [the red](../evidence/w4/red-concurrent-simulator.json)
and [corrected evidence](../evidence/w4/simulator-concurrency.json).

On committed runtime/fixtures `ddd28b18aec830287ede2b5939d95b3d00d5e169`,
`nix develop --command bash tools/check.sh` exited 0 (12 Rust tests, fmt, clippy, spec-lock).
`PROVISIOND_HARNESS_PORT_BASE=55500 nix develop --command bash tools/postgres.sh --seed 20261106
--evidence <run-dir>/final-complete.json` exited 0 across all nine phases: 140 pass results,
30 recognized intended-negative controls, the normal funded HTTP create, dropped reply, concurrent
simulator replay and successful cleanup. All 55 operational hashes match the tested commit.
Versions were PostgreSQL 18.6, Rust 1.97.1, Cargo 1.97.0, Python 3.14.7 and Nix 2.34.9.
[Validation](../evidence/w4/validation.json) also preserves two earlier failed canonical attempts:
a resource-admission test received 202/503 instead of 202/409, then a duplicate pg_receivewal
process exceeded its existing 5-second shutdown wait. Both cleanups passed; neither failure is
acceptance or a new earned red. No policy or fixture was changed for either repeat. The cause of
the checkout refusal is unestablished; pg_receivewal wait hardening remains deferred.
[Tested source](../evidence/w4/tested-source.json) and [report](../evidence/w4/report.md) retain
identities and limits. D-6/7/8/9 remain Open.

### D-27. Coherent simulator search during creation

**Measured 2026-11-08.** PRV-26/OPS-27's supported verification path reads the independent
simulator while other orders can create resources. Search now copies resource values under the
existing lock and filters that detached tuple with the same account/channel/correlator predicate
and authoritative result. The resource values are never mutated after insertion. The lock covers
only the copy; it does not cover HTTP I/O or the test barrier. No application runtime, classifier,
provider retry, accounting, dependency, deployment profile or timing policy changed; all 31
runtime/migration/dependency/pin hashes match the preceding implementation.

The explicit test-only `--search-read-barrier` holds the iterator after its first value. A second
real loopback HTTP order accepts, creates and returns before that read resumes. Both calls then
complete within the existing finite test bounds, with exactly A returned by A's search, B found
by its own search/get, two distinct four-stage durable resources, no handler stderr and exact replay
after owned-process restart. Negative account/channel/correlator searches and the intentional
`search_throttle` reply remain exercised. The existing concurrent-order/replay and application
accepted/create/drop-reply arms remain in the complete runner.

Against `5c5124d1a24d2e4383ce6f052c9c20f964192d38`'s simulator with only the iterator scheduling
hooks overlaid, `nix develop --command python3 <run-dir>/search-probe.py
<run-dir>/original-with-search-barrier.py <run-dir>/red-search.json` exited 1 at the intended
HTTP-completion assertion, after observed iterator hold, B creation/return and resume.
`RemoteDisconnected` and `RuntimeError: dictionary changed size during iteration` were captured;
both resources survived live reads and replay, and cleanup passed. Original acceptance, allocation
and effect logic are unchanged. [The red](../evidence/w4/red-search-simulator.json) records exact
source/overlay/fixture/driver hashes, versions, commands and observations. This is a third tooling
regression, separate from the 28 application-runtime reds and 30 sensitivity controls.

With slot 55500, `nix develop --command python3 tests/postgres/harness.py --phase workflow
--case w4_simulator --seed 20261106 --evidence <run-dir>/focused.json` exited 0 before commit
on the final operational bytes. On committed runtime/fixtures
`01ccfab0bff3126289529a80caf83f870740eb12`, `nix develop --command bash tools/check.sh` exited 0
(12 Rust tests, fmt, clippy, spec-lock). `PROVISIOND_HARNESS_PORT_BASE=55500 nix develop --command
bash tools/postgres.sh --seed 20261106 --evidence <run-dir>/final-complete.json` exited 0 across
all nine phases: 141 pass results, 30 recognized intended-negative controls and successful cleanup.
The port unit command exited 0 with three tests. Versions were PostgreSQL 18.6, Rust 1.97.1,
Cargo 1.97.0, Python 3.14.7 and Nix 2.34.9. All 55 operational hashes match the clean tested commit.
[Search evidence](../evidence/w4/simulator-search.json), [validation](../evidence/w4/validation.json),
[tested source](../evidence/w4/tested-source.json) and [report](../evidence/w4/report.md) retain
identities, statuses and limits. Historical failed attempts remain fail; automatic reconciliation,
W3-E/pg_receivewal hardening and W5–W11 remain deferred. D-6/7/8/9 remain Open.

### D-28. Lock observation independent of executable dispatch

**Measured 2026-11-08.** OPS-47/21 use two pinned executable futures: ordinary dispatch and
serialized lock observation. Main polls both and observes either path's failure/completion.
Dispatch errors first join `Engine::observe_lock`, so a post-loss refusal cannot bypass pending
records. Main exits before dropping either pending future. Claim reservation, sent-COMMIT ownership,
late registration with the original loss timestamp, generation checks and the record registry remain
in their existing boundaries; no COMMIT deadline or new task framework was introduced.

`--phase workflow --case w4_lock_dispatch` establishes readiness and an active worker, attributes
an ordinary claim to its backend and COMMIT owner, observes a real `SyncRep` wait, then terminates
only the idle advisory-lock backend. A forwarded 57P01 and one new client driver-death event attribute
that loss while the claim transport stays separate. Loss is published, the represented worker is
stopped, no new claim attempt starts and the executable exits 71 with the original owner incomplete.
Two reply-only arms acknowledge a late or empty claim after loss while an older record remains
held. The process survives that completion and exits at the original record bound; the late claim
is registered/stopped under the original loss. These arms are distinct from SyncRep evidence.
The final observed exits were 976, 980 and 970 ms after the fixture observed driver death;
these are local observations, not a detection guarantee. Semaphore counts are inferred from the
unchanged ownership path and supplemented by existing core capacity tests, not sampled directly.

**Recorded basis:** runtime/fixtures `03516ef719334f1d7a2dd751b37953b84705edc5`, unchanged pin;
PostgreSQL 18.6, Rust 1.97.1, Cargo 1.97.0, Python 3.14.7, Nix 2.34.9 and tokio-postgres 0.7.18.
`nix develop --command bash tools/check.sh` exited 0. With slot 55500,
`nix develop --command bash tools/postgres.sh --seed 20261106 --evidence <run-dir>/final-complete.json`
exited 0 across all nine phases: 150 pass results, 30 recognized sensitivity controls, two other
expected negative demonstrations and four declared not-run results; cleanup passed. The focused
three-arm command exited 0 before commit. The same final assertion driver on unchanged `eace82c`
runtime with five fixture-only overlays exited 1 at the intended starvation assertion after actual
SyncRep and attributed lock-driver death; owned export/build/data cleanup passed.
[Original red](../evidence/w4/red-lock-dispatch.json), [workflow](../evidence/w4/final-workflow.json),
[source](../evidence/w4/tested-source.json) and [validation](../evidence/w4/validation.json) retain
exact identities, commands/statuses, observations and limits. D-6 remains Open; the full ENG-07
stop-record drill and automatic reconciliation remain deferred.

### D-29. Surviving identifiers on accepted verification failure

**Measured 2026-11-08.** OPS-13, PRV-5 and WIR-10b use a small in-memory collector around existing
settle-time verification. It copies only safe opaque `external_id` strings (nonempty, at most 128
bytes, ASCII alphanumeric or `_`, `-`, `:`), which includes the local simulator's resource IDs.
`error.details.provider_identifiers` groups sorted distinct strings by `order_reply`, `search` and
`get`. IDs from a non-authoritative or malformed read remain evidence even when that read cannot
verify a machine. Typed errors retain their kind and details; an untyped verification error becomes
a safe internal error with accepted knowledge and collected IDs before the Outcome handoff.
A stop during verification retains its cut-off conflict and evidence. Absorbed post-confirmation
constraint reads still return their verified outcome. Existing claim-guarded settlement performs
recursive redaction and persistence. No raw provider reply or executable request is copied.

`--phase workflow --case w4_search_error` checks seven branches: search throttle/network,
non-authoritative/many/malformed results, failed direct read and unconfirmed presence. Each exact
operation has one independent accepted/created/returned order. A separate real successful order
supplies distinct verification IDs for intentionally wrong/ambiguous reads. Acknowledged stored
errors and authenticated detail/list retain every learned ID, exact error shape, false advice,
correlation, actual failure kind and applicable retry/status details. Original offer/correlator/
setup/unit evidence, open 43-sat reserve, null result, no machine/debit and identical restart reads
are checked; the independent provider records no second purchase.

**Recorded basis for the seven ordinary branches:** same committed runtime, versions and full commands/statuses as D-28. The focused
seven-branch command exited 0 before commit. On unchanged `eace82c` runtime with only five fixture
overlays, the final driver exited 1 at the intended stored-ID assertion after actual acceptance,
verification throttle and acknowledged unresolved settlement; cleanup passed. All 31 original
runtime/migration/dependency/pin hashes match the Git objects. [Original red](../evidence/w4/red-provider-identifiers.json),
[workflow](../evidence/w4/final-workflow.json), [source](../evidence/w4/tested-source.json) and
[validation](../evidence/w4/validation.json) record exact hashes/diffs, builds, commands and outcomes.
Earlier reds and historical W1–W3 evidence retain their original source identities. D-6 remains Open.

**Additional cancellation coverage, measured 2026-11-08.** `verification_stop` independently holds
search after an accepted order reply, or get after successful search, then loses only the exact
idle lifetime-lock session. Both windows observe attributed transport death, unchanged generation,
acknowledged claim-guarded cut-off and autonomous nonzero exit while the provider request remains
held. Stored errors and authenticated detail/list preserve the learned identifiers, accepted
knowledge, attempted action, false advice and frozen terms, with the original open 43-sat reserve,
purged request, null result and no machine/debit. An ordinary successor retains those facts and
produces no second purchase. This exercises `ProviderIdentifiers::failure`'s existing
`ExecutionStopped` conversion and Outcome handoff; it establishes no new runtime defect.

The simulator records selected search IDs before sending the response that permits get. The real
held get then proves receipt. The short evidence write finishes before HTTP I/O, and additional
records remain limited to explicit holds. The current transport reads replies to EOF, after the
handler's evidence writes; no reachable search/get ordering failure is established. The per-account
simulator `call_seconds=5` allowance is finite and restored by fixture cleanup. The 1000 ms record,
startup/checkout/store/retry bounds, runtime and ownership remain unchanged.

The order-reply bookkeeping correction independently awaits the truthful `response_returned`
record, retaining all four exact order stages and matching resource IDs. Its causal comparison uses
`resource_created`, published before reply delivery, and the subsequent actual held verification
request. With the current Rust transport, `http::call` reads each reply to EOF. Python 3.14.7's
server closes the connection after the handler returns, including its `response_returned` write.
Thus the current engine cannot begin verification before that record. The revised fixture avoids
depending on this transport detail; it does not repair a reachable failure of the current client.
A controlled in-process probe of actual handler methods with stubbed reply delivery and extracted
assertion blocks produced creation 3, held search 5 and completion 6: both old assertions failed
and revised assertions passed (command exit 0). The stub removes the EOF dependency, so this is
a fixture-model observation, not a real HTTP/executable/PG regression or proof of reachability.
[Validation](../evidence/w4/validation.json) records command, hashes and scope. No simulator or
runtime edit was needed; the selected-search-ID to actual held-get proof remains.

On final fixture bytes, `nix develop --command python3 tests/postgres/harness.py --phase workflow
--case w4_verify_stop --seed 20261106 --evidence <run-dir>/order-evidence-t2_p9xbu/focused-verify-stop.json` exited 0
with slot 55500 and both windows passing. On committed runtime/fixtures
`6410302aef1b96cc2d83e9c0caad5fd6c404ff4b`, `nix develop --command bash tools/check.sh` exited 0
(12 Rust tests, fmt, clippy, spec-lock). `PROVISIOND_HARNESS_PORT_BASE=55500 nix develop --command
bash tools/postgres.sh --seed 20261106 --evidence <run-dir>/order-evidence-t2_p9xbu/final-complete.json`
exited 0 across all nine phases: 152 pass results, 30 recognized sensitivity controls, two other
expected negative demonstrations and four declared not-run entries; cleanup passed. Versions remain
PostgreSQL 18.6, Rust 1.97.1, Cargo 1.97.0, Python 3.14.7 and Nix 2.34.9. All 31 runtime inputs
match `03516ef`; all 56 operational hashes match the new clean execution commit.

The two [historical original-runtime stop reds](../evidence/w4/red-verification-stop.json) each
exited 1 at the intended stored-ID assertion after acknowledged cut-off, autonomous exit and
successful cleanup on unchanged `eace82c`. Their exact earlier fixture overlays are reconstructable;
the final simulator and driver ordering/read-completion differences are disclosed rather than relabeling those
executions. [Validation](../evidence/w4/validation.json) retains unsuccessful attempts, including
a cached gate test executable that embedded a removed target path. The final gate used a fresh
isolated target with `CARGO_BUILD_BUILD_DIR` unset. The canonical command used newly created private sequential
Cargo intermediates, fresh per-invocation targets and recorded executable hashes; the owned cache
was removed after all assertions. [Source](../evidence/w4/tested-source.json) and
[workflow](../evidence/w4/final-workflow.json) identify the final executions. No production choice
is settled; D-6 remains Open and the rail-history bounded-delivery limit remains deferred.

### D-30. Canonical provider status and independent caller pacing

**Measured 2026-11-11.** PRV-5/OPS-11 settle the status field: `details.status` is the driver's
retained upstream status. `Engine::execute_order` reads that field while preserving worker position,
known acceptance, reported error/advice and guarded settlement. A definitive provider/400 refusal
closes its original commitment at zero and removes its resource exclusion through the existing
LDG-35/STO-3 transaction, with no machine/setup/unit debit. Dispatched provider 500/no-status and
accepted-then-provider-400 guards retain their reserves and surviving errors across restart.

API-3/29/50 settle the separation: `State::handle` identifies a valid principal before checking the
failed-auth source allowance. Credential verification uses ordinary bounded checked read sessions;
there is no pre-database source-work cap. Authentication refusals still consume bounded in-memory
source budgets independent of token spelling. Principal read/write allowances, auth-before-body,
pending/suspended restrictions and replay ordering remain. No new admission mechanism is selected.

**Recorded basis:** runtime/fixtures `27a75ffe84c633178c6b90e2e856817f09bb1c8f`; unchanged specification
pin; PostgreSQL 18.6, Rust 1.97.1, Cargo 1.97.0, Python 3.14.7, Nix 2.34.9 and tokio-postgres 0.7.18.
`nix develop --command bash tools/check.sh` exited 0. `nix develop --command python3
tests/postgres/harness.py --phase workflow --seed 20261111 --evidence <run-dir>/workflow-final.json`
exited 0 with 73 pass entries. Focused `w4_provider_refusal`, `w4_mixed_pacing`,
`w4_provider_ambiguity`, default-feature `w4_expiry` and `--phase default-workflow` each exited 0.
The first two selectors on unchanged `17a40da` runtime, with identical final fixture overlays,
each exited 1 at its intended assertion after the actual provider error/source exhaustion.
All 31 baseline runtime/build inputs match Git. Six existing activated controls each exited 1
at their exact assertion; their runner exited 0. All cleanups passed. The mixed fixture uses
30 reads/two writes per 10000 ms, admitting the three-read cadence, and completes within one
window. These are test parameters, not production capacities.

[Report](../evidence/refusal-pacing/report.md), [source](../evidence/refusal-pacing/tested-source.json),
[validation](../evidence/refusal-pacing/validation.json) and [observations](../evidence/refusal-pacing/observations.json)
retain the exact final comparisons, features, binaries, commands/statuses and raw-log hashes.
Earlier W4/expiry evidence remains historical. No nine-phase run is claimed; D-6/7/8/9 remain Open.

### D-31. Provider reads use their separately priced deadline

**Measured 2026-11-11.** PRV-13b/OVR-19 give the mutating call and confirming read
separate sequential waits. `Client::call_entered` applies `read_seconds` to all four
existing GET routes (`/offers`, `/search`, `/get`, `/constraints`) and the separate
post-delete confirming GET. POSTs retain `call_seconds`; one-second fallbacks and
descriptor validation remain. The seven-term reserve formula is unchanged.

With admitted mutation/read terms of 5/1 seconds and the other terms one, the priced
sum remains 11 seconds. Accepted-create read expiry keeps the existing timeout,
accepted knowledge, false caller advice, safe identifiers, frozen terms and complete
43-sat open commitment through ordinary restart. There is no machine/debit, second
order or timeout-based absence/billing-stop inference. The test-only stop-cost
adapter retains network/accepted-true/false-retryability on failed confirmation.
PRV-13c still absorbs the post-confirmation constraint failure. No resolution
scheduler, production cancellation path or mutation retry is added.

Existing cancellation/observation fixtures explicitly price the former effective
three/five-second read allowance, preserving actual lock-loss and retained-outcome
faults. Application startup, checkout, store/retry and stop-record bounds stay fixed.

**Recorded basis:** runtime/fixtures `aa5c0bd8bc417a8c1c7b37fbae118ba0b83470d2`,
unchanged specification pin; PostgreSQL 18.6, Rust 1.97.1, Cargo 1.97.0, Python 3.14.7,
Nix 2.34.9 and tokio-postgres 0.7.18. The final exact `w4_read_deadline` fixture on
unchanged `8c961c81e0746d2e88ca4934b2ea927a4bbc89bf` plus seven fixture-only overlays
exited 1 at the intended timing assertion: confirming read still open after 3.002728 s,
priced allowance 1 s and scheduling tolerance 0.4 s. Baseline runtime/build inputs
all match Git. The identical focused fixed-source run exited 0; its recorded HEAD
predates the fix commit, with all 57 operational hashes matching that later commit.

On the clean fix commit, `nix develop --command python3 tests/postgres/harness.py
--phase workflow --seed 20261111 --evidence <run-dir>/workflow-final.json` exited 0
with 80 pass entries, repeating all seven new arms. Receipt-to-wait-end read times
were 1.000257–1.001037 s, including the separate delete-confirmation call; a delayed
mutation completed in 3.033655 s under its 5 s term. Focused default-feature
`w4_expiry`, `--phase default-workflow` and core `stops`, `licensing_stop`,
`outcome_stop`, `deadline` each exited 0. The unchanged canonical runner restricted
to nine affected controls exited 0; every child exited 1 at its exact assertion after
activation, with cleanup passing. `nix develop --command bash tools/check.sh` exited
0 after those checks. Credentials/data/targets were removed and all ten ports freed.

[Report](../evidence/read-deadline/report.md), [source](../evidence/read-deadline/tested-source.json),
[observations](../evidence/read-deadline/observations.json) and
[validation](../evidence/read-deadline/validation.json) retain commands, feature/binary
identities, raw-proof hashes, tolerances, outcomes and limits. Independent review
is not run in this implementation phase; acceptance is not asserted. Earlier evidence
is historical, no nine-phase run is claimed, and D-6/7/8/9 remain Open.

### D-32. One finite operation replay and readability horizon (STO-33 reading)

**Decided 2026-11-11 by the owner (owner pick 2026-11-11).** The retention repair stopped
before implementation because STO-33's "can refuse a reused key" could be read two ways
against STO-25's tombstone, which keeps refusing a key after its operation is removed:

- Reading A: refusal includes a permanent tombstone's refusal, so settled operations must
  stay readable without limit.
- Reading B, verbatim as put to the reading panel: "STO-33's horizon is the finite interval
  during which a same-key replay returns the existing operation (API-11's promise, given an
  explicit stated expiry as STO-25 permits). One finite number is both the replay horizon and
  the operation-readability horizon. After it, reading the operation answers `gone`, and the
  retained record keeps refusing the key for good, as STO-25's tombstone."

A five-reader panel read STO-33, STO-25, STO-14, STO-35, API-11, DOM-21 and CNF-158 at the
pin. Three readers picked B; two judged the text ambiguous and left the choice to the owner,
while calling B the stronger whole-text reading; none picked A. Three of five is below the
pre-accepted ratio, so the question went to the owner, who chose Reading B.

- Strongest argument for B: A makes STO-14's retention MUST, STO-25's tombstone, CNF-158's
  post-horizon `gone` and STO-33's own premise ("STO-25 makes idempotency records outlive"
  terminal operations) impossible to meet together; the only finite number left to state is
  API-11's replay expiry.
- Strongest argument against settling it from the text: STO-33 never says in so many words
  that refusal by a retained record or tombstone after the published replay expiry falls
  outside "can refuse a reused key"; B narrows that phrase by structure, not by wording.

**Spec clarification owed: STO-33 wording.** Its smallest form: STO-33's "can refuse a reused
key" excludes refusal by a retained record or tombstone after the published replay expiry.
The specification is read-only in this phase; the clarification is raised against it later.

### D-33. Admission-origin keyed retention with one logical cutoff

**Measured 2026-11-11.** D-32's owner ruling governs STO-33 and
STO-25. Keep both parameter names for compatibility but require equality at startup, in
addition to positive, timestamp-safe validation. The ordinary fixture uses 86400 seconds
for each: it preserves deposit receipt lifetime and extends the previous one-hour operation
readability promise to that same day. This selects no production horizon.

`CreateRequest::receipt_seconds` supplies admission's database transaction timestamp plus H
inside `Transaction::admit_create_inner`; operation and receipt remain atomic (STO-35).
`State::create` changes only the response snapshot, never expiry. For a receipted operation,
E is the maximum persisted expiry across every resolving operation receipt, across keys and
scopes. Existing deadlines survive configuration changes and replays. No terminal-age H is
added. `operation_history` supplies one statement snapshot and database observation time to
replay, detail and list. Unsettled operations remain available beyond E; settled operations
are logically removed at E. Detail and replay share `operation_result` and the same E.
Fingerprint mismatch still precedes expiry and deposit replay retains its same-session
post-read destination check.

Full database timestamps decide eligibility, with equality expired. `retained_until` uses
existing `wire_time` second precision, truncating less than one second for presentation only.
No comparison or deletion rounds down; every unexpired stored receipt protects its operation.
The probe and actual DELETE share `SETTLED_RETENTION`, including its all-receipt guard.
Receipts/fingerprints survive permanently; ledger, commitment financial history and episodes
are outside retention. GETs perform no writes. A queued/running/reconciliation-pending
operation never becomes gone merely because E passed. If it settles after E, retention
applies as soon as that settlement is visible, without another H; the caller may first see
gone instead of its final body. No reconciliation or settlement work is added for retention.

The original correlated work probe timed out on 20,000 settled operations and 200,000
operation receipts, stopping the actual API. The replacement `SETTLED_RETENTION` materializes
settled candidates, hash-joins resolving receipts, aggregates their maximum persisted expiry,
and joins once back to candidates. Probe and DELETE evaluate this same relation afresh.
Measured plans scan receipts once, with no candidate-by-live-receipt comparison loop.

The unchanged global read aggregate also reached its bound: at two million receipts the
actual detail, operation replay, list and deposit replay returned internal errors. With only
queued operations, 8 successful housekeeping probes and a live API isolate that read
failure. `operation_history` now selects IDs before aggregation: detail filters both inputs
by ID; keyed replay uses the selected receipt through a lateral relation; list filters existing
operations by tenant and retains every receipt resolving to those IDs. Tenant authorization
remains separate from the all-scope maximum. Deposit replay avoids operation-history work.

No index or migration was needed. On PostgreSQL 18.6 with ordinary planner settings and the
unchanged 1000 ms statement bound, no-work/mixed probe and DELETE execution times were
41.897–48.675 ms at 20k operations/200k receipts, 135.727–162.578 ms at 20k/1M, and
193.266–200.734 ms at 50k/1M (server EXPLAIN ANALYZE execution). The binary's observed
query-to-completion probe intervals reached 155.748 ms at 20k/200k, 519.500 ms at 20k/1M and
500.375 ms at 50k/1M, with no statement error. HTTP reads on these shapes took 4.01–178.24 ms; the separate 20k/2M queued-history
fixture took 4.39–165.53 ms. These are local observations, not a production capacity guarantee. Receipts have no
`resource_id` index, so housekeeping and read cost still grow with permanent receipt history,
and housekeeping shares the single read checkout (250 ms bound) with customer reads. A review
driver at 20k/1M with three concurrent detail readers made housekeeping miss that checkout and
the API exit 1, the documented checkout-exhaustion response; the same driver at 20k/200k kept the
API up. An index on the receipt resource id would raise that threshold; none is selected here.
The shapes separately increase permanent receipt history and existing operation count across
100 tenant identities. Receiptless rows keep the pre-existing terminal-age sweep fallback
and lack a permanent deletion identity, so their later not_found gap remains outside this
keyed repair. Bodies already deleted by the old sweep cannot be recovered. F3 retained rail
history and descriptor-at-startup deadline limits remain deferred. D-32's specification
wording clarification is still owed; the specification was not modified.

**Historical basis, superseded for final runtime validation:** runtime/fixtures
`684cbb21cce88f81c884115ceae1a4bbc5335b1b`, unchanged pin;
PostgreSQL 18.6, Rust 1.97.1, Cargo 1.97.0, Python 3.14.7, Nix 2.34.9 and tokio-postgres 0.7.18.
`nix develop --command bash tools/check.sh` exited 0. The identical final `w4_retention` fixture
on unchanged `8ea285a` exited 1 at SAME-KEY REPLAY EXPECTS EXISTING OPERATION, with the row
physically deleted but 25.304408 seconds of live receipt remaining. The fixed focused command
exited 0. Complete workflow exited 0 with 84 pass entries; default-feature expiry/default workflow,
four core stop/deadline commands and a nine-control subset recognizing parent each exited 0.
Each control child exited 1 at its exact assertion after activation and cleanup. Final core/control
runs capture actual executable hashes/features; no fault or application bound was widened.
[Report](../evidence/retention-horizon/report.md), [source](../evidence/retention-horizon/tested-source.json),
[validation](../evidence/retention-horizon/validation.json) and [observations](../evidence/retention-horizon/observations.json)
record timings, config/source/raw hashes, byte-identical overlays, sole allowed config-value
difference and cleanup. The retained [pre-review record](../evidence/retention-horizon/review.json)
is Implementer pre-review, not Run acceptance; it establishes no independent acceptance quorum.

**Current measured basis:** the [query-bound companion](../evidence/retention-horizon/query-bound/report.md)
binds final runtime/fixtures, exact commands and exits, actual plans/timings, unchanged-baseline
executable reds, final greens and cleanup. The one-second statement, two-second store-call and
250 ms checkout bounds, permanent receipts and task-failure handling remain unchanged. Independent
Run review is pending. No new nine-phase run is claimed.

### D-34. Distinguish definitely unsent claims and check exhaustion before another attempt

**Measured 2026-11-12.** OPS-49 distinguishes ordinary pre-COMMIT claim errors from uncertain
claim completion. `CommitOutcome::NotDispatched` carries the former without interpreting a
sent COMMIT's transport loss as rollback. `own_commit` closes a failed verification checkout and
releases its component permit before publishing this outcome. `Engine::claim_owned` may then
repeat the whole claim under the existing bound, charging the complete known-pre-dispatch
interval including owner handoff. Its external label remains `rejected`; this is a transaction
interface distinction for the retry decision, with startup ordering unchanged. Sent/unknown claim
completion still exits fatally before worker registration or provider work.

Both claim and worker transaction loops check charged exhaustion before beginning another
attempt. Their existing backoff cannot authorize another transaction after consuming the bound.
The two [unchanged-runtime reds](../evidence/w5/regressions.json) used fixture-only overlays on
`aceea4eea8382c8d3b6fa554706c54920999369c`, with protected runtime/migration/dependency hashes.
Both exited 1 at their intended assertions; the recognizing comparison exited 0. Their final
green selectors are `w5_claim_precommit` and `w5_backoff_budget`. The latter supplements the
real-binary scenarios with the actual runtime's defer primitive; claim-loop symmetry is source
inspection, not a separate claim-backoff red. D-36 and D-39 record the final accounting implementation and proof.

### D-35. Observe test boundaries without changing ordinary builds

**Measured 2026-11-12.** Narrow `store-test-hooks` controls expose handler disconnect/reply
loss and real engine stop-record boundaries. Checkout observation samples the owner's actual
retained pooled object and transport, rather than inferring ownership from a live backend.
An independent simulator hold separates acceptance from resource creation. Default builds
ignore the fault files and defect environment, demonstrated by `w5_default_hooks` with actual
executable hashes and one order per operation.

The W5 parent requires all twenty-six selectors and successful cleanup; a focused child cannot
satisfy that inventory. The five W5 controls require an activated unsafe observation, exit 1
at the exact assertion and cleanup, preserving all thirty earlier controls. A separate direct-helper
probe accepts zero outstanding checkouts and rejects retained counts one through three at its exact assertion; it
demonstrates fixture sensitivity and makes no claim of a runtime checkout leak. Trace readers parse
only complete newline-terminated byte records. An incomplete final UTF-8/JSON tail is an
`inconclusive` diagnostic and cannot establish application behavior; malformed complete records
still fail. The [final fixture tests](../evidence/w5/python-units.json) record exit 0 for the
complete-record and inventory assertions; the earlier parser correction is a fixture change,
not another runtime defect. Test control payloads are closed before atomic path
publication, avoiding consumption of an empty operation UUID. Expiry-session closure evidence
accepts either attributed proxy read direction before the same original deadline.

D-34/D-35's final measured basis is runtime/fixtures
`c7e71c803f7c10158636ff908856507713a608cb`, unchanged specification pin, PostgreSQL 18.6,
Rust 1.97.1, Cargo 1.97.0, Python 3.14.7, Nix 2.34.9 and tokio-postgres 0.7.18.
`nix develop --command bash tools/check.sh` exited 0 and
`nix develop --command bash tools/postgres.sh --seed 20261112 --evidence <raw-final-manifest>`
exited 0 with all ten phases, twenty-six W5 selectors and thirty-five expected negative controls,
plus the direct release-helper probe.
`nix develop --command python3 tests/postgres/w5_baseline.py --evidence <private-baseline-manifest>`
exited 0 after all three protected-base children exited 1 at their intended assertions.
[Validation](../evidence/w5/validation.json), [source](../evidence/w5/tested-source.json),
[baseline protection](../evidence/w5/source-baseline.json), [audit](../evidence/w5/audit.json)
and [report](../evidence/w5/report.md) retain commands, hashes, configuration, assertions and cleanup.
D-6 stays Open for W8.

### D-36. Attribute actual standby wait before closing OPS-49 accounting

**Measured 2026-11-12; settled by D-37/D-38 and the final proof below.** The historical
reproducer gated a real settlement's COMMIT reply after the server finished, observed the same
backend `idle:ClientRead` with healthy synchronous replication for 2197 ms, then severed only its
reply transport. The old runtime charged 21 ms against a 2000 ms bound, retried and remained alive.
Its exit 1 was at `OPS-49 non-standby COMMIT reply delay must consume retry budget`.
Money and one purchase remained intact: this demonstrated a budget violation, not duplicate debit.

The [canonical comparison](../evidence/w5/regressions.json) preserves that historical reproducer
and repeats the same final assertion driver against protected unchanged
`aceea4eea8382c8d3b6fa554706c54920999369c`: intended child exit 1, actual non-standby delay,
unsafe retry and cleanup. Protected source/dependency hashes are equal before/after, and driver
hashes match the final green. On `c7e71c803f7c10158636ff908856507713a608cb`, the
[complete final W5 phase](../evidence/w5/final-w5.json) exits 0: healthy post-COMMIT delay is
charged, real SyncRep is conservatively exempted, mixed intervals are separated, unavailable
observations charge time, a real observer failure closes an established matching run, checked
observation resumes, and original stop bounds still win. The skip-closure sensitivity fails its
internal invariant assertion after the open run is observed; separate completion arithmetic
and independent server samples establish the executed unobserved interval remains charged.
The exact commands, versions and exit statuses are in the preceding final basis and
[validation](../evidence/w5/validation.json). D-39 records precision, source-premise status and limits.
No specification, provider policy or production parameter was changed.

### D-37. Attribute sent-COMMIT standby wait with an observer on stock PostgreSQL

**Decided 2026-11-12 by the owner (owner pick 2026-11-12).** This settles D-36's design
question; W5 implements it with evidence. Asked how the engine tells actual standby wait from
other sent-COMMIT time, the owner chose **"A: stock database (Recommended)"**: stock PostgreSQL
with an application-side observer, in its observer re-establishment form.

- One dedicated, lifetime, read-only engine observer session, opened at startup through the
  existing checked `Store::dedicated()` path, outside every pool, counted in the startup capacity
  check and in a D-6 connection-budget row. The API process gets none.
- While a sent COMMIT is registered, the observer samples `pg_stat_activity` for its backend at
  a fixed period. The exempt span runs from the first to the last sample that matches the
  backend's pid and start time as `active`, waiting on `SyncRep`, running the store COMMIT.
  Everything else is charged, including missing or failed samples and identity mismatch.
- The engine charges the attempt's elapsed time, from attempt start to receipt delivery, minus
  that span. Sampling error only overcharges.
- Observer loss is logged loudly and the time no observer saw is charged. The engine then
  re-establishes the observer through the ordinary checked connection path within the existing
  bounds, with no pool or bound increase, so attribution resumes for later attempts.
- Attribution never changes the commit outcome, checkout or permit retention, or stop
  deadlines. No PostgreSQL change, no new privilege grant and no new build dependency.

A four-reader panel (the fifth seat was unavailable) stayed tied 2-2 after one consensus round:
two readers chose this form, two chose a patched PostgreSQL 18.6 build that meters the SyncRep
wait inside the server. The tie went to the owner.

- Strongest argument for the stock observer: no checked session can open while a standby stall
  lasts, so even exact server-side attribution would not change the outcome of the case that
  splits the panel, and a patched server would still need this charging rule once a primary
  restart erases its meter.
- Strongest argument for the patched server: it measures exactly the interval OPS-49 excludes
  (pinned `03-operation-lifecycle.md:1976-1977`, STO-7 `05-persistence.md:180-182`), while a lost
  observer charges real standby wait and can turn it into exhaustion.

**Known limit.** If the observer is lost during a standby stall and the COMMIT reply is then
lost, that standby time stays charged. The result is at most one alarmed engine exit on OPS-49's
ordinary exhaustion path, with no money loss.

### D-38. Charge sent-COMMIT time at completion only

**Decided 2026-11-12, accepted under the owner's 2026-11-05 panel rule (three of four readers).**
The engine charges a sent COMMIT's time to the OPS-49 retry bound when its receipt resolves: the
reply arrives, or keepalives find the transport dead. Accounting triggers no exit while a COMMIT
is still outstanding; TCP keepalives bound the difference. Three of four readers picked this;
one picked in-flight exhaustion, in which the receipt resolves early as unresolved once charged
time reaches the remaining budget and the engine takes its fatal exit. No narrowing or widening.

**Known limit.** A live transport that keeps answering keepalives but withholds the COMMIT reply
forever never triggers an accounting exit.

### D-39. Conservative matching runs and completion-only attempt arithmetic

**Measured 2026-11-12.** Implement D-37/D-38 on stock PostgreSQL 18.6 from D-3, with
no dependency or privilege change. `EngineDatabase::start` acquires the startup lock, opens one
read-only observer through `Store::dedicated()`, then advances generation as its first write.
The observer is outside all pools, separate from the lock and compatibility checkout. The API
gets none. Capacity checks include seven possible engine startup sessions: three worker checkouts,
one compatibility read, lock, observer and transient generation session; steady engine capacity is
six. The core run refuses both the formerly-fitting 18-worker configuration and 25 workers before
lock acquisition/work. The local ordinary capacity is 24 minus three reserved slots.

`own_commit` registers owner, pid, backend start and dispatch instant before first polling COMMIT,
then deregisters at completion. `Accounting::observe` samples one autocommit statement per fixed
20 ms period while registrations exist. Identity, `active`, `SyncRep` and exact store COMMIT text
must match. Choose the conservative unbroken-run adjustment: a failed/missing/nonmatching sample
or reconnect ends the run. Do not hold a span open through reconnect. Each run exempts
`max(0, last_sample_sent - first_sample_received)`. The engine charges attempt-start to receipt
delivery minus summed exemptions, once, including precommit verification, queueing, non-standby
reply delay and read-back; ordinary backoff is also charged. Serialization/constraint treatment,
claim uncertainty and startup exceptions remain unchanged.

Error/timeout/closure/missing visibility is logged at error level. Re-establishment uses the
ordinary checked construction path and existing connection/check bounds, with one observer and
no pool increase. Unobserved gaps remain charged. The [accounting evidence](../evidence/w5/accounting.json)
contains exact backend identities, first/last sample send/receive instants, run arithmetic, sample
counts/round trips, actual observer failures and passing checked replacement. The long-wait, mixed
and idle cases retain their
measured exemption and charge arithmetic there. `w5_accounting_failure_run` first establishes a
real matching run and independently observed settlement SyncRep, then terminates only its idle
observer. A test-only observation records the run endpoints before/after failure; the isolated
`w5_observer_run` defect skips only `end_run` and fails the closure assertion with a genuinely
open run. The ordinary run separately measures more than two seconds of unobserved SyncRep,
retains the sent checkout, restores the same standby and admits a checked replacement. Receipt
death charges that interval, causes the ordinary exhaustion exit, and preserves the full succeeded
state and one purchase through actual successor startup. These are fixture measurements; the
mutation proves the internal closure invariant rather than a second SyncRep wait on that COMMIT.

Normal sampling can only overcharge under the premise of one contiguous SyncRep wait per COMMIT;
edge error is approximately `2 × (period + sample round trip)` per uninterrupted run.
Lost/missing observations can overcharge much more. The PostgreSQL 18.6 source check of that
contiguous-wait premise is **unverified**: this run inspected no server source to establish it.
Executed real-server samples demonstrate the named histories, not every possible server schedule.
This selects measurement precision for the prototype, not production values or an outage bound.

The two accepted limits remain as D-37/D-38 state: observer loss during a standby stall followed
by lost COMMIT reply can cause one alarmed ordinary exhaustion exit without money loss; a live
transport answering keepalives while withholding the COMMIT reply forever causes no accounting
exit. No in-flight exhaustion, sent-COMMIT timeout or stop-bound extension was added.

Basis: final runtime/fixtures, versions, gate/full-suite/baseline exits 0 and evidence paths in
D-34/D-35's final measured basis above. [Source](../evidence/w5/tested-source.json) and
[validation](../evidence/w5/validation.json) bind the exact bytes and commands.

### D-40. Observe arbitration, original admissions and failed-attempt release

**Measured 2026-11-12.** The executable's second test stop calls real `record_stop` arbitration,
then captures deadline, record identity, error and fatal state, including the fatal return. The
isolated reset sensitivity changes the pending-deadline branch to reset and return success; its
child exits 1 at `OPS-21 later stop arbitration must preserve original absolute deadline`, after
observing the changed deadline and nonfatal return. The final unmutated create arms pass.
This is a fixture sensitivity correction, not an old-runtime deadline-reset defect.

Healthy designated history/identity/catch-up is independently asserted before each withheld
API/engine connection-check response. Cancelled admission requires its original owner's actual
Acknowledged classification, then identifies the original persisted receipt/operation before
replay. Process-death recovery identifies that original durable admission and residual completion
before restart/replay; it makes no posthumous application-acknowledgement claim.

Failed engine statements use an acknowledged ROLLBACK when transport permits, then release their
original checkout and component permit before backoff; transport failures discard the session.
Unique transaction identifiers associate retry, rollback command/reply, backend identity and
actual semaphore/pool observations. Contention records server ROLLBACK/idle acknowledgement;
severed settlement records independent backend absence. These observations supplement actual
pool/semaphore release, rather than treating a proxy closed flag as component ownership.
Provider outcome and confirming-read time remain outside the retry closure. The quiesced
single-operation fixtures now require exactly zero checked-out worker write connections as well
as the released component permit before backoff. The direct helper accepts actual release and
rejects otherwise-identical retained-checkout records; the original helper accepted count one.
Blocked settlement also executes ordinary successor startup for the same operation, asserting
complete durable snapshot, provider evidence and single-order preservation before its final trace.
[Release evidence](../evidence/w5/release-assertion.json) retains the direct comparison and real
PostgreSQL observations.

[Scenario assertions](../evidence/w5/scenario-assertions.json), [controls](../evidence/w5/final-controls.json),
[regressions](../evidence/w5/regressions.json) and the final measured basis above record exact
versions, commands/exits, driver/source identity and cleanup. API retry semantics, ordinary pool
bounds, money guards and the accepted deployment profile are unchanged.

### D-41. Delete the machine as W6's non-create mutation

**Settled 2026-11-12 by a four-reader implementation consultation, four of four** (the fifth seat
was unavailable). W6's non-create mutation is `delete_machine` on the local immediate-destroy
simulator profile, with no surviving billable attachments. It is implemented only after the create
reconciliation scenarios pass.

The first private pilot's scenarios end on a termination path, and only a delete ends provider
cost: a powered-off machine still bills (`LDG-37`), and `PRV-13b` requires an API path that stops
the cost. Power on/off was the alternative and was not chosen. The local profile narrows the
provider shape to `PRV-13`'s immediate destroy; it waives no rule the delete reaches. The scheduled
cancellation shape is not built.

### D-42. W6's delete covers every cleanup W6 makes reachable

**Decided 2026-11-12, accepted under the owner's 2026-11-05 panel rule (four of four readers;
the fifth seat was unavailable).** The delete worker runs the caller's delete and every
exposure-reducing cancellation W6 itself makes reachable, `OPS-36`'s late-attach cleanup included,
with `OPS-39`, `OPS-41`, `OPS-42`, `OPS-44` and `OPS-48` applied in full. All four readers rejected
a caller-only worker that leaves system deletes refused.

Reachability: a create whose reply is lost stays unresolved; once `OPS-33`'s bounded negative
window elapses its commitment is released while the operation stays open, and the account sweep
keeps searching (`03-operation-lifecycle.md:1735-1738`). A later exact correlator match is
`OPS-27`'s row 1, "`OPS-36` where it was already released" (`:570`), whose transaction attaches the
machine and enqueues its cleanup (`:663`). Refusing that cleanup leaves an attached, unfunded
machine billing with nothing to stop it; refusing the attach instead is the hole `OPS-36` closes
(`:716`).

Conditions, each four of four:

- **(a)** The late-attach cleanup runs with `OPS-42`'s fence column (`machines.destroy_committed`),
  `OPS-41`'s ordered decision in the `LDG-35` transaction that writes the fence, `OPS-48`'s episode
  states and close reasons as `STO-52` stores them, and `LDG-40`'s wind-down floor or `LDG-66`'s
  deficiency in the attach transaction.
- **(b)** `OPS-41`'s restore deferral is `not run`, owned by W7, and ordinary startup keeps refusing
  an open recovery incident. The exhaustion sweep and the suspension fan-out stay refused entry
  points, reported `not run`.

No narrowing or widening of an earlier owner pick.

### D-43. Stalled-episode retry and keep are built in W6

**Decided 2026-11-12, accepted under the owner's 2026-11-05 panel rule (three of four readers,
with the reader the rule requires in the majority; the fifth seat was unavailable).** `API-64`
retry and `API-68` keep are built for a stalled episode. A cleanup attempt that settles `failed` or
`not_applied` leaves its episode `stalled` (`OPS-48`), and these two operator routes are its only
exits; today both are hidden. One reader would have kept them refused. No narrowing or widening of
an earlier owner pick.

### D-44. No extend-runway for a late-attached machine

**Decided 2026-11-12 by the owner.** Asked whether W6 builds `LDG-62` extend-runway, which lets a
tenant fund an `OPS-36` late-attached machine and keep it, the owner chose **"Leave out
(Recommended)"**. The late-attach cleanup always runs. A four-reader panel stayed tied 2-2 after
one consensus round, so the question went to the owner.

**Known limit.** `OPS-36`'s survival path, in which the tenant extends the runway and the machine
is kept, is `not run`. W6 adds no extend-runway route.

### D-45. A queued suspension cleanup runs through the same worker

**Decided 2026-11-12 by the owner.** Asked how W6 treats a queued delete whose enqueue reason is
`tenant_suspended`, the owner chose **"Same worker"**. The cleanup worker runs every queued
exposure-reducing cancellation whatever its enqueue reason, including a queued `tenant_suspended`
attempt. `OPS-41`'s ordered decision handles it on the tenant's current state, by step 2 while the
tenant is still suspended. The suspension fan-out
enqueuer itself stays refused, `not run`. The panel had no majority on this point.

### D-46. The no-rate wait is not run

**Decided 2026-11-12 by the owner.** Asked whether W6 builds `OPS-41` steps 4-5, the branches
taken when the machine's currency has no rate, the owner chose **"Leave out (Recommended)"**.
Steps 4-5 are `not run`. If an attempt reaches the no-rate branch, it stops safely: it makes no
provider call and writes no fence, and the path is stopped and reported. Only fixtures write rates
in this profile. The panel had no majority on this point.

### D-47. A delete reply that reports gone settles the delete

**Decided 2026-11-12 by the owner.** Asked whether a delete reply that itself reports the machine
gone settles the delete, the owner chose **"Yes, reply settles (Recommended)"**. That reply is the
mutation endpoint's own report that the goal state holds, the strongest source `PRV-36` ranks
(`02-provider-contract.md:1147`). It settles the attempt `succeeded` and is the gone-write and
meter stop in the terminal transaction, at the reply's observation instant (`PRV-29`, `LDG-74`). A
read inside the visibility window that still shows the machine is no evidence (`:1172`) and does
not override it. Three of four readers chose this form, but the reader the owner's 2026-11-05 rule
requires was in the minority, so it went to the owner.

### D-48. An acceptance-only delete reply goes to reconciliation

**Decided 2026-11-12, accepted under the owner's 2026-11-05 panel rule (three of four readers,
with the reader the rule requires in the majority; the fifth seat was unavailable).** A successful
immediate-delete reply that reports acceptance but not that the machine is gone does not
establish absence (`PRV-29`, `02-provider-contract.md:480-485`). The attempt enters
`needs_reconciliation` as known acceptance, with no gone-write on the reply. `OPS-27`'s non-create
evidence path resolves it after the visibility window without repeating the delete: absent
resolves `applied` with the gone-write, and present past the window resolves `not_applied`.
`WIR-35` is the operator fallback. A reply that reports gone is D-47's case. One reader would have
settled every successful reply as gone. No narrowing or widening of an earlier owner pick.


### D-49. Local reconciliation evidence and independent lifetimes

**Demonstrated 2026-11-12; timing corrected 2026-11-13.** Schema/semantics 3 retains write-once resolution/evidence,
append-only provider observations and original pending terms. Timing is conservative: the persisted
wait end is a settlement-time upper bound for worker classification, or the first startup
classification timestamp for an interrupted running mutation. Previously unresolved rows with no
origin freeze their last updated_at once under tenant serialization, before evidence writes.
Evidence changes and later ordinary restarts cannot renew that origin. Sample start uses the
acknowledged licensing/write-start marker. These bounds are not a measurement of
the exact wire-dispatch instant. Independent simulator acceptance/allocation timestamps provide the
separate measured allocation basis. Resolution uses its own conditional
needs-reconciliation/write-once guard, separate from a worker's claim-number guard. Exact
all-channel union plus direct confirmation can land one machine; failed, partial and duplicate
evidence cannot. OPS-33 releases money independently of STO-58's exclusion. Late attach atomically
records unpaid seed, separate fee/unit deficiencies, floor-or-deficiency and cleanup.

The [W6 measurement](../evidence/w6/final-w6.json) records twenty ordinary simulator allocation
samples. Its negative window is ceiling(worst observed allocation) plus 59 conservative seconds,
60 seconds in the recorded run. The descriptor's one-second visibility declaration remains
explicitly unmeasured; stored request-to-observation samples widen the effective maximum and
surface declaration breaches. These are local simulator parameters, not another provider's bound.

### D-50. Isolated static operator authority

**Demonstrated 2026-11-12.** Optional loopback operator authority is loaded from a private
principal/environment-name/admin file; token values come from environment, are hashed and are
checked against customer/recovery credential digests. Customer routes hide operator verbs and
preflight without CORS. Supported synchronous per-kind decisions and stalled retry/keep retain
principal-scoped receipts, authenticated identity and evidence in the business transaction.
Operation enumeration supports intersecting status, origin, reason and terminal filters, with
principal/effective-tenant scoped pagination under OPS-26/API-23.
Operator observed attachment does no external I/O and stores a null constraint date. This is the
local authority boundary, not production credential custody.

### D-51. Immediate-destroy execution and reached exit accounting

**Demonstrated 2026-11-12.** D-41–D-48 are unchanged. Caller, late-attach and already-queued
suspension cleanup share the delete worker. The ordered current funding decision and conditional
open-episode fence share LDG-35; reason history and never-fenced operator origin do not create
step-2 authority. A gone reply settles at its observation instant; mere acceptance requires
non-mutating later evidence and never a second delete. Retry/keep and permanent episode closure
are exercised against real rows and concurrent requests.

Exit increments use durable fixture rate-acceptance history, UTC month splits and reduced exact
rounding credits. Clamp deficiencies retain their piece's valuation; rate-outage records remain
native-only and cover maximal spans. Offline migration audits key-derived one-sided high-water
and credit range. Subject quarantine alerts preserve the meter while admitting deletion and closure
of an already-open outage. Fixture inputs are not the general rate estimator or periodic meter.
No-rate cancellation steps 4–5 stop without fence/call; W7 incident work remains refused.

### D-52. W6 verification and evidence boundary

**Recorded 2026-11-13.** PostgreSQL 18.6, Rust 1.97.1, Cargo 1.97.0, Python 3.14.7,
Nix 2.34.9; tokio 1.54.1, tokio-postgres 0.7.18 and deadpool 0.12.3. Exact versions,
configuration, source inventory, full command/status proof and output hashes are in
[validation](../evidence/w6/validation.json). On its final committed operational source:

- `nix develop --command bash tools/check.sh`: exit 0.
- `nix develop --command bash tools/postgres.sh --seed 20261112 --evidence <private-evidence>/all.json`:
  exit 0, all eleven phases and 31 W6 groups.
- `nix develop --command python3 -m unittest discover -s tests/postgres -p 'test_*.py'`:
  exit 0, nine tests.

All 39 isolated controls exit 1 at recognized assertions after activation, with cleanup passing;
the four new W6 controls detect blind repeat, non-exact attachment, premature money release and
startup-pass skipping. Two separate unchanged-base regressions reproduce actual old worker/adapter
faults; their final drivers pass on the fixed source. All 32 protected base runtime/migration/
dependency hashes are unchanged before/after. Create reconciliation passed its focused checkpoint
before delete implementation began. Historical W4/W5 unresolved and accounting fixtures now hold
explicit provider/evidence boundaries; no retry bound, observer fault or original release assertion
was weakened. New evidence, source protection, cleanup and recursive redaction are recorded in
[report](../evidence/w6/report.md). D-6 remains Open for production/W8.

### D-53. Blocking reconciliation and operator repairs

**Demonstrated 2026-11-13.** The nine current-W6 regressions are recorded as `w6_r1` through
`w6_r9` in the [final W6 inventory](../evidence/w6/final-w6.json). Their unchanged-current-W6
reds protect 37 runtime, migration and dependency/configuration files. Each exits 1 at its intended
assertion; each corresponding final green passes with cleanup. These are distinct from the two
original review-base defects. [Validation](../evidence/w6/validation.json) records the exact versions,
commands, true exit statuses and source identity for the final fast gate, eleven-phase PostgreSQL
suite and nine Python units, all exit 0.

OPS-33 local expiry follows eligible pending evidence updates and precedes account enumeration.
It applies to healthy and unhealthy accounts even when PRV-36 forbids reading, retains positive
in-progress knowledge and releases only money. D-49's persisted origin prevents an evidence write
or ordinary successor from restarting the timer. The one-second unmeasured declaration and the
retained observed maximum keep their existing PRV-36/STO-53 meaning; the separately measured
negative window remains 60 seconds. Pass duration includes ordinary evidence I/O and is not a
production outage guarantee. Startup also applies OPS-48's current-open-episode transition in the
same serialized, generation-guarded transaction, without changing its actual fence or permanent close.

PRV-41 rejection is durable `credentials_rejected` status, with monitorable account/path evidence.
Every reached reconciliation provider call checks the current credential state, so a previously read
pending snapshot cannot issue remaining calls after rejection. Other accounts and local expiry
continue. A confirming read followed by failed constraint authentication still settles the confirmed
create with a null date under PRV-13c. Malformed successful keys replies and general throttle pacing
retain their deferred behavior.

SEC-39's provisional resolution allowance is `operator_resolution_limit` when supplied, otherwise
`created_limit` (100 in the ordinary local fixture), per `ceiling_seconds` (3600 locally). A supplied
allowance must be a positive wire-safe integer. Principal serialization spans target tenants; replay
returns before allowance checks, and the consumed slot, resolution and receipt commit together.
The regression uses two slots in a stated 86400-second fixture interval and verifies exhaustion,
concurrency, another principal and actual API restart. This selects no production allowance or
incident-override policy. Retry retains its separate ceiling; keep gains none.

Structured resolve/retry/keep events name principal, effective tenant, target, correlation and safe
reason reference. Free-form retry reasons are hashed. Intent, acknowledged database commit,
unacknowledged commit uncertainty, refusal and replay are distinct outcomes. A retry's acknowledged
event names its fresh operation. Operator operation detail shares the existing logical expiry and
retained-receipt horizon; a retained episode supplies tenant scope for its retired current attempt.
Unknown and out-of-scope IDs remain hidden; no receipt horizon is extended.

OPS-2 retains the validated live caller-delete body, and OPS-23 repeats caller fields first, then
API-written target evidence, before the fence decision, capability, marker or provider call. System
cleanup and operator retries require no customer acknowledgment. The offline schema-2-to-3 program
fills only the supported empty-summary system cleanup shape from its matching tenant, machine,
account and open episode; unrelated malformed rows fail safely before dispatch. No operation is
re-enqueued, and no mutation success is inferred by the migration.

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
W5 adds one dedicated read-only observer and includes the transient generation-advance session
in startup capacity checking. W3 did not implement a periodic scheduler. W4 adds local create dispatch polling; general
maintenance/reconciliation remains deferred. The Store-retry bound is 2000 ms. Production capacity,
retry policy, alarm thresholds and restart policy remain unselected.
The lifecycle fixture uses a 1000 ms stop bound and exits unresolved at that bound; two complete
read/check attempts are allowed for a broken destination-read session. These are test values,
not outage guarantees.

| OVR-19 owning row | Requirement | Provisional local fixture value |
|---|---|---|
| Store timeouts | STO-7, STO-55 | statement 1000 ms; lock 500 ms; idle transaction 1000 ms |
| API read/write concurrency | STO-55 | one checkout in each ordinary pool |
| Engine worker concurrency | STO-55 | three retained worker permits and three write checkouts; one compatibility read checkout; startup lock and one read-only accounting observer outside pools |
| Startup wait / Store retry | OPS-15, OPS-49 | 500 ms / 2000 ms |
| Pool checkout bound | STO-55 | 250 ms |
| Startup-lock connection keepalives | OPS-47, OVR-19 | explicit local assignment sharing pooled values; client/server idle 2 s, interval 1 s, count 2; validated and observed on lock session |
| Pooled-connection keepalives | STO-7 | client/server idle 2 s, interval 1 s, count 2 |
| Stop-record bound | OPS-21 | 1000 ms in lifecycle and W3 worker record fixtures |
| W6 reconciliation scheduling | ENG-05, OPS-32/33 | one-second delay between supervised passes on ordinary admitted pools; pass duration includes evidence I/O; no outage bound or general maintenance scheduler; production policy remains W8 |
| Connection budget | STO-55 | local max 24, reserved 3 (21 ordinary); engine 6 steady / 7 startup; API 2; offline migrator plus waiting runner 2 with components stopped; remaining operator/fixture/replication headroom, no residual-backend upper bound |

Client call/connect 2000 ms and check 3000 ms are implementation bounds obeying STO-7/OPS-49's
server-timeout floor. The store owner has no commit deadline. D-6 selects no production retry,
reconnect or alarm policy; full capacity and recovery evidence belongs to W8.

When an open entry is settled, move it to "Settled" with the exact versions, commands, exit statuses
and evidence paths that meet its basis. Preference, scope and cost questions go to the owner.
