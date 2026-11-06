# Scope

Specification pin: `douglaz/provisiond@ac37c7cae5d3ff746515137774a4cdd5a3531375` (see
`spec-lock.json`). The pinned requirement bodies own every behavioural rule. This file records what
this repository does and does not claim; it adds no requirement of its own.

**This repository is a reduced internal prototype, not a conforming service.** It must not be
exposed as a public provisiond service, pointed at real provider accounts, or given real funds.

## What exists

| Category | Contents | Status |
|---|---|---|
| Implemented behaviour | The two-node harness bootstraps/rebuilds a standby and restores an archived-WAL fixture. The store spike applies physical-session settings, checks a fixed flushed prefix before admission and after destination reads, discards interrupted reads/transactions, closes retained pre-COMMIT connections on deadline, and supervises sent COMMITs with warning-aware outcomes on process-owned I/O. The `migrate` binary applies offline versioned migrations through the same COMMIT owner. The engine checks compatibility, holds a lifetime lock, advances a UUID generation, fences transactions and performs ordinary restart recovery. Process-owned claims retain worker capacity after caller cancellation. Execution cancellation covers licensing through dispatch; stop records preserve returned outcomes and enforce absolute deadlines without a caller waiter. Concurrent observers share one lock-loss fan-out. Engine-only methods reject unfenced transactions. Typed library methods implement claims, guarded settlements, reservations and payment credits. The API checks compatibility and remains non-serving. | demonstrated subset; [W3 evidence](../evidence/w3.md) |
| Simulator-only behaviour | Deterministic acceptance/reply pause hooks and Rust simulator entries; verified-create outcomes drive the atomic settlement primitive, without real provider/rail drivers. Simulator evidence is never substituted for provider evidence. | simulator paths implemented |
| Unsupported behaviour | Everything in "Not supported in this profile" below, and every v1 requirement not marked implemented or demonstrated in `docs/conformance-map.md`. | by design |
| Production work not yet attempted | Real-provider drivers, rescue installation, every operation kind beyond the first slice, a customer UI, security review, key custody, billing operations, separate-host deployment rehearsal, operator runbooks, exposure limits. | not attempted |

## Operating assumptions

These are the accepted initial deployment profile's constraints, taken from the implementation plan
(section 1.2) and owned by STO-7, STO-12, STO-13, STO-54, STO-55, STO-59, OPS-47, OPS-50, ADR-0047
and ADR-0048 at the pin. Implementation work preserves them.

| Area | Accepted initial contract | Consequence for the work |
|---|---|---|
| Processes | One engine and one API instance in the supported initial profile | Do not build a distributed worker scheduler or add API replicas as an implementation prerequisite. Preserve the existing database concurrency primitives. |
| Database | One PostgreSQL primary and one designated synchronous physical standby in separate failure domains | The local harness may share a host, but that does not demonstrate failure-domain independence. |
| Commit durability | State-changing application work, migrations, and incident transactions require synchronous acknowledgment | Standby loss can block these commits. Do not introduce an asynchronous fallback. |
| Connections | New physical sessions pass a read-only flush-position check | Test the actual client and pool, not only SQL issued manually. |
| Deposit responses | A stored destination receives a post-read check on the same uninterrupted session | A previously checked connection is not sufficient for a later destination read. |
| Pooling | Ordinary bounded pools, no transaction-mode pooler | Do not rebuild the withdrawn external connection-reservation authority. |
| Saturation | Connection exhaustion and loss of fresh observer access are accepted outage outcomes | Bound retry behavior and document recovery. Do not claim that local pool limits globally bound residual backends. |
| Shutdown | Bounded stop-record handling remains in force | Holding a sent COMMIT while a process survives does not make engine shutdown unbounded. |
| Promotion | Operator initiated, always a recovery incident | Do not add automatic promotion or a lossless-promotion fast path. |
| Releases | Offline, with explicit binary/schema compatibility | Do not design rolling upgrades or mixed-release availability. |
| Engine fencing | Startup lock plus startup-generation checks remain | A single intended engine is not a reason to remove generation fencing. |
| Trust boundary | Deployment maintains replication configuration and excludes transaction pooling | No runtime configuration-drift controller is required by the accepted design. |

Two of these are stated explicitly because they are easy to reintroduce by accident:

- **No automatic promotion.** Promotion is always operator initiated and always opens a recovery
  incident, including a lossless promotion.
- **No continuous configuration-drift enforcement.** The deployment maintains the replication
  configuration. Deployment inspection, a documented configuration and negative tests of the trust
  boundary are not claims of continuous enforcement.

## Not supported in this profile

Each of these needs a separately approved specification change before any work here:

- automatic failover or automatic promotion;
- external connection admission or a durable connection-grant service;
- transaction-mode pooling;
- a runtime configuration-drift controller;
- a general write barrier before every response;
- rolling upgrades or mixed-release availability;
- an asynchronous commit fallback when the standby is unavailable.

## Non-goals for the first executable milestone

Full production launch, all provider drivers, rescue installation, every operation kind, a customer
UI, automatic failover, a new event bus, and a generalized workflow framework are not attempted
before the core path is demonstrated. PostgreSQL, the ledger, the operation lifecycle and the formal
models are not replaced.

These are implementation milestones, not removal of v1 requirements. A passing prototype subset is
not full service conformance.

## Evidence limits

- A local two-process PostgreSQL cluster tests PostgreSQL behaviour. It does not demonstrate
  failure-domain independence, arbitrary hardware durability or production fencing.
- The specification repository's archived PostgreSQL experiments used a primary with two standbys.
  They are historical evidence, not a result for this profile.
- Lean proofs in the specification repository are model results under stated assumptions, not
  implementation tests.

The prototype uses direct loopback TCP with TLS disabled in its isolated harness. It is not a
production transport/deployment implementation. Components retain transaction ownership through
cancelled receipts and sent COMMIT completion; provider/rail/rescue instrumentation refuses an
external entry while that component owns a transaction. A later generation cannot retract an
already-licensed simulator call, prove a stale process dead or fence two writable histories.

The schema supports the represented `create`, `refresh` and `power` primitives. Only create has a
cancellable execute simulator; provider execution for other kinds is explicitly refused. Ordinary restart
moves interrupted writes to reconciliation, fails refreshes and conditionally fails pre-order
creates with atomic commitment/exclusion release. Existing errors and markers survive. Open
recovery incidents stop the unsupported startup path. Parent fan-out, install/rescue workflows,
late attach, resolution, full metering/rates/solvency and operator recovery remain unimplemented.
Unsupported kinds are refused rather than silently requeued.

Lock loss stops new claims and permits shielded records for current workers. Existing records
keep their original bounds; after-return outcomes retain their settlement loop. Expiry, a later
stop, retry exhaustion and supersession require process exit, retaining uncertainty about any last
reply. The dedicated replication-freeze stop-record drill (ENG-07) remains not run. ENG-05 automatic
provider reconciliation and DB-10 deployment-wide saturation also remain not run. Local pool
budgets do not guarantee observer access or bound residual backends across process lifetimes.

The API has no listener: after a supported-schema check its ordinary entry explicitly exits 78.
The engine entry is a startup/lifetime monitor, with transactional worker primitives exercised by
the integration fixture. It is not a complete customer service or provider dispatcher. The local
profile's three workers, pool sizes, timeouts and retry/stop bounds remain provisional under D-6.

[W3 acceptance](../evidence/w3.md) records the complete seven-phase run on committed source,
including the full W1/W2 inventory, transactional-core scenarios and ten intended negative controls.
Both the repository gate and canonical PostgreSQL command exited 0. Demonstration applies only to
the explicitly traced arms; the deferred scenarios above remain not run.
