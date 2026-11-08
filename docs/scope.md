# Scope

Specification pin: `douglaz/provisiond@ac37c7cae5d3ff746515137774a4cdd5a3531375` (see
`spec-lock.json`). The pinned requirement bodies own every behavioural rule. This file records what
this repository does and does not claim; it adds no requirement of its own.

**This repository is a reduced internal prototype, not a conforming service.** It must not be
exposed as a public provisiond service, pointed at real provider accounts, or given real funds.

## What exists

| Category | Contents | Status |
|---|---|---|
| Implemented behaviour | The two-node harness bootstraps/rebuilds a standby and restores an archived-WAL fixture. The store spike applies physical-session settings, checks a fixed flushed prefix before admission and after destination reads, discards interrupted reads/transactions, closes retained pre-COMMIT connections on deadline, and supervises sent COMMITs with warning-aware outcomes on process-owned I/O. The `migrate` binary applies offline versioned migrations through the same COMMIT owner. The engine checks compatibility, holds a lifetime lock, advances a UUID generation, fences transactions and performs ordinary restart recovery. Process-owned claims and defers retain worker capacity through caller cancellation. Acknowledged defer completion retires only its claim. Execution cancellation covers licensing through dispatch; stop records preserve returned outcomes and enforce absolute deadlines without a caller waiter. Concurrent observers share one lock-loss fan-out. Terminal stop records retire only their execution, report zero-row refusals accurately and return capacity after acknowledgment. Detection and user stops share a consistent ordering. Engine-only methods reject unfenced transactions. Typed library methods implement claims, guarded settlements, reservations and payment credits. | demonstrated subset; [historical W3 evidence](../evidence/w3.md), [defer supplement](../evidence/defer/report.md) |
| Local service behaviour | Loopback customer HTTP funding/create subset, common simulated rail ingestion and automatic healthy assignment, ordered local create dispatch and verified atomic settlement. The supported path and its limits are described below. | implemented; final W4 recording pending |
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

Lock loss refuses queued claims after their component/pool waits, before BEGIN dispatch, and
permits shielded records for current workers. Already-dispatched claims keep bounded stop ownership.
Active records remain visible by operation/claim even after replacement of the worker registry entry;
expiry names the unobserved claims and later observers continue waiting for them. Existing records
keep their original bounds; after-return outcomes retain their settlement loop. Each claim owns
its returned outcome independently of the operation-id registry: a displaced execution retains its
verified return for its own guarded record and cannot supply a newer claim's result. The simulator
regression checks the stale zero-row record, the newer claim's refusal without its own result and
its distinct verified settlement/repeat with no stale machine or money effects. A true user stop
after loss detection takes the later-stop exit even before fan-out registration. A terminal guard
refusal leaves the newer row and money unchanged, reports that observation and ends the stale
worker. Completed records release capacity while captured observers retain their result. Expiry, a later
stop, retry exhaustion and supersession require process exit, retaining uncertainty about any last
reply. The dedicated replication-freeze stop-record drill (ENG-07) remains not run. ENG-05 automatic
provider reconciliation and DB-10 deployment-wide saturation also remain not run. Local pool
budgets do not guarantee observer access or bound residual backends across process lifetimes.

The API serves only the explicit loopback `local-simulator` profile after schema compatibility.
The engine dispatches queued creates after its acknowledged startup generation and ordinary restart
pass, retaining the existing lock-loss, claim and stop ownership. Unsupported operation kinds are
refused. This is a reduced customer service, with no real provider or rail adapter. The local
profile's three workers, pool sizes, timeouts and retry/stop bounds remain provisional under D-6.

[Historical W3 acceptance](../evidence/w3.md) records its complete seven-phase run on committed source,
including the full W1/W2 inventory, 80 positive scenarios and twenty-six intended negative controls.
Queued-entry evidence requires the original pending acquisition, one attempt, no precommit retry
and checked refusal. Normal lock-observer evidence requires the actual post-registration read.
Both the repository gate and canonical PostgreSQL command exited 0. Demonstration applies only to
the explicitly traced arms; the deferred scenarios above remain not run.

The [defer cancellation supplement](../evidence/defer/report.md) records fresh gate and seven-phase
PostgreSQL exits 0 on `966751005c34721fecefedff7a210a390d657cb8`, with 81 positive entries and 27 expected negative controls.
Four cancelled defers reuse three slots; delayed true/false completions preserve a replacement
worker and its state. Cancelled and unpolled fatal receipts cannot suppress supersession exit.
The earlier evidence retains its original implementation identities. These checks add no service
conformance claim or production availability guarantee.


## Local funding and provisioning subset

The loopback customer listener supports `POST /v1/enrol/token`, `POST /v1/enrol`,
`GET /v1/enrol/{handle}`, deposit create/detail, assigned providers/offers, machine create/list/detail,
operation list/detail, balance with optional commitments, and unauthenticated OPTIONS. Credentials
are shown once and stored hashed. Recovery credentials do not authenticate ordinary routes; recovery,
maintenance and operator APIs remain unsupported. Traffic and enrolment concurrency state stays in
memory without persisted caller addresses. Principal create/spend ceilings are durable and share the
admitted operation's transaction. Replays precede suspension and ceiling refusal.

Deposits use nonpayable literal `TEST/SIMULATOR/NONPAYABLE` strings, with two independent simulator
destinations bound to one retained deposit. Final settled Lightning and sufficiently confirmed
on-chain observations enter one trusted internal credit boundary, using payment hash/outpoint identity
and actual settled amounts. Nonfinal observations cannot activate or credit. Both destinations can
credit; there is no refund. Below-threshold credits leave enrolment pending until its original expiry.
Destination responses, including replay, require a new post-read flush check on the same uninterrupted
PostgreSQL session. Orphan credit remains retained; recovery by re-attribution is unsupported here.

Activation atomically credits and assigns one healthy assignable account by least-assigned count,
with account-name order breaking ties. Activation without any healthy assignable account still
succeeds and emits a diagnostic, leaving assignment empty. A later assignment sweep is not implemented.
Lightning expiry and cancellation are modeled separately from on-chain expiry: an expired on-chain
destination remains payable but unwatched; during a solvency halt the simulated invoice is cancelled
and observed on-chain credits remain unsecured claims. No simulator string can receive actual funds.

The stored listing supplies create's capability gate, ordered refusal collection, price and frozen
summary. Admission conditionally guards the account and resource offer, then atomically opens the
commitment, queued operation, receipt and principal counter increments. The worker repeats ordered
checks against one current listing, compares native coverage at the accepted conversion basis, validates
forwarded values, and awaits acknowledged account/tenant/correlator licensing before temporary-key
registration or ordering. The independent provider process records request, acceptance, creation and
returned-response stages outside PostgreSQL. Verification searches the configured channel and directly
reads the resource before the post-order cancellation-constraint read and atomic settlement.

Only one virtual simulator product, one reachable ordering channel per account, hourly native pricing,
no billable attachments, unlimited traffic and an already-effective cancellation bound are supported.
The driver adapter can stop a separate simulator fixture and confirm absence. Production deletion,
suspension fan-out and settled-key cleanup are not demonstrated: a verified attach while suspended
retains its required queued cancellation obligation without claiming a complete cancellation worker.
An accepted order with a dropped reply remains `needs_reconciliation`, with its reserve intact and no
blind resend. This is a partial lost-reply demonstration, not ENG-05 automatic reconciliation.

Pricing uses checked rational native amounts, margin on machine time, one conversion/haircut and
upward commitment rounding. Settlement retains accepted setup/unit authorization, pairs customer
debit with commitment decrement, clamps at remaining authority and records native operator remainder.
The first-unit debit reference exists only for a nonzero customer debit; paid-first-unit seed and
protected runway projections are implemented. If the current currency rate is unavailable, accepted
settlement and the eligible seed still commit; runway re-derivation stops and the new machine's
runway/rate/protection fields stay null. No exhaustion is inferred. Later re-derivation, general rate
construction, recurring metering, late attach and reconciliation remain deferred. Rate fixtures are
controlled inputs, not a real market feed.

`tests/postgres/workflow.py` owns the disclosed local parameter fixture: activation 100 sats, a 3600 s
deposit, 600 s finality allowance, 10800 s pending lifetime, 64 pending tenants, a 50 ms admission delay,
60 s token validity, 256 spent nonces, two held token requests per source, separate 1000-request read
and write budgets per 1000 ms, 100 creates and 1000000 sats per 3600 s principal interval, 3600 s
operation retention, 86400 s receipt retention, 3600 s listing age, 1/20 sat Lightning/on-chain floors
and confirmation depth two. Enrolment concurrency pacing is a constant 1000 ms independent of delay.
Supported traffic configurations must allow list, balance and machine reads at the finest advertised
1000 ms cadence. Parameter/descriptor files and simulator addresses are required; these settings do
not select production capacities or close D-6/7/8/9.

The canonical runner requires nine phases and retains the full W1–W3 inventory plus the workflow,
default-feature workflow and three additional sensitivity controls. Historical W3 evidence above
retains its original source identity. Fresh W4 evidence is recorded separately.

The local fixtures have observed cold API/engine setup refusals at the unchanged 250 ms checkout
bound. Their underlying cause is not established. The application process exits on that refusal.
For ordinary API setup only, the fixture may make at most three separate deployment attempts after
recording a failed process that never opened its listener or began business/funding/external work.
It records each actual exit and both log hashes, and repeats nothing while a transport fault is
armed or a defect build is selected. Engine startup and negative startup tests receive no such
retry. These are fixture deployment attempts, not runtime recovery or a production availability
claim; failed or unactivated fault scenarios remain failures or inconclusive observations.
