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
| Local service behaviour | Loopback customer HTTP funding/create subset, common simulated rail ingestion and automatic healthy assignment, ordered local create dispatch and verified atomic settlement. The supported path and its limits are described below. | demonstrated subset; [W4 evidence](../evidence/w4/report.md) |
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

The [W5 report](../evidence/w5/report.md) records activated local interruption scenarios through
real API/engine processes and independent simulators, with complete inventory, default-feature
checks and exact-assertion controls. It demonstrates selected create/record and connection arms,
not every operation kind in CNF-366, complete CNF-365, or deployment-wide CNF-373 recovery.
The process-death drill restores the same designated standby on unchanged history; it does not
exercise a promotion incident, production supervisor policy or loss of observer access.

**OPS-49 accounting is implemented for the supported engine paths, with conservative observation.**
One dedicated checked read-only observer, outside the pools, samples registered COMMITs every
20 ms. Exact pid and backend start, active state, SyncRep wait and store COMMIT text are required.
Only spans inside uninterrupted matching runs are exempt; missing/failed/mismatched observations,
reconnect gaps, idle reply delay and ordinary backoff stay charged. A receipt's completion charges
its complete attempt once, without abandoning a sent COMMIT. Engine startup includes the observer
and transient generation session in its ordinary capacity check; the API gets no such observer.
The final run records real wait, mixed wait/reply delay, failure during an established SyncRep
sample run, checked re-establishment, original stop precedence, and same-driver unchanged-runtime
reds. A test-only closure observation rejects skipping `end_run`; separate completion arithmetic
checks charging of the unobserved interval. Failed-attempt fixtures require zero outstanding
worker write checkouts before backoff. Blocked settlement runs an actual successor and preserves
the same operation, complete durable snapshot and single provider order. D-36 is settled by that
evidence.

The accepted limits remain: observer loss during a standby stall followed by a lost COMMIT reply
can cause one alarmed ordinary exhaustion exit without money loss; a live transport that answers
keepalives but withholds the COMMIT reply forever triggers no accounting exit. The PostgreSQL 18.6
one-contiguous-SyncRep-wait-per-COMMIT source premise is **unverified** here. D-39 distinguishes
executed attribution from source assumptions and production parameter selection. No timeout,
pool bound or deployment profile was changed. Independent acceptance is separate from these runs.

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

The schema supports the represented `create`, `refresh`, `power` and `delete` primitives. Create
and immediate-destroy delete have cancellable simulator execution; other provider mutations remain
refused. Ordinary restart
moves interrupted writes to reconciliation, fails refreshes and conditionally fails pre-order
creates with atomic commitment/exclusion release. Existing errors and markers survive. Open
recovery incidents stop the unsupported startup path. Parent fan-out, install/rescue workflows,
full metering/rates/solvency and operator recovery remain unimplemented. W6 implements create/delete
resolution and late attach with its atomic cleanup obligation.
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
reply. W5 demonstrates the dedicated replication-freeze stop-record drill's named create arm
(ENG-07): an older record retains its original deadline through lock loss and exits nonzero
while its sent COMMIT remains unacknowledged. W6 demonstrates selected ENG-05 simulator
reconciliation; DB-10 deployment-wide saturation remains not run. Local pool
budgets do not guarantee observer access or bound residual backends across process lifetimes.

The API serves only the explicit loopback `local-simulator` profile after schema compatibility.
It validates required parameter files and both API/rail socket addresses before binding or reporting
readiness. Missing, malformed, zero-port and nonloopback addresses refuse startup; standalone
`api --check-schema` requires no unused service parameters. Dedicated operator namespaces and the
method-scoped operator list return 404 on the customer listener, including for pending credentials.
Shared customer methods retain their ordinary projections or explicit unsupported outcome.

Periodic task errors, unexpected completion and panics are observed independently of customer
connections. A failure closes the listener, cancels and joins both periodic tasks, and gives owned
request handlers one second to finish before cancelling and joining the remainder. The existing
Process then has its unchanged one-second drain bound. A sent COMMIT remains process-owned even
after its handler is cancelled; unresolved work at that bound requires exit 1, with no rollback or
nonexecution inferred. Ordinary background checkout exhaustion also exits 1 with its component/error
diagnostic; it has no in-process engine retry loop. Recovery requires a separate supervisor/operator
restart after the underlying capacity/configuration problem is addressed, followed by ordinary keyed
request or adapter replay. This is bounded failure handling, not an availability guarantee.
The engine dispatches queued creates after its acknowledged startup generation and ordinary restart
pass. Executable lock observation runs independently of dispatch/claim COMMIT waits. A dispatch
refusal after loss joins the original record drain; late acknowledged claims inherit the original
loss bound. Pending sent transactions and worker capacity retain their existing owners.
The local executable regression covers actual SyncRep and separate late/empty reply cases; the full
older record/outcome matrix remains in core tests. This selects no guaranteed detection time.
Unsupported operation kinds are refused. OPS-49 also covers each worker preflight account, tenant
and channel SELECT as a complete autocommit read. Checkout, connection, check, query and backoff consume the existing 2000 ms
store-retry budget; sessions are released before backoff. Transient errors retain the running claim
and commitment. Exhaustion exits nonzero without worker classification, leaving ordinary restart
to classify the null-marker create. A survived stop cancels the read/retry and uses the existing
shielded cut-off record. No provider call is inside this retry loop; API reads keep their semantics.
Generated ordinary-restart errors include an empty details object. A create moved to
`needs_reconciliation` receives `retryable: false` in its generated stored error and HTTP view;
the null-marker failed-create exception retains `true`. Survived-stop cut-offs include a
human-readable message alongside their existing reason and retryability. Authenticated operation
detail and list reads check the exact four-field inner error, its types, agreement with the
authoritative view retryability and the view-level correlation identity for both restart branches
and both stop classifications. Ordinary errors and null-error successes share those HTTP checks.
Existing stored errors remain preserved under OPS-16.
This is a reduced customer service, with no real provider or rail adapter. The local
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
`GET /v1/enrol/{handle}`, deposit create/detail, assigned providers/offers, machine create/list/detail
and immediate delete,
operation list/detail, balance with optional commitments, and unauthenticated OPTIONS. Credentials
are shown once and stored hashed. Recovery credentials do not authenticate ordinary routes; recovery
and maintenance remain unsupported. W6 adds an isolated operator subset, described below. Traffic
and enrolment concurrency state stays in
memory without persisted caller addresses. Ordinary checked credential verification precedes the
failed-auth source allowance, so a full abuse bucket cannot refuse a valid principal sharing the
address. This source allowance is not a pre-database work bound. Principal create/spend ceilings are
durable and share the admitted operation's transaction. Replays precede suspension and ceiling refusal.

Deposits use nonpayable literal `TEST/SIMULATOR/NONPAYABLE` strings, with two independent simulator
destinations bound to one retained deposit. Final settled Lightning and sufficiently confirmed
on-chain observations enter one trusted internal credit boundary, using payment hash/outpoint identity
and actual settled amounts. Trusted positive final Lightning underpayments and overpayments credit
the received value while preserving the deposit intent. Nonfinal observations cannot activate or credit.
Both destinations can credit; there is no refund. Below-threshold credits leave enrolment pending until its original expiry.
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
returned-response stages outside PostgreSQL. Each order derives its resource ID from its own
fsync'd acceptance event. A separate-process HTTP regression forces overlapping acceptance/allocation,
checks two distinct commands and resources with their account/correlator/channel associations, and
checks inspection, search and direct reads before and after replay of the same durable store. This
is simulator evidence for the supported channel, without an automatic reconciliation claim.
Search copies the resource values under the existing lock before filtering, so concurrent creation
cannot invalidate its iterator. A separate-process HTTP case holds that iterator after its first
value while another order accepts, creates and returns, then checks the completed response, both
resources and replay. The explicit test-only barrier is absent from ordinary invocations.
Verification searches the configured channel and directly reads the resource before the post-order
cancellation-constraint read and atomic settlement.
The successful confirming read captures `state_observed_at` in its retained outcome. Later constraint
I/O, shielded stop records and whole-transaction repeats persist that same instant. The local first
seed still uses the billable row's creation instant, plus copied billing seconds only when the first
unit was actually debited. Its high-water mark and settlement effects commit together. General
metering remains deferred.

The profile supports one virtual simulator product, explicit ordering channels, hourly native pricing,
no billable attachments, unlimited traffic and an already-effective cancellation bound. W6 adds
immediate deletion, eligible temporary-key cleanup and execution of already-queued suspended-attach
cancellations; general suspension fan-out and production deletion remain untested. An accepted order
with a dropped reply initially remains `needs_reconciliation`, preserving its original reserve and
never blindly resending. W6 then resolves supported independent evidence or retains explicit uncertainty.
Accepted verification failures retain safe opaque provider IDs from the order reply, searches and
direct reads in `error.details.provider_identifiers`, grouped by observation source. These are
unverified evidence, never attachment or debit authority. Actual error kind/details, accepted
knowledge, frozen terms and the open reserve survive payload purge and ordinary restart.

The same retained evidence is demonstrated for survived stops during a pending search
and confirming get, before confirmation succeeds. The simulator publishes selected search IDs
before sending the response that permits get; the subsequent held get proves receipt. This
ordering check alone does not claim response delivery. Both real-executable windows acknowledge
their shielded cut-off record, exit nonzero and preserve errors/terms/reserve through ordinary
successor startup without another purchase. Their finite per-account simulator call allowance is
5 seconds, restored after the assertions; the original 1000 ms stop record and all application
startup, checkout, store-call and retry bounds remain unchanged. This adds cancellation coverage
of the existing correction, without establishing a new runtime defect.

The rail simulator returns its entire retained observation history, while the client caps response
bodies at 2 MiB. Bounded local fixtures do not demonstrate ingestion of arbitrary retained histories;
bounded delivery remains follow-up work.

Pricing uses checked rational native amounts, margin on machine time, one conversion/haircut and
upward commitment rounding. Settlement retains accepted setup/unit authorization, pairs customer
debit with commitment decrement, clamps at remaining authority and records native operator remainder.
The first-unit debit reference exists only for a nonzero customer debit; paid-first-unit seed and
protected runway projections are implemented. If the current currency rate is unavailable, accepted
settlement and the eligible seed still commit; runway re-derivation stops and the new machine's
runway/rate/protection fields stay null. No exhaustion is inferred. General background re-derivation,
rate construction and recurring metering remain deferred. W6 implements re-derivation for delete
decisions and keep, reached exit increments and late attach. Rate fixtures are controlled inputs,
not a real market feed.

`tests/postgres/workflow.py` owns the disclosed local parameter fixture: activation 100 sats, a 3600 s
deposit, 600 s finality allowance, 10800 s pending lifetime, 64 pending tenants, a 50 ms admission delay,
60 s token validity, 256 spent nonces, two held token requests per source, separate 1000-request read
and write budgets per 1000 ms, 100 creates and 1000000 sats per 3600 s principal interval, 86400 s
operation replay/readability and receipt horizon, 3600 s listing age, 1/20 sat Lightning/on-chain floors
and confirmation depth two. Enrolment concurrency pacing is a constant 1000 ms independent of delay.
Supported traffic configurations must allow list, balance and machine reads at the finest advertised
1000 ms cadence. Parameter/descriptor files and simulator addresses are required; these settings do
not select production capacities or close D-6/7/8/9.

The canonical runner requires ten phases and retains the full W1–W3 inventory plus the workflow,
default-feature workflow, W5 and thirty-five sensitivity controls, plus the direct release-assertion
probe. Historical W3 evidence above
retains its original source identity. Fresh W4 evidence is recorded separately. Default API startup
checks build into a separate target directory. The workflow records executable hashes and feature
modes for API/engine/migrator builds
and every API/engine invocation, plus the observation fixture's probe. Later lifecycle cases retain
the original hook-enabled API; the independent default workflow uses no test features. The delayed
observation cases use the independent simulator's attributed constraint hold. The existing
default-build defect-environment check remains part of acceptance.
The separate startup target is registered before building. Cleanup explicitly checks that its
default API was used and that both build directories were removed after child completion. Earlier
cleanup results covered registered resources only and did not cover the unregistered startup target.

The local fixtures have observed cold API/engine setup refusals at the unchanged 250 ms checkout
bound. Their underlying cause is not established. The application process exits on that refusal.
For ordinary API setup only, the fixture may make at most three separate deployment attempts after
recording a failed process that never opened its listener or began business/funding/external work.
It records each actual exit and both log hashes, and repeats nothing while a transport fault is
armed or a defect build is selected. Engine startup and negative startup tests receive no such
retry. These are fixture deployment attempts, not runtime recovery or a production availability
claim; failed or unactivated fault scenarios remain failures or inconclusive observations.

Physical database fixture setup has explicit 180 s command bounds for initdb, offline replication-role
creation, pg_basebackup and pg_verifybackup; server readiness is bounded at 60 s. Backup verification,
fsync and synchronous commit remain enabled. Fixture shutdown uses pg_ctl wait 60 s inside a 75 s
command bound. Recorded setup fsync exceeded the old 20 s readiness bound, a checkpoint shutdown took
21.907 s, and a later base backup exceeded the old 45 s command bound. These are fixture I/O allowances,
not changes to the 250 ms application checkout, 1000 ms stop record or deferred pg_receivewal path.
The cause of occasional host setup delays is not established. Failed earlier runs remain failures.

The [W4 report](../evidence/w4/report.md) and source inventory record the committed runtime/fixtures
used by the final nine-phase run. Later documentation/evidence changes preserve those hashes.

For ordinary authenticated, keyed POST requests to deposits/machines that expect success, the fixture
client makes at most three HTTP attempts with identical bytes and key after a retryable checkout
overload. It follows both body and HTTP pacing hints, waiting at most one second per resend, and records
every such response with correlation and key/body digests. Expected errors, reads, enrolment, armed
transport faults and defect controls receive the first response directly. Engine startup and business
errors gain no retry. This is test-client protocol handling, not a runtime timeout or capacity policy.
Core fixture setup similarly makes at most three API admission requests with the same key and body,
only after a whole-checkout refusal, mirroring its funding helper. Target fault probes and engine
startups are direct.

The [refusal and pacing supplement](../evidence/refusal-pacing/report.md) records separate unchanged-runtime
reds and fixed-runtime greens for definitive provider/400 rejection and mixed-source caller pacing.
The three focused workflow selectors are `w4_provider_refusal`, `w4_provider_ambiguity` and
`w4_mixed_pacing`; all also run in the complete workflow. Definitive refusal releases the original
commitment/exclusion without a debit, while dispatched provider 5xx/no-status and known acceptance
retain unresolved reservations. This supplement runs the complete workflow, focused default-feature
checks and six affected controls; it makes no new nine-phase or production-conformance claim.


## W6 reconciliation and termination subset

The [W6 report](../evidence/w6/report.md) and [final aggregate](../evidence/w6/final-all.json)
record 22 W6 groups, all prior phases, 39 recognized controls and two unchanged-base regressions.
Automatic create resolution uses exact correlators on every channel, direct identity/account
confirmation, an effective visibility maximum and the independently measured simulator negative
window. Original terms, safe IDs, errors and pending obligations survive payload purge and restart.
A money-only negative-window release leaves an unresolved offer exclusion intact. A late attach
commits fee/unit deficiencies, unpaid seed, wind-down floor-or-deficiency and cleanup together.
Human observed attachment performs no provider read and records a null cancellation date.

Caller deletion and all W6-reachable queued cleanup run the same fenced worker. OPS-41's ordered
current funding decision shares the tenant transaction with its episode fence. A mutation reply
reporting gone is authoritative immediately; acceptance-only and dropped replies retain funding
until non-mutating evidence or an authenticated decision resolves them. Stalled episodes have no
timer retry. Operator retry creates a fresh attempt under the same episode; keep closes/clears
without money movement. Closed episodes remain closed under later actions and observations.

Every reached gone writer closes the elapsed exit tail before releasing funding, with period and
durable fixture rate-acceptance splits, reduced rational credit, clamped paired debit/decrement,
native deficiencies and no-entry mark updates. Paid-unit exits discard elapsed tails inside the
prepaid mark. Schema-3's offline audit checks credit range and one-sided key-derived high-water
before serving; quarantined deletion preserves meter evidence and closes only an existing outage.
This is an exit-only subset, not general periodic billing or a real rate estimator.

Optional environment-provisioned static operator authority runs on a separate loopback listener,
without customer CORS. Only supported per-kind resolutions, operation list/detail, episode list/detail,
retry and keep are exposed. Principal-scoped receipts retain the authenticated decision. Production
authority distribution and credential management remain outside the prototype.

Open recovery incidents still refuse startup: continuation, restore grace and exception suppression
belong to W7. General exhaustion and suspension fan-out, extension/survival by extension, scheduled
cancellation, surviving attachments, other mutations and import/rescue remain `not run`, owned by
deferred follow-on work. No-rate OPS-41 steps 4–5 remain `not run`; reaching them stops safely before
fence/provider dispatch. Production D-6 policy and saturation/recovery are W8; W9–W11 and any real
pilot remain unauthorized. Known P2/P3 and preexisting limits are retained in the W6 report.

Demonstrating no duplicate purchase for the supported traces is not a proof of exactly-once execution by an arbitrary provider. External uncertainty remains part of the product model. [S5] [S10]
