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
| Production work not yet attempted | Real-provider drivers, rescue installation, every operation kind beyond the first slice, a customer UI, security review, key custody, billing operations, separate-host deployment rehearsal, production operator runbooks, exposure limits. | not attempted |

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
legacy recovery incidents stop the unsupported startup path. Established incidents continue through
the phased recovery described below. Parent fan-out, install/rescue workflows and full
metering/rates/solvency remain unimplemented. W6 implements create/delete
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
reconciliation; DB-10 deployment-wide saturation is now evidenced narrowly by [W8](../evidence/w8/report.md). Local pool
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
nonexecution inferred. D-70 supersedes the earlier background-checkout exit behavior: pretransaction
housekeeping checkout/probe failures skip the tick and try again after one second. Errors after
transaction entry still end the service under the existing ownership policy; API components do not
run the engine's whole-transaction retry loop. Recovery from such an exit uses the supervisor/operator
and ordinary keyed request or adapter replay after the underlying problem is addressed. This is
bounded failure handling, not an availability guarantee.
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
credentials authorize only spending-token replacement, including pending/suspended maintenance.
Other recovery/maintenance routes remain unsupported. W6 adds an isolated operator subset, described below. Traffic
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
not select production capacities or close D-6/8/9; D-7 settles only the local mechanism below.

The canonical runner requires twelve phases and retains the full W1–W3 inventory plus the workflow,
default-feature workflow, W5, W6, W7 and forty-four sensitivity controls, plus the direct release-assertion
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
For ordinary API setup, the fixture may make at most three separate deployment attempts after
recording a failed process that never opened its listener or began business/funding/external work.
An empty read-only rail-history replay is permitted by shape; any credit or other business/external
work excludes the retry. Engine deployment may retry only a cold whole-checkout refusal before
session admission, including while awaiting the admission standby check. Each failed start, exit
and both log hashes are recorded before port reservation or another attempt. Fault-armed, defect
and negative startup cases remain single invocations. D-68 amends D-20's historical engine rule;
the 250 ms product checkout bound is unchanged. These are fixture deployment attempts, not runtime recovery or a production availability
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

For ordinary POST enrolment and authenticated, keyed POST requests to deposits/machines that expect
success, `Workflow.request` makes at most three HTTP attempts after a retryable checkout overload.
It freezes exact request bytes before the first send, retains any key (ignored by enrolment), honours
both body and HTTP pacing hints and waits at most one second per resend; longer hints stop the
fixture rather than shorten the wait. Each resend records correlation and key/body digests. Expected
errors, reads, armed transport faults and defect controls receive the first response directly.
Business errors gain no retry. D-71 supersedes the historical exclusion of enrolment; the seven
[client checks](../evidence/kin/regressions.json) cover its finite policy. This is test-client protocol
handling, not a runtime timeout or production capacity policy.
Core fixture setup similarly makes at most three API admission requests with the same key and body,
only after a whole-checkout refusal, mirroring its funding helper. Target fault probes are direct; the narrowly shaped deployment retry above is separate.

The [refusal and pacing supplement](../evidence/refusal-pacing/report.md) records separate unchanged-runtime
reds and fixed-runtime greens for definitive provider/400 rejection and mixed-source caller pacing.
The three focused workflow selectors are `w4_provider_refusal`, `w4_provider_ambiguity` and
`w4_mixed_pacing`; all also run in the complete workflow. Definitive refusal releases the original
commitment/exclusion without a debit, while dispatched provider 5xx/no-status and known acceptance
retain unresolved reservations. This supplement runs the complete workflow, focused default-feature
checks and six affected controls; it makes no new nine-phase or production-conformance claim.


## W6 reconciliation and termination subset

The [W6 report](../evidence/w6/report.md) and [final aggregate](../evidence/w6/final-all.json)
record 52 W6 groups, all prior phases and 39 recognized controls. Two review-base regressions,
two early lifetime regressions, nine regressions against `9454e45` and nine against `5061fe3`
retain their actual distinct source identities.
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

W7 supplies continuation, restore grace and exception suppression for established incidents.
Unsupported legacy incidents still refuse startup. General exhaustion and suspension fan-out, extension/survival by extension, scheduled
cancellation, surviving attachments, other mutations and import/rescue remain `not run`, owned by
deferred follow-on work. No-rate OPS-41 steps 4–5 remain `not run`; reaching them stops safely before
fence/provider dispatch. Production D-6 policy remains open; local saturation/recovery implementation is W8; W9–W11 and any real
pilot remain unauthorized. Known P2/P3 and preexisting limits are retained in the W6 report.

Demonstrating no duplicate purchase for the supported traces is not a proof of exactly-once execution by an arbitrary provider. External uncertainty remains part of the product model. [S5] [S10]

The nine continuation groups cover stable startup timing, money release before a larger effective
visibility window, atomic uncertain-episode startup, durable principal resolution ceilings,
terminal authentication handling at every reached reconciliation read, operator retention,
legacy cleanup evidence upgrade, monitorable operator decision outcomes and repeated caller-delete
validation. Their recorded decision traces and protected-source reds are in the W6 report. These are selected
simulator arms. Nine further groups cover failed confirmation after a single exact match, both
older cleanup outcomes, receiptless cleanup/retry retirement, ordinary verification credential
stops and early authenticated refusals. The report retains twenty-four deferred P2/P3 defects.

A retained authoritative single exact match awaiting confirmation is positive evidence, so its
original commitment survives failed/incomplete rereads and ordinary restart. Later complete evidence
uses the existing resolution rules; genuine negative/non-authoritative/duplicate expiry remains
independent of visibility and account health. The offline schema-3 release is first released with
its final migration content and supports the scoped schema-2 upgrade. Discarded intermediate W6
test schemas are not a supported production upgrade line.

Receiptless settled attempts use the same configured age fallback for logical expiry and actual
housekeeping retirement. Retirement atomically retains minimal identity, tenant, actual operation
kind, nullable machine identity, creation time and the actual horizon, while the episode outlives the
deleted operation. Keyed operations still use their
maximum stored receipt horizon; unresolved operations remain readable. No request body or synthetic
receipt is retained to make a retired attempt discoverable.


The final retirement repair carries each grouped all-scope receipt maximum into the same atomic
operation removal and permanent minimal identity insert. Canonical `w6_e1` activates 990 due rows
amid 20,000 added operations and 200,000 added receipts. Supplemental exact scale/read selectors
retain their existing no-work, mixed, protected-alias, unsettled, receiptless and financial-history
checks; they do not substitute for canonical activation. Eventual retired-history lookup scaling
is deferred. Preflight offers, temporary-key registration, ordering, pre-acceptance key cleanup
and deletion share the durable credential stop, with one external permit per entered call and no
provider I/O in a store transaction. Other accounts and local negative expiry remain independent.
Every reached non-clamp deficiency now carries null `clamped_sats`, while actual clamp overflow
keeps its exact satoshi remainder and native partition. Schema 3 remains the unpublished offline
forward release; earlier migration meanings and deferred billing work are unchanged; W7 adds the offline schema-4 incident release.


The reached exit clamp producer separates LDG-66 opening evidence from LDG-38 historical
usage pricing. Both an unavailable opening rate and a different current rate are demonstrated,
with historical debit/native arithmetic, exact credit/mark, lost settlement receipt, whole-store
repeat, ordinary successor and caller replay preserved. Null opening fields are never backfilled.
Operator resolution replays survive actual housekeeping retirement, retaining fingerprint-first
conflicts, the all-scope maximum horizon, effective-tenant privacy and structured refusal events.
The retired identity supplies the actual kind and machine for strict validation and truthful
audit events independently of any prior operator receipt. The two additional real-service arms
cover normally completed customer creates and deletes with only tenant receipts: cross-target
key mismatches stay 409 after actual retirement, wrong-kind bodies stay invalid_request, and
a fresh key cannot mutate a retired identity. General rate construction and OPS-41 no-rate
decision steps remain outside this reached caller-delete path.


## Recovery incident subset

The [runbook](recovery.md) and [report](../evidence/w7/report.md) describe the local
STO-54/56 implementation. Schema/semantics 4 is an offline contract release, preserving schema-3
money/retirement rules and conservatively backfilling nullable install/rescue provenance. It adds
no admission or execution for install, rescue or suspension parents; their tests seed historical
metadata. Unsupported prerequisite history remains unavailable.

The local fencing mechanism stops and joins the complete owned component inventory, stops the old
PostgreSQL process, quarantines its data/launch identity and refuses ordinary relaunch. Existing
sessions, new connections and the retired launch identity are independently probed. The replacement
is a verified physical backup of the new authority, authenticated as the sole designated peer and
sampled through flush catch-up. This assumes exclusive control of the local launch/data paths; it
is no proof of production fencing, separate failure domains or hardware durability. The backup
stream is distinct and uses no maintenance commit-mode exception.

Every new promotion/history replacement establishes a fresh incident before components start.
Same-event retries preserve instant, predecessor, evidence and all progress. A successor resumes
unmarked phases and still runs the ordinary interrupted-running pass. New events atomically
supersede retained predecessors. The engine's first write remains its separate UUID advance;
pre-lock preparation belongs to a separate, supervised recovery actor.

Step 3 reconstructs the unexpired watch query and independently replays the two simulated rails
through retained bindings, including expired deposits with known settlements. It reseeds surviving
admission counters, invalidates spending generations and advances deposit allocation. Its mark and
original grace end commit together. Payment replay does not reconstruct lost commitments or
extensions. Known correlators can reconcile without repurchase; missing correlators and unknown
external effects remain operator-owned. Unrecorded machines are reported per account.

The wall-clock freeze and incident lifetime are separate. System exposure-reducing attempts read
the live claim gate before funding or suspension/retry exemptions; caller deletion remains usable.
Only unfunded rate-present system decisions reach paused grace. Fixture rate inputs atomically
retain stamps used by current reads, exit spans and recovery grace; no second independent history
is fed by recovery tests. This is not a production median/quorum estimator. Close reads current
machine disposition, every waiting parent, the second account pass and each currency's remaining
grace. Settled parents can retire while their incident disposition and operation identity survive.

Both possible-loss endpoints remain unknown without certified clock and matching-prefix evidence.
Absent/malformed reporting files impose no startup hold; continuation reuses retained evidence.
Certificate ingestion is not implemented, and certified-known lower/upper/lagging-standby variants
are `not run`, owned by the deployment operator. The finite fixture gap of 10000 covers only the
removed allocations actually observed in its backup/restore trace; it is not an unbounded non-reuse
proof. Re-derivation interval 3 seconds is a fixture value, not production policy.

The report binds the final serial run on `f302686` to 96 committed operational files: fast gate,
Python units and all twelve canonical phases exited 0. It preserves the earlier failures and the
known timing class's one recorded serial rerun. Each iteration uses `tools/iteration.sh` (quick
checks, units, W7 and controls); before publication `tools/postgres.sh --jobs N` runs the full suite
with 2–3 workers or serially. These fixtures select no production availability guarantee.
D-70 implements bounded engine/API retry handling. D-71 and the [pvr-kin report](../evidence/kin/report.md)
record paced enrolment recovery and the post-admission CONFIGURATION lock-timeout recovery arm;
D-72 and the [startup report](../evidence/startup/report.md) separately demonstrate the startup
acquisition-failure arm in an empty local database, without a provider-repeat proof.
The W7-era statement that the fixture customer client does not retry enrolment's retryable 503 is
historical and superseded by D-71's three-attempt ordinary fixture policy. The broader
[W8 live journey](../evidence/w8/live.json) used a separate four-attempt client. The current targeted
pvr-kin script sends one busy request and one paced resend, then a separate used-code check; it has
no automatic HTTP retry loop.
CNF-295's different-person and before-first-customer-payment obligations remain operator-owned
`not run`. General fan-out, extensions, scheduled
cancellation, OPS-41 no-rate steps 4–5, production alarms/fencing and W9–W11 remain deferred; W8 completed validation and its separate focused W5 result are stated below.

Recovery CLI actions return one result row on stdout; diagnostics are quiet by default and can be
appended through `PROVISIOND_RECOVERY_LOG`. Reporting notices/errors remain on stderr; the
engine-supervised preparation child sends traces to the engine log without a result row. Absent
reporting input retains unknown endpoints and is distinct from malformed input (D-66).


## W8 local evidence and completed validation

The final canonical invocation failed (exit 1); the separate unchanged-source W5 follow-up passed
(exit 0). Neither result replaces the other. See
[report](../evidence/w8/report.md), [validation](../evidence/w8/validation.json) and
[source inventory](../evidence/w8/source-inventory.json).

Schema/semantics 5 is an offline contract release. `Store::migrate` checks NOLOGIN and zero
application backends before DDL; every runtime entry checks schema, semantics and declarations.
The old schema-4 binary is excluded. Migration history conservatively backfills declarations for
1–4; schema 5 tightens attachment ownership with rollback floor 5. STO-12/13 and CNF-57 own these
guards; no migration uses CONCURRENTLY, so the invalid-index arm is inapplicable and not run.
The numbered [release](release.md) and [saturation recovery](recovery.md#saturation-recovery-on-unchanged-history)
procedures retain the one-primary/one-designated-standby manual profile.

Runtime read/write checkout failure streaks alarm at 2000 ms; fatal service exits alarm immediately.
Post-admission startup and configuration implement bounded Store retry. At W8 publication their
targeted failure/recovery and paced enrolment verification were not run; the later D-71 evidence
below demonstrates enrolment recovery and only the CONFIGURATION arm. D-72 separately records
[startup acquisition recovery](../evidence/startup/live.json) for the engine-only local arm.
Request checkout refusal remains retryable without service exit;
pretransaction housekeeping failures skip a tick. Safe provider IDs
accumulate independently of latest verdicts, and conflicting external identities are refused before
fee/unit debits. Generation fencing and money flags are unchanged.

The recorded local simulator journey i7 exited 0 in 170.318 seconds on runtime aa1ad12 and driver
014edf9. It demonstrates the exercised offline release and unchanged-history saturation arms,
including ordinary observer refusal, bounded supervisor backoff, alarms and standby restoration.
The waiting engine XID 6164 equals the succeeded outcome xmin 6164. Typed 503 overload begins no
business transaction and writes no refusal receipt across observed API generations. Exact resend,
one purchase/charge and balance checks were exercised. Primary shutdown fallback was not run.
Reserved administrator fixture assertions are separate from out-of-band recovery controls.

D17/D18 were not reachable: functional results are not run. Historical runtime RED and recorded
GREEN evidence for identifier retention and conflicting attachment are in
[regressions](../evidence/w8/regressions.json). Final canonical W6 demonstrates safe identifier
retention and both tenant scopes of attachment conflict before debit. Observer sent-COMMIT retention
is demonstrated in the separate focused W5 invocation; canonical W5 did not reach that case.

Tests are restored. The iteration exited 0 in 855.070254 seconds at 18574b3 plus the reviewed patch,
verified byte-identical to committed b1b0c721. It includes the fast gate (exit 0), 50 Python tests
(exit 0), all 21 W7 selectors and 44 controls plus five candidate negative demonstrations. The
[final canonical](../evidence/w8/canonical-final.json) started clean b1b0c721 and exited 1 in
1844.113252 seconds: all 12 phase processes ran; 11 passed, W5 failed on the F27 pre-admission
fixture allowlist. Its 259 scenario rows are 251 pass, one fail, two expected negative
demonstrations and five not run. W4 workflow has 84 pass rows, W6 has 53 selectors / 54 pass plus
one not-run row, and W7 has 21 pass rows. The single separate unchanged-source W5 follow-up exited
0 in 137.348692 seconds with all 27 selectors / 30 pass rows. The complete phase collection
(eleven canonical phases plus focused W5) has 280 rows: 273 pass, two expected negative
demonstrations and five not run. This is a collection of separate invocations. All 11 ordinary
canonical cleanups, 49 control-child cleanups and focused W5 cleanup passed. The first canonical
failure (exit 1 / 1678.641406 seconds on 18574b3) and earlier failures remain recorded.

The 104-file tested operational map remains equal after documentation, evidence and tracker writes.
Runtime bytes equal aa1ad12. The [source inventory](../evidence/w8/source-inventory.json) records
the tested commit and a separate equality observation; no circular publication hash is embedded.

The local D-6 profile is measured only within these prototype runs. Four total starts, bounded
backoff, 16 GiB per unit, no swap and a 5-second stop allowance are local settings. Production
capacity, alert routing, separate-host rehearsal and availability guarantees remain open.
pvr-31s remains open P1: only the failure_run idle-observer site is repaired. Preexisting
pvr-2g0 and pvr-oqf remain unfixed. The W7 first-snapshot budget can fail closed after later
window widening (F24, pvr-5rl); F27 (pvr-58k) rejects a clean pre-admission driver closure.
Both are fail-closed fixture limits. Six final follow-ups F21–F24/F26/F27 are filed;
[report](../evidence/w8/report.md) lists their IDs. No deterministic-suite claim is made. W6 proves a stable window only; its
batch-snapshot runtime implication remains preexisting and unmeasured. W9–W11 remain deferred.


## pvr-kin local availability observations

The [baseline diagnostic](../evidence/kin/r1.json) confirmed a P1 under STO-55/DOM-17: c5045df
returned retryable 503 with a 250 ms body hint and 1 s header, then refused the identical unexpired
enrolment request with 400 after 1.081186978 seconds in the same process; a fresh code returned 200.
D-71 releases only the atomic admission reservation belonging to a typed pre-BEGIN checkout refusal.
Successful use, validation precedence and failures from BEGIN onward retain single-use semantics;
there is no durable enrolment replay or server retry loop.

The [default release live run](../evidence/kin/live.json) on 5deadfa exited 0 / 10.065245687 seconds:
503, identical-body 200 after 1.077288216 seconds, then used-code 400, one account, no refusal receipt,
same API process. After generation advance and restart-pass, an actual configuration lock timeout
55P03 rolled back, spent 500 ms in one whole-transaction retry, acknowledged configuration COMMIT
and completed 71 ordinary claim polls during one second, same engine PID/invocation, no restart
and no repeated external call. That run demonstrates only the post-admission CONFIGURATION arm;
D-72's later [startup run](../evidence/startup/live.json) separately demonstrates startup acquisition
recovery in an empty local database. The earlier accept-i2-a setup failure
remains inconclusive (exit 1 / 0.241099419 seconds), both arms not run.

The separate startup run used default release master dc772365e46f8160caaccdff691cc31d28383c75,
driver ba722c9731abc25a519b823fb930260d45aaecae: first attempt pass, driver exit 0 /
8.655418857 seconds including verified cleanup, launcher exit 0. A planned short loss of new
connections produced actual native PostgreSQL 53300 refusals after generation COMMIT and before
the restart pass; the cold ordinary write pool retried with spent values 2/16 ms. Early connection
application names were unknown; only this engine used the app role in the fresh owned database.
The exact role limit -1 returned after 47.515952 ms restriction, 16.109659 ms after first observation.
The same PID/start time/invocation with zero restarts completed one restart pass, configuration
COMMIT and 53 ordinary polls spanning 1.025000095 seconds. Provider/rail requests and state and
business rows stayed empty. The [report](../evidence/startup/report.md) distinguishes native
server timestamps from driver receipt times and binds retained private artifacts. This is only
a local policy observation of D-70's engine startup recovery, recorded in D-72. It reuses the existing
OPS-49 retry bound without demonstrating its worker/periodic store-call retry mandate. No
repeat-provider proof, production tuning, general availability or
whole OPS-47/49/21 demonstration. Primary shutdown fallback, sent-COMMIT ownership, generation
fencing and all existing bounds remain unchanged.

The [required validation](../evidence/startup/validation.json) ran against clean master dc772365, whose runtime, test, tool, migration and deployment bytes match this publication. The first iteration passed, exit 0 / 546.696320233 seconds, at port base 25500 with no rerun. The first serial canonical invocation failed, exit 1 / 2044.788749327 seconds: all twelve phase processes completed, ten passed; core and controls failed. Core stopped before its planned generation-reply check. Its engine_start probe met a pre-lock checkout deadline; the later planned reply event never occurred. Core also recorded a failed cleanup verdict for the unexpected exit 71. The intended check and later core arms are not run, never pass; P2 pvr-wlm tracks the fixture investigation. Controls rejected w6_non_exact at a different assertion from its required no-attachment assertion, with passing child cleanup; P2 pvr-aqm tracks that investigation. W5 and W6 passed in this invocation. The first failed result is retained. No product correction or pvr-31s change is claimed.

On 2026-11-25, the owner requested one additional full canonical invocation on clean master dc772365e46f8160caaccdff691cc31d28383c75. All 110 operational files matched the first invocation. The command, serial jobs setting, seed 20261124 and port base 25500 were unchanged. The additional invocation exited 1 in 2038.233505776 seconds: all twelve phases completed, eleven passed, and controls failed. Core passed, including the planned generation-advance reply check; probe 158 acquired the startup lock and reported the expected unacknowledged-generation result after the planned reply loss. Core cleanup passed. The earlier pre-lock checkout deadline did not repeat; pvr-wlm remains open, with no cause or correction established. The w6_non_exact control repeated the unlabelled pending() status assertion at w6.py:31, called from conflicting() at line 153. Its no-attachment assertion at line 152 was reached and passed; it did not produce the intended negative failure. Child cleanup passed. The other 43 controls and five candidate regressions remained recognized expected negative demonstrations; release-helper sensitivity results were unchanged. No other scenario verdict changed. The first failed invocation, exit 1 in 2044.788749327 seconds with ten passing phases, remains retained. No product code, check, production bound or configuration was changed, and pvr-31s was not continued.

Immediately before launch at 2026-11-25T00:14:31.760783Z, load averages (1/5/15 minutes) were 0.70/0.82/0.71, MemAvailable was 49074276 kB, and free space on / was 163757268992 bytes (152.511 GiB). Immediately after exit at 2026-11-25T00:48:30.229962Z, the readings were 4.80/2.55/2.72, 48550768 kB and 149339873280 bytes (139.084 GiB). These are endpoint observations on a shared machine, not evidence of a cause for either failure.

[Regression evidence](../evidence/kin/regressions.json) retains baseline red exit 1 / 51.024309556
seconds at the intended retry-should-return-200 assertion (actual 400), fixed green exit 0 /
60.319961566 seconds (503→200→400), and seven client checks exit 0 / 1.516944277 seconds.
[Validation](../evidence/kin/validation.json) records clean tested 70d86db: first iteration fail
(exit 1 / 554.955759231 seconds), exactly one timing-class rerun pass (exit 0 / 509.967890345
seconds), then the single serial canonical pass (exit 0 / 1831.510351548 seconds), all twelve phase
processes passing. Its 281 non-control rows are 274 pass, two expected negative demonstrations and
five not run; workflow has 85 pass rows including `W4-enrolment-pre-BEGIN-busy-retry` once. All 44
controls and five candidate regressions retain expected negative classifications. The first
iteration's 25900 port selection exposed a preexisting unit assumption; the controlled comparison
establishes only that port-unit cause, not the independent W7 checkout timing cause or a cure for it.
The rerun used documented 25500. Historical W8 failed canonicals and separate focused W5 pass above
remain unchanged. Source/runtime/test-byte bindings, cleanup and deferred follow-ups are in the
report. These observations select no production capacity/outage guarantee, automatic failover,
history change, real dispatch, purchase, funds or new accepted profile.
