# W6 recorded implementation

W6 implements automatic create reconciliation and one additional mutation: local immediate-destroy deletion with no surviving billable attachments. Deletion serves termination of the future pilot's cost: power-off continues billing. This remains an internal pre-conformance simulator prototype.

The final operational commit, complete source inventory, exact dependency/tool versions, seed and configuration are in [validation.json](validation.json) and [source-inventory.json](source-inventory.json). The [aggregate](final-all.json) requires all eleven phases. Every final phase records cleanup `pass`. All commands ran as foreground descendants of the isolated test unit; complete stdout and stderr were captured separately and their true statuses saved before inspection. Private logs, databases, builds, credentials and superseded attempts are excluded from this bundle.

The canonical fast gate and all-phase PostgreSQL command exited 0. Nine Python harness unit tests also passed with exit 0. The canonical command is `nix develop --command bash tools/postgres.sh --seed 20261112 --evidence <private-evidence>/all.json`; the gate is `nix develop --command bash tools/check.sh`. See validation for full command/status and output-hash proof. Later documentation/evidence publication preserves every tested operational hash.

## Activated scenarios and established facts

Every row below is `pass` in [final-w6.json](final-w6.json). It inherits the manifest's pinned specification, tested commit/source hashes, versions, effective configuration, clock basis and cleanup result. Its `w6_*` events retain the operation-boundary trace, relevant complete database snapshot and independently durable provider evidence. A received request, an observed acknowledgment, durable external acceptance/effect and visible database state are distinct observations. Harness deadlines are not product outage guarantees. The CNF references below name selected arms, not complete checklist items.

| Scenario | Activated boundary and established fact | Requirement/test mapping |
|---|---|---|
| `w6_measurement` | Twenty ordinary real-executable simulator purchases; independent fsync'd acceptance-to-allocation intervals establish the local allocation basis. The 60-second negative window is ceiling(worst allocation) plus a disclosed 59-second conservative margin. Engine conservative license-to-observation samples remain separate, with the immutable one-second visibility declaration still marked unmeasured. | OPS-33; PRV-36; STO-53 |
| `w6_positive` | Accepted purchase, actual reply loss and real successor; exact search plus direct presence resolves observed atomically, retaining one independent acceptance/order and one machine. Same-key admission and later startup repeat no effects. | W6 1/2; ENG-05; CNF-82/83/90, selected 85/89 |
| `w6_empty` | Actual remote resource removed; authoritative empty before/after the disclosed negative window. No early release; later absence closes the original money reservation and settles its independent exclusion lifetime. Separate matched-but-gone arms evidence fee only, unit only and both; only corresponding operation-subjected native deficiencies are recorded. | W6 1/3; CNF-87/282 simulator empty response |
| `w6_delayed` | Direct absence inside visibility does not attach/resolve; later exact presence succeeds. A separate actually accepted order holds allocation in progress beyond the negative-window fixture age; its original reserve stays through a failed/incomplete reread until allocation completes. | OPS-27/33; PRV-36; selected CNF-292 timing arm |
| `w6_conflicting` | Wrong correlator, failed direct read and wrong-account direct evidence leave no attachment, debit or guessed success; original reserve/terms remain. | W6 1/7; CNF-84 |
| `w6_multiple` | One accepted purchase exposes two candidate IDs; neither is attached and both safe IDs remain visible for operator action. | OPS-38; CNF-84 selected ambiguity arm |
| `w6_startup` | Real interrupted null/set licensing rows, retained closed incident marker and ordinary successor. Running-row count/classification, marker/error preservation and distinct reserve/exclusion outcomes are checked before another purchase. The null exception records zero purchases. | W6 4/5; ENG-03; OPS-15/16; selected CNF-361 |
| `w6_operator` | Automatic evidence attempt precedes fixture human action. Operator operation enumeration uses status/origin/reason/terminal intersections and pagination, with principal/effective-tenant cursor refusals. Authenticated synchronous absent/abandoned/observed decisions retain principal/evidence; exact same-key replay repeats none and fresh-key second resolution is refused. Per-kind validation, opaque references, customer 404/no CORS and null operator constraint date are checked. | W6 7; OPS-26/31; API-23; STO-19; WIR-26/34/35; CNF-89 |
| `w6_delete` | Caller admission/acknowledgment and ownership-before-body parsing; one real delete reply records gone/stop and closes funding atomically. Same-key replay and already-gone attempt issue no extra delete. | W6 6; OPS-11/45; PRV-29/36; LDG-38/74 |
| `w6_delete_evidence` | Acceptance-only applied/not-applied and actually dropped delete reply retain funding/meter uncertainty, survive a successor, then resolve by non-mutating direct evidence with one deletion acceptance. | OPS-27/45; PRV-29/36; ENG-05 non-create extension |
| `w6_episodes` | Suspended accepted attach queues cleanup; deterministic rejection stalls with its episode fence. Fresh operator retry still dispatches after resume because that inherited fence exists. Keep closes/clears with identity/evidence and unchanged money. | OPS-39/41/42/48; API-64/68; selected CNF-271/272/288 |
| `w6_late_attach` | Incomplete evidence past the negative window releases money but preserves exclusion and refuses a competing API admission without a new operation; later exact match atomically attaches with unpaid seed, separate fee/unit deficiencies, floor-or-deficiency and cleanup. Covered, insufficient, no-rate and unreadable-account expiry arms are observed; unreadable expiry performs no provider read before later healthy evidence. No-rate decision stops before fence/provider call. | OPS-33/36; LDG-38/39/40/66; STO-58; selected CNF-308 |
| `w6_gone_reply` | Independent real simulator deletion plus stale direct resource source; the mutation's gone reply wins immediately. | PRV-36; baseline regression |
| `w6_exact` | Actual worker receives a non-exact settle-time candidate; no machine attaches, while the independent purchase remains one. | OPS-29; baseline regression; CNF-84 |
| `w6_account` | Present cache update, multiple local pages, unrecorded resources reported without attachment, incomplete/throttled enumeration and failed direct confirmation do not write gone; complete pass plus direct absence does. Eligible settled temporary key is removed. | OPS-32; PRV-9/38; selected CNF-88; LDG-74 |
| `w6_delete_classification` | Worker faults before/after acknowledged marker, and real restart with null/set markers. Persisted deciding marker controls survived failure; restart remains interrupted in both. No speculative delete; operator abandonment leaves running funding untouched. | W6 5/6; OPS-11/15/45; selected CNF-361/362 |
| `w6_resolution_reply` | Resolution COMMIT actually lands; its exact session's reply is lost. The fixture explicitly ages that operation beyond the current effective visibility maximum before arming the unchanged transaction-fault gate. Whole store-unit retry preserves the complete machine/money/seed/exclusion snapshot and one purchase. | ENG-06; STO-3/19/23; OPS-49 |
| `w6_exit` | Explicit two-hour fixture tail closes before funding release; remaining authority is debited/decremented, overflow is native operator deficiency, and mark/stop/close commit together. | LDG-31/35/38/66/74; STO-45 |
| `w6_channels` | Union over both channels precedes cardinality; an incomplete channel cannot prove absence. Same external identity across channels deduplicates; later late attach is one machine. | OPS-27/38; PRV-38; selected CNF-281 |
| `w6_episode_order` | Concurrent production-helper joins yield one episode and reason set; historical suspension reason and never-fenced operator origin do not bypass funding. Uncertain/not-applied stalls without timer retry; keep/retry has one winner. Uncertain abandonment/applied close permanently and later actions cannot overwrite the close. | OPS-39/41/42/48; STO-52; selected CNF-271/272/288/291/312 |
| `w6_meter_edges` | Exit crosses UTC month and durable fixture rate boundaries; one maximal native-only outage, exact reduced credit and period-attributed usage. Explicit one-sided high-water and range audits quarantine subjects. Quarantined deletion leaves meter/ledger unchanged while closing an already-open outage without opening another deficiency. | LDG-38/64/66/72/74; STO-37/45 |
| `w6_default_hooks` | Ordinary binaries built without hooks encounter all four new defect environments and new control files: one purchase, no non-exact attachment, original reserve retained and interrupted row classified. Delete control files and reconciliation evidence hold are inert. | Feature isolation; OPS-12/15/49 |

The earlier core phase additionally exercises ENG-04 stale-claim refusal, ENG-06 whole-settlement repeats, ENG-08 rejection of provider/rail/rescue entry while a component owns a transaction, and the existing generation/stop guards. W5 retains all interruption assertions: explicit provider/evidence barriers isolate its historical unresolved/snapshot boundaries from W6 periodic updates. The original failed-checkout assertion, observer faults and narrow timing bounds remain intact. Retention ignores only explicitly tagged background evidence-read events while retaining every purchase/effect and the whole independent resource state.

## Required negative controls and baseline regressions

All 39 isolated controls are recognized as `expected negative demonstration` in [final-controls.json](final-controls.json), each with actual child exit 1 and cleanup `pass`. The four new controls require their exact fault and assertion, not arbitrary setup failure:

| Defect | Observed fault | Exact failed assertion |
|---|---|---|
| `w6_blind_repeat` | Two independently durable accepted purchases after actual lost reply | `one purchase/no repeated order` |
| `w6_non_exact` | Wrong returned correlator actually attaches in defect build | `exact-correlator/no-attachment` |
| `w6_release_timeout` | Original reservation closed at local timeout before negative window | `original-reservation-preserved before negative window` |
| `w6_skip_startup` | Closed-marker ordinary startup leaves running row and reports zero classified | `interrupted-running-row classification/count` |

The unchanged review base is `a56660d142371d208b05e3f280eac1d2344a9bcf`. The final fixture drivers, with simulator-only overlays, also reproduce two actual old-code faults: [gone reply](red-baseline-gone.json) fails `PRV-36 gone reply must override stale resource read`; [non-exact candidate](red-baseline-exact.json) fails `OPS-29 settle-time non-exact candidate must not attach`. Both child exits are 1 at the intended assertion after activation; cleanup passes, and all 32 protected runtime/migration/dependency hashes match the base before and after. Their identical final drivers pass in the W6 inventory. These real regressions are separate from deliberate sensitivity defects; newly implemented features are not portrayed as baseline bugs.

Two W6 lifetime arms were completed after the earlier inventory passed. Their ordinary regression
fixtures first fail on the unchanged W6 runtime `8f0253d`, without any deliberate defect build:
[unreadable account](red-w6-unsearchable.json) fails `OPS-33 unavailable account must release expired original reservation`,
and [failed reread](red-w6-in-progress.json) fails `OPS-33 failed reread must preserve known in-progress reservation`.
Each observes the named boundary before its intended assertion, exits 1, preserves all protected
runtime/migration/dependency hashes and cleans up. These are gaps in the initial W6 implementation,
not claims about features the pre-reconciliation review base lacked. Their final greens are in
`w6_late_attach` and `w6_delayed`.

OPS-33 owns both decisions. The first reproducer is an accepted/lost create, known unreadable
account and expired fixture age: the old healthy-account filter froze its money. The impact was
an indefinite reservation. The correction runs local expiry bookkeeping without provider reads,
retaining the unresolved exclusion. The second is a known in-progress order with allocation still
held past the negative window, followed by an incomplete reread: the old pass discarded the
positive knowledge and released authorization. The correction retains that knowledge until complete
delivery evidence or an explicit resolution. Neither correction changes specification policy.

## Source and publication audit

The operational commits and changed paths are listed in the [source change report](source-changes.md). AGENTS.md and spec-lock.json are unchanged. No prior evidence is rewritten; no new branches/tags, history rewrites, push, pull request or merge was performed. All old components must stop and be prevented from restarting before the offline schema-3 migration; mixed releases and rollback past this release are not supported. Every test owns its recorded ports/directories/credentials; cleanup is restricted to recorded invocation resources. Externally retained raw proof is private.

The publication audit recursively removes host paths, local coordination identifiers, IPs, credential/payment identifiers and test sentinel secrets. Stable opaque replacements preserve equality. Activation, operation/resource IDs, true statuses, source hashes, relevant complete state and independent effects remain assessable. High-frequency protocol/observer transport records are omitted from compact manifests; complete private raw trace hashes remain. Source inventory distinguishes later documentation from tested operational files.

## Explicit limits and follow-ups

- W7, `not run`: open-incident continuation of running-row recovery, null-create exception suppression throughout an incident, restore grace/deferral, restore/promotion/standby replacement and incident procedures. Startup still refuses an open incident. Closed-marker ordinary startup proves none of these.
- Deferred follow-on owner, `not run`: general exhaustion and suspension fan-out enqueuers (refused), extend-runway and late-attach survival by extension, no-rate OPS-41 steps 4–5 (reached branch stops safely without fence/call), scheduled cancellation, other mutation kinds, billable surviving attachments, import/rescue and real providers. Immediate destruction explicitly declares no survivors.
- W8, `not run`: production D-6 pool/capacity/timeout/reconnect/restart/backoff/supervisor policy, deployment saturation or observer-access guarantees and offline deployment/recovery procedures. W9–W11 and any real-provider pilot remain unauthorized.
- Deferred follow-on owner: general periodic billing, rate construction/solvency, full cross-kind conformance, rail-history response-size bounds, descriptor-at-startup deadlines and unrelated harness hardening. Fixture rate acceptance records support the reached exits; they are not a market feed or an implementation of the general rate estimator.
- Unresolved outcomes remain external uncertainty: non-authoritative/multiple/conflicting/failed evidence never invents success. The original reserve has its bounded OPS-33 lifetime; an unresolved resource-offer exclusion has its own outcome lifetime. Matched in-progress orders have no negative timer. Operator decisions retain evidence and identity.
- Prior P2 `review-followup`, retained: missing attempt backend can evict a healthy observer; conservative accounting but unnecessary reconnect/lost attribution. Observer error/reconnect churn remains 20 ms without backoff, including no active attempts. W5 raw external-effects label still omits W5. Early control-helper failure can omit the parent manifest but still fails closed.
- Prior P3 `review-nit`, retained: stale elapsed-precommit comment in engine and historical W5 publication sentence. Earlier evidence is unchanged.
- Prior P2 suspended-attach unsupported-delete finding is resolved for W6's reachable queue: the same worker executes it, with the fenced retry/keep lifecycle. General suspension fan-out remains refused and untested.
- Prior `review-preexisting`, retained: malformed rail settlements can end watcher/API; transient listener accept failure can end API; a tenant activated without a healthy account is never assigned later. Their specification assessment remains deferred.
- Prior P3, retained: rail watcher scans all retained history; request headers are read one byte at a time; rate allowance consumption always returns true.
- Prior timing limits, retained: observer termination can race sampler idle; historical external snapshots can precede the simulator's final record; the backoff-budget probe has a 7 ms scheduling window. No fault is retried away or bound widened. New W6 cases use attributed completion barriers and fail closed if their named fault never occurs.
- D-37/D-38 remain unchanged: missing observer samples are charged, completion-only accounting cannot force an exit while a live transport withholds completion, and the PostgreSQL one-contiguous-SyncRep-span premise remains unverified. Review judgment does not replace execution evidence.

Demonstrating no duplicate purchase for the supported traces is not a proof of exactly-once execution by an arbitrary provider. External uncertainty remains part of the product model. [S5] [S10]
