# W6 recorded implementation and blocking repairs

W6 implements automatic create reconciliation and one additional mutation: local immediate-destroy deletion with no surviving billable attachments. Deletion serves termination of the future pilot's cost: power-off continues billing. This remains an internal pre-conformance simulator prototype.

The final operational commit, complete source inventory, exact dependency/tool versions, seed and configuration are in [validation.json](validation.json) and [source-inventory.json](source-inventory.json). The [aggregate](final-all.json) requires all eleven phases. Every final phase records cleanup `pass`. All commands ran as foreground descendants of the isolated test unit; complete stdout and stderr were captured separately and their true statuses saved before inspection. Private logs, databases, builds, credentials and superseded attempts are excluded from this bundle.

The canonical fast gate and all-phase PostgreSQL command exited 0. Nine Python harness unit tests also passed with exit 0. The canonical command is `nix develop --command bash tools/postgres.sh --seed 20261112 --evidence <private-evidence>/all.json`; the gate is `nix develop --command bash tools/check.sh`. See validation for full command/status and output-hash proof. Later documentation/evidence publication preserves every tested operational hash.

Recorded 2026-11-13: this final bundle covers 47 W6 groups: the original 22, nine earlier repair groups, nine continuation groups and seven final repair groups. All 39 sensitivity controls remain. W7–W11 and the accepted scope choices remain outside this implementation.

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
| `w6_r1` | Real marked-create interruption and two ordinary successors; authoritative/incomplete evidence actually alternates through the original 60-second window. The original 43 sats close without a purchase or successful resolution; the unresolved resource exclusion still rejects competing admission. The persisted timing origin is unchanged across every sample. | OPS-15/33; STO-58 |
| `w6_r2` | Healthy incomplete evidence with effective visibility at least 120 seconds: pre-expiry reserve stays, expired reserve closes after a completed pass without an early provider read. Known in-progress knowledge survives an unhealthy-account pass; later exact evidence attaches once and settles exclusion. | OPS-27/33/36; PRV-36; STO-58 |
| `w6_r3` | Actual episode-backed cleanup interruption before and after marker/fence; ordinary startup atomically makes the current open episode uncertain, preserving its actual null/set fence. A closed episode remains closed under a later interruption. | OPS-15/48; STO-52 |
| `w6_r4` | Authenticated resolutions across target tenants exhaust a two-slot fixture allowance; concurrent requests yield one admission and one ceiling refusal. Refusal preserves complete target state and writes no receipt. Replay spends no slot, changed fingerprints conflict, another principal has its own budget, and a real API successor retains exhaustion. | API-7/10; SEC-39; STO-35 |
| `w6_r5` | Attributed authentication failures activate at create search/direct/constraint reads, account enumeration/absence confirmation, key listing/deletion and delete evidence. Exactly one actual credential failure occurs per site, with no later calls against it; the healthy account continues. Stored rejection and monitorable account/path evidence remain. Local expiry continues; a failed post-confirmation constraint read preserves confirmed success with null date. | PRV-41/13c; OPS-33 |
| `w6_r6` | A held housekeeping DELETE leaves a logically expired physical row observable: operator detail returns 410 and the original retained horizon. Real retirement retains the same identity, including a retained episode current attempt with an operator-only receipt and effective-tenant authorization. Unknown/out-of-scope IDs return 404; non-admin override is ignored, customer operator verbs hide, unresolved history remains readable. | WIR-27/34/51; STO-33 |
| `w6_r7` | Offline schema-2-to-3 migration retains the exact old empty-summary cleanup operation and episode identities. The real worker performs one acceptance-only delete and later resolves by direct evidence without another deletion or successor failure. Unrelated malformed, missing-target and wrong-account rows fail incomplete_record before marker/provider effects. | STO-12/13; OPS-23/35/45 |
| `w6_r8` | Authenticated resolve/retry/keep emit correlated intent and observed commit outcomes with principal, target and safe reason reference. An actual landed COMMIT response loss emits uncertainty; exact replay emits replayed and preserves all effects. Acknowledged retry events name the fresh attempt. Free-form retry prose is hashed. | SEC-32/39; API-10; STO-35 |
| `w6_r9` | Real API delete retains its acknowledged caller body. Controlled missing/false/unknown caller fields fail invalid_request, purge payload and leave marker/provider/money untouched; missing API-written target evidence fails retryable incomplete_record. Normal caller deletions and the shared system cleanup worker still execute. | OPS-2/23; WIR-22; DOM-10 |
| `w6_c1` | One authoritative exact match and attributed failed confirming reads cross the 60-second negative window and ordinary successors. The original 43-sat commitment and unresolved exclusion survive incomplete/failed rereads. Recovery attaches using ordinary 20/11-sat debits and a 12-sat running reserve; one purchase, no late deficiency, episode or deletion. | OPS-27/33; LDG-32/39; STO-58; selected CNF-84/85 arm |
| `w6_c2_failed` | Real schema-2 failed current attempts migrate stalled with original identities and a null fence. They are listed and never automatically retried. Operator retry creates a distinct attempt, performs one ordered deletion and closes resource_gone; a separate operator keep closes without money/provider effects. | OPS-23/45/48; STO-52; API-64/68; selected CNF-271/272/288 arms |
| `w6_c2_unresolved` | Real schema-2 unresolved target survives migration as uncertain; failed non-mutating reads leave the engine alive and no repeated delete. Operator not_applied then keep supplies the supported exit. A separate running legacy row receives its target offline and is classified uncertain by the real startup pass, retaining unresolved evidence rather than guessing success. | OPS-15/23/45/48; STO-52 |
| `w6_c3_system` | A system cleanup automatically settles with zero operation receipts. The actual job removes its precisely aged settled row; logical and physical detail return 410 with the same configured horizon. Episode identity remains; actual tenant override succeeds, wrong-tenant/unknown IDs stay 404 and customer operator routes stay hidden. | WIR-27/51; STO-14/25/33/35; selected CNF-158 readability arm |
| `w6_c3_retry` | A fresh operator retry automatically settles without a resolving operation receipt. Actual housekeeping retirement preserves its current episode identity and the same global/scoped 410, horizon and privacy checks. | WIR-27/51; STO-14/33; API-64 |
| `w6_c4_search` | A normal accepted create reply reaches its selected ordinary search authentication fault. Exactly one rejection persists credentials_rejected; no later credential calls occur, a healthy account continues, and one purchase remains unresolved with its original 43 sats and safe reply identifiers. | PRV-41; OPS-11/13 |
| `w6_c4_get` | The selected ordinary confirming get rejects authentication after an exact search. The credential stops durably without periodic repetition; original terms, reply/search identifiers, positive knowledge and 43 sats remain unresolved with one purchase. | PRV-41; OPS-27/33 |
| `w6_c4_constraints` | The selected ordinary post-confirmation constraint read rejects authentication once. The credential stops, another account continues, and the confirmed create succeeds with null date, two ordinary debits, 12 sats remaining and one purchase. | PRV-41/13c; OPS-21 |
| `w6_c5` | Real authenticated resolve/retry/keep requests fail missing/invalid key, unknown/out-of-scope target, invalid fields/body, invalid override and rate validation. Each emits a correlated refused event with safe known identity/reason/metadata and no secret sentinel, acknowledgment, receipt, ceiling, money or provider effect. | SEC-32/39; WIR-34/35/51/55 |
| `w6_e2_offers` | The ordinary preflight offers call rejects authentication once. The durable credential stop prevents remaining work, sweeps and a subsequent admission; the pre-step-9 failure releases its original reserve with zero purchases. A healthy account completes one purchase. | PRV-41/5/7; OPS-11/12/43; CNF-284 |
| `w6_e2_keys` | The selected temporary-key registration rejects once after licensing, with no ownership inferred and no order. Durable stop, queued refusal, successor and healthy-account continuation are observed. | PRV-41/9; OPS-11/12/43; CNF-284 |
| `w6_e2_order` | The selected order rejects authentication before acceptance, retaining the licensed worker position and zero purchases. No cleanup/read/scheduled call can reuse that credential; queued and subsequent creates are refused while a healthy account continues. | PRV-41/5/9; OPS-11/12/43; CNF-284 |
| `w6_e2_key_cleanup` | A genuinely owned temporary key and deterministic provider/400 order refusal reach pre-acceptance cleanup, which rejects authentication once. The original mutating refusal is retained, the key remains safely owned/eligible, and no sweep repeats against the rejected credential. | PRV-41/5/9; OPS-11/12; CNF-284 |
| `w6_e2_delete` | A real suspended-tenant cleanup reaches its licensed/fenced delete and rejects authentication once. The attempt fails with a stalled episode and retained fence/running-machine funding; no gone-write or later credential call is invented. Other-account work continues. | PRV-41; OPS-11/41/42/45/48; CNF-284 |
| `w6_e3_reply` | A matched but independently removed machine has both charged obligations. The resolution commits native 2/1 costs with null clamped_sats; its exact session's COMMIT reply is lost. Whole-store retry and an ordinary successor preserve the exact two deficiency rows, original money state and one purchase. | STO-37; LDG-39/66; OPS-27/49; selected CNF-162/197 |
| `w6_e1` | The real API/engine prefix precedes 20k added operations and 200k added receipts, 990 actually due. The ordinary housekeeping transaction retires them and retains exact horizons, with financial/history state unchanged and the API/job still live. The red observes attributed SQLSTATE 57014, failed housekeeping and full rollback. | STO-14/24/25/33; WIR-27/51; selected CNF-158 |

The earlier core phase additionally exercises ENG-04 stale-claim refusal, ENG-06 whole-settlement repeats, ENG-08 rejection of provider/rail/rescue entry while a component owns a transaction, and the existing generation/stop guards. W5 retains all interruption assertions: explicit provider/evidence barriers isolate its historical unresolved/snapshot boundaries from W6 periodic updates. The original failed-checkout assertion, observer faults and narrow timing bounds remain intact. Retention ignores only explicitly tagged background evidence-read events while retaining every purchase/effect and the whole independent resource state.

## Required negative controls and baseline regressions

All 39 isolated controls are recognized as `expected negative demonstration` in [final-controls.json](final-controls.json), each with actual child exit 1 and cleanup `pass`. The four new controls require their exact fault and assertion, not arbitrary setup failure:

| Defect | Observed fault | Exact failed assertion |
|---|---|---|
| `w6_blind_repeat` | Two independently durable accepted purchases after actual lost reply | `one purchase/no repeated order` |
| `w6_non_exact` | Wrong returned correlator actually attaches in defect build | `exact-correlator/no-attachment` |
| `w6_release_timeout` | Original reservation closed at local timeout before negative window | `original-reservation-preserved before negative window` |
| `w6_skip_startup` | Closed-marker ordinary startup leaves running row and reports zero classified | `interrupted-running-row classification/count` |

The unchanged review base is `a56660d142371d208b05e3f280eac1d2344a9bcf`. The historical fixture drivers recorded with simulator-only overlays reproduce two actual old-code faults: [gone reply](red-baseline-gone.json) fails `PRV-36 gone reply must override stale resource read`; [non-exact candidate](red-baseline-exact.json) fails `OPS-29 settle-time non-exact candidate must not attach`. Both child exits are 1 at the intended assertion after activation; cleanup passes, and all 32 protected runtime/migration/dependency hashes match the base before and after. Their corresponding scenarios remain in the final W6 inventory; those older artifacts retain their recorded driver identities. These real regressions are separate from deliberate sensitivity defects; newly implemented features are not portrayed as baseline bugs.

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

## Nine current-W6 regression reds

The same maintained drivers also ran against an invocation-owned export of unchanged pre-fix W6 runtime `9454e453b2aa27b20820f98269a8d4877fb0dcad`. All 37 protected runtime, migration and dependency/configuration hashes match that commit before and after each invocation. Only fixture files are overlaid; each raw manifest truthfully names its export metadata commit. This is evidence about existing W6 defects, separate from the original pre-W6 baseline regressions.

Each child exits 1 at its named application assertion after activation, with cleanup `pass`: [R1](red-w6_r1.json), [R2](red-w6_r2.json), [R3](red-w6_r3.json), [R4](red-w6_r4.json), [R5](red-w6_r5.json), [R6](red-w6_r6.json), [R7](red-w6_r7.json), [R8](red-w6_r8.json), [R9](red-w6_r9.json). Their fixed greens are the nine named rows in the complete final W6 inventory. No required fault is retried away.

R2 records an actual effective visibility maximum of 120.0 seconds at its retained-sample boundary, while the immutable declaration remains one unmeasured second. Its explicit fixture sample is 120 seconds; other retained samples can be larger. Ordinary mutation/resolution observations continue widening the maximum used by admission and wind-down sizing under PRV-36/STO-53. The negative window remains the separately measured 60 seconds; provider-read eligibility cannot freeze its money.

The survived step-8/pre-step-9 worker-position classification arm remains in W5 `w5_step8`. This is distinct from R1's persisted-marker ordinary-restart arm.

## Remaining blocking regression reds

[New regression proof](continuation-regressions.json) records nine intended exit-1 assertions on
unchanged `5061fe34e127c68de68e8312dbca6f875f6dc790`, with 37 runtime/migration/dependency
hashes protected before and after. Only fixtures overlay the isolated export. Each arm records its
independent activation and cleanup; the corresponding green is in the final 47-group inventory.
Search/get baseline faults occur first in ordinary verification and then wrongly repeat in periodic
resolution; the constraint baseline leaves credential health unchanged. Neither is misattributed to
a prior lost reply or a periodic call that won the activation race.

The historical assertion-driver and imported-helper identities remain recorded with those reds.
Their behavioral assertions remain in the current greens. The earlier R2 fixture change only waits for its stored in-progress fact before the controlled restart; the imported
new-regression helpers are unchanged. R2 preserves its named healthy-expiry assertion; its later
in-progress arm now waits for the retained row before the controlled stop. Older `9454e45`,
`8f0253d` and review-base red artifacts retain their actual source identities and bytes.

Single-match confirmation uses its own provider observation history, so earlier visibility samples
cannot alter its pre-expiry setup. Its disclosed fixture age is 61 seconds against the separately
measured 60-second negative window. No allocation bound, provider-read deadline or PRV-36 sample is
shortened. New account fixtures restore the previous assignability state. Schema-reset fixtures run
last while components are stopped. Receiptless retirement advances only the settled row's due-time;
the actual housekeeping job performs deletion. These are isolated fixture clocks, not production policy.

Superseded private executions remain failures: one full run stopped at the old deadline fixture's
exact metadata comparison after the new positive evidence was added; one W6 preflight omitted the
worker-added correlator in a new frozen-summary assertion; another full run stopped before the R2
positive fact was retained. The corrected fixtures preserve the deadline, reserve, original-origin,
no-repeat and restart assertions. Only the final complete source-bound exit-0 run is exported here.

## Final retirement, credential and financial-field regressions

[Eleven protected reds](blocking-regressions.json) use unchanged
`ac83e172dae1d3c0cc7934c9633a403cfca0a679` runtime, migrations and dependencies. The recognizing
runner exits 0 only after every child exits 1 at its exact intended assertion with fault activation
and cleanup pass. All 37 protected hashes match before/after; the isolated export is removed.
Every fixture overlay is byte-identical to the corresponding final green. The older review-base,
initial-W6, `9454e45` and `5061fe3` artifacts remain unchanged with their actual source identities.

E1's real due transaction times out with SQLSTATE 57014 on the baseline; housekeeping exits,
the listeners stop, all 20k selected rows remain and no partial retired identity survives. The
fixed canonical driver observes 990 retirements, retained horizons and later API/job liveness.
The [supplemental matrix](retention-bounds.json) separately runs all three original exact scale
shapes. No-work and mixed probes and retirement plans each scan receipts once. Mixed retirement
execution is 49.982 ms at 20k/200k, 144.978 ms at 20k/1M and 195.318 ms at 50k/1M, under the
unchanged 1000 ms statement bound. Each real API removes the exact 991 returned IDs: 990 expired
receipted rows and one old receiptless row. Other-scope live aliases, unsettled statuses, recent
receiptless settlement and financial/receipt history survive; rolled-back measurement writes leave
no identity. Default 20k/2M retained reads and ordinary retention also exit 0. These are local
measurements, not production sizing. Minimal retired identities remain permanent; the separate
eventual retired-history lookup scan remains deferred.

E2 independently selects offers, key registration, order, pre-acceptance key cleanup and deletion.
Each fault activation sets `PROVISIOND_WORKERS=1` to serialize already queued work; the ordinary
successor uses the profile's three workers. Per-call, statement, store-retry and stop bounds are
unchanged. The baseline leaves the credential healthy or repeats authentication before a sweep stops it; the
fixed runs record exactly one rejection and a durable stop. Already queued work, later sweeps,
ordinary successors and another admission issue no later credential call; another account buys
once. Rejected creates buy zero times. Delete rejection preserves its stalled episode, fence and
running funding without inventing absence. Generic order/delete refusal fixtures now use genuine
provider/400 refusal; the existing secret-redaction authentication fault stays intact on a separate
account. No runtime credential repair, rotation or automatic health reset is introduced.

E3's old real producers fail the null-field assertion on charged absence, abandoned estimates,
released late attachment and a reached outage exit. Final records retain their native amounts,
opening-rate/null provenance, absorbed windows and subject/account attribution, with SQL/JSON null
clamped_sats for all non-clamp causes. Quarantined outage closure uses the same nullable shape.
The two-hour exit independently computes the positive exact satoshi clamp and native partition.
A charged-absence resolution loses its actually landed COMMIT reply; retry and successor preserve
its exact deficiency identities and financial state, without another purchase. This covers reached
exit accounting; excluded OPS-41 no-rate decision steps and general billing remain not run.

Final validation is bound to the committed operational source in [validation](validation.json).
The final-only bundle replaces the earlier final manifests. Early ownership/fixture prechecks and
validation stopped on a source-hash change are excluded; their raw results remain private. No
required activated fault was retried away or counted as a pass.

## Source and publication audit

The operational commits and changed paths are listed in the [source change report](source-changes.md). AGENTS.md and spec-lock.json are unchanged. Earlier W1–W5 evidence is unchanged; the superseded W6 final manifests are replaced; no new branches/tags, history rewrites, push, pull request or merge was performed. All old components must stop and be prevented from restarting before the offline schema-3 migration; mixed releases and rollback past this release are not supported. Every test owns its recorded ports/directories/credentials; cleanup is restricted to recorded invocation resources. Externally retained raw proof is private.

The publication audit recursively removes host paths, local coordination identifiers, IPs, credential/payment identifiers and test sentinel secrets. Stable opaque replacements preserve equality. Activation, operation/resource IDs, true statuses, source hashes, relevant complete state and independent effects remain assessable. High-frequency protocol/observer transport records are omitted from compact manifests; complete private raw trace hashes remain. Source inventory distinguishes later documentation from tested operational files.

## Explicit limits and follow-ups

- W7, `not run`: open-incident continuation of running-row recovery, null-create exception suppression throughout an incident, restore grace/deferral, restore/promotion/standby replacement and incident procedures. Startup still refuses an open incident. Closed-marker ordinary startup proves none of these.
- Deferred follow-on owner, `not run`: general exhaustion and suspension fan-out enqueuers (refused), extend-runway and late-attach survival by extension, no-rate OPS-41 steps 4–5 (reached branch stops safely without fence/call), scheduled cancellation, other mutation kinds, billable surviving attachments, import/rescue and real providers. Immediate destruction explicitly declares no survivors.
- W8, `not run`: production D-6 pool/capacity/timeout/reconnect/restart/backoff/supervisor policy, deployment saturation or observer-access guarantees and offline deployment/recovery procedures. W9–W11 and any real-provider pilot remain unauthorized.
- Deferred follow-on owner: general periodic billing, rate construction/solvency, full cross-kind conformance, rail-history response-size bounds, descriptor-at-startup deadlines and unrelated harness hardening. Fixture rate acceptance records support the reached exits; they are not a market feed or an implementation of the general rate estimator.
- Unresolved outcomes remain external uncertainty: non-authoritative/multiple/conflicting/failed evidence never invents success. The original reserve has its bounded OPS-33 lifetime; an unresolved resource-offer exclusion has its own outcome lifetime. Matched in-progress orders and authoritative single matches awaiting confirmation are positive evidence, not negative timeouts. Operator decisions retain evidence and identity.

The twenty-three follow-ups retain their ratings, affected paths and deferred scope:

1. **P2 review-followup — missing attempt backend evicts a healthy observer.** Prior review; `src/store/accounting.rs:127–155,196–198`. One absent backend tears down shared attribution for other attempts. Conservative charging, no undercount/money loss; not higher. Separate backend absence from observer visibility failure; test two registered attempts and one exited backend.
2. **P2 review-followup — observer reconnect/error churn lacks backoff.** Prior review; `src/store/accounting.rs:161–194`. A persistent failure can retry/log every 20 ms, including with no registration. Availability/noise rather than a proved unsafe charge; not higher. Pace/coalesce within existing bounds, verify bounded attempt/log count and later recovery.
3. **P3 review-nit — stale precommit accounting comment.** Prior review; `src/engine.rs` comment by transaction accounting (search “precommit”). Actual whole-attempt arithmetic differs. Comment accuracy only. Correct wording in a later change; inspect against arithmetic, no runtime change.
4. **P2 review-followup — W5 raw external-effects label omits W5.** Prior review; `tests/postgres/harness.py` manifest `external_provider_rail_evidence` predicate. Nested effects remain present and sanitized summaries are truthful; not higher. Add the phase to the label in separate work and assert manifest classification.
5. **P2 review-followup — early control-helper failure omits parent manifest.** Prior review; `tests/postgres/controls.py` direct release-helper probe early return. It still exits nonzero, so no green masking; not higher. Write a failed parent with child pointer before returning; exercise deliberate helper failure.
6. **P3 review-nit — historical W5 publication sentence.** Prior review; `evidence/w5/report.md:20`. Dated sentence says nothing was published, now stale. Wording only; do not rewrite prior evidence in W6. Future publication context can explain the date.
7. **P2 review-followup — malformed rail settlement response ends watcher/API.** Prior review/preexisting W6; `src/api/funding.rs:133`. No false credit; unsupported malformed trusted-adapter shape fails closed, so not higher in this package. Separate observation refusal from watcher lifetime and verify later valid observation can recover under a separately selected policy.
8. **P2 review-followup — transient accept error ends API.** Prior review/preexisting; `src/api/mod.rs` customer/operator `listener.accept()` error propagation (around 1237–1251). Availability failure, no false success/financial write; not higher. Classify transient errors/pacing in later work, test an attributed transient refusal. Do not change the loop while adding R8 events.
9. **P2 review-followup — activation without a healthy assignable account is never assigned later.** Prior review/preexisting; `src/api/funding.rs:84–88`. Assignment sweep absent; specification duty not yet assessed and explicitly deferred, so not higher. First assess pin, then add the required later assignment or retain a documented limit with an account-recovery scenario.
10. **P3 review-nit — funding watcher scans all retained history.** Prior classification retained; `src/api/funding.rs:135`. The prior finding's proposed polish concerns avoiding needless repeated already-credited observation work; no demonstrated acceptance defect. No optimization in W6; assess cost separately and preserve replay semantics. If later evidence establishes behavioral performance failure, reassess severity then.
11. **P3 review-nit — one-byte header reads / constant consume flag.** Prior style findings; `src/http.rs:28`, `src/api/mod.rs` `rate_allowance` consume argument. No demonstrated supported behavior change required here. Any future simplification must preserve wire/rate semantics; do not optimize this Run.
12. **P2 review-followup — observer termination races sampler idle.** Prior review; `tests/postgres/w5_accounting.py:203`. Correct runs can fail before fault activation; cannot make broken runs green, so not higher. In separate work wait with a bound for the attributed observer idle point, then terminate only its precise PID.
13. **P2 review-followup — historical full-trace snapshot can precede final simulator event.** Prior review; `tests/postgres/simulator.py` read_response_returned event after delivery and `tests/postgres/w5.py` exact snapshot comparison (original lines 340–345). Fails closed on a missing final event, so not higher. Later wait for the attributed completion before freezing, preserving exact equality. New R1–R9 traces must not add this race, but do not repair unrelated historical comparisons.
14. **P2 review-followup — backoff probe has about 7 ms scheduling window.** Prior review; `tests/postgres/w5.py` 1.993 s boundary and 1990–2000 ms assertion (original lines 903/911). False reds only; no broken green, so not higher. A separately authorized controlled retry clock/hook can make the exact boundary deterministic without changing runtime budget.
15. **P2 review-followup — excluded no-rate decision exits the whole engine.** New round 1; `src/engine/deletion.rs:29–34`, `tests/postgres/w6.py:370–375`. No fence/call, no unsafe monetary effect; fixture-controlled omitted branch, so not higher. Future scope can isolate the stopped path within the prescribed no-rate lifecycle; do not implement the deferred steps or requeue by an invented policy in W6. Verify unrelated work continues once that policy is authorized.
16. **P2 review-followup — malformed keys listing terminates the periodic component/engine.** New round 1; `src/engine/reconciliation.rs:286–297`, `src/bin/engine.rs:64–75`. Wrong-shape success from the simulator/provider boundary is outside the currently exercised supported reply shape; fail-closed availability, no invented absence/money loss, so not P1. Later handle malformed evidence locally and test continuation. This is distinct from R7's valid legacy-row crash; do not silently fix this malformed-reply branch with R5 authentication handling.
17. **P2 review-followup — periodic throttle pacing and remaining account calls.** New round 1; `src/engine/reconciliation.rs:167–171,286–296` and `src/bin/engine.rs:64–75`. A throttled channel ends its page traversal but other calls/one-second passes can run without honoring a larger retry hint. Current absence handling fails closed and production pacing is W8-deferred, so not higher here. In later pacing work yield/coalesce account work using provider hints within the selected policy; test an attributed throttle and call history. Do not confuse it with R5 terminal authentication, which is mandatory now.

18. **P2 review-followup — admin effective-tenant overrides incorrectly require UUIDs.** API-6/DOM-1 allow 1–128 ASCII letters, digits, dot, underscore, colon and hyphen, so a valid identifier such as `agent-7` is rejected before scoped lookup. Ordinary enrollment generates UUIDs in this prototype, so the supported ordinary path is unaffected. Validate the pinned identifier grammar in separate work, preserving malformed/unknown-tenant privacy. This grammar remains unchanged by the retention and audit repairs.
19. **P2 review-followup — external-id validation differs between deletion and resolution.** `src/engine/deletion.rs` accepts dot while `src/engine/reconciliation.rs` excludes it. A valid dotted identity can pass deletion and be refused by resolution filtering. The simulator generates non-dotted identities; align parsing in separate work while preserving exact attribution and malformed/non-exact refusal. Parsing is unchanged here.

20. **P2 review-followup — retired-history detail/list predicates scan the permanent retired table.** `retired_operations` has only its id primary key, while `operation_history` uses id::text equality and tenant filtering. Eventual read cost is unmeasured and beyond the represented retired-history scale. Measure it separately; preserve permanent identity and finite cutoff. E1 changes only due retirement.
21. **P3 review-nit — older PRV-41 conformance anchor points one line before the clause.** The older R5 row links line 1267, while C4 links 1268. Navigation only; the old anchor is unchanged in this repair.
22. **P3 review-nit — DOM-21 conformance prose still says the receiptless gap remains.** The older row disagrees with the current C3 implementation/evidence. Wording only; that row is left unchanged here.
23. **P3 review-nit — legacy unresolved fixture error is not text-identical to the base.** `w6_continuation.py` says `engine restarted while operation was running`; the base said `interrupted by ordinary restart`. Migration never reads that text. Keep its state/identity/boolean assertions; literal alignment remains deferred.

- D-37/D-38 remain unchanged: missing observer samples are charged, completion-only accounting cannot force an exit while a live transport withholds completion, and the PostgreSQL one-contiguous-SyncRep-span premise remains unverified. Review judgment does not replace execution evidence.

Receiptless cleanup/retry retirement is covered by the new actual-housekeeping arms above. D-6 production ceiling values, credential custody/rotation, incident override and deployment policy remain unselected.

Demonstrating no duplicate purchase for the supported traces is not a proof of exactly-once execution by an arbitrary provider. External uncertainty remains part of the product model. [S5] [S10]
