# Retention query bounds

Fix/runtime/fixtures: `4812b111619acc0fbb0273c407ace1e143b0fb4a`. Specification pin: `ac37c7cae5d3ff746515137774a4cdd5a3531375`.
This companion supersedes the parent retention supplement for final runtime validation.
Independent Run review is **pending**. No publication or nine-phase suite is claimed.

`SETTLED_RETENTION` materializes settled candidates, joins all resolving operation receipts,
aggregates maximum persisted expiry once per candidate, and joins that result back to candidates.
Probe and DELETE share this relation and reevaluate eligibility in their own statement snapshots.
Actual plans show one receipt scan and hash joins/aggregation, with each receipt relation executing
once. The baseline plan retains the nested-loop anti-join over live receipts. There is no new index
or migration; both original migration hashes in [source](tested-source.json) remain unchanged.

D-32 Reading B, STO-33/25 and API-11 remain intact: one finite admission-origin replay/readability
horizon, permanent key/fingerprint refusal, maximum expiry across all resolving keys/scopes,
full timestamps with equality expired, and no new horizon on replay, configuration change or late
settlement. Queued/running/reconciliation-pending operations remain available beyond expiry.
Receiptless rows retain strict terminal-age fallback. Ledger, commitments, machines, counters and
episodes remain outside deletion. The original retention green checks matching/mismatched replays,
maximum expiry through another scope/alias, config changes, fractional timestamps, logical gone
before DELETE, genuine uncertainty/restart and late settlement without another purchase.

The genuine once-per-second housekeeping regression on unchanged `d298828bea920f285a77540aa97728d545fbce64`
failed at **RETENTION HOUSEKEEPING MUST STAY LIVE WITHIN STATEMENT BOUND**. It observed SQLSTATE
57014, the actual retention query error, “API housekeeping failed”, listener shutdown and API exit 1
in 2.193646 seconds, with zero successful probes. Child exit 1 and cleanup pass
were independently recognized by the audit (exit 0). The identical final fixture observes five
successful probes before its read/mixed checks on each final shape, with no probe error.

All figures below are PostgreSQL 18.6 `EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON)` execution milliseconds
under ordinary planner settings and unchanged statement_timeout=1000 ms. DELETE measurements roll
back, then the actual binary independently executes DELETE and acknowledges its transaction.
Each mixed shape removes exactly 991 expected IDs: 990 expired receipted settled operations plus
one old receiptless failure. Ten other-scope live aliases, recent receiptless settlement, all three
unsettled statuses and unexpired settled operations survive. Complete financial/history snapshots,
receipt count plus full-row digest, and independent simulator effects agree before/after.

| Existing operations | Operation receipts | No-work probe | No-work DELETE | Mixed probe | Mixed DELETE |
|---:|---:|---:|---:|---:|---:|
| 20,000 | 200,000 | 45.651 | 46.088 | 41.897 | 48.675 |
| 20,000 | 1,000,000 | 162.578 | 155.206 | 135.727 | 145.084 |
| 50,000 | 1,000,000 | 193.266 | 200.734 | 195.825 | 197.036 |

The base state is exactly 20,000 live settled operation receipts plus 180,000 expired operation
receipt-only rows. Each shape also has one ordinary deposit receipt from real simulator funding,
reported separately in seed counts. Increasing receipts to one million tests permanent-history
accumulation; increasing operations to 50,000 tests a larger current candidate set across 100 tenant
identities. The largest selected housekeeping statement execution is 200.734 ms (EXPLAIN ANALYZE). The
binary's observed probe intervals, query sent to command complete, reached 519.500 ms at 20k/1M. These local observations select no production capacity or horizon.

The unchanged global `OPERATION_HISTORY` was measured before the read repair. Initial ordinary
200k-receipt HTTP reads took 97–475 ms; at one million receipts detail/replay took 804/867 ms and
replay EXPLAIN ANALYZE timed out. Supplemental exact unchanged-query SQL measurements on 200k/1M
capture all six paths and baseline probe plans; at 1M, replay and deposit-replay EXPLAIN ANALYZE
exit 3 at the unchanged statement bound. Those are SQL-only measurements, not binary reds.
[Plans](plans.json) retain actual plans, parameters, timings and dispositions.

The independent real-binary read red uses 20,000 queued operations and two million operation
receipts so the unchanged housekeeping probe remains cheap. On unchanged `d298828`, detail,
operation replay, ordinary list, filtered list and deposit replay returned HTTP 500; no matching
cursor was available after filtered-list refusal, so that final red cursor read is **not run**.
The API remained alive, with eight successful housekeeping probes and no probe error. Seven actual
read-query protocol error responses and SQLSTATE 57014 are independently audited. The child exits 1
at **RETAINED HISTORY READS MUST MEET STATEMENT AND HTTP BOUNDS**, with cleanup pass; recognizing
audit exit 0. This is independent of housekeeping preemption. An earlier unchanged-runtime read
measurement exercised a matching cursor near its bound; it remains supplemental, not final-fixture
acceptance evidence.

`operation_history` now filters ID/tenant candidates before receipt aggregation. Detail selects
both inputs by ID; replay uses a lateral relation tied to the selected key's resource, and deposits
skip operation history. Lists select existing tenant operations, but aggregate receipts across
**every** scope for those IDs. Tombstone tenant authorization remains separate. Unknown/foreign IDs,
fingerprint precedence, signed cursor/filter/order semantics, read-only GETs and the uninterrupted
post-read destination check remain. Final HTTP reads on the three settled shapes took 4.01–178.24 ms;
the identical queued 2M fixture passed all six paths, including a matching signed cursor, in
4.39–165.53 ms. Statement execution, protocol intervals and complete HTTP time are separate fields;
complete store-call time is not independently instrumented. No statement, store-call, checkout,
idle, stop or priced read bound changed. Genuine housekeeping errors still stop the API.

The original retention proof was rerun because parser/workflow overlays changed. Unchanged
`8ea285aae86f8366689a2b44ea6884aa56486de0` failed at **SAME-KEY REPLAY EXPECTS EXISTING OPERATION**:
HTTP 410, physical_rows=0, 25.434929 seconds of live
receipt remaining after 4.226997 seconds. The final runtime returned HTTP
202 with the original complete operation, physical_rows=1, and
25.473473 seconds remaining. Its four retention arms pass. The documented
baseline-only retention_seconds=2 versus final 30 exception remains limited to this older baseline;
receipt_seconds=30 in both. d298828 scale/read fixtures have identical parameters/bounds and overlays.
All original baseline files and unchanged compiled-probe sources were audited before overlays and
again before exact export removal. [Source](tested-source.json) retains original and overlay hashes,
fix-bound operational files, configurations, dependencies, builds and actual executable launches.
The 516 launch records explicitly label selected proofs, baseline sources and excluded/superseded
attempts; historical launches are not represented as the final runtime.

Each command below ran after `nix develop --command`, with the private environment in
[validation](validation.json). The gate was exactly `nix develop --command bash tools/check.sh`;
its entire stdout/stderr went to fresh separate raw files and its saved true status was 0.
It ran on the final operational bytes before the fix commit; subsequent blob/hash audit binds those
bytes to the fix. The complete workflow and safeguards ran on the clean fix commit. The complete
workflow has 84 pass entries including existing expiry, refusal, ambiguity, mixed-pacing,
read-deadline and retention arms. Scale selectors run separately and do not replace existing arms.

| Exact child command | Exit | Result |
|---|---:|---|
| `python3 tests/postgres/harness.py --phase workflow --case w4_retention_scale --seed 20261111 --evidence <run-dir>/proofs/scale-red-fixed.json` | 1 | expected negative demonstration; recognizing audit exit 0 |
| `python3 tests/postgres/harness.py --phase workflow --case w4_retention_scale --seed 20261111 --evidence <run-dir>/proofs/scale-fixed.json` | 0 | pass |
| `python3 tests/postgres/harness.py --phase workflow --case w4_retention_scale --seed 20261111 --evidence <run-dir>/proofs/scale-fixed-million.json` | 0 | pass |
| `python3 tests/postgres/harness.py --phase workflow --case w4_retention_scale --seed 20261111 --evidence <run-dir>/proofs/scale-fixed-larger.json` | 0 | pass |
| `python3 tests/postgres/harness.py --phase workflow --case w4_retention_reads --seed 20261111 --evidence <run-dir>/proofs/reads-red-fixed.json` | 1 | expected negative demonstration; recognizing audit exit 0 |
| `python3 tests/postgres/harness.py --phase workflow --case w4_retention_reads --seed 20261111 --evidence <run-dir>/proofs/reads-green-fixed.json` | 0 | pass |
| `python3 tests/postgres/harness.py --phase workflow --case w4_retention --seed 20261111 --evidence <run-dir>/proofs/retention-red-fixed.json` | 1 | expected negative demonstration; recognizing audit exit 0 |
| `python3 tests/postgres/harness.py --phase workflow --case w4_retention --seed 20261111 --evidence <run-dir>/proofs/retention-green-fixed.json` | 0 | pass |
| `python3 tests/postgres/harness.py --phase workflow --seed 20261111 --evidence <run-dir>/proofs/workflow-final-2.json` | 0 | pass |
| `python3 tests/postgres/harness.py --phase workflow --seed 20261111 --evidence <run-dir>/proofs/expiry-final.json --case w4_expiry` | 0 | pass |
| `python3 tests/postgres/harness.py --phase default-workflow --seed 20261111 --evidence <run-dir>/proofs/default-final.json` | 0 | pass |
| `python3 tests/postgres/harness.py --phase core --seed 20261111 --evidence <run-dir>/proofs/stops-final.json --case stops` | 0 | pass |
| `python3 tests/postgres/harness.py --phase core --seed 20261111 --evidence <run-dir>/proofs/licensing-final.json --case licensing_stop` | 0 | pass |
| `python3 tests/postgres/harness.py --phase core --seed 20261111 --evidence <run-dir>/proofs/outcome-final.json --case outcome_stop` | 0 | pass |
| `python3 tests/postgres/harness.py --phase core --seed 20261111 --evidence <run-dir>/proofs/deadline-final.json --case deadline` | 0 | pass |
| `python3 tests/postgres/harness.py --phase controls --seed 20261111 --evidence <run-dir>/proofs/controls-final.json` | 0 | 30 expected negative demonstrations; each child exit 1 |
| `bash tools/check.sh` | 0 | pass |
| `python3 <run-dir>/diagnostic.py` | 0 | SQL-only measurement; two read EXPLAIN ANALYZE attempts exit 3 at statement timeout |

The canonical controls parent inventories all 30 controls, not the historical nine-control subset.
All thirty children exited 1 at their exact intended assertion after observed activation and cleanup;
recognizing parent exit 0. [Validation](validation.json) binds each actual raw child manifest and
its activation/assertion. No other eight-phase/full-suite coverage is implied.

Failed and superseded attempts are retained with actual exits. The first scale setup attempted
receipt deletion and failed at its permanent-retention trigger before any intended fault. An early
1M read attempt was preempted by a genuine housekeeping checkout failure and is inconclusive for
the intended read red. A pre-repair scale attempt failed at unchanged read EXPLAIN ANALYZE timeout.
Formatting changed the purge-string layout; all selected executable red/green proofs were repeated
with the final formatter-compatible fixture. The first full workflow attempt failed after 34 pass
entries at an engine startup checkout deadline; cause is not established. Cleanup passed; a fresh
whole phase on the same source, fixtures and bounds exited 0. No runtime or per-engine startup retry
was introduced. A supplemental SQL cursor setup was corrected and separately rerun; earlier raw
measurements remain excluded. No failed/unactivated attempt is counted as a selected pass.

[Resources](resources.json) records exact-path cleanup: 2,127 recorded owned PIDs absent, all ten
allocated ports rebound/released, and owned data, credentials, archives, build/cache/tmp directories
and both audited baseline exports removed after assertions. Two later SQL-only diagnostic pairs
were separately stopped/cleaned and ports checked again. The capped unit retained MemoryMax=16 GiB,
MemorySwapMax=0, OOMPolicy=continue and KillMode=mixed. Private raw logs/proofs remain under
`<run-dir>`; the external locator maps that placeholder and every native manifest root unambiguously.
[Raw inventory](raw-artifacts.json) records 1797 actual artifacts with byte counts and SHA-256.

No review tally is added. The parent supplement's retained review material is explicitly historical
Implementer pre-review, not Run acceptance. Independent review is pending. Older unrelated W4 P2s,
F3 retained rail delivery, startup-descriptor deadline limits, receiptless deletion identity,
already-deleted bodies, production sizing and D-32's specification wording clarification remain
outside this repair. D-6/7/8/9 stay Open. No real providers, purchases, funds, failover, transaction
pooling, external admission authority or durability fallback was introduced.
