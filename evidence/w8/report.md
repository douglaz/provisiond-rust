# W8 offline release and local saturation evidence

Final canonical **fail**: exit 1 in 1844.113252 seconds on clean tested commit
`b1b0c721e5503d5ee7ff4f94efca7b8f91205afc`. All twelve phase processes executed; eleven passed.
W5 failed at successor startup because the fixture's pre-admission allowlist rejects a clean
`driver_closed` event after a 250 ms admission checkout refusal (F27, pvr-58k). No session was
admitted, no generation/business work or transport fault was armed. The immutable failed run is
preserved in [canonical results](canonical-final.json). No product policy changed.

The single separate W5 follow-up on unchanged clean b1b0c721 **passed**, exit 0 in 137.348692
seconds, all 27 required selectors and 30 pass rows. Its observer sent-COMMIT and saturation
proofs were not reached by canonical W5. The follow-up never turns the canonical result into
pass. No further tests or full-suite rerun occurred during finalization.

| Invocation or collection | Actual result rows | Process result |
|---|---|---|
| Final canonical, all 12 phases | 259: 251 pass, 1 fail, 2 expected negative demonstrations, 5 not run | fail, exit 1 |
| Separate focused W5 | 30 pass, all 27 selectors | pass, exit 0 |
| Complete phase collection: 11 canonical phases plus focused W5 | 280: 273 pass, 2 expected negative demonstrations, 5 not run | Separate collection; no single green all-phase run |
| Final canonical controls, outside scenario counts | 44 activated negatives and 5 candidate negatives | Parent pass, exit 0; each negative exit 1 |

[Coverage](coverage.json) compares every exact historical W7 result name and recorded selector
inventory. Historical W7 has 278 scenario rows; two new normal cases are W5 saturation and W6
attachment conflict. All names survive in the separate complete phase collection. Canonical W5's
failed aggregate row is retained as failure, excluded from functional selector comparison, and
its unvisited selectors explicitly remain not run in that invocation. The five retained not-run
rows are commits DB-10; core ENG-05, ENG-07 and DB-10; and W6 OPS-41 no-rate steps 4–5. No
inconclusive row was observed in the final canonical. General saturation evidence does not
rewrite historical phase-specific DB-10 statuses.

Final canonical workflow has 84 pass rows; W6 has all 53 selectors, 54 pass rows and one not-run
row; W7 has all 21 pass selectors. All eleven ordinary phase cleanups and all 49 nested negative
child cleanups pass. Controls aggregate through child evidence/path manifests; they have no events
list. Focused W5 cleanup passes. The retained harness-compatible SO_REUSEADDR check binds all 500
ports after canonical. The earlier aggregation KeyError and no-reuse EADDRINUSE probe were artifact
check outcomes, not scenario or cleanup failures. The latter does not establish a listener leak.

The final iteration **passed**, exit 0 in 855.070254 seconds: fast gate exit 0 (runner integer
wall 1 second), 50 Python tests exit 0 (runner 2 seconds; framework 2.354 seconds), all 21 W7
selectors and 44 controls plus five candidate negative demonstrations. It began at 18574b3 with
reviewed combined patch `7a4ae2b91a20dd2922cf81271ff693bd61c3c6404fc45b32988d2f15931a2419`,
then committed byte-identically as b1b0c721. It is not a clean b1b0c721 iteration run. [Validation](validation.json)
and [commands](commands.json) give completed argv, source identity, timings, Nix mode and versions:
PostgreSQL 18.6, Python 3.14.7, rustc 1.97.1, cargo 1.97.0, Nix 2.35.6, canonical systemd 262.
Deployment templates use systemd 260; host artifact checks use Python 3.14.10. Per-unit caps were
16 GiB, no swap, OOMPolicy continue.

The first [canonical](canonical-first.json) failed, exit 1 in 1678.641406 seconds on clean
historical 18574b3, workflow and W6 failed. Prior failed iterations and live attempts i1–i6
(including i1b) remain in [history](history.json) and private retained artifacts. Focused repairs
and later passes do not replace those failures. No scheduling cause or deterministic-suite
claim is established.

[Live](live.json) is the recorded local simulator journey i7, exit 0 in 170.318 seconds,
runtime aa1ad12, driver 014edf9. Exact old/new deployed binary and release identities are retained.
It exercises offline schema/semantics 4/4 to 5/5, stopped/fenced old sessions, serialized migration,
compatibility exclusion, ordinary observer refusal, supervisor bounds/alarms and standby restoration.
The waiting engine XID 6164 equals succeeded outcome xmin 6164. The owned API tenant-advisory
checkout has null XID. Typed 503 starts no business transaction and leaves no refusal receipt
across observed API generations. Four customer attempts obey retry advice and exact resend;
one simulated purchase, charge and balance are verified. D17/D18 functional result is not run,
reachability not reachable. Administrator assertion queries are separate from recovery controls;
out-of-band recovery needs no fresh SQL. Primary shutdown fallback remains not run. No separate-host
rehearsal, production SLA or continuous availability is demonstrated.

[Regression proofs](regressions.json) preserve three historical runtime RED/current recorded GREEN
pairs with actual historical executable/fixture hashes and intended assertions. Final current
GREEN for sent-COMMIT observer retention is the separate W5 follow-up, not the failed canonical.
Safe identifier retention is final canonical `w6_c1`; external identity conflicts before debit
are canonical `w6_attachment_conflict`, both same-tenant and other-tenant, two attempts each,
zero fee/unit debits and unchanged state. Final fixture identity differs from historical RED;
no byte-identical fixture claim is made. pvr-kin remains open: controlled post-admission engine
checkout/configuration failure plus recovery and paced enrolment retry are not run. C14's
supervisor bound does not prove those injections. D-70 and OPS-47/49/21 remain partially verified.

[Fixtures](fixtures.json) retain W4's owned SEND / SELECT 1 CommandComplete / idle ReadyForQuery
and two distinct execution proofs; four real helper tests, RED 12 intended failures/zero setup
errors, GREEN exit 0. W6 checks the absolute deadline before acceptance and captures request
evidence before its closing database snapshot with unchanged append-only maximum/cutoff; five
real helper tests, RED three intended failures/zero setup errors, GREEN exit 0. Its eligible
missing-search negative driver's exit 0 verifies the intended rejection, not a functional pass
without evidence. Final canonical workflow and W6 pass these repaired fixtures. W6 demonstrates
only a stable window; widened-window behavior is not run and batch-snapshot runtime implications
remain preexisting/unmeasured. W7 controlled old-checker RED and repaired GREEN retain measured
windows and eligible later absence; original scheduling cause remains unmeasured.

Known limits remain: pvr-31s open P1 has only failure_run repaired; its reconnect-site setup race
persists. Preexisting pvr-2g0 non-UTF8 diagnostic and pvr-oqf two-snapshot balance read remain
unfixed. F24 is a fail-closed first-window budget limit after later window widening, not observed
in the controlled runs. F27 is the observed canonical fail-closed fixture rejection. Six final
follow-ups are filed in addition to 18 earlier items; the entries are recorded in the
[tracker](../../.beads/issues.jsonl):

| Follow-up | Issue | Scope |
|---|---|---|
| F21 | pvr-f6w | Redundant DDL transport assertion |
| F22 | pvr-zav | Immutable saturation attribution source |
| F23 | pvr-wlk | Separate alarm regression test source identity |
| F24 | pvr-5rl | Bounded fixture wait after window widening |
| F26 | pvr-yqr | Default-resolution witness identity guards |
| F27 | pvr-58k | Clean pre-admission driver closure allowlist |

No CONCURRENTLY migration exists; invalid-index/crash-resume is inapplicable and not run.
Production capacity/alerting, real providers/funds and W9–W11 remain outside the demonstrated scope.
D-6 is settled only for the measured local prototype; production parameters remain a separate open choice.

[Source inventory](source-inventory.json) gives the complete 104-file tested operational map
`b19007e2cda34a4ef9d19c1ce30683f9090f98280af49f0d0098bf1e2fa99a2e`, unchanged during
canonical and focused W5 and equal after documentation/evidence writes. Runtime files equal
`aa1ad12d1ff2b9085ee2c11b84730dffbc32ff61`. Documentation and tracker metadata were uncommitted at the recorded equality observation;
this does not relabel the historical dirty iteration or clean canonical. This bundle identifies
the tested commit separately from publication metadata and embeds no circular publication hash.

[Retention](retention.json) extends the original selected 392-artifact index with cited final
records and child manifests. Every indexed source is frozen privately before hashing. Only safe
field/type whitelists, hashes/byte counts and private placeholders are published. Raw requests,
streams, credentials, database/WAL/backups, patch bodies and journals stay private. Local links
are checked; external links receive syntax-only checks. Pattern scans supplement the field schema
and do not establish universal secret absence.

STO-7/12/13/55, OPS-13/21/27/47/49, OVR-18/19, LDG-32 and CNF-57/373 at pin
ac37c7cae5d3ff746515137774a4cdd5a3531375 own these narrow claims. Pinned source lines and
implementation symbols are in the [conformance map](../../docs/conformance-map.md).
