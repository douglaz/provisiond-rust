# Conformance map

Specification pin: `douglaz/provisiond@ac37c7cae5d3ff746515137774a4cdd5a3531375` (see `spec-lock.json`).

Rows record individual implemented or planned arms; the local bootstrap arm has recorded evidence.
The test IDs are implementation test IDs from the implementation plan's failure-test inventory, not
new CNF identifiers. A CNF entry here is a traceability pointer: one passing scenario does not
discharge every arm of a larger conformance item. The requirement's complete current body at the pin
owns the behaviour; read it before implementing the row.

Status vocabulary for implementation rows: `not implemented`, `implemented`, `not demonstrated`,
`demonstrated`. Test results use `pass`, `fail`, `inconclusive`, `not run`, and `expected negative
demonstration`. A row reaches `demonstrated` only with an evidence reference to a recorded run.

## First-slice rows

| Requirement | Pinned source | Implementation symbol | Test ID | Evidence | Status |
|---|---|---|---|---|---|
| STO-7 | [05-persistence.md:169](https://github.com/douglaz/provisiond/blob/ac37c7cae5d3ff746515137774a4cdd5a3531375/05-persistence.md#L169) | `Store::commit`, `own_commit`, `commit_scenarios` | DB-01: Designated standby unavailable | [W2 lifecycle](../evidence/r2-final-commits.json), database/lifecycle arm only | demonstrated |
| CNF-296 | [10-conformance-checklist.md:1186](https://github.com/douglaz/provisiond/blob/ac37c7cae5d3ff746515137774a4cdd5a3531375/10-conformance-checklist.md#L1186) | `Store::commit`, `own_commit`, `commit_scenarios` | DB-01: Designated standby unavailable | [W2 lifecycle](../evidence/r2-final-commits.json), database/lifecycle arm only | demonstrated |
| STO-7 | [05-persistence.md:169](https://github.com/douglaz/provisiond/blob/ac37c7cae5d3ff746515137774a4cdd5a3531375/05-persistence.md#L169) | `CommitReceipt`, `own_commit`, `Store::recycle_idle` | DB-02: API request cancelled after COMMIT was sent | [W2 lifecycle](../evidence/r2-final-commits.json), database/lifecycle arm only | demonstrated |
| CNF-296 | [10-conformance-checklist.md:1186](https://github.com/douglaz/provisiond/blob/ac37c7cae5d3ff746515137774a4cdd5a3531375/10-conformance-checklist.md#L1186) | `CommitReceipt`, `own_commit`, `Store::recycle_idle` | DB-02: API request cancelled after COMMIT was sent | [W2 lifecycle](../evidence/r2-final-commits.json), database/lifecycle arm only | demonstrated |
| STO-55 | [05-persistence.md:291](https://github.com/douglaz/provisiond/blob/ac37c7cae5d3ff746515137774a4cdd5a3531375/05-persistence.md#L291) | `Process`, `residual_and_stop` | DB-03: Client process exits during `SyncRep` | [W2 lifecycle](../evidence/r2-final-commits.json), database/lifecycle arm only | demonstrated |
| CNF-373 | [10-conformance-checklist.md:1228](https://github.com/douglaz/provisiond/blob/ac37c7cae5d3ff746515137774a4cdd5a3531375/10-conformance-checklist.md#L1228) | `Process`, `residual_and_stop` | DB-03: Client process exits during `SyncRep` | [W2 lifecycle](../evidence/r2-final-commits.json), database/lifecycle arm only | demonstrated |
| STO-7 | [05-persistence.md:169](https://github.com/douglaz/provisiond/blob/ac37c7cae5d3ff746515137774a4cdd5a3531375/05-persistence.md#L169) | `Manager::create`, `Session::check`, `crash_and_restart` | DB-04: Backend crash and automatic primary reinitialization | [W2 lifecycle](../evidence/r2-final-commits.json), database/lifecycle arm only | demonstrated |
| CNF-365 | [10-conformance-checklist.md:1250](https://github.com/douglaz/provisiond/blob/ac37c7cae5d3ff746515137774a4cdd5a3531375/10-conformance-checklist.md#L1250) | `Manager::create`, `Session::check`, `crash_and_restart` | DB-04: Backend crash and automatic primary reinitialization | [W2 lifecycle](../evidence/r2-final-commits.json), database/lifecycle arm only | demonstrated |
| STO-7 | [05-persistence.md:169](https://github.com/douglaz/provisiond/blob/ac37c7cae5d3ff746515137774a4cdd5a3531375/05-persistence.md#L169) | `Manager::create`, `Session::check`; `check_scenarios`, `replication_negatives` | DB-05: Missing/duplicate designated walsender or insufficient statistics access | [W2 checks](../evidence/r2-final-checks.json), database arm only | demonstrated |
| CNF-56 | [10-conformance-checklist.md:432](https://github.com/douglaz/provisiond/blob/ac37c7cae5d3ff746515137774a4cdd5a3531375/10-conformance-checklist.md#L432) | `Manager::create`, `replication_negatives` | DB-05: Fresh settings/designated-name negatives | [W2 checks](../evidence/r2-final-checks.json), backup sender and separate replication-protocol negative | demonstrated |
| STO-7 | [05-persistence.md:169](https://github.com/douglaz/provisiond/blob/ac37c7cae5d3ff746515137774a4cdd5a3531375/05-persistence.md#L169) | `Session::destination`, `Session::check` | DB-06: Destination read follows the connection check's old target | [W2 checks](../evidence/r2-final-checks.json), database arm only | demonstrated |
| CNF-365 | [10-conformance-checklist.md:1250](https://github.com/douglaz/provisiond/blob/ac37c7cae5d3ff746515137774a4cdd5a3531375/10-conformance-checklist.md#L1250) | `Session::destination`, `Session::check` | DB-06: Destination read follows the connection check's old target | [W2 checks](../evidence/r2-final-checks.json), database arm only | demonstrated |
| STO-7 | [05-persistence.md:169](https://github.com/douglaz/provisiond/blob/ac37c7cae5d3ff746515137774a4cdd5a3531375/05-persistence.md#L169) | `Store::destination` | DB-07: Session lost after destination read | [W2 checks](../evidence/r2-final-checks.json), database arm only | demonstrated |
| CNF-365 | [10-conformance-checklist.md:1250](https://github.com/douglaz/provisiond/blob/ac37c7cae5d3ff746515137774a4cdd5a3531375/10-conformance-checklist.md#L1250) | `Store::destination` | DB-07: Session lost after destination read | [W2 checks](../evidence/r2-final-checks.json), database arm only | demonstrated |
| STO-7 | [05-persistence.md:169](https://github.com/douglaz/provisiond/blob/ac37c7cae5d3ff746515137774a4cdd5a3531375/05-persistence.md#L169) | `own_commit`, `Transport`, `commit_scenarios` | DB-08: Replication warning accompanies apparent command completion | [W2 lifecycle](../evidence/r2-final-commits.json), database/lifecycle arm only | demonstrated |
| CNF-365 | [10-conformance-checklist.md:1250](https://github.com/douglaz/provisiond/blob/ac37c7cae5d3ff746515137774a4cdd5a3531375/10-conformance-checklist.md#L1250) | `Manager::create`, `Session::check`, `Session::drop` | DB-09: Connection-check response withheld | [W2 checks](../evidence/r2-final-checks.json), database arm only | demonstrated |
| CNF-373 | [10-conformance-checklist.md:1228](https://github.com/douglaz/provisiond/blob/ac37c7cae5d3ff746515137774a4cdd5a3531375/10-conformance-checklist.md#L1228) | none | DB-10: Full connection capacity, including observer access | not run: W8 saturation work | not implemented |
| OPS-50 | [03-operation-lifecycle.md:2042](https://github.com/douglaz/provisiond/blob/ac37c7cae5d3ff746515137774a4cdd5a3531375/03-operation-lifecycle.md#L2042) | none | ENG-01: Old engine attempts a write after generation advance | none | not implemented |
| CNF-367 | [10-conformance-checklist.md:1859](https://github.com/douglaz/provisiond/blob/ac37c7cae5d3ff746515137774a4cdd5a3531375/10-conformance-checklist.md#L1859) | none | ENG-01: Old engine attempts a write after generation advance | none | not implemented |
| CNF-368 | [10-conformance-checklist.md:1864](https://github.com/douglaz/provisiond/blob/ac37c7cae5d3ff746515137774a4cdd5a3531375/10-conformance-checklist.md#L1864) | none | ENG-01: Old engine attempts a write after generation advance | none | not implemented |
| CNF-369 | [10-conformance-checklist.md:1870](https://github.com/douglaz/provisiond/blob/ac37c7cae5d3ff746515137774a4cdd5a3531375/10-conformance-checklist.md#L1870) | none | ENG-01: Old engine attempts a write after generation advance | none | not implemented |
| CNF-370 | [10-conformance-checklist.md:1883](https://github.com/douglaz/provisiond/blob/ac37c7cae5d3ff746515137774a4cdd5a3531375/10-conformance-checklist.md#L1883) | none | ENG-01: Old engine attempts a write after generation advance | none | not implemented |
| OPS-47 | [03-operation-lifecycle.md:135](https://github.com/douglaz/provisiond/blob/ac37c7cae5d3ff746515137774a4cdd5a3531375/03-operation-lifecycle.md#L135) | none | ENG-02: Startup-lock session lost | none | not implemented |
| CNF-366 | [10-conformance-checklist.md:1834](https://github.com/douglaz/provisiond/blob/ac37c7cae5d3ff746515137774a4cdd5a3531375/10-conformance-checklist.md#L1834) | none | ENG-02: Startup-lock session lost | none | not implemented |
| OPS-49 | [03-operation-lifecycle.md:1968](https://github.com/douglaz/provisiond/blob/ac37c7cae5d3ff746515137774a4cdd5a3531375/03-operation-lifecycle.md#L1968) | none | ENG-03: Claim acknowledgment lost | none | not implemented |
| STO-3 | [05-persistence.md:19](https://github.com/douglaz/provisiond/blob/ac37c7cae5d3ff746515137774a4cdd5a3531375/05-persistence.md#L19) | none | ENG-04: Same operation reclaimed before stale execution writes | none | not implemented |
| OPS-6 | [03-operation-lifecycle.md:177](https://github.com/douglaz/provisiond/blob/ac37c7cae5d3ff746515137774a4cdd5a3531375/03-operation-lifecycle.md#L177) | none | ENG-04: Same operation reclaimed before stale execution writes | none | not implemented |
| OPS-11 | [03-operation-lifecycle.md:241](https://github.com/douglaz/provisiond/blob/ac37c7cae5d3ff746515137774a4cdd5a3531375/03-operation-lifecycle.md#L241) | none | ENG-05: Provider accepted purchase, reply lost | none | not implemented |
| OPS-27 | [03-operation-lifecycle.md:556](https://github.com/douglaz/provisiond/blob/ac37c7cae5d3ff746515137774a4cdd5a3531375/03-operation-lifecycle.md#L556) | none | ENG-05: Provider accepted purchase, reply lost | none | not implemented |
| OPS-43 | [03-operation-lifecycle.md:764](https://github.com/douglaz/provisiond/blob/ac37c7cae5d3ff746515137774a4cdd5a3531375/03-operation-lifecycle.md#L764) | none | ENG-05: Provider accepted purchase, reply lost | none | not implemented |
| STO-3 | [05-persistence.md:19](https://github.com/douglaz/provisiond/blob/ac37c7cae5d3ff746515137774a4cdd5a3531375/05-persistence.md#L19) | none | ENG-06: Settlement reply lost | none | not implemented |
| STO-23 | [05-persistence.md:999](https://github.com/douglaz/provisiond/blob/ac37c7cae5d3ff746515137774a4cdd5a3531375/05-persistence.md#L999) | none | ENG-06: Settlement reply lost | none | not implemented |
| OPS-49 | [03-operation-lifecycle.md:1968](https://github.com/douglaz/provisiond/blob/ac37c7cae5d3ff746515137774a4cdd5a3531375/03-operation-lifecycle.md#L1968) | none | ENG-06: Settlement reply lost | none | not implemented |
| OPS-21 | [03-operation-lifecycle.md:1836](https://github.com/douglaz/provisiond/blob/ac37c7cae5d3ff746515137774a4cdd5a3531375/03-operation-lifecycle.md#L1836) | none | ENG-07: Stop-record deadline expires during replication freeze | none | not implemented |
| CNF-298 | [10-conformance-checklist.md:1206](https://github.com/douglaz/provisiond/blob/ac37c7cae5d3ff746515137774a4cdd5a3531375/10-conformance-checklist.md#L1206) | none | ENG-08: External call while a store transaction is open | none | not implemented |
| STO-30 | [05-persistence.md:1107](https://github.com/douglaz/provisiond/blob/ac37c7cae5d3ff746515137774a4cdd5a3531375/05-persistence.md#L1107) | none | MONEY-01: Duplicate rail settlement | none | not implemented |
| STO-31 | [05-persistence.md:1114](https://github.com/douglaz/provisiond/blob/ac37c7cae5d3ff746515137774a4cdd5a3531375/05-persistence.md#L1114) | none | MONEY-01: Duplicate rail settlement | none | not implemented |
| API-45 | [04-api-contract.md:888](https://github.com/douglaz/provisiond/blob/ac37c7cae5d3ff746515137774a4cdd5a3531375/04-api-contract.md#L888) | none | MONEY-02: Funding request replay | none | not implemented |
| STO-7 | [05-persistence.md:169](https://github.com/douglaz/provisiond/blob/ac37c7cae5d3ff746515137774a4cdd5a3531375/05-persistence.md#L169) | none | MONEY-02: Funding request replay | none | not implemented |
| STO-27 | [05-persistence.md:1003](https://github.com/douglaz/provisiond/blob/ac37c7cae5d3ff746515137774a4cdd5a3531375/05-persistence.md#L1003) | none | MONEY-03: Concurrent admission against one available balance | none | not implemented |
| LDG-35 | [12-billing-and-ledger.md:431](https://github.com/douglaz/provisiond/blob/ac37c7cae5d3ff746515137774a4cdd5a3531375/12-billing-and-ledger.md#L431) | none | MONEY-03: Concurrent admission against one available balance | none | not implemented |
| CNF-299 | [10-conformance-checklist.md:1216](https://github.com/douglaz/provisiond/blob/ac37c7cae5d3ff746515137774a4cdd5a3531375/10-conformance-checklist.md#L1216) | none | MONEY-04: Pool checkout refused | none | not implemented |
| STO-30 | [05-persistence.md:1107](https://github.com/douglaz/provisiond/blob/ac37c7cae5d3ff746515137774a4cdd5a3531375/05-persistence.md#L1107) | none | MONEY-05: One deposit paid over both rails | none | not implemented |
| STO-31 | [05-persistence.md:1114](https://github.com/douglaz/provisiond/blob/ac37c7cae5d3ff746515137774a4cdd5a3531375/05-persistence.md#L1114) | none | MONEY-05: One deposit paid over both rails | none | not implemented |
| LDG-55 | [12-billing-and-ledger.md:1671](https://github.com/douglaz/provisiond/blob/ac37c7cae5d3ff746515137774a4cdd5a3531375/12-billing-and-ledger.md#L1671) | none | MONEY-05: One deposit paid over both rails | none | not implemented |
| CNF-365 | [10-conformance-checklist.md:1250](https://github.com/douglaz/provisiond/blob/ac37c7cae5d3ff746515137774a4cdd5a3531375/10-conformance-checklist.md#L1250) | none | REC-01: Lossless operator promotion | none | not implemented |
| CNF-365 | [10-conformance-checklist.md:1250](https://github.com/douglaz/provisiond/blob/ac37c7cae5d3ff746515137774a4cdd5a3531375/10-conformance-checklist.md#L1250) | none | REC-02: Promoted primary has no replacement standby | none | not implemented |
| STO-7 | [05-persistence.md:169](https://github.com/douglaz/provisiond/blob/ac37c7cae5d3ff746515137774a4cdd5a3531375/05-persistence.md#L169) | `Harness.backup`, `Harness.identity`, `Harness.drill` | REC-03: Bootstrap without an acknowledgment partner | [W1](../evidence/r2-final-w1.json), bootstrap arm only; product recovery absent | demonstrated |
| CNF-365 | [10-conformance-checklist.md:1250](https://github.com/douglaz/provisiond/blob/ac37c7cae5d3ff746515137774a4cdd5a3531375/10-conformance-checklist.md#L1250) | `Harness.backup`, `Harness.identity`, `Harness.drill` | REC-03: Bootstrap without an acknowledgment partner | [W1](../evidence/r2-final-w1.json), bootstrap arm only; product recovery absent | demonstrated |
| STO-56 | [05-persistence.md:1504](https://github.com/douglaz/provisiond/blob/ac37c7cae5d3ff746515137774a4cdd5a3531375/05-persistence.md#L1504) | none | REC-04: Process crashes during nonzero incident progress | none | not implemented |
| CNF-302 | [10-conformance-checklist.md:3248](https://github.com/douglaz/provisiond/blob/ac37c7cae5d3ff746515137774a4cdd5a3531375/10-conformance-checklist.md#L3248) | none | REC-04: Process crashes during nonzero incident progress | none | not implemented |
| CNF-365 | [10-conformance-checklist.md:1250](https://github.com/douglaz/provisiond/blob/ac37c7cae5d3ff746515137774a4cdd5a3531375/10-conformance-checklist.md#L1250) | none | REC-04: Process crashes during nonzero incident progress | none | not implemented |
| STO-56 | [05-persistence.md:1504](https://github.com/douglaz/provisiond/blob/ac37c7cae5d3ff746515137774a4cdd5a3531375/05-persistence.md#L1504) | none | REC-05: New promotion while an incident is open | none | not implemented |
| CNF-365 | [10-conformance-checklist.md:1250](https://github.com/douglaz/provisiond/blob/ac37c7cae5d3ff746515137774a4cdd5a3531375/10-conformance-checklist.md#L1250) | none | REC-05: New promotion while an incident is open | none | not implemented |
| STO-54 | [05-persistence.md:1689](https://github.com/douglaz/provisiond/blob/ac37c7cae5d3ff746515137774a4cdd5a3531375/05-persistence.md#L1689) | none | REC-06: Unknown possible-loss endpoint | none | not implemented |
| CNF-365 | [10-conformance-checklist.md:1250](https://github.com/douglaz/provisiond/blob/ac37c7cae5d3ff746515137774a4cdd5a3531375/10-conformance-checklist.md#L1250) | none | REC-06: Unknown possible-loss endpoint | none | not implemented |
| CNF-297 | [10-conformance-checklist.md:1199](https://github.com/douglaz/provisiond/blob/ac37c7cae5d3ff746515137774a4cdd5a3531375/10-conformance-checklist.md#L1199) | none | REC-07: Restore removes allocated deposit rows | none | not implemented |
| STO-12 | [05-persistence.md:1581](https://github.com/douglaz/provisiond/blob/ac37c7cae5d3ff746515137774a4cdd5a3531375/05-persistence.md#L1581) | none | REL-01: Migration interrupted or reconnect refused | none | not implemented |
| STO-13 | [05-persistence.md:1604](https://github.com/douglaz/provisiond/blob/ac37c7cae5d3ff746515137774a4cdd5a3531375/05-persistence.md#L1604) | none | REL-01: Migration interrupted or reconnect refused | none | not implemented |
| CNF-57 | [10-conformance-checklist.md:438](https://github.com/douglaz/provisiond/blob/ac37c7cae5d3ff746515137774a4cdd5a3531375/10-conformance-checklist.md#L438) | none | REL-01: Migration interrupted or reconnect refused | none | not implemented |
| STO-13 | [05-persistence.md:1604](https://github.com/douglaz/provisiond/blob/ac37c7cae5d3ff746515137774a4cdd5a3531375/05-persistence.md#L1604) | none | REL-02: Old binary does not support current schema/semantics | none | not implemented |
| CNF-57 | [10-conformance-checklist.md:438](https://github.com/douglaz/provisiond/blob/ac37c7cae5d3ff746515137774a4cdd5a3531375/10-conformance-checklist.md#L438) | none | REL-02: Old binary does not support current schema/semantics | none | not implemented |

## Additional W2 database fixtures

| Requirement | Pinned source | Implementation symbol | Test ID | Evidence | Status |
|---|---|---|---|---|---|
| STO-7 | [05-persistence.md:169](https://github.com/douglaz/provisiond/blob/ac37c7cae5d3ff746515137774a4cdd5a3531375/05-persistence.md#L169) | `own_commit`, `Transport` | Lost COMMIT reply | [W2 lifecycle](../evidence/r2-final-commits.json) | demonstrated |
| OPS-49 | [03-operation-lifecycle.md:1968](https://github.com/douglaz/provisiond/blob/ac37c7cae5d3ff746515137774a4cdd5a3531375/03-operation-lifecycle.md#L1968) | `CommitOutcome`, `own_commit` | Lost COMMIT reply, uncertainty classification only | [W2 lifecycle](../evidence/r2-final-commits.json); engine retry/claim policy absent | demonstrated |
| OPS-21 | [03-operation-lifecycle.md:1836](https://github.com/douglaz/provisiond/blob/ac37c7cae5d3ff746515137774a4cdd5a3531375/03-operation-lifecycle.md#L1836) | `Process::shutdown`, `residual_and_stop` | Bounded lifecycle subprocess | [W2 lifecycle](../evidence/r2-final-commits.json); no product Stop-record/ENG-07 claim | demonstrated |
| STO-7 | [05-persistence.md:169](https://github.com/douglaz/provisiond/blob/ac37c7cae5d3ff746515137774a4cdd5a3531375/05-persistence.md#L169) | `Session::close`, `Session::query`, `Transaction::execute` | Retained session/transaction pre-COMMIT deadline closure | [Fresh regressions](../evidence/r2-final-regressions.json), [red/green](../evidence/r2.md) | demonstrated |
| STO-7 | [05-persistence.md:169](https://github.com/douglaz/provisiond/blob/ac37c7cae5d3ff746515137774a4cdd5a3531375/05-persistence.md#L169) | `Process::run`, `Manager::create`, `own_commit` | Initiating runtime destroyed after observed SyncRep; constructor cancellation/deadline | [Fresh regressions](../evidence/r2-final-regressions.json), [red/green](../evidence/r2.md); database ownership only | demonstrated |
| STO-55 | [05-persistence.md:291](https://github.com/douglaz/provisiond/blob/ac37c7cae5d3ff746515137774a4cdd5a3531375/05-persistence.md#L291) | `Process::run`, `Store::write_checked_out`, `own_commit` | Original checkout retained across initiating runtime destruction | [Fresh regressions](../evidence/r2-final-regressions.json); no product concurrency/saturation claim | demonstrated |

## Operating-assumption anchors without a planned test yet

These requirements own constraints recorded in `docs/scope.md`. No planned test ID covers them yet;
a later work package adds one or records why a deployment inspection is the only evidence.

| Requirement | Pinned source | Implementation symbol | Test ID | Evidence | Status |
|---|---|---|---|---|---|
| OVR-19 | [00-overview.md:366](https://github.com/douglaz/provisiond/blob/ac37c7cae5d3ff746515137774a4cdd5a3531375/00-overview.md#L366) | none | none planned | none | not implemented |
| STO-59 | [05-persistence.md:113](https://github.com/douglaz/provisiond/blob/ac37c7cae5d3ff746515137774a4cdd5a3531375/05-persistence.md#L113) | none | none planned | none | not implemented |

## Evidence that exists elsewhere and is not implementation evidence

- **Historical SQL experiments** ([pgharness results](https://github.com/douglaz/provisiond/blob/ac37c7cae5d3ff746515137774a4cdd5a3531375/docs/research/panels-2026-10/r63/pgharness/results.md)) ran a primary with two standbys under
  `ANY 1`. They are dated evidence about PostgreSQL behaviour in that topology. They are not a passing
  test of this repository and do not validate the one-designated-standby profile.
- **Lean proofs** in the specification repository's formal layer establish properties of their models
  under the models' stated assumptions. A theorem about a durable WAL prefix does not establish
  authenticated standby identity, driver cancellation behaviour, deployment fencing, or provider
  semantics, and is never recorded here as a passing implementation test.
