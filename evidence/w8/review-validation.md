# Supplemental release-inventory validation

This supplements the [earlier W8 report](report.md). The earlier canonical failure and its separate W5 pass remain unchanged in [canonical-final.json](canonical-final.json). References to that earlier final canonical in the original publication describe that recorded run. The new invocation below records validation after preserving the complete application-login inventory at the migration boundary.

The runtime remains byte-identical to `aa1ad12d1ff2b9085ee2c11b84730dffbc32ff61`. The previous 104 operational inputs are unchanged. The new 106-file map additionally binds `docs/release.md`, which the new regression executes, and `tests/postgres/test_release_runbook.py`. Full commands, versions, source hashes, statuses and retained private-artifact hashes are in [the supplemental manifest](review-validation.json).

| Check | Observed result |
| --- | --- |
| New iteration, source `0465516c53e7c520a4eb4117154f90e8c610544c` | pass; exit 0; 734.764 s |
| New canonical, source `5442e9cf7f5878b394e64b56b43dc6d9490f504b` | pass; exit 0; 1758.960 s |
| Canonical phases | 12 of 12 phase processes exited 0 |
| Explicit canonical result rows | 280; 2 expected negative demonstration, 5 not run, 273 pass |
| Repository gate | pass; exit 0 |
| Python units | pass; exit 0; 51 test methods |
| Inventory regression against original document | expected negative demonstration; exit 1; six intended multi-role assertions |
| Inventory regression against corrected document | pass; exit 0; one test method with fifteen subcases |
| Focused real PostgreSQL probe | pass; exit 0; owned primary and designated synchronous standby |
| Repair and branch reviews | no introduced P0/P1 remains; review is separate from executed validation |

The inventory regression extracts the actual step-1 and step-5 shell blocks and runs them through `sh -c`. Only the migration executable is replaced by a capture stub. It verifies both the single-login local default and an explicitly selected `app,worker` inventory, including stale role settings in the sourced migrator configuration. The old document forwards only `app`; the corrected document preserves the selected inventory across configuration sourcing.

The real migration probe tested the independent pre-DDL interlock with manual prerequisites deliberately unsatisfied. With a worker login still enabled, and separately with one surviving worker session despite NOLOGIN, the original command exits 0 and completes migration. The corrected command exits 1 with the specific worker prerequisite diagnostic before creating migration history. The same surviving worker session can write the app-writable fixture after the original invocation. This does not show a survivor arising after every manual stop, login-disable and drain prerequisite was correctly completed. The all-offline positive case retains an already-completed five-version history; it does not demonstrate fresh-schema positive migration through the corrected command.

All recorded ordinary and negative-child cleanup checks passed. Probe servers exited 0; its owned data and private credential files were removed and ports released. Raw streams, configuration, database files and operational identifiers remain private; only selected hashes and sanitized summaries are published.

Remaining review limits are deferred in `pvr-rrn` (login restoration ordering), `pvr-34b` (housekeeping elapsed-time lower bound), `pvr-alp` (capacity transitions and observer-refusal attribution), `pvr-ajb` (missing runbook negative cases), `pvr-qex` (incident acknowledgement wait) and `pvr-0b0` (attachment-conflict availability). These are follow-ups, without bundled P2/P3 fixes. The targeted post-admission and successful paced-enrolment verification in `pvr-kin` remains not run; the remaining old reconnect race in `pvr-31s` remains open. Unmeasured retry behavior is outside the measured local parameter subset. Production tuning, primary shutdown fallback and live D17/D18 results remain outside the demonstrated claims.
