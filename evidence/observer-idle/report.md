# W5 recorded-observer idle wait

The reconnect setup now waits for the recorded observer to become idle before ending that database connection. Both `reconnect` and `failure_run` call `terminate_idle_observer`; saturation still delegates to `failure_run`. The single shared SQL predicate retains the original PID, `state='idle'`, the separate non-`SyncRep` condition and the required true termination result. A replacement PID is never rediscovered for termination. Product code, production bounds and every scenario assertion are unchanged.

This closes pvr-31s on this branch as P1 under the owner clause “a test or gate that cannot fail or fails for the wrong reason”: the one-shot setup assumption could reject a correct run before its planned connection loss. This is a test-tooling correction, not a demonstrated product defect or a new conformance claim. The W8 evidence and its partial repair remain historical records.

## Baseline and live acceptance

Three native `w5_accounting_reconnect` runs on unchanged master `dc772365e46f8160caaccdff691cc31d28383c75` all exited 0 and passed cleanup. The old failure was **not reproduced** within the cheap bound. Build time was 49.270 s, case execution 27.967 s, whole invocation 77.525 s. No live red run or causal explanation of which activity overlapped the idle check is claimed.

After the code commit `d5d67a4b7197768aad8011d16e2330c09383fd28`, five fresh native executions of the same case all exited 0. Each preserved the existing assertions for actual planned observer loss, a different checked replacement, a later real COMMIT and cleanup without errors. The five-case invocation exited 0 in 76.033 s including 37.261 s of build setup and 38.473 s of case execution. This fits the stated 900 s bound and repeats the complete affected journey five times; it is a bounded acceptance sample, not a statistical guarantee.

Each child used the suite's normal `runner.invocation` builds and `python3 tests/postgres/harness.py --phase w5 --case w5_accounting_reconnect --jobs 1 --seed 20261125 --evidence <case.json>` against isolated local primary/standby PostgreSQL and simulators, on port base 25500. Native manifests, exact commands, versions, exit codes, source hashes, original/replacement PIDs, activation and cleanup fields, and raw stream SHA-256 bindings are in [live.json](live.json). Raw logs remain private.

The helper still uses the existing `eventually` defaults: a 20 s polling timeout and 50 ms interval. Its final SQL call retains the existing separate query limit; no hard 20 s PostgreSQL wall-clock claim is made. After live acceptance, a separate controlled SQL-result check exercised empty-then-true (two calls, same PID/guards) and never-idle (expected `AssertionError`, observer PID and original timeout cause retained). It exited 0 in 20.107 s; the never-idle arm took 20.048 s. This is helper sensitivity, **not** a live PostgreSQL run or a reproduced baseline failure. A planned connection loss that never occurs remains a failure.

## Restored suites

`nix develop --command bash tools/iteration.sh --jobs 3 --seed 20261124 --evidence <iteration.json>` exited 0 in 542.516 s on the clean code commit and port base 25500. The fast gate and 58 Python unit checks exited 0; W7 and controls passed. All 44 negative controls and five candidate regressions were recognized, including `w6_non_exact` in this invocation. This observation does not correct or close pvr-aqm.

The **one** complete serial `nix develop --command bash tools/postgres.sh --jobs 1 --seed 20261124 --evidence <canonical.json>` invocation on that same frozen source exited 0 in 1935.537 s. No canonical or timing-class rerun was used.

| Phase | Native result | Exit | Seconds |
|---|---|---:|---:|
| w1 | pass | 0 | 8.068 |
| checks | pass | 0 | 31.540 |
| commits | pass | 0 | 17.915 |
| regressions | pass | 0 | 12.379 |
| corrections | pass | 0 | 9.275 |
| core | pass | 0 | 64.006 |
| workflow | pass | 0 | 231.769 |
| default-workflow | pass | 0 | 6.732 |
| w5 | pass | 0 | 94.598 |
| w6 | pass | 0 | 580.128 |
| w7 | pass | 0 | 314.488 |
| controls | pass | 0 | 471.828 |

All twelve phase processes passed. This single observation does not establish a cause or a correction for older intermittent failures.

The w6_non_exact child produced its intended negative failure at `exact-correlator/no-attachment`, and its cleanup passed; the parent recognized that negative demonstration.

[validation.json](validation.json) retains each phase result, counts, non-pass rows, all control-child results and SHA-256 bindings, cleanup verdicts, commands and source identity. Machine endpoints are observations only, not a cause. No missing or unactivated case is relabelled pass. pvr-aqm and pvr-wlm belong to the separate startup change; pvr-kd7 was not investigated here.

## Scope and follow-ups

The operational source map binds the clean tested code commit to the publication checkout. Its only changed operational file from master is `tests/postgres/w5_accounting.py`; documents, evidence and tracker are published afterwards. All observer termination call sites in that file use the helper directly or through saturation's delegation. No runtime, migration, deployment, production retry, accepted profile or pinned requirement status changes.

Three initial P3 items remain open with label `review-nit`: pvr-9un (distinguish an immediate SQL error from an idle-timeout diagnostic), pvr-xyn (establish what activity can overlap reconnect, or retain inconclusive cause), and pvr-nyr (direct session correlation in the observer-loss event). Their current diagnostics/evidence fail closed or remain auditable; they do not leave this acceptance unmet and are not corrected here.

This branch starts independently from master dc77236, not from startup PR #13. Its tracker and scope edits will need reconciliation/rebase after that change lands. No merge is authorized by this evidence.

The final review added P3 pvr-1p1 (supplemental helper-check source and overlap traceability) and pvr-hs3 (navigation to independent tracker references), both open with label `review-nit`. The path-alias export nit is covered by existing P2 pvr-xbh. None is corrected in this change.
