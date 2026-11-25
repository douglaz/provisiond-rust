# W7 recovery setup checkout deadline: bounded investigation

The underlying cause is **inconclusive**. The retained record directly identifies admission polling as the stage present at the deadline. No operational correction is justified by this one iteration; pvr-kd7 stays open at P2. This is an investigation of a local test setup failure, with no new product or conformance claim.

The single investigation ran from 2026-11-25T04:26:40Z to 04:42:50.938111Z, 970.938111 seconds (16 minutes 10.938 seconds), within its two-hour limit. The fixed eight-case matrix completed; no second pass with new hypotheses was started. Review, publication and the quick gate are outside that interval. [The manifest](investigation.json) binds the selected retained records, source maps, native focused results, commands, machine endpoints and cleanup.

## What the retained failure shows

The first iteration on 70d86db89da633e17cbfc28d44dd7ec41f27009b exited 1 in 554.955759231 seconds. Its W7 child w7_markers exited 1, with 49.179529935 seconds parent-measured child wall time. The child failure event was at 30.446980330 seconds; child cleanup passed at 31.447820528 seconds, with no errors. Parent W7 cleanup also passed. The later iteration exited 0 in 509.967890345 seconds on the same source, but used documented port base25500 instead of25900; it is not a controlled explanation of this timing failure.

During the second promotion/bootstrap/establish sequence, the replacement standby identity check saw primary-reported flush 0/9000000 at 30.160002410 seconds and verified the replacement at 30.179017475 seconds. A subsequent fresh recovery command had completed physical-session authentication and settings before admission. Its diagnostic trace records admission target 0/9001050, then six samples of the designated streaming/synchronous standby at 0/9000000, with no session_admitted or schema_compatible event. The error was “whole pool checkout deadline (waiting/create/admission): deadline has elapsed”. The previous identity barrier sampled an earlier target; it did not guarantee this next target would already be reported.

Wire records place the admission target send at 30.224930233 seconds and the first connection close at 30.433643936 seconds: 208.714 ms spanning admission polling. The outer checkout bound was250 ms; the separate check bound was3000 ms. There is no retained exact checkout-start timestamp, so the record does not allocate every millisecond. Two sampled query round trips also took about39 and35 ms; this is an observation, not an established scheduling cause.

This was the first schema acquisition in the fresh recovery process (`src/bin/recovery.rs`, Store construction and compatible_schema_for before the action dispatch). `Store::checkout` applies the whole bound around pool.get; physical creation configures the session, reads its identity, then calls check("admission") before emitting session_admitted (`src/store/mod.rs`). A connection existed and admission was reached, which rules out an entirely absent physical connection and ordinary full-pool slot ownership for this first acquisition. It does not rule out time spent in earlier creation/setup or scheduling, and does not explain why the reported prefix stayed behind. The raw diagnostic trace and harness failure retain the error. Separate recovery-296 stdout/stderr files were not written after the nonzero command; pvr-rt5 tracks that evidence-retention limitation.

## Fixed focused runs

The original source70d86db and current master9215105946affb2a48566755f47ebc813564e244 have exactly the same110 operational file hashes, including the runtime, harness, tests, tools, migrations, deployment and spec lock. Each detached clean source was built once through the native suite invocation builder, with hooks and default binaries, then reused for four fresh native cases. Each source ran two cases without added load and two with four owned CPU workers; all workers were stopped and reaped. Both builds exited0. Total source-group times, including builds, were134.178397112 and116.323944452 seconds; build/setup times were41.695506160 and42.471058369 seconds.

The case command was `python3 -B tests/postgres/harness.py --phase w7 --case w7_markers --jobs 1 --seed 20261124 --evidence <owned-output>`, through `nix develop` and the suite's invocation builder. Port base25500, one primary and one designated synchronous standby, isolated directories and local simulators were used. Each case had a180-second bound, each group a1200-second bound. No case was retried. The native scenario/assertion results, source identities, manifest/output hashes and cleanup are retained in the manifest; a missing planned event was never treated as a pass.

| Source cohort | Case | Owned CPU workers | Exit / result | Seconds | Cleanup |
|---|---:|---:|---|---:|---|
| original | 1 | 0 | 0 / pass | 21.167710 | pass |
| original | 2 | 0 | 0 / pass | 24.675598 | pass |
| original | 3 | 4 | 0 / pass | 24.174774 | pass |
| original | 4 | 4 | 0 / pass | 22.217496 | pass |
| current | 1 | 0 | 0 / pass | 18.857309 | pass |
| current | 2 | 0 | 0 / pass | 18.307173 | pass |
| current | 3 | 4 | 0 / pass | 18.910102 | pass |
| current | 4 | 4 | 0 / pass | 17.503874 | pass |

All eight cases passed and all eight cleanups passed. These are four cases without added load and four with controlled load, not a statistical guarantee. Both cohorts run identical operational code. The serial focused profile does not reproduce the original concurrent whole iteration or its25900 port selection. It cannot rule out rare timing, concurrent phase interactions, shared-host scheduling/I/O or replication/reporting delay. Load averages, MemAvailable and free disk were recorded before and after every case and after owned load stopped. Those readings are endpoint observations, not a cause.

## Comparison with pvr-wlm

The retained canonical-a1 core probe158 (engine start before the startup lock) shows admission target0/3B9A000 and nine primary-reported observations0/3B99FE8, then the same whole-checkout deadline. No startup-lock or generation-advance event was observed for that probe. Its parent core cleanup verdict remains **fail**, recording the unexpected process exit71; it is not relabelled as a pass. These records directly show a shared admission-stage pattern. A shared underlying cause is only a candidate, not established evidence. No pvr-wlm run or correction is included here.

## Separate findings and bounded next evidence

The harness writes `wal_receiver_status_interval = '100ms'`. In both frozen environments the PostgreSQL18.6 configuration parser, invoked as `postgres -D <owned-empty-directory> -C wal_receiver_status_interval -c config_file=/dev/null -c wal_receiver_status_interval=100ms`, exited0 and printed0; the same command with1s exited0 and printed1. This parses configuration without starting another database; it is not a historical running-standby SHOW observation. The release source declares integer-second units and skips unforced reports for a non-positive interval. That makes the setting a plausible reporting-delay candidate, not a demonstrated explanation of the retained deadline. PostgreSQL primary references: [parameter documentation](https://www.postgresql.org/docs/18/runtime-config-replication.html), [18.6 setting declaration](https://raw.githubusercontent.com/postgres/postgres/REL_18_6/src/backend/utils/misc/guc_tables.c), and [18.6 reporting implementation](https://raw.githubusercontent.com/postgres/postgres/REL_18_6/src/backend/replication/walreceiver.c). No simultaneous standby-local position or zero-versus-positive live comparison was captured. P2 pvr-0jb tracks the configuration discrepancy; it is not corrected here. P3 pvr-rt5 tracks standalone command-stream retention; it is not corrected here. Neither finding establishes a product defect or a false pass.

Next evidence, not run or authorized as another iteration here: one20-minute group containing four matched fresh-cluster pairs on one frozen source, current zero-effective setting versus a positive whole-second setting. Record the running standby's effective interval, standby-local receive/flush progress, primary-reported progress and acquisition/admission boundaries. A difference must distinguish local lag from delayed reporting before a cause or correction is claimed. Preserve all production bounds, assertions and the accepted profile; if the discriminating condition is absent, stop inconclusive. pvr-kd7 remains open.

Only documents, evidence and tracker records change. Under the owner's suite split for this task, the actual-tip `nix develop --command bash tools/check.sh` is the required publication gate; no iteration or full canonical suite is required or claimed for this metadata change. No historical failed result, pvr-wlm, pvr-aqm, W5 observer tooling or requirement coverage is changed. This branch is independent of open PR14; their tracker and scope edits will need reconciliation after it lands.
