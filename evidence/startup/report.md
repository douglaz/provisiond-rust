# Local startup connection recovery

Result: **pass**, first live attempt `startup-a1`, driver exit 0 in
8.655418857 seconds including verified cleanup; launcher exit 0.
The [manifest](live.json) binds the actual retained private result, actions, process records,
native logs, engine trace, simulator state and build/install records by SHA-256 and byte count.
Raw logs stay private. No product defect was established and no runtime change was needed.

## Commands and identities

The recorded build command was:

```
nix develop /var/tmp/provisiond-manager/fastlane-startup/build-source --command cargo build --release --locked --bins
```

It exited 0 in 60.203742645 seconds, finishing 2026-11-24T19:56:24.916525+00:00.
Source was clean detached default master `dc772365e46f8160caaccdff691cc31d28383c75`, tree `a130f7a1f892fdbcc2ec41d21ea02e9c41ea87f0`,
with default features. The installed manifest supplies the single complete 43-file runtime
map in `live.json`, digest `1540583e4a967d4e257f189bfb1e8cb170e7d7247a87bf31a8ae7bfcb4b2ec39` (sorted compact JSON),
and all four installed binary hashes. Installed engine SHA-256 is
`ebe9a1b8502cbb04b868a3eb01f08e88c693a74bdb7f320df55320c896e78ab0`. The private driver repository
`provisiond-live-drivers` was at `ba722c9731abc25a519b823fb930260d45aaecae`.

The recorded live command was:

```
nix develop /var/tmp/provisiond-manager/fastlane-startup/build-source --command python3 -I /var/tmp/provisiond-manager/fastlane-w8/live/startup.py --commit dc772365e46f8160caaccdff691cc31d28383c75 --source-dir /var/tmp/provisiond-manager/fastlane-startup/build-source --install-dir /var/tmp/provisiond-manager/fastlane-startup/builds/master-dc/install --run-dir /var/tmp/provisiond-manager/fastlane-startup/runs/startup-a1
```

`live.json` also records the exact `launch-unit --kind live` wrapper with outer `timeout 1260`.
The driver limit was 1200 seconds including cleanup. Launcher elapsed time was not separately
recorded; 8.655418857 seconds is the driver's measured duration.
Build versions were PostgreSQL 18.6, Python 3.14.7,
rustc 1.97.1 (8bab26f4f, 2026-07-14; built from a source tarball), and
cargo 1.97.0 (c980f4866, 2026-06-30); version collection exited 0.
Source, installed binary, build/install manifest and driver input identities matched again
at final-inputs. The recorded build and live run preceded publication; required suite results are recorded below.

## Observed startup boundary

This arm observes `Engine::begin_startup`'s existing local startup policy from
[D-70](../../docs/decisions.md#d-70-offline-release-declarations-and-bounded-local-saturation-policy),
recorded in [D-72](../../docs/decisions.md#d-72-observed-startup-recovery-after-a-short-refusal-of-new-connections).
It reuses the existing [OPS-49](https://github.com/douglaz/provisiond/blob/ac37c7cae5d3ff746515137774a4cdd5a3531375/03-operation-lifecycle.md#L1968)
retry bound; this startup observation does not demonstrate OPS-49's worker/periodic store-call retry
mandate. The policy remains within unchanged [OPS-47](https://github.com/douglaz/provisiond/blob/ac37c7cae5d3ff746515137774a4cdd5a3531375/03-operation-lifecycle.md#L135)
and [STO-55](https://github.com/douglaz/provisiond/blob/ac37c7cae5d3ff746515137774a4cdd5a3531375/05-persistence.md#L291)
constraints. An ordinary preopened operator transaction held the generation row until the
engine held its startup lock and opened its dedicated generation session. A planned short
loss of new application connections used PostgreSQL role `CONNECTION LIMIT 0`; existing
sessions stayed open. Releasing the row let the dedicated generation transaction commit.
The cold ordinary write pool then could not obtain a new connection and startup retried
within the existing 2000 ms Store-retry bound, with 10 ms backoff. The startup wait remains
500 ms and checkout bound 250 ms. No sent COMMIT was abandoned or treated as rolled back.

| Native PostgreSQL log time (2026-11-24 UTC) | Observation |
|---|---|
| 20:14:04.464 | Dedicated generation backend 1606928 completed `COMMIT /* store COMMIT */`. |
| 20:14:04.467 | Backend 1606934: `FATAL: 53300: too many connections for role "app"`. |
| 20:14:04.481 | Backend 1606935: the same native refusal. |
| 20:14:04.533 | Backend 1606946 read provider configuration. |
| 20:14:04.544 | That backend completed the configuration COMMIT. |
| 20:14:04.586–20:14:05.611 | 53 ordinary claim polls, server span 1.025000095 seconds. |

The failed connections had application name `[unknown]` at this early boundary. Attribution
rests on the fresh owned database, the engine being the sole `app` actor, native role refusal
after the dedicated generation COMMIT, and engine retries before the restart pass. No API
was started. The failed checkout sent no business COMMIT. Startup-lock backend 1606926,
observer 1606927 and generation 1606928 were distinct; lock and observer remained present
after the refusal.

The retained trace has exactly one `generation_advanced`, then `startup_checkout_retry`
with spent values 2 and 16 ms, then exactly one `restart_pass_complete` (`running_found=0`).
Selected startup trace entries and the acknowledged generation COMMIT event appear in
`live.json`. Trace `observed_epoch`/`observed_monotonic` are driver receipt times, not product
emission times; stream indexes preserve order. The native table uses server log timestamps.
The secondary configuration/quarantine acknowledgement is not independently attributed here;
the native configuration SELECT followed by COMMIT on the same backend supplies that proof.

The original role limit -1 was restored exactly after 47.515952 ms of restriction and
16.109659 ms after the first observed refusal. Later control checks found limit -1 and
the generation-row owner idle. These intervals use driver monotonic observations and do not
measure outage duration from server emission. No second generation advance was needed.

## Same process, polling and cleanup

Before the fault, after recovery and after the polling window, engine PID was 1606891,
process start time 239292593, InvocationID `d114c7a61daa4628bf50baf107ba864d`,
NRestarts 0 and Restart `no`. Observation lasted 1.089068815 seconds; the 53 native polls
spanned 1.025000095 seconds. Both simulators recorded zero requests, empty account/resource
state and zero purchases/payments. Database operations, providers and tenants stayed zero.
No business row, provider call, purchase or real funds were driven.

Cleanup recorded no errors or surviving child PIDs, stopped all three owned units and both
owned PostgreSQL nodes, and removed the run's data. Shutdown at cleanup does not demonstrate
the primary-shutdown saturation fallback. This is the engine-only local startup arm in an
empty database: no repeat-provider proof, production tuning, general availability or outage
guarantee follows. No whole-requirement claim for OPS-47/49/21 follows. Primary shutdown
fallback and other existing limits remain unchanged. Historical W8 and pvr-kin manifests,
identities and failures remain intact; new suite results are recorded below and in validation.json.

## Required suite validation

The [required validation](validation.json) ran against clean master dc772365, whose runtime, test, tool, migration and deployment bytes match this publication. The first iteration passed, exit 0 / 546.696320233 seconds, at port base 25500 with no rerun. The one serial canonical invocation failed, exit 1 / 2044.788749327 seconds: all twelve phase processes completed, ten passed; core and controls failed. Core stopped before its planned generation-reply check. Its engine_start probe met a pre-lock checkout deadline; the later planned reply event never occurred. Core also recorded a failed cleanup verdict for the unexpected exit 71. The intended check and later core arms are not run, never pass; P2 pvr-wlm tracks the fixture investigation. Controls rejected w6_non_exact at a different assertion from its required no-attachment assertion, with passing child cleanup; P2 pvr-aqm tracks that investigation. W5 and W6 passed in this invocation. No canonical rerun, product correction or pvr-31s change is claimed.

Commands ran in the clean detached tested source, with `PROVISIOND_HARNESS_PORT_BASE=25500`; the final branch changes only documents, evidence and tracker records. The validation manifest binds the tested source and all unchanged operational bytes separately from publication metadata.

| Command | Exit | Wall seconds |
|---|---:|---:|
| `nix develop --command bash tools/iteration.sh --jobs 3 --seed 20261124 --evidence <private-evidence>/validation/iteration-a1/iteration.json` | 0 | 546.696320233 |
| `nix develop --command bash tools/postgres.sh --jobs 1 --seed 20261124 --evidence <private-evidence>/validation/canonical-a1/canonical.json` | 1 | 2044.788749327 |

Recorded non-control scenario rows: 263 pass, 2 expected negative demonstration, 2 not run, 1 fail. These are recorded rows, not a count of unexecuted later core arms. Full stdout/stderr, original native failed verdicts and child manifests remain retained and hash-bound. The live cleanup passed; the separate core cleanup verdict remains failed.

## Attribution correction validation

The attribution-only correction separately ran the iteration check on the same unchanged operational bytes: exit 0 / 575.560000174 seconds at port base 25500, first attempt, no rerun. The original failed canonical was not rerun; its failures and unrun checks remain unchanged.

The new attribution distinguishes D-70/D-72 local startup policy from OPS-49’s worker/periodic-engine mandate; no behavior, requirement or check was changed. Exact commands, statuses, times and raw bindings are in [validation.json](validation.json).
