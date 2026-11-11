# Definitive refusal and independent caller pacing

Runtime and fixtures: `27a75ffe84c633178c6b90e2e856817f09bb1c8f`.
Baseline: unchanged `17a40da11fdafb53e03c76e8ce6bce2a7949f67a`.
Specification: `ac37c7cae5d3ff746515137774a4cdd5a3531375`.

`Engine::execute_order` now reads the driver's canonical `details.status`
(PRV-5, OPS-11). A rejected order with kind `provider`, status 400 and
`accepted: false` settles `failed` through the existing guarded transaction.
`State::handle` verifies credentials before consulting the failed-auth source
allowance (API-3/29/50). Valid principals retain their own read/write budgets.
Verification uses the ordinary checked read pool; source throttling is not a
pre-database work bound. No production capacity or timeout policy is selected.

The [source inventory](tested-source.json) binds 31 runtime/build inputs and
56 operational files. All 240 baseline tracked files match their original Git
objects except three explicitly listed fixture overlays. Those fixtures are
byte-identical in the final red and green comparisons. The two runtime changes
are confined to the dispatcher and HTTP handler. Each invocation builds into its
own target and records the hashes/features of the binaries actually launched.
The baseline is a private export; the working branch was never reset or switched.

## Independent comparisons

| Case | Baseline | Fixed source | Observed distinction |
|---|---|---|---|
| `w4_provider_refusal` | exit 1, expected negative demonstration | exit 0, pass | Real client preserved provider/400/accepted-false; baseline retained reconciliation and 43 sats, fixed source failed and closed the original commitment at zero |
| `w4_mixed_pacing` | exit 1, expected negative demonstration | exit 0, pass | Invalid writes first produced 401/401/429/429; the next valid deposit was baseline 429 versus fixed 200 |

The exact red assertions are:

- `OPS-11 provider/400/accepted-false order must settle failed and release original commitment`
- `API-50 valid caller must reach independent write budget after failed-auth source exhaustion`

The refusal fixture observes the admitted original commitment and available
balance before allowing settlement: 43 sats reserved, 72 available from 115.
It establishes acknowledged step 8, steps 1–9, an independently recorded order
receipt/error/response, no accepted or created purchase, and owned temporary-key
deletion. Fixed settlement preserves the complete error and its `retryable: true`
advice in stored/detail/list views, removes the resource exclusion, creates no
machine or setup/unit debit, and restores exactly 115 available. Keyed replay
and ordinary API/engine restart preserve the full closed commitment row and
operation view without another order. A subsequent valid create succeeds.

The mixed-traffic fixture uses one loopback source, real HTTP and PostgreSQL,
30 reads and two writes per 10000 ms window. Thirty reads admit the advertised
three-read cadence. After actual source exhaustion, each of two unrelated
principals receives two deposit responses and 30 correct fleet reads before its
own write/read allowances refuse. Missing, unknown, rotated and recovery
credentials remain source-throttled; every 429 checks error shape, correlation,
body pacing and rounded `Retry-After`. Pending/malformed-body and suspended
requests retain their refusals. The final focused burst took 297 ms, within one
window; this deliberately exhausts allowances rather than claiming obedient
polling. The existing sustained-cadence case is also run.

## Verification

Commands use the isolated unit, fresh credentials/directories, and the reserved
ten-port slot beginning at 55500. Complete command arrays, actual exits, build
identities, raw-log hashes, cleanup and activation observations are in
[validation](validation.json); consequential DB/HTTP/provider facts are in
[observations](observations.json). Raw artifacts remain outside Git at
`<run-dir>`, with a full raw artifact inventory and cleanup audit. Here `<run-dir>`
names the artifact directory; harness traces/build paths include its `harness/`
child explicitly. Control child command arrays retain the fixture's generic
`<run-dir>/control-manifest.json` token: their actual outputs are
`<run-dir>/harness/controls-jsvy7nk9/<defect>.json`, while each control's `evidence`
field names the parent-finalized manifest. Raw manifests retain the harness's
original local placeholder convention.

The committed observations are a focused projection of final raw manifests:
repeated frozen summaries and workflow duplicates are omitted, and mixed HTTP
responses are grouped into status/count/timing summaries. No response is invented
or changed. The inventoried `finalize_observations.py` reproduces the committed
projection byte for byte from those raw manifests (exit 0). Full unsummarized
responses remain in the hash-bound raw manifests.

| Command after `nix develop --command` | Actual exit / result |
|---|---|
| `bash tools/check.sh` | 0; formatting, Clippy, 12 Rust tests and spec-lock pass |
| `python3 tests/postgres/harness.py --phase workflow --seed 20261111 --evidence <run-dir>/workflow-final.json` | 0; 73 scenario entries pass, including authentication, provider, operation-error, preflight/restart/stop, expiry and the new cases |
| Same harness with `--phase workflow --case w4_provider_ambiguity` and the same seed/evidence shape | 0; dispatched provider 500/no-status and accepted-then-provider-400 retain 43 sats, complete false advice and no second order across restart |
| Same harness with `--phase workflow --case w4_expiry` | 0; default-feature pending-expiry regression pass |
| Same harness with `--phase default-workflow` | 0; default-feature workflow and disabled defect-environment check pass |
| Existing control runner restricted to the six affected controls | runner 0; each activated child 1 at its exact intended assertion, cleanup pass |
| Three new selectors paired deliberately with `--phase core` | each 2; incompatible phase refused before fixture setup |

The controls are `w4_atomic`, `w4_ack`, `w4_overload`, `generation`, `claim_guard`
and `predispatch`. They cover funding/admission atomicity, acknowledged dispatch,
changed-handler business-BEGIN refusal, fenced settlement and live worker
position. Other transport/cancellation/deadline/recovery controls were not run
for these narrow repairs. No nine-phase acceptance run is claimed.

PostgreSQL 18.6, Rust 1.97.1, Cargo 1.97.0, Python 3.14.7, Nix 2.34.9,
tokio-postgres 0.7.18, deadpool 0.12.3 and tokio 1.54.1 were used. Focused
comparisons and the complete workflow use `store-test-hooks`; the separately
identified default checks use no test features. Gate checks all features.
The gate ran on the fix commit before the documentation/evidence commits; its
status file does not independently snapshot source hashes. Source binding uses
the clean workflow inventory immediately afterward and equality of all 31
runtime/build inputs between the fix and review tips. This is a recorded limit,
not a claim that the gate ran on the later documentation commit.
A provider 4xx before step 9 keeps its reported advice under the same canonical
field correction; that specific pre-step-9 provider error is not separately
injected by these fixtures.
Every gate's complete stdout/stderr was captured separately and its true status
saved before either log was read. Inherited Cargo target/intermediate overrides,
wrappers and Nix overrides were unset; targets were assigned per invocation.

All 23 owned harness invocations, including superseded attempts and controls,
reported cleanup pass. Children completed before data/target/credential removal;
additional exact-path cleanup removed owned WAL archives and both gate targets.
All ten ports were rebound together after cleanup. The raw audits retain exact
removed paths and hashes. Two unsuccessful fixture-development attempts remain
failures in raw history and are excluded from the final comparisons.

This remains simulator-only and pre-conformance. Automatic reconciliation,
production parameter selection, full saturation/recovery and arbitrary retained
rail-history delivery remain outside this work. Historical `evidence/w4/` and
`evidence/expiry/` retain their original sources and claims.
