# Provider read deadlines

Measured 2026-11-11 on simulator-only runtime
`aa5c0bd8bc417a8c1c7b37fbae118ba0b83470d2`, against unchanged baseline
`8c961c81e0746d2e88ca4934b2ea927a4bbc89bf` and specification
`ac37c7cae5d3ff746515137774a4cdd5a3531375`. This is a pre-conformance subset.
Independent review is `not run` in this implementation phase; acceptance is not asserted.

`Client::call_entered` now bounds `/offers`, `/search`, `/get` and `/constraints`
with `read_seconds`, including the separate confirming `/get` after `/delete`.
Every POST retains `call_seconds`. Both duration lookups retain their one-second
fallback. Descriptor validation, all seven sequential pricing terms, error mapping,
external-call ownership and cancellation remain unchanged. No scheduler or production
delete workflow is introduced.

## Exact-fixture comparison

The selectable real-binary PostgreSQL case is `--phase workflow --case w4_read_deadline`.
All harness commands use seed `20261111`, fresh targets, isolated data and credentials, the
same ten-port slot, and a private temporary directory outside `/tmp`. Cargo target,
intermediate-directory and compiler-wrapper environment overrides were cleared;
the harness sets its own fresh target, and each gate uses another fresh target.
Build concurrency is four. The capped unit supplies 16 GiB memory, zero swap,
`OOMPolicy=continue` and `KillMode=mixed`.

The original export matched all 244 tracked Git objects before seven fixture-only
overlays. All 31 baseline runtime, migration, dependency and pin inputs stayed
identical. The overlays are enumerated and hashed in [tested-source.json](tested-source.json);
none changes runtime behavior. Separate targets and recorded executable hashes
identify the baseline engine and fixed engines. Final fixture bytes are identical
in the red, focused green and clean complete workflow.

The admitted descriptor has `call_seconds=5`, `read_seconds=1`, and the five other
wind-down terms equal to one: the unchanged priced sum is 11 seconds. The API is
restarted before admission so its immutable catalog freezes those exact terms.
An attributed confirming GET is delayed three seconds. The independent simulator
records request receipt, acceptance, delay activation and monotonic peer EOF;
timing excludes builds, startup, database queues and settlement. It also records
the full delay's completion before outcome snapshots and teardown.

The unchanged baseline command exited **1**, specifically at:

```
PRV-13b confirming read wait must not exceed read_seconds plus scheduling tolerance
```

Its confirming GET remained open for at least **3.002728 s** after delay activation
and **3.018558 s** after receipt. This lower bound exceeds the priced one-second
term plus the explicit **0.4 s** scheduling tolerance. The timing assertion precedes
the differing outcome assertions. A real accepted/created order preceded the read;
cleanup passed. The raw child remains `fail`; the recognized result is
`expected negative demonstration`.

The identical focused fixed-runtime fixture exited **0**. Its recorded HEAD was still
the baseline before the fix commit; every one of its 57 operational hashes equals
the later fix commit. The complete workflow repeated all seven arms on the clean
fix commit and exited **0**. Those clean-run receipt-to-wait-end observations were:

| Request | Seconds | Result |
|---|---:|---|
| Accepted-create confirming `/get` | 1.000257 | pass |
| Accepted-create verification `/search` | 1.000545 | pass |
| Post-confirmation `/constraints` | 1.000680 | pass |
| Pre-order `/offers` | 1.001037 | pass |
| Separate post-delete confirming `/get` | 1.000844 | pass |

Each read has a one-second allowance, a three-second activated delay and the same
0.4-second tolerance. The clean-run mutation, with prompt verification reads,
completed in **3.033655 s**: longer than the read allowance plus tolerance and shorter
than the five-second mutation allowance minus tolerance. The focused green measured
that mutation at **3.054987 s**. All-one terms with prompt reads also succeeded.

## Outcomes and compatibility

For both accepted-create read failures, stored errors and authenticated detail/list
views retain `timeout`, `accepted: true`, false retry advice, attempted action and
the independently learned safe identifiers. The original complete commitment remains
unchanged at 43 sats and open, with no machine, debit or result. Frozen accepted terms,
errors and identifiers survive ordinary restart without a second order. The failed
pre-order offers read retains its existing timeout/advice and deterministic release.
PRV-13c's constraint timeout remains absorbed: the verified create succeeds with a
null cancellation date and its ordinary atomic debits.

The separate stop-cost probe observes an actual accepted delete and a delayed
confirming GET. It retains the existing `network`, `accepted: true`, false retryability
and unconfirmed-absence result. Its independently created simulator resource is
deleted once; database operations, machines, commitments and ledger counts remain
unchanged. The timeout itself establishes no absence or billing-stop fact.

The existing held search/get and delayed-constraint fixtures explicitly set
`read_seconds` to their former effective three/five-second allowance. This prices
the already exercised wait while preserving the intended lock-loss cancellation,
retained observation, lost settlement acknowledgement and shielded outcome faults.
The complete workflow observes actual attributed lock death in both verification
windows, all three constraint arms and the real settlement repeat. No startup,
checkout, store-call/retry or stop-record bound changes.

## Validation

The following commands each exited **0** on the clean fix commit. Evidence arguments
use `<run-dir>/<name>.json`; [validation.json](validation.json) records the full commands,
environment choices, versions, feature modes, binary identities and raw-file hashes.

- `nix develop --command python3 tests/postgres/harness.py --phase workflow --seed 20261111 --evidence <run-dir>/workflow-final.json`: 80 pass entries, including all named affected workflow arms.
- The same harness with `--phase workflow --case w4_expiry`: default-feature expiry, pass.
- The same harness with `--phase default-workflow`: default-feature workflow, pass.
- The same harness with `--phase core --case stops`, `licensing_stop`, `outcome_stop` and `deadline`: four separate commands, pass.
- A restricted external invocation of the unchanged canonical `tests/postgres/controls.py`: all nine required controls, each child exit 1 at its exact intended assertion with activation and successful cleanup; parent exit 0.
- `nix develop --command bash tools/check.sh`: exit 0, formatting, lint, 11 library tests, one entry-point test and spec-lock check. Whole stdout and stderr were captured separately; no output filter supplied the status.

The controls are `w4_atomic`, `w4_ack`, `w4_overload`, `generation`, `claim_guard`,
`predispatch`, `licensing_stop`, `outcome_stop` and `deadline`. The complete workflow
supplies explicitly indexed focused coverage for `w4_live_stop`, both `w4_verify_stop`
windows, `w4_stop_cost`, all `w4_observation` arms, refusal, ambiguity and mixed pacing.
Default-feature builds are separately identified. No nine-phase run is claimed.

PostgreSQL is 18.6, Rust 1.97.1, Cargo 1.97.0, Python 3.14.7 and Nix 2.34.9;
nixpkgs is `aff8a0b28396750446e5537a96461bc4facdb287`, tokio-postgres 0.7.18.
All child cleanup assertions passed. Private credentials, database data, build targets
and the isolated baseline export were removed after proof collection. All ten ports
were reservable together after cleanup. Final raw proof is retained privately outside
Git; the committed inventory uses placeholders and hashes. Earlier fixture setup and
intermediate attempts remain separate raw history and are not final proof.

[Observations](observations.json) retain the actual read/mutation records, durable
outcomes and existing fault activation. [Source](tested-source.json) separates tested
runtime inputs from this later report-only commit. Historical W4, expiry and refusal
evidence is unchanged. D-6/7/8/9, automatic reconciliation, production cancellation and
the deferred rail-history response-size limit remain open. No real provider, purchase,
funds, production endpoint, remote publication or merge was used.
