# W6 control observation ordering

The control now checks a completed resolution pass. Previously it waited for a
search event, slept 0.2 seconds and sampled twice while the pass could still be
reading or committing. pvr-aqm is corrected in test tooling at 0ba3fc61be4f669ab13774714b0f793146412c51.

The first snapshot could arrive after settlement and fail at the intended
`exact-correlator/no-attachment` assertion; settlement between snapshots instead
failed the status assertion at `pending():31`; settlement after both assertions
let the child return 0. The strict parent rejected the latter two outcomes.
The retained exit-0 case has a later checkpoint containing the attached machine
and two ledger rows for the same operation. The held-reply comparison places its
first snapshot before the reply is released. These are operation-specific ordering
observations, not an attribution to machine load. [investigation.json](investigation.json)
binds source, operation, snapshot and event summaries to the retained record hashes.

`tests/postgres/w6.py:148-160` records the durable attempt timestamp before
starting/releasing the engine. It then waits under the existing 20-second
`eventually()` bound for a durable resolution or a changed non-null durable
attempt timestamp for that operation. It applies the existing no-attachment
assertion to the returned snapshot. The predicate never tests attachment or
waits for a desired failure. A completed-pass event records the observed boundary.
The code change is 12 additions and 2 removals in one file.

All 100 existing assertion ASTs and the strict control recognizer bytes are
unchanged. Product source and production bounds are unchanged. Pinned OPS-29,
`03-operation-lifecycle.md:1423-1426` at `ac37c7cae5d3ff746515137774a4cdd5a3531375`,
requires exact matching and excludes heuristic matching; it does not prescribe
this test synchronization. No product policy or conformance status is changed.

Both focused groups used one frozen source each, fresh isolated local two-node
database histories and the native control recognizer. Only its inventory was
selected to this one control. The extra comparison used the existing simulator
hold for `/constraints`, released about 0.45 seconds after its held-request event,
below the normal call bound. It changed no response or assertion. Toolchain and
dependency caches were pre-provisioned, with external network disabled and only
local loopback available during execution. UID 1000, port base 25500, owned short
harness roots, 16 GiB memory cap and no swap were used.

| Frozen group | Exact natural failures recognized | Held-reply control | Default witnesses | Actual group exit | Seconds |
|---|---|---|---|---|---|
| Before, d7ad22c | 3/5; two child exit 0 results rejected | child exit 0 rejected; hold confirmed | conflicting case pass | 0 | 242.603402793 |
| After, 0ba3fc61be | 5/5 | exact failure recognized; hold confirmed | conflicting and default-binary cases pass | 0 | 132.314813519 |

Every focused cleanup passed. After correction, all six controlled children
returned 1 at the exact assertion with `attached=true`, and all six parents
returned 0 recognizing the expected negative demonstration. Both default
witnesses returned 0. The before driver's group exit0 means evidence collection
completed; its rejected controls remain rejected. Five natural repeats and one
held reply per source are a bounded sample, not a statistical reliability guarantee.

After the perfect live group, the ordinary-checkout iteration and the full suite
ran on the same operational bytes. [validation.json](validation.json) records
their exact commands, statuses, times, phase results and cleanup. The full suite
ran once with `tools/postgres.sh --jobs 1 --seed 20261124` at port base 25500.

| Invocation | Actual exit | Seconds | Failed phases |
|---|---|---|---|
| iteration-a1 | 0 | 994.519800618 | none |
| canonical-a1 | 0 | 2109.380292681 | none |

The iteration and all twelve full-suite phases passed with their recorded cleanup results. No validation rerun was performed. The known pvr-wlm, pvr-kd7 and pvr-s48 failure sites were not observed.

[source-binding.json](source-binding.json) records the tested operational path
map and the assertion/recognizer comparison. Later publication changes are
limited to documentation, sanitized evidence and tracker records for this correction and its follow-ups.
Raw logs stay private. Three local diff seats reviewed the correction; all
reported no findings. Publication review and the final quick gate are recorded
separately. Failed runs remain failures.


Uncorrected publication follow-ups: P3 pvr-m9j records the private explanation filenames in the summary field; P3 pvr-2dw records the internal terminology for local reviews. Both are labelled review-nit and remain unchanged here. The published explanation, original observations and verdicts are unchanged.
