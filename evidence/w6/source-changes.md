# Tested operational commits and paths

Continuation of the preserved W6 implementation at `5061fe3`.

```text
8a9bea3 Preserve positive reconciliation evidence and stop rejected credentials
4e22232 Preserve legacy cleanup outcomes and scoped retired identities with refusal audits
6979d74 Exercise remaining reconciliation repairs with isolated real-service regressions
da4798c Retain exact single-match evidence in verification deadline expectations
96c312a Check frozen create terms with the worker correlator in credential regressions
16c4f11 Establish durable in-progress evidence before the retention restart
```

```text
M	migrations/0003-reconciliation.sql
M	src/api/mod.rs
M	src/api/operator.rs
M	src/engine/dispatch.rs
M	src/engine/reconciliation.rs
M	src/store/reconciliation.rs
M	tests/postgres/simulator.py
M	tests/postgres/w4_read_deadline.py
M	tests/postgres/w6.py
A	tests/postgres/w6_continuation.py
M	tests/postgres/w6_repairs.py
```

The final documentation/evidence commit preserves every tested operational hash. Earlier W6 source histories remain in their original regression artifacts.


Final bounded repairs continue unchanged `ac83e17` with ordinary commits:

```text
0d51c84 Bound retirement and complete credential and deficiency handling
cb355b1 Honor worker cancellation during credential health checks
```

```text
M	migrations/0003-reconciliation.sql
M	src/api/mod.rs
M	src/engine.rs
M	src/engine/deletion.rs
M	src/engine/dispatch.rs
M	src/engine/reconciliation.rs
M	src/store/business.rs
M	src/store/deletion.rs
M	src/store/reconciliation.rs
M	tests/postgres/simulator.py
M	tests/postgres/w4_retention_scale.py
M	tests/postgres/w6.py
A	tests/postgres/w6_blocking.py
A	tests/postgres/w6_blocking_baseline.py
M	tests/postgres/w6_edges.py
M	tests/postgres/workflow.py
```

The source inventory binds these tested operational files; the later documentation/evidence commit changes none of them.


Opening-rate and retired-replay repairs continue `96c6001` with these ordinary commits:

```text
db55b42 Correct exit opening rates and retained operator replays
c6636b1 Activate retained replay beyond the observed visibility window
a063df0 Measure the entered exit instant in opening-rate regressions
```

```text
M	src/api/operator.rs
M	src/store/deletion.rs
M	tests/postgres/proxy.py
M	tests/postgres/w6.py
M	tests/postgres/w6_blocking_baseline.py
A	tests/postgres/w6_final_repairs.py
```

The final source inventory binds the committed operational bytes. Later documentation and evidence changes preserve those bytes.
