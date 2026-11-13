# Tested operational commits and paths

```text
0962ff8 Implement create reconciliation with durable evidence and independent purchase traces
b851401 Add isolated operator resolution and immediate delete lifecycle
59a0550 Exercise reconciliation, operator decisions and delete evidence with isolated controls
a89d02e Cover reached billing and episode edges while preserving interruption assertions
d8ca8bd Attribute historical interruption boundaries alongside periodic reconciliation
8a37b75 Format the delete evidence boundary
f35e12f Separate retention observations and verify meter quarantine boundaries
8f0253d Activate transaction faults after visibility and cover absence obligations
39d6808 Preserve unresolved reservations and expose scoped operator history
9454e45 Record W6 scope and final committed-source reconciliation evidence
f102437 Fix reconciliation clocks, expiry, episode recovery and credential stops
7ac52d6 Enforce operator allowances, retention and decision audit events
0604f80 Upgrade legacy cleanup targets and repeat caller delete validation
42e663d Exercise nine reconciliation and operator regressions on real services
8cb6ae1 Isolate repair fixtures and attribute credential faults to exact reads
02b7203 Give credential-stop scenarios an independent observation history
```

```text
M	README.md
M	docs/conformance-map.md
M	docs/decisions.md
M	docs/scope.md
A	evidence/w6/final-all.json
A	evidence/w6/final-checks.json
A	evidence/w6/final-commits.json
A	evidence/w6/final-controls.json
A	evidence/w6/final-core.json
A	evidence/w6/final-corrections.json
A	evidence/w6/final-default-workflow.json
A	evidence/w6/final-regressions.json
A	evidence/w6/final-w1.json
A	evidence/w6/final-w5.json
A	evidence/w6/final-w6.json
A	evidence/w6/final-workflow.json
A	evidence/w6/red-baseline-exact.json
A	evidence/w6/red-baseline-gone.json
A	evidence/w6/red-caller_sql.json
A	evidence/w6/red-checkout.json
A	evidence/w6/red-claim_cancel.json
A	evidence/w6/red-claim_guard.json
A	evidence/w6/red-commit_setting.json
A	evidence/w6/red-deadline.json
A	evidence/w6/red-defer_cancel.json
A	evidence/w6/red-displaced_record.json
A	evidence/w6/red-execute_cancel.json
A	evidence/w6/red-generation.json
A	evidence/w6/red-licensing_stop.json
A	evidence/w6/red-lock_observers.json
A	evidence/w6/red-lock_record_reset.json
A	evidence/w6/red-loss_stop.json
A	evidence/w6/red-notice_sticky.json
A	evidence/w6/red-notices.json
A	evidence/w6/red-outcome_stop.json
A	evidence/w6/red-payment_atomic.json
A	evidence/w6/red-predispatch.json
A	evidence/w6/red-queued_claim.json
A	evidence/w6/red-queued_claim_pool.json
A	evidence/w6/red-replacement_outcome.json
A	evidence/w6/red-startup_order.json
A	evidence/w6/red-surface.json
A	evidence/w6/red-terminal_capacity.json
A	evidence/w6/red-terminal_zero.json
A	evidence/w6/red-verify_cancel.json
A	evidence/w6/red-w4_ack.json
A	evidence/w6/red-w4_atomic.json
A	evidence/w6/red-w4_overload.json
A	evidence/w6/red-w5_blind_retry.json
A	evidence/w6/red-w5_checkout.json
A	evidence/w6/red-w5_generation.json
A	evidence/w6/red-w5_observer_run.json
A	evidence/w6/red-w5_stop_reset.json
A	evidence/w6/red-w6-in-progress.json
A	evidence/w6/red-w6-unsearchable.json
A	evidence/w6/red-w6_blind_repeat.json
A	evidence/w6/red-w6_non_exact.json
A	evidence/w6/red-w6_release_timeout.json
A	evidence/w6/red-w6_skip_startup.json
A	evidence/w6/report.md
A	evidence/w6/source-changes.md
A	evidence/w6/source-inventory.json
A	evidence/w6/validation.json
A	migrations/0003-reconciliation.sql
M	src/api/mod.rs
A	src/api/operator.rs
M	src/bin/engine.rs
M	src/engine.rs
A	src/engine/deletion.rs
M	src/engine/dispatch.rs
A	src/engine/reconciliation.rs
M	src/ledger.rs
M	src/local.rs
M	src/pricing.rs
M	src/provider.rs
M	src/store/business.rs
M	src/store/customer.rs
A	src/store/deletion.rs
M	src/store/migration.rs
M	src/store/mod.rs
A	src/store/reconciliation.rs
M	tests/postgres/controls.py
M	tests/postgres/core.py
M	tests/postgres/core_client.rs
M	tests/postgres/harness.py
M	tests/postgres/ownership.py
M	tests/postgres/simulator.py
A	tests/postgres/test_w6_inventory.py
M	tests/postgres/w4_publication.py
M	tests/postgres/w4_read_deadline.py
M	tests/postgres/w4_retention.py
M	tests/postgres/w4_round2.py
M	tests/postgres/w5.py
A	tests/postgres/w6.py
A	tests/postgres/w6_edges.py
A	tests/postgres/w6_exact.py
A	tests/postgres/w6_gone_reply.py
A	tests/postgres/w6_repairs.py
M	tests/postgres/workflow.py
```

The later publication commit adds only README, scope, conformance map, implementation decisions and this final evidence bundle. It preserves the tested operational hashes.
