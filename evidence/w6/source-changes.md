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
```

```text
A	migrations/0003-reconciliation.sql
M	src/api/mod.rs
A	src/api/operator.rs
M	src/bin/engine.rs
M	src/engine.rs
A	src/engine/deletion.rs
M	src/engine/dispatch.rs
A	src/engine/reconciliation.rs
M	src/ledger.rs
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
M	tests/postgres/workflow.py
```

The later publication commit changes only README, scope, conformance map, implementation decisions and this final evidence bundle. It preserves the tested operational hashes.
