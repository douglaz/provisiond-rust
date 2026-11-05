W2 physical sessions and destination reads: pass.

Command: `nix develop --command python3 tests/postgres/harness.py --phase checks --evidence evidence/w2-checks.json`.
Observed exit status: 0. The [manifest](w2-checks.json) records parent commit 31bd698 and exact dirty
source hashes, actual tools, effective settings, protocol/application traces and cleanup. The full
gate `nix develop --command bash tools/check.sh` exited 0; its complete log is
`/var/tmp/rloop-units/provisiond-rust-w1w2-1105T2104/harness/w2-checks-gate.qIZWHe`.

Fresh sessions passed idle and active-primary checks with one fixed primary flush target.
DB-05 covered real zero and duplicate senders, denied permissions, hidden/NULL positions,
wrong credentials and replication-role HBA rejection. The duplicate was an explicitly isolated
`pg_receivewal` protocol connection, not another physical standby. A separate SQL-function
adapter exercised the wrong-role predicate; it supplies no topology/history proof.
`pg_basebackup -X stream` itself ran while the designated receiver was paused: its backup sender
reported NULL and its streaming sender reported a numeric flush position. A separately caught-up,
non-designated `pg_receivewal` stream reported beyond the fixed target. None authorized admission.

DB-06 used the same pre-admitted physical reader. An isolated, deliberately prohibited backend
cancellation made a later destination visible below the intended standby's flush. The old target
was lower than the new post-read target; no destination escaped until the standby caught up.
DB-07 broke the session between SELECT and its post-read target: the old value was discarded and
attempt 2 repeated SELECT and check on a new admitted physical session. DB-09 actually withheld a
position reply through the call deadline, observed closure unused, and observed no check-created
SyncRep waiter. The check SQL is read-only; each poll has a fresh implicit statement snapshot.

Pre-COMMIT abandon, failed transaction and task cancellation closed their sessions. The
uncommitted inserted fixture remained absent, and replacements used different backend IDs.
The protocol observer counted zero client PostgreSQL cancel packets throughout these paths.
Deliberate negative backend cancellation was issued only by isolated harness administration.
Privilege tests denied file reading, configuration reload, owner-role assumption and protected
schema mutation. Exact grants and boundary tests are versioned; application roles have no direct
`pg_read_all_stats` membership.

Earlier attempts exposed a text-format mismatch for inet addresses, an unactivated pause that
observed only simple-query messages, and a setup ALTER ROLE blocked after standby shutdown. These
are failed/incomplete runs, not resilience passes. The final run observes Parse as well as Query
messages, compares host addresses, and makes all ordinary setup mutations with replication healthy.
No sync setting was lowered. Sent-COMMIT classification/retention is subsequent work; DB-10 and
all product engine/ledger/recovery/release behavior remain not run. External effect evidence is
not applicable. Cleanup stopped exact owned processes, deleted registered data, and retained raw logs.
