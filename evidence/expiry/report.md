# Authenticated pending-tenant expiry supplement

Fix: `c81fae6a16c86b68329eba8d44e72b8126b8f070`; specification pin
`ac37c7cae5d3ff746515137774a4cdd5a3531375`. This repairs the pending-deletion race in
`State::handle`'s post-serialization status read. An absent principal returns the existing
401 `authentication` refusal, message `authentication required`, `retryable: false`,
`details.reason: token`. The receipt recheck still precedes that read; suspension and
ceilings keep their ordering. A present pending tenant past its mint cap still returns
409 `conflict` / `signup_window_closed` (API-3/7/34/42/43, WIR-5/9/9a/9b, DOM-17).

The checked optional row uses the existing pre-COMMIT transaction destructor: it discards
its session, without changing component, cancellation or sent-COMMIT ownership (STO-7/55).
LDG-35 explicitly permits locking an identifier with no tenant row. No schema, dependency,
build feature, application deadline or accepted deployment profile changed. No new open
specification choice was settled; the ordinary optional-row behavior was already specified.

## Reproduction and observations

`tests/postgres/w4_expiry.py::expiry` runs through real HTTP executables and a synchronous
primary/standby PostgreSQL pair, with independent local provider/rail simulators. The
selectable `--phase workflow --case w4_expiry` builds **default features**; the same case is
also included in the complete workflow's hook-enabled build.

The proxy holds the first receipt SELECT's Parse **after** `State::authenticate`'s
constant-time comparison and successful return, before business BEGIN. This is a later
control-flow boundary than delivery of the credential lookup result. The target has valid
HTTP-issued credentials, no deposits and an unused key. An unrelated HTTP-enrolled caller
has already funded through the simulated rail, and all business/counter/effect baselines
are recorded.

The deletion mechanism is an **app-role fixture transaction reproducing housekeeping's
owned SQL**, rather than deletion by the periodic scheduler in this arm. It takes the
capacity lock `(1347572816,0)` and LDG-35 tenant lock, ages only that empty signup while
preserving its 10800-second TTL, checks the pending/expiry/deposit-finality predicate,
deletes the tenant and then its enrolment, and receives synchronous COMMIT acknowledgment.
Both rows are independently observed absent before the request is released. Using this
pre-BEGIN boundary and a single deletion transaction avoids waiting a periodic tick inside
a request transaction; the runtime's 250 ms checkout, 1000 ms server and 2000 ms call bounds
remain unchanged. The full workflow separately retains actual scheduled TTL/orphan cleanup.

The proxy then witnesses the request's serialized status read returning `SELECT 0` on its
attributed business session. The final original/fixed comparison uses the identical three
fixture files; [source identity](source.json) lists every overlay and hash. The isolated
original tree was a full baseline export at
`096d31cfd994031808b7f11e5d9280cab43df600`, with all non-overlay files checked against git
blobs and all 31 runtime/migration/dependency/build-input hashes recorded. Each invocation
used a fresh private target directory; executable hashes and feature sets are in the
manifests. The fix was never overlaid onto the original tree.

- [Original runtime](red.json): command exit **1**, expected negative demonstration. Setup,
  successful authentication, acknowledged deletion and `SELECT 0` all occurred. The exact
  intended assertion was `expired authenticated deposit must receive HTTP 401 authentication refusal`.
  Instead HTTP raised `RemoteDisconnected`, chained as the assertion's cause. API stderr
  recorded `index out of bounds: the len is 0 but the index is 0` at `src/api/mod.rs:480:20`
  and `API request task panicked`; the original API exited **1**, emitted `api_stopping`,
  and its listener refused a new connection. This is an activated runtime red, not a setup
  failure or an accepted transport exception.
- [Fixed runtime](green.json): command exit **0**, pass. The exact 401 error object, correlation
  id and CORS headers matched. The original API process/listener stayed live, with no panic,
  fatal event or restart. After the refusal, housekeeping completed another query and the
  rail watcher entered another cycle. The unrelated authenticated caller read its 115-sat
  balance, minted a deposit and replayed it successfully.
- Both runs witnessed `precommit_session_discarded`, client EOF and absence of the exact
  business backend. Green reacquired the same tenant primitive with a bounded lock wait.
  Before unrelated follow-up writes, global deposit, receipt, payment, ledger, operation,
  machine, commitment, assignment and complete principal-counter state stayed unchanged;
  provider and rail effect snapshots were equal. Only the intended tenant/enrolment removal
  occurred, the retained identity count did not change, and the deleted principal stayed absent.
- The case also checks pending deposit success and replay, activation, replay and fingerprint
  precedence while suspended, new-key suspension refusal and the distinct live-signup
  409 mint-cap refusal. Cleanup passed for every final positive and negative invocation.

Runs preceded the fix commit and truthfully report baseline HEAD plus dirty source. Every
recorded fixed operational file was subsequently compared to the full fix commit, with
matching hashes. README, this supplement and tracing changes are documentation only.
Earlier W4 evidence and its tested source remain historical and unchanged.

## Commands and scope

All harness commands used seed `20261110` and `--evidence <run-dir>/<name>.json`.
`<run-dir>` denotes the private evidence root; raw invocation directories are under
`<run-dir>/harness/`. Exact argv, statuses, raw-manifest/output/trace hashes, executable
hashes and features are in [validation](validation.json), [red](red.json), [green](green.json)
and [source identity](source.json). No raw logs or credentials are committed.

| Command | Observed status | Evidence |
|---|---|---|
| `nix develop --command bash tools/check.sh` | **0**, pass; format, lint, Rust tests and lock shape | `validation.json`, `<run-dir>/gate-final.log` |
| `nix develop --command python3 tests/postgres/harness.py --phase workflow --case w4_expiry --seed 20261110 --evidence <run-dir>/red-final.json` in original export | **1**, intended assertion above | `red.json` |
| Same focused command in fixed tree, evidence `green-final.json` | **0**, pass, default features | `green.json` |
| `nix develop --command python3 tests/postgres/harness.py --phase workflow --seed 20261110 --evidence <run-dir>/workflow-final.json` | **0**, 70 pass results, including expiry, authentication, deposit/replay/suspension, actual TTL cleanup and API lifecycle checks | `validation.json` |
| `nix develop --command python3 tests/postgres/harness.py --phase default-workflow --seed 20261110 --evidence <run-dir>/default-final.json` | **0**, two pass results; actual default executable rejects opt-in defect environment | `validation.json` |
| `nix develop --command python3 tests/postgres/harness.py --phase workflow --case w4_atomic --defect w4_atomic --seed 20261110 --evidence <run-dir>/control-atomic.json` | **1**, expected negative demonstration: `rail payment and credit must be atomic`, observed `0:1` payment/credit counts | `validation.json` |
| Same control command with case/defect `w4_overload`, evidence `control-overload.json` | **1**, expected negative demonstration: `overload must precede request business BEGIN`, observed one forbidden entry | `validation.json` |
| `nix develop --command python3 tests/postgres/harness.py --phase core --case w4_expiry` | **2**, expected incompatible selector refusal before setup | `validation.json` |

The selected controls cover no invented funding effects and the changed handler's
business-BEGIN boundary. The separate `w4_ack` negative control was not rerun: it changes
engine licensing acknowledgment, which this repair does not touch; its positive checks run
in both workflow phases. The complete nine-phase suite and all thirty controls were **not run**.
Versions: PostgreSQL 18.6, rustc 1.97.1, Cargo 1.97.0, Python 3.14.7, Nix 2.34.9;
exact dependency and nixpkgs identities are in `source.json`.

All children inherited the checked capped unit: MemoryMax 17179869184 bytes, MemorySwapMax 0,
OOMPolicy continue, KillMode mixed. Each harness invocation checked and reserved the entire
55500..55509 slot. Teardown joined application/simulator/proxy children, stopped PostgreSQL,
removed its explicitly registered data/build directories and credential files, and released
its ports after assertions. Raw evidence and the private original source export are retained
outside git for inspection. No production endpoint, real provider or funds were used.

## Database row audit

Search: `rg -n '\[0\]|\.first\(|\.remove\(0\)|query_one|query_opt' src`, then inspect the
SQL, migration grants/triggers and ownership for request-reachable results. Only the status
lookup is reachable by this pending-tenant deletion race. The baseline line references below
identify the reported candidates; the fix shifts subsequent lines by ten.

| Baseline location | Concrete protection or correction |
|---|---|
| `src/api/mod.rs:328`, enrolment capacity | `SELECT count(*) FROM provisiond.tenants WHERE status='pending'` has no GROUP BY and returns one scalar even for no tenants. Enrolment holds the capacity advisory lock; housekeeping uses that same capacity lock. |
| `src/api/mod.rs:480`, status | **Corrected.** `SELECT status ... WHERE id=$1` can return no row despite holding LDG-35, because authentication preceded that lock and LDG-35 does not require a live tenant. |
| `src/api/mod.rs:528`, funding health | Migration 0002 inserts the sole `funding_health` row, with primary key and `CHECK(singleton)`. The app receives SELECT/INSERT/UPDATE, no DELETE/TRUNCATE or ownership; bootstrap makes the migrator schema owner. No runtime deletion path removes this row and no tenant FK/cascade reaches it. The guarantee is the supported privilege/configuration boundary, not the table name or protection against an administrator. |
| `src/api/mod.rs:535`, pending enrolment | Enrolment inserts tenant and enrolment in one transaction. `handle` holds the same tenant advisory lock through status read and `deposit`. Housekeeping holds it through both tenant deletion and conditional enrolment deletion. `funding::credit` first calls `credit_payment`, which acquires that tenant lock, then updates activation in the same transaction; activation neither deletes the enrolment nor interleaves with this held lock. Thus a still-present pending status cannot lose its enrolment in this transaction. |
| `src/api/mod.rs:567`, ceiling | Outer `SELECT COALESCE((SELECT used ...),0)` has no FROM and always produces one scalar. An absent counter becomes zero. |
| `src/api/mod.rs:617/729`, provider status | Migration 0002's `account_configured` AFTER INSERT trigger creates `provider_account_status` in the configuration transaction. Snapshot uses a status row with LEFT JOIN listings/rates. App has no DELETE/TRUNCATE on configured accounts/status, and runtime provides no removal path. These are provider-global rows with no tenant FK/cascade. A pending caller cannot enter create (API-43); pending housekeeping deletes neither account nor status. Administrator/offline configuration corruption is outside this race. |
| `src/api/mod.rs:793`, operation view | Read follows `enqueue` insertion in the same transaction. `operation_views` selects operations with a LEFT JOIN to commitments, no filtering inner join or tenant join. The uncommitted queued operation is visible to its own transaction and cannot be deleted by another transaction; housekeeping deletes only old succeeded/failed operations. |

The wider request-reachable search included `src/api/funding.rs`, `src/store/customer.rs`,
`src/store/business.rs`, `src/store/component.rs`, `src/store/mod.rs` and `src/ledger.rs`:

- Funding's indexed deposit read follows a checked binding and `credit_payment`; deposits have
  no tenant FK and migration 0001's `deposits_retained` trigger rejects DELETE/TRUNCATE.
  Pending tenant removal therefore cannot remove that row. Credit's initial deposit lookup
  uses `.first().context(...)`; orphan funding remains permitted.
- Request-side `admit_create_inner` uses scalar `SELECT gen_random_uuid()`, unconditional
  `INSERT ... RETURNING` for commitments, and checked optional receipt/health reads. Ledger
  balance/audit outer SELECTs have no FROM; ledger append's `INSERT ... SELECT ... RETURNING`
  also has no outer FROM/filter. Payment insertion is unconditional RETURNING. Missing
  tenant rows do not suppress these SQL results; constraint failures return errors.
- Worker-only licensing/attachment/settlement paths require an admitted active tenant's
  operation, so a pending caller cannot create their inputs. Attachment checks its optional
  operation/machine result; rate is an outer scalar, descriptor is configured provider-global
  state, and episode UPSERT has unconditional RETURNING. Claim reads `FOR UPDATE SKIP LOCKED`
  before its indexed UPDATE result. Verified settlement first obtains an UPDATE row lock via
  `settle_inner`; its later operation read stays protected. Commitments referenced by that row
  cannot disappear while its owning transaction holds it (operation FK); pending housekeeping
  deletes no commitments. Other optional operation/machine reads are checked.
- `src/engine.rs`'s restart/stop reads are worker-only. Restart uses a checked optional
  UPDATE result; stop checks its first operation lookup before the indexed about-to-order
  lookup. Pending tenants cannot admit those operation inputs, and the pending deletion
  statements touch neither operation rows nor provider state. These are outside this
  customer request race; no broader retention/error-policy repair is claimed.
- Shared replication reads enforce `rows.len()==1` before indexing; generation checks either
  short-circuit on length or explicitly enforce it. Connection host indexing follows validated
  configuration. Protocol bytes, refusal vectors and socket buffers are not database rows.

No invariant-backed index was changed, and no unrelated error-policy repair was absorbed.
