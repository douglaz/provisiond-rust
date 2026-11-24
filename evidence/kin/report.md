# Local enrolment and configuration availability check

On 2026-11-24 the default release candidate recovered the customer's identical enrolment request
following a real pre-BEGIN checkout refusal, then rejected reuse after success. The engine also
recovered from an actual post-admission CONFIGURATION lock timeout in the same invocation.
These are local prototype observations with simulators, one primary and one designated synchronous
standby. Separate startup acquisition failure was not driven. No production availability guarantee
or broader conformance claim follows.

## Customer problem and bounded correction

[R1](r1.json) diagnosed a P1 on baseline `c5045df725e8856ef9ca88ea2d4cc9e133308d2f`:
503 `overloaded`, `retryable: true`, body `retry_after_ms=250`, header `Retry-After: 1`,
then the identical unexpired request returned generic 400 `invalid admission token` after
1.081186978 seconds in the same API process. A fresh code returned 200. The diagnostic is an
expected negative demonstration (exit 0 / 10.105560480 seconds); engine recovery was not run.
The customer had obeyed the refusal's advice but the code had already been consumed.

The owning pinned bodies are
[STO-55, 05-persistence.md:291](https://github.com/douglaz/provisiond/blob/ac37c7cae5d3ff746515137774a4cdd5a3531375/05-persistence.md#L291),
[DOM-17, 01-domain-model.md:700](https://github.com/douglaz/provisiond/blob/ac37c7cae5d3ff746515137774a4cdd5a3531375/01-domain-model.md#L700),
[API-33, 04-api-contract.md:371](https://github.com/douglaz/provisiond/blob/ac37c7cae5d3ff746515137774a4cdd5a3531375/04-api-contract.md#L371),
[API-40, 04-api-contract.md:815](https://github.com/douglaz/provisiond/blob/ac37c7cae5d3ff746515137774a4cdd5a3531375/04-api-contract.md#L815)
and [WIR-12, 13-wire-contract.md:450](https://github.com/douglaz/provisiond/blob/ac37c7cae5d3ff746515137774a4cdd5a3531375/13-wire-contract.md#L450).
[D-71](../../docs/decisions.md#d-71-release-enrolment-admission-only-after-typed-pre-begin-checkout-refusal)
settles the checkout-only correction within D-70, W8 and D-6.

[`Admission::consume`](../../src/auth.rs) reserves a nonce atomically and returns
`ConsumedAdmission`, with no release on drop. [`Store::begin_checked`](../../src/store/mod.rs)
marks a failed whole checkout with typed `CheckoutRefused` before BEGIN submission. Only that
cause makes [`State::handle`'s enrol path](../../src/api/mod.rs) call
`ConsumedAdmission::release_after_checkout_refusal` for its own reservation. Single use after
success, admission-token validation precedence and all failures from BEGIN onward remain intact.
There is no new replay route, durable enrolment idempotency or automatic server retry loop.
Other refusals and transaction uncertainty are outside this correction.

## Live observations

[Live evidence](live.json) binds default release binaries to
`5deadfa51435d7d6dcf46c5137b53b9183080c1c`, source tree
`b8be95eaf28d20358df9a940a753d454f1dce39a`, and private driver
`b82d0d0339b5f87b23c213615cccf8023d0640a9`. Run `accept-i3-b` passed, exit 0 /
10.065245687 seconds. The first candidate attempt `accept-i2-a` remains inconclusive, exit 1 /
0.241099419 seconds: launcher needed owned file streams, customer and engine arms both not run,
cleanup verified. It is retained in the same JSON and is not upgraded by the later run.

The customer saw actual 503→200→400. The successful resend used the same exact bytes (equal body
SHA256 digests), started 1.077288216 seconds after the busy reply, honoured the larger 1 s header
hint over the 250 ms body hint and completed before expiry. There was one pending account, no
refusal receipt, no credentials on used-code refusal and no API restart: PID 367547 and invocation
`66364f43177c476fa1fdc3465c69f794` remained the same. The targeted `Acceptance.customer_arm`
script makes exactly two recovery sends (one busy request, one paced resend), then a separate
used-code 400 verification. Unexpected replies or transport errors stop it; it has no automatic
four-attempt HTTP retry loop. The historical [W8 journey](../w8/live.json) had a separate
four-attempt client. Current ordinary `Workflow.request` uses at most three attempts, freezes
bytes before the first send and honours both hints. Armed gates, expected errors and defect
controls remain direct; longer-than-one-second hints stop the ordinary fixture rather than be
shortened. These are distinct clients and budgets.

After generation advance and restart-pass, `Acceptance.engine_arm` observed actual PostgreSQL
configuration lock timeout `55P03`, acknowledged rollback and one whole-transaction retry with
`spent_ms=500`. Configuration then committed with acknowledged COMMIT; 71 ordinary claim polls
completed during one second. Engine PID 368077, invocation `6733e00d34fb4fcb81f890343bf1b965`
and restart count 0 remained the same. No external call repeated. This demonstrates only
[`Engine::configuration`](../../src/engine.rs)'s post-admission CONFIGURATION recovery arm under
[OPS-49, 03-operation-lifecycle.md:1968](https://github.com/douglaz/provisiond/blob/ac37c7cae5d3ff746515137774a4cdd5a3531375/03-operation-lifecycle.md#L1968).
The separate `Engine::begin_startup` acquisition-failure arm is not run.
[OPS-47:135](https://github.com/douglaz/provisiond/blob/ac37c7cae5d3ff746515137774a4cdd5a3531375/03-operation-lifecycle.md#L135)
and [OPS-21:1836](https://github.com/douglaz/provisiond/blob/ac37c7cae5d3ff746515137774a4cdd5a3531375/03-operation-lifecycle.md#L1836)
remain constraints; this is not a demonstration of all their lock-loss, stop or exhaustion arms.
No dispatch, purchase, funds, promotion or history replacement was driven.

Recorded commands below are evidence, not instructions executed during this documentation update.
Path placeholders preserve argument order/options; private absolute paths and credentials are not
published. Exact selected environments, wrappers, stdout/stderr hashes and raw identities remain
in the linked JSON.

R1 command (private driver `ec96df9fdff54778f2cc65e38b768d64ef59f9ec`):

```sh
nix develop <product-checkout> --command python3 -I <private-driver>/kin.py --install-dir <baseline-install> --run-dir <private-evidence>/runs/r1-i1-20261124T1634
```

Default release build (exit 0 / 0.597465137 seconds), then live command (exit 0 / 10.065245687 seconds):

```sh
nix develop '<product-checkout>' --command cargo build --release --locked --bins
timeout 1260 nix develop '<product-checkout>' --command python3 -I '<private-driver>/accept.py' --commit 5deadfa51435d7d6dcf46c5137b53b9183080c1c --install-dir '<install>' --run-dir '<private-evidence>/runs/accept-i3-b'
```

## Regression and client evidence

[`w4_round2.enrolment_retry`](../../tests/postgres/w4_round2.py) is registered in its shared
`CASES` registry and admitted by [`harness.py`](../../tests/postgres/harness.py) as
`w4_enrolment_retry`; the full workflow includes it. [Regressions](regressions.json) retain the
same test bytes for baseline red and fixed green; 47 protected runtime/build files matched each
checkout's HEAD and remained unchanged. These focused runs used the suite proxy; the independent
live run above used default release binaries.

| Recorded check | Result | Exit | Wall seconds | Observation |
|---|---|---|---|---|
| Baseline c5045df focused regression | expected negative demonstration; harness fail | 1 | 51.024309556 | Actual 503, unexpired identical resend 400; intended `paced identical-body enrolment resend after pre-BEGIN busy must return 200` assertion |
| Candidate 5deadfa focused regression | pass | 0 | 60.319961566 | 503→200→400, one account, no new refusal receipt, unchanged API process, invalid token precedes unknown field |
| `test_enrolment_retry.py`, seven client checks | pass | 0 | 1.516944277 | Frozen raw/serialized bytes, same key where applicable, finite three attempts, pacing and direct fault/expected-error guards |

Exact normalized commands, each with recorded 1200 s timeout:

```sh
nix develop --offline --command python3 tests/postgres/harness.py --phase workflow --case w4_enrolment_retry --seed 20261124 --jobs 1 --evidence '<private-evidence>/i4/impl-tests/red/manifest.json'
nix develop --offline --command python3 tests/postgres/harness.py --phase workflow --case w4_enrolment_retry --seed 20261124 --jobs 1 --evidence '<private-evidence>/i4/impl-tests/green/manifest.json'
nix develop --offline --command python3 -m unittest discover -s tests/postgres -p test_enrolment_retry.py -v
```

Red ran in the disposable baseline source, green/client in the product checkout. Selected port
base was 25960 with separate run roots/targets and `PROVISIOND_HARNESS_INVOCATION` removed.
The actual activated checkout fault left BEGIN count unchanged; the red failed the intended
assertion rather than setup. Both focused cleanup records pass with no errors.

## Iteration and complete canonical evidence

[Validation](validation.json) records tested clean source
`70d86db89da633e17cbfc28d44dd7ec41f27009b`, tree
`b80eda2db8b86099e9079b6d7f353a298315d90f`, tested source digest
`5d7360ce21834de129dbdca2fac77b4dd270e686f7304078fb8ea3493714cebc`.
Exactly one known timing-class rerun was allowed and used under D-69; the runner itself performs
no automatic rerun. Both iterations used three workers and seed 20261124.

| Invocation | Result | Exit | Wall seconds | Recorded scope |
|---|---|---|---|---|
| First iteration, port base 25900 | fail | 1 | 554.955759231 | Quick gate 0 / 36 s; Python units 1 / 3 s; iteration phase set 1 / 515 s |
| One recorded rerun, documented port base 25500 | pass | 0 | 509.967890345 | Quick gate 0 / 40 s; Python units 0 / 2 s; iteration phase set 0 / 468 s |
| Single serial canonical, port base 25500 | pass | 0 | 1831.510351548 | All twelve phase processes pass; actual new W4 row present once |

First-iteration W7 failed (exit 1 / 366.120018251 seconds): `w7_markers`' recovery process hit a
whole-pool checkout deadline, 20 pass rows plus child and aggregate failure rows. Controls passed
(exit 0 / 443.518572415 seconds). The Python unit run executed 58 tests and failed in three
assertions of `test_runner_refuses_overlap_before_build_or_reservation`. The launcher selected
25900 rather than documented 25500, exposing that preexisting unit's port-range assumption.
The controlled pure-unit comparison changed only `PROVISIOND_HARNESS_PORT_BASE`, with test bytes
identical to c5045df: 25500 exited 0 / 0.214510250 seconds, 25900 exited 1 / 0.214641422 seconds.
This establishes only the unit's port-profile cause. It does not establish the independent W7
checkout timing cause, or prove that changing ports cured it. Two comparison setup failures
(import before assertions; missing working-directory Cargo.toml) are retained in validation.
The port unit is deferred as [pvr-q4o](../../.beads/issues.jsonl) and was not fixed here.

The rerun's W7 passed, exit 0 / 280.940576039 seconds, 21 pass rows; controls passed, exit 0 /
402.181792183 seconds. Both iterations retained 44 controls and five candidate regressions as
expected negative demonstrations, each child exit 1. Their aggregate phase pass does not turn
those child exits into successful product behavior. Each W7 cleanup count is 22 pass, and each
controls/candidate set records 49 child cleanups without errors.

Exact iteration/canonical commands including wrappers:

```sh
timeout 1200 nix develop --command bash tools/iteration.sh --jobs 3 --seed 20261124 --evidence '<private-evidence>/i4/iteration/iteration.json'
timeout 1200 nix develop --command bash tools/iteration.sh --jobs 3 --seed 20261124 --evidence '<private-evidence>/i4/iteration-corrected/iteration.json'
timeout 3300 nix develop --command bash tools/postgres.sh --jobs 1 --seed 20261124 --evidence '<private-evidence>/i4/canonical/canonical.json'
```

Controlled comparison command, run once per selected port base above:

```sh
nix develop --command python3 -m unittest discover -s tests/postgres -p test_ports.py -k test_runner_refuses_overlap_before_build_or_reservation -v
```

Canonical phase observations (same `harness.py --phase <phase> --seed 20261124 --evidence
<private-evidence>/i4/canonical/canonical-<phase>.json` child command, as recorded in validation):

| Phase | Result / exit | Wall seconds | Scenario rows excluding controls |
|---|---|---|---|
| w1 | pass / 0 | 6.962716752 | 3 pass |
| checks | pass / 0 | 30.294051406 | 13 pass |
| commits | pass / 0 | 19.278271603 | 13 pass, 2 expected negative demonstration, 1 not run |
| regressions | pass / 0 | 13.381153700 | 9 pass |
| corrections | pass / 0 | 9.879772336 | 8 pass |
| core | pass / 0 | 67.585375776 | 36 pass, 3 not run |
| workflow | pass / 0 | 239.049450329 | 85 pass |
| default-workflow | pass / 0 | 7.435355033 | 2 pass |
| w5 | pass / 0 | 94.381254090 | 30 pass |
| w6 | pass / 0 | 603.874230934 | 54 pass, 1 not run |
| w7 | pass / 0 | 321.208221360 | 21 pass |
| controls | pass / 0 | 353.861667799 | 44 controls + 5 candidate regressions, all expected negative demonstration / exit 1; release sensitivity pass |

The canonical's 281 non-control rows are 274 pass, two expected negative demonstrations and five
not run. Workflow has 85 pass rows, including `W4-enrolment-pre-BEGIN-busy-retry` exactly once.
The other two enrolment-named rows are existing limits/TTL/orphan and pacing/no-delay-oracle checks;
they are not extra runs of the new regression. W5 has 30 pass rows, W6 has 54 pass / one not run,
W7 has 21 pass. All twelve phase commands exited 0 in this invocation; five not-run rows remain
not run. Eleven ordinary phase cleanup records plus W7's 21 child cleanups (32 total ordinary
cleanup records) pass, and 49 control/candidate child cleanups pass, with no recorded errors.

These results do not replace any historical W8 result. [W8 validation](../w8/validation.json)
retains first canonical fail exit 1 / 1678.641406 seconds on 18574b3, final canonical fail exit 1 /
1844.113252 seconds on b1b0c721, and the single separate unchanged-source W5 pass exit 0 /
137.348692 seconds. Historical final canonical F27 remains a failure: 11 of 12 phases pass,
251 pass / one fail / two expected negative demonstrations / five not run. Its separate phase
collection is not one green canonical. Earlier W8 and W7 failures retain their original identities.

## Source identity, retention and cleanup

[Runtime binding](runtime-binding.json) covers 43 runtime/build/package inputs equal to deployed
5deadfa, map digest `1540583e4a967d4e257f189bfb1e8cb170e7d7247a87bf31a8ae7bfcb4b2ec39`.
The candidate build used `cargo build --release --locked --bins` with default features; the live
driver did not rebuild. Installed migrate/engine/API/recovery SHA256 values match the input
identity and build manifest in live.json; manifest digest is
`829026899165095a63bf60dcd5ae62601ddf68cc22725283e7f42ca296d86ada`.
The later canonical uses 70d86db with unchanged runtime bytes and committed test bytes.
Validation's 110-file operational map digest is
`982d89d5422528430311a834655cfd14979db45b3332b31d316c2f1b616831e7`, with equality observed;
tracker, docs and evidence are publication metadata outside that map. Test bindings include:

| Test file | SHA256 |
|---|---|
| `tests/postgres/harness.py` | `cded62474ac53f3809e6786256b944f4d112771ee893c44c079419a0638b9e01` |
| `tests/postgres/w4_round2.py` | `b130405a6823aa7ddfc690677a261364921d8649e7edce270286b7d733f9d1c1` |
| `tests/postgres/workflow.py` | `bdcdff36ffaeda5c446ce4454bcdfad46b7834878e20b4df7cf380aa0e971cac` |
| `tests/postgres/test_enrolment_retry.py` | `16986834fc1a21db07299d24b9fefc2247f5736c3a1aace2db04f9db49344f39` |

Recorded versions are Nix 2.35.6, systemd 262, git 2.56.0, rustc 1.97.1, cargo 1.97.0,
Python 3.14.7 and PostgreSQL 18.6; version commands exited 0 in the evidence. No review verdict
stands in for these executions.

[Publication transform](publication-transform.json) documents path normalization and selected
live observations, omission of success credentials/unrelated raw streams, private raw retention
and raw hashes. R1 normalization changes only paths plus its publication note; observations,
results, times and digests are preserved. `<product-checkout>`, `<private-driver>`, `<install>`,
`<private-evidence>` and `<run-dir>` are normalization placeholders, not publicly retrievable
artifacts. Private raw argv, separate stdout/stderr streams and action/process/result hashes
allow provenance checks without publishing credentials or absolute private paths. Auxiliary
status wording/transform-label follow-ups below remain deferred; the setup failure is not erased.

Live and R1 cleanup record no errors or alive child PIDs and owned data removed. The live owned
API, engine, provider simulator and rail simulator units were cleaned up; prior setup cleanup is
verified separately. Focused and canonical cleanup classifications above cover the resources
registered by their runs, not arbitrary host resources. This documentation update started no
services and ran no test/build command.

## Limits and filed follow-ups

The supported checkout-only customer correction and CONFIGURATION recovery observation are
recorded; no unresolved P0/P1 hypothesis is established within those exercised arms. This is not
closure of every availability path. Separate startup acquisition failure remains not run, and
other enrolment refusals retain their current semantics pending assessment. The one-primary,
one-designated-synchronous-standby, manual recovery, ordinary bounded pool and offline release
profile is unchanged. No external admission authority, transaction pooling, automatic failover,
runtime drift enforcement, production capacity/outage bound, real provider, purchase or funds
are introduced. Historical uncertainty, sent-COMMIT ownership, generation fences and stop bounds
remain intact. pvr-31s is outside this work.

Filed P2/P3 follow-ups are retained in the [local tracker](../../.beads/issues.jsonl); no tracker
write was made for this report:

- [pvr-yk9](../../.beads/issues.jsonl): assess retryability after other enrolment refusals, including
  pending-capacity and BEGIN failures. Source hypotheses only, no live reproducer claimed here.
- [pvr-opq](../../.beads/issues.jsonl): preserve useful bounded checkout cause context in diagnostics.
- [pvr-m3h](../../.beads/issues.jsonl): wait for fixed-client server backends to finish closing.
- [pvr-mj6](../../.beads/issues.jsonl): clarify the isolated replication-observer bootstrap boundary.
- [pvr-lan](../../.beads/issues.jsonl): retain cleaned response metadata on driver JSON decode failure.
- [pvr-it2](../../.beads/issues.jsonl): explain or remove the unused duplicate build-record digest.
- [pvr-q4o](../../.beads/issues.jsonl): preexisting nondefault-port unit assumption, independently
  demonstrated above; no claim about the W7 timing cause.
- [pvr-8tr](../../.beads/issues.jsonl): declare the first candidate setup result label normalization.
- [pvr-i7e](../../.beads/issues.jsonl): normalize R1's auxiliary acceptance wording while retaining
  its top-level expected negative demonstration and actual exit/time.
