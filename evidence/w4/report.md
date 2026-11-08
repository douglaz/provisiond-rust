# Local funding-to-provisioning evidence

Recorded 2026-11-08. The loopback simulator workflow is implemented and demonstrated on committed
runtime/fixtures `03516ef719334f1d7a2dd751b37953b84705edc5`, against specification
`ac37c7cae5d3ff746515137774a4cdd5a3531375`. This remains a pre-conformance prototype.

The HTTP path obtains a delayed admission token, enrols a pending tenant, requests one deposit,
credits distinct final Lightning and on-chain payments once, activates and assigns the tenant, admits
an idempotent create, claims and licenses it, purchases once, verifies the resource and atomically
settles its machine and money. Replays preserve operation/payment identities and external counts.
A second independently enrolled tenant establishes authorization and cursor isolation.
The fixture client may resend an ordinary positive keyed deposit/create after a recorded retryable
checkout overload, preserving bytes/key and honoring body/HTTP pacing, at most three attempts.
Expected errors, reads, enrolment, armed faults and defect controls expose their first response.
This changes no application policy or engine startup behavior.
[Six fake-transport fixture checks](fixture-client-check.json), exit 0, verify identity, pacing arguments,
attempt limits and direct-response guards; they are not real-checkout acceptance evidence. All destinations
are literal nonpayable TEST/SIMULATOR values; no real provider, purchase or funds are involved.

## Recorded commands

`nix develop --command bash tools/check.sh` exited **0** (12 Rust tests; formatting, clippy and
spec-lock checks). [Gate](gate.json) records complete command/log hashes and dependency versions.
The earlier `nix develop --command python3 tests/postgres/test_ports.py` exited 0 (three unit tests, including
busy-port refusal at every offset); [unit evidence](port-unit.json) retains its separate stdout/stderr hashes.

`PROVISIOND_HARNESS_PORT_BASE=55500 nix develop --command bash tools/postgres.sh --seed 20261106
--evidence <run-dir>/final-complete.json` exited **0**, covering all nine required phases. Whole stdout and
stderr were captured separately before their saved statuses were read. [Aggregate](final.json) and
[validation](validation.json) record statuses; each non-control phase and every control child records
successful cleanup after its assertions. All phase inventories match. Source was clean throughout
every final phase;
every runtime, migration, fixture, dependency and tool hash matches the commit. The
[tested source](tested-source.json) records the clean verification tree and exact tested files. Later
documentation/evidence edits preserve the tested runtime, migrations, fixtures, dependencies and tooling.
The [publication audit](publication-audit.json) records the final operational hash, inventory, cleanup,
local-link and sanitation checks. The tested commit has 56 operational paths; later documentation
and evidence commits preserve those bytes and do not claim to have rerun the runtime.

The final commands used the default Nix daemon with NIX_REMOTE and NIX_CONFIG unset, with
Nix 2.34.9. [Build environment](build-environment.json) retains the earlier successful read-only
mode observations and the subsequent failed uncached-source lock attempt. No host configuration was
changed, and these tool-mode observations establish no cause for application checkout refusals.

| Phase | Scenario results | Exit |
|---|---|---|
| w1 | 3 pass | 0 |
| checks | 13 pass | 0 |
| commits | 13 pass; 2 expected negative demonstration; 1 not run | 0 |
| regressions | 9 pass | 0 |
| corrections | 8 pass | 0 |
| core | 35 pass; 3 not run | 0 |
| workflow | 67 pass | 0 |
| default-workflow | 2 pass | 0 |
| controls | 30 expected negative demonstration | 0 |

PostgreSQL tools 18.6, Rust 1.97.1, Cargo 1.97.0, Python 3.14.7 and Nix 2.34.9 were observed.
The nixpkgs pin is `aff8a0b28396750446e5537a96461bc4facdb287`; manifests contain all dependency
versions and configuration. This is one primary and one synchronous physical standby on one host,
not evidence of separate failure domains or production fencing/capacity.

## Independent executable lock observation

The `w4_lock_dispatch` selector and canonical workflow cover three real-binary arms: a claim COMMIT
in observed PostgreSQL SyncRep, a late claim reply during lock-loss recording, and an empty reply
while an original record remains pending. Readiness and one represented active worker precede the
fault. The idle advisory-lock backend alone is terminated, with forwarded 57P01 and a new client
driver-death event attributing loss. The claim transport is separate. Each fixed executable logs
loss, stops the active worker and exits 71 under its original 1000 ms record bound. Final observed
intervals from the fixture's driver-death observation were 976/980/970 ms. No guaranteed detection
time is selected. In the SyncRep arm the original claim owner has not completed when the process
exits; no new claim attempt follows detection. Late registration receives the original loss bound.
Reply completion does not bypass the older drain. Existing core tests retain the earlier exact
record identity/deadline, generation, outcome and capacity matrix. Capacity retention in the binary
arm is inferred from the unchanged process-owned claim path plus owner/registration observations,
not a direct semaphore sample. Periodic business scheduling remains unsupported.

[Original-runtime red](red-lock-dispatch.json) records unchanged `eace82c` runtime with only fixture
overlays, exit 1 at the intended missing lock handling assertion after actual SyncRep and attributed
driver death. The original engine stays alive until post-assertion fixture cleanup. The fixed
focused selector exits 0 for all three arms, and the final committed canonical workflow repeats them.
All owned export/build/data cleanup passed. Losing a reply never establishes rollback or absence.

## Surviving provider identifiers

`w4_search_error` now checks seven accepted verification faults and ordinary restart. Independent
provider stages establish exactly one order, accepted creation and returned actual resource ID for
each operation. A separately admitted real order supplies distinct IDs for deliberately ambiguous
or wrong verification reads. Acknowledged stored errors retain all learned IDs under
`details.provider_identifiers`, grouped by order reply/search/get. Authenticated operation detail
and list agree on the full four-field error, false retryability, correlation and applicable throttle/
status details. Accepted knowledge survives typed and untyped errors. Offer/correlator/fee/unit
terms, the open 43-sat reserve and null result persist; no machine, debit, release or second purchase
is authorized by these unverified IDs.

[Original-runtime red](red-provider-identifiers.json) exits 1 at the intended stored-ID assertion
after acceptance, activated throttle and acknowledged unresolved settlement. All 31 runtime inputs
match unchanged `eace82c`; five fixture-only overlays and their exact diffs/hashes are recorded.
The fixed seven-branch focused selector and the final committed canonical workflow exit 0.
Together with the preserved 28 earlier runtime reds, there are now 30; the three tooling reds and
30 sensitivity controls remain distinct. [Source](tested-source.json), [validation](validation.json)
and [regression index](regressions.json) retain historical identities.

The rail-history limit remains deferred: the simulator returns all retained observations while
the client caps a response at 2 MiB. Prior review input reported 8,000 valid observations producing
2,184,000 bytes; that probe was not rerun here. Bounded delivery needs a later repair. These bounded
fixtures make no arbitrary-history or high-throughput ingestion claim. D-6 and production deployment
choices remain Open; automatic reconciliation and the full ENG-07 drill remain unimplemented/not run.

## Coherent search during simulator creation

Search now filters a resource tuple copied under the existing simulator lock. The copy preserves
the account/channel/correlator predicate and authoritative flag while allowing another order to
insert without invalidating the active iterator. Resource dictionaries are never changed after
insertion. Application runtime, conservative unresolved classification, provider retry behavior,
accounting and deadlines remain unchanged.

The existing `--phase workflow --case w4_simulator` selector now includes this separate-process
HTTP interleaving as well as the original concurrent-order regression. With A already created,
the explicit test-only `--search-read-barrier` pauses A's iterator after its first value.
Independent fsync'd events show B's acceptance, creation and returned response before A resumes.
A then returns exactly A within the existing finite HTTP bound; B remains searchable/directly
readable, unmatched account/channel/correlator filters remain empty and both four-stage resources
survive restart and exact durable replay. Both owned child processes are terminated/reaped,
handler stderr is empty and the port is returned to its reservation. Ordinary invocations do not
arm the barrier. The intentional throttle reply is also checked. The complete canonical workflow
retains the full funded HTTP create and partial accepted/create/drop-reply demonstration.

[The original search red](red-search-simulator.json) uses unchanged `5c5124d` simulator behavior
with scheduling hooks only. Its source matches `ddd28b1`; acceptance, ID allocation and effects
are unchanged. The held live dictionary iterator overlaps B's real creation/return, then raises
`RuntimeError: dictionary changed size during iteration`, closing A's HTTP request with
`RemoteDisconnected`. The driver still verifies both durable resources live and after replay,
then exits 1 at the intended HTTP-completion assertion; cleanup passes. Exact original/overlay,
fixture and driver hashes, the small scheduling diff, versions, command/status and independent
events are retained. This is a tooling regression; it is separate from the application-runtime reds
and sensitivity controls. [Corrected evidence](simulator-search.json) records the pre-commit focused
exit 0 on identical operational bytes and the fresh canonical pass on the committed source above.

## Distinct concurrent simulator resources

The simulator uses each order's fsync'd acceptance-event sequence for its resource ID, preserving
that command's account/correlator/channel association. Its inspection snapshot is locked against
concurrent event/resource writes. Application runtime and accounting remain byte-identical to the
preceding implementation (31 runtime/migration/dependency/pin hashes).

The complete workflow includes the focused `--phase workflow --case w4_simulator` regression.
Two loopback HTTP orders reach acceptance before either allocates; both allocations finish before
either resource is created. Recorded barrier events establish that overlap. Each command has separate
request-received, accepted, resource-created and response-returned stages. Inspection retains two
resources, each correlator search yields exactly one, and each direct read has the expected identity.
The test restarts that invocation-owned provider on the same independent durable store and repeats
those assertions with the original resource IDs. Both child processes are reaped and the port is
returned to the invocation's reservation before harness cleanup.

[The original simulator red](red-concurrent-simulator.json) exited 1 at the intended uniqueness/
retention assertion: two distinct accepted commands both returned `SIMULATOR-RESOURCE-5`, and
only one resource survived live inspection and replay. Its original `06986dd` allocation/effect
logic is unchanged; only the two scheduling barriers and coherent inspection snapshot are overlaid.
The manifest identifies original, overlay, corrected simulator and fixture hashes, versions,
commands, observed events and cleanup pass. The corrected pre-commit focused command exited 0;
[committed-source evidence](simulator-concurrency.json) independently records the canonical pass.
There are 30 original-runtime regressions and three tooling regressions (startup cleanup, concurrent
simulator retention and search during creation), separate from 30 activated sensitivity controls. Earlier original reds and
historical W3 evidence retain their identities.

Two earlier complete-command attempts on the preceding simulator commit `ddd28b1` remain fail: the first passed six phases
and the new concurrency, funded HTTP and dropped-reply arms before the existing resource-admission
concurrency assertion received 202/503 instead of 202/409; the second stopped in checks when a
duplicate pg_receivewal process exceeded its existing 5-second shutdown wait. Both cleanups passed.
[Validation](validation.json) preserves commands, statuses and raw-log/manifest hashes. No source,
policy or expectation changed for the fresh repeats. The checkout-refusal cause is unestablished;
pg_receivewal wait hardening remains deferred. These failures are not earned regression reds.

## Reconciliation-pending retryability

WIR-9b/API-51 make the operation view authoritative and require false for
`needs_reconciliation`. The restart producer now generates `retryable: false` for an unresolved
create, preserving OPS-16's existing-error rule and the null-marker failed-create exception's true
advice. Classification, money, markers, guards, revisions, external calls and bounds are unchanged.
The shared HTTP helper requires the error and view to agree as well as match each expected outcome.

[The original-runtime red](red-restart-retryability.json) exited 1 at the intended equality
assertion on unchanged `8104047507bd696b742d2923384781036130e568` runtime. One workflow-only
overlay matches the final committed fixture; all 31 runtime/migration/dependency/pin hashes match
the original source. Its patch and build identities are retained. The create was independently
HTTP-enrolled, funded and admitted. Its marker and licensing acknowledgement were observed at the
step-9 hold; signal 15 actually terminated the engine, leaving the running claim unchanged. The
successor completed its one-row restart pass. Both authenticated endpoint responses were captured
before the assertion: view false, error true, with the same operation/correlation, 43 sats reserved,
no machine/debit and no key/order call. Cleanup passed. This is an earned runtime regression,
separate from the thirty sensitivity controls and the earlier error-shape regressions.

The same focused command, `nix develop --command python3 tests/postgres/harness.py --phase workflow
--case w4_restart_error --seed 20261106 --evidence <run-dir>/focused-retryability.json`, with slot
55500, exited 0 on the corrected pre-commit bytes. The final canonical run independently tested
those committed operational hashes. The marked-create error/view are both false; null-marker
restart and live pre-step-9 worker failure remain failed, fully released and true. Accepted-provider
ambiguity, survived cut-offs and successful null-error reads retain their existing correct advice.
The existing restart selector includes these arms; canonical execution already reaches them.

## Complete operation error objects

WIR-9/10b and DOM-17 require an operation's non-null error to contain exactly `kind`, `message`,
`retryable` and `details`. Both ordinary-restart branches now include an empty details object;
survived-stop recording uses the existing wire error constructor to supply a nonempty human message.
The kind and cut-off reason remain unchanged. The retryability correction above follows the
pinned precedence; OPS-16 preserves an existing stored error and its evidence. Classification,
markers, revision/result behavior, claim/generation guards, redaction, money, retry/stop bounds
and external dispatch are unchanged.

The HTTP-funded/admitted preflight exhaustion/restart and survived-stop arms read the completed
outcome through both authenticated operation endpoints. The shared assertion captures detail and
list responses before checking exact error keys, string/message/boolean/object types, expected
kind/retryability/details, and the operation view's retained correlation identity. An inner envelope
or correlation field fails the exact key check. The existing marked-create crash/unresolved and
ambiguous provider-stop arms use the same assertion; ordinary business and accepted-provider errors
and successful null-error operations are also checked. Existing full-release/reserved-money,
zero-debit and single-order/no-order assertions remain.

[Original restart](red-operation-restart.json) and [original survived-stop](red-operation-stop.json)
each exited 1 at the intended HTTP shape assertion with cleanup pass. Both authenticated responses
show the missing field: details on restart, message on stop. All 31 runtime/migration/dependency/pin
hashes match `758649f72411022476236b369a95685052e1e955`; only three observation fixtures changed in
the live handoff checkout before the runtime fix. The manifests preserve their exact dirty fixture
identities. The original shape-check logic remains in the stronger final helper. The audit
reconstructs those original overlay hashes from the first implementation commit, excluding two
later checks in other cases; the original manifests retain their historical fixture identities.
These are original-runtime regressions, separate from the thirty sensitivity controls.
The focused `--phase workflow --case w4_restart_error` and `w4_stop_error` selectors isolate the
existing arms, with no new fault scenario. The five-arm preflight, ambiguous-stop and full-workflow
focused commands exited 0; final nine-phase verification independently tested committed source.

## Worker preflight and startup cleanup

OPS-49 now covers each worker preflight SELECT at the account/request, tenant-status and
channel-closure sites. Each attempt includes checkout/new-connection checks/query and releases
its session before backoff, within the existing 2000 ms budget. Serialization remains uncharged;
constraint/generation exceptions remain distinct. OPS-43 first-refusal order is unchanged and
provider calls are outside the retry unit. Stop cancellation closes an in-flight read and uses the
existing bounded shielded record. API components do not inherit the loop.

Five final HTTP-funded/admitted cases prove the fault activates on the intended read transport.
Two or more failed transports leave the same claim running, 43 sats reserved, no marker/debit
and no provider request. Recovery at each site completes steps 1–9 once and makes exactly one
order before normal atomic settlement and keyed replay. The sustained account-read fault produces
35 failed transports and real engine exit 1, with the exhaustion diagnostic and 2018 ms accounted
against its 2000 ms bound. The operation remains running until ordinary restart classifies it
under OPS-15's null-marker exception, releasing the commitment without any debit or provider call.
A survived stop during tenant-read retry records failed/cut_off under the existing 1000 ms bound,
with full release and no further read attempt or provider dispatch.

[The original-runtime red](red-preflight-store.json) uses all 31 unchanged runtime/migration/
dependency/pin files from `ca36547551f5d0d1d14ca3180656ec362f8f985c` in a private source archive.
Five exact fixture overlays provide the new test and transport observation. It exits 1 at the
intended live-claim assertion after one real failure: the original runtime instead settled internal
and released its commitment. Cleanup passed. This is a runtime regression, separate from the 30
sensitivity controls. The pre-commit focused corrected case also exits 0; final committed-source
verification independently exercises all five arms.

[The tooling red](red-startup-cleanup.json) leaves the original startup fixture unchanged in that
same private source. Startup assertions pass and its nine invocations use the separate default
binary, but cleanup exits 1 at the intended missing-directory-removal check. The normal target is
gone while `default-startup-target` remains. After all children were reaped and assertions finished,
only this reproducer's exact leftover target and private source copy were removed; the original
cleanup result remains fail. The corrected focused startup command exits 0. In the final full
workflow, both outputs were created/used and explicitly observed absent after child completion.
Earlier cleanup passes covered registered resources only; they never proved the missing startup
output was removed. No historical invocation data was deleted.

## Provider observation and fixture modes

The confirming `/get` captures the provider-state observation instant before subsequent constraint
I/O. The retained verified outcome carries that instant across normal settlement, shielded stops
and whole-transaction repeats. State and its observation commit atomically with the existing
machine/money effects. The local billing seed remains at billable creation, offset only by an
actually debited first unit's copied seconds; it no longer relies on equality with observation time.
Zero-unit and no-current-rate assertions independently check the seed and high-water mark.

The three final observation arms begin with real HTTP funding/admission. Independent, fsync'd
provider records identify the successful `/get` and this operation's held `/constraints` request.
A controlled delay exceeds public second precision within finite driver call bounds. Persisted
and HTTP observation timestamps identify the earlier read. Losing a real settlement acknowledgement
forces a whole-transaction repeat with unchanged state/seed/revision/money and no repeated provider
call. A bounded constraint timeout and a survived stop during the held read each still settle
successfully with a null constraint date and the original observation. Cleanup passes in all arms.

[The new original-runtime red](red-provider-observation.json) exits 1 at the intended timestamp
assertion after the attributed delay and actual lost-acknowledgement repeat, with cleanup pass.
All 30 runtime/migration/dependency hashes match `d62e1f4` (runtime-equivalent to `62333c6`);
six fixture-only overlays supply observation/testing. It ran in the unchanged handoff runtime before
the correction. The pre-commit focused observation and regression-sequence commands then each
exited 0; [validation](validation.json) retains their distinct dirty-source identities and log hashes.
Together with the preserved 23 earlier reds, the preflight-store red, the two operation-error
shape reds and the restart retryability red, that earlier stage had 28 original-runtime regressions.
The two publication-repair reds above bring the current portfolio to 30.

The default API startup check now builds into a separate target directory. Effective configuration
records API/engine/migrator executable hashes and feature lists; invocation events identify the
executable used, including the observation probe. All nine default startup invocations (eight
negatives plus schema-only) use no features. The restored API and subsequent lifecycle cases keep
the original `store-test-hooks` build. The observation cases run after those lifecycle cases in the
actual aggregate order. The independent default workflow uses no test features and still rejects
defect activation through environment alone. No new runtime observation hook was added. Prior
evidence's feature field did not identify the API overwritten by its startup rebuild; validation
retains that historical metadata limitation without rejecting its lifecycle outcomes.

## Corrections and sensitivity

Seven additional original-runtime regressions on `efd0be7` each exit 1 at their intended assertion
with cleanup pass, and pass in the final workflow. Fixture-only overlays add observations and tests;
all original runtime/migration/dependency hashes are checked against that commit. Their raw source
identities remain intact in the manifests. They cover trusted Lightning underpayment and overpayment,
the complete operator list, required startup configuration, idle watcher failure, real housekeeping
checkout exhaustion, and sent-COMMIT/request ownership during failure.

The actual-value cases retain requested 120 while crediting Lightning 41 plus on-chain 66, or
Lightning 121 plus on-chain 3. Atomic linked payment/ledger values, cumulative activation, healthy
assignment and concurrent delivery/replay leave two credits only. Operator checks exercise 88
active/pending plain/query-qualified requests with exact 404s and no request business BEGIN, receipt
or provider effect, plus preserved customer reads and OPTIONS. The raw principal_status trace field
copies the initial enrolment response; the manifest explains the active first 44 requests (funded()
explicitly polls active) and newly pending last 44. Default-binary startup checks refuse
eight invalid required address configurations before readiness and preserve standalone schema checks.

A malformed simulator response ends the real watcher with no customer traffic. A held 1.6-second
balance reply crosses housekeeping's actual 250 ms checkout deadline while remaining below its
2000 ms call bound; the held request completes 200, and the API exits 1 with a housekeeping diagnostic.
Neither path panics or aborts. Owned handlers drain for one second, then are cancelled/joined; periodic
tasks are cancelled/joined before stores drop. A withheld sent-COMMIT reply and a partial HTTP request
exercise both owners: after handler cancellation the live process still retains the checkout, then
exits 1 at the existing Process bound with unresolved work. Restart and same-key replay recover the
one durable deposit. This is bounded termination with retained uncertainty, not in-process recovery
or a claim that a waiting COMMIT rolled back.

[The sixteen historical original-runtime regressions](regressions.json) reproduce intended assertion failures with
exit 1 and successful cleanup on the original transferred runtime, then pass in final workflow/core
evidence. They cover native reserve offsets, empty user data, exhausted-ceiling precedence,
query-qualified token abandonment, failed-auth source limiting, polling-window budgets,
delay-independent token pacing, decoded queries with raw fingerprints, rational/floor/clamp settlement,
unavailable and absent opening rates, accepted admission-record integrity and separate migration
transactions. Four accepted-record groups exercise 26 corruptions before any driver read.

Original sources are identified as initial commit `128d9d69c4a94dc418a93c13fc97ff5e62a88fa8` plus
the 18 transferred file hashes, with explicitly recorded fixture-only observation overlays. Some
probes borrow Git metadata from `194cb29` while their runtime hashes still match that original source;
their metadata is retained and never relabeled as clean initial/current runtime. The
[preservation inventory](source-preservation.json) verifies all 18 initial contents.

[Thirty sensitivity controls](final-controls.json) each exit 1 at their exact intended assertion after
the required fault observation; their parent exits 0 only after all 30 are recognized and cleaned up.
They are separate from real original-runtime regressions. W4 controls expose split payment/credit,
unacknowledged dispatch and overload after business entry. Default-feature workflow independently
shows environment variables cannot activate opt-in defects.

Nonfinal rail assertions wait for the exact observation's completed ingestion decision. Overload
evidence includes a lost first reply, a real checkout-refused old-key resend with actual BEGIN counts,
then recovery of the existing result. Settlement checks partial/zero debit, exact native partitions,
opening conversion evidence, first-unit references and repeat stability. Physical migration evidence
includes separate transactions and populated release-1 preservation. The worker/core evidence retains
the W1–W3 stop, lock, sent-COMMIT, cancellation and startup inventory.

## Limits and earlier failures

The first operation-error canonical run on `55f629b` exited 1 in a new test assertion: the controlled
worker failure correctly reported `network`, while the test expected `internal`. All first six phases
and workflow cleanup passed. Correcting only that expectation yielded a focused 56-scenario workflow
exit 0, followed by the final nine-phase exit 0 on `3eeac6f`. The failed run remains fail and is not an
original-runtime regression. The pre-commit fast gate initially used the existing ignored repository
build target; both committed-source gates used the invocation-owned external target, which was
removed after all commands completed. No inherited build output was deleted.

The first new canonical attempt on `6c8a1be` exited 1 in the workflow phase at the preserved
unauthenticated `/v1/provider-accounts` 404 assertion; the first six phases passed and workflow cleanup
passed. Restoring dedicated namespace hiding and adding root/tenant checks produced focused exits 0
and the final canonical exit 0 on the commit named above. That earlier run remains fail. Two probe
development attempts (an event-field serialization collision and a completion assertion including
previous session use) are excluded from earned reds. The initial fast gate's clippy exit 101 is also
retained; the corrected focused and final gates exited 0.

The accepted-and-created dropped-reply test is deliberately partial: independent fsync'd provider
effects survive restart, the application remains unresolved with its reserve, and there is no blind
second purchase. Automatic ENG-05 reconciliation, later assignment sweeps, cancellation execution,
settled temporary-key cleanup, ongoing metering, later runway re-derivation, real market rates,
multichannel providers, recovery, full saturation and production availability are **not run** here.
No-healthy activation and accepted suspended-tenant attach with a queued cancellation obligation are
demonstrated; their later workers are not. General W5–W11, deferred W3-E and pg_receivewal wait hardening
remain outside this change. D-6/7/8/9 remain Open.

[Validation](validation.json) retains earlier failed canonical attempts, rather than counting
them as passes: a pre-fault setup checkout refusal, a control whose standby readiness did not activate,
two physical base-backup fixture timeouts, additional bounded checkout refusals during fixtures,
a lock-observer fixture race and a separate 200 ms client-disconnection precondition timeout.
Their recorded completed phases do not establish final
acceptance. Physical fixture command/readiness/shutdown bounds and the narrowly bounded, recorded
API deployment attempts are disclosed in scope/D-20. No application checkout/stop-record policy,
durability setting or pg_receivewal behavior was changed to accommodate setup delay. The underlying
cause of occasional host setup delay is not established.

Superseded focused runs also remain failures: initial missing probe environment, cold setup refusals,
and an original-source probe whose intended assertion occurred but whose 15 s shutdown wait failed.
That probe is excluded from the 16 earned reds. A subsequent recorded observation found its owned
process and pidfile gone, shutdown logged and all ten ports bindable; it does not turn raw cleanup
failure into a pass. Validation retains these summaries and the observation hash.

In the lock-observer failure, the fixture invoked observers before the killed lock connection's
`driver_dead` event, and its claimed observation boundary never activated. Three related fixtures
now wait for that probe's post-termination client-disconnection event before invoking the observers;
queued claim, stop-bound, admission and accounting assertions remain unchanged. That failed run's
cleanup reported a retained probe process reference and remains fail. A later observation found all
111 recorded owned processes absent, both node pidfiles absent, shutdown logged and all ten ports
bindable. This fixture correction does not change runtime lock-loss handling or deferred W3-E work.
A separate stop fixture also waits for the actual client disconnection instead of assuming delivery
within 200 ms; its already-running 1000 ms record deadline and all settlement assertions remain
unchanged. Its failed run retained its actual nonzero cleanup status; later owned-process/node
observations establish termination without relabeling that status.

One failed invocation attempted a process-local Nix `min-free=0` override. The daemon warned that
it ignored the restricted setting. Client configuration inspection printing `0` was not proof of
daemon application. No host configuration was changed and no causal attribution to cache collection is made.

A separate [cold-checkout diagnostic](cold-checkout-diagnostic.json), exit 0 with cleanup pass,
recorded 120 shuffled same-binary trials: 60 default-runtime processes with 18 threads and 60 with
two configured runtime workers (four process threads). Both modes had zero deadline refusals;
observed median/max were 68.5/103.3 ms and 66.5/92.6 ms respectively. It did not reproduce the
earlier failures or identify their cause. A full-core two-worker fixture experiment still failed a
cold checkout; it was discarded.
The final application/fixture keeps ordinary thread settings and all pool/permit/deadline values.
A later focused core attempt stopped at a negative API startup checkout refusal before reaching
the intended missing-parameters diagnostic; it remains fail with successful cleanup. The affected
queued-claim case was verified separately before committing its API setup replay.
Two earlier diagnostic setup attempts collected no trials and remain failed; they are not evidence
of checkout behavior or part of the acceptance count.

Only sanitized manifests and summaries are committed. Raw protocol/application logs, credentials,
backups and private responses remain in the isolated owned test area; each published trace/log has
its retained hash. Historical W3 evidence keeps its original identity.

Optional suggestions left unchanged: the older overlay diff could be embedded alongside its hash;
the older focused D-26 prose could include the port environment; the simulator row label could name
tooling more explicitly; an explicit expected HTTP status in operation_reads; one
expression for refresh status/retryability or an explicit untested-refresh label; distinct human
restart messages; an explicit successful-result check in the null-error helper; separate documentation hash lists; fixture
clock-domain/default-feature labels; operator trace provenance; the invalid-settlement mode name;
per-unit retry-budget wording; last-error diagnostics/stderr duplication; stop-count pairs; and
redundant aggregate identity fields. These add no acceptance obligation or tracker item.
