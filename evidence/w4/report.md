# Local funding-to-provisioning evidence

Recorded 2026-11-08. The loopback simulator workflow is implemented and demonstrated on committed
runtime/fixtures `36c9142a6870c10bac055b64d41f423d3f39cc99`, against specification
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

`PROVISIOND_HARNESS_PORT_BASE=55500 nix develop --command bash tools/postgres.sh --seed 20261106
--evidence <run-dir>/final.json` exited **0**, covering all nine required phases. Whole stdout and
stderr were captured separately before their saved statuses were read. [Aggregate](final.json) and
[validation](validation.json) record statuses; each non-control phase and every control child records
successful cleanup after its assertions. All phase inventories match. Source was clean throughout
every final phase;
every runtime, migration, fixture, dependency and tool hash matches the commit. The
[tested source](tested-source.json) records the clean verification tree and exact tested files. Later
documentation/evidence edits preserve the tested runtime, migrations, fixtures, dependencies and tooling.
The [publication audit](publication-audit.json), command `python3 <run-dir>/round4-2cdt51w9/audit-publication.py`,
exited 0 after checking all 54 runtime/fixture hashes against the commit, result inventories, cleanup,
local links and credential/private-path/name leakage.

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
| workflow | 56 pass | 0 |
| default-workflow | 2 pass | 0 |
| controls | 30 expected negative demonstration | 0 |

PostgreSQL tools 18.6, Rust 1.97.1, Cargo 1.97.0, Python 3.14.7 and Nix 2.34.9 were observed.
The nixpkgs pin is `aff8a0b28396750446e5537a96461bc4facdb287`; manifests contain all dependency
versions and configuration. This is one primary and one synchronous physical standby on one host,
not evidence of separate failure domains or production fencing/capacity.

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
34 failed transports and real engine exit 1, with the exhaustion diagnostic and 2023 ms accounted
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
Together with the preserved 23 earlier reds and the preflight-store red, there are 25 original-runtime
regressions.

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
