# Packaged historical W7 inputs

A fresh single-branch copy of unchanged master could not run required iteration setup because two executable test inputs existed only in old Git objects. This is the older P1 pvr-ag5, under the owner clause “a test or gate that fails for the wrong reason.” The correction stores the exact small inputs in the tree and consumes checked bytes. It changes test tooling only; product code, production bounds, scenario assertions and pass requirements are unchanged.

## Inputs and provenance

The original runner selected 41 baseline files at `8339aae5ecf20e6337f4d5d5e22ec4f4573efbfb`: 588433 bytes. The complete historical `tests/postgres/w7.py` at `06b7636cfed13cd4d6049dbe316ed8b191c93fa2` is 75601 bytes. Total: 42 payload files, 664034 bytes, measured before selecting tracked files. Including the manifest and provenance text, the fixture tree has 44 files and 675323 bytes. [inputs.json](inputs.json) records every original path, source commit, blob, length and SHA-256. Every payload was independently compared with its original Git bytes.

The loader validates fixed source identities, path containment, duplicates, lengths, Git blob hashes and SHA-256 before candidate builds. Both callers consume its returned bytes. The original AST extraction of the historical operator function, five assertion strings, control recognition, candidate binary separation, protected before/after map and cleanup are unchanged. The committed manifest is the hash authority; this does not attest against coordinated edits of both that trusted record and its data. Neither required-input reader has a historical-object lookup, network fallback or automatic repair.

These are this repository's own historical inputs. The original baseline root has no licence file, and its package is `publish = false` without a declared licence field. Original metadata and comments are preserved, no licence grant is introduced, and no third-party dependency source is copied. `Cargo.lock` retains dependency metadata. Standalone replay tools are separate from the two corrected required readers.

## Fresh-copy red, green and sensitivity

The real contributor command was `nix develop --offline --command bash tools/iteration.sh --jobs 3 --seed 20261124 --evidence <owned-output>/iteration.json`, at port base 25500. Fresh copies used real single-branch Git transport, without tags, pull-request refs, alternates or cached historical objects. An unprivileged network namespace preserved UID 1000, enabled only loopback and had no external routes after checkout. Nix and Cargo dependencies were already provisioned; this is not dependency-free setup on a bare machine.

The unchanged-master invocation at `d7ad22c74cc0ea3346aabf6ed9ae5986269b1a25` failed: exit 1 in 74.238451234 seconds. Its quick gate and 58 Python checks passed. Then `runner.py:125` ran `git ls-tree` for the missing baseline and exited 128 before PostgreSQL phases started. There was no retry, and owned build cleanup completed. A supplemental baseline object-absence observation was recorded externally after the run; the native command and offline runner did not change.

The corrected fresh copy at `a27b9949e736a372fcd5d1f35be31ad2188b2e18` passed: exit 0 in 794.760602724 seconds. Both original commit objects remained absent before and after the invocation. All 44 ordinary controls and five historical W7 candidate controls were recognized at their exact intended assertion failures. All 71 native cleanup manifests passed. The candidate source's 41-file map before and after execution matched the original bytes, and its verified manifest includes the exact CLI input. No known-failure exception or rerun was used. [fresh-checkout.json](fresh-checkout.json) retains actual exits, intended messages, cleanup, hashes and source proof.

Three separate scratch copies exercised the real entrypoint with unchanged manifests:

| Deliberate input change | Actual exit | Seconds | Observed error |
|---|---|---|---|
| Alter baseline `Cargo.toml` | 1 | 49.842273411 | Historical-input integrity error naming the path |
| Remove CLI `w7.py` | 1 | 48.638658973 | Historical-input missing error naming the path |
| Alter CLI `w7.py` | 1 | 48.307046935 | Historical-input integrity error naming the path |

Each failed before database scenarios began; owned build cleanup completed. These are expected negative demonstrations, not successful iteration invocations. The product checkout was never altered for them.

## Ordinary-checkout validation

Only after live acceptance was perfect, the ordinary-checkout iteration exited 1 in 129.395170911 seconds at port base 25500, jobs 3. It failed during setup at `runner.py:43`: the manager-selected private harness root produced a 108-byte `ports.sock` path, too long for the Unix socket API. Both inputs had loaded and the 41-file protected source map matched; the quick gate and 58 Python checks passed. No database phase began, and owned build targets were removed. This is a failed invocation, not a timing failure, and it was not rerun.

Only the private harness root for the still-required single canonical invocation was shortened. Repository code, checks and bounds were unchanged. P3 pvr-lba tracks the late configuration diagnostic, rather than a fixture-loading defect.

The one full canonical invocation exited 1 in 2519.849645005 seconds, jobs 1, at the same clean code tip. Both ordinary commands used seed 20261124 and port base 25500. There was no suite retry. Core failed in stops()/poll_lock, a different site from pvr-wlm, after the generation-advance reply-loss check passed. Probe 203 reported the original stop bound expired with records unresolved and exited 71; the fixture hit its 12-second reply deadline and cleanup retained fail. P2 pvr-s48 tracks this newly observed failure in unchanged core/runtime code, with its cause unestablished and no correction included. No pvr-aqm failure occurred in the canonical invocation. The failed ordinary iteration did not reach database phases. [core-failure.json](core-failure.json) retains the relevant observations and bounded next evidence. Historical results elsewhere are unchanged.

| Canonical phase | Result | Exit | Seconds |
|---|---|---|---|
| w1 | pass | 0 | 9.762053 |
| checks | pass | 0 | 33.236843 |
| commits | pass | 0 | 13.283856 |
| regressions | pass | 0 | 12.000740 |
| corrections | pass | 0 | 9.711092 |
| core | fail | 1 | 88.465575 |
| workflow | pass | 0 | 291.689366 |
| default-workflow | pass | 0 | 19.190364 |
| w5 | pass | 0 | 110.207949 |
| w6 | pass | 0 | 632.481989 |
| w7 | pass | 0 | 669.633117 |
| controls | pass | 0 | 546.041026 |

[validation.json](validation.json) keeps each native phase, candidate results, cleanup, source/log hashes and machine endpoint readings. Load averages, available memory and disk readings are observations, not an explanation of a deadline. [source-binding.json](source-binding.json) binds the tested operational files; subsequent publication edits are documents, evidence and tracker only. The final quick gate is run separately at the publication tip.

## Original reference audit and limits

[references.json](references.json) classifies every member of the original 135-ID cohort separately: two required-gate input IDs corrected here, four executable standalone historical replay inputs left unchanged, and 129 citation-only IDs left unchanged. The four standalone IDs are `aceea4eea8382c8d3b6fa554706c54920999369c` (W5, master ancestor) and `ac83e172dae1d3c0cc7934c9633a403cfca0a679`, `96c6001999a50fb4b3247b77ba29e60500baf72f`, `ab014865b46f1f0c15c5ead33a85f24fdbaa3c81` (W6, already pvr-scn). None is called by the required iteration/full-suite paths. This correction does not restore old history or fix standalone replay/citation availability; pvr-scn and pvr-x16 remain open. This is the original audit cohort, not a count of every hash in the new manifest.

pvr-ag5 is closed on this branch. P3 pvr-tel, `review-nit`, tracks consistent wording for directory/unreadable-input OSErrors at `historical_inputs.py:14-20`. Those errors already fail nonzero with a path before builds, so no false pass is shown. P3 pvr-lba, `review-nit`, tracks a clearer early unsupported-path diagnostic. Both are deferred and uncorrected. pvr-aqm, pvr-wlm, pvr-kd7 and unrelated follow-ups remain open. No requirement coverage or conformance status is upgraded, and no production availability claim follows from this test-input packaging.

Two further P3 `review-nit` follow-ups remain uncorrected: pvr-7ty pairs the internal evidence key `fixtures-F3` with its already filed issue pvr-s48; pvr-h0e corrects the recorded diagnostic citation, whose range 14-20 misses the wrapper raise at line 21. These are navigation/citation issues only. No evidence field, native observation, verdict, bound or check is changed.

## Stored bytes at checkout

A contributor attributes file containing `*.py text eol=crlf` converted the
stored CLI input at checkout while leaving its Git blob unchanged. The repository
had no `.gitattributes`; this observation requires a contributor line-ending
setting and does not measure the default failure rate. The uncorrected loader
failed clearly, as its strict hash check requires.

The repository now marks `tests/postgres/fixtures/w7/** -text`. Normal and
CRLF-configured fresh single-branch checkouts both loaded all 44 files
byte-identical, with both historical commit objects absent. An altered scratch
input still failed with the integrity diagnostic. All payload and manifest bytes
remain unchanged. [The focused record](checkout-attributes.json) retains statuses
and times. The iteration check and full suite were not rerun for this one-line
attributes correction; it changes no test code, stored byte, bound or assertion.
The earlier failed suite invocations remain failed.


## Validation after the control correction landed

PR #17 landed at `ebeada41b05b5a0e21a27566aa2d1b0e116585ed`. This branch was rebased from `75dd6232d3be07ba094e1f91bef6353b67d867e9` onto that master and validated at `22655c13635b6a81a08d739a4a7dfdfa4c2b89c8`. The earlier exit-1 iteration and full-suite runs above remain historical observations. Their seven original JSON files and hashes are unchanged, and the earlier report bytes survive as an exact prefix. Their results were not replaced by the following runs.

The tracker reconciles 166 records from the old branch and 155 from the new master into 168 unique records. All portable record fields survive. The native `br 0.7.6` JSONL-only label update also omitted the legacy machine-local `source_repo_path` field from export; the before generations are retained privately, and an independent field comparison found no other change beyond the approved label and timestamp. `pvr-fk50` remains P3 and now has label `review-nit`.

All 44 files in the stored-input tree, including the manifest and README, and the root attribute file remain byte-identical. The manifest still binds the two inputs to `8339aae5ecf20e6337f4d5d5e22ec4f4573efbfb` and `06b7636cfed13cd4d6049dbe316ed8b191c93fa2`. The combined 45-path SHA-256 map is `6875c44588dd054c2ef35118a59a61fcc7ca23beef26f6eec8bf69c3f2eeeded`.

In the fresh single-branch clone, the new master and rebased implementation commit are ancestors (exit 0). The five old branch commits and both historical input objects are absent (object and ancestry lookups exit 128). Old branch ids remain only as historical evidence citations; no executable source or evidence command field reads them. Both required inputs load from the checked stored bytes without those objects.

| Additional invocation | Actual exit | Seconds | Failed phases |
|---|---|---|---|
| iteration-a1 | 1 | 570.334149894 | controls |
| canonical-a1 | 0 | 2117.413906425 | none |
| fresh single-branch offline iteration | 0 | 596.453952266 | none |

Ordinary checks use short private harness roots, port base 25500 and seed 20261124; the iteration uses jobs 3 and the single full suite uses jobs 1. The fresh clone runs the actual iteration with external network disabled and only local loopback available. Exact commands, failures, cleanup, ancestry and source bindings are in [rebase-validation.json](rebase-validation.json). Any failed invocation remains failed. No validation result is inferred from machine load.

The ordinary iteration failed at `tests/postgres/w7.py:284` during the supersession control's recovery establish call, before its intended observation or assertion. Recovery exited 1 with a whole-pool checkout deadline; cleanup passed. This additional setup observation is recorded against open P2 `pvr-kd7`. The original issue records markers; this is a different case at the same recovery-establish checkout boundary, with no shared underlying cause established. No rerun or correction was made. All 21 ordinary W7 rows passed, with 43/44 ordinary controls and all 5 stored-input controls recognized in that invocation. The subsequent single full-suite run passed all 12 phases, recognized all 44 ordinary and all 5 stored-input controls, and recorded passing cleanup. That later observation does not explain the earlier failure.

No new three-review round was needed. The sole operational change inherited by this rebase is the exact `tests/postgres/w6.py` correction already reviewed in PR #17; its four evidence files were inherited too. New work is tracker reconciliation, the scope entries and this additional validation record. The old fixture implementation, its assertions and all stored bytes are unchanged. The old source map remains a record of its original run; the new operational map is in the additional record.
