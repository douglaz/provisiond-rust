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
