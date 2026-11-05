# AGENTS.md

## What this repository is

The Rust implementation of the provisiond specification, which lives at
`github.com/douglaz/provisiond` and is pinned in `spec-lock.json`. The pinned requirement bodies own
every behavioural rule. This repository holds the runtime, database client, migrations, deployment
packaging, provider and rail simulators, and integration tests against real PostgreSQL.

It is a pre-conformance prototype. `docs/scope.md` says what is and is not claimed;
`docs/conformance-map.md` traces requirements to symbols, tests and evidence; `docs/decisions.md`
records implementation choices the specification leaves open.

## Rules

- **Cite the pinned requirement.** Code and tests that implement a rule name its requirement ID, and
  the conformance map row points at the pinned file and line. Read the requirement's complete body at
  the pin before implementing it; superseded ADR paragraphs and research records are not obligations.
- **Do not change policy to pass a test.** When the implementation conflicts with the specification,
  stop the affected path and record the requirement, a minimal reproducer, the impact and the
  smallest proposed decision. Specification changes go to the specification repository.
- **Preserve the accepted profile** in `docs/scope.md`: one primary and one designated synchronous
  standby, manual recovery, ordinary bounded pools, offline releases. Do not add automatic failover,
  external connection admission, transaction pooling or runtime drift enforcement.
- **Record open choices in `docs/decisions.md`** with the evidence that settles them: exact versions,
  commands, exit statuses and evidence paths.
- **Status words are earned.** A conformance-map row moves off `not implemented` only with an
  implementation symbol, and to `demonstrated` only with an evidence reference to a recorded run. Test
  results use `pass`, `fail`, `inconclusive`, `not run` or `expected negative demonstration`; a test
  whose fault never occurred is not a pass.
- **No real providers, purchases or funds.** Provider and payment-rail behaviour runs against
  simulators unless the owner approves a specific pilot with a spending limit.
- **Isolate test resources.** Every PostgreSQL instance a test starts gets its own ports, directories
  and credentials, and cleanup removes only what that run created, after its assertions finish.
- **No secrets in the repository.** Evidence under `evidence/` is sanitized manifests and summaries.
- **No model or assistant names, trailers or session links** in commits, pull requests, code or docs.
- **Branches and pull requests only.** Nothing goes straight to `master`; merges need the owner.

<!-- agent-discipline-v1 -->

## Working agreement

- Verify edits in the resulting diff, including intended additions and removals.
  For scripted replacements, assert the expected matches before writing and use
  literal replacement values.
- Stage only intended changes and inspect the staged diff, including any hunks
  that were already dirty. Check the commit command's exit status and inspect the
  committed content; a clean worktree alone does not prove the change landed.
- Run the complete gate without piping it through an output filter. If capturing
  output, redirect the whole gate to a fresh log, save its status before reading
  the log, and return that status. Use the repository's existing runner when
  timeout or process supervision is needed; keep that machinery out of this file.
- Report the command and observed exit status when claiming work passes, is clean,
  works, is verified or is done.
  Interrupted or incomplete execution is unverified. A clean review is independent
  judgment, not evidence that a build or test ran.
- Support claims about tool behavior with a reproducible command, relevant version
  and mode, and observed result; capture stdout and stderr separately when the
  distinction matters. Label unmeasured claims and distinguish an observed result
  from a cause established by a controlled comparison or documentation.
- For a bug regression test, demonstrate that it catches the bug on unfixed code
  and passes with the fix. Check the failure is the intended assertion, rather
  than setup or an unrelated error. Use a disposable environment for a deliberate
  break that could affect live systems or funds; otherwise report that the red
  run was not performed.

Gate for this repo: `nix develop --command bash tools/check.sh`
<!-- end-agent-discipline -->
