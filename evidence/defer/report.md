# Defer cancellation supplement, 2026-11-06

The focused repair is demonstrated on `966751005c34721fecefedff7a210a390d657cb8` against
specification `ac37c7cae5d3ff746515137774a4cdd5a3531375`, from base
`8d7e89110898ef8ad7addfc291adaa5bee7e8625`. Tested source-bundle SHA-256:
`9b5410a0395126ab6fc9c747fd8d29c81c2aaeb5e83331fb0642bfabdd920ce2`. Source was clean.
The later documentation/evidence commit preserves all tested runtime, migrations, dependency
inputs and fixtures; [source preservation](source-preservation.json) records their hashes.
[Historical W3 evidence](../w3.md) retains its original implementation identity.

`Engine::defer` now submits its existing transaction loop and claim-specific retirement to the
surviving process supervisor. It retains the original worker permit across replacement until
acknowledgment. Receipt cancellation registers no user stop. Acknowledged false remains a normal
guard refusal. Required fatal exits do not depend on result receipt delivery.

The required complete commands, with the capped user unit, private harness/TMPDIR and explicit
port base 55430 recorded in [validation](validation.json), were:

```sh
nix develop --command bash tools/check.sh
PROVISIOND_HARNESS_PORT_BASE=55430 nix develop --command bash tools/postgres.sh --seed 20261106 --evidence <run-dir>/defer-repair/final-3/w3-defer-final.json
```

Both exited **0**. The [aggregate](w3-defer-final.json) contains all seven phases, 81 positive
entries and [27 expected negative controls](w3-defer-final-controls.json), each child exiting 1
at its exact intended assertion after independently observed activation and successful cleanup.
Every final positive/control cleanup passed. Separate full stdout/stderr hashes, exact versions,
features, commands/environment and complete source inventories are retained in the manifests.
Raw logs remain outside git. No real provider, purchase or funds were used.

[Core observations](w3-defer-final-core.json) show four successive cancelled defers using three
slots. Each actual sent COMMIT reply remains held with one worker/two free slots before and
after joined waiter cancellation, with no stop or simulator effect. After acknowledgment the
row is queued, zero workers remain and all three slots return; another claim succeeds. Delayed
true and false completions retain A's permit even after B replaces the registry entry: one
worker/one free slot pending, then one worker/two free slots after completion. B's row, money
and exclusion are unchanged. Stale defer returns false; B's own defer returns true and capacity.
Cancelled and live unpolled supersession receipts produce autonomous exit 71, one generation
refusal, no retry and unchanged row/money/exclusion state.

The [final red manifest](w3-red-defer_cancel.json) also retains the original-runtime reproduction.
Its disposable clone used the exact base runtime, with only five fixture files dirty; all 17
runtime/migration/dependency hashes match the base. It built and completed setup, observed the
attributed COMMIT hold, joined cancellation with the process alive, released the reply and
observed acknowledgment and queued state. It exited **1** at
`acknowledged cancelled defer must retire original worker capacity`, with the old worker/slot
still held. Cleanup passed. The retained fixture patch hash, complete source inventory, bundle
hash, honest dirty state, command/status, activation/DB/ownership observations and raw trace/log
hashes distinguish it from the final-source control. Repetitive owner-poll responses are summarized
in committed red evidence; the complete trace remains at its hashed private path.

An initial fixture compile failure occurred before fault setup and is retained privately. The
first canonical attempt on `35f8616` exited 1 because its generation fixture waited for an API
reply after the required autonomous fatal exit; cleanup also failed due the unset expected child
status. [Validation](validation.json) preserves that failed attempt and its log hashes. The fixture
now observes actual process exit, supersession and no retry without an acknowledgment-order race.
The fresh final run above supplies acceptance; failed attempts are not relabelled.

This remains a pre-conformance local simulator prototype. ENG-05, ENG-07 and deployment-wide
DB-10 saturation remain **not run**. D-6/7/8/9 remain open. The accepted one-primary/one-designated-
synchronous-standby, bounded-pool, manual-recovery and offline-release profile is unchanged.
