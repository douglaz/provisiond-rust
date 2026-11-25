# Historical W7 inputs

These are byte-for-byte copies from this repository's own history. They let a
fresh checkout run the five W7 candidate controls without old Git objects or
network access. They are frozen test data; do not format or repair them.

- `baseline/`: 41 files, 588433 bytes, from commit
  `8339aae5ecf20e6337f4d5d5e22ec4f4573efbfb`. The original runner selected paths
  starting with `src/` or `migrations/`, exactly `Cargo.toml` and `Cargo.lock`,
  and paths starting with `tests/postgres/` and ending in `.rs`.
- `cli/w7.py`: the complete 75601-byte `tests/postgres/w7.py` from commit
  `06b7636cfed13cd4d6049dbe316ed8b191c93fa2`. The candidate driver still extracts
  only its original `operator` function with the existing AST procedure.

`manifest.json` records each original commit/path identity, Git blob ID, byte
length and SHA-256. `tests/postgres/historical_inputs.py` checks both inputs
before candidate builds and returns the verified bytes for consumption. Missing
or altered data is an error; restore the tracked files explicitly. There is no
Git-object, alternate-checkout, network or automatic-repair fallback. The
candidate-source proof includes the verified manifest and its SHA-256.

The original baseline root contains no license file. Its package metadata has
`publish = false` and no `license` field. Original Cargo metadata and source
comments are preserved. These copies introduce no license grant and contain no
third-party dependency source; `Cargo.lock` only records dependencies.
