#!/usr/bin/env bash
# The repository gate: format, lint, tests, and the spec lock's shape. CI runs this same script.
set -euo pipefail
cd "$(dirname "$0")/.."

cargo fmt --all --check
cargo clippy --all-targets --all-features --locked -- -D warnings
cargo test --all-targets --all-features --locked
python3 - <<'PY'
import json
d = json.load(open("spec-lock.json"))
commit = d["specification"]["commit"]
assert len(commit) == 40 and all(c in "0123456789abcdef" for c in commit), commit
assert d["files"] and all(len(f["sha256"]) == 64 for f in d["files"])
PY
echo "check.sh: all checks passed"
