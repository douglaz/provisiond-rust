#!/usr/bin/env bash
# W1 then W2, foreground only; failures include each child's cleanup status.
set -euo pipefail
cd "$(dirname "$0")/.."
exec python3 tests/postgres/harness.py --phase all "$@"
