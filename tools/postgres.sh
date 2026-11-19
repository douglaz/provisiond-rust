#!/usr/bin/env bash
# All twelve phases, bounded foreground children; preserve every true exit and cleanup verdict.
set -euo pipefail
cd "$(dirname "$0")/.."
exec python3 tests/postgres/harness.py --phase all "$@"
