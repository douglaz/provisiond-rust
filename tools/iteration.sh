#!/usr/bin/env bash
# Per-iteration check; run the full suite, tools/postgres.sh, before publication.
# Foreground children require the same capped unit and private environment as tools/postgres.sh.
set -euo pipefail
cd "$(dirname "$0")/.."

status=0
run_check() {
    local started=$SECONDS code
    if "$@"; then
        code=0
    else
        code=$?
    fi
    printf 'iteration.sh: command='
    printf '%q ' "$@"
    printf 'exit_status=%d wall_seconds=%d\n' "$code" "$((SECONDS - started))"
    if (( status == 0 && code != 0 )); then
        status=$code
    fi
}

run_check bash tools/check.sh
run_check python3 -m unittest discover -s tests/postgres -p 'test_*.py'
run_check python3 tests/postgres/harness.py --phase iteration "$@"
exit "$status"
