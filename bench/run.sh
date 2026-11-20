#!/bin/sh
# Usage: bench/run.sh <scenario>   (bench/run.sh list shows the names)
# The scenarios themselves are in bench.py, so run.sh and run.ps1 behave the same.
cd "$(dirname "$0")/.." || exit 1
PY=
for c in python3 python; do
    if "$c" -c "import sys" >/dev/null 2>&1; then PY=$c; break; fi
done
[ -n "$PY" ] || { echo "Python 3 is needed on this machine to run the benchmark" >&2; exit 1; }
MSYS_NO_PATHCONV=1 exec "$PY" bench/bench.py "$@"
