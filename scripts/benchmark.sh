#!/usr/bin/env bash
# Runs single-threaded, multi-threaded, and taskset-pinned benchmarks for poly.x.
# Usage: ./scripts/benchmark.sh [input_file]
# Defaults to samples/sample-100k.txt if no argument is given.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
POLY=("$REPO_ROOT/PALP/poly.x" -N)
INPUT="${1:-$REPO_ROOT/samples/sample-100k.txt}"
RUNS=3
JOBS=32

TMPDIR="$(mktemp -d)"
trap 'rm -rf "$TMPDIR"' EXIT

echo "=== poly.x benchmark ==="
echo "Input:   $INPUT"
echo "Binary:  ${POLY[*]}"
echo "Date:    $(date '+%Y-%m-%d %H:%M:%S')"
echo ""
echo "--- CPU ---"
grep "model name" /proc/cpuinfo | head -1 | awk -F': ' '{print $2}'
echo "Physical cores : $(grep "cpu cores" /proc/cpuinfo | head -1 | awk -F': ' '{print $2}')"
echo "Logical threads: $(nproc)"
echo ""

# ---- single-threaded ----
echo "--- Single-threaded ($RUNS runs) ---"
for i in $(seq 1 "$RUNS"); do
    OUT="$TMPDIR/st_run${i}.txt"
    RESULT=$( { time "${POLY[@]}" "$INPUT" "$OUT"; } 2>&1 )
    echo "$RESULT" | grep -E "^real|^user|^sys" || echo "$RESULT"
done

# ---- multi-threaded ----
echo ""
echo "--- Multi-threaded ($JOBS workers, $RUNS runs) ---"
CHUNKS_DIR="$TMPDIR/chunks"
MT_OUT_DIR="$TMPDIR/mt_out"
mkdir -p "$CHUNKS_DIR" "$MT_OUT_DIR"
split -n "l/$JOBS" "$INPUT" "$CHUNKS_DIR/chunk_"

for i in $(seq 1 "$RUNS"); do
    # clear previous outputs so each run starts fresh
    rm -f "$MT_OUT_DIR"/*.out
    RESULT=$( { time parallel -j"$JOBS" "${POLY[*]}"' {} '"$MT_OUT_DIR"'/{/}.out' ::: "$CHUNKS_DIR"/chunk_*; } 2>&1 )
    echo "$RESULT" | grep -E "^real|^user|^sys" || echo "$RESULT"
done

# ---- taskset-pinned ----
# Each chunk is pinned to exactly one logical CPU (cpu_id = job_slot - 1).
# The OS scheduler will not migrate the process, eliminating inter-core
# cache traffic and scheduling jitter.
echo ""
echo "--- Taskset-pinned ($JOBS workers, 1 CPU each, $RUNS runs) ---"
TS_OUT_DIR="$TMPDIR/ts_out"
mkdir -p "$TS_OUT_DIR"

for i in $(seq 1 "$RUNS"); do
    rm -f "$TS_OUT_DIR"/*.out
    RESULT=$( { time parallel -j"$JOBS" \
        'taskset -c $(({%}-1)) '"${POLY[*]}"' {} '"$TS_OUT_DIR"'/{/}.out' \
        ::: "$CHUNKS_DIR"/chunk_*; } 2>&1 )
    echo "$RESULT" | grep -E "^real|^user|^sys" || echo "$RESULT"
done

echo ""
echo "Done."
