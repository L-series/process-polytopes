#!/usr/bin/env bash
# Runs single-threaded, multi-threaded, and taskset-pinned benchmarks for poly.x.
# Usage: ./scripts/benchmark.sh [input_file]
# Defaults to samples/sample-100k.txt if no argument is given.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
POLY_BIN="${POLY_BIN:-$REPO_ROOT/PALP/poly.x}"
BASELINE_BIN="${BASELINE_BIN:-$REPO_ROOT/PALP/poly.x.baseline}"
POLY=("$POLY_BIN" -N)
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

# ---- correctness check ----
# Compare optimized output against baseline binary (if available).
# The baseline produces the old multi-line format (header + padded rows);
# the optimized binary produces compact [[row],[row],...] lines.
# normalize_matrix converts the old format to the compact one for comparison.
normalize_matrix() {
    awk '
    /^[0-9]+ [0-9]+/ {
        # header line: "d nv  ..." — read d rows that follow
        split($0, hdr)
        d = hdr[1]; nv = hdr[2]
        printf "["
        for (r = 0; r < d; r++) {
            getline
            if (r) printf ","
            printf "["
            n = split($0, vals)
            for (c = 1; c <= n; c++) {
                if (c > 1) printf ","
                printf "%s", vals[c]+0
            }
            printf "]"
        }
        print "]"
    }
    '
}

if [[ -x "$BASELINE_BIN" ]]; then
    echo ""
    echo "--- Correctness check (vs baseline) ---"
    BASELINE_OUT="$TMPDIR/baseline.txt"
    OPTIMIZED_OUT="$TMPDIR/optimized.txt"
    "$BASELINE_BIN" -N "$INPUT" "$BASELINE_OUT" 2>/dev/null
    "${POLY[@]}" "$INPUT" "$OPTIMIZED_OUT" 2>/dev/null

    if diff <(normalize_matrix < "$BASELINE_OUT") "$OPTIMIZED_OUT" > /dev/null 2>&1; then
        echo "PASS: optimized output matches baseline"
    else
        echo "FAIL: output differs from baseline"
        diff <(normalize_matrix < "$BASELINE_OUT") "$OPTIMIZED_OUT" | head -20
        exit 1
    fi
else
    echo ""
    echo "--- Correctness check skipped (no baseline at $BASELINE_BIN) ---"
    echo "To enable: set BASELINE_BIN=/path/to/original/poly.x"
fi

echo ""
echo "Done."
