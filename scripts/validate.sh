#!/usr/bin/env bash
# validate.sh — Validate classifier results against PALP poly.x
#
# Modes:
#   ./validate.sh [N]                        Quick: N random CWS via poly.x (default 50)
#   ./validate.sh --full [N]                 Full:  N (or all) CWS via poly.x.baseline
#                                                   distributed, deduped, vs classifier.
#   ./validate.sh --full [N] --workers W     Set parallel workers (default 32)
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
POLY_BIN="$REPO_ROOT/PALP/poly.x"
POLY_BASELINE="$REPO_ROOT/PALP/poly.x.baseline"
CLASSIFIER="$REPO_ROOT/src/classify/build/classifier"
SAMPLE_PARQUET="$REPO_ROOT/samples/reflexive/ws-5d-reflexive-0000.parquet"

# ─── Parse arguments ────────────────────────────────────────────────────
MODE="quick"
N_TESTS=50
N_WORKERS=32

while [[ $# -gt 0 ]]; do
    case "$1" in
        --full)
            MODE="full"
            # Optional next arg: row limit (number)
            if [[ ${2:-} =~ ^[0-9]+$ ]]; then
                N_TESTS="$2"; shift
            else
                N_TESTS=0   # 0 = all rows
            fi
            ;;
        --workers)
            N_WORKERS="${2:?--workers requires a number}"; shift
            ;;
        -h|--help)
            echo "Usage: $0 [N]                       Quick test with N random CWS (default 50)"
            echo "       $0 --full [N]                Full test with N (or all) CWS"
            echo "       $0 --full [N] --workers W    Parallel workers (default 32)"
            exit 0
            ;;
        *)
            if [[ "$1" =~ ^[0-9]+$ ]]; then
                N_TESTS="$1"
            else
                echo "Unknown option: $1" >&2; exit 1
            fi
            ;;
    esac
    shift
done

# ─── Sanity checks ──────────────────────────────────────────────────────
for bin in "$POLY_BIN" "$CLASSIFIER"; do
    [[ -x "$bin" ]] || { echo "Missing: $bin" >&2; exit 1; }
done
[[ -f "$SAMPLE_PARQUET" ]] || { echo "Missing: $SAMPLE_PARQUET" >&2; exit 1; }
if [[ "$MODE" == "full" ]]; then
    [[ -x "$POLY_BASELINE" ]] || { echo "Missing: $POLY_BASELINE" >&2; exit 1; }
fi

# ═══════════════════════════════════════════════════════════════════════
# QUICK MODE — N random CWS, compare poly.x vs classifier
# ═══════════════════════════════════════════════════════════════════════
run_quick() {
    echo "=== Quick Validation: classifier vs poly.x -N ==="
    echo "Testing $N_TESTS random CWS..."
    echo ""

    python3 -c "
import pyarrow.parquet as pq
import random
random.seed(42)

table = pq.read_table('$SAMPLE_PARQUET',
    columns=['weight0','weight1','weight2','weight3','weight4','weight5'])
n = table.num_rows
indices = sorted(random.sample(range(n), min($N_TESTS, n)))

for i in indices:
    w = [table.column(f'weight{j}')[i].as_py() for j in range(6)]
    d = sum(w)
    print(f'{d} {\" \".join(str(x) for x in w)}')
" > /tmp/validate_cws.txt

    echo "Running poly.x -N on $N_TESTS CWS..."
    "$POLY_BIN" -N /tmp/validate_cws.txt /tmp/validate_palp_out.txt 2>/dev/null
    local n_palp
    n_palp=$(wc -l < /tmp/validate_palp_out.txt)
    echo "  poly.x produced $n_palp normal forms"

    python3 -c "
import pyarrow as pa
import pyarrow.parquet as pq

cws = []
with open('/tmp/validate_cws.txt') as f:
    for line in f:
        parts = list(map(int, line.strip().split()))
        cws.append(parts[1:])

table = pa.table({
    f'weight{i}': pa.array([c[i] for c in cws], type=pa.int32())
    for i in range(6)
} | {
    'vertex_count':     pa.array([0]*len(cws), type=pa.int32()),
    'facet_count':      pa.array([0]*len(cws), type=pa.int32()),
    'point_count':      pa.array([0]*len(cws), type=pa.int32()),
    'dual_point_count': pa.array([0]*len(cws), type=pa.int32()),
    'h11':              pa.array([0]*len(cws), type=pa.int32()),
    'h12':              pa.array([0]*len(cws), type=pa.int32()),
    'h13':              pa.array([0]*len(cws), type=pa.int32()),
})
pq.write_table(table, '/tmp/validate_input.parquet')
print(f'Created validation parquet with {len(cws)} rows')
"

    mkdir -p /tmp/validate_classifier_out
    "$CLASSIFIER" \
        --input /tmp \
        --output /tmp/validate_classifier_out \
        --benchmark "$N_TESTS" \
        --threads 1 2>/dev/null

    python3 -c "
import pyarrow.parquet as pq

results = pq.read_table('/tmp/validate_classifier_out/unique_polytopes.parquet')
n_unique = results.num_rows
total_count = sum(results.column('count').to_pylist())

print(f'Classifier: {n_unique} unique polytopes from {total_count} CWS')

palp_nfs = set()
with open('/tmp/validate_palp_out.txt') as f:
    for line in f:
        line = line.strip()
        if line:
            palp_nfs.add(line)

print(f'poly.x unique NFs: {len(palp_nfs)}')
print(f'Classifier unique: {n_unique}')

if len(palp_nfs) == n_unique:
    print()
    print('✓ PASS: unique polytope counts match!')
else:
    print()
    print('✗ FAIL: counts differ!')
    print(f'  Difference: {abs(len(palp_nfs) - n_unique)}')
"

    rm -f /tmp/validate_cws.txt /tmp/validate_palp_out.txt /tmp/validate_input.parquet
    rm -rf /tmp/validate_classifier_out

    echo ""
    echo "Done."
}

# ═══════════════════════════════════════════════════════════════════════
# FULL MODE — parquet → text → poly.x.baseline (distributed) → dedup
#             vs classifier on same data
# ═══════════════════════════════════════════════════════════════════════
run_full() {
    local row_limit=$N_TESTS   # 0 = all rows
    local workers=$N_WORKERS

    VALIDATE_TMPDIR="/tmp/validate_full_$$"
    local tmpdir="$VALIDATE_TMPDIR"
    mkdir -p "$tmpdir"
    trap 'rm -rf "$VALIDATE_TMPDIR" 2>/dev/null' EXIT

    echo "════════════════════════════════════════════════════════════════"
    echo "  Full Validation: classifier vs poly.x.baseline (distributed)"
    echo "════════════════════════════════════════════════════════════════"
    echo "  Parquet:    $SAMPLE_PARQUET"
    echo "  Workers:    $workers"
    if (( row_limit > 0 )); then
        echo "  Row limit:  $row_limit"
    else
        echo "  Row limit:  ALL"
    fi
    echo ""

    local wall_start=$SECONDS

    # ── Step 1: Parquet → CWS text ──────────────────────────────────
    echo "[1/6] Converting parquet to CWS text file..."
    local t1=$SECONDS
    python3 << PYEOF
import pyarrow.parquet as pq
import numpy as np
import sys

limit = $row_limit if $row_limit > 0 else None

pf = pq.ParquetFile('$SAMPLE_PARQUET')
total_rows = pf.metadata.num_rows
if limit is not None:
    limit = min(limit, total_rows)
else:
    limit = total_rows

written = 0
# Use a large binary buffer; np.savetxt is ~10x faster than row-by-row Python
with open('$tmpdir/all_cws.txt', 'wb', buffering=1<<23) as f:
    for batch in pf.iter_batches(columns=[f'weight{j}' for j in range(6)],
                                  batch_size=2_000_000):
        remaining = limit - written
        if remaining <= 0:
            break
        if batch.num_rows > remaining:
            batch = batch.slice(0, remaining)
        # Stack into (N, 6) int32 array
        cols = np.column_stack([batch.column(f'weight{j}').to_pydict()
                                 if False else
                                 np.asarray(batch.column(f'weight{j}'))
                                 for j in range(6)])
        # Degree = sum of weights
        deg = cols.sum(axis=1, keepdims=True)
        # Build (N, 7): [d, w0..w5]
        out = np.concatenate([deg, cols], axis=1)
        np.savetxt(f, out, fmt='%d', delimiter=' ')
        written += batch.num_rows

print(f'  Wrote {written:,} CWS ({written/1e6:.2f}M)')
PYEOF
    local total_cws
    total_cws=$(wc -l < "$tmpdir/all_cws.txt")
    echo "  Time: $((SECONDS - t1))s"
    echo ""

    # ── Step 2: Split into chunks ───────────────────────────────────
    echo "[2/6] Splitting into $workers chunks..."
    local lines_per_chunk=$(( (total_cws + workers - 1) / workers ))
    split -l "$lines_per_chunk" -d -a 4 "$tmpdir/all_cws.txt" "$tmpdir/chunk_"
    local n_chunks
    n_chunks=$(find "$tmpdir" -maxdepth 1 -name 'chunk_*' | wc -l)
    echo "  $n_chunks chunks × ~${lines_per_chunk} lines"
    echo ""

    # ── Step 3: Run poly.x.baseline on each chunk ──────────────────
    echo "[3/6] Running poly.x.baseline -N on $n_chunks workers..."
    local t3=$SECONDS

    # AWK parser: extracts the 5×N normal-form matrix from poly.x
    # verbose output and emits it as a single pipe-delimited line.
    local AWK_PARSE='
/Normal form/ {
    nrows = $1; nf = ""
    for (i = 0; i < nrows; i++) {
        getline
        gsub(/^ +/, ""); gsub(/ +$/, ""); gsub(/ +/, " ")
        if (nf != "") nf = nf "|"
        nf = nf $0
    }
    print nf
}'

    local pids=()
    for chunk in "$tmpdir"/chunk_*; do
        (
            "$POLY_BASELINE" -N < "$chunk" 2>/dev/null \
                | awk "$AWK_PARSE" \
                | sort -u -S 256M \
                > "${chunk}.nf"
        ) &
        pids+=($!)
    done

    # Wait with progress
    local completed=0
    for pid in "${pids[@]}"; do
        wait "$pid"
        completed=$((completed + 1))
        printf "\r  Workers done: %d / %d" "$completed" "${#pids[@]}"
    done
    echo ""

    local baseline_wall=$((SECONDS - t3))
    local baseline_cps=0
    (( baseline_wall > 0 )) && baseline_cps=$(( total_cws / baseline_wall ))
    echo "  Wall time: ${baseline_wall}s  (~${baseline_cps} CWS/s)"
    echo ""

    # ── Step 4: Merge + deduplicate ─────────────────────────────────
    echo "[4/6] Merging worker results and deduplicating..."
    local t4=$SECONDS

    # Each worker's .nf file is already sorted + locally unique.
    # sort -m merges sorted files, -u removes cross-worker duplicates.
    sort -u -m -S 1G "$tmpdir"/chunk_*.nf > "$tmpdir/baseline_unique.txt"

    local baseline_total baseline_unique
    baseline_total=$(cat "$tmpdir"/chunk_*.nf | wc -l)
    baseline_unique=$(wc -l < "$tmpdir/baseline_unique.txt")
    echo "  Locally deduped NFs (sum of workers): $baseline_total"
    echo "  Globally unique NFs after merge:      $baseline_unique"
    echo "  Merge time: $((SECONDS - t4))s"
    echo ""

    # ── Step 5: Run classifier ──────────────────────────────────────
    echo "[5/6] Running classifier on the same data..."
    local t5=$SECONDS

    local classifier_input="$tmpdir/classifier_in"
    mkdir -p "$classifier_input"

    if (( row_limit > 0 )); then
        # Build a trimmed parquet with exactly row_limit rows
        python3 << PYEOF2
import pyarrow.parquet as pq
table = pq.read_table('$SAMPLE_PARQUET')
table = table.slice(0, $row_limit)
pq.write_table(table, '$classifier_input/ws-5d-reflexive-0000.parquet')
print(f'  Trimmed parquet: {table.num_rows:,} rows')
PYEOF2
    else
        # Symlink the full file
        ln -sf "$SAMPLE_PARQUET" "$classifier_input/ws-5d-reflexive-0000.parquet"
        echo "  Using full parquet (symlink)"
    fi

    local classifier_out="$tmpdir/classifier_out"
    mkdir -p "$classifier_out"

    "$CLASSIFIER" \
        --input "$classifier_input" \
        --output "$classifier_out" \
        --threads "$workers" 2>&1 | sed 's/^/  /'

    local classifier_wall=$((SECONDS - t5))
    echo "  Classifier wall time: ${classifier_wall}s"
    echo ""

    # ── Step 6: Compare ─────────────────────────────────────────────
    echo "[6/6] Comparing results..."
    echo ""

    python3 - "$tmpdir" << 'PYCOMPARE'
import pyarrow.parquet as pq
import sys, os

tmpdir = sys.argv[1]
classifier_pq = os.path.join(tmpdir, "classifier_out", "unique_polytopes.parquet")
baseline_file = os.path.join(tmpdir, "baseline_unique.txt")
cws_file      = os.path.join(tmpdir, "all_cws.txt")

classifier = pq.read_table(classifier_pq)
classifier_unique = classifier.num_rows
classifier_total  = sum(classifier.column('count').to_pylist())

with open(baseline_file) as f:
    baseline_unique = sum(1 for _ in f)

with open(cws_file) as f:
    total_cws = sum(1 for _ in f)

W = 55
print(f"  ┌{'─' * W}┐")
print(f"  │{'VALIDATION RESULTS':^{W}}│")
print(f"  ├{'─' * W}┤")
print(f"  │  Total CWS processed         {total_cws:>18,}       │")
print(f"  │{'':^{W}}│")
print(f"  │  poly.x.baseline unique NFs  {baseline_unique:>18,}       │")
print(f"  │  classifier unique polytopes {classifier_unique:>18,}       │")
print(f"  │  classifier total CWS sum    {classifier_total:>18,}       │")
print(f"  │{'':^{W}}│")

exit_code = 0

if baseline_unique == classifier_unique:
    print(f"  │  ✅  PASS — unique counts match!{' ' * (W - 34)}│")
else:
    diff = abs(baseline_unique - classifier_unique)
    msg = f"  ❌  FAIL — unique counts differ by {diff:,}!"
    print(f"  │{msg:<{W}}│")
    exit_code = 1

if classifier_total == total_cws:
    print(f"  │  ✅  PASS — CWS totals match!{' ' * (W - 31)}│")
else:
    diff = abs(total_cws - classifier_total)
    msg = f"  ❌  FAIL — CWS totals differ by {diff:,}!"
    print(f"  │{msg:<{W}}│")
    exit_code = 1

dedup_rate = 100.0 * (1.0 - baseline_unique / total_cws) if total_cws else 0
print(f"  │{'':^{W}}│")
print(f"  │  Deduplication rate:          {dedup_rate:>17.1f}%       │")
print(f"  └{'─' * W}┘")

sys.exit(exit_code)
PYCOMPARE
    local compare_status=$?

    echo ""
    local total_wall=$((SECONDS - wall_start))
    echo "  Total wall time: ${total_wall}s  ($((total_wall / 60))m $((total_wall % 60))s)"
    echo ""

    return $compare_status
}

# ─── Dispatch ──────────────────────────────────────────────────────────
if [[ "$MODE" == "full" ]]; then
    run_full
else
    run_quick
fi
