#!/usr/bin/env bash
# validate.sh — Validate classifier results against PALP poly.x -N
#
# Takes N random CWS from the input parquet, runs them through both
# the classifier pipeline (via PALP library) and the standalone poly.x,
# and compares the normal forms.
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
POLY_BIN="$REPO_ROOT/PALP/poly.x"
N_TESTS="${1:-50}"

echo "=== Validation: classifier vs poly.x -N ==="
echo "Testing $N_TESTS random CWS..."
echo ""

# Extract random CWS from parquet
python3 -c "
import pyarrow.parquet as pq
import random
random.seed(42)

table = pq.read_table('$REPO_ROOT/samples/reflexive/ws-5d-reflexive-0000.parquet',
    columns=['weight0','weight1','weight2','weight3','weight4','weight5'])
n = table.num_rows
indices = sorted(random.sample(range(n), min($N_TESTS, n)))

for i in indices:
    w = [table.column(f'weight{j}')[i].as_py() for j in range(6)]
    d = sum(w)
    print(f'{d} {\" \".join(str(x) for x in w)}')
" > /tmp/validate_cws.txt

# Run through poly.x
echo "Running poly.x -N on $N_TESTS CWS..."
"$POLY_BIN" -N /tmp/validate_cws.txt /tmp/validate_palp_out.txt 2>/dev/null

# Count lines in poly.x output (should match N_TESTS, one NF per line)
n_palp=$(wc -l < /tmp/validate_palp_out.txt)
echo "  poly.x produced $n_palp normal forms"

# Run through classifier on same CWS
# We need a small parquet file with just these CWS
python3 -c "
import pyarrow as pa
import pyarrow.parquet as pq

cws = []
with open('/tmp/validate_cws.txt') as f:
    for line in f:
        parts = list(map(int, line.strip().split()))
        d = parts[0]
        w = parts[1:]
        cws.append(w)

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
"$REPO_ROOT/src/classify/build/classifier" \
    --input /tmp \
    --output /tmp/validate_classifier_out \
    --benchmark "$N_TESTS" \
    --threads 1 2>/dev/null

# Now compare: check that unique polytope count matches
python3 -c "
import pyarrow.parquet as pq

results = pq.read_table('/tmp/validate_classifier_out/unique_polytopes.parquet')
n_unique = results.num_rows
total_count = sum(results.column('count').to_pylist())

print(f'Classifier: {n_unique} unique polytopes from {total_count} CWS')
print(f'poly.x:     {open(\"/tmp/validate_palp_out.txt\").read().count(chr(10))} normal forms')
print()

# Both should produce the same number of unique NFs
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

# Cleanup
rm -f /tmp/validate_cws.txt /tmp/validate_palp_out.txt /tmp/validate_input.parquet
rm -rf /tmp/validate_classifier_out

echo ""
echo "Done."
