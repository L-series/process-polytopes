#!/usr/bin/env bash
# End-to-end smoke test for combined-CWS Parquet classification.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
. "$SCRIPT_DIR/env_local.sh"

BUILD_DIR="$REPO_ROOT/src/classify/build"
TMPDIR="$(mktemp -d)"
if [[ "${KEEP_TMP:-0}" != "1" ]]; then
    trap 'rm -rf "$TMPDIR"' EXIT
else
    echo "KEEP_TMP=1, leaving $TMPDIR" >&2
fi

cmake -S "$REPO_ROOT/src/classify" -B "$BUILD_DIR" -DCMAKE_BUILD_TYPE=Release -GNinja >/dev/null
cmake --build "$BUILD_DIR" --parallel "$(nproc)" >/dev/null
make -C "$REPO_ROOT/PALP" -f GNUmakefile cws-5d.x >/dev/null

mkdir -p "$TMPDIR/input" "$TMPDIR/output"
mkdir -p "$TMPDIR/merged"

(
    cd "$REPO_ROOT/PALP"
    ./cws-5d.x -c5 -n3 \
        tests/input/4.2.8-cws-w2.txt \
        tests/input/4.2.7-cws-c5-overlap3.txt \
        tests/input/4.2.8-cws-w4.txt \
        -s15 > "$TMPDIR/structure15.txt"
)

"$BUILD_DIR/cws_to_parquet" \
    --input "$TMPDIR/structure15.txt" \
    --output "$TMPDIR/input/structure15.parquet" \
    --structure-id 15 \
    --nw 3 \
    --N 8 >/dev/null

"$BUILD_DIR/classifier" \
    --input "$TMPDIR/input" \
    --output "$TMPDIR/output" \
    --threads 2 >/dev/null

"$BUILD_DIR/add_nf" \
    --input "$TMPDIR/output/unique_polytopes.parquet" \
    --output "$TMPDIR/output/enriched.parquet" \
    --threads 2 \
    --verify-hash >/dev/null

"$BUILD_DIR/classifier" \
    --merge "$TMPDIR/output/checkpoints" \
    --output "$TMPDIR/merged" \
    --threads 2 >/dev/null

"$BUILD_DIR/add_nf" \
    --input "$TMPDIR/merged/unique_polytopes.parquet" \
    --output "$TMPDIR/merged/enriched.parquet" \
    --threads 2 \
    --verify-hash >/dev/null

grep -q '"total_cws": 3' "$TMPDIR/output/summary.json"
grep -q '"failed_cws": 0' "$TMPDIR/output/summary.json"
grep -q '"unique_polytopes": 3' "$TMPDIR/output/summary.json"
test -s "$TMPDIR/output/unique_polytopes.parquet"
test -s "$TMPDIR/output/enriched.parquet"
test -s "$TMPDIR/merged/unique_polytopes.parquet"
test -s "$TMPDIR/merged/enriched.parquet"

echo "PASS: combined-CWS Parquet classifier smoke test"
