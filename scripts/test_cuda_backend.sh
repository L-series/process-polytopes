#!/usr/bin/env bash
# Build and smoke-test the optional CUDA geometry backend.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
. "$SCRIPT_DIR/env_local.sh"

BUILD_DIR="${CUDA_BUILD_DIR:-$REPO_ROOT/src/classify/build-cuda}"

if ! command -v nvcc >/dev/null 2>&1; then
    echo "SKIP: nvcc is not available on PATH"
    exit 0
fi

cmake -S "$REPO_ROOT/src/classify" -B "$BUILD_DIR" \
    -DCMAKE_BUILD_TYPE=Release \
    -DENABLE_CUDA=ON \
    -GNinja >/dev/null
cmake --build "$BUILD_DIR" --target classifier add_nf cws_to_parquet compare_polytope_outputs cuda_backend_smoke cuda_type3_55_scan cuda_dim5_cws_scan \
    --parallel "$(nproc)"

"$BUILD_DIR/cuda_backend_smoke"
W5_POOL="${W5_POOL:-$REPO_ROOT/results/cache/w5.ip}"
if [[ -r "$W5_POOL" ]]; then
    SCAN_PREFIX=()
    if ! command -v nvidia-smi >/dev/null 2>&1 && command -v srun >/dev/null 2>&1; then
        SCAN_PREFIX=(srun --gres=gpu:1 --cpus-per-task="${SLURM_CPUS_PER_TASK:-4}" \
            --mem="${SLURM_MEM:-16G}" --time="${SLURM_TIME:-00:10:00}")
    fi
    "${SCAN_PREFIX[@]}" "$BUILD_DIR/cuda_type3_55_scan" \
        --w5 "$W5_POOL" \
        --pair-count 4096 \
        --emit-capacity 16 \
        --verify-cpu >/dev/null
    "${SCAN_PREFIX[@]}" "$BUILD_DIR/cuda_dim5_cws_scan" \
        --w5 "$W5_POOL" \
        --palp-cws "$REPO_ROOT/PALP/cws.c" \
        --structure-id 5 \
        --emit-capacity 4 | grep -q '^5,285,285,285,285,'
    "${SCAN_PREFIX[@]}" "$BUILD_DIR/cuda_dim5_cws_scan" \
        --w5 "$W5_POOL" \
        --palp-cws "$REPO_ROOT/PALP/cws.c" \
        --structure-id 5 \
        --emit-capacity 16 \
        --ip-check \
        --ip-max-points 1024 2>&1 | grep -q 'gpu_ip structure 5 candidates: 16 processed: 16 ip: 16'
    "${SCAN_PREFIX[@]}" "$BUILD_DIR/cuda_dim5_cws_scan" \
        --w5 "$W5_POOL" \
        --palp-cws "$REPO_ROOT/PALP/cws.c" \
        --structure-id 5 \
        --emit-capacity 64 \
        --stream-ip \
        --ip-max-points 1024 2>&1 | grep -q 'gpu_ip structure 5 candidates: 285 processed: 285 ip: 285'
else
    echo "SKIP: $W5_POOL is not available for type-3 CUDA scan smoke"
fi

TMPDIR="$(mktemp -d)"
if [[ "${KEEP_TMP:-0}" != "1" ]]; then
    trap 'rm -rf "$TMPDIR"' EXIT
else
    echo "KEEP_TMP=1, leaving $TMPDIR" >&2
fi

mkdir -p "$TMPDIR/input" "$TMPDIR/cpu" "$TMPDIR/auto"
"$SCRIPT_DIR/generate_dim5_cws_parquet.sh" \
    --structure-id 5 \
    --output-dir "$TMPDIR/input" >/dev/null

"$BUILD_DIR/classifier" \
    --input "$TMPDIR/input" \
    --output "$TMPDIR/cpu" \
    --threads 2 \
    --backend cpu \
    --max-rows 64 >/dev/null

"$BUILD_DIR/classifier" \
    --input "$TMPDIR/input" \
    --output "$TMPDIR/auto" \
    --threads 2 \
    --backend auto \
    --max-rows 64 >/dev/null

python3 - "$TMPDIR/cpu/summary.json" "$TMPDIR/auto/summary.json" <<'PY'
import json
import sys

with open(sys.argv[1], "r", encoding="utf-8") as handle:
    cpu = json.load(handle)
with open(sys.argv[2], "r", encoding="utf-8") as handle:
    auto = json.load(handle)

for key in ("total_cws", "failed_cws", "duplicate_cws", "unique_polytopes"):
    if cpu[key] != auto[key]:
        raise SystemExit(f"summary mismatch for {key}: cpu={cpu[key]} auto={auto[key]}")
PY

grep -q '"failed_cws": 0' "$TMPDIR/auto/summary.json"
test -s "$TMPDIR/cpu/unique_polytopes.parquet"
test -s "$TMPDIR/auto/unique_polytopes.parquet"
"$BUILD_DIR/compare_polytope_outputs" \
    "$TMPDIR/cpu/unique_polytopes.parquet" \
    "$TMPDIR/auto/unique_polytopes.parquet" >/dev/null

echo "PASS: CUDA backend build and CPU-vs-auto parity smoke test"