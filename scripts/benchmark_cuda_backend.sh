#!/usr/bin/env bash
# Compare CPU and CUDA-backend classifier runs on the same combined-CWS input.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
. "$SCRIPT_DIR/env_local.sh"

STRUCTURE_ID="${STRUCTURE_ID:-5}"
THREADS="${THREADS:-$(nproc)}"
MAX_ROWS="${MAX_ROWS:-0}"
BUILD_DIR="${CUDA_BUILD_DIR:-$REPO_ROOT/src/classify/build-cuda}"
RUN_DIR="${RUN_DIR:-$REPO_ROOT/results/benchmarks/cuda-$(date +%Y%m%d-%H%M%S)-s${STRUCTURE_ID}}"

if ! command -v nvcc >/dev/null 2>&1; then
    echo "nvcc is not available on PATH" >&2
    exit 1
fi

mkdir -p "$RUN_DIR/input" "$RUN_DIR/cpu" "$RUN_DIR/cuda" "$RUN_DIR/logs"
"$SCRIPT_DIR/discover_gpu_env.sh" > "$RUN_DIR/logs/environment.txt"

cmake -S "$REPO_ROOT/src/classify" -B "$BUILD_DIR" \
    -DCMAKE_BUILD_TYPE=Release \
    -DENABLE_CUDA=ON \
    -GNinja >/dev/null
cmake --build "$BUILD_DIR" --target classifier add_nf cws_to_parquet compare_polytope_outputs cuda_backend_smoke \
    --parallel "$THREADS" >/dev/null

"$BUILD_DIR/cuda_backend_smoke" | tee "$RUN_DIR/logs/cuda_smoke.log"

"$SCRIPT_DIR/generate_dim5_cws_parquet.sh" \
    --structure-id "$STRUCTURE_ID" \
    --output-dir "$RUN_DIR/input" 2>&1 | tee "$RUN_DIR/logs/generate.log"

EXTRA_ARGS=()
if [[ "$MAX_ROWS" != "0" ]]; then
    EXTRA_ARGS+=(--max-rows "$MAX_ROWS")
fi

SECONDS=0
"$BUILD_DIR/classifier" \
    --input "$RUN_DIR/input" \
    --output "$RUN_DIR/cpu" \
    --threads "$THREADS" \
    --backend cpu \
    "${EXTRA_ARGS[@]}" 2>&1 | tee "$RUN_DIR/logs/classifier_cpu.log"
CPU_SECONDS=$SECONDS

SECONDS=0
"$BUILD_DIR/classifier" \
    --input "$RUN_DIR/input" \
    --output "$RUN_DIR/cuda" \
    --threads "$THREADS" \
    --backend auto \
    "${EXTRA_ARGS[@]}" 2>&1 | tee "$RUN_DIR/logs/classifier_cuda.log"
CUDA_SECONDS=$SECONDS

python3 - "$RUN_DIR/cpu/summary.json" "$RUN_DIR/cuda/summary.json" <<'PY'
import json
import sys

with open(sys.argv[1], "r", encoding="utf-8") as handle:
    cpu = json.load(handle)
with open(sys.argv[2], "r", encoding="utf-8") as handle:
    cuda = json.load(handle)

for key in ("total_cws", "failed_cws", "duplicate_cws", "unique_polytopes"):
    if cpu[key] != cuda[key]:
        raise SystemExit(f"summary mismatch for {key}: cpu={cpu[key]} cuda={cuda[key]}")
PY

"$BUILD_DIR/compare_polytope_outputs" \
    "$RUN_DIR/cpu/unique_polytopes.parquet" \
    "$RUN_DIR/cuda/unique_polytopes.parquet" | tee "$RUN_DIR/logs/hash_parity.log"

{
    echo "structure_id=$STRUCTURE_ID"
    echo "threads=$THREADS"
    echo "max_rows=$MAX_ROWS"
    echo "cpu_seconds=$CPU_SECONDS"
    echo "cuda_backend_seconds=$CUDA_SECONDS"
    echo "run_dir=$RUN_DIR"
} | tee "$RUN_DIR/logs/benchmark.log"

echo "Benchmark artifacts: $RUN_DIR"