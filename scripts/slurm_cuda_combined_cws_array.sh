#!/usr/bin/env bash
#SBATCH --job-name=dim5-cws-cuda
#SBATCH --output=logs/slurm/dim5-cws-cuda-%A_%a.out
#SBATCH --error=logs/slurm/dim5-cws-cuda-%A_%a.err
#SBATCH --cpus-per-task=32
#SBATCH --mem=0
#SBATCH --gres=gpu:1
#SBATCH --time=24:00:00

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
. "$SCRIPT_DIR/env_local.sh"

STRUCTURE_ID="${STRUCTURE_ID:-${SLURM_ARRAY_TASK_ID:-}}"
SHARD_COUNT="${SHARD_COUNT:-}"
SHARD_INDEX="${SHARD_INDEX:-}"
THREADS="${THREADS:-${SLURM_CPUS_PER_TASK:-32}}"
RUN_ROOT="${RUN_ROOT:-$REPO_ROOT/results/combined-cws-cuda}"
BUILD_DIR="${CUDA_BUILD_DIR:-$REPO_ROOT/src/classify/build-cuda}"

if [[ -z "$STRUCTURE_ID" ]]; then
    echo "STRUCTURE_ID is required, or submit with --array=<structure ids>" >&2
    exit 1
fi
if [[ -n "$SHARD_COUNT" && -z "$SHARD_INDEX" ]]; then
    SHARD_INDEX="${SLURM_ARRAY_TASK_ID:-}"
fi
if [[ -n "$SHARD_COUNT" && -z "$SHARD_INDEX" ]]; then
    echo "SHARD_COUNT was set but SHARD_INDEX/SLURM_ARRAY_TASK_ID is missing" >&2
    exit 1
fi

TAG=$(printf "structure-%02d" "$STRUCTURE_ID")
if [[ -n "$SHARD_COUNT" ]]; then
    TAG+=$(printf "-shard-%03d-of-%03d" "$SHARD_INDEX" "$SHARD_COUNT")
fi

INPUT_DIR="$RUN_ROOT/input/$TAG"
OUTPUT_DIR="$RUN_ROOT/output/$TAG"
LOG_DIR="$RUN_ROOT/logs/$TAG"
mkdir -p "$INPUT_DIR" "$OUTPUT_DIR" "$LOG_DIR" "$REPO_ROOT/logs/slurm"

"$SCRIPT_DIR/discover_gpu_env.sh" > "$LOG_DIR/environment.txt"

cmake -S "$REPO_ROOT/src/classify" -B "$BUILD_DIR" \
    -DCMAKE_BUILD_TYPE=Release \
    -DENABLE_CUDA=ON \
    -GNinja
cmake --build "$BUILD_DIR" --target classifier add_nf cws_to_parquet cuda_backend_smoke \
    --parallel "$THREADS"
make -C "$REPO_ROOT/PALP" -f GNUmakefile cws-5d.x

"$BUILD_DIR/cuda_backend_smoke" 2>&1 | tee "$LOG_DIR/cuda_smoke.log"

GEN_ARGS=(--structure-id "$STRUCTURE_ID" --output-dir "$INPUT_DIR")
if [[ -n "$SHARD_COUNT" ]]; then
    GEN_ARGS+=(--shards "$SHARD_COUNT" --worker "$SHARD_INDEX")
fi
"$SCRIPT_DIR/generate_dim5_cws_parquet.sh" "${GEN_ARGS[@]}" 2>&1 | tee "$LOG_DIR/generate.log"

"$BUILD_DIR/classifier" \
    --input "$INPUT_DIR" \
    --output "$OUTPUT_DIR" \
    --threads "$THREADS" \
    --backend cuda \
    --cuda-device 0 2>&1 | tee "$LOG_DIR/classifier.log"

"$BUILD_DIR/add_nf" \
    --input "$OUTPUT_DIR/unique_polytopes.parquet" \
    --output "$OUTPUT_DIR/enriched.parquet" \
    --threads "$THREADS" \
    --verify-hash 2>&1 | tee "$LOG_DIR/add_nf.log"

echo "DONE $TAG"