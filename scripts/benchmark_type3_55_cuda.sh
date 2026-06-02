#!/usr/bin/env bash
# Benchmark direct CUDA enumeration of the dim-5 type-3 (5,5) CWS pair space.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
. "$SCRIPT_DIR/env_local.sh"

BUILD_DIR="${CUDA_BUILD_DIR:-$REPO_ROOT/src/classify/build-cuda}"
W5_POOL="${W5_POOL:-$REPO_ROOT/results/cache/w5.ip}"
PAIR_COUNT="${PAIR_COUNT:-100000000}"
START_PAIR="${START_PAIR:-0}"
SHARD_COUNT="${SHARD_COUNT:-1}"
SHARD_INDEX="${SHARD_INDEX:-0}"
CUDA_DEVICE="${CUDA_DEVICE:-$(_slurm_cuda_device)}"
THREADS="${THREADS:-$(nproc)}"

mkdir -p "$(dirname "$W5_POOL")"

if [[ ! -r "$W5_POOL" ]]; then
    tmp_gz="$(mktemp)"
    trap 'rm -f "$tmp_gz"' EXIT
    curl -fsSL "https://hep.itp.tuwien.ac.at/~kreuzer/CY/W/w5.ip.gz" -o "$tmp_gz"
    gzip -dc "$tmp_gz" > "$W5_POOL"
fi

cmake -S "$REPO_ROOT/src/classify" -B "$BUILD_DIR" \
    -DCMAKE_BUILD_TYPE=Release \
    -DENABLE_CUDA=ON \
    -DPROCESS_POLYTOPES_CUDA_ARCHITECTURES="120-real" \
    -GNinja >/dev/null
cmake --build "$BUILD_DIR" --target cuda_type3_55_scan --parallel "$THREADS" >/dev/null

RUN_PREFIX=()
if ! command -v nvidia-smi >/dev/null 2>&1 && command -v srun >/dev/null 2>&1; then
    RUN_PREFIX=(srun --gres=gpu:1 --cpus-per-task="${SLURM_CPUS_PER_TASK:-4}" \
        --mem="${SLURM_MEM:-16G}" --time="${SLURM_TIME:-00:10:00}")
fi

SCAN_ARGS=(
    --w5 "$W5_POOL"
    --cuda-device "$CUDA_DEVICE"
    --start-pair "$START_PAIR"
    --pair-count "$PAIR_COUNT"
    --shard-count "$SHARD_COUNT"
    --shard-index "$SHARD_INDEX"
)

if [[ "${FULL:-0}" == "1" ]]; then
    SCAN_ARGS+=(--full)
fi
if [[ "${VERIFY_CPU:-0}" == "1" ]]; then
    SCAN_ARGS+=(--verify-cpu)
fi
if [[ "${EMIT_CAPACITY:-0}" != "0" ]]; then
    SCAN_ARGS+=(--emit-capacity "$EMIT_CAPACITY")
fi
if [[ "${PRINT_CANDIDATES:-0}" != "0" ]]; then
    SCAN_ARGS+=(--print-candidates "$PRINT_CANDIDATES")
fi

"${RUN_PREFIX[@]}" "$BUILD_DIR/cuda_type3_55_scan" "${SCAN_ARGS[@]}"