#!/usr/bin/env bash

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
PALP_DIR="${PALP_DIR:-$REPO_ROOT/PALP}"
CWS_BIN="${CWS_BIN:-$PALP_DIR/cws.x}"
WF_FILE="${WF_FILE:-$PALP_DIR/cws/wf4-d1-20.txt}"
RUNTIME_BUILD_DIR="${RUNTIME_BUILD_DIR:-$REPO_ROOT/.type3-cuda-runtime}"
CUDA_RUNTIME="${CUDA_RUNTIME:-$RUNTIME_BUILD_DIR/libtype3_bounds_runtime.so}"
OUTPUT_DIR="${OUTPUT_DIR:-$REPO_ROOT/.type3-frontier-bench/multi-gpu-$(date +%Y%m%d-%H%M%S)}"
TOTAL_SHARDS="${TOTAL_SHARDS:-32}"
SHARD_START="${SHARD_START:-1}"
SHARD_END="${SHARD_END:-$TOTAL_SHARDS}"
GPU_LIST="${GPU_LIST:-auto}"
WORKERS_PER_GPU="${WORKERS_PER_GPU:-1}"
FRONTIER_BATCH="${FRONTIER_BATCH:-65536}"
CUDA_CANDIDATE_BATCH="${CUDA_CANDIDATE_BATCH:-16}"
CUDA_BATCH_LANES="${CUDA_BATCH_LANES:-8}"
AUTO_BUILD_CUDA="${AUTO_BUILD_CUDA:-1}"
CUDA_NIXPKGS_SET="${CUDA_NIXPKGS_SET:-cudaPackages_12_6}"

absolutize_path() {
    local path="$1"

    if [[ "$path" == /* ]]; then
        printf '%s\n' "$path"
    else
        printf '%s\n' "$REPO_ROOT/$path"
    fi
}

detect_host_cuda_lib_dir() {
    local candidate

    for candidate in /run/opengl-driver/lib /usr/lib /usr/lib64; do
        if [[ -e "$candidate/libcuda.so.1" ]]; then
            printf '%s\n' "$candidate"
            return 0
        fi
    done
    return 1
}

detect_gpu_list() {
    nvidia-smi --query-gpu=index --format=csv,noheader | paste -sd, -
}

claim_next_shard() {
    local next_shard

    exec 9<>"$QUEUE_LOCK_FILE"
    flock 9
    next_shard="$(cat "$QUEUE_FILE")"
    if (( next_shard > SHARD_END )); then
        flock -u 9
        exec 9>&-
        return 1
    fi
    printf '%s\n' "$((next_shard + 1))" >"$QUEUE_FILE"
    flock -u 9
    exec 9>&-
    printf '%s\n' "$next_shard"
}

run_shard() {
    local shard_index="$1"
    local gpu_id="$2"
    local slot_id="$3"
    local output_path
    local log_path
    local start_seconds end_seconds wall_seconds
    local -a env_args=(
        CUDA_VISIBLE_DEVICES="$gpu_id"
        PALP_TYPE3_FRONTIER=cuda
        PALP_TYPE3_FRONTIER_BATCH="$FRONTIER_BATCH"
        PALP_TYPE3_CUDA_RUNTIME="$CUDA_RUNTIME"
        PALP_TYPE3_CUDA_CANDIDATE_BATCH="$CUDA_CANDIDATE_BATCH"
        PALP_TYPE3_CUDA_BATCH_LANES="$CUDA_BATCH_LANES"
    )

    output_path="$OUTPUT_DIR/outputs/shard-$(printf '%04d' "$shard_index").out"
    log_path="$OUTPUT_DIR/logs/shard-$(printf '%04d' "$shard_index").log"

    if [[ -n "$HOST_CUDA_LIB_DIR" ]]; then
        env_args+=(LD_LIBRARY_PATH="$HOST_CUDA_LIB_DIR${LD_LIBRARY_PATH:+:$LD_LIBRARY_PATH}")
    fi

    start_seconds="$(date +%s.%N)"
    (
        cd "$PALP_DIR"
        env "${env_args[@]}" \
            "$CWS_BIN" -c5 -T -I -n2 "$WF_FILE" "$WF_FILE" -s3 \
            -j"$TOTAL_SHARDS" -k"$shard_index" "$output_path" 2>"$log_path"
    )
    end_seconds="$(date +%s.%N)"
    wall_seconds="$(awk -v start="$start_seconds" -v end="$end_seconds" 'BEGIN {printf "%.6f", end - start}')"

    printf '%s\t%s\t%s\t%s\n' "$slot_id" "$gpu_id" "$shard_index" "$wall_seconds" >>"$OUTPUT_DIR/shard_timings.tsv"
}

worker_loop() {
    local slot_id="$1"
    local gpu_id="$2"
    local shard_index

    while shard_index="$(claim_next_shard)"; do
        echo "slot=$slot_id gpu=$gpu_id shard=$shard_index"
        run_shard "$shard_index" "$gpu_id" "$slot_id"
    done
}

sum_metric() {
    local pattern="$1"

    grep -h "$pattern" "$OUTPUT_DIR"/logs/*.log 2>/dev/null |
        awk '{sum += $5} END {printf "%.6f", sum}'
}

PALP_DIR="$(absolutize_path "$PALP_DIR")"
CWS_BIN="$(absolutize_path "$CWS_BIN")"
WF_FILE="$(absolutize_path "$WF_FILE")"
RUNTIME_BUILD_DIR="$(absolutize_path "$RUNTIME_BUILD_DIR")"
CUDA_RUNTIME="$(absolutize_path "$CUDA_RUNTIME")"
OUTPUT_DIR="$(absolutize_path "$OUTPUT_DIR")"

if [[ ! -x "$CWS_BIN" ]]; then
    echo "cws binary not found: $CWS_BIN" >&2
    exit 1
fi

if [[ "$GPU_LIST" == auto ]]; then
    if ! command -v nvidia-smi >/dev/null 2>&1; then
        echo "nvidia-smi not found; set GPU_LIST explicitly" >&2
        exit 1
    fi
    GPU_LIST="$(detect_gpu_list)"
fi

IFS=',' read -r -a GPU_IDS <<<"$GPU_LIST"
if [[ "${#GPU_IDS[@]}" -eq 0 ]]; then
    echo "no GPUs selected" >&2
    exit 1
fi

if (( SHARD_START < 1 || SHARD_END < SHARD_START || SHARD_END > TOTAL_SHARDS )); then
    echo "invalid shard range: SHARD_START=$SHARD_START SHARD_END=$SHARD_END TOTAL_SHARDS=$TOTAL_SHARDS" >&2
    exit 1
fi

if [[ ! -f "$CUDA_RUNTIME" ]]; then
    if [[ "$AUTO_BUILD_CUDA" == 1 ]]; then
        BUILD_DIR="$RUNTIME_BUILD_DIR" CUDA_NIXPKGS_SET="$CUDA_NIXPKGS_SET" \
            "$SCRIPT_DIR/build_type3_cuda_runtime.sh" >/dev/null
    else
        echo "missing CUDA runtime: $CUDA_RUNTIME" >&2
        exit 1
    fi
fi

mkdir -p "$OUTPUT_DIR/logs" "$OUTPUT_DIR/outputs" "$OUTPUT_DIR/state"
QUEUE_FILE="$OUTPUT_DIR/state/next_shard"
QUEUE_LOCK_FILE="$OUTPUT_DIR/state/next_shard.lock"
printf '%s\n' "$SHARD_START" >"$QUEUE_FILE"
touch "$QUEUE_LOCK_FILE"
HOST_CUDA_LIB_DIR="$(detect_host_cuda_lib_dir || true)"

slot_count=$(( ${#GPU_IDS[@]} * WORKERS_PER_GPU ))
start_seconds="$(date +%s.%N)"

echo "=== Type-3 multi-GPU launcher ==="
echo "WF file:             $WF_FILE"
echo "GPU list:            $GPU_LIST"
echo "Workers per GPU:     $WORKERS_PER_GPU"
echo "Worker slots:        $slot_count"
echo "Shard range:         $SHARD_START..$SHARD_END / $TOTAL_SHARDS"
echo "Candidate batch:     $CUDA_CANDIDATE_BATCH"
echo "CUDA batch lanes:    $CUDA_BATCH_LANES"
echo "Frontier batch:      $FRONTIER_BATCH"
echo "Output dir:          $OUTPUT_DIR"
echo ""

declare -a worker_pids=()
slot_id=0
for gpu_id in "${GPU_IDS[@]}"; do
    for (( worker_index = 1; worker_index <= WORKERS_PER_GPU; worker_index++ )); do
        worker_loop "$slot_id" "$gpu_id" >"$OUTPUT_DIR/logs/worker-${slot_id}.meta.log" 2>&1 &
        worker_pids+=("$!")
        slot_id=$((slot_id + 1))
    done
done

status=0
for pid in "${worker_pids[@]}"; do
    if ! wait "$pid"; then
        status=1
    fi
done
if (( status != 0 )); then
    echo "one or more worker slots failed" >&2
    exit "$status"
fi

end_seconds="$(date +%s.%N)"
wall_seconds="$(awk -v start="$start_seconds" -v end="$end_seconds" 'BEGIN {printf "%.6f", end - start}')"
combined_hash="$(cat "$OUTPUT_DIR"/outputs/*.out | LC_ALL=C sort | sha256sum | awk '{print $1}')"
line_count="$(cat "$OUTPUT_DIR"/outputs/*.out | wc -l | awk '{print $1}')"
make_total="$(sum_metric '#   Make_CWS_Points total')"
ip_total="$(sum_metric '#   IP_Check total')"
timed_total="$(sum_metric '#   Timed-stage total')"

cat >"$OUTPUT_DIR/summary.txt" <<EOF
wall_seconds=$wall_seconds
combined_sorted_sha256=$combined_hash
output_lines=$line_count
make_cws_points_total=$make_total
ip_check_total=$ip_total
timed_stage_total=$timed_total
gpu_list=$GPU_LIST
workers_per_gpu=$WORKERS_PER_GPU
total_shards=$TOTAL_SHARDS
shard_start=$SHARD_START
shard_end=$SHARD_END
cuda_candidate_batch=$CUDA_CANDIDATE_BATCH
cuda_batch_lanes=$CUDA_BATCH_LANES
frontier_batch=$FRONTIER_BATCH
EOF

echo "wall_seconds=$wall_seconds"
echo "combined_sorted_sha256=$combined_hash"
echo "output_lines=$line_count"
echo "make_cws_points_total=$make_total"
echo "ip_check_total=$ip_total"
echo "timed_stage_total=$timed_total"