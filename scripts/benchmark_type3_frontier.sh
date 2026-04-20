#!/usr/bin/env bash

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
PALP_DIR="${PALP_DIR:-$REPO_ROOT/PALP}"
CWS_BIN="${CWS_BIN:-$PALP_DIR/cws.x}"
WF_FILE="${WF_FILE:-$PALP_DIR/cws/wf4-d1-20.txt}"
BENCH_DIR="${BENCH_DIR:-$REPO_ROOT/.type3-frontier-bench}"
RUNTIME_BUILD_DIR="${RUNTIME_BUILD_DIR:-$REPO_ROOT/.type3-cuda-runtime}"
CUDA_RUNTIME="${CUDA_RUNTIME:-$RUNTIME_BUILD_DIR/libtype3_bounds_runtime.so}"
CUDA_NIXPKGS_SET="${CUDA_NIXPKGS_SET:-cudaPackages_12_6}"
SHARD_COUNT="${SHARD_COUNT:-32}"
SHARD_INDEX="${SHARD_INDEX:-1}"
FRONTIER_BATCH="${FRONTIER_BATCH:-65536}"
CUDA_CANDIDATE_BATCH="${CUDA_CANDIDATE_BATCH:-1}"
CUDA_BATCH_LANES="${CUDA_BATCH_LANES:-4}"
MODES="${MODES:-baseline cpu cuda}"
AUTO_BUILD_CUDA="${AUTO_BUILD_CUDA:-1}"

if [[ "$BENCH_DIR" != /* ]]; then
    BENCH_DIR="$REPO_ROOT/$BENCH_DIR"
fi

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

run_case() {
    local mode="$1"
    local output_path="$BENCH_DIR/$mode.out"
    local log_path="$BENCH_DIR/$mode.log"
    local hash_path="$BENCH_DIR/$mode.sha256"
    local start_seconds end_seconds wall_seconds hash_value
    local -a env_args=()

    case "$mode" in
        baseline)
            ;;
        cpu)
            env_args+=(PALP_TYPE3_FRONTIER=cpu PALP_TYPE3_FRONTIER_BATCH="$FRONTIER_BATCH")
            ;;
        cuda)
            if [[ ! -f "$CUDA_RUNTIME" ]]; then
                if [[ "$AUTO_BUILD_CUDA" == 1 ]]; then
                    BUILD_DIR="$RUNTIME_BUILD_DIR" CUDA_NIXPKGS_SET="$CUDA_NIXPKGS_SET" \
                        "$SCRIPT_DIR/build_type3_cuda_runtime.sh" >/dev/null
                else
                    echo "skipping cuda mode; runtime library missing" >&2
                    return 0
                fi
            fi
            if ! nvidia-smi -L >/dev/null 2>&1; then
                echo "skipping cuda mode; no visible NVIDIA GPU" >&2
                return 0
            fi
            env_args+=(PALP_TYPE3_FRONTIER=cuda PALP_TYPE3_FRONTIER_BATCH="$FRONTIER_BATCH" PALP_TYPE3_CUDA_RUNTIME="$CUDA_RUNTIME" PALP_TYPE3_CUDA_CANDIDATE_BATCH="$CUDA_CANDIDATE_BATCH" PALP_TYPE3_CUDA_BATCH_LANES="$CUDA_BATCH_LANES")
            local host_cuda_lib_dir
            host_cuda_lib_dir="$(detect_host_cuda_lib_dir || true)"
            if [[ -n "$host_cuda_lib_dir" ]]; then
                env_args+=(LD_LIBRARY_PATH="$host_cuda_lib_dir${LD_LIBRARY_PATH:+:$LD_LIBRARY_PATH}")
            fi
            ;;
        *)
            echo "unknown mode: $mode" >&2
            exit 1
            ;;
    esac

    start_seconds="$(date +%s.%N)"
    (
        cd "$PALP_DIR"
        env "${env_args[@]}" \
            "$CWS_BIN" -c5 -T -I -n2 "$WF_FILE" "$WF_FILE" -s3 \
            -j"$SHARD_COUNT" -k"$SHARD_INDEX" "$output_path" 2>"$log_path"
    )
    end_seconds="$(date +%s.%N)"
    wall_seconds="$(awk -v start="$start_seconds" -v end="$end_seconds" 'BEGIN {printf "%.6f", end - start}')"

    hash_value="$(sha256sum "$output_path" | awk '{print $1}')"
    printf '%s\n' "$hash_value" >"$hash_path"

    printf '%s wall_seconds=%s sha256=%s\n' "$mode" "$wall_seconds" "$hash_value"
    grep -E '#   Make_CWS_Points total|#   IP_Check total|#   Timed-stage total' "$log_path" || true
    printf '\n'
}

mkdir -p "$BENCH_DIR"

if [[ ! -x "$CWS_BIN" ]]; then
    echo "cws binary not found: $CWS_BIN" >&2
    exit 1
fi

for mode in $MODES; do
    run_case "$mode"
done

if [[ -f "$BENCH_DIR/baseline.sha256" ]]; then
    baseline_hash="$(cat "$BENCH_DIR/baseline.sha256")"
    for mode in $MODES; do
        if [[ "$mode" == baseline ]] || [[ ! -f "$BENCH_DIR/$mode.sha256" ]]; then
            continue
        fi
        mode_hash="$(cat "$BENCH_DIR/$mode.sha256")"
        if [[ "$mode_hash" != "$baseline_hash" ]]; then
            echo "hash mismatch: $mode differs from baseline" >&2
            exit 1
        fi
    done
fi