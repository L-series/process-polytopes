#!/usr/bin/env bash

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
BUILD_DIR="${BUILD_DIR:-$REPO_ROOT/.type3-cuda-bench}"
DATASET="${DATASET:-$BUILD_DIR/type3_bounds.synthetic.bin}"
CPU_BIN="$BUILD_DIR/type3_bounds_cpu"
CUDA_BIN="$BUILD_DIR/type3_bounds_cuda"
CPU_CXX="${CPU_CXX:-c++}"
JOBS="${JOBS:-1000000}"
ITERATIONS="${ITERATIONS:-10}"
MAX_CONSTRAINTS="${MAX_CONSTRAINTS:-8}"
SEED="${SEED:-12345}"
SEED_EMPTY_PCT="${SEED_EMPTY_PCT:-74}"
TIGHTEN_EMPTY_PCT="${TIGHTEN_EMPTY_PCT:-6}"
SURVIVE_PCT="${SURVIVE_PCT:-20}"
CUDA_ARCH="${CUDA_ARCH:-}"
BLOCK_SIZE="${BLOCK_SIZE:-256}"
BLOCK_SIZES="${BLOCK_SIZES:-}"
CUDA_NIXPKGS_SET="${CUDA_NIXPKGS_SET:-}"
INPUT_DATASET="${INPUT_DATASET:-0}"

detect_cuda_arch() {
    local capability

    if ! command -v nvidia-smi >/dev/null 2>&1; then
        return 1
    fi

    capability="$(nvidia-smi --query-gpu=compute_cap --format=csv,noheader 2>/dev/null | head -n1 | tr -d '[:space:].')"
    if [[ -z "$capability" ]]; then
        return 1
    fi

    printf 'sm_%s\n' "$capability"
}

resolve_nix_cuda_output() {
    local package_set="$1"
    local package_name="$2"

    if [[ -z "$package_set" ]]; then
        return 1
    fi
    if ! command -v nix >/dev/null 2>&1; then
        return 1
    fi

    NIXPKGS_ALLOW_UNFREE=1 nix build --impure --no-link --print-out-paths \
        "nixpkgs#${package_set}.${package_name}" 2>/dev/null | head -n1
}

detect_nix_cudart_root() {
    local nvcc_path

    nvcc_path="$(command -v nvcc 2>/dev/null || true)"
    if [[ -n "$nvcc_path" ]]; then
        nvcc_path="$(readlink -f "$nvcc_path" 2>/dev/null || printf '%s\n' "$nvcc_path")"
    fi
    if [[ "$nvcc_path" != /nix/store/* ]]; then
        return 1
    fi
    resolve_nix_cuda_output cudaPackages cuda_cudart
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

if [[ -z "$CUDA_ARCH" ]]; then
    CUDA_ARCH="$(detect_cuda_arch || true)"
fi

if [[ -z "$CUDA_ARCH" ]]; then
    CUDA_ARCH="sm_89"
fi

NVCC_EXTRA_FLAGS=()
NVCC_BIN="$(command -v nvcc 2>/dev/null || true)"
NIX_NVCC_ROOT=""
NIX_CUDART_ROOT=""
HOST_CUDA_LIB_DIR="$(detect_host_cuda_lib_dir || true)"

if [[ -n "$CUDA_NIXPKGS_SET" ]]; then
    NIX_NVCC_ROOT="$(resolve_nix_cuda_output "$CUDA_NIXPKGS_SET" cuda_nvcc || true)"
    NIX_CUDART_ROOT="$(resolve_nix_cuda_output "$CUDA_NIXPKGS_SET" cuda_cudart || true)"
    if [[ -n "$NIX_NVCC_ROOT" ]]; then
        NVCC_BIN="$NIX_NVCC_ROOT/bin/nvcc"
    fi
else
    NIX_CUDART_ROOT="$(detect_nix_cudart_root || true)"
fi

if [[ -n "$NIX_CUDART_ROOT" ]]; then
    NVCC_EXTRA_FLAGS+=(
        -I"$NIX_CUDART_ROOT/include"
        -L"$NIX_CUDART_ROOT/lib"
        -Xlinker
        -rpath
        -Xlinker
        "$NIX_CUDART_ROOT/lib"
    )
fi

mkdir -p "$BUILD_DIR"

echo "=== type-3 bounds microbenchmark ==="
echo "Build dir:        $BUILD_DIR"
echo "Dataset:          $DATASET"
echo "Jobs:             $JOBS"
echo "Iterations:       $ITERATIONS"
echo "Max constraints:  $MAX_CONSTRAINTS"
echo "Seed:             $SEED"
echo "Target mix:       seed-empty=$SEED_EMPTY_PCT tighten-empty=$TIGHTEN_EMPTY_PCT survive=$SURVIVE_PCT"
echo "CPU compiler:     $CPU_CXX"
echo "CUDA arch:        $CUDA_ARCH"
if [[ -n "$CUDA_NIXPKGS_SET" ]]; then
    echo "CUDA toolkit:     nixpkgs#$CUDA_NIXPKGS_SET"
fi
if [[ -n "$NIX_CUDART_ROOT" ]]; then
    echo "CUDA runtime:     $NIX_CUDART_ROOT"
fi
if [[ -n "$HOST_CUDA_LIB_DIR" ]]; then
    echo "Driver libcuda:   $HOST_CUDA_LIB_DIR/libcuda.so.1"
fi
echo ""

"$CPU_CXX" -O3 -std=c++17 -march=native -I"$REPO_ROOT/src/verify" \
    "$REPO_ROOT/src/verify/harness_type3_cuda_bounds.cpp" \
    -o "$CPU_BIN"

if [[ "$INPUT_DATASET" != 1 ]]; then
    "$CPU_BIN" \
        --jobs "$JOBS" \
        --max-constraints "$MAX_CONSTRAINTS" \
        --seed "$SEED" \
        --seed-empty-pct "$SEED_EMPTY_PCT" \
        --tighten-empty-pct "$TIGHTEN_EMPTY_PCT" \
        --survive-pct "$SURVIVE_PCT" \
        --output "$DATASET" \
        --generate-only
fi

echo "--- CPU reference ---"
"$CPU_BIN" --input "$DATASET" --iterations "$ITERATIONS"
echo ""

if [[ -z "$NVCC_BIN" ]] || [[ ! -x "$NVCC_BIN" ]]; then
    echo "nvcc not found; skipping CUDA build"
    exit 0
fi

if ! nvidia-smi -L >/dev/null 2>&1; then
    echo "no visible NVIDIA GPU; skipping CUDA run"
    exit 0
fi

"$NVCC_BIN" -O3 -std=c++17 --cudart shared -Wno-deprecated-gpu-targets \
    -arch="$CUDA_ARCH" \
    -I"$REPO_ROOT/src/verify" "${NVCC_EXTRA_FLAGS[@]}" \
    "$REPO_ROOT/src/verify/harness_type3_cuda_bounds.cu" \
    -o "$CUDA_BIN"

echo "--- CUDA kernel ---"
run_cuda_once() {
    local block_size="$1"

    echo "block_size=$block_size"
    if [[ -n "$HOST_CUDA_LIB_DIR" ]]; then
        LD_LIBRARY_PATH="$HOST_CUDA_LIB_DIR${LD_LIBRARY_PATH:+:$LD_LIBRARY_PATH}" \
            "$CUDA_BIN" --input "$DATASET" --iterations "$ITERATIONS" --block-size "$block_size"
    else
        "$CUDA_BIN" --input "$DATASET" --iterations "$ITERATIONS" --block-size "$block_size"
    fi
}

if [[ -n "$BLOCK_SIZES" ]]; then
    for block_size in $BLOCK_SIZES; do
        run_cuda_once "$block_size"
    done
else
    run_cuda_once "$BLOCK_SIZE"
fi