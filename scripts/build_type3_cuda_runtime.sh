#!/usr/bin/env bash

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
BUILD_DIR="${BUILD_DIR:-$REPO_ROOT/.type3-cuda-runtime}"
OUTPUT="${OUTPUT:-$BUILD_DIR/libtype3_bounds_runtime.so}"
CUDA_ARCH="${CUDA_ARCH:-}"
CUDA_NIXPKGS_SET="${CUDA_NIXPKGS_SET:-}"

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

if [[ -z "$CUDA_ARCH" ]]; then
    CUDA_ARCH="$(detect_cuda_arch || true)"
fi

if [[ -z "$CUDA_ARCH" ]]; then
    CUDA_ARCH="sm_89"
fi

NVCC_BIN="$(command -v nvcc 2>/dev/null || true)"
NVCC_EXTRA_FLAGS=()
NIX_CUDART_ROOT=""

if [[ -n "$CUDA_NIXPKGS_SET" ]]; then
    NIX_NVCC_ROOT="$(resolve_nix_cuda_output "$CUDA_NIXPKGS_SET" cuda_nvcc || true)"
    NIX_CUDART_ROOT="$(resolve_nix_cuda_output "$CUDA_NIXPKGS_SET" cuda_cudart || true)"
    if [[ -n "$NIX_NVCC_ROOT" ]]; then
        NVCC_BIN="$NIX_NVCC_ROOT/bin/nvcc"
    fi
else
    NIX_CUDART_ROOT="$(detect_nix_cudart_root || true)"
fi

if [[ -z "$NVCC_BIN" ]] || [[ ! -x "$NVCC_BIN" ]]; then
    echo "nvcc not found" >&2
    exit 1
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

"$NVCC_BIN" -O3 -std=c++17 --cudart shared -shared -Xcompiler -fPIC \
    -Wno-deprecated-gpu-targets -arch="$CUDA_ARCH" \
    -I"$REPO_ROOT/src/verify" "${NVCC_EXTRA_FLAGS[@]}" \
    "$REPO_ROOT/src/verify/type3_bounds_runtime.cu" \
    -o "$OUTPUT"

printf '%s\n' "$OUTPUT"