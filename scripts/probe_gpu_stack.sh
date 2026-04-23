#!/usr/bin/env bash

set -euo pipefail

MODE="${1:-all}"
STATUS=0

have_cmd() {
    command -v "$1" >/dev/null 2>&1
}

probe_cuda() {
    local status=0

    echo "=== CUDA probe ==="
    if have_cmd nvcc; then
        nvcc --version | tail -n2
    else
        echo "nvcc: missing"
        status=1
    fi

    if have_cmd nvidia-smi; then
        if ! nvidia-smi -L; then
            status=1
        fi
        nvidia-smi --query-gpu=name,driver_version,memory.total,compute_cap --format=csv,noheader || true
    else
        echo "nvidia-smi: missing"
        status=1
    fi

    echo ""
    return "$status"
}

probe_rocm() {
    local status=0
    local info_file

    info_file="$(mktemp)"
    trap 'rm -f "$info_file"' RETURN

    echo "=== ROCm probe ==="
    if have_cmd hipcc; then
        hipcc --version | head -n2
    else
        echo "hipcc: missing"
        status=1
    fi

    if have_cmd rocminfo; then
        if rocminfo >"$info_file" 2>&1; then
            grep -E '^[[:space:]]*(Marketing Name|Name):' "$info_file" | head -n12 || true
        else
            echo "rocminfo failed:"
            sed -n '1,20p' "$info_file"
            status=1
        fi
    else
        echo "rocminfo: missing"
        status=1
    fi

    if have_cmd rocm-smi; then
        rocm-smi --showproductname --showdriverversion 2>/dev/null | sed -n '1,20p' || true
    else
        echo "rocm-smi: missing"
        status=1
    fi

    if have_cmd amd-smi; then
        amd-smi list 2>/dev/null | sed -n '1,20p' || true
    elif have_cmd amdsmi; then
        amdsmi list 2>/dev/null | sed -n '1,20p' || true
    else
        echo "amd-smi: missing"
    fi

    echo ""
    return "$status"
}

case "$MODE" in
    cuda)
        probe_cuda || STATUS=$?
        ;;
    rocm)
        probe_rocm || STATUS=$?
        ;;
    all)
        probe_cuda || STATUS=$?
        probe_rocm || STATUS=$?
        ;;
    *)
        echo "usage: $0 [cuda|rocm|all]" >&2
        exit 2
        ;;
esac

exit "$STATUS"