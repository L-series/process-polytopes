#!/usr/bin/env bash
# Capture local/SLURM/GPU environment details for reproducible runs.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
. "$SCRIPT_DIR/env_local.sh"

echo "=== process-polytopes environment ==="
echo "date=$(date -Is)"
echo "host=$(hostname -f 2>/dev/null || hostname)"
echo "repo=$REPO_ROOT"
echo "git=$(git -C "$REPO_ROOT" rev-parse --short HEAD 2>/dev/null || echo unknown)"
echo "palp_git=$(git -C "$REPO_ROOT/PALP" rev-parse --short HEAD 2>/dev/null || echo unknown)"
echo "user=$(id -un)"
echo "pwd=$PWD"
echo ""

echo "=== local toolchain ==="
command -v gcc >/dev/null 2>&1 && gcc --version | head -1 || true
command -v g++ >/dev/null 2>&1 && g++ --version | head -1 || true
command -v cmake >/dev/null 2>&1 && cmake --version | head -1 || true
command -v ninja >/dev/null 2>&1 && ninja --version | sed 's/^/ninja /' || true
command -v lean >/dev/null 2>&1 && lean --version | head -1 || true
command -v lake >/dev/null 2>&1 && lake --version | head -1 || true
pkg-config --modversion arrow parquet 2>/dev/null | sed '1s/^/arrow /;2s/^/parquet /' || true
echo "CONDA_PREFIX=${CONDA_PREFIX:-}"
echo ""

echo "=== CPU/RAM ==="
grep -m1 'model name' /proc/cpuinfo | sed 's/^/cpu /' || true
echo "logical_cpus=$(nproc)"
awk '/MemTotal/ {printf "mem_total_gb=%.1f\n", $2/1024/1024}' /proc/meminfo || true
df -h . | sed 's/^/disk /'
echo ""

echo "=== SLURM ==="
env | grep -E '^SLURM_' | sort || true
if command -v sinfo >/dev/null 2>&1; then
    sinfo -N -o '%N %P %G %c %m %t' | sed 's/^/sinfo /' || true
fi
if command -v scontrol >/dev/null 2>&1 && [[ -n "${SLURM_JOB_ID:-}" ]]; then
    scontrol show job "$SLURM_JOB_ID" || true
fi
echo ""

echo "=== GPU/CUDA ==="
if command -v nvidia-smi >/dev/null 2>&1; then
    nvidia-smi -L || true
    nvidia-smi --query-gpu=index,name,memory.total,driver_version,cuda_version,compute_cap --format=csv || true
else
    echo "nvidia-smi=unavailable"
fi
command -v nvcc >/dev/null 2>&1 && nvcc --version || echo "nvcc=unavailable"
