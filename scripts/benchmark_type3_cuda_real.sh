#!/usr/bin/env bash

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
PALP_DIR="${PALP_DIR:-$REPO_ROOT/PALP}"
BUILD_DIR="${BUILD_DIR:-$REPO_ROOT/.type3-cuda-real}"
DATASET="${DATASET:-$BUILD_DIR/type3_bounds.real.bin}"
EXPORT_LIMIT="${EXPORT_LIMIT:-250000}"
BLOCK_SIZES="${BLOCK_SIZES:-64 128 256 512 1024}"
CWS_BIN="${CWS_BIN:-$PALP_DIR/cws.x}"
WF_FILE="${WF_FILE:-$PALP_DIR/cws/wf4-d1-20.txt}"
SHARD_COUNT="${SHARD_COUNT:-32}"
SHARD_INDEX="${SHARD_INDEX:-1}"
ITERATIONS="${ITERATIONS:-20}"
CUDA_NIXPKGS_SET="${CUDA_NIXPKGS_SET:-cudaPackages_12_6}"

mkdir -p "$BUILD_DIR"
rm -f "$DATASET"

echo "=== type-3 real-job export ==="
echo "PALP binary:      $CWS_BIN"
echo "Input file:       $WF_FILE"
echo "Export limit:     $EXPORT_LIMIT"
echo "Shard:            $SHARD_INDEX / $SHARD_COUNT"
echo "Dataset:          $DATASET"
echo "Block sizes:      $BLOCK_SIZES"
echo ""

if [[ ! -x "$CWS_BIN" ]]; then
    echo "cws binary not found: $CWS_BIN" >&2
    exit 1
fi

(
    cd "$PALP_DIR"
    PALP_TYPE3_BOUNDS_EXPORT="$DATASET" \
    PALP_TYPE3_BOUNDS_EXPORT_LIMIT="$EXPORT_LIMIT" \
    "$CWS_BIN" -c5 -T -I -n2 "$WF_FILE" "$WF_FILE" -s3 \
        -j"$SHARD_COUNT" -k"$SHARD_INDEX" /dev/null >/dev/null
)

if [[ ! -s "$DATASET" ]]; then
    echo "real-job export did not produce a dataset" >&2
    exit 1
fi

INPUT_DATASET=1 \
DATASET="$DATASET" \
ITERATIONS="$ITERATIONS" \
BLOCK_SIZES="$BLOCK_SIZES" \
CUDA_NIXPKGS_SET="$CUDA_NIXPKGS_SET" \
"$SCRIPT_DIR/benchmark_type3_cuda.sh"