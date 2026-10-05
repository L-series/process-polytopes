#!/usr/bin/env bash
# Repair Hodge/geometric invariants in the sieved weight-system ML dataset.
#
# Output columns:
#   nf_vertices, weight_systems, count,
#   vertex_count, facet_count, point_count, dual_point_count,
#   h11, h12, h13, h22, chi, bh_mp, bh_mv, bh_np, bh_nv
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
export CONDA_PREFIX="${CONDA_PREFIX:-$HOME/.local/share/micromamba/envs/process-polytopes}"
export LD_LIBRARY_PATH="$CONDA_PREFIX/lib:${LD_LIBRARY_PATH:-}"

WEIGHTS="${WEIGHTS:-$HOME/data/ws5d_sieved_dataset_v2.parquet}"
CLEAN="${CLEAN:-$HOME/data/unique_polytopes_clean.parquet}"
OUTPUT="${OUTPUT:-$HOME/data/ws5d_sieved_dataset_v2_clean.parquet}"
LIMIT_ARGS=()

while [[ $# -gt 0 ]]; do
    case "$1" in
        --weights) WEIGHTS="$2"; shift 2 ;;
        --clean)   CLEAN="$2"; shift 2 ;;
        --output)  OUTPUT="$2"; shift 2 ;;
        --limit-weight-row-groups) LIMIT_ARGS=(--limit-weight-row-groups "$2"); shift 2 ;;
        --help|-h)
            echo "usage: scripts/repair_ws_dataset.sh [--weights F] [--clean F] [--output F]"
            exit 0
            ;;
        *) echo "unknown arg: $1"; exit 1 ;;
    esac
done

BIN="$REPO_ROOT/src/process/build/repair_ws_dataset"
[[ -x "$BIN" ]] || { echo "build first: bash src/process/build.sh"; exit 1; }
[[ -e "$WEIGHTS" ]] || { echo "weights file not found: $WEIGHTS"; exit 1; }
[[ -e "$CLEAN" ]] || { echo "clean file not found: $CLEAN"; exit 1; }
[[ ! -e "$OUTPUT" && ! -e "$OUTPUT.tmp" ]] || {
    echo "output already exists: $OUTPUT or $OUTPUT.tmp"; exit 1;
}

"$BIN" --weights "$WEIGHTS" --clean "$CLEAN" --output "$OUTPUT.tmp" "${LIMIT_ARGS[@]}"
mv -f "$OUTPUT.tmp" "$OUTPUT"
echo "done: $OUTPUT"
