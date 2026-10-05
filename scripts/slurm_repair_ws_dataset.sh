#!/usr/bin/env bash
#SBATCH --job-name=repair_ws
#SBATCH --partition=all
#SBATCH --nodes=1
#SBATCH --ntasks=1
#SBATCH --cpus-per-task=1
#SBATCH --mem=96G
#SBATCH --time=12:00:00
#SBATCH --output=logs/proc_poly/repair-ws-%j.out
#SBATCH --error=logs/proc_poly/repair-ws-%j.err
set -euo pipefail

REPO_ROOT="${REPO_ROOT:-$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)}"
export CONDA_PREFIX="${CONDA_PREFIX:-$HOME/.local/share/micromamba/envs/process-polytopes}"
export LD_LIBRARY_PATH="$CONDA_PREFIX/lib:${LD_LIBRARY_PATH:-}"

WEIGHTS="${WEIGHTS:-$HOME/data/ws5d_sieved_dataset_v2.parquet}"
CLEAN="${CLEAN:-$HOME/data/unique_polytopes_clean.parquet}"
OUTPUT="${OUTPUT:-$HOME/data/ws5d_sieved_dataset_v2_clean.parquet}"
LIMIT_ARGS=()
BIN="$REPO_ROOT/src/process/build/repair_ws_dataset"

while [[ $# -gt 0 ]]; do
    case "$1" in
        --weights) WEIGHTS="$2"; shift 2 ;;
        --clean)   CLEAN="$2"; shift 2 ;;
        --output)  OUTPUT="$2"; shift 2 ;;
        --limit-weight-row-groups) LIMIT_ARGS=(--limit-weight-row-groups "$2"); shift 2 ;;
        --help|-h)
            echo "usage: scripts/slurm_repair_ws_dataset.sh [--weights F] [--clean F] [--output F]"
            exit 0
            ;;
        *) echo "unknown arg: $1"; exit 1 ;;
    esac
done

tmp="${OUTPUT}.tmp"
rm -f "$tmp"

echo "weights: $WEIGHTS"
echo "clean:   $CLEAN"
echo "output:  $OUTPUT"
echo "tmp:     $tmp"

"$BIN" --weights "$WEIGHTS" --clean "$CLEAN" --output "$tmp" "${LIMIT_ARGS[@]}"
mv -f "$tmp" "$OUTPUT"
echo "done: $OUTPUT"
