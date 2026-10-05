#!/usr/bin/env bash
# submit_process_polytopes.sh — Launch the reprocessing SLURM array.
#
# Derives the task count from the parquet row-group count and submits the array
# with a concurrency cap. Each task processes RG_PER_TASK row groups.
#
# Usage:
#   scripts/submit_process_polytopes.sh [--max-concurrent N] [--rg-per-task K] \
#       [--input F] [--outdir D] [--dry-run]
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
export CONDA_PREFIX="${CONDA_PREFIX:-$HOME/.local/share/micromamba/envs/process-polytopes}"

INPUT="$HOME/data/unique_polytopes.parquet"
OUTDIR="$HOME/data/unique_polytopes_processed"
MAX_CONCURRENT=768          # ~6 nodes x 128 cores
RG_PER_TASK=1
DRY_RUN=0

while [[ $# -gt 0 ]]; do
    case "$1" in
        --max-concurrent) MAX_CONCURRENT="$2"; shift 2 ;;
        --rg-per-task)    RG_PER_TASK="$2"; shift 2 ;;
        --input)          INPUT="$2"; shift 2 ;;
        --outdir)         OUTDIR="$2"; shift 2 ;;
        --dry-run)        DRY_RUN=1; shift ;;
        *) echo "unknown arg: $1"; exit 1 ;;
    esac
done

BIN="$REPO_ROOT/src/process/build/process_polytopes"
[[ -x "$BIN" ]] || { echo "build first: bash src/process/build.sh"; exit 1; }
[[ -e "$INPUT" ]] || { echo "input not found: $INPUT"; exit 1; }

# Row-group count straight from the parquet footer (tiny C++ probe is overkill;
# use the worker's own reader by asking for an out-of-range group is awkward, so
# read it via python-free arrow is not available — use the cached known value if
# the helper is absent).
NRG=$("$REPO_ROOT/src/process/build/rg_count" "$INPUT" 2>/dev/null || true)
if [[ -z "${NRG:-}" ]]; then
    echo "ERROR: cannot determine row-group count (build rg_count helper)."; exit 1
fi

NTASKS=$(( (NRG + RG_PER_TASK - 1) / RG_PER_TASK ))
LAST=$(( NTASKS - 1 ))

mkdir -p "$OUTDIR" "$REPO_ROOT/logs/proc_poly"

echo "input:           $INPUT"
echo "row groups:      $NRG"
echo "rg per task:     $RG_PER_TASK"
echo "array tasks:     0-$LAST (%$MAX_CONCURRENT)"
echo "output dir:      $OUTDIR"

# Free-space sanity (output is larger than the 84 GB input due to nf lists).
avail_gb=$(df -P "$OUTDIR" | awk 'NR==2{print int($4/1024/1024)}')
echo "free space:      ${avail_gb} GB"
if (( avail_gb < 200 )); then
    echo "WARNING: < 200 GB free at $OUTDIR"
fi

CMD=(sbatch --array="0-${LAST}%${MAX_CONCURRENT}"
     --export=ALL,INPUT="$INPUT",OUTDIR="$OUTDIR",RG_PER_TASK="$RG_PER_TASK",CONDA_PREFIX="$CONDA_PREFIX"
     "$REPO_ROOT/scripts/slurm_process_polytopes.sh")

if (( DRY_RUN )); then
    echo "DRY RUN: ${CMD[*]}"
else
    "${CMD[@]}"
fi
