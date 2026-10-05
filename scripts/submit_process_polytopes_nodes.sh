#!/usr/bin/env bash
# submit_process_polytopes_nodes.sh - Launch one multi-node SLURM job.
#
# This intentionally avoids a SLURM job array. The submitted job allocates CPU
# nodes and uses srun ranks inside that allocation to process parquet shards.
#
# Usage:
#   scripts/submit_process_polytopes_nodes.sh [--nodes N] [--ntasks-per-node N] \
#       [--partition P] [--rg-per-shard K] [--input F] [--outdir D] [--dry-run]
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
export CONDA_PREFIX="${CONDA_PREFIX:-$HOME/.local/share/micromamba/envs/process-polytopes}"

INPUT="$HOME/data/unique_polytopes.parquet"
OUTDIR="$HOME/data/unique_polytopes_computed"
NODES=3
NTASKS_PER_NODE=128
PARTITION=all
RG_PER_SHARD=4
DRY_RUN=0

while [[ $# -gt 0 ]]; do
    case "$1" in
        --nodes)            NODES="$2"; shift 2 ;;
        --ntasks-per-node)  NTASKS_PER_NODE="$2"; shift 2 ;;
        --partition)        PARTITION="$2"; shift 2 ;;
        --rg-per-shard)     RG_PER_SHARD="$2"; shift 2 ;;
        --input)            INPUT="$2"; shift 2 ;;
        --outdir)           OUTDIR="$2"; shift 2 ;;
        --dry-run)          DRY_RUN=1; shift ;;
        *) echo "unknown arg: $1"; exit 1 ;;
    esac
done

BIN="$REPO_ROOT/src/process/build/process_polytopes"
RG_COUNT="$REPO_ROOT/src/process/build/rg_count"
[[ -x "$BIN" ]] || { echo "build first: bash src/process/build.sh"; exit 1; }
[[ -x "$RG_COUNT" ]] || { echo "build first: bash src/process/build.sh"; exit 1; }
[[ -e "$INPUT" ]] || { echo "input not found: $INPUT"; exit 1; }

NRG=$("$RG_COUNT" "$INPUT")
NTASKS=$(( NODES * NTASKS_PER_NODE ))
NSHARDS=$(( (NRG + RG_PER_SHARD - 1) / RG_PER_SHARD ))

mkdir -p "$OUTDIR" "$REPO_ROOT/logs/proc_poly"

echo "input:           $INPUT"
echo "row groups:      $NRG"
echo "rg per shard:    $RG_PER_SHARD"
echo "output shards:   $NSHARDS"
echo "nodes:           $NODES"
echo "ntasks/node:     $NTASKS_PER_NODE"
echo "total tasks:     $NTASKS"
echo "partition:       $PARTITION"
echo "output dir:      $OUTDIR"

avail_gb=$(df -P "$OUTDIR" | awk 'NR==2{print int($4/1024/1024)}')
echo "free space:      ${avail_gb} GB"
if (( avail_gb < 200 )); then
    echo "WARNING: < 200 GB free at $OUTDIR"
fi

CMD=(sbatch
     --partition="$PARTITION"
     --nodes="$NODES"
     --ntasks-per-node="$NTASKS_PER_NODE"
     --export=ALL,REPO_ROOT="$REPO_ROOT",INPUT="$INPUT",OUTDIR="$OUTDIR",RG_PER_SHARD="$RG_PER_SHARD",NRG="$NRG",CONDA_PREFIX="$CONDA_PREFIX"
     "$REPO_ROOT/scripts/slurm_process_polytopes_nodes.sh")

if (( DRY_RUN )); then
    echo "DRY RUN: ${CMD[*]}"
else
    "${CMD[@]}"
fi
