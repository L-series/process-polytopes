#!/usr/bin/env bash
#SBATCH --job-name=proc_poly
#SBATCH --partition=all
#SBATCH --cpus-per-task=1
#SBATCH --mem-per-cpu=4G
#SBATCH --time=08:00:00
#SBATCH --output=logs/proc_poly/%A_%a.out
#SBATCH --error=logs/proc_poly/%A_%a.err
#
# SLURM array body: each task processes one input row group and writes one
# output shard. The array index == input row-group index == output part index.
#
# Submit via scripts/submit_process_polytopes.sh (sets --array and env).
#
# Env (exported by the submit wrapper):
#   INPUT       path to unique_polytopes.parquet
#   OUTDIR      output dataset directory
#   RG_PER_TASK row groups per task (default 1)
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
BIN="$REPO_ROOT/src/process/build/process_polytopes"

: "${INPUT:?INPUT not set}"
: "${OUTDIR:?OUTDIR not set}"
RG_PER_TASK="${RG_PER_TASK:-1}"

# Arrow uses the micromamba env's shared libs (rpath is baked in, but be safe).
export LD_LIBRARY_PATH="${CONDA_PREFIX:-$HOME/.local/share/micromamba/envs/process-polytopes}/lib:${LD_LIBRARY_PATH:-}"

task="${SLURM_ARRAY_TASK_ID:?must run as an array task}"
rg_start=$(( task * RG_PER_TASK ))
rg_end=$(( rg_start + RG_PER_TASK ))

out=$(printf "%s/part-%05d.parquet" "$OUTDIR" "$task")
tmp="${out}.tmp"

# Idempotent: skip tasks whose final shard already exists (clean resume).
if [[ -s "$out" ]]; then
    echo "task $task: $out already exists, skipping"
    exit 0
fi

echo "task $task: row groups [$rg_start,$rg_end) -> $out  (host $(hostname))"
srun "$BIN" --input "$INPUT" --output "$tmp" \
     --rg-start "$rg_start" --rg-end "$rg_end"

mv -f "$tmp" "$out"
echo "task $task: done"
