#!/usr/bin/env bash
#SBATCH --job-name=merge_poly_parts
#SBATCH --partition=all
#SBATCH --nodes=1
#SBATCH --cpus-per-task=1
#SBATCH --mem-per-cpu=3G
#SBATCH --time=04:00:00
#SBATCH --output=logs/proc_poly/merge-parts-%j.out
#SBATCH --error=logs/proc_poly/merge-parts-%j.err
set -euo pipefail

REPO_ROOT="${REPO_ROOT:-$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)}"
BIN="$REPO_ROOT/src/process/build/merge_computed"

INPUT="${INPUT:-$HOME/data/unique_polytopes.parquet}"
COMPUTED_DIR="${COMPUTED_DIR:-$HOME/data/unique_polytopes_computed}"
OUTDIR="${OUTDIR:-$HOME/data/unique_polytopes_clean_dataset}"
RG_PER_SHARD="${RG_PER_SHARD:-4}"
NRG="${NRG:?NRG not set}"
WORKER_OFFSET="${WORKER_OFFSET:-0}"
TOTAL_WORKERS="${TOTAL_WORKERS:?TOTAL_WORKERS not set}"

export LD_LIBRARY_PATH="${CONDA_PREFIX:-$HOME/.local/share/micromamba/envs/process-polytopes}/lib:${LD_LIBRARY_PATH:-}"

mkdir -p "$OUTDIR" "$REPO_ROOT/logs/proc_poly"
NSHARDS=$(( (NRG + RG_PER_SHARD - 1) / RG_PER_SHARD ))
export REPO_ROOT BIN INPUT COMPUTED_DIR OUTDIR RG_PER_SHARD NRG NSHARDS WORKER_OFFSET TOTAL_WORKERS

echo "job:           ${SLURM_JOB_ID:-manual}"
echo "node:          ${SLURM_JOB_NODELIST:-unknown}"
echo "ntasks:        ${SLURM_NTASKS:-unknown}"
echo "input:         $INPUT"
echo "computed dir:  $COMPUTED_DIR"
echo "outdir:        $OUTDIR"
echo "rg/shard:      $RG_PER_SHARD"
echo "shards:        $NSHARDS"
echo "worker offset: $WORKER_OFFSET / $TOTAL_WORKERS"

srun --kill-on-bad-exit=1 bash -lc '
set -euo pipefail

rank="${SLURM_PROCID:?missing SLURM_PROCID}"
worker=$(( WORKER_OFFSET + rank ))
host="$(hostname)"
echo "rank ${rank} worker ${worker}/${TOTAL_WORKERS} on ${host}: starting"

for (( shard = worker; shard < NSHARDS; shard += TOTAL_WORKERS )); do
    rg_start=$(( shard * RG_PER_SHARD ))
    rg_end=$(( rg_start + RG_PER_SHARD ))
    if (( rg_end > NRG )); then
        rg_end="$NRG"
    fi

    out=$(printf "%s/part-%05d.parquet" "$OUTDIR" "$shard")
    tmp="${out}.${SLURM_JOB_ID:-manual}.${rank}.tmp"
    lock="${out}.lock"

    if [[ -s "$out" ]]; then
        echo "rank ${rank}: shard ${shard} already exists, skipping"
        continue
    fi
    if ! mkdir "$lock" 2>/dev/null; then
        echo "rank ${rank}: shard ${shard} is locked by another worker, skipping"
        continue
    fi
    trap "rm -rf \"$lock\" \"$tmp\"" EXIT

    rm -f "$tmp"
    echo "rank ${rank}: merge shard ${shard} row groups [${rg_start},${rg_end}) -> ${out}"
    "$BIN" --input "$INPUT" \
           --computed-dir "$COMPUTED_DIR" \
           --output "$tmp" \
           --rg-per-shard "$RG_PER_SHARD" \
           --rg-start "$rg_start" \
           --rg-end "$rg_end"
    mv -f "$tmp" "$out"
    rm -rf "$lock"
    trap - EXIT
done

echo "rank ${rank} worker ${worker}/${TOTAL_WORKERS} on ${host}: done"
'
