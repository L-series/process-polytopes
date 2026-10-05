#!/usr/bin/env bash
#SBATCH --job-name=proc_poly_nodes
#SBATCH --partition=all
#SBATCH --nodes=1
#SBATCH --ntasks-per-node=128
#SBATCH --cpus-per-task=1
#SBATCH --mem-per-cpu=4G
#SBATCH --time=08:00:00
#SBATCH --output=logs/proc_poly/%j.out
#SBATCH --error=logs/proc_poly/%j.err
#
# One SLURM allocation on a CPU node. Work is parallelized inside the job with
# one srun task per CPU; each rank processes a strided subset of parquet
# row-group shards and writes idempotent output parts. Multiple copies of this
# job can be launched with JOB_INDEX/TOTAL_JOBS to split the shard stream.
#
# Env (exported by the submit wrapper):
#   INPUT        path to unique_polytopes.parquet
#   OUTDIR       computed-shard output directory
#   RG_PER_SHARD row groups per output shard (default 4)
#   NRG          input row-group count
#   JOB_INDEX    zero-based index among coordinated jobs (default 0)
#   TOTAL_JOBS   number of coordinated jobs (default 1)
#   WORKER_OFFSET first global worker id for this job (overrides JOB_INDEX)
#   TOTAL_WORKERS total coordinated workers (overrides TOTAL_JOBS calculation)
set -euo pipefail

REPO_ROOT="${REPO_ROOT:-$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)}"
BIN="$REPO_ROOT/src/process/build/process_polytopes"

: "${INPUT:?INPUT not set}"
: "${OUTDIR:?OUTDIR not set}"
: "${NRG:?NRG not set}"
RG_PER_SHARD="${RG_PER_SHARD:-4}"
JOB_INDEX="${JOB_INDEX:-0}"
TOTAL_JOBS="${TOTAL_JOBS:-1}"
WORKER_OFFSET="${WORKER_OFFSET:-}"
TOTAL_WORKERS="${TOTAL_WORKERS:-}"

export REPO_ROOT BIN INPUT OUTDIR NRG RG_PER_SHARD JOB_INDEX TOTAL_JOBS WORKER_OFFSET TOTAL_WORKERS
export LD_LIBRARY_PATH="${CONDA_PREFIX:-$HOME/.local/share/micromamba/envs/process-polytopes}/lib:${LD_LIBRARY_PATH:-}"

mkdir -p "$OUTDIR" "$REPO_ROOT/logs/proc_poly"

NSHARDS=$(( (NRG + RG_PER_SHARD - 1) / RG_PER_SHARD ))
export NSHARDS

echo "job:             $SLURM_JOB_ID"
echo "nodes:           ${SLURM_JOB_NODELIST:-unknown}"
echo "ntasks:          ${SLURM_NTASKS:-unknown}"
echo "input:           $INPUT"
echo "row groups:      $NRG"
echo "rg per shard:    $RG_PER_SHARD"
echo "output shards:   $NSHARDS"
echo "output dir:      $OUTDIR"
echo "job index:       $JOB_INDEX / $TOTAL_JOBS"
if [[ -n "$TOTAL_WORKERS" ]]; then
    echo "worker offset:   ${WORKER_OFFSET:-0} / $TOTAL_WORKERS"
fi

srun --kill-on-bad-exit=1 bash -lc '
set -euo pipefail

rank="${SLURM_PROCID:?missing SLURM_PROCID}"
ntasks="${SLURM_NTASKS:?missing SLURM_NTASKS}"
if [[ -n "${TOTAL_WORKERS:-}" ]]; then
    worker=$(( ${WORKER_OFFSET:-0} + rank ))
    total_workers="$TOTAL_WORKERS"
else
    worker=$(( JOB_INDEX * ntasks + rank ))
    total_workers=$(( TOTAL_JOBS * ntasks ))
fi
host="$(hostname)"
echo "rank ${rank}/${ntasks} worker ${worker}/${total_workers} on ${host}: starting"

for (( shard = worker; shard < NSHARDS; shard += total_workers )); do
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
    echo "rank ${rank}: shard ${shard} row groups [${rg_start},${rg_end}) -> ${out}"
    "$BIN" --input "$INPUT" --output "$tmp" --rg-start "$rg_start" --rg-end "$rg_end" --computed-only
    mv -f "$tmp" "$out"
    rm -rf "$lock"
    trap - EXIT
done

echo "rank ${rank}/${ntasks} worker ${worker}/${total_workers} on ${host}: done"
'
