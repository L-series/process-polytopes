#!/usr/bin/env bash
#SBATCH --job-name=np-profile
#SBATCH --partition=std
#SBATCH --nodes=1
#SBATCH --cpus-per-task=128
#SBATCH --mem=0
#SBATCH --time=01:00:00
#SBATCH --output=logs/slurm/np-profile-%j.out
#SBATCH --error=logs/slurm/np-profile-%j.err
#
# Gather the pre-IP lattice-point-count distribution of dim-5 CWS candidates.
#
# Reuses PALP's own dim-5 enumeration: `cws-5d.x -c5 -s<id>` walks every
# canonical candidate, runs Make_CWS_Points on each, and (built with the
# PALP_PROFILE_NP=<stride> hook in PALP/cws.c) prints `NP <np> <dim> <ip>` for
# every <stride>-th candidate BEFORE the IP check.  The size-5 weight pool is
# read from the published w5.ip (PALP_W5_POOL) instead of being regenerated.
#
# Parallelism: each worker is one slot-0 shard (`-j J -k k`).  Workers run
# concurrently on one node; their NP lines are concatenated and analyzed.
#
# Env knobs (with sensible per-structure defaults below):
#   STRUCTURE_ID   2..47          (required; 3 and 12 are the targets)
#   SHARD_J        -j value
#   STRIDE         PALP_PROFILE_NP (sample 1/STRIDE of traversed candidates)
#   KLIST          space-separated -k worker indices
#   WORKER_TIMEOUT seconds, per-worker safety cap (default 1800)
#   OUTDIR         output directory
set -euo pipefail

# Under sbatch the script is copied into the slurmd spool, so BASH_SOURCE no
# longer points into the repo; use SLURM_SUBMIT_DIR (the dir sbatch was invoked
# from = repo root) when running as a batch job, else derive from this file.
if [[ -n "${SLURM_SUBMIT_DIR:-}" ]]; then
  REPO_ROOT="$SLURM_SUBMIT_DIR"
  SCRIPT_DIR="$REPO_ROOT/scripts"
else
  SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
  REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
fi

W5_POOL="${PALP_W5_POOL:-$REPO_ROOT/results/cache/w5.ip}"
CWS_BIN="$REPO_ROOT/PALP/cws-5d.x"
POOL5_SIZE=1833327          # size-5 / shared-3 selected pool (both type-3 slots)

STRUCTURE_ID="${STRUCTURE_ID:?set STRUCTURE_ID (e.g. 3 or 12)}"
WORKER_TIMEOUT="${WORKER_TIMEOUT:-1800}"
OUTDIR="${OUTDIR:-$REPO_ROOT/results/point-count-profile/structure-$STRUCTURE_ID}"

# ---- per-structure sampling defaults ------------------------------------
if [[ -z "${KLIST:-}" || -z "${SHARD_J:-}" || -z "${STRIDE:-}" ]]; then
  case "$STRUCTURE_ID" in
    3)
      # type 3: full type-3 enumeration is ~10^13 candidates, far too many to
      # traverse.  Sub-sample slot 0 to 120 anchors spread evenly across the
      # whole 1.83M pool (so every degree band is represented), then sample
      # 1/STRIDE within each anchor's full slot-1 sweep.
      SHARD_J="${SHARD_J:-$POOL5_SIZE}"
      STRIDE="${STRIDE:-30}"
      W=120
      KLIST=""
      for ((i=1; i<=W; i++)); do
        KLIST+=" $(( 1 + (i-1)*(POOL5_SIZE-1)/(W-1) ))"
      done
      ;;
    12)
      # type 12: slot 0 has only 95 systems, so -j 95 with k=1..95 partitions
      # slot 0 exactly one-per-worker and the union is the FULL type-12
      # enumeration (slot1 x slot2 swept completely per worker).  Sample
      # 1/STRIDE of the traversed candidates.
      # block ~2.9e9 candidates per slot-0 anchor (~245s loop-only); 95 anchors
      # => ~2.75e11 total.  stride 50000 -> ~5.5M samples.
      SHARD_J="${SHARD_J:-95}"
      STRIDE="${STRIDE:-50000}"
      KLIST="$(seq 1 95 | tr '\n' ' ')"
      ;;
    *)
      echo "no default sampling plan for structure $STRUCTURE_ID; set SHARD_J, STRIDE, KLIST" >&2
      exit 1
      ;;
  esac
fi

mkdir -p "$OUTDIR" "$REPO_ROOT/logs/slurm"
echo "structure=$STRUCTURE_ID  J=$SHARD_J  stride=$STRIDE  workers=$(wc -w <<<"$KLIST")  out=$OUTDIR"
echo "w5 pool: $W5_POOL"
[[ -x "$CWS_BIN" ]] || { echo "missing $CWS_BIN (build: make -C PALP -f GNUmakefile cws-5d.x)"; exit 1; }

rm -f "$OUTDIR"/np_k*.txt
START=$(date +%s)
for k in $KLIST; do
  (
    PALP_W5_POOL="$W5_POOL" PALP_PROFILE_NP="$STRIDE" \
      timeout "$WORKER_TIMEOUT" "$CWS_BIN" -c5 -s"$STRUCTURE_ID" -j "$SHARD_J" -k "$k" \
      2>/dev/null | grep '^NP ' > "$OUTDIR/np_k${k}.txt" || true
  ) &
done
wait
END=$(date +%s)
echo "workers done in $((END-START))s"

cat "$OUTDIR"/np_k*.txt > "$OUTDIR/np_all.txt"
echo "total NP lines: $(wc -l < "$OUTDIR/np_all.txt")"
python3 "$SCRIPT_DIR/analyze_point_counts.py" "$OUTDIR/np_all.txt" | tee "$OUTDIR/summary.txt"
