#!/usr/bin/env bash
#SBATCH --job-name=gpu-prof
#SBATCH --partition=gpu
#SBATCH --gres=gpu:RTX6000BW:1
#SBATCH --cpus-per-task=8
#SBATCH --mem=32G
#SBATCH --time=00:40:00
#SBATCH --output=logs/slurm/gpu-prof-%j.out
#SBATCH --error=logs/slurm/gpu-prof-%j.err
#
# Profile the GPU CWS-generation + point-enumeration + IP-check pipeline
# (cuda_dim5_cws_scan, NO normal-form stage) for the 5-5 (type 3) structure.
#
# Sources of truth:
#   * generation throughput   : count-only scan kernel (prefix_candidates/sec)
#   * processing throughput   : gpu_ip "candidates_per_second"
#   * stage split             : --ip-stage-profile clock64 counters
#                               (point_cycles vs ip_cycles + IP substages)
#   * early rejection / np    : DeviceIpStats counters + avg/max points
#   * occupancy               : nvidia-smi sampling (SM util %) during each run,
#                               plus ncu achieved-occupancy if permitted, plus
#                               nsys timeline (kernel durations vs gaps).
#
# Two buffer regimes are compared because the per-candidate point buffer
# (--ip-max-points, default 2,000,000 == PALP POINT_Nmax) dominates VRAM and
# caps the launch grid:
#   * default 2,000,000  -> ~800 concurrent candidates  (current reality)
#   * 4,096              -> covers ALL type-3 np (max ~2033) and frees the grid
#                           (what a per-np bucketing optimization could achieve)
set -euo pipefail

if [[ -n "${SLURM_SUBMIT_DIR:-}" ]]; then
  REPO_ROOT="$SLURM_SUBMIT_DIR"
else
  REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
fi
BIN="$REPO_ROOT/src/classify/build-cuda/cuda_dim5_cws_scan"
W5="$REPO_ROOT/results/cache/w5.ip"
STRUCT="${STRUCT:-3}"
SHARDS="${SHARDS:-4000}"; SHARD_IDX="${SHARD_IDX:-2000}"   # mid-space => representative
EMIT_DEFAULT="${EMIT_DEFAULT:-20000}"      # 2M-buffer runs are ~750 cand/s => ~30s
EMIT_SMALL="${EMIT_SMALL:-500000}"         # 4096-buffer runs (full grid)
RUN_TMO="${RUN_TMO:-150}"                   # per-run safety cap (s)
OUTDIR="${OUTDIR:-$REPO_ROOT/results/pipeline-profile/gpu-structure-$STRUCT}"

[[ -x "$BIN" ]] || { echo "missing $BIN"; exit 1; }
mkdir -p "$OUTDIR" "$REPO_ROOT/logs/slurm"
rm -f "$OUTDIR"/*.log "$OUTDIR"/*.csv "$OUTDIR"/summary.txt
cd "$REPO_ROOT"

GPU_PHYS="${CUDA_VISIBLE_DEVICES%%,*}"; GPU_PHYS="${GPU_PHYS:-0}"
echo "host=$(hostname) gpu_phys=$GPU_PHYS struct=$STRUCT shard=$SHARD_IDX/$SHARDS"
nvidia-smi --query-gpu=name,memory.total,clocks.max.sm --format=csv,noheader -i "$GPU_PHYS"

# run a command while sampling SM/mem utilization every 100 ms into a CSV
run_sampled() {  # tag  cmd...
  local tag="$1"; shift
  local csv="$OUTDIR/${tag}.csv" log="$OUTDIR/${tag}.log"
  ( nvidia-smi --query-gpu=utilization.gpu,utilization.memory,memory.used,power.draw,clocks.sm \
      --format=csv,noheader,nounits -i "$GPU_PHYS" -lms 100 > "$csv" 2>/dev/null ) &
  local smp=$!
  timeout "$RUN_TMO" "$@" > "$log" 2>&1 || true
  sleep 0.2; kill "$smp" 2>/dev/null || true; wait "$smp" 2>/dev/null || true
  # mean SM util while the GPU was actually doing work (>1%)
  awk -F, '{u=$1+0; if(u>1){s+=u;n++}; if(u>mx)mx=u} END{if(n>0) printf "%s: mean_SM_busy=%.1f%% peak=%.0f%% (samples_busy=%d)\n","'"$tag"'",s/n,mx,n; else print "'"$tag"': no busy samples"}' "$csv"
}

echo; echo "########## GPU runs ##########"

# 1) generation throughput (count-only scan kernel)
echo "### GEN: count-only scan ###"
run_sampled gen "$BIN" --structure-id "$STRUCT" --w5 "$W5" \
  --shard-count "$SHARDS" --shard-index "$SHARD_IDX"

# 2-5) IP filtering: {serial,block} x {2M,4096} point buffers
for mode in serial block; do
  flag=""; [[ "$mode" == block ]] && flag="--block-ip"
  for pts in 2000000 4096; do
    emit=$EMIT_DEFAULT; [[ "$pts" == 4096 ]] && emit=$EMIT_SMALL
    tag="ip_${mode}_pts${pts}"
    echo "### ${tag} (emit=$emit) ###"
    run_sampled "$tag" "$BIN" --structure-id "$STRUCT" --w5 "$W5" \
      --shard-count "$SHARDS" --shard-index "$SHARD_IDX" \
      --emit-capacity "$emit" --ip-check --ip-stage-profile --ip-max-points "$pts" $flag
  done
done

# 6) ncu achieved occupancy / throughput on the IP kernel (small launch)
echo; echo "### ncu (achieved occupancy / SM & DRAM throughput) ###"
timeout 300 ncu --target-processes all --launch-count 1 --kernel-name-base demangled \
    --metrics sm__throughput.avg.pct_of_peak_sustained_elapsed,gpu__compute_memory_throughput.avg.pct_of_peak_sustained_elapsed,sm__warps_active.avg.pct_of_peak_sustained_active,launch__waves_per_multiprocessor \
    "$BIN" --structure-id "$STRUCT" --w5 "$W5" --shard-count 500000 --shard-index 250000 \
    --emit-capacity 40000 --ip-check --ip-max-points 4096 \
    > "$OUTDIR/ncu.log" 2>&1 || echo "  (ncu failed/timed out — see ncu.log; may be admin-gated)"
grep -E "sm__throughput|memory_throughput|warps_active|waves_per|No permission|ERR_NVGPUCTRPERM|not permitted" "$OUTDIR/ncu.log" | head -20 || true

# 7) nsys timeline (kernel durations vs idle gaps)
echo; echo "### nsys (kernel timeline) ###"
timeout 150 nsys profile -o "$OUTDIR/nsys_ip" --force-overwrite true --stats=true \
    "$BIN" --structure-id "$STRUCT" --w5 "$W5" --shard-count 50000 --shard-index 25000 \
    --emit-capacity 500000 --ip-check --ip-max-points 4096 \
    > "$OUTDIR/nsys.log" 2>&1 || echo "  (nsys failed — see nsys.log)"
grep -A18 "CUDA GPU Kernel Summary\|gpukernsum" "$OUTDIR/nsys.log" 2>/dev/null | head -24 || true

# ---- summarize ----------------------------------------------------------
PY="$REPO_ROOT/scripts/analyze_gpu_profile.py"
{
  echo "########## GPU PIPELINE PROFILE — structure $STRUCT ($(hostname)) ##########"
  nvidia-smi --query-gpu=name,memory.total --format=csv,noheader -i "$GPU_PHYS"
  echo
  python3 "$PY" "$OUTDIR"
} | tee "$OUTDIR/summary.txt"
echo; echo "wrote $OUTDIR/summary.txt"
