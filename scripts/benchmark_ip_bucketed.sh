#!/usr/bin/env bash
#SBATCH --job-name=ip-bucket-bench
#SBATCH --partition=gpu
#SBATCH --gres=gpu:RTX6000BW:1
#SBATCH --cpus-per-task=8
#SBATCH --mem=64G
#SBATCH --time=00:25:00
#SBATCH --output=logs/slurm/ip-bucket-bench-%j.out
#SBATCH --error=logs/slurm/ip-bucket-bench-%j.err
#
# Throughput + SM occupancy for the split bucketed GPU IP pipeline vs the legacy
# fused kernels, on a representative coarse shard of the 5-5 (type 3) structure.
# No --ip-stage-profile (its clock64 atomics distort throughput).
# NB: no `set -e` here -- run() parses optional fields with grep, whose nonzero
# "no match" must not abort the whole benchmark sweep.
set -uo pipefail
cd "${SLURM_SUBMIT_DIR:-$(pwd)}"
BIN=src/classify/build-cuda/cuda_dim5_cws_scan
W5=results/cache/w5.ip
STRUCT=3
CS=4000; CI=2000; EMIT=300000            # mid-range shard, no per-candidate OOM at np_cap<=128
T=$(mktemp -d)
GPU_PHYS="${CUDA_VISIBLE_DEVICES%%,*}"; GPU_PHYS="${GPU_PHYS:-0}"
echo "host=$(hostname) shard=$CI/$CS emit=$EMIT"
nvidia-smi --query-gpu=name,memory.total --format=csv,noheader -i "$GPU_PHYS"

run() { # label  cmd...
  local label="$1"; shift
  local csv="$T/s.csv"
  ( nvidia-smi --query-gpu=utilization.gpu --format=csv,noheader,nounits -i "$GPU_PHYS" -lms 100 >"$csv" 2>/dev/null ) & local smp=$!
  "$@" >"$T/o" 2>"$T/e" || true
  sleep 0.2; kill "$smp" 2>/dev/null || true; wait "$smp" 2>/dev/null || true
  local sm cps ip ovf cap
  sm=$(awk -F, '{u=$1+0; if(u>1){s+=u;n++}} END{if(n)printf "%.0f",s/n; else print 0}' "$csv" 2>/dev/null || echo 0)
  cps=$(grep -oE 'candidates_per_second: [0-9.]+' "$T/e" 2>/dev/null | grep -oE '[0-9.]+' | head -1 || true)
  ip=$(grep -oE ' ip: [0-9]+' "$T/e" 2>/dev/null | grep -oE '[0-9]+' | head -1 || true)
  ovf=$(grep -oE 'point_overflow: [0-9]+' "$T/e" 2>/dev/null | grep -oE '[0-9]+' | head -1 || true)
  cap=$(grep -oE 'reducing blocks from [0-9]+ to [0-9]+' "$T/e" 2>/dev/null | tail -1 || true)
  if [ -z "${cps:-}" ]; then echo "    [$label] no throughput; stderr tail:"; tail -3 "$T/e" | sed 's/^/      /'; fi
  printf "  %-26s cand/s=%-10s ip=%-5s overflow=%-7s meanSM=%s%%  %s\n" \
    "$label" "${cps:-?}" "${ip:-?}" "${ovf:-0}" "${sm:-?}" "${cap:-}"
}

echo; echo "### baselines (legacy fused kernels, ip-max-points 4096) ###"
run "legacy_serial_4096"  $BIN --structure-id $STRUCT --w5 $W5 --shard-count $CS --shard-index $CI \
      --emit-capacity $EMIT --ip-check --ip-max-points 4096
run "legacy_block_4096"   $BIN --structure-id $STRUCT --w5 $W5 --shard-count $CS --shard-index $CI \
      --emit-capacity $EMIT --ip-check --block-ip --ip-max-points 4096

echo; echo "### bucketed: np_cap sweep (threads 128, default blocks) ###"
for cap in 32 48 64 96 128; do
  run "bucketed_np${cap}" $BIN --structure-id $STRUCT --w5 $W5 --shard-count $CS --shard-index $CI \
      --emit-capacity $EMIT --ip-check --ip-bucketed --np-cap $cap --overflow-output "$T/ovf"
done

echo; echo "### bucketed: threads-per-block sweep (np_cap 64) ###"
for th in 64 128 256; do
  run "bucketed_np64_th${th}" $BIN --structure-id $STRUCT --w5 $W5 --shard-count $CS --shard-index $CI \
      --emit-capacity $EMIT --ip-check --ip-bucketed --np-cap 64 --threads $th --overflow-output "$T/ovf"
done

echo; echo "### bucketed: block-count sweep (np_cap 64, threads 128) ###"
SMN=188
for mult in 8 16 32 64; do
  run "bucketed_np64_b${mult}xSM" $BIN --structure-id $STRUCT --w5 $W5 --shard-count $CS --shard-index $CI \
      --emit-capacity $EMIT --ip-check --ip-bucketed --np-cap 64 --blocks $(( SMN * mult )) --overflow-output "$T/ovf"
done

rm -rf "$T"; echo; echo DONE
