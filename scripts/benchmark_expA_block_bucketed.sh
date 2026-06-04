#!/usr/bin/env bash
#SBATCH --job-name=expA-block
#SBATCH --partition=gpu
#SBATCH --gres=gpu:RTX6000BW:1
#SBATCH --cpus-per-task=8
#SBATCH --mem=80G
#SBATCH --time=00:40:00
#SBATCH --output=logs/slurm/expA-block-%j.out
#SBATCH --error=logs/slurm/expA-block-%j.err
#
# Exp A: block-cooperative point-enum in the bucketed pipeline (--ip-bucketed
# --block-ip) vs the current 1-thread/candidate serial bucketed kernel.
# Builds (capturing ptxas -v register/local-mem for both enum kernels), checks
# bit-exact accepted-set equivalence, then sweeps np_cap throughput A/B.
set -uo pipefail
cd "${SLURM_SUBMIT_DIR:-$(pwd)}"
BUILD=src/classify/build-cuda
BIN=$BUILD/cuda_dim5_cws_scan
W5=results/cache/w5.ip
STRUCT=3
GPU_PHYS="${CUDA_VISIBLE_DEVICES%%,*}"; GPU_PHYS="${GPU_PHYS:-0}"
echo "### host=$(hostname) $(date) ###"
nvidia-smi --query-gpu=name,memory.total --format=csv,noheader -i "$GPU_PHYS"

echo; echo "### BUILD (ptxas -v) ###"
cmake --build "$BUILD" --target cuda_dim5_cws_scan -j8 2>"$BUILD/expA_build.log" \
  && echo "build OK" || { echo "build FAIL"; tail -30 "$BUILD/expA_build.log"; exit 1; }
echo "--- register / local-mem footprint (point-enum kernels) ---"
grep -A2 -E "point_enum_kernel|point_enum_block_kernel" "$BUILD/expA_build.log" \
  | grep -E "Function properties|registers|stack frame|spill" | sed 's/^/  /' | head -20

# ---- shared benchmark shard (mid-range, no per-candidate OOM at np_cap<=128) ----
CS=4000; CI=2000; EMIT=300000
TMP=$(mktemp -d)
run() { # label cmd...
  local label="$1"; shift
  local csv="$TMP/s.csv"
  ( nvidia-smi --query-gpu=utilization.gpu --format=csv,noheader,nounits -i "$GPU_PHYS" -lms 100 >"$csv" 2>/dev/null ) & local smp=$!
  "$@" >"$TMP/o" 2>"$TMP/e" || true
  sleep 0.2; kill "$smp" 2>/dev/null || true; wait "$smp" 2>/dev/null || true
  local sm cps ovf
  sm=$(awk -F, '{u=$1+0; if(u>1){s+=u;n++}} END{if(n)printf "%.0f",s/n; else print 0}' "$csv")
  cps=$(grep -oE 'candidates_per_second: [0-9.]+' "$TMP/e" | grep -oE '[0-9.]+' | head -1)
  ovf=$(grep -oE 'point_overflow: [0-9]+' "$TMP/e" | grep -oE '[0-9]+' | head -1)
  printf "  %-28s cand/s=%-10s overflow=%-8s meanSM=%s%%\n" "$label" "${cps:-?}" "${ovf:-0}" "${sm:-?}"
}

echo; echo "### THROUGHPUT: serial-bucketed vs block-bucketed (np_cap sweep) ###"
echo "shard=$CI/$CS emit=$EMIT"
for cap in 16 32 64; do
  run "serial_np${cap}" $BIN --structure-id $STRUCT --w5 $W5 --shard-count $CS --shard-index $CI \
      --emit-capacity $EMIT --ip-check --ip-bucketed --np-cap $cap --overflow-output "$TMP/ovf"
  for th in 32 64 128; do
    run "block_np${cap}_th${th}" $BIN --structure-id $STRUCT --w5 $W5 --shard-count $CS --shard-index $CI \
        --emit-capacity $EMIT --ip-check --ip-bucketed --block-ip --np-cap $cap --threads $th \
        --blocks $((188*8)) --overflow-output "$TMP/ovf"
  done
done

echo; echo "### CORRECTNESS: block vs serial accepted∪overflow on a non-truncating shard ###"
EMITC=200000
SC=0; SI=0
for sc in 4000000 40000000 400000000; do
  si=$((sc/2))
  g=$($BIN --structure-id $STRUCT --w5 $W5 --shard-count $sc --shard-index $si --emit-capacity $EMITC 2>&1 \
        | grep -oE 'generated_candidates_seen: [0-9]+' | grep -oE '[0-9]+')
  echo "  probe sc=$sc -> generated=$g"
  if [ -n "$g" ] && [ "$g" -gt 1000 ] && [ "$g" -lt "$EMITC" ]; then SC=$sc; SI=$si; break; fi
done
if [ "$SC" = 0 ]; then echo "  no non-truncating shard found; skipping correctness"; else
  for cap in 64; do
    $BIN --structure-id $STRUCT --w5 $W5 --shard-count $SC --shard-index $SI --emit-capacity $EMITC \
      --ip-check --ip-bucketed --np-cap $cap --accepted-output "$TMP/serA" --overflow-output "$TMP/serO" >/dev/null 2>&1
    $BIN --structure-id $STRUCT --w5 $W5 --shard-count $SC --shard-index $SI --emit-capacity $EMITC \
      --ip-check --ip-bucketed --block-ip --threads 64 --blocks $((188*8)) --np-cap $cap \
      --accepted-output "$TMP/blkA" --overflow-output "$TMP/blkO" >/dev/null 2>&1
    sort "$TMP/serA" >"$TMP/serA.s"; sort "$TMP/blkA" >"$TMP/blkA.s"
    cat "$TMP/serA" "$TMP/serO" | sort -u >"$TMP/serU"; cat "$TMP/blkA" "$TMP/blkO" | sort -u >"$TMP/blkU"
    echo "  np_cap=$cap: serial accepted=$(wc -l <"$TMP/serA.s") block accepted=$(wc -l <"$TMP/blkA.s")"
    diff -q "$TMP/serA.s" "$TMP/blkA.s" >/dev/null && echo "    accepted sets IDENTICAL ✓" || { echo "    accepted DIFFER ✗"; diff "$TMP/serA.s" "$TMP/blkA.s" | head; }
    diff -q "$TMP/serU" "$TMP/blkU" >/dev/null && echo "    accepted∪overflow IDENTICAL ✓" || echo "    accepted∪overflow DIFFER ✗"
  done
fi
rm -rf "$TMP"; echo; echo "### DONE $(date) ###"
