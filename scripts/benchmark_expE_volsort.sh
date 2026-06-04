#!/usr/bin/env bash
#SBATCH --job-name=expE-volsort
#SBATCH --partition=gpu
#SBATCH --gres=gpu:RTX6000BW:1
#SBATCH --cpus-per-task=8
#SBATCH --mem=80G
#SBATCH --time=00:35:00
#SBATCH --output=logs/slurm/expE-volsort-%j.out
#SBATCH --error=logs/slurm/expE-volsort-%j.err
#
# Exp E: reorder candidates by box-volume proxy (--vol-sort) for warp homogeneity,
# on top of the int32 (Exp B) serial bucketed kernel. A/B throughput + bit-exact
# accepted-set check (a permutation must not change the accepted∪overflow set).
set -uo pipefail
cd "${SLURM_SUBMIT_DIR:-$(pwd)}"
BUILD=src/classify/build-cuda
BIN=$BUILD/cuda_dim5_cws_scan
W5=results/cache/w5.ip
STRUCT=3
GPU_PHYS="${CUDA_VISIBLE_DEVICES%%,*}"; GPU_PHYS="${GPU_PHYS:-0}"
echo "### host=$(hostname) $(date) ###"
cmake --build "$BUILD" --target cuda_dim5_cws_scan -j8 2>"$BUILD/expE_build.log" \
  && echo "build OK" || { echo "build FAIL"; tail -30 "$BUILD/expE_build.log"; exit 1; }

CS=4000; CI=2000; EMIT=300000
TMP=$(mktemp -d)
run() { local label="$1"; shift
  local csv="$TMP/s.csv"
  ( nvidia-smi --query-gpu=utilization.gpu --format=csv,noheader,nounits -i "$GPU_PHYS" -lms 100 >"$csv" 2>/dev/null ) & local smp=$!
  "$@" >"$TMP/o" 2>"$TMP/e" || true
  sleep 0.2; kill "$smp" 2>/dev/null || true; wait "$smp" 2>/dev/null || true
  local sm cps; sm=$(awk -F, '{u=$1+0; if(u>1){s+=u;n++}} END{if(n)printf "%.0f",s/n; else print 0}' "$csv")
  cps=$(grep -oE 'candidates_per_second: [0-9.]+' "$TMP/e" | grep -oE '[0-9.]+' | head -1)
  printf "  %-22s cand/s=%-10s meanSM=%s%%\n" "$label" "${cps:-?}" "${sm:-?}"
}
echo; echo "### THROUGHPUT: int32 serial, no-sort vs --vol-sort ###"
for cap in 16 32 64; do
  run "nosort_np${cap}" $BIN --structure-id $STRUCT --w5 $W5 --shard-count $CS --shard-index $CI \
      --emit-capacity $EMIT --ip-check --ip-bucketed --np-cap $cap --overflow-output "$TMP/ovf"
  run "volsort_np${cap}" $BIN --structure-id $STRUCT --w5 $W5 --shard-count $CS --shard-index $CI \
      --emit-capacity $EMIT --ip-check --ip-bucketed --vol-sort --np-cap $cap --overflow-output "$TMP/ovf"
done

echo; echo "### CORRECTNESS: --vol-sort vs no-sort accepted∪overflow (non-truncating shard) ###"
EMITC=200000; SC=0; SI=0
for sc in 4000000 40000000 400000000; do
  si=$((sc/2)); g=$($BIN --structure-id $STRUCT --w5 $W5 --shard-count $sc --shard-index $si --emit-capacity $EMITC 2>&1 | grep -oE 'generated_candidates_seen: [0-9]+' | grep -oE '[0-9]+')
  if [ -n "$g" ] && [ "$g" -gt 1000 ] && [ "$g" -lt "$EMITC" ]; then SC=$sc; SI=$si; break; fi
done
echo "  shard sc=$SC si=$SI"
$BIN --structure-id $STRUCT --w5 $W5 --shard-count $SC --shard-index $SI --emit-capacity $EMITC \
  --ip-check --ip-bucketed --np-cap 64 --accepted-output "$TMP/nA" --overflow-output "$TMP/nO" >/dev/null 2>&1
$BIN --structure-id $STRUCT --w5 $W5 --shard-count $SC --shard-index $SI --emit-capacity $EMITC \
  --ip-check --ip-bucketed --vol-sort --np-cap 64 --accepted-output "$TMP/vA" --overflow-output "$TMP/vO" >/dev/null 2>&1
cat "$TMP/nA" "$TMP/nO" | sort -u >"$TMP/nU"; cat "$TMP/vA" "$TMP/vO" | sort -u >"$TMP/vU"
echo "  nosort accepted=$(wc -l <"$TMP/nA") volsort accepted=$(wc -l <"$TMP/vA")"
diff -q <(sort "$TMP/nA") <(sort "$TMP/vA") >/dev/null && echo "  accepted IDENTICAL ✓" || echo "  accepted DIFFER ✗"
diff -q "$TMP/nU" "$TMP/vU" >/dev/null && echo "  accepted∪overflow IDENTICAL ✓" || echo "  accepted∪overflow DIFFER ✗"
rm -rf "$TMP"; echo; echo "### DONE $(date) ###"
