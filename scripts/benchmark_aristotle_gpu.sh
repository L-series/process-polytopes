#!/usr/bin/env bash
#SBATCH --job-name=aristotle-gpu
#SBATCH --partition=gpu
#SBATCH --gres=gpu:RTX6000BW:1
#SBATCH --cpus-per-task=8
#SBATCH --mem=80G
#SBATCH --time=00:25:00
#SBATCH --output=logs/slurm/aristotle-gpu-%j.out
#SBATCH --error=logs/slurm/aristotle-gpu-%j.err
#
# A/B the Aristotle-1 nested-loop point walk on the GPU (-DNESTED_WALK) vs the
# legacy dynamic-level walk, in the --ip-bucketed pipeline.
#   LEG = results/aristotle-validation/bin/cws_legacy   (current device_make_points_serial)
#   NST = results/aristotle-validation/bin/cws_nested   (Aristotle-1 5-nested-loop walk)
# Phase A correctness: accepted CWS sets must be BIT-IDENTICAL (LEG vs NST) on a
#   non-truncating shard at --np-cap 4096 -> proves the port preserves the point
#   set / IP verdict. Phase B throughput: cand/s at np_cap 32/64 on a mid shard.
set -uo pipefail
cd "${SLURM_SUBMIT_DIR:-$(pwd)}"
LEG=results/aristotle-validation/bin/cws_legacy
NST=results/aristotle-validation/bin/cws_nested
W5=results/cache/w5.ip
STRUCT=3
GPU="${CUDA_VISIBLE_DEVICES%%,*}"; GPU="${GPU:-0}"
T=$(mktemp -d)
echo "host=$(hostname) gpu=$GPU"; nvidia-smi --query-gpu=name,memory.total --format=csv,noheader -i "$GPU"
for b in "$LEG" "$NST"; do [ -x "$b" ] || { echo "missing $b"; exit 1; }; done

# ============================================ Phase A: correctness ============
EMIT=200000
gen_count() { "$1" --structure-id $STRUCT --w5 $W5 --shard-count "$2" --shard-index "$3" \
  --emit-capacity "$EMIT" 2>&1 | grep -oE 'generated_candidates_seen: [0-9]+' | grep -oE '[0-9]+' | head -1; }
SC=0
for sc in 4000000 40000000 400000000 4000000000; do
  si=$(( sc / 2 )); g=$(gen_count "$LEG" "$sc" "$si")
  echo "  probe shard-count=$sc index=$si -> generated=${g:-0}"
  if [ "${g:-0}" -gt 0 ] && [ "${g:-0}" -lt "$EMIT" ]; then SC=$sc; SI=$si; GEN=$g; break; fi
done
[ "$SC" = 0 ] && { echo "no non-truncating shard"; rm -rf "$T"; exit 1; }
echo "### Phase A: correctness on shard-count=$SC index=$SI generated=$GEN (np_cap 4096) ###"
ip_line() { grep -oE 'candidates: [0-9]+ processed: [0-9]+ ip: [0-9]+ .*ip_reject: [0-9]+'; }
run_acc() { # bin outfile
  "$1" --structure-id $STRUCT --w5 $W5 --shard-count $SC --shard-index $SI \
    --emit-capacity $EMIT --ip-check --ip-bucketed --np-cap 4096 \
    --accepted-output "$2" --overflow-output "$T/ovf" 2>&1 | ip_line; }
echo "  LEG: $(run_acc "$LEG" "$T/leg.acc")"; sort "$T/leg.acc" > "$T/leg.s"
echo "  NST: $(run_acc "$NST" "$T/nst.acc")"; sort "$T/nst.acc" > "$T/nst.s"
echo "  accepted: LEG=$(wc -l < "$T/leg.s")  NST=$(wc -l < "$T/nst.s")"
if diff -q "$T/leg.s" "$T/nst.s" >/dev/null; then
  echo "  ACCEPTED SETS IDENTICAL ✓  (nested walk is bit-exact on GPU)"
else
  echo "  ACCEPTED SETS DIFFER ✗:"; diff "$T/leg.s" "$T/nst.s" | head -12 | sed 's/^/    /'
fi

# ============================================ Phase B: throughput =============
CS=4000; CI=2000; BEMIT=300000
echo; echo "### Phase B: throughput (shard $CI/$CS, emit $BEMIT) ###"
run_tp() { # label bin np_cap
  local csv="$T/s.csv"
  ( nvidia-smi --query-gpu=utilization.gpu --format=csv,noheader,nounits -i "$GPU" -lms 100 >"$csv" 2>/dev/null ) & local smp=$!
  "$2" --structure-id $STRUCT --w5 $W5 --shard-count $CS --shard-index $CI \
    --emit-capacity $BEMIT --ip-check --ip-bucketed --np-cap "$3" --overflow-output "$T/ovf" \
    >"$T/o" 2>"$T/e" || true
  kill "$smp" 2>/dev/null || true; wait "$smp" 2>/dev/null || true
  local sm cps ovf
  sm=$(awk -F, '{u=$1+0; if(u>1){s+=u;n++}} END{if(n)printf "%.0f",s/n; else print 0}' "$csv" 2>/dev/null || echo 0)
  cps=$(grep -oE 'candidates_per_second: [0-9.]+' "$T/e" 2>/dev/null | grep -oE '[0-9.]+' | head -1 || true)
  ovf=$(grep -oE 'point_overflow: [0-9]+' "$T/e" 2>/dev/null | grep -oE '[0-9]+' | head -1 || true)
  [ -z "${cps:-}" ] && { echo "    [$1] no throughput; stderr tail:"; tail -3 "$T/e" | sed 's/^/      /'; }
  printf "  %-18s cand/s=%-10s overflow=%-7s meanSM=%s%%\n" "$1" "${cps:-?}" "${ovf:-0}" "${sm:-?}"
  echo "${cps:-0}" > "$T/rate"
}
for cap in 32 64; do
  run_tp "legacy_np${cap}" "$LEG" "$cap"; l=$(cat "$T/rate")
  run_tp "nested_np${cap}" "$NST" "$cap"; n=$(cat "$T/rate")
  awk -v c="$cap" -v l="$l" -v n="$n" 'BEGIN{ if(l>0) printf "    => np_cap %s: nested/legacy = %.3fx\n", c, n/l }'
done
rm -rf "$T"; echo; echo DONE
