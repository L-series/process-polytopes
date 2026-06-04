#!/usr/bin/env bash
#SBATCH --job-name=expG-lllfp
#SBATCH --partition=gpu
#SBATCH --gres=gpu:RTX6000BW:1
#SBATCH --cpus-per-task=8
#SBATCH --mem=80G
#SBATCH --time=00:45:00
#SBATCH --output=logs/slurm/expG-lllfp-%j.out
#SBATCH --error=logs/slurm/expG-lllfp-%j.err
#
# Exp G: LLL + Fincke-Pohst point walk on the GPU (device_make_points_fp), built
# on top of the int32 (Exp B) + vol-sort (Exp E) bucketed kernel. Three things:
#   1. CORRECTNESS  -- accepted∪overflow set must equal the triangular reference
#      (the completeness gate) for --fp-walk (all) and --fp-walk --fp-gate (heavy
#      only). Also report whether the accepted set itself is identical.
#   2. NODE DENSITY -- --ip-stage-profile confirms the FP walk cuts tree nodes.
#   3. THROUGHPUT   -- triangular vs fp-all vs fp-gated at np_cap 16/32/64.
set -uo pipefail
cd "${SLURM_SUBMIT_DIR:-$(pwd)}"
BUILD=src/classify/build-cuda
BIN=$BUILD/cuda_dim5_cws_scan
W5=results/cache/w5.ip
export PALP_W5_POOL="$W5"
STRUCT=3
GPU_PHYS="${CUDA_VISIBLE_DEVICES%%,*}"; GPU_PHYS="${GPU_PHYS:-0}"
echo "### host=$(hostname) gpu=$GPU_PHYS $(date) ###"

cmake --build "$BUILD" --target cuda_dim5_cws_scan -j8 2>"$BUILD/expG_build.log" \
  && echo "build OK" || { echo "build FAIL"; tail -40 "$BUILD/expG_build.log"; exit 1; }
echo "### point_enum_kernel ptxas (registers / spill) ###"
grep -A2 -E "point_enum_kernel" "$BUILD/expG_build.log" | grep -E "registers|spill|stack" | head -8 || true

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

# ---- pick a non-truncating shard (full IP set fits in emit-capacity) ----
EMITC=200000; SC=0; SI=0
for sc in 4000000 40000000 400000000; do
  si=$((sc/2)); g=$($BIN --structure-id $STRUCT --w5 $W5 --shard-count $sc --shard-index $si --emit-capacity $EMITC 2>&1 | grep -oE 'generated_candidates_seen: [0-9]+' | grep -oE '[0-9]+')
  if [ -n "$g" ] && [ "$g" -gt 1000 ] && [ "$g" -lt "$EMITC" ]; then SC=$sc; SI=$si; break; fi
done
echo; echo "### CORRECTNESS (shard sc=$SC si=$SI, np_cap 64) ###"
chk() { # $1 label  $2.. extra flags for the candidate run
  local lbl="$1"; shift
  $BIN --structure-id $STRUCT --w5 $W5 --shard-count $SC --shard-index $SI --emit-capacity $EMITC \
    --ip-check --ip-bucketed --vol-sort --np-cap 64 "$@" \
    --accepted-output "$TMP/A" --overflow-output "$TMP/O" >/dev/null 2>&1
  cat "$TMP/A" "$TMP/O" | sort -u >"$TMP/U_$lbl"
  sort "$TMP/A" >"$TMP/A_$lbl"
  echo "  $lbl: accepted=$(wc -l <"$TMP/A") overflow=$(wc -l <"$TMP/O") union=$(wc -l <"$TMP/U_$lbl")"
}
chk ref                       # triangular int32 reference
chk fpall  --fp-walk          # FP for all candidates
chk fpg20  --fp-walk --fp-gate 20   # FP only for heavy (box-vol score>=20)
for v in fpall fpg20; do
  diff -q "$TMP/U_ref" "$TMP/U_$v" >/dev/null && echo "  $v union==ref  ✓ (COMPLETE)" || echo "  $v union DIFFERS ✗"
  diff -q "$TMP/A_ref" "$TMP/A_$v" >/dev/null && echo "  $v accepted==ref ✓" || echo "  $v accepted DIFFERS (bucket move; check union)"
done

echo; echo "### NODE DENSITY (--ip-stage-profile, np_cap 64) ###"
for mode in "triangular:" "fp-all:--fp-walk" "fp-g20:--fp-walk --fp-gate 20"; do
  lbl=${mode%%:*}; flags=${mode#*:}
  $BIN --structure-id $STRUCT --w5 $W5 --shard-count 4000 --shard-index 2000 \
    --emit-capacity 300000 --ip-check --ip-bucketed --vol-sort --ip-stage-profile \
    --np-cap 64 $flags --overflow-output "$TMP/ovf" 2>"$TMP/pe" >/dev/null || true
  line=$(grep -oE 'avg_points: [0-9.]+ max_points: [0-9]+.*nodes_per_cand: [0-9.]+' "$TMP/pe" | head -1)
  ov=$(grep -oE 'point_overflow: [0-9]+' "$TMP/pe" | grep -oE '[0-9]+' | head -1)
  printf "  %-10s %s overflow=%s\n" "$lbl" "${line:-(no node counter on this build)}" "${ov:-0}"
done

echo; echo "### THROUGHPUT (vol-sort; triangular vs fp-all vs fp-gated) ###"
CS=4000; CI=2000; EMIT=300000
for cap in 16 32 64; do
  run "tri_np${cap}"     $BIN --structure-id $STRUCT --w5 $W5 --shard-count $CS --shard-index $CI \
      --emit-capacity $EMIT --ip-check --ip-bucketed --vol-sort --np-cap $cap --overflow-output "$TMP/ovf"
  run "fpall_np${cap}"   $BIN --structure-id $STRUCT --w5 $W5 --shard-count $CS --shard-index $CI \
      --emit-capacity $EMIT --ip-check --ip-bucketed --vol-sort --fp-walk --np-cap $cap --overflow-output "$TMP/ovf"
  run "fpg20_np${cap}"   $BIN --structure-id $STRUCT --w5 $W5 --shard-count $CS --shard-index $CI \
      --emit-capacity $EMIT --ip-check --ip-bucketed --vol-sort --fp-walk --fp-gate 20 --np-cap $cap --overflow-output "$TMP/ovf"
done
rm -rf "$TMP"; echo; echo "### DONE $(date) ###"
