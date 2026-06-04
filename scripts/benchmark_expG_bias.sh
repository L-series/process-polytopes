#!/usr/bin/env bash
#SBATCH --job-name=expG-bias
#SBATCH --partition=gpu
#SBATCH --gres=gpu:RTX6000BW:1
#SBATCH --cpus-per-task=8
#SBATCH --mem=80G
#SBATCH --time=00:40:00
#SBATCH --output=logs/slurm/expG-bias-%j.out
#SBATCH --error=logs/slurm/expG-bias-%j.err
#
# Exp G BIAS AUDIT. The CPU LLL+FP prototype's 9x collapsed to 1.4-2.1x in
# production because the sample was heavy-biased; FP LOST on light candidates
# (its ~1e4-cycle LLL/ellipsoid overhead dominates a small box). This job tests
# whether the GPU's ~13x has the same bias, two ways:
#   (1) GATE SWEEP at np_cap 16 (light-sensitive): raising --fp-gate moves lighter
#       candidates OFF the FP path onto the proven triangular int32 walk. Watch
#       point_cycles: if FP hurts light candidates, EXCLUDING them (higher gate)
#       LOWERS total cycles; if FP helps them, excluding them RAISES cycles.
#   (2) HEAVINESS SWEEP across shard regions: report avg_points (the real type-3
#       mean np is ~8.7) and fp-vs-tri speedup per region. A win that holds at
#       light avg_points (~5-9) is not a single-anchor artifact.
set -uo pipefail
cd "${SLURM_SUBMIT_DIR:-$(pwd)}"
BUILD=src/classify/build-cuda; BIN=$BUILD/cuda_dim5_cws_scan
W5=results/cache/w5.ip; export PALP_W5_POOL="$W5"; STRUCT=3
echo "### host=$(hostname) $(date) ###"
cmake --build "$BUILD" --target cuda_dim5_cws_scan -j8 >/dev/null 2>&1 && echo "build OK" || { echo "build FAIL"; exit 1; }
TMP=$(mktemp -d); EMIT=300000

avg_of() { grep -oE 'avg_points: [0-9.]+' "$1" | grep -oE '[0-9.]+' | head -1; }
pc_of()  { grep -oE 'point_cycles: [0-9]+'  "$1" | grep -oE '[0-9]+'   | head -1; }
pcpct_of(){ grep -oE 'point_cycles: [0-9]+ \([0-9.]+% top\)' "$1" | grep -oE '[0-9.]+% top' | head -1; }
cps_of() { grep -oE 'candidates_per_second: [0-9.]+' "$1" | grep -oE '[0-9.]+' | head -1; }

echo; echo "### (1) GATE SWEEP @ np_cap=16, shard sc=4000 si=2000 (does FP hurt light?) ###"
echo "    gate=999 ~ all-triangular baseline; gate=0 ~ all-FP. point_cycles is total enum cost."
for gate in 999 30 26 22 18 14 10 0; do
  $BIN --structure-id $STRUCT --w5 $W5 --shard-count 4000 --shard-index 2000 \
    --emit-capacity $EMIT --ip-check --ip-bucketed --vol-sort --ip-stage-profile \
    --fp-walk --fp-gate $gate --np-cap 16 --overflow-output "$TMP/ovf" 2>"$TMP/g" >/dev/null || true
  printf "  gate=%-4s point_cycles=%-16s (%s)  cand/s=%-10s avg_points=%s\n" \
    "$gate" "$(pc_of "$TMP/g")" "$(pcpct_of "$TMP/g")" "$(cps_of "$TMP/g")" "$(avg_of "$TMP/g")"
done
echo "  [pure triangular reference @ np_cap16]:"
$BIN --structure-id $STRUCT --w5 $W5 --shard-count 4000 --shard-index 2000 \
  --emit-capacity $EMIT --ip-check --ip-bucketed --vol-sort --ip-stage-profile \
  --np-cap 16 --overflow-output "$TMP/ovf" 2>"$TMP/t" >/dev/null || true
printf "  tri       point_cycles=%-16s (%s)  cand/s=%-10s avg_points=%s\n" \
  "$(pc_of "$TMP/t")" "$(pcpct_of "$TMP/t")" "$(cps_of "$TMP/t")" "$(avg_of "$TMP/t")"

echo; echo "### (2) HEAVINESS SWEEP: avg_points + tri/fp speedup across shard regions (np_cap 64) ###"
printf "  %-26s %-10s %-12s %-12s %s\n" "shard(sc/si)" "avg_pts" "tri_cand/s" "fp_cand/s" "speedup"
for spec in "4000/0" "4000/1000" "4000/2000" "4000/3000" "4000/3999" "400000/200000" "40000000/20000000"; do
  sc=${spec%%/*}; si=${spec##*/}
  $BIN --structure-id $STRUCT --w5 $W5 --shard-count $sc --shard-index $si \
    --emit-capacity $EMIT --ip-check --ip-bucketed --vol-sort --ip-stage-profile \
    --np-cap 64 --overflow-output "$TMP/ovf" 2>"$TMP/rt" >/dev/null || true
  $BIN --structure-id $STRUCT --w5 $W5 --shard-count $sc --shard-index $si \
    --emit-capacity $EMIT --ip-check --ip-bucketed --vol-sort --fp-walk \
    --np-cap 64 --overflow-output "$TMP/ovf" 2>"$TMP/rf" >/dev/null || true
  av=$(avg_of "$TMP/rt"); tc=$(cps_of "$TMP/rt"); fc=$(cps_of "$TMP/rf")
  sp=$(awk -v t="${tc:-0}" -v f="${fc:-0}" 'BEGIN{ if(t>0) printf "%.1fx", f/t; else print "?"}')
  printf "  %-26s %-10s %-12s %-12s %s\n" "$spec" "${av:-?}" "${tc:-?}" "${fc:-?}" "$sp"
done
rm -rf "$TMP"; echo; echo "### DONE $(date) ###"
