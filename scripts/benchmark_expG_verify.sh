#!/usr/bin/env bash
#SBATCH --job-name=expG-verify
#SBATCH --partition=gpu
#SBATCH --gres=gpu:RTX6000BW:1
#SBATCH --cpus-per-task=8
#SBATCH --mem=80G
#SBATCH --time=00:30:00
#SBATCH --output=logs/slurm/expG-verify-%j.out
#SBATCH --error=logs/slurm/expG-verify-%j.err
#
# Exp G verification: the throughput shard showed ~13x (fp-all vs triangular).
# That is huge -- this job proves it is REAL (FP enumerates the SAME points, just
# far cheaper) and not a measurement artifact, and finds the new bottleneck:
#   1. Pure-generation ceiling (no --ip-check) -> the rate FP could be bounded by.
#   2. --ip-stage-profile tri vs fp-all: total_points/avg_points/max_points MUST
#      match (same enumeration); point_cycles ratio = the true per-walk speedup;
#      the point% vs ip% split shows whether enum is still the bottleneck.
set -uo pipefail
cd "${SLURM_SUBMIT_DIR:-$(pwd)}"
BUILD=src/classify/build-cuda
BIN=$BUILD/cuda_dim5_cws_scan
W5=results/cache/w5.ip
export PALP_W5_POOL="$W5"
STRUCT=3; CS=4000; CI=2000; EMIT=300000
echo "### host=$(hostname) $(date) ###"
cmake --build "$BUILD" --target cuda_dim5_cws_scan -j8 >/dev/null 2>&1 && echo "build OK" || { echo "build FAIL"; exit 1; }
TMP=$(mktemp -d)

echo; echo "### shard size (truncating?) ###"
$BIN --structure-id $STRUCT --w5 $W5 --shard-count $CS --shard-index $CI --emit-capacity $EMIT 2>&1 \
  | grep -oE 'generated_candidates_seen: [0-9]+|stored_candidates: [0-9]+' | head -4

echo; echo "### pure-GENERATION ceiling (no --ip-check), vol-sort off ###"
$BIN --structure-id $STRUCT --w5 $W5 --shard-count $CS --shard-index $CI --emit-capacity $EMIT 2>&1 \
  | grep -oE 'candidates_per_second: [0-9.]+' | head -1 | sed 's/^/  gen-only /'

echo; echo "### ENUM WORK + cycle split (--ip-stage-profile, vol-sort, np_cap 64) ###"
prof() { local lbl="$1"; shift
  $BIN --structure-id $STRUCT --w5 $W5 --shard-count $CS --shard-index $CI \
    --emit-capacity $EMIT --ip-check --ip-bucketed --vol-sort --ip-stage-profile \
    --np-cap 64 "$@" --overflow-output "$TMP/ovf" 2>"$TMP/e" >/dev/null || true
  echo "  --- $lbl ---"
  grep -oE 'point_cycles: [0-9]+ \([0-9.]+% top\) ip_cycles: [0-9]+ \([0-9.]+% top\) point_candidates: [0-9]+ total_points: [0-9]+ avg_points: [0-9.]+ max_points: [0-9]+' "$TMP/e" | head -1 | sed 's/^/    /'
  grep -oE 'candidates_per_second: [0-9.]+|point_overflow: [0-9]+|accepted: [0-9]+' "$TMP/e" | head -3 | sed 's/^/    /'
}
prof "triangular"
prof "fp-all"   --fp-walk
prof "fp-gate20" --fp-walk --fp-gate 20

echo; echo "### per-np_cap point_cycles ratio (enum-only cost) ###"
for cap in 16 32 64; do
  for mode in "tri:" "fp:--fp-walk"; do
    lbl=${mode%%:*}; fl=${mode#*:}
    pc=$($BIN --structure-id $STRUCT --w5 $W5 --shard-count $CS --shard-index $CI \
      --emit-capacity $EMIT --ip-check --ip-bucketed --vol-sort --ip-stage-profile \
      --np-cap $cap $fl --overflow-output "$TMP/ovf" 2>&1 | grep -oE 'point_cycles: [0-9]+' | grep -oE '[0-9]+' | head -1)
    printf "  np_cap=%-3s %-4s point_cycles=%s\n" "$cap" "$lbl" "${pc:-?}"
  done
done
rm -rf "$TMP"; echo; echo "### DONE $(date) ###"
