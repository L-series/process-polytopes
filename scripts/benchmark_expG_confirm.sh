#!/usr/bin/env bash
#SBATCH --job-name=expG-confirm
#SBATCH --partition=gpu
#SBATCH --gres=gpu:RTX6000BW:1
#SBATCH --cpus-per-task=8
#SBATCH --mem=80G
#SBATCH --time=00:20:00
#SBATCH --output=logs/slurm/expG-confirm-%j.out
#SBATCH --error=logs/slurm/expG-confirm-%j.err
set -uo pipefail
cd "${SLURM_SUBMIT_DIR:-$(pwd)}"
BUILD=src/classify/build-cuda; BIN=$BUILD/cuda_dim5_cws_scan
W5=results/cache/w5.ip; export PALP_W5_POOL="$W5"; STRUCT=3
echo "### host=$(hostname) $(date) ###"
cmake --build "$BUILD" --target cuda_dim5_cws_scan -j8 >/dev/null 2>&1 && echo "build OK" || { echo "build FAIL"; exit 1; }
TMP=$(mktemp -d)
line() { grep -oE 'point_cycles: [0-9]+ \([0-9.]+% top\) ip_cycles: [0-9]+ \([0-9.]+% top\) point_candidates: [0-9]+ total_points: [0-9]+ avg_points: [0-9.]+ max_points: [0-9]+' "$1" | head -1; }

echo; echo "### CORRECTNESS of enumeration on deterministic clean shard (sc=4e8 si=2e8) ###"
echo "    (total_points / avg_points / max_points MUST match tri vs fp)"
for mode in "tri:" "fp:--fp-walk"; do
  lbl=${mode%%:*}; fl=${mode#*:}
  $BIN --structure-id $STRUCT --w5 $W5 --shard-count 400000000 --shard-index 200000000 \
    --emit-capacity 200000 --ip-check --ip-bucketed --vol-sort --ip-stage-profile \
    --np-cap 64 $fl --overflow-output "$TMP/ovf" 2>"$TMP/$lbl" >/dev/null || true
  printf "  %-4s %s\n" "$lbl" "$(line "$TMP/$lbl")"
done

echo; echo "### NEW BOTTLENECK: cycle split on 300k working shard (sc=4000 si=2000) ###"
for mode in "tri:" "fp:--fp-walk"; do
  lbl=${mode%%:*}; fl=${mode#*:}
  $BIN --structure-id $STRUCT --w5 $W5 --shard-count 4000 --shard-index 2000 \
    --emit-capacity 300000 --ip-check --ip-bucketed --vol-sort --ip-stage-profile \
    --np-cap 64 $fl --overflow-output "$TMP/ovf" 2>"$TMP/b$lbl" >/dev/null || true
  printf "  %-4s %s\n" "$lbl" "$(line "$TMP/b$lbl")"
done

echo; echo "### Can we now run a HIGHER np_cap cheaply? (fp, vol-sort, throughput) ###"
GPU_PHYS="${CUDA_VISIBLE_DEVICES%%,*}"; GPU_PHYS="${GPU_PHYS:-0}"
for cap in 64 128 256; do
  cps=$($BIN --structure-id $STRUCT --w5 $W5 --shard-count 4000 --shard-index 2000 \
    --emit-capacity 300000 --ip-check --ip-bucketed --vol-sort --fp-walk \
    --np-cap $cap --overflow-output "$TMP/ovf" 2>&1 | grep -oE 'candidates_per_second: [0-9.]+' | grep -oE '[0-9.]+' | head -1)
  ov=$(wc -l <"$TMP/ovf" 2>/dev/null || echo 0)
  printf "  fp np_cap=%-4s cand/s=%-10s overflow_rows=%s\n" "$cap" "${cps:-?}" "$ov"
done
rm -rf "$TMP"; echo; echo "### DONE $(date) ###"
