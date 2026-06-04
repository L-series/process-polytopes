#!/usr/bin/env bash
#SBATCH --job-name=expD-nodes
#SBATCH --partition=gpu
#SBATCH --gres=gpu:RTX6000BW:1
#SBATCH --cpus-per-task=8
#SBATCH --mem=80G
#SBATCH --time=00:30:00
#SBATCH --output=logs/slurm/expD-nodes-%j.out
#SBATCH --error=logs/slurm/expD-nodes-%j.err
#
# Exp D prototype: measure triangular-walk TREE-NODE density (nodes_per_cand) on
# the GPU's actual bucketed workload, via --ip-stage-profile. The LLL+FP walk
# reduces nodes ~74x (proven on CPU, LLL_FP_WALK.md) but adds per-candidate LLL
# setup + float state. This quantifies how much headroom FP could recover on the
# np<=np_cap candidates the GPU actually processes (heavy candidates overflow to
# the CPU, where LLL+FP is already deployed).
set -uo pipefail
cd "${SLURM_SUBMIT_DIR:-$(pwd)}"
BUILD=src/classify/build-cuda
BIN=$BUILD/cuda_dim5_cws_scan
W5=results/cache/w5.ip
STRUCT=3; CS=4000; CI=2000; EMIT=300000
TMP=$(mktemp -d)
echo "### host=$(hostname) $(date) ###"
cmake --build "$BUILD" --target cuda_dim5_cws_scan -j8 2>"$BUILD/expD_build.log" \
  && echo "build OK" || { echo "build FAIL"; tail -30 "$BUILD/expD_build.log"; exit 1; }

echo; echo "### triangular walk node density by np_cap (--ip-stage-profile) ###"
for cap in 16 64 512 2048; do
  $BIN --structure-id $STRUCT --w5 $W5 --shard-count $CS --shard-index $CI \
    --emit-capacity $EMIT --ip-check --ip-bucketed --vol-sort --ip-stage-profile \
    --np-cap $cap --overflow-output "$TMP/ovf" 2>"$TMP/e" >/dev/null || true
  line=$(grep -oE 'avg_points: [0-9.]+ max_points: [0-9]+ walk_nodes: [0-9]+ nodes_per_cand: [0-9.]+' "$TMP/e" | head -1)
  ovf=$(grep -oE 'point_overflow: [0-9]+' "$TMP/e" | grep -oE '[0-9]+' | head -1)
  printf "  np_cap=%-5s %s  overflow=%s\n" "$cap" "$line" "${ovf:-0}"
done
echo
echo "Reference (LLL_FP_WALK.md, CPU, full type-3): triangular median ~2031 div/pt,"
echo "FP ~144 (14x/pt); 74x fewer tree NODES, 57x fewer divisions, bit-identical."
rm -rf "$TMP"; echo; echo "### DONE $(date) ###"
