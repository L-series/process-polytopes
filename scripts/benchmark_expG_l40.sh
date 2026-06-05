#!/usr/bin/env bash
#SBATCH --job-name=expG-l40
#SBATCH --partition=gpu
#SBATCH --gres=gpu:L40:1
#SBATCH --cpus-per-task=8
#SBATCH --mem=80G
#SBATCH --time=00:20:00
#SBATCH --output=logs/slurm/expG-l40-%j.out
#SBATCH --error=logs/slurm/expG-l40-%j.err
#
# Small L40 (Ada, sm_89) throughput check: triangular (int32) vs --fp-walk at
# np_cap 16/32/64, vol-sort on, working type-3 shard. Just the per-GPU rate so we
# can firm up the fleet estimate (RTX6000BW was ~5.5M/s with FP).
set -uo pipefail
cd "${SLURM_SUBMIT_DIR:-$(pwd)}"
BUILD=src/classify/build-cuda; BIN=$BUILD/cuda_dim5_cws_scan
W5=results/cache/w5.ip; export PALP_W5_POOL="$W5"
STRUCT=3; CS=4000; CI=2000; EMIT=300000
GPU_PHYS="${CUDA_VISIBLE_DEVICES%%,*}"; GPU_PHYS="${GPU_PHYS:-0}"
echo "### host=$(hostname) gpu=$GPU_PHYS $(date) ###"
nvidia-smi --query-gpu=name,compute_cap --format=csv,noheader -i "$GPU_PHYS" 2>/dev/null | sed 's/^/  device: /'
cmake --build "$BUILD" --target cuda_dim5_cws_scan -j8 >/dev/null 2>&1 && echo "build OK" || { echo "build FAIL"; exit 1; }
TMP=$(mktemp -d)
run() { local label="$1"; shift
  "$@" >"$TMP/o" 2>"$TMP/e" || true
  local cps; cps=$(grep -oE 'candidates_per_second: [0-9.]+' "$TMP/e" | grep -oE '[0-9.]+' | head -1)
  printf "  %-18s cand/s=%s\n" "$label" "${cps:-?}"
}
echo; echo "### THROUGHPUT (vol-sort): triangular vs fp-all ###"
for cap in 16 32 64; do
  run "tri_np${cap}"   $BIN --structure-id $STRUCT --w5 $W5 --shard-count $CS --shard-index $CI \
      --emit-capacity $EMIT --ip-check --ip-bucketed --vol-sort --np-cap $cap --overflow-output "$TMP/ovf"
  run "fpall_np${cap}" $BIN --structure-id $STRUCT --w5 $W5 --shard-count $CS --shard-index $CI \
      --emit-capacity $EMIT --ip-check --ip-bucketed --vol-sort --fp-walk --np-cap $cap --overflow-output "$TMP/ovf"
done
rm -rf "$TMP"; echo; echo "### DONE $(date) ###"
