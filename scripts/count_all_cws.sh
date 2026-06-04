#!/usr/bin/env bash
#SBATCH --job-name=count-cws
#SBATCH --partition=gpu
#SBATCH --nodes=1
#SBATCH --gres=gpu:1
#SBATCH --cpus-per-task=8
#SBATCH --mem=32G
#SBATCH --time=00:30:00
#SBATCH --output=logs/slurm/count-cws-%j.out
#SBATCH --error=logs/slurm/count-cws-%j.err
#
# Count-only pass: total number of CWS candidates (prefixes) to process across
# all 46 dim-5 structures. No --emit-capacity => scan kernel only, reports
# selection_tuples / canonical / prefix_candidate counts per structure.
set -uo pipefail
cd "${SLURM_SUBMIT_DIR:-$(pwd)}"
source scripts/env_local.sh 2>/dev/null || true
REPO="$(pwd)"
export PALP_W5_POOL="${PALP_W5_POOL:-$REPO/results/cache/w5.ip}"
BIN="$REPO/src/classify/build-cuda/cuda_dim5_cws_scan"
DEV="$(_slurm_cuda_device 2>/dev/null || echo 0)"

echo "### node $(hostname) $(date) device=$DEV ###"
"$BIN" --all --w5 "$REPO/results/cache/w5.ip" --cuda-device "$DEV"
echo "### DONE $(date) ###"
