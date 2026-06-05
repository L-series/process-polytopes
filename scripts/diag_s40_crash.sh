#!/usr/bin/env bash
#SBATCH --job-name=diag-s40
#SBATCH --partition=gpu
#SBATCH --gres=gpu:RTX6000BW:1
#SBATCH --cpus-per-task=4
#SBATCH --mem=40G
#SBATCH --time=00:15:00
#SBATCH --output=logs/slurm/diag-s40-%j.out
#SBATCH --error=logs/slurm/diag-s40-%j.err
set -uo pipefail
cd "${SLURM_SUBMIT_DIR:-$(pwd)}"
BIN=src/classify/build-cuda/cuda_dim5_cws_scan
W5=results/cache/w5.ip; export PALP_W5_POOL="$W5"
SAN=/usr/local/cuda/bin/compute-sanitizer
echo "### host=$(hostname) $(date) ###"
# tiny slice of s40 that still emits candidates -> faults fast; sanitizer prints
# the exact kernel + source line of the illegal access.
$SAN --tool memcheck --launch-timeout 60 \
  "$BIN" --structure-id 40 --w5 "$W5" --stream-ip --ip-bucketed --fp-walk --vol-sort \
  --np-cap 256 --emit-capacity 200000 --shard-count 2000 --shard-index 0 \
  --accepted-output /dev/null --overflow-output /dev/null 2>&1 | \
  grep -iE "Invalid|out-of-bounds|illegal|=== ERROR|device_|_kernel|\.cu:[0-9]+|Address|by thread|Saved host" | head -40
echo "### DONE $(date) ###"
