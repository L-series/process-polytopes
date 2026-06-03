#!/usr/bin/env bash
#SBATCH --job-name=ptenum-gpu-prof
#SBATCH --partition=gpu
#SBATCH --gres=gpu:RTX6000BW:1
#SBATCH --cpus-per-task=8
#SBATCH --mem=80G
#SBATCH --time=00:15:00
#SBATCH --output=logs/slurm/ptenum-gpu-prof-%j.out
#SBATCH --error=logs/slurm/ptenum-gpu-prof-%j.err
#
# GPU point_enum_kernel sub-stage profiling: device_make_cws_basis (basis) vs the
# lattice walk, via clock64 + global atomic counters (instrumented throwaway
# build cws_gpu_prof). Confirms walk-vs-basis split on the GPU and points/cand.
set -uo pipefail
cd "${SLURM_SUBMIT_DIR:-$(pwd)}"
BIN=results/aristotle-validation/bin/cws_gpu_prof
W5=results/cache/w5.ip
[ -x "$BIN" ] || { echo "missing $BIN"; exit 1; }
echo "host=$(hostname) gpu=${CUDA_VISIBLE_DEVICES:-?}"
nvidia-smi --query-gpu=name --format=csv,noheader -i "${CUDA_VISIBLE_DEVICES%%,*}"
echo; echo "### point_enum_kernel basis-vs-walk (np_cap 64, mid shard 2000/4000) ###"
"$BIN" --structure-id 3 --w5 "$W5" --shard-count 4000 --shard-index 2000 \
  --emit-capacity 300000 --ip-check --ip-bucketed --np-cap 64 --ip-stage-profile \
  --overflow-output /tmp/ovf_gpu_prof 2>&1 | grep -E 'MKP_GPU|ip_stage_profile|candidates_per_second' | head
echo DONE
