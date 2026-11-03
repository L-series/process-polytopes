#!/usr/bin/env bash
# Complete local correctness regression suite. No production GPU run is started.
set -euo pipefail
cd "$(dirname "$0")/.."
export ASAN_OPTIONS="${ASAN_OPTIONS:-detect_leaks=0}"
python3 scripts/test-gpu-proofs.py
python3 scripts/test_ip_proofs.py
python3 scripts/test-gpu-walk-proofs.py
python3 scripts/audit_ip_arithmetic.py
python3 scripts/test-gpu-join-source.py --sanitize --exhaustive-search
python3 scripts/test-gpu-walk-source.py --sanitize
python3 scripts/test_gpu_ip_source.py --sanitize
python3 scripts/test_gpu_ip_source.py --sanitize --overflow-probe
python3 scripts/test_gpu_pipeline_source.py --sanitize
python3 scripts/test-gpu-host-bounds.py
bash scripts/build_cpu_completion.sh
python3 tests/pipeline/test_completion.py --driver build/cpu-completion/complete_deferred
python3 tests/pipeline/test_production_scheduler.py
python3 scripts/test-gpu-join-match.py
python3 scripts/test-gpu-join-match.py --nonvacuity
python3 scripts/test-gpu-walk-rank.py
python3 scripts/test-gpu-walk-rank.py --nonvacuity
python3 scripts/test-gpu-join-search.py --weighted
python3 scripts/test-gpu-join-search.py --rows 2 --max-keys 2 --weighted --shape 0 --shape 1 --shape 3
python3 scripts/test-gpu-join-search.py --range
python3 scripts/test-gpu-join-search.py --weighted --shape 0 --nonvacuity
python3 scripts/test-gpu-join-search.py --range --nonvacuity
python3 scripts/test-gpu-join-search.py --weighted --mutation
bash scripts/test-gpu-generation.sh
make gpu gpu-type3 BUILD_DIR=build/correctness-gpu CUDA_ARCH="${CUDA_ARCH:-sm_75}" \
    NVCC="${NVCC:-nvcc}" NVCCFLAGS="${NVCCFLAGS:--O3 -std=c++17 -lineinfo}"
printf '%s\n' 'All requested proof audits, source regressions, bounded checks and CUDA builds passed.'
printf '%s\n' 'See proofs/GPU_PIPELINE_STATUS.md for the remaining formal source-refinement boundaries.'
