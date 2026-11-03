#!/usr/bin/env bash
# Complete GPU-code correctness regression suite (CI: .github/workflows/gpu-verification.yml).
# Needs Lean (proofs/), and CBMC, clang and gcc (nix develop .#gpu-verify). No GPU run is started.
set -euo pipefail
cd "$(dirname "$0")/.."
export ASAN_OPTIONS="${ASAN_OPTIONS:-detect_leaks=0}"
part="${1:-all}"   # proofs (Lean), source (CBMC, audits, harnesses) or all
case "$part" in all | proofs | source) ;; *) echo "Usage: $0 [all|proofs|source]" >&2; exit 2 ;; esac
if [[ "$part" != source ]]; then
  python3 scripts/test-gpu-proofs.py
  python3 scripts/test_ip_proofs.py
  python3 scripts/test-gpu-walk-proofs.py
  [[ "$part" == proofs ]] && exit 0
fi
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
python3 scripts/test-gpu-join-prefix.py
python3 scripts/test-gpu-join-type3.py
python3 scripts/test-gpu-join-leaf.py
python3 scripts/test-gpu-join-leaf.py --nonvacuity
python3 scripts/test-gpu-join-leaf.py --mutation
python3 scripts/test-gpu-walk-rank.py
python3 scripts/test-gpu-walk-rank.py --nonvacuity
python3 scripts/test-gpu-join-search.py --weighted
python3 scripts/test-gpu-join-search.py --rows 2 --max-keys 2 --weighted --shape 0 --shape 1 --shape 3
python3 scripts/test-gpu-join-search.py --range
python3 scripts/test-gpu-join-search.py --weighted --shape 0 --nonvacuity
python3 scripts/test-gpu-join-search.py --range --nonvacuity
python3 scripts/test-gpu-join-search.py --weighted --mutation
bash scripts/test-gpu-generation.sh
# The CUDA compile needs a toolkit; CI runners without one skip it.
if command -v "${NVCC:-nvcc}" > /dev/null; then
  make gpu gpu-type3 BUILD_DIR=build/correctness-gpu CUDA_ARCH="${CUDA_ARCH:-sm_120}" \
      NVCC="${NVCC:-nvcc}" NVCCFLAGS="${NVCCFLAGS:--O3 -std=c++17 -lineinfo}"
else
  echo "No CUDA compiler: skipping the CUDA build"
fi
printf '%s\n' 'All requested proof audits, source regressions, bounded checks and CUDA builds passed.'
printf '%s\n' 'See proofs/GPU_PIPELINE_STATUS.md for the remaining formal source-refinement boundaries.'
