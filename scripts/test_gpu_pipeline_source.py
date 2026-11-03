#!/usr/bin/env python3
"""Run extracted production identity/routing kernels with injected corruption.

CUDA indexing and atomics have sequential host shims; the IP routine is a
three-outcome stub so these tests isolate routing. This is source validation,
not a GPU scheduling or CUB correctness proof.
"""
import argparse
import importlib.util
from pathlib import Path
import subprocess
import tempfile

ROOT = Path(__file__).resolve().parents[1]
spec = importlib.util.spec_from_file_location('join_source_extract', ROOT / 'scripts/test-gpu-join-source.py')
module = importlib.util.module_from_spec(spec)
spec.loader.exec_module(module)
block = module.block


def translation_unit():
    common = (ROOT / 'src/gpu/common.cuh').read_text()
    pipeline = (ROOT / 'src/gpu/pipeline.cuh').read_text()
    pieces = ['''#include <algorithm>
#include <array>
#include <cassert>
#include <cstdint>
#include <iostream>
#include <numeric>
#include <random>
#include <vector>
#define __device__
#define __global__
struct HostDim { unsigned int x = 0; };
HostDim blockIdx, threadIdx, blockDim{1}, gridDim{1};
inline int atomicCAS(int *p, int expected, int desired) {
    int old = *p; if (old == expected) *p = desired; return old;
}
inline int atomicExch(int *p, int value) { int old = *p; *p = value; return old; }
inline unsigned long long atomicAdd(unsigned long long *p, unsigned long long value) {
    auto old = *p; *p += value; return old;
}
struct DeviceIpScratch {};
// Controlled outcomes; no geometric property is assumed by routing tests.
inline int device_ip_check(const long long *points, int, DeviceIpScratch *, int *reason) {
    if (points[0] == 1000) return 1;
    *reason = static_cast<int>(points[0]); return 0;
}
''']
    pieces.append(common[common.index('static constexpr int kMaxSlots'):common.index('inline void check_cuda')])
    pieces.append(block(common, 'struct DeviceCwsCandidate {', True))
    pieces.append(block(pipeline, 'struct DeviceIpStats {', True))
    pieces.append(pipeline[pipeline.index('static constexpr int kRouteGenerated'):
                           pipeline.index('__device__ bool device_same_candidate(')])
    for signature in (
        '__device__ bool device_same_candidate(', '__device__ bool device_route(',
        '__global__ void initialize_routes_kernel(', '__global__ void audit_selected_kernel(',
        '__global__ void audit_compaction_kernel(', '__global__ void audit_output_kernel(',
        '__global__ void audit_terminal_kernel(', '__device__ void device_defer(',
        '__global__ void gather_candidates_kernel(', '__global__ void ip_check_kernel(',
    ):
        pieces.append(block(pipeline, signature))
    return '\n'.join(pieces)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--sanitize', action='store_true')
    parser.add_argument('--emit', type=Path)
    args = parser.parse_args()
    unit = translation_unit() + '\n' + (ROOT / 'tests/gpu/pipeline_source_harness.cpp').read_text()
    if args.emit:
        args.emit.write_text(unit)
    with tempfile.TemporaryDirectory(prefix='gpu-pipeline-source-') as directory:
        cpp = Path(directory) / 'pipeline.cpp'
        exe = Path(directory) / 'pipeline-test'
        cpp.write_text(unit)
        flags = ['-O1', '-g', '-fsanitize=address,undefined', '-fno-sanitize-recover=all'] if args.sanitize else ['-O2']
        subprocess.run(['g++', '-std=c++17', *flags, str(cpp), '-o', str(exe)], check=True)
        subprocess.run([str(exe)], check=True, timeout=60)


if __name__ == '__main__':
    main()
