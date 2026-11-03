#!/usr/bin/env python3
"""Extract the actual serial GPU IP path for exact host source regressions.

Only CUDA annotations/intrinsics are shimmed. This is source validation,
not a CUDA compiler or memory-model proof.
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
    source = (ROOT / 'src/gpu/ip_check.cuh').read_text()
    lattice = (ROOT / 'src/gpu/lattice.cuh').read_text()
    pieces = ['''#include <algorithm>
#include <array>
#include <cassert>
#include <cstdint>
#include <cstdlib>
#include <iostream>
#include <limits>
#include <random>
#include <vector>
#define __device__
#define __forceinline__ inline
inline int atomicExch(int *p, int v) { int old = *p; *p = v; return old; }
inline int __popcll(unsigned long long x) { return __builtin_popcountll(x); }
''']
    for name in ('DeviceEquation5', 'DeviceCEqList5', 'DeviceEqList5', 'DeviceIpScratch'):
        pieces.append(block(source, 'struct ' + name + ' {', True))
    for name in ('add', 'sub', 'mul', 'div', 'rem'):
        pieces.append(block(source, '__device__ __forceinline__ long long device_ip_' + name + '('))
    for name in ('abs_ll', 'nngcd', 'egcd', 'round_q', 'w_to_glz'):
        pieces.append(block(source, '__device__ long long device_ip_' + name + '('))
    pieces.append(block(lattice, '__device__ int device_vec_greater_than('))
    for result, name in (
        ('long long','device_eval_eq'), ('long long','device_ip_eval_eq'),
        ('int','device_inci_abs'), ('unsigned long long','device_eq_to_inci'),
        ('long long','device_vz_to_base'), ('int','device_orthbase_red_by_v'),
        ('int','device_new_start_vertex'), ('int','device_glz_start_simplex'),
        ('DeviceEquation5','device_eev_to_equation'), ('int','device_is_good_ceq'),
        ('int','device_search_new_vertex'), ('void','device_make_new_ceqs'),
        ('int','device_equation_has_normal'), ('int','device_ip_search_bad_eq'),
        ('int','device_ip_supporting_certificate'), ('int','device_ip_check'),
    ):
        pieces.append(block(source, '__device__ ' + result + ' ' + name + '('))
    return '\n'.join(pieces)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--sanitize', action='store_true')
    parser.add_argument('--overflow-probe', action='store_true')
    parser.add_argument('--emit', type=Path)
    args = parser.parse_args()
    unit = translation_unit() + '\n' + (ROOT / 'tests/gpu/ip_source_harness.cpp').read_text()
    if args.emit:
        args.emit.write_text(unit)
    with tempfile.TemporaryDirectory(prefix='gpu-ip-source-') as directory:
        cpp = Path(directory) / 'ip.cpp'
        exe = Path(directory) / 'ip-test'
        cpp.write_text(unit)
        flags = ['-O1', '-g', '-fsanitize=address,undefined', '-fno-sanitize-recover=all'] if args.sanitize else ['-O2']
        if args.overflow_probe:
            flags.append('-DIP_OVERFLOW_PROBE')
        subprocess.run(['g++', '-std=c++20', *flags, str(cpp), '-o', str(exe)], check=True)
        subprocess.run([str(exe)], check=True, timeout=120)


if __name__ == '__main__':
    main()
