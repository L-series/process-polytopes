#!/usr/bin/env python3
"""Compile actual lattice.cuh with host CUDA shims and check exact walk contracts."""
import argparse
import importlib.util
from pathlib import Path
import subprocess
import tempfile

ROOT = Path(__file__).resolve().parents[1]


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--sanitize', action='store_true')
    args = parser.parse_args()
    spec = importlib.util.spec_from_file_location('join_source', ROOT / 'scripts/test-gpu-join-source.py')
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    common = (ROOT / 'src/gpu/common.cuh').read_text()
    source = '''#include <algorithm>
#include <array>
#include <cassert>
#include <cmath>
#include <cstdint>
#include <cstdlib>
#include <iostream>
#include <functional>
#include <limits>
#include <random>
#include <set>
#include <vector>
#include "catalogue_bridge.h"
using std::isfinite;
#define __device__
#define __forceinline__ inline
inline int __ffs(unsigned int x) { return __builtin_ffs(x); }
'''
    source += common[common.index('static constexpr int kMaxSlots'):common.index('inline void check_cuda')]
    source += module.block(common, 'struct DeviceCwsCandidate {', True)
    source += (ROOT / 'src/gpu/lattice.cuh').read_text().replace('#include "common.cuh"', '').replace('#pragma once', '')
    source += (ROOT / 'tests/gpu/walk_source_harness.cpp').read_text()
    with tempfile.TemporaryDirectory(prefix='walk-source-') as directory:
        directory = Path(directory)
        cpp = directory / 'walk.cpp'
        cpp.write_text(source)
        flags = ['-O1', '-g', '-fsanitize=address,undefined', '-fno-omit-frame-pointer'] if args.sanitize else ['-O2']
        objects = []
        for name, path in [('catalogue', ROOT / 'src/cws/catalogue.c'), ('bridge', ROOT / 'src/gpu/catalogue_bridge.c')]:
            obj = directory / f'{name}.o'
            subprocess.run(['cc', '-std=c11', *flags, '-DPOLY_Dmax=5', '-I', str(ROOT / 'PALP'),
                            '-I', str(ROOT / 'src/cws'), '-I', str(ROOT / 'src/gpu'), '-c', str(path), '-o', str(obj)], check=True)
            objects.append(str(obj))
        exe = directory / 'walk-check'
        subprocess.run(['g++', '-std=c++17', *flags, '-I', str(ROOT / 'src/gpu'), str(cpp), *objects, '-o', str(exe)], check=True)
        subprocess.run([str(exe)], check=True)


if __name__ == '__main__':
    main()
