#!/usr/bin/env python3
"""Compile the actual join functions on the host and compare with direct enumeration.

This is differential validation of the source, not a CUDA/compiler proof.
Only CUDA memory/annotation/intrinsic operations are replaced by host shims.
"""
import argparse
from pathlib import Path
import re
import subprocess
import tempfile

ROOT = Path(__file__).resolve().parents[1]
GPU = ROOT / "src/gpu"


def block(source, marker, semicolon=False):
    start = source.index(marker)
    begin = source.index("{", start)
    # Count C++ braces while ignoring comments and quoted strings.
    tokens = re.finditer(r'//[^\n]*|/\*[\s\S]*?\*/|"(?:\\.|[^"\\])*"|\'(?:\\.|[^\'\\])*\'|[{}]', source[begin:])
    depth = 0
    for token in tokens:
        if token[0] == "{":
            depth += 1
        elif token[0] == "}":
            depth -= 1
            if depth == 0:
                end = begin + token.end()
                return source[start:end] + (";\n" if semicolon else "\n")
    raise ValueError(f"Unclosed block: {marker}")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--sanitize", action="store_true")
    parser.add_argument("--exhaustive-search", action="store_true")
    args = parser.parse_args()
    common = (GPU / "common.cuh").read_text()
    pools = (GPU / "pools.cuh").read_text()
    join = (GPU / "join.cuh").read_text()
    type3 = (GPU / "join_type3.cuh").read_text()
    includes = """#include <algorithm>
#include <array>
#include <cassert>
#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <functional>
#include <exception>
#include <iostream>
#include <limits>
#include <random>
#include <sstream>
#include <stdexcept>
#include <thread>
#include <vector>
#include "catalogue_bridge.h"
#define __device__
#define __forceinline__ inline
inline int __ffs(unsigned int x) { return __builtin_ffs(x); }
constexpr int cudaMemcpyHostToDevice = 0;
template<class T> int cudaMalloc(T **p, std::size_t n) { *p = static_cast<T*>(std::malloc(n)); if (!*p) throw std::bad_alloc(); return 0; }
inline int cudaMemcpy(void *d, const void *s, std::size_t n, int) { if (n) std::memcpy(d,s,n); return 0; }
inline void check_cuda(int s, const char*) { assert(s == 0); }
"""
    pieces = [includes, common[common.index("static constexpr int kMaxSlots"):common.index("inline void check_cuda")]]
    for name in ("SelectedEntry", "PoolInfo", "DeviceCwsCandidate"):
        pieces.append(block(common, f"struct {name} {{", True))
    pieces.append(block(pools, "inline int pool_key("))
    pieces.append("using PoolArray = std::array<PoolInfo, kPoolKeyStride * kPoolKeyStride>;\n")
    for name in ("GJoinKey", "GJoinIdx", "GJoinTable", "GJoinPlan", "GJoinCensus"):
        pieces.append(block(join, f"struct {name} {{", True))
    pieces.append(block(join, "inline void validate_gjoin_fibres("))
    for name in ("JoinKey", "JoinTables", "HostJoinTables"):
        pieces.append(block(type3, f"struct {name} {{", True))
    for name in ("device_join_cat", "device_join_lo", "device_join_hi"):
        pieces.append(block(type3, "__device__ __forceinline__ unsigned int " + name + "("))
    for marker in (
        "__device__ __forceinline__ unsigned int device_join_find(",
        "__device__ __forceinline__ unsigned long long device_join_pack(",
        "__device__ __forceinline__ unsigned long long device_spread7(",
        "__device__ __forceinline__ void device_join_cert_bits(",
        "__device__ int device_join_decide(",
        "inline void build_join_table(",
    ):
        pieces.append(block(type3, marker))
    pieces.append(block(join, "__device__ void device_gjoin_begin_level("))
    pieces.append(block(join, "__device__ bool device_gjoin_match_key("))
    pieces.append(block(join, "__device__ int device_gjoin_decide("))
    pieces.append(block(join, "struct HostGJoinTable {", True))
    pieces.append(block(join, "inline void validate_gjoin_index("))
    pieces.append(block(join, "inline void build_gjoin_table("))
    pieces.append(block(join, "inline void validate_gjoin_plan("))
    pieces.append(block(join, "inline GJoinPlan build_gjoin_plan("))
    if args.exhaustive_search:
        pieces.append("#define CWS_JOIN_EXHAUSTIVE 1\n")
        pieces.append((ROOT / "tests/gpu/join_search_host.cpp").read_text())
    pieces.append((ROOT / "tests/gpu/join_source_harness.cpp").read_text())
    with tempfile.TemporaryDirectory(prefix="join-source-") as directory:
        cpp = Path(directory) / "join.cpp"
        exe = Path(directory) / "join-test"
        cpp.write_text("\n".join(pieces))
        flags = ["-O1", "-g", "-fsanitize=address,undefined", "-fno-omit-frame-pointer"] if args.sanitize else ["-O2"]
        objects = []
        for name, path in (("catalogue", ROOT / "src/cws/catalogue.c"), ("bridge", GPU / "catalogue_bridge.c")):
            obj = Path(directory) / f"{name}.o"
            subprocess.run(["cc", "-std=c11", *flags, "-DPOLY_Dmax=5", "-I", str(ROOT / "PALP"), "-I", str(ROOT / "src/cws"),
                            "-I", str(GPU), "-c", str(path), "-o", str(obj)], check=True)
            objects.append(str(obj))
        subprocess.run(["g++", "-std=c++17", "-pthread", *flags, "-I", str(GPU), str(cpp), *objects, "-o", str(exe)],
                       check=True)
        subprocess.run([str(exe)], check=True)


if __name__ == "__main__":
    main()
