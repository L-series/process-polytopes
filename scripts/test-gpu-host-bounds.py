#!/usr/bin/env python3
"""Check actual host arithmetic/sharding helpers against unsigned __int128."""
import importlib.util
from pathlib import Path
import subprocess
import tempfile

ROOT = Path(__file__).resolve().parents[1]
spec = importlib.util.spec_from_file_location('extract_blocks', ROOT/'scripts/test-gpu-join-source.py')
module = importlib.util.module_from_spec(spec)
spec.loader.exec_module(module)
common = (ROOT/'src/gpu/common.cuh').read_text()
generation = (ROOT/'src/gpu/generate.cuh').read_text()
pipeline = (ROOT/'src/gpu/pipeline.cuh').read_text()
parts = ['''#include <cassert>
#include <cstdint>
#include <iostream>
#include <limits>
#include <random>
#include <stdexcept>
''', common[common.index('static constexpr int kMaxSlots'):common.index('inline void check_cuda')]]
for name in ('DeviceDescriptor',):
    parts.append(module.block(common, 'struct '+name+' {', True))
for name in ('checked_mul', 'checked_add'):
    parts.append(module.block(common, 'inline std::uint64_t '+name+'('))
for marker in ('inline std::uint64_t triangular_count(', 'inline bool use_pair_scan(',
               'inline std::uint64_t descriptor_scan_space(', 'inline void descriptor_shard_range('):
    parts.append(module.block(generation, marker))
for name in ('DeviceIpStats', 'HostIpResult'):
    parts.append(module.block(pipeline, 'struct '+name+' {', True))
for marker in ('inline int np_key_bits(', 'inline void accumulate(', 'inline std::uint64_t outcome_sum('):
    parts.append(module.block(pipeline, marker))
parts.append('''
int main() {
    std::mt19937_64 random(20261102);
    const auto max = std::numeric_limits<std::uint64_t>::max();
    for (int t=0;t<200000;++t) {
        DeviceDescriptor d;
        d.selection_product = t<8 ? max-t : random();
        int shards = t%2 ? 2147483647 : int(random()%1000+1);
        int index = int(random()%shards);
        if (t%3==0) index=shards-1;
        std::uint64_t begin=0,count=0;
        descriptor_shard_range(d,shards,index,&begin,&count);
        using Wide = unsigned __int128;
        auto expected = std::uint64_t(Wide(d.selection_product)*index/shards);
        auto end = std::uint64_t(Wide(d.selection_product)*(std::uint64_t(index)+1)/shards);
        assert(begin==expected && count==end-expected);
        auto n = random()%2147483648ULL;
        assert(triangular_count(n)==std::uint64_t(Wide(n)*(n+1)/2));
        auto a=random(),b=random();
        bool overflow = Wide(a)*b>max;
        bool failed=false;
        try { assert(checked_mul(a,b)==std::uint64_t(Wide(a)*b)); }
        catch (const std::runtime_error&) { failed=true; }
        assert(failed==overflow);
    }
    for (int cap=6;cap<10000;++cap) {
        int bits=0;for (unsigned v=cap;v;v>>=1) ++bits;
        assert(np_key_bits(cap)==bits);
    }
    assert(np_key_bits(2147483647)==31);
    HostIpResult all, chunk;
    chunk.stats.accepted=3;chunk.stats.deferred=4;chunk.stats.rejected=5;
    chunk.stats.np_mismatch=1;chunk.candidates=12;chunk.scanned_selections=7;
    accumulate(&all,chunk);accumulate(&all,chunk);
    assert(all.candidates==24 && all.scanned_selections==14 && outcome_sum(all.stats)==24);
    assert(all.stats.np_mismatch==2);
    all.stats.accepted=max;
    bool failed=false;try { accumulate(&all,chunk); } catch(const std::runtime_error&) {failed=true;}
    assert(failed);
    std::cout << "Host source bounds: 200000 full-width shard/arithmetic cases; counter and key-width edges passed\\n";
}
''')
with tempfile.TemporaryDirectory(prefix='gpu-host-bounds-') as work:
    source=Path(work)/'bounds.cpp';binary=Path(work)/'bounds'
    source.write_text('\n'.join(parts))
    subprocess.run(['g++','-std=c++17','-O1','-g','-fsanitize=address,undefined',
                    '-fno-omit-frame-pointer',str(source),'-o',str(binary)],check=True)
    subprocess.run([str(binary)],check=True)
