#!/usr/bin/env python3
"""Pin the join leaf and verdict split proof to the reviewed CUDA source.

JoinLeafCount.lean transcribes the count product, zero/division guard and final
subtraction/addition guard. Its max is 2^64-1. Each successful intermediate is
bounded by max, so unsigned wrap cannot occur. The extracted-source CBMC query
test-gpu-join-leaf.py verifies the mask operations and verdict. It fixes counts
to one and the previous count to zero: on the successful path the only reads
of count/cnt/np are in the arithmetic statements modeled in JoinLeafCount;
none of the mask statements depend on them. A failed count returns keep in
device_gjoin_decide, before any partially updated masks can yield a verdict.

The whole functions are pinned (including the caller's failure routing), not
only the arithmetic lines. Changes require reviewing both halves of the split
and rerunning the proof and CBMC checks; --print never updates the pins.
"""
import hashlib
from pathlib import Path
import re
import sys
from test_gpu_ip_source import block

ROOT = Path(__file__).resolve().parents[1]
PINS = {
    'device_gjoin_leaf': 'c4783da6f0dbab83acd81be78def6f76e9a12d020616bf2e4d6dc07a2a5f176e',
    'device_gjoin_verdict': 'dd7a08fe25b0b0cf6382c654101c845ee712fd53b84ced35915d4590a20f14df',
    'device_gjoin_decide': 'b1d720f5b4c53cdc92a4eb11d15c8bcd7354e7f36e2509b4c833ba09f2dec504',
}


def main():
    source = (ROOT / 'src/gpu/join.cuh').read_text()
    for name, expected in PINS.items():
        result = block(source, f'__device__ {"bool" if name.endswith("leaf") else "int"} {name}(')
        normalized = re.sub(r'//[^\n]*|/\*[\s\S]*?\*/|\s+', '', result)
        digest = hashlib.sha256(normalized.encode()).hexdigest()
        if '--print' in sys.argv:
            print(f'{name}: {digest}')
        elif digest != expected:
            raise SystemExit(f'{name} changed; review JoinLeafCount.lean and the leaf/verdict CBMC split, then repin')
    if '--print' not in sys.argv:
        print('Join leaf arithmetic, mask/verdict source and failure routing match the reviewed split model')


if __name__ == '__main__':
    main()
