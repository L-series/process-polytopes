#!/usr/bin/env python3
"""Pin the join search loop to its Lean transcription (JoinLoopSource.lean).

The transcribed source is device_gjoin_begin_level, device_gjoin_match_key and
the search loop of device_gjoin_decide (from `int depth = 0;` to the leaf).
Each is compared by the SHA-256 of its text with comments and whitespace
removed. One reviewed representational step: begin_level packs a lookup key
with `(want << 16) | val[..]`, transcribed as `want * 2^16 + val`; they agree
because val only ever holds 16-bit key fields (match_key) or the initial 0. A change fails this check until JoinLoopSource.lean is reviewed
against the new text, reproved, and the hashes below are updated.
"""
import hashlib
from pathlib import Path
import re
import sys
from test_gpu_ip_source import block

ROOT = Path(__file__).resolve().parents[1]
PINS = {
    'device_gjoin_begin_level': '6c1fc571c82675d5b5a69983df4a3c3054cf4776b8963ae3e23ae25656d1e128',
    'device_gjoin_match_key': '17bfa75ba986e7b9f0abf52932b60eee9da6d18dd2f41db5ebbd8d1917cc3855',
    'search loop': 'aa917ddeacca32270badea3bf1923a516da4c88b15958d2e7e6cc7b7a97b3661',
}


def normalized(text):
    return re.sub(r'//[^\n]*|/\*[\s\S]*?\*/|\s+', '', text)


def pieces():
    source = (ROOT / 'src/gpu/join.cuh').read_text()
    decide = block(source, '__device__ int device_gjoin_decide(')
    loop = decide[decide.index('int depth = 0;'):decide.index('// Leaf: one block')]
    return {
        'device_gjoin_begin_level': block(source, '__device__ void device_gjoin_begin_level('),
        'device_gjoin_match_key': block(source, '__device__ bool device_gjoin_match_key('),
        'search loop': loop,
    }


def main():
    digests = {name: hashlib.sha256(normalized(text).encode()).hexdigest() for name, text in pieces().items()}
    if '--print' in sys.argv:
        for name, digest in digests.items():
            print(f'{name}: {digest}')
        return
    bad = [name for name, digest in digests.items() if PINS[name] != digest]
    if bad:
        raise SystemExit('Join loop source changed (' + ', '.join(bad) + '); review JoinLoopSource.lean and repin')
    print('Join search loop source matches its pinned Lean transcription')


if __name__ == '__main__':
    main()
