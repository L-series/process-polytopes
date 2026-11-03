#!/usr/bin/env python3
"""Pin validate_gjoin_index to JoinIndexValidator.lean.

The model represents [begin,end) by drop/take, seen[target-begin] by a set of
absolute targets, and the previous record by an Option key. The output loop
p=-1..shared-1, skipping position, is positions(shared,position): position
followed by the other positions in increasing order. Unpacking is the source
model's field, division/modulo by powers of two. Width tests are unchanged.
The model assumes source unsigned-word inputs; the shape guard precedes scan.
Normal return is Some visited; every throw is None. scan_sound proves bounds,
distinct targets, strict key ordering and decoded fields; validateIndex_complete
proves no canonical index is omitted. This is a reviewed transcription pin,
not a verified C++ frontend.
"""
import hashlib
from pathlib import Path
import re
import sys
from test_gpu_ip_source import block

ROOT = Path(__file__).resolve().parents[1]
PIN = 'f4494a41ca77a2fdb247b1ec365e6b78c8d8c45e494629bcca102927b80b485a'


def main():
    function = block((ROOT / 'src/gpu/join.cuh').read_text(), 'inline void validate_gjoin_index(')
    normalized = re.sub(r'//[^\n]*|/\*[\s\S]*?\*/|\s+', '', function)
    digest = hashlib.sha256(normalized.encode()).hexdigest()
    if '--print' in sys.argv:
        print(digest)
    elif digest != PIN:
        raise SystemExit('validate_gjoin_index changed; review JoinIndexValidator.lean, reprove and repin')
    else:
        print('Join position-index validator matches its pinned Lean transcription')


if __name__ == '__main__':
    main()
