#!/usr/bin/env python3
"""Pin the Lean walk-validator model to lattice.cuh.

This reviewed transcription covers `device_walk_lex_greater` and
`device_walk_validate_points`. The Lean model uses `List.Lex` for the ordered
five-coordinate comparison and finite sets of active coordinate bits for the
unsigned masks. `device_walk_validate_points` assumes its caller already
passed the certified int32 root-box check and supplies a readable point slice;
these are explicit model premises, not checks silently added to the function.
"""
import hashlib
from pathlib import Path
import re
import sys

from test_gpu_ip_source import block

ROOT = Path(__file__).resolve().parents[1]
MODEL = ROOT / 'proofs/Refpoly/GpuWalkValidator.lean'


def source_digest() -> str:
    source = (ROOT / 'src/gpu/lattice.cuh').read_text()
    pieces = (
        block(source, '__device__ int device_walk_lex_greater('),
        block(source, '__device__ int device_walk_validate_points('),
    )
    normalized = re.sub(r'//[^\n]*|/\*[\s\S]*?\*/|\s+', '', ''.join(pieces))
    return hashlib.sha256(normalized.encode()).hexdigest()


def main() -> None:
    header = f'-- Source-function SHA256: {source_digest()}\n'
    model = MODEL.read_text()
    if '--print' in sys.argv:
        print(header.strip().split(': ', 1)[1])
    elif '--write' in sys.argv:
        model = re.sub(r'^-- Source-function SHA256: [0-9a-f]+\n', '', model)
        MODEL.write_text(header + model)
        print('Pinned walk-validator Lean model to current source')
    elif not model.startswith(header):
        raise SystemExit('walk validator changed; review GpuWalkValidator.lean, reprove and repin')
    else:
        print('Walk validator source matches its pinned Lean model')


if __name__ == '__main__':
    main()
