#!/usr/bin/env python3
"""Check whether a .ckpt file is sorted by its 128-bit key (lo, hi)."""
import struct, sys, mmap, os

if len(sys.argv) < 2:
    print(f"Usage: {sys.argv[0]} <file.ckpt>")
    sys.exit(1)

path = sys.argv[1]
RECORD = 72  # bytes per MergeRecord

with open(path, "rb") as f:
    n = struct.unpack("<Q", f.read(8))[0]
    data_bytes = n * RECORD
    expected = 8 + data_bytes
    actual = os.path.getsize(path)
    if actual != expected:
        print(f"ERROR: file size {actual:,} != expected {expected:,} (n={n:,})")
        sys.exit(2)

    mm = mmap.mmap(f.fileno(), 0, access=mmap.ACCESS_READ)
    prev_lo = prev_hi = 0
    violations = 0
    for i in range(n):
        off = 8 + i * RECORD
        lo, hi = struct.unpack_from("<QQ", mm, off)
        if i > 0 and (hi, lo) < (prev_hi, prev_lo):
            if violations < 10:
                print(f"  violation at record {i:,}: "
                      f"prev=({prev_hi:#x},{prev_lo:#x}) > cur=({hi:#x},{lo:#x})")
            violations += 1
        prev_hi, prev_lo = hi, lo
        if i % 50_000_000 == 0 and i > 0:
            print(f"  checked {i:,} / {n:,} records ...", flush=True)

    mm.close()

if violations == 0:
    print(f"✓ {path}: {n:,} records, correctly sorted")
else:
    print(f"✗ {path}: {violations:,} sort violations out of {n:,} records")
    sys.exit(1)
