#!/usr/bin/env python3
"""
Verify a merge_checkpoints output parquet against the source binary shard files.

Usage:
    python3 verify_merge.py <output.parquet> <shard1.ckpt> [shard2.ckpt ...]

Example (2-file test):
    python3 verify_merge.py /tmp/merge_test_out.parquet \
        /tmp/merge_test/batch-1000-1049.ckpt \
        /tmp/merge_test/batch-1050-1099.ckpt
"""

import struct, sys, pathlib
import numpy as np
import pyarrow.parquet as pq
import pandas as pd

# MergeRecord layout:
#   Hash128 key       = 16 bytes   (lo uint64, hi uint64)
#   PolytopeInfo info = 56 bytes
#     count           uint64  @ offset 16
#     first_weights   int32×6 @ offset 24
#     vertex_count    int16   @ offset 48
#     facet_count     int16   @ offset 50
#     point_count     int32   @ offset 52
#     dual_pt_count   int32   @ offset 56
#     h11,h12,h13     int16×3 @ offset 60
#     (2 bytes pad)           @ offset 66
# Total: 72 bytes

RECORD_BYTES = 72
COUNT_OFFSET = 16   # byte offset of `count` within a MergeRecord

if len(sys.argv) < 3:
    print(__doc__)
    sys.exit(1)

parquet_path = sys.argv[1]
shard_paths  = sys.argv[2:]

# ── Read shard stats ──────────────────────────────────────────────────────
print("Input shards:")
total_input_records = 0
total_input_count   = 0

for path in shard_paths:
    with open(path, "rb") as f:
        n, = struct.unpack("<Q", f.read(8))
        raw = f.read(n * RECORD_BYTES)

    data = np.frombuffer(raw, dtype=np.uint8).reshape(n, RECORD_BYTES)
    # count is 8 bytes at COUNT_OFFSET, little-endian uint64
    count_raw = np.ascontiguousarray(data[:, COUNT_OFFSET:COUNT_OFFSET+8])
    counts = np.frombuffer(count_raw.tobytes(), dtype="<u8")
    shard_sum = int(counts.sum())

    print(f"  {pathlib.Path(path).name}: "
          f"{n:,} records  "
          f"({n * RECORD_BYTES / 1e9:.2f} GB)  "
          f"sum(count)={shard_sum:,}")
    total_input_records += n
    total_input_count   += shard_sum

print(f"\nTotal input records:   {total_input_records:,}"
      f"  (checkpoint-level, already within-shard deduped)")
print(f"Total input count sum: {total_input_count:,}"
      f"  (raw polytopes covered by these shards)")

# ── Read parquet ──────────────────────────────────────────────────────────
print(f"\nOutput: {parquet_path}")
df = pq.read_table(parquet_path).to_pandas()

unique_count  = len(df)
output_count  = int(df["count"].sum())
shard_dups    = total_input_records - unique_count

print(f"Unique polytopes:      {unique_count:,}")
print(f"Checkpoint dups removed: {shard_dups:,}"
      f"  ({100 * shard_dups / total_input_records:.1f}% of shard records)")
print(f"Output sum(count):     {output_count:,}")

# ── Checks ────────────────────────────────────────────────────────────────
ok = True

n_distinct = df[["hash_lo", "hash_hi"]].drop_duplicates().shape[0]
if n_distinct == unique_count:
    print("\n✓ All output hashes are unique")
else:
    print(f"\n✗ HASH COLLISION: {unique_count - n_distinct} duplicate keys in output!")
    ok = False

bad_counts = (df["count"] < 1).sum()
if bad_counts == 0:
    print("✓ All counts ≥ 1")
else:
    print(f"✗ {bad_counts} rows have count < 1")
    ok = False

# The critical invariant: no raw polytope lost or double-counted.
if output_count == total_input_count:
    print(f"✓ sum(output count) == sum(input count) == {output_count:,}")
else:
    diff = output_count - total_input_count
    print(f"✗ sum(output count) {output_count:,} != sum(input count) {total_input_count:,}")
    print(f"  diff = {diff:+,}  ({'over' if diff > 0 else 'under'}-counted)")
    ok = False

print()
if ok:
    print("ALL CHECKS PASSED")
else:
    print("SOME CHECKS FAILED")
    sys.exit(1)
