#!/usr/bin/env python3
"""
Count unique polytopes and duplicates per vertex_count.
Reads only vertex_count + count columns — safe on any RAM.
"""
import sys
import pyarrow.parquet as pq
import numpy as np
import collections

path = sys.argv[1] if len(sys.argv) > 1 else "~/results/output.parquet/unique_polytopes.parquet"

unique_per_vc   = collections.Counter()
rawcount_per_vc = collections.Counter()

pf = pq.ParquetFile(path)
for batch in pf.iter_batches(columns=["vertex_count", "count"], batch_size=4_000_000):
    vcs    = batch.column("vertex_count").to_numpy(zero_copy_only=False)
    counts = batch.column("count").to_numpy(zero_copy_only=False)

    for v in np.unique(vcs):
        mask = vcs == v
        unique_per_vc[int(v)]   += int(mask.sum())
        rawcount_per_vc[int(v)] += int(counts[mask].sum())

# ── Print table ───────────────────────────────────────────────────────────
print(f"{'vertex_count':>14}  {'unique_polytopes':>18}  {'raw_total':>14}  {'duplicates':>14}  {'dup_%':>7}")
print("-" * 78)

total_unique = total_raw = 0
for v in sorted(unique_per_vc):
    u = unique_per_vc[v]
    r = rawcount_per_vc[v]
    d = r - u
    pct = 100.0 * d / r if r else 0.0
    print(f"{v:>14}  {u:>18,}  {r:>14,}  {d:>14,}  {pct:>6.1f}%")
    total_unique += u
    total_raw    += r

print("-" * 78)
total_dups = total_raw - total_unique
print(f"{'TOTAL':>14}  {total_unique:>18,}  {total_raw:>14,}  {total_dups:>14,}  {100.0*total_dups/total_raw:>6.1f}%")

# ── Print table ───────────────────────────────────────────────────────────
print(f"{'vertex_count':>14}  {'unique_polytopes':>18}  {'raw_total':>14}  {'duplicates':>14}  {'dup_%':>7}")
print("-" * 78)

total_unique = total_raw = 0
for v in sorted(unique_per_vc):
    u = unique_per_vc[v]
    r = rawcount_per_vc[v]
    d = r - u
    pct = 100.0 * d / r if r else 0.0
    print(f"{v:>14}  {u:>18,}  {r:>14,}  {d:>14,}  {pct:>6.1f}%")
    total_unique += u
    total_raw    += r

print("-" * 78)
total_dups = total_raw - total_unique
print(f"{'TOTAL':>14}  {total_unique:>18,}  {total_raw:>14,}  {total_dups:>14,}  {100.0*total_dups/total_raw:>6.1f}%")
