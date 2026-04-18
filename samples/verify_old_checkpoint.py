#!/usr/bin/env python3
"""
Definitively verify the resume-skip hypothesis by cross-referencing the two
batch checkpoint files against the final merged Parquet.

Hypothesis:
  - Keys ONLY in OLD  ->  all present in final Parquet  (came from other shards)
  - Keys ONLY in NEW  ->  all present in final Parquet  (real polytopes, missed by old run)

Usage:
    python3 verify_old_checkpoint.py <old.ckpt> <new.ckpt> <final.parquet>
"""
import sys, struct
import numpy as np
import pyarrow.parquet as pq

RECORD = 72

def load_keys(path):
    """Returns (hi, lo) as two uint64 arrays, sorted by (hi, lo)."""
    with open(path, "rb") as f:
        n, = struct.unpack("<Q", f.read(8))
        raw = np.frombuffer(f.read(n * RECORD), dtype=np.uint8).reshape(n, RECORD)
    lo = np.frombuffer(raw[:, 0:8].tobytes(),  dtype=np.uint64).copy()
    hi = np.frombuffer(raw[:, 8:16].tobytes(), dtype=np.uint64).copy()
    order = np.lexsort((lo, hi))
    return hi[order], lo[order]

def sorted_diff(hi_a, lo_a, hi_b, lo_b):
    """
    Return indices into a where (hi_a[i], lo_a[i]) is NOT in b.
    Fully vectorized via np.in1d on a structured array — O(n log n), no loops.
    """
    dt = np.dtype([('hi', np.uint64), ('lo', np.uint64)])
    a = np.empty(len(hi_a), dtype=dt); a['hi'] = hi_a; a['lo'] = lo_a
    b = np.empty(len(hi_b), dtype=dt); b['hi'] = hi_b; b['lo'] = lo_b
    in_b = np.isin(a, b, assume_unique=True)
    return np.where(~in_b)[0]


def lookup_in_parquet(hi_q, lo_q, parquet_path):
    """Look up sorted query keys in a sorted Parquet file."""
    found = np.zeros(len(hi_q), dtype=bool)
    if len(hi_q) == 0:
        return found

    pf = pq.ParquetFile(parquet_path)
    meta = pf.metadata
    total_rg = meta.num_row_groups

    rg0 = meta.row_group(0)
    col_names = [rg0.column(i).path_in_schema for i in range(rg0.num_columns)]
    hi_col = col_names.index('hash_hi')
    lo_col = col_names.index('hash_lo')

    q_ptr = 0
    rgs_read = 0

    for rg in range(total_rg):
        if q_ptr >= len(hi_q):
            break

        rg_meta  = meta.row_group(rg)
        hi_stats = rg_meta.column(hi_col).statistics
        lo_stats = rg_meta.column(lo_col).statistics

        if hi_stats and hi_stats.has_min_max:
            min_hi = np.uint64(hi_stats.min)
            min_lo = np.uint64(lo_stats.min)
            max_hi = np.uint64(hi_stats.max)
            max_lo = np.uint64(lo_stats.max)

            # Advance q_ptr past keys below rg_min
            while q_ptr < len(hi_q):
                if hi_q[q_ptr] < min_hi or (hi_q[q_ptr] == min_hi and lo_q[q_ptr] < min_lo):
                    q_ptr += 1
                else:
                    break
            if q_ptr >= len(hi_q):
                break
            # Skip rg if first remaining query is above rg_max
            if hi_q[q_ptr] > max_hi or (hi_q[q_ptr] == max_hi and lo_q[q_ptr] > max_lo):
                continue

        batch  = pf.read_row_group(rg, columns=['hash_hi', 'hash_lo'])
        rg_hi  = batch.column('hash_hi').to_numpy(zero_copy_only=False).astype(np.uint64)
        rg_lo  = batch.column('hash_lo').to_numpy(zero_copy_only=False).astype(np.uint64)
        rgs_read += 1

        rg_min_hi, rg_min_lo = rg_hi[0],  rg_lo[0]
        rg_max_hi, rg_max_lo = rg_hi[-1], rg_lo[-1]

        while q_ptr < len(hi_q):
            if hi_q[q_ptr] < rg_min_hi or (hi_q[q_ptr] == rg_min_hi and lo_q[q_ptr] < rg_min_lo):
                q_ptr += 1
            else:
                break

        q_scan = q_ptr
        while q_scan < len(hi_q):
            if hi_q[q_scan] > rg_max_hi or (hi_q[q_scan] == rg_max_hi and lo_q[q_scan] > rg_max_lo):
                break
            lo_p = np.searchsorted(rg_hi, hi_q[q_scan], side='left')
            hi_p = np.searchsorted(rg_hi, hi_q[q_scan], side='right')
            if lo_p < hi_p:
                sub = rg_lo[lo_p:hi_p]
                j = np.searchsorted(sub, lo_q[q_scan])
                if j < len(sub) and sub[j] == lo_q[q_scan]:
                    found[q_scan] = True
            q_scan += 1

        pct = 100.0 * (rg + 1) / total_rg
        print(f"\r  Parquet scan: {pct:.1f}%  (read {rgs_read}/{total_rg} groups)"
              f"  resolved: {found.sum():,}/{len(hi_q):,}   ", end='', flush=True)

    print()
    return found


if len(sys.argv) != 4:
    print(__doc__); sys.exit(1)

old_path, new_path, parquet_path = sys.argv[1], sys.argv[2], sys.argv[3]

print(f"Loading OLD: {old_path}")
old_hi, old_lo = load_keys(old_path)
print(f"  {len(old_hi):,} records")

print(f"Loading NEW: {new_path}")
new_hi, new_lo = load_keys(new_path)
print(f"  {len(new_hi):,} records")

print("\nComputing set differences...")
only_old_idx = sorted_diff(old_hi, old_lo, new_hi, new_lo)
only_new_idx = sorted_diff(new_hi, new_lo, old_hi, old_lo)
both = len(old_hi) - len(only_old_idx)

print(f"  Keys in BOTH:      {both:>10,}")
print(f"  Keys ONLY in OLD:  {len(only_old_idx):>10,}  (phantom)")
print(f"  Keys ONLY in NEW:  {len(only_new_idx):>10,}  (missing from old run)")

only_old_hi, only_old_lo = old_hi[only_old_idx], old_lo[only_old_idx]
only_new_hi, only_new_lo = new_hi[only_new_idx], new_lo[only_new_idx]

print(f"\nLooking up ONLY_OLD keys in Parquet...")
old_in_pq = lookup_in_parquet(only_old_hi, only_old_lo, parquet_path)
print(f"  ONLY_OLD in Parquet: {old_in_pq.sum():,} / {len(only_old_idx):,}"
      f"  ({100*old_in_pq.mean():.2f}%)")

print(f"\nLooking up ONLY_NEW keys in Parquet...")
new_in_pq = lookup_in_parquet(only_new_hi, only_new_lo, parquet_path)
print(f"  ONLY_NEW in Parquet: {new_in_pq.sum():,} / {len(only_new_idx):,}"
      f"  ({100*new_in_pq.mean():.2f}%)")

print("\n== VERDICT ==")
if old_in_pq.all() and new_in_pq.all():
    print("CONFIRMED: Resume-skip hypothesis holds.")
    print(f"  All {len(only_old_idx):,} OLD-only keys exist in Parquet -> came from other shards.")
    print(f"  All {len(only_new_idx):,} NEW-only keys exist in Parquet -> were skipped by old run.")
elif not new_in_pq.all():
    print(f"FAIL: {(~new_in_pq).sum():,} NEW-only keys NOT in Parquet -- unexplained.")
elif not old_in_pq.all():
    print(f"FAIL: {(~old_in_pq).sum():,} OLD-only keys NOT in Parquet -- true phantoms.")
    print("  Old run processed different source data.")
