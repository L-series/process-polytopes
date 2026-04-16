#!/usr/bin/env python3
"""
Compare two checkpoint files for the same batch to diagnose resume-bug artifacts.

Usage:
    python3 compare_checkpoints.py <old.ckpt> <new.ckpt>

Reports:
  - Record counts and sum(count) for each
  - Keys only in OLD  → phantom entries (double-processing created extras, or
                         different input produced different polytopes)
  - Keys only in NEW  → missed entries (files were skipped during old run)
  - Keys in both      → check if counts differ (double-counting inflated them)
"""
import sys, struct, numpy as np

RECORD  = 72
COUNT_OFF = 16   # byte offset of count (uint64) within MergeRecord

def load(path):
    with open(path, "rb") as f:
        n, = struct.unpack("<Q", f.read(8))
        raw = np.frombuffer(f.read(n * RECORD), dtype=np.uint8).reshape(n, RECORD)
    keys   = np.frombuffer(raw[:, :16].tobytes(),       dtype=np.uint64).reshape(n, 2)  # (lo, hi)
    counts = np.frombuffer(raw[:, COUNT_OFF:COUNT_OFF+8].tobytes(), dtype=np.uint64)
    return keys, counts

if len(sys.argv) != 3:
    print(__doc__); sys.exit(1)

old_path, new_path = sys.argv[1], sys.argv[2]
print(f"Loading {old_path} ...")
old_keys, old_counts = load(old_path)
print(f"Loading {new_path} ...")
new_keys, new_counts = load(new_path)

print()
print(f"{'':30s}  {'OLD':>18}  {'NEW':>18}")
print("-" * 70)
old_sum = int(old_counts.sum())
new_sum = int(new_counts.sum())
print(f"{'Records (unique polytopes)':30s}  {len(old_keys):>18,}  {len(new_keys):>18,}")
print(f"{'sum(count)':30s}  {old_sum:>18,}  {new_sum:>18,}")
print(f"{'Δ records':30s}  {len(new_keys)-len(old_keys):>+18,}")
print(f"{'Δ sum(count)':30s}  {new_sum-old_sum:>+18,}")
print()

# Build sorted structured arrays for set operations
# Key = (hi, lo) so sort order matches the merge sort
def make_structured(keys):
    dt = np.dtype([('hi', np.uint64), ('lo', np.uint64)])
    arr = np.empty(len(keys), dtype=dt)
    arr['lo'] = keys[:, 0]
    arr['hi'] = keys[:, 1]
    return arr

print("Building key sets (this may take a moment for large files)...")
old_s = make_structured(old_keys)
new_s = make_structured(new_keys)

old_sorted_idx = np.argsort(old_s, order=('hi','lo'))
new_sorted_idx = np.argsort(new_s, order=('hi','lo'))
old_sorted = old_s[old_sorted_idx]
new_sorted = new_s[new_sorted_idx]

# Two-pointer set difference
def set_diff_counts(a_sorted, a_counts_sorted, b_sorted):
    """Return (only_in_a_count, only_in_a_sum_count)"""
    i, j = 0, 0
    n_a, n_b = len(a_sorted), len(b_sorted)
    only_count = 0
    only_sum   = np.uint64(0)
    while i < n_a and j < n_b:
        if a_sorted[i] < b_sorted[j]:
            only_count += 1
            only_sum   += a_counts_sorted[i]
            i += 1
        elif a_sorted[i] > b_sorted[j]:
            j += 1
        else:
            i += 1; j += 1
    while i < n_a:
        only_count += 1
        only_sum   += a_counts_sorted[i]
        i += 1
    return only_count, int(only_sum)

old_counts_sorted = old_counts[old_sorted_idx]
new_counts_sorted = new_counts[new_sorted_idx]

print("Computing set differences...")
only_old_n, only_old_sum = set_diff_counts(old_sorted, old_counts_sorted, new_sorted)
only_new_n, only_new_sum = set_diff_counts(new_sorted, new_counts_sorted, old_sorted)

print()
print(f"Keys ONLY in OLD (phantom / skipped files on new side): {only_old_n:>12,}  sum(count)={only_old_sum:>14,}")
print(f"Keys ONLY in NEW (missing from old / skipped on old):   {only_new_n:>12,}  sum(count)={only_new_sum:>14,}")
print(f"Keys in BOTH:                                           {len(old_keys)-only_old_n:>12,}")

print()
print("── Diagnosis ──────────────────────────────────────────────────────")
if only_new_n > 0 and only_old_sum > 0 and abs(only_old_sum - only_new_sum - (old_sum - new_sum)) < 1000:
    print(f"  ✓ Resume-skip + double-count confirmed:")
    print(f"    {only_new_n:,} polytopes were SKIPPED in the old run  (sum(count)={only_new_sum:,})")
    print(f"    {only_old_n:,} polytopes are phantom in old            (sum(count)={only_old_sum:,})")
    print(f"    Net sum(count) difference: {old_sum-new_sum:+,}")
elif only_old_n == 0 and only_new_n > 0:
    print(f"  ✓ Pure skip confirmed: old run missed {only_new_n:,} polytopes.")
elif only_old_n == 0 and only_new_n == 0:
    print("  ✓ Key sets are identical. Count differences are pure double-counting.")
else:
    print(f"  Δ Records: old has {only_old_n:,} phantom keys, new has {only_new_n:,} extra keys.")
    print(f"  Check if source data actually changed between runs.")
