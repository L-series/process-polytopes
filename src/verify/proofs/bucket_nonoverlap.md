# Proof: Bucket-Local Dedup = Global Dedup (Plan 1.10, Property 4)

## Statement

In `fast_merge_checkpoints` (classifier.cpp), records are scattered into 256
buckets by the top byte of `key.hi`:

    bucket(r) = (r.key.hi >> 56) & 0xFF

**Claim:** If two records `x` and `y` have the same key (`x.key == y.key`),
then `bucket(x) == bucket(y)`. Therefore, deduplication within each bucket
catches all global duplicates.

## Proof

Let `x` and `y` be MergeRecords with `x.key == y.key`.

By definition of `Hash128::operator==`:
  `x.key == y.key  <==>  x.key.lo == y.key.lo  AND  x.key.hi == y.key.hi`

Since `x.key.hi == y.key.hi`, it follows immediately that:
  `(x.key.hi >> 56) & 0xFF == (y.key.hi >> 56) & 0xFF`

Therefore `bucket(x) == bucket(y)`.  **QED.**

## Stronger Property: Bucket Ordering

**Claim:** Records in bucket `b1` always sort before records in bucket `b2`
when `b1 < b2` under `key_less`.

**Proof:** Let `x` be in bucket `b1` and `y` in bucket `b2` with `b1 < b2`.

- `x.key.hi` has top byte `b1`, so `x.key.hi ∈ [b1 << 56, (b1+1) << 56)`
- `y.key.hi` has top byte `b2`, so `y.key.hi ∈ [b2 << 56, (b2+1) << 56)`
- Since `b1 < b2`: `(b1+1) << 56 ≤ b2 << 56`, so `x.key.hi < y.key.hi`
- `key_less` compares `hi` first, so `key_less(x.key, y.key)` is true.

Therefore concatenating bucket-sorted results in bucket order yields a
globally sorted sequence.  **QED.**

## Consequence

These two properties together mean that:
1. Every duplicate pair lands in the same bucket (no cross-bucket duplicates)
2. Sorting within each bucket and concatenating produces global sorted order
3. `fast_merge_checkpoints` correctly deduplicates without needing a
   global sort/merge pass

## Assumptions

- `key_less` is a strict weak ordering (verified by CBMC harness 1.1)
- `Hash128::operator==` checks both `lo` and `hi` (verified by inspection)
- The bucket function uses `>> 56` on a `uint64_t` (unsigned right shift,
  no sign extension)
