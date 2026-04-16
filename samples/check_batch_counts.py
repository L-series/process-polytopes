#!/usr/bin/env python3
"""
Print row counts per input Parquet file and compare batch range sums
against known checkpoint sum(count) values.

Usage:
    python3 check_batch_counts.py --input /path/to/reflexive/parquet/files
"""
import sys, argparse, pathlib, itertools
import pyarrow.parquet as pq

# Hardcoded checkpoint data: (start, end_inclusive, ckpt_sum_count)
CHECKPOINTS = [
    (   0,  149,  6_947_861_331),
    ( 150,  299,  6_947_632_885),
    ( 300,  449,  6_947_816_716),
    ( 450,  599,  6_947_626_213),
    ( 600,  749,  6_947_920_365),
    ( 750,  899,  6_947_763_841),
    ( 900,  999,  4_631_774_714),
    (1000, 1049,  2_315_847_500),
    (1050, 1099,  2_315_892_381),
    (1100, 1149,  2_315_914_595),
    (1150, 1199,  2_315_749_424),
    (1200, 1249,  2_315_768_146),
    (1250, 1299,  2_315_889_871),
    (1300, 1349,  2_315_673_771),
    (1350, 1399,  2_315_755_098),
    (1400, 1449,  2_315_752_311),
    (1450, 1499,  2_315_971_213),
    (1500, 1549,  2_315_898_682),
    (1550, 1599,  2_315_837_843),
    (1600, 1649,  2_315_723_831),
    (1650, 1699,  2_315_809_400),
    (1700, 1749,  2_315_893_278),
    (1750, 1799,  2_316_013_234),
    (1800, 1849,  2_315_873_551),
    (1850, 1899,  2_315_818_956),
    (1900, 1949,  2_315_803_886),
    (1950, 1999,  2_315_993_102),
    (2000, 2049,  2_315_954_314),
    (2050, 2099,  2_315_983_645),
    (2100, 2149,  2_316_060_395),
    (2150, 2199,  2_315_959_402),
    (2200, 2249,  2_315_852_177),
    (2250, 2299,  2_315_769_762),
    (2300, 2349,  2_315_690_740),
    (2350, 2399,  2_315_781_075),
    (2400, 2449,  2_315_738_875),
    (2450, 2499,  2_315_803_337),
    (2500, 2549,  2_315_726_536),
    (2550, 2599,  2_315_835_507),
    (2600, 2649,  2_315_772_444),
    (2650, 2699,  2_315_738_120),
    (2700, 2749,  2_315_792_765),
    (2750, 2799,  2_315_932_782),
    (2800, 2849,  2_315_864_009),
    (2850, 2899,  2_315_840_449),
    (2900, 2949,  2_316_083_971),
    (2950, 2999,  2_315_978_678),
    (3000, 3049,  2_315_924_030),
    (3050, 3099,  2_315_831_112),
    (3100, 3149,  2_315_810_392),
    (3150, 3199,  2_315_982_731),
    (3200, 3249,  2_315_949_886),
    (3250, 3299,  2_315_864_764),
    (3300, 3349,  2_315_664_098),
    (3350, 3399,  2_315_646_897),
    (3400, 3449,  2_315_884_888),
    (3450, 3499,  2_315_765_760),
    (3500, 3549,  2_315_888_293),
    (3550, 3599,  2_315_918_648),
    (3600, 3649,  2_315_870_273),
    (3650, 3699,  2_315_915_439),
    (3700, 3749,  2_315_965_900),
    (3750, 3799,  2_315_781_167),
    (3800, 3849,  2_315_995_374),
    (3850, 3899,  2_315_738_973),
    (3900, 3949,  2_315_714_779),
    (3950, 3999,  2_315_960_346),
]

parser = argparse.ArgumentParser()
parser.add_argument("--input", required=True, help="Dir with input .parquet files")
args = parser.parse_args()

parquet_files = sorted(pathlib.Path(args.input).glob("*.parquet"))
if not parquet_files:
    sys.exit(f"No .parquet files found in {args.input}")

print(f"Reading row counts from {len(parquet_files)} Parquet files (metadata only)...")
row_counts = [pq.read_metadata(f).num_rows for f in parquet_files]
cumulative = [0] + list(itertools.accumulate(row_counts))

print(f"Grand total rows: {cumulative[-1]:,}\n")

# ── Per-file table ────────────────────────────────────────────────────────
print(f"{'index':>6}  {'filename':<50}  {'rows':>16}  {'cumulative':>16}")
print("-" * 95)
for i, (f, n) in enumerate(zip(parquet_files, row_counts)):
    print(f"{i:>6}  {f.name:<50}  {n:>16,}  {cumulative[i+1]:>16,}")
print("-" * 95)
print(f"{'TOTAL':>6}  {'':50}  {cumulative[-1]:>16,}\n")

# ── Batch comparison ──────────────────────────────────────────────────────
print(f"{'batch':<25}  {'input_rows':>16}  {'ckpt_sum':>16}  {'diff':>10}  status")
print("-" * 78)

any_mismatch = False
total_input = total_ckpt = 0
for start, end, ckpt_sum in CHECKPOINTS:
    input_rows = cumulative[min(end+1, len(row_counts))] - cumulative[start]
    diff = ckpt_sum - input_rows
    status = "✓" if diff == 0 else f"✗  DIFF={diff:+,}"
    if diff != 0:
        any_mismatch = True
    print(f"batch-{start:04d}-{end:04d}        {input_rows:>16,}  {ckpt_sum:>16,}  {diff:>10,}  {status}")
    total_input += input_rows
    total_ckpt  += ckpt_sum

print("-" * 78)
print(f"{'TOTAL':<25}  {total_input:>16,}  {total_ckpt:>16,}  {total_ckpt-total_input:>10,}")
print()
if any_mismatch:
    print("✗ Mismatches found above.")
else:
    print("✓ All batch sums match input row counts exactly.")
