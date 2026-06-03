#!/usr/bin/env python3
"""Summarize the pre-IP lattice-point-count distribution gathered by the
PALP_PROFILE_NP hook in PALP/cws.c.

Input: one or more text files (or stdin) containing lines of the form
    NP <np> <dim> <ip>
emitted by `cws-5d.x -c5 -s<id>` when run with PALP_PROFILE_NP=<stride>.
  <np>  = number of lattice points produced by Make_CWS_Points (pre-IP)
  <dim> = polytope dimension P->n  (== 5 iff non-degenerate)
  <ip>  = 1 / 0 for full-dim candidates (IP / non-IP), -1 if degenerate

The "non-degenerate, before IP check" distribution the analysis targets is the
set of samples with dim == 5, over all values of <ip>.
"""
import sys
import math
from collections import Counter


def load(paths):
    # Population = every candidate that completed point enumeration
    # (one NP line each).  We keep all of them; degeneracy is not filtered.
    np_vals = []          # point counts
    ip_flags = []         # matching ip flag (0/1, -1 if not computed)
    dim_counter = Counter()
    total = 0
    for line in _lines(paths):
        if not line.startswith("NP "):
            continue
        parts = line.split()
        if len(parts) < 4:
            continue
        try:
            np_, dim, ip = int(parts[1]), int(parts[2]), int(parts[3])
        except ValueError:
            continue
        total += 1
        dim_counter[dim] += 1
        np_vals.append(np_)
        ip_flags.append(ip)
    return total, dim_counter, np_vals, ip_flags


def _lines(paths):
    if not paths:
        for line in sys.stdin:
            yield line
        return
    for p in paths:
        with open(p) as f:
            for line in f:
                yield line


def percentile(sorted_vals, q):
    if not sorted_vals:
        return float("nan")
    idx = min(len(sorted_vals) - 1, int(q / 100.0 * len(sorted_vals)))
    return sorted_vals[idx]


def main():
    paths = sys.argv[1:]
    total, dim_counter, np_vals, ip_flags = load(paths)
    n = len(np_vals)
    print(f"# candidates sampled (NP)    : {total}")
    print(f"# ambient-dim breakdown      : "
          + ", ".join(f"P->n={d}:{c}" for d, c in sorted(dim_counter.items()))
          + "   (always == POLY_Dmax for valid CWS)")
    if n == 0:
        print("no samples")
        return

    sv = sorted(np_vals)
    s = sum(sv)
    mean = s / n
    var = sum((x - mean) ** 2 for x in sv) / n
    print()
    print("=== point-count (np) distribution over all candidates (pre-IP) ===")
    print(f"  min      = {sv[0]}")
    print(f"  max      = {sv[-1]}")
    print(f"  mean     = {mean:.2f}")
    print(f"  stdev    = {math.sqrt(var):.2f}")
    for q in (1, 5, 10, 25, 50, 75, 90, 95, 99, 99.9, 99.99):
        print(f"  p{q:<5} = {percentile(sv, q)}")

    # IP acceptance overall and by np bucket
    ip1 = sum(1 for f in ip_flags if f == 1)
    print()
    print(f"=== IP acceptance (dim==5) : {ip1}/{n} = {100.0*ip1/n:.4f}% ===")

    # Power-of-two buckets (the natural GPU-bucketing breakpoints)
    print()
    print("=== cumulative buckets (np <= 2^k) ===")
    print(f"  {'bucket':>10} {'count':>12} {'cum%':>8} {'ip_rate%':>9}")
    edges = [16, 32, 64, 128, 256, 512, 1024, 2048, 4096, 8192,
             65536, 1 << 20, 1 << 21]
    cum = 0
    prev = 0
    for e in edges:
        in_bin = [ip_flags[i] for i in range(n) if prev < sv[i] <= e]
        # cum count of np<=e
        cum = sum(1 for x in sv if x <= e)
        bin_ct = len(in_bin)
        iprate = (100.0 * sum(1 for f in in_bin if f == 1) / bin_ct) if bin_ct else float("nan")
        print(f"  <= {e:<7} {bin_ct:>12} {100.0*cum/n:>7.3f} {iprate:>8.3f}")
        prev = e
    over = sum(1 for x in sv if x > edges[-1])
    if over:
        print(f"  >  {edges[-1]:<7} {over:>12} {100.0:>7.3f}")

    # log2 histogram (machine-readable)
    print()
    print("=== log2 histogram (lo<np<=hi : count) ===")
    hist = Counter()
    for x in sv:
        hist[0 if x <= 0 else x.bit_length()] += 1
    for b in sorted(hist):
        lo = 0 if b == 0 else (1 << (b - 1))
        hi = (1 << b) - 1 if b else 0
        print(f"  2^{b-1 if b else 0:<2} ({lo:>7}..{hi:<7}] : {hist[b]}")


if __name__ == "__main__":
    main()
