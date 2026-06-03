#!/usr/bin/env python3
"""Aggregate PALP pipeline-timing PROF lines into a throughput / stage report.

Input: one or more files (or stdin) containing the lines emitted by the
PALP_PROFILE_TIMING / PALP_PROFILE_GENONLY hook in PALP/cws.c:

    PROF mode=<1|2> candidates=<n> wall_secs=<s> points_cycles=<c> \
         ip_cycles=<c> total_cycles=<c> total_cycles_sq=<x> ip_pass=<n> \
         np_sum=<n> np_lt6=<n> min_cycles=<c> max_cycles=<c>
    PROF_HIST <bucket>:<count> ...        (log2 buckets of per-candidate cycles)

mode 1 = timing (point enumeration + IP check timed per candidate)
mode 2 = gen-only (candidates counted, no point/IP work)

Usage:
    analyze_pipeline_profile.py [--label L] [--batch-wall S] [--genonly G] FILE...
      --batch-wall S : wall-clock of the concurrent batch (multithread runs),
                       used for aggregate multithread throughput. If omitted,
                       uses the max per-worker wall_secs.
      --genonly G    : a gen-only summary file (the .npz-like KEY=VAL line this
                       script prints with --emit-kv) to subtract generation cost
                       and split processing wall into points vs IP in seconds.
"""
import sys
import math
import argparse
from collections import Counter


def parse(files):
    workers = []   # one dict per PROF line
    hist = Counter()
    for path in files:
        f = sys.stdin if path == "-" else open(path)
        with (f if path != "-" else _nullctx(f)):
            for line in f:
                if line.startswith("PROF_HIST"):
                    for tok in line.split()[1:]:
                        b, c = tok.split(":")
                        hist[int(b)] += int(c)
                elif line.startswith("PROF "):
                    d = {}
                    for tok in line.split()[1:]:
                        k, v = tok.split("=")
                        d[k] = v
                    workers.append(d)
    return workers, hist


class _nullctx:
    def __init__(self, f):
        self.f = f
    def __enter__(self):
        return self.f
    def __exit__(self, *a):
        return False


def fi(d, k):
    return int(d[k])


def ff(d, k):
    return float(d[k])


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("files", nargs="+")
    ap.add_argument("--label", default="")
    ap.add_argument("--batch-wall", type=float, default=0.0)
    ap.add_argument("--sequential", action="store_true",
                    help="workers ran one-after-another on ONE core: "
                         "throughput = sum(candidates)/sum(wall)")
    ap.add_argument("--tsc-ghz", type=float, default=0.0,
                    help="nominal TSC GHz for cycle->time (else self-calibrate)")
    ap.add_argument("--gen-ns-per-cand", type=float, default=0.0,
                    help="gen-only ns/candidate to subtract (isolates points+IP)")
    args = ap.parse_args()

    workers, hist = parse(args.files)
    if not workers:
        print("no PROF lines found")
        return

    mode = fi(workers[0], "mode")
    n = sum(fi(w, "candidates") for w in workers)
    sum_wall = sum(ff(w, "wall_secs") for w in workers)
    max_wall = max(ff(w, "wall_secs") for w in workers)
    nworkers = len(workers)

    print(f"==== {args.label or ('mode '+str(mode))} ====")
    print(f"workers              : {nworkers}")
    print(f"candidates total     : {n:,}")

    # Per-worker single-thread throughput distribution
    per = sorted(fi(w, "candidates") / ff(w, "wall_secs")
                 for w in workers if ff(w, "wall_secs") > 0)
    if per:
        med = per[len(per) // 2]
        print(f"per-worker cand/s    : min={per[0]:,.0f} "
              f"median={med:,.0f} max={per[-1]:,.0f}")

    # Aggregate throughput
    if args.sequential:
        thru = n / sum_wall if sum_wall else 0
        print(f"sum wall_secs        : {sum_wall:.3f}   (sequential, one core)")
        print(f"throughput cand/s    : {thru:,.0f}   (single thread, {nworkers} anchors)")
    elif nworkers == 1:
        thru = n / max_wall if max_wall else 0
        print(f"throughput cand/s    : {thru:,.0f}   (single thread)")
    else:
        wall = args.batch_wall if args.batch_wall > 0 else max_wall
        thru = n / wall if wall else 0
        basis = "batch wall" if args.batch_wall > 0 else "max worker wall"
        print(f"batch wall_secs      : {wall:.3f}   ({basis})")
        print(f"throughput cand/s    : {thru:,.0f}   (aggregate, {nworkers} workers)")
        print(f"per-core cand/s      : {thru/nworkers:,.0f}   (aggregate/{nworkers})")

    if mode == 2:
        # gen-only: report ns/candidate (for subtraction by the timing pass)
        ns = (sum_wall / n * 1e9) if n else 0
        print(f"gen ns/candidate     : {ns:.1f}   (single-thread-equivalent)")
        print()
        return

    # ---- timing mode: stage split, cycles, unpredictability ----
    pc = sum(fi(w, "points_cycles") for w in workers)
    ic = sum(fi(w, "ip_cycles") for w in workers)
    tc = sum(fi(w, "total_cycles") for w in workers)
    tsq = sum(ff(w, "total_cycles_sq") for w in workers)
    ip_pass = sum(fi(w, "ip_pass") for w in workers)
    np_sum = sum(fi(w, "np_sum") for w in workers)
    np_lt6 = sum(fi(w, "np_lt6") for w in workers)
    maxc = max(fi(w, "max_cycles") for w in workers)
    minc = min(fi(w, "min_cycles") for w in workers)

    mean = tc / n
    var = tsq / n - mean * mean
    std = math.sqrt(var) if var > 0 else 0.0

    print()
    print(f"stage split (rdtsc cycles, points+IP only):")
    print(f"  point enumeration  : {100.0*pc/tc:6.2f}%   "
          f"({pc/n:,.0f} cyc/cand avg)")
    print(f"  IP check           : {100.0*ic/tc:6.2f}%   "
          f"({ic/n:,.0f} cyc/cand avg)")
    print()
    print(f"per-candidate cost (points+IP, TSC cycles):")
    print(f"  mean               : {mean:,.0f}")
    print(f"  stdev              : {std:,.0f}")
    print(f"  CV (stdev/mean)    : {std/mean:.2f}   (unpredictability)")
    print(f"  min / max          : {minc:,} / {maxc:,}   "
          f"(max/mean = {maxc/mean:,.0f}x)")
    print()
    print(f"outcomes:")
    print(f"  IP pass rate       : {100.0*ip_pass/n:.4f}%  ({ip_pass:,}/{n:,})")
    print(f"  avg np             : {np_sum/n:.2f}")
    print(f"  np<6 (degenerate)  : {100.0*np_lt6/n:.3f}%")

    # Self-calibrate TSC->time if gen ns/candidate given
    wall_for_ns = sum_wall if args.sequential else max_wall
    if args.gen_ns_per_cand > 0 and (args.sequential or nworkers == 1) and wall_for_ns > 0:
        proc_ns = (wall_for_ns / n * 1e9) - args.gen_ns_per_cand
        if proc_ns > 0:
            tsc_ghz = (tc / n) / proc_ns
            print()
            print(f"timing decomposition (per candidate, single thread):")
            full_ns = wall_for_ns / n * 1e9
            print(f"  full wall          : {full_ns:,.0f} ns")
            print(f"  generation         : {args.gen_ns_per_cand:,.0f} ns "
                  f"({100.0*args.gen_ns_per_cand/full_ns:.1f}%)")
            print(f"  point enumeration  : {proc_ns*pc/tc:,.0f} ns "
                  f"({100.0*(proc_ns*pc/tc)/full_ns:.1f}%)")
            print(f"  IP check           : {proc_ns*ic/tc:,.0f} ns "
                  f"({100.0*(proc_ns*ic/tc)/full_ns:.1f}%)")
            print(f"  (self-calibrated TSC: {tsc_ghz:.3f} GHz)")
    if args.tsc_ghz > 0:
        print(f"  mean points+IP time: {(tc/n)/args.tsc_ghz:,.0f} ns "
              f"(at {args.tsc_ghz} GHz nominal TSC)")

    # cycle histogram
    if hist:
        print()
        print(f"per-candidate cycle histogram (log2 of points+IP cycles):")
        tot = sum(hist.values())
        for b in sorted(hist):
            lo = 1 << (b - 1) if b else 0
            bar = "#" * int(60.0 * hist[b] / max(hist.values()))
            print(f"  2^{b:<2} (>={lo:>10,}): {100.0*hist[b]/tot:6.2f}%  {bar}")
    print()


if __name__ == "__main__":
    main()
