#!/usr/bin/env python3
"""Consolidate cuda_dim5_cws_scan profiling logs (written by
profile_gpu_pipeline.sh) into a throughput / stage / occupancy report.

Reads OUTDIR/{gen,ip_*}.log (binary stdout+stderr) and OUTDIR/{tag}.csv
(nvidia-smi utilization samples)."""
import sys
import os
import re
import glob


def kv_pairs(line):
    """parse '... key: value key2: value2 ...' (value = next token)."""
    toks = line.replace("(", " ").replace(")", " ").split()
    d = {}
    for i, t in enumerate(toks):
        if t.endswith(":") and i + 1 < len(toks):
            d[t[:-1]] = toks[i + 1]
    return d


def num(s):
    try:
        return float(s)
    except (TypeError, ValueError):
        return None


def read(path):
    try:
        with open(path) as f:
            return f.read().splitlines()
    except OSError:
        return []


def scan_line(lines):
    for ln in lines:
        m = re.match(r"^(\d+),(\d+),(\d+),(\d+),(\d+),([\d.]+),([\d.]+),([\d.]+)", ln)
        if m:
            g = m.groups()
            return {"prefix_candidates": int(g[4]), "seconds": float(g[5]),
                    "prefix_per_s": float(g[7])}
    return None


def occupancy(csv):
    rows = read(csv)
    sm, busy = [], []
    memu = []
    for r in rows:
        p = [x.strip() for x in r.split(",")]
        if len(p) < 5:
            continue
        u = num(p[0])
        if u is None:
            continue
        sm.append(u)
        memu.append(num(p[1]) or 0)
        if u > 1:
            busy.append(u)
    if not sm:
        return None
    mean_busy = sum(busy) / len(busy) if busy else 0
    return {"mean_busy_sm": mean_busy, "peak_sm": max(sm),
            "mean_mem_ctrl": (sum(memu) / len(memu)) if memu else 0,
            "n": len(sm), "n_busy": len(busy)}


def fmt(n):
    return f"{n:,.0f}" if n is not None else "?"


def main():
    outdir = sys.argv[1]

    # generation
    gen = scan_line(read(os.path.join(outdir, "gen.log")))
    print("=== GENERATION (count-only scan kernel) ===")
    if gen:
        print(f"  prefix_candidates/s : {fmt(gen['prefix_per_s'])}  "
              f"({fmt(gen['prefix_candidates'])} in {gen['seconds']:.4f}s)")
    print()

    print("=== IP FILTERING (generate -> point-enum -> IP check) ===")
    hdr = f"{'config':22} {'proc cand/s':>13} {'IP%':>6} {'pt-enum%':>8} {'ipchk%':>7} {'meanSM%':>8} {'blocks':>7} {'avgnp':>6} {'maxnp':>7}"
    print(hdr)
    print("-" * len(hdr))
    detail = []
    for log in sorted(glob.glob(os.path.join(outdir, "ip_*.log"))):
        tag = os.path.basename(log)[:-4]
        lines = read(log)
        gpu_ip = {}
        stage = {}
        subs = {}
        blocks = None
        vram = None
        for ln in lines:
            if "gpu_ip structure" in ln:
                gpu_ip = kv_pairs(ln)
            elif "ip_stage_profile structure" in ln:
                stage = kv_pairs(ln)
            elif "ip_substages" in ln:
                subs = kv_pairs(ln)
            elif ln.strip().startswith("blocks:"):
                blocks = ln.split(":")[1].strip()
            elif "vram_cap" in ln:
                vram = ln.strip()
        occ = occupancy(os.path.join(outdir, tag + ".csv"))
        proc_s = num(gpu_ip.get("candidates_per_second"))
        # point/ip % of "top" (point+ip) come parenthesized as 'X% top' -> next tok
        # kv_pairs captured point_cycles/ip_cycles raw counts; recompute %
        pc = num(stage.get("point_cycles"))
        ic = num(stage.get("ip_cycles"))
        pt_pct = ip_pct = None
        if pc is not None and ic is not None and (pc + ic) > 0:
            pt_pct = 100.0 * pc / (pc + ic)
            ip_pct = 100.0 * ic / (pc + ic)
        seconds = num(gpu_ip.get("seconds"))
        ip_share = None  # IP-filter share is the whole row (gen is separate)
        meansm = occ["mean_busy_sm"] if occ else None
        avgnp = num(stage.get("avg_points"))
        maxnp = num(stage.get("max_points"))
        print(f"{tag:22} {fmt(proc_s):>13} {'100':>6} "
              f"{(f'{pt_pct:.1f}' if pt_pct is not None else '?'):>8} "
              f"{(f'{ip_pct:.1f}' if ip_pct is not None else '?'):>7} "
              f"{(f'{meansm:.1f}' if meansm is not None else '?'):>8} "
              f"{(blocks or '?'):>7} "
              f"{(f'{avgnp:.1f}' if avgnp is not None else '?'):>6} "
              f"{fmt(maxnp):>7}")
        detail.append((tag, gpu_ip, stage, subs, blocks, vram, occ, seconds))

    # per-config detail
    for tag, gpu_ip, stage, subs, blocks, vram, occ, seconds in detail:
        print()
        print(f"--- {tag} ---")
        if vram:
            print(f"  {vram}")
        proc = num(gpu_ip.get("processed"))
        if proc:
            def pct(k):
                v = num(gpu_ip.get(k))
                return f"{100.0*v/proc:.2f}%" if v is not None else "?"
            print(f"  processed={fmt(proc)} ip={fmt(num(gpu_ip.get('ip')))}"
                  f" ({pct('ip')})  seconds={seconds}")
            print(f"  early reject: precheck={pct('precheck_fail')} "
                  f"point_fail={pct('point_fail')} simplex_fail(np<6)={pct('simplex_fail')} "
                  f"initial_inci={pct('initial_inci_fail')} "
                  f"vtx_overflow={pct('vertex_overflow')} ip_reject={pct('ip_reject')} "
                  f"point_overflow={pct('point_overflow')}")
        if stage:
            pc = num(stage.get("point_cycles")); ic = num(stage.get("ip_cycles"))
            tot = (pc or 0) + (ic or 0)
            if seconds and tot > 0:
                print(f"  stage time (of {seconds}s IP-filter): "
                      f"point-enum={seconds*pc/tot:.3f}s ({100*pc/tot:.1f}%)  "
                      f"ip-check={seconds*ic/tot:.3f}s ({100*ic/tot:.1f}%)")
            if subs:
                order = ["glz", "initial_inci", "search_bad_eq",
                         "search_new_vertex", "make_new_ceqs"]
                parts = []
                sub_tot = sum((num(subs.get(k)) or 0) for k in order)
                for k in order:
                    v = num(subs.get(k))
                    if v is not None and sub_tot > 0:
                        parts.append(f"{k}={100*v/sub_tot:.1f}%")
                if parts:
                    print(f"  IP substages: " + "  ".join(parts))
        if occ:
            print(f"  occupancy: mean_SM_busy={occ['mean_busy_sm']:.1f}% "
                  f"peak={occ['peak_sm']:.0f}% mem_ctrl={occ['mean_mem_ctrl']:.1f}% "
                  f"(busy samples {occ['n_busy']}/{occ['n']})")
    print()


if __name__ == "__main__":
    main()
