#!/usr/bin/env python3
"""Recompute historical planning scenarios; these are not complete-search forecasts."""

import argparse
from functools import lru_cache
import json
from math import comb, isqrt
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
CPU_YEAR_SECONDS = 365.25 * 86400


def divisors(n):
    values = []
    for k in range(1, isqrt(n) + 1):
        if n % k == 0:
            values.append(k)
            if k * k != n:
                values.append(n // k)
    return values


@lru_cache(maxsize=None)
def exact_index_hnfs(dimension, index):
    if dimension == 1:
        return 1
    return sum(k ** (dimension - 1) * exact_index_hnfs(dimension - 1, index // k)
               for k in divisors(index))


def cumulative_hnfs(dimension, cap):
    return sum(exact_index_hnfs(dimension, n) for n in range(1, cap + 1))


def estimates():
    n, ell = 20_000_000_000, 153
    scenarios = []
    for name, surviving, nonreflexive, descendants in [
        ("exceptional reduction", .0001, .10, 100),
        ("hypothetically 4D-like reduction", .0015, .25, 1000),
        ("one percent retained, harder residual", .01, .25, 10000),
        ("no root reduction", 1, .25, 1000),
    ]:
        calls = n + n * surviving * ((ell - 1) + ell * nonreflexive * descendants)
        scenarios.append({
            "name": name, "s": surviving, "ell": ell, "f": nonreflexive,
            "b": descendants, "main_geometry_calls": round(calls),
        })
    lattice_counts = [
        {"cap": k, "exact_index": exact_index_hnfs(5, k),
         "cumulative": cumulative_hnfs(5, k),
         "candidate_states_for_20B": n * cumulative_hnfs(5, k)}
        for k in [1, 2, 3, 4, 5, 10, 100]
    ]
    h2401 = cumulative_hnfs(5, 2401)
    measured = json.loads((ROOT / "docs/data/ws-rmax-profile-20261104.json").read_text())
    return {
        "historical_assumed_inputs": n,
        "historical_assumptions": [
            "One new root geometry decision per input, unless existing metadata is reused.",
            "s is the fraction retained by a lattice-complete root sieve.",
            "ell is an assumed effective count of geometry-triggering lattice states, not a complete index cutoff.",
            "f and b concern retained lattice states; neither was measured in the initial estimate.",
            "Normal forms and global maximality are additional costs.",
            "The measured 4D reduction fraction is not a proved 5D reduction rate.",
        ],
        "formula": "N + N*s*((ell-1) + ell*f*b)",
        "scenarios": scenarios,
        "hnf_counts": lattice_counts,
        "496_point_example": {
            "D": 2401, "cumulative_hnfs": h2401,
            "worker_years_at_one_million_states_per_second":
                h2401 / 1e6 / CPU_YEAR_SECONDS,
        },
        "index_at_most_3_rank_filter_sensitivity": [
            {"nonidentity_geometry_fraction": rho,
             "geometry_calls": round(n * (1 + 152 * rho))}
            for rho in [1, .1, .01, .001]
        ],
        "full_five_child_tree": [
            {"depth": depth, "nodes": (5 ** (depth + 1) - 1) // 4}
            for depth in [5, 10, 15, 20]
        ],
        "determinant_frame_counts": [
            {"vertices": v, "five_vertex_frames": comb(v, 5)}
            for v in [6, 10, 15, 20, 30, 50]
        ],
        "hard_tail_example": {
            "fraction": .0001, "roots": 2_000_000,
            "extra_calls_per_root": 1_000_000, "extra_calls": 2_000_000_000_000,
        },
        "hypothetical_timing": {
            "one_trillion_calls_at_10ms_worker_years": 1e12 * .01 / CPU_YEAR_SECONDS,
            "days_on_1000_cores_at_70_percent": 1e12 * .01 / (1000 * .7 * 86400),
        },
        "measured_ws_counts": measured["counts"],
        "measured_original_plus_index_2_3_scenarios":
            measured["conditional_scaling_original_and_indices_2_3_only"],
        "status": "Historical assumptions retained for comparison; measured WS profiling supersedes the initial point-tail and root-time assumptions.",
    }


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path)
    args = parser.parse_args()
    text = json.dumps(estimates(), indent=2, sort_keys=True) + "\n"
    if args.output:
        args.output.write_text(text)
    else:
        print(text, end="")


if __name__ == "__main__":
    main()
