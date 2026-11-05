#!/usr/bin/env python3
"""Reproduce the historical 308 + 25 + 7 r-maximal 4D catalogue."""

import argparse
from collections import Counter
import hashlib
import json
from pathlib import Path
import subprocess
import time

from rmax_search import Palp, search


ROOT = Path(__file__).resolve().parents[1]

# KS, hep-th/0002240v1, Table 8, printed p. 18. Comparison data, not search inputs.
TABLE_8_DUAL_ROWS = (
    ((1, 0, -2, 0, 0, 2), (0, 1, -1, 0, 0, 0),
     (0, 0, 0, 1, 0, -1), (0, 0, 0, 0, 1, -1)),
    ((1, 0, 0, -2, 0, -3), (0, 1, 1, -3, 0, -4),
     (0, 0, 2, -3, 0, -4), (0, 0, 0, 0, 1, -1)),
    ((1, -1, 0, 0, -2, 0, 2), (0, 0, 1, 0, -1, 0, -1),
     (0, 0, 0, 1, -1, 0, 0), (0, 0, 0, 0, 0, 1, -1)),
    ((1, 0, -1, 0, -2, 0, -1), (0, 1, 1, 0, -2, 0, 2),
     (0, 0, 0, 1, -1, 0, 0), (0, 0, 0, 0, 0, 1, -1)),
    ((1, 0, -1, 0, -1, 0, -2), (0, 1, 1, 0, 2, 0, -4),
     (0, 0, 0, 1, -1, 0, 0), (0, 0, 0, 0, 0, 1, -1)),
    ((1, 1, -3, 0, -4, 0, 1), (0, 2, -2, 0, -3, 0, 3),
     (0, 0, 0, 1, -1, 0, 0), (0, 0, 0, 0, 0, 1, -1)),
    ((1, 1, -3, 0, -5, 0, 1), (0, 2, -2, 0, -3, 0, 3),
     (0, 0, 0, 1, -1, 0, 0), (0, 0, 0, 0, 0, 1, -1)),
)


def read_normal_forms(text):
    lines, i = text.splitlines(), 0
    while i < len(lines):
        if lines[i].startswith("np="):
            return
        d, count = map(int, lines[i].split())
        matrix = tuple(tuple(map(int, row.split())) for row in lines[i + 1:i + d + 1])
        if d != 4 or len(matrix) != d or any(len(row) != count for row in matrix):
            raise ValueError("Malformed PALP normal form")
        yield tuple(zip(*matrix))
        i += d + 1


def run(oracle, flags, text=""):
    return oracle.run(oracle.classifier, flags, text)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--work-dir", type=Path, required=True, help="New artifact directory")
    args = parser.parse_args()
    args.work_dir.mkdir(parents=True, exist_ok=False)
    work = args.work_dir.resolve()
    oracle = Palp(ROOT / "PALP/poly-4d.x", ROOT / "PALP/class-4d.x")
    started = time.monotonic()
    ws = ROOT / "docs/data/single-weight-systems/4d.txt"
    cws = ROOT / "tests/fixtures/cws/4d.cws"
    singles = [
        f"{sum(weights)} {' '.join(map(str, weights))}\n"
        for line in ws.read_text().splitlines()
        if (weights := tuple(map(int, line.split())))
    ]
    records = singles + cws.read_text().splitlines(keepends=True)
    if len(records) != 201346:
        raise ValueError(f"Expected 201346 WS/CWS, received {len(records)}")
    reduced = run(oracle, ["-f", "-mr"], "".join(records))
    (work / "rminimal.cws").write_text(reduced)
    if len(reduced.splitlines()) != 308:
        raise ValueError("The complete WS/CWS reduction did not yield 308 rows")
    reduction_seconds = time.monotonic() - started

    binary = work / "unchanged.bin"
    (work / "unchanged.log").write_text(run(
        oracle, ["-f", "-sv", "-mr", f"-po{binary}"], reduced
    ))
    exported = run(oracle, ["-b", "-mr", f"-pi{binary}"])
    (work / "unchanged.nf").write_text(exported)
    unchanged = {oracle.normal_form(p) for p in read_normal_forms(exported)}

    stats = Counter()
    bounded = list(search(reduced.splitlines(keepends=True), oracle, 3, stats))
    (work / "index3.jsonl").write_text("".join(json.dumps(p) + "\n" for p in bounded))
    originals = {
        tuple(map(tuple, p["vertices"])) for p in bounded if p["index"] == 1
    }
    bounded_forms = {tuple(map(tuple, p["vertices"])) for p in bounded}
    extra = bounded_forms - (originals | unchanged)
    expected_extra = set()
    for rows in TABLE_8_DUAL_ROWS:
        _, facets = oracle.hull(tuple(zip(*rows)))
        if not facets or any(f[-1] != 1 for f in facets):
            raise ValueError("Invalid Table 8 comparison polytope")
        expected_extra.add(oracle.normal_form(tuple(f[:-1] for f in facets)))
    catalogue = sorted(bounded_forms | unchanged)
    if len(originals) != 308 or len(originals | unchanged) != 333:
        raise ValueError("Unchanged-shape stage did not reproduce 308 + 25")
    if extra != expected_extra or len(catalogue) != 340:
        raise ValueError("Changed-hull search did not reproduce the seven Table 8 duals")
    for vertices in catalogue:
        _, facets = oracle.hull(vertices)
        if not facets or any(f[-1] != 1 for f in facets) or not oracle.maximal(vertices):
            raise ValueError("Output failed reflexivity or global r-maximality check")
    output = "".join(json.dumps({"vertices": vertices}) + "\n" for vertices in catalogue)
    (work / "maximal-340.jsonl").write_text(output)
    summary = {
        "dimension": 4,
        "input_records": len(records),
        "rminimal_cws": len(originals),
        "unchanged_shape_maxima": len(originals | unchanged),
        "changed_hull_maxima": len(extra),
        "maximal_classes": len(catalogue),
        "table_8_exact_match": True,
        "index3_search": dict(stats),
        "discovery_index_cap": 3,
        "exhaustive_lattice_search": False,
        "scope": "Historical 4D regression, not an exhaustive run of the general algorithm",
        "reduction_seconds": round(reduction_seconds, 3),
        "total_seconds": round(time.monotonic() - started, 3),
        "sha256": {
            str(ws.relative_to(ROOT)): hashlib.sha256(ws.read_bytes()).hexdigest(),
            str(cws.relative_to(ROOT)): hashlib.sha256(cws.read_bytes()).hexdigest(),
            "maximal-340.jsonl": hashlib.sha256(output.encode()).hexdigest(),
        },
        "palp_commit": subprocess.check_output(
            ["git", "-C", str(ROOT / "PALP"), "rev-parse", "HEAD"], text=True
        ).strip(),
    }
    (work / "summary.json").write_text(json.dumps(summary, indent=2, sort_keys=True) + "\n")
    print(json.dumps(summary, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
