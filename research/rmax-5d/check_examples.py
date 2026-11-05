#!/usr/bin/env python3
"""Check the fixed-lattice 5D examples, without enumerating their image lattices."""

import argparse
from itertools import product
import json
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "scripts"))
import rmax_search as rmax


def axis(i, sign=1):
    return tuple(sign if j == i else 0 for j in range(5))


def ip_minimal(points, vertices, oracle):
    for vertex in vertices:
        _, facets = oracle.hull(tuple(p for p in points if p != vertex))
        if facets and all(f[-1] > 0 for f in facets):
            return False
    return True


def inspect(name, vertices, oracle):
    points = oracle.points(rmax.matrix_input(vertices))
    hull, facets = oracle.hull(points)
    if not facets:
        raise ValueError(f"{name}: not full dimensional")
    reflexive = all(f[-1] == 1 for f in facets)
    return {
        "name": name, "vertices": hull, "facets": facets,
        "point_count": len(points), "reflexive": reflexive,
        "ip": all(f[-1] > 0 for f in facets),
        "ip_minimal": ip_minimal(points, hull, oracle),
        "r_maximal": oracle.maximal(hull) if reflexive else None,
    }


def examples(oracle):
    a = tuple(axis(i, sign) for i in range(5) for sign in (-1, 1))
    b = (axis(0), axis(1), (-1, -1, 0, 0, 0),
         *(axis(i, sign) for i in range(2, 5) for sign in (-1, 1)))
    cube = tuple(product((-1, 1), repeat=5))
    triangle = ((-1, -1), (-1, 2), (2, -1))
    other_maximum = tuple((*x, *z) for x in triangle for z in product((-1, 1), repeat=3))
    records = [inspect(name, vertices, oracle) for name, vertices in [
        ("A: r-minimal cross-polytope", a),
        ("B: another r-minimum inside A-dual", b),
        ("A-dual: maximal cube", cube),
        ("B-dual: maximal triangle times cube", other_maximum),
    ]]
    if [r["point_count"] for r in records] != [11, 10, 243, 270]:
        raise ValueError("Minimal-first counterexample point counts disagree")
    if not all(r["reflexive"] for r in records):
        raise ValueError("Counterexample is not reflexive")
    if not all(r["ip_minimal"] for r in records[:2]):
        raise ValueError("Expected IP-minimal counterexample")
    if not all(r["r_maximal"] for r in records[2:]):
        raise ValueError("Expected globally maximal duals")
    if not all(all(abs(x) <= 1 for x in v) for v in b):
        raise ValueError("B is not contained in the cube")

    bottom = [(-1, -1, -1, -1, -1)]
    for i in range(4):
        v = [-1] * 5
        v[i] = 6
        bottom.append(tuple(v))
    q_top = [(-1, -1, -1, -1, 2)]
    p_top = [(-1, -1, -1, -1, 1)]
    for i in range(4):
        q, p = list(q_top[0]), list(p_top[0])
        q[i], p[i] = 0, 2
        q_top.append(tuple(q))
        p_top.append(tuple(p))
    seed = (*tuple(axis(i) for i in range(5)), (-1, -1, -1, -1, -2))
    obstruction = [
        inspect("Q: degree-seven nonreflexive WS container", tuple(bottom + q_top), oracle),
        inspect("P: reflexive maximal descendant", tuple(bottom + p_top), oracle),
        inspect("S: integer polar hull of Q", seed, oracle),
    ]
    q, p, s = obstruction
    if q["point_count"] != 496 or q["reflexive"] or not q["ip"]:
        raise ValueError("Expected 496-point nonreflexive container")
    if p["point_count"] != 491 or not p["r_maximal"]:
        raise ValueError("Expected 491-point maximal descendant")
    if s["reflexive"] or not s["ip_minimal"] or s["point_count"] != 7:
        raise ValueError("Expected nonreflexive IP-minimal integer polar hull")
    return {
        "polarity_convention": "<y,x> >= -1; standard dual bases identify both lattices with Z^5",
        "minimal_first_pruning_counterexample": records,
        "nonreflexive_container_obstruction": obstruction,
        "qualification": "IP-minimality plus reflexivity implies r-minimality. Nonreflexive S has no reflexive subpolytope; the proof that it is Q's integer polar hull is in the research note.",
    }


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--poly", type=Path, default=ROOT / "PALP/poly-5d.x")
    parser.add_argument("--class", dest="classifier", type=Path, default=ROOT / "PALP/class-5d.x")
    parser.add_argument("--output", type=Path)
    args = parser.parse_args()
    result = examples(rmax.Palp(args.poly, args.classifier))
    text = json.dumps(result, indent=2, sort_keys=True) + "\n"
    if args.output:
        args.output.write_text(text)
    else:
        print(text, end="")


if __name__ == "__main__":
    main()
