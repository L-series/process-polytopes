#!/usr/bin/env python3
from collections import Counter
from concurrent.futures import ThreadPoolExecutor, as_completed
import importlib.util
from itertools import combinations
import json
from math import comb, gcd, prod
import os
from pathlib import Path
import resource
import statistics
import subprocess
import sys
import time

import numpy as np

OUT = Path(__file__).resolve().parent
ROOT = Path("/home/ahat01/repos/process-polytopes")
sys.path.insert(0, str(ROOT / "scripts"))
import rmax_search as rmax


def load_points(ident, dual=False):
    a = np.fromfile(OUT / f"{ident}.{'dual' if dual else 'points'}.bin", dtype="<i8")
    n, d = map(int, a[:2])
    if d != 5 or len(a) != 2 + n * d:
        raise ValueError("Malformed native point dump")
    return a[2:].reshape(n, d)


class Budget(RuntimeError):
    pass


class Oracle(rmax.Palp):
    def __init__(self, seconds=10):
        super().__init__(ROOT / "PALP/poly-5d.x", ROOT / "PALP/class-5d.x")
        self.deadline = time.perf_counter() + seconds
        self.calls = 0
        self.oracle_seconds = 0.0

    def run(self, binary, flags, text):
        remaining = self.deadline - time.perf_counter()
        if remaining <= 0:
            raise Budget()
        start = time.perf_counter()
        self.calls += 1
        try:
            p = subprocess.run([binary, *flags], input=text, capture_output=True,
                               text=True, timeout=min(remaining, 3))
        except subprocess.TimeoutExpired as e:
            raise Budget() from e
        finally:
            self.oracle_seconds += time.perf_counter() - start
        if p.returncode or p.stderr:
            raise RuntimeError(f"Geometry failure: {p.returncode} {p.stderr} {p.stdout[-1000:]}")
        return p.stdout


def cover(sample, points=None, label="identity"):
    if points is None:
        points = load_points(sample["id"])
    points = tuple(sorted(tuple(map(int, row)) for row in points))
    original = {p: i for i, p in enumerate(points)}
    pending, visited = [(frozenset(), 0)], set()
    stats = Counter()
    oracle = Oracle(10)
    start = time.perf_counter()
    first_leaf, leaves, distinct = None, [], set()
    complete = True
    try:
        while pending:
            if time.perf_counter() >= oracle.deadline or stats["nodes"] >= 1000:
                raise Budget()
            removed, depth = pending.pop()
            if removed in visited:
                stats["duplicate_nodes"] += 1
                continue
            visited.add(removed)
            current = tuple(p for i, p in enumerate(points) if i not in removed)
            stats["nodes"] += 1
            stats["max_depth"] = max(stats["max_depth"], depth)
            vertices, facets = rmax.Palp.hull.__wrapped__(oracle, current)
            if not facets or any(f[-1] <= 0 for f in facets):
                stats["non_ip"] += 1
                continue
            bad = next((f for f in facets if f[-1] != 1), None)
            if bad is None:
                stats["reflexive_leaves"] += 1
                if first_leaf is None:
                    first_leaf = time.perf_counter() - start
                if vertices not in distinct:
                    distinct.add(vertices)
                    if len(leaves) < 3:
                        leaves.append(vertices)
                continue
            stats["bad_facet_nodes"] += 1
            basis = []
            for v in vertices:
                if sum(a * x for a, x in zip(bad[:-1], v)) + bad[-1]:
                    continue
                if rmax.independent_rows([*basis, v], len(basis) + 1):
                    basis.append(v)
                if len(basis) == 5:
                    break
            if len(basis) != 5:
                raise ValueError("Bad facet has no frame")
            for v in reversed(basis):
                pending.append((removed | {original[v]}, depth + 1))
    except Budget:
        complete = False
    return {"id": sample["id"], "label": label, "status": "complete" if complete else "censored",
            "stats": dict(stats), "pending": len(pending), "geometry_calls": oracle.calls,
            "oracle_seconds": oracle.oracle_seconds, "seconds": time.perf_counter() - start,
            "first_leaf_seconds": first_leaf, "distinct_coordinate_leaves": len(distinct),
            "leaf_examples": leaves}


def determinant_case(sample, native):
    vertices = native["vertices"]
    start = time.perf_counter()
    count, bound = 0, 0
    complete = True
    for frame in combinations(vertices, 5):
        bound = max(bound, abs(rmax.determinant(frame)))
        count += 1
        if count % 256 == 0 and time.perf_counter() - start >= 5:
            complete = False
            break
    w = sample["weights"]
    upper = gcd(*w) * sum(w) ** 4 * max(w) // prod(w)
    if bound > upper:
        raise ValueError("WS determinant bound violated")
    # Every index-n family includes n^4 HNFs; this is a rigorous lower bound.
    hnf_lower = bound * (bound + 1) * (2 * bound + 1) * (3 * bound**2 + 3 * bound - 1) // 30
    return {"id": sample["id"], "status": "complete" if complete else "censored",
            "determinants": count, "all_frames": comb(len(vertices), 5), "D_lower": bound,
            "D_exact": bound if complete else None, "D_ws_upper": upper,
            "hnf_count_lower": hnf_lower, "seconds": time.perf_counter() - start}


def maximal_case(ident, vertices):
    text = rmax.matrix_input(tuple(tuple(v) for v in vertices))
    start = time.perf_counter()
    try:
        p = subprocess.run([os.environ.get("WS_CLASS", str(ROOT / "PALP/class-5d.x")), "-f", "-mr"],
                           input=text, capture_output=True, text=True, timeout=5)
    except subprocess.TimeoutExpired:
        return {"id": ident, "status": "censored", "timeout_seconds": 5,
                "seconds": time.perf_counter() - start}
    elapsed = time.perf_counter() - start
    if p.returncode or p.stderr or p.stdout not in ("", "\n"):
        return {"id": ident, "status": "error", "seconds": elapsed,
                "returncode": p.returncode, "stderr": p.stderr,
                "stdout": p.stdout[-2000:]}
    result = {"id": ident, "status": "complete", "seconds": elapsed, "maximal": p.stdout == "\n"}
    if os.environ.get("WS_NO_COUNTER") == "1":
        result["counter_status"] = "not_requested_for_alternate_build"
        return result
    try:
        c = subprocess.run([str(OUT / "class-counted"), "-f", "-mr"], input=text,
                           capture_output=True, text=True, timeout=5)
    except subprocess.TimeoutExpired:
        result["counter_status"] = "censored"
        return result
    if c.returncode or c.stdout != p.stdout:
        result.update(counter_status="error", counter_returncode=c.returncode,
                      counter_stderr=c.stderr, counter_stdout=c.stdout[-2000:])
    else:
        result["counts"] = json.loads(c.stderr)
    return result


def main(mode):
    stack_mb = int(os.environ.get("WS_STACK_MB", "0"))
    if stack_mb:
        _, hard = resource.getrlimit(resource.RLIMIT_STACK)
        requested = stack_mb * 1024 * 1024
        if hard != resource.RLIM_INFINITY and requested > hard:
            raise RuntimeError("Requested stack exceeds hard limit")
        resource.setrlimit(resource.RLIMIT_STACK, (requested, hard))
    samples = json.loads((OUT / "sample.json").read_text())["records"]
    by_id = {s["id"]: s for s in samples}
    native = {r["id"]: r for r in map(json.loads, (OUT / "native.jsonl").read_text().splitlines())}
    if mode == "determinants":
        jobs = [(s, native[s["id"]]) for s in samples]
        fn = lambda pair: determinant_case(*pair)
    elif mode == "maximality":
        selected = None
        if os.environ.get("WS_FAILED_ONLY") == "1":
            previous = [json.loads(line) for line in
                        (OUT / "maximality-stack256.jsonl").read_text().splitlines()]
            selected = {r["id"] for r in previous if r["status"] != "complete"} | {87, 141}
        jobs = [(s["id"], native[s["id"]]["vertices"]) for s in samples
                if s["kind"] == "reflexive" and s["id"] < 154
                and (selected is None or s["id"] in selected)]
        fn = lambda pair: maximal_case(*pair)
    elif mode == "cover":
        chosen = []
        for kind in ["ip", "reflexive"]:
            group = [s for s in samples if s["kind"] == kind]
            chosen += [s for s in group if any(not l.startswith("random") for l in s["labels"])]
            chosen += [s for s in group if any(l.startswith("random") for l in s["labels"])][:8]
        jobs = list({s["id"]: s for s in chosen}.values())
        fn = cover
    else:
        raise ValueError(mode)
    start = time.perf_counter()
    filename = f"{mode}-stack{stack_mb}.jsonl" if stack_mb else f"{mode}.jsonl"
    filename = os.environ.get("WS_RUN_LABEL", filename)
    with (OUT / filename).open("w") as stream, ThreadPoolExecutor(max_workers=2) as pool:
        futures = [pool.submit(fn, job) for job in jobs]
        for i, f in enumerate(as_completed(futures), 1):
            result = f.result()
            stream.write(json.dumps(result) + "\n")
            stream.flush()
            if result["status"] != "complete" or i % 10 == 0:
                print(mode, i, len(jobs), result["id"], result["status"], flush=True)
    print("FINISHED", mode, len(jobs), "wall_seconds", time.perf_counter() - start, flush=True)


if __name__ == "__main__":
    main(sys.argv[1])
