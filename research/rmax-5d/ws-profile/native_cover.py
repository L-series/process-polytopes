#!/usr/bin/env python3
from collections import Counter
from concurrent.futures import ThreadPoolExecutor, as_completed
import ctypes
import json
import os
from pathlib import Path
import subprocess
import sys
import time

import numpy as np

from searches import load_points, rmax

OUT = Path(__file__).resolve().parent


class Hull:
    def __init__(self):
        self.lib = ctypes.CDLL(str(OUT / "hull.so"))
        pointer = ctypes.POINTER(ctypes.c_long)
        self.lib.ws_hull.argtypes = [pointer, ctypes.c_int, pointer,
                                    ctypes.POINTER(ctypes.c_int), pointer,
                                    ctypes.POINTER(ctypes.c_int)]
        self.lib.ws_hull.restype = ctypes.c_int
        self.vertices = np.empty((64, 5), dtype=np.int64)
        self.facets = np.empty((1280, 6), dtype=np.int64)

    def __call__(self, points):
        points = np.ascontiguousarray(points, dtype=np.int64)
        nv, nf = ctypes.c_int(), ctypes.c_int()
        pointer = ctypes.POINTER(ctypes.c_long)
        status = self.lib.ws_hull(
            points.ctypes.data_as(pointer), len(points),
            self.vertices.ctypes.data_as(pointer), ctypes.byref(nv),
            self.facets.ctypes.data_as(pointer), ctypes.byref(nf))
        if status < 0:
            raise RuntimeError(f"Native hull status {status}")
        if not status:
            return (), ()
        return (tuple(sorted(tuple(map(int, row)) for row in self.vertices[:nv.value])),
                tuple(sorted(tuple(map(int, row)) for row in self.facets[:nf.value])))


def worker(ident, policy):
    points = load_points(ident)
    positions = {tuple(map(int, row)): i for i, row in enumerate(points)}
    hull = Hull()
    pending, visited = [(frozenset(), frozenset(), 0)], set()
    stats = Counter()
    start = time.perf_counter()
    first_leaf, distinct, leaves, geometry_seconds = None, set(), [], 0.0
    seconds = float(os.environ.get("WS_COVER_SECONDS", "10"))
    node_cap = int(os.environ.get("WS_COVER_NODES", "25000"))
    status = "complete"
    while pending:
        if time.perf_counter() - start >= seconds or stats["nodes"] >= node_cap:
            status = "censored"
            break
        removed, kept, depth = pending.pop()
        key = removed, kept
        if key in visited:
            stats["duplicate_nodes"] += 1
            continue
        visited.add(key)
        current = points if not removed else np.delete(points, list(removed), axis=0)
        stats["nodes"] += 1
        stats["max_depth"] = max(stats["max_depth"], depth)
        t = time.perf_counter()
        vertices, facets = hull(current)
        geometry_seconds += time.perf_counter() - t
        if not facets:
            stats["non_ip"] += 1
            continue
        bads = [f for f in facets if f[-1] != 1]
        if not bads:
            stats["reflexive_leaves"] += 1
            if first_leaf is None:
                first_leaf = time.perf_counter() - start
            if vertices not in distinct:
                distinct.add(vertices)
                if len(leaves) < 3:
                    leaves.append(vertices)
            continue
        bad = max(bads, key=lambda f: (f[-1], f)) if policy == "distance" else bads[0]
        basis = []
        for v in vertices:
            if sum(a * x for a, x in zip(bad[:-1], v)) + bad[-1]:
                continue
            if rmax.independent_rows([*basis, v], len(basis) + 1):
                basis.append(v)
            if len(basis) == 5:
                break
        if len(basis) != 5:
            raise ValueError("Missing bad-facet frame")
        stats["bad_facet_nodes"] += 1
        if policy == "partition":
            prefix, children = set(kept), []
            for v in basis:
                i = positions[v]
                if i not in prefix:
                    children.append((removed | {i}, frozenset(prefix), depth + 1))
                prefix.add(i)
            if not children:
                stats["kept_obstructions"] += 1
            pending.extend(reversed(children))
        else:
            for v in reversed(basis):
                pending.append((removed | {positions[v]}, kept, depth + 1))
    print(json.dumps({"id": ident, "policy": policy, "status": status, "stats": dict(stats),
                      "pending": len(pending), "seconds": time.perf_counter() - start,
                      "geometry_seconds": geometry_seconds, "first_leaf_seconds": first_leaf,
                      "distinct_coordinate_leaves": len(distinct), "leaf_examples": leaves,
                      "budget_seconds": seconds, "budget_nodes": node_cap}))


def one(job):
    ident, policy = job
    start = time.perf_counter()
    try:
        p = subprocess.run([sys.executable, __file__, "worker", str(ident), policy],
                           capture_output=True, text=True, timeout=14)
    except subprocess.TimeoutExpired:
        return {"id": ident, "policy": policy, "status": "hard_timeout",
                "seconds": time.perf_counter() - start}
    if p.returncode or p.stderr:
        return {"id": ident, "policy": policy, "status": "error",
                "returncode": p.returncode, "stderr": p.stderr,
                "stdout": p.stdout[-2000:]}
    return json.loads(p.stdout)


def main(partition=False):
    cases = [json.loads(line)["id"] for line in (OUT / "cover.jsonl").read_text().splitlines()]
    samples = {s["id"]: s for s in json.loads((OUT / "sample.json").read_text())["records"]}
    if partition:
        jobs = [(i, "partition") for i in cases if samples[i]["kind"] == "ip"]
    else:
        jobs = [(i, "lex") for i in cases]
        jobs += [(i, "distance") for i in cases if samples[i]["kind"] == "ip"]
    start = time.perf_counter()
    filename = "partition-cover.jsonl" if partition else "native-cover.jsonl"
    with (OUT / filename).open("w") as stream, ThreadPoolExecutor(max_workers=2) as pool:
        for n, f in enumerate(as_completed([pool.submit(one, job) for job in jobs]), 1):
            result = f.result()
            stream.write(json.dumps(result) + "\n")
            stream.flush()
            if result["status"] != "complete" or n % 10 == 0:
                print("PROGRESS", n, len(jobs), result["id"], result["policy"],
                      result["status"], flush=True)
    print("FINISHED", len(jobs), "seconds", time.perf_counter() - start, flush=True)


if __name__ == "__main__":
    if len(sys.argv) > 1 and sys.argv[1] == "worker":
        worker(int(sys.argv[2]), sys.argv[3])
    else:
        main(len(sys.argv) > 1)
