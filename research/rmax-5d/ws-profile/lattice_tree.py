#!/usr/bin/env python3
from collections import Counter, deque
from concurrent.futures import ThreadPoolExecutor, as_completed
import json
from math import gcd, prod
from pathlib import Path
import subprocess
import time

import numpy as np
from sympy import Matrix
from sympy.matrices.normalforms import hermite_normal_form

from searches import load_points

OUT = Path(__file__).resolve().parent


def hnf_count(n, dimension=5):
    if dimension == 1:
        return 1
    return sum(k ** (dimension - 1) * hnf_count(n // k, dimension - 1)
               for k in range(1, n + 1) if n % k == 0)


def smooth(n):
    for p in (2, 3):
        while n % p == 0:
            n //= p
    return n == 1


def one(sample):
    ident = sample["id"]
    weights = sample["weights"]
    cap = min(36, gcd(*weights) * sum(weights) ** 4 * max(weights) // prod(weights))
    blind = sum(hnf_count(n) for n in range(1, cap + 1) if smooth(n))
    queue = deque([(np.eye(5, dtype=np.int64), load_points(ident), 1)])
    seen = {tuple(np.eye(5, dtype=np.int64).ravel())}
    stats = Counter()
    start = time.perf_counter()
    status = "complete"
    path = OUT / f"tree-current-{ident}.bin"
    while queue:
        if time.perf_counter() - start > 15 or stats["ip_parents_expanded"] >= 100:
            status = "censored"
            break
        basis, points, index = queue.popleft()
        if index * 2 > cap:
            continue
        stats["ip_parents_expanded"] += 1
        with path.open("wb") as stream:
            np.array([len(points), 5], dtype="<i8").tofile(stream)
            points.astype("<i8", copy=False).tofile(stream)
        p = subprocess.run([str(OUT / "lattices"), str(path)],
                           capture_output=True, text=True, timeout=5)
        if p.returncode or p.stderr:
            raise RuntimeError(f"Lattice kernel failed: {p.stderr} {p.stdout[-1000:]}")
        data = json.loads(p.stdout)
        stats["kernel_ns"] += data["kernel_ns"]
        for state in data["states"]:
            prime, pivot, code = state["prime"], state["pivot"], state["code"]
            if index * prime > cap:
                continue
            h = np.eye(5, dtype=np.int64)
            h[pivot, pivot] = prime
            for j in range(pivot + 1, 5):
                h[pivot, j] = code // prime ** (4 - j) % prime
            child_basis = basis @ h
            key = tuple(map(int, hermite_normal_form(Matrix(child_basis)).flat()))
            if key in seen:
                stats["duplicate_lattices"] += 1
                continue
            seen.add(key)
            stats["unique_lattices_tested"] += 1
            if state["cheap_non_ip"]:
                stats["cheap_non_ip"] += 1
            else:
                stats["hull_calls"] += 1
            if not state["ip"]:
                stats["non_ip"] += 1
                continue
            stats["ip_children"] += 1
            stats["reflexive_children"] += state["reflexive"]
            value = points[:, pivot] - points[:, pivot + 1:] @ h[pivot, pivot + 1:]
            mask = value % prime == 0
            child = points[mask].copy()
            child[:, pivot] = value[mask] // prime
            assert len(child) == state["points"]
            queue.append((child_basis, child, index * prime))
    path.unlink(missing_ok=True)
    return {"id": ident, "status": status, "cap": cap, "primes": [2, 3],
            "blind_smooth_lattices": blind, "stats": dict(stats),
            "pending_ip_parents": len(queue), "seconds": time.perf_counter() - start,
            "note": "All 2,3-smooth indices through cap only; other primes are not covered. "
                    "Kernel also evaluates some redundant/over-cap candidates; unique counts "
                    "describe a scheduler that deduplicates before hull work."}


def main():
    samples = {s["id"]: s for s in json.loads((OUT / "sample.json").read_text())["records"]}
    selected = [10, 1, 0, 3, 33, 32, 103, 87, 141, 76, 77, 78]
    with (OUT / "lattice-tree.jsonl").open("w") as stream, ThreadPoolExecutor(max_workers=2) as pool:
        for f in as_completed([pool.submit(one, samples[i]) for i in selected]):
            result = f.result()
            stream.write(json.dumps(result) + "\n")
            stream.flush()
            print("TREE", result["id"], result["status"], result["stats"], flush=True)


if __name__ == "__main__":
    main()
