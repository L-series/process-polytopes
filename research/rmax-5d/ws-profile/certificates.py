#!/usr/bin/env python3
from itertools import permutations, product
import json
from pathlib import Path
import time

import numpy as np

OUT = Path(__file__).resolve().parent


def main():
    samples = json.loads((OUT / "sample.json").read_text())["records"]
    native = {r["id"]: r for r in map(json.loads, (OUT / "native.jsonl").read_text().splitlines())}
    maps = []
    for perm in permutations(range(5)):
        for signs in product([-1, 1], repeat=5):
            a = np.zeros((5, 5), dtype=np.int64)
            for i, j in enumerate(perm):
                a[i, j] = signs[i]
            maps.append(a)
    maps = np.array(maps)
    anchors = [87, 141, 32]
    results = []
    for sample in samples:
        if not any(label.startswith("random") for label in sample["labels"]):
            continue
        vertices = np.array(native[sample["id"]]["vertices"], dtype=np.int64)
        t = time.perf_counter()
        transformed = maps @ vertices.T
        found = None
        for target in anchors:
            facets = np.array(native[target]["facets"], dtype=np.int64)
            if 5 * int(np.abs(facets[:, :5]).max()) * int(np.abs(vertices).max()) >= 2**63:
                raise ValueError("int64 safety bound exceeded")
            pairings = facets[:, :5] @ transformed + facets[:, 5][None, :, None]
            valid = np.flatnonzero(np.all(pairings >= 0, axis=(1, 2)))
            if len(valid):
                witness = maps[int(valid[0])]
                for v in vertices:
                    image = [sum(int(a) * int(x) for a, x in zip(row, v)) for row in witness]
                    assert all(sum(a * x for a, x in zip(f[:5], image)) + f[5] >= 0
                               for f in native[target]["facets"])
                found = {"target": target, "matrix": witness.tolist()}
                break
        results.append({"id": sample["id"], "seconds": time.perf_counter() - t,
                        "witness": found, "status": "certified" if found else "inconclusive"})
    # Known-map verification is distinct from finding an embedding.
    verify = []
    for sample in samples[:154]:
        r = native[sample["id"]]
        vertices = np.array(r["vertices"], dtype=np.int64)
        facets = np.array(r["facets"], dtype=np.int64)
        t = time.perf_counter_ns()
        for _ in range(100):
            assert np.all(facets[:, :5] @ vertices.T + facets[:, 5, None] >= 0)
        verify.append({"id": sample["id"], "ns_per_verification":
                       (time.perf_counter_ns() - t) / 100})
    (OUT / "certificates.json").write_text(json.dumps({
        "anchors": anchors, "signed_permutations": len(maps), "results": results,
        "known_map_verification": verify,
        "note": "Search over signed permutations only, not all GL(5,Z). Failure is inconclusive. "
                "Root removal is safe only while retaining the anchor's complete lattice-descendant obligation."
    }, indent=2) + "\n")
    print("CERTIFIED", sum(r["witness"] is not None for r in results), "OF", len(results),
          "SEARCH_SECONDS", sum(r["seconds"] for r in results), flush=True)


if __name__ == "__main__":
    main()
