#!/usr/bin/env python3
from concurrent.futures import ThreadPoolExecutor, as_completed
import json
from pathlib import Path
import random
import time

import pyarrow as pa
import pyarrow.parquet as pq

import lattice_runs
import native_runs

OUT = Path(__file__).resolve().parent
pa.set_cpu_count(2)
pa.set_io_thread_count(2)


def main():
    data = json.loads((OUT / "sample.json").read_text())
    records = data["records"]
    original = [r for r in records if any(l.startswith("random-") for l in r["labels"])]
    seen = {(r["kind"], tuple(r["weights"])) for r in records}
    added = []
    rng = random.Random(20261105)
    for source in original:
        start = time.perf_counter()
        pf = pq.ParquetFile(source["path"])
        kind = source["kind"]
        prefix = "first_weight" if kind == "reflexive" else "weight"
        cols = [f"{prefix}{i}" for i in range(6)] + [
            "vertex_count", "facet_count", "point_count", "count"]
        if kind == "reflexive":
            cols.append("dual_point_count")
        table = pf.read_row_group(source["row_group"], columns=cols, use_threads=False)
        data["row_group_reads"].append({
            "kind": kind, "path": source["path"], "group": source["row_group"],
            "rows": len(table), "arrow_bytes": table.nbytes,
            "seconds": time.perf_counter() - start, "extension": True})
        for j in range(3):
            row = rng.randrange(len(table))
            values = table.slice(row, 1).to_pylist()[0]
            weights = [values.pop(f"{prefix}{i}") for i in range(6)]
            key = kind, tuple(weights)
            if key in seen:
                raise ValueError("Unexpected duplicate random draw")
            seen.add(key)
            r = {"id": len(records) + len(added), "kind": kind,
                 "labels": [f"random-extra-{source['id']}-{j}"],
                 "cluster_source": source["id"], "weights": weights,
                 "path": source["path"], "row_group": source["row_group"],
                 "row": row, **values}
            added.append(r)
    data["records"].extend(added)
    data["random_draws_per_kind"] = 256
    data["sampling_note"] = (
        "64 independent uniform record draws per dataset identify PPS row-group clusters; "
        "three additional uniform records per selected cluster give 256 random probes per "
        "dataset. Within-cluster observations are correlated. Extrema/bin probes are biased "
        "and must not enter population means. Seed 20261104, extension seed 20261105.")
    (OUT / "sample.json").write_text(json.dumps(data, indent=2, sort_keys=True) + "\n")
    print("ADDED", len(added), "TOTAL", len(data["records"]), flush=True)
    for name, fn, jobs in [
        ("native", native_runs.one, added),
        ("lattices", lattice_runs.one, [r["id"] for r in added]),
    ]:
        with (OUT / f"{name}.jsonl").open("a") as stream, ThreadPoolExecutor(max_workers=2) as pool:
            for f in as_completed([pool.submit(fn, job) for job in jobs]):
                result = f.result()
                if result["status"] != "complete":
                    raise ValueError(result)
                stream.write(json.dumps(result) + "\n")
                stream.flush()
        print("FINISHED", name, len(added), flush=True)


if __name__ == "__main__":
    main()
