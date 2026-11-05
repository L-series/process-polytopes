#!/usr/bin/env python3
import bisect
import json
import random
import time
from pathlib import Path

import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq

OUT = Path(__file__).resolve().parent
IP = Path("/work/edih210/ahat01/refpoly-runs/20260703-223852/final_v2/ip-polytopes-5d-valid-parquet-parts")
REF = Path("/work/edih210/data/unique_polytopes.parquet")
SEED = 20261104
pa.set_cpu_count(2)
pa.set_io_thread_count(2)


def save(name, value):
    (OUT / name).write_text(json.dumps(value, indent=2, sort_keys=True) + "\n")


def main():
    inventory, samples, reads = {}, [], []
    for kind, paths, expected in [
        ("ip", sorted(IP.glob("*.parquet")), 11469405637),
        ("reflexive", [REF], 2377115412),
    ]:
        groups, stops, extrema, files = [], [], {}, []
        total = 0
        start = time.perf_counter()
        for path in paths:
            pf = pq.ParquetFile(path)
            files.append({"path": str(path), "bytes": path.stat().st_size,
                          "rows": pf.metadata.num_rows, "groups": pf.num_row_groups})
            names = pf.schema_arrow.names
            for rg in range(pf.num_row_groups):
                group = pf.metadata.row_group(rg)
                groups.append((str(path), rg, group.num_rows))
                total += group.num_rows
                stops.append(total)
                for name in ["point_count", "vertex_count", "facet_count", "dual_point_count"]:
                    if name not in names:
                        continue
                    stats = group.column(names.index(name)).statistics
                    if not stats or not stats.has_min_max:
                        raise ValueError(f"Missing footer statistics: {path} {rg} {name}")
                    for direction in ["min", "max"]:
                        value = getattr(stats, direction)
                        key = f"{name}_{direction}"
                        old = extrema.get(key)
                        if old is None or (value < old["value"] if direction == "min"
                                           else value > old["value"]):
                            extrema[key] = {"value": value, "path": str(path), "group": rg}
        if total != expected:
            raise ValueError(f"{kind}: {total} != {expected}")
        inventory[kind] = {"rows": total, "expected": expected, "files": files,
                           "groups": len(groups), "extrema": extrema,
                           "footer_seconds": time.perf_counter() - start}
        requests = {}
        rng = random.Random(SEED + (kind == "reflexive"))
        for draw in range(64):
            position = rng.randrange(total)
            gi = bisect.bisect_right(stops, position)
            path, rg, _ = groups[gi]
            row = position - (stops[gi - 1] if gi else 0)
            requests.setdefault((path, rg), []).append((row, f"random-{draw:02d}"))
        for label, item in extrema.items():
            requests.setdefault((item["path"], item["group"]), []).append((None, label))
        have_bins = set()
        for (path, rg), targets in sorted(requests.items()):
            begin = time.perf_counter()
            pf = pq.ParquetFile(path)
            prefix = "first_weight" if kind == "reflexive" else "weight"
            columns = [f"{prefix}{i}" for i in range(6)]
            columns += ["vertex_count", "facet_count", "point_count", "count"]
            if kind == "reflexive":
                columns += ["dual_point_count"]
            table = pf.read_row_group(rg, columns=columns, use_threads=False)
            reads.append({"kind": kind, "path": path, "group": rg, "rows": len(table),
                          "arrow_bytes": table.nbytes, "seconds": time.perf_counter() - begin})
            selected = []
            for row, label in targets:
                if row is None:
                    name, direction = label.rsplit("_", 1)
                    values = table[name].to_numpy()
                    row = int(np.argmin(values) if direction == "min" else np.argmax(values))
                    if values[row] != extrema[label]["value"]:
                        raise ValueError("Footer extremum mismatch")
                selected.append((row, label))
            values = table["point_count"].to_numpy()
            for lo, hi in [(0, 32), (33, 100), (101, 300), (301, 1000),
                           (1001, 10000), (10001, 100000), (100001, 1000000)]:
                label = f"points-{lo}-{hi}"
                if label not in have_bins:
                    matches = np.flatnonzero((values >= lo) & (values <= hi))
                    if len(matches):
                        selected.append((int(matches[len(matches) // 2]), label))
                        have_bins.add(label)
            for row, label in selected:
                record = table.slice(row, 1).to_pylist()[0]
                weights = [record.pop(f"{prefix}{i}") for i in range(6)]
                samples.append({"kind": kind, "label": label, "path": path,
                                "row_group": rg, "row": row, "weights": weights, **record})
        print(kind, "count verified", total, "samples", sum(s["kind"] == kind for s in samples),
              flush=True)
    unique = {}
    for sample in samples:
        key = sample["kind"], tuple(sample["weights"])
        if key in unique:
            unique[key]["labels"].append(sample["label"])
        else:
            sample["labels"] = [sample.pop("label")]
            sample["id"] = len(unique)
            unique[key] = sample
    save("inventory.json", inventory)
    save("sample.json", {"seed": SEED, "random_draws_per_kind": 64,
                          "records": list(unique.values()), "row_group_reads": reads,
                          "sampling_note": "64 uniform record draws per dataset, plus deliberately biased "
                                           "extrema and point-bin probes; do not pool these for population means."})
    print("SAVED", len(unique), "unique records", "read_seconds",
          sum(r["seconds"] for r in reads), flush=True)


if __name__ == "__main__":
    main()
