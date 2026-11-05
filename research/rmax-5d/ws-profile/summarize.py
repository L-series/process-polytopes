#!/usr/bin/env python3
from collections import Counter
import hashlib
import json
from pathlib import Path
import statistics

import numpy as np

OUT = Path(__file__).resolve().parent
COUNTS = {"ip": 11469405637, "reflexive": 2377115412}


def lines(name):
    return [json.loads(line) for line in (OUT / name).read_text().splitlines()]


def describe(values):
    a = np.array(values, dtype=float)
    return {"n": len(a), "min": float(a.min()), "median": float(np.median(a)),
            "mean": float(a.mean()), "p95": float(np.quantile(a, .95)),
            "max": float(a.max())}


def mean_ci(values, ids, samples):
    clusters = {}
    for value, ident in zip(values, ids):
        source = samples[ident].get("cluster_source", ident)
        clusters.setdefault(source, []).append(value)
    means = np.array([statistics.mean(v) for v in clusters.values()])
    if len(means) != 64 or any(len(v) != 4 for v in clusters.values()):
        raise ValueError("Unexpected cluster structure")
    rng = np.random.default_rng(20261104)
    boots = means[rng.integers(0, len(means), size=(2000, len(means)))].mean(axis=1)
    return list(map(float, np.quantile(boots, [.025, .975])))


def main():
    sample_data = json.loads((OUT / "sample.json").read_text())
    samples = {r["id"]: r for r in sample_data["records"]}
    native = {r["id"]: r for r in lines("native.jsonl")}
    lattices = {r["id"]: r for r in lines("lattices.jsonl")}
    summary = {
        "date": "2026-11-04", "counts": COUNTS, "ws_total": sum(COUNTS.values()),
        "inventory": json.loads((OUT / "inventory.json").read_text()),
        "sample_records": len(samples), "sampling": sample_data["sampling_note"],
        "native_statuses": dict(Counter(r["status"] for r in native.values())),
        "lattice_statuses": dict(Counter(r["status"] for r in lattices.values())),
        "prime_states_measured": sum(len(r["states"]) for r in lattices.values()),
        "timing_unit": "seconds", "warmup": "Native repetition zero excluded; medians of next three.",
        "limitations": [
            "Metadata counts are not independent validation of every record or deduplication.",
            "Only representative WS records and bounded search obligations were tested.",
            "Elapsed native kernel time is used as single-worker cost, not a hardware CPU counter.",
            "Bootstrap intervals account for 64 sampled clusters, not unseen extreme tails.",
            "No complete lattice enumeration or complete 5D classification was performed.",
            "CWS timing is not inferred from WS timing.",
            "Global maximality probes use reflexive WS roots, not post-pruning leaf candidates.",
        ],
    }
    random_stats = {}
    g_base, residual = sum(COUNTS.values()), COUNTS["ip"]
    root_seconds = lattice_seconds = 0.0
    for kind in COUNTS:
        ids = [i for i, s in samples.items() if s["kind"] == kind and
               any(label.startswith("random") for label in s["labels"])]
        result = {"random_records": len(ids)}
        for field in ["point_ns", "hull_ns", "vertex_hull_ns", "nf_ns", "dual_ns"]:
            values = [statistics.median(r[field] for r in native[i]["runs"][1:]) / 1e9 for i in ids]
            result[field.removesuffix("_ns") + "_seconds"] = {
                **describe(values), "cluster_bootstrap_mean_95": mean_ci(values, ids, samples)}
        root = [sum(statistics.median(r[f] for r in native[i]["runs"][1:])
                    for f in ["point_ns", "hull_ns"]) / 1e9 for i in ids]
        result["root_reconstruction_seconds"] = {
            **describe(root), "cluster_bootstrap_mean_95": mean_ci(root, ids, samples)}
        result["lattice_kernel_seconds"] = describe([lattices[i]["kernel_ns"] / 1e9 for i in ids])
        states = [state for i in ids for state in lattices[i]["states"]]
        count = {
            "states": len(states),
            "cheap_non_ip": sum(x["cheap_non_ip"] for x in states),
            "hull_calls": sum(not x["cheap_non_ip"] for x in states),
            "ip": sum(x["ip"] for x in states),
            "reflexive": sum(x["reflexive"] for x in states),
            "ip_nonreflexive": sum(x["ip"] and not x["reflexive"] for x in states),
        }
        result["lattice_counts"] = count
        result["lattice_counts_per_root"] = {k: v / len(ids) for k, v in count.items()}
        g_base += COUNTS[kind] * count["hull_calls"] / len(ids)
        residual += COUNTS[kind] * count["ip_nonreflexive"] / len(ids)
        root_seconds += COUNTS[kind] * statistics.mean(root)
        lattice_seconds += COUNTS[kind] * result["lattice_kernel_seconds"]["mean"]
        random_stats[kind] = result
    summary["random_statistics"] = random_stats
    summary["conditional_scaling_original_and_indices_2_3_only"] = {
        "baseline_geometry_calls": g_base,
        "nonreflexive_starting_states": residual,
        "root_single_worker_days": root_seconds / 86400,
        "lattice_single_worker_years": lattice_seconds / 31557600,
        "extra_nodes_scenarios": {str(b): g_base + residual * b for b in [10, 100, 1000, 25000]},
        "prune_fraction_for_100B_with_b1000":
            1 - (100e9 - sum(COUNTS.values())) /
            (g_base - sum(COUNTS.values()) + residual * 1000),
        "note": "No root reduction or cross-record caching. Scenario b is additional nodes per "
                "nonreflexive start, not an observed completed-search mean. Other indices, "
                "CWS and global maximality costs excluded.",
    }
    summary["determinant_probes"] = lines("determinants.jsonl")
    summary["recursive_probes"] = {
        "reference": lines("cover.jsonl"),
        "native_overlapping": lines("native-cover.jsonl"),
        "native_partition": lines("partition-cover.jsonl"),
    }
    random_cover = lines("random-cover.jsonl")
    summary["random_recursive_probes"] = {
        "n": len(random_cover), "statuses": dict(Counter(r["status"] for r in random_cover)),
        "observed_nodes": describe([r["stats"]["nodes"] for r in random_cover]),
        "first_leaf_found": sum(r["first_leaf_seconds"] is not None for r in random_cover),
        "results": random_cover,
        "note": "Censored node counts are lower bounds, never completed-search estimates.",
    }
    normal = {r["id"]: r for r in lines("maximality-stack256.jsonl")}
    wide = {r["id"]: r for r in lines("maximality-wide128.jsonl")}
    merged = {}
    for ident, r in normal.items():
        merged[ident] = r if r["status"] == "complete" else wide[ident]
    completed = [r for r in merged.values() if r["status"] == "complete"]
    summary["maximality"] = {
        "default_8MiB_stack": dict(Counter(r["status"] for r in lines("maximality-default-stack.jsonl"))),
        "stack_256MiB_VERT64": dict(Counter(r["status"] for r in normal.values())),
        "failed_cases_stack_1GiB_VERT128": dict(Counter(r["status"] for r in wide.values())),
        "combined_statuses": dict(Counter(r["status"] for r in merged.values())),
        "completed_seconds": describe([r["seconds"] for r in completed]),
        "note": "Completed-only times are selection-biased. All unresolved final cases censored "
                "at five seconds. Function counters are from separate instrumented repeats.",
        "results": list(merged.values()),
    }
    summary["lattice_tree"] = lines("lattice-tree.jsonl")
    summary["certificates"] = json.loads((OUT / "certificates.json").read_text())
    summary["validation"] = {
        "coordinates_and_incidence": json.loads((OUT / "validation.json").read_text()),
        "branching": json.loads((OUT / "branch-validation.json").read_text()),
        "wrapper_agreement_and_timing": json.loads((OUT / "wrapper-timing.json").read_text()),
    }
    summary["artifact_sha256"] = {
        path.name: hashlib.sha256(path.read_bytes()).hexdigest()
        for path in OUT.iterdir()
        if path.is_file() and path.suffix in {".py", ".c", ".jsonl"} and path.name != "summary.json"
    }
    (OUT / "summary.json").write_text(json.dumps(summary, indent=2, sort_keys=True) + "\n")
    concise = {k: summary[k] for k in [
        "date", "counts", "ws_total", "sample_records", "prime_states_measured",
        "native_statuses", "lattice_statuses", "random_statistics",
        "conditional_scaling_original_and_indices_2_3_only", "limitations"]}
    concise["random_recursive"] = {k: v for k, v in summary["random_recursive_probes"].items()
                                    if k != "results"}
    concise["maximality"] = {k: v for k, v in summary["maximality"].items() if k != "results"}
    (OUT / "concise.json").write_text(json.dumps(concise, indent=2, sort_keys=True) + "\n")
    print(json.dumps(concise, indent=2))


if __name__ == "__main__":
    main()
