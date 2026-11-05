#!/usr/bin/env python3
import hashlib
import json
from pathlib import Path

OUT = Path(__file__).resolve().parent
ROOT = Path("/home/ahat01/repos/process-polytopes")


def main():
    full = json.loads((OUT / "summary.json").read_text())
    report = json.loads((OUT / "concise.json").read_text())
    samples = {s["id"]: s for s in json.loads((OUT / "sample.json").read_text())["records"]}
    selected = [0, 1, 2, 3, 8, 10, 32, 33, 84, 87, 103, 122, 141]
    native = {s["id"]: s for s in map(json.loads, (OUT / "native.jsonl").read_text().splitlines())}
    determinant = {s["id"]: s for s in full["determinant_probes"]}
    report["hardware"] = {
        "cpu": "AMD EPYC 9554P 64-Core Processor", "logical_cpus_available": 64,
        "native_threads_per_process": 1, "maximum_workers_per_phase": 2,
        "note": "Several bounded phases ran concurrently; no cold-cache or isolated-host claim.",
    }
    report["provenance"] = json.loads((OUT / "provenance.json").read_text())
    report["verified_counts_method"] = "Sum of Parquet footer row counts; not a full-content or deduplication audit."
    report["selected_cases"] = []
    for ident in selected:
        sample = samples[ident]
        report["selected_cases"].append({
            "id": ident, **{k: sample[k] for k in [
                "kind", "weights", "point_count", "vertex_count", "facet_count", "labels"]},
            "dual_point_count": sample.get("dual_point_count"),
            "native_runs": native[ident]["runs"],
            "determinant_probe": determinant[ident],
            "cover_probes": [{k: v for k, v in r.items() if k != "leaf_examples"}
                             for group in full["recursive_probes"].values()
                             for r in group if r["id"] == ident],
        })
    report["lattice_tree"] = full["lattice_tree"]
    certificate = full["certificates"]
    values = [r["ns_per_verification"] for r in certificate["known_map_verification"]]
    report["containment_certificate_probe"] = {
        "anchors": certificate["anchors"], "maps_per_anchor": 3840,
        "random_inputs": len(certificate["results"]),
        "certified": sum(r["witness"] is not None for r in certificate["results"]),
        "search_seconds": sum(r["seconds"] for r in certificate["results"]),
        "verification_mean_ns": sum(values) / len(values),
        "note": certificate["note"],
    }
    report["reference_source_sha256"] = hashlib.sha256(
        (ROOT / "scripts/rmax_search.py").read_bytes()).hexdigest()
    report["validation"] = {
        "lattice_states_scalar_cross_checked": 1368,
        "vertex_facet_incidence_lists_checked": 538,
        "native_shared_and_cli_hulls_agree": 9,
        "synthetic_points": 14, "synthetic_reflexive_subsets": 5,
        "branch_policies_exhaustively_checked": ["lex", "distance", "partition"],
        "existing_regressions_passed": 10,
    }
    destination = ROOT / "docs/data/ws-rmax-profile-20261104.json"
    destination.write_text(json.dumps(report, indent=2, sort_keys=True) + "\n")
    print(destination, destination.stat().st_size, "bytes")


if __name__ == "__main__":
    main()
