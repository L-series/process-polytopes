#!/usr/bin/env python3
"""Explicitly record reference-oracle results and fixture provenance hashes.

Only the manifest's ``inputs``, per-case ``outputs`` and reference summary
fields, descriptor-case summaries and repeated-row counts, and the known-defect
list are written. Existing golden rows, normal forms, counts, types and the
reviewed descriptor table are never touched. Review every difference before
committing; this recorder is not a test oracle.
"""

import argparse
import json
import pathlib
import subprocess
import tempfile

import test_cws_pipeline as gates
from cws_reference import compare_with_palp, sha256_file
from cws_support import FIXTURES, configure_limits, cws_rows


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--accept-reference", action="store_true", required=True)
    parser.add_argument("--bin-dir", type=pathlib.Path, default=pathlib.Path(__file__).resolve().parents[1] / "PALP")
    parser.add_argument("--jobs", type=int, default=8)
    options = parser.parse_args()
    gates.BIN_DIR = options.bin_dir.resolve()
    subprocess.run(["git", "-C", str(gates.BIN_DIR), "diff", "--exit-code", "HEAD", "--", "*.c", "*.h", "*.inc"], check=True)
    configure_limits()
    manifest_path = FIXTURES / "manifest.json"
    manifest = json.loads(manifest_path.read_text())
    manifest["inputs"] = {
        p.name: sha256_file(p) for p in sorted((FIXTURES / "inputs").iterdir())
    }
    descriptor_cases = [gates.descriptor_case(e) for e in manifest["descriptor_cases"]]
    references = gates.compute_references(manifest["cases"] + descriptor_cases, options.jobs)
    missing_lines = []
    for case in manifest["cases"]:
        reference = references[case["name"]]
        if reference.failures:
            raise SystemExit(f"{case['name']}: oracle failures {reference.failures[:3]}")
        case["outputs"] = {
            kind: sha256_file(FIXTURES / f"{case['name']}.{kind}") for kind in ("cws", "nf")
        }
        rows = cws_rows((FIXTURES / f"{case['name']}.cws").read_text(), case["dimension"])
        result = compare_with_palp(reference, rows)
        if result["unexpected"] or result["rejected_by_oracle"] or result["undecided"]:
            raise SystemExit(f"{case['name']}: PALP rows disagree with the oracle")
        case["reference"].update(reference.summary())
        case["reference"]["palp_repeated_rows"] = result["repeated"]
        missing_lines += [f"{case['name']}\t{key}\n" for key in result["missing"]]
    with tempfile.TemporaryDirectory() as workspace:
        for entry, case in zip(manifest["descriptor_cases"], descriptor_cases):
            reference = references[case["name"]]
            if reference.failures:
                raise SystemExit(f"{case['name']}: oracle failures {reference.failures[:3]}")
            for mode in entry["palp_repeated_rows"]:
                rows = gates.descriptor_rows(case, mode, pathlib.Path(workspace))
                result = compare_with_palp(reference, rows)
                if any(result[k] for k in ("missing", "unexpected", "rejected_by_oracle", "undecided")):
                    raise SystemExit(f"{case['name']} {mode}: PALP disagrees with the oracle")
                entry["palp_repeated_rows"][mode] = result["repeated"]
            entry["reference"] = reference.summary()
    manifest_path.write_text(json.dumps(manifest, indent=2) + "\n")
    gates.KNOWN_MISSING.write_text(
        "# Reference IP classes absent from pinned PALP output (known generator defects).\n"
        "# case<TAB>canonical CWS class; see tests/README.md.\n" + "".join(missing_lines)
    )


if __name__ == "__main__":
    main()
