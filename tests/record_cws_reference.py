#!/usr/bin/env python3
"""Explicitly record reference-oracle results and fixture provenance hashes.

Only the manifest's ``inputs``, per-case ``outputs`` and reference summary
fields, and the known-defect list, are written. Existing golden rows, normal
forms, counts and types are never touched. Review every difference before
committing; this recorder is not a test oracle.
"""

import argparse
import json

from cws_reference import compare_with_palp, sha256_file
from cws_support import FIXTURES, cws_rows
from test_cws_pipeline import KNOWN_MISSING, compute_references


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--accept-reference", action="store_true", required=True)
    parser.add_argument("--jobs", type=int, default=8)
    options = parser.parse_args()
    manifest_path = FIXTURES / "manifest.json"
    manifest = json.loads(manifest_path.read_text())
    manifest["inputs"] = {
        p.name: sha256_file(p) for p in sorted((FIXTURES / "inputs").iterdir())
    }
    references = compute_references(manifest["cases"], options.jobs)
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
    manifest_path.write_text(json.dumps(manifest, indent=2) + "\n")
    KNOWN_MISSING.write_text(
        "# Reference IP classes absent from pinned PALP output (known generator defects).\n"
        "# case<TAB>canonical CWS class; see tests/README.md.\n" + "".join(missing_lines)
    )


if __name__ == "__main__":
    main()
