#!/usr/bin/env python3
"""Explicitly replace golden CWS and poly -N fixtures with current PALP output."""

import argparse
import json
import pathlib
import subprocess

from cws_support import (
    FIXTURES, arguments, compute_forms, configure_limits, cws_rows,
    run, serialize_forms, type_counts,
)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--accept-current-palp", action="store_true", required=True)
    parser.add_argument("--bin-dir", type=pathlib.Path, default=pathlib.Path(__file__).resolve().parents[1] / "PALP")
    options = parser.parse_args()
    bin_dir = options.bin_dir.resolve()
    palp_root = pathlib.Path(__file__).resolve().parents[1] / "PALP"
    subprocess.run(["git", "-C", str(palp_root), "diff", "--exit-code", "HEAD", "--", "*.c", "*.h", "GNUmakefile", "Makefile"], check=True)
    configure_limits()
    manifest_path = FIXTURES / "manifest.json"
    manifest = json.loads(manifest_path.read_text())
    for case in manifest["cases"]:
        output = run(bin_dir / f"cws-{case['capacity']}d.x", arguments(case)).stdout
        rows = cws_rows(output, case["dimension"])
        if not rows:
            raise AssertionError(f"No CWS generated for {case['name']}")
        forms = compute_forms(bin_dir, case["capacity"], rows)
        if len(forms) != len(rows) or any(form[0] != case["dimension"] for form in forms):
            raise AssertionError(f"Missing or wrong-dimensional normal forms for {case['name']}")
        case["count"] = len(rows)
        case["types"] = type_counts(rows)
        case["distinct_normal_forms"] = len(set(forms))
        (FIXTURES / (case["name"] + ".cws")).write_text("".join(line + "\n" for line, _ in rows))
        (FIXTURES / (case["name"] + ".nf")).write_text(serialize_forms(forms))
        print(f"{case['name']}: {len(rows)} CWS, {len(set(forms))} distinct normal forms", flush=True)
    manifest["baseline_palp_commit"] = subprocess.check_output(["git", "-C", str(palp_root), "rev-parse", "HEAD"], text=True).strip()
    manifest_path.write_text(json.dumps(manifest, indent=2) + "\n")


if __name__ == "__main__":
    main()
