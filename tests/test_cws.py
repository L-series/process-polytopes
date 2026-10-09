#!/usr/bin/env python3
"""Run CWS generation, overlap-type, and vertex normal-form regressions."""

import argparse
import json
import pathlib
import time
import unittest

from cws_support import (
    FIXTURES, arguments, compute_forms, configure_limits, cws_rows,
    normal_forms, run, type_counts,
)


class CWSRegression(unittest.TestCase):
    def check_case(self, case):
        start = time.monotonic()
        output = run(BIN_DIR / f"cws-{case['capacity']}d.x", arguments(case)).stdout
        actual = cws_rows(output, case["dimension"])
        self.assertEqual(len(actual), case["count"], "Accepted CWS count")
        self.assertEqual(type_counts(actual), case["types"], "Combination-type counts")
        expected = cws_rows((FIXTURES / (case["name"] + ".cws")).read_text(), case["dimension"])
        self.assertEqual(len(actual), len(expected), "CWS fixture count")
        for index, (got, want) in enumerate(zip(actual, expected)):
            self.assertEqual(got[0], want[0], f"CWS row {index + 1}")
        forms = compute_forms(BIN_DIR, case["capacity"], actual)
        expected_forms = normal_forms((FIXTURES / (case["name"] + ".nf")).read_text(), annotated=False)
        self.assertEqual(len(forms), case["count"], "One normal form per accepted CWS")
        self.assertEqual(len(expected_forms), case["count"], "Normal-form fixture count")
        for index, (got, want) in enumerate(zip(forms, expected_forms)):
            self.assertEqual(got[0], case["dimension"], f"Normal-form dimension at row {index + 1}")
            self.assertEqual(got, want, f"Normal form at row {index + 1}: {actual[index][0]}")
        self.assertEqual(len(set(forms)), case["distinct_normal_forms"])
        print(f"\n{case['name']}: {len(actual)} CWS, {len(set(forms))} distinct NFs, {time.monotonic() - start:.2f}s", flush=True)

    def check_malformed(self, case):
        data = (FIXTURES / "malformed" / case["file"]).read_text()
        for binary, args in [("poly-5d.x", ["-fN"]), ("cws-5d.x", ["-i", "-f"])]:
            with self.subTest(parser=binary):
                result = run(BIN_DIR / binary, args, data=data, check=False, timeout=10)
                self.assertNotIn("Normal form of vertices", result.stdout)
                self.assertNotIn(" M:", result.stdout)
                diagnostic = result.stdout + result.stderr
                self.assertIn(case["diagnostic"], diagnostic)
                if case["nonzero_exit"]:
                    self.assertNotEqual(result.returncode, 0)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--bin-dir", type=pathlib.Path, default=pathlib.Path(__file__).resolve().parents[1] / "PALP")
    parser.add_argument("--case", help="Run just the named generation case")
    options = parser.parse_args()
    BIN_DIR = options.bin_dir.resolve()
    configure_limits()
    manifest = json.loads((FIXTURES / "manifest.json").read_text())
    cases = [case for case in manifest["cases"] if not options.case or case["name"] == options.case]
    if not cases:
        parser.error("No matching cases")
    for case in cases:
        def test(self, case=case):
            self.check_case(case)
        setattr(CWSRegression, "test_" + case["name"].replace("-", "_"), test)
    if not options.case:
        for case in manifest["malformed"]:
            def test(self, case=case):
                self.check_malformed(case)
            setattr(CWSRegression, "test_malformed_" + pathlib.Path(case["file"]).stem.replace("-", "_"), test)
    unittest.main(argv=[__file__, "-v"])
