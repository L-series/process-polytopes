#!/usr/bin/env python3
"""Reference and provenance gates for CWS generation and point enumeration.

Suites:
  generator  input/golden hashes, then live PALP rows against the independent
             reference enumeration (missing, unexpected, repeated rows) for the
             legacy cases and every dimension-five descriptor (V1)
  points     reference point-set and IP oracle self-tests, plus golden M:p counts
"""

import argparse
import collections
import concurrent.futures
import itertools
import json
import math
import os
import pathlib
import random
import shutil
import sys
import tempfile
import time
import unittest

from cws_reference import (
    OracleFailure, Reference, canonical, class_matrix, compare_with_palp, is_ip,
    lattice_points, read_pool, sha256_file, structure_matrix,
)
from cws_support import (
    FIXTURES, arguments, combination_type, configure_limits, cws_rows,
    read_descriptor_include, run,
)

MANIFEST = json.loads((FIXTURES / "manifest.json").read_text())
KNOWN_MISSING = FIXTURES / "known-palp-missing.txt"
LANDED_SUITES = ("generator", "points")
DESCRIPTORS = {d["id"]: d for d in MANIFEST["descriptors"]}


def descriptor_case(entry):
    """A reference case for one recorded descriptor run over bounded pools."""
    descriptor = DESCRIPTORS[entry["descriptor"]]
    pools = MANIFEST["descriptor_pools"][entry["pools"]]
    structure = {"pools": [pools[str(size)] for size in descriptor["sizes"]], "blocks": descriptor["blocks"]}
    return {**entry, "dimension": 5, "reference": {**entry.get("reference", {}), "structures": [structure]}}


def descriptor_rows(case, mode, workspace):
    """Run PALP on one descriptor: builtin pools, one shared file per size, or per-slot copies."""
    descriptor = DESCRIPTORS[case["descriptor"]]
    if mode == "builtin":
        args = ["-c5", f"-s{descriptor['id']}"]
    else:
        pools = MANIFEST["descriptor_pools"][case["pools"]]
        paths = []
        for slot, size in enumerate(descriptor["sizes"]):
            source = FIXTURES / "inputs" / pools[str(size)]
            if mode == "copies":
                target = workspace / f"slot{slot}-{source.name}"
                shutil.copyfile(source, target)
                source = target
            paths.append(str(source))
        args = ["-c5", f"-n{len(paths)}", *paths, f"-s{descriptor['id']}"]
    return cws_rows(run(BIN_DIR / "cws-5d.x", args).stdout, 5)


def known_missing():
    entries = collections.defaultdict(list)
    for line in KNOWN_MISSING.read_text().splitlines():
        if line and not line.startswith("#"):
            name, key = line.split("\t")
            entries[name].append(key)
    return {name: sorted(keys) for name, keys in entries.items()}


def compute_reference(case):
    start = time.monotonic()
    reference = Reference(case["reference"], FIXTURES / "inputs", case["dimension"])
    return case["name"], reference, time.monotonic() - start


def compute_references(cases, jobs):
    with concurrent.futures.ProcessPoolExecutor(max_workers=jobs) as pool:
        results = {}
        for name, reference, seconds in pool.map(compute_reference, cases):
            print(f"\nreference {name}: {reference.summary()['classes']} classes, {seconds:.1f}s", flush=True)
            results[name] = reference
        return results


class GeneratorGates(unittest.TestCase):
    """Generation provenance and the PALP/reference class comparison."""

    references = {}

    @classmethod
    def setUpClass(cls):
        cls.references = compute_references(CASES + DESCRIPTOR_CASES, JOBS)

    def test_input_pool_hashes(self):
        recorded = MANIFEST["inputs"]
        present = sorted(p.name for p in (FIXTURES / "inputs").iterdir())
        self.assertEqual(present, sorted(recorded), "Unlisted or missing input pool")
        for name, digest in recorded.items():
            self.assertEqual(sha256_file(FIXTURES / "inputs" / name), digest, name)
            read_pool(FIXTURES / "inputs" / name)

    def test_golden_output_hashes(self):
        for case in MANIFEST["cases"]:
            for kind, digest in case["outputs"].items():
                with self.subTest(case=case["name"], kind=kind):
                    path = FIXTURES / f"{case['name']}.{kind}"
                    self.assertEqual(sha256_file(path), digest)

    def test_structures_match_recorded_types(self):
        for case in MANIFEST["cases"]:
            with self.subTest(case=case["name"]):
                signatures = {
                    combination_type(structure_matrix(s["blocks"]))
                    for s in case["reference"]["structures"]
                }
                self.assertEqual(signatures, set(case["types"]))

    def test_descriptor_table_matches_palp(self):
        native = read_descriptor_include(BIN_DIR / "dim5_structures.inc")
        self.assertEqual(native, MANIFEST["descriptors"], "PALP descriptors differ from the reviewed table")
        self.assertEqual([d["id"] for d in native], list(range(2, 48)), "Descriptor IDs 2-47")
        for descriptor in native:
            with self.subTest(descriptor=descriptor["id"]):
                blocks = descriptor["blocks"]
                ambient = structure_matrix(blocks)
                self.assertEqual(len(ambient[0]) - 1, descriptor["ambient"])
                self.assertEqual(descriptor["ambient"] - len(blocks), 5, "Polytope dimension")
                uses = collections.Counter(c for block in blocks for c in block)
                for block, shared in zip(blocks, descriptor["shared"]):
                    self.assertEqual(block[:shared], [c for c in block if uses[c] > 1], "Shared prefix")
                    self.assertEqual(len(block[:shared]), len([c for c in block if uses[c] > 1]))

    def test_descriptor_cases_cover_catalogue(self):
        explicit = sorted(e["descriptor"] for e in MANIFEST["descriptor_cases"] if e["pools"] == "explicit")
        self.assertEqual(explicit, sorted(DESCRIPTORS), "One explicit-file case per descriptor")
        for entry in MANIFEST["descriptor_cases"]:
            modes = set(entry["palp_repeated_rows"])
            expected = {"builtin"} if entry["pools"] == "builtin" else {"same-file", "copies"}
            self.assertEqual(modes, expected, entry["name"])

    def test_isomorphic_descriptors_generate_equal_classes(self):
        groups = collections.defaultdict(list)
        for entry in MANIFEST["descriptor_cases"]:
            if entry["pools"] == "explicit" and entry["name"] in self.references:
                key = canonical(tuple(map(tuple, structure_matrix(DESCRIPTORS[entry["descriptor"]]["blocks"]))))
                groups[key].append(entry["name"])
        for names in groups.values():
            first = self.references[names[0]].ip_keys()
            for name in names[1:]:
                self.assertEqual(self.references[name].ip_keys(), first, f"{names[0]} vs {name}")

    def check_descriptor(self, case):
        reference = self.references[case["name"]]
        summary = reference.summary()
        for field in ("candidates", "classes", "ip_classes", "non_ip_classes", "failures", "digest"):
            self.assertEqual(summary[field], case["reference"][field], f"Reference {field}")
        self.assertEqual(reference.failures, [], "Oracle failures are never a pass")
        with tempfile.TemporaryDirectory() as workspace:
            for mode, repeated in case["palp_repeated_rows"].items():
                with self.subTest(mode=mode):
                    rows = descriptor_rows(case, mode, pathlib.Path(workspace))
                    result = compare_with_palp(reference, rows)
                    for kind in ("missing", "unexpected", "rejected_by_oracle", "undecided"):
                        self.assertEqual(result[kind], [], f"{kind} classes")
                    self.assertEqual(result["repeated"], repeated, "Repeated PALP rows")
                    self.assertEqual(len(rows), summary["ip_classes"] + repeated, "PALP row accounting")

    def test_known_missing_cases_exist(self):
        names = {case["name"] for case in MANIFEST["cases"]}
        self.assertLessEqual(set(known_missing()), names)

    def check_case(self, case):
        reference = self.references[case["name"]]
        recorded = case["reference"]
        summary = reference.summary()
        for field in ("candidates", "classes", "ip_classes", "non_ip_classes", "failures", "digest"):
            self.assertEqual(summary[field], recorded[field], f"Reference {field}")
        self.assertEqual(reference.failures, [], "Oracle failures are never a pass")
        self.assertEqual(
            summary["classes"],
            summary["ip_classes"] + summary["non_ip_classes"] + summary["failures"],
            "Class accounting",
        )
        output = run(BIN_DIR / f"cws-{case['capacity']}d.x", arguments(case)).stdout
        rows = cws_rows(output, case["dimension"])
        self.assertEqual(len(rows), case["count"], "Accepted PALP rows")
        result = compare_with_palp(reference, rows)
        self.assertEqual(result["unexpected"], [], "PALP rows outside the reference universe")
        self.assertEqual(result["rejected_by_oracle"], [], "PALP accepted a non-IP class")
        self.assertEqual(result["undecided"], [], "PALP accepted an undecided class")
        self.assertEqual(
            result["missing"], known_missing().get(case["name"], []),
            "IP classes missing from PALP output differ from the recorded known defects",
        )
        self.assertEqual(result["repeated"], recorded["palp_repeated_rows"], "Repeated PALP rows")
        self.assertEqual(
            len(rows),
            summary["ip_classes"] - len(result["missing"]) + result["repeated"],
            "PALP row accounting",
        )

    def test_gate_detects_missing_repeated_and_changed_rows(self):
        case = next(c for c in MANIFEST["cases"] if c["name"] == "3d")
        reference = Reference(case["reference"], FIXTURES / "inputs", case["dimension"])
        rows = cws_rows((FIXTURES / "3d.cws").read_text(), 3)
        clean = compare_with_palp(reference, rows)
        self.assertEqual((clean["missing"], clean["unexpected"], clean["repeated"]), ([], [], 0))
        self.assertEqual(len(compare_with_palp(reference, rows[1:])["missing"]), 1)
        self.assertEqual(compare_with_palp(reference, rows + rows[:1])["repeated"], 1)
        line, weights = rows[0]
        # A non-normalized multiple of the first equation lies outside every pool.
        changed = [list(row) for row in weights]
        changed[0] = [2 * value for value in changed[0]]
        result = compare_with_palp(reference, [(line, changed)] + rows[1:])
        self.assertEqual(len(result["missing"]), 1)
        self.assertEqual(len(result["unexpected"]), 1)
        non_ip = next(k for k, r in reference.classes.items() if r["ip"] is False)
        result = compare_with_palp(reference, rows + [("", [list(r) for r in class_matrix(non_ip)])])
        self.assertEqual(len(result["rejected_by_oracle"]), 1)


class PointOracle(unittest.TestCase):
    """Self-tests of the reference point enumeration and IP decision."""

    def test_projective_plane(self):
        self.assertEqual(len(lattice_points(((3, 1, 1, 1),))), 10)
        self.assertTrue(is_ip(((3, 1, 1, 1),), lattice_points(((3, 1, 1, 1),))))

    def test_points_match_brute_force(self):
        generator = random.Random(20261015)
        for _ in range(150):
            rows = []
            width = generator.randint(3, 6)
            for _ in range(generator.randint(1, 2)):
                weights = [generator.randint(0, 3) for _ in range(width)]
                if not any(weights):
                    weights[0] = 1
                rows.append((sum(weights) + generator.randint(0, 3), *weights))
            if any(not any(r[j] for r in rows) for j in range(1, width + 1)):
                continue
            bound = max(r[0] for r in rows)
            expected = sorted(
                x for x in itertools.product(range(bound + 1), repeat=width)
                if all(sum(r[j + 1] * x[j] for j in range(width)) == r[0] for r in rows)
            )
            self.assertEqual(sorted(lattice_points(tuple(rows))), expected, rows)

    def test_point_limit_is_a_failure(self):
        with self.assertRaises(OracleFailure):
            lattice_points(((3, 1, 1, 1),), limit=5)
        case = next(c for c in MANIFEST["cases"] if c["name"] == "3d")
        reference = Reference(case["reference"], FIXTURES / "inputs", 3, point_limit=5)
        summary = reference.summary()
        self.assertGreater(summary["failures"], 0)
        self.assertEqual(summary["classes"], summary["ip_classes"] + summary["non_ip_classes"] + summary["failures"])

    def test_ip_lists_reproduced_from_scratch(self):
        # Every normalized weight system with 2, 3 or 4 weights up to the largest
        # listed degree; the IP ones must be exactly the checked-in complete lists.
        for pool, width, top in (("ws-1d.txt", 2, 2), ("ws-2d.txt", 3, 6), ("ws-3d.txt", 4, 66)):
            with self.subTest(pool=pool):
                expected = {(d, *w) for d, w in read_pool(FIXTURES / "inputs" / pool)}
                found = set()
                for degree in range(2, top + 1):
                    for weights in _partitions(degree, width):
                        if math.gcd(*weights) != 1:
                            continue
                        row = ((degree, *weights),)
                        if is_ip(row, lattice_points(row)):
                            found.add(row[0])
                self.assertEqual(found, expected)

    def test_golden_point_counts(self):
        # PALP's M:p counts lattice points of P; each must equal the reference count.
        for case in MANIFEST["cases"]:
            with self.subTest(case=case["name"]):
                cache = {}
                for line, weights in cws_rows((FIXTURES / f"{case['name']}.cws").read_text(), case["dimension"]):
                    key = canonical(tuple(map(tuple, weights)))
                    if key not in cache:
                        cache[key] = len(lattice_points(tuple(map(tuple, weights))))
                    palp = int(line.split(" M:")[1].split()[0])
                    self.assertEqual(cache[key], palp, line)


def _partitions(total, parts, smallest=1):
    if parts == 1:
        if total >= smallest:
            yield (total,)
        return
    for first in range(smallest, total // parts + 1):
        for rest in _partitions(total - first, parts - 1, first):
            yield (first, *rest)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--bin-dir", type=pathlib.Path, default=pathlib.Path(__file__).resolve().parents[1] / "PALP")
    parser.add_argument("--suite", required=True, help="generator or points")
    parser.add_argument("--case", help="Restrict generator reference cases to one name")
    parser.add_argument("--jobs", type=int, default=min(os.cpu_count() or 1, 8))
    options = parser.parse_args()
    if options.suite not in LANDED_SUITES:
        parser.error(
            f"suite {options.suite!r} has not landed; available: {', '.join(LANDED_SUITES)}"
        )
    BIN_DIR = options.bin_dir.resolve()
    JOBS = max(1, options.jobs)
    configure_limits()
    CASES = [c for c in MANIFEST["cases"] if not options.case or c["name"] == options.case]
    DESCRIPTOR_CASES = [
        descriptor_case(e) for e in MANIFEST["descriptor_cases"]
        if not options.case or e["name"] == options.case
    ]
    if not CASES and not DESCRIPTOR_CASES:
        parser.error("No matching cases")
    if options.suite == "generator":
        for case in CASES:
            def test(self, case=case):
                self.check_case(case)
            setattr(GeneratorGates, "test_case_" + case["name"].replace("-", "_"), test)
        for case in DESCRIPTOR_CASES:
            def test(self, case=case):
                self.check_descriptor(case)
            setattr(GeneratorGates, "test_descriptor_" + case["name"].replace("-", "_"), test)
        suite = unittest.defaultTestLoader.loadTestsFromTestCase(GeneratorGates)
    else:
        suite = unittest.defaultTestLoader.loadTestsFromTestCase(PointOracle)
    result = unittest.TextTestRunner(verbosity=2).run(suite)
    sys.exit(not result.wasSuccessful())
