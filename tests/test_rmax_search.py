import importlib.util
from itertools import combinations, product
import json
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest


ROOT = Path(__file__).resolve().parents[1]
SPEC = importlib.util.spec_from_file_location("rmax_search", ROOT / "scripts/rmax_search.py")
rmax = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(rmax)


class LatticeTests(unittest.TestCase):
    def test_hnf_indices_and_residues(self):
        for index, count in [(1, 1), (2, 15), (3, 40)]:
            bases = list(rmax.hnf_bases(4, index))
            self.assertEqual(len(bases), count)
            self.assertEqual(len(set(bases)), count)
            self.assertTrue(all(rmax.determinant(h) == index for h in bases))

    def test_nondiagonal_lattice_membership(self):
        h = ((2, 1), (0, 1))
        self.assertEqual(rmax.lattice_coordinates(h, (1, 1)), (0, 1))
        self.assertEqual(rmax.lattice_coordinates(h, (-1, 1)), (-1, 1))
        self.assertIsNone(rmax.lattice_coordinates(h, (0, 1)))

    def test_determinant_with_pivoting(self):
        self.assertEqual(rmax.determinant(((0, 1), (2, 3))), -2)
        self.assertEqual(rmax.determinant(((1, 2), (2, 4))), 0)


@unittest.skipUnless((ROOT / "PALP/poly-5d.x").is_file(), "Build PALP 5D binaries")
class GeometryTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.oracle = rmax.Palp(ROOT / "PALP/poly-5d.x", ROOT / "PALP/class-5d.x")

    def test_maximality_and_non_ip(self):
        square = ((-1, -1), (-1, 1), (1, -1), (1, 1))
        diamond = ((-1, 0), (0, -1), (0, 1), (1, 0))
        self.assertTrue(self.oracle.maximal(square))
        self.assertFalse(self.oracle.maximal(diamond))
        _, facets = self.oracle.hull(((0, 0), (0, 1), (1, 0)))
        self.assertTrue(any(f[-1] == 0 for f in facets))

    def test_sublattice_changes_hull(self):
        points = self.oracle.points("5 1 1 1 1 1\n")
        original_vertices, _ = self.oracle.hull(points)
        found = []
        for h in rmax.hnf_bases(4, 2):
            restricted = tuple(sorted(
                q for p in points if (q := rmax.lattice_coordinates(h, p)) is not None
            ))
            vertices, facets = self.oracle.hull(restricted)
            if facets and all(f[-1] == 1 for f in facets):
                if self.oracle.maximal(vertices) and len(vertices) != len(original_vertices):
                    found.append(vertices)
        self.assertTrue(found, "An unchanged-vertex lattice interval misses this maximal hull")

    def test_complete_two_dimensional_search(self):
        records = [
            "3 1 1 1\n", "4 1 1 2\n", "6 1 2 3\n",
            "2 1 1 0 0 2 0 0 1 1\n",
        ]
        result = list(rmax.search(records, self.oracle))
        self.assertEqual(len(result), 3)

    def test_five_dimensional_bad_facet(self):
        cross = tuple(
            tuple(sign if i == axis else 0 for i in range(5))
            for axis in range(5) for sign in (-1, 1)
        )
        points = self.oracle.points(rmax.matrix_input((*cross, (2, 0, 0, 0, 0))))
        stats = rmax.Counter()
        leaves = list(rmax.reflexive_leaves(points, self.oracle, stats))
        self.assertEqual(leaves, [tuple(sorted(cross))])
        self.assertEqual(stats["bad_facet_nodes"], 1)
        self.assertEqual(stats["hull_nodes"], 6)

    def test_vertex_matrix_orientation_above_twenty_vertices(self):
        cube = tuple(product((-1, 1), repeat=5))
        vertices, facets = self.oracle.hull(cube)
        self.assertEqual(vertices, cube)
        self.assertEqual(len(facets), 10)
        self.assertTrue(all(f[-1] == 1 for f in facets))
        normal_form = self.oracle.normal_form(vertices)
        self.assertEqual(len(normal_form), len(cube))
        self.assertTrue(all(len(vertex) == 5 for vertex in normal_form))

    def test_minimal_first_cannot_prune_by_known_maximal_container(self):
        cross = tuple(
            tuple(sign if i == axis else 0 for i in range(5))
            for axis in range(5) for sign in (-1, 1)
        )
        other = (
            (1, 0, 0, 0, 0), (0, 1, 0, 0, 0), (-1, -1, 0, 0, 0),
            *(v for v in cross if v[0] == v[1] == 0),
        )
        cube = tuple(product((-1, 1), repeat=5))
        other_dual = tuple(
            (*xy, *z)
            for xy in ((-1, -1), (-1, 2), (2, -1))
            for z in product((-1, 1), repeat=3)
        )
        self.assertTrue(all(all(abs(x) <= 1 for x in v) for v in other))
        for minimal in [cross, other]:
            points = self.oracle.points(rmax.matrix_input(minimal))
            vertices, facets = self.oracle.hull(points)
            self.assertTrue(all(f[-1] == 1 for f in facets))
            for v in vertices:
                _, minor_facets = self.oracle.hull(tuple(p for p in points if p != v))
                self.assertFalse(minor_facets and all(f[-1] > 0 for f in minor_facets))
        self.assertTrue(self.oracle.maximal(cube))
        self.assertTrue(self.oracle.maximal(other_dual))
        self.assertGreater(
            len(self.oracle.points(rmax.matrix_input(other_dual))),
            len(self.oracle.points(rmax.matrix_input(cube))),
        )

    def test_jsonl_cli_complete_search(self):
        with tempfile.TemporaryDirectory() as work:
            path = Path(work) / "square.jsonl"
            path.write_text(json.dumps({"vertices": list(product((-1, 1), repeat=2))}) + "\n")
            result = subprocess.run(
                [sys.executable, str(ROOT / "scripts/rmax_search.py"), str(path),
                 "--format", "jsonl", "--poly", self.oracle.poly,
                 "--class", self.oracle.classifier],
                text=True, capture_output=True, check=True,
            )
        self.assertEqual(len(result.stdout.splitlines()), 1)
        summary = json.loads(result.stderr)
        self.assertFalse(summary["lattice_search_truncated"])
        self.assertEqual(summary["lattices"], 4)

    def test_bad_facet_search_against_all_subsets(self):
        points = self.oracle.points(rmax.matrix_input(((-1, -1), (1, -1), (0, 2))))
        leaves = list(rmax.reflexive_leaves(points, self.oracle, rmax.Counter()))
        self.assertTrue(leaves)
        self.assertTrue(any(f[-1] > 1 for f in self.oracle.hull(points)[1]))
        for size in range(3, len(points) + 1):
            for subset in combinations(points, size):
                vertices, facets = self.oracle.hull(subset)
                if facets and all(f[-1] == 1 for f in facets):
                    self.assertTrue(any(
                        all(all(
                            sum(a * x for a, x in zip(f[:-1], v)) + f[-1] >= 0
                            for f in self.oracle.hull(leaf)[1]
                        ) for v in vertices)
                        for leaf in leaves
                    ))


if __name__ == "__main__":
    unittest.main()
