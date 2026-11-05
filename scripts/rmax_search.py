#!/usr/bin/env python3
"""Exact, small-instance reference search; not a production 5D classifier."""

import argparse
from collections import Counter
from functools import lru_cache
import hashlib
from itertools import combinations, product
import json
from math import isqrt, prod
from pathlib import Path
import subprocess
import sys


def determinant(rows):
    a = [list(row) for row in rows]
    sign, previous = 1, 1
    for k in range(len(a) - 1):
        pivot = next((i for i in range(k, len(a)) if a[i][k]), None)
        if pivot is None:
            return 0
        if pivot != k:
            a[k], a[pivot] = a[pivot], a[k]
            sign = -sign
        value = a[k][k]
        for i in range(k + 1, len(a)):
            for j in range(k + 1, len(a)):
                a[i][j] = (a[i][j] * value - a[i][k] * a[k][j]) // previous
            a[i][k] = 0
        previous = value
    return sign * a[-1][-1]


def independent_rows(points, dimension):
    basis, pivots = [], []
    for point in points:
        row = list(point)
        for b, pivot in zip(basis, pivots):
            if row[pivot]:
                factor = row[pivot]
                row = [b[pivot] * x - factor * y for x, y in zip(row, b)]
        pivot = next((i for i, x in enumerate(row) if x), None)
        if pivot is not None:
            basis.append(row)
            pivots.append(pivot)
            if len(basis) == dimension:
                return True
    return False


def divisors(n):
    result = []
    for k in range(1, isqrt(n) + 1):
        if n % k == 0:
            result.append(k)
            if k * k != n:
                result.append(n // k)
    return sorted(result)


def diagonal_tuples(index, dimension):
    if dimension == 1:
        yield (index,)
    else:
        for first in divisors(index):
            for rest in diagonal_tuples(index // first, dimension - 1):
                yield (first,) + rest


def hnf_bases(dimension, index):
    """Column HNF: upper triangular, 0 <= H[i,j] < H[i,i] for i < j."""
    for diagonal in diagonal_tuples(index, dimension):
        positions = [(i, j) for i in range(dimension) for j in range(i + 1, dimension)]
        for residues in product(*(range(diagonal[i]) for i, _ in positions)):
            h = [[0] * dimension for _ in range(dimension)]
            for i, value in enumerate(diagonal):
                h[i][i] = value
            for (i, j), value in zip(positions, residues):
                h[i][j] = value
            yield tuple(tuple(row) for row in h)


def lattice_coordinates(h, point):
    coordinates = [0] * len(h)
    for i in reversed(range(len(h))):
        value = point[i] - sum(h[i][j] * coordinates[j] for j in range(i + 1, len(h)))
        quotient, remainder = divmod(value, h[i][i])
        if remainder:
            return None
        coordinates[i] = quotient
    return tuple(coordinates)


def matrix_input(points):
    if not points or not points[0]:
        raise ValueError("A point matrix must be nonempty")
    d = len(points[0])
    if any(len(point) != d or any(type(x) is not int for x in point) for point in points):
        raise ValueError("Point coordinates must be integer vectors of equal dimension")
    return f"{d} {len(points)}\n" + "\n".join(
        " ".join(str(point[i]) for point in points) for i in range(d)
    ) + "\n"


def sha256_file(path):
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


class Palp:
    def __init__(self, poly, classifier):
        self.poly = str(Path(poly).resolve())
        self.classifier = str(Path(classifier).resolve())

    @staticmethod
    def run(binary, flags, text):
        result = subprocess.run(
            [binary, *flags], input=text, text=True, capture_output=True, check=True
        )
        if result.stderr:
            raise RuntimeError(f"PALP diagnostic: {result.stderr.strip()}")
        return result.stdout

    @staticmethod
    def block(output, label, transposed):
        lines = output.splitlines()
        matches = [
            i for i, line in enumerate(lines)
            if label in line and not (label == "Vertices of P" and "P-dual" in line)
        ]
        if len(matches) != 1:
            raise ValueError(f"Expected one PALP {label!r} block:\n{output}")
        i = matches[0]
        header = lines[i].split()
        rows, columns = map(int, header[:2])
        matrix = tuple(tuple(map(int, line.split())) for line in lines[i + 1:i + 1 + rows])
        if len(matrix) != rows or any(len(row) != columns for row in matrix):
            raise ValueError(f"Malformed PALP {label!r} matrix:\n{output}")
        return tuple(zip(*matrix)) if transposed else matrix

    def points(self, text):
        output = self.run(self.poly, ["-fp"], text)
        matrix = self.block(output, "Points of P", False)
        if len(matrix) < len(matrix[0]):
            matrix = tuple(zip(*matrix))
        return tuple(sorted(matrix))

    @lru_cache(maxsize=256)
    def hull(self, points):
        d = len(points[0])
        differences = [tuple(x - y for x, y in zip(p, points[0])) for p in points[1:]]
        if len(points) <= d or not independent_rows(differences, d):
            return (), ()
        output = self.run(self.poly, ["-fnve"], matrix_input(points))
        vertices = self.block(output, "Vertices of P", False)
        if len(vertices) == d:
            vertices = tuple(zip(*vertices))
        if any(len(vertex) != d for vertex in vertices):
            raise ValueError(f"Malformed PALP vertex matrix:\n{output}")
        vertices = tuple(sorted(vertices))
        if "Equations of P" not in output:
            raise ValueError(f"Missing PALP facet block:\n{output}")
        if "Vertices of P-dual" in output:
            normals = self.block(output, "Vertices of P-dual", False)
            facets = tuple(sorted(row + (1,) for row in normals))
        else:
            lines = output.splitlines()
            i = next(i for i, line in enumerate(lines) if "Equations of P" in line)
            count, dimension = map(int, lines[i].split()[:2])
            facets = tuple(sorted(tuple(map(int, line.split()))
                                  for line in lines[i + 1:i + 1 + count]))
            if dimension != d or len(facets) != count or any(
                len(row) != d + 1 for row in facets
            ):
                raise ValueError(f"Malformed PALP facet matrix:\n{output}")
        return vertices, facets

    def normal_form(self, vertices):
        output = self.run(self.poly, ["-fnN"], matrix_input(vertices))
        return self.block(output, "Normal form of vertices", True)

    def maximal(self, vertices):
        _, facets = self.hull(vertices)
        if not facets or any(f[-1] != 1 for f in facets):
            raise ValueError("The maximality oracle requires a reflexive input")
        # Max_check prints one empty CWS row for accepted matrix input (nw=0).
        output = self.run(self.classifier, ["-f", "-mr"], matrix_input(vertices))
        if output not in ("", "\n"):
            raise ValueError(f"Unexpected PALP maximality response:\n{output}")
        return output == "\n"


def reflexive_leaves(points, oracle, stats):
    pending, visited = [points], set()
    while pending:
        current = pending.pop()
        if current in visited:
            continue
        visited.add(current)
        stats["hull_nodes"] += 1
        vertices, facets = oracle.hull(current)
        if not facets or any(f[-1] <= 0 for f in facets):
            stats["non_ip_nodes"] += 1
            continue
        bad = next((f for f in facets if f[-1] != 1), None)
        if bad is None:
            stats["reflexive_leaves"] += 1
            yield vertices
            continue
        stats["bad_facet_nodes"] += 1
        d, basis = len(current[0]), []
        for vertex in vertices:
            if sum(a * x for a, x in zip(bad[:-1], vertex)) + bad[-1]:
                continue
            if independent_rows([*basis, vertex], len(basis) + 1):
                basis.append(vertex)
            if len(basis) == d:
                break
        if len(basis) != d:
            raise ValueError("Nonunit facet lacks a full affine frame")
        for vertex in reversed(basis):
            pending.append(tuple(point for point in current if point != vertex))


def search(records, oracle, max_index=None, stats=None):
    if stats is None:
        stats = Counter()
    tested, emitted = {}, set()
    for record_number, text in enumerate(records, 1):
        points = oracle.points(text)
        facets = oracle.hull(points)[1] if points else ()
        if not facets:
            raise ValueError(f"Record {record_number} is not full dimensional")
        if any(f[-1] <= 0 for f in facets):
            raise ValueError(f"Record {record_number} is not origin-IP")
        vertices = oracle.hull(points)[0]
        d = len(points[0])
        bound = max_index
        if bound is None:
            bound = max(abs(determinant(frame)) for frame in combinations(vertices, d))
        stats["containers"] += 1
        for index in range(1, bound + 1):
            for h in hnf_bases(d, index):
                stats["lattices"] += 1
                restricted = tuple(sorted(
                    coordinates for point in points
                    if (coordinates := lattice_coordinates(h, point)) is not None
                ))
                for candidate in reflexive_leaves(restricted, oracle, stats):
                    nf = oracle.normal_form(candidate)
                    if nf not in tested:
                        tested[nf] = oracle.maximal(nf)
                        stats["maximality_checks"] += 1
                    if tested[nf] and nf not in emitted:
                        emitted.add(nf)
                        stats["maximal_classes"] += 1
                        yield {
                            "vertices": nf,
                            "source_vertices": candidate,
                            "container_record": record_number,
                            "lattice_basis": h,
                            "index": prod(h[i][i] for i in range(d)),
                        }


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("input", type=Path, help="PALP WS/CWS records, one per line")
    parser.add_argument(
        "--format", choices=("cws", "jsonl"), default="cws",
        help="jsonl: one object per line, with integer vertex vectors in 'vertices'",
    )
    parser.add_argument("--poly", type=Path, required=True)
    parser.add_argument("--class", dest="classifier", type=Path, required=True)
    parser.add_argument(
        "--max-index", type=int,
        help="EXPERIMENT ONLY: truncate the lattice search; output is not exhaustive",
    )
    args = parser.parse_args()
    if args.max_index is not None and args.max_index < 1:
        parser.error("--max-index must be positive")
    oracle = Palp(args.poly, args.classifier)
    stats = Counter()
    with args.input.open() as stream:
        records = (line for line in stream if line.strip())
        if args.format == "jsonl":
            records = (matrix_input(json.loads(line)["vertices"]) for line in records)
        for result in search(records, oracle, args.max_index, stats):
            print(json.dumps(result), flush=True)
    print(json.dumps({
        **stats,
        "lattice_search_truncated": args.max_index is not None,
        "input_sha256": sha256_file(args.input),
    }, sort_keys=True), file=sys.stderr)


if __name__ == "__main__":
    main()
