"""Independent bounded reference for CWS generation and IP acceptance.

Nothing here reuses PALP's selection, gluing, point-enumeration, or IP code.
Every assignment of input weight systems to an explicit coordinate-incidence
structure is enumerated, combined weight systems are identified up to equation
and coordinate permutations, Newton-polytope points are enumerated directly from
``W x = d, x >= 0``, and the IP property is decided by an exact rational linear
program. This is a bounded test oracle for the listed inputs, not a proof of
completeness for any larger universe.
"""

import collections
import hashlib
import itertools
import json
import math
from fractions import Fraction

DEFAULT_POINT_LIMIT = 200_000


class OracleFailure(Exception):
    """A candidate the oracle could not decide; counted, never treated as a pass."""


def sha256_file(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def read_pool(path):
    """Read one weight system per line: degree followed by positive weights."""
    pool = []
    for number, line in enumerate(path.read_text().splitlines(), 1):
        if not line.strip():
            continue
        values = tuple(map(int, line.split()))
        degree, weights = values[0], values[1:]
        if len(weights) < 2 or min(weights) <= 0 or sum(weights) != degree:
            raise ValueError(f"{path.name}:{number}: invalid weight system {line!r}")
        pool.append((degree, weights))
    if not pool:
        raise ValueError(f"{path.name}: empty weight-system pool")
    return pool


def validate_structure(blocks):
    """Blocks list the one-based ambient coordinates used by each weight system."""
    used = sorted(slot for block in blocks for slot in block)
    ambient = max(used)
    if any(len(set(block)) != len(block) for block in blocks):
        raise ValueError(f"Repeated coordinate in a block: {blocks}")
    if set(used) != set(range(1, ambient + 1)):
        raise ValueError(f"Structure leaves a coordinate unused: {blocks}")
    return ambient


def structure_matrix(blocks):
    """A 0/1 incidence matrix with the CWS row layout, for type signatures."""
    ambient = validate_structure(blocks)
    return [
        [len(block)] + [int(slot in block) for slot in range(1, ambient + 1)]
        for block in blocks
    ]


def _block_assignments(weights, shared_count):
    """Distinct weight tuples on the shared slots, with the remaining multiset.

    Private slots belong to one block only, so permuting their values is a
    coordinate permutation and only the multiset left for them matters.
    """
    seen = set()
    for chosen in itertools.permutations(range(len(weights)), shared_count):
        values = tuple(weights[i] for i in chosen)
        if values in seen:
            continue
        seen.add(values)
        rest = list(weights)
        for value in values:
            rest.remove(value)
        yield values, tuple(sorted(rest))


def enumerate_candidates(blocks, pools):
    """Yield ``(candidate_id, matrix)`` for every assignment of pool entries.

    ``candidate_id`` is ``(entry indices, shared-slot values)``: one-based pool
    line numbers and, per block, the weights placed on its shared coordinates in
    increasing coordinate order. It is stable for fixed pool contents.
    """
    ambient = validate_structure(blocks)
    counts = collections.Counter(slot for block in blocks for slot in block)
    layouts = []
    for block in blocks:
        ordered = sorted(block)
        layouts.append((
            [slot for slot in ordered if counts[slot] > 1],
            [slot for slot in ordered if counts[slot] == 1],
        ))
    if len(pools) != len(blocks):
        raise ValueError("Each structure block needs exactly one pool")
    for entries in itertools.product(*(enumerate(pool, 1) for pool in pools)):
        if any(len(w) != len(block) for (_, (_, w)), block in zip(entries, blocks)):
            raise ValueError(f"Pool entry width does not match structure {blocks}")
        choices = [
            list(_block_assignments(w, len(shared)))
            for (_, (_, w)), (shared, _) in zip(entries, layouts)
        ]
        indices = tuple(index for index, _ in entries)
        for selection in itertools.product(*choices):
            matrix = []
            for (_, (degree, _)), (shared, private), (values, rest) in zip(
                entries, layouts, selection
            ):
                row = [0] * (ambient + 1)
                row[0] = degree
                for slot, value in zip(shared, values):
                    row[slot] = value
                for slot, value in zip(private, rest):
                    row[slot] = value
                matrix.append(tuple(row))
            yield (indices, tuple(values for values, _ in selection)), tuple(matrix)


def canonical(matrix):
    """Minimal form under equation and coordinate permutations."""
    width = len(matrix[0])
    best = None
    for order in itertools.permutations(matrix):
        key = (
            tuple(row[0] for row in order),
            tuple(sorted(tuple(row[j] for row in order) for j in range(1, width))),
        )
        if best is None or key < best:
            best = key
    return best


def format_class(key):
    degrees, columns = key
    return "  ".join(
        " ".join(map(str, (degree, *(column[i] for column in columns))))
        for i, degree in enumerate(degrees)
    )


def class_matrix(key):
    degrees, columns = key
    return tuple(
        (degree, *(column[i] for column in columns))
        for i, degree in enumerate(degrees)
    )


def _row_reduce(rows):
    """Reduced row echelon form over Q; returns (rows, pivot columns)."""
    rows = [[Fraction(v) for v in row] for row in rows]
    pivots = []
    r = 0
    for c in range(len(rows[0]) if rows else 0):
        p = next((i for i in range(r, len(rows)) if rows[i][c]), None)
        if p is None:
            continue
        rows[r], rows[p] = rows[p], rows[r]
        lead = rows[r][c]
        rows[r] = [v / lead for v in rows[r]]
        for i in range(len(rows)):
            if i != r and rows[i][c]:
                factor = rows[i][c]
                rows[i] = [a - factor * b for a, b in zip(rows[i], rows[r])]
        pivots.append(c)
        r += 1
        if r == len(rows):
            break
    return rows[:r], pivots


def rank(vectors):
    """Exact rank of integer vectors by fraction-free elimination."""
    rows = [list(v) for v in vectors if any(v)]
    result = 0
    width = len(rows[0]) if rows else 0
    for c in range(width):
        p = next((i for i, row in enumerate(rows) if row[c]), None)
        if p is None:
            continue
        pivot = rows.pop(p)
        result += 1
        reduced = []
        for row in rows:
            if row[c]:
                row = [a * pivot[c] - b * row[c] for a, b in zip(row, pivot)]
            g = math.gcd(*row)
            if g:
                reduced.append([v // g for v in row])
        rows = reduced
    return result


def lattice_points(matrix, limit=DEFAULT_POINT_LIMIT):
    """All nonnegative integer x with sum_j W[i][j] x_j = d_i for every row."""
    degrees = [row[0] for row in matrix]
    weights = [row[1:] for row in matrix]
    width = len(weights[0])
    if any(not any(row[j] for row in weights) for j in range(width)):
        raise OracleFailure("Unbounded coordinate with no positive weight")
    finishing = [[] for _ in range(width)]
    for i, row in enumerate(weights):
        finishing[max(j for j in range(width) if row[j])].append(i)
    columns = [[(i, row[j]) for i, row in enumerate(weights) if row[j]] for j in range(width)]
    remaining = list(degrees)
    point = [0] * width
    points = []

    def visit(j):
        if j == width:
            points.append(tuple(point))
            if len(points) > limit:
                raise OracleFailure(f"More than {limit} lattice points")
            return
        if finishing[j]:
            i, w = next((i, w) for i, w in columns[j] if i == finishing[j][0])
            if remaining[i] % w:
                return
            values = (remaining[i] // w,)
            if any(remaining[k] < v * values[0] for k, v in columns[j]):
                return
        else:
            values = range(min(remaining[i] // w for i, w in columns[j]) + 1)
        for value in values:
            for i, w in columns[j]:
                remaining[i] -= w * value
            if all(remaining[i] == 0 for i in finishing[j]):
                point[j] = value
                visit(j + 1)
            for i, w in columns[j]:
                remaining[i] += w * value
        point[j] = 0

    visit(0)
    return points


def _strictly_positive_dependency(columns):
    """Exact phase-I simplex: is there mu >= 1 with sum mu_s a_s = 0?

    With mu = 1 + nu this is nu >= 0, A nu = -A 1. Rows are kept as integer
    multiples of the rational tableau (fraction-free pivoting with gcd
    reduction), so every comparison is exact. Bland's rule guarantees
    termination.
    """
    m = len(columns[0])
    n = len(columns)
    rhs = [-sum(column[i] for column in columns) for i in range(m)]
    table = []
    for i in range(m):
        sign = -1 if rhs[i] < 0 else 1
        table.append(
            [sign * column[i] for column in columns]
            + [int(t == i) for t in range(m)]
            + [sign * rhs[i]]
        )
    basis = [n + i for i in range(m)]
    objective = [-sum(row[k] for row in table) for k in range(n)] + [0] * m
    objective.append(-sum(row[-1] for row in table))

    def reduce(row):
        g = math.gcd(*row)
        return [v // g for v in row] if g > 1 else row

    while True:
        entering = next((k for k in range(n + m) if objective[k] < 0), None)
        if entering is None:
            return objective[-1] == 0
        leaving = None
        for i, row in enumerate(table):
            if row[entering] > 0:
                # Compare row[-1] / row[entering] exactly; both denominators are positive.
                if leaving is None:
                    leaving = i
                    continue
                lhs = row[-1] * table[leaving][entering]
                rhs_best = table[leaving][-1] * row[entering]
                if lhs < rhs_best or (lhs == rhs_best and basis[i] < basis[leaving]):
                    leaving = i
        if leaving is None:
            raise OracleFailure("Unbounded phase-I objective")
        pivot_row = table[leaving]
        pivot = pivot_row[entering]
        for i, row in enumerate(table):
            if i != leaving and row[entering]:
                factor = row[entering]
                table[i] = reduce([a * pivot - b * factor for a, b in zip(row, pivot_row)])
        factor = objective[entering]
        objective = reduce([a * pivot - b * factor for a, b in zip(objective, pivot_row)])
        table[leaving] = reduce(pivot_row)
        basis[leaving] = entering


def is_ip(matrix, points):
    """Whether (1, ..., 1) lies in the relative interior of conv(points).

    The point is interior exactly when the polytope has dimension N - rank(W)
    and some strictly positive combination of the vectors s - 1 vanishes.
    Those vectors lie in ker W, so they are compared on its free coordinates.
    """
    weights = [row[1:] for row in matrix]
    width = len(weights[0])
    _, pivots = _row_reduce([list(row) for row in weights])
    free = [j for j in range(width) if j not in pivots]
    for j in range(width):
        values = {p[j] for p in points}
        if min(values) >= 1 and values != {1}:
            return False  # x_j >= 1 is a supporting inequality through 1.
    vectors = sorted({tuple(p[j] - 1 for j in free) for p in points} - {(0,) * len(free)})
    if rank([list(v) for v in vectors]) != len(free):
        return False
    return _strictly_positive_dependency(vectors)


class Reference:
    """Oracle results for one fixture case."""

    def __init__(self, case_reference, inputs_dir, dimension, point_limit=DEFAULT_POINT_LIMIT):
        self.dimension = dimension
        self.point_limit = point_limit
        self.classes = {}
        self.accounting = collections.Counter()
        self.failures = []
        for name, structure in enumerate(case_reference["structures"]):
            blocks = structure["blocks"]
            pools = [read_pool(inputs_dir / pool) for pool in structure["pools"]]
            for candidate_id, matrix in enumerate_candidates(blocks, pools):
                self.accounting["candidates"] += 1
                key = canonical(matrix)
                identity = (name, *candidate_id)
                if key in self.classes:
                    if identity < self.classes[key]["representative"]:
                        self.classes[key]["representative"] = identity
                    continue
                self.classes[key] = {"representative": identity}
        for key, record in self.classes.items():
            matrix = class_matrix(key)
            try:
                if rank([list(row[1:]) for row in matrix]) != len(matrix):
                    raise OracleFailure("Rank-deficient weight matrix")
                if len(matrix[0]) - 1 - len(matrix) != dimension:
                    raise OracleFailure("Wrong polytope dimension")
                points = lattice_points(matrix, point_limit)
                record["points"] = len(points)
                record["ip"] = is_ip(matrix, points)
            except OracleFailure as error:
                record["failure"] = str(error)
                self.failures.append((format_class(key), str(error)))
        self.accounting["classes"] = len(self.classes)
        self.accounting["ip_classes"] = sum(1 for r in self.classes.values() if r.get("ip") is True)
        self.accounting["non_ip_classes"] = sum(1 for r in self.classes.values() if r.get("ip") is False)
        self.accounting["failures"] = len(self.failures)

    def ip_keys(self):
        return {key for key, record in self.classes.items() if record.get("ip") is True}

    def summary(self):
        return {
            "candidates": self.accounting["candidates"],
            "classes": self.accounting["classes"],
            "ip_classes": self.accounting["ip_classes"],
            "non_ip_classes": self.accounting["non_ip_classes"],
            "failures": self.accounting["failures"],
            "digest": self.digest(),
        }

    def digest(self):
        """SHA-256 of every class, its label, point count, and representative ID."""
        lines = []
        for key in sorted(self.classes):
            record = self.classes[key]
            label = "fail" if "failure" in record else ("ip" if record["ip"] else "non-ip")
            lines.append(
                f"{format_class(key)} | {label} | {record.get('points', '-')} | "
                f"{json.dumps(record['representative'], separators=(',', ':'))}\n"
            )
        return hashlib.sha256("".join(lines).encode()).hexdigest()


def compare_with_palp(reference, palp_rows):
    """Reconcile PALP's accepted rows with the oracle's IP classes.

    Returns a dict of discrepancy lists; every list must be empty, except
    ``repeated`` which counts extra rows of an already-emitted class.
    """
    emitted = collections.Counter(canonical(tuple(map(tuple, weights))) for _, weights in palp_rows)
    ip_keys = reference.ip_keys()
    known = reference.classes
    return {
        "missing": sorted(format_class(k) for k in ip_keys - emitted.keys()),
        "unexpected": sorted(format_class(k) for k in emitted.keys() - known.keys()),
        "rejected_by_oracle": sorted(
            format_class(k) for k in emitted.keys() & known.keys() if known[k].get("ip") is False
        ),
        "undecided": sorted(
            format_class(k) for k in emitted.keys() & known.keys() if "failure" in known[k]
        ),
        "repeated": sum(count - 1 for count in emitted.values()),
    }
