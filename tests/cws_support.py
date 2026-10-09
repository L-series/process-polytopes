"""Shared parsing and process helpers for the pinned PALP baseline."""

import collections
import itertools
import pathlib
import re
import resource
import subprocess

FIXTURES = pathlib.Path(__file__).parent / "fixtures" / "cws"


def configure_limits():
    # PALP's dimension-5 routines use large stack allocations.
    _, hard = resource.getrlimit(resource.RLIMIT_STACK)
    resource.setrlimit(resource.RLIMIT_STACK, (hard, hard))
    resource.setrlimit(resource.RLIMIT_CORE, (0, 0))


def run(binary, args, data=None, timeout=600, check=True):
    result = subprocess.run(
        [str(binary), *args],
        input=data,
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        timeout=timeout,
    )
    if check and (result.returncode or result.stderr):
        raise AssertionError(
            f"{binary.name} {args}: exit {result.returncode}\n"
            f"{result.stderr}\n{result.stdout[-2000:]}"
        )
    return result


def arguments(case):
    return [
        arg.replace("{inputs}", str(FIXTURES / "inputs"))
        for arg in case["args"]
    ]


def cws_rows(output, dimension):
    rows = []
    for line in output.splitlines():
        line = " ".join(line.split())
        numbers, separator, annotation = line.partition(" M:")
        if not separator or not re.fullmatch(
            r"\d+ \d+ (?:N:\d+ \d+|F:\d+ N:\d+)", annotation
        ):
            raise AssertionError(f"Unexpected CWS output: {line}")
        values = list(map(int, numbers.split()))
        ranks = [
            r for r in range(1, 6)
            if len(values) == r * (dimension + r + 1)
        ]
        if len(ranks) != 1:
            raise AssertionError(f"Wrong CWS width: {line}")
        rank = ranks[0]
        width = dimension + rank + 1
        weights = [
            values[i * width:(i + 1) * width] for i in range(rank)
        ]
        for row in weights:
            if row[0] <= 0 or min(row[1:]) < 0 or sum(row[1:]) != row[0]:
                raise AssertionError(f"Invalid degree or weights: {line}")
        if any(not any(row[j] for row in weights) for j in range(1, width)):
            raise AssertionError(f"Unused CWS coordinate: {line}")
        rows.append((line, weights))
    return rows


def combination_type(weights):
    """Column incidence, canonical under equation and coordinate permutations.

    Bit i means equation i uses that coordinate. Unlike equation count alone,
    this distinguishes overlaps, products, and the component dimensions.
    """
    rank = len(weights)
    signatures = []
    for permutation in itertools.permutations(range(rank)):
        counts = collections.Counter(
            sum(1 << i for i, row in enumerate(permutation) if weights[row][j] > 0)
            for j in range(1, len(weights[0]))
        )
        signatures.append(tuple(counts[mask] for mask in range(1, 1 << rank)))
    return f"{rank}WS:" + ",".join(map(str, min(signatures)))


def type_counts(rows):
    counts = collections.Counter(combination_type(w) for _, w in rows)
    return dict(sorted(counts.items()))


def normal_forms(output, annotated=True):
    lines = output.splitlines()
    forms = []
    position = 0
    while position < len(lines):
        line = lines[position].strip()
        position += 1
        if not line:
            continue
        pattern = (
            r"(\d+)\s+(\d+)\s+Normal form of vertices of P(?:\s+perm=\S+)?"
            if annotated else r"(\d+)\s+(\d+)"
        )
        header = re.fullmatch(pattern, line)
        if not header:
            raise AssertionError(f"Unexpected normal-form output: {line}")
        dimension, vertices = map(int, header.groups())
        matrix = []
        for _ in range(dimension):
            if position >= len(lines):
                raise AssertionError("Truncated normal form")
            row = tuple(map(int, lines[position].split()))
            position += 1
            if len(row) != vertices:
                raise AssertionError(f"Wrong normal-form matrix width: {row}")
            matrix.append(row)
        forms.append((dimension, vertices, tuple(matrix)))
    return forms


def serialize_forms(forms):
    return "".join(
        f"{dimension} {vertices}\n"
        + "".join(" ".join(map(str, row)) + "\n" for row in matrix)
        for dimension, vertices, matrix in forms
    )


def compute_forms(bin_dir, capacity, rows):
    data = "".join(line.split(" M:")[0] + "\n" for line, _ in rows)
    output = run(bin_dir / f"poly-{capacity}d.x", ["-fN"], data=data).stdout
    return normal_forms(output)
