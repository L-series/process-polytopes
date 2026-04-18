#!/usr/bin/env python3

from __future__ import annotations

import argparse
from collections import Counter, defaultdict
from fractions import Fraction
from itertools import combinations
from pathlib import Path
import subprocess
import sys


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--palp-root", required=True)
    parser.add_argument("--files", nargs="+", required=True)
    parser.add_argument("--row-count", type=int, default=3)
    parser.add_argument("--ambient", type=int, default=8)
    parser.add_argument("--top-classes", type=int, default=5)
    return parser.parse_args()


def is_reflexive(line: str) -> bool:
    return (" N:" in line) and (" F:" not in line)


def split_prefix_ints(line: str) -> tuple[int, ...]:
    ints: list[int] = []
    for token in line.split():
        if ":" in token:
            break
        ints.append(int(token))
    return tuple(ints)


def parse_rows(line: str, row_count: int, ambient: int) -> tuple[tuple[int, tuple[int, ...], tuple[int, ...]], ...]:
    values = split_prefix_ints(line)
    stride = ambient + 1
    expected = row_count * stride
    if len(values) != expected:
        raise ValueError(f"expected {expected} integers before metadata, got {len(values)}")

    rows = []
    for index in range(row_count):
        chunk = values[index * stride : (index + 1) * stride]
        degree = chunk[0]
        weights = tuple(chunk[1:])
        nonzero = tuple(weight for weight in weights if weight)
        rows.append((degree, weights, nonzero))
    return tuple(rows)


def rref_signature(rows: tuple[tuple[int, tuple[int, ...], tuple[int, ...]], ...]) -> tuple[tuple[int, ...], ...]:
    matrix = [[Fraction(value) for value in row[1]] for row in rows]
    row_count = len(matrix)
    col_count = len(matrix[0])
    pivot_row = 0

    for pivot_col in range(col_count):
        pivot = None
        for candidate in range(pivot_row, row_count):
            if matrix[candidate][pivot_col] != 0:
                pivot = candidate
                break
        if pivot is None:
            continue
        matrix[pivot_row], matrix[pivot] = matrix[pivot], matrix[pivot_row]
        pivot_value = matrix[pivot_row][pivot_col]
        matrix[pivot_row] = [value / pivot_value for value in matrix[pivot_row]]
        for candidate in range(row_count):
            if candidate == pivot_row:
                continue
            factor = matrix[candidate][pivot_col]
            if factor == 0:
                continue
            matrix[candidate] = [
                value - factor * pivot_value
                for value, pivot_value in zip(matrix[candidate], matrix[pivot_row])
            ]
        pivot_row += 1
        if pivot_row == row_count:
            break

    signature = []
    for row in matrix:
        if all(value == 0 for value in row):
            continue
        denominator_lcm = 1
        for value in row:
            denominator_lcm = denominator_lcm * value.denominator // gcd(denominator_lcm, value.denominator)
        integer_row = [int(value * denominator_lcm) for value in row]
        row_gcd = 0
        for value in integer_row:
            row_gcd = gcd(row_gcd, abs(value))
        if row_gcd:
            integer_row = [value // row_gcd for value in integer_row]
        for value in integer_row:
            if value != 0:
                if value < 0:
                    integer_row = [-entry for entry in integer_row]
                break
        signature.append(tuple(integer_row))
    signature.sort()
    return tuple(signature)


def gcd(left: int, right: int) -> int:
    while right:
        left, right = right, left % right
    return abs(left)


def row_signature(rows: tuple[tuple[int, tuple[int, ...], tuple[int, ...]], ...]) -> tuple[tuple[int, ...], ...]:
    return tuple(sorted((row[0],) + row[2] for row in rows))


def degree_signature(rows: tuple[tuple[int, tuple[int, ...], tuple[int, ...]], ...]) -> tuple[int, ...]:
    return tuple(sorted(row[0] for row in rows))


def column_signature(rows: tuple[tuple[int, tuple[int, ...], tuple[int, ...]], ...]) -> tuple[tuple[int, ...], ...]:
    ambient = len(rows[0][1])
    columns = []
    for column in range(ambient):
        values = tuple(row[1][column] for row in rows)
        if any(values):
            columns.append(values)
    columns.sort()
    return tuple(columns)


def primitive_column_signature(rows: tuple[tuple[int, tuple[int, ...], tuple[int, ...]], ...]) -> tuple[tuple[int, ...], ...]:
    ambient = len(rows[0][1])
    columns = []
    for column in range(ambient):
        values = tuple(row[1][column] for row in rows)
        if not any(values):
            continue
        column_gcd = 0
        for value in values:
            column_gcd = gcd(column_gcd, abs(value))
        columns.append(tuple(value // column_gcd for value in values))
    columns.sort()
    return tuple(columns)


def column_sum_signature(rows: tuple[tuple[int, tuple[int, ...], tuple[int, ...]], ...]) -> tuple[int, ...]:
    ambient = len(rows[0][1])
    sums = []
    for column in range(ambient):
        total = sum(row[1][column] for row in rows)
        if total:
            sums.append(total)
    sums.sort()
    return tuple(sums)


def weight_multiset_signature(rows: tuple[tuple[int, tuple[int, ...], tuple[int, ...]], ...]) -> tuple[int, ...]:
    weights = []
    for row in rows:
        weights.extend(row[2])
    weights.sort()
    return tuple(weights)


def slot_signature(rows: tuple[tuple[int, tuple[int, ...], tuple[int, ...]], ...], index: int) -> tuple[int, ...]:
    row = rows[index]
    return (row[0],) + row[2]


def ordered_pair_signature(
    rows: tuple[tuple[int, tuple[int, ...], tuple[int, ...]], ...],
    left: int,
    right: int,
) -> tuple[tuple[int, ...], tuple[int, ...]]:
    return (slot_signature(rows, left), slot_signature(rows, right))


def sorted_pair_signature(
    rows: tuple[tuple[int, tuple[int, ...], tuple[int, ...]], ...],
    left: int,
    right: int,
) -> tuple[tuple[int, ...], tuple[int, ...]]:
    pair = [slot_signature(rows, left), slot_signature(rows, right)]
    pair.sort()
    return tuple(pair)


BASE_FEATURES = {
    "row": row_signature,
    "degree": degree_signature,
    "column": column_signature,
    "primitive-column": primitive_column_signature,
    "column-sum": column_sum_signature,
    "weights": weight_multiset_signature,
    "row-rref": rref_signature,
}


def feature_extractors_for(row_count: int) -> dict[str, object]:
    features = dict(BASE_FEATURES)

    for index in range(row_count):
        feature_name = f"slot{index + 1}"
        features[feature_name] = lambda rows, row_index=index: slot_signature(rows, row_index)

    for left, right in combinations(range(row_count), 2):
        ordered_name = f"slot{left + 1}{right + 1}"
        sorted_name = f"sorted-slot{left + 1}{right + 1}"
        features[ordered_name] = (
            lambda rows, left_index=left, right_index=right: ordered_pair_signature(
                rows, left_index, right_index
            )
        )
        features[sorted_name] = (
            lambda rows, left_index=left, right_index=right: sorted_pair_signature(
                rows, left_index, right_index
            )
        )

    return features


def cache_path_for(source: Path) -> Path:
    return source.with_name(source.stem + ".reflexive.nf-lines.txt")


def load_or_compute_normal_forms(lines: list[str], source: Path, palp_root: Path) -> list[str]:
    cache_path = cache_path_for(source)
    if cache_path.exists():
        cached = cache_path.read_text().splitlines()
        if len(cached) == len(lines):
            return cached

    poly_path = palp_root / "poly.x"
    payload = "\n".join(lines) + "\n"
    completed = subprocess.run(
        [str(poly_path), "-f", "-N"],
        input=payload,
        text=True,
        capture_output=True,
        check=True,
        cwd=palp_root,
    )
    normal_forms = completed.stdout.splitlines()
    if len(normal_forms) != len(lines):
        raise RuntimeError(
            f"normal form count mismatch for {source}: expected {len(lines)}, got {len(normal_forms)}"
        )
    cache_path.write_text("\n".join(normal_forms) + "\n")
    return normal_forms


def combined_feature_names(feature_names: list[str]) -> list[tuple[str, ...]]:
    names = list(feature_names)
    combos: list[tuple[str, ...]] = []
    for size in (1, 2, 3):
        combos.extend(combinations(names, size))
    return combos


def build_feature_map(
    rows: tuple[tuple[int, tuple[int, ...], tuple[int, ...]], ...],
    feature_extractors: dict[str, object],
) -> dict[str, object]:
    return {name: extractor(rows) for name, extractor in feature_extractors.items()}


def summarize_dataset(source: Path, palp_root: Path, row_count: int, ambient: int, top_classes: int) -> None:
    all_lines = source.read_text().splitlines()
    reflexive_lines = [line for line in all_lines if is_reflexive(line)]
    normal_forms = load_or_compute_normal_forms(reflexive_lines, source, palp_root)
    feature_extractors = feature_extractors_for(row_count)
    slot_feature_names = [f"slot{index}" for index in range(1, row_count + 1)]

    records = []
    for line, normal_form in zip(reflexive_lines, normal_forms):
        rows = parse_rows(line, row_count, ambient)
        records.append(
            {
                "line": line,
                "nf": normal_form,
                "rows": rows,
                "features": build_feature_map(rows, feature_extractors),
            }
        )

    nf_groups: dict[str, list[dict[str, object]]] = defaultdict(list)
    for record in records:
        nf_groups[record["nf"]].append(record)

    duplicate_group_sizes = sorted((len(group) for group in nf_groups.values() if len(group) > 1), reverse=True)
    baseline_nf_set = set(nf_groups)

    print(f"\n== {source.name} ==")
    print(f"reflexive_lines={len(records)}")
    print(f"unique_normal_forms={len(baseline_nf_set)}")
    print(f"duplicate_extra_lines={len(records) - len(baseline_nf_set)}")
    print(f"duplicate_nf_classes={sum(1 for group in nf_groups.values() if len(group) > 1)}")
    print(f"max_nf_multiplicity={duplicate_group_sizes[0] if duplicate_group_sizes else 1}")

    for feature_name in ("row", "column", "primitive-column", "row-rref"):
        distinct_counter = Counter(
            len({record["features"][feature_name] for record in group})
            for group in nf_groups.values()
            if len(group) > 1
        )
        distinct_gt_one = sum(count for distinct_count, count in distinct_counter.items() if distinct_count > 1)
        print(
            f"duplicate_classes_with_multiple_{feature_name}_signatures={distinct_gt_one}"
        )

    print("top_duplicate_classes:")
    ranked_groups = sorted(nf_groups.values(), key=len, reverse=True)[:top_classes]
    for index, group in enumerate(ranked_groups, start=1):
        row_sig_count = len({record["features"]["row"] for record in group})
        col_sig_count = len({record["features"]["column"] for record in group})
        rref_sig_count = len({record["features"]["row-rref"] for record in group})
        slot_counts = [
            len({record["features"][feature_name] for record in group})
            for feature_name in slot_feature_names
        ]
        print(
            f"  class_{index}: multiplicity={len(group)} row_sigs={row_sig_count} "
            f"column_sigs={col_sig_count} row_rref_sigs={rref_sig_count} "
            f"slot_sigs={slot_counts}"
        )
        print(f"    sample_cws={group[0]['line']}")

    print("heuristic_candidates:")
    for combo in combined_feature_names(list(feature_extractors)):
        groups: dict[tuple[object, ...], list[dict[str, object]]] = defaultdict(list)
        for record in records:
            key = tuple(record["features"][name] for name in combo)
            groups[key].append(record)

        selected = [min(group, key=lambda record: record["line"]) for group in groups.values()]
        selected_nf_set = {record["nf"] for record in selected}
        mixed_group_count = sum(
            1 for group in groups.values() if len({record["nf"] for record in group}) > 1
        )
        exact = selected_nf_set == baseline_nf_set
        if exact or mixed_group_count <= max(10, len(groups) // 1000):
            print(
                f"  {'+'.join(combo)}: groups={len(groups)} selected_nf={len(selected_nf_set)} "
                f"exact={int(exact)} mixed_groups={mixed_group_count}"
            )


def main() -> int:
    args = parse_args()
    palp_root = Path(args.palp_root).resolve()
    for name in args.files:
        summarize_dataset((palp_root / name).resolve(), palp_root, args.row_count, args.ambient, args.top_classes)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())