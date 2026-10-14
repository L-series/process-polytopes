#!/usr/bin/env python3
"""Audit compiler-derived proof closures and reviewed statement semantics."""

import argparse
import hashlib
import json
import re
from pathlib import Path

FOUNDATIONS = frozenset({"propext", "Classical.choice", "Quot.sound"})
TOOLCHAIN = "leanprover/lean4:v4.30.0"
MANIFEST_SHA256 = "0d6def3ad257f03d2d0301868135497e764c1121813de38a7edbf81903510c1f"
SEMANTIC_MODULES = (
    "Refpoly.Basic",
    "Refpoly.WeightSystem",
    "Refpoly.KS.Model",
    "Refpoly.KS.Contracts",
    "Refpoly.KS.Pipeline",
)
HOLE_FILES = frozenset(
    {"Refpoly/KS/Implementation.lean", "Refpoly/KS/Obligations.lean"}
)
FORBIDDEN = frozenset(
    {"axiom", "unsafe", "partial", "native_decide", "implemented_by", "extern"}
)


def check_pins(root):
    errors = []
    if (root / "lean-toolchain").read_text().strip() != TOOLCHAIN:
        errors.append("Lean toolchain differs from the reviewed pin")
    digest = hashlib.sha256((root / "lake-manifest.json").read_bytes()).hexdigest()
    if digest != MANIFEST_SHA256:
        errors.append("Dependency manifest differs from the reviewed nine-package lock")
    return errors


def source_tokens(text):
    """Skip nested Lean comments and strings before checking escape hatches."""
    tokens = []
    index = 0
    depth = 0
    quoted = False
    while index < len(text):
        if depth:
            if text.startswith("/-", index):
                depth += 1
                index += 2
            elif text.startswith("-/", index):
                depth -= 1
                index += 2
            else:
                index += 1
        elif quoted:
            if text[index] == "\\":
                index += 2
            elif text[index] == '"':
                quoted = False
                index += 1
            else:
                index += 1
        elif text.startswith("/-", index):
            depth = 1
            index += 2
        elif text.startswith("--", index):
            newline = text.find("\n", index)
            index = len(text) if newline == -1 else newline + 1
        elif text[index] == '"':
            quoted = True
            index += 1
        else:
            match = re.match(r"[A-Za-z_][A-Za-z_0-9]*", text[index:])
            if match:
                tokens.append(match.group())
                index += len(match.group())
            else:
                index += 1
    if depth or quoted:
        raise ValueError("Unterminated Lean comment or string")
    return frozenset(tokens)


def check_sources(root):
    errors = []
    for path in sorted((root / "Refpoly").rglob("*.lean")) + [root / "Refpoly.lean"]:
        relative = path.relative_to(root).as_posix()
        tokens = source_tokens(path.read_text())
        rejected = sorted(tokens & FORBIDDEN)
        if rejected:
            errors.append(f"{relative}: prohibited execution/proof escape hatches: {rejected}")
        if tokens & {"sorry", "admit"} and relative not in HOLE_FILES:
            errors.append(f"{relative}: proof holes outside assigned obligation files")
    return errors


def target_names(targets):
    return [name for names in targets.values() for name in names]


def semantic_snapshot(rows, targets):
    by_name = {row["name"]: row for row in rows}
    signatures = {name: by_name[name]["type"] for name in target_names(targets)}
    # Compiler module names are not guessed from declaration-name prefixes.
    # The exporter records the actual defining module for each constant.
    semantics = {
        row["name"]: {
            "type": row["type"],
            "definition": row["definition"],
            "dependencies": sorted(set(row["dependencies"])),
        }
        for row in rows
        if row["module"] in SEMANTIC_MODULES
        and not row["is_theorem"]
        and not row["name"].startswith("_private.")
    }
    return {"schema": 1, "targets": targets, "signatures": signatures, "semantics": semantics}


def check_lock(rows, targets, lock):
    errors = []
    if lock.get("schema") != 1:
        return ["Unsupported statement-lock schema"]
    if targets != lock["targets"]:
        errors.append("Audit target inventory changed without a reviewed statement-lock update")
    by_name = {row["name"]: row for row in rows}
    for name, expected in lock["signatures"].items():
        if name not in by_name:
            errors.append(f"Missing locked statement: {name}")
        elif by_name[name]["type"] != expected:
            errors.append(f"Statement signature changed: {name}")
    for name, expected in lock["semantics"].items():
        row = by_name.get(name)
        if row is None:
            errors.append(f"Missing locked definition: {name}")
        elif {
            "type": row["type"],
            "definition": row["definition"],
            "dependencies": sorted(set(row["dependencies"])),
        } != expected:
            errors.append(f"Mathematical definition changed: {name}")
    return errors


def check_axioms(rows, targets, strict):
    errors = []
    by_name = {row["name"]: row for row in rows}
    permitted_holes = set(target_names(targets)) - set(targets["closed"])
    for name in target_names(targets):
        if name not in by_name:
            errors.append(f"Missing audit target: {name}")
    for row in rows:
        axioms = set(row["axioms"])
        unexpected = sorted(axioms - FOUNDATIONS - {"sorryAx"})
        if unexpected:
            errors.append(f"{row['name']}: unexpected axioms: {unexpected}")
        if row["name"] in targets["derived"] and row["direct_sorry"]:
            errors.append(f"{row['name']}: derived composition must not have its own hole")
        if "sorryAx" in axioms and (strict or row["name"] not in permitted_holes):
            relation = "direct hole" if row["direct_sorry"] else "transitive hole"
            errors.append(f"{row['name']}: {relation} (sorryAx)")
    return errors


def progress(row):
    unexpected = set(row["axioms"]) - FOUNDATIONS - {"sorryAx"}
    if unexpected:
        return "unexpected axiom"
    if "sorryAx" not in row["axioms"]:
        return "closed"
    return "direct hole" if row["direct_sorry"] else "conditional"


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("mode", choices=("conditional", "strict", "freeze"))
    parser.add_argument("--inventory", required=True, type=Path)
    args = parser.parse_args()
    root = Path(__file__).resolve().parents[1]
    rows = json.loads(args.inventory.read_text())
    targets = json.loads((root / "ci/targets.json").read_text())
    errors = check_pins(root) + check_sources(root)
    errors += check_axioms(rows, targets, strict=args.mode == "strict")
    lock_path = root / "ci/statement-lock.json"
    if args.mode == "freeze":
        if errors:
            parser.exit(1, "\n".join(errors) + "\n")
        lock_path.write_text(json.dumps(semantic_snapshot(rows, targets), indent=2) + "\n")
        print("Wrote statement lock; this command requires prior semantic review.")
        return
    errors += check_lock(rows, targets, json.loads(lock_path.read_text()))
    by_name = {row["name"]: row for row in rows}
    print("Compiler-derived major-obligation status:")
    for name in target_names(targets):
        if name in by_name:
            print(f"  {progress(by_name[name]):12} {name}")
    if errors:
        parser.exit(1, "Proof audit failed:\n" + "\n".join(errors) + "\n")
    print(f"{args.mode.capitalize()} proof audit passed.")


if __name__ == "__main__":
    main()
