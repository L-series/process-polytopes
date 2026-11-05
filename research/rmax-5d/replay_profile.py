#!/usr/bin/env python3
"""Replay bounded profiling from stored representative weights, without the large catalogues."""

import argparse
import ast
import datetime
import hashlib
import json
import os
from pathlib import Path
import shutil
import subprocess
import sys

ROOT = Path(__file__).resolve().parents[2]
SOURCE = Path(__file__).resolve().parent / "ws-profile"
OLD_ROOT = "/home/ahat01/repos/process-polytopes"
PHASES = [
    "native", "lattices", "cover", "determinants", "maximality",
    "native-cover", "partition", "random-cover", "tree", "certificates",
]


class RuntimePaths(ast.NodeTransformer):
    def __init__(self, root, output):
        self.root, self.output = root, output

    def visit_Constant(self, node):
        if not isinstance(node.value, str):
            return node
        value = node.value
        if value == OLD_ROOT or value.startswith(OLD_ROOT + "/"):
            value = str(self.root) + value[len(OLD_ROOT):]
        elif value == "PALP/poly-5d.x":
            value = str(self.output / "poly-5d.x")
        elif value == "PALP/class-5d.x":
            value = str(self.output / "class-5d.x")
        elif value == "2026-11-04":
            value = datetime.date.today().isoformat()
        return ast.copy_location(ast.Constant(value=value), node)


def compile_binary(cc, flags, sources, output, includes):
    command = [cc, "-O3", "-g", "-DPOLY_Dmax=5", *flags]
    for directory in includes:
        command += ["-I", str(directory)]
    command += [str(p) for p in sources] + ["-lm", "-o", str(output)]
    with output.with_suffix(".build.log").open("w") as log:
        subprocess.run(command, stdout=log, stderr=subprocess.STDOUT, check=True)


def build(root, work, cc):
    palp = root / "PALP"
    common = [palp / name for name in ["Coord.c", "Rat.c", "Vertex.c", "Polynf.c"]]
    for name in ["native", "lattices"]:
        compile_binary(cc, [], [work / f"{name}.c", *common, palp / "LG.c"],
                       work / name, [palp])
    compile_binary(cc, ["-shared", "-fPIC"], [work / "hull.c", *common, palp / "LG.c"],
                   work / "hull.so", [palp])
    compile_binary(cc, [], [palp / "poly.c", *common, palp / "LG.c"],
                   work / "poly-5d.x", [palp])
    classifier = [palp / name for name in ["class.c", "Subpoly.c", "Subadd.c", "Subdb.c"]]
    compile_binary(cc, [], [*classifier, *common], work / "class-5d.x", [palp])
    subprocess.run([cc, "-O3", "-DPOLY_Dmax=5", "-I", str(palp), "-c",
                    str(work / "counts.c"), "-o", str(work / "counts.o")], check=True)
    compile_binary(cc, ["-finstrument-functions"], [*classifier, *common, work / "counts.o"],
                   work / "class-counted", [palp])
    wide = SOURCE / "backend128"
    compile_binary(cc, [], sorted(wide.glob("*.c")), work / "class-128", [palp])


def run(work, script, args=(), overrides=None):
    env = {**os.environ, "PYTHONDONTWRITEBYTECODE": "1",
           "OPENBLAS_NUM_THREADS": "1", "OMP_NUM_THREADS": "1"}
    for key in ["WS_STACK_MB", "WS_CLASS", "WS_NO_COUNTER", "WS_FAILED_ONLY", "WS_RUN_LABEL"]:
        env.pop(key, None)
    env.update(overrides or {})
    subprocess.run([sys.executable, str(work / script), *args], cwd=work, env=env, check=True)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--work-dir", required=True, type=Path,
                        help="New output directory; existing directories are never overwritten")
    parser.add_argument("--repo-root", type=Path, default=ROOT)
    parser.add_argument("--cc", default="cc")
    parser.add_argument("--stages", default="native,lattices",
                        help="'all' or a comma-separated subset of " + ",".join(PHASES))
    args = parser.parse_args()
    stages = PHASES if args.stages == "all" else args.stages.split(",")
    if any(stage not in PHASES for stage in stages):
        parser.error("Unknown profiling stage")
    selected = set(stages)
    if selected - {"native"}:
        selected.add("native")
    if selected & {"native-cover", "partition"}:
        selected.add("cover")
    if "tree" in selected:
        selected.add("lattices")
    stages = [stage for stage in PHASES if stage in selected]
    if not (args.repo_root / "PALP/Coord.c").is_file():
        parser.error("Initialize the pinned PALP submodule first")
    work = args.work_dir.resolve()
    work.mkdir(parents=True, exist_ok=False)
    root = args.repo_root.resolve()
    for path in SOURCE.glob("*.py"):
        tree = RuntimePaths(root, work).visit(ast.parse(path.read_text()))
        ast.fix_missing_locations(tree)
        (work / path.name).write_text(ast.unparse(tree) + "\n")
    for path in SOURCE.glob("*.c"):
        shutil.copy2(path, work / path.name)
    for name in ["sample.json", "inventory.json"]:
        shutil.copy2(SOURCE / name, work / name)
    build(root, work, args.cc)
    sample_text = (work / "sample.json").read_text()
    historical = json.loads(sample_text)
    first = {**historical, "records": [r for r in historical["records"] if r["id"] < 154]}
    for stage in stages:
        print(f"=== {stage} ===", flush=True)
        if stage in ["cover", "determinants", "maximality"]:
            (work / "sample.json").write_text(json.dumps(first))
        else:
            (work / "sample.json").write_text(sample_text)
        if stage == "native":
            run(work, "native_runs.py")
        elif stage == "lattices":
            run(work, "lattice_runs.py")
        elif stage in ["cover", "determinants"]:
            run(work, "searches.py", [stage])
        elif stage == "maximality":
            run(work, "searches.py", ["maximality"])
            shutil.copy2(work / "maximality.jsonl", work / "maximality-default-stack.jsonl")
            run(work, "searches.py", ["maximality"], {"WS_STACK_MB": "256"})
            run(work, "searches.py", ["maximality"], {
                "WS_STACK_MB": "1024", "WS_CLASS": str(work / "class-128"),
                "WS_NO_COUNTER": "1", "WS_FAILED_ONLY": "1",
                "WS_RUN_LABEL": "maximality-wide128.jsonl"})
        elif stage == "native-cover":
            run(work, "native_cover.py")
        elif stage == "partition":
            run(work, "native_cover.py", ["partition"])
        elif stage == "random-cover":
            run(work, "random_cover.py")
        elif stage == "tree":
            run(work, "lattice_tree.py")
        elif stage == "certificates":
            run(work, "certificates.py")
    (work / "sample.json").write_text(sample_text)
    (work / "replay-provenance.json").write_text(json.dumps({
        "date": datetime.datetime.now(datetime.timezone.utc).isoformat(),
        "stages": stages, "historical_sample_sha256":
            hashlib.sha256((SOURCE / "sample.json").read_bytes()).hexdigest(),
        "note": "Stored representative weights replay scientific probes without reading the original "
                "catalogues. Original sampling/I/O metadata are historical. Timings and censored "
                "node counts will vary by host; no full classification is attempted.",
    }, indent=2) + "\n")
    print(f"Results saved in {work}")


if __name__ == "__main__":
    main()
