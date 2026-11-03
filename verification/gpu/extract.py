#!/usr/bin/env python3
"""Copy named declarations verbatim from src/gpu into one host header for CBMC.

Only CUDA annotations are replaced (`__device__`, `__forceinline__`,
`__noinline__`), `atomicAdd` is a sequential stand-in, and the
limits are declared `static const`; function bodies and record layouts are the
production text.
Usage: extract.py OUTPUT NAME...   (NAME: struct X | function marker text)
"""
from pathlib import Path
import re
import sys

ROOT = Path(__file__).resolve().parents[2]
GPU = ROOT / "src/gpu"
SOURCES = {p.name: p.read_text() for p in sorted(GPU.glob("*.cuh"))}


def block(source, marker, semicolon=False):
    start = source.index(marker)
    begin = source.index("{", start)
    tokens = re.finditer(r'//[^\n]*|/\*[\s\S]*?\*/|"(?:\\.|[^"\\])*"|\'(?:\\.|[^\'\\])*\'|[{}]', source[begin:])
    depth = 0
    for token in tokens:
        if token[0] == "{":
            depth += 1
        elif token[0] == "}":
            depth -= 1
            if depth == 0:
                return source[start:begin + token.end()] + (";\n" if semicolon else "\n")
    raise ValueError(f"unclosed block: {marker}")


def find(marker):
    hits = [name for name, text in SOURCES.items() if marker in text]
    if len(hits) != 1:
        raise SystemExit(f"{marker!r} found in {hits or 'no file'}")
    return SOURCES[hits[0]]


def main():
    out, names = Path(sys.argv[1]), sys.argv[2:]
    common = SOURCES["common.cuh"]
    # CBMC's C++ front end parses neither the host's libstdc++ headers nor
    # constexpr, so the fixed-width type and the four limits are spelled out.
    limits = common[common.index("static constexpr int kMaxSlots"):common.index("inline void check_cuda")]
    parts = ["#pragma once", "namespace std { typedef unsigned long uint64_t; }", '#include "catalogue_bridge.h"',
             "#define __device__", "#define __forceinline__ inline", "#define __noinline__",
             "extern \"C\" double sqrt(double);",
             "inline unsigned long long atomicAdd(unsigned long long *p, unsigned long long v) "
             "{ unsigned long long old = *p; *p = old + v; return old; }",
             limits.replace("static constexpr int", "static const int")]
    for name in names:
        if name.startswith("struct "):
            parts.append(block(find(name + " {"), name + " {", True))
        else:
            parts.append(block(find(name), name))
    out.write_text("\n".join(parts))


if __name__ == "__main__":
    main()
