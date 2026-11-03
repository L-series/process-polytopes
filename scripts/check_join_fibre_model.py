#!/usr/bin/env python3
"""Pin validate_gjoin_fibres to JoinFibreValidator.lean.

The Lean model consumes `g.count` rows per key using sourceChunks, checks the
same global lexicographic chain, the sequential remaining/weight guards, the
packed shared key and the cats/pairs OR summaries. It proves the chunks cover
the input, the rows are exactly the row-solution set, and each canonical chunk
is exactly the points with its key. The source uses signed-int point arrays;
successful nonnegative values are represented as naturals. The caller's
five-slot `x{}` initialization, supported shape and positive-weight checks
are pinned below because those are preconditions of this validator model.

This is a reviewed transcription boundary, not a verified C++ frontend.
"""
import hashlib
from pathlib import Path
import re
import subprocess
import sys

from test_gpu_ip_source import block

ROOT = Path(__file__).resolve().parents[1]
PIN = "a21ab404549ef7686a1efb360d9b87f6e1d46e3a596f06452316f5c68ee90156"
BUILDER_PIN = "23db39c7c3a5da9b9ad3fc88eec76e38242af15926e8cb8c75e8d4876e38fcd8"


def normalized(text):
    return re.sub(r"//[^\n]*|/\*[\s\S]*?\*/|\s+", "", text)


def main():
    source = (ROOT / "src/gpu/join.cuh").read_text()
    function = block(source, "inline void validate_gjoin_fibres(")
    digest = hashlib.sha256(normalized(function).encode()).hexdigest()
    if "--print" in sys.argv:
        print(digest)
        return
    if digest != PIN:
        raise SystemExit(
            "validate_gjoin_fibres changed; review JoinFibreValidator.lean and repin"
        )

    builder = block(source, "inline void build_gjoin_table(")
    builder_text = normalized(builder)
    if hashlib.sha256(builder_text.encode()).hexdigest() != BUILDER_PIN:
        raise SystemExit("build_gjoin_table changed; review positive weights, degree, padding, enumeration and validator call, then repin")
    anchors = (
        "if(size<1||size>kMaxSize||shared<0||shared>size||shared>4)throwstd::runtime_error(\"gjoin:unsupportedtableshape\");",
        "if(entry.w[p]<=0)throwstd::runtime_error(\"gjoin:nonpositiverowweight\");",
        "std::array<int,5>x{};",
        "std::sort(pts.begin(),pts.end());",
        "validate_gjoin_fibres(en,size,shared,pts,o);",
    )
    if any(anchor not in builder_text for anchor in anchors):
        raise SystemExit(
            "the fibre validator caller shape/order assumptions changed; review JoinFibreValidator.lean"
        )

    # The final fibre guard delegates its solution count to this separately
    # pinned checked DP, so keep that source/model boundary in this audit too.
    subprocess.run(
        [sys.executable, str(ROOT / "scripts/check_join_row_count_model.py")],
        cwd=ROOT,
        check=True,
    )
    print("Fibre validator source and caller assumptions match JoinFibreValidator.lean")


if __name__ == "__main__":
    main()
