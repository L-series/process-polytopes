#!/usr/bin/env bash
# build.sh — Build the process_polytopes worker against CLEAN PALP (v2.21).
#
# PALP objects are compiled from PALP-clean (git worktree at tag v2.21) with
# -DPOLY_Dmax=5; the worker links Arrow/Parquet C++ from the micromamba env.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
BUILD_DIR="$SCRIPT_DIR/build"
PALP_DIR="$REPO_ROOT/PALP-clean"
CLASSIFY_DIR="$REPO_ROOT/src/classify"

if [[ ! -e "$PALP_DIR/Global.h" ]]; then
    echo "ERROR: clean PALP not found at $PALP_DIR"
    echo "  create it with:  git -C $REPO_ROOT/PALP worktree add --detach $PALP_DIR v2.21"
    exit 1
fi

# Toolchain + Arrow from the user-local micromamba env.
ENV="${CONDA_PREFIX:-$HOME/.local/share/micromamba/envs/process-polytopes}"
export PKG_CONFIG_PATH="$ENV/lib/pkgconfig:${PKG_CONFIG_PATH:-}"

ARROW_INC=$(pkg-config --cflags-only-I arrow 2>/dev/null || echo "-I$ENV/include")
ARROW_LIB=$(pkg-config --libs arrow parquet 2>/dev/null || echo "-L$ENV/lib -larrow -lparquet")
RPATH="-Wl,-rpath,$ENV/lib"

# Clean PALP buffer sizing: POLY_Dmax=5 gives POINT_Nmax=2e6, VERT_Nmax=64,
# EQUA_Nmax=1280. CEQ_Nmax (intermediate cutting equations) is bumped to the
# value proven on this dataset by the existing classifier. These are buffer
# sizes only — the PALP algorithm sources are unmodified v2.21.
PALP_DEFINES="-DPOLY_Dmax=5 -DCEQ_Nmax=2048"
CFLAGS="-O3 -march=native -funroll-loops -fomit-frame-pointer"

PALP_SOURCES=(Coord Rat Vertex Polynf LG)

mkdir -p "$BUILD_DIR"
echo "=== Building clean PALP objects (POLY_Dmax=5) ==="
objs=()
for base in "${PALP_SOURCES[@]}"; do
    obj="$BUILD_DIR/${base}.o"
    gcc -c $CFLAGS $PALP_DEFINES -w -o "$obj" "$PALP_DIR/${base}.c"
    objs+=("$obj")
done
gcc -c $CFLAGS -w -o "$BUILD_DIR/palp_globals.o" "$CLASSIFY_DIR/palp_globals.c"
objs+=("$BUILD_DIR/palp_globals.o")
ar rcs "$BUILD_DIR/libpalpclean.a" "${objs[@]}"

echo "=== Building process_polytopes ==="
g++ $CFLAGS $PALP_DEFINES -std=c++17 \
    -I"$PALP_DIR" -I"$SCRIPT_DIR" $ARROW_INC \
    -o "$BUILD_DIR/process_polytopes" \
    "$SCRIPT_DIR/process_polytopes.cpp" \
    -L"$BUILD_DIR" -lpalpclean \
    $ARROW_LIB $RPATH

echo "=== Building rg_count helper ==="
g++ $CFLAGS -std=c++17 $ARROW_INC \
    -o "$BUILD_DIR/rg_count" "$SCRIPT_DIR/rg_count.cpp" \
    $ARROW_LIB $RPATH

echo "=== Building merge_computed helper ==="
g++ $CFLAGS -std=c++17 $ARROW_INC \
    -o "$BUILD_DIR/merge_computed" "$SCRIPT_DIR/merge_computed.cpp" \
    $ARROW_LIB $RPATH

echo "=== Building concat_parquet helper ==="
g++ $CFLAGS -std=c++17 $ARROW_INC \
    -o "$BUILD_DIR/concat_parquet" "$SCRIPT_DIR/concat_parquet.cpp" \
    $ARROW_LIB $RPATH

echo "=== Building repair_ws_dataset helper ==="
g++ $CFLAGS -std=c++17 -I"$CLASSIFY_DIR" $ARROW_INC \
    -o "$BUILD_DIR/repair_ws_dataset" "$SCRIPT_DIR/repair_ws_dataset.cpp" \
    $ARROW_LIB $RPATH

echo "=== Building convert_nf_vertices helper ==="
g++ $CFLAGS -std=c++17 $ARROW_INC \
    -o "$BUILD_DIR/convert_nf_vertices" "$SCRIPT_DIR/convert_nf_vertices.cpp" \
    $ARROW_LIB $RPATH

echo ""
echo "Binary: $BUILD_DIR/process_polytopes"
ls -lh "$BUILD_DIR/process_polytopes" "$BUILD_DIR/rg_count" \
       "$BUILD_DIR/merge_computed" "$BUILD_DIR/concat_parquet" \
       "$BUILD_DIR/repair_ws_dataset" "$BUILD_DIR/convert_nf_vertices"
