#!/usr/bin/env bash
# build.sh — Build the polytope classifier with optional PGO
#
# Usage:
#   ./build.sh              # Regular optimised build (cmake)
#   ./build.sh pgo          # PGO: instrument → profile → rebuild (manual gcc)
#   ./build.sh clean        # Remove build artefacts
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
BUILD_DIR="$SCRIPT_DIR/build"
PALP_DIR="$REPO_ROOT/PALP"

do_cmake_build() {
    mkdir -p "$BUILD_DIR"
    cmake -S "$SCRIPT_DIR" -B "$BUILD_DIR" \
        -DCMAKE_BUILD_TYPE=Release \
        -GNinja 2>&1

    cmake --build "$BUILD_DIR" --parallel "$(nproc)" 2>&1

    echo ""
    echo "Binary: $BUILD_DIR/classifier"
}

do_pgo_build() {
    local PALP_SOURCES=(
        "$PALP_DIR/Coord.c"
        "$PALP_DIR/Rat.c"
        "$PALP_DIR/Vertex.c"
        "$PALP_DIR/Polynf.c"
        "$PALP_DIR/LG.c"
    )
    local PALP_DEFINES="-DPOLY_Dmax=5 -DPALP_FAST_ASSERT -DPALP_THREADSAFE -DCEQ_Nmax=2048"
    local COMMON_CFLAGS="-O3 -march=native -flto -funroll-loops -fomit-frame-pointer"
    local SAMPLE_INPUT="$REPO_ROOT/samples/reflexive"
    local PROFILE_ROWS=500000

    if [ ! -d "$SAMPLE_INPUT" ] || [ -z "$(ls "$SAMPLE_INPUT"/*.parquet 2>/dev/null)" ]; then
        echo "Error: No parquet files in $SAMPLE_INPUT for PGO training."
        exit 1
    fi

    mkdir -p "$BUILD_DIR/pgo"

    build_lib() {
        local extra="${1:-}"
        local objs=()
        for src in "${PALP_SOURCES[@]}"; do
            local base obj
            base=$(basename "$src" .c)
            obj="$BUILD_DIR/${base}.o"
            gcc -c $COMMON_CFLAGS $PALP_DEFINES $extra -w -o "$obj" "$src"
            objs+=("$obj")
        done
        gcc -c $COMMON_CFLAGS $extra -w -o "$BUILD_DIR/palp_globals.o" \
            "$SCRIPT_DIR/palp_globals.c"
        objs+=("$BUILD_DIR/palp_globals.o")
        ar rcs "$BUILD_DIR/libpalp.a" "${objs[@]}"
    }

    build_exe() {
        local extra="${1:-}"
        local ARROW_INC ARROW_LIB
        ARROW_INC=$(pkg-config --cflags-only-I arrow 2>/dev/null || echo "-I/usr/include")
        ARROW_LIB=$(pkg-config --libs arrow parquet 2>/dev/null || echo "-larrow -lparquet")
        g++ $COMMON_CFLAGS $PALP_DEFINES $extra \
            -std=c++20 \
            -I"$PALP_DIR" -I"$SCRIPT_DIR" $ARROW_INC \
            -o "$BUILD_DIR/classifier" \
            "$SCRIPT_DIR/classifier.cpp" \
            -L"$BUILD_DIR" -lpalp \
            $ARROW_LIB -lpthread
    }

    echo "=== PGO Phase 1: Instrumented build ==="
    find "$BUILD_DIR" -name '*.gcda' -delete 2>/dev/null || true
    build_lib "-fprofile-generate=$BUILD_DIR/pgo"
    build_exe "-fprofile-generate=$BUILD_DIR/pgo"

    echo "=== PGO Phase 2: Collecting profile ($PROFILE_ROWS rows) ==="
    "$BUILD_DIR/classifier" \
        --input "$SAMPLE_INPUT" \
        --output "$BUILD_DIR/pgo_scratch" \
        --benchmark "$PROFILE_ROWS" \
        --threads "$(nproc)" 2>&1 | tail -5
    rm -rf "$BUILD_DIR/pgo_scratch"

    echo "=== PGO Phase 3: Optimised rebuild ==="
    find "$BUILD_DIR" -name '*.o' -delete
    rm -f "$BUILD_DIR/libpalp.a"
    build_lib "-fprofile-use=$BUILD_DIR/pgo -fprofile-correction"
    build_exe "-fprofile-use=$BUILD_DIR/pgo -fprofile-correction"
    strip "$BUILD_DIR/classifier"

    echo ""
    echo "PGO build complete: $BUILD_DIR/classifier"
    ls -lh "$BUILD_DIR/classifier"
}

do_asan_build() {
    local PALP_SOURCES=(
        "$PALP_DIR/Coord.c"
        "$PALP_DIR/Rat.c"
        "$PALP_DIR/Vertex.c"
        "$PALP_DIR/Polynf.c"
        "$PALP_DIR/LG.c"
    )
    local PALP_DEFINES="-DPOLY_Dmax=5 -DPALP_FAST_ASSERT -DPALP_THREADSAFE -DCEQ_Nmax=2048"
    # -O1 keeps the code comprehensible; -g gives source-level backtraces.
    # No -flto: ASAN and LTO do not mix reliably.
    local ASAN_FLAGS="-fsanitize=address -fno-omit-frame-pointer -g -O1"
    local ASAN_DIR="$SCRIPT_DIR/build-asan"

    mkdir -p "$ASAN_DIR"

    echo "=== ASAN build ==="
    local objs=()
    for src in "${PALP_SOURCES[@]}"; do
        local base obj
        base=$(basename "$src" .c)
        obj="$ASAN_DIR/${base}.o"
        gcc -c $ASAN_FLAGS $PALP_DEFINES -w -o "$obj" "$src"
        objs+=("$obj")
    done
    gcc -c $ASAN_FLAGS -w -o "$ASAN_DIR/palp_globals.o" "$SCRIPT_DIR/palp_globals.c"
    objs+=("$ASAN_DIR/palp_globals.o")
    ar rcs "$ASAN_DIR/libpalp.a" "${objs[@]}"

    local ARROW_INC ARROW_LIB
    ARROW_INC=$(pkg-config --cflags-only-I arrow 2>/dev/null || echo "-I/usr/include")
    ARROW_LIB=$(pkg-config --libs arrow parquet 2>/dev/null || echo "-larrow -lparquet")

    g++ $ASAN_FLAGS $PALP_DEFINES \
        -std=c++20 \
        -I"$PALP_DIR" -I"$SCRIPT_DIR" $ARROW_INC \
        -o "$ASAN_DIR/classifier" \
        "$SCRIPT_DIR/classifier.cpp" \
        -L"$ASAN_DIR" -lpalp \
        $ARROW_LIB -lpthread

    echo ""
    echo "ASAN binary: $ASAN_DIR/classifier"
    echo ""
    echo "Run example (resume from checkpoint, files 10-11):"
    echo "  ASAN_OPTIONS=detect_leaks=0 $ASAN_DIR/classifier \\"
    echo "      --input samples/reflexive --output results/run-500 \\"
    echo "      --start 10 --end 11 --resume --threads 32"
}

case "${1:-build}" in
    clean) rm -rf "$BUILD_DIR" "$SCRIPT_DIR/build-asan" ;;
    pgo)   do_pgo_build ;;
    asan)  do_asan_build ;;
    build) do_cmake_build ;;
    *)     echo "Usage: $0 [build|pgo|asan|clean]"; exit 1 ;;
esac

echo ""
echo "Usage examples:"
echo "  # Benchmark on 100K rows"
echo "  $BUILD_DIR/classifier --input ./samples/reflexive --output ./results --benchmark 100000"
echo ""
echo "  # Process all files in a directory"
echo "  $BUILD_DIR/classifier --input ./data --output ./results --threads 32"
echo ""
echo "  # Process file range (for multi-runner)"
echo "  $BUILD_DIR/classifier --input ./data --output ./results --start 0 --end 999"
