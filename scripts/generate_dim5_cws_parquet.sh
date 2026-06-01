#!/usr/bin/env bash
# Generate combined-CWS Parquet from PALP's recovered dimension-5 generator.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
. "$SCRIPT_DIR/env_local.sh"

STRUCTURE_ID=""
OUTPUT_DIR=""
SHARD_COUNT=""
SHARD_INDEX=""

usage() {
    cat >&2 <<EOF
Usage: $0 --structure-id <2..47> --output-dir <dir> [--shards <n> --worker <k>]

Generates one Parquet file containing PALP cws-5d.x -c5 -s<structure-id> rows.
EOF
}

while [[ $# -gt 0 ]]; do
    case "$1" in
        --structure-id) STRUCTURE_ID="$2"; shift 2 ;;
        --output-dir) OUTPUT_DIR="$2"; shift 2 ;;
        --shards) SHARD_COUNT="$2"; shift 2 ;;
        --worker) SHARD_INDEX="$2"; shift 2 ;;
        -h|--help) usage; exit 0 ;;
        *) echo "Unknown option: $1" >&2; usage; exit 1 ;;
    esac
done

if [[ -z "$STRUCTURE_ID" || -z "$OUTPUT_DIR" ]]; then
    usage
    exit 1
fi
if [[ "$STRUCTURE_ID" -lt 2 || "$STRUCTURE_ID" -gt 47 ]]; then
    echo "structure-id must be in the combined range 2..47" >&2
    exit 1
fi
if [[ -n "$SHARD_COUNT" && -z "$SHARD_INDEX" ]] || [[ -z "$SHARD_COUNT" && -n "$SHARD_INDEX" ]]; then
    echo "--shards and --worker must be supplied together" >&2
    exit 1
fi

if [[ "$STRUCTURE_ID" -le 7 ]]; then
    NW=2
elif [[ "$STRUCTURE_ID" -le 32 ]]; then
    NW=3
elif [[ "$STRUCTURE_ID" -le 45 ]]; then
    NW=4
else
    NW=5
fi
N=$((NW + 5))

mkdir -p "$OUTPUT_DIR"

if [[ ! -x "$REPO_ROOT/PALP/cws-5d.x" ]]; then
    make -C "$REPO_ROOT/PALP" -f GNUmakefile cws-5d.x >/dev/null
fi
if [[ ! -x "$REPO_ROOT/src/classify/build/cws_to_parquet" ]]; then
    cmake -S "$REPO_ROOT/src/classify" -B "$REPO_ROOT/src/classify/build" \
        -DCMAKE_BUILD_TYPE=Release -GNinja >/dev/null
    cmake --build "$REPO_ROOT/src/classify/build" --target cws_to_parquet \
        --parallel "$(nproc)" >/dev/null
fi

OUT_NAME=$(printf "dim5-cws-structure-%02d" "$STRUCTURE_ID")
SHARD_ARGS=()
if [[ -n "$SHARD_COUNT" ]]; then
    OUT_NAME+=$(printf "-shard-%03d-of-%03d" "$SHARD_INDEX" "$SHARD_COUNT")
    SHARD_ARGS=(-j"$SHARD_COUNT" -k"$SHARD_INDEX")
fi
OUT_PATH="$OUTPUT_DIR/$OUT_NAME.parquet"

echo "Generating structure $STRUCTURE_ID (nw=$NW, N=$N) -> $OUT_PATH" >&2
"$REPO_ROOT/PALP/cws-5d.x" -c5 -s"$STRUCTURE_ID" "${SHARD_ARGS[@]}" |
    "$REPO_ROOT/src/classify/build/cws_to_parquet" \
        --input - \
        --output "$OUT_PATH" \
        --structure-id "$STRUCTURE_ID" \
        --nw "$NW" \
        --N "$N"
