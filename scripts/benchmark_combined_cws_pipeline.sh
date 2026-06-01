#!/usr/bin/env bash
# Reproducible local benchmark for the combined-CWS CPU pipeline.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
. "$SCRIPT_DIR/env_local.sh"

STRUCTURE_ID="${STRUCTURE_ID:-5}"
THREADS="${THREADS:-$(nproc)}"
RUN_DIR="${RUN_DIR:-$REPO_ROOT/results/benchmarks/combined-$(date +%Y%m%d-%H%M%S)-s${STRUCTURE_ID}}"

mkdir -p "$RUN_DIR/input" "$RUN_DIR/output" "$RUN_DIR/logs"

"$SCRIPT_DIR/discover_gpu_env.sh" > "$RUN_DIR/logs/environment.txt"

cmake -S "$REPO_ROOT/src/classify" -B "$REPO_ROOT/src/classify/build" \
    -DCMAKE_BUILD_TYPE=Release -GNinja >/dev/null
cmake --build "$REPO_ROOT/src/classify/build" --parallel "$THREADS" >/dev/null
make -C "$REPO_ROOT/PALP" -f GNUmakefile cws-5d.x >/dev/null

echo "=== combined-CWS benchmark ===" | tee "$RUN_DIR/logs/benchmark.log"
echo "structure_id=$STRUCTURE_ID" | tee -a "$RUN_DIR/logs/benchmark.log"
echo "threads=$THREADS" | tee -a "$RUN_DIR/logs/benchmark.log"
echo "run_dir=$RUN_DIR" | tee -a "$RUN_DIR/logs/benchmark.log"

SECONDS=0
"$SCRIPT_DIR/generate_dim5_cws_parquet.sh" \
    --structure-id "$STRUCTURE_ID" \
    --output-dir "$RUN_DIR/input" 2>&1 | tee "$RUN_DIR/logs/generate.log"
GEN_SECONDS=$SECONDS

SECONDS=0
"$REPO_ROOT/src/classify/build/classifier" \
    --input "$RUN_DIR/input" \
    --output "$RUN_DIR/output" \
    --threads "$THREADS" 2>&1 | tee "$RUN_DIR/logs/classifier.log"
CLASSIFY_SECONDS=$SECONDS

SECONDS=0
"$REPO_ROOT/src/classify/build/add_nf" \
    --input "$RUN_DIR/output/unique_polytopes.parquet" \
    --output "$RUN_DIR/output/enriched.parquet" \
    --threads "$THREADS" \
    --verify-hash 2>&1 | tee "$RUN_DIR/logs/add_nf.log"
ADD_NF_SECONDS=$SECONDS

{
    echo "generate_seconds=$GEN_SECONDS"
    echo "classify_seconds=$CLASSIFY_SECONDS"
    echo "add_nf_seconds=$ADD_NF_SECONDS"
    cat "$RUN_DIR/output/summary.json"
} | tee -a "$RUN_DIR/logs/benchmark.log"

echo "Benchmark artifacts: $RUN_DIR"
