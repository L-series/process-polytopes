#!/usr/bin/env bash
# run_distributed.sh — Run polytope classification across multiple machines
#
# This script manages distributed processing of parquet files across
# multiple runner machines.  Each runner processes a non-overlapping
# range of input files.  After all runners complete, a merge step
# combines the checkpoint files into a single result.
#
# Prerequisites on each runner:
#   - This repo cloned at the same path (or adjust REPO_ROOT)
#   - The classifier binary built (./src/classify/build.sh pgo)
#   - Input parquet files accessible (same path or symlinked)
#   - SSH access from the coordinator to each runner
#
# Usage:
#   # Coordinator: launch all runners
#   ./run_distributed.sh launch
#
#   # Wait for all runners, then merge results
#   ./run_distributed.sh merge
#
#   # Or do everything:
#   ./run_distributed.sh all
#
# Configuration: edit the RUNNERS array below.
# ═══════════════════════════════════════════════════════════════════════════

set -euo pipefail

# ── Configuration ─────────────────────────────────────────────────────────

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
CLASSIFIER="$REPO_ROOT/src/classify/build/classifier"

# Total number of parquet files in the dataset
TOTAL_FILES=4000

# Input directory containing parquet files (must be accessible on all runners)
INPUT_DIR="${INPUT_DIR:-$REPO_ROOT/data/reflexive}"

# Output base directory
OUTPUT_BASE="${OUTPUT_BASE:-$REPO_ROOT/results/distributed}"

# Number of threads per runner (auto-detect if not set)
THREADS="${THREADS:-0}"

# ── Runner definitions ────────────────────────────────────────────────────
# Format: "hostname:start_file:end_file"
# Files are 0-indexed. Ranges are inclusive.
#
# With 4000 files and 3 runners, split as:
#   Runner 0: files 0000-1332    (1333 files)
#   Runner 1: files 1333-2665    (1333 files)
#   Runner 2: files 2666-3999    (1334 files)

RUNNERS=(
    "localhost:0:1332"
    # "runner2.local:1333:2665"
    # "runner3.local:2666:3999"
)

# For single-machine testing, process a smaller range:
# RUNNERS=("localhost:0:0")

# ── Functions ─────────────────────────────────────────────────────────────

get_runner_host()  { echo "$1" | cut -d: -f1; }
get_runner_start() { echo "$1" | cut -d: -f2; }
get_runner_end()   { echo "$1" | cut -d: -f3; }

launch_runner() {
    local spec="$1"
    local idx="$2"
    local host start end out_dir

    host=$(get_runner_host "$spec")
    start=$(get_runner_start "$spec")
    end=$(get_runner_end "$spec")
    out_dir="$OUTPUT_BASE/runner-$idx"

    echo "Launching runner $idx on $host: files $start–$end → $out_dir"

    local cmd="$CLASSIFIER \
        --input '$INPUT_DIR' \
        --output '$out_dir' \
        --checkpoint '$out_dir/checkpoints' \
        --start $start \
        --end $end \
        --threads $THREADS"

    if [[ "$host" == "localhost" || "$host" == "$(hostname)" ]]; then
        mkdir -p "$out_dir/checkpoints"
        eval "nohup $cmd > '$out_dir/runner.log' 2>&1 &"
        echo "  PID: $!"
        echo "$!" > "$out_dir/runner.pid"
    else
        ssh "$host" "mkdir -p '$out_dir/checkpoints' && nohup $cmd > '$out_dir/runner.log' 2>&1 &"
        echo "  Launched via SSH"
    fi
}

do_launch() {
    echo "=== Launching ${#RUNNERS[@]} runners ==="
    echo "Input:  $INPUT_DIR"
    echo "Output: $OUTPUT_BASE"
    echo ""

    for i in "${!RUNNERS[@]}"; do
        launch_runner "${RUNNERS[$i]}" "$i"
    done

    echo ""
    echo "All runners launched."
    echo "Monitor with: tail -f $OUTPUT_BASE/runner-*/runner.log"
    echo "Merge when done: $0 merge"
}

do_status() {
    echo "=== Runner Status ==="
    for i in "${!RUNNERS[@]}"; do
        local spec="${RUNNERS[$i]}"
        local host out_dir
        host=$(get_runner_host "$spec")
        out_dir="$OUTPUT_BASE/runner-$i"

        echo ""
        echo "Runner $i ($host):"
        if [ -f "$out_dir/runner.pid" ]; then
            local pid
            pid=$(cat "$out_dir/runner.pid")
            if kill -0 "$pid" 2>/dev/null; then
                echo "  Status: RUNNING (PID $pid)"
            else
                echo "  Status: COMPLETED"
            fi
        else
            echo "  Status: NOT STARTED"
        fi

        if [ -f "$out_dir/summary.json" ]; then
            echo "  Summary:"
            cat "$out_dir/summary.json" | sed 's/^/    /'
        fi

        local ckpts
        ckpts=$(find "$out_dir/checkpoints" -name '*.ckpt' 2>/dev/null | wc -l)
        echo "  Checkpoints: $ckpts"
    done
}

do_merge() {
    echo "=== Merging results from ${#RUNNERS[@]} runners ==="

    local merge_dir="$OUTPUT_BASE/merged"
    mkdir -p "$merge_dir/checkpoints"

    # Collect all checkpoint files from all runners
    local all_ckpts=()
    for i in "${!RUNNERS[@]}"; do
        local out_dir="$OUTPUT_BASE/runner-$i"
        if [ -d "$out_dir/checkpoints" ]; then
            for ckpt in "$out_dir/checkpoints"/*.ckpt; do
                if [ -f "$ckpt" ]; then
                    # Copy with runner prefix to avoid name collisions
                    local dest="$merge_dir/checkpoints/runner${i}-$(basename "$ckpt")"
                    cp "$ckpt" "$dest"
                    all_ckpts+=("$ckpt")
                fi
            done
        fi
    done

    echo "Found ${#all_ckpts[@]} checkpoint files"

    if [ ${#all_ckpts[@]} -eq 0 ]; then
        echo "Error: No checkpoint files found!"
        exit 1
    fi

    # Use the classifier's --merge mode
    "$CLASSIFIER" --merge "$merge_dir/checkpoints" --output "$merge_dir"

    echo ""
    echo "Merged results: $merge_dir/unique_polytopes.parquet"

    # Aggregate statistics
    echo ""
    echo "=== Aggregate Statistics ==="
    local total_cws=0 total_unique=0 total_fail=0
    for i in "${!RUNNERS[@]}"; do
        local summary="$OUTPUT_BASE/runner-$i/summary.json"
        if [ -f "$summary" ]; then
            local cws unique fail
            cws=$(python3 -c "import json; print(json.load(open('$summary'))['total_cws'])" 2>/dev/null || echo 0)
            unique=$(python3 -c "import json; print(json.load(open('$summary'))['unique_polytopes'])" 2>/dev/null || echo 0)
            fail=$(python3 -c "import json; print(json.load(open('$summary'))['failed_cws'])" 2>/dev/null || echo 0)
            total_cws=$((total_cws + cws))
            total_unique=$((total_unique + unique))
            total_fail=$((total_fail + fail))
            echo "  Runner $i: $cws CWS → $unique unique polytopes"
        fi
    done
    echo "  TOTAL: $total_cws CWS → merged unique count in output parquet"
}

do_all() {
    do_launch
    echo ""
    echo "Waiting for all runners to complete..."
    echo "(Press Ctrl+C to stop waiting; runners will continue in background)"

    local all_done=false
    while ! $all_done; do
        sleep 30
        all_done=true
        for i in "${!RUNNERS[@]}"; do
            local out_dir="$OUTPUT_BASE/runner-$i"
            if [ -f "$out_dir/runner.pid" ]; then
                local pid
                pid=$(cat "$out_dir/runner.pid")
                if kill -0 "$pid" 2>/dev/null; then
                    all_done=false
                fi
            fi
        done
        if ! $all_done; then
            do_status 2>/dev/null | grep "Status:" | head -5
        fi
    done

    echo "All runners complete."
    do_merge
}

# ── Main ──────────────────────────────────────────────────────────────────
case "${1:-help}" in
    launch) do_launch ;;
    status) do_status ;;
    merge)  do_merge ;;
    all)    do_all ;;
    *)
        echo "Usage: $0 {launch|status|merge|all}"
        echo ""
        echo "  launch  — Start all runners in background"
        echo "  status  — Show runner status"
        echo "  merge   — Merge results from all runners"
        echo "  all     — Launch, wait, and merge"
        ;;
esac
