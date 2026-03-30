#!/usr/bin/env bash
# run_ryzen.sh — Download-process-delete pipeline for the Ryzen runner
#
# Processes files in two phases:
#   Phase 1: Process locally-available parquet files (already downloaded)
#   Phase 2: Download → process → delete cycle for remaining files
#
# Each batch starts with an empty hash map, writes one checkpoint at the
# end, then moves on.  Checkpoints accumulate in CKPT_DIR for final merge.
#
# Usage:
#   ./scripts/run_ryzen.sh
#
# ═══════════════════════════════════════════════════════════════════════════
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"

# ── Configuration ─────────────────────────────────────────────────────────

# Files assigned to this runner (global indices into the 4000-file dataset)
RUNNER_START=0
RUNNER_END=1799

# Local parquet directory
INPUT_DIR="${INPUT_DIR:-$REPO_ROOT/samples/reflexive}"

# Output — use /mnt/external (1.8 TB NVMe, 414 GB free) for checkpoints
OUTPUT_DIR="${OUTPUT_DIR:-/mnt/external/home/ahatziiliou/results/official-ryzen}"
CKPT_DIR="$OUTPUT_DIR/checkpoints"
LOG_DIR="$OUTPUT_DIR/logs"

# Batch size: files per classifier invocation
BATCH_SIZE="${BATCH_SIZE:-150}"

# Threads
THREADS="${THREADS:-0}"

# Classifier binary
CLASSIFIER="${CLASSIFIER:-$REPO_ROOT/src/classify/build/classifier}"

# Download script
DOWNLOADER="$REPO_ROOT/samples/download_cws.py"

# ── Sanity checks ────────────────────────────────────────────────────────

if [[ ! -x "$CLASSIFIER" ]]; then
    echo "Error: classifier binary not found at $CLASSIFIER"
    echo "Build it first: cd src/classify && ./build.sh pgo"
    exit 1
fi

# Source .env for HF_TOKEN if present (needed by download_cws.py)
if [[ -f "$REPO_ROOT/.env" ]]; then
    set -a; source "$REPO_ROOT/.env"; set +a
fi
if [[ -z "${HF_TOKEN:-}" ]]; then
    echo "Warning: HF_TOKEN not set. Downloads will fail unless all files are local."
    echo "  Set it via: export HF_TOKEN=hf_... or create .env in $REPO_ROOT"
fi

mkdir -p "$CKPT_DIR" "$LOG_DIR"

echo "═══════════════════════════════════════════════════════════════"
echo " Ryzen Runner — Download-Process-Delete Pipeline"
echo "═══════════════════════════════════════════════════════════════"
echo " Assigned range: $RUNNER_START .. $RUNNER_END"
echo " Input dir:      $INPUT_DIR"
echo " Output dir:     $OUTPUT_DIR"
echo " Batch size:     $BATCH_SIZE"
echo "═══════════════════════════════════════════════════════════════"
echo ""
echo "CPU:  $(grep -m1 'model name' /proc/cpuinfo | cut -d: -f2 | xargs)"
echo "RAM:  $(awk '/MemTotal/ {printf "%.0f GB", $2/1024/1024}' /proc/meminfo)"
echo "Disk: $(df -h "$INPUT_DIR" | awk 'NR==2 {print $4, "free of", $2}')"
echo ""

# ── Helper: check which files exist locally ──────────────────────────────

files_exist_locally() {
    # Args: start end
    # Returns: count of files present in INPUT_DIR
    local count=0
    for i in $(seq "$1" "$2"); do
        local fname
        fname=$(printf "ws-5d-reflexive-%04d.parquet" "$i")
        [[ -f "$INPUT_DIR/$fname" ]] && (( count++ )) || true
    done
    echo "$count"
}

# ── Helper: download a range of files ────────────────────────────────────

download_range() {
    local dl_start="$1" dl_end="$2"
    echo "  Downloading files $dl_start .. $dl_end ..."
    # download_cws.py puts files into <out-dir>/reflexive/<name>.parquet
    # so we pass the PARENT of INPUT_DIR so they land in the right place.
    local dl_out_dir
    dl_out_dir="$(dirname "$INPUT_DIR")"
    (cd "$REPO_ROOT" && python3 "$DOWNLOADER" \
        --start "$dl_start" \
        --end "$dl_end" \
        --out-dir "$dl_out_dir" \
        --workers 8)
}

# ── Helper: delete processed parquet files ───────────────────────────────

delete_range() {
    local del_start="$1" del_end="$2"
    local count=0
    for i in $(seq "$del_start" "$del_end"); do
        local fname
        fname=$(printf "ws-5d-reflexive-%04d.parquet" "$i")
        if [[ -f "$INPUT_DIR/$fname" ]]; then
            rm -f "$INPUT_DIR/$fname"
            count=$(( count + 1 ))
        fi
    done
    echo "  Deleted $count parquet files ($del_start..$del_end)"
    echo "  Disk free: $(df -h "$INPUT_DIR" | awk 'NR==2 {print $4}')"
}

# ── Main batch loop ──────────────────────────────────────────────────────

BATCH_START=$RUNNER_START
BATCH_NUM=0
TOTAL_BATCHES=$(( (RUNNER_END - RUNNER_START + 1 + BATCH_SIZE - 1) / BATCH_SIZE ))
STARTED_AT=$(date +%s)

while (( BATCH_START <= RUNNER_END )); do
    BATCH_END=$(( BATCH_START + BATCH_SIZE - 1 ))
    (( BATCH_END > RUNNER_END )) && BATCH_END=$RUNNER_END
    BATCH_NUM=$(( BATCH_NUM + 1 ))

    BATCH_TAG=$(printf "batch-%04d-%04d" "$BATCH_START" "$BATCH_END")
    BATCH_OUT="$OUTPUT_DIR/$BATCH_TAG"
    BATCH_LOG="$LOG_DIR/${BATCH_TAG}.log"

    echo ""
    echo "───────────────────────────────────────────────────────────"
    echo " Batch $BATCH_NUM / $TOTAL_BATCHES : files $BATCH_START .. $BATCH_END"
    echo "───────────────────────────────────────────────────────────"

    # Skip if checkpoint already exists (resume support)
    FINAL_CKPT_NAME="${BATCH_TAG}.ckpt"
    if [[ -f "$CKPT_DIR/$FINAL_CKPT_NAME" ]]; then
        echo " ✓ Checkpoint $FINAL_CKPT_NAME exists — skipping"
        BATCH_START=$(( BATCH_END + 1 ))
        continue
    fi

    # Download any missing files in this range
    LOCAL_COUNT=$(files_exist_locally "$BATCH_START" "$BATCH_END")
    NEEDED=$(( BATCH_END - BATCH_START + 1 ))
    if (( LOCAL_COUNT < NEEDED )); then
        echo "  Have $LOCAL_COUNT / $NEEDED files locally"
        download_range "$BATCH_START" "$BATCH_END"
    else
        echo "  All $NEEDED files present locally"
    fi

    # Process
    mkdir -p "$BATCH_OUT/checkpoints"
    echo "  Processing..."

    "$CLASSIFIER" \
        --input "$INPUT_DIR" \
        --output "$BATCH_OUT" \
        --checkpoint "$BATCH_OUT/checkpoints" \
        --start "$BATCH_START" \
        --end "$BATCH_END" \
        --threads "$THREADS" \
        2>&1 | tee "$BATCH_LOG"

    if [[ ${PIPESTATUS[0]} -ne 0 ]]; then
        echo "ERROR: Batch $BATCH_NUM failed! Check $BATCH_LOG"
        exit 1
    fi

    # Copy final checkpoint to central checkpoint dir
    SRC_CKPT=$(ls -t "$BATCH_OUT/checkpoints"/*.ckpt 2>/dev/null | head -1 || true)
    if [[ -n "$SRC_CKPT" ]]; then
        cp "$SRC_CKPT" "$CKPT_DIR/$FINAL_CKPT_NAME"
        echo "  → Saved: $CKPT_DIR/$FINAL_CKPT_NAME ($(du -h "$CKPT_DIR/$FINAL_CKPT_NAME" | cut -f1))"
    fi

    # Delete processed parquet files to free disk for next batch
    delete_range "$BATCH_START" "$BATCH_END"

    # Clean up the per-batch output dir (parquet + sub-checkpoints) to save disk
    rm -rf "$BATCH_OUT"

    # Progress
    ELAPSED=$(( $(date +%s) - STARTED_AT ))
    FILES_DONE=$(( BATCH_END - RUNNER_START + 1 ))
    FILES_LEFT=$(( RUNNER_END - BATCH_END ))
    if (( FILES_DONE > 0 )); then
        SECS_PER_FILE=$(( ELAPSED / FILES_DONE ))
        ETA_SECS=$(( SECS_PER_FILE * FILES_LEFT ))
        ETA_HRS=$(( ETA_SECS / 3600 ))
        ETA_MIN=$(( (ETA_SECS % 3600) / 60 ))
        echo "  Elapsed: $(( ELAPSED / 3600 ))h$(( (ELAPSED % 3600) / 60 ))m  ETA: ${ETA_HRS}h${ETA_MIN}m"
    fi

    BATCH_START=$(( BATCH_END + 1 ))
done

echo ""
echo "═══════════════════════════════════════════════════════════════"
echo " Ryzen runner complete!"
echo " Checkpoints: $CKPT_DIR/"
ls -lh "$CKPT_DIR"/*.ckpt 2>/dev/null
echo " Total size: $(du -sh "$CKPT_DIR" | cut -f1)"
echo "═══════════════════════════════════════════════════════════════"
