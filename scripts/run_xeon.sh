#!/usr/bin/env bash
# run_xeon.sh — Straight-through batch processing for Xeon runners (NFS)
#
# Since the Xeon machines have all 4000 parquet files available via NFS,
# no download/delete cycle is needed.  Just process in batches.
#
# Usage:
#   RUNNER_START=1800 RUNNER_END=3149 ./scripts/run_xeon.sh    # Xeon Gold
#   RUNNER_START=3150 RUNNER_END=3999 ./scripts/run_xeon.sh    # Xeon E5
#
# ═══════════════════════════════════════════════════════════════════════════
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"

# ── Configuration (set via environment or edit here) ──────────────────────

RUNNER_START="${RUNNER_START:?Set RUNNER_START}"
RUNNER_END="${RUNNER_END:?Set RUNNER_END}"

INPUT_DIR="${INPUT_DIR:?Set INPUT_DIR to NFS parquet path}"
OUTPUT_DIR="${OUTPUT_DIR:-$REPO_ROOT/results/official-xeon}"
BATCH_SIZE="${BATCH_SIZE:-150}"
THREADS="${THREADS:-0}"
CLASSIFIER="${CLASSIFIER:-$REPO_ROOT/src/classify/build/classifier}"

# ── Setup ─────────────────────────────────────────────────────────────────

CKPT_DIR="$OUTPUT_DIR/checkpoints"
LOG_DIR="$OUTPUT_DIR/logs"
mkdir -p "$CKPT_DIR" "$LOG_DIR"

TOTAL_FILES=$(( RUNNER_END - RUNNER_START + 1 ))
TOTAL_BATCHES=$(( (TOTAL_FILES + BATCH_SIZE - 1) / BATCH_SIZE ))

echo "═══════════════════════════════════════════════════════════════"
echo " Xeon Runner"
echo "═══════════════════════════════════════════════════════════════"
echo " Assigned range: $RUNNER_START .. $RUNNER_END ($TOTAL_FILES files)"
echo " Input dir:      $INPUT_DIR"
echo " Output dir:     $OUTPUT_DIR"
echo " Batch size:     $BATCH_SIZE"
echo " Batches:        $TOTAL_BATCHES"
echo " Threads:        ${THREADS:-auto}"
echo "═══════════════════════════════════════════════════════════════"
echo ""
echo "CPU:  $(grep -m1 'model name' /proc/cpuinfo | cut -d: -f2 | xargs)"
echo "RAM:  $(awk '/MemTotal/ {printf "%.0f GB", $2/1024/1024}' /proc/meminfo)"
echo "Disk: $(df -h "$OUTPUT_DIR" | awk 'NR==2 {print $4, "free of", $2}')"
echo ""

if [[ ! -x "$CLASSIFIER" ]]; then
    echo "Error: classifier binary not found at $CLASSIFIER"
    echo "Build first: cd src/classify && ./build.sh build   (or pgo)"
    exit 1
fi

# ── Main batch loop ──────────────────────────────────────────────────────

BATCH_START=$RUNNER_START
BATCH_NUM=0
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

    # Skip if checkpoint already exists
    FINAL_CKPT_NAME="${BATCH_TAG}.ckpt"
    if [[ -f "$CKPT_DIR/$FINAL_CKPT_NAME" ]]; then
        echo " ✓ Checkpoint exists — skipping"
        BATCH_START=$(( BATCH_END + 1 ))
        continue
    fi

    mkdir -p "$BATCH_OUT/checkpoints"

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

    # Copy final checkpoint to central dir
    SRC_CKPT=$(ls -t "$BATCH_OUT/checkpoints"/*.ckpt 2>/dev/null | head -1)
    if [[ -n "$SRC_CKPT" ]]; then
        cp "$SRC_CKPT" "$CKPT_DIR/$FINAL_CKPT_NAME"
        echo "  → Saved: $CKPT_DIR/$FINAL_CKPT_NAME ($(du -h "$CKPT_DIR/$FINAL_CKPT_NAME" | cut -f1))"
    fi

    # Clean up per-batch intermediate output
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
    echo "  Disk free: $(df -h "$OUTPUT_DIR" | awk 'NR==2 {print $4}')"

    BATCH_START=$(( BATCH_END + 1 ))
done

echo ""
echo "═══════════════════════════════════════════════════════════════"
echo " Xeon runner complete!"
echo " Checkpoints: $CKPT_DIR/"
ls -lh "$CKPT_DIR"/*.ckpt 2>/dev/null
echo " Total size: $(du -sh "$CKPT_DIR" | cut -f1)"
echo "═══════════════════════════════════════════════════════════════"
