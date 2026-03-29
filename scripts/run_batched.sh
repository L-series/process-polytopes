#!/usr/bin/env bash
# run_batched.sh — Process parquet files in memory-safe batches
#
# Each batch processes BATCH_SIZE files from scratch (empty hash map),
# writes a single checkpoint at the end, then exits.  This keeps
# per-batch RSS under ~60 GB even for large unique-polytope counts.
#
# Checkpoints from different batches are independent and can be merged
# at the very end with:
#   classifier --merge <dir_with_all_checkpoints> --output <output_dir>
#
# Usage:
#   ./scripts/run_batched.sh               # process all files with defaults
#   ./scripts/run_batched.sh 0 459         # process file indices 0..459
#   ./scripts/run_batched.sh 0 2099 150    # custom batch size
#
# Environment variables:
#   INPUT_DIR    — parquet directory  (default: samples/reflexive)
#   OUTPUT_DIR   — results base dir   (default: results/official)
#   THREADS      — thread count       (default: auto)
#   BATCH_SIZE   — files per batch    (default: 150)
#   CLASSIFIER   — binary path        (default: src/classify/build/classifier)
#
# ═══════════════════════════════════════════════════════════════════════════
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"

# ── Defaults ──────────────────────────────────────────────────────────────
INPUT_DIR="${INPUT_DIR:-$REPO_ROOT/samples/reflexive}"
OUTPUT_DIR="${OUTPUT_DIR:-$REPO_ROOT/results/official}"
THREADS="${THREADS:-0}"
BATCH_SIZE="${BATCH_SIZE:-150}"
CLASSIFIER="${CLASSIFIER:-$REPO_ROOT/src/classify/build/classifier}"

# ── Arguments: start_index  end_index  [batch_size] ──────────────────────
START_IDX="${1:-0}"
END_IDX="${2:-}"
BATCH_SIZE="${3:-$BATCH_SIZE}"

# If end index not specified, count parquet files
if [[ -z "$END_IDX" ]]; then
    NFILES=$(find "$INPUT_DIR" -name '*.parquet' | wc -l)
    END_IDX=$((NFILES - 1))
fi

# ── Sanity checks ────────────────────────────────────────────────────────
if [[ ! -x "$CLASSIFIER" ]]; then
    echo "Error: classifier binary not found at $CLASSIFIER"
    echo "Build it first: cd src/classify && ./build.sh pgo"
    exit 1
fi

if [[ ! -d "$INPUT_DIR" ]]; then
    echo "Error: input directory not found: $INPUT_DIR"
    exit 1
fi

TOTAL_FILES=$(( END_IDX - START_IDX + 1 ))
N_BATCHES=$(( (TOTAL_FILES + BATCH_SIZE - 1) / BATCH_SIZE ))

# ── Create output structure ──────────────────────────────────────────────
CKPT_DIR="$OUTPUT_DIR/checkpoints"
LOG_DIR="$OUTPUT_DIR/logs"
mkdir -p "$CKPT_DIR" "$LOG_DIR"

echo "═══════════════════════════════════════════════════════════════"
echo " Batched Polytope Classification"
echo "═══════════════════════════════════════════════════════════════"
echo " Input:       $INPUT_DIR"
echo " Output:      $OUTPUT_DIR"
echo " File range:  $START_IDX .. $END_IDX  ($TOTAL_FILES files)"
echo " Batch size:  $BATCH_SIZE files"
echo " Batches:     $N_BATCHES"
echo " Threads:     ${THREADS:-auto}"
echo " Classifier:  $CLASSIFIER"
echo "═══════════════════════════════════════════════════════════════"
echo ""

# ── CPU info ─────────────────────────────────────────────────────────────
echo "CPU: $(grep -m1 'model name' /proc/cpuinfo | cut -d: -f2 | xargs)"
echo "RAM: $(awk '/MemTotal/ {printf "%.0f GB", $2/1024/1024}' /proc/meminfo)"
echo "Disk free: $(df -h "$OUTPUT_DIR" | awk 'NR==2 {print $4}')"
echo ""

# ── Process batches ──────────────────────────────────────────────────────
BATCH_NUM=0
BATCH_START=$START_IDX

while (( BATCH_START <= END_IDX )); do
    BATCH_END=$(( BATCH_START + BATCH_SIZE - 1 ))
    if (( BATCH_END > END_IDX )); then
        BATCH_END=$END_IDX
    fi

    BATCH_NUM=$(( BATCH_NUM + 1 ))
    BATCH_TAG=$(printf "batch-%04d-%04d" "$BATCH_START" "$BATCH_END")
    BATCH_OUT="$OUTPUT_DIR/$BATCH_TAG"
    BATCH_LOG="$LOG_DIR/${BATCH_TAG}.log"

    echo "───────────────────────────────────────────────────────────"
    echo " Batch $BATCH_NUM / $N_BATCHES : files $BATCH_START .. $BATCH_END"
    echo " Output: $BATCH_OUT"
    echo " Log:    $BATCH_LOG"
    echo "───────────────────────────────────────────────────────────"

    # Skip if this batch's checkpoint already exists (for resume)
    EXPECTED_CKPT="$BATCH_OUT/checkpoints/checkpoint-$(printf '%04d' "$BATCH_END").ckpt"
    if [[ -f "$EXPECTED_CKPT" ]]; then
        echo " ✓ Checkpoint already exists — skipping batch"
        echo ""
        BATCH_START=$(( BATCH_END + 1 ))
        continue
    fi

    mkdir -p "$BATCH_OUT/checkpoints"

    # Run the classifier for this batch
    # Each batch starts from scratch (no --resume), so the hash map is empty.
    # The checkpoint cleanup code in classifier.cpp will remove any stale
    # .ckpt files in the batch output dir from a previously interrupted run.
    "$CLASSIFIER" \
        --input "$INPUT_DIR" \
        --output "$BATCH_OUT" \
        --checkpoint "$BATCH_OUT/checkpoints" \
        --start "$BATCH_START" \
        --end "$BATCH_END" \
        --threads "$THREADS" \
        2>&1 | tee "$BATCH_LOG"

    BATCH_EXIT=${PIPESTATUS[0]}

    if [[ $BATCH_EXIT -ne 0 ]]; then
        echo ""
        echo "ERROR: Batch $BATCH_NUM failed (exit code $BATCH_EXIT)"
        echo "Check log: $BATCH_LOG"
        exit $BATCH_EXIT
    fi

    # Copy the final checkpoint to the central checkpoint directory
    # with a unique name for later merging
    FINAL_CKPT=$(ls -t "$BATCH_OUT/checkpoints"/*.ckpt 2>/dev/null | head -1)
    if [[ -n "$FINAL_CKPT" ]]; then
        cp "$FINAL_CKPT" "$CKPT_DIR/${BATCH_TAG}.ckpt"
        echo " → Checkpoint copied to $CKPT_DIR/${BATCH_TAG}.ckpt"
        echo "   Size: $(du -h "$CKPT_DIR/${BATCH_TAG}.ckpt" | cut -f1)"
    fi

    echo ""
    echo " Batch $BATCH_NUM complete."
    echo " Disk free: $(df -h "$OUTPUT_DIR" | awk 'NR==2 {print $4}')"
    echo ""

    BATCH_START=$(( BATCH_END + 1 ))
done

echo ""
echo "═══════════════════════════════════════════════════════════════"
echo " All $N_BATCHES batches complete!"
echo "═══════════════════════════════════════════════════════════════"
echo ""
echo " Checkpoints for merging: $CKPT_DIR/"
ls -lh "$CKPT_DIR"/*.ckpt 2>/dev/null
echo ""
echo " Total checkpoint size: $(du -sh "$CKPT_DIR" | cut -f1)"
echo ""
echo " To merge all checkpoints into final output:"
echo "   $CLASSIFIER --merge $CKPT_DIR --output $OUTPUT_DIR/merged"
