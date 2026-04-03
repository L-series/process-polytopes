#!/usr/bin/env bash
# run_cloud.sh — Cloud spot/preemptible runner with checkpoint upload
#
# Supports AWS S3 and Google Cloud Storage as checkpoint backends.
# Set STORAGE_BACKEND=gcs for GCP, or STORAGE_BACKEND=s3 (default) for AWS.
#
# Prerequisites on the instance:
#   - GCS: gcloud CLI authenticated via VM service account
#   - AWS: AWS CLI configured via instance profile or env vars
#   - Python 3.10+ with huggingface_hub, hf_transfer, python-dotenv
#   - Arrow/Parquet dev libs (apt install libarrow-dev libparquet-dev)
#   - GCC 12+ with C++20 support
#
# Usage (GCS):
#   RUNNER_START=150 RUNNER_END=549 STORAGE_BACKEND=gcs \
#     CHECKPOINT_BUCKET=gs://my-bucket/checkpoints \
#     HF_TOKEN=hf_... ./scripts/run_cloud.sh
#
# Usage (S3):
#   RUNNER_START=150 RUNNER_END=549 STORAGE_BACKEND=s3 \
#     CHECKPOINT_BUCKET=s3://my-bucket/checkpoints \
#     HF_TOKEN=hf_... ./scripts/run_cloud.sh
#
# ═══════════════════════════════════════════════════════════════════════════
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"

# Ensure AWS CLI v2 is in PATH (installed to /usr/local/bin on EC2)
export PATH="/usr/local/bin:$PATH"

# ── Configuration ─────────────────────────────────────────────────────────

# Required: file range for this runner
RUNNER_START="${RUNNER_START:?Set RUNNER_START (first file index)}"
RUNNER_END="${RUNNER_END:?Set RUNNER_END (last file index)}"

# Storage backend: "s3" (AWS) or "gcs" (Google Cloud Storage)
STORAGE_BACKEND="${STORAGE_BACKEND:-s3}"

# Required: bucket for checkpoints
# GCS: gs://my-bucket/checkpoints
# S3:  s3://my-bucket/checkpoints
CHECKPOINT_BUCKET="${CHECKPOINT_BUCKET:-${S3_BUCKET:-}}"
if [[ -z "$CHECKPOINT_BUCKET" ]]; then
    echo "Error: Set CHECKPOINT_BUCKET (e.g. gs://my-bucket or s3://my-bucket)"
    exit 1
fi

# Local directories
INPUT_DIR="${INPUT_DIR:-/tmp/parquet-input/reflexive}"
OUTPUT_DIR="${OUTPUT_DIR:-/tmp/polytope-output}"
CKPT_DIR="$OUTPUT_DIR/checkpoints"
LOG_DIR="$OUTPUT_DIR/logs"

# Batch size — smaller batches = less lost work on spot reclaim
# 50 files ≈ 2.5 hrs on c7a.16xlarge, ~8 GB checkpoint
BATCH_SIZE="${BATCH_SIZE:-50}"

# Threads (0 = auto-detect)
THREADS="${THREADS:-0}"

# Classifier binary
CLASSIFIER="${CLASSIFIER:-$REPO_ROOT/src/classify/build/classifier}"

# Download script
DOWNLOADER="$REPO_ROOT/samples/download_cws.py"

# ── Spot interrupt handler ───────────────────────────────────────────────

INTERRUPTED=0
handle_spot_interrupt() {
    echo ""
    echo "⚠ SPOT INTERRUPTION DETECTED — attempting to save state..."
    INTERRUPTED=1
    # The classifier will finish its current file and checkpoint.
    # We'll upload whatever we have when the current batch finishes or
    # the script catches this flag.
}
# AWS spot gives 2 minutes warning via SIGTERM
trap handle_spot_interrupt SIGTERM SIGINT

# Monitor for spot/preemptible termination (background)
# AWS gives ~2 min warning; GCP gives only ~30 sec
monitor_spot_termination() {
    while true; do
        if [[ "$STORAGE_BACKEND" == "gcs" ]]; then
            local status
            status=$(curl -sf -m 2 -H "Metadata-Flavor: Google" \
                "http://metadata.google.internal/computeMetadata/v1/instance/preempted" 2>/dev/null || true)
            if [[ "$status" == "TRUE" ]]; then
                echo "$(date): GCP preemption notice — saving state!"
                kill -TERM $$ 2>/dev/null || true
                break
            fi
        else
            if curl -sf -m 2 http://169.254.169.254/latest/meta-data/spot/instance-action >/dev/null 2>&1; then
                echo "$(date): AWS spot termination notice — saving state!"
                kill -TERM $$ 2>/dev/null || true
                break
            fi
        fi
        sleep 5
    done
}
monitor_spot_termination &
MONITOR_PID=$!

# ── Sanity checks ────────────────────────────────────────────────────────

if [[ ! -x "$CLASSIFIER" ]]; then
    echo "Classifier not found at $CLASSIFIER — building..."
    (cd "$REPO_ROOT/src/classify" && ./build.sh build)
fi

if [[ "$STORAGE_BACKEND" == "gcs" ]]; then
    if ! gcloud storage ls "${CHECKPOINT_BUCKET%/}/" >/dev/null 2>&1; then
        echo "Warning: Cannot list GCS bucket $CHECKPOINT_BUCKET — check service account roles"
    fi
else
    if ! aws s3 ls "${CHECKPOINT_BUCKET%/}/" >/dev/null 2>&1; then
        echo "Warning: Cannot list S3 bucket $CHECKPOINT_BUCKET — uploads may fail"
    fi
fi

# Source .env for HF_TOKEN if present
if [[ -f "$REPO_ROOT/.env" ]]; then
    set -a; source "$REPO_ROOT/.env"; set +a
fi
if [[ -z "${HF_TOKEN:-}" ]]; then
    echo "Error: HF_TOKEN not set. Required for downloading parquet files."
    exit 1
fi

mkdir -p "$CKPT_DIR" "$LOG_DIR" "$INPUT_DIR"

if [[ "$STORAGE_BACKEND" == "gcs" ]]; then
    _itype=$(curl -sf -m 2 -H "Metadata-Flavor: Google" \
        "http://metadata.google.internal/computeMetadata/v1/instance/machine-type" 2>/dev/null \
        | awk -F/ '{print $NF}' || echo 'unknown')
else
    _itype=$(curl -sf -m 2 http://169.254.169.254/latest/meta-data/instance-type 2>/dev/null || echo 'unknown')
fi
echo "═══════════════════════════════════════════════════════════════"
echo " Cloud Runner  [backend: $STORAGE_BACKEND]"
echo "═══════════════════════════════════════════════════════════════"
echo " Instance:     $_itype"
echo " Assigned:     $RUNNER_START .. $RUNNER_END"
echo " Batch size:   $BATCH_SIZE"
echo " Bucket:       $CHECKPOINT_BUCKET"
echo " Input dir:    $INPUT_DIR"
echo " Output dir:   $OUTPUT_DIR"
echo "═══════════════════════════════════════════════════════════════"
echo ""
echo "CPU:  $(grep -m1 'model name' /proc/cpuinfo | cut -d: -f2 | xargs)"
echo "RAM:  $(awk '/MemTotal/ {printf "%.0f GB", $2/1024/1024}' /proc/meminfo)"
echo "Disk: $(df -h /tmp | awk 'NR==2 {print $4, "free of", $2}')"
echo ""

# ── Helper: download a range of files ────────────────────────────────────

download_range() {
    local dl_start="$1" dl_end="$2"
    echo "  Downloading files $dl_start .. $dl_end ..."
    local dl_out_dir
    dl_out_dir="$(dirname "$INPUT_DIR")"
    (cd "$REPO_ROOT" && python3 "$DOWNLOADER" \
        --start "$dl_start" \
        --end "$dl_end" \
        --out-dir "$dl_out_dir" \
        --workers 8)
}

# ── Helper: delete local parquet files ───────────────────────────────────

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
}

# ── Helper: upload checkpoint to S3 ─────────────────────────────────────

upload_checkpoint() {
    local ckpt_file="$1"
    local ckpt_name remote_dest
    ckpt_name="$(basename "$ckpt_file")"
    remote_dest="${CHECKPOINT_BUCKET%/}/$ckpt_name"
    local local_size
    local_size=$(stat --format='%s' "$ckpt_file" 2>/dev/null || stat -f'%z' "$ckpt_file" 2>/dev/null)
    echo "  Uploading $ckpt_name ($(numfmt --to=iec "$local_size" 2>/dev/null || echo "${local_size} bytes")) → $STORAGE_BACKEND ..."

    local upload_ok=0
    for attempt in 1 2; do
        if [[ "$STORAGE_BACKEND" == "gcs" ]]; then
            gcloud storage cp "$ckpt_file" "$remote_dest" && upload_ok=1 && break
        else
            aws s3 cp "$ckpt_file" "$remote_dest" && upload_ok=1 && break
        fi
        echo "  ⚠ Upload attempt $attempt failed — retrying..."
    done

    if (( upload_ok )); then
        local remote_info
        if [[ "$STORAGE_BACKEND" == "gcs" ]]; then
            remote_info=$(gcloud storage ls -l "$remote_dest" 2>/dev/null | head -1 || true)
        else
            remote_info=$(aws s3 ls "$remote_dest" 2>/dev/null || true)
        fi
        if [[ -n "$remote_info" ]]; then
            echo "  ✓ Verified: $remote_info"
        else
            echo "  ⚠ Upload seemed ok but remote ls returned nothing!"
        fi
    else
        echo "  ✗ Both upload attempts failed. Keeping local: $ckpt_file"
        return 1
    fi
}

# ── Helper: check if batch checkpoint already exists in remote storage ───

batch_done_remotely() {
    local batch_tag="$1"
    if [[ "$STORAGE_BACKEND" == "gcs" ]]; then
        gcloud storage ls "${CHECKPOINT_BUCKET%/}/${batch_tag}.ckpt" >/dev/null 2>&1
    else
        aws s3 ls "${CHECKPOINT_BUCKET%/}/${batch_tag}.ckpt" >/dev/null 2>&1
    fi
}

# ── Main batch loop ──────────────────────────────────────────────────────

BATCH_START=$RUNNER_START
BATCH_NUM=0
TOTAL_BATCHES=$(( (RUNNER_END - RUNNER_START + 1 + BATCH_SIZE - 1) / BATCH_SIZE ))
STARTED_AT=$(date +%s)

while (( BATCH_START <= RUNNER_END )); do
    # Check for spot interruption
    if (( INTERRUPTED )); then
        echo "⚠ Stopping due to spot interruption. Last completed batch is saved."
        break
    fi

    BATCH_END=$(( BATCH_START + BATCH_SIZE - 1 ))
    (( BATCH_END > RUNNER_END )) && BATCH_END=$RUNNER_END
    BATCH_NUM=$(( BATCH_NUM + 1 ))

    BATCH_TAG=$(printf "batch-%04d-%04d" "$BATCH_START" "$BATCH_END")
    BATCH_OUT="$OUTPUT_DIR/$BATCH_TAG"
    BATCH_LOG="$LOG_DIR/${BATCH_TAG}.log"
    FINAL_CKPT_NAME="${BATCH_TAG}.ckpt"

    echo ""
    echo "───────────────────────────────────────────────────────────"
    echo " Batch $BATCH_NUM / $TOTAL_BATCHES : files $BATCH_START .. $BATCH_END"
    echo "───────────────────────────────────────────────────────────"

    # Skip if checkpoint already uploaded to remote storage
    if batch_done_remotely "$BATCH_TAG"; then
        echo " ✓ $FINAL_CKPT_NAME already in $STORAGE_BACKEND — skipping"
        BATCH_START=$(( BATCH_END + 1 ))
        continue
    fi

    # Also skip if local checkpoint exists (in case S3 upload succeeded
    # but we didn't advance, or we're re-running)
    if [[ -f "$CKPT_DIR/$FINAL_CKPT_NAME" ]]; then
        echo " ✓ Local checkpoint exists — uploading and skipping"
        upload_checkpoint "$CKPT_DIR/$FINAL_CKPT_NAME"
        BATCH_START=$(( BATCH_END + 1 ))
        continue
    fi

    # Download files for this batch
    download_range "$BATCH_START" "$BATCH_END"

    # Process
    mkdir -p "$BATCH_OUT/checkpoints"
    echo "  Processing..."

    # NOTE: Do NOT pass --start/--end here. Those flags are indices into the
    # file list found in --input, not dataset file numbers. Since we download
    # only this batch's files into INPUT_DIR, we process ALL files in the dir.
    "$CLASSIFIER" \
        --input "$INPUT_DIR" \
        --output "$BATCH_OUT" \
        --checkpoint "$BATCH_OUT/checkpoints" \
        --threads "$THREADS" \
        2>&1 | tee "$BATCH_LOG"

    if [[ ${PIPESTATUS[0]} -ne 0 ]]; then
        echo "ERROR: Batch $BATCH_NUM failed! Check $BATCH_LOG"
        # Clean up parquet files even on failure to avoid filling disk
        delete_range "$BATCH_START" "$BATCH_END"
        rm -rf "$BATCH_OUT"
        BATCH_START=$(( BATCH_END + 1 ))
        continue
    fi

    # Copy final checkpoint to central dir
    SRC_CKPT=$(ls -t "$BATCH_OUT/checkpoints"/*.ckpt 2>/dev/null | head -1 || true)
    if [[ -n "$SRC_CKPT" ]]; then
        cp "$SRC_CKPT" "$CKPT_DIR/$FINAL_CKPT_NAME"
        echo "  → Saved: $CKPT_DIR/$FINAL_CKPT_NAME ($(du -h "$CKPT_DIR/$FINAL_CKPT_NAME" | cut -f1))"

        # Upload to S3 immediately
        if upload_checkpoint "$CKPT_DIR/$FINAL_CKPT_NAME"; then
            # Delete local checkpoint after verified S3 upload to save disk
            rm -f "$CKPT_DIR/$FINAL_CKPT_NAME"
        else
            echo "  Keeping local checkpoint (S3 upload failed)"
        fi
    fi

    # Delete local parquet files and batch output to free disk
    delete_range "$BATCH_START" "$BATCH_END"
    rm -rf "$BATCH_OUT"

    # Progress
    ELAPSED=$(( $(date +%s) - STARTED_AT ))
    FILES_DONE=$(( BATCH_END - RUNNER_START + 1 ))
    FILES_LEFT=$(( RUNNER_END - BATCH_END ))
    if (( FILES_DONE > 0 && ELAPSED > 0 )); then
        SECS_PER_FILE=$(( ELAPSED / FILES_DONE ))
        ETA_SECS=$(( SECS_PER_FILE * FILES_LEFT ))
        ETA_HRS=$(( ETA_SECS / 3600 ))
        ETA_MIN=$(( (ETA_SECS % 3600) / 60 ))
        echo "  Elapsed: $(( ELAPSED / 3600 ))h$(( (ELAPSED % 3600) / 60 ))m  ETA: ${ETA_HRS}h${ETA_MIN}m"
    fi

    BATCH_START=$(( BATCH_END + 1 ))
done

# Clean up monitor
kill $MONITOR_PID 2>/dev/null || true

echo ""
echo "═══════════════════════════════════════════════════════════════"
echo " Runner finished!"
echo " Checkpoints in $STORAGE_BACKEND: $CHECKPOINT_BUCKET"
echo "═══════════════════════════════════════════════════════════════"
