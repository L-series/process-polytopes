#!/usr/bin/env bash
# setup_gcp.sh — Bootstrap a GCP VM for polytope classification
#
# Run this on a fresh Ubuntu 24.04 GCP instance to install all deps,
# clone the repo, build the classifier, and start processing.
#
# Usage (on the VM after SSH):
#   chmod +x setup_gcp.sh
#   ./setup_gcp.sh --start 150 --end 549 --bucket gs://my-bucket/checkpoints --token hf_...
#
# ═══════════════════════════════════════════════════════════════════════════
set -euo pipefail

# ── Parse args ───────────────────────────────────────────────────────────

RUNNER_START=""
RUNNER_END=""
GCS_BUCKET=""
HF_TOKEN="${HF_TOKEN:-}"
REPO_URL="${REPO_URL:-https://github.com/ahatziiliou/process-polytopes.git}"
BATCH_SIZE="${BATCH_SIZE:-100}"

while [[ $# -gt 0 ]]; do
    case "$1" in
        --start)  RUNNER_START="$2"; shift 2 ;;
        --end)    RUNNER_END="$2"; shift 2 ;;
        --bucket) GCS_BUCKET="$2"; shift 2 ;;
        --token)  HF_TOKEN="$2"; shift 2 ;;
        --repo)   REPO_URL="$2"; shift 2 ;;
        --batch)  BATCH_SIZE="$2"; shift 2 ;;
        *) echo "Unknown arg: $1"; exit 1 ;;
    esac
done

[[ -z "$RUNNER_START" ]] && { echo "Error: --start required"; exit 1; }
[[ -z "$RUNNER_END" ]]   && { echo "Error: --end required"; exit 1; }
[[ -z "$GCS_BUCKET" ]]   && { echo "Error: --bucket required (e.g. gs://my-bucket/checkpoints)"; exit 1; }
[[ -z "$HF_TOKEN" ]]     && { echo "Error: --token or HF_TOKEN env var required"; exit 1; }

echo "═══════════════════════════════════════════════════════════════"
echo " GCP Instance Setup — Polytope Classifier"
echo "═══════════════════════════════════════════════════════════════"
echo " Files:    $RUNNER_START .. $RUNNER_END"
echo " GCS:      $GCS_BUCKET"
echo " Batch:    $BATCH_SIZE files"
echo "═══════════════════════════════════════════════════════════════"

# ── 1. System packages ──────────────────────────────────────────────────

echo ""
echo "=== Installing system packages ==="
export DEBIAN_FRONTEND=noninteractive
sudo apt-get update -qq
sudo apt-get install -y -qq \
    build-essential cmake ninja-build \
    libarrow-dev libparquet-dev \
    python3-pip \
    pkg-config git curl

# ── 2. Python deps ───────────────────────────────────────────────────────

echo ""
echo "=== Installing Python packages ==="
pip3 install --break-system-packages -q \
    huggingface_hub hf_transfer python-dotenv 2>/dev/null \
|| pip3 install -q huggingface_hub hf_transfer python-dotenv

# ── 3. Clone repo + build ────────────────────────────────────────────────

echo ""
echo "=== Cloning repository ==="
WORK_DIR="$HOME/process-polytopes"
if [[ -d "$WORK_DIR" ]]; then
    echo "  Repo exists, pulling latest..."
    (cd "$WORK_DIR" && git pull --ff-only)
else
    git clone "$REPO_URL" "$WORK_DIR"
fi

echo ""
echo "=== Building classifier ==="
(cd "$WORK_DIR/src/classify" && ./build.sh build)

CLASSIFIER="$WORK_DIR/src/classify/build/classifier"
[[ ! -x "$CLASSIFIER" ]] && { echo "Build failed!"; exit 1; }
echo "✓ Build successful"

# ── 4. Write .env ────────────────────────────────────────────────────────

echo "HF_TOKEN=$HF_TOKEN" > "$WORK_DIR/.env"

# ── 5. Verify GCS access ──────────────────────────────────────────────────

echo ""
echo "=== Checking GCS access ==="
if gcloud storage ls "${GCS_BUCKET%/}/" >/dev/null 2>&1; then
    echo "✓ GCS bucket accessible"
else
    echo "⚠ Cannot access $GCS_BUCKET"
    echo "  Ensure the VM service account has 'Storage Object Admin' role."
    echo "  In GCP Console: VM → Edit → Service account → add Storage Object Admin"
    echo "  Continuing anyway (will fail at first upload if not fixed)..."
fi

# ── 6. CPU governor ──────────────────────────────────────────────────────

echo ""
echo "=== CPU tuning ==="
if [[ -d /sys/devices/system/cpu/cpu0/cpufreq ]]; then
    echo performance | sudo tee /sys/devices/system/cpu/cpu*/cpufreq/scaling_governor >/dev/null 2>/dev/null || true
fi
echo "  CPU MHz: $(grep -m1 'cpu MHz' /proc/cpuinfo | cut -d: -f2 | xargs)"

# ── 7. Launch runner ────────────────────────────────────────────────────

echo ""
echo "=== Launching runner ==="

OUTPUT_DIR="/tmp/polytope-output"
mkdir -p "$OUTPUT_DIR"
LOG="$OUTPUT_DIR/run.log"

RUNNER_START="$RUNNER_START" \
RUNNER_END="$RUNNER_END" \
CHECKPOINT_BUCKET="$GCS_BUCKET" \
STORAGE_BACKEND="gcs" \
BATCH_SIZE="$BATCH_SIZE" \
OUTPUT_DIR="$OUTPUT_DIR" \
HF_TOKEN="$HF_TOKEN" \
nohup bash "$WORK_DIR/scripts/run_ec2.sh" > "$LOG" 2>&1 &

RUNNER_PID=$!
echo "✓ Runner started (PID: $RUNNER_PID)"
echo ""
echo "Monitor:"
echo "  tail -f $LOG"
echo "  gcloud storage ls $GCS_BUCKET/"
echo "  gcloud storage ls --long $GCS_BUCKET/ | wc -l   # count done batches"
