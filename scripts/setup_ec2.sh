#!/usr/bin/env bash
# setup_ec2.sh — Bootstrap an EC2 instance for polytope classification
#
# Run this on a fresh Ubuntu 24.04 EC2 instance to install all deps,
# clone the repo, build the classifier, and start processing.
#
# Usage:
#   curl -sL <raw-github-url>/scripts/setup_ec2.sh | bash -s -- \
#     --start 150 --end 549 --bucket s3://my-bucket/polytopes/checkpoints
#
# Or copy to the instance and run:
#   chmod +x setup_ec2.sh
#   ./setup_ec2.sh --start 150 --end 549 --bucket s3://my-bucket/polytopes/checkpoints
#
# ═══════════════════════════════════════════════════════════════════════════
set -euo pipefail

# ── Parse args ───────────────────────────────────────────────────────────

RUNNER_START=""
RUNNER_END=""
S3_BUCKET=""
HF_TOKEN="${HF_TOKEN:-}"
REPO_URL="${REPO_URL:-https://github.com/ahatziiliou/process-polytopes.git}"
BATCH_SIZE="${BATCH_SIZE:-50}"

while [[ $# -gt 0 ]]; do
    case "$1" in
        --start)  RUNNER_START="$2"; shift 2 ;;
        --end)    RUNNER_END="$2"; shift 2 ;;
        --bucket) S3_BUCKET="$2"; shift 2 ;;
        --token)  HF_TOKEN="$2"; shift 2 ;;
        --repo)   REPO_URL="$2"; shift 2 ;;
        --batch)  BATCH_SIZE="$2"; shift 2 ;;
        *) echo "Unknown arg: $1"; exit 1 ;;
    esac
done

[[ -z "$RUNNER_START" ]] && { echo "Error: --start required"; exit 1; }
[[ -z "$RUNNER_END" ]]   && { echo "Error: --end required"; exit 1; }
[[ -z "$S3_BUCKET" ]]    && { echo "Error: --bucket required"; exit 1; }
[[ -z "$HF_TOKEN" ]]     && { echo "Error: --token or HF_TOKEN env required"; exit 1; }

echo "═══════════════════════════════════════════════════════════════"
echo " EC2 Instance Setup — Polytope Classifier"
echo "═══════════════════════════════════════════════════════════════"
echo " Files:     $RUNNER_START .. $RUNNER_END"
echo " S3:        $S3_BUCKET"
echo " Batch:     $BATCH_SIZE"
echo "═══════════════════════════════════════════════════════════════"

# ── 1. System packages ──────────────────────────────────────────────────

echo ""
echo "=== Installing system packages ==="
export DEBIAN_FRONTEND=noninteractive

sudo apt-get update -qq
sudo apt-get install -y -qq \
    build-essential cmake ninja-build \
    libarrow-dev libparquet-dev \
    python3-pip python3-venv \
    awscli \
    pkg-config git curl

# ── 2. Python deps (for HuggingFace download) ───────────────────────────

echo ""
echo "=== Installing Python packages ==="
python3 -m pip install --break-system-packages --quiet \
    huggingface_hub hf_transfer python-dotenv 2>/dev/null || \
python3 -m pip install --quiet \
    huggingface_hub hf_transfer python-dotenv

# ── 3. Clone repo & build ───────────────────────────────────────────────

echo ""
echo "=== Cloning repository ==="
WORK_DIR="/home/ubuntu/process-polytopes"
if [[ -d "$WORK_DIR" ]]; then
    echo "  Repo already exists, pulling latest..."
    (cd "$WORK_DIR" && git pull --ff-only)
else
    git clone "$REPO_URL" "$WORK_DIR"
fi

echo ""
echo "=== Building classifier ==="
(cd "$WORK_DIR/src/classify" && ./build.sh build)

# Verify build
CLASSIFIER="$WORK_DIR/src/classify/build/classifier"
if [[ ! -x "$CLASSIFIER" ]]; then
    echo "Error: Build failed — $CLASSIFIER not found"
    exit 1
fi
echo "✓ Build successful: $CLASSIFIER"

# ── 4. Write .env ───────────────────────────────────────────────────────

echo "HF_TOKEN=$HF_TOKEN" > "$WORK_DIR/.env"

# ── 5. Set CPU governor to performance (if available) ────────────────────

echo ""
echo "=== CPU tuning ==="
if [[ -d /sys/devices/system/cpu/cpu0/cpufreq ]]; then
    echo performance | sudo tee /sys/devices/system/cpu/cpu*/cpufreq/scaling_governor 2>/dev/null || true
    echo "  Governor: $(cat /sys/devices/system/cpu/cpu0/cpufreq/scaling_governor 2>/dev/null || echo 'N/A')"
fi
# Disable turbo boost throttling
echo 0 | sudo tee /sys/devices/system/cpu/intel_pstate/no_turbo 2>/dev/null || true
echo "  CPU MHz: $(grep -m1 'cpu MHz' /proc/cpuinfo | cut -d: -f2 | xargs)"

# ── 6. Launch the runner ─────────────────────────────────────────────────

echo ""
echo "=== Starting runner ==="

OUTPUT_DIR="/tmp/polytope-output"
mkdir -p "$OUTPUT_DIR"

export RUNNER_START RUNNER_END S3_BUCKET HF_TOKEN
export BATCH_SIZE
export OUTPUT_DIR

# Run in background with nohup so it survives SSH disconnect
nohup bash "$WORK_DIR/scripts/run_ec2.sh" \
    > "$OUTPUT_DIR/run.log" 2>&1 &

RUNNER_PID=$!
echo "Runner started (PID: $RUNNER_PID)"
echo "Log: tail -f $OUTPUT_DIR/run.log"
echo ""
echo "Monitor progress:"
echo "  tail -f $OUTPUT_DIR/run.log"
echo "  aws s3 ls $S3_BUCKET/"
