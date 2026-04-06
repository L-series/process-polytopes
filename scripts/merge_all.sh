#!/usr/bin/env bash
# merge_all.sh — Merge checkpoints from all runners into final output
#
# Collects .ckpt files from all runner checkpoint directories, copies
# them into a staging area, and runs the classifier's --merge mode.
#
# Usage:
#   # If all checkpoint dirs are local (e.g., after scp):
#   ./scripts/merge_all.sh /path/to/staging
#
#   # Copy from remote machines first:
#   scp -r ryzen:results/official-ryzen/checkpoints/*.ckpt /staging/
#   scp -r xeon-gold:results/official-xeon-gold/checkpoints/*.ckpt /staging/
#   scp -r xeon-e5:results/official-xeon-e5/checkpoints/*.ckpt /staging/
#   ./scripts/merge_all.sh /staging
#
# ═══════════════════════════════════════════════════════════════════════════
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
CLASSIFIER="${CLASSIFIER:-$REPO_ROOT/src/classify/build/classifier}"

STAGING_DIR="${1:?Usage: $0 <dir_with_all_ckpt_files>}"
OUTPUT_DIR="${2:-$REPO_ROOT/results/final}"

if [[ ! -x "$CLASSIFIER" ]]; then
    echo "Error: classifier binary not found at $CLASSIFIER"
    exit 1
fi

CKPT_COUNT=$(find "$STAGING_DIR" -name '*.ckpt' | wc -l)
CKPT_SIZE=$(du -sh "$STAGING_DIR" | cut -f1)

echo "═══════════════════════════════════════════════════════════════"
echo " Merging all runner checkpoints"
echo "═══════════════════════════════════════════════════════════════"
echo " Staging dir:   $STAGING_DIR"
echo " Checkpoints:   $CKPT_COUNT files ($CKPT_SIZE)"
echo " Output dir:    $OUTPUT_DIR"
echo "═══════════════════════════════════════════════════════════════"
echo ""

if (( CKPT_COUNT == 0 )); then
    echo "Error: no .ckpt files found in $STAGING_DIR"
    exit 1
fi

echo "Files to merge:"
ls -lhS "$STAGING_DIR"/*.ckpt | head -30
echo ""

# Estimate RAM needed: sum of checkpoint file sizes ÷ ~0.55 (hash map overhead)
CKPT_BYTES=$(find "$STAGING_DIR" -name '*.ckpt' -exec stat --format='%s' {} + | awk '{s+=$1}END{print s}')
EST_RAM_GB=$(echo "$CKPT_BYTES" | awk '{printf "%.0f", $1/1024/1024/1024 * 1.8}')
AVAIL_RAM=$(awk '/MemAvailable/ {printf "%.0f", $2/1024/1024}' /proc/meminfo)

echo "Estimated RAM for merge: ~${EST_RAM_GB} GB  (sort phase: ~1 shard at a time)"
echo "Available RAM:           ~${AVAIL_RAM} GB"

# Sort-merge also needs temporary disk space ≈ checkpoint total size
AVAIL_DISK_KB=$(df --output=avail "$OUTPUT_DIR" 2>/dev/null | tail -1 || echo 0)
AVAIL_DISK_GB=$(( AVAIL_DISK_KB / 1024 / 1024 ))
CKPT_GB=$(echo "$CKPT_BYTES" | awk '{printf "%.0f", $1/1024/1024/1024}')
echo "Temp disk needed:        ~${CKPT_GB} GB  (sorted intermediates)"
echo "Available disk:          ~${AVAIL_DISK_GB} GB"

if (( EST_RAM_GB > AVAIL_RAM )); then
    echo ""
    echo "⚠  WARNING: Estimate exceeds available RAM!"
    echo "   Note: estimate assumes zero deduplication (worst case)."
    echo "   Actual usage is typically 5-10x lower due to polytope overlap."
    # Skip interactive prompt when running non-interactively (e.g. under nohup)
    if [[ -t 0 && "${FORCE:-0}" != "1" ]]; then
        read -p "   Continue anyway? [y/N] " -n 1 -r
        echo ""
        [[ $REPLY =~ ^[Yy]$ ]] || exit 0
    else
        echo "   Running non-interactively — continuing..."
    fi
fi

echo ""
echo "Starting sort-merge..."
mkdir -p "$OUTPUT_DIR"

N_THREADS="${THREADS:-$(nproc)}"
"$CLASSIFIER" --merge "$STAGING_DIR" --output "$OUTPUT_DIR" --threads "$N_THREADS"

echo ""
echo "═══════════════════════════════════════════════════════════════"
echo " Merge complete!"
echo "═══════════════════════════════════════════════════════════════"
echo " Output: $OUTPUT_DIR/unique_polytopes.parquet"
ls -lh "$OUTPUT_DIR/unique_polytopes.parquet" 2>/dev/null
echo ""
