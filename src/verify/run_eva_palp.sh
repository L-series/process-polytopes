#!/usr/bin/env bash
# run_eva_palp.sh — Run Frama-C Eva abstract interpretation on PALP sources
#
# Usage: ./run_eva_palp.sh [--precision N] [--log FILE]
# Requires: frama-c (from nix develop .#proofing)
#
# Analyses the full PALP computation pipeline (Make_CWS_Points → Find_Equations
# → Sort_VL → Make_Poly_Sym_NF) with abstract inputs and reports:
#   - Potential buffer overflows / out-of-bounds accesses
#   - Integer overflow warnings
#   - Division by zero risks
#   - Uninitialized memory reads
#   - Status of ACSL assertions

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_DIR="$(cd "$SCRIPT_DIR/../.." && pwd)"
PRECISION="${1:-2}"
LOG_FILE="${2:-/tmp/eva_palp.log}"

GREEN='\033[0;32m'
RED='\033[0;31m'
YELLOW='\033[1;33m'
NC='\033[0m'

if ! command -v frama-c &>/dev/null; then
    echo -e "${RED}Error: frama-c not found. Enter nix devshell: nix develop .#proofing${NC}"
    exit 1
fi

echo "Frama-C $(frama-c -version 2>&1 | head -1)"
echo "Eva precision: $PRECISION"
echo "Log: $LOG_FILE"
echo "=========================================="
echo ""

cd "$PROJECT_DIR"

echo "Running Eva analysis on PALP (this may take a few minutes)..."
echo ""

frama-c -eva \
    -eva-precision "$PRECISION" \
    -cpp-extra-args="-DPOLY_Dmax=5 -DPALP_FAST_ASSERT -DCEQ_Nmax=2048 -I PALP" \
    PALP/Coord.c PALP/Rat.c PALP/Vertex.c PALP/Polynf.c PALP/LG.c \
    src/classify/palp_globals.c src/verify/eva_palp_driver.c \
    -main eva_palp_main \
    -eva-log "a:$LOG_FILE" \
    2>&1 | grep -E 'summary|alarm|assert|coverage|errors|valid|unknown|invalid|ANALYSIS' | head -30

echo ""
echo "=========================================="

# Extract alarm counts by category
echo ""
echo "Alarm breakdown:"
for category in "out of bounds" "uninitialized" "overflow" "memory access" "division by zero"; do
    count=$(grep -c "$category" "$LOG_FILE" 2>/dev/null || echo 0)
    if [ "$count" -gt 0 ]; then
        echo -e "  ${YELLOW}$count${NC} $category"
    fi
done

# Check our specific assertions
echo ""
echo "Custom assertions (eva_palp_driver.c):"
grep 'eva_palp_driver' "$LOG_FILE" 2>/dev/null | grep -v 'signed overflow' | while read -r line; do
    echo "  $line"
done

echo ""
echo -e "Full log: ${LOG_FILE}"
