#!/usr/bin/env bash
# run_verification.sh — Run all CBMC verification harnesses
#
# Usage: ./run_verification.sh [--verbose]
# Requires: cbmc (C Bounded Model Checker) on PATH
#
# Exit code: 0 if all harnesses pass, 1 if any fail

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
VERBOSE="${1:-}"
PASS=0
FAIL=0
ERRORS=()

# Color codes
GREEN='\033[0;32m'
RED='\033[0;31m'
YELLOW='\033[1;33m'
NC='\033[0m'

if ! command -v cbmc &>/dev/null; then
    echo -e "${RED}Error: cbmc not found on PATH${NC}"
    echo "Install: yay -S cbmc  (Arch) or apt install cbmc (Debian/Ubuntu)"
    exit 1
fi

echo "CBMC version: $(cbmc --version 2>&1 | head -1)"
echo "=========================================="
echo ""

run_harness() {
    local name="$1"
    local file="$2"
    shift 2
    local extra_flags=("$@")

    printf "%-50s " "[${name}]"

    local output
    local rc=0
    output=$(cbmc "$file" \
        --function harness \
        --cpp11 \
        --bounds-check \
        --pointer-check \
        "${extra_flags[@]}" \
        2>&1) || rc=$?

    if echo "$output" | grep -q "VERIFICATION SUCCESSFUL"; then
        echo -e "${GREEN}PASS${NC}"
        PASS=$((PASS + 1))
        if [[ "$VERBOSE" == "--verbose" ]]; then
            echo "$output" | tail -5
            echo ""
        fi
    elif echo "$output" | grep -q "VERIFICATION FAILED"; then
        echo -e "${RED}FAIL${NC}"
        FAIL=$((FAIL + 1))
        ERRORS+=("$name")
        echo "$output" | grep -A2 "FAILED"
        echo ""
    else
        echo -e "${YELLOW}ERROR (exit $rc)${NC}"
        FAIL=$((FAIL + 1))
        ERRORS+=("$name (error)")
        if [[ "$VERBOSE" == "--verbose" ]]; then
            echo "$output" | tail -20
        else
            echo "$output" | tail -5
        fi
        echo ""
    fi
}

cd "$SCRIPT_DIR"

# ── Phase 1 harnesses ────────────────────────────────────────────────────────

run_harness \
    "1.1 key_less strict weak ordering" \
    harness_key_less.cpp

run_harness \
    "1.2 Hash128Hasher consistency" \
    harness_hasher.cpp

run_harness \
    "1.3 hash_normal_form byte layout" \
    harness_hash_nf.cpp \
    --unwind 15

run_harness \
    "1.4 MergeRecord binary layout" \
    harness_layout.cpp

# ── Phase 2 harnesses ────────────────────────────────────────────────────────

run_harness \
    "1.5 merge_dedup count preservation" \
    harness_merge_dedup.cpp \
    --unwind 6

run_harness \
    "1.6 k-way merge count preservation" \
    harness_kway_merge.cpp \
    --unwind 8

# ── Summary ───────────────────────────────────────────────────────────────────

echo ""
echo "=========================================="
echo -e "Results: ${GREEN}${PASS} passed${NC}, ${RED}${FAIL} failed${NC}"

if [[ ${#ERRORS[@]} -gt 0 ]]; then
    echo ""
    echo "Failed harnesses:"
    for e in "${ERRORS[@]}"; do
        echo -e "  ${RED}✗${NC} $e"
    done
    exit 1
fi

echo -e "${GREEN}All verification harnesses passed.${NC}"
exit 0
