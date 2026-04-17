#!/usr/bin/env bash

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
PALP_DIR="$REPO_ROOT/PALP"

CWS_BIN="${CWS_BIN:-$PALP_DIR/cws-5d.x}"
BASELINE_BIN="${BASELINE_BIN:-}"
SAMPLE_SECONDS="${SAMPLE_SECONDS:-10}"
SHARD_COUNT="${SHARD_COUNT:-0}"

if [[ $# -gt 0 ]]; then
    STRUCTURES=("$@")
else
    STRUCTURES=(11 24)
fi

if [[ ! -x "$CWS_BIN" ]]; then
    echo "cws binary not found or not executable: $CWS_BIN" >&2
    exit 1
fi

HAVE_STRACE=0
if command -v strace >/dev/null 2>&1; then
    HAVE_STRACE=1
fi

TMPDIR="$(mktemp -d)"
trap 'rm -rf "$TMPDIR"' EXIT

run_time_sample() {
    local label="$1"
    shift
    local timing

    TIMEFORMAT='elapsed=%R user=%U sys=%S cpu=%P'
    timing=$({ time timeout "${SAMPLE_SECONDS}"s "$@" >/dev/null; } 2>&1 || true)
    printf "%s %s\n" "$label" "$timing"
}

run_strace_sample() {
    local label="$1"
    local trace_file="$2"
    shift 2

    if [[ "$HAVE_STRACE" -eq 0 ]]; then
        printf "%s strace=unavailable\n" "$label"
        return
    fi

    strace -f -c -o "$trace_file" timeout "${SAMPLE_SECONDS}"s "$@" >/dev/null 2>&1 || true
    printf "%s\n" "$label"
    grep -E 'wait4|mmap|munmap|read|write|lseek|total' "$trace_file" || cat "$trace_file"
}

echo "=== dim-5 cws benchmark ==="
echo "Binary:           $CWS_BIN"
if [[ -n "$BASELINE_BIN" ]]; then
    echo "Baseline binary:  $BASELINE_BIN"
fi
echo "Sample seconds:   $SAMPLE_SECONDS"
echo "Structures:       ${STRUCTURES[*]}"
echo "Date:             $(date '+%Y-%m-%d %H:%M:%S')"
echo "CPU model:        $(grep -m1 'model name' /proc/cpuinfo | awk -F': ' '{print $2}')"
echo "Logical threads:  $(nproc)"
echo ""

for structure_id in "${STRUCTURES[@]}"; do
    echo "--- structure ${structure_id} ---"
    run_time_sample "current" "$CWS_BIN" -c5 -s"$structure_id"

    if [[ -n "$BASELINE_BIN" && -x "$BASELINE_BIN" ]]; then
        run_time_sample "baseline" "$BASELINE_BIN" -c5 -s"$structure_id"
    fi

    run_strace_sample "current strace" "$TMPDIR/current-${structure_id}.strace" \
        "$CWS_BIN" -c5 -s"$structure_id"

    if [[ -n "$BASELINE_BIN" && -x "$BASELINE_BIN" ]]; then
        run_strace_sample "baseline strace" "$TMPDIR/baseline-${structure_id}.strace" \
            "$BASELINE_BIN" -c5 -s"$structure_id"
    fi

    if [[ "$SHARD_COUNT" -gt 1 ]]; then
        shard_index=1
        while [[ "$shard_index" -le "$SHARD_COUNT" ]]; do
            run_time_sample "shard ${shard_index}/${SHARD_COUNT}" \
                "$CWS_BIN" -c5 -s"$structure_id" -j"$SHARD_COUNT" -k"$shard_index"
            shard_index=$((shard_index + 1))
        done
    fi

    echo ""
done