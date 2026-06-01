#!/usr/bin/env bash
# audit_dim5_cws.sh - Build and smoke-test PALP's dimension-5 CWS generator.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
PALP_DIR="$REPO_ROOT/PALP"
CWS_BIN="$PALP_DIR/cws-5d.x"

echo "=== dim-5 CWS generator audit ==="
echo "Repo: $REPO_ROOT"
echo "PALP: $(git -C "$PALP_DIR" rev-parse --short HEAD)"

if ! git -C "$PALP_DIR" grep -q "Dim5StructureDescriptor" HEAD -- cws.c; then
    echo "ERROR: PALP/cws.c at $(git -C "$PALP_DIR" rev-parse --short HEAD) lacks dim-5 descriptor generation." >&2
    echo "Expected the optimized dim-5 CWS generator branch or equivalent implementation." >&2
    exit 1
fi

echo "Building cws-5d.x..."
make -C "$PALP_DIR" -f GNUmakefile cws-5d.x >/tmp/audit_dim5_cws_build.log

if ! "$CWS_BIN" -h 2>&1 | grep -q "builtin canonical dim-5 structures"; then
    echo "ERROR: cws-5d.x help does not advertise canonical dim-5 structures." >&2
    exit 1
fi

echo "Running targeted dim-5 generator regressions..."
(
    cd "$PALP_DIR"
    export DIM=5
    tests/4.2.7-cws-c5-overlap3.sh
    tests/4.2.8-cws-c5-structure15.sh
    tests/4.2.9-cws-c5-structure37.sh
    tests/4.2.10-cws-c5-structure47.sh
    tests/4.2.13-cws-c5-structure11-canonical.sh
    bash tests/4.2.15-cws-c5-auto-structure5-sharded.sh
    bash tests/4.2.16-cws-c5-structure12-sharded.sh
)

echo "PASS: recovered dim-5 CWS generator builds and passes smoke regressions."
