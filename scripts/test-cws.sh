#!/usr/bin/env bash
set -euo pipefail

repo_root="$(git rev-parse --show-toplevel)"
palp_root="${PALP_SOURCE_DIR:-$repo_root/PALP}"
make -C "$palp_root" cws-4d.x poly-4d.x cws-5d.x poly-5d.x
exec python3 "$repo_root/tests/test_cws.py" --bin-dir "$palp_root" "$@"
