#!/usr/bin/env bash

set -euo pipefail

if [[ $# -lt 1 ]]; then
    echo "usage: $0 <shell-name> [nix-develop args ...]" >&2
    exit 2
fi

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
SHELL_NAME="$1"
shift

BUILD_DIR="${NIX_LOCAL_BUILD_DIR:-$REPO_ROOT/.nix-build-tmp}"
mkdir -p "$BUILD_DIR"

export TMPDIR="$BUILD_DIR"
export NIX_BUILD_TOP="$BUILD_DIR"
export NIXPKGS_ALLOW_UNFREE="${NIXPKGS_ALLOW_UNFREE:-1}"

cd "$REPO_ROOT"
exec nix develop ".#$SHELL_NAME" \
    --option sandbox-build-dir "$BUILD_DIR" \
    --option build-dir "$BUILD_DIR" \
    "$@"