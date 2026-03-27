#!/usr/bin/env bash
# build_and_run.sh — one-shot build + example invocation
set -euo pipefail

# ── 1. Install dependencies (Ubuntu / Debian) ─────────────────────────────────
install_deps() {
  echo "Installing Arrow + Parquet C++ libraries..."
  sudo apt-get update -qq
  sudo apt-get install -y \
    cmake ninja-build \
    libarrow-dev libparquet-dev libarrow-dataset-dev \
    libssl-dev libomp-dev
}

# ── 2. Build ───────────────────────────────────────────────────────────────────
build() {
  local build_dir="build"
  mkdir -p "$build_dir"
  cmake -S . -B "$build_dir" \
    -DCMAKE_BUILD_TYPE=Release \
    -GNinja
  cmake --build "$build_dir" --parallel "$(nproc)"
  echo "Binary: $build_dir/sort_parquet"
}

# ── 3. Run (edit paths to match your setup) ───────────────────────────────────
run() {
  local THREADS
  THREADS="$(nproc)"

  ./build/sort_parquet \
    --input   ./ws-5d-reflexive \
    --tmp     ./ws-5d-sorted-tmp \
    --output  ./ws-5d-sorted \
    --threads "$THREADS"

  # To resume a crashed run without re-sorting phase-1:
  # ./build/sort_parquet \
  #   --input  ./ws-5d-reflexive \
  #   --tmp    ./ws-5d-sorted-tmp \
  #   --output ./ws-5d-sorted     \
  #   --threads "$THREADS"        \
  #   --skip-phase1
}

# ── Main ──────────────────────────────────────────────────────────────────────
case "${1:-all}" in
  deps)  install_deps ;;
  build) build        ;;
  run)   run          ;;
  all)   install_deps; build; run ;;
  *) echo "Usage: $0 [deps|build|run|all]"; exit 1 ;;
esac
