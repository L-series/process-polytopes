#!/usr/bin/env bash
set -euo pipefail

repo_root="$(git rev-parse --show-toplevel)"
mode="${1:-conditional}"
case "$mode" in
conditional | strict) ;;
*)
  echo "Usage: $0 [conditional|strict]" >&2
  exit 2
  ;;
esac

cd "$repo_root/proofs"
mkdir -p ci-output
python3 -c 'import sys; sys.path.insert(0, "ci"); from pathlib import Path; from proof_audit import check_pins; errors = check_pins(Path(".")); sys.exit("\n".join(errors) if errors else 0)'
if [[ "$(lean --version)" != "Lean (version 4.30.0,"* ]]; then
  echo "Expected the reviewed Lean 4.30.0 compiler" >&2
  exit 1
fi
lake build
PROOF_CI_OUTPUT=ci-output/inventory.json lake env lean ci/Export.lean
lake env lean ci/PipelineTests.lean
python3 ../tests/test_proof_ci.py
python3 ci/proof_audit.py "$mode" --inventory ci-output/inventory.json
if [[ "$mode" == strict ]]; then
  lake env lean ci/Executable.lean
fi
