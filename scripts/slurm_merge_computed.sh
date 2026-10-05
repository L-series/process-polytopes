#!/usr/bin/env bash
#SBATCH --job-name=merge_poly
#SBATCH --partition=all
#SBATCH --nodes=1
#SBATCH --ntasks=1
#SBATCH --cpus-per-task=1
#SBATCH --mem=8G
#SBATCH --time=08:00:00
#SBATCH --output=logs/proc_poly/merge-%j.out
#SBATCH --error=logs/proc_poly/merge-%j.err
set -euo pipefail

REPO_ROOT="${REPO_ROOT:-$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)}"
BIN="$REPO_ROOT/src/process/build/merge_computed"

INPUT="${INPUT:-$HOME/data/unique_polytopes.parquet}"
COMPUTED_DIR="${COMPUTED_DIR:-$HOME/data/unique_polytopes_computed}"
OUTPUT="${OUTPUT:-$HOME/data/unique_polytopes_clean.parquet}"
RG_PER_SHARD="${RG_PER_SHARD:-4}"

export LD_LIBRARY_PATH="${CONDA_PREFIX:-$HOME/.local/share/micromamba/envs/process-polytopes}/lib:${LD_LIBRARY_PATH:-}"

tmp="${OUTPUT}.tmp"
rm -f "$tmp"

echo "input:        $INPUT"
echo "computed dir: $COMPUTED_DIR"
echo "output:       $OUTPUT"
echo "tmp:          $tmp"
echo "rg/shard:     $RG_PER_SHARD"

"$BIN" --input "$INPUT" \
       --computed-dir "$COMPUTED_DIR" \
       --output "$tmp" \
       --rg-per-shard "$RG_PER_SHARD"

mv -f "$tmp" "$OUTPUT"
echo "done: $OUTPUT"
