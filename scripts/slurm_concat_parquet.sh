#!/usr/bin/env bash
#SBATCH --job-name=concat_poly
#SBATCH --partition=all
#SBATCH --nodes=1
#SBATCH --ntasks=1
#SBATCH --cpus-per-task=1
#SBATCH --mem=8G
#SBATCH --time=08:00:00
#SBATCH --output=logs/proc_poly/concat-%j.out
#SBATCH --error=logs/proc_poly/concat-%j.err
set -euo pipefail

REPO_ROOT="${REPO_ROOT:-$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)}"
BIN="$REPO_ROOT/src/process/build/concat_parquet"
INPUT_DIR="${INPUT_DIR:-$HOME/data/unique_polytopes_clean_dataset_final}"
OUTPUT="${OUTPUT:-$HOME/data/unique_polytopes_clean.parquet}"

export LD_LIBRARY_PATH="${CONDA_PREFIX:-$HOME/.local/share/micromamba/envs/process-polytopes}/lib:${LD_LIBRARY_PATH:-}"

tmp="${OUTPUT}.tmp"
rm -f "$tmp"

echo "input dir: $INPUT_DIR"
echo "output:    $OUTPUT"
echo "tmp:       $tmp"

"$BIN" "$INPUT_DIR" "$tmp"
mv -f "$tmp" "$OUTPUT"
echo "done: $OUTPUT"
