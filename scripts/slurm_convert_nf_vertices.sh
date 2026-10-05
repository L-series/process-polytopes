#!/usr/bin/env bash
#SBATCH --job-name=convert_nf
#SBATCH --partition=all
#SBATCH --nodes=1
#SBATCH --ntasks=1
#SBATCH --cpus-per-task=1
#SBATCH --mem=32G
#SBATCH --time=04:00:00
#SBATCH --output=logs/proc_poly/convert-nf-%j.out
#SBATCH --error=logs/proc_poly/convert-nf-%j.err
set -euo pipefail

REPO_ROOT="${REPO_ROOT:-$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)}"
export CONDA_PREFIX="${CONDA_PREFIX:-$HOME/.local/share/micromamba/envs/process-polytopes}"
export LD_LIBRARY_PATH="$CONDA_PREFIX/lib:${LD_LIBRARY_PATH:-}"

INPUT="${INPUT:-$HOME/data/ws5d_sieved_dataset_v2_clean.parquet}"
OUTPUT="${OUTPUT:-$HOME/data/ws5d_sieved_dataset_v2_clean_lists.parquet}"
BIN="$REPO_ROOT/src/process/build/convert_nf_vertices"

tmp="${OUTPUT}.tmp"
rm -f "$tmp"

echo "input:  $INPUT"
echo "output: $OUTPUT"
echo "tmp:    $tmp"

"$BIN" --input "$INPUT" --output "$tmp"
mv -f "$tmp" "$OUTPUT"
echo "done: $OUTPUT"
