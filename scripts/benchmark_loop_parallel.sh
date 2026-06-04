#!/usr/bin/env bash
#SBATCH --job-name=fp-par
#SBATCH --partition=std
#SBATCH --nodes=1
#SBATCH --cpus-per-task=16
#SBATCH --mem=16G
#SBATCH --time=00:30:00
#SBATCH --output=logs/slurm/fp-par-%j.out
#SBATCH --error=logs/slurm/fp-par-%j.err
#
# Part 2: (a) re-measure the LLL+FP walk including the LLL-reduction cost
#         (fp_enum now times lll_reduce separately), and
#         (b) the breadth-parallelism experiment (loop_parallel.c): OpenMP over
#         the outermost x4 loop on the heaviest candidates, scaling across
#         thread counts, verifying identical point counts.
# Reuses results/point-walk-opt/candidates.txt from benchmark_fp_enum.sh.
set -uo pipefail
cd "${SLURM_SUBMIT_DIR:-$(pwd)}"
REPO="$(pwd)"
OUT="$REPO/results/point-walk-opt"
TMP="${CLAUDE_JOB_DIR:-/tmp}/tmp"; mkdir -p "$TMP" "$REPO/logs/slurm"
CAND="$OUT/candidates.txt"
[ -s "$CAND" ] || { echo "missing $CAND (run benchmark_fp_enum.sh first)"; exit 1; }

echo "### node $(hostname) cpus=${SLURM_CPUS_PER_TASK:-?} candidates=$(wc -l < "$CAND") $(date) ###"

echo "### build ###"
gcc -O2 -march=native -o "$TMP/fp_enum" "$REPO/scripts/fp_enum.c" -lm \
  && echo "  fp_enum OK" || { echo "  fp_enum FAIL"; exit 1; }
gcc -O3 -march=native -fopenmp -o "$TMP/loop_parallel" "$REPO/scripts/loop_parallel.c" \
  && echo "  loop_parallel OK" || { echo "  loop_parallel FAIL"; exit 1; }

echo; echo "### (a) LLL+FP including LLL-reduction cost ###"
"$TMP/fp_enum" --csv < "$CAND" > "$OUT/fp_enum.csv" 2> "$OUT/fp_enum_summary.txt"
grep -E "tri replica|gen\(LLL\)|LLL det|tri \(PALP\)|gen\(LLL\)|ratio|wall|tri_enum" "$OUT/fp_enum_summary.txt"

echo; echo "### (b) breadth-parallelism over x4 (OpenMP), heaviest candidates ###"
for TH in 1 2 4 8 16; do
  echo "--- OMP_NUM_THREADS=$TH ---"
  OMP_NUM_THREADS=$TH "$TMP/loop_parallel" --top 8 --reps 7 < "$CAND" 2>/dev/null
done
echo DONE
