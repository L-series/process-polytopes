#!/usr/bin/env bash
#SBATCH --job-name=ovf-cpu
#SBATCH --partition=std
#SBATCH --nodes=1
#SBATCH --cpus-per-task=128
#SBATCH --mem=200G
#SBATCH --time=03:00:00
#SBATCH --output=/home/ahat01/cws43run/logs/ovf-cpu-%j.out
#SBATCH --error=/home/ahat01/cws43run/logs/ovf-cpu-%j.out
#
# Process the GPU overflow (np>256, deferred) on the CPU: pipe each overflow CWS
# row through PALP cws-5d.x -i (full point enumeration + IP/reflexivity check, no
# np cap). Output lines (M:.. N:..) are the reflexive overflow CWS -> add to the
# accepted set for completeness. The GPU overflow format IS PALP CWS input format
# (verified), so no conversion. Parallelized 128-way with split+xargs.
set -uo pipefail
cd "${SLURM_SUBMIT_DIR:-$(pwd)}"
export PALP_W5_POOL=results/cache/w5.ip
BIN="$PWD/PALP/cws-5d.x"
OUT=/home/ahat01/cws43run; WORK=$OUT/ovf_cpu
rm -rf "$WORK"; mkdir -p "$WORK/chunks" "$WORK/out"
echo "### host=$(hostname) $(date) ###"
echo "concatenating overflow..."
cat "$OUT"/overflow/*.ovf > "$WORK/all_overflow.txt" 2>/dev/null
N=$(wc -l < "$WORK/all_overflow.txt"); echo "total overflow rows: $N"
echo "splitting into chunks of 20000..."
split -l 20000 -d -a 5 "$WORK/all_overflow.txt" "$WORK/chunks/c"
NCH=$(ls "$WORK/chunks" | wc -l); echo "chunks: $NCH"
echo "### IP-checking on 128 cores $(date) ###"
start=$(date +%s)
ls "$WORK/chunks"/c* | xargs -P 128 -I{} bash -c '"'"$BIN"'" -i -f < "{}" > "'"$WORK"'/out/$(basename {}).out" 2>/dev/null'
echo "### collecting $(date) (took $(($(date +%s)-start))s) ###"
cat "$WORK"/out/*.out > "$OUT/overflow_reflexive.txt"
REFL=$(wc -l < "$OUT/overflow_reflexive.txt")
echo "overflow rows processed : $N"
echo "overflow REFLEXIVE (IP-pass): $REFL"
echo "  -> overflow IP-pass rate: $(python3 -c "print(f'{100*$REFL/$N:.3f}%')")"
echo "result: $OUT/overflow_reflexive.txt"
rm -rf "$WORK/chunks" "$WORK/out" "$WORK/all_overflow.txt"
echo "### DONE $(date) ###"
