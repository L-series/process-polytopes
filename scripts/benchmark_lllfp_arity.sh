#!/usr/bin/env bash
#SBATCH --job-name=lllfp-arity
#SBATCH --partition=std
#SBATCH --nodes=1
#SBATCH --cpus-per-task=8
#SBATCH --mem=16G
#SBATCH --time=01:00:00
#SBATCH --output=logs/slurm/lllfp-arity-%j.out
#SBATCH --error=logs/slurm/lllfp-arity-%j.err
#
# Is the LLL+FP benefit structured by arity (nw)?  Measure ungated fp (gate off)
# vs triangular per-candidate cycles across structures spanning every arity, to
# decide whether "use fp iff nw==2 (type-3 geometry)" is a clean production rule.
set -uo pipefail
cd "${SLURM_SUBMIT_DIR:-$(pwd)}"
REPO="$(pwd)"; PALP="$REPO/PALP"; OUT="$REPO/results/point-walk-opt"
mkdir -p "$OUT" "$REPO/logs/slurm"
export PALP_W5_POOL="${PALP_W5_POOL:-$REPO/results/cache/w5.ip}"
TIME_LIMIT="${TIME_LIMIT:-150000}"
nw_of(){ local s=$1; if [ "$s" -le 7 ]; then echo 2; elif [ "$s" -le 32 ]; then echo 3; elif [ "$s" -le 45 ]; then echo 4; else echo 5; fi; }

echo "### node $(hostname) $(date)  TIME_LIMIT=$TIME_LIMIT ###"
rm -f "$PALP/cws-5d.x" "$PALP/Coord-5d.o" "$PALP/cws-5d.o"
make -C "$PALP" -f GNUmakefile cws-5d.x CPPFLAGS='-DLLLFP_WALK -fno-math-errno' \
  >/dev/null 2>"$OUT/lllfp_build.log" && echo "build OK" || { echo "build FAIL"; tail "$OUT/lllfp_build.log"; exit 1; }
BIN="$PALP/cws-5d.x"

# modulo spread shard for the big pools; head for small structures
declare -A ARGS=( [2]="-j 89 -k 44" [3]="-j 89 -k 44" [4]="-j 89 -k 44" [5]="" [6]="-j 89 -k 44" [7]="-j 89 -k 44" \
                  [11]="-j 17 -k 8" [20]="-j 17 -k 8" [38]="" [40]="" [46]="" )
cyc(){ # $1=mode $2=struct $3=args -> "cyc_per_cand candidates"
  local raw="$OUT/ar.$1.s$2.txt"
  PALP_WALK="$1" PALP_PROFILE_TIMING=1 PALP_PROFILE_LIMIT="$TIME_LIMIT" \
    "$BIN" -c5 -s"$2" $3 >/dev/null 2>"$raw" || true
  local p c pc; p=$(grep -m1 '^PROF ' "$raw")
  c=$(sed -n 's/.*candidates=\([0-9]*\).*/\1/p' <<<"$p")
  pc=$(sed -n 's/.*points_cycles=\([0-9]*\).*/\1/p' <<<"$p")
  awk -v c="${c:-0}" -v pc="${pc:-0}" 'BEGIN{printf "%.0f %d",(c>0?pc/c:0),c}'
}
printf "%-6s %-4s %-12s %-12s %-12s %-10s\n" struct nw tri_cyc "fp_cyc_t0" cand speedup
for s in 2 3 4 5 6 7 11 20 38 40 46; do
  read tcyc tc < <(cyc tri "$s" "${ARGS[$s]}")
  read fcyc fc < <(cyc fp  "$s" "${ARGS[$s]}")
  awk -v s="$s" -v nw="$(nw_of "$s")" -v tcyc="$tcyc" -v fcyc="$fcyc" -v fc="$fc" 'BEGIN{
    printf "%-6s %-4s %-12s %-12s %-12s %-10s\n","s"s,nw,tcyc,fcyc,fc,(fcyc>0?sprintf("%.2fx",tcyc/fcyc):"-")}'
done
rm -f "$OUT"/ar.*.txt
echo "### DONE $(date) ###"
