#!/usr/bin/env bash
#SBATCH --job-name=lllfp-sweep
#SBATCH --partition=std
#SBATCH --nodes=1
#SBATCH --cpus-per-task=8
#SBATCH --mem=16G
#SBATCH --time=02:00:00
#SBATCH --output=logs/slurm/lllfp-sweep-%j.out
#SBATCH --error=logs/slurm/lllfp-sweep-%j.err
#
# Tune the heavy-tail gate (PALP_LF_LOGVOL_MIN) for the in-tree LLL+FP walk and
# confirm the cheaper (incremental-mu) LLL still enumerates identical point sets.
#
#   1. CORRECTNESS re-check (PALP_WALK=check) on a sample spanning nw=2..5,
#      after the LLL incremental-mu optimization -> mismatch must stay 0.
#   2. THRESHOLD SWEEP: per representative structure, time tri (baseline) and
#      fp at several PALP_LF_LOGVOL_MIN gate thresholds; report hybrid cyc/cand,
#      speedup vs tri, and the light(gated-to-triangular) fraction.
set -uo pipefail
cd "${SLURM_SUBMIT_DIR:-$(pwd)}"
REPO="$(pwd)"; PALP="$REPO/PALP"; OUT="$REPO/results/point-walk-opt"
mkdir -p "$OUT" "$REPO/logs/slurm"
export PALP_W5_POOL="${PALP_W5_POOL:-$REPO/results/cache/w5.ip}"
CHECK_MAX="${CHECK_MAX:-50000}"
TIME_LIMIT="${TIME_LIMIT:-200000}"
THRESHOLDS=(0 20 25 30 35 40 45 50)
nw_of(){ local s=$1; if [ "$s" -le 7 ]; then echo 2; elif [ "$s" -le 32 ]; then echo 3; elif [ "$s" -le 45 ]; then echo 4; else echo 5; fi; }

echo "### node $(hostname) $(date)  CHECK_MAX=$CHECK_MAX TIME_LIMIT=$TIME_LIMIT ###"
echo "### W5=$PALP_W5_POOL ($([ -s "$PALP_W5_POOL" ] && wc -l < "$PALP_W5_POOL" || echo MISSING) rows) ###"
echo; echo "### build cws-5d.x -DLLLFP_WALK ###"
rm -f "$PALP/cws-5d.x" "$PALP/Coord-5d.o" "$PALP/cws-5d.o"
make -C "$PALP" -f GNUmakefile cws-5d.x CPPFLAGS='-DLLLFP_WALK -fno-math-errno' \
  >/dev/null 2>"$OUT/lllfp_build.log" && echo "  build OK" || { echo "  build FAIL"; tail "$OUT/lllfp_build.log"; exit 1; }
BIN="$PALP/cws-5d.x"

echo; echo "############## 1. correctness re-check (incremental-mu LLL) ##############"
CFAIL=0
printf "%-6s %-4s %-12s %-10s %-12s\n" struct nw candidates mismatch fp-fallback
for s in 3 11 38 40 46; do
  rep=$(PALP_WALK=check PALP_LFCHK_MAX="$CHECK_MAX" "$BIN" -c5 -s"$s" 2>&1 >/dev/null | grep -m1 '^\[LFCHK\] candidates=')
  cand=$(sed -n 's/.*candidates=\([0-9]*\).*/\1/p' <<<"$rep")
  mism=$(sed -n 's/.*mismatch=\([0-9]*\).*/\1/p' <<<"$rep")
  fb=$(sed -n 's/.*fp-fallback=\([0-9]*\).*/\1/p' <<<"$rep")
  printf "%-6s %-4s %-12s %-10s %-12s\n" "s$s" "$(nw_of "$s")" "${cand:-?}" "${mism:-?}" "${fb:-?}"
  [ "${mism:-1}" = "0" ] || CFAIL=1
done
[ "$CFAIL" = 0 ] && echo "  CORRECTNESS PASS (0 mismatches)" || echo "  CORRECTNESS FAIL"

# run -> "cyc_per_cand candidates light_frac_pct"
run() { # $1=mode $2=struct $3=args $4=logvol
  local raw="$OUT/sw.$1.s$2.t${4:-x}.txt"
  PALP_WALK="$1" PALP_LF_LOGVOL_MIN="${4:-0}" PALP_PROFILE_TIMING=1 PALP_PROFILE_LIMIT="$TIME_LIMIT" \
    "$BIN" -c5 -s"$2" $3 >/dev/null 2>"$raw" || true
  local pc c lf fpw
  c=$(grep -m1 '^PROF ' "$raw"  | sed -n 's/.*candidates=\([0-9]*\).*/\1/p')
  pc=$(grep -m1 '^PROF ' "$raw" | sed -n 's/.*points_cycles=\([0-9]*\).*/\1/p')
  lf=$(grep -m1 '^\[LFFP\]' "$raw" | sed -n 's/.*light(triangular-gate)=\([0-9]*\).*/\1/p')
  awk -v c="${c:-0}" -v pc="${pc:-0}" -v lf="${lf:-0}" 'BEGIN{
    printf "%.0f %d %.1f", (c>0?pc/c:0), c, (c>0?100.0*lf/c:0) }'
}

echo; echo "############## 2. gate threshold sweep (cyc/cand, speedup vs tri) ##############"
declare -A ARGS=( [3]="-j 89 -k 44" [11]="-j 17 -k 8" [40]="" )
for s in 3 11 40; do
  read tcyc tc tl < <(run tri "$s" "${ARGS[$s]}" 0)
  echo "--- s$s (nw=$(nw_of "$s"), shard='${ARGS[$s]:-head}') : tri baseline ${tcyc} cyc/cand over ${tc} cand ---"
  printf "  %-10s %-14s %-10s %-12s\n" logvol fp_cyc/cand speedup light%
  for t in "${THRESHOLDS[@]}"; do
    read fcyc fc fl < <(run fp "$s" "${ARGS[$s]}" "$t")
    awk -v t="$t" -v fcyc="$fcyc" -v tcyc="$tcyc" -v fl="$fl" 'BEGIN{
      printf "  %-10s %-14s %-10s %-12s\n", t, fcyc, (fcyc>0?sprintf("%.2fx",tcyc/fcyc):"-"), fl"%" }'
  done
done
rm -f "$OUT"/sw.*.txt
echo; echo "### DONE $(date) ###"
