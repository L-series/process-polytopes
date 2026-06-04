#!/usr/bin/env bash
#SBATCH --job-name=lllfp-int
#SBATCH --partition=std
#SBATCH --nodes=1
#SBATCH --cpus-per-task=8
#SBATCH --mem=16G
#SBATCH --time=02:00:00
#SBATCH --output=logs/slurm/lllfp-int-%j.out
#SBATCH --error=logs/slurm/lllfp-int-%j.err
#
# Integration test + benchmark for the in-tree LLL + Fincke-Pohst point walk
# wired into PALP's Make_CWS_Points (PALP/Coord.c, -DLLLFP_WALK; mode via the
# PALP_WALK env var: tri | fp | check).
#
#   Part A  CROSS-TYPE CORRECTNESS (PALP_WALK=check): for EVERY canonical dim-5
#           structure (s2..s47, spanning arity nw=2,3,4,5 / ambient N=7..10) run
#           BOTH walks per candidate and assert the enumerated point SETS are
#           identical (the binary prints "[LFCHK] ... mismatch=M"); M must be 0.
#
#   Part B  THROUGHPUT (PALP_PROFILE_TIMING): time Make_CWS_Points over a fixed
#           candidate budget under PALP_WALK=tri vs PALP_WALK=fp and report the
#           per-candidate cycle + wall speedup.  The dominant type-3 hot path
#           (s3) is sampled with a modulo shard to spread across the 1.83M-pair
#           pool (the head is a biased heavy/IP-rich anchor -- see
#           profile_cpu_pipeline.sh).  As an at-scale correctness cross-check
#           np_sum and ip_pass (from the profiler) MUST match between modes.
#
# All compute runs on the compute node (sbatch); nothing on the head node.
set -uo pipefail
cd "${SLURM_SUBMIT_DIR:-$(pwd)}"
REPO="$(pwd)"
PALP="$REPO/PALP"
OUT="$REPO/results/point-walk-opt"
mkdir -p "$OUT" "$REPO/logs/slurm"

CHECK_MAX="${CHECK_MAX:-100000}"     # max candidates per structure in check mode
TIME_LIMIT="${TIME_LIMIT:-200000}"   # candidates per mode in the timing benchmark

# canonical dim-5 structure arity: nw=2 (s2-7), 3 (s8-32), 4 (s33-45), 5 (s46-47)
nw_of() { local s=$1
  if   [ "$s" -le 7 ];  then echo 2
  elif [ "$s" -le 32 ]; then echo 3
  elif [ "$s" -le 45 ]; then echo 4
  else echo 5; fi; }

# Load the cached size-5 IP weight-system pool (same file the profiling and CUDA
# scripts use); without it cws-5d.x regenerates it via Make_34_Weights (>1 min)
# on EVERY invocation of a size-5 structure -- the dominant cost otherwise.
export PALP_W5_POOL="${PALP_W5_POOL:-$REPO/results/cache/w5.ip}"
if [ -s "$PALP_W5_POOL" ]; then echo "### PALP_W5_POOL=$PALP_W5_POOL ($(wc -l < "$PALP_W5_POOL") rows) ###"
else echo "### WARN: W5 pool cache missing at $PALP_W5_POOL -> will regenerate (slow) ###"; fi

CHECK_STRUCTS=( $(seq 2 47) )             # every canonical dim-5 structure
# bench: structure -> extra cws args (modulo spread shard for the huge pools)
declare -A BENCH_ARGS=( [3]="-j 89 -k 44" [11]="-j 17 -k 8" [40]="" )
BENCH_ORDER=(3 11 40)                     # nw=2 (type-3 hot path), nw=3, nw=4

echo "### node $(hostname)  cpus=${SLURM_CPUS_PER_TASK:-?}  $(date) ###"
echo "### CHECK_MAX=$CHECK_MAX  TIME_LIMIT=$TIME_LIMIT ###"

echo; echo "### build cws-5d.x with -DLLLFP_WALK ###"
rm -f "$PALP/cws-5d.x" "$PALP/Coord-5d.o" "$PALP/cws-5d.o"
make -C "$PALP" -f GNUmakefile cws-5d.x CPPFLAGS='-DLLLFP_WALK -fno-math-errno' \
  >/dev/null 2> "$OUT/lllfp_build.log" \
  && echo "  build OK" || { echo "  build FAIL"; tail -20 "$OUT/lllfp_build.log"; exit 1; }
BIN="$PALP/cws-5d.x"

echo; echo "############## Part A: cross-type correctness (check) ##############"
A_FAIL=0; A_TOTC=0; A_TOTM=0; A_TOTFB=0
printf "%-6s %-4s %-12s %-14s %-10s %-12s\n" struct nw candidates set-identical mismatch fp-fallback
for s in "${CHECK_STRUCTS[@]}"; do
  rep=$(PALP_WALK=check PALP_LFCHK_MAX="$CHECK_MAX" "$BIN" -c5 -s"$s" 2>&1 >/dev/null \
            | grep -m1 '^\[LFCHK\] candidates=')
  cand=$(sed -n 's/.*candidates=\([0-9]*\).*/\1/p' <<<"$rep")
  ident=$(sed -n 's/.*set-identical=\([0-9]*\).*/\1/p' <<<"$rep")
  mism=$(sed -n 's/.*mismatch=\([0-9]*\).*/\1/p' <<<"$rep")
  fb=$(sed -n 's/.*fp-fallback=\([0-9]*\).*/\1/p' <<<"$rep")
  printf "%-6s %-4s %-12s %-14s %-10s %-12s\n" "s$s" "$(nw_of "$s")" "${cand:-?}" "${ident:-?}" "${mism:-?}" "${fb:-?}"
  [ "${mism:-1}" = "0" ] && [ -n "${cand:-}" ] || A_FAIL=1
  A_TOTC=$((A_TOTC + ${cand:-0})); A_TOTM=$((A_TOTM + ${mism:-0})); A_TOTFB=$((A_TOTFB + ${fb:-0}))
done
echo "  totals: candidates=$A_TOTC  mismatch=$A_TOTM  fp-fallback=$A_TOTFB"
if [ "$A_FAIL" = "0" ]; then echo "  PART A PASS: 0 mismatches across all structures (nw=2..5)"
else echo "  PART A FAIL: see mismatches above"; fi

echo; echo "############## Part B: throughput tri vs fp ##############"
prof() {  # $1=mode $2=struct $3=extra-args -> echoes "candidates points_cycles wall np_sum ip_pass"
  local raw="$OUT/prof.$1.s$2.txt"
  PALP_WALK="$1" PALP_PROFILE_TIMING=1 PALP_PROFILE_LIMIT="$TIME_LIMIT" \
    "$BIN" -c5 -s"$2" $3 >/dev/null 2>"$raw" || true
  local p c pc wall np ipp; p=$(grep '^PROF ' "$raw" | head -1)
  c=$(sed -n 's/.*candidates=\([0-9]*\).*/\1/p' <<<"$p")
  pc=$(sed -n 's/.*points_cycles=\([0-9]*\).*/\1/p' <<<"$p")
  wall=$(sed -n 's/.*wall_secs=\([0-9.]*\).*/\1/p' <<<"$p")
  np=$(sed -n 's/.*np_sum=\([0-9]*\).*/\1/p' <<<"$p")
  ipp=$(sed -n 's/.*ip_pass=\([0-9]*\).*/\1/p' <<<"$p")
  echo "${c:-0} ${pc:-0} ${wall:-0} ${np:-0} ${ipp:-0}"
}
printf "%-6s %-4s %-10s | %-20s %-20s | %-7s %-7s | %s\n" \
  struct nw shard "tri cyc/cand(wall)" "fp cyc/cand(wall)" "cyc_x" "wall_x" "np/ip/n match"
B_FAIL=0
for s in "${BENCH_ORDER[@]}"; do
  args="${BENCH_ARGS[$s]}"
  read tc tpc tw tnp tip < <(prof tri "$s" "$args")
  read fc fpc fw fnp fip < <(prof fp  "$s" "$args")
  awk -v s="$s" -v nw="$(nw_of "$s")" -v shard="${args:-head}" \
      -v tc="${tc:-0}" -v tpc="${tpc:-0}" -v tw="${tw:-0}" -v tnp="${tnp:-0}" -v tip="${tip:-0}" \
      -v fc="${fc:-0}" -v fpc="${fpc:-0}" -v fw="${fw:-0}" -v fnp="${fnp:-0}" -v fip="${fip:-0}" 'BEGIN{
    tcc=(tc>0)?tpc/tc:0; fcc=(fc>0)?fpc/fc:0;
    cyx=(fcc>0)?tcc/fcc:0; wlx=(fw>0)?tw/fw:0;
    mok=((tnp==fnp)&&(tip==fip)&&(tc==fc))?"YES":"NO";
    printf "s%-5s %-4s %-10s | %11.0f(%5.2fs)  %11.0f(%5.2fs)  | %6.2fx %6.2fx | %s\n",
      s,nw,shard,tcc,tw,fcc,fw,cyx,wlx,mok;
  }'
  [ "${tnp:-0}" = "${fnp:-0}" ] && [ "${tip:-0}" = "${fip:-0}" ] && [ "${tc:-0}" = "${fc:-0}" ] || B_FAIL=1
done
if [ "$B_FAIL" = "0" ]; then echo "  PART B cross-check PASS: candidates, np_sum & ip_pass identical (tri==fp)"
else echo "  PART B cross-check FAIL: aggregate mismatch (see PROF files in $OUT)"; fi

echo; echo "### DONE $(date)  (A_FAIL=$A_FAIL B_FAIL=$B_FAIL) ###"
