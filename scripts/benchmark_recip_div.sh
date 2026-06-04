#!/usr/bin/env bash
#SBATCH --job-name=recip-div
#SBATCH --partition=std
#SBATCH --nodes=1
#SBATCH --exclusive
#SBATCH --cpus-per-task=128
#SBATCH --mem=0
#SBATCH --time=00:30:00
#SBATCH --output=logs/slurm/recip-div-%j.out
#SBATCH --error=logs/slurm/recip-div-%j.err
#
# CPU point-walk division experiment: replace the hardware idiv inside PD_Floor
# (the per-level bound computation CLB) with a libdivide reciprocal-multiply,
# using dividers precomputed once per candidate for each basis pivot.  Builds
# baseline vs -DRECIP_DIV from one dual-mode Coord.c, verifies bit-identical
# per-candidate output (NP signature), then measures Make_CWS_Points cycles via
# the in-binary rdtsc profiler (PALP_PROFILE_TIMING) over the 16 spread anchors
# used by profile_makepoints_substages.sh.  See PIPELINE_PROFILING.md s9-s10 and
# POINT_WALK_ALGORITHMS.md.
set -uo pipefail
cd "${SLURM_SUBMIT_DIR:-$(pwd)}"
PEXP="${PEXP:-$CLAUDE_JOB_DIR/tmp/pexp}"
export PALP_W5_POOL="${PALP_W5_POOL:-$(pwd)/results/cache/w5.ip}"
POOL5=1833327
LIMIT="${LIMIT:-100000}"
NANCH="${NANCH:-16}"
CF="${CF:--O3 -march=native}"
OUT=results/point-walk-opt
mkdir -p "$OUT" logs/slurm
echo "host=$(hostname)  PEXP=$PEXP  CFLAGS='$CF'  LIMIT=$LIMIT  NANCH=$NANCH"
[ -f "$PEXP/Coord.c" ] || { echo "missing $PEXP/Coord.c (dual-mode RECIP_DIV source)"; exit 1; }
[ -f "$PEXP/libdivide.h" ] || { echo "missing $PEXP/libdivide.h"; exit 1; }

cd "$PEXP"
echo "### build baseline ###"
make clean >/dev/null 2>&1; make cws-5d.x CFLAGS="$CF" >/tmp/bd_base.log 2>&1 \
  && cp cws-5d.x cws-base.x && echo "  baseline OK" || { echo "  baseline FAIL"; tail -20 /tmp/bd_base.log; exit 1; }
echo "### build recip ###"
make clean >/dev/null 2>&1; make cws-5d.x CFLAGS="$CF -DRECIP_DIV" >/tmp/bd_recip.log 2>&1 \
  && cp cws-5d.x cws-recip.x && echo "  recip OK" || { echo "  recip FAIL"; tail -30 /tmp/bd_recip.log; exit 1; }
cd "$OLDPWD"

anchors(){ local n=$1 i; for((i=1;i<=n;i++)); do echo $((1+(i-1)*(POOL5-1)/(n-1))); done; }

echo; echo "### correctness: per-candidate NP signature (np,dim,ip) must be identical ###"
cpass=1
for k in 1 $((POOL5/3)) $((2*POOL5/3)) $((POOL5-1)); do
  PALP_PROFILE_NP=1 "$PEXP/cws-base.x"  -c5 -s3 -j $POOL5 -k $k 2>/dev/null | head -n 60000 > /tmp/np_b.txt
  PALP_PROFILE_NP=1 "$PEXP/cws-recip.x" -c5 -s3 -j $POOL5 -k $k 2>/dev/null | head -n 60000 > /tmp/np_r.txt
  if diff -q /tmp/np_b.txt /tmp/np_r.txt >/dev/null; then echo "  anchor $k: MATCH ($(wc -l </tmp/np_b.txt))";
  else echo "  anchor $k: MISMATCH"; diff /tmp/np_b.txt /tmp/np_r.txt | head -4; cpass=0; fi
done
echo "  => $([ $cpass = 1 ] && echo BIT-IDENTICAL || echo BROKEN)"

run_prof(){ # $1=bin  -> echo "candidates points_cycles ip_cycles"
  local bin=$1 tc=0 tp=0 ti=0 a
  for a in $(anchors "$NANCH"); do
    line=$(PALP_PROFILE_TIMING=1 PALP_PROFILE_LIMIT=$LIMIT "$bin" -c5 -s3 -j $POOL5 -k $a 2>&1 >/dev/null | grep '^PROF mode=')
    c=$(sed -n 's/.*candidates=\([0-9]*\).*/\1/p' <<<"$line")
    p=$(sed -n 's/.*points_cycles=\([0-9]*\).*/\1/p' <<<"$line")
    i=$(sed -n 's/.*ip_cycles=\([0-9]*\).*/\1/p' <<<"$line")
    tc=$((tc+${c:-0})); tp=$((tp+${p:-0})); ti=$((ti+${i:-0}))
  done
  echo "$tc $tp $ti"
}

echo; echo "### timing: Make_CWS_Points cycles/candidate (rdtsc), $NANCH anchors x $LIMIT cand ###"
read cb pb ib <<<"$(run_prof "$PEXP/cws-base.x")"
read cr pr ir <<<"$(run_prof "$PEXP/cws-recip.x")"
awk -v cb=$cb -v pb=$pb -v ib=$ib -v cr=$cr -v pr=$pr -v ir=$ir 'BEGIN{
  bpc=pb/cb; rpc=pr/cr; bic=ib/cb; ric=ir/cr;
  printf "  baseline : cand=%d  points_cyc/cand=%.1f  ip_cyc/cand=%.1f\n", cb, bpc, bic;
  printf "  recip    : cand=%d  points_cyc/cand=%.1f  ip_cyc/cand=%.1f\n", cr, rpc, ric;
  printf "  Make_CWS_Points speedup (points_cyc): %.4fx  (%.1f%% faster)\n", bpc/rpc, 100*(bpc-rpc)/bpc;
  printf "  IP_Check control (should be ~1.0x)  : %.4fx\n", bic/ric;
}'
echo; echo "raw build logs: /tmp/bd_base.log /tmp/bd_recip.log"; echo DONE
