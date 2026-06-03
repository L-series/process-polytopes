#!/usr/bin/env bash
#SBATCH --job-name=mkpts-prof
#SBATCH --partition=std
#SBATCH --nodes=1
#SBATCH --exclusive
#SBATCH --cpus-per-task=128
#SBATCH --mem=0
#SBATCH --time=00:15:00
#SBATCH --output=logs/slurm/mkpts-prof-%j.out
#SBATCH --error=logs/slurm/mkpts-prof-%j.err
#
# Sub-stage profiling of Make_CWS_Points (dim-5 fast path): prologue (basis
# construction) vs walk, and within the walk the PD_Floor / CLB / point-store
# operation counts. Uses the MKPTS_PROFILE-instrumented cws-5d.x over the same
# 16 spread anchors as profile_cpu_pipeline.sh (representative candidate mix),
# each capped at MKP_LIMIT candidates so it completes quickly.
set -uo pipefail
cd "${SLURM_SUBMIT_DIR:-$(pwd)}"
BIN=results/aristotle-validation/bin/cws_mkpts_prof
export PALP_W5_POOL="${PALP_W5_POOL:-$(pwd)/results/cache/w5.ip}"
POOL5=1833327
LIMIT="${MKP_LIMIT:-100000}"
NANCH="${NANCH:-16}"
OUT=results/aristotle-validation/mkpts
mkdir -p "$OUT" logs/slurm
[ -x "$BIN" ] || { echo "missing $BIN"; exit 1; }
echo "host=$(hostname) MKP_LIMIT=$LIMIT NANCH=$NANCH"

anchors(){ local n=$1 i; for((i=1;i<=n;i++)); do echo $((1+(i-1)*(POOL5-1)/(n-1))); done; }
: > "$OUT/raw.txt"
for a in $(anchors "$NANCH"); do
  MKP_LIMIT="$LIMIT" "$BIN" -c5 -s3 -j "$POOL5" -k "$a" 2>>"$OUT/raw.txt" >/dev/null || true
done

echo; echo "### aggregate over $NANCH spread anchors (MKP_LIMIT=$LIMIT each) ###"
awk '/^MKP calls=/{
  for(i=2;i<=NF;i++){split($i,p,"=");v[p[1]]+=p[2]}
}
END{
  c=v["calls"]; tot=v["prologue_cyc"]+v["walk_cyc"];
  printf "candidates (calls)      : %d\n", c;
  printf "Make_CWS_Points cyc/cand: %.0f  (prologue %.0f + walk %.0f)\n",(v["prologue_cyc"]+v["walk_cyc"])/c, v["prologue_cyc"]/c, v["walk_cyc"]/c;
  printf "  prologue (Make_CWS_Basis + perm + Xmax/Amin) : %5.2f%%\n", 100*v["prologue_cyc"]/tot;
  printf "  walk (5 nested loops, CLB bounds + store)    : %5.2f%%\n", 100*v["walk_cyc"]/tot;
  printf "\nwithin the walk, per candidate:\n";
  printf "  PD_Floor (64-bit integer divisions) : %12.1f\n", v["pdfloor"]/c;
  printf "  CLB calls (bound computations)      : %12.1f   (%.1f PD_Floor / CLB)\n", v["clb"]/c, v["clb"]?v["pdfloor"]/v["clb"]:0;
  printf "  lattice points stored               : %12.2f\n", v["points"]/c;
  printf "  pruned subtrees (CLB R=0 infeasible) : %12.2f\n", v["prune"]/c;
  printf "  loop trips  x4=%.1f x3=%.1f x2=%.1f x1=%.1f\n", v["trips4"]/c,v["trips3"]/c,v["trips2"]/c,v["trips1"]/c;
  printf "\nyield + division-cost attribution:\n";
  printf "  points / PD_Floor          : %.4f   (divisions per point produced = %.1f)\n", v["points"]/v["pdfloor"], v["pdfloor"]/v["points"];
  printf "  walk cyc / PD_Floor        : %.2f   (effective cost per division-step on this CPU)\n", v["walk_cyc"]/v["pdfloor"];
  printf "  PD_Floor share of walk @~7c: %.1f%%  (16-bit log: divisions dominate the walk)\n", 100*7.0*v["pdfloor"]/v["walk_cyc"];
}' "$OUT/raw.txt"
echo; echo "raw per-anchor MKP lines in $OUT/raw.txt"; echo DONE
