#!/usr/bin/env bash
#SBATCH --job-name=fp-enum
#SBATCH --partition=std
#SBATCH --nodes=1
#SBATCH --cpus-per-task=8
#SBATCH --mem=16G
#SBATCH --time=00:45:00
#SBATCH --output=logs/slurm/fp-enum-%j.out
#SBATCH --error=logs/slurm/fp-enum-%j.err
#
# LLL + Fincke-Pohst-style walk experiment (LLL_FP_WALK.md / POINT_WALK_ALGORITHMS.md §3).
#
# 1. Build PALP's cws-5d.x with -DDUMP_BASIS (Coord.c emits, per candidate, the
#    triangular enumeration basis + X0 + Xmax + np to stderr as "BASIS ..." lines).
# 2. Collect a representative candidate dataset (spread anchors + the pathological
#    heavy anchor k=1) across the full structure-3 / W5 slot-0 pool.
# 3. Build scripts/fp_enum.c and run it: for every candidate it (a) replays PALP's
#    exact triangular walk (reference point set + node/division counts), (b) LLL-
#    reduces the basis in the box-scaled metric, (c) enumerates the SAME box with a
#    general Fincke-Pohst-style box-propagation walk on the LLL basis, and verifies
#    the point SETS are identical. Reports node/division ratios LLL vs triangular.
#
# All compute runs on the SLURM-allocated node; nothing on the head node.
set -uo pipefail
cd "${SLURM_SUBMIT_DIR:-$(pwd)}"
REPO="$(pwd)"
export PALP_W5_POOL="${PALP_W5_POOL:-$REPO/results/cache/w5.ip}"
POOL5=1833327

NANCH="${NANCH:-17}"           # spread anchors across the pool
PERANCH="${PERANCH:-9000}"     # candidates kept per spread anchor
STRIDE="${STRIDE:-11}"         # candidate sampling stride (spread anchors)
HEAVY_K="${HEAVY_K:-1}"        # pathological heavy anchor (slot-0 index 1)
HEAVY_N="${HEAVY_N:-40000}"    # candidates kept from the heavy anchor
HEAVY_STRIDE="${HEAVY_STRIDE:-3}"
PERRUN_TIMEOUT="${PERRUN_TIMEOUT:-40}"

OUT="$REPO/results/point-walk-opt"
TMP="${CLAUDE_JOB_DIR:-/tmp}/tmp"; mkdir -p "$TMP" "$OUT" "$REPO/logs/slurm"
BUILD="$TMP/palp-dump"

echo "### node: $(hostname)  cpus=${SLURM_CPUS_PER_TASK:-?}  $(date) ###"

# ---- 1. build the DUMP_BASIS dumper from the tracked PALP submodule ----
echo "### build cws-5d.x -DDUMP_BASIS ###"
rm -rf "$BUILD"; cp -r "$REPO/PALP" "$BUILD"
( cd "$BUILD" && make clean >/dev/null 2>&1
  make cws-5d.x CFLAGS="-O3 -march=native -DDUMP_BASIS" >"$TMP/build_dump.log" 2>&1 ) \
  && echo "  dumper OK" || { echo "  dumper FAIL"; tail -25 "$TMP/build_dump.log"; exit 1; }
DUMP="$BUILD/cws-5d.x"

# ---- 2. build fp_enum ----
echo "### build fp_enum ###"
gcc -O2 -march=native -Wall -o "$TMP/fp_enum" "$REPO/scripts/fp_enum.c" -lm \
  >"$TMP/build_fp.log" 2>&1 && echo "  fp_enum OK" \
  || { echo "  fp_enum FAIL"; cat "$TMP/build_fp.log"; exit 1; }

# ---- 3. collect candidates ----
CAND="$OUT/candidates.txt"; : > "$CAND"
collect(){ # k stride cap
  local k=$1 s=$2 cap=$3
  DUMP_STRIDE="$s" timeout "$PERRUN_TIMEOUT" "$DUMP" -c5 -s3 -j "$POOL5" -k "$k" 2>&1 >/dev/null \
    | grep --line-buffered '^BASIS' | head -n "$cap" >> "$CAND"
}
echo "### collect heavy anchor k=$HEAVY_K (cap $HEAVY_N, stride $HEAVY_STRIDE) ###"
collect "$HEAVY_K" "$HEAVY_STRIDE" "$HEAVY_N"
echo "  running total: $(wc -l < "$CAND")"
echo "### collect $NANCH spread anchors (cap $PERANCH each, stride $STRIDE) ###"
for ((i=0;i<NANCH;i++)); do
  k=$((1 + i*(POOL5-1)/(NANCH-1)))
  collect "$k" "$STRIDE" "$PERANCH"
done
NC=$(wc -l < "$CAND")
echo "  collected $NC candidate basis lines -> $CAND"
[ "$NC" -gt 0 ] || { echo "no candidates collected"; exit 1; }

# ---- 4. run the enumerator comparison ----
echo; echo "### fp_enum: correctness + node/division comparison ###"
"$TMP/fp_enum" --csv < "$CAND" > "$OUT/fp_enum.csv" 2> "$OUT/fp_enum_summary.txt"
cat "$OUT/fp_enum_summary.txt"

# ---- 5. per-candidate distribution (medians) from the CSV ----
echo; echo "### per-candidate distributions (awk over CSV) ###"
# CSV cols: idx,N,np,tri_nodes,tri_divs,gtri_nodes,gtri_divs,glll_nodes,glll_divs,np_ok,gtri_ok,glll_ok,lll_det
awk -F, 'NR>1 && $12==1 {
    np=$3; td=$5; ld=$9;
    # divisions per point produced (the §9.3 metric)
    if(np>0){ tpp=td/np; lpp=ld/np }
    print np, td, ld, (np>0?td/np:0), (np>0?ld/np:0), (td>0?ld/td:0)
  }' "$OUT/fp_enum.csv" | sort -n > "$TMP/dist.txt"
python3 - "$TMP/dist.txt" <<'PY'
import sys, statistics as st
rows=[l.split() for l in open(sys.argv[1]) if l.strip()]
if not rows: print("no rows"); sys.exit()
np=[float(r[0]) for r in rows]; td=[float(r[1]) for r in rows]; ld=[float(r[2]) for r in rows]
tpp=[float(r[3]) for r in rows]; lpp=[float(r[4]) for r in rows]; rat=[float(r[5]) for r in rows]
def pct(x,p):
    x=sorted(x); import math; i=min(len(x)-1,int(p/100*len(x))); return x[i]
print(f"candidates (glll_ok): {len(rows)}")
print(f"np            median={st.median(np):.0f}  p90={pct(np,90):.0f}  max={max(np):.0f}")
print(f"tri divs/point  median={st.median(tpp):.1f}  p90={pct(tpp,90):.1f}  max={max(tpp):.1f}")
print(f"LLL divs/point  median={st.median(lpp):.1f}  p90={pct(lpp,90):.1f}  max={max(lpp):.1f}")
print(f"per-cand LLL/tri div ratio  median={st.median(rat):.3f}  p10={pct(rat,10):.3f}  p90={pct(rat,90):.3f}")
# heavy tail np>=64
H=[(r) for r in rows if float(r[0])>=64]
if H:
    htd=sum(float(r[1]) for r in H); hld=sum(float(r[2]) for r in H)
    hr=[float(r[5]) for r in H]
    print(f"--- heavy tail np>=64: {len(H)} cand ---")
    print(f"  total tri divs={htd:.0f}  LLL divs={hld:.0f}  ratio={hld/htd:.3f}")
    print(f"  per-cand div ratio median={st.median(hr):.3f}")
PY
echo DONE
