#!/usr/bin/env bash
#SBATCH --job-name=aristotle-bench
#SBATCH --partition=std
#SBATCH --nodes=1
#SBATCH --exclusive
#SBATCH --cpus-per-task=128
#SBATCH --mem=0
#SBATCH --time=00:50:00
#SBATCH --output=logs/slurm/aristotle-bench-%j.out
#SBATCH --error=logs/slurm/aristotle-bench-%j.err
#
# Validate + benchmark the two Aristotle optimization passes against the baseline
# PALP, on the dim-5 CWS pipeline (structures 3 = "5-5" and 6 = "4-5"). Reuses
# the project's rdtsc hook (PALP_PROFILE_TIMING / GENONLY) and the spread-anchor
# representativeness method from scripts/profile_cpu_pipeline.sh.
#
# Trees compared (each rebuilt clean here for identical flags/node):
#   BASE = process-polytopes/PALP               (project baseline)
#   A1   = ~/aristotle_1/PALP_aristotle         (Make_CWS_Points rewrite)
#   A2   = ~/aristotle_2/output-final_aristotle (A1 + Xmax<=1 reject + micro-opts)
#
# Phases:
#   BUILD  clean rebuild of cws-5d.x in all three trees
#   T      TIMING: PALP_PROFILE_TIMING over spread anchors, fixed per-anchor
#          limit -> identical candidate subset across binaries -> compare
#          points_cycles/cand (Make_CWS_Points) and ip_cycles/cand. (A2's
#          profiler path does NOT apply the Xmax reject, so this isolates the
#          point-walk rewrite.)
#   C      CORRECTNESS: production output (-c5 -s# -jJ -kK) diffed pairwise.
#          BASE==A1  proves the point-walk rewrite is byte-identical.
#          A1 ==A2   proves the Xmax reject + dim-exit drop NO IP polytope
#                    (A2 only ever REMOVES candidates, so equality == soundness).
#   P      PRODUCTION THROUGHPUT: wall time to run a full shard to completion,
#          BASE vs A2 -> the real end-to-end speedup incl. the Xmax reject.
set -uo pipefail

REPO_ROOT="${SLURM_SUBMIT_DIR:-$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)}"
cd "$REPO_ROOT"
W5="${PALP_W5_POOL:-$REPO_ROOT/results/cache/w5.ip}"
export PALP_W5_POOL="$W5"
BASE="$REPO_ROOT/PALP"
A1="$HOME/aristotle_1/PALP_aristotle"
A2="$HOME/aristotle_2/output-final_aristotle"
POOL5=1833327                         # type-3 slot-0 pool size (for spread anchors)
LIMIT="${LIMIT:-100000}"              # candidates per anchor in TIMING phase
NANCHORS="${NANCHORS:-8}"
STRUCTS="${STRUCTS:-3 6}"
CORR_J="${CORR_J:-}"                  # production shard -j (auto-probed if empty)
OUT="$REPO_ROOT/results/aristotle-validation"
mkdir -p "$OUT" "$REPO_ROOT/logs/slurm"

echo "host=$(hostname) nproc=$(nproc)  w5=$W5  LIMIT=$LIMIT NANCHORS=$NANCHORS STRUCTS='$STRUCTS'"
lscpu 2>/dev/null | grep -E "Model name|^CPU\(s\)|MHz" || true

# ---------- BUILD ----------
echo; echo "### BUILD: clean rebuild cws-5d.x in all three trees ###"
for d in "$BASE" "$A1" "$A2"; do
  ( cd "$d" && rm -f *-5d.o cws-5d.x && make -f GNUmakefile cws-5d.x ) >"$OUT/build_$(basename "$d").log" 2>&1 \
    && echo "  OK  $d" || { echo "  FAIL $d"; tail -8 "$OUT/build_$(basename "$d").log"; exit 1; }
done
declare -A BIN=( [BASE]="$BASE/cws-5d.x" [A1]="$A1/cws-5d.x" [A2]="$A2/cws-5d.x" )

# parse one PROF stderr file -> "candidates points_cycles ip_cycles total_cycles np_sum ip_pass"
prof_fields() { awk '/^PROF /{for(i=1;i<=NF;i++){split($i,a,"=");v[a[1]]=a[2]}}
  END{printf "%s %s %s %s %s %s", v["candidates"]+0,v["points_cycles"]+0,v["ip_cycles"]+0,v["total_cycles"]+0,v["np_sum"]+0,v["ip_pass"]+0}' "$1"; }

# spread anchors over [1, POOL5]
anchors() { local n=$1 i; for ((i=1;i<=n;i++)); do echo $(( 1 + (i-1)*(POOL5-1)/(n-1) )); done; }

# ============================================================ TIMING ==========
echo; echo "### Phase T: Make_CWS_Points / IP timing (spread anchors, limit=$LIMIT) ###"
for S in $STRUCTS; do
  echo "  -- structure $S --"
  for tag in BASE A1 A2; do
    : > "$OUT/T_s${S}_${tag}.agg"
    for a in $(anchors "$NANCHORS"); do
      env PALP_PROFILE_TIMING=1 PALP_PROFILE_LIMIT="$LIMIT" \
        "${BIN[$tag]}" -c5 -s"$S" -j "$POOL5" -k "$a" >/dev/null 2>"$OUT/T_s${S}_${tag}_a${a}.prof" || true
      prof_fields "$OUT/T_s${S}_${tag}_a${a}.prof" >> "$OUT/T_s${S}_${tag}.agg"; echo >> "$OUT/T_s${S}_${tag}.agg"
    done
  done
  # aggregate + report per structure
  for tag in BASE A1 A2; do
    read -r C PC IC TC NPS IPP < <(awk '{c+=$1;pc+=$2;ic+=$3;tc+=$4;np+=$5;ip+=$6}END{print c,pc,ic,tc,np,ip}' "$OUT/T_s${S}_${tag}.agg")
    awk -v t="$tag" -v c="$C" -v pc="$PC" -v ic="$IC" -v np="$NPS" -v ip="$IPP" \
      'BEGIN{ if(c>0) printf "    %-4s cand=%-8d points_cyc/cand=%-10.1f ip_cyc/cand=%-9.1f avg_np=%-6.2f ip_pass=%d\n", t,c,pc/c,ic/c,np/c,ip }'
    eval "PC_$tag=$PC; C_$tag=$C; IC_$tag=$IC"
  done
  awk -v b="$PC_BASE" -v c1="$C_BASE" -v a1="$PC_A1" -v ca1="$C_A1" -v a2="$PC_A2" -v ca2="$C_A2" \
    'BEGIN{ if(a1>0&&c1>0&&ca1>0) printf "    => Make_CWS_Points speedup  BASE/A1 = %.3fx   BASE/A2 = %.3fx\n",
        (b/c1)/(a1/ca1), (b/c1)/(a2/ca2) }'
done

# ====================================================== CORRECTNESS ===========
echo; echo "### Phase C: production-output correctness diffs ###"
# auto-probe a production shard -j that yields a few x10k candidates on -k1
probe_J() { local S=$1 J n;
  for J in 200000 400000 800000 2000000 8000000; do
    n=$(env PALP_PROFILE_GENONLY=1 PALP_PROFILE_LIMIT=400000 "${BIN[BASE]}" -c5 -s"$S" -j "$J" -k1 2>&1 \
        | awk '/^PROF /{for(i=1;i<=NF;i++){split($i,a,"=");v[a[1]]=a[2]}} END{print v["candidates"]+0}')
    if [ "${n:-0}" -gt 0 ] && [ "${n:-0}" -lt 300000 ]; then echo "$J $n"; return; fi
  done
  echo "8000000 0"
}
for S in $STRUCTS; do
  if [ -n "$CORR_J" ]; then J="$CORR_J"; n="?"; else read -r J n < <(probe_J "$S"); fi
  echo "  -- structure $S : production shard -j$J -k1  (~$n candidates) --"
  for tag in BASE A1 A2; do
    "${BIN[$tag]}" -c5 -s"$S" -j "$J" -k1 >"$OUT/C_s${S}_${tag}.out" 2>/dev/null || true
    echo "       $tag: $(wc -l < "$OUT/C_s${S}_${tag}.out") IP lines"
  done
  if diff -q "$OUT/C_s${S}_BASE.out" "$OUT/C_s${S}_A1.out" >/dev/null; then
    echo "       BASE==A1  : BYTE-IDENTICAL ✓  (point-walk rewrite correct)"
  else
    echo "       BASE!=A1  : MISMATCH ✗"; diff "$OUT/C_s${S}_BASE.out" "$OUT/C_s${S}_A1.out" | head -8
  fi
  if diff -q "$OUT/C_s${S}_A1.out" "$OUT/C_s${S}_A2.out" >/dev/null; then
    echo "       A1==A2    : BYTE-IDENTICAL ✓  (Xmax reject + dim-exit lose NO IP)"
  else
    echo "       A1!=A2    : MISMATCH ✗  -- candidates dropped by A2 that ARE IP (FALSE NEGATIVE):"
    diff "$OUT/C_s${S}_A1.out" "$OUT/C_s${S}_A2.out" | grep '^<' | head -12
  fi
done

# ================================================= PRODUCTION THROUGHPUT ======
echo; echo "### Phase P: end-to-end production wall time (BASE vs A2), full shard ###"
for S in $STRUCTS; do
  if [ -n "$CORR_J" ]; then J="$CORR_J"; else read -r J n < <(probe_J "$S"); fi
  for tag in BASE A2; do
    t0=$(date +%s.%N)
    "${BIN[$tag]}" -c5 -s"$S" -j "$J" -k1 >/dev/null 2>/dev/null || true
    t1=$(date +%s.%N)
    eval "W_${tag}=$(awk "BEGIN{print $t1-$t0}")"
  done
  awk -v S="$S" -v b="$W_BASE" -v a="$W_A2" \
    'BEGIN{ printf "  s%s: BASE %.2fs   A2 %.2fs   end-to-end speedup = %.3fx\n", S,b,a,(a>0)?b/a:0 }'
done

echo; echo "DONE  (artifacts in $OUT)"
