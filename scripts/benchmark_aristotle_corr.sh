#!/usr/bin/env bash
#SBATCH --job-name=aristotle-corr
#SBATCH --partition=std
#SBATCH --nodes=1
#SBATCH --exclusive
#SBATCH --cpus-per-task=128
#SBATCH --mem=0
#SBATCH --time=00:25:00
#SBATCH --output=logs/slurm/aristotle-corr-%j.out
#SBATCH --error=logs/slurm/aristotle-corr-%j.err
#
# Correctness + end-to-end throughput for the Aristotle passes (binaries already
# rebuilt by benchmark_aristotle.sh). Companion to that script's Phase T (timing).
#
#   C  CORRECTNESS: per anchor, production output bounded by `head -n N` (the
#      pipe close SIGPIPEs cws-5d.x -> deterministic, work-bounded). Concatenate
#      over a spread of anchors, diff pairwise:
#        BASE==A1  -> point-walk rewrite is byte-identical
#        A1 ==A2   -> Xmax<=1 reject + dim-exit drop NO IP polytope (A2 only ever
#                     removes candidates, so equality == no false negative)
#   P  THROUGHPUT: pick moderate single-anchor FULL sweeps (probed to be bounded),
#      run each binary to completion, time wall:
#        BASE->A1 = point-walk speedup ; A1->A2 = Xmax-reject benefit
set -uo pipefail
REPO_ROOT="${SLURM_SUBMIT_DIR:-$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)}"
cd "$REPO_ROOT"
export PALP_W5_POOL="${PALP_W5_POOL:-$REPO_ROOT/results/cache/w5.ip}"
declare -A BIN=( [BASE]="$REPO_ROOT/PALP/cws-5d.x"
                 [A1]="$HOME/aristotle_1/PALP_aristotle/cws-5d.x"
                 [A2]="$HOME/aristotle_2/output-final_aristotle/cws-5d.x" )
POOL5=1833327
STRUCTS="${STRUCTS:-3 6}"
HEADN="${HEADN:-40000}"          # IP lines kept per anchor (work bound)
NANCH="${NANCH:-12}"             # spread anchors for correctness
OUT="$REPO_ROOT/results/aristotle-validation"
mkdir -p "$OUT" "$REPO_ROOT/logs/slurm"
for t in BASE A1 A2; do [[ -x "${BIN[$t]}" ]] || { echo "missing ${BIN[$t]} (run benchmark_aristotle.sh first)"; exit 1; }; done
echo "host=$(hostname)  HEADN=$HEADN NANCH=$NANCH STRUCTS='$STRUCTS'  w5=$PALP_W5_POOL"

anchors() { local n=$1 i; for ((i=1;i<=n;i++)); do echo $(( 1 + (i-1)*(POOL5-1)/(n-1) )); done; }
gen_count() { env PALP_PROFILE_GENONLY=1 "${BIN[BASE]}" -c5 -s"$1" -j "$POOL5" -k "$2" 2>&1 \
  | awk '/^PROF /{for(i=1;i<=NF;i++){split($i,a,"=");v[a[1]]=a[2]}} END{print v["candidates"]+0}'; }

# =================================================== CORRECTNESS ==============
echo; echo "### Phase C: correctness (spread anchors, head -n $HEADN per anchor) ###"
for S in $STRUCTS; do
  echo "  -- structure $S --"
  for t in BASE A1 A2; do : > "$OUT/CC_s${S}_${t}.out"; done
  for a in $(anchors "$NANCH"); do
    # per-anchor: head bounds work for IP-rich anchors; timeout caps the rare
    # many-candidate/low-IP anchor. Truncate-to-min prefix below makes any
    # timeout cut harmless (the candidate stream is identical across binaries).
    for t in BASE A1 A2; do
      timeout --signal=TERM 30 "${BIN[$t]}" -c5 -s"$S" -j "$POOL5" -k "$a" 2>/dev/null \
        | head -n "$HEADN" >> "$OUT/CC_s${S}_${t}_a${a}.part" || true
    done
    m=$(wc -l < "$OUT/CC_s${S}_BASE_a${a}.part")
    for t in A1 A2; do mm=$(wc -l < "$OUT/CC_s${S}_${t}_a${a}.part"); [ "$mm" -lt "$m" ] && m=$mm; done
    for t in BASE A1 A2; do head -n "$m" "$OUT/CC_s${S}_${t}_a${a}.part" >> "$OUT/CC_s${S}_${t}.out"; rm -f "$OUT/CC_s${S}_${t}_a${a}.part"; done
  done
  for t in BASE A1 A2; do echo "       $t: $(wc -l < "$OUT/CC_s${S}_${t}.out") IP lines"; done
  if diff -q "$OUT/CC_s${S}_BASE.out" "$OUT/CC_s${S}_A1.out" >/dev/null; then
    echo "       BASE==A1 : BYTE-IDENTICAL ✓  (point-walk rewrite correct)"
  else echo "       BASE!=A1 : MISMATCH ✗"; diff "$OUT/CC_s${S}_BASE.out" "$OUT/CC_s${S}_A1.out" | head -8; fi
  if diff -q "$OUT/CC_s${S}_A1.out" "$OUT/CC_s${S}_A2.out" >/dev/null; then
    echo "       A1==A2   : BYTE-IDENTICAL ✓  (Xmax reject loses NO IP -> no false negative)"
  else echo "       A1!=A2   : MISMATCH ✗  FALSE NEGATIVES (IP dropped by A2):"; diff "$OUT/CC_s${S}_A1.out" "$OUT/CC_s${S}_A2.out" | grep '^<' | head -12; fi
done

# =================================================== THROUGHPUT ===============
echo; echo "### Phase P: end-to-end wall time on bounded full-anchor sweeps ###"
for S in $STRUCTS; do
  # find 2 anchors whose FULL sweep is in [40k, 400k] candidates (bounded, non-trivial)
  picks=(); for a in $(anchors 24); do
    [ "${#picks[@]}" -ge 2 ] && break
    n=$(gen_count "$S" "$a"); if [ "${n:-0}" -ge 40000 ] && [ "${n:-0}" -le 400000 ]; then picks+=("$a:$n"); fi
  done
  [ "${#picks[@]}" -eq 0 ] && { echo "  s$S: no bounded anchor found, skipping"; continue; }
  echo "  -- structure $S : timing anchors ${picks[*]} --"
  for pk in "${picks[@]}"; do
    a="${pk%%:*}"; n="${pk##*:}"
    for t in BASE A1 A2; do
      t0=$(date +%s.%N); ipl=$(timeout --signal=TERM 180 "${BIN[$t]}" -c5 -s"$S" -j "$POOL5" -k "$a" 2>/dev/null | wc -l); t1=$(date +%s.%N)
      eval "W_$t=$(awk "BEGIN{print $t1-$t0}")"; eval "L_$t=$ipl"
    done
    awk -v S="$S" -v a="$a" -v n="$n" -v wb="$W_BASE" -v w1="$W_A1" -v w2="$W_A2" -v lb="$L_BASE" -v l2="$L_A2" \
      'BEGIN{ printf "    s%s a%-8s cand=%-7s  BASE %.2fs  A1 %.2fs  A2 %.2fs  | walk %.2fx  reject %.2fx  total %.2fx  (IP: BASE=%d A2=%d)\n",
        S,a,n,wb,w1,w2,(w1>0?wb/w1:0),(w2>0?w1/w2:0),(w2>0?wb/w2:0),lb,l2 }'
  done
done
echo; echo "DONE"
