#!/usr/bin/env bash
#SBATCH --job-name=cpu-prof
#SBATCH --partition=std
#SBATCH --nodes=1
#SBATCH --exclusive
#SBATCH --cpus-per-task=128
#SBATCH --mem=0
#SBATCH --time=00:45:00
#SBATCH --output=logs/slurm/cpu-prof-%j.out
#SBATCH --error=logs/slurm/cpu-prof-%j.err
#
# Profile the CPU CWS-generation + point-enumeration + IP-check pipeline
# (NO normal-form stage) for the 5-5 (type 3) structure.
#
# perf_event_paranoid is locked on this cluster, so timing comes from the rdtsc
# hook compiled into PALP/cws.c (PALP_PROFILE_TIMING / PALP_PROFILE_GENONLY) —
# no privileges required.
#
# REPRESENTATIVENESS: slot-0 index 1 (lowest-degree W5) is a pathological heavy
# anchor (avg np ~80, IP rate ~50% vs the true ~8.7 / 0.08%), and the enumerator
# visits slot 0 in index order. So:
#   * single-thread stats use SPREAD anchors over the full 1.83M slot-0 pool,
#     each processing its FULL slot-1 sweep (-j POOL -k anchor) -> unbiased
#     throughput (sum candidates / sum wall) and unbiased per-candidate stats.
#   * multithread + occupancy use modulo shards (-j NCORES -k k) over a fixed
#     window; contention efficiency is extracted from the SAME shard (k=1) run
#     alone vs under full contention, so the anchor bias cancels.
#
# Phases:
#   A  single-thread TIMING,  spread anchors, full blocks   (authoritative stats)
#   B  single-thread GENONLY, spread anchors, full blocks   (generation cost)
#   C1 single worker  -j NCORES -k1, TIMING, window         (contention baseline)
#   C2 NCORES workers -j NCORES -k1..N, TIMING, window      (multithread+occupancy)
#   D  NCORES workers -j NCORES -k1..N, GENONLY, window     (gen multithread)
set -euo pipefail

if [[ -n "${SLURM_SUBMIT_DIR:-}" ]]; then
  REPO_ROOT="$SLURM_SUBMIT_DIR"; SCRIPT_DIR="$REPO_ROOT/scripts"
else
  SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
  REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
fi

CWS_BIN="$REPO_ROOT/PALP/cws-5d.x"
W5_POOL="${PALP_W5_POOL:-$REPO_ROOT/results/cache/w5.ip}"
STRUCTURE_ID="${STRUCTURE_ID:-3}"
POOL5=1833327                       # size-5/shared-3 slot pool (both type-3 slots)
NCORES="${NCORES:-128}"
W_SPREAD="${W_SPREAD:-16}"          # spread anchors for single-thread stats
T_WIN="${T_WIN:-20}"               # multithread window (s)
ANCHOR_TMO="${ANCHOR_TMO:-150}"    # per-anchor safety cap for full blocks (s)
OUTDIR="${OUTDIR:-$REPO_ROOT/results/pipeline-profile/cpu-structure-$STRUCTURE_ID}"

[[ -x "$CWS_BIN" ]] || { echo "missing $CWS_BIN (build: make -C PALP cws-5d.x)"; exit 1; }
mkdir -p "$OUTDIR" "$REPO_ROOT/logs/slurm"
rm -f "$OUTDIR"/*.prof "$OUTDIR"/summary.txt

echo "host=$(hostname) nproc=$(nproc) struct=$STRUCTURE_ID NCORES=$NCORES W_SPREAD=$W_SPREAD T_WIN=$T_WIN"
echo "w5_pool=$W5_POOL"
lscpu 2>/dev/null | grep -E "Model name|^CPU\(s\)|Thread|Socket|MHz" || true

# spread anchors over [1, POOL5]
ANCHORS=()
for ((i=1; i<=W_SPREAD; i++)); do
  ANCHORS+=( $(( 1 + (i-1)*(POOL5-1)/(W_SPREAD-1) )) )
done
echo "spread anchors: ${ANCHORS[*]}"

worker() {  # mode_env  J  k  timeout  outfile
  env PALP_W5_POOL="$W5_POOL" $1 \
      timeout --signal=TERM --kill-after=10 "$4" \
      "$CWS_BIN" -c5 -s"$STRUCTURE_ID" -j "$2" -k "$3" \
      >/dev/null 2>"$5" || true
}

# ---- Phase A: single-thread TIMING, spread anchors, full blocks ----------
echo; echo "### Phase A: single-thread TIMING, $W_SPREAD spread anchors (full blocks) ###"
for a in "${ANCHORS[@]}"; do
  worker "PALP_PROFILE_TIMING=1" "$POOL5" "$a" "$ANCHOR_TMO" "$OUTDIR/A_timing_a${a}.prof"
done

# ---- Phase B: single-thread GENONLY, spread anchors, full blocks ---------
echo "### Phase B: single-thread GENONLY, spread anchors (full blocks) ###"
for a in "${ANCHORS[@]}"; do
  worker "PALP_PROFILE_GENONLY=1" "$POOL5" "$a" "$ANCHOR_TMO" "$OUTDIR/B_genonly_a${a}.prof"
done

# ---- Phase C1: single worker on modulo shard k=1 (contention baseline) ---
echo "### Phase C1: single worker -j$NCORES -k1 TIMING (${T_WIN}s) ###"
worker "PALP_PROFILE_TIMING=1" "$NCORES" 1 "$T_WIN" "$OUTDIR/C1_timing_k1.prof"

# ---- Phase C2: NCORES workers modulo TIMING (multithread + occupancy) ----
echo "### Phase C2: $NCORES workers -j$NCORES TIMING (${T_WIN}s) ###"
read -r idle0 total0 < <(awk '/^cpu /{idle=$5+$6;t=0;for(i=2;i<=NF;i++)t+=$i;print idle,t}' /proc/stat)
b_start=$(date +%s.%N)
for ((k=1; k<=NCORES; k++)); do
  worker "PALP_PROFILE_TIMING=1" "$NCORES" "$k" "$T_WIN" "$OUTDIR/C2_timing_k${k}.prof" &
done
wait
b_end=$(date +%s.%N)
read -r idle1 total1 < <(awk '/^cpu /{idle=$5+$6;t=0;for(i=2;i<=NF;i++)t+=$i;print idle,t}' /proc/stat)
C2_WALL=$(awk "BEGIN{print $b_end-$b_start}")
BUSY=$(awk "BEGIN{di=$idle1-$idle0;dt=$total1-$total0; if(dt>0) printf \"%.1f\", 100*(1-di/dt); else print \"NA\"}")
echo "C2 batch wall=${C2_WALL}s  node CPU busy=${BUSY}%  loadavg=$(cat /proc/loadavg)"

# ---- Phase D: NCORES workers modulo GENONLY -----------------------------
echo "### Phase D: $NCORES workers -j$NCORES GENONLY (${T_WIN}s) ###"
d_start=$(date +%s.%N)
for ((k=1; k<=NCORES; k++)); do
  worker "PALP_PROFILE_GENONLY=1" "$NCORES" "$k" "$T_WIN" "$OUTDIR/D_genonly_k${k}.prof" &
done
wait
d_end=$(date +%s.%N)
D_WALL=$(awk "BEGIN{print $d_end-$d_start}")

# ---- Derived scaling / contention efficiency ----------------------------
rate() { awk '/^PROF /{for(i=1;i<=NF;i++){split($i,a,"=");v[a[1]]=a[2]}; if(v["wall_secs"]>0) printf "%.0f", v["candidates"]/v["wall_secs"]}' "$1"; }
sum_rate() { awk '/^PROF /{for(i=1;i<=NF;i++){split($i,a,"=");v[a[1]]=a[2]}; n+=v["candidates"]} END{printf "%.0f", n/W}' W="$1" "${@:2}"; }
R1=$(rate "$OUTDIR/C1_timing_k1.prof")                   # shard1 alone
RC2k1=$(rate "$OUTDIR/C2_timing_k1.prof")                # shard1 under contention
C2_TOTAL=$(awk '/^PROF /{for(i=1;i<=NF;i++){split($i,a,"=");v[a[1]]=a[2]}; n+=v["candidates"]} END{print n}' "$OUTDIR"/C2_timing_k*.prof)
D_TOTAL=$(awk '/^PROF /{for(i=1;i<=NF;i++){split($i,a,"=");v[a[1]]=a[2]}; n+=v["candidates"]} END{print n}' "$OUTDIR"/D_genonly_k*.prof)

# ---- Analysis report ----------------------------------------------------
PY="$SCRIPT_DIR/analyze_pipeline_profile.py"
GEN_NS=$(awk '/^PROF /{for(i=1;i<=NF;i++){split($i,a,"=");v[a[1]]=a[2]}; c+=v["candidates"]; w+=v["wall_secs"]} END{if(c>0) printf "%.3f", w/c*1e9}' "$OUTDIR"/B_genonly_a*.prof)
{
  echo "########## CPU PIPELINE PROFILE — structure $STRUCTURE_ID ($(hostname)) ##########"
  echo
  python3 "$PY" --sequential --label "Phase A: single-thread TIMING (spread anchors, AUTHORITATIVE)" \
      --gen-ns-per-cand "${GEN_NS:-0}" "$OUTDIR"/A_timing_a*.prof
  python3 "$PY" --sequential --label "Phase B: single-thread GENONLY (spread anchors)" \
      "$OUTDIR"/B_genonly_a*.prof
  echo
  echo "==== Multithread scaling, occupancy, and derived throughput ===="
  echo "cores (NCORES)              : $NCORES   (node nproc=$(nproc))"
  echo "shard-1 alone (C1)         : ${R1} cand/s"
  echo "shard-1 under contention   : ${RC2k1} cand/s   (C2 worker k=1)"
  awk "BEGIN{ if($R1>0) printf \"contention efficiency eta : %.3f   (per-core under %d-way load)\n\", $RC2k1/$R1, $NCORES }"
  echo "measured 128-core aggregate (modulo, window) :"
  awk "BEGIN{printf \"  TIMING  : %.0f cand/s  (%d cand / %.1fs)\n\", $C2_TOTAL/$T_WIN, $C2_TOTAL, $T_WIN}"
  awk "BEGIN{printf \"  GENONLY : %.0f cand/s  (%d cand / %.1fs)\n\", $D_TOTAL/$T_WIN, $D_TOTAL, $T_WIN}"
  echo "node CPU busy during multithread TIMING : ${BUSY}%"
  echo
  echo "DERIVED representative multithread throughput (R_single_spread * NCORES * eta):"
  RS=$(python3 "$PY" --sequential --label x "$OUTDIR"/A_timing_a*.prof 2>/dev/null | awk '/throughput cand\/s/{print $4; exit}' | tr -d ',')
  awk "BEGIN{ if($R1>0 && \"$RS\"!=\"\") printf \"  processing : %.0f cand/s  (= %s * %d * %.3f)\n\", ($RS+0)*$NCORES*($RC2k1/$R1), \"$RS\", $NCORES, $RC2k1/$R1 }"
} | tee "$OUTDIR/summary.txt"

echo; echo "wrote $OUTDIR/summary.txt"
