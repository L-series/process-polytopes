#!/usr/bin/env bash
#SBATCH --job-name=run43
#SBATCH --partition=gpu
#SBATCH --gres=gpu:RTX6000BW:1
#SBATCH --array=0-5
#SBATCH --cpus-per-task=8
#SBATCH --mem=80G
#SBATCH --time=08:00:00
#SBATCH --output=/home/ahat01/cws43run/logs/task-%a.out
#SBATCH --error=/home/ahat01/cws43run/logs/task-%a.err
#
# Classify the 43 least-populated CWS types (all except s3,s12,s13 = 118.7B cands,
# 0.978% of total) on 6 RTX6000BW GPUs with the streaming FP pipeline.
# Each array task = 1 GPU = shard-index G of shard-count 6: it processes 1/6 of
# EVERY structure (balanced), streaming the full range (stream_descriptor_ip_bucketed).
# Output -> shared /home (NFS; /local is node-local so a 2-node run can't collect there;
# write rate ~35 MB/s is trivial for NFS). Per-(structure,gpu) files merge afterward.
set -uo pipefail
cd "${SLURM_SUBMIT_DIR:-$(pwd)}"
BIN=src/classify/build-cuda/cuda_dim5_cws_scan
W5=results/cache/w5.ip; export PALP_W5_POOL="$W5"
OUT=/home/ahat01/cws43run
G="${SLURM_ARRAY_TASK_ID:-0}"   # 0..5 == shard-index
NG=6                             # shard-count
NPCAP=256
EMIT=1500000
mkdir -p "$OUT/accepted" "$OUT/overflow" "$OUT/logs"
[ -x "$BIN" ] || { echo "binary $BIN missing (build first)"; exit 1; }
echo "### task=$G host=$(hostname) gpu=${CUDA_VISIBLE_DEVICES:-?} $(date) np_cap=$NPCAP emit=$EMIT ###"

# biggest-first so the heavy structures finish early and stragglers are tiny
IDS="27 26 25 29 20 15 6 43 35 40 14 11 21 36 38 4 24 28 10 2 7 45 8 32 31 30 42 34 22 18 17 16 33 46 5 44 9 23 41 39 19 37 47"
start=$(date +%s)
for id in $IDS; do
  t0=$(date +%s)
  $BIN --structure-id "$id" --w5 "$W5" --stream-ip --ip-bucketed --fp-walk --vol-sort \
       --np-cap "$NPCAP" --emit-capacity "$EMIT" --shard-count "$NG" --shard-index "$G" \
       --accepted-output "$OUT/accepted/s${id}.g${G}.acc" \
       --overflow-output "$OUT/overflow/s${id}.g${G}.ovf" \
       2>"$OUT/logs/s${id}.g${G}.log" >/dev/null || { echo "  s$id FAILED"; continue; }
  acc=$(wc -l <"$OUT/accepted/s${id}.g${G}.acc" 2>/dev/null || echo 0)
  ovf=$(wc -l <"$OUT/overflow/s${id}.g${G}.ovf" 2>/dev/null || echo 0)
  seen=$(grep -oE 'stored_candidates_seen: [0-9]+' "$OUT/logs/s${id}.g${G}.log" | grep -oE '[0-9]+' | head -1)
  ch=$(grep -oE 'chunks: [0-9]+' "$OUT/logs/s${id}.g${G}.log" | grep -oE '[0-9]+' | head -1)
  printf "  s%-3s accepted=%-9s overflow=%-9s seen=%-13s chunks=%-5s (%ds, %ds total)\n" \
    "$id" "$acc" "$ovf" "${seen:-?}" "${ch:-?}" "$(($(date +%s)-t0))" "$(($(date +%s)-start))"
done
echo "### task=$G DONE $(date) total=$(($(date +%s)-start))s ###"
