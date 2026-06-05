#!/usr/bin/env bash
#SBATCH --job-name=s12
#SBATCH --partition=gpu
#SBATCH --gres=gpu:RTX6000BW:1
#SBATCH --array=0-4
#SBATCH --cpus-per-task=8
#SBATCH --mem=80G
#SBATCH --time=18:00:00
#SBATCH --output=/home/ahat01/cws12run/logs/task-%a.out
#SBATCH --error=/home/ahat01/cws12run/logs/task-%a.err
#
# Classify s12 (987.91B candidates) on 5 RTX6000BW GPUs, streaming FP pipeline.
# Interleaved sharding to dodge the contiguous-shard load imbalance: 40 shards,
# task G (GPU G) processes shards G, G+5, ... G+35 -> each GPU samples the whole
# selection range, so all 5 finish together. ~9-12 h expected.
set -uo pipefail
cd "${SLURM_SUBMIT_DIR:-$(pwd)}"
BIN=src/classify/build-cuda/cuda_dim5_cws_scan
W5=results/cache/w5.ip; export PALP_W5_POOL="$W5"
OUT=/home/ahat01/cws12run
G="${SLURM_ARRAY_TASK_ID:-0}"
NTASK=5; NSHARD=40; ID=12; NPCAP=256; EMIT=1500000
mkdir -p "$OUT/accepted" "$OUT/overflow" "$OUT/logs"
[ -x "$BIN" ] || { echo "binary missing"; exit 1; }
echo "### task=$G host=$(hostname) gpu=${CUDA_VISIBLE_DEVICES:-?} $(date) ###"
start=$(date +%s)
for (( i=G; i<NSHARD; i+=NTASK )); do
  t0=$(date +%s)
  $BIN --structure-id $ID --w5 "$W5" --stream-ip --ip-bucketed --fp-walk --vol-sort \
     --np-cap $NPCAP --emit-capacity $EMIT --shard-count $NSHARD --shard-index $i \
     --accepted-output "$OUT/accepted/s${ID}.sh${i}.acc" \
     --overflow-output "$OUT/overflow/s${ID}.sh${i}.ovf" \
     2>"$OUT/logs/s${ID}.sh${i}.log" >/dev/null || { echo "  shard $i FAILED"; continue; }
  acc=$(wc -l <"$OUT/accepted/s${ID}.sh${i}.acc" 2>/dev/null||echo 0)
  ovf=$(wc -l <"$OUT/overflow/s${ID}.sh${i}.ovf" 2>/dev/null||echo 0)
  seen=$(grep -oE 'stored_candidates_seen: [0-9]+' "$OUT/logs/s${ID}.sh${i}.log"|grep -oE '[0-9]+'|head -1)
  printf "  shard %-3s done acc=%-9s ovf=%-9s seen=%-13s (%ds, %ds total)\n" "$i" "$acc" "$ovf" "${seen:-?}" "$(($(date +%s)-t0))" "$(($(date +%s)-start))"
done
echo "### task=$G DONE $(date) total=$(($(date +%s)-start))s ###"
