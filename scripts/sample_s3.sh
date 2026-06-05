#!/usr/bin/env bash
#SBATCH --job-name=s3-sample
#SBATCH --partition=gpu
#SBATCH --gres=gpu:RTX6000BW:1
#SBATCH --cpus-per-task=8
#SBATCH --mem=80G
#SBATCH --time=00:40:00
#SBATCH --output=/home/ahat01/cws3sample/logs/sample-%j.out
#SBATCH --error=/home/ahat01/cws3sample/logs/sample-%j.out
#
# Measure s3's IP-accept rate on a representative sample: 5 shards spread across
# the selection range (density varies along it, so spread + average). shard-count
# 50000 -> each shard ~ 10.05T/50000 ~ 201M candidates; 5 shards ~ 1.0B (0.01% of s3).
set -uo pipefail
cd "${SLURM_SUBMIT_DIR:-$(pwd)}"
BIN=src/classify/build-cuda/cuda_dim5_cws_scan
W5=results/cache/w5.ip; export PALP_W5_POOL="$W5"
OUT=/home/ahat01/cws3sample
NSH=50000; NPCAP=256; EMIT=1500000
echo "### host=$(hostname) gpu=${CUDA_VISIBLE_DEVICES:-?} $(date) ###"
TA=0; TO=0; TS=0
for idx in 5000 15000 25000 35000 45000; do
  t0=$(date +%s)
  $BIN --structure-id 3 --w5 "$W5" --stream-ip --ip-bucketed --fp-walk --vol-sort \
     --np-cap $NPCAP --emit-capacity $EMIT --shard-count $NSH --shard-index $idx \
     --accepted-output "$OUT/accepted/s3.sh${idx}.acc" --overflow-output "$OUT/overflow/s3.sh${idx}.ovf" \
     2>"$OUT/logs/s3.sh${idx}.log" >/dev/null || { echo "  shard $idx FAILED"; continue; }
  a=$(wc -l <"$OUT/accepted/s3.sh${idx}.acc" 2>/dev/null||echo 0)
  o=$(wc -l <"$OUT/overflow/s3.sh${idx}.ovf" 2>/dev/null||echo 0)
  s=$(grep -oE 'stored_candidates_seen: [0-9]+' "$OUT/logs/s3.sh${idx}.log"|grep -oE '[0-9]+'|head -1)
  printf "  shard %-6s seen=%-12s accepted=%-9s overflow=%-9s acc%%=%s (%ds)\n" "$idx" "${s:-0}" "$a" "$o" "$(python3 -c "print(f'{100*$a/max(${s:-1},1):.4f}')")" "$(($(date +%s)-t0))"
  TA=$((TA+a)); TO=$((TO+o)); TS=$((TS+${s:-0}))
done
echo "### AGGREGATE: seen=$TS accepted=$TA overflow=$TO ###"
python3 -c "s=$TS;a=$TA;o=$TO; print(f'  GPU IP-accept rate = {100*a/s:.4f}%  overflow rate = {100*o/s:.4f}%'); print(f'  projected s3 (10.05T): GPU-accept ~ {10.05e12*a/s/1e9:.1f}B, overflow ~ {10.05e12*o/s/1e9:.1f}B')"
echo "### DONE $(date) ###"
