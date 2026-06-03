#!/usr/bin/env bash
#SBATCH --job-name=ip-bucket-corr
#SBATCH --partition=gpu
#SBATCH --gres=gpu:RTX6000BW:1
#SBATCH --cpus-per-task=8
#SBATCH --mem=80G
#SBATCH --time=00:20:00
#SBATCH --output=logs/slurm/ip-bucket-corr-%j.out
#SBATCH --error=logs/slurm/ip-bucket-corr-%j.err
#
# Exact-match correctness for --ip-bucketed, on a candidate set with NO
# emit-capacity truncation (so the set is deterministic between processes).
# The non-stream --ip-check path stores candidates in nondeterministic atomic
# arrival order and truncates at --emit-capacity, so a fair comparison needs a
# shard whose full generated count is below emit_capacity. We search shard
# granularities until that holds, then require:
#   * legacy serial 4096 deterministic run-to-run
#   * bucketed 4096    deterministic run-to-run
#   * accepted CWS sets identical (bucketed 4096 == legacy serial 4096)
set -euo pipefail
cd "${SLURM_SUBMIT_DIR:-$(pwd)}"
BIN=src/classify/build-cuda/cuda_dim5_cws_scan
W5=results/cache/w5.ip
STRUCT=3
T=$(mktemp -d)
EMIT=200000          # bucketed@4096 buffer = generated*4096*40B (<=~32GB at this cap)
echo "host=$(hostname)"; nvidia-smi --query-gpu=name --format=csv,noheader -i "${CUDA_VISIBLE_DEVICES%%,*}"

gen_count() { # shard_count shard_index
  $BIN --structure-id $STRUCT --w5 $W5 --shard-count "$1" --shard-index "$2" \
    --emit-capacity "$EMIT" 2>&1 | grep -oE 'generated_candidates_seen: [0-9]+' | grep -oE '[0-9]+'
}

# search for a non-truncating shard near the middle of the range
SC=0; SI=0; GEN=0
for sc in 4000000 40000000 400000000 4000000000; do
  si=$(( sc / 2 ))
  g=$(gen_count "$sc" "$si")
  echo "  probe shard-count=$sc index=$si -> generated=$g"
  if [ "$g" -gt 0 ] && [ "$g" -lt "$EMIT" ]; then SC=$sc; SI=$si; GEN=$g; break; fi
done
if [ "$SC" = 0 ]; then echo "could not find a non-truncating shard; aborting"; rm -rf "$T"; exit 1; fi
echo "### using non-truncating shard-count=$SC index=$SI generated=$GEN (emit=$EMIT) ###"

ip_line() { grep -oE 'candidates: [0-9]+ processed: [0-9]+ ip: [0-9]+ .*ip_reject: [0-9]+'; }

echo
echo "### legacy serial (ip-max-points 4096) x2 ###"
for r in 1 2; do
  $BIN --structure-id $STRUCT --w5 $W5 --shard-count $SC --shard-index $SI \
    --emit-capacity $EMIT --ip-check --ip-max-points 4096 \
    --accepted-output "$T/L.$r" 2>&1 | ip_line | sed "s/^/  run$r: /"
  sort "$T/L.$r" > "$T/L.$r.s"
done
diff -q "$T/L.1.s" "$T/L.2.s" >/dev/null && echo "  legacy deterministic: YES" || echo "  legacy deterministic: NO"

echo
echo "### bucketed (np-cap 4096) x2 ###"
for r in 1 2; do
  $BIN --structure-id $STRUCT --w5 $W5 --shard-count $SC --shard-index $SI \
    --emit-capacity $EMIT --ip-check --ip-bucketed --np-cap 4096 \
    --accepted-output "$T/B.$r" --overflow-output "$T/ovf.$r" 2>&1 | ip_line | sed "s/^/  run$r: /"
  sort "$T/B.$r" > "$T/B.$r.s"
done
diff -q "$T/B.1.s" "$T/B.2.s" >/dev/null && echo "  bucketed deterministic: YES" || echo "  bucketed deterministic: NO"
echo "  overflow rows @4096: $(wc -l < "$T/ovf.1") (expect 0)"

echo
echo "### accepted-set match: legacy serial 4096 vs bucketed 4096 ###"
echo "  legacy=$(wc -l < "$T/L.1.s") bucketed=$(wc -l < "$T/B.1.s")"
if diff -q "$T/L.1.s" "$T/B.1.s" >/dev/null; then
  echo "  ACCEPTED SETS IDENTICAL ✓"
else
  echo "  ACCEPTED SETS DIFFER ✗:"; diff "$T/L.1.s" "$T/B.1.s" | head -10 | sed 's/^/    /'
fi

echo
echo "### completeness @ np-cap 64: overflow rows == point_overflow, accepts subset of 4096 ###"
PO=$($BIN --structure-id $STRUCT --w5 $W5 --shard-count $SC --shard-index $SI \
       --emit-capacity $EMIT --ip-check --ip-bucketed --np-cap 64 \
       --accepted-output "$T/B64" --overflow-output "$T/ovf64" 2>&1 \
       | grep -oE 'point_overflow: [0-9]+' | head -1 | grep -oE '[0-9]+')
echo "  point_overflow=$PO overflow_rows=$(wc -l < "$T/ovf64") $([ "$PO" = "$(wc -l < "$T/ovf64")" ] && echo MATCH ✓ || echo MISMATCH ✗)"
sort "$T/B64" > "$T/B64.s"
echo "  accepted@64 not in accepted@4096: $(comm -23 "$T/B64.s" "$T/B.1.s" | wc -l) (expect 0)"
# union of accepted@64 and overflow@64 must cover accepted@4096 (completeness):
sort "$T/ovf64" > "$T/ovf64.s"
cat "$T/B64.s" "$T/ovf64.s" | sort -u > "$T/cover.s"
echo "  accepted@4096 missed by (accepted@64 ∪ overflow@64): $(comm -23 "$T/B.1.s" "$T/cover.s" | wc -l) (expect 0)"

rm -rf "$T"; echo; echo DONE
