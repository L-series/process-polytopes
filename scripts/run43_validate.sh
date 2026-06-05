#!/usr/bin/env bash
#SBATCH --job-name=run43-val
#SBATCH --partition=gpu
#SBATCH --gres=gpu:RTX6000BW:1
#SBATCH --cpus-per-task=8
#SBATCH --mem=80G
#SBATCH --time=00:25:00
#SBATCH --output=logs/slurm/run43-val-%j.out
#SBATCH --error=logs/slurm/run43-val-%j.err
#
# Validate the new streaming-bucketed/FP path (stream_descriptor_ip_bucketed)
# before the full 43-type run:
#   (a) /local reachable + writable from the COMPUTE node (not just head node).
#   (b) chunk-count invariance: a structure classified with a SMALL emit-capacity
#       (many chunks) must give the IDENTICAL accepted∪overflow set as with a LARGE
#       emit-capacity (one chunk) -- proves the chunk loop drops/duplicates nothing.
#   (c) streaming == single-shot: vs the non-streaming --ip-check bucketed path.
set -uo pipefail
cd "${SLURM_SUBMIT_DIR:-$(pwd)}"
BUILD=src/classify/build-cuda; BIN=$BUILD/cuda_dim5_cws_scan
W5=results/cache/w5.ip; export PALP_W5_POOL="$W5"
OUT=/local/edih210/ahat01/cws43run
echo "### host=$(hostname) $(date) ###"
echo "=== /local from compute node ==="
mkdir -p "$OUT/val" 2>&1 && echo "mkdir $OUT/val OK" || echo "mkdir FAILED"
touch "$OUT/val/.wtest" 2>/dev/null && { echo "/local WRITABLE from $(hostname) ✓"; rm -f "$OUT/val/.wtest"; } || echo "/local NOT writable from compute node ✗"
df -h "$OUT" 2>/dev/null | tail -1

cmake --build "$BUILD" --target cuda_dim5_cws_scan -j8 2>"$BUILD/run43_build.log" \
  && echo "build OK" || { echo "build FAIL"; tail -30 "$BUILD/run43_build.log"; exit 1; }

ST=24   # s24 = 3,207,408 candidates: small enough to fit one chunk, big enough to force several
COMMON="--structure-id $ST --w5 $W5 --ip-check --ip-bucketed --fp-walk --vol-sort --np-cap 256"
T=$OUT/val
echo; echo "=== (b/c) classify s$ST three ways, compare accepted∪overflow ==="
# 1) streaming, SMALL emit-capacity -> many chunks
$BIN $COMMON --stream-ip --emit-capacity 400000 --accepted-output "$T/s_small.acc" --overflow-output "$T/s_small.ovf" 2>"$T/s_small.log" >/dev/null || true
# 2) streaming, LARGE emit-capacity -> one chunk
$BIN $COMMON --stream-ip --emit-capacity 4000000 --accepted-output "$T/s_big.acc" --overflow-output "$T/s_big.ovf" 2>"$T/s_big.log" >/dev/null || true
# 3) non-streaming single-shot, LARGE emit-capacity
$BIN $COMMON --emit-capacity 4000000 --accepted-output "$T/once.acc" --overflow-output "$T/once.ovf" 2>"$T/once.log" >/dev/null || true

for tag in s_small s_big once; do
  ch=$(grep -oE 'chunks: [0-9]+' "$T/$tag.log" | head -1)
  seen=$(grep -oE 'stored_candidates_seen: [0-9]+|generated_candidates_seen: [0-9]+' "$T/$tag.log" | head -1)
  printf "  %-7s accepted=%-7s overflow=%-7s %s %s\n" "$tag" \
    "$(wc -l <"$T/$tag.acc" 2>/dev/null)" "$(wc -l <"$T/$tag.ovf" 2>/dev/null)" "${ch:-chunks:1}" "$seen"
  cat "$T/$tag.acc" "$T/$tag.ovf" 2>/dev/null | sort -u >"$T/$tag.union"
done
echo "  prefix_candidates for s$ST should be 3,207,408"
echo; echo "=== verdict ==="
diff -q "$T/s_small.union" "$T/s_big.union"  >/dev/null && echo "  chunk-count invariant (small==big) ✓" || echo "  small != big ✗"
diff -q "$T/s_big.union"   "$T/once.union"   >/dev/null && echo "  streaming == single-shot ✓"        || echo "  streaming != single-shot ✗"
diff -q <(sort "$T/s_small.acc") <(sort "$T/once.acc") >/dev/null && echo "  accepted set identical ✓" || echo "  accepted DIFFERS ✗"
rm -f "$T"/s_small.* "$T"/s_big.* "$T"/once.*
echo; echo "### DONE $(date) ###"
