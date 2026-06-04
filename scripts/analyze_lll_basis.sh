#!/usr/bin/env bash
#SBATCH --job-name=lll-basis
#SBATCH --partition=std
#SBATCH --nodes=1
#SBATCH --cpus-per-task=16
#SBATCH --mem=16G
#SBATCH --time=00:25:00
#SBATCH --output=logs/slurm/lll-basis-%j.out
#SBATCH --error=logs/slurm/lll-basis-%j.err
#
# LLL basis study (POINT_WALK_ALGORITHMS.md s3): build a Coord.c that dumps the
# triangular enumeration basis per candidate (-DDUMP_BASIS), collect bases over
# spread anchors, then compute the orthogonality defect of the PALP triangular
# (HNF) basis vs an LLL-reduced basis offline.  A large triangular defect that
# LLL collapses to ~1 means the basis is the kind LLL dramatically improves.
set -uo pipefail
cd "${SLURM_SUBMIT_DIR:-$(pwd)}"
PEXP="${PEXP:-$CLAUDE_JOB_DIR/tmp/pexp}"
export PALP_W5_POOL="${PALP_W5_POOL:-$(pwd)/results/cache/w5.ip}"
POOL5=1833327
NANCH="${NANCH:-12}"
PERANCH="${PERANCH:-2500}"     # bases kept per anchor
STRIDE="${STRIDE:-7}"          # candidate sampling stride
OUT=results/point-walk-opt
mkdir -p "$OUT" logs/slurm
[ -f "$PEXP/Coord.c" ] || { echo "missing $PEXP/Coord.c"; exit 1; }

cd "$PEXP"
echo "### build basis dumper (-DDUMP_BASIS) ###"
make clean >/dev/null 2>&1
make cws-5d.x CFLAGS="-O3 -DDUMP_BASIS" >/tmp/bd_dump.log 2>&1 \
  && cp cws-5d.x cws-dump.x && echo "  OK" || { echo "  FAIL"; tail -20 /tmp/bd_dump.log; exit 1; }
cd "$OLDPWD"

anchors(){ local n=$1 i; for((i=0;i<n;i++)); do echo $((1+i*(POOL5-1)/(n-1))); done; }
BASES="$OUT/bases.txt"; : > "$BASES"
echo "### collect bases (${NANCH} anchors x ${PERANCH}) ###"
for k in $(anchors "$NANCH"); do
  DUMP_STRIDE="$STRIDE" PALP_PROFILE_NP=1 "$PEXP/cws-dump.x" -c5 -s3 -j $POOL5 -k $k 2>/tmp/bd_err.txt >/dev/null &
  pid=$!; ( sleep 6; kill $pid 2>/dev/null ) & w=$!; wait $pid 2>/dev/null; kill $w 2>/dev/null
  grep '^BASIS' /tmp/bd_err.txt | head -n "$PERANCH" >> "$BASES"
done
echo "  collected $(wc -l < "$BASES") bases"

echo; echo "### LLL orthogonality-defect analysis ###"
python3 scripts/lll_defect_analysis.py < "$BASES" | tee "$OUT/lll_defect.txt"
echo DONE
