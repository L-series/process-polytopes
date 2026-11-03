#!/usr/bin/env bash
# CBMC equivalence of the GPU generation predicates with the verified CPU ones.
# Run inside `nix develop .#gpu-verify`. Every query must verify, including its
# unwinding assertions; tool failures are fatal.
set -euo pipefail
cd "$(dirname "$0")/.."
work="$(mktemp -d)"
trap 'rm -rf "$work"' EXIT
checks=(--bounds-check --pointer-check --signed-overflow-check --div-by-zero-check --unwind 7 --unwinding-assertions)
cflags=(-DPOLY_Dmax=5 -IPALP -Isrc/cws -Isrc/gpu)

python3 verification/gpu/extract.py "$work/gpu_extract.h" "struct SelectedEntry" "struct DeviceDescriptor" \
  "__device__ int device_weight_at_coordinate(" "__device__ int device_slot_uses_coordinate(" \
  "__device__ int device_prefix_is_canonical("
for c in src/cws/catalogue.c src/gpu/catalogue_bridge.c verification/gpu/cpu_reference.c; do
  goto-cc "${cflags[@]}" -c "$c" -o "$work/$(basename "$c" .c).gb"
done

slots=0
for id in ${GEN_IDS:-$(seq 2 47)}; do
  rows=$(python3 -c "import json; print(len(next(d for d in json.load(open('tests/descriptor-reference.json')) if d['id']==$id)['blocks']))")
  for slot in $(seq 0 $((rows - 1))); do
    goto-cc -I"$work" "${cflags[@]}" -DCATALOGUE_ID="$id" -DSLOT="$slot" -c verification/gpu/prefix_canonical.cpp \
      -o "$work/h.gb"
    goto-cc "$work/h.gb" "$work/catalogue.gb" "$work/catalogue_bridge.gb" "$work/cpu_reference.gb" -o "$work/q.gb"
    if ! cbmc "${checks[@]}" "$work/q.gb" > "$work/log" 2>&1 || ! grep -q "VERIFICATION SUCCESSFUL" "$work/log"; then
      tail -30 "$work/log"
      echo "FAIL: shared-prefix test, structure $id slot $slot" >&2
      exit 1
    fi
    slots=$((slots + 1))
  done
done
echo "GPU shared-prefix test equals CPU PrefixIsCanonical on $slots slots"

# Non-vacuity: where pruning applies (structure 12 slot 0, structure 3 slot 1,
# structure 42 slot 1) the GPU test must be able to reject.
for probe in "12 0" "3 1" "42 1"; do
  set -- $probe
  goto-cc -I"$work" "${cflags[@]}" -DPROBE -DCATALOGUE_ID="$1" -DSLOT="$2" -c verification/gpu/prefix_canonical.cpp -o "$work/h.gb"
  goto-cc "$work/h.gb" "$work/catalogue.gb" "$work/catalogue_bridge.gb" "$work/cpu_reference.gb" -o "$work/q.gb"
  if cbmc "${checks[@]}" "$work/q.gb" > "$work/log" 2>&1 || ! grep -q "probe: the GPU test never rejects: FAILURE" "$work/log"; then
    tail -20 "$work/log"
    echo "FAIL: probe did not find a rejection for structure $1 slot $2" >&2
    exit 1
  fi
done
echo "Probes: the shared-prefix test rejects where pruning applies"
