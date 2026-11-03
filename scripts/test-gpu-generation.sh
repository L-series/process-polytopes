#!/usr/bin/env bash
# Equivalence of the GPU generation stages with the verified CPU generator:
# exhaustive host checks on the production pools, then CBMC queries.
# Run inside `nix develop .#gpu-verify`. Every query must verify, including its
# unwinding assertions; tool failures are fatal.
set -euo pipefail
cd "$(dirname "$0")/.."
work="$(mktemp -d)"
trap 'rm -rf "$work"' EXIT
checks=(--bounds-check --pointer-check --signed-overflow-check --div-by-zero-check --unwinding-assertions)
cflags=(-DPOLY_Dmax=5 -IPALP -Isrc/cws -Isrc/gpu)

# Host stage: selection pools and slot order, exhaustively on the production
# pools (needs nvcc for the host code and `make` for the PALP objects).
make -s build/palp-Coord.o build/palp-Rat.o build/palp-Vertex.o build/palp-Polynf.o build/palp-LG.o \
  build/project-weight_pool.o build/project-catalogue.o build/gpu-catalogue_bridge.o
cc "${cflags[@]}" -O1 -c verification/gpu/cpu_reference.c -o "$work/cpu_reference.o"
${NVCC:-nvcc} -std=c++17 -Isrc/gpu ${NVCCFLAGS:-} verification/gpu/generation_host.cu "$work/cpu_reference.o" \
  build/palp-Coord.o build/palp-Rat.o build/palp-Vertex.o build/palp-Polynf.o build/palp-LG.o \
  build/project-weight_pool.o build/project-catalogue.o build/gpu-catalogue_bridge.o -o "$work/generation_host"
"$work/generation_host"

python3 verification/gpu/extract.py "$work/gpu_extract.h" "struct SelectedEntry" "struct DeviceDescriptor" \
  "struct DeviceScanStats" "struct DeviceCwsCandidate" "__device__ int device_next_permutation(" \
  "__device__ int device_weight_at_coordinate(" "__device__ int device_slot_uses_coordinate(" \
  "__device__ int device_prefix_is_canonical(" "__device__ __noinline__ void device_store_candidate(" \
  "__device__ __forceinline__ bool device_type3_prefix_ok(" "__device__ unsigned long long device_count_prefixes(" \
  "__device__ unsigned long long device_count_last_slot(" "__device__ unsigned long long device_count_after_slot0(" \
  "__device__ void device_emit_prefixes(" "__device__ void device_emit_last_slot(" \
  "__device__ void device_emit_after_slot0(" "__device__ unsigned long long device_count_small_prefixes(" \
  "__device__ void device_emit_small_prefixes("

# Emission recursion: exhaustive over every per-slot weight order type (the
# reduction is argued in the harness header).
${CXX:-g++} -O2 -I"$work" "${cflags[@]}" verification/gpu/emission_host.cpp "$work/cpu_reference.o" \
  build/palp-Coord.o build/palp-Rat.o build/palp-Vertex.o build/palp-Polynf.o build/palp-LG.o \
  build/project-weight_pool.o build/project-catalogue.o build/gpu-catalogue_bridge.o -o "$work/emission_host"
"$work/emission_host"
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
    if ! cbmc "${checks[@]}" --unwind 7 "$work/q.gb" > "$work/log" 2>&1 || ! grep -q "VERIFICATION SUCCESSFUL" "$work/log"; then
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
  if cbmc "${checks[@]}" --unwind 7 "$work/q.gb" > "$work/log" 2>&1 || ! grep -q "probe: the GPU test never rejects: FAILURE" "$work/log"; then
    tail -20 "$work/log"
    echo "FAIL: probe did not find a rejection for structure $1 slot $2" >&2
    exit 1
  fi
done
echo "Probes: the shared-prefix test rejects where pruning applies"

# One query: compile HARNESS with DEFINES, link the CPU references, verify.
query() {
  local harness=$1 label=$2 unwind=$3; shift 3
  goto-cc -I"$work" "${cflags[@]}" "$@" -c "verification/gpu/$harness" -o "$work/h.gb"
  goto-cc "$work/h.gb" "$work/catalogue.gb" "$work/catalogue_bridge.gb" "$work/cpu_reference.gb" -o "$work/q.gb"
  if ! cbmc "${checks[@]}" --unwind "$unwind" "$work/q.gb" > "$work/log" 2>&1 || ! grep -q "VERIFICATION SUCCESSFUL" "$work/log"; then
    tail -30 "$work/log"
    echo "FAIL: $label" >&2
    exit 1
  fi
}
for u in 2 3 4 5; do query permutation.cpp "permutation step, $u values" 7 -DU="$u"; done
echo "GPU permutation step equals NextPrefixPermutation for 2-5 values"
for id in ${GEN_IDS:-$(seq 2 47)}; do query store.cpp "stored candidate, structure $id" 12 -DCATALOGUE_ID="$id"; done
echo "Stored candidates equal EmbedWeightInCWS rows"
query type3_prefix.cpp "fused structure-3 prefix test" 7
echo "Fused structure-3 prefix test equals the general one"
# Tuple and pair index decoding: proved in Lean (proofs/Refpoly/GpuDecode.lean);
# symbolic 64-bit division and multiplication are out of CBMC's reach here.
