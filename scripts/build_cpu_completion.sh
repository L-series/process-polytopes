#!/usr/bin/env bash
# Build a separate, assertion-enabled PALP completion driver with UB traps.
set -euo pipefail
root="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)"
palp_dir="${1:-$root/PALP}"
build_dir="${2:-$root/build/cpu-completion}"
mkdir -p "$build_dir"
sources=(Coord Rat Vertex Polynf LG)
objects=()
for source in "${sources[@]}"; do
    "${CC:-gcc}" -std=gnu11 -O2 -g -DPOLY_Dmax=5 -DCEQ_Nmax=2048 \
        -fsanitize=undefined -fno-sanitize-recover=all -fno-omit-frame-pointer \
        -I "$palp_dir" -c "$palp_dir/$source.c" -o "$build_dir/$source.o"
    objects+=("$build_dir/$source.o")
done
"${CC:-gcc}" -std=gnu11 -O2 -g -DPOLY_Dmax=5 -DCEQ_Nmax=2048 \
    -fsanitize=undefined -fno-sanitize-recover=all -fno-omit-frame-pointer \
    -I "$palp_dir" "$root/src/verify/complete_deferred.c" "${objects[@]}" \
    -lm -o "$build_dir/complete_deferred"
printf 'Built %s\n' "$build_dir/complete_deferred"
