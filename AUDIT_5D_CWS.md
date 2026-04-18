# 5D CWS Audit

Date: 2026-04-16

## Scope

This audit covered four linked questions:

1. Whether the Lean formalization really proves the claimed 47 canonical 5D overlap structures.
2. Whether the legacy 3D/4D `cws.c` generator is still structurally sound.
3. Whether the newer descriptor-driven 5D PALP path matches the paper's combinatorics.
4. Whether the 5D PALP output is canonical, rather than containing symmetry-equivalent duplicates.

## What Changed

The concrete code fix is in `PALP/cws.c`.

- Added `Dim5WeightAtCoordinate`.
- Added `Dim5PrefixIsCanonical`.
- Updated `EnumerateDim5Permutations` to prune shared-prefix permutations that differ only by swapping ambient vertices that are still indistinguishable in the already embedded partial CWS.

This is the 5D analogue of the old anchor-aware canonicalization already present in the legacy 2-file path.

Without this fix, the descriptor generator could emit multiple lines related by a global permutation of shared equal-weight vertices. A minimal explicit structure-11 example exhibited this failure before the patch.

## Regression Changes

- Updated `PALP/tests/4.2.8-cws-c5-structure15.sh` to reflect the now-canonical output.
- Added `PALP/tests/4.2.13-cws-c5-structure11-canonical.sh` to lock down the shared-prefix symmetry case directly.

## Verified Results

### Lean

Built successfully in the repository's nix Lean shell:

```sh
cd /home/ahatziiliou/math/process-polytopes/lean
nix develop ..#lean -c lake build
```

The key formal claims remain theorems in `lean/MinimalPolytopes5D/Structures.lean`:

- `canonicalFiveDimensionalStructures_length = 47`
- `countsByProfile_correct = [([6], 1), ([5, 2], 2), ([4, 3], 4), ([4, 2, 2], 7), ([3, 3, 2], 18), ([3, 2, 2, 2], 13), ([2, 2, 2, 2, 2], 2)]`

### Paper / combinatorics

The paper states explicitly that `[6]` is the simplex case and therefore the single-weight-system case; only the other profiles are genuine CWS cases.

That distinction matters operationally:

- `-w5` covers the simplex `[6]` WS case.
- `-c5` covers the genuine combined structures, i.e. ids `2..47`.

### PALP 5D descriptors

The descriptor table in `PALP/dim5_structures.inc` was checked mechanically against `paper/minimal_polytopes_5d_table.tex`.

Result:

- ids `2..47` match the paper exactly after normalizing ordered profiles to sorted profiles.
- id `1` is not in the descriptor table because it is the single-weight-system simplex case, not a genuine combined pattern.

### Legacy 3D/4D generator

The legacy automatic generators remain structurally clean.

Direct duplicate checks on the rebuilt `cws-5d.x` and `cws-6d.x` binaries gave identical results. The dimension-specific binary to prefer for 5D work is `cws-5d.x`; `cws-6d.x` is only the default higher-`POLY_Dmax` build.

The duplicate checks were:

```text
c3 total=21
c3 unique=21

c4 total=17320
c4 unique=17320
```

So the old hardcoded `d <= 4` constructor split is still duplicate-free in these aggregate outputs.

### 5D canonicalization fix

The minimal explicit structure-11 repro now canonicalizes correctly:

```text
total=16
unique=16
```

Before the patch, this case produced symmetry-equivalent duplicate rows.

### Targeted PALP tests

The following dim-5 regression slice passes after the patch:

```sh
cd /home/ahatziiliou/math/process-polytopes/PALP
./tests/4.2.7-cws-c5-overlap3.sh
./tests/4.2.8-cws-c5-structure15.sh
./tests/4.2.9-cws-c5-structure37.sh
./tests/4.2.10-cws-c5-structure47.sh
./tests/4.2.11-cws-c5-auto-structure47.sh
./tests/4.2.12-cws-c5-auto-structure5.sh
./tests/4.2.13-cws-c5-structure11-canonical.sh
```

## Commands You Can Run Now

### Rebuild the PALP CWS binaries

```sh
cd /home/ahatziiliou/math/process-polytopes/PALP
make cws-5d.x cws-6d.x
```

This rebuild succeeded on this machine, with warnings in `cws.c`, but without blocking `cws-5d.x`, `cws-6d.x`, or the Lean build.

### Re-run the Lean proof build

```sh
cd /home/ahatziiliou/math/process-polytopes/lean
nix develop ..#lean -c lake build
```

### Test the simplex 5D WS case

```sh
cd /home/ahatziiliou/math/process-polytopes/PALP
./cws-5d.x -w5 6 6
```

Expected leading output:

```text
6  1 1 1 1 1 1 r
#primepartitions=1 #IPpolys=1
```

### Test a genuine 5D builtin CWS structure

```sh
cd /home/ahatziiliou/math/process-polytopes/PALP
./cws-5d.x -c5 -s47
```

### Run the entire builtin 5D CWS classification for geometry types `2..47`

This is the full genuine-CWS run. It can take a long time, so do not run it expecting immediate terminal feedback. Write it to a file.

```sh
cd /home/ahatziiliou/math/process-polytopes/PALP
./cws-5d.x -c5 > all_5d_cws_2_47.txt
```

If you want elapsed time and a persistent log-friendly workflow:

```sh
cd /home/ahatziiliou/math/process-polytopes/PALP
time ./cws-5d.x -c5 > all_5d_cws_2_47.txt
```

If you want it in the background so you can keep using the terminal:

```sh
cd /home/ahatziiliou/math/process-polytopes/PALP
nohup ./cws-5d.x -c5 > all_5d_cws_2_47.txt 2> all_5d_cws_2_47.err &
```

Useful follow-up commands while or after it runs:

```sh
wc -l all_5d_cws_2_47.txt
tail -n 5 all_5d_cws_2_47.txt
tail -n 20 all_5d_cws_2_47.err
```

### Run the entire minimal-geometry 5D universe, including the simplex type `[6]`

This is the mathematically complete split of the 47 minimal geometry types:

- geometry type `1` is the simplex WS case
- geometry types `2..47` are the genuine CWS cases

Run them as two separate commands:

```sh
cd /home/ahatziiliou/math/process-polytopes/PALP
./cws-5d.x -w5 6 6 > all_5d_ws_type_1.txt
./cws-5d.x -c5 > all_5d_cws_2_47.txt
```

If you want one combined file, concatenate them explicitly:

```sh
cat all_5d_ws_type_1.txt all_5d_cws_2_47.txt > all_5d_minimal_types_1_47.txt
```

### Run one geometry type from `2..47`

For one builtin geometry type, use `-sN`.

Example for type `15`:

```sh
cd /home/ahatziiliou/math/process-polytopes/PALP
./cws-5d.x -c5 -s15 > geom_15.txt
```

Example for type `47`:

```sh
cd /home/ahatziiliou/math/process-polytopes/PALP
./cws-5d.x -c5 -s47 > geom_47.txt
```

This is the right command family if what you mean by “classification for one geometry type” is:

- use PALP's builtin descriptor for a specific canonical overlap geometry
- enumerate all CWS belonging to that one geometry type only

### Inspect one geometry type interactively

If the output is short enough that you want to inspect it directly in the terminal instead of writing it to a file:

```sh
cd /home/ahatziiliou/math/process-polytopes/PALP
./cws-5d.x -c5 -s15 | head -n 20
```

### Test a builtin structure that exercises 3- and 4-weight sources

```sh
cd /home/ahatziiliou/math/process-polytopes/PALP
TMP=$(mktemp)
./cws-5d.x -c5 -s5 > "$TMP"
wc -l "$TMP"
head -n 1 "$TMP"
tail -n 1 "$TMP"
rm -f "$TMP"
```

Expected summary:

```text
lines=285
first=3 1 1 1 0 0 0 0  4 0 0 0 1 1 1 1  M:350 12 N:8 7
last=6 1 2 3 0 0 0 0  66 0 0 0 5 6 22 33  M:63 12 N:45 7
```

### Test the generic structure-15 path

```sh
cd /home/ahatziiliou/math/process-polytopes/PALP
./cws-5d.x -c5 -n3 \
  tests/input/4.2.8-cws-w2.txt \
  tests/input/4.2.7-cws-c5-overlap3.txt \
  tests/input/4.2.8-cws-w4.txt \
  -s15
```

This should now print three canonical rows rather than the old five-row output with symmetry duplicates.

### Test the explicit shared-prefix symmetry repro

```sh
cd /home/ahatziiliou/math/process-polytopes/PALP
A=$(mktemp)
B=$(mktemp)
OUT=$(mktemp)
printf '4 1 1 1 1\n' > "$A"
printf '9 1 2 3 3\n' > "$B"
./cws-5d.x -c5 -n3 "$A" "$B" "$B" -s11 > "$OUT"
printf 'total=%s\n' "$(wc -l < "$OUT")"
printf 'unique=%s\n' "$(sort "$OUT" | uniq | wc -l)"
head -n 1 "$OUT"
tail -n 1 "$OUT"
rm -f "$OUT" "$A" "$B"
```

Expected summary:

```text
total=16
unique=16
```

## Remaining Known Issues

### PALP full build now succeeds, but warnings remain

`make -j2` for the full PALP tree now succeeds on this machine. The earlier failures came from non-self-contained headers (`Subpoly.h`, `Nef.h`, `Mori.h`) that assumed `Global.h` had already been included. Those headers now include `Global.h` directly, and `Global.h` is protected by an include guard so the default build completes.

There are still compiler warnings in unrelated legacy code paths (for example `Polynf.c`, `E_Poly.c`, `MoriCone.c`, and `SingularInput.c`), but they do not currently block the PALP build or the 5D CWS workflow.

### WS vs CWS split is intentional but easy to misread

The repository now has a mathematically correct split:

- simplex `[6]` lives on the WS path (`-w5`)
- genuine 5D combined patterns live on the CWS path (`-c5`, ids `2..47`)

Operationally, that means there are two different meanings of “run the entire classification”:

- if you mean all genuine combined 5D geometry types, run `./cws-5d.x -c5`
- if you mean the full 47-type minimal-geometry universe, run both `./cws-5d.x -w5 6 6` and `./cws-5d.x -c5`

If you want a single public-facing entry point for “all 47 minimal 5D geometry types,” that should be implemented as a wrapper/documentation layer rather than by pretending `[6]` is a combined structure.

## Suggested Next Steps

1. Run the downstream 5D classification pipeline on a representative subset and compare its aggregate counts against the Lean/paper profile counts.
2. Decide whether you want an explicit user-facing wrapper that reports the full 47-type universe as `1 x WS + 46 x CWS`.
3. If you care about stronger regression protection, add one or two more explicit canonicalization tests for structures with shared-count `3` and mixed previous-row symmetries.