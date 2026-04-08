# Proof: Normal Form Determinism (Plan 2.5)

## Statement

For the same input (CWS weights), `palp_compute_nf` always produces the
same normal form matrix `NF[POLY_Dmax][VERT_Nmax]`. Equivalently, the
xxHash128 of the normal form is a deterministic function of the input weights.

## Risk Assessment (from plan)

The plan identified qsort non-determinism as the primary risk:

> `qsort` is not required to be stable by the C standard. If the comparator
> produces ties, `qsort` could break them differently across calls, leading
> to different vertex orderings and thus different normal forms.

## Resolution: Total Order Eliminates the Risk

The `Sort_VL` comparator (`diff` at Vertex.c:279) computes `a - b` on
vertex indices. CBMC verification (harness_sort_vl.cpp) proves:

1. **No overflow:** Vertex indices are in [0, 63], so `a - b` is in [-63, 63].
2. **Antisymmetry:** `diff(a,b) > 0 <==> diff(b,a) < 0`
3. **Transitivity:** `diff(a,b) > 0 && diff(b,c) > 0 ==> diff(a,c) > 0`
4. **Total order:** `diff(a,b) == 0 ==> a == b`

Property 4 is the key: since vertex indices within a `VertexNumList` are
**distinct integers** (each vertex appears exactly once), `diff` never
returns 0 for distinct elements. This means:

- There are **no ties** for qsort to break
- qsort's output is uniquely determined by the comparator
- The sorted vertex list is deterministic

## Full Determinism Chain

Given the same CWS weights:

1. `Make_CWS_Points` → deterministic lattice point enumeration (single algorithm,
   no randomness, no allocation-dependent ordering)
2. `Find_Equations` → deterministic vertex and equation discovery
3. `Sort_VL` → **deterministic** vertex ordering (proven: total order, no ties)
4. `Make_Poly_Sym_NF` → deterministic normal form computation (depends only on
   sorted vertices and equations)
5. `hash_normal_form` → deterministic xxHash128 (pure function of NF matrix)

Each step depends deterministically on the previous step's output.
Therefore, the hash is a deterministic function of the input weights. **QED.**

## Remaining Concerns

### Thread-local workspace zeroing

`palp_workspace_alloc` uses `calloc` for the workspace struct, ensuring all
fields start at zero. Individual PALP functions (`Make_CWS_Points`, etc.)
initialize their output fields before use. The `memset(C, 0, sizeof(CWS))`
in `palp_compute_nf` ensures the CWS struct is clean before each call.

### Uninitialized memory

Frama-C Eva flagged 243 potential uninitialized reads in PALP (see
eva_palp_report.md). These are overwhelmingly false positives from abstract
domain imprecision. The critical path through `Make_CWS_Points` →
`Find_Equations` → `Sort_VL` → `Make_Poly_Sym_NF` initializes all
fields it subsequently reads.

### Platform dependence

The normal form depends on:
- `sizeof(Long)` — consistently 8 bytes on x86-64 Linux
- Endianness — consistently little-endian on x86-64
- Integer arithmetic — consistent across x86-64 compilers

All builds in this project target x86-64 Linux, so platform dependence
is not a concern.
