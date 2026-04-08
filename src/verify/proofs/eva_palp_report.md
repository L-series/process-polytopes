# Frama-C Eva Analysis Report — PALP Sources

**Date:** 2026-04-08
**Tool:** Frama-C 31.0 (Gallium), Eva plugin, precision 2
**Target:** PALP Coord.c, Rat.c, Vertex.c, Polynf.c, LG.c
**Defines:** `-DPOLY_Dmax=5 -DPALP_FAST_ASSERT -DCEQ_Nmax=2048`

## Summary

| Metric | Value |
|--------|-------|
| Functions analyzed | 43 / 317 (13%) |
| Statement coverage | 2273 / 2346 (96%) |
| Eva errors | 0 |
| Eva warnings | 0 |
| Kernel warnings | 3 (fprintf format strings — benign) |
| Total alarms | 601 |

## Alarm Breakdown

| Category | Count | Severity | Notes |
|----------|-------|----------|-------|
| Uninitialized reads | 243 | Low | Mostly false positives from abstract domain imprecision |
| Integer overflows | 238 | Medium | PALP uses `Long` (64-bit) arithmetic; many are unreachable |
| Invalid memory access | 74 | High | Need manual triage |
| Out-of-bounds index | 27 | High | Includes `Coord.c:709` and `Coord.c:740` |
| Division by zero | 19 | Medium | Protected by preceding checks in most cases |

## Key Findings

### 2.2 Array Bounds in Make_CWS_Points (Coord.c)

- **P.np range:** Eva computes `P.np ∈ [1..2000000]` (= POINT_Nmax)
- **Coord.c:1175:** Alarm on `P->np + 1` — potential int overflow if np reaches
  INT_MAX. Not reachable in practice (np ≤ 2M), but shows Eva tracks the counter.
- **Verdict:** Eva cannot prove `np ≤ POINT_Nmax` without more precise loop
  invariants. The bound is maintained by PALP's construction logic, which
  Eva's abstract domain approximates too coarsely.

### 2.3 Vertex Count Bounds (Vertex.c)

- **V.nv range:** Eva computes `V.nv ∈ [1..2147483647] or UNINITIALIZED`
- **Verdict:** Eva cannot tighten this bound at precision 2. Higher precision
  (3-4) or ACSL loop invariants on `Find_Equations` would be needed.
  The existing runtime assert `assert(V->nv < VERT_Nmax)` at Vertex.c:1082
  (compiled with PALP_FAST_ASSERT) is the actual safety net.

### 2.6 palp_compute_nf Wrapper

- `P.n > 0` after Make_CWS_Points: **valid** (Eva proves it)
- `V.nv > 0` after Find_Equations: **unknown** (Eva cannot track through
  the conditional return path precisely enough)
- Degree overflow `degree + w`: **alarm** — theoretically possible if
  6 weights of 500 sum to 3000, which fits in int32. False positive.

### Coord.c:709 — Out of Bounds Index

This alarm is in the `SL2Z_Make_Poly_UnitNormal` function, where an index `P`
could exceed `POLY_Dmax * POLY_Dmax = 25`. This function transforms coordinates
using SL(2,Z) matrices. The index depends on loop counters that Eva cannot
bound precisely without knowing the polytope dimension.

**Risk:** Low — the dimension is always 5 for our dataset, and the index
computation `P = n*POLY_Dmax + m` with `n,m < POLY_Dmax` gives `P < 25`.

### Integer Overflows in Rat.c

The GCD / extended Euclidean algorithm in `Rat.c` performs divisions and
multiplications on `Long` values. Eva flags 19 division-by-zero and many
overflow warnings. These are known-safe in practice because:
- GCD inputs are always nonzero (from lattice point coordinates)
- Products fit in 64-bit `Long` for 5D polytopes (coordinates << 2^32)

## Plan 2.4: PALP_FAST_ASSERT is Load-Bearing

**Method:** Ran Eva twice — once with `-DPALP_FAST_ASSERT`, once with `-DNDEBUG`.

| Run | Functions | Statements | Alarms | Sure alarms |
|-----|-----------|------------|--------|-------------|
| PALP_FAST_ASSERT | 43 (13%) | 2273 (96%) | 601 | 0 |
| NDEBUG (standard) | 12 (3%) | 618 (71%) | 150 | **1** |

**Sure alarm with NDEBUG:** `Vertex.c:598` — read from uninitialized `G[i+1][j]`.

**Root cause:** Line 593: `assert(VZ_to_Base(W, r, G))` calls `VZ_to_Base` which
populates `G` as a side effect. With `-DNDEBUG`, `assert()` becomes a no-op,
the call to `VZ_to_Base` is elided, and `G` remains uninitialized. Line 598
then reads garbage from `G`.

**Conclusion:** Compiling PALP with `-DNDEBUG` (without PALP_FAST_ASSERT)
causes **undefined behavior** via uninitialized memory reads. The
`PALP_FAST_ASSERT` macro is **essential for correctness**, not just debugging.
This also explains why only 12 functions (vs 43) are reachable with NDEBUG —
elided assert expressions change control flow and abort paths.

## Recommendations

1. **Add runtime check for P->np overflow:** Before `P->np++` in the
   lattice point enumeration loop, add `assert(P->np < POINT_Nmax)`.
   This is already partially done but should be made explicit.

2. **Increase Eva precision for V.nv:** Run with `-eva-precision 3` or `4`
   to see if Eva can tighten the vertex count bound.

3. **Add ACSL loop invariants:** For Plan 2.3, annotate the main loop in
   `Find_Equations` with `/*@ loop invariant 0 <= V->nv < VERT_Nmax; */`
   and use Frama-C WP to prove it.

4. **Triage Coord.c:709 manually:** Verify the index computation is bounded
   by checking the calling context restricts `n,m < POLY_Dmax`.

## Assertions Status

| Assertion | Location | Status |
|-----------|----------|--------|
| `P.np >= 0` | driver:69 | **valid** |
| `P.np <= POINT_Nmax` | driver:70 | unknown |
| `V.nv >= 0` | driver:76 | unknown |
| `V.nv <= VERT_Nmax` | driver:77 | unknown |
| `P.n > 0` | driver:85 | **valid** |
| `V.nv > 0` | driver:86 | unknown |
