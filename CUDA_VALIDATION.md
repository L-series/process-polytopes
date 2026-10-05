# Validating the CUDA CWS pipeline: equivalence to PALP, memory safety, formal verification

Target: `src/classify/cuda_dim5_cws_scan.cu` on `gpu-run-43types` (3717 lines), the
binary that produces the production IP-accepted CWS set. Line references are to that
version.

Status: validation plan, preserved during the 2026-10-05 repository audit. Proposed
harnesses and checks below are not evidence that they have been implemented or
passed. Existing GPU accepted-set parity and CPU point-set comparisons have
different scopes; the latter do not establish GPU point-set equivalence.

This document defines what "correct" means for this code, what is already
established, what is not, and the concrete artifacts that close each gap. It is
written to be executable as a work plan, not as reassurance.

---

## 0. The three claims, stated separately

They are independent and need different evidence. Conflating them is the main way
a GPU port of an exact algorithm goes wrong.

| # | Claim | Failure mode if false | Method |
|---|---|---|---|
| **C1** | **Equivalence.** For every candidate CWS, the GPU's IP verdict equals PALP's. | Wrong dataset; undetectable downstream. | Differential testing against PALP + structural proof for the FP walk |
| **C2** | **Completeness.** Every candidate in the shard range is classified by GPU or handed to CPU; none is silently dropped. | Missing polytopes; the dataset is quietly incomplete. | Accounting identity + exhaustive audit of every early-return and capacity path |
| **C3** | **Memory safety.** No OOB access, no race, no UB, no reliance on uninitialised state. | Corrupted results that *look* plausible; nondeterminism. | `compute-sanitizer` + static write-site audit + CBMC bounds proofs |

C2 is the one most likely to be violated silently, because a dropped candidate
produces no error and a slightly-too-small output file. C3 is the one most likely
to be violated *invisibly correct* on the test corpus and wrong at 10^13 scale.

---

## 1. The oracle and the comparison boundary

PALP is the reference. The boundary is `PALP/cws-5d.x` and the routines behind it:

```
Make_CWS_Points   → lattice point list
Find_Equations    → vertices, facets, IP flag
IP_Check          → the boolean the GPU reproduces
```

The GPU is *not* required to reproduce PALP's internal ordering, only these
observables — but the more of them you compare, the earlier a bug localises.
Comparison points, in increasing strength:

| Level | Observable | GPU source | Why it matters |
|---|---|---|---|
| **L0** | accepted CWS set (as a set) | `--accepted-output` | what the dataset actually is |
| **L1** | IP verdict per candidate | `np_out` + `stats` | localises to a candidate |
| **L2** | point count `np` per candidate | `np_out[]` | separates point-walk bugs from IP bugs |
| **L3** | point **set** per candidate (as a set) | `d_points` slot | catches a walk that finds the right *count* by luck |
| **L4** | reject reason | `reject_reason` 1..4 | distinguishes "rejected correctly" from "rejected for the wrong reason" |

**Current practice stops at L0/L1.** That is the main methodological gap: every
published parity result (s5 285==285, s12 replays, the `--ip-bucketed` 258==258)
compares accepted sets or aggregate counts. A point walk that drops a
non-vertex interior point and still yields the same IP verdict passes every
existing test. **L3 must be added** — see §3.2.

An important asymmetry: because `device_ip_check` consumes points as an unordered
list (it computes its own lexicographic min/max in `device_glz_start_simplex`,
:1975), point *order* need not match PALP. Compare as sorted sets. The FP walk
explicitly relies on this — it emits points in a different order than the
triangular walk and maps back through `U` to the same 5-D coordinates (:1563).

---

## 2. What is already established

Recording this honestly matters, because the gaps are defined relative to it.

| Evidence | Level | Scope | Source |
|---|---|---|---|
| s5: 285 GPU rows == 285 PALP rows, empty normalized diff | L0 | one structure, complete | `CUDA_ARCHITECTURE.md` |
| s12 sample: 500k candidates, 87,173 accepted, replayed through CPU PALP, **0 failed rows** | L0 (one-directional) | sample | ditto |
| s12 streaming: 2,741,554 candidates, 153,162 accepted, same replay result | L0 | sample | ditto |
| `--ip-bucketed` vs legacy serial: accepted sets **byte-identical** (258==258) | L0 | 25,152 candidates, s3 | `GPU_IP_BUCKETING.md` |
| `--ip-bucketed` completeness: `accept@64 ∪ overflow@64` ⊇ `accept@4096`, 0 missed; `point_overflow == overflow rows written` (2955==2955) | C2 | same shard | ditto |
| Determinism: legacy and bucketed each byte-stable run-to-run | C3 (weak) | same shard | ditto |
| `--fp-walk` bit-exactness: accepted 105==105, overflow 2955==2955; prototype proven bit-exact on 2.6M candidates | L0 | sample | `LLL_FP_WALK.md`, `PRODUCTION_RUN_IDEAS.md` |
| Run 1: 43 structures, 118,676,087,105 candidates, coverage **100.000%** | C2 | production | `CWS_RUN_LEDGER.md` |
| Run 3: s12, 987,911,532,890 candidates, coverage 100.000% | C2 | production | ditto |

**Three limits of this evidence:**

1. **The PALP replay is one-directional.** "Every GPU-accepted row is accepted by
   PALP" (no false positives) is verified. "Every PALP-accepted row is
   GPU-accepted" (no false *negatives*) is only verified where the full candidate
   set was enumerated on both sides — s5 (285 rows) and the 25,152-candidate s3
   shard. False negatives are the dangerous direction: they shrink the dataset
   without any error.
2. **The documented GPU/PALP comparisons do not cover all 46 structures.** s3
   is 82.75% of all work and has small-shard parity evidence, but no exhaustive
   full-corpus comparison. Its later billion-candidate rate sample is not a new
   parity result.
3. **Determinism ≠ correctness.** A kernel with a deterministic OOB read is
   deterministically wrong.

---

## 3. Tier 1 — Equivalence to PALP (C1)

### 3.1 Bidirectional differential harness

Deliverable: `scripts/validate_cuda_vs_palp.sh`.

For a shard small enough that the *complete* candidate set fits under
`--emit-capacity` (the search loop in `validate_ip_bucketed_correctness.sh:31`
already does this probe — reuse it):

```
GPU:  cuda_dim5_cws_scan --structure-id S --shard-count N --shard-index K \
        --ip-check --ip-bucketed --np-cap CAP \
        --accepted-output gpu.acc --overflow-output gpu.ovf
CPU:  cws-5d.x -i -f  < all_candidates          → palp.acc
Assert:  sort(gpu.acc ∪ ip_filter(gpu.ovf))  ==  sort(palp.acc)     [set equality]
```

Set equality, both directions, not `wc -l`. The overflow file must be folded in —
that is the completeness half of the claim, and it is what makes the comparison
fair at small `np_cap`.

Run the matrix: `np_cap ∈ {16, 64, 256, 4096}` × `{--fp-walk, triangular}` ×
`{--vol-sort, off}` × `{--block-ip, off}`. Every cell must produce the *same set*.
Exclude unsupported combinations (`--fp-walk` with `--block-ip` is rejected by
the production scanner). The remaining supported cells are a high-value test: it
makes the optimisation flags provably observationally equivalent, so the
production configuration is not a distinct correctness surface.

### 3.2 Point-set (L3) comparison

Deliverable: `--dump-points <path>` in the scanner + `scripts/compare_points.py`.

Add a debug flag that writes, per candidate, the `np` and the sorted point list
from the `d_points` slot. Compare against PALP's `Make_CWS_Points` output for the
same CWS. This is cheap to implement (the buffer already exists and is already
copied for the np vector) and it is the only test that can catch:

- a point walk that finds a correct-sized but wrong point set,
- an int32 narrowing that shifts a bound by exactly one lattice step,
- an FP-walk `Lo/Hi` root box that is *not* a superset (i.e. `FP_GUARD=4` is
  insufficient for some skewed basis) — a missing point at a leaf.

The last is the one that most needs it: the FP walk's safety argument (:1330)
depends on the root box being a superset, which is asserted from a float
computation plus a fixed integer pad. §5.3 covers proving it; L3 testing is how
you *detect* a counterexample cheaply across millions of candidates.

### 3.3 Corpus design

Random shards under-sample exactly the cases that break things. Stratify:

| Stratum | Why | How to select |
|---|---|---|
| Max box volume | int32 narrowing headroom (§6) | top-N by `vol_key_kernel` key |
| Max `np` | `np_cap`, overflow path, 64-vertex ceiling | candidates with np near and above cap |
| Max skew | FP walk vs triangular divergence | high ratio of box volume to `np` |
| Near-degenerate basis | `device_spd_solve_f` singular fallback (:1512) | rank-deficient / near-singular Gram |
| `np` exactly 5, 6, 7 | simplex boundary (`np < 6` reject, :2660) | direct construction |
| 63, 64, 65 vertices | 64-bit incidence ceiling (:2048) | direct construction |
| Every structure 2..47 | descriptor-specific enumeration paths | one shard each |

The last row is not optional: s38/s40/s42 crashed in job 66815 on a
descriptor-scan stack overflow (fixed in `16bb8fb`) — a bug that only exists in
the nested-permutation structures and would never appear in an s3-only test.

### 3.4 Statistical statement for the un-exhaustible part

12.14T candidates cannot be exhaustively cross-checked; s3 alone is 10.05T. Be
precise about what is claimed instead:

- **Exhaustive** for structures where the full set was enumerated on both sides
  (s5, and any structure small enough — from `CWS_TYPE_COUNTS.md` that is
  everything at rank ≥ 19, i.e. ~5.5M candidates and below: s4, s24, s28, s10,
  s2, s7, s45, s8, s32, s31, s30, s42, s34, s22, s18, s17, s16, s33, s46, s5,
  s44, s9, s23, s41, s39, s19, s37, s47 — 28 of 46 structures are *fully*
  checkable against PALP; measure the actual runtime). **Do this.** It converts
  sampled comparisons to exhaustive comparisons for about 60% of the structure
  space on the specified input pool and builds.
- **Sampled with stated confidence** for s3/s12/s13/s25–29. Report the sample
  size and the resulting upper bound on the disagreement rate (e.g. 0 mismatches
  in 10^7 candidates ⇒ rate < 3×10⁻⁷ at 95%, by the rule of three). Do not write
  "verified" for these.
- **Structural** for the properties that hold uniformly by construction (§5).

---

## 4. Tier 2 — Memory safety (C3)

### 4.1 compute-sanitizer matrix

`compute-sanitizer` is at `/usr/local/cuda/bin/compute-sanitizer`. All four tools,
because they catch disjoint bug classes:

| Tool | Catches | Priority here |
|---|---|---|
| `--tool memcheck` | OOB global/local/shared, misaligned, leaks | **critical** — fixed-size 64-entry lists |
| `--tool racecheck` | shared-memory races | **critical** for `point_enum_block_kernel` (shared basis) |
| `--tool initcheck` | reads of uninitialised device memory | **critical** — `d_points` slots are never zeroed |
| `--tool synccheck` | divergent/invalid `__syncthreads` | needed for the block-cooperative kernels |

Deliverable: `scripts/sanitize_cuda.sh`, running a small shard of **every**
structure under all four tools, with `--fp-walk` and `--block-ip` variants.
Build with `-lineinfo -G` for a debug variant so reports carry line numbers.
Note this is ~100–1000× slower — size the shard accordingly (a few thousand
candidates per structure is enough; these are per-thread bugs, not scale bugs).

`initcheck` deserves emphasis. `d_points` is `cudaMalloc`'d and never memset
(:3170). A candidate that returns early leaves its slot uninitialised; the
contract is that `np_out[i] < 6` prevents `ip_check_bucketed_kernel` from ever
reading it. That contract is currently enforced only by the host-side filter
`if (np >= 6)` at :3242. `initcheck` is what proves the contract holds rather
than assuming it.

### 4.2 Static write-site audit

Every device write must have a provable bound. The table below is the audit
target; each row needs either a proof (§5) or a runtime guard.

| Write site | Bound | Currently enforced by | Status |
|---|---|---|---|
| `device_append_ip_point` (:1310) | `index < max_points` | explicit check, returns 0 | **safe** |
| `points + index*np_cap*5` (:2634) | `index < candidate_count` | grid-stride loop bound | **safe** |
| `vertices[(*vertex_count)++]` (:2010, :2369) | `< 64` | `vertex_count >= 64` check at :2364 — but only in `device_ip_check`, **after** `glz_start_simplex` already appended | **audit** — prove `glz_start_simplex` appends ≤ 6 |
| `candidate_equations->e[ne]` (:2014, :2059) | `< 64` | `DeviceCEqList5.e[64]`; `ne` grows in `device_make_new_ceqs` | **audit** — needs a bound proof or guard |
| `facets->e[ne]` | `< 64` | same | **audit** |
| `ceq_inci[64]` / `facet_inci[64]` / `bad_inci[64]` | `< 64` | indexed by `ne` | **audit** — same obligation as above |
| `accepted_output[atomicAdd(...)]` (:2795) | `< accepted_capacity` | explicit `if (accepted_index < accepted_capacity)` | **safe**, and prove the guard is never hit (§5.2) |
| `overflow_output[atomicAdd(...)]` (:2652) | `< overflow_capacity` | explicit check | **safe**, same |
| `accum[depth+1][A]`, `cur/lo_s/hi_s[depth]` (:1542-1577) | `depth < FP_NMAX=5` | loop structure: descend only when `depth+1 < n`, `n = basis_dim = 5` | **audit** — prove `basis_dim ≤ 5` always |
| `bred[FP_NMAX][10]`, `xu[10]` (:1476) | `nA ≤ 10` | `device_candidate_basic_precheck` rejects `nA > 10` (:1260) | **safe**, precheck is a precondition |
| `basis_store[15][5]` (:1993) | `b_offset[4]+rank ≤ 15` | triangular offset formula | **audit** — arithmetic proof |

The four `**audit**` rows on the equation lists are the highest-risk items: they
are fixed 64-entry arrays inside an ~11 KB per-thread scratch struct, written in a
loop whose trip count depends on the polytope's facet count. PALP's own
`EQUA_Nmax` bounds this on the CPU side; the GPU port must carry the *same* bound
and, unlike PALP, has no `assert` to catch violation. **Add an explicit
`ne < 64` guard that sets a `stats->eq_overflow` counter and rejects the
candidate to the CPU overflow file** — the same completeness-preserving pattern
already used for `np > np_cap` and `FP_NODE_CAP`. This converts a potential silent
buffer overrun into a counted, handled case.

### 4.3 Determinism as a safety probe

Byte-identical output across: repeated runs, different `--blocks`/`--threads`,
and different GPU models (RTX 6000 BW vs L40). The atomic-arrival order of
`accepted[]` is *not* deterministic, so compare **sorted** sets. A difference
across launch geometry is a strong signal of a race or an
occupancy-dependent OOB — this is cheap and should run in CI.

---

## 5. Tier 3 — Formal verification

### 5.1 What is and is not achievable

Be honest about the boundary, or the exercise becomes theatre.

**Not achievable:** end-to-end formal verification of the CUDA kernels. There is
no mature tool that verifies a 3700-line CUDA program with floats, atomics,
grid-stride loops and data-dependent loop bounds. Anyone claiming otherwise is
selling something.

**Achievable, and worth doing:** the code decomposes into three layers with
different tractable methods.

| Layer | Content | Method | Precedent in-repo |
|---|---|---|---|
| **Combinatorial** | structure descriptors, canonicality, sharding | Lean 4 | `lean/` — 11 theorems, already done |
| **Sequential numeric** | every `__device__` routine, as scalar C | **CBMC** | `src/verify/` — 15 harnesses, already done |
| **Concurrent** | grid-stride indexing, atomics, shared memory | separation argument by inspection, machine-checked where possible | new |

The key enabling observation: **`__device__` functions are ordinary C.** With
`#define __device__` and `#define __global__` stubbed out, `device_pd_floor32`,
`device_fp_bounds`, `device_egcd`, `device_make_cws_basis` and the rest compile as
host C and can be handed straight to CBMC — which is exactly the pattern
`src/verify/harness_dim5_*.c` already uses for the CPU enumeration routines.
This is the single highest-leverage realisation in this document: the existing
verification infrastructure extends to the GPU code essentially unchanged.

### 5.2 CBMC proof obligations (sequential layer)

Extend `src/verify/run_verification.sh` with a `4.x` series. Each is a new
`harness_cuda_*.c` including the device routine verbatim.

| # | Harness | Property | Bound |
|---|---|---|---|
| 4.1 | `device_pd_floor32` / `fdiv32` / `cdiv32` | agree with the int64 versions for all inputs in range; no division by zero; no signed overflow | full range via `__CPROVER_assume` on the argument bounds from §6 |
| 4.2 | `device_egcd` / `device_nngcd` | returns a true gcd; Bézout identity holds; terminates | unbounded ints, `--unwind` on the Euclid loop |
| 4.3 | `device_append_ip_point` | never writes outside `points[0 .. max_points*5)` | any `max_points` |
| 4.4 | `device_fp_bounds` | returned `[lo,hi]` never excludes an integer satisfying all box constraints (**soundness: no missed points**) | `n ≤ 5`, `N ≤ 10`, small coefficient range |
| 4.5 | equation-list growth | `ne < 64` at every append in `device_make_new_ceqs` | bounded point count |
| 4.6 | `device_glz_start_simplex` | appends at most 6 vertices; `rank ≤ 5`; `b_offset[depth]+rank ≤ 15` | structural |
| 4.7 | `device_candidate_basic_precheck` | postcondition `nA ≤ 10 ∧ nw ≤ 5 ∧ nA-nw == 5` — the precondition every later routine assumes | full |
| 4.8 | int32 narrowing safety | see §6 | — |

**4.4 is the important one.** It is the formal statement of the FP walk's
soundness: pruning never removes a valid lattice point. Everything else about
the FP walk (LLL quality, enumeration order, node count) is *performance*; only
this is *correctness*. It is decidable at these dimensions and it is exactly the
kind of interval-arithmetic property CBMC handles well.

### 5.3 The FP walk: structural argument, formalised

The comment at :1330 gives the safety argument. It is sound, and it should be
elevated to a proof document (`src/verify/proofs/fp_walk_soundness.md`, matching
the existing `bucket_nonoverlap.md` style). The argument has three legs:

1. **Lattice invariance.** `device_lll_f` performs only integer row operations
   (`b[k][A] -= q*b[l][A]`, :1379) and row swaps, tracking `U` identically. Both
   are unimodular over ℤ *by construction* — independent of the float `mu` that
   chose `q`. Therefore the row span over ℤ is invariant, and so is the
   enumerated point set. Float error can only yield a *less reduced* basis, never
   a different lattice. **This leg is airtight and is why FP32 is admissible.**
2. **Root box is a superset.** `Lo/Hi` come from the ellipsoid solve
   (:1519-1524) plus `FP_GUARD=4`. This is the leg that is *argued* but not
   proven: it depends on FP32 error in `device_spd_solve_f` and `sqrtf` being
   absorbed by 4 integer units. **Obligation:** derive a bound on the float error
   for the actual coefficient ranges (§6) and show it is < 4, or replace the
   fixed guard with a computed one. Until then, treat L3 point-set testing (§3.2)
   as the mitigation and say so explicitly.
3. **Exact leaf recheck.** Every leaf is re-tested in pure integer arithmetic
   (:1560-1561) before being emitted. So the walk can never emit a *wrong* point
   — the only possible error is a *missing* one, which is exactly leg 2. This
   asymmetry is worth stating: **the FP walk cannot produce false points, only
   miss true ones**, which is why leg 2 carries all the risk and why L3 testing
   targets it.

`FP_NODE_CAP = 4,000,000` (:1342) returning −1 routes runaway candidates to the
CPU. Prove this is on the completeness path (it is: −1 is the same status the
overflow handler consumes at :2649), so the cap is a performance guard, not a
correctness one.

### 5.4 Concurrent layer — separation argument

Not a tool problem; a proof-by-inspection with a small number of obligations:

- **Point buffer disjointness.** Thread handling candidate `i` writes only
  `d_points[i*np_cap*5 .. (i+1)*np_cap*5)` and `np_out[i]`. Since the grid-stride
  loop assigns each `i` to exactly one thread, slots are pairwise disjoint. **No
  synchronisation needed and none present — correct.**
- **Scratch disjointness.** `d_scratch + tid` (:2778), one `DeviceIpScratch` per
  launched thread, `launch_threads = blocks*threads` allocated (:3176). Obligation:
  `tid < launch_threads` for every thread — true by construction, worth an
  assertion.
- **Atomics.** `accepted`/`overflow` indices come from `atomicAdd` returning a
  unique slot per caller; capacity is checked before the write. Obligation:
  prove `capacity` can never be exceeded, i.e. `valid_count ≤ candidate_count ==
  accepted_capacity` and at most one overflow row per candidate. Both hold; state
  them.
- **Shared memory.** Only `point_enum_block_kernel` / the `_block` IP path use
  `__shared__`. These need `racecheck` and `synccheck` (§4.1) rather than
  inspection — the basis is built once per block and read by all threads, so the
  obligation is a correct barrier between build and read.

### 5.5 Lean layer — a caveat worth recording

The existing `lean/` proofs (11 theorems: `canonicalFiveDimensionalStructures_length`,
`countsByVertex_correct`, `allSortedProfilesAccountedFor`, …) establish that the
46-structure descriptor table is complete and canonical — a genuinely valuable
result, since a missing structure is an entire missing slice of the dataset.

They are proved with `native_decide`. That is worth stating plainly in any
verification claim: `native_decide` compiles the proposition to machine code and
trusts the result, extending the trusted base to the Lean compiler, its runtime,
and the CPU. It is not the same guarantee as a kernel-checked proof. This is a
reasonable engineering trade for a finite enumeration of this size, but a
write-up that says "formally verified" without it is overclaiming.

---

## 6. The int32 narrowing — an open, concrete obligation

Exp B narrowed the entire point walk to 32-bit. The narrowing happens at four
unguarded sites:

```
:1479   bred[r][c] = static_cast<int>(basis64[r][c]);     // FP walk
:1480   xu[c]      = static_cast<int>(x_upper64[c]);      // FP walk
:1600   b[r][c]    = static_cast<int>(basis[r][c]);       // triangular walk
:1601   xu[c]      = static_cast<int>(x_upper64[c]);      // triangular walk
```

There is **no runtime check** at any of them. The safety argument is a comment
(:1105-1108): max W5 degree 3486, max weight 1743, worst bound product ~3×10⁷,
comfortably inside 2.1×10⁹. `PRODUCTION_RUN_IDEAS.md` already carries this as an
open TODO ("int32 FP overflow guard", commit `b1fa277`).

This is the highest-value single obligation in this document, because:

- it is **silent** — truncation produces a plausible wrong point set, not a crash;
- it affects **both** walks, so the `--fp-walk`/triangular cross-check in §3.1
  would not catch a bound that overflows in both;
- the argument covers `basis` entries and `x_upper`, but the *intermediate* sums
  in the walk are what actually overflow: `low -= x[k]*b[k][source_coord]`
  (:1649) and, in `device_fp_bounds`, `sLo += bb*Lo[j]` / `sHi += bb*Hi[j]`
  (:1437-1438) accumulate products of basis entries with root-box bounds. Those
  are not bounded by the comment's `degree × weight` argument.

**Close it two ways, both cheap:**

1. **Runtime guard (do this first, today).** At each narrowing site, check the
   int64 value against `INT32_MAX` before casting; on violation, route the
   candidate to the overflow file exactly like `np > np_cap` — completeness
   preserved, and `stats->narrowing_overflow` tells you empirically whether it
   ever fires. If it never fires across a full s3 run, that is strong evidence,
   and it costs a comparison per coordinate against a walk that does millions of
   divisions.
2. **CBMC proof (obligation 4.8).** Given `__CPROVER_assume` bounds on degree,
   weight and `nA` taken from the actual W5/W4 pools, prove no signed overflow
   occurs in `device_make_points_serial` or `device_fp_bounds`. CBMC's
   `--signed-overflow-check` does this directly. This is the durable answer.

Until one of these lands, the correct statement is "believed safe by a
range argument on inputs, not verified" — not "verified".

---

## 7. Gating

What must pass before committing GPU-hours to the s3 run (82.75% of all work,
and the run whose output is hardest to re-derive):

**Blocking:**
- [ ] §6 runtime narrowing guard in place, counter wired to `print_ip_result`
- [ ] §4.2 equation-list `ne < 64` guard with overflow-to-CPU routing
- [ ] §3.1 bidirectional set equality on all 32 flag cells, ≥ 3 structures
- [ ] §4.1 `compute-sanitizer` clean (all four tools) on a shard of every structure 2..47
- [ ] §3.3 exhaustive PALP parity for all 28 fully-enumerable structures

**Before publication:**
- [ ] §5.2 CBMC harnesses 4.1–4.8 in `run_verification.sh`
- [ ] §5.3 `fp_walk_soundness.md` with leg 2 either proven or explicitly caveated
- [ ] §3.2 L3 point-set comparison across ≥ 10^6 candidates spanning the §3.3 strata
- [ ] §4.3 cross-GPU-model determinism (RTX 6000 BW vs L40)
- [ ] Statistical statement (§3.4) with sample sizes and confidence bounds, replacing any bare "verified" for s3/s12/s13/s25–29

**Recording:** every run should capture node, SLURM job ID, GPU model, driver and
CUDA runtime version, compiler version, git commit, and full command line — the
standard `CUDA_ARCHITECTURE.md` already sets. Parity results without the corpus
hash and commit are not reproducible claims.

---

## 8. Deliverables

| Path | Content |
|---|---|
| `scripts/validate_cuda_vs_palp.sh` | §3.1 bidirectional set-equality matrix |
| `scripts/compare_points.py` + `--dump-points` flag | §3.2 L3 point-set comparison |
| `scripts/sanitize_cuda.sh` | §4.1 four-tool sanitizer sweep |
| `src/verify/harness_cuda_*.c` (8 files) | §5.2 CBMC obligations 4.1–4.8 |
| `src/verify/run_verification.sh` | extended with the 4.x series |
| `src/verify/proofs/fp_walk_soundness.md` | §5.3 three-leg argument |
| `src/verify/proofs/cuda_memory_bounds.md` | §4.2 write-site audit with proofs |
| `cuda_dim5_cws_scan.cu` | §6 narrowing guard, §4.2 `ne` guard, both with counters |

---

## 9. Summary of the honest claim

After the above, a candidate statement to substantiate with actual run artifacts
is the following. It is not a statement of current validation status:

> The CUDA IP filter is **exhaustively verified equivalent to PALP** for 28 of 46
> structures (complete enumeration, both directions), and **sampled-equivalent
> with a disagreement rate below 3×10⁻⁷ (95%)** for the remaining 18 including
> s3/s12/s13. Memory safety is established by `compute-sanitizer` across all
> structures and by CBMC bounds proofs on every device routine's write sites.
> The point-enumeration and IP-check routines are **formally verified as
> sequential C** (CBMC, bounded); the concurrent layer is verified by a
> disjointness argument plus race/sync sanitizers. The 46-structure descriptor
> table is **machine-checked complete** in Lean 4 (via `native_decide`, which
> trusts the Lean compiler). The Fincke-Pohst walk's soundness rests on a
> proven unimodularity argument plus exact integer leaf rechecking; the
> float-derived root-box superset property is [proven / empirically validated
> across N candidates].

That is a strong claim, and every clause in it is backed by an artifact in §8.
It is also materially weaker than "formally verified", which this codebase cannot
honestly assert and does not need to.
