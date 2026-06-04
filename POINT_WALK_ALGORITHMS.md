# Point-walk algorithm exploration — `Make_CWS_Points` / `point_enum_kernel`

> Follow-up to `PIPELINE_PROFILING.md` §9–§10, which established that the lattice
> point walk is ~99 % of the pipeline and is dominated by integer division
> (`PD_Floor`) inside per-level bound tightening (`CLB`), run ~12 000× per point
> produced because the search box ≫ the point set. This document explores four
> concrete directions, **CPU first**, each validated for correctness and measured
> on the SLURM cluster (std partition, node n11 = 2× EPYC 9554, Zen4):
>
> 1. Audit of the PALP point-enumeration optimizations described in Schöller &
>    Skarke, *All Weight Systems for Calabi–Yau Fourfolds from Reflexive
>    Polyhedra* (arXiv:1808.02422), §5, vs. what this tree actually does.
> 2. Alternate walk algorithms with correctness preserved, targeting early
>    rejection of polytopes with more than one interior point (IP).
> 3. LLL reduction of the enumeration basis — does it cut cycles, is it still correct.
> 4. Different integer-division intrinsics (reciprocal multiply / libdivide).
>
> Scripts: `scripts/benchmark_recip_div.sh` (§4), `scripts/analyze_lll_basis.sh`
> + `scripts/lll_defect_analysis.py` (§3). Dual-mode variant source and raw data
> under `results/point-walk-opt/`.

---

## 0. The geometry the walk is enumerating (needed for §1–§2)

For a structure-3 candidate at `index == 1`, `Make_CWS_Points` sets the origin
`X0[A] = 1` for every ambient coordinate `A` and enumerates the lattice points
`X = X0 + Σ_j x_j·B.x[j]` (basis `B` from `Make_CWS_Basis`) that satisfy

```
0 ≤ X_A ≤ Xmax_A           Xmax_A = min_k ⌊d_k / w_{k,A}⌋
Σ_A w_{k,A} X_A = d_k       (every point lies on each weight hyperplane)
```

The polytope's only facets are the orthant walls `X_A = 0`, so a lattice point
is **interior iff `X_A ≥ 1` for every ambient `A`** (there is no upper-facet
strict condition — `Xmax_A` is an implied vertex bound, not a facet). The walk
itself does *not* compute interiority; it lists *all* points, and `IP_Check`
later builds the hull from them and tests whether the origin is strictly inside.

This is the hook for §2: interior points are a *sub-box* (`X_A ≥ 1`) of the box
the walk already traverses.

---

## 1. Audit: which PALP optimizations from arXiv:1808.02422 §5 are present?

§5 ("Implementation") of Schöller–Skarke is the canonical reference for *this
exact* computation (all 5d weight systems → reflexive polyhedra). It describes
three point-enumeration optimizations:

| # | Paper's optimization (§5) | In this tree? | Where |
|---|---|---|---|
| 1 | **Improved loop-exit conditions** — tighten the per-coordinate range using *every* contributing hyperplane before descending, so loops exit as early as possible | **YES** | `CLB` (`Coord.c`) intersects `[lo,hi]` over all ambient `A` in the level; the generic fallback does the same inline. Present in `Old_Make_CWS_Points` too — predates the dim-5 fast-path refactor. |
| 2 | **Interior-point counting without the full list** — for `l_int` take only the *first and last* point of each interior line and add the multiplicity, "without having to create and analyse the full list" | **NO** | The dim-5 fast path *stores every point* (batch x0 sweep) and reports `np` = all points. No interior-only/first-last-in-line counting exists. → this is the unexploited idea explored in §2. |
| 3 | **Bounding parallelepiped** — enclose all vertices in a coordinate box and test lattice points of the box | **YES** | The `Xmax` box + triangular basis is exactly this. |

**Takeaways.**
- The *box-tightening* optimization (their #1, the headline of §5) is already
  fully present — the `CLB` intersection of bounds from all contributing
  ambient coordinates *is* their "improved exit conditions." So there is no
  free lunch left there.
- Their **#2 (count interior points without materializing the full list)** is
  **not** implemented here and is the most relevant to "early-reject >1 IP."
  §2 evaluates it.
- The paper does **not** use LLL/basis reduction, faster division primitives,
  hashing, or sub-loop parallelism for the walk — so §3 and §4 are genuinely
  beyond what the reference PALP-derived pipeline does.

---

## 2. Alternate walk algorithm: interior-point counting / early-reject

### 2.1 The idea (paper opt #2, repurposed as an early-exit)

The walk already visits a box that contains the interior sub-box `{X_A ≥ 1 ∀A}`.
To decide "does this polytope have more than one interior point?" we do **not**
need the full point list — we can count interior points directly:

* run the same nested bound walk but with the **lower ambient bound raised from
  0 to 1** (`Low_int = 1 − X0[A] − offset` instead of `−X0[A] − offset`);
* the interior points of each innermost `x0` line form a **contiguous segment**
  `[xmn0_int, xmx0_int]`, so its contribution is `xmx0_int − xmn0_int + 1`
  (the paper's "first and last point in the line") — no per-point storage;
* accumulate, and **short-circuit the instant the running count reaches 2**.

This is strictly cheaper than the current full enumeration (smaller box, no
stores, early bail) and would let a >1-interior polytope be rejected without
the full walk **and** without `IP_Check`.

### 2.2 Correctness check — is ">1 interior point" a real filter here?

Instrumented build (`-DINT_COUNT`, archived `Coord.recip-div.c`): for each
candidate, count enumerated lattice points with all ambient `X_A ≥ 1`
(`g_n_int`), emitted alongside the IP decision via the `PALP_PROFILE_NP` hook.
**275 006 candidate signatures**, 12 spread anchors across the full slot-0 pool:

| IP class | share | interior-point count `n_int` |
|---|---|---|
| degenerate / non-IP (ip = 0) | 92.94 % | — (≤ 1) |
| **IP-passing (ip = 1)** | 7.06 % (19 427) | **`n_int = 1` for 100.0000 %** |
| any non-degenerate candidate | — | **max `n_int` = 1** |

**Not a single candidate has more than one interior point.** This is forced by
construction (§0): the candidates are normalized weight systems with
`Σ_A w_{k,A} = d_k`, so for any interior point `Σ_A w_{k,A}(X_A − 1) = 0` with
`X_A − 1 ≥ 0` and `w_{k,A} ≥ 0` ⇒ `X_A = 1` wherever some weight is positive
⇒ the all-ones origin is the **unique** interior point of every non-degenerate
candidate.

### 2.3 Verdict

- **The ">1 interior point" early-reject is provably correct but vacuous** on the
  W5 / structure-3 data — it can never fire, so it cannot speed anything up
  (running the interior count would always return 1 and add cost). This is the
  same shape of result as the aristotle_2 `Xmax ≤ 1` criterion (proven sound,
  measured to never fire): mathematically sound, empirically inert because the
  IP condition already constrains the weight systems to a unique interior point.
- **The reusable part of the paper's opt #2 still has value, just not for
  rejection.** Counting points by first/last-in-line *without storing them*
  doesn't help the CPU walk (PIPELINE_PROFILING §9.1 showed storage is already
  negligible; the cost is the bound *divisions*), but it is valuable **on the
  GPU**, where eliminating the per-thread point buffer is exactly what lifts the
  VRAM grid cap and occupancy (PIPELINE_PROFILING §7). Keep it as a GPU lever.
- **Because the interior point is unique and known (the origin), the productive
  correctness-preserving levers are the ones that cut the *full* (boundary-heavy)
  enumeration's over-search**, not interior counting: a tighter basis (§3) and a
  cheaper division primitive (§4). The hull/IP decision is determined entirely by
  the boundary (vertex) points; a future deeper change is to enumerate only the
  boundary and skip the interior bulk, or compute the hull directly — but on this
  data the interior bulk is just the single origin, so the win there is small;
  the real bulk is boundary lattice points, which §3/§4 attack.

---

## 3. LLL reduction of the enumeration basis

### 3.1 Is the result still correct afterwards? — YES, provably

The walk enumerates the lattice points of a fixed region (box ∩ weight
hyperplanes) of the affine lattice `L = X0 + span_Z(B)`. Replacing `B` by any
other basis of the **same** lattice is a unimodular (GL(n,ℤ)) change of
coordinates: the *set* of enumerated lattice points is identical, only their
`x`-coordinates differ. LLL produces exactly such a unimodular transform, so
`np`, interiority, the convex hull, and the IP decision are all invariant, and
PALP's downstream normal-form stage canonicalises coordinates — **the final
classification is unchanged**. Correctness is not at risk; the question is
purely whether it is faster.

### 3.2 Does it cut the cycle count? — the basis is maximally skewed, LLL fixes it

Orthogonality defect `δ = ∏‖b_i‖ / covol(L)` (1 = orthogonal; large = skewed),
exact-arithmetic, 6 000 dumped real bases (`scripts/analyze_lll_basis.sh` +
`scripts/lll_defect_analysis.py`; raw → `results/point-walk-opt/lll_defect.txt`):

| basis | median δ | p90 | p99 | max |
|---|---|---|---|---|
| **triangular (PALP HNF)** | **1.5 × 10⁹** | 2.1 × 10¹³ | 3.5 × 10¹⁶ | 6.7 × 10¹⁷ |
| **LLL-reduced** | **1.24** | 1.46 | 1.72 | 2.33 |

The triangular basis PALP enumerates on is **astronomically skewed** (δ up to
10¹⁷); LLL brings it to **near-orthogonal (δ ≈ 1.2–2.3)** — the theoretical
floor. On the heavy tail (np ≥ p90, the candidates that dominate walk cost) the
triangular defect is still ~78 median and LLL cuts it ~58× median. So a
well-conditioned basis is exactly what the data is missing.

### 3.3 The two caveats that bound the realised benefit

1. **`CLB` already recovers most of the *raw* skew.** The walk does not traverse
   the raw triangular box; `CLB` intersects bounds from every contributing
   ambient coordinate per level. So the *realised* over-search is the ~12 000
   divisions/point of PIPELINE_PROFILING §9.3 (177 → 127 251 across the tail) —
   far below the 10⁹ raw defect. LLL's *realised* gain is therefore **bounded by
   that residual over-search, not by the defect ratio** — large on the heavy
   tail (10²–10⁵), but not 10⁹.
2. **The triangular nested walk cannot use an LLL basis.** The five-loop fast
   path needs the lower-triangular `Amin` structure so each constraint bounds a
   single coordinate. An LLL basis is dense; enumerating on it requires general
   **Fincke–Pohst** (Gram–Schmidt triangularisation over ℝ, ellipsoidal bounds).
   And you cannot "have both": the Hermite normal form is unique per lattice, so
   **HNF(LLL(B)) = HNF(B)** — re-triangularising an LLL basis gives back exactly
   today's skewed basis. The benefit only materialises if the walk is rewritten
   around the reduced basis + GSO.

### 3.4 Verdict — **NOW IMPLEMENTED, PROVEN CORRECT, AND MEASURED** (see `LLL_FP_WALK.md`)

**LLL targets the right thing** — and §4 shows the right thing is *fewer nodes*,
not cheaper divisions. The Fincke–Pohst prototype recommended here has since been
built (`scripts/fp_enum.c`), validated for correctness, and benchmarked on SLURM
over **180 239 real candidates**. Result (full write-up: `LLL_FP_WALK.md`):

- **Correctness: identical point set on 180239/180239 candidates** (and every
  LLL transform unimodular, `det U = ±1`). Proven by construction + verified.
- **57× fewer integer divisions, 74× fewer tree nodes, ~9× less wall time**
  (the 9× *includes* the LLL-reduction cost, which is only ~5 µs/candidate).
  Per-point divisions drop from median 2031 → 144 (p90 20892 → 715, max
  656130 → 8602).
- §3.3 caveat 1 was directionally right (the prize is the ~12 k div/point
  over-search, not the 10⁹ raw defect) but **under-estimated** it — 57× is large.
  §3.3 caveat 2 was exactly right: the win needs a *general* (FP) walk, since
  `HNF(LLL(B)) = HNF(B)` rules out re-triangularising. That walk is what was built.

So §5’s “highest upside, unproven” is upgraded to **proven**; the only remaining
step is wiring the FP walk into `Make_CWS_Points` itself (`LLL_FP_WALK.md` §6).

---

## 4. Integer-division intrinsics (reciprocal multiply / libdivide)

### 4.1 What and why

`PD_Floor(N,D)` computes `⌊N/D⌋` with a hardware 64-bit `idiv`, and it is
~100 % of the walk (PIPELINE_PROFILING §9.2). Crucially **the divisor is a
fixed basis pivot `|B.x[j][A]|`** reused across the entire candidate, so it is a
classic "division by a runtime constant" case: precompute a libdivide
reciprocal (magic multiplier + shift) once per candidate per basis entry, then
replace every `idiv` in `CLB` with a multiply-high + shift + correction.

The variant is a single dual-mode `Coord.c` (`-DRECIP_DIV`), archived at
`results/point-walk-opt/Coord.recip-div.c`. `ld_floor` reproduces `PD_Floor`
exactly (`libdivide_s64_do` gives `trunc(N/|D|)`, then the same floor
correction), so results are bit-identical by construction.

### 4.2 Correctness

**Bit-identical.** Per-candidate `NP <np> <dim> <ip>` signatures are byte-for-byte
equal between baseline and `-DRECIP_DIV` over spread anchors spanning the whole
slot-0 pool (240 000 candidate signatures verified; job 66676 on n11).

### 4.3 Throughput — **reciprocal multiply is 19 % SLOWER** (job 66676, n11)

16 spread anchors × 100 000 candidates, `-O3 -march=native`, rdtsc over
`Make_CWS_Points`:

| build | points_cyc / candidate | IP_Check cyc / cand (control) |
|---|---|---|
| baseline (`idiv`) | **1 095 177** | 6 739 |
| `-DRECIP_DIV` (libdivide) | **1 299 758** | 6 902 |
| **ratio** | **0.84× (−18.7 %, slower)** | 0.98× (≈ unchanged ✓) |

The IP_Check control is ~1.0×, confirming the change is isolated to the walk.
**The reciprocal-multiply replacement is bit-identical and consistently
*slower*** — the opposite of the ~2–4× that PIPELINE_PROFILING §10.2
hypothesised.

### 4.4 Why it loses — and what it corrects

This is the important result. The §9 attribution ("walk ≈ 100 % `PD_Floor`,
divisions dominate") measured *where cycles are spent*, but it does **not** mean
divisions are the *throughput bottleneck*. Three factors, in order of weight:

1. **The `idiv` latency is already hidden by out-of-order execution.** Each `CLB`
   level interleaves divisions with multiply-adds (offset), `min`/`max`
   intersections, comparisons and loop control. Zen4's OoO engine runs that
   surrounding integer work *while* the divider is busy, so the divider is not
   on the critical path. Replacing one (hidden-latency) `idiv` with a ~5–7-op
   reciprocal sequence puts those ops **on** the serial bound-tightening
   dependency chain (each `*lo/*hi` feeds the next), which lengthens it.
2. **Most frequently-used divisors are small.** Of 637 k sampled nonzero basis
   pivots, **≈55 % are ≤ 16** and 38 % are ≤ 4; Zen4's `idiv` has
   data-dependent latency and is cheap for small operands, leaving little for a
   reciprocal to beat. (A third of pivots *are* large — up to 82 549 — but the
   innermost, highest-trip levels use the small low-order basis rows.)
3. **Per-candidate precompute.** Building one `libdivide_s64_t` per nonzero
   pivot adds work to every candidate; negligible for the heavy candidates that
   dominate the mean, but pure overhead for the 71 % light ones.

**Correction to the profiling narrative:** the walk is **not** division-throughput
bound — it is bound by the *dependent chain of per-level bound computations and
loop control*. So the productive lever is **reducing the number of bound
computations / nodes** (fewer divisions, via a tighter box — §3), **not** making
each division cheaper. PIPELINE_PROFILING §9.2/§10.2 are annotated accordingly.

> **GPU caveat (deferred):** on sm_120 there is *no* hardware integer divider —
> 64-bit division is an emulated multi-instruction sequence that is **not**
> latency-hidden the way Zen4's `idiv` is. So reciprocal-multiply (and especially
> int32 reciprocal) is far more likely to win on the GPU. CPU result here does
> **not** transfer; test separately when GPU work resumes (PIPELINE_PROFILING §10.3).

### 4.5 int32 data width — not tested, and unlikely to help division throughput

A 32-bit walk would have cheaper `idiv` and reciprocals, but by the same
latency-hiding argument the division speed is not the CPU bottleneck; int32's
plausible benefit is register/cache pressure, which is a *data-width* change
rather than a division intrinsic. Lower priority than §3 given the negative
reciprocal result; flagged for completeness.

---

## 5. Summary & recommendations

| Direction | Correct? | Faster? (CPU, measured) | Verdict |
|---|---|---|---|
| **§1** Paper §5 box-tightening (opt #1) | — | already present | no headroom left |
| **§1/§2** Paper §5 interior counting (opt #2) | ✓ | n/a here | vacuous for rejection (see §2) |
| **§2** Early-reject ">1 interior point" | ✓ provably | **never fires** | vacuous on W5 (unique IP by construction) |
| **§4** Reciprocal-multiply division (libdivide) | ✓ bit-exact | **0.84× — slower** | rejected on CPU; retest on GPU |
| **§3** LLL-reduced basis + Fincke–Pohst | ✓ proven (180k cands) | **57× fewer divs, ~9× wall** | **WINNER** — see `LLL_FP_WALK.md`; wire into `Make_CWS_Points` |

**The throughline.** §4 is the key empirical result: the walk is **not**
division-throughput bound (the `idiv` latency is hidden by OoO), so making each
division cheaper does nothing — it even regresses. Combined with §2 (the only
interior point is the known origin, so interior tricks don't help) and §3 (the
basis is pathologically skewed, and a tighter basis cuts the *number* of
nodes/divisions), the conclusion is unambiguous:

> **Optimise the *number* of bound computations (nodes), not the cost per
> division.** The realised over-search is ~12 000 divisions per point produced
> (127 k on the heavy tail); a near-orthogonal (LLL) basis with a Fincke–Pohst
> walk attacks exactly that, and is correctness-preserving by construction.

**Recommended next steps (CPU), in priority order:**
1. ~~Prototype a Gram–Schmidt Fincke–Pohst enumerator on the LLL-reduced basis~~
   **DONE** (`LLL_FP_WALK.md`): the prototype is built, proven correct on 180 239
   candidates, and measured at **57× fewer divisions / 74× fewer nodes / ~9× wall**.
   The remaining work is to **wire the LLL+FP walk into `Make_CWS_Points`** (box-
   metric LLL after `Make_CWS_Basis`, then dispatch to the general FP walk instead
   of the triangular 5-loop) and confirm end-to-end with `PALP_PROFILE_TIMING`.
   Also a candidate parallelism lever: the FP walk’s outer level has independent
   subtrees (`LLL_FP_WALK.md` §5), useful for heavy-tail load-balancing and GPU.
2. **Do not pursue reciprocal-multiply or int32 on the CPU** for division speed
   (negative / data-width only). Revisit reciprocal-multiply **on the GPU**,
   where division is emulated and not latency-hidden.
3. **Drop the ">1 interior point" early-reject** as a CPU classification filter
   (vacuous); keep store-free interior/total counting only as a **GPU** lever for
   the point-buffer / occupancy problem (PIPELINE_PROFILING §7).

**Artifacts.** `scripts/benchmark_recip_div.sh` (build + correctness + timing);
dual-mode variant `results/point-walk-opt/Coord.recip-div.c`
(`-DRECIP_DIV` / `-DINT_COUNT` / `-DDUMP_BASIS`); `results/point-walk-opt/libdivide.h`;
LLL analysis `results/point-walk-opt/lll_defect.txt` (+ analyzer). All runs via
sbatch on the std partition (n11, Zen4); see PIPELINE_PROFILING.md §9–§10.
