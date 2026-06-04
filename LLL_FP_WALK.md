# LLL + Fincke–Pohst lattice-point walk — implementation, correctness, speedup

> Follow-up to `POINT_WALK_ALGORITHMS.md` §3, which identified an LLL-reduced
> basis + Fincke–Pohst enumeration as *“the highest-upside correctness-preserving
> direction”* but left it **unmeasured** (the §5 recommended next step). This
> document **implements** that walk, **proves it correct** (identical point set on
> 180 239 real candidates), and **measures** it on the SLURM cluster. It also
> answers the parallelism question: which work in the 5-fold loop is
> dependent / parallelizable / precomputable.
>
> **Headline result (prototype/offline) — the §3 hypothesis is confirmed.** On
> real structure-3 / W5 data the LLL+FP walk enumerates the *exact same lattice
> points* as PALP’s triangular walk while doing **57× fewer integer divisions,
> 74× fewer tree nodes, and ~9× less wall time (including the LLL-reduction
> cost)**.
>
> ⚠ **The 9× is on a heavy-biased sample and does NOT transfer to the in-tree
> integration. See §6.** Once wired into `Make_CWS_Points` and measured on the
> *real* candidate distribution, the per-candidate LLL+ellipsoid overhead
> (~1e4 cyc) dominates the light-candidate bulk: the realized speedup is
> **~1.4–2.1× on the type-3/overlapping-size-5 hot path (up to 1.77× gated) and
> overhead-bound (slower) on light or higher-arity structures**. Correctness,
> however, is proven in-tree on **all 46 structures / 2.6M candidates** (§6.1).
> The 57×/74× node/division *counts* still hold — they just don't convert to
> wall once fixed per-candidate overhead is paid.

| metric (180 239 real candidates, n11/Zen4) | PALP triangular | LLL + FP | ratio |
|---|---|---|---|
| point set | reference | **identical** | ✅ 180239/180239 |
| tree nodes (bound computations) | 1.843 × 10⁹ | 2.49 × 10⁷ | **0.0135 (74× fewer)** |
| integer divisions | 1.044 × 10¹⁰ | 1.836 × 10⁸ | **0.0176 (57× fewer)** |
| divisions / point — median | 2 031 | 144 | 14× |
| divisions / point — p90 | 20 892 | 715 | 29× |
| divisions / point — max | 656 130 | 8 602 | 76× |
| wall, enumeration only | 21.82 s | 1.51 s | **14.5×** |
| wall, **incl. LLL reduction** | 21.82 s | 2.41 s | **9.1×** |
| LLL det(U) ≠ ±1 (unimodularity failures) | — | **0** | ✅ |

> Scripts: `scripts/fp_enum.c` (the prototype), `scripts/benchmark_fp_enum.sh`
> (build + collect + correctness + work comparison), `scripts/loop_parallel.c` +
> `scripts/benchmark_loop_parallel.sh` (LLL-cost + OpenMP parallelism). PALP’s
> `Coord.c` gained an inert `-DDUMP_BASIS` hook that emits the per-candidate
> basis/X0/Xmax. Raw data → `results/point-walk-opt/` (gitignored).

---

## 1. What the walk enumerates (recap of `POINT_WALK_ALGORITHMS.md` §0)

For a structure-3 candidate `Make_CWS_Points` lists the lattice points

```
X = X0 + Σ_j x_j · b_j        (b_j = the j-th enumeration basis row B.x[j])
0 ≤ X_A ≤ Xmax_A   for every ambient coordinate A
```

The weight-hyperplane constraints are automatically satisfied because the `b_j`
span the weight lattice, so the region is purely the **box** `[0,Xmax]` pulled
back to a polytope in the 5 lattice coordinates `x`. PALP enumerates it with a
5-deep nested loop on a **block-lower-triangular** basis (`Amin` structure):
each `CLB` call bounds one `x_j` using only the ambient coordinates that basis
row introduces, deferring the shared lower coordinates to deeper levels. Because
the triangular basis is **pathologically skewed** (orthogonality defect median
~10⁹, `POINT_WALK_ALGORITHMS.md` §3.2), those deferred constraints make the
search box ≫ the point set ⇒ the ~12 000-divisions-per-point over-search of
`PIPELINE_PROFILING.md` §9.3.

## 2. What was implemented (`scripts/fp_enum.c`)

A standalone prototype with three enumerators over the identical region, each
collecting the ambient point set and counting work (internal bound-nodes +
integer divisions):

1. **`tri_enum`** — a byte-faithful replica of PALP’s dim-5 fast path (same
   `Amin`, same `CLB`, same incremental `lev` accumulators, same `PD_Floor`).
   This is the **reference**: its point count is checked against PALP’s own `np`
   for every candidate (180239/180239 ✅), so it provably reproduces production.

2. **LLL reduction** of the basis, in the **box-scaled metric**
   `⟨u,v⟩ = Σ_A u_A v_A / r_A²` (with `r_A = Xmax_A/2`), which is the metric the
   box geometry actually induces — reducing in this metric, not raw Euclidean,
   is what makes the basis well-conditioned *for this box*. Standard LLL
   (δ = 0.99) with floating-point GSO and integer row operations; the unimodular
   transform `U` is tracked and its determinant checked exactly (Bareiss) to be
   ±1. **Correctness of the lattice is guaranteed regardless of FP rounding** —
   size-reduction (`b_k −= q·b_l`) and swaps are unimodular by construction, so
   the lattice (hence the enumerated set) is invariant; a “wrong” FP `q` only
   means less reduction, never a different lattice.

3. **`gen_enum`** — a general, **Fincke–Pohst-style** enumerator that works on an
   *arbitrary* (dense, non-triangular) basis:
   - the box’s circumscribed **ellipsoid** `Σ_A ((X_A−c_A)/r_A)² ≤ ρ` is built;
     its Gram matrix `G = MᵀM` (`M = D⁻¹B`) and Cholesky give the least-squares
     center `x̂` and per-axis half-widths `√(ρ′·(G⁻¹)_jj)` — this yields a valid
     **root search box** (a superset, by the ellipsoid ⊇ box containment) without
     any LP solve. This is the Fincke–Pohst ingredient (GSO-derived bounds).
   - inside that box it enumerates with **exact-integer box-constraint
     propagation**: at each level the current variable’s interval is tightened
     against every ambient half-plane `0 ≤ X_A ≤ Xmax_A`, bounding the not-yet-
     fixed variables by their current ranges (interval arithmetic, floor/ceil
     division — never floating point in the bound, never an LP). Each leaf is
     filtered against the exact integer box before being recorded.
   - enumeration order = longest-GSO-vector-outermost (the FP convention).

`gen_enum` is run **twice** per candidate: on the triangular basis (validates
the enumerator itself, and exhibits the over-search) and on the LLL basis (the
experiment). All three point sets are sorted and compared.

## 3. Correctness — proven and verified

- **Mathematical guarantee.** Replacing the basis by any other basis of the same
  lattice is a GL(5,ℤ) change of coordinates: the *set* of enumerated lattice
  points is identical, only the integer coordinates `x` differ. LLL produces
  exactly such a unimodular transform (`det U = ±1`, verified exactly on every
  candidate, 0 failures). The ellipsoid strictly **contains** the box, and every
  leaf is re-checked against the exact integer box, so `gen_enum` can neither
  miss a box point nor admit a non-box point.
- **Empirical.** Over **180 239** structure-3 candidates (the pathological heavy
  anchor `k=1` plus 17 anchors spread across the full 1 833 327-element W5 slot-0
  pool):
  - `tri_enum` point count == PALP’s `np`: **180239 / 180239**.
  - `gen_enum(LLL)` point set == `tri_enum` point set: **180239 / 180239**.
  - LLL `det(U) ≠ ±1`: **0**.
  - `gen_enum(triangular)` matched on the tractable validation subset
    (177 481 / 177 488); the 7 “misses” are **not** wrong answers — they are the
    node-cap safety bail-out on the skewed basis (the very over-search pathology
    LLL removes). `gen_enum(LLL)` never hit the cap (0 overflows).

**The LLL+FP walk is correct.**

## 4. Work and wall time — the measured win

Over all 180 239 candidates (`scripts/benchmark_fp_enum.sh`, job 66677/66679 on
n11):

```
tri (PALP)   nodes = 1,842,751,825   divisions = 10,436,178,150
gen (LLL)    nodes =    24,945,658   divisions =    183,554,762
             nodes ratio = 0.0135 (74× fewer)   divs ratio = 0.0176 (57× fewer)

per-point divisions:   tri  median 2031 · p90 20892 · max 656130
                       LLL  median  144 · p90   715 · max   8602
per-candidate LLL/tri division ratio:  median 0.073 · p10 0.007 · p90 0.430

wall:  tri_enum 21.82 s   |   LLL-reduce 0.90 s + gen 1.51 s = 2.41 s   (9.1×)
       (LLL reduction costs ~5 µs/candidate — cheap enough to apply to ALL
        candidates, not only the heavy tail.)
```

Heavy tail (np ≥ 64, 22 233 candidates — the cost-dominating set): division
ratio 0.0174 (**57× fewer**), per-candidate median ratio 0.047 (**21×**).

### 4.1 Reconciliation with `POINT_WALK_ALGORITHMS.md` §4 (why 57× divisions → only 9× wall)

§4 established the counter-intuitive fact that the CPU walk is **not
division-throughput bound** — Zen4’s `idiv` latency is hidden by out-of-order
execution, so making each division *cheaper* (libdivide) was 19 % *slower*.
This experiment uses the *other* lever: **57× fewer divisions** (a tighter
basis ⇒ fewer tree nodes), and that **does** pay off — ~9× wall. The wall gain
(9×) being smaller than the division ratio (57×) is exactly what §4 predicts:

1. the walk’s time is dominated by the dependent per-level bound-computation
   chain and loop control, of which divisions are only one part, and
2. `gen_enum`’s per-node cost is genuinely higher than `CLB`’s — it carries the
   FP-derived root box and does O(n·N) interval arithmetic per node — so 74×
   fewer nodes does not become 74× less time.

Both effects are real and consistent: **the productive lever is fewer nodes, and
LLL delivers them.** This is the result §3/§4 pointed at and is now measured.

### 4.2 What §3 got right, and where it was too cautious

§3.3 listed two caveats that “bound the realised benefit”. With data:
- ✅ Caveat 1 was directionally right (the realised over-search is the ~12 k
  div/point of §9.3, not the 10⁹ raw defect) — but it **under-estimated** the
  prize: 57× fewer divisions is a large, not marginal, win, because PALP’s `CLB`
  recovers the skew per-level only *locally*, while LLL removes it *globally*.
- ✅ Caveat 2 was exactly right: the triangular nested walk cannot use an LLL
  basis (`HNF(LLL(B)) = HNF(B)`), so a **general** (Fincke–Pohst-style) walk is
  required — which is what was built here.

§3’s verdict (“highest upside, **unproven**”) and §5’s recommendation are hereby
**resolved**: the upside is real and now proven.

## 5. The 5-fold loop: dependent / parallelizable / precomputable work

The user asked whether there is dependent work in the 5 nested loops that can be
parallelized or precomputed. Classifying every piece of per-step work:

| work in the loop | nature | verdict |
|---|---|---|
| sibling subtrees (distinct `x4`; distinct `x3` within an `x4`; …) | **independent** — share no mutable state but the output buffer | **embarrassingly parallel** |
| `lev` accumulators (`lev4 += B4` each step, etc.) | loop-carried recurrence **with a closed form**: after fixing `x4`, `lev4[A] == x4·B4[A]` | **not a barrier** — any subtree can be *started* from its index alone (the recurrence is an O(1) optimisation, not a serialisation) |
| divisor reciprocals `1/|b_j[A]|` | loop-invariant per candidate | **precomputable** (this is the libdivide of §4 — valid to precompute, but doesn’t help CPU throughput) |
| `Xmax[A]`, `Amin[]`, first pivots, `zero[]` | loop-invariant per candidate | **precomputable** (already hoisted by PALP) |
| per-level bound tightening along **one** root→leaf path (each level’s `[lo,hi]` depends on the outer levels’ chosen values) | genuinely **serial**, depth = dim = 5 | the only true dependency; limits single-*path* ILP (and is exactly why per-division speedups didn’t help, §4) — but does **not** obstruct breadth parallelism |

**So: the breadth of the tree is parallel; only the depth-5 path is serial.**

### 5.1 Parallelism experiment (`scripts/loop_parallel.c`, OpenMP over `x4`)

Because `lev4 = x4·B4` is closed-form, each `x4` value starts an independent
subtree. Parallelising the outer loop with OpenMP (`schedule(dynamic,1)`,
per-thread counts) on the heaviest candidates (np ≈ 1500–1955, x4-range 45–61),
**verifying identical point counts (`match=YES` on every run)**:

| threads | speedup (heaviest candidates) |
|---|---|
| 1 | 1.00× (baseline) |
| 2 | 1.4–1.6× |
| 4 | 2.0–2.9× |
| 8 | 3.2–3.5× |
| 16 | 5.3–6.1× |

Correct and useful, but **sub-linear** — because even the heaviest single
candidate is only ~1–2 ms of work and the `x4` range is short (≈ 45–61), so
OpenMP fork/join overhead and load imbalance across the short outer range eat
into scaling.

### 5.2 Where intra-loop parallelism actually belongs

The production pipeline is **already candidate-parallel and core-saturated**
(`PIPELINE_PROFILING.md`: 128 cores, 97.6 % busy). In steady state, intra-loop
parallelism competes with candidate-level parallelism for the same cores → no
throughput gain. Its real value is two-fold:
- **heavy-tail load-balancing.** Per-candidate cost has CV = 6.5, max/mean
  1544× — a few monster candidates serialise the tail of a batch while other
  cores idle. Splitting a monster’s `x4` loop across the idle cores cuts that
  tail latency (the 5.1 measurement is exactly this regime).
- **GPU.** The independent-subtree structure is the natural source of the
  thousands of threads a GPU wants per candidate; combined with the closed-form
  `lev` it maps cleanly to a block-cooperative walk (`PIPELINE_PROFILING.md` §7).

This composes with §2–4: a Fincke–Pohst walk has the same independent-subtree
structure (outer GSO level → independent inner enumerations), so LLL+FP and
breadth parallelism stack.

## 6. In-tree integration into `Make_CWS_Points` (production)

The walk is now wired into PALP's dim-5 fast path (`PALP/Coord.c`, gated by
`-DLLLFP_WALK`; the stock build is byte-for-byte unchanged). Mode via the
`PALP_WALK` env var: `tri` (triangular, default), `fp` (LLL+Fincke–Pohst),
`check` (run both, assert identical point sets). Build:
`make -C PALP cws-5d.x CPPFLAGS='-DLLLFP_WALK -fno-math-errno'`.

**Key adaptation vs the prototype.** `_P->x[]` stores the *basis-B coefficient
vector*, not the ambient point. An LLL-reduced basis `B'=U·B` changes those
coordinates, so each leaf maps the reduced-basis coeff `x'` back to B-coords via
`x[j] = Σᵢ x'ᵢ·U[i][j]` (U integer, `b'ᵢ=Σⱼ U[i][j]·bⱼ`). Output is therefore
bit-identical to the triangular walk and all downstream code (sublattice
reduction, `IP_Check`) is untouched. A per-candidate **node budget** falls back
to the triangular walk if the ellipsoid box is loose (it never was, in practice).

### 6.1 Correctness — proven in-tree across every CWS type

`PALP_WALK=check` ran both walks on **all 46 canonical structures (s2–s47),
2,623,150 candidates**, spanning every arity nw=2,3,4,5 (ambient N=7..10):
**0 mismatches, 0 fallbacks**. The profiler's `np_sum`/`ip_pass` aggregates are
identical between `tri` and `fp`. So the integration is correct for *all* CWS
kinds, not just the type-3 (two-5WS-overlap, s3) case the prototype used.
(`scripts/benchmark_lllfp_integration.sh`, jobs 66755/66762; W5 pool **must** be
cached via `PALP_W5_POOL=results/cache/w5.ip` or every invocation regenerates
the 184026-row size-5 pool — see memory `project_w5_pool_cache`.)

### 6.2 Throughput — the prototype's 9× does NOT transfer; here is why

In-tree per-candidate `Make_CWS_Points` cycles (`PALP_PROFILE_TIMING`, n11,
representative modulo shards / full small structures):

| structure | weights | nw | tri cyc | fp cyc (ungated) | speedup |
|-----------|---------|----|--------:|-----------------:|--------:|
| s3 (type-3) | 5+5 | 2 | 47 408 | 34 143 | **1.39×** |
| s6 | 4+5 | 2 | 70 847 | 33 984 | **2.08×** |
| s5 | 3+4 | 2 | 5 108 | 65 991 | 0.08× |
| s7 | — | 2 | 13 017 | 71 396 | 0.18× |
| s11 | — | 3 | 16 800 | 23 761 | 0.71× |
| s38 / s40 | — | 4 | ~7 800 | ~22 000 | 0.36× |
| s46 | — | 5 | 27 793 | 56 830 | 0.49× |

Two things flip the prototype's 9×:

1. **The 9× was a heavy-biased sample.** The prototype's 180k type-3 candidates
   were drawn 22% from the pathological heavy anchor; per candidate it averaged
   ~121 µs. The *real* type-3 distribution averages ~15 µs/candidate (np≈8.7) —
   ~8× lighter. fp's node pruning only pays on the heavy tail, so on the true
   distribution the realized speedup collapses toward the overhead floor.
2. **Per-candidate fixed overhead.** The box-metric LLL + ellipsoid (Gram +
   Gauss-Jordan) setup costs **~1×10⁴ cycles/candidate**, independent of np. For
   a light candidate (s40 tri ≈ 7 900 cyc) that overhead alone exceeds the
   *entire* triangular walk, so fp loses. The original LLL re-ran a full
   Gram-Schmidt after every size reduction; replacing that with the standard
   **O(n) incremental-`mu` update** (size reduction leaves the GSO vectors
   unchanged) lifted ungated type-3 from 1.22× → 1.39×.

**The real discriminant is basis skew + a large heavy-candidate pool, not
arity.** fp wins on the *overlapping size-5* structures (s3, s6) that carry the
bulk of the classification's heavy, maximally-skewed candidates; it is
overhead-bound on light or small-pool structures (s5, s7) and higher arity.

### 6.3 The heavy-tail gate (`PALP_LF_LOGVOL_MIN`)

A cheap pre-walk gate skips the LLL+ellipsoid for candidates whose box is too
small to repay it — score = Σ bit-length(Xmax_A) (≈ log₂ box volume); fp only if
score ≥ threshold. Sweep on the type-3 hot path:

| `PALP_LF_LOGVOL_MIN` | s3 speedup | s11 (nw3) | s40 (nw4) |
|---------:|----:|----:|----:|
| 0 (off) | 1.42× | 0.69× | 0.36× |
| 20 | **1.77×** | 0.70× | 0.38× |
| ≥30 | 1.00× | ~1.00× | ~0.99× |

The gate lifts type-3 to **1.77×** and recovers the light/higher-arity
structures to ~1.0× — but **no single global threshold is optimal**: the score
overlaps between s3's beneficial candidates and s11's (which never benefit),
because the true discriminant is basis skew, not box volume. Default is 0 (off);
`PALP_LF_LOGVOL_MIN=20` is the recommended type-3 setting.

**Recommended deployment:** enable fp for the type-3 / overlapping-size-5
structures (s3, s6 — the hot path), `PALP_LF_LOGVOL_MIN≈20`; keep the triangular
walk elsewhere. **Cleaner future work:** an *adaptive* gate — run the triangular
walk with a node budget and switch to fp only when it is exceeded — is structure-
agnostic and would give ≥1.0× everywhere plus the type-3 gains, without a tuned
threshold; it requires instrumenting the triangular hot loop (deferred).

## 7. Limitations & history

- The 57×/74× **node and division** reductions are **algorithmic and
  implementation-independent** — they are counts, not timings. The **9× wall**
  is a prototype-to-prototype comparison (both `tri_enum` and `gen_enum` are
  un-tuned `-O2/-O3` C; `tri_enum` faithfully mirrors PALP’s incremental
  structure, so it is a fair proxy, but a production LLL+FP would need its own
  tuning — and `gen_enum`’s per-node overhead is higher than `CLB`’s, room to
  optimise).
- **The prototype 9× wall did not transfer to the in-tree integration** — see
  §6.2. On the real candidate distribution the per-candidate LLL+ellipsoid
  overhead (~1e4 cyc) dominates the bulk of (light) candidates; the realized
  win is ~1.4–2.1× on the type-3/overlapping-size-5 hot path and overhead-bound
  elsewhere. The 57×/74× node/division counts still hold; they just don't
  convert to wall once fixed overhead is paid per candidate.
- Reducing in the **box-scaled** metric matters: raw-Euclidean LLL would not
  target the geometry the enumeration actually sees.

## 8. Reproduce

```bash
# prototype (offline, on dumped bases): node/division ratios + correctness
sbatch scripts/benchmark_fp_enum.sh        # build DUMP_BASIS dumper, collect
                                           # 180k candidates, run fp_enum
sbatch scripts/benchmark_loop_parallel.sh  # LLL-cost-inclusive wall + OpenMP x4

# in-tree integration (PALP/Coord.c -DLLLFP_WALK):
sbatch scripts/benchmark_lllfp_integration.sh  # check ALL 46 structures (0 mism)
                                               # + tri-vs-fp throughput
sbatch scripts/benchmark_lllfp_sweep.sh        # PALP_LF_LOGVOL_MIN gate sweep
sbatch scripts/benchmark_lllfp_arity.sh        # per-structure ungated speedup
```

Outputs: `results/point-walk-opt/fp_enum_summary.txt`, `fp_enum.csv`,
`candidates.txt`; SLURM logs in `logs/slurm/`. The DUMP_BASIS hook in
`PALP/Coord.c` is inert unless built with `-DDUMP_BASIS`; the `LLLFP_WALK` walk
likewise compiles only with `-DLLLFP_WALK` (`PALP_WALK=tri|fp|check`). **Always**
`export PALP_W5_POOL=results/cache/w5.ip` or cws-5d.x regenerates the size-5
weight pool (>1 min) every run. See
`POINT_WALK_ALGORITHMS.md` (§3 in particular) and `PIPELINE_PROFILING.md` §9–§10.
```
