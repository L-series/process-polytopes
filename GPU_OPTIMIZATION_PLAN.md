# GPU Point-Enumeration Optimization Plan

> Goal: squeeze more throughput out of the `cuda_dim5_cws_scan` point-enumeration
> kernel — the ~99.9% bottleneck of GPU Stage 1. Grounded in the measured
> profiling (`PIPELINE_PROFILING.md` §9–§10), the bucketed-kernel benchmarks
> (`PRODUCTION_RUN_IDEAS.md` §2), and the actual kernel code in
> `src/classify/cuda_dim5_cws_scan.cu`. Current best measured: **264k cand/s/GPU**
> (np_cap 16) → ~106k (np_cap 64), occupancy-bound. Target: **0.5–1 M/s/GPU**
> (`PIPELINE_PROFILING.md` §6), i.e. 3–10×, which would make the GPU fleet the
> primary engine instead of a ~2–3-node-equivalent sidecar.

Arch: `sm_120` (RTX 6000 Blackwell) + `sm_89` (L40). All measurements via
`sbatch` on the `gpu` partition.

---

## 1. What the profiling actually says (the constraints any plan must respect)

From `PIPELINE_PROFILING.md` §9–§10 and the code:

1. **The walk is ~100% of the kernel; basis build & point store are noise**
   (basis ≈ 0%, walk ≈ 100%, IP check 0.1%). Optimize the *walk*.
2. **The walk is dominated by integer division inside per-level bound tightening**
   — ~176k `device_pd_floor` (64-bit signed `/`) per candidate, ~12,000 divisions
   per point produced, because the search box ≫ the point set. 25 `pd_floor`
   call sites; `sm_120` has **no hardware integer divider** → each is an emulated
   multi-instruction sequence (much costlier than Zen4's ~6-cycle `idiv`).
3. **It is occupancy/latency/divergence bound, NOT bandwidth bound** (mem-ctrl
   util 0%; ncu blocked so occupancy is inferred). Two concrete causes in code:
   - **Per-thread local-memory spill.** `device_make_points_serial` carries
     `long long basis[5][10]` (400 B) + `x_upper[10]` + `x0[10]` + `xmin/xmax/x[5]`
     ≈ 600–700 B/thread in local memory; `device_make_cws_basis` transiently uses
     `long long next_basis[10][10]` (800 B). This caps warps/SM.
   - **Warp divergence** from the CV-6.5 per-candidate cost spread (np 1 → 2033):
     one heavy lane stalls 31 others. Bucketing by `np_cap` only coarsely tames it.
4. **Cycle-attribution caveat:** "divisions dominate" is *where cycles are spent*,
   not proof of a throughput bound. On CPU, replacing `idiv` with reciprocal-
   multiply was **19% slower** because the walk is bound by the *dependent chain
   of bound computations*, not division throughput. **On GPU this may differ**
   (emulated division is not latency-hidden) — but the same lesson applies: the
   highest-value lever is **fewer nodes / a shorter dependent chain**, and only
   then cheaper division. Every experiment below states which it attacks.

Redundant-work observation from the code (not yet in the profiling doc): the
walk recomputes the offset `Σ_{k>walk_dim} x[k]·basis[k][i]` **from scratch** at
every bound and every coordinate (`cuda_dim5_cws_scan.cu:1335,1346,1361`). That
is O(depth × coords) redundant multiply-adds on the serial critical path — a
direct target for incremental maintenance (Exp. C).

---

## 2. Experiments, ordered by reward ÷ risk

Each: **hypothesis · change · attacks · expected · measure · risk**. Benchmark
harness is the existing `scripts/benchmark_ip_bucketed_lowcap.sh` (type-3 shard
2000/4000, np_cap sweep, SM sampling); correctness gate is
`scripts/validate_ip_bucketed_correctness.sh` (accept∪overflow byte-identical to
legacy serial). **Every experiment must pass that gate** — the output point set
(hence IP set) must stay bit-identical.

### Exp. A — Fold block-cooperative enum into the bucketed kernel ★ start here
- **Hypothesis.** The biggest single win is killing the local-mem spill +
  divergence by making one *block* (not thread) process a candidate, with the
  basis in shared memory and the wide x4/x3 frontier split across threads. The
  ingredients already exist: `device_make_points_block` (:1438) +
  `device_make_points_walk_seed` (:1362) already use `__shared__ long long
  basis[5][10]` / `x_upper[10]` (:1474–1475) and atomic point append, and the
  legacy `--block-ip` path measured **3.7× the serial kernel at np 4096**. But
  the *bucketed* kernel (`point_enum_kernel`) still uses the 1-thread/candidate
  `device_make_points_serial`.
- **Change.** Wire the block-cooperative walk into the bucketed point-enum kernel
  (block-per-candidate, shared basis, compact `np_cap` shared/global point
  buffer). Reuse the existing seed-split; keep `--np-cap` overflow semantics.
- **Attacks.** Occupancy (shared basis → no 400 B/thread spill) + divergence
  (threads cooperate on one candidate's subtrees instead of 32 unrelated np's).
- **Expected.** 2–4× over the current bucketed serial (combines the proven 3.7×
  block win with the bucketing already in place).
- **Measure.** np_cap sweep A/B vs current bucketed; watch meanSM and overflow.
- **Risk.** Low-medium: code exists, mostly integration + tuning threads/block.
  Block-per-candidate wastes lanes on light (np≤4) candidates — mitigate by
  routing only the heavier bucket (e.g. np 16–cap) to the block kernel and the
  light bucket to the serial kernel (a 2-tier bucket).

### Exp. B — int32 walk + division intrinsics
- **Hypothesis.** For valid candidates all coords/products fit in 32-bit (with an
  overflow guard). 32-bit emulated division is ~2–4× cheaper than 64-bit on
  `sm_120` *and* halves register/local-mem pressure (stacks with Exp. A's
  occupancy win). ~55% of divisors are ≤16, so a small-divisor fast path
  (or reciprocal-multiply, since the pivot `basis[walk_dim][·]` is constant for a
  whole level sweep) can replace many `pd_floor` calls.
- **Change.** Templatize the walk on coord type; add an `int` path with an
  overflow guard that falls back to the `long long` path. Replace `device_pd_floor`
  with (a) a branchful small-divisor path and/or (b) precomputed per-pivot
  reciprocal-multiply (`__umulhi`/Granlund–Montgomery) where the divisor is loop-
  invariant. Keep bit-exact floor semantics (verify against `pd_floor`).
- **Attacks.** Division cost (the dominant op) + register pressure (occupancy).
- **Expected.** 1.3–2.5× on the walk *if* GPU emulated division is not fully
  latency-hidden (unknown — this is the per-device retest the CPU result flagged).
- **Measure.** A/B int32-vs-int64; isolate the reciprocal change from the width
  change. Capture `ptxas -v` reg/local-mem deltas (add `-Xptxas -v`,
  `--ptxas-options=-v`) — currently not emitted by the build.
- **Risk.** Medium: overflow guard correctness; reciprocal floor edge cases
  (negative numerators — note `pd_floor` handles sign). Must pass bit-exact gate.

### Exp. C — Incremental offset maintenance (shorter dependent chain)
- **Hypothesis.** The walk's true bound (per the CPU finding) is the *serial
  dependent chain of bound computations*, not raw division throughput. The code
  recomputes `Σ_{k>j} x[k]·basis[k][A]` from scratch at every level/coord
  (:1335,1346,1361). Maintaining these partial sums incrementally — update by
  `±basis[k][A]` when a single `x[k]` increments — removes most of the
  multiply-add chain and shortens the latency-critical path feeding each division.
- **Change.** Keep a running `offset[A]` (per ambient coord) updated on each
  `++x[walk_dim]` / level change, instead of the inner `for k` reductions. This is
  the GPU analog of PALP's `lev` recurrences (closed-form per
  POINT_WALK_ALGORITHMS.md §5).
- **Attacks.** The dependent-chain length (the real latency bound) + redundant ALU.
- **Expected.** 1.2–1.8×, and it *compounds* with everything else by shortening
  the per-node critical path (helps divergence recovery too).
- **Measure.** A/B same kernel ± incremental offsets; bit-exact gate.
- **Risk.** Low-medium: pure arithmetic refactor, easy to verify, but fiddly
  index bookkeeping in the flattened DFS.

### Exp. D — LLL-reduced basis + Fincke–Pohst walk on GPU ★ highest upside
- **Hypothesis.** This is the user's strongest instinct and the most GPU-suited
  rethink. The PALP triangular basis has orthogonality defect ~10⁹ (up to 10¹⁷);
  LLL collapses it to ~1.2–2.3, and the FP walk over it visited **74× fewer tree
  nodes / 57× fewer divisions** (proven bit-identical, `LLL_FP_WALK.md`). On the
  CPU this was overhead-bound (per-candidate LLL setup ~10⁴ cyc dominated the
  light bulk → only 1.4–2.1× on the heavy tail). **The GPU changes that calculus
  in three ways:** (1) fewer nodes = far less warp divergence (shorter, more
  *uniform* subtrees — directly attacks the CV-6.5 enemy); (2) fewer emulated
  divisions where division is most expensive; (3) the LLL/GSO setup is
  floating-point Gram–Schmidt + Cholesky — work GPUs are *good* at, and it can be
  done **once per block cooperatively** (block-per-candidate, Exp. A) so the
  setup overhead that sank the CPU is amortized across the block's threads.
- **Change.** Port the proven `lf_*` walk (PALP `Coord.c` `LLLFP_WALK` block) to
  device code: box-metric LLL (`lf_lll_reduce`), circumscribed-ellipsoid root box
  (`lf_fp_enumerate`), exact-integer box propagation, back-convert via U to
  B-coords (bit-identical). Run LLL cooperatively per block; distribute FP subtree
  frontier across threads (composes with Exp. A).
- **Attacks.** Node count *and* divergence *and* division count — all three at once.
- **Expected.** The big one: plausibly the path to the §6 0.5–1 M/s/GPU. Wide
  error bars (FP node-count win is proven; the GPU constant factors are not).
- **Measure.** First a **device node-count prototype** (count FP vs triangular
  tree nodes per candidate on-GPU, no full kernel) to confirm the 74× transfers,
  then a throughput A/B. Bit-exact gate on the final point set.
- **Risk.** High: most code, FP/exact-integer correctness, register pressure of
  the GSO state. De-risk by prototyping node counts before committing to the full
  kernel. This is the experiment most likely to deliver *and* most likely to be
  hard.

### Exp. E — Warp-homogeneous bucketing / candidate sorting
- **Hypothesis.** Even within an `np_cap` bucket, np varies (e.g. 1–16), so warps
  still diverge. Sorting/binning candidates so a warp's 32 lanes have similar
  predicted box volume (the §-noted `Σ bit-length(Xmax)` proxy, free pre-walk)
  makes warps homogeneous and lets the cheap serial kernel run efficiently.
- **Change.** Host- or device-side radix/bucket by the box-volume proxy before
  the IP-filter launch (the bucketed kernel already host-builds an np-sorted index
  list post-enum; do an a-priori volume sort pre-enum).
- **Attacks.** Divergence (homogeneous warps).
- **Expected.** 1.2–1.6× on the serial path; less needed if Exp. A lands (block
  kernel is intrinsically less divergent). Cheap insurance / stacks.
- **Measure.** A/B sorted-vs-unsorted launch order, same kernel.
- **Risk.** Low: reordering only, cannot change correctness; sort cost is tiny vs
  the walk.

### Exp. F — Don't enumerate all points: hull/vertices directly (research)
- **Hypothesis.** IP_Check only needs the convex hull / whether the origin is
  strictly interior — the vertices+facets, a tiny subset of enumerated points.
  For the dim-5 CWS polytope (positive-orthant box ∩ weight hyperplanes) the
  vertices are computable from the weight system, bypassing the walk entirely and
  breaking the "99% in the walk" wall.
- **Attacks.** Everything — eliminates the over-search.
- **Expected.** Potentially >10×, but it changes the algorithm and the
  CPU/GPU output contract (PALP needs points *listed* for the hull). Largest
  payoff, largest rewrite, real correctness-equivalence proof burden.
- **Risk.** Very high / research-grade. Park until A–D are exhausted, but keep in
  view — it is the only path past ~1 M/s/GPU.

---

## 3. Recommended sequence

```
A (block-coop bucketed)  ──►  C (incremental offsets)  ──►  B (int32 + intrinsics)
        │                                                          │
        └──────────────►  D (LLL+FP on GPU, prototype first) ◄─────┘
                                   (E sorting = cheap stack-on at any point)
```

1. **A first** — biggest proven, lowest-risk structural win (shared basis +
   cooperation); unblocks the occupancy ceiling that flatlines every launch-geometry
   sweep today.
2. **C next** — cheap, compounds, shortens the critical path that bounds
   everything.
3. **B** — the division/int32 retest the CPU result explicitly deferred to GPU;
   measure before believing.
4. **D in parallel as a prototype** — node-count prototype early (it's the
   highest-upside and decides whether the whole kernel is worth rewriting); full
   port only if the 74× node win transfers and A/C have raised the occupancy
   floor it will run on.
5. **E** any time as cheap insurance; **F** only as a research track.

**Decision gates.** After A+C: re-run the np_cap sweep; if we're not ≥2× over
today's 264k/106k, the occupancy model is wrong and we re-profile (push for `ncu`
access — `ERR_NVGPUCTRPERM` is admin-gated; real achieved-occupancy numbers would
remove the single biggest measurement blind spot). After D-prototype: if FP node
count on-GPU isn't ≥10× fewer, drop the full FP port and bank A+B+C.

---

## 4. Cross-cutting enablers (do alongside)

- **Emit `ptxas -v`** (`-Xptxas -v`) and `-lineinfo` in the CUDA build so every
  experiment reports register/local-mem/occupancy deltas — right now we infer
  occupancy from block counts (§1.3), which is exactly why the levers are
  uncertain.
- **Pursue `ncu` access.** Achieved warp occupancy + issue-slot utilization are
  the numbers that would turn this plan from "plausible" to "directed."
- **Measure the L40** (half the fleet, currently unmeasured) once a kernel
  improves — the production sizing in `PRODUCTION_RUN_IDEAS.md` depends on it.
- **Keep the bit-exact completeness contract** (`validate_ip_bucketed_correctness.sh`):
  accept@cap ∪ overflow@cap = full IP set, every experiment, no exceptions.

---

## 5. Results log

### Exp A — block-cooperative bucketed enum (branch `gpu-opt-A-block-bucketed`)
Job 66779, n31 (RTX 6000 Blackwell), type-3 shard 2000/4000, emit 300k.
Script `scripts/benchmark_expA_block_bucketed.sh`. **Correctness ✓** (np_cap 64,
non-truncating shard 400M/2: serial vs block accepted sets identical, 105==105;
accepted ∪ overflow identical).

`ptxas -v` (sm_120) — the occupancy story:
| kernel | registers | stack frame (local-mem) | smem |
|---|---|---|---|
| `point_enum_kernel` (serial) | 78 | **4160 B** | 0 |
| `point_enum_block_kernel` (Exp A) | 80 | **3488 B** | 536 B |

Throughput (cand/s), serial vs block at various threads/block:
| np_cap | serial | block th32 | block th64 | block th128 |
|---|---|---|---|---|
| 16 | **256.8k** | 190.6k | 185.2k | 130.3k |
| 32 | **143.5k** | 138.8k | 138.1k | 95.9k |
| 64 | 106.5k | **119.8k** | 107.2k | 76.7k |

**Verdict: mostly neutral-to-negative; one narrow win — 1.13× at np_cap 64 /
32 threads.** Why it underdelivered vs the hypothesized 2–4×:
1. **Spill only partially removed.** Moving the basis to `__shared__` dropped the
   stack frame 4160→3488 B (−16%), *not* to ~0: `device_make_points_walk_seed`
   still carries per-thread `x0[10]` + `xmin/xmax/x[5]` + walk state in local
   memory. The basis was not the only spiller.
2. **The serial bucketed kernel is already SM-saturated** (meanSM ~95–100% in
   *both*) — the bottleneck was never grid coverage, it's per-SM efficiency
   (local-mem latency + divergence *within* the walk). Block-cooperation doesn't
   touch that; it only helps when a candidate has enough top-seeds to split.
3. **Low np_cap loses candidate parallelism.** At np_cap 16 most candidates are
   light (few seeds), so 32 lanes/block sit idle while the serial kernel keeps 32
   *independent* candidates in flight → serial wins 1.35×. Cooperation only pays
   as np grows (np_cap 64, th32: 1.13×).

**Leads (not yet done):** (a) shrink `walk_seed` per-thread state (fold into
Exp C's incremental offsets + Exp B's int32 → less spill = the real occupancy
lever); (b) 2-tier routing — serial kernel for the light bucket, block kernel
only for np≳48 candidates — but that needs np known pre-enum (couples to Exp E).
Kept as an *option for the high-np_cap regime*, not a default. The decisive lever
is reducing per-thread state (B/C), confirming §1.3's "occupancy-bound" model.

### Exp C — incremental offset maintenance (branch `gpu-opt-C-incremental-offset`)
Job 66780, n31, same shard. Replaced the three inner `Σ_{k>walk_dim} x[k]·basis[k][i]`
recomputations in `device_make_points_serial` with a maintained `off[10]`
accumulator (updated `+= x·basis` on descent, `-= x·basis` on ascent).
**Correctness ✓** (serial-C vs *unmodified* block path: accepted 105==105,
accepted ∪ overflow identical — a clean differential test since the block walk
was untouched on this branch).

`ptxas -v` (sm_120): `point_enum_kernel` stack frame **4160 → 4240 B** (+80 =
exactly the `off[10]` array), 78 → 80 registers.

Throughput (cand/s), C-serial vs the Exp-A baseline serial:
| np_cap | baseline serial | C serial | ratio |
|---|---|---|---|
| 16 | 256.8k | 228.7k | **0.89×** |
| 32 | 143.5k | 148.5k | 1.03× |
| 64 | 106.5k | 95.4k | **0.90×** |

**Verdict: neutral-to-negative (≈−10% at np16/64, +3% at np32).** The CPU lesson —
"the walk is bound by the dependent chain, so shorten it" — **does not transfer to
the GPU.** Here the kernel is occupancy-bound: the +80 B of local memory (`off[]`)
lowered occupancy by *more* than the removed multiply-adds saved, because the GPU
already hides that recompute latency across warps. **Decisive finding for the
whole plan: on this kernel, adding *any* per-thread state is a net loss — the only
lever is *reducing* state.** This promotes Exp B (int32 halves the walk's
data footprint) from "measure before believing" to the critical experiment, and
demotes any node-count/chain-length idea that costs memory. C is not merged.

### Exp B — int32 walk + 32-bit division ★ WINNER (branch `gpu-opt-B-int32-div`)
Job 66781, n31, same shard. Converted `device_make_points_serial` entirely to
int32 (narrow the basis + x_upper to `int` after the int64 basis build; walk
arithmetic, bounds and `device_pd_floor32` all 32-bit). Overflow-safe with margin:
max W5 degree 3486 / weight 1743 ⇒ worst bound product ~3e7 ≪ 2.1e9, so **no
guard needed**. **Correctness ✓** (serial-int32 vs *unmodified int64* block path:
accepted 105==105, accepted ∪ overflow identical).

Throughput (cand/s), int32-serial vs the int64 baseline serial:
| np_cap | int64 baseline | **int32** | speedup |
|---|---|---|---|
| 16 | 256.8k | **671.7k** | **2.62×** |
| 32 | 143.5k | **448.5k** | **3.13×** |
| 64 | 106.5k | **302.2k** | **2.84×** |

`ptxas -v` (sm_120): stack frame **4160 → 4272 B** — essentially *unchanged*.

**Verdict: ~2.6–3.1× — the decisive GPU win, and it reframes the bottleneck.**
The footprint did *not* shrink, so the gain is **not** occupancy — it is that
**64-bit emulated integer division/arithmetic was the real bottleneck on sm_120**
(no hardware integer divider), and 32-bit ops are natively far cheaper. This
*confirms* PIPELINE_PROFILING §9.4/§10.2's GPU hypothesis (the CPU reciprocal-
multiply result did NOT transfer because GPU division is emulated, not latency-
hidden) and *resolves* the apparent contradiction with Exp C: C added int64 state
to an int64-division-bound kernel and lost; B removed the int64 *division* and
won, even at a slightly larger frame. **New single-GPU best: 671.7k/s (np_cap 16),
302k/s (np_cap 64)** — ~2.8× the prior bucketed best. This is the kernel to ship.
Follow-ups: (a) int32 the block path's `walk_seed` too (would lift Exp A's
high-np_cap regime); (b) revisit a *zero-added-state* incremental offset on top of
int32; (c) try `__umulhi` reciprocal-multiply for the small constant divisors —
may stack further now that division is the proven lever.
