# CPU & GPU Pipeline Profiling — CWS gen + point-enum + IP check (type 3)

> Structure 3 (two overlapping size-5 weight systems, the "5-5"). No normal-form
> stage. Measured 2026-06-03 on the SLURM cluster. Companion:
> `PIPELINE_CODE_PATHS.md` (what the code does), this doc (how fast it runs).
>
> **Tooling note:** `perf_event_paranoid = 4` and GPU `ncu` returns
> `ERR_NVGPUCTRPERM` — i.e. **OS-level and GPU hardware-counter profiling are
> both admin-gated** (as anticipated). So CPU timing uses an rdtsc hook compiled
> into PALP (`PALP_PROFILE_TIMING/GENONLY`), and GPU timing uses the binary's
> own `clock64` `--ip-stage-profile` counters + `nvidia-smi` sampling + `nsys`
> (which *does* work). Achieved warp-occupancy % (ncu) could not be collected;
> where occupancy is stated it is derived from block counts / SM counts.
>
> Scripts: `scripts/profile_cpu_pipeline.sh`, `scripts/profile_gpu_pipeline.sh`,
> analyzers `scripts/analyze_pipeline_profile.py`, `scripts/analyze_gpu_profile.py`.
> Raw data: `results/pipeline-profile/{cpu,gpu}-structure-3/`.

---

## TL;DR

| | CPU (2× EPYC 9554, 128 cores) | GPU (1× RTX PRO 6000 Blackwell, 96 GB) |
|---|---|---|
| **Generation** (count only) | 23.7 M/s (1 core) · 2.93 G/s (128) | **182 G/s** (scan kernel) |
| **Processing** (gen+point+IP), default | 8.8 k/s (1 core) · ~1.1 M/s (128) | **671 /s** ⚠ (grid capped to 6 blocks) |
| **Processing**, best achievable in current binary | — | **110.8 k/s** (`--block-ip --ip-max-points 4096`) |
| Time in **point enumeration** | **98.9 %** | **98–99.9 %** |
| Time in **IP check** | 1.0 % | 0.1–7.7 % |
| Time in **generation** | 0.04 % | 0.8 % (nsys) |
| Hardware occupancy | **97.6 % busy (full)** | default **~3 % of SMs** ⚠; best config ~full grid |
| Per-candidate cost spread (CV) | **6.5** (max/mean 1544×) | same workload, manifests as warp divergence |

**Headlines**
1. **Point enumeration is ~99 % of the work** on both devices. Everything else
   (candidate generation, IP check) is noise by comparison.
2. **The GPU is severely under-utilised in the default config**: the 2 MB-per-slot
   point buffer (`--ip-max-points 2000000`) caps the launch grid to **6 blocks**
   (~3 % of the 188 SMs) → **671 cand/s**. Freeing it (4096-point buffer, valid
   for type 3 whose max np is ~2033) restores the full grid → **55× (serial)**;
   one-block-per-candidate (`--block-ip`) on top → **165×** to 110.8 k/s.
3. **The CPU is fully occupied and scales linearly** (η = 0.995, 97.6 % busy).
4. **Right now the 128-core CPU node (~1.1 M/s) beats one GPU (~0.11 M/s) by ~10×**
   for this workload — the point walk is serial/branchy, which is GPU-hostile.
   Closing that gap is the optimization opportunity (see §6).

---

## 1. Method & representativeness

- **Engine:** CPU = `PALP/cws-5d.x -c5 -s3`; GPU = `cuda_dim5_cws_scan --structure-id 3`.
- **Representativeness trap:** slot-0 index 1 (lowest-degree W5) is pathological
  (avg np ~80, IP rate ~50 % vs the true ~9 / ~0.9 %), and the enumerator visits
  slot 0 in index order. CPU single-thread stats therefore use **16 spread
  anchors** across the full 1.83 M slot-0 pool (each a full slot-1 sweep). GPU
  uses a **mid-shard** (`--shard-count 4000 --shard-index 2000`).
- **Stage timing** is the binaries' own instrumentation (rdtsc on CPU, clock64
  on GPU), so it is not perturbed by an external sampler.
- **Caveats:** (a) some CPU spread anchors hit a 150 s safety cap, slightly
  truncating their slot-1 tail (mild light-bias). (b) The GPU mid-shard has avg
  np ≈ 16 (higher-degree W5 ⇒ more points than the global mean 8.7), so GPU
  numbers are if anything slightly *conservative*. (c) No ncu ⇒ no true warp
  occupancy.

---

## 2. Where the time goes (per the question "time per step")

Per-candidate wall decomposition, **CPU single thread, representative** (TSC
self-calibrated to 3.094 GHz):

```
full pipeline           113,076 ns / candidate   (= 8,844 cand/s)
  ├─ candidate generation     42 ns   ( 0.04 %)   enumeration + canonicalization
  ├─ point enumeration   111,879 ns   (98.94 %)   Make_CWS_Points (basis + lattice walk)
  └─ IP check              1,155 ns   ( 1.02 %)   IP_Check (GLZ_Start_Simplex + facet loop)
```

GPU, fraction of the IP-filter kernel (clock64), best config `--block-ip
--ip-max-points 4096`:

```
point enumeration  98.3 %      device_make_points_block
IP check            1.7 %      device_ip_check_block
   └─ within IP: GLZ_Start_Simplex ~99 %, everything else <1 %
generation (separate scan kernel): 0.8 % of total GPU time (nsys)
```

> Both devices agree: **point enumeration dominates (~99 %)**; the IP check is
> cheap because 60–72 % of candidates are degenerate (np < 6) and `GLZ_Start_Simplex`
> rejects them almost immediately, while the surviving candidates' IP cost is
> still tiny next to their point walk.

---

## 3. Throughput (CWS generated and processed per second)

### CPU — 2× AMD EPYC 9554 (128 cores @ 3.094 GHz), node n11

| metric | 1 core | 128 cores |
|---|---|---|
| **generation** (count only) | 23.7 M cand/s | 2.93 G cand/s |
| **processing** (gen+point+IP) | **8,844 cand/s** | **~1.1 M cand/s** (representative) … 2.0 M/s (lighter mix) |

- Scaling efficiency η = **0.995** (shard-1 alone 14,643/s vs under 128-way load
  14,564/s) — essentially perfect; the pipeline is independent processes with a
  few-MB working set, no shared state.
- Measured 128-core aggregate (modulo shards): processing 2.03 M/s, generation
  2.93 G/s. The "representative ~1.1 M/s" = single-thread spread-anchor rate ×
  128 × η; the 2.0 M figure reflects the lighter candidate mix the windowed
  modulo run happened to hit. **Take 1.1–2.0 M cand/s** as the 128-core band.

### GPU — 1× RTX PRO 6000 Blackwell (96 GB, 188 SMs, sm_120), node n31

| config | point buffer | blocks | **processing cand/s** | note |
|---|---|---|---|---|
| serial (default) | 2,000,000 | **6** | **671** | grid VRAM-capped; ~3 % of SMs |
| serial | 4,096 | 3,008 | 37,029 | full grid → **55×** |
| `--block-ip` | 2,000,000 | 886 | 39,125 | 1 slot/block ⇒ 886 fit even at 2 MB |
| `--block-ip` | 4,096 | 48,128 | **110,764** | **best; 165× over default** |

- **Generation (count-only scan): 182 G prefix-candidates/s** — never the bottleneck.
- One GPU's best processing rate (110.8 k/s) is **~12.5× one CPU core** but
  **~10× slower than the 128-core CPU node**. Even 4 GPUs/node (~0.44 M/s) lose
  to the CPU node (~1.1 M/s) on the *current* code.

---

## 4. Hardware occupancy ("how busy is the hardware")

- **CPU: fully occupied.** Node CPU busy during the 128-worker run = **97.6 %**;
  η = 0.995. There is no idle to reclaim — only per-core efficiency (§6).
- **GPU: badly under-utilised in the default config, and never compute-saturated.**
  - `nvidia-smi` "utilization" reads **98–100 %** in every run, but that metric
    is "≥1 kernel active", *not* SM occupancy. In the default serial+2M config
    only **6 of 188 SMs** ever hold a block (~3 % of the machine) — the
    "100 % util" is 6 SMs spinning while 182 sit idle. This is exactly the GPU
    idling the project suspected, and it is caused by the VRAM grid cap, not by
    lack of work.
  - **Memory-controller utilisation = 0 %** in all IP runs ⇒ the kernel is
    **not** bandwidth-bound; it is latency/divergence/occupancy bound.
  - `nsys`: the IP kernel is **99.2 %** of GPU time (13.84 s for 500 k serial
    candidates), the scan kernel 0.8 % — confirms the IP filter is the whole show.
  - True warp-occupancy % unavailable (`ncu` → `ERR_NVGPUCTRPERM`, admin-gated).

---

## 5. Per-candidate unpredictability ("how unpredictable per CWS")

Measured on CPU (per-candidate point+IP cost), representative sample of ~20 M:

| | value |
|---|---|
| mean | 349,735 cycles (113 µs) |
| stdev | 2,278,649 cycles |
| **coefficient of variation** | **6.52** |
| min / max | 2,666 / 540,117,154 cycles |
| **max / mean** | **1,544×** |
| cost histogram (log2) | spans 2¹² … 2³⁰ — **>4 orders of magnitude** |

Branch / early-rejection profile (per candidate, representative):

| outcome | CPU | GPU (mid-shard) |
|---|---|---|
| degenerate, np < 6 (IP bails at `GLZ_Start_Simplex`) | **71.5 %** | ~60–66 % |
| reaches full IP search but rejected | ~27.6 % | ~34–48 % |
| **IP accepted** | **0.89 %** | 0.008–0.04 % |
| avg np (lattice points enumerated) | 9.17 | 16.1 |

> The cost is driven almost entirely by **np**, which the earlier profiling
> showed is heavy-tailed (median 4, p99 73, max ~2033 for type 3). 71 % of
> candidates are nearly free; a thin tail of high-np candidates dominates the
> mean (CV 6.5, max/mean 1544×). **On the GPU this variance is the enemy:** in
> the serial kernel one fat candidate stalls its whole warp; `--block-ip`
> (1 candidate/block, 32 threads cooperating on the walk) is faster precisely
> because it removes that intra-warp divergence.

---

## 6. Theoretical maximum throughput (perfect utilisation + perfect code)

> Bounds, with assumptions stated; treat as order-of-magnitude. (No ncu ⇒ the
> GPU compute-utilisation fraction is inferred, not measured.)

### CPU
- Already 97.6 % occupied and η ≈ 1.0, so there is **no parallel-scaling headroom**
  — the practical ceiling is `128 × single-core`.
- Single-core headroom is **micro-architectural only**: the point walk is a
  data-dependent integer loop with poor vectorisation prospects; realistic gains
  from branch-layout / AVX2 partial-vectorisation of the inner accumulation and
  cutting redundant basis work are ~**1.5–2×**.
- **CPU ceiling ≈ 1.7–4 M cand/s** on this 128-core node (from the 1.1–2.0 M/s
  representative band). The algorithm, not the hardware, is the wall.

### GPU
- Generation is effectively free (182 G/s), so the ceiling = IP/point-enum.
- **Bandwidth bound:** N/A — mem controller is at 0 %, so DRAM is not the limit.
- **Compute bound (loose upper bound):** Blackwell INT32 throughput is on the
  order of 10¹⁴ ops/s; at ~3.5×10⁵ integer-ish ops/candidate (≈ the CPU cycle
  count) that is ~**10⁸ cand/s** *if* the walk parallelised perfectly with zero
  divergence — physically unreachable because each lattice-point walk is a serial,
  data-dependent recurrence.
- **Grounded estimate:** the best current kernel (`block-ip`, 4096) hits
  110.8 k/s at a full grid but with heavy intra-block divergence (variable np per
  seed) and only 32 threads/candidate. A divergence-aware, **np-bucketed** kernel
  (uniform np per launch, right-sized point buffers, more lanes per candidate,
  shared-memory point storage) should plausibly reach **0.5–1 M cand/s per GPU**
  — i.e. **~5–10× the current best**, bringing one GPU level with (and 4 GPUs
  well past) the 128-core CPU node. Beyond that needs algorithmic change to the
  walk itself.

---

## 7. Direct implications for the optimization effort

1. **Optimise the point-enumeration walk — nothing else matters** (99 % of time
   on both devices). Generation and IP check are already negligible.
2. **Kill the VRAM grid cap via per-np bucketing.** `--ip-max-points 2000000`
   (PALP `POINT_Nmax`) is catastrophic on GPU (6 blocks). The np distribution
   (median 4, p99.99 ≈ 530, max 2033 for type 3) means a **4096-point buffer is
   already correct and safe for type 3** and gives 55–165×. Bucket candidates by
   np class (16 / 64 / 256 / 4096) so each launch sizes its buffer minimally and
   fills the grid. This is the single highest-value change.
3. **Prefer `--block-ip` (1 candidate/block)** to tame the CV-6.5 divergence; or,
   better, sort/bucket by np so warps are homogeneous and the serial kernel
   regains its candidate-level parallelism.
4. **The CPU is the current production workhorse** (~1.1–2.0 M/s, fully scaled).
   The GPU only wins after (2)+(3); until then, run the CPU nodes in parallel.

---

## 8. Reproduce

```bash
sbatch scripts/profile_cpu_pipeline.sh        # std partition, 128 cores, exclusive
sbatch scripts/profile_gpu_pipeline.sh        # gpu partition, 1× RTX6000BW
# summaries land in results/pipeline-profile/{cpu,gpu}-structure-3/summary.txt
```
Env knobs: CPU `W_SPREAD`, `T_WIN`, `ANCHOR_TMO`, `NCORES`; GPU `SHARDS`,
`SHARD_IDX`, `EMIT_DEFAULT`, `EMIT_SMALL`, `RUN_TMO`.

---

## 9. Inside `Make_CWS_Points` / `point_enum_kernel` — what eats the time

> §2 showed point enumeration is ~99 % of the pipeline. This section opens that
> 99 % up. Measured 2026-06-03 with a dedicated sub-stage profiler:
> `scripts/profile_makepoints_substages.sh` (CPU, rdtsc + op-counts via the
> `MKPTS_PROFILE` build of `Coord.c`) and `scripts/profile_pointenum_gpu.sh`
> (GPU, `clock64` basis-vs-walk split, instrumented `cws_gpu_prof`). Raw:
> `results/aristotle-validation/mkpts/raw.txt`.

### 9.1 `Make_CWS_Points` decomposes into three parts — one dominates

```
Make_CWS_Points(candidate):
  ├─ PROLOGUE  CWS_to_PermCWS + Make_CWS_Basis + Compute_X0 + Amin/Xmax setup
  ├─ WALK      5 nested loops (x4→x3→x2→x1→x0); each node calls CLB(level)
  │              CLB = compute [xmin,xmax] for that level via PD_Floor divisions
  └─ STORE     batch-write the x0 sweep into the point list
```

CPU, 16 spread anchors, 1.50 M candidates (heavy-weighted: avg np 14.5):

| part | share of `Make_CWS_Points` cycles |
|---|---|
| **PROLOGUE** (incl. `Make_CWS_Basis`) | **0.17 %** |
| **WALK** (CLB bounds + loop control) | **99.83 %** |
| **STORE** (point writes) | ⊂ walk, negligible (~14.5 writes/cand) |

**The walk is the whole story; basis construction and point storage are noise.**

### 9.2 Inside the walk: it is almost entirely integer division

Per candidate (same sample):

| quantity | value |
|---|---|
| `PD_Floor` (64-bit integer **divisions**) | **176,068** |
| `CLB` calls (per-level bound computations) | 30,195  (**5.8 divisions / CLB**) |
| loop trips | x4 = 5.9 → x3 = 206 → x2 = 1,062 → **x1 = 28,921** |
| lattice points **produced** | **14.5** |
| effective cost per `PD_Floor` (Zen4) | **~6.2 cycles** → 176 k × 6.2 ≈ 100 % of walk |

So the walk is **≈100 % bound computation, and bound computation is ≈100 %
`PD_Floor` (integer division).** The five-deep loop visits ~29 k innermost
nodes, runs ~176 k divisions to tighten the per-level ranges, and almost every
node is pruned — only ~14.5 survive as points.

### 9.3 The core inefficiency: ~12,000 divisions per point produced

```
divisions per output point = 176,068 / 14.5 ≈ 12,100   (this heavy-weighted sample)
per-anchor range: 177  (low-degree, points-dense)  →  127,251  (high-degree, points-sparse)
```

The enumerator walks a **bounding box that is far larger than the actual point
set** and narrows it with a division at every tree node. For high-degree weight
systems (large `Xmax`) the box dwarfs the ~handful of real points, so it burns
tens of thousands of divisions pruning empty lattice space per point found. The
cost is **searching, not emitting** — `div/point` (not points themselves) is the
work metric.

> Mix note: this sample equal-weights 16 anchors at a fixed candidate cap, so it
> over-weights heavy candidates (avg np 14.5 vs the representative 8.86, and
> 288 k cyc/cand representative — §2/§5). The *structure* (walk ≫ prologue;
> division-dominated; thousands of div/point) holds across every anchor
> (walk share 94.8–99.96 %); only the absolute magnitudes scale with np.

### 9.4 GPU `point_enum_kernel`: same shape, division is even costlier

`scripts/profile_pointenum_gpu.sh` (n31, mid shard, `--ip-bucketed --np-cap 64`):

* `point_cycles` = **99.9 %** of the IP-filter kernel (clean `clock64` stage split);
  IP check 0.1 %.
* basis-vs-walk inside the point kernel: **basis ≈ 0 %, walk ≈ 100 %**
  (same as CPU; absolute cycle counts are atomic-contention-distorted so only the
  ratio is cited), avg points/cand ≈ 15.6 — the op-counts of §9.2 transfer
  unchanged (identical deterministic algorithm).
* **Each `PD_Floor` is far more expensive on the GPU.** sm_120 has *no hardware
  integer divider* — 64-bit signed division is emulated as a multi-instruction
  sequence, where the CPU spends ~6 cycles on a hardware `idiv`. So the same
  ~176 k divisions/candidate cost proportionally *more* of the GPU's budget.
* The basis (`basis[5][10]`, the 4,160-byte stack frame → local memory) is
  re-read in the inner loop; memory-controller util is 0 % (§4) so these hit
  L1/L2 — it is **division latency + local-mem latency + warp divergence**, not
  DRAM bandwidth.

**Headline:** on both devices the point walk is dominated by **integer division
inside per-level bound tightening**, run ~12 k× per point because the search box
≫ the point set. Everything else (basis build, point storage, IP check) is
negligible. Optimisation must attack *the divisions and the over-search* — see
§10.

---

## 10. Is the walk parallelizable beyond the outer loop? Better algorithms?

### 10.1 The walk is a wide, data-dependent DFS tree

`x4 → x3 → x2 → x1 → x0`: each level's range `[xmin_j,xmax_j]` depends on the
outer levels' chosen values (offset `Σ_{k>j} x_k·B[k][A]`). So a **root-to-leaf
path is sequential**, but **sibling subtrees are independent** and the tree is
wide (≈6 → 206 → 1,062 → 28,921 nodes by level). That width — not just the outer
candidate loop — is exploitable parallelism.

### 10.2 CPU / AVX

* **AVX cannot do integer division** (no vector `idiv` in AVX2/AVX-512). You
  cannot SIMD `PD_Floor` directly — which is exactly the hot op.
* **But the divisors repeat massively**: the pivot `R = B[j][A]` is constant for
  an entire level sweep. Replacing `PD_Floor` with a **precomputed
  reciprocal-multiply** (libdivide-style `mulhi`+shift) turns each division into
  a multiply+shift — *and that is vectorizable*. This is the single biggest CPU
  lever (helps even scalar, because of divisor reuse): est. **~2–4× on the walk**.
* SIMD width is then usable across the ≤10 ambient coords in `CLB`, or across
  sibling `x` values, once division is multiply-based. Realistic combined CPU
  gain **~1.5–3×** beyond the current scalar code; **int32** arithmetic (coords
  and products fit in 32-bit for valid candidates, with an overflow guard) stacks
  on top (smaller data, cheaper multiply, better ILP).
* Aristotle-1's restructuring already captured branch/accumulator gains (~1.2–1.3×);
  reciprocal-multiply + int32 are the *next* and larger CPU steps.

### 10.3 GPU / whole-block

* **Block-cooperative enumeration is the right model and is already partly
  proven**: the legacy `--block-ip` kernel (1 candidate/block, 32 lanes split the
  top-level seed range, atomic-append points) ran **3.7× the serial kernel at
  np 4096**. Generalize it: split the wide x4/x3 frontier across a whole block
  (256–1024 threads), each lane walking an independent subtree.
* Two structural wins fall out:
  1. **Basis in shared memory** (built once per block) eliminates the
     ~4 KB/thread local-mem spill → big occupancy gain (the §-confirmed
     occupancy/latency bound). Today every thread carries its own `basis[5][10]`.
  2. **Dynamic load balancing** (work queue / persistent threads pulling
     subtrees) handles the 177→127,251 div/point imbalance that static
     seed-splitting suffers.
* **int32 coordinates** are even more valuable on the GPU: 32-bit emulated
  division is ~2–4× cheaper than 64-bit *and* halves register/local-mem pressure
  (the §-noted 78→occupancy bottleneck; adding state regressed throughput 4–10 %,
  so the lever is *less* per-thread state). Stacking block-cooperative + shared
  basis + int32 is the path to the §6 estimate of **0.5–1 M cand/s/GPU**.

### 10.4 A more efficient algorithm?

The current walk is a **Fincke–Pohst-style lattice-point enumeration** (tighten
per-coordinate bounds with division, DFS, prune). Levers, in increasing depth:

1. **Cheaper division primitive** (reciprocal-multiply + int32) — same algorithm,
   2–4× on the hot op. Lowest risk, both devices.
2. **LLL-reduce the basis before enumerating.** The 177→127,251 div/point spread
   is a symptom of a *skewed* bounding box (box ≫ point set). An LLL/size-reduced
   basis makes the box tighter → far fewer pruned nodes → fewer divisions per
   point. Potentially **orders of magnitude** on the heavy (high-degree) tail
   that dominates the mean. This is the highest-upside algorithmic change.
3. **Don't enumerate all lattice points at all.** The IP check only needs the
   convex hull / whether the origin is strictly interior — a property of the
   polytope's *vertices and facets*, a tiny subset of the enumerated points. For
   the dim-5 CWS polytope (positive-orthant box ∩ weight hyperplanes) the
   vertices are computable directly from the weight system; emitting only the
   hull would bypass the point walk — the deepest rethink, the largest payoff,
   and the one that breaks the "~99 % in the walk" wall entirely.

> Counting interior points (Barvinok) does **not** apply — PALP needs the points
> *listed* for the hull, not just counted. The win is fewer points to list
> (LLL/tighter box) or listing only the hull (vertex enumeration), plus a cheaper
> division primitive for whatever enumeration remains.
