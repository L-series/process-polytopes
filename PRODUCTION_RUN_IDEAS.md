# Production Run Ideas & Sizing — dim-5 CWS classification

> Working notes for the full Stage-1 production run (CWS generation → point
> enumeration → IP check) over **all 46 dim-5 overlap structures**. Captures the
> exact problem size, measured per-device throughput, and the CPU/GPU hybrid
> scheduling strategy with honest wall-clock estimates. Companion docs:
> `PIPELINE_CODE_PATHS.md` (full flow), `PIPELINE_PROFILING.md` (per-stage
> profiling), `LLL_FP_WALK.md` / `POINT_WALK_ALGORITHMS.md` (CPU walk opt),
> memory `project_gpu_ip_bucketing` (the `--ip-bucketed` kernel).

Date: 2026-06-04. Cluster: SLURM, partitions `std` (n11–13, 2×EPYC 9554,
128 cores @3.094 GHz each) and `gpu` (n31/32 = 4×RTX 6000 Blackwell each;
n21/22 = 4×L40 each).

---

## 1. Exact problem size (measured)

Count-only GPU pass over all 46 structures (`scripts/count_all_cws.sh`,
job 66766; sums the `prefix_candidates` column = #CWS that actually reach point
enumeration, after shared-prefix permutation expansion):

| set | candidates | share |
|---|---|---|
| **TOTAL** | **12,140,535,288,504  (12.14 T)** | 100% |
| type-3 (s3, two size-5 / shared-3) | 10,046,036,135,619  (10.05 T) | **82.7%** |
| s12 + s13 (nw=3) | 0.988 T each = 1.98 T | 16.3% |
| all remaining (43 structures) | ~0.12 T | ~1% |

By arity: nw=2 = 10.05 T (≈ all s3); nw=3 = 2.09 T; nw=4 = 661 M; nw=5 = 527.
**type-3 + the nw=3 family ≈ 100% of the work.** Any production plan is, to first
order, a plan for type-3.

### Point-count (np) distribution — sets the GPU/CPU split

From `results/point-count-profile/structure-{3,12}/summary.txt` (use struct-3 for
type-3, struct-12 for the nw=3 rest, which s12/s13 dominate):

| | mean np | np ≤ 15 | np ≤ 31 | np ≤ 63 | max np |
|---|---|---|---|---|---|
| type-3 | 8.69 | 88.4% cand / ~48% Σnp-work | 96.1% / 66% | 98.7% / 80% | 2033 |
| struct-12 | 4.76 | 95.6% / 73% | 99.0% / ~90% | 99.8% / ~96% | 682 |

Key fact: **np is a heavy-tailed work distribution.** A small fraction of
candidates (the high-np tail) carries a large fraction of the *compute* (walk
cost grows super-linearly in np — box volume in 5-D, see PIPELINE_PROFILING §10).
So a split by point count is candidate-cheap but work-expensive on the tail.

---

## 2. Measured per-device throughput

### CPU (PALP `cws-5d.x`, point-enum bound)
- 8.8k cand/s/core; ~1.1 M/s per 128-core node (97.6% core-saturated, near-linear
  scaling). 3-node `std` cluster = **384 cores ≈ 3.38 M/s** (conservative);
  up to ~2.0 M/s/node = 6.0 M/s optimistic.
- LLL+FP walk (`-DLLLFP_WALK`) gives **1.4–2.1× on the type-3 heavy tail**
  specifically — i.e. exactly the candidates the GPU offloads. Overhead-bound
  (<1×) on light candidates, so apply it *only* to the offloaded heavy stream.

### GPU (`cuda_dim5_cws_scan --ip-bucketed`, RTX 6000 Blackwell)
Measured on a type-3 shard, job 66775 (`scripts/benchmark_ip_bucketed_lowcap.sh`).
The bucketed kernel enumerates every candidate up to `np_cap` points (compact
buffer → full grid, no VRAM cap) and ships np>np_cap candidates to
`--overflow-output` for CPU completion:

| np_cap | cand/s (1 GPU) | overflow→CPU (this shard) | meanSM |
|---|---|---|---|
| 16 | 264k | 28% | 100% |
| 24 | 198k | 17% | 91% |
| 32 | 161k | 11% | 74% |
| 48 | 118k | 6.4% | 74% |
| 64 | 106k | 4.4% | 80% |
| 96 | 80k | 2.0% | 85% |
| 128 | 71k | 1.1% | 85% |

Legacy fused baselines: serial 29k/s, block-ip 108k/s.
**Thread (64/128/256) and block (8–64×SM) sweeps are FLAT** → occupancy-bound
(register/local-mem spill ~4 KB/thread), not coverage-bound. No more throughput
from launch geometry.

> **Reality check:** one Blackwell GPU ≈ **17–40 CPU cores** depending on np_cap.
> The lattice point-walk is fundamentally GPU-hostile (serial, branchy,
> divergent). The full 16-GPU fleet ≈ **2–3.5 of the CPU nodes**, NOT a 10×
> accelerator. (Earlier informal "1–2 week hybrid" estimate was over-optimistic
> by ~2× and is retracted.)

`np_cap` is the **GPU-throughput ↔ CPU-offload knob**: lower cap = faster GPU but
more (and heavier) candidates dumped on the CPU.

> **UPDATE 2026-06-04 — int32 walk (GPU_OPTIMIZATION_PLAN.md Exp B) lifts these
> ~2.8×.** Converting the bucketed point-enum kernel to 32-bit (bit-exact) gives,
> per single Blackwell GPU: np_cap 16 → **671.7k/s** (was 264k), 32 → **448.5k/s**,
> 64 → **302.2k/s** (was 106k). 64-bit emulated division was the real sm_120
> bottleneck. Re-running the §3 hybrid model with these rates roughly **halves the
> GPU-arm and combined wall-clock** (e.g. the ~27-day np_cap≈32 central case →
> ~14–16 days), and makes the GPU fleet a genuine co-engine rather than a sidecar.
> Numbers below are the pre-int32 figures; treat them as conservative.

> **UPDATE 2026-06-04 — LLL+Fincke–Pohst GPU walk (Exp G) is the game-changer:
> ~13× over int32+vol-sort, ~52× over the original.** `--fp-walk` replaces the
> triangular point walk with an LLL-reduced basis + Fincke–Pohst enumeration
> (FP32, bit-exact: accepted 105==105, overflow 2955==2955). Per single Blackwell
> GPU, IP-filter rate: np_cap 16 → **7.37M/s**, 64 → **5.94M/s**, 128 → 5.76M/s,
> 256 → **5.36M/s**. **Audited for the CPU §6.2 heavy-bias trap and clear** — the
> 8–14× holds across the heaviness spectrum (avg_points 3.4–30; real type-3 mean
> ≈8.7), and a gate sweep shows light candidates add *zero* extra enum cycles
> (FP is neutral on light here, not a loss as on CPU). **Two production
> consequences:** (a) the GPU can run **np_cap 256 at 5.36M/s with only ~0.1%
> overflow** → the CPU hand-off is nearly eliminated and the GPU fleet alone can
> carry the run; (b) at ~5.5M/s/GPU the IP filter is no longer the bottleneck —
> CWS **generation** (unmeasured) is the new ceiling and the next thing to profile.
> **Revised single-GPU planning rate: ~5.5M cand/s** (range 4.5–6.3M by region),
> up from 0.30M (int32) / 0.106M (int64). Fleet & wall-clock in §3 are recomputed
> below with this rate; the int32/pre-int32 tables are now ~18× conservative.

GPU fleet (effective): 8× Blackwell + 8× L40. L40 unmeasured here; estimate
0.7–0.8× Blackwell (142 vs 188 SM, similar register pressure) ⇒ fleet ≈
**12–14 Blackwell-equivalent GPUs**. *(TODO: measure L40 rate to firm this up.)*

---

## 3. The hybrid strategy (GPU-light / CPU-heavy)

**Idea (this is already built — `--ip-bucketed --np-cap N --overflow-output`):**
1. GPU fleet runs the bucketed kernel with a *low* `np_cap` so the light bulk
   (the ≥96% of candidates with small np) runs at maximum occupancy.
2. Candidates that exceed `np_cap` (the divergent heavy tail that wrecks SIMT
   efficiency) are streamed to the CPU cluster, which finishes them with PALP
   (+ LLL+FP, which wins precisely on these).
3. CPU cluster also chews its own disjoint shards concurrently.

### Why np>15 specifically is the wrong threshold
At `np_cap = 15`, the CPU receives only ~10% of *candidates* but ~50% of the
*compute* (the super-linear tail) — it would hand the smaller compute resource
(384 cores) the larger half of the work and become the long pole. **Raise the
cap.** `np_cap ≈ 32` keeps the CPU at ~4–11% of candidates / a manageable work
share while the GPU still runs fast; the workspace (32×5×8 = 1.3 KB/candidate) is
trivially full-grid.

### Combined-throughput model (disjoint shards + GPU-overflow→CPU)
Treating GPU and CPU as processing independent shard sets, with the CPU also
completing the GPU's overflow stream (heavy candidates cost CPU ~5× a mean
candidate; using full-distribution overflow fractions, 13.6 GPU-equiv, CPU
3.38 M/s):

| np_cap | GPU fleet | overflow frac | CPU diverted to overflow | combined rate | wall-clock (12.14 T) |
|---|---|---|---|---|---|
| 16 | 3.6 M/s | ~11% | ~50% of CPU | ~5.0 M/s | ~28 d |
| **32** | **2.2 M/s** | **~3.9%** | **~12% of CPU** | **~5.1 M/s** | **~27 d** |
| 64 | 1.4 M/s | ~1.3% | ~3% of CPU | ~4.7 M/s | ~30 d |

The optimum is **broad and flat at np_cap ≈ 24–32, ~27–28 days**, combined
~5 M/s. Lowering the cap speeds the GPU but the extra CPU diverted to overflow
cancels the gain.

### Honest wall-clock summary

| configuration | wall-clock |
|---|---|
| CPU-only, 3 nodes (conservative / optimistic) | ~42 d / ~24 d |
| GPU fleet only (np_cap 16) | ~40–45 d (≈ the CPU cluster; not faster alone) |
| **Hybrid, both concurrent, np_cap ≈ 32 (central)** | **~27–28 d** |
| Hybrid + LLL+FP on overflow + optimistic CPU + lighter avg shards | **~18–20 d** |

Bottom line (pre-Exp-G): the hybrid was a real **~1.5–2.3× over CPU-only**,
landing around **3–4 weeks** — *not* an order-of-magnitude win, because
per-candidate the GPU was only worth a couple dozen CPU cores.

### Honest wall-clock summary — Exp G (LLL+FP GPU walk) era

With `--fp-walk` the per-GPU IP-filter rate is **~5.5M cand/s** (np_cap 64–256),
so one Blackwell GPU is now worth **~600+ CPU cores** — the GPU stops being a
sidecar and becomes the primary engine, with the CPU offload (~0.1% at np_cap 256)
nearly gone. Classifying the full **12.14 T** CWS, **GPU-IP-filter-bound**:

| GPU fleet (IP-filter @ ~5.5M/s each) | aggregate | wall-clock (12.14 T) |
|---|---|---|
Cluster GPU inventory: **8× RTX6000BW (n31+n32, 4/node) + 8× L40 (n21+n22, 4/node)
= 16 GPUs total.** (Not 16+16 — corrected 2026-06-05.)

| GPU fleet (IP-filter @ FP rate) | aggregate | wall-clock (12.14 T) |
|---|---|---|
| 8 RTX6000BW only (n31+n32) @ ~5.36M | ~43 M/s | **~3.0 d** |
| 8 RTX6000BW + 8 L40 (L40 **measured 0.92×**) | ~82 M/s | **~1.7 d** |
| + 256 CPU cores on independent shards | ~85 M/s | **~1.65 d** |

**L40 rate measured (job 66806, n22):** FP np_cap 64 = **5.49M/s** (RTX6000BW
5.94M) → **0.92×** — far above the old 0.7× guess, because the FP walk is FP32-heavy
and the L40 (Ada, strong FP32) nearly matches Blackwell on it. (On the *old*
triangular walk the L40 was 0.72× — FP closes the gap.) So the L40 half of the
fleet pulls its weight: at np_cap 256, 8×5.36M + 8×~4.9M ≈ **82 M/s**.

Type-3 alone (10.05 T) is ~0.83× of these (~1.4 d full fleet). **Caveats:** (1)
these are the **IP-filter** rate (the stage FP accelerates); the full
generate→filter pipeline may now be **CWS-generation-bound** — measure generation
before treating ~2 days as firm. (2)
Numbers use the representative ~5.36M/s @ np_cap 256; light/empty regions vary
4.5–6.3M/s.

Bottom line (Exp G): the FP walk converts the GPU arm from "couple-dozen-cores
sidecar" into a 16-GPU fleet that classifies all 12.14 T in **~1.85 days
(IP-filter-bound, full fleet)** / ~3 d Blackwell-only — vs ~3–4 weeks for the
int32 hybrid — pending the CWS-generation-rate check that now sets the real ceiling.

---

## 4. Risks / things to nail down before committing

1. **Overflow hand-off I/O.** At np_cap=32, ~3.9% of 12.14 T ≈ **0.47 T rows**
   spill to CPU; at np_cap=16 it's ~1.3 T. As flat `--overflow-output` files that
   is multiple–tens of TB. Needs a **streamed/sharded producer→consumer** queue
   (GPU writes sharded overflow, CPU shards pull), not one file. This is the
   single biggest engineering gap in the current `--ip-bucketed` plumbing.
2. **Shard weight variance.** The benchmark shard (2000/4000) overflowed ~28% at
   np_cap=16 vs ~11% from the full distribution → it's a heavy shard, so the
   measured GPU rates are likely a **lower bound**; average shards run faster.
   Confirm with a multi-shard sweep before trusting the absolute days.
3. **L40 rate unmeasured.** Half the GPU fleet is L40; the 12–14 GPU-equiv
   assumes 0.7–0.8×. Measure it (`--ip-bucketed` on `--gres=gpu:L40:1`).
4. **CPU-overflow cost factor.** Assumed heavy candidates cost CPU ~5× a mean
   candidate; this drives the balance. Measure the actual cost of completing a
   np>32 candidate stream (with LLL+FP) on PALP.
5. **Determinism / completeness.** `--ip-bucketed` is bit-verified
   (project_gpu_ip_bucketing): accept@cap ∪ overflow@cap = full IP set, run-to-run
   deterministic. Preserve this — never use `--emit-capacity` truncation for the
   real run; shard so each shard's generated count is processed in full. (Solved
   for full ranges by `--stream-ip --ip-bucketed`, the in-process chunk loop
   `stream_descriptor_ip_bucketed`, validated chunk-count-invariant on s24.)

## 4a. TODO (open work items, found during the 43-type run, job 66815)

1. **Load-balance the sharding.** Contiguous range-sharding (`--shard-count N
   --shard-index i` splitting the selection-product range into N equal *position*
   slices) is **badly unbalanced**: candidate *density* varies wildly along the
   range. In the 43-type run, shard 0 drew the sparse end of the big structures
   and finished all 43 in ~51 min, while shards 2–4 drew the dense end (~6.9B each
   of s27 vs near-empty for shard 0) and ran much longer — and shard 0's GPU sat
   idle after. **Fix before the 12.14 T run:** interleaved sharding (assign by
   `selection_index mod N`) or a **dynamic work queue** (small work units pulled by
   whichever GPU is free) so all GPUs finish together and none strand. This is the
   single biggest efficiency loss observed in a real multi-GPU run.
2. **int32 overflow guard for the FP (and triangular) walk.** The int32/FP walks
   have **no runtime overflow detection** — safety rests only on an a-priori bound
   (~3e7 ≪ 2.1e9) that holds for the current W5 pool but isn't checked. Add the
   O(1) **pre-flight guard** (discussed 2026-06-05): after the basis is built,
   compute `worst = (basis_dim+1)·max(x_upper)·max|basis entry|` in int64; if
   `worst ≥ 2^30`, route the candidate to the int64 path / CPU overflow instead of
   silently wrapping (Check A). For FP add **Check B** after the ellipsoid root box
   is known: `Σ|bred|·max|Lo,Hi| + max(x_upper) ≥ 2^30` → fall back. Converts a
   possible silent wrong answer into a safe deferral; needed if the input domain
   ever widens (larger pool / new structure family). Pairs with the certification
   pass (compute global `max|basis entry|` to *prove* no type-3 candidate overflows).

---

## 5. The only paths to a *dramatic* (>3×) speedup — all unbuilt

The hybrid above is bounded by the GPU's per-candidate hostility. To break it you
need a better *algorithm*, not better scheduling:

- **Block-cooperative point-enum on GPU.** One block per candidate, threads split
  the top-coordinate seed range (the existing `device_make_points_block` /
  `--block-ip` is already 3.7× the serial walk at np 4096). Folding block-cooperation
  into the *bucketed* kernel could lift the occupancy ceiling that flatlines the
  current 1-thread/candidate design.
- **Skip full enumeration.** IP_Check only needs the **vertices/hull**, not every
  lattice point. Computing the convex hull directly (or the facet equations from
  the weight system) would sidestep the over-search entirely (~12,000 divisions
  per point produced today — most pruned). Largest potential win; largest rewrite.
- **int32 GPU walk + reciprocal-multiply.** Division is *emulated* on GPU (unlike
  Zen4 where libdivide was 19% slower, see project_point_walk_algorithms), so the
  74× node / 57× division reduction from LLL+FP may actually pay on GPU — retest
  the LLL+FP walk on-device (it's CPU-only today).

---

## 6. Reproduce

- Total count: `sbatch scripts/count_all_cws.sh` → `logs/slurm/count-cws-*.out`.
- GPU np_cap sweep: `sbatch scripts/benchmark_ip_bucketed_lowcap.sh` (or the
  committed `benchmark_ip_bucketed.sh` for caps 32–128).
- CPU rates: `scripts/profile_cpu_pipeline.sh`; LLL+FP: `benchmark_lllfp_*.sh`.
- Always `export PALP_W5_POOL=results/cache/w5.ip` (else +1 min/run regenerating
  the 184026 size-5 weights). Always submit via `sbatch` (never head-node/srun).
