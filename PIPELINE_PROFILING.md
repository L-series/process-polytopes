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
