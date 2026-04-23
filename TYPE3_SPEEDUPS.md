# Type 3 `(5,5)` Speedups and Limits

This note captures the implemented dim-5 CWS speedups, the current scaling
limits for builtin structure `-c5 -s3`, and the non-implemented ideas that are
worth revisiting later.

## Implemented speedups

- The old dim-5 canonical path spent most of its time in tempfile and allocator
  churn.
- The current path keeps dim-5 weight pools in memory, caches shared-prefix
  selections by simplex size and overlap count, and reuses `PRINT_CWS`
  workspaces.
- Representative measured gains from `BENCHMARKS.md`:
  - structure 24: `63.98s -> 25.29s` on one worker (`2.53x`)
  - structure 24 with two shards: `25.29s -> 12.28s/12.71s` (`~2.0x` over the
    optimized single-worker run)
  - longer canonical structure runs: about `1.8x` to `2.5x` faster on one worker

## Type 3 `(5,5)` specifics

- Paper type id `3` is builtin structure `-c5 -s3`.
- Descriptor summary:
  - two size-5 simplex slots
  - overlap count `3` on each slot
  - ambient coordinate count `7`
- The current implementation shards only the slot-0 selected-weight index via
  `-j# -k#`.
- Each worker still rebuilds the builtin size-5 base pool before the real pair
  search begins.

## Current bottlenecks

1. Rebuilding the size-5 base pool in every worker.
2. Building the size-5 / shared-count-3 selection cache before enumeration.
3. Quadratic pair search over selected slot-0 and slot-1 candidates.
4. Shared-prefix permutation search for each surviving pair.
5. Per-candidate geometry:
   - `Make_CWS_Points`
   - `IP_Check`
   - `Make_Dual_Poly`
   - second `IP_Check` on the dual as an assertion
6. Output volume once many valid CWS start being emitted.

## Non-implemented speedup ideas discussed

### 1. Reuse the published size-5 single-weight list

- The current code regenerates the size-5 pool by calling `Make_34_Weights(4, 0,
  0)` and piping the output through a tempfile.
- Since the published Kreuzer-Skarke `A_5 = 184,026` list is fixed, we can avoid
  recomputing it every worker.
- Possible forms:
  - embed the list as a static asset in the binary
  - ship it as a checked-in data file and load it once
  - generate it once per machine and cache it on disk
  - move to a single multithreaded process with one shared read-only pool
- Expected benefit:
  - reduces duplicated startup cost per worker
  - improves multi-worker scaling
  - does not change the steady-state quadratic search or the downstream PALP
    geometry cost

### 2. Reuse selection caches globally

- For structure 3, both slots use the same size-5 / overlap-3 selection cache.
- Today each process rebuilds that cache locally.
- Sharing it across workers would save startup time and memory traffic.

### 3. Use many logical shards, not just `nproc` shards

- Current sharding already partitions slot-0 work.
- It is legal to choose many more shards than cores and schedule them
  dynamically across machines.
- This helps with long-tail imbalance when some slot-0 seeds produce more valid
  completions than others.

### 4. Stronger automorphism canonicalization before `PRINT_CWS`

- Current pruning is local:
  - same-family slot order canonicalization
  - shared-prefix permutation canonicalization
- A stronger whole-assignment automorphism check may reject additional duplicate
  states before expensive geometry.

### 5. Early feasibility precheck before full point construction

- Extract a lightweight precheck from the early `Make_CWS_Points` logic:
  basis, `X0`, and bound consistency.
- If a candidate fails there, skip full point enumeration and `IP_Check`.

### 6. Arithmetic necessary conditions for IP/reflexivity

- If a mathematically sound necessary condition can be found for the embedded
  `(5,5)` candidate, it could reject states before geometry.
- High upside, but dangerous unless proved correct.

### 7. Skip downstream work when full metadata is not needed

- `PRINT_CWS` currently computes the dual and prints `M/N` or `F/N` metadata for
  every accepted candidate.
- If a future workflow only needs counts or raw CWS lines, this extra geometry
  work could be made optional.

### 8. Batch writes for candidate streaming

- If the goal is only to materialize a raw candidate stream, per-candidate
  `fprintf` calls are avoidable overhead.
- Each worker can accumulate candidate lines in a large in-memory buffer and
  flush in batches to a per-worker file.
- Final outputs can be concatenated or merged afterward.
- This does not reduce the candidate count, but it can substantially reduce
  syscall overhead and make the pure-streaming path more nearly I/O-limited.
- A binary spill format (e.g. indices plus compact permutation tags) would cut
  output volume even further and defer text formatting to a later pass.

## Practical scaling guidance

- Replicating the raw size-5 pool per worker is cheap enough in memory.
- Replicating the full search and geometry stack per worker is the real cost.
- More workers should still help, but scaling eventually runs into:
  - duplicated startup work
  - cache / memory-bandwidth contention
  - output bandwidth
  - load imbalance across slot-0 seeds

## Highest-confidence next improvements

1. Stop regenerating the size-5 base pool in every worker.
2. Stop rebuilding the size-5 / overlap-3 selection cache in every worker.
3. Use more logical shards than physical cores and schedule them dynamically.
4. Explore stronger duplicate-pruning before `PRINT_CWS`.

## Ranked next bets

There are really two rankings worth keeping separate.

Raw upside if everything works:

1. Stronger safe pruning before [PALP/cws.c](PALP/cws.c#L2894).
2. A successful GPU offload of the seed/tighten kernel in [PALP/Coord.c](PALP/Coord.c#L1128).
3. More CPU-only refactoring inside `Make_CWS_Points`.

Probability-weighted next action:

1. More CPU work inside [PALP/Coord.c](PALP/Coord.c#L1128).
  Expected upside: another `1.3x` to `2.0x` on the hot routine, likely `1.2x` to `1.6x` end-to-end on the emitted-candidate front end.
  Why: the measured hotspot is already isolated, and the code is branch-heavy integer arithmetic that still has some structure left to simplify.
2. Stronger pre-geometry pruning before [PALP/cws.c](PALP/cws.c#L2894).
  Expected upside: potentially larger than CPU refactoring if a sound rejector exists, because it cuts the candidate count itself.
  Why not first: the mathematical risk is higher, and the last few descriptor-level shortcuts have not paid off.
3. GPU work, but only after a standalone kernel benchmark.
  Expected upside: likely `1.5x` to `3.0x` on the seed/tighten kernel if divergence is tolerable, but materially less on the whole PALP path unless more of the front end moves with it.
  Why third: the hot loop is not a dense numeric kernel, so the uncertainty is much higher than for more CPU cleanup.

For multi-machine runs, there is a separate engineering track with high confidence but smaller per-candidate upside:

- use many more logical shards than cores and schedule them dynamically across machines
- stop rebuilding the shared size-5 pool and overlap-3 selection cache in every worker process

Those cluster-facing changes do not alter the per-candidate math, but they should reduce wall time on a 256-core fleet more reliably than a speculative GPU port.

## CUDA Probe Harness

To test the GPU idea without porting PALP itself, the repo now contains a small standalone microbenchmark of the seed/tighten logic from [PALP/Coord.c](PALP/Coord.c#L1128):

- CPU reference generator and benchmark: [src/verify/harness_type3_cuda_bounds.cpp](src/verify/harness_type3_cuda_bounds.cpp)
- shared kernel model: [src/verify/type3_bounds_bench_common.h](src/verify/type3_bounds_bench_common.h)
- CUDA kernel benchmark: [src/verify/harness_type3_cuda_bounds.cu](src/verify/harness_type3_cuda_bounds.cu)
- runner script: [scripts/benchmark_type3_cuda.sh](scripts/benchmark_type3_cuda.sh)
- real-job export and replay script: [scripts/benchmark_type3_cuda_real.sh](scripts/benchmark_type3_cuda_real.sh)

What it does:

- generates a synthetic batch of independent seed/tighten jobs
- biases the divisor mix toward the measured large-shard type-3 `-T` histograms
- can export real seed/tighten jobs from the timing-enabled [PALP/Coord.c](PALP/Coord.c#L1128) path via `PALP_TYPE3_BOUNDS_EXPORT`
- stores self-validating records, so both CPU and GPU replay fail if any replayed result differs from the exported PALP result
- reports CPU reference throughput now
- reports CUDA kernel throughput later on any machine with `nvcc` and a visible GPU
- can sweep block sizes in one run

What it does not do yet:

- it does not include host-device transfer cost in the GPU timing
- it does not model later `IP_Check` or dual-polytope work
- it does not yet splice GPU replay back into `PRINT_CWS`; it is still a narrow replay/offload prototype

That is deliberate. The question here is only whether the hot bound kernel itself behaves like something worth moving to SIMT. If the kernel-only result is not compelling, a full CUDA port is almost certainly not worth the effort.

## Current GPU Replay Results

Synthetic replay on the local GTX 1060 6 GB with CUDA 12.6 and exact CPU/GPU parity checks:

- synthetic dataset: `200,000` jobs
- CPU reference: about `61.2M` jobs/s
- GPU replay:
  - block `128`: about `627.7M` jobs/s
  - block `256`: about `609.4M` jobs/s
  - block `512`: about `583.3M` jobs/s
- best synthetic block size on this card: `128`
- validation: `cpu_validation mismatches=0`, `gpu_validation mismatches=0`

Real-job replay from the actual `./cws.x -c5 -T -I -n2 ... -s3` path on `wf4-d1-20`, shard `-j32 -k1`, exported from [PALP/Coord.c](PALP/Coord.c#L1128):

- exported records: `200,000`
- observed real-job mix:
  - `61.8%` tighten-empty
  - `6.2%` zero-fail
  - `32.0%` survive
  - average `2.146` steps/job
- CPU reference: about `172.8M` jobs/s
- GPU replay:
  - block `64`: about `575.3M` jobs/s
  - block `128`: about `594.4M` jobs/s
  - block `256`: about `594.4M` jobs/s
  - block `512`: about `592.5M` jobs/s
- best real-job block sizes on this card: effectively `128` and `256`
- measured real-job GPU/CPU speedup: about `3.44x`
- validation: `cpu_validation mismatches=0`, `gpu_validation mismatches=0`

Interpretation:

- the synthetic `~10x` result on the GTX 1060 was real, but too optimistic for the true exported workload
- on real PALP jobs, the narrow kernel replay still wins cleanly, but by about `3.4x`, not `10x`
- that makes a GPU path plausible, but only for a narrow offload with low marshaling overhead; it does not justify a blind full CUDA rewrite of PALP

## Live Frontier Trial

The repo now also contains an env-gated live frontier path inside [PALP/Coord.c](PALP/Coord.c#L1278) plus a loadable CUDA runtime:

- CUDA runtime build: [scripts/build_type3_cuda_runtime.sh](scripts/build_type3_cuda_runtime.sh)
- end-to-end benchmark and hash check: [scripts/benchmark_type3_frontier.sh](scripts/benchmark_type3_frontier.sh)
- runtime ABI: [src/verify/type3_bounds_runtime.h](src/verify/type3_bounds_runtime.h)
- runtime implementation: [src/verify/type3_bounds_runtime.cu](src/verify/type3_bounds_runtime.cu)

How it works today:

- `PALP_TYPE3_FRONTIER=cpu` switches the type-3 `(5,5)` `Make_CWS_Points` hot path to a breadth-first frontier enumerator that batches the existing seed/tighten jobs
- `PALP_TYPE3_FRONTIER=cuda` uses the same frontier path, but sends each batch of jobs through the loadable CUDA runtime named by `PALP_TYPE3_CUDA_RUNTIME`
- the original depth-first PALP path remains the default and is untouched unless the env flag is set
- the benchmark script compares output hashes against baseline so the experimental path has an immediate correctness check

Measured results on this GTX 1060 6 GB:

- shard `-j128 -k1`, batch `8192`
  - baseline: `1.64s`
  - cpu frontier: `2.55s`
  - cuda frontier: `6.42s`
- shard `-j32 -k1`, batch `65536`
  - baseline: `7.49s`
  - cuda frontier: `27.12s`

Interpretation:

- the new path is functionally correct on the tested shards: baseline, CPU frontier, and CUDA frontier produced identical output hashes
- but this specific implementation is slower, because it keeps frontier expansion on the host and pays host-device transfer and synchronization costs at every level
- in other words, this bridges the replay PoC to a live path, but it does **not** yet achieve the larger architectural step needed for speedups
- the next meaningful CUDA step is a more device-resident frontier expansion, not more tuning of the current host-managed batching layer

## Device-Resident Frontier Follow-Up

The CUDA runtime now has a second stage beyond the original live batching path: it can keep the per-coordinate type-3 frontier on the device, expand it there, and copy the final point list back only once per candidate.

What changed:

- the original live path built jobs on the host, shipped each batch to the GPU, downloaded the bounds back, and then expanded the frontier on the host
- the current runtime can upload the candidate-specific basis/X0/Xmax problem, initialize the last-coordinate frontier on the device, expand each remaining coordinate there, and download only the final points and aggregate stats

Measured results on the same GTX 1060 6 GB:

- shard `-j128 -k1`
  - baseline: `1.68s`
  - previous live CUDA frontier: `6.42s`
  - current device-resident CUDA frontier: `4.26s`
- shard `-j32 -k1`
  - baseline: `7.26s`
  - previous live CUDA frontier: `27.12s`
  - current device-resident CUDA frontier: `16.64s`

Interpretation:

- this is a real improvement over the previous live CUDA attempt: about `1.5x` faster on the small validation shard and about `1.6x` faster on the more representative shard
- but it is still slower than the CPU baseline on this card, which means the remaining bottleneck is no longer just host-side frontier expansion
- at this point the GTX 1060 is useful for correctness and for detecting directional improvements, but it is a weak platform for estimating the best-case live CUDA upside of the next stages
- the next meaningful step is likely cross-candidate batching or a broader device-side front-end, because per-candidate launch/orchestration overhead is still too expensive relative to the small amount of work in many individual `Make_CWS_Points` calls

## Candidate Batch Status

The repo also contains an opt-in cross-candidate batching path. It remains
default-off, but it is now validated enough to treat as the next real live-CUDA
lever rather than just profiling scaffolding.

- `PALP_TYPE3_CUDA_CANDIDATE_BATCH=1` keeps the normal single-candidate live CUDA path
- values greater than `1` opt into the queued candidate path explicitly
- on `wf4-d1-20`, shard `-j32 -k1`, with a freshly rebuilt `cws.x` and CUDA runtime, the single-candidate CUDA path is still clearly slower than CPU on this GTX 1060:
  - CPU baseline: `7.35s`
  - current working-tree CUDA runtime, `batch=1`: `15.39s`
  - last committed control runtime, `batch=1`: `14.87s`
- that control comparison shows the current per-candidate runtime is still within about `3.5%` of the last committed behavior; the important new result is the queueing layer itself
- on the same shard, queued cross-candidate batching now removes most of the launch/orchestration penalty:
  - `batch=16`, `lanes=8`: `7.11s`
  - `batch=32`, `lanes=8`: `6.40s`
- after moving the toolchain into the new `cuda` Nix shell and rebuilding `cws.x` plus the CUDA runtime inside that shell, the same shard still shows the same qualitative result:
  - CPU baseline: `7.57s`
  - `batch=1`, `lanes=8`: `15.78s`
  - `batch=16`, `lanes=8`: `6.47s`
  - `batch=32`, `lanes=8`: `6.54s`
- on this card, `batch=16` and `batch=32` with `PALP_TYPE3_CUDA_BATCH_LANES=8` are effectively tied in the fresh shell-based rebuild, and both beat the CPU baseline while preserving exact output hashes
- this is still not enough evidence to make batching the default, but it is now the right path to test on a stronger NVIDIA GPU and over a broader shard sweep

## Nix GPU Shell Status

The repo now has dedicated Nix shells for the GPU work:

- `cuda` provides `nvcc`, the CUDA runtime build dependencies, and the type-3 benchmark scripts
- `rocm` provides `hipcc`, ROCm probe tools, and enough runtime/compiler state to validate basic HIP compilation
- on this host, direct `nix develop` tries to use `/nix/var/nix/builds` and fails, so the practical entry point is [scripts/nix_develop_local.sh](scripts/nix_develop_local.sh)
- quick probes live in [scripts/probe_gpu_stack.sh](scripts/probe_gpu_stack.sh)

Current shell validation results on this machine:

- CUDA shell:
  - `scripts/probe_gpu_stack.sh cuda` sees the GTX 1060 6 GB and CUDA 12.6
  - rebuilding `PALP/cws.x` and [scripts/build_type3_cuda_runtime.sh](scripts/build_type3_cuda_runtime.sh) from inside the shell succeeds
- ROCm shell:
  - `hipcc` now compiles a trivial HIP runtime probe successfully
  - `rocm-smi` sees the RX 7700 XT as `gfx1102`
  - `rocminfo` still fails with `HSA_STATUS_ERROR_OUT_OF_RESOURCES`
  - a trivial `hipGetDeviceCount` probe currently returns `status=100` and `devices=0`

Interpretation:

- the CUDA shell is now a usable reproducible path for this repo on this machine
- the ROCm shell is sufficient for toolchain/probe validation, but the AMD runtime is still not usable for real HIP execution here; the remaining blocker is the host ROCm runtime state, not the flake packaging

## Multi-Worker GPU Launcher

The repo now contains a GPU-aware shard launcher for the live CUDA path:

- launcher: [scripts/run_type3_multi_gpu.sh](scripts/run_type3_multi_gpu.sh)
- it schedules a shard range across one or more GPU worker slots using separate PALP processes
- each run records per-shard logs, aggregate timings, and an order-insensitive combined sorted SHA-256 over the emitted CWS lines

Representative launcher benchmarks on the GTX 1060, using `wf4-d1-20`, shard subset `1..8 / 32`, all with the same combined sorted hash `849563a99352395ec00c62e658ef3e287086b16648395e6e0c487b104215ddf7`:

- `1 worker/GPU`, `batch=16`, `lanes=8`: `45.54s`
- `2 workers/GPU`, `batch=16`, `lanes=8`: `40.34s`
- `2 workers/GPU`, `batch=32`, `lanes=8`: `39.94s`
- `2 workers/GPU`, `batch=16`, `lanes=4`: `54.40s`
- `4 workers/GPU`, `batch=16`, `lanes=4`: `60.28s`

Interpretation:

- on this GPU, moderate oversubscription helps: `2 workers/GPU` is about `12.3%` faster than `1 worker/GPU` on this shard subset when using `batch=32`, `lanes=8`
- lowering per-process lane count to `4` was clearly the wrong direction here
- this gives a practical starting point for future RTX multi-GPU tests: keep one or two PALP processes per GPU, start from `batch=32`, `lanes=8`, and scale out with explicit `GPU_LIST`