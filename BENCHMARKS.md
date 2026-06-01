# Benchmarks

This document tracks performance benchmarks for the polytope processing pipeline across development milestones.

## Combined-CWS CPU Pipeline

The combined-CWS path uses the recovered `PALP/cws-5d.x -c5 -s#` generator,
converts PALP text rows to the explicit combined Parquet schema, runs the CPU
PALP classifier, and verifies replay with `add_nf --verify-hash`.

Smoke/regression:

```bash
scripts/test_combined_cws_pipeline.sh
```

Generate production input for one canonical structure:

```bash
scripts/generate_dim5_cws_parquet.sh --structure-id 15 --output-dir data/combined-cws
```

Run a local benchmark with environment capture:

```bash
STRUCTURE_ID=5 THREADS=32 scripts/benchmark_combined_cws_pipeline.sh
```

On SLURM, submit structures directly as an array:

```bash
sbatch --array=2-47 scripts/slurm_combined_cws_array.sh
```

For sharded generation of a single expensive structure:

```bash
sbatch --array=1-64 --export=STRUCTURE_ID=12,SHARD_COUNT=64 scripts/slurm_combined_cws_array.sh
```

Every benchmark directory contains `logs/environment.txt` from
`scripts/discover_gpu_env.sh`, including SLURM, CPU, local toolchain, and GPU
details when available.

## CUDA Backend

The classifier can be built with an optional CUDA backend boundary:

```bash
scripts/test_cuda_backend.sh
```

This configures `src/classify/build-cuda` with `-DENABLE_CUDA=ON`, builds the
CUDA smoke executable, runs a CUDA runtime visibility check, and compares CPU
and `--backend auto` classifier summaries and exact hash/count records on a
small combined-CWS slice. On the head node this may report a CUDA driver/runtime
mismatch and use CPU fallback; inside a GPU SLURM allocation it exercises the
CUDA point, equation-scan, VM/VPM, and canonical normal-form kernels.

Benchmark CPU and CUDA-backend runs on the same input:

```bash
STRUCTURE_ID=5 THREADS=32 MAX_ROWS=10000 scripts/benchmark_cuda_backend.sh
```

Submit CUDA-backend classification jobs under SLURM:

```bash
sbatch --array=2-47 scripts/slurm_cuda_combined_cws_array.sh
```

Current status: the CUDA backend infrastructure, device runtime probe, batched
device CWS validation kernel, exact device `Make_CWS_Points` generation for
index-1 CWS rows, CUDA reductions for the expensive `Find_Equations` all-point
equation scans, CUDA VM/VPM construction, CUDA canonical VPM permutation search,
CUDA GL(5,Z) triangular normal-form reduction, parity smoke, benchmark wrapper,
direct type-3 `(5,5)` GPU pair-space scanner, and SLURM entry point are
implemented. PALP candidate-equation bookkeeping
inside `Find_Equations` and CPU hash/dedup/checkpoint merge remain the known CPU
parts.

### 2026-06-01 CUDA Backend Smoke

Short SLURM allocation on `n32`:

- GPU: NVIDIA RTX PRO 6000 Blackwell Max-Q Workstation Edition.
- Driver: 580.142.
- Compute capability: 12.0.
- Build architecture fix: real SASS for `sm_75`, `sm_89`, `sm_120` to avoid
  CUDA 13 PTX JIT on the cluster driver.

Smoke result:

- `cuda_backend_smoke`: PASS.
- CUDA point-count probe matched PALP on the smoke simplex: `462` points.
- CUDA point-list generation feeds CUDA equation-scan reductions, CUDA VM/VPM
  construction, and CUDA canonical normal-form reduction.
- Backend name at this milestone: `cuda-points+equation-scans+canonical-nf`.

Benchmark wrapper smoke:

```text
RUN_DIR=/tmp/process-polytopes-cuda-equation-benchmark
STRUCTURE_ID=5
THREADS=2
MAX_ROWS=285
CPU:       285 CWS, 0 failed, 42 duplicate, 243 unique, ~10934 CWS/s
CUDA-auto: 285 CWS, 0 failed, 42 duplicate, 243 unique, ~1108 CWS/s
Parity:    243 hash/count records matched exactly
```

### 2026-06-01 CUDA Canonical NF Smoke

Short SLURM allocation on `n32`, same structure-5 benchmark corpus:

```text
RUN_DIR=/tmp/process-polytopes-cuda-canonical-benchmark
STRUCTURE_ID=5
THREADS=2
MAX_ROWS=285
CPU:       285 CWS, 0 failed, 42 duplicate, 243 unique, ~11012 CWS/s
CUDA-auto: 285 CWS, 0 failed, 42 duplicate, 243 unique, ~625 CWS/s
Parity:    243 hash/count records matched exactly
Backend:   cuda-points+equation-scans+canonical-nf
```

The CUDA-auto path is slower at this milestone because the current device point
kernel is exact but serial per CWS, the canonical NF port runs one CUDA thread
per CWS for correctness parity, and each candidate-equation scan still pays a
kernel launch/copy synchronization cost. This benchmark is a correctness and
runtime-integration baseline before batching and occupancy work.

### 2026-06-01 CUDA Scheduler And Point-Generation Pass

Short SLURM allocation on `n32`, RTX PRO 6000 Blackwell Max-Q. The CUDA/auto
classifier scheduler now uses all requested workers with 32-row blocks, and the
device point generator distributes the outer PALP coordinate range across CUDA
threads with atomic point append. Exact CPU-vs-CUDA parity remained the gate.

Structure 5, full 285-row corpus, exact parity matched 243 hash/count records:

| Threads | CPU CWS/s | CUDA CWS/s |
|---:|---:|---:|
| 1 | 11897 | 635 |
| 2 | 8482 | 843 |
| 4 | 11174 | 1072 |
| 8 | 11768 | 1029 |

The selected-candidate equation-scan fast path was restored after testing a
batched all-candidate scan. Batching all candidates was correct but slower on
structures 5 and 8 because PALP often finds the next bad equation before every
candidate needs a point scan. The batched path is retained behind a large-row
threshold for future high-point-count probes.

Latest selected-fast-path benchmarks:

| Corpus | Rows | Threads | CPU CWS/s | CUDA CWS/s | Parity |
|---|---:|---:|---:|---:|---|
| structure 5 | 285 | 4 | 12030 | 1095 | 243 hash/count records matched |
| structure 8 | 2755 | 8 | 41791 | 2199 | 405 hash/count records matched |

Interpretation: current CUDA is a correct geometry/NF backend, but it is not the
fast production backend yet. For these small and medium rows, CPU PALP wins by
roughly 11x on structure 5 and 19x on structure 8. The gap is launch/sync/copy
overhead plus remaining CPU `Find_Equations` bookkeeping, not arithmetic
throughput.

Nsight Systems profile of 64 structure-5 rows, one CUDA worker:

| Metric | Count / time |
|---|---:|
| CWS rows | 64 |
| CUDA kernel launches | 1335 |
| `cudaMemcpyAsync` calls | 1849 |
| `cudaStreamSynchronize` calls | 1335 |
| Device-to-host copies | 1527 copies, 0.777 MB total |
| Host-to-device copies | 322 copies, 0.049 MB total |
| Total GPU kernel time | 86.3 ms |
| `nf_canonical_kernel` | 64 launches, 68.8 ms GPU time |
| `cws_make_points_parallel_kernel` | 64 launches, 11.3 ms GPU time |
| `equation_min_vertex_kernel` | 1141 launches, 6.1 ms GPU time |

This trace shows the main issue clearly: the GPU arithmetic is small, but the
current backend launches and synchronizes many tiny kernels per CWS and copies
point/candidate data back to CPU for PALP bookkeeping. The performance fix is
not simply “more CUDA kernels”; it is batching many CWS rows inside persistent
device-resident workspaces and removing the per-candidate host round trip.

### 2026-06-01 Direct Type-3 `(5,5)` CUDA Enumeration

The type-3 descriptor is the `5-5` case: two 5-weight systems overlapping in
three coordinates. Instead of materializing PALP text rows first, the dedicated
CUDA scanner builds PALP's exact `u=3` selected W5 pool, shards the upper
triangular selected-pair space, applies the canonical shared-prefix permutation
rule on device, and optionally stores a bounded buffer of generated CWS rows in
device memory.

Implementation and wrapper:

```bash
src/classify/build-cuda/cuda_type3_55_scan
scripts/benchmark_type3_55_cuda.sh
```

The W5 pool should live under the workspace, not `/tmp`, because `/tmp` is
node-local under SLURM:

```bash
results/cache/w5.ip
```

RTX PRO 6000 Blackwell Max-Q run on `n32`, compute capability 12.0:

| Quantity | Value |
|---|---:|
| Published W5 base pool | 184026 |
| PALP `u=3` selected W5 pool | 1833327 |
| Arity-only unordered W5 base pairs | 16932876351 |
| Selected-pair space before prefix multiplicity | 1680544861128 |
| Canonical-prefix candidates before IP filtering | 10046036135619 |
| Full selected-pair scan time, one RTX 6000 | 86.53 s |
| Selected-pair throughput | 19.42B pairs/s |
| Canonical-prefix candidate throughput | 116.10B candidates/s |

A 4096-pair smoke range was checked against the CPU verifier exactly. A 100M
selected-pair sample scanned in 0.00499 s at about 20.0B selected pairs/s.

This changes the blocker diagnosis for type 3: enumeration and prefix
permutation are no longer the bottleneck. The remaining production work is to
fuse this generator with device-resident IP geometry: generate a candidate CWS,
build points, run equation/IP checks, and append only accepted IP rows to a
large GPU output buffer before transferring compressed chunks to the host for
disk and global dedup.

### 2026-06-01 Generic Dim-5 CUDA Descriptor Scan

The descriptor-generic scanner extends the direct type-3 strategy to all 46
combined dim-5 structures. It loads the W5 pool once, parses the PALP W4 table,
builds selected pools for every simplex size/shared-count pair, computes slot
automorphism orbits, and applies selection-order plus shared-prefix canonical
checks on GPU. It does not materialize Parquet rows and does not run IP or NF.
With `--emit-capacity`, it also constructs canonical CWS candidates directly on
the device into a bounded output buffer; this is a smokeable producer interface
for the upcoming fused IP kernel.

Implementation and wrapper:

```bash
src/classify/build-cuda/cuda_dim5_cws_scan
scripts/benchmark_dim5_cws_cuda_scan.sh
```

The generic scanner reproduces the dedicated type-3 count exactly. With the
descriptor-3 fast path enabled, one RTX PRO 6000 scanned type 3 in 55.17 s:

```text
3,3361087888929,1680544861128,1680544861128,10046036135619,55.168190
```

All-structure run on `n32`, one RTX PRO 6000 Blackwell Max-Q:

| Quantity | Value |
|---|---:|
| Loose descriptor-overlap upper bound | 114133723488574 |
| Exact prefix-pruned pre-IP candidates | 12140535288504 |
| Prefix-pruned fraction of loose upper bound | 10.637% |
| Reduction from loose upper bound | 9.40x |
| Canonical selected tuples scanned | 1877351991499 |
| Total scan time, one GPU | 223.26 s |
| Average selected-tuple scan rate | 8.41B/s |
| Average prefix-candidate count rate | 54.38B/s |

Dominant descriptor rows from the all-structure CSV:

| Structure | Selection product | Canonical selected tuples | Prefix-pruned pre-IP candidates | Seconds |
|---:|---:|---:|---:|---:|
| 3 | 3361087888929 | 1680544861128 | 10046036135619 | 55.35 |
| 12 | 91611350190 | 91611350190 | 987911532890 | 78.77 |
| 13 | 91611350190 | 91611350190 | 987911532890 | 78.95 |
| 25 | 2892990006 | 2892990006 | 28358376718 | 2.36 |
| 26 | 2892990006 | 2892990006 | 28358376718 | 2.35 |
| 27 | 2892990006 | 2892990006 | 28358376718 | 2.37 |
| 29 | 1044996390 | 1044996390 | 22300333662 | 1.87 |
| 20 | 1963493217 | 1963493217 | 6535293765 | 0.67 |

Interpretation: after accounting for prefix pruning, descriptor automorphisms,
and early canonical selection returns, the working pre-IP search is about
`12.14T` candidate CWS rows, not the loose `~114T` upper bound. The scan is
already short enough that production planning should focus on the fused GPU IP
filter and accepted-row buffering, not on further CPU-side generation or Parquet
conversion.

Prefix-only timing estimates from this run:

| Execution model | Estimated prefix-only wall time |
|---|---:|
| 1 RTX PRO 6000, current sequential all-structure scanner | 3.72 min |
| 4 RTX PRO 6000 GPUs, ideal sharded scan | 55.8 s |
| 8 comparable GPUs, ideal sharded scan | 27.9 s |

Fused IP-filter runtime cannot be inferred directly from prefix counting because
each surviving prefix candidate must construct a CWS, generate points, and run
IP/facet checks. Starting from the exact `12.14T` pre-IP candidates, the wall
time envelope for one GPU is:

| Sustained fused IP candidate rate | One-GPU wall time | Four-GPU ideal wall time |
|---:|---:|---:|
| 100B candidates/s | 2.0 min | 30 s |
| 10B candidates/s | 20.2 min | 5.1 min |
| 1B candidates/s | 3.37 h | 50.6 min |
| 100M candidates/s | 33.7 h | 8.4 h |
| 10M candidates/s | 14.1 days | 3.5 days |

The next benchmark needed for a reliable production estimate is a fused IP pilot
kernel on a representative shard. NF is intentionally excluded from this phase
and can run as post-processing on accepted IP CWS rows.

Device candidate-emission smoke on structure 5:

```bash
STRUCTURE_ID=5 scripts/benchmark_dim5_cws_cuda_scan.sh --emit-capacity 4 --print-candidates 4
```

This counted all 285 canonical structure-5 candidates on GPU and stored a
bounded sample of four CWS rows in the device output buffer before copying the
sample back for inspection.

### 2026-06-01 Structure-12 Sampled Pipeline Probe

Structure 12, together with structure 13, is the second-largest combined type
after type 3 `(5,5)` in the prefix-pruned count table. Its all-structure scan
row is:

```text
12,91611350190,91611350190,91611350190,987911532890,78.770380
```

Faithful PALP generator probes, using `Only_IP_CWS=1`, did not produce flushed
accepted rows in short samples. Five line-buffered 30-second probes across
shards `1`, `24`, `48`, `72`, and `95` of `95` all timed out with zero output
rows. This appears to be dominated by PALP's CPU startup/generation path for
large 5-weight pools and is not a useful throughput estimator for the desired
GPU-resident pipeline.

CUDA candidate construction/storage samples on structure 12, shard index `0`:

| Shard count | Canonical selected tuples | Prefix candidates | Emit capacity | GPU scan seconds | Candidate rate |
|---:|---:|---:|---:|---:|---:|
| 10000000 | 9161 | 25552 | 200000 | 0.000347 | 73.6M/s |
| 1000000 | 91611 | 269920 | 1000000 | 0.000851 | 317.2M/s |
| 100000 | 916113 | 2741554 | 4000000 | 0.009517 | 288.1M/s |

The largest sample stores every generated candidate in a device buffer. At the
measured `288M candidates/s/GPU`, storing every prefix candidate for structure
12 would take about `57 min` on one GPU or `14 min` on four ideal GPUs. Storing
every prefix candidate for all structures would take about `11.7 h` on one GPU
or `2.9 h` on four ideal GPUs before any IP work. This is intentionally a
conservative measurement of the current bounded-output implementation, not a
claim about the final fused accepted-row writer.

Debug text output is much slower and much larger than the intended device chunk
format. Printing 100000 sampled structure-12 candidates wrote `11.44 MB` in
about `0.3 s` beyond fixed launch/pool overhead, roughly `333k text rows/s` and
`114 bytes/candidate`. Writing all `12.14T` pre-IP candidates in this debug text
format would be around `1.38 PB`, so the production pipeline must write only
accepted rows in a compact chunk format.

For a CPU reference on the same CUDA-generated candidates, 100000 sampled
structure-12 rows were converted to Parquet and classified with the existing
CPU backend using four threads:

| Metric | Value |
|---|---:|
| Total candidate rows | 100000 |
| Failed/non-IP rows | 77985 |
| Duplicate accepted rows | 3768 |
| Unique polytopes | 18247 |
| Classifier throughput | 54525 candidates/s |
| Unique output Parquet size | 583650 bytes |
| Checkpoint size | 5839064 bytes |

This CPU reference sample is useful for scale intuition, not for production
planning: at `54525 candidates/s` on four CPU threads, processing all `12.14T`
pre-IP candidates would take years.

The first GPU-only IP filter is now implemented in `cuda_dim5_cws_scan`. It
builds the CWS lattice basis and point list on device, runs a
serial-per-candidate 5D port of PALP's `IP_Check`, stores accepted CWS rows in a
device buffer, and optionally flushes accepted rows to a text file. The bounded
`--ip-check` path checks the first emitted candidate buffer. The production-shaped
`--stream-ip` path scans the full shard in device chunks, IP-filters each chunk,
and flushes accepted rows only. The point workspace is now per launched CUDA
thread, not per candidate, so large candidate chunks no longer require a point
buffer for every generated row.

Structure-5 validation:

```bash
STRUCTURE_ID=5 scripts/benchmark_dim5_cws_cuda_scan.sh \
  --emit-capacity 285 --ip-check --ip-max-points 4096 \
  --accepted-output results/cuda_ip_structure5_all.rows
```

Result on `n32`, RTX PRO 6000 Blackwell Max-Q:

| Metric | Value |
|---|---:|
| Candidates checked | 285 |
| GPU IP accepted | 285 |
| Point overflows | 0 |
| IP seconds | 0.019418 |
| IP throughput | 14677 candidates/s |

After normalizing whitespace and removing PALP metadata, the GPU accepted-row
set matches `./PALP/cws-5d.x -c5 -s5` exactly (`285` rows, empty diff).

Structure-12 GPU IP samples on shard index `0`:

| Shard count | Emit capacity | Point cap | Checked | Accepted IP | Overflows | IP seconds | IP throughput |
|---:|---:|---:|---:|---:|---:|---:|---:|
| 100000 | 500000 | 2048 | 500000 | 87173 | 0 | 1.760298 | 284043/s |

The accepted output file for the 500000-candidate / 2048-point-cap run contains
`87173` text rows and is `4.8 MB`. Replaying the full accepted sample through
the CPU PALP classifier produced `0` failed rows, `23038` duplicate CWS rows,
and `64135` unique polytopes. The sampled acceptance fraction varies with the
non-deterministic bounded device buffer fill order, so this is a throughput and
false-positive guard, not a global structure-12 acceptance estimate.

At the clean bounded 500000-candidate rate (`284043 candidates/s/GPU`), the
current bounded pilot would take about `40.3 days` on one GPU for all `987.9B`
structure-12 pre-IP candidates, or `10.1 days` on four ideal GPUs. Applying the
same rate to all `12.14T` pre-IP candidates gives about `495 days` on one GPU or
`124 days` on four ideal GPUs. The streaming benchmarks below supersede this
for production planning.

Streaming IP benchmarks, same GPU and shard index `0` for structure 12:

| Structure | Shard count | Emit capacity | Point cap | Prefix candidates | Chunks | Accepted IP | Overflows | IP seconds | IP throughput |
|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| 5 | 1 | 64 | 4096 | 285 | 5 | 285 | 0 | 0.054879 | 5193/s |
| 12 | 100000 | 500000 | 2048 | 2741554 | 22 | 153162 | 0 | 17.887159 | 153269/s |
| 12 | 100000 | 1000000 | 2048 | 2741554 | 11 | 153162 | 0 | 13.742039 | 199501/s |
| 12 | 100000 | 3000000 | 2048 | 2741554 | 4 | 153162 | 0 | 11.719359 | 233934/s |
| 12 | 100000 | 12000000 | 2048 | 2741554 | 1 | 153162 | 0 | 9.214456 | 297528/s |

The 12M-capacity structure-12 run writes `153162` accepted text rows (`8.5 MB`).
CPU PALP replay of the full accepted file produced `0` failed rows, `68631`
duplicates, and `84531` unique polytopes.

The 2026-06-01 optimization pass moved persistent IP bookkeeping
(`vertices`, candidate equations, facets, and incidence masks) out of the CUDA
thread stack and into explicit reusable device scratch. On `sm_120` this reduced
`cws_ip_filter_kernel` resource usage from `STACK:12048` to `STACK:4352` bytes
per thread. A launch-geometry sweep found `--threads 128` faster than the
previous `256` default for the high-register IP kernel, so `128` is now the
scanner default. A safe early discard also skips PALP simplex construction when
point generation produces fewer than 6 lattice points.

Optimized streaming IP benchmarks on the same GPU:

| Structure | Shard count | Shard index | Emit capacity | Threads | Prefix candidates | Accepted IP | IP seconds | IP throughput |
|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| 12 | 100000 | 0 | 3000000 | 128 | 2741554 | 153162 | 8.155322 | 336168/s |
| 3 | 50000000 | 25000000 | 500000 | 128 | 200631 | 8 | 3.196944 | 62757/s |
| 3 | 50000000 | 40000000 | 500000 | 128 | 201558 | 0 | 1.250539 | 161177/s |

Type 3 `(5,5)` dominates the global candidate count and remains substantially
slower than structure 12 under the current serial-per-candidate IP port.
Pre-optimization interior type-3 streaming shard samples with
`--stream-ip --ip-max-points 2048 --threads 256` were:

| Shard count | Shard index | Emit capacity | Prefix candidates | Accepted IP | Overflows | IP seconds | IP throughput |
|---:|---:|---:|---:|---:|---:|---:|---:|
| 50000000 | 25000000 | 500000 | 200631 | 8 | 0 | 3.862972 | 51937/s |
| 50000000 | 40000000 | 500000 | 201558 | 0 | 0 | 1.432308 | 140723/s |

Block-count tuning at 256 threads did not help: `--blocks 752` reached
`52565/s`, while `--blocks 188` fell to `47380/s`. With 128 threads, block-count
results on the slower type-3 sample were `59334/s` at 752 blocks, `61887/s` at
1504 blocks, `60645/s` at 3008 blocks, and `60627/s` at 6016 blocks. The best
simple launch-level win is the 128-thread geometry; the remaining bottleneck is
inside per-candidate point generation/IP equation work.

Using the measured exact counts (`10046036135619` type-3 candidates and
`2094499152885` non-type-3 candidates), and assuming non-type-3 structures run
near the optimized structure-12 streaming rate, the current one-GPU full
generation+IP wall-time envelope is:

| Type-3 sustained rate | One GPU | Four ideal GPUs | Eight ideal GPUs |
|---:|---:|---:|---:|
| 62757/s | 5.27 years | 481 days | 241 days |
| 100000/s | 3.38 years | 309 days | 154 days |
| 161177/s | 2.17 years | 198 days | 99 days |

The old structure-12-only extrapolation (`~1.29 years` on one GPU) is too
optimistic because type 3 is both the largest structure and slower per candidate.
The next optimization must still parallelize point generation and equation/IP
scans within each candidate, especially for type 3. A gated one-block-per-CWS
cooperative IP experiment was attempted during this pass, but it did not pass
structure-5 parity/stability and was removed from the production path.

### 2026-06-01 Dim-5 Structure Count Probe

Generator command:

```bash
timeout 10s PALP/cws-5d.x -c5 -s<ID> | wc -l
```

Completed within the 10-second per-structure probe:

| Structure | Rows | Structure | Rows | Structure | Rows |
|---:|---:|---:|---:|---:|---:|
| 5 | 285 | 7 | 28221 | 8 | 2755 |
| 9 | 95 | 10 | 16040 | 16 | 1122 |
| 17 | 1122 | 18 | 1122 | 19 | 6 |
| 22 | 2748 | 23 | 51 | 30 | 10298 |
| 31 | 10298 | 32 | 10298 | 33 | 87 |
| 34 | 493 | 37 | 3 | 39 | 17 |
| 41 | 36 | 42 | 1504 | 44 | 65 |
| 45 | 3756 | 46 | 29 | 47 | 1 |

These 24 structures sum to 90452 emitted CWS rows. The structures that did not
finish within 10 seconds were `2`, `3`, `4`, `6`, `11`, `12`, `13`, `14`, `15`,
`20`, `21`, `24`, `25`, `26`, `27`, `28`, `29`, `35`, `36`, `38`, `40`, and
`43`. Structures involving 5-weight base systems can spend substantial time in
PALP generation/filtering before emitting rows, so full production estimates
must measure generation, classification, and merge separately.

### Current Candidate-Count Estimates

The arity-only estimate uses the Kreuzer-Skarke single-weight-system pool sizes

```text
A2 = 1, A3 = 3, A4 = 95, A5 = 184026
```

and counts multisets of constituent simplex weight systems:

```text
N(T) = product_k binomial(A_k + m_k(T) - 1, m_k(T))
```

where `m_k(T)` is the number of `k`-weight simplices in CWS type `T`. This is
the clean mathematical candidate count by arity. It intentionally does not yet
multiply by every overlap-subset/prefix-permutation choice in the descriptor;
those choices explain why some measured generator outputs can exceed this
arity-only value.

| Type | Simplex sizes | Arity candidates |
|---:|---|---:|
| 2 | (2,5) | 184026 |
| 3 | (5,5) | 16932876351 |
| 4 | (3,5) | 552078 |
| 5 | (3,4) | 285 |
| 6 | (4,5) | 17482470 |
| 7 | (4,4) | 4560 |
| 8 | (2,4,4) | 4560 |
| 9 | (2,2,4) | 95 |
| 10 | (2,4,4) | 4560 |
| 11 | (4,4,4) | 147440 |
| 12 | (4,4,5) | 839158560 |
| 13 | (4,4,5) | 839158560 |
| 14 | (4,4,4) | 147440 |
| 15 | (2,5,4) | 17482470 |
| 16 | (2,3,4) | 285 |
| 17 | (2,3,4) | 285 |
| 18 | (2,3,4) | 285 |
| 19 | (2,3,3) | 6 |
| 20 | (3,5,4) | 52447410 |
| 21 | (3,5,3) | 1104156 |
| 22 | (3,3,4) | 570 |
| 23 | (3,3,3) | 10 |
| 24 | (3,4,4) | 13680 |
| 25 | (3,4,5) | 52447410 |
| 26 | (3,4,5) | 52447410 |
| 27 | (3,4,5) | 52447410 |
| 28 | (3,4,4) | 13680 |
| 29 | (3,4,5) | 52447410 |
| 30 | (3,4,3) | 570 |
| 31 | (3,4,3) | 570 |
| 32 | (3,4,3) | 570 |
| 33 | (2,4,2,3) | 285 |
| 34 | (2,4,3,3) | 570 |
| 35 | (2,5,3,3) | 1104156 |
| 36 | (2,5,3,2) | 552078 |
| 37 | (2,2,2,3) | 3 |
| 38 | (2,2,3,5) | 552078 |
| 39 | (2,2,3,3) | 6 |
| 40 | (2,3,5,3) | 1104156 |
| 41 | (2,3,3,3) | 10 |
| 42 | (2,3,3,4) | 570 |
| 43 | (3,5,3,3) | 1840260 |
| 44 | (3,3,3,3) | 15 |
| 45 | (3,3,3,4) | 950 |
| 46 | (2,4,2,2,2) | 95 |
| 47 | (2,2,2,2,2) | 1 |

Arity-only combined total for types `2..47`: `18915730405` CWS candidates.
Including completed single-weight type `1` with `A6 = 183000000000`, the total
arity-only count is `201915730405`.

Exact shared-selection pool sizes from the published `w5.ip.gz` pool and the
builtin lower-dimensional pools are:

| Simplex size | Shared count 0 | 1 | 2 | 3 | 4 | 5 |
|---:|---:|---:|---:|---:|---:|---:|
| 2 | 1 | 1 | 1 |  |  |  |
| 3 | 3 | 6 | 6 | 3 |  |  |
| 4 | 95 | 357 | 526 | 357 | 95 |  |
| 5 | 184026 | 917799 | 1833327 | 1833327 | 917799 | 184026 |

Multiplying these selection pools with a conservative maximum `u!` factor for
shared-prefix permutations gives a loose descriptor-overlap upper bound of
`114133723488574` pre-filter cases across types `2..47`. This is deliberately
an upper bound: PALP's canonical prefix checks and descriptor automorphisms cut
it down, and the final `Only_IP_CWS` filter cuts it down again before downstream
row-format export or classification.

The CUDA descriptor scanner now measures the exact post-canonicalization,
pre-IP count across structures `2..47` as `12140535288504`. This is 10.637% of
the loose descriptor-overlap upper bound, a 9.40x reduction before any IP
filtering. Parquet is no longer part of the generation/IP hot path for this
phase; any row-format export should happen after GPU IP filtering.

The estimates below are for geometry/classification runs after CWS rows exist;
they do not include slow PALP CWS generation for the timeout structures or the
final global merge.

Measured current rates on `n32`:

- CPU PALP classifier: about 12000-14000 CWS/s per effective CPU worker on small
  rows; structure 8 reached 41791 CWS/s using 3 effective CPU workers.
- Current CUDA backend: 1095 CWS/s on structure 5 and 2199 CWS/s on structure 8
  on one RTX PRO 6000 Blackwell Max-Q GPU.

For the arity-only combined count `18915730405`:

| Execution model | Assumed rate | Estimated wall time |
|---|---:|---:|
| 1 CPU worker | 12000 CWS/s | 18.2 days |
| 1 64-core CPU node, ideal scaling | 768000 CWS/s | 6.8 hours |
| 10 such CPU nodes | 7.68M CWS/s | 41.1 minutes |
| 100 such CPU nodes | 76.8M CWS/s | 4.1 minutes |
| Current CUDA backend, 1 GPU | 2200 CWS/s | 99.5 days |
| Current CUDA backend, 4 GPUs | 8800 CWS/s | 24.9 days |
| Current CUDA backend, 100 GPUs | 220000 CWS/s | 23.9 hours |

Conclusion: the relevant generator-side planning count is now the measured
`12.14T` prefix-pruned pre-IP candidate count, not the loose `~114T` upper bound
and not only the arity-only `18.9B` constituent-weight multiset count. This
should not be interpreted as `12.14T` normal-form runs: the next production stage
is a device-resident IP filter, with NF deferred until after accepted IP CWS rows
exist. The current correct row-based CUDA backend is still slower than CPU on
the measured small/mid rows because it launches/synchronizes per CWS and keeps
PALP candidate-equation bookkeeping on CPU.

To reproduce any benchmark recorded here, run:

```bash
./scripts/benchmark.sh [input_file]
```

The script defaults to `samples/sample-100k.txt`. It prints CPU info, then runs `poly.x` for 3 iterations each of: single-threaded, multi-threaded (32 GNU parallel workers), and taskset-pinned (each chunk pinned to one logical CPU).

---

## Environment

| Property | Value |
|---|---|
| CPU | AMD Ryzen 9 7950X3D 16-Core Processor |
| Physical cores | 16 |
| Logical threads | 32 |
| L1d cache | 512 KiB (16 instances) |
| L1i cache | 512 KiB (16 instances) |
| L2 cache | 16 MiB (16 instances) |
| L3 cache | 128 MiB (2 instances, 3D V-Cache) |
| RAM | 93 GiB |
| OS | Arch Linux |
| Kernel | 6.19.8-arch1-1 |
| Parallelism tool | GNU parallel 20260222 |

---

## 2026-03-25 — Baseline: `poly.x` on `sample-100k.txt`

**Binary:** `./PALP/poly.x`
**Input:** `samples/sample-100k.txt` (100,000 lines)
**Command (single-threaded):** `./PALP/poly.x samples/sample-100k.txt outfile.txt`
**Command (multi-threaded):** input split into 32 equal chunks, processed with `parallel -j32`
**Command (taskset-pinned):** same 32 chunks, each job pinned to a dedicated logical CPU via `taskset -c <cpu_id>`

`poly.x` is a single-threaded binary with no OpenMP support. The multi-threaded and taskset scenarios measure throughput when the workload is partitioned across all 32 logical CPUs using GNU parallel.

### Single-threaded

| Run | User time (s) | Sys time (s) | Wall time (s) | CPU usage |
|-----|--------------|-------------|--------------|-----------|
| 1   | 14.94        | 0.04        | 15.04        | 99%       |
| 2   | 14.97        | 0.05        | 15.07        | 99%       |
| 3   | 14.81        | 0.05        | 14.92        | 99%       |
| **Mean** | **14.91** | **0.05** | **15.01** | **99%** |

### Multi-threaded (32 parallel workers via GNU parallel)

| Run | User time (s) | Sys time (s) | Wall time (s) | CPU usage |
|-----|--------------|-------------|--------------|-----------|
| 1   | 24.43        | 0.25        | 1.39         | 1779%     |
| 2   | 24.60        | 0.26        | 1.39         | 1794%     |
| 3   | 24.63        | 0.24        | 1.38         | 1804%     |
| **Mean** | **24.55** | **0.25** | **1.39** | **1792%** |

### Taskset-pinned (32 workers, each locked to 1 logical CPU)

Each chunk is assigned to exactly one logical CPU via `taskset -c $(({%}-1))`, preventing the OS from migrating processes between cores.

| Run | User time (s) | Sys time (s) | Wall time (s) |
|-----|--------------|-------------|--------------|
| 1   | 25.96        | 0.26        | 1.57         |
| 2   | 25.83        | 0.23        | 1.58         |
| 3   | 25.72        | 0.25        | 1.59         |
| **Mean** | **25.84** | **0.25** | **1.58** |

### Summary

| Mode | Mean wall time (s) | Speedup vs single-threaded |
|------|-------------------|---------------------------|
| Single-threaded (1 core) | 15.01 | 1.0× |
| Multi-threaded (32 workers, unbound) | 1.39 | **10.8×** |
| Taskset-pinned (32 workers, 1 CPU each) | 1.58 | **9.5×** |

The taskset-pinned mode is ~14% slower in wall time than the unbound parallel run. Pinning eliminates cross-core migration but also prevents the OS from coalescing work onto the fastest cores (this CPU has asymmetric boost clocks across its CCDs). The unbound scheduler naturally gravitates toward the highest-clocked cores, more than compensating for any migration overhead on this workload.

---

## 2026-03-25 — Normal form (`-N` flag) on `sample-100k.txt`

**Binary:** `./PALP/poly.x -N`
**Input:** `samples/sample-100k.txt` (100,000 lines)
**Change from baseline:** added `-N` (compute normal form of each polytope)

### Single-threaded

| Run | User time (s) | Sys time (s) | Wall time (s) |
|-----|--------------|-------------|--------------|
| 1   | 6.76         | 0.07        | 6.86         |
| 2   | 6.69         | 0.06        | 6.78         |
| 3   | 6.66         | 0.07        | 6.75         |
| **Mean** | **6.70** | **0.07** | **6.80** |

### Multi-threaded (32 parallel workers via GNU parallel)

| Run | User time (s) | Sys time (s) | Wall time (s) |
|-----|--------------|-------------|--------------|
| 1   | 10.52        | 0.24        | 0.82         |
| 2   | 10.47        | 0.24        | 0.77         |
| 3   | 10.58        | 0.24        | 0.80         |
| **Mean** | **10.52** | **0.24** | **0.80** |

### Taskset-pinned (32 workers, each locked to 1 logical CPU)

| Run | User time (s) | Sys time (s) | Wall time (s) |
|-----|--------------|-------------|--------------|
| 1   | 11.53        | 0.25        | 0.94         |
| 2   | 11.43        | 0.22        | 0.95         |
| 3   | 11.21        | 0.28        | 0.95         |
| **Mean** | **11.39** | **0.25** | **0.95** |

### Summary

| Mode | Mean wall time (s) | Speedup vs single-threaded |
|------|-------------------|---------------------------|
| Single-threaded (1 core) | 6.80 | 1.0× |
| Multi-threaded (32 workers, unbound) | 0.80 | **8.5×** |
| Taskset-pinned (32 workers, 1 CPU each) | 0.95 | **7.2×** |

With `-N`, single-threaded wall time drops from 15.0s to 6.8s compared to the baseline — the normal form computation path is significantly lighter than the full default output. The parallel speedup ratio is slightly lower (8.5× vs 10.8×) as the shorter per-chunk runtime makes fixed dispatch overhead a larger fraction of total wall time. The taskset penalty (~19%) is consistent with the baseline observation.

---

## 2026-03-25 — Optimized build: `poly.x -N` on `sample-100k.txt`

**Binary:** `./PALP/poly.x -N` (optimized build)
**Input:** `samples/sample-100k.txt` (100,000 lines)

### Changes from previous entry

#### Compilation flags

| Flag | Purpose |
|------|---------|
| `-O3 -march=native` | Full optimization with CPU-specific instructions (AVX-512 etc.) |
| `-flto` | Link-time optimization: cross-file inlining and dead code elimination |
| `-DPOLY_Dmax=5` | Compile specifically for dimension 5 (reduces all `POLY_Dmax`-sized arrays from 6 → 5) |
| `-DPALP_FAST_ASSERT` | Evaluate assert expressions for side effects without aborting (see bug fixes below) |
| PGO (`-fprofile-generate` / `-fprofile-use`) | Profile-guided optimization trained on the full 100K dataset |

Binary size: 1.4 MB → 154 KB after `strip`.

#### Tighter dimension-5 bounds (`Global.h`)

Added a `PALP_TIGHT_5D` compile flag (activated in the `#else` branch of the bounds `#if` chain) with bounds tuned for 5D CWS polytopes. These reduce heap allocation sizes and improve cache utilization:

| Constant | Default (dim ≥ 5) | Optimized | Rationale |
|---|---|---|---|
| `POINT_Nmax` | 2,000,000 | 200,000 | Max observed: 190K |
| `VERT_Nmax` | 64 | 64 (unchanged) | Max observed: 47; 64 keeps `INCI` as `unsigned long long` |
| `FACE_Nmax` | 10,000 | 1,024 | Not used in `-N` path |
| `SYM_Nmax` | 46,080 | 3,840 | = 2^5 * 5! (5-cube symmetry group) |
| `EQUA_Nmax` | 1,280 | 64 | Max observed: 59 facets |

#### Bug fixes in PALP source

**Side effects inside `assert()` (critical correctness bug).** PALP has numerous `assert()` calls containing side effects (increments, decrements, assignments, malloc). Standard `-DNDEBUG` silently breaks the program by removing these side effects. Fixed instances:

- **Vertex.c:898,928** — `assert(IsGoodCEq(&(_C->e[_C->ne++]), ...))`: the `_C->ne++` increment was only executed when assertions were enabled. Extracted side effect before `assert`.
- **LG.c:1314,1375,1459** — `assert(0 < (c--))`: counter decrement inside assert. Changed to `assert(c > 0); c--;`.
- **LG.c:1896** — `assert(0 < (b--))`: same pattern.
- **LG.c:1961** — `assert(0 < (a--))`: same pattern.
- **LG.c:1128,2149,2150,2222,2223** — `assert(NULL != (x = malloc(...)))`: malloc call inside assert, allocation never happens with `NDEBUG`. Extracted malloc before assert.
- **Polynf.c:1309** — `assert((*nw)++ < *Wmax)`: increment inside assert.
- **Polynf.c:2819,2896** — `assert(NULL != (A = malloc(...)))`: same malloc-in-assert pattern.
- **Polynf.c:3093,4369,5222** — `assert(++nk < ...)` / `assert(C++ < p)`: increment inside assert.
- **Polynf.c:3814,4318** — `assert(0 == g % (vg = GL_V_to_GLZ(...)))`: function call with assignment inside assert.

**LG.c include order** — `LG.c` included `LG.h` before `Global.h`, but `LG.h` uses types defined in `Global.h` (`Long`, `PolyPointList`, etc.). Reordered to `Global.h` → `Rat.h` → `LG.h`.

### How to build the optimized binary

```bash
cd PALP

# Clean previous build
make -f GNUmakefile clean

# Step 1: Build instrumented binary for profile collection
make -f GNUmakefile poly.x CC=gcc \
  CFLAGS="-O3 -march=native -flto -DPALP_FAST_ASSERT -DPOLY_Dmax=5 -fprofile-generate"

# Step 2: Collect profile data by running the target workload
cd ..
./PALP/poly.x -N samples/sample-100k.txt /dev/null
cd PALP

# Step 3: Rebuild using collected profile data
rm -f *.o
make -f GNUmakefile poly.x CC=gcc \
  CFLAGS="-O3 -march=native -flto -DPALP_FAST_ASSERT -DPOLY_Dmax=5 \
          -fprofile-use -fprofile-correction"

# Step 4: Strip debug symbols
strip poly.x
```

If you do not need PGO, a simpler single-step build (still captures most of the gain):

```bash
make -f GNUmakefile clean
make -f GNUmakefile poly.x CC=gcc \
  CFLAGS="-O3 -march=native -flto -DPALP_FAST_ASSERT -DPOLY_Dmax=5"
strip poly.x
```

### Single-threaded

| Run | User time (s) | Sys time (s) | Wall time (s) |
|-----|--------------|-------------|--------------|
| 1   | 5.77         | 0.06        | 5.86         |
| 2   | 5.66         | 0.05        | 5.73         |
| 3   | 5.67         | 0.06        | 5.75         |
| **Mean** | **5.70** | **0.06** | **5.78** |

### Multi-threaded (32 parallel workers via GNU parallel)

| Run | User time (s) | Sys time (s) | Wall time (s) |
|-----|--------------|-------------|--------------|
| 1   | 8.81         | 0.27        | 0.68         |
| 2   | 8.89         | 0.26        | 0.64         |
| 3   | 8.83         | 0.27        | 0.69         |
| **Mean** | **8.84** | **0.27** | **0.67** |

### Taskset-pinned (32 workers, each locked to 1 logical CPU)

| Run | User time (s) | Sys time (s) | Wall time (s) |
|-----|--------------|-------------|--------------|
| 1   | 9.42         | 0.25        | 0.76         |
| 2   | 9.59         | 0.24        | 0.76         |
| 3   | 9.56         | 0.25        | 0.76         |
| **Mean** | **9.52** | **0.25** | **0.76** |

### Summary

| Mode | Baseline wall (s) | Optimized wall (s) | Speedup |
|------|-------------------|--------------------|---------|
| Single-threaded (1 core) | 6.80 | 5.78 | **1.18x** |
| Multi-threaded (32 workers, unbound) | 0.80 | 0.67 | **1.19x** |
| Taskset-pinned (32 workers, 1 CPU each) | 0.95 | 0.76 | **1.25x** |

The ~18% single-threaded improvement comes from a combination of `-march=native` (CPU-specific codegen), LTO (cross-file inlining), PGO (branch prediction / code layout), `POLY_Dmax=5` (smaller inner array dimensions), and `PALP_FAST_ASSERT` (skipping assertion abort checks).

### Profile analysis

Profiling (`gprof`) of the unoptimized binary on the full 100K dataset reveals the execution time breakdown:

| Function | % Time | Called | Description |
|---|---|---|---|
| `Make_CWS_Points` | 31.1% | 100K | CWS → lattice point enumeration |
| `FE_Search_Bad_Eq` | 17.9% | 1.1M | Scan all points per candidate equation |
| `Search_New_Vertex` | 15.1% | 1.0M | Scan all points to find minimum vertex |
| `New_Start_Vertex` | 13.6% | 400K | Scan all points for extreme vertices |
| `Make_New_CEqs` | 6.8% | 1.0M | Generate candidate equations from incidences |
| `GLZ_Start_Simplex` | 4.9% | 100K | Initial simplex construction |

---

## 2026-04-17 — Dim-5 `cws -c5` pipeline refactor

**Binary:** `./PALP/cws-5d.x`
**Baseline binary:** committed `HEAD` in a temporary PALP worktree
**Reproduction:** `BASELINE_BIN=/path/to/baseline/cws-5d.x ./scripts/benchmark_dim5_cws.sh 11 24`

### Changes from previous dim-5 implementation

- Replaced dim-5 `tmpfile()` and `rewind()` enumeration with in-memory weight pools.
- Reused `PRINT_CWS` scratch `PolyPointList` buffers instead of allocating and freeing them per candidate.
- Added `-j#` and `-k#` to shard canonical dim-5 enumeration by the first slot.

### Ten-second sampling on representative canonical structures

| Structure | Binary | User (s) | Sys (s) | Wall (s) | Output lines in 10s |
|---|---|---:|---:|---:|---:|
| 11 | current | 9.97 | 0.00 | 10.01 | 32,162 |
| 11 | baseline | 5.25 | 4.36 | 10.00 | 19,848 |
| 24 | current | 9.97 | 0.00 | 10.00 | 54,194 |
| 24 | baseline | 5.00 | 4.66 | 10.00 | 21,970 |

The optimized build shifts the workload from kernel time back into user-space arithmetic. On structure 11 the 10-second work rate improves by about **1.62x**. On structure 24 it improves by about **2.47x**.

### `strace -c` syscall profile

In the old dim-5 path, 10-second samples on structures 11 and 24 both executed about **529K syscalls**, dominated by about **264K `mmap`** and **264K `munmap`** calls. After the refactor, the same 10-second samples drop to roughly **858 syscalls** on structure 11 and **1,283 syscalls** on structure 24, with only **19 `mmap`** and **2 `munmap`** calls in each run.

This confirms that the allocator churn from the old tempfile and per-candidate scratch allocation path was the dominant systems bottleneck.

### Full representative runs

| Structure | Binary | Wall time | Output lines |
|---|---|---:|---:|
| 24 | current | 25.29s | 103,274 |
| 24 | baseline | 63.98s | 103,274 |

Structure 24 therefore sees an end-to-end speedup of about **2.53x**.

For the heavier structure 11, the optimized binary still had not completed after 120 seconds and had emitted 200,573 lines. The baseline binary emitted only 76,484 lines in 60 seconds. That gives a conservative throughput improvement of about **1.79x** on this longer-running case, implying that any full structure-11 run should now take about **56%** of the old wall time.

### Sharding

Measured on structure 24 with two shards:

| Command | Wall time | Output lines |
|---|---:|---:|
| `./cws-5d.x -c5 -s24` | 25.29s | 103,274 |
| `./cws-5d.x -c5 -s24 -j2 -k1` | 12.28s | 46,874 |
| `./cws-5d.x -c5 -s24 -j2 -k2` | 12.71s | 56,400 |

The measured two-shard wall time is therefore about **2.0x** better than the optimized single-process run for a reasonably balanced structure.

### Runtime estimate

For exact measured work, canonical structure 24 drops from about **64s** to about **25s**, and to about **13s** with two shards.

For longer dim-5 canonical workloads, the measured throughput improvement falls in the **1.8x to 2.5x** range. A practical estimate for the total `-c5` processing time on one core is therefore:

- **before:** `T`
- **after:** about `T / 1.8` to `T / 2.5`

On balanced shardable workloads, two workers reduce that further to about `T / 3.6` to `T / 5.0` relative to the old single-process implementation.
| `Aux_vNF_Line` | 2.3% | 1.1M | Normal form VPM line processing |
| All other | 8.3% | — | — |

**78% of execution time is spent in `Find_Equations` subroutines** that repeatedly scan the full point list (up to 190K points). Each scan evaluates a 5D dot product (`Eval_Eq_on_V`) against every point. The normal form computation itself (`Make_Poly_Sym_NF` and children) accounts for only ~4% of total time.

Further speedup beyond compilation optimizations would require algorithmic changes to the point-scanning loops in `Vertex.c` (e.g., spatial indexing, point filtering, or SIMD batch evaluation).
