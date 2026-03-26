# Benchmarks

This document tracks performance benchmarks for the polytope processing pipeline across development milestones.

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
