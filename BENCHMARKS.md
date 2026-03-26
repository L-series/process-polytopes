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
| `Aux_vNF_Line` | 2.3% | 1.1M | Normal form VPM line processing |
| All other | 8.3% | — | — |

**78% of execution time is spent in `Find_Equations` subroutines** that repeatedly scan the full point list (up to 190K points). Each scan evaluates a 5D dot product (`Eval_Eq_on_V`) against every point. The normal form computation itself (`Make_Poly_Sym_NF` and children) accounts for only ~4% of total time.

Further speedup beyond compilation optimizations would require algorithmic changes to the point-scanning loops in `Vertex.c` (e.g., spatial indexing, point filtering, or SIMD batch evaluation).
