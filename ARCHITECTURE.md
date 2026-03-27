# Polytope Classification Pipeline — Architecture & Analysis

This document describes the architecture, design decisions, benchmarks, and
operational guide for classifying all unique 5D reflexive polytopes from the
[ws-5d dataset](https://huggingface.co/datasets/calabi-yau-data/ws-5d/tree/main/reflexive)
(183 billion CWS across 4000 Parquet files, ~3.2 TB).

---

## Table of Contents

1. [Overview](#overview)
2. [Pipeline Architecture](#pipeline-architecture)
3. [Design Decisions](#design-decisions)
4. [Sorting Pre-Processing Analysis](#sorting-pre-processing-analysis)
5. [Hashing & Deduplication](#hashing--deduplication)
6. [Memory & Storage Strategy](#memory--storage-strategy)
7. [Parallelism & Load Balancing](#parallelism--load-balancing)
8. [Multi-Runner Setup](#multi-runner-setup)
9. [Benchmarks](#benchmarks)
10. [Projections for Full Dataset](#projections-for-full-dataset)
11. [Build & Run Guide](#build--run-guide)

---

## Overview

**Goal:** Find all unique 5D polytopes (up to GL(5,ℤ) isomorphism) generated
from ~183 billion combined weight systems (CWS).  For each unique polytope,
record:
- Its normal form (canonical representative under GL(5,ℤ))
- How many CWS generate it (frequency)
- Metadata: vertex count, facet count, Hodge numbers, etc.
- An example CWS that generates it

**Method:**
1. Read CWS from Parquet files
2. For each CWS, compute the polytope's **normal form** via PALP
3. Hash the normal form (xxHash128) and insert into a hash map
4. Deduplicate: if the hash already exists, increment the frequency counter
5. Periodically checkpoint the hash map to disk
6. At the end, write all unique polytopes to output Parquet files

---

## Pipeline Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                    Parquet Input File                        │
│              (46M CWS × 13 columns, ~800 MB)                │
└──────────────────────┬──────────────────────────────────────┘
                       │  Arrow columnar read (~2s)
                       ▼
┌─────────────────────────────────────────────────────────────┐
│                   Row Extraction                            │
│         6 weights + 7 metadata columns → CWSRow[]           │
└──────────────────────┬──────────────────────────────────────┘
                       │
                       ▼
┌─────────────────────────────────────────────────────────────┐
│              Dynamic Work-Stealing Pool (32 threads)         │
│                                                             │
│   ┌─────────┐ ┌─────────┐ ┌─────────┐     ┌─────────┐     │
│   │Thread 0 │ │Thread 1 │ │Thread 2 │ ... │Thread 31│     │
│   │         │ │         │ │         │     │         │     │
│   │ PALP    │ │ PALP    │ │ PALP    │     │ PALP    │     │
│   │Workspace│ │Workspace│ │Workspace│     │Workspace│     │
│   │         │ │         │ │         │     │         │     │
│   │ Local   │ │ Local   │ │ Local   │     │ Local   │     │
│   │HashMap  │ │HashMap  │ │HashMap  │     │HashMap  │     │
│   └─────────┘ └─────────┘ └─────────┘     └─────────┘     │
│         │           │           │               │           │
│         └───────────┴───────┬───┴───────────────┘           │
│                             │ merge under lock              │
│                             ▼                               │
│                    ┌────────────────┐                        │
│                    │ Global HashMap │                        │
│                    │  (xxHash128 →  │                        │
│                    │  PolytopeInfo) │                        │
│                    └───────┬────────┘                        │
└────────────────────────────┼────────────────────────────────┘
                             │
              ┌──────────────┴──────────────┐
              ▼                             ▼
     ┌───────────────┐            ┌──────────────────┐
     │  Checkpoint    │            │  Output Parquet   │
     │  (binary .ckpt)│            │  (ZSTD compressed) │
     └───────────────┘            └──────────────────┘
```

### Per-CWS Computation Path

For each CWS (6 weights w₀…w₅):

1. **`Make_CWS_Points`** — Enumerate lattice points of the polytope defined
   by the weight system.  This is the most expensive step, scaling with the
   number of lattice points (up to 190K for extreme cases).

2. **`Find_Equations`** — Compute vertices and facet equations.  Repeatedly
   scans all lattice points to find vertices (O(n_points × n_vertices²)).

3. **`Sort_VL`** — Sort vertex list for canonical ordering.

4. **`Make_Poly_Sym_NF`** — Compute the normal form of the vertex matrix
   under GL(5,ℤ) symmetry.  This is the unique canonical representative
   of the polytope's isomorphism class.

5. **`xxHash128`** — Hash the 5×nv normal form matrix to a 128-bit key.

6. **Hash map lookup** — O(1) amortised insert/lookup.

---

## Design Decisions

### Why link PALP as a library instead of calling poly.x as a subprocess?

| Approach | Throughput | Overhead |
|----------|-----------|----------|
| `poly.x` subprocess (GNU parallel) | ~6,000 CWS/s per core | Process fork + I/O parsing |
| PALP library (direct function calls) | ~7,500 CWS/s per core | None — direct memory access |

The library approach eliminates:
- Process creation overhead (fork + exec)
- Text serialisation of CWS input and NF output
- File I/O for intermediate data

It also enables thread-level parallelism within a single process, sharing
a single hash map for deduplication.

### Thread-safety patches to PALP

PALP was designed as a single-threaded CLI tool.  Two changes were needed:

1. **`Vertex.c:Make_New_CEqs`** — Contains `static CEqList Bad_C` and
   `static INCI Bad_C_I[]` used as scratch buffers.  Changed to
   `__thread` storage when `PALP_THREADSAFE` is defined.

2. **`inFILE` / `outFILE`** — Global FILE pointers.  Set to `/dev/null`
   at initialisation since we bypass PALP's I/O entirely.

### Why xxHash128 instead of the full normal form as key?

The normal form matrix is 5×nv longs (up to 5×64×8 = 2560 bytes).
Using it directly as a hash map key would:
- Waste memory (each key up to 2.5 KB vs 16 bytes for xxHash128)
- Slow down comparisons (memcmp of 2.5 KB vs 16 bytes)

xxHash128 provides:
- 128-bit collision resistance (birthday bound at 2⁶⁴ ≈ 10¹⁹)
- 40 GB/s hashing throughput on modern CPUs
- With ~500M expected unique polytopes, collision probability is ~10⁻²⁰

If paranoid, the checkpoint files can be post-processed to verify no collisions
exist by loading all NF matrices and comparing within hash buckets.

---

## Sorting Pre-Processing Analysis

**Question:** Does sorting the Parquet files by vertex count before
processing save computational time?

**Answer: No.**  Hash-based deduplication is O(1) per lookup regardless of
input order.  Sorting adds significant overhead:

| Factor | Impact |
|--------|--------|
| Sorting 3.2 TB of data | ~6-12 hours (external merge sort) |
| Hash map lookup | O(1) amortised, order-independent |
| Cache locality | Natural random order provides good load balance |
| Dedup benefit | None — hash map finds duplicates in any order |

The original hypothesis was that sorting by vertex count would allow
batch-processing polytopes of the same size, enabling early termination
when all polytopes of a given vertex count have been seen.  However:

1. The vertex count alone is not sufficient to determine isomorphism
2. The hash map already handles deduplication efficiently
3. The cost of sorting 3.2 TB far exceeds any potential savings
4. Natural input order provides better load balance for work-stealing

**When sorting IS useful:**
- If you want to process vertex-count groups in isolation (e.g., for
  separate analysis of 6-vertex vs 40-vertex polytopes)
- If you need sorted output (the output Parquet can be sorted after
  classification at much lower cost since it's ~1000× smaller)

---

## Hashing & Deduplication

### Hash Map Structure

```
Key:   Hash128 { uint64_t lo, hi }     — 16 bytes
Value: PolytopeInfo {                  — ~50 bytes
         uint64_t count;               — CWS frequency
         int32_t  first_weights[6];    — example CWS
         int16_t  vertex_count;
         int16_t  facet_count;
         int32_t  point_count;
         int32_t  dual_point_count;
         int16_t  h11, h12, h13;
       }
```

Total per entry including hash map overhead: ~130 bytes.

### Thread-Local Maps → Global Merge

Each thread maintains its own `std::unordered_map`.  After processing
a file, all thread-local maps are merged into the global map under a
single mutex.  This design:

- Eliminates lock contention during the hot processing loop
- Allows each thread's map to be cache-local
- Merge cost is amortised over millions of rows per file

---

## Memory & Storage Strategy

### In-Memory Capacity

With 96 GB RAM and ~130 bytes per entry:

| Unique Polytopes | Hash Map Size | Fits in RAM? |
|-----------------|--------------|-------------|
| 16M (1 file)    | 2.1 GB       | ✅ easily    |
| 100M            | 13 GB        | ✅ yes       |
| 500M            | 65 GB        | ✅ tight     |
| 700M            | 91 GB        | ⚠️ limit     |

### Checkpoint Strategy

Binary checkpoint files are written every 10 input files.  Format:

```
[uint64_t n_entries]
[Hash128 key₀][PolytopeInfo value₀]
[Hash128 key₁][PolytopeInfo value₁]
...
```

This enables:
- **Resume after crash:** Load last checkpoint, skip processed files
- **Multi-runner merge:** Combine checkpoints from different machines
- **Incremental updates:** Process new files and merge with existing results

Checkpoint size: ~66 bytes × N entries (e.g., 100M entries = 6.6 GB).

### What if unique polytopes exceed RAM?

If the total unique count exceeds ~700M (unlikely based on deduplication
rates observed), options include:

1. **Shard by hash prefix:** Split the hash map into 256 shards (by
   high byte of `hash_lo`).  Process all files, writing each shard to
   disk.  Then load shards one at a time for output.

2. **Two-pass approach:** First pass counts unique hashes (using a
   Bloom filter or HyperLogLog for approximate counting).  Second pass
   processes only CWS that hash to a specific shard range.

3. **RocksDB backend:** Replace `std::unordered_map` with a RocksDB
   instance for disk-backed hash storage.  This would reduce throughput
   by ~10× but handle unlimited unique polytopes.

Based on 1-file results (16M unique from 46M CWS with 65% dedup), and
assuming cross-file dedup increases as more files are processed, the
likely total is **50M–200M unique polytopes**, well within 96 GB RAM.

---

## Parallelism & Load Balancing

### Dynamic Work-Stealing

The processing cost per CWS varies by 1000×:

| Point Count | Approx. Time per CWS | % of Dataset |
|-------------|----------------------|-------------|
| < 100       | ~10 µs               | 59%         |
| 100-500     | ~50 µs               | 34%         |
| 500-5K      | ~200 µs              | 6.3%        |
| 5K-50K      | ~5 ms                | 0.6%        |
| 50K+        | ~50 ms               | 0.03%       |

Static partitioning (equal chunks) leads to severe imbalance: the thread
that gets the 100K-point polytopes takes 50× longer than the others.

**Solution:** Dynamic work-stealing with atomic block counter.

```cpp
std::atomic<int64_t> next_block{0};
constexpr int64_t BLOCK_SIZE = 1024;

// Each thread:
for (;;) {
    int64_t b = next_block.fetch_add(1);
    if (b >= n_blocks) break;
    process_block(b * BLOCK_SIZE, min((b+1) * BLOCK_SIZE, n));
}
```

Threads that finish fast blocks immediately grab the next one.
Block size of 1024 balances:
- Low contention on the atomic counter (~45K increments for 46M rows)
- Good cache locality within each block
- Fast rebalancing when a thread hits an expensive polytope

### Thread Count Optimization

| Threads | Throughput (CWS/s) | CPU Usage | Notes |
|---------|-------------------|-----------|-------|
| 1       | 1,730             | 100%      | Baseline |
| 8       | ~12K              | 800%      | Linear scaling |
| 16      | ~20K              | 1600%     | Near-linear |
| 32      | 241K              | 2365%     | Hyperthreading helps |

The Ryzen 9 7950X3D has 16 physical cores / 32 logical threads.
Using all 32 threads gives ~14× speedup over single-threaded.
The sub-linear scaling is due to:
- Memory bandwidth contention (hash map updates)
- L3 cache sharing across CCDs
- Work imbalance on heavy polytopes (mitigated by work-stealing)

---

## Multi-Runner Setup

### Architecture

```
┌────────────┐  ┌────────────┐  ┌────────────┐
│  Runner 0  │  │  Runner 1  │  │  Runner 2  │
│ Files 0-1332│  │Files 1333-2665│ │Files 2666-3999│
│            │  │            │  │            │
│ checkpoint │  │ checkpoint │  │ checkpoint │
│  files     │  │  files     │  │  files     │
└──────┬─────┘  └──────┬─────┘  └──────┬─────┘
       │               │               │
       └───────────────┼───────────────┘
                       │ collect + merge
                       ▼
              ┌─────────────────┐
              │   Merge Step    │
              │                 │
              │ Load all .ckpt  │
              │ Merge hash maps │
              │ Write final     │
              │ Parquet output  │
              └─────────────────┘
```

### Usage

```bash
# Edit scripts/run_distributed.sh — set RUNNERS array:
RUNNERS=(
    "machine1.local:0:1332"
    "machine2.local:1333:2665"
    "machine3.local:2666:3999"
)

# Launch all runners
./scripts/run_distributed.sh launch

# Monitor progress
./scripts/run_distributed.sh status

# After all complete, merge results
./scripts/run_distributed.sh merge
```

### Requirements per runner

- Same binary built with `./src/classify/build.sh pgo`
- Access to the parquet files (NFS mount, local copy, or download range)
- Sufficient RAM for its share of the hash map (~30 GB each)

### Merge Protocol

1. Each runner writes checkpoint files to its output directory
2. The merge step loads all checkpoints into a single hash map
3. Entries with matching hashes have their counts summed
4. Final output is written as a single Parquet file

Merge is fast (minutes) since only unique entries need to be combined,
and each runner's checkpoint is ~3-20 GB.

---

## Benchmarks

### Single File: ws-5d-reflexive-0000.parquet

**Environment:** AMD Ryzen 9 7950X3D, 96 GB RAM, Arch Linux

| Configuration | Throughput | Wall Time | CPU Usage |
|--------------|-----------|-----------|-----------|
| 1 thread, no optimisation | 1,730 CWS/s | 26,780s | 100% |
| 32 threads, contiguous chunks | 325K CWS/s | 142s | ~2900% |
| 32 threads, work-stealing | **241K CWS/s** | **192s** | 2365% |
| 32 threads, work-stealing + PGO | 236K CWS/s | 196s | 2365% |

*Note: The contiguous approach showed higher throughput but had a long
tail — some threads finished 2× earlier than others.  Work-stealing
provides more consistent completion times across files.*

**File results:**
| Metric | Value |
|--------|-------|
| Total CWS | 46,320,497 |
| Unique polytopes | 15,998,759 |
| Duplication rate | 65.5% |
| Most common polytope | 162,613 occurrences |
| RSS memory | 5.1 GB |

### Duplication by Vertex Count

| Vertex Count | Input CWS | Unique Polytopes | Dedup Rate |
|-------------|-----------|-----------------|-----------|
| 6 | 327,520 | 2,862 | 99.1% |
| 7 | 1,218,255 | 21,869 | 98.2% |
| 8 | 2,476,004 | 94,161 | 96.2% |
| 12 | 5,214,537 | 1,369,197 | 73.7% |
| 15 | 3,821,620 | 1,915,186 | 49.9% |
| 20 | 802,154 | 648,008 | 19.2% |
| 30 | 2,705 | 2,695 | 0.4% |
| 43 | 1 | 1 | 0% |

**Key insight:** Low vertex-count polytopes are massively duplicated
(a 6-vertex polytope appears ~114× on average), while high vertex-count
polytopes are nearly all unique.  This is expected from the combinatorial
structure: there are very few distinct polytopes with few vertices.

---

## Projections for Full Dataset

### Time

| Scenario | Time per file | Total (4000 files) | With 3 machines |
|----------|-------------|-------------------|----------------|
| Current (241K CWS/s) | 3.3 min | 9.3 days | **3.1 days** |
| PGO (236K CWS/s) | 3.3 min | 9.3 days | **3.1 days** |
| Optimistic (300K CWS/s) | 2.6 min | 7.1 days | **2.4 days** |

*Note: PGO (Profile-Guided Optimisation) provides no measurable benefit
for this workload — the bottleneck is PALP's integer arithmetic and
matrix operations, which are not branch-prediction-limited.*

### Memory

Assuming cross-file deduplication increases (likely, since many CWS in
different files will generate the same polytope):

| Cross-file dedup | Total unique | Hash map RAM |
|-----------------|-------------|-------------|
| 50% | ~32B → ~200M unique | 26 GB |
| 75% | ~32B → ~100M unique | 13 GB |
| 90% | ~32B → ~50M unique | 6.5 GB |

All scenarios fit comfortably in 96 GB RAM.

### Storage

| Output | Size |
|--------|------|
| Final Parquet (100M unique polytopes) | ~2-5 GB |
| Checkpoint files (per runner) | ~3-20 GB |
| Total intermediate storage | ~50-100 GB |

---

## Build & Run Guide

### Prerequisites

```bash
# Arch Linux
sudo pacman -S cmake ninja gcc arrow parquet

# Ubuntu/Debian
sudo apt install cmake ninja-build gcc \
    libarrow-dev libparquet-dev libarrow-dataset-dev
```

### Build

```bash
cd src/classify

# Regular build
./build.sh build

# PGO build (no measurable benefit for this workload)
./build.sh pgo
```

### Run

```bash
# Benchmark on first 100K rows
./build/classifier --input ./samples/reflexive --output ./results \
    --benchmark 100000 --threads 32

# Process all files in a directory
./build/classifier --input /data/ws-5d-reflexive --output ./results \
    --threads 32

# Process a file range (for multi-runner)
./build/classifier --input /data/ws-5d-reflexive --output ./results \
    --start 0 --end 999 --threads 32

# Resume from checkpoint
./build/classifier --input /data/ws-5d-reflexive --output ./results \
    --resume --threads 32

# Merge results from multiple runners
./build/classifier --merge ./runner-checkpoints --output ./merged-results
```

### Validate

```bash
# Test classifier matches poly.x -N on 500 random CWS
./scripts/validate.sh 500
```
