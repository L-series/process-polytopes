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