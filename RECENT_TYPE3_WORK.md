# Recent Type-3 PALP Work

Date: 2026-04-18

## Scope

This note summarizes the recent work on PALP dim-5 type-3 `(5,5)` combined
weight-system generation, mainly around `cws -c5 -s3`, profiling, and
`Make_CWS_Points` hot-path analysis.

## Branches And Repo State

- Root repo branch: `type3-per-cws-timers`
- PALP submodule branch: `type3-per-cws-timers`
- Root repo current status at time of writing:
  - `.env` modified locally and intentionally not committed
  - `PALP` submodule pointer dirty because PALP has new uncommitted work
- PALP current status at time of writing:
  - `Coord.c` modified with deep-trace instrumentation helpers

## Recent Root Commits

- `8030dbb` Update PALP pointer for timing summary mode
- `1edd80a` Update PALP pointer after benchmark ignore cleanup
- `477f256` Checkpoint runner, analysis, and PALP updates

## Recent PALP Commits

- `9d7d260` Ignore local PALP benchmark corpora
- `df5e477` Add IP-only mode for combined CWS generation
- `793c44c` Optimize dim-5 canonical CWS enumeration and add sharding
- `9e13543` Fix unsound dim-5 CWS slot-order pruning

## What Was Implemented

### 1. Safe checkpointing and repo cleanup

- Created the working branch in both the root repo and the PALP submodule.
- Committed safe pending work.
- Deliberately kept `.env` local because it contains a live token change.
- Added ignore rules for generated local artifacts:
  - root `.gitignore`: LaTeX/Lean-generated files
  - `PALP/.gitignore`: local `cws/` corpora

### 2. Runtime `-I` mode for combined CWS generation

- Added a real runtime `-I` mode in `PALP/cws.c`.
- This stops the pipeline after `Make_CWS_Points` and `IP_Check`, skipping the
  later full polytope completion path.
- Purpose: isolate the front-end combined-CWS cost from later geometry.

### 3. Runtime `-T` timing mode for combined CWS generation

- Added `-T` timing support in `PALP/cws.c`.
- This records per-candidate timings for:
  - `Make_CWS_Points`
  - `IP_Check`
  - `Complete_Poly`
- Added an optional hook in `PALP/Vertex.c` so `Complete_Poly` contributes to
  the same timing summary.
- The summary is printed to `stderr` after a combined-CWS run.

### 4. Deep trace path for `Make_CWS_Points`

- Pure `perf` was not enough because optimized builds inline too much of the
  relevant subtree.
- Built a one-off no-inline instrumented binary `PALP/cws-trace.x` using
  `-finstrument-functions`.
- Added trace hook support in `results/traces/tracehooks.c`.
- Added optional trace-only region timing and `PD_Floor` caller-site aggregation
  in `PALP/Coord.c`.
- The extra `Coord.c` instrumentation is guarded through weak hooks and only
  activates when the trace helper is linked and active.

### 5. Summary-only trace mode

- Added `PALP_TRACE_SUMMARY_ONLY=1` support to `results/traces/tracehooks.c`.
- This suppresses the 180k-line raw call log while keeping the appended region
  summary and `PD_Floor` caller-site summary.
- The compact artifact for the target call is:
  - `results/traces/make_cws_points_call_2197_summary_only.txt`
- That file is only 30 lines long and is now the practical default artifact for
  future targeted `Make_CWS_Points` traces.

## Key Measurements

### Combined-CWS timing summary on real type-3 work

For the `wf4-d1-20` shard, full mode showed approximately:

- candidates: `82410`
- IP successes: `32933`
- `Make_CWS_Points total`: `4.244660 s`
- `IP_Check total`: `1.579320 s`
- `Complete_Poly total`: `0.857085 s`
- timed-stage total: `6.681065 s`

For the same shard in IP-only mode:

- `Make_CWS_Points total`: `4.215206 s`
- `IP_Check total`: `1.563856 s`
- timed-stage total: `5.779062 s`

Conclusion from that stage: `Make_CWS_Points` dominates the explicitly timed
front-end work.

### Targeted deep trace of `Make_CWS_Points`

Target trace:

- workload: `-c5 -I -n2 cws/wf4-d1-10.txt cws/wf4-d1-10.txt -s3 /dev/null`
- target call: `Make_CWS_Points` call `2197`

Generated artifacts:

- raw call tree: `results/traces/make_cws_points_call_2197.txt`
- first per-function aggregate: `results/traces/make_cws_points_call_2197.summary.tsv`
- enriched raw call tree with appended summaries:
  `results/traces/make_cws_points_call_2197_deep.txt`
- compact summary-only trace:
  `results/traces/make_cws_points_call_2197_summary_only.txt`
- extracted region summary:
  `results/traces/make_cws_points_call_2197_deep.regions.tsv`
- extracted `PD_Floor` caller-site summary:
  `results/traces/make_cws_points_call_2197_deep.pd_floor_sites.tsv`

## Main Findings From The Deep Trace

The trace build inflates absolute timings, but it is reliable for structure,
relative weight, and call counts.

### Region split inside `Make_CWS_Points`

From `make_cws_points_call_2197_deep.regions.tsv`:

- `enumeration_total`: `123.414382 ms`
- `enum.tighten_bounds`: `78.303023 ms`
- `enum.seed_bounds`: `42.921631 ms`
- `enum.post_tighten_update`: `0.304583 ms`
- `enum.emit_points`: `0.281592 ms`
- `make_basis`: `0.070513 ms`
- `init_last_coord_bounds`: `0.003085 ms`
- `compute_xmax`: `0.000081 ms`
- `build_amin`: `0.000040 ms`
- `compute_x0`: `0.000040 ms`

Interpretation:

- `Make_CWS_Points` is overwhelmingly dominated by the main enumeration loop.
- Inside that loop, the work is almost entirely in bound seeding and bound
  tightening.
- Basis construction, `X0`, `Amin`, and `Xmax` are negligible by comparison.

### `PD_Floor` caller-site split

From `make_cws_points_call_2197_deep.pd_floor_sites.tsv`:

- total `PD_Floor` calls: `90774`
- `main_loop:seed_bounds:xmin`: `16104` calls
- `main_loop:seed_bounds:xmax`: `16104` calls
- `main_loop:tighten_pos_upper`: `14641` calls
- `main_loop:tighten_neg_upper`: `14641` calls
- `main_loop:tighten_neg_lower`: `14641` calls
- `main_loop:tighten_pos_lower`: `14641` calls
- only `2` calls came from the initial last-coordinate bound setup

Interpretation:

- `PD_Floor` traffic is almost entirely generated by the main-loop bound logic.
- If optimizing this path, the real target is the repeated bound-generation code
  inside `Make_CWS_Points`, not a coarse boundary around all of `Coord.c`.

## Practical Conclusion So Far

- Batching at a broad `Coord.c` boundary is not the first thing to do.
- The hot path is much narrower:
  - the main-loop seed-bound calculations
  - the repeated tighten-bound updates
  - the associated `PD_Floor` calls
- That is the place where an actual CPU-side optimization pass should start.

## Optimization Attempt That Was Rejected

- Tried a residual-cache optimization inside `Make_CWS_Points` that replaced the
  repeated inner `for (k = j + 1; ...)` offset reconstruction with a lazily
  rebuilt per-level cache.
- Result on the real optimized binary was worse, not better.

Benchmark used for the decision:

- command:
  `./cws.x -c5 -T -I -n2 cws/wf4-d1-20.txt cws/wf4-d1-20.txt -s3 -j32 -k1 /dev/null`
- pre-change `Make_CWS_Points total`: `4.285342 s`
- cached version `Make_CWS_Points total`: `4.575400 s`
- regression: `+0.290058 s` = `+6.77%`
- after revert `Make_CWS_Points total`: `4.282939 s`

Conclusion:

- `PD_Floor` is already inlined in the optimized object code.
- The per-level cache attacked the wrong constant and added more overhead than
  it removed.
- The cache optimization was reverted immediately.

- Tried a second source-level pass that replaced the tiny `for (k = j + 1; ...)`
  tail-sum loops with a small-dimension helper and reused that tail sum in the
  `R == 0` branch.
- Result on the same real optimized benchmark was again worse.

Benchmark used for the decision:

- pre-change `Make_CWS_Points total`: `4.283681 s`
- tail-sum helper version `Make_CWS_Points total`: `4.345386 s`
- regression: `+0.061705 s` = `+1.44%`

Conclusion:

- Even a smaller arithmetic cleanup of the hot loop did not help wall time.
- The hot path is sensitive enough that source-level loop reshaping needs to be
  justified by measurement, not intuition.
- This tail-sum optimization was also reverted.

## CPU Codegen Findings

- GCC vectorization diagnostics for `Coord.c` show that the hot
  `Make_CWS_Points` loops are **not vectorized**.
- The relevant messages on the bound-tightening loops were:
  - `not vectorized: vectorization is not profitable`
  - other nearby loops are blocked by control flow or unsupported scalar types
- In practice this means SIMD is not the limiting lever for the current hot
  path.

### Native-codegen test

Tried a machine-specific build on the local CPU with:

- `-O3 -march=native -mtune=native`

Result on the same type-3 shard benchmark:

- default restored build `Make_CWS_Points total`: `4.283681 s`
- native build `Make_CWS_Points total`: `5.952188 s`

Conclusion:

- Native codegen on this machine was dramatically worse for this workload.
- The default generic `-O3` build remains the best measured choice here.

### PGO test

Tried profile-guided optimization using the representative type-3 IP-only shard
as the training run.

Result on the same benchmark:

- restored default build `Make_CWS_Points total`: `4.283681 s`
- PGO build `Make_CWS_Points total`: `4.286460 s`

Conclusion:

- PGO was effectively flat to slightly worse on this workload.
- No compiler-side CPU acceleration tested so far beats the restored default
  `-O3` build.

## Current Best Measured Type-3 Generation Rate

Current restored default benchmark command:

- `./cws.x -c5 -T -I -n2 cws/wf4-d1-20.txt cws/wf4-d1-20.txt -s3 -j32 -k1 /dev/null`

Measured result:

- candidates: `82410`
- `Make_CWS_Points total`: `4.283681 s`
- `IP_Check total`: `1.569155 s`
- timed-stage total: `5.852836 s`

That is the current best measured CPU result from this branch.

## Refreshed Runtime Estimate

The repo does not currently contain an exact full type-3 emitted-candidate count
for builtin structure `-c5 -s3`.

The only global count available here is the paper-side heuristic multiset
baseline:

- `A_5 = 184,026`
- `\binom{A_5 + 1}{2} = 16,932,876,351`

Using the current restored shard timing as a heuristic per-candidate rate gives:

### IP-only front-end estimate

- per-candidate timed stage: about `7.10e-05 s`
- single worker: about `1,202,589 s` = `334.1 h` = `13.9 days`
- 32 perfectly balanced workers/shards: about `37,581 s` = `10.44 h`

### Full-mode estimate

Using the earlier full-mode shard timing (`6.681065 s` on the same 82,410-candidate shard):

- per-candidate timed stage: about `8.11e-05 s`
- single worker: about `1,372,766 s` = `381.3 h` = `15.9 days`
- 32 perfectly balanced workers/shards: about `42,899 s` = `11.92 h`

### Downstream classifier estimate

From `results/bench-full-v5.log` on the 32-thread Ryzen 7950X3D run:

- `46.320497M` CWS classified in `191.9 s` (`3.2 min`)

So if generation dominates, the classifier stage is comparatively small.

Important caveat:

- the `16,932,876,351` figure is explicitly a heuristic multiset baseline, not
  the exact emitted type-3 candidate count
- real wall time scales with the true candidate count, shard imbalance, and
  output volume

## Persistent Dim-5 Cache Work

Implemented persistent disk-backed dim-5 cache reuse in `PALP/cws.c`.

What is now cached:

- builtin dim-5 size-5 base pool for `Make_5_CWS`
- builtin dim-5 selection pools keyed by `(simplex_size, shared_count)`
- explicit-file dim-5 selection pools keyed by input path plus file size/mtime
  and `shared_count`

Default cache directory:

- `.palp-dim5-cache` in the current working directory
- override with `PALP_DIM5_CACHE_DIR=/path/to/cache`

Validation done so far:

- representative sampled explicit-file shard:
  - command:
    `./cws.x -c5 -T -I -n2 cws/wf4-d1-20.txt cws/wf4-d1-20.txt -s3 -j32 -k1 /dev/null`
  - produced cache file:
    `dim5-v1-input-c0ea5fe306f39d9f-shared3.weights`
  - size: about `16K`
  - entries: `1116`
- full published size-5 pool path:
  - command used to materialize cache:
    `./cws.x -c5 -I -n2 cws/wf4-all.txt cws/wf4-all.txt -s3 -j1000000 -k1 /dev/null`
  - produced cache file:
    `dim5-v1-input-cce4e1dd5b1a3283-shared3.weights`
  - size: about `36M`
  - entries: `1,833,327`
- warm-path verification:
  - `strace -e trace=file` on the warmed full-input run showed an immediate
    `openat(... dim5-v1-input-cce4e1dd5b1a3283-shared3.weights, O_RDONLY)`
  - this confirms the overlap-3 selection pool is loaded from cache instead of
    being rebuilt in each worker

Important interpretation:

- the existing `-T` summary measures per-candidate geometry (`Make_CWS_Points`,
  `IP_Check`, `Complete_Poly`) and does **not** include the pre-enumeration
  cache build/load step
- so this cache work should improve repeated worker startup and multi-worker
  scaling without materially changing the reported per-candidate `-T` totals

## Early-Rejection Win Inside `Make_CWS_Points`

Implemented a semantics-preserving branch-prune in `PALP/Coord.c` inside the
hot `Make_CWS_Points` enumeration loop.

Change:

- after seeding `xmin[j]` / `xmax[j]`, immediately mark the branch infeasible if
  `xmin[j] > xmax[j]`
- while tightening remaining constraints for the same coordinate, stop scanning
  as soon as either:
  - the interval becomes empty (`xmin[j] > xmax[j]`), or
  - an `R == 0` constraint already sets `RangeFlag`

Why this is safe:

- the remaining logic in that loop only tightens bounds or finds additional
  contradictions
- once the interval is already empty, no later constraint can make it feasible
  again
- once `RangeFlag` is set, the branch already backtracks, so finishing the rest
  of the scan was redundant work

This is the first direct `Make_CWS_Points` hot-loop optimization in this work
that survived real benchmarking.

Representative benchmark:

- command:
  `./cws.x -c5 -T -I -n2 cws/wf4-d1-20.txt cws/wf4-d1-20.txt -s3 -j32 -k1 /dev/null`
- before change:
  - candidates: `82410`
  - IP successes: `32933`
  - `Make_CWS_Points total`: `4.291389 s`
  - `Timed-stage total`: `5.852065 s`
- after change:
  - candidates: `82410`
  - IP successes: `32933`
  - `Make_CWS_Points total`: `2.740807 s`
  - `Timed-stage total`: `4.303195 s`

Repeated post-change runs on the same shard stayed in the same range:

- `Make_CWS_Points total`: `2.714484 s` to `2.740807 s`
- `Timed-stage total`: `4.263432 s` to `4.303195 s`

So on this representative real workload the direct point-construction stage
improved by about `36%`, with unchanged candidate and IP-success counts.

Regression coverage rerun after the change:

- dim-5 overlap-3 two-file enumeration (`tests/input/4.2.7-cws-c5-overlap3.txt`)
- dim-5 generic structure 15 output invariants
- dim-5 structure 11 shared-prefix canonicalization invariants
- dim-5 builtin structure 5 sharding invariants

All of those matched the known-good outputs/invariants exactly.

## Larger `wf4-all` Branch-Death Measurement

Implemented aggregated branch-death counters in the existing `-T` timing path,
with the accounting active only when combined-CWS timing mode is enabled so the
normal hot path stays unchanged.

Large explicit-file measurement:

- command:
  `./cws.x -c5 -T -I -n2 cws/wf4-all.txt cws/wf4-all.txt -s3 -j1000000 -k1 /dev/null`
- candidates: `6,833,280`
- IP successes: `181,482`
- `Make_CWS_Points total`: `209.801055 s`
- `Timed-stage total`: `218.756689 s`

New branch summary from that run:

- branch prunes total: `32,794,577,204`
  - seed-empty: `30,518,967,705`
  - tighten-empty: `2,274,918,320`
  - zero-range: `691,179`
- singleton branches total: `1,979,093,525`
  - seed singleton: `1,898,485,272`
  - tighten singleton: `80,608,253`
  - remaining constraints after first singleton: `3,877,132,610`
- tighten checks:
  - nonzero: `2,850,186,127`
  - zero: `894,750`
  - skipped after early death: `62,540,775,105`

Interpretation:

- `R == 0` failures are negligible on this workload, so further work there is
  not a good speed target
- the dominant surviving cost is the seed-bound stage itself
- singleton branches are common enough to test, but only if the real wall-clock
  time agrees

## Singleton Fast Path That Was Rejected

Tried a follow-up optimization based on the new singleton counts: once a branch
coordinate collapsed to a single value, switch from further bound tightening to
direct constraint checks for the remaining constraints.

Result on the same large `wf4-all` shard was worse and was reverted.

- pre-change `Make_CWS_Points total`: `209.801055 s`
- singleton-fast-path `Make_CWS_Points total`: `212.080891 s`
- regression: `+2.279836 s` = about `+1.09%`

Conclusion:

- the measured singleton frequency was real, but the direct-check replacement
  still lost to the existing bound-tightening code in optimized builds
- the next safe optimization target should stay focused on the seed-bound path,
  not on `R == 0` handling or singleton-specialized scanning

## Current Uncommitted Work

- `PALP/cws.c`
  - contains the persistent dim-5 base/selection cache logic
  - contains the extended `-T` branch summary reporting for `Make_CWS_Points`
- `PALP/Coord.c`
  - contains the deep-trace region instrumentation and labeled `PD_Floor` site
    tracking used by the trace binary
  - contains the early branch-prune win inside `Make_CWS_Points`
  - contains the new `-T`-only branch accounting hooks used for the large
    `wf4-all` measurement
- `results/traces/tracehooks.c`
  - contains the special trace helper for `cws-trace.x`, including the new
    summary-only mode
- `results/traces/`
  - contains the raw and summarized deep trace artifacts listed above

## Recommended Next Steps

1. Add a summary-only trace mode so the region/site summaries can be collected
  without emitting the full 180k-line raw call log.
  Status: done.
2. Optimize the `Make_CWS_Points` inner bound loop directly, starting with the
  `seed_bounds` and `tighten_bounds` paths.
  Status: early branch-prune landed; residual-cache and singleton-fast-path
  attempts regressed and were reverted.
3. Use the new `-T` branch summary on additional representative shards only when
  a candidate optimization targets the seed-bound path directly.