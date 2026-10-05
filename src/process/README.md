# Single-weight dataset post-processing

These tools preserve the existing post-processing work for the single-weight
dimension-5 classifier dataset. They are not a combined-CWS classifier:
`process_polytopes` reads `first_weight0` through `first_weight5` and builds a
single weight row (`nw=1`). Do not use it to replay combined-CWS records, which
require their full matrices and degrees.

`process_polytopes` recomputes normal forms and geometric/Hodge invariants using
upstream PALP v2.21. `merge_computed` joins computed shards back to the original
dataset, replacing the Hodge columns with int32 values. `concat_parquet` joins
ordered part files and `rg_count` reads the row-group count.
`repair_ws_dataset` transfers corrected invariants to the sieved weight dataset
by its normal-form key; `convert_nf_vertices` converts packed vertex matrices
to nested lists. Related SLURM and submission scripts live in `scripts/`.

## Build

The clean PALP checkout is an external build prerequisite, not a vendored copy:

```bash
git -C PALP worktree add --detach ../PALP-clean v2.21
bash src/process/build.sh
```

The build uses GCC/G++, pkg-config, and Arrow/Parquet C++ libraries. It uses
`CONDA_PREFIX` when set and otherwise the existing local micromamba environment
path. Generated files live under `src/process/build/`.

The tools are preserved as existing implementation work, not certified
production releases. The SLURM scripts use cluster-specific defaults. Before
production use, add fixture tests for recomputation, schema preservation, shard
coverage, merge alignment, normal-form key joins, and failures. Extending replay
to combined-CWS inputs requires an explicit schema-aware implementation.
