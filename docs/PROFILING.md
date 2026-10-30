# Local CPU performance history

The [dashboard](performance/index.html) tracks native time/CWS, CWS/s and
per-function instruction counts. [Full latest measurements](performance/latest.json),
[history](performance/history.json), [function CSV](performance/functions.csv),
[call edges](performance/callgraph.json) and the
[recorded function tables](performance/function-map.md) retain the measured data.

## Run and record locally

From the repository root:

```sh
nix develop .#profiling -i -c python3 scripts/profile-cws.py \
  --machine workstation --output /tmp/cws-run
python3 scripts/report-cws-profile.py --run /tmp/cws-run/run.json \
  --history docs/performance/history.json --output docs/performance
```

Use a fresh output directory for each run. Without Nix, provide GCC, binutils
(`addr2line`), Python 3 and Valgrind on PATH. `--valgrind PATH` selects a
nonstandard Valgrind installation. `--limit 8 --repetitions 1` is a smoke run;
smoke and incomplete runs cannot be appended to performance history.

Review and commit the generated site/data to publish a local measurement. The
GitHub workflow renders these committed records and publishes the default-branch
site through Pages. It never benchmarks GitHub runners. Repository Pages must
use GitHub Actions as its source. PRs produce a preview artifact.

Keep complete run directories, binaries, build logs and raw profiles outside
Git. The site needs the JSON measurements, not those large local archives.

## Measured work

The profiler compiles project sources in `src/cws/` with PALP's `cws.c` and
geometry routines. No separate `cws_generate.c` exists. The point and clock
harnesses are in `src/benchmarks/`; the HTML template is in `src/reporting/`.
Builds and profiling outputs stay outside PALP's source directory.

Three geometry workloads process all 10,000 entries in each
[corpus group](data/cws-benchmarks/README.md). They preload input matrices, build
CWS objects, enumerate primal lattice points and construct/check the primal
hull. Every row's point/vertex counts, dimension, IP and reflexivity label must
match the corpus metadata. Dual construction and normal forms are excluded.

Two generation workloads enumerate bounded historical types 3 and 11 using the
first 12 committed pool rows per dimension (all three available 2D rows).
Their boundary includes pool loading, selections, prefix permutations, CWS
assembly, primal IP, accepted-row dual construction and output. Native,
instrumented and Callgrind executions must produce identical accepted output.
Fixed-range elapsed time is important when pruning changes the candidate count.

The separate `scripts/benchmark-cws-descriptors.py` runner measures complete
selected descriptor ranges; its [original record](experiments/cws-descriptors.md)
uses complete types 5 and 7. Build `build/cws.x` with root `make` first.

## Metrics and interpretation

- Native optimized runs report means of five repetitions after one warmup,
  time/CWS and CWS/s. Geometry also times `Make_CWS_Points` and `IP_Check`.
- A separate `-finstrument-functions` build reports calls, exclusive/inclusive
  function times and means per call/CWS. It has substantial overhead; these are
  instrumented times, not estimates of uninstrumented per-function time.
- Callgrind records executed instruction reads (`Ir`) in the optimized binary.
  Exclusive counts reconcile with its total. Inclusive costs overlap, including
  recursive contexts. Inlined functions may have no separately attributable
  instruction cost; missing values remain blank.
- Optional hardware PMU measurements report user-space retired instructions
  for point/hull stages. Their intervals include counter/timer wrappers. Lack
  of PMU access is recorded explicitly. Callgrind remains required.

Instruction counts are not CPU cycles. No CPU-cycle metric has been added by
this cleanup. See the [Callgrind manual](https://valgrind.org/docs/manual/cl-manual.html)
for event semantics. These measurements do not prove mathematical correctness.

## Comparing runs

Each run records root/PALP commits, dirty state, source/harness hashes, compiler,
flags, CPU, machine label, workload and measurement configuration. Series are
separated by these measurement-related settings, including the producer-script
hash, repetitions, warmups, methodology and execution configuration. Use
`--methodology serial-v1` and `--measurement-note "description"` to explain a
protocol change. Notes alone do not split a series. Both regressions and
improvements remain visible; no downward trend is manufactured.

The current runner is serial. Future isolated-worker runs must record their
worker count and partitioning and use total completed CWS divided by elapsed
wall time across all workers for aggregate throughput. They must form separate
series from serial measurements. Adding workers need not reduce instructions/CWS.

Existing baselines are historical working-tree runs on a Ryzen 9 7950X3D with
GCC 15.2.0 and 16.2.1, before the current source layout. Their original source
identifiers, function names and measurements remain unchanged after the Git
rewrite. They are not measurements of the new commit IDs. New full local runs
will establish baselines for the current implementation. CPU frequency and
competing system load were not fixed in the historical runs.

PALP-linked binaries retain PALP's GPLv3 requirements.
