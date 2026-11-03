# Run every combined structure with mandatory deferred completion

The scheduler runs structures 2 through 47. Structure 1 is outside this run;
the user reports it already complete. Structure 3 has eight disjoint shards,
one on each of eight visible CUDA devices. The remaining 45 structures each
have one job, giving **53 jobs**. Each device has one sequential worker, starting
with its structure-3 shard. Every job runs the exact CPU completion driver on
all of its deferred records before that worker starts its next job.

Build the production GPU binary for the actual GPU architecture using the
Makefile's `gpu` target (or `gpu-type3` for the optional fused structure-3 build).
Build the separate CPU completion driver:

```sh
bash scripts/build_cpu_completion.sh
```

Inspect all commands without creating output or launching GPU/CPU jobs:

```sh
python3 scripts/run_cws_production.py \
  --binary build/cws-gpu.x \
  --cpu-driver build/cpu-completion/complete_deferred \
  --run-root production/cws-all-001 \
  --devices 0 1 2 3 4 5 6 7 \
  --dry-run
```

Run with the same command after removing `--dry-run`. The run directory must
not already exist. Device numbers are the CUDA-visible IDs; custom IDs must
still name exactly eight distinct devices. The scheduler reserves devices
within this run, not against unrelated processes or other scheduler instances.

The default capacities are `--chunk-capacity 100000 --slice-capacity 10000
--np-cap 256`, with `--threads 128 --blocks 0`. All are passed unchanged to the
single-job runner. CPU completion defaults to one CPU worker per GPU job and
20,000 records per CPU chunk; use `--cpu-workers` and `--cpu-chunk-rows` to change
those limits. There can be eight active CPU-completion jobs at once.

## Completion and failures

`production-plan.json` records all 53 commands and the binary, CPU-driver,
weight-pool and reference-count hashes. Each shard directory contains the
single-job runner's GPU outputs, logs, receipt and CPU-completion work directory.
The scheduler's own logs are in `scheduler-logs`.

If a job fails, queued work stops and already running workers finish their
current jobs. All original outputs and logs remain. A failed scheduler exits
nonzero and does not run the final coverage audit. It never creates
`production-execution.json` for that failed run.

After all 53 GPU/CPU jobs succeed, the scheduler invokes:

```sh
python3 scripts/audit_cws_run.py --run-root production/cws-all-001 --require-cpu
```

Only a successful audit that produces `execution-coverage.json` allows the
scheduler to write `production-execution.json`. This accounts for execution and
files; **it is not a complete formal no-omissions certificate**. The remaining
source-refinement obligations and proof boundaries are documented in
[GPU_PIPELINE_STATUS.md](../proofs/GPU_PIPELINE_STATUS.md).

## Resume CPU work without losing deferred records

The scheduler deliberately does not overwrite or automatically reuse an old run.
For a job whose GPU receipt exists but CPU completion failed, retain its
`deferred.ovf`, `job.json` and `cpu-completion` directory. Resume the existing
CPU chunks with the same driver and chunk size, for example:

```sh
python3 scripts/complete_deferred.py \
  --driver build/cpu-completion/complete_deferred \
  --work-dir production/cws-all-001/s03-shard-000-of-008/cpu-completion \
  --expected-rows N --chunk-rows 20000 --workers 1 \
  production/cws-all-001/s03-shard-000-of-008/deferred.ovf
```

Replace `N` with that job's **GPU receipt's deferred count**, not a count inferred
from whichever input files happen to be present. Completed chunks are checked
and reused; failed chunks are retried. A different driver, input list, expected
count or chunk size requires a new completion work directory. Original deferred
files are never deleted.

A manual CPU resume does not rewrite the single-job runner's binding in
`job.json`. Once every one of the 53 GPU receipts exists, the final auditor can
instead complete and bind the entire deferred corpus globally:

```sh
python3 scripts/audit_cws_run.py \
  --run-root production/cws-all-001 \
  --cpu-driver build/cpu-completion/complete_deferred \
  --workers 8 --chunk-rows 20000 --require-cpu
```

That may repeat CPU work already completed per job. When it creates a global
`cpu-completion/accepted-cpu.txt`, use that global CPU output once, rather than
also adding the per-job CPU outputs. Preserve failed GPU attempts separately;
a missing or failed GPU receipt cannot be repaired by completing its partial
deferred file. The auditor requires the full 53-job identity set.
