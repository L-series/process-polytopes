# Experiment index

Experiment records and the manifest-backed runner are planned. Historical CPU
and GPU notes remain on their archival branches; this index does not certify
their results or introduce new speedup claims.

Each record will identify the question, code/source revisions, correctness
evidence, kept/optional/rejected/proposed decision, measured boundary, raw
measurement provenance, missing metadata, and integration commits.

Use the [benchmark contract](../BENCHMARKING.md): compare identical completed
work, including required output and CPU recovery. Kernel-only and count-only
timings are diagnostic, not full-classification throughput.

Missing reconstructable measurements must be reproduced. Unavailable evidence
must remain explicitly missing or blocked, never estimated. The future repeated
measurement protocol fixes inputs, hardware, warm-ups, repetitions, and summary
statistics.

See [the consolidation status](../PORTING_PLAN.md) for the planned benchmark
chunks and their approval gates.
