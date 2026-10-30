# Experiment index

The [CPU function-profiling baseline](../PROFILING.md) records full-corpus
geometry timings, per-function instruction counts and the HTML history workflow.
The [descriptor baseline](cws-descriptors.md) separately records generation
throughput at its stated complete-work boundary. Historical CPU and GPU notes
remain on their archival branches; these new records do not certify historical
results or introduce comparative speedup claims.

A reproducible record must identify the question, code/source revisions, correctness
evidence, kept/optional/rejected/proposed decision, measured boundary, raw
measurement provenance, missing metadata, and integration commits.

Use the [benchmark contract](../BENCHMARKING.md): compare identical completed
work, including required output and CPU recovery. Kernel-only and count-only
timings are diagnostic, not full-classification throughput.

Missing reconstructable measurements must be reproduced. Unavailable evidence
must remain explicitly missing or blocked, never estimated. Repeated
measurements must fix inputs, hardware, warm-ups, repetitions, and summary
statistics.

The [proof package](../../proofs/README.md) establishes mathematical coverage
for its Lean reference algorithms, not benchmark results for native CPU/GPU
implementations.
