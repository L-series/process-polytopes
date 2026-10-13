Use end-to-end wall time for a fixed workload as your primary metric, and express it as candidate CWS classified per second. That works across CPU, GPU, and mixed implementations.

```text
                   fixed number of input candidates
throughput=────────────────────────────────────────────────
           elapsed wall time until all results are complete
```

“Complete” must include CPU fallback for overflow or numerical uncertainty. Merely submitting candidates to the GPU does not count.

| Metric                                      | Role                                                       |
| ------------------------------------------- | ---------------------------------------------------------- |
| Wall time for a fixed workload              | Primary measure of optimization progress                   |
| Candidate CWS classified/second             | Convenient throughput presentation                         |
| CWS/core/second                             | Useful for CPU scaling; unsuitable for CPU–GPU comparisons |
| CPU cycles and instruction counts           | Diagnose CPU changes                                       |
| Kernel time, bandwidth, occupancy, stalls   | Diagnose GPU changes                                       |
| Candidates/joule or cost/million candidates | Optional hardware-efficiency comparison                    |

There is no useful equivalence between a CPU core and a CUDA core. Compare specified resource configurations instead: one CPU core, a fixed multicore allocation, or one GPU with a stated number of host CPU cores.

For this project, I would maintain two complementary benchmark suites:

- Fixed candidate inputs: Feed the same candidates into point enumeration and IP classification. This isolates improvements to those routines.
- Fixed enumeration ranges: Process an entire specified descriptor/shard range. This captures pruning, canonicality improvements, scheduling, transfers, fallback, and output handling.

That distinction matters: an optimization that avoids constructing unnecessary candidates could reduce “candidates processed/second” while finishing the classification faster. Time to complete the fixed range remains the deciding metric.

Keep the benchmark contract explicit. An IP-only pipeline and PALP’s current generation command—which also constructs duals and prints additional information—perform different work. Compare implementations at matching boundaries, and verify the same accepted set and required outputs.

For CPU–GPU progress, record:

workload/input hash
implementation commit
CPU model and host-core allocation; GPU model
end-to-end seconds and candidates/second
accepted/rejected/fallback counts
correctness result

Report both GPU speedup against one CPU core and against your practical multicore CPU implementation. The former tracks acceleration relative to today’s baseline; the latter informs deployment.

For measurement, keep startup-inclusive time and steady-state throughput separate. Use host wall timing for the complete pipeline and CUDA events for kernel-level measurements. GPU launches are asynchronous, so stop the end-to-end timer only after all required device work and CPU completion have finished. NVIDIA documents these timing requirements in its CUDA performance measurement guidance (https://docs.nvidia.com/cuda/cuda-c-best-practices-guide/index.html#performance-metrics).

Given our measurements, the initial suite should retain full 4D, add fixed 5D ranges with high rejection rates, and include expensive and overflow cases. Report each workload separately: the current small 5D fixtures have unusually high acceptance rates and could give a misleading aggregate score.
