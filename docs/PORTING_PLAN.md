I suggest creating `production/cws-dim5` from `main` in a separate worktree, then rebuilding the successful work as a short sequence of independently tested commits. Preserve the existing branches as the experimental record.

| Milestone commit | Contents | Regression gate |
|---|---|---|
| 1\. Reproducible baseline | Pinned PALP revision, dependency/toolchain setup, small checked-in fixtures, CPU CI | Clean checkout builds; known fixtures match expected outputs |
| 2\. Combined-CWS CPU pipeline | Descriptor generation, canonicality fixes, in-memory pools, scratch reuse, sharding, schema and NF replay | Small structures match reference; shard union equals unsharded output; replay hashes match |
| 3\. GPU enumeration and IP pipeline | Descriptor scanner, split point/IP kernels, streaming, CPU overflow completion | GPU accepted rows plus CPU-accepted overflow equal the PALP accepted set |
| 4\. Arithmetic and scheduling optimizations | int32 walk, volume sorting, optional block path | Same results across supported configurations; boundary and overflow cases |
| 5\. LLL/Fincke–Pohst walks | CPU and GPU implementations, gates and fallbacks | Point-set comparisons, IP parity, numerical-boundary fixtures |
| 6\. Production orchestration | Stack fix, balanced shard assignment, resumability, run manifests and accounting | No missing/duplicated shard ranges; failures propagate; restart produces identical results |
| 7\. Consolidated documentation | Architecture, benchmark evidence, corpus ledger, experiment summary | Commands run from a clean checkout |

Include unsuccessful optimizations in the experiment summary and reproducible benchmark harnesses. Keep their implementations where they provide useful comparison modes. **Each milestone should introduce its tests alongside the implementation.**

For CI, use three tiers:

- **Every PR:** CPU build, generator/schema/replay tests, small exhaustive fixtures, shell checks.
- **GPU changes:** CUDA build plus mandatory execution on a GPU runner. GPU tests must fail when CUDA is unavailable rather than pass through CPU fallback.
- **Scheduled or release checks:** all-structure sampled comparisons, exhaustive smaller structures, four CUDA sanitizer tools, cross-device checks, and benchmarks on fixed hardware.

Before calling the FP pipeline production-ready, close the known int32 intermediate-overflow checks and establish conservative FP search bounds or a reliable fallback. Tests should distinguish **IP acceptance, reflexivity, and distinct normal forms** explicitly.

Keep PALP’s curated changes in a corresponding PALP branch and pin each milestone to its exact submodule commit. Move experiment notes into `docs/experiments/`, consolidate operational documentation, and keep run outputs outside Git with manifests containing input hashes, commands, commits, and toolchain versions.
