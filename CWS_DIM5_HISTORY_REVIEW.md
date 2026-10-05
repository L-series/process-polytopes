# Dimension-5 combined weight systems: history and corpus status

Repository audit: 2026-10-05. Latest recorded production status: 2026-06-05,
`gpu-run-43types` at `452fbe8`. This review uses the available local branches,
cached `origin/*` references, the PALP submodule history, and local benchmark logs;
it does not establish whether newer work exists on the remote or cluster.

## What has been computed?

The combined corpus comprises **46 canonical structures, s2 through s47**, with
**12,140,535,288,504 pre-IP candidates** after selection and prefix canonicality
filters. All structures have been counted. Counting candidates is much cheaper
than constructing their polytopes and checking IP.

The production ledger records **44 structures fully IP-checked**, covering
**1,106,587,619,995 candidates (9.114817% of the workload)** and yielding
**94,319,619 IP-accepted rows before deduplication**.

| Portion | Structures | Candidate count | Recorded computation | Final IP-accepted rows |
|---|---:|---:|---|---:|
| Smaller types: s2–s47 excluding s3, s12, s13 | 43 | 118,676,087,105 | Complete GPU pass and CPU overflow pass | 80,087,604 |
| s12 | 1 | 987,911,532,890 | Complete GPU pass and CPU overflow pass | 14,232,015 |
| s13 | 1 | 987,911,532,890 | Counted; production IP pass not yet run in latest ledger | — |
| s3, two size-5 systems sharing three coordinates | 1 | 10,046,036,135,619 | Counted; approximately 1.008 billion candidates sampled, no complete production pass recorded | — |
| Completed production total | **44 / 46** | **1,106,587,619,995** | **9.114817% of candidates** | **94,319,619** |
| Remaining complete passes: s3 + s13 | 2 | 11,033,947,668,509 | 90.885183% of candidates; s3 sample excluded from completed total | — |

**IP-accepted does not mean reflexive in dimension 5.** These are Stage-1
generation/point-enumeration/IP results. The ledger does not establish a finished
reflexivity census, global normal-form deduplication, or a count of distinct
combined-corpus polytopes. The separate single-weight dataset and its classifier
results must not be used as evidence of combined-corpus completion.

Production evidence comes from `CWS_RUN_LEDGER.md` on `gpu-run-43types`:

| Run | Jobs | GPU accepted | Deferred to CPU | CPU accepted | Runtime |
|---|---|---:|---:|---:|---|
| 43 smaller types | 66815 + repair 66822; CPU 66824 | 76,216,562 | 68,066,584 | 3,871,042 | GPU ~79 min + ~1 min repair; CPU ~21 min on 128 cores |
| s12 | GPU 66825; CPU 66875 | 14,201,801 | 14,121,722 | 30,214 | GPU ~7.75 h on 5 GPUs; CPU ~24 min |
| s3 sample | 66880 | 0.0126% sample acceptance | 0.163% sample overflow | Not recorded | Five spread shards, ~1.008B candidates |

The s3 sample covers only approximately **0.0100% of s3**. Its projected accepted
counts and full-run timing are estimates, not computed results. The ledger
suggests increasing the point cap from 256 to 512–1024 before a full s3 run to
reduce a projected ~16.4B-row CPU overflow stream.

## Optimization history

Rates below refer to their stated benchmark stages and inputs. They are not all
end-to-end rates, and speedups from different rows should not be multiplied.
`np_cap` is the maximum number of points handled on GPU before deferral to CPU.

| Change | Branch / commit | Measured result | Current disposition |
|---|---|---|---|
| Recover descriptor-driven dimension-5 CWS generation; repair unsound slot-order pruning | `formal-verification` ancestry, `688edf3`; PALP `d84b4ab`, `9e13543` | Functional canonical generator and regression cases | Foundation for later work; pruning fix is relevant to completeness |
| In-memory weight pools, reusable point scratch, CPU sharding | `3be2423`; PALP `793c44c` | s24: 63.98 → 25.29 s, **2.53×**; two balanced shards ~12.71 s; representative longer workloads **1.8–2.5×** | Implemented CPU generator path |
| W5 pool caching | PALP profiling lineage; `PALP_W5_POOL=results/cache/w5.ip` | Avoids rebuilding the 184,026-row W5 base pool, documented as ~1 min startup per invocation | Used in production scripts |
| Full CUDA geometry / canonical-NF backend | `d221b7d`, `164925b` | Exact parity on small corpora, but CPU ~11× faster on s5 and ~19× faster on s8 | Implemented correctness baseline; distinct from fast production IP scanner |
| Direct type-3 and generic descriptor GPU scanners | Same CUDA lineage | All 46 types counted in ~223 s initially; later count pass ~104 s; exact total **12.1405T** | Implemented; canonical count scan does not perform IP/NF classification |
| Cooperative fused point/IP kernel and launch tuning | `a7a808e` | Example s12 sample **2.48×**, s3 sample **2.92×** over serial; profiling benchmark ~106.9k/s vs ~29.3k/s | Retained option; superseded for production by split FP path |
| Nested / Aristotle point-walk variants | `formal-verification` lineage, scripts and logs 66646/66649 | CPU point stage **1.30–1.32×** on sampled inputs; GPU **0.90–0.96×** | CPU sampled benefit; GPU variant not a throughput win |
| Split point enumeration, point-count bucketing, separate IP kernel, CPU overflow | `gpu-ip-bucketing`, `68af99e` | s3 cap32 ~146k/s vs fused block ~107k/s; lower caps trade throughput for more overflow; thread/grid sweeps mostly flat | Core production architecture, with later walk replacement |
| Reciprocal-multiply integer division on CPU | `POINT_WALK_ALGORITHMS.md`, job 66676 | **0.84×**, about 19% slower | Rejected on CPU |
| Reject using multiple interior points / simple bounds | Point-walk studies | Proposed predicates never fire on the tested normalized candidates | No useful speedup; not a production filter |
| CPU LLL + Fincke–Pohst walk | `278610c`, `7a8b0fa`; PALP `f48c533` | Prototype **74× fewer nodes**, **57× fewer divisions**, ~9× wall; actual in-tree **1.4–2.1×** on s3/s6, slower on light/higher-arity inputs | Implemented optional CPU walk and volume gate; prototype timing was heavy-biased |
| A: block-cooperative enumeration inside bucketed pipeline | `gpu-opt-A-block-bucketed`, `107b68d` | **1.13×** at cap64/32 threads; slower at low caps | Retained option; not chosen production default |
| C: maintain offsets incrementally | `gpu-opt-C-incremental-offset`, `5898d20` | **0.89–1.03×**; adds per-thread state | Rejected implementation; later B restores recomputation despite C remaining in ancestry |
| B: 32-bit walk and floor division | `gpu-opt-B-int32-div`, `45df050` | **2.62–3.13×**; cap64 ~302k/s vs ~106k/s | Incorporated into later E/G/production branches; division cost fell, stack footprint did not |
| E: sort candidates by estimated box volume | `gpu-opt-E-volsort`, `bd59c92` | **1.02–1.32×** on top of B; cap64 ~416k/s, ~3.9× original int64 path | Incorporated; `--vol-sort` used in production |
| D: GPU triangular-walk node-count instrumentation | `gpu-opt-D-lllfp-prototype`, `add4306` | ~27,387 nodes/candidate at cap64; inferred FP headroom from CPU study and justified full port | Instrumentation only, no FP walk yet; side experiment superseded by G |
| G: GPU LLL + Fincke–Pohst walk | `gpu-opt-G-lllfp-gpu`, `031ff88` | cap64 **5.94M/s**, **13.2×** B+E; cap256 **5.36M/s**; roughly **52×** original ~106k/s | Main production algorithm; FP32 setup and exact integer leaf checks |
| Full-range streaming of bucketed/FP chunks | `gpu-run-43types`, `fdd73f6` | s24: streaming equals single-shot; small/large chunk accepted sets equal; all **3,207,408** candidates processed | Removes bounded-sample limitation; validated in log 66808 |
| Descriptor recursion stack repair | `16bb8fb` | Repair completed s38/s40/s42 after original production crash | Incorporated; repair outputs included in ledger totals |
| Balance work by distributing many shards across GPUs | `5c9d398` | s12 GPU runtimes 26,307–27,905 s, ~6% spread | Production script assigns 40 contiguous shards round-robin across 5 GPUs; not selection-index modulo sharding |
| Measure L40 instead of estimating fleet performance | `63c074a` | cap64 **5.49M/s**, **0.92×** Blackwell 5.94M/s | Measured hardware comparison; whole-fleet run estimates remain projections |

The production invocation combines:

```text
--stream-ip --ip-bucketed --fp-walk --vol-sort --np-cap 256 --emit-capacity 1500000
```

Candidates are generated in chunks, their lattice points are enumerated, and IP
is checked on GPU. Accepted rows and deferred rows are written separately. CPU
PALP `cws-5d.x -i -f` completes the deferred stream. Normal forms, reflexivity,
and global deduplication require subsequent work.

## Branch state and evidence limits

* `main` / cached `origin/main`: `7c4dc2e`, 2026-04-06; predates the combined-CWS
  optimization series. This work is not merged into main in the inspected refs.
* `formal-verification`: `44964cd`, 2026-06-03; combined generator, classifier,
  verification, initial CUDA work, and profiling foundation.
* Current checkout `gpu-ip-bucketing`: `7a8b0fa`, 2026-06-04; split pipeline and
  CPU LLL/FP integration, before the GPU A–G experiments and production runs.
* A → C → B → E are successive ancestors of G; D forks from E. Branch names
  describe experiment milestones, not wholly independent implementations.
* `gpu-opt-G-lllfp-gpu`: `752ea8e`, 2026-06-05; winning GPU algorithm and count
  table. `gpu-run-43types`: `452fbe8`, same date; streaming, production fixes,
  scripts, and latest run ledger, including s12 despite its branch name.
* Local and cached origin refs agree for those named branches. No remote fetch
  was performed. `THEORY` and `feature/polytope-classifier` contain older work.

The ledger output directories `/home/ahat01/cws43run` and
`/home/ahat01/cws12run` are absent from this workspace. Production completion
and row totals therefore rely on committed records, not a fresh recount of the
original files. Local logs corroborate benchmark and streaming-validation work.

The latest ledger incorrectly labels its cumulative total **“45 of 46”**:
43 smaller types plus s12 is **44 of 46**. It also reports that the per-type CPU
attribution matched **3,871,041 of 3,871,042** recovered rows, leaving one row
unattributed. Use the aggregate total for the final accepted-row count.

Correctness evidence includes exact CPU/GPU comparisons and accepted/deferred
set comparisons on selected shards, CPU walk comparisons across all 46 types,
and streaming chunk-invariance checks. These do not constitute an exhaustive
formal proof of the complete GPU run. The production notes leave runtime int32
overflow guards open; earlier claims of safety depend on bounds for the current
weight pool. Preserve that limitation when interpreting the census.

## Source map

* Latest counts and runs: `gpu-run-43types:CWS_RUN_LEDGER.md` at `452fbe8`;
  `gpu-opt-G-lllfp-gpu:CWS_TYPE_COUNTS.md` at `752ea8e`.
* GPU A–G measurements: `gpu-run-43types:GPU_OPTIMIZATION_PLAN.md`, results log.
* Production projections and open items:
  `gpu-run-43types:PRODUCTION_RUN_IDEAS.md`. Earlier sections retain estimates
  superseded by later FP and L40 updates; projections are not completion records.
* CPU refactor / original CUDA work: `BENCHMARKS.md`; PALP submodule history.
* CPU algorithm findings: `POINT_WALK_ALGORITHMS.md`, `LLL_FP_WALK.md`.
* Split architecture: `GPU_IP_BUCKETING.md`, `PIPELINE_CODE_PATHS.md`,
  `PIPELINE_PROFILING.md`.
* Local experimental evidence: `logs/slurm/exp*-*.out`,
  `logs/slurm/aristotle-{bench,gpu}-*.out`, `logs/slurm/run43-val-66808.out`.

Inspect a source absent from the current checkout without switching branches:

```bash
git show gpu-run-43types:CWS_RUN_LEDGER.md
git show gpu-run-43types:GPU_OPTIMIZATION_PLAN.md
git show gpu-opt-G-lllfp-gpu:CWS_TYPE_COUNTS.md
```
