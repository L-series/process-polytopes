# GPU correctness checkpoint and handoff

Branch: `integration/cws-5d-gpu`, pushed to `origin` on 2026-11-03 with the user's approval for the
production run.

**The end-to-end source-level proof is still incomplete.** The current Lean
model theorems compile, but two source/interface assemblies below remain open.
Do not equate a source hash check or a conditional model theorem with a proved
CUDA-to-CWS refinement. The user requested this checkpoint for another model.

## Completed verification pieces

- Generation: the existing CBMC, exhaustive emission/pool/catalogue checks and
  `GpuEmission.lean`; fused generation agrees with general generation.
- General search: `JoinLoopSource.crun_perm_full`, with the source pinned by
  `check_join_loop_model.py`. `crun_leaf_values` proves every emitted tuple has
  a source state holding all chosen shared fields.
- Leaf arithmetic: `JoinLeafCount.lean` proves exact checked products/sums and
  overflow/zero-count failure, pinned by `check_join_leaf_model.py`. All 25
  general leaf mask queries and the verdict passed; nonvacuity and mutation
  controls passed. No symbolic uint64 division is needed in CBMC.
- Fibre validator: `JoinFibreValidator.lean`, source-pinned together with its
  caller builder by `check_join_fibre_model.py`. Accepted count-sized chunks
  are exactly their canonical key's points; stored counts, decoded fields,
  class nibbles and private-pair bits have exact list-level meanings. Complete
  row solutions follow from the pinned DP (`JoinRowCount.lean`).
- Index validator: `JoinIndexValidator.lean` and `check_join_index_model.py`:
  sorted permutation, bounds, distinct targets, decoded fields, no missing index.
- Walk validator: `GpuWalkValidator.lean`, `check_walk_validator_model.py`, and
  `ci/GpuWalkValidatorAudit.lean`. Accepted point bounds/order, distinctness,
  kernel weight equations and coordinate/pair occupancy are proved under the
  explicit certified root-box and embedding inputs.
- Row/tuple interfaces: `JoinTupleBridge.lean` and
  `JoinCandidateCertificate.lean` derive row-equation equivalence from a checked
  prefix bijection, unchanged private coefficients, support and row solutions.
  `JoinValidatedFibres.lean` constructs finite row functions and proves the
  accepted global row-solution equation. Its per-key adapter is NOT finished.
- Mask theory: `JoinMaskAssembly.lean` proves shared/private class union,
  shared-coordinate constants, independent factors, same-row joint summaries,
  accumulated functional sides and model rejection soundness.
- Point-to-search coverage: `JoinSourceCoverage.crun_covers_layout_point`
  derives a source-emitted tuple for every Layout point, using normal key-set
  representations and shared-coordinate geometry. The emitted-event wrapper
  described below is NOT finished.
- Fused type 3: `JoinType3.lean` proves safe midpoint, exact lookup, remapping,
  matched-pair completeness/uniqueness, uint64 count headroom and capped keep.
  `JoinType3Search.lean` and `JoinType3Filters.lean` cover both shorter-side
  iterations, candidate-order presence checks, wrapped Bloom hashes and checked
  filter completeness. `JoinType3Geometry.lean` derives coverage/compatibility
  and values from the fixed descriptor. `JoinType3Sound.lean` and
  `JoinType3Refinement.lean` derive model counts, masks and CWS rejection.
  **Their public scan still uses fibre-derived `eventSummary`; the raw stored
  count and row-mask substitution is the open boundary below.**
  `check_join_type3_model.py` pins the source, builder and layout guard, while
  `test-gpu-join-type3.py` passed PACK, PERM and CERT (including arbitrary prior
  mask bits). This is not yet the full fused source proof.
- Existing IP rejection/acceptance certificates, routing/identity audits and
  CPU-defer handling remain unchanged.

## Finish next (in order)

1. **Validator chunks to Layout fibres.** Extend `JoinValidatedFibres.lean`:
   use `accepted_chunk_rows_exact` to identify each finite-function fibre with
   its decoded chunk. Prove its card equals stored `g.count`, nonemptiness,
   shared fields, and raw cats/pairs bits using `checkedChunk_shared_field`,
   `checkedChunk_cat_class`, `checkedChunk_pair_low/high`. The base finite
   decode, injectivity, rowSol membership and row equation already compile.
2. **Fused raw-source summary adapter.** `FibreFacts.count_bound` in
   `JoinType3Refinement.lean` is only a bound. CUDA adds stored
   `kl.count * kr.count`, while public `hsource` scans `eventSummary.count =`
   a product of Layout fibre cards. Likewise its sides are fibre-derived while
   CUDA reads `cat_a`, `cat_b`, and `pair`. Add explicit stored fields and the
   validator-derived equalities; define a raw source summary and prove it equals
   `eventSummary` on matched keys. Lift that equality through `sourceScan`.
   Then update the public left/right checked-source endpoints to take the raw
   scan, not the already-substituted semantic scan. The counts, remapping,
   filters, geometric coverage and conditional CWS theorem are already proved.
3. **General emitted-event assembly.** Compose `crun_leaf_values`,
   `sourceTableTuple`, the new per-key facts and source/Layout coordinate map to
   derive `Values` and compatibility for each emitted tuple. Derive valid
   decoded tuple/event uniqueness and its set from `crun_perm_full` and strict
   table keys. `crun_covers_layout_point` supplies the reverse point coverage.
   Use `source_leaf_count_product` and `leafCounts_exact` to identify the sum
   with `index_card`; use `accumulated_masks_exact` and
   `RowCertificate.equations`, then call `leaf_spec_rejection_sound`. The current
   generic `source_rejection_sound` still takes event/coverage interfaces.
4. Update this checkpoint after those links are actually discharged. Keep the
   existing trusted-frontend boundary explicit: source pins/translators are
   reviewed, not a verified C++ semantics. CUDA/compiler/memory, OS/filesystem
   and PALP CPU completion remain trusted as previously agreed.

## GPU change and measured validation

`device_join_find` now uses `lo + ((hi - lo) >> 1)`: the previous uint32 sum
could overflow. `legacy_midpoint_counterexample` proves the concrete old failure
and `midpoint_exact`/`midpoint_u32` prove the replacement. No other kernel code
changed. Both GPU binaries built on the server (now CUDA 13.4.92; the documented
13.3 path is absent).

On `bigbeautifulgpu.mtl.sfl`, structure 3, shard 2600/4000, np cap 8192:

- Both binaries: 2,520,772,011 candidates, 227,854 accepted, 0 deferred.
- Saved binary: 10.0703 s; changed binary: 9.86077 s (single measurements).
- Identical sorted output; expected server-locale SHA256:
  `61c95c595f9e896331cded4b43a4198cdd2888419d081c02164f4fe46b551e1c`.
- C-locale sorted SHA256 for both:
  `91b1499b342f2f0dcfd00236a05c846928a4f69b1382c929dc6c65c135e111b9`.
- Binary identities and arguments: `verification/gpu/results/type3-midpoint-20261103.json`.
- Server outputs: `~/cws-bench/gpu-int2/build/verify-midpoint-20261103/`.

The new focused type-3 CBMC queries and module builds passed. The expanded
GPU proof/axiom audit is included in the handoff. Long regression suites were
not restarted, per the user's instruction to stop repeating tests. The earlier
full proofs gate passed; the earlier full source gate was stopped during its
repeat leaf checks after sanitizer/CPU/matcher/prefix checks passed.

## Pre-production benchmark (2026-11-03, branch head before push)

`bigbeautifulgpu.mtl.sfl` (RTX PRO 6000 Blackwell, CUDA 13.4, `sm_120`), arguments
`--np-cap 8192 --chunk-capacity 8000000 --slice-capacity 100000`:

| run | generated | accepted | deferred | GPU s | wall s |
| --- | --- | --- | --- | --- | --- |
| fused binary, structure 3 shard 2600/4000 | 2,520,772,011 | 227,854 | 0 | 9.86 | 14.3 |
| general binary, same shard | 2,520,772,011 | 227,854 | 0 | 18.48 | 27.4 |

Both type-3 outputs sort to `61c95c595f9e…` (server locale), as before. The
general join's latest source changes (one-key lookup ranges, extracted leaf and
verdict) ran on the GPU here for the first time. Structures 2, 4, 7, 8 and 10
(general join, full runs) generated 184,026 / 5,506,794 / 63,903 / 49,970 /
267,379 candidates with 184,026 / 1,828,179 / 28,221 / 2,755 / 16,040 accepted
and none deferred; their IP sets equal the PALP-verified 43-type results row for
row (weights compared after removing PALP's trailing polytope data).

## Scope

The user explicitly selected formal verification plus the required GPU
measurement. The separate production campaign (eight type-3 shard receipts,
structure-1 artifacts and final production audit) is outside this handoff task.
Never read or ship `.env`. No commits have been pushed.
