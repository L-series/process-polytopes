# GPU correctness checkpoint

Branch: `integration/cws-5d-gpu`.

**The full source-level no-omissions proof is not complete.** The general join's
search loop is proved complete and duplicate-free. Leaf/verdict source checks,
checked count arithmetic, shared-value state invariants and an abstract CWS
rejection bridge are now proved, but their remaining table/fibre-to-mask
interface facts still need to be assembled. The fused type-3 join also remains
open. The detailed boundaries below distinguish source checks from conditional
semantic bridges; passing the regression gate does not close those boundaries.

## What decides a candidate, and why it is sound

| Stage | Verdict | Soundness argument |
| --- | --- | --- |
| Generation | which candidates exist | CBMC: prefix test, permutation step and stored rows equal PALP's (149 slots, 46 descriptors). Exhaustive: selection pools (5,871,755 entries, shared prefixes sorted) and slot order (179 pairs). Exhaustive emission over all rank-fixed order types (12,528,582 cases), covering all positive weights by `agree_on_positive` (GpuEmission.lean), whose premises are CBMC order-invariance of both comparing primitives and a clang audit that the GPU and PALP recursions only copy weights. Lean: tuple and pair decoding, pair-space size, shard partition. Fused type 3: exhaustive equality with the general generator. |
| Join | reject (np < 6, coordinate/pair certificate); exact np for the walk | Lean: rejection rules, binary searches, packed ranges. Search loop: source transcription proved complete and duplicate-free (`crun_perm_full`); each emitted tuple has a leaf state holding its chosen key fields (`crun_leaf_values`). Leaf counts: source-pinned checked product/sum guards, exact abstract point count (`leafCounts_points`). CBMC: all leaf mask bits (1..5 rows, symbolic layouts/masks, unit counts), verdict (1..10 coordinates), and weight-based prefix matching (0..4 positions, arbitrary signed weights, including repeats). Builder: every row enumeration validated and counted against the proved `row_solution_count` DP. Position-index validator source-pinned and proved a sorted permutation (`JoinIndexValidator.lean`). **Open: assembly of the remaining leaf interface facts and the fused type-3 join (item 1).** |
| Walk | point set, or reject by coordinate/pair certificate | Basis rows certified in the kernel and of full rank (mod-2 witness, CBMC on all parity matrices). Points validated inside the certified int32 box, strictly increasing (hence distinct), on the weight equations (`kernel_rows_preserve_weights`). Completeness from count equality with the join's exact np (`checked_walk_complete`); no count means defer. |
| IP check | accept or reject | Hull construction untrusted. Reject only with a checked supporting inequality (reasons 1 and 4, `sourceSupportingCertificate_sound`). Accept only with a checked positive-spanning certificate (`sourceAcceptance_sound`, source-pinned). Checked 64-bit arithmetic everywhere (source-generated guards; clang audit); overflow defers. |
| Routing and output | no loss or duplication | Identity tags and ownership transitions audited per chunk; receipts. Lean: output-audit model coverage and uniqueness. |
| CPU completion | deferred rows | PALP (proof deferred, as agreed); every deferred row must be decided. |

## Measured cost (type 3, shard 2600/4000, fused binary)

Accepted set identical to the pre-verification build (same sorted sha256),
nothing deferred. GPU time 7.6 s before the verification work and 10.1 s now.
The first verified build was much slower, mostly from an atomic per checked
operation and a sort in the walk validator. Those are removed; the remaining
overhead is the IP check's checked arithmetic (about 1.2 s) and the walk
validator (about 0.8 s).

## Regression gate

`.github/workflows/gpu-verification.yml` runs `scripts/test-gpu-correctness.sh`
in two parts on every push: `proofs` (Lean modules, source pins, axiom audits)
and `source` (CBMC queries, clang audits, sanitizer harnesses, CPU completion
and scheduler tests, in `nix develop .#gpu-verify`). No GPU is needed; the CUDA
compile runs only where a toolkit is present.

### Validation checkpoint (2026-11-03)

- The complete new standalone leaf suite passed: 25 mask queries, the verdict,
  the `ffs` shim, nonvacuity and the deliberate mask mutation. The standalone
  prefix matcher query also passed.
- The final `scripts/test-gpu-correctness.sh proofs` gate passed: 116 GPU/join,
  44 IP and 10 walk theorem axiom closures, foundational axioms only.
- The final `source` gate passed the arithmetic/source audits, sanitizer
  harnesses, CPU completion and scheduler tests, matcher and prefix checks.
  The user requested stopping tests during its repeat of the leaf queries;
  **the full final source gate was not completed**. Its later walk/search,
  generation and CUDA-build steps were not reached in that run.
- No kernel source changed; no new GPU timing measurement was required.

## Remaining formal boundaries

1. **Join leaf and verdict; fused join.** The search loop itself is proved:
   `crun_perm_full` (JoinLoopSource.lean, source-pinned) shows
   `device_gjoin_decide` visits every compatible key tuple exactly once.
   The leaf/verdict boundary now has the following checked pieces:
   - `test-gpu-join-leaf.py`: extracted source, 25 mask queries (five mask
     groups at each of 1..5 rows), the verdict, and an independently checked
     32-bit `ffs` shim. Counts are fixed to one and the previous count to zero;
     successful-path mask statements never read counts. Nonvacuity and a
     deliberately corrupted `tz` update produce the expected counterexamples.
   - `JoinLeafCount.lean`: `leafCount_iff` proves success iff all factors are
     nonzero and the exact sum fits, including every intermediate product.
     `leafCounts_exact` proves accumulation over any ordered list of leaves.
     `check_join_leaf_model.py` pins the full leaf, verdict and caller,
     including failure-to-keep routing. No symbolic 64-bit division in CBMC.
   - `JoinLeafPoints.lean`: gluing is injective as well as onto; `point_card`
     and `leafCounts_points` connect the sum of products to the actual point
     count for a complete duplicate-free list of compatible tuples.
   - `JoinLeafMasks.lean`: the plain leaf low/high formulas equal the proved
     class formulas; class occupancies recover exact integer pair thresholds
     for nonempty independent factors. The verdict equals `decideExactJoin 5`.
   - `JoinLeafState.lean`: the actual source step preserves prefix key fields,
     even through failed matches and backtracking; `crun_leaf_values` attaches
     a state with all key fields to every emitted tuple. Packed bounded fibre
     values equal decoded key fields. A weight-preserving bijection preserves
     row equations (`matched_weights_equation`). The extracted prefix matcher
     constructs such a bijection on its full supported finite shape domain
     (`test-gpu-join-prefix.py`).
   - `JoinLeafVerdict.lean`: `leaf_spec_rejection_sound` feeds an exact count,
     exact functional sides and a row-equation equivalence into
     `cws_join_rejection_sound_all_dimensions`. **These are explicit premises**;
     this theorem does not yet prove the complete CUDA-to-CWS refinement.

   Still to assemble: the source tuple/slot representation with the validated
   fibre counts; the candidate/table row-equation equivalence using the checked
   prefix bijection and unchanged private positions; and the accumulated
   source masks with `functionalSides`, using `JoinPoints` occupancy lemmas,
   shared-coordinate constants, private-factor independence and the fibre
   summaries. The fibre validator is still not source-pinned. The entire
   fused type-3 join (`join_type3.cuh`) remains open; its execution path has
   not been replaced by the slower general join.
2. **Trusted frontends.** Source pins and generated models are small reviewed
   translators (scripts/generate_ip_*.py, check_*_model.py), not a verified
   C++ semantics. The CUDA compiler, atomics and memory model, CUB, the OS and
   the filesystem are trusted. `validate_gjoin_index` is now source-pinned to
   `JoinIndexValidator.lean`: decoded fields, strict ordering, target bounds,
   distinctness and complete index coverage are proved. The walk validator
   and the fibre validator are tested and audited, but not source-pinned.
3. **PALP** (CPU completion of deferred rows) is trusted, as agreed.
4. **Production execution.** All eight type-3 shard receipts, structure 1's
   artifacts and the final audit must still be produced and checked.
