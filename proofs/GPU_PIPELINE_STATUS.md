# GPU correctness checkpoint

Branch: `integration/cws-5d-gpu`.

**The full source-level no-omissions proof is not complete**: the join's search
loop (item 1 below) is the one remaining unproved link on the rejection path.
Everything else on the path from generation to a GPU verdict is either proved
in Lean against a model that is source-pinned or source-generated, model-checked
by CBMC on the extracted source, or exhaustively checked over a finite domain
whose sufficiency is itself proved. Every GPU verdict, accept or reject, now
carries an independently checked certificate. Anything else defers to exact CPU
completion.

## What decides a candidate, and why it is sound

| Stage | Verdict | Soundness argument |
| --- | --- | --- |
| Generation | which candidates exist | CBMC: prefix test, permutation step and stored rows equal PALP's (149 slots, 46 descriptors). Exhaustive: selection pools (5,871,755 entries, shared prefixes sorted) and slot order (179 pairs). Exhaustive emission over all rank-fixed order types (12,528,582 cases), covering all positive weights by `agree_on_positive` (GpuEmission.lean), whose premises are CBMC order-invariance of both comparing primitives and a clang audit that the GPU and PALP recursions only copy weights. Lean: tuple and pair decoding, pair-space size, shard partition. Fused type 3: exhaustive equality with the general generator. |
| Join | reject (np < 6, coordinate/pair certificate); exact np for the walk | Lean: rejection rules, binary searches, packed ranges, stack machine. CBMC: matcher on its full domain; bounded search/range queries. Builder: every row enumeration validated and counted against `row_solution_count`, a DP proved to count all solutions (JoinRowCount.lean), so no table entry can miss a point. **Open: the search loop's source-level refinement (item 1).** |
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

## Remaining formal boundaries

1. **Join search loop.** The CUDA loop in `device_gjoin_decide` (cursors
   `it`/`end`, chosen keys, mutable `val`) and the fused type-3 join are not yet
   refined to the proved stack machine (JoinStack.lean). Needed: a source-pinned
   Lean transcription of the loop and a simulation proof into that machine,
   with the range initialization tied to `packed_range_exact`.
2. **Trusted frontends.** Source pins and generated models are small reviewed
   translators (scripts/generate_ip_*.py, check_*_model.py), not a verified
   C++ semantics. The CUDA compiler, atomics and memory model, CUB, the OS and
   the filesystem are trusted. The walk validator and the fibre validator are
   tested and audited, but not source-pinned.
3. **PALP** (CPU completion of deferred rows) is trusted, as agreed.
4. **Production execution.** All eight type-3 shard receipts, structure 1's
   artifacts and the final audit must still be produced and checked.
