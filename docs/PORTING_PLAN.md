# CWS consolidation: scope, provenance, and status

This replaces the preliminary seven-milestone proposal. The approved integration
base is the current production branch, not `main`. Execution uses small,
approval-gated chunks and fresh `integration/cws-5d` branches in both repositories.

## Delivery boundary

The goal is a reproducible census of generated CWS Newton polytopes on the
implemented/PALP reference lattice, retaining IP-but-nonreflexive and reflexive
classes. New quotient/sublattice enumeration, reflexive-subpolytope expansion, and
r-maximal search are excluded.

The proof deliverable is the existing abstract theory with an honest audit, not
new formal verification of the native descriptor census. Abstract coverage must
not be advertised as verification of CUDA, PALP normal forms, or census counts.

## Integration and reviewed source

| Source                | Pinned identity                            |
| --------------------- | ------------------------------------------ |
| Root integration base | `8fed7d6e5fc349b13de99c866eed35aba5a87ba4` |
| PALP integration base | `968bb4b3c6c42a0893c95340e81718548e13f12a` |
| Sibling proof HEAD    | `6d71721e24f4cb4320cbcd059e6a9dc9dbeb50d1` |

The reviewed proof package includes approved working-tree changes, not only
sibling HEAD. All 29 tracked package files were archived outside Git and checked
against the reviewed raw-file ledger. The sibling checkout remains untouched.

```text
HEAD-to-working-tree proof patch SHA-256:
f4acf9b4c35bae350e8aac0a9f202e823d83d6f66d9f6cadbf74d397ec72590f

Complete proof-source ledger SHA-256:
04ed9be4f1df0e0013f84a13566a6c39bbaef7f3f74503ae4cc09696acca4091

Proof lake-manifest.json SHA-256:
0d6def3ad257f03d2d0301868135497e764c1121813de38a7edbf81903510c1f
```

The ledger is SHA-256 over sorted UTF-8 lines
`<raw-file SHA256><two spaces><repository-relative path><LF>`. The patch is produced
by `git diff --no-ext-diff --no-textconv --no-color --binary HEAD -- proofs`.
Changed source fingerprints require review before using a replacement snapshot.

The proof port is authorized under MIT with attribution retained. Preserve
dependency notices, including Mathlib's Apache-2.0 and PALP's GPLv3. Do not label
PALP-linked distributions MIT-only.

## Current execution order

**C00 introduces the operating baseline and frozen source inventory here.**
Literature, the fresh build/audit, and proof modules have not landed.

| Group                                   | Planned work                                                                  | Acceptance boundary                                                            |
| --------------------------------------- | ----------------------------------------------------------------------------- | ------------------------------------------------------------------------------ |
| T-L1, T-L2, T-L3                        | Seven primary-source literature topics in three batches                       | Verified references, hypotheses, and exact proof-step support                  |
| T-A0                                    | Fresh coherent snapshot build and proof-chain/claim audit                     | Scoped claims, dependency graph, transitive axiom evidence                     |
| T-P0 through T-P9                       | Pinned Lean/Mathlib setup and dependency-ordered proof port                   | Each commit builds; proof CI runs `lake build` after setup                     |
| C01 through C13, including split chunks | CPU references, generation, classification, replay, and tools                 | Exact CWS/point/NF parity, safe arithmetic, lossless records                   |
| T-C0                                    | Archived Lean catalogue versus integrated PALP descriptors                    | Deferred until C02; computational correspondence is not geometric completeness |
| G00 through G09                         | Approved GPU setup, reference scanner, completion, and optional optimizations | Real-device tests, exact CPU recovery, conservative bounds, sanitizers         |
| B00 through B04                         | Experiment inventory, reproducible measurements, and source maps              | Sourced numbers and matching complete-work boundaries                          |
| R00 through R05                         | Approved production runs, reconciliation, catalogues, and release             | Complete descriptor coverage and exact NF reduction                            |

The table groups related chunks, not a strict checklist. Actual prerequisites
govern execution: C01/B00 establish reference and benchmark contracts before
caching or GPU setup; benchmark closure comes later. No row authorizes the next
chunk. GPU provisioning and production allocations need separate resource
approval. T-C0 is the exception to theory-first ordering because the integrated
descriptor implementation does not exist yet.

## Rules for every chunk

- One to three logical commits; split a larger or riskier chunk before continuing.
- Follow the existing Conventional Commit hook; separate refactors and behavior.
- Introduce tests and relevant README/manual updates with each change.
- Report root/PALP commits, exact validation and CI, deviations, and the next chunk.
- Stop after each chunk until the requester approves continuing.
- Publish every new PALP SHA before publishing a root commit that pins it.
- Preserve all original main, production, and experiment refs without rewriting.

## Correctness and completion gates

Preserve the existing [CWS/NF fixtures](../tests/README.md); never silently replace
goldens. New generation tests must compare complete multisets and shard unions.
Point optimizations require exact coordinate-set equality, not only matching
counts. GPU IP acceptance plus CPU IP acceptance of deferred candidates must
equal the reference IP multiset.

New records must retain complete CWS rows, degrees, and provenance. Incompatible
legacy checkpoints are rejected explicitly, not automatically converted.
Deduplication requires full NF equality; hashes are indexing keys only.

Int32 intermediates and every floating-point pruning interval require checked
eligibility/conservative bounds or an exact fallback before points can be lost.
Uncertified FP modes remain off. Missing measurements are not estimated.

The historical ledger describes 43 minor descriptors plus s12: 44 of 46, not 45. Those records still require reconciliation; full s3, full s13, and final
classification remain planned. Final counts require complete reference evidence,
recovered failures, separate IP/reflexive outputs, and exact NF multiplicities.
