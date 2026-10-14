# CWS consolidation: scope, provenance, and status

This replaces the preliminary seven-milestone proposal. The approved integration
base is the current production branch, not `main`. Execution uses small,
approval-gated chunks and fresh `integration/cws-5d` branches in both repositories.

## Delivery boundary

The [long-term mathematical roadmap](learning/THEORY_ROADMAP.md) targets complete
KS WS/CWS-based enumeration in every fixed dimension, with explicit lattice
realizations and subpolytope recovery, with **reflexive-only final targets**.
The newly approved first proof-CI chunk specifies that endpoint, proves its
conditional composition, and separates conditional success from strict
incompleteness. Supporting proofs and scientific routines remain future
obligations; nonreflexive IP classification is excluded from this theory scope.
`SINGLE_WS_5d.tex` is the author's separate single-weight paper and a source of
reference leads/cross-checks, not the official manuscript or specification here.

The goal is a reproducible census of generated CWS Newton polytopes on the
implemented/PALP reference lattice, retaining IP-but-nonreflexive and reflexive
classes. New quotient/sublattice enumeration, reflexive-subpolytope expansion, and
r-maximal search are excluded.

The proof programme now includes the dimension-generic executable KS endpoint
specified in [proofs/STATEMENTS.md](../proofs/STATEMENTS.md). The scaffold is not
that completed endpoint, and no new verification of the native descriptor census
is claimed. Conditional coverage must not be advertised as verification of CUDA,
PALP normal forms, or census counts.

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

**C00 is complete, and the approved consolidated T-L1–T-L3 research batch has
produced all seven literature notes.** The [learning index](learning/README.md)
links the notes and their standalone LaTeX compilation with introductory
definitions, citations, scope limitations, and source provenance.

A pinned staging build and paired round-2 statement checks are recorded outside
Git. Round 1 is superseded because its blind provenance was unconfirmed. The
frozen round-2 reports, blind addendum, and integrator comparison are evidence
for T-A0, not its completed tracked synthesis: the full source/claim/dependency
matrix and issue dispositions still belong in the pending `docs/proof-audit.md`.
The new `proofs/` scaffold extracts foundational definitions/equivalence lemmas,
specifies eighteen major generic contracts and seven unfinished routines, and
contains a genuine conditional composition proof. Its scoped interface review
is separate from the pending full frozen-source synthesis.

| Group                                   | Work and current status                                                             | Acceptance boundary                                                                      |
| --------------------------------------- | ----------------------------------------------------------------------------------- | ---------------------------------------------------------------------------------------- |
| T-L1, T-L2, T-L3                        | Seven primary-source research notes and consolidated LaTeX review complete          | Verified references, hypotheses, and exact proof-step support                            |
| T-A0                                    | Staging build and paired statement checks recorded; tracked audit synthesis pending | Reconciled statement reports, scoped claims, dependency graph, transitive axiom evidence |
| Initial proof-CI scaffold               | Pinned generic contracts, conditional composition, explicit supporting/data holes   | Conditional proof and CPU gates pass; strict completeness intentionally remains red      |
| Subsequent proof chunks                 | Applicable source ports and missing executable/lattice/recovery proofs              | Preserve reviewed interfaces; discharge one approved unit at a time                      |
| C01 through C13, including split chunks | CPU references, generation, classification, replay, and tools                       | Exact CWS/point/NF parity, safe arithmetic, lossless records                             |
| T-C0                                    | Archived Lean catalogue versus integrated PALP descriptors                          | Deferred until C02; computational correspondence is not geometric completeness           |
| T-O0                                    | Final proof clarity/concision and pinned Mathlib reuse after T-P9 and T-C0          | Unchanged audited statement scope and trust; clean full build and axiom guards           |
| G00 through G09                         | Approved GPU setup, reference scanner, completion, and optional optimizations       | Real-device tests, exact CPU recovery, conservative bounds, sanitizers                   |
| B00 through B04                         | Experiment inventory, reproducible measurements, and source maps                    | Sourced numbers and matching complete-work boundaries                                    |
| R00 through R05                         | Approved production runs, reconciliation, catalogues, and release                   | Complete descriptor coverage and exact NF reduction                                      |

The table groups related chunks, not a strict checklist. Actual prerequisites
govern execution: C01/B00 establish reference and benchmark contracts before
caching or GPU setup; benchmark closure comes later. No row authorizes the next
chunk. GPU provisioning and production allocations need separate resource
approval. T-C0 is the exception to theory-first ordering because the integrated
descriptor implementation does not exist yet.

The [permanent theory agents](THEORY_AGENTS.md) define one reusable literature
researcher, two independent statement checkers and a final proof optimizer.
Both checker reports are frozen before comparison; the blind checker sees only
formal types and necessary definitions. Important discrepancies need explicit
dispositions. The first proof-contract scaffold has its own approval. Completing it does not
authorize the next proof port, routine implementation or optimization chunk.

## Rules for every chunk

- One to three logical commits; split a larger or riskier chunk before continuing.
- Follow the existing Conventional Commit hook; separate refactors and behavior.
- Introduce tests and relevant README/manual updates with each change.
- Report root/PALP commits, exact validation and CI, deviations, and the next chunk.
- Stop after each chunk until the requester approves continuing.
- Publish every new PALP SHA before publishing a root commit that pins it.
- Preserve all original main, production, and experiment refs without rewriting.
- The user-approved strict proof-completeness check may remain red while the
  scaffold is unfinished; conditional proof and CPU checks must pass. Never
  hide that failure with a soft-failure setting or claim the headline proved.

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
