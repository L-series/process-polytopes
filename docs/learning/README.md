# Learning notes

## Existing reference

[SINGLE_WS_5d.tex](SINGLE_WS_5d.tex) is the single-weight manuscript and the
reference for scientific conventions. Its historical proof-status statements
must be reconciled during the planned audit and proof port; this index does not
certify them.

## Planned theory-first notes

- Weight systems, weighted projective spaces, and simplex witnesses.
- IP/reflexive polytopes, toric interpretation, and pyramid hypotheses.
- Kreuzer-Skarke minimal seeds, circuits, and the descriptor model.
- Weight enumeration, half weights, and degree-bound claims.
- Combined weights, rank, lattices, saturation, and index data.
- Finiteness, volume, and lattice-box reduction.
- Normal forms, equivalence, and collision-safe classification.

Each note will cite verified primary sources with precise hypotheses and identify
the Lean declarations it supports. Abstract existence, executable enumeration,
and completed census evidence remain distinct.

The [permanent theory agents](../THEORY_AGENTS.md) provide bounded literature
research, independent checking in both directions between prose and Lean
statements, and final proof simplification using pinned Mathlib. The audit
compares the two independently completed reports; semantic revisions trigger
affected checks again. These definitions are available, but research, auditing
and optimization still require approval of their respective chunks.

See [the consolidation status](../PORTING_PLAN.md) for the reviewed source and
approval-gated sequence. Literature notes and the formal package are not yet
ported.
