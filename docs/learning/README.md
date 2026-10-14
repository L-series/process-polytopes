# Learning notes

## Existing reference

[SINGLE_WS_5d.tex](SINGLE_WS_5d.tex) is the author's separate paper on the
single-weight-system part of the five-dimensional classification programme,
not the official manuscript for this theory development. Its rough proof chain
and theorem references can help locate primary sources and cross-check claims.
The new work must independently verify those sources and the Lean statements;
this index does not certify the paper's historical proof-status assertions.

The [mathematical roadmap](THEORY_ROADMAP.md) records the long-term goal of
complete WS/CWS-based enumeration in every fixed dimension, its proposed
dependency structure and the distinction from the current scoped proof port.

The approved final theory target is now **reflexive polytopes only**.
Nonreflexive IP examples in the notes remain useful for delimiting hypotheses
and for understanding intermediate containers; they are not an additional
classification promise. The [Lean scaffold](../../proofs/README.md) specifies
the generic endpoint with conditional composition and an intentionally failing
strict completeness gate.

## Reusable primary documents

During approved research, agents may save PDFs and available author TeX sources
in [`sources/`](sources/README.md), alongside the existing single-weight paper.
Check this local cache before fetching a document again, retain exact versions
and provenance, and direct later agents to matching cached sources. Downloads
are ignored by Git; citations and provenance references belong in the notes.

## Literature review

The seven completed research notes cover:

- [Weight systems, weighted projective spaces, and simplex witnesses](weight-systems-and-wps.md).
- [IP/reflexive polytopes, toric interpretation, and pyramid hypotheses](ip-reflexivity-and-pyramids.md).
- [Kreuzer-Skarke minimal seeds, circuits, and the descriptor model](minimal-ip-circuits-and-types.md).
- [Weight enumeration, half weights, and degree-bound claims](weight-enumeration-and-degree-bounds.md).
- [Combined weights, rank, lattices, saturation, and index data](cws-and-lattice-realizations.md).
- [Finiteness, volume, and lattice-box reduction](finiteness-and-box-reduction.md).
- [Normal forms, equivalence, and collision-safe classification](normal-forms-and-classification.md).

Each note cites primary sources with precise hypotheses and identifies the Lean
declarations it supports. Abstract existence, executable enumeration, and
completed census evidence remain distinct.

[LITERATURE_REVIEW.tex](LITERATURE_REVIEW.tex) is a standalone compilation of all
seven notes, preceded by basic definitions of lattices, polytopes, IP/reflexivity,
weights, Newton hulls, CWS, lattice equivalence, and normal forms. It retains the
full notes, tables, citations, examples, and provenance; links between included
notes become internal references. The appendix records the Markdown source
hashes. The Markdown notes remain authoritative, so refresh the compiled review
when their content changes. Use `$...$` and `$$...$$` for Markdown mathematics so
the repository formatter preserves its symbols. The roadmap, this index, and the separate
single-weight paper are not included as review chapters.

Build it with LuaLaTeX, `latexmk`, and the DejaVu Serif / DejaVu Sans Mono fonts.
From the repository root, keep all PDF and auxiliary outputs outside the checkout:

```bash
review_build="$(mktemp -d)"
latexmk -lualatex -interaction=nonstopmode -halt-on-error \
  -outdir="$review_build" docs/learning/LITERATURE_REVIEW.tex
```

The PDF is written to `$review_build/LITERATURE_REVIEW.pdf`. Building this review
does not run Lean or certify the mathematical or computational claims.

The [permanent theory agents](../THEORY_AGENTS.md) provide bounded literature
research, independent checking in both directions between prose and Lean
statements, and final proof simplification using pinned Mathlib. The audit
compares the two independently completed reports; semantic revisions trigger
affected checks again. These definitions are available, but research, auditing
and optimization still require approval of their respective chunks.

See [the consolidation status](../PORTING_PLAN.md) for the reviewed source and
approval-gated sequence. The [tracked frozen-source audit synthesis](../proof-audit.md)
is complete; clean donor blind-review closure and the full proof port remain
pending. The initial contract scaffold is present but its supporting proofs and
scientific routines are unfinished. The synthesis changes no literature-note
claims or mathematical contracts.
The complete `Basic` and `ReflexiveDuality` foundation modules are now faithfully
ported and imported into the audited package. This port changes no literature
claims and closes no KS obligation.
`WeightSystem` and `BoxReduction` are also faithfully ported and imported;
their 30 results preserve the kernel-interior, projection-lifting and
existential box-bound hypotheses. No literature claim or compilation changes.
