# Literature and mathematical background

These notes explain the primary sources, geometric conventions and computational
boundaries behind the [Lean classification](../../proofs/README.md).
The formal endpoint covers reflexive lattice polytopes in every fixed
dimension. IP weights and possibly nonreflexive containers are intermediate
objects, not a promise to classify all nonreflexive IP polytopes.

## Reading guide

- [Weight systems, weighted projective spaces and simplex witnesses](weight-systems-and-wps.md)
- [IP/reflexive polytopes, polarity and pyramid hypotheses](ip-reflexivity-and-pyramids.md)
- [Minimal seeds, positive circuits and structural data](minimal-ip-circuits-and-types.md)
- [Weight enumeration, half weights and degree bounds](weight-enumeration-and-degree-bounds.md)
- [Combined weights, rank, lattices and reconstruction](cws-and-lattice-realizations.md)
- [Finiteness, volume and lattice-box reduction](finiteness-and-box-reduction.md)
- [Normal forms, exact equality and classification output](normal-forms-and-classification.md)

Each note gives primary references, precise hypotheses, source versions and
formal declaration boundaries. Supporting-library declarations may be weaker
than the concrete theorem; its full chain is specified in
[STATEMENTS.md](../../proofs/STATEMENTS.md).
Abstract existence, executable enumeration, native implementation and measured
census evidence are distinct.

## Compiled review

[LITERATURE_REVIEW.tex](LITERATURE_REVIEW.tex) collects all seven notes, preceded
by definitions of lattices, IP/reflexivity, weights, Newton hulls, CWS and lattice
equivalence. Its appendix identifies the Markdown source bytes by SHA-256.
The Markdown notes are authoritative and the compilation must stay in sync.
Use `$...$` and `$$...$$` for Markdown mathematics.

Build with LuaLaTeX, `latexmk` and the DejaVu Serif / DejaVu Sans Mono fonts.
From the repository root:

```bash
review_build="$(mktemp -d)"
latexmk -lualatex -interaction=nonstopmode -halt-on-error \
  -outdir="$review_build" docs/learning/LITERATURE_REVIEW.tex
```

The PDF is `$review_build/LITERATURE_REVIEW.pdf`. Keep PDF and auxiliary outputs
outside the checkout. Compiling the review does not run Lean.

## References and source documents

[SINGLE_WS_5d.tex](SINGLE_WS_5d.tex) is a separate paper on the single-weight
dimension-five sector, not the specification or proof-status authority for the
generic classifier.

Primary PDFs and available author TeX may be kept in the ignored
[source cache](sources/README.md). Citations, exact editions and original-file
hashes remain in the notes; downloaded documents are not distributed in Git.
