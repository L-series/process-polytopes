# Proof source provenance and licensing

This scaffold uses the repository's [MIT license](../LICENSE). The requester
authorized extracting the reviewed sibling proof source under that license,
with source attribution retained. The original package is `refpoly-5d`, reviewed
at HEAD `6d71721e24f4cb4320cbcd059e6a9dc9dbeb50d1` together with its approved
working-tree changes. The dirty sibling checkout was not modified.

The frozen identities recorded by the operating bootstrap are:

| Artifact                             | SHA-256                                                            |
| ------------------------------------ | ------------------------------------------------------------------ |
| HEAD-to-working-tree proof patch     | `f4acf9b4c35bae350e8aac0a9f202e823d83d6f66d9f6cadbf74d397ec72590f` |
| Complete 29-file source ledger       | `04ed9be4f1df0e0013f84a13566a6c39bbaef7f3f74503ae4cc09696acca4091` |
| Original `Refpoly/Basic.lean`        | `a716a1508842b4f4ed1b24eaf978d43758efbdb87990b4a64e80c8795089dfc4` |
| Original `Refpoly/WeightSystem.lean` | `c874f2101fb944637922b022b765a357f96f38ba1a1595c716e811df5ca5f85a` |
| Full dependency manifest             | `0d6def3ad257f03d2d0301868135497e764c1121813de38a7edbf81903510c1f` |

`Refpoly/Basic.lean` extracts the existing ambient/lattice/reflexivity/polarity
definitions and the three `GLEquiv` equivalence lemmas. `WeightSystem.lean`
extracts the dot functional, kernel, integer-hull and relative-IP definitions.
Their mathematical meanings and namespaces are retained; their original module
headers are replaced with extraction notices. The remainder of both original
modules and the other original proof modules are **not** ported by this chunk.

`Refpoly/KS/` is the new dimension-generic contract scaffold, not a claim that
the frozen sibling already supplied its stronger executable endpoint. The
[statement inventory](STATEMENTS.md) identifies its open obligations.

Lean is pinned to `leanprover/lean4:v4.30.0`. Mathlib is pinned by the unchanged
nine-package manifest to `c5ea00351c28e24afc9f0f84379aa41082b1188f`; its tag is
`v4.30.0`. Mathlib's Apache-2.0 license and transitive dependency notices remain
in their fetched repositories. Dependency caches and frozen source archives
are not committed. PALP retains GPLv3; this proof license must not be used to
label PALP-linked distributions MIT-only.

The literature and exact source-version citations are in
[`docs/learning/`](../docs/learning/README.md). The separate
`SINGLE_WS_5d.tex` paper is not the specification for this scaffold.

The [tracked audit synthesis](../docs/proof-audit.md) records all 71 designated
donor declarations, issue dispositions and the source map for the eighteen
contracts and seven routines. It qualifies the historical round-2 blind report's
context exposure; clean donor blind-review closure remains outstanding. The
synthesis does not change the source identities or certify an executable port.
