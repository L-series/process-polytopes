# Proof source provenance and licensing

This scaffold uses the repository's [MIT license](../LICENSE). The requester
authorized extracting the reviewed sibling proof source under that license,
with source attribution retained. The original package is `refpoly-5d`, reviewed
at HEAD `6d71721e24f4cb4320cbcd059e6a9dc9dbeb50d1` together with its approved
working-tree changes. The dirty sibling checkout was not modified.

The frozen identities recorded by the operating bootstrap are:

| Artifact                                   | SHA-256                                                            |
| ------------------------------------------ | ------------------------------------------------------------------ |
| HEAD-to-working-tree proof patch           | `f4acf9b4c35bae350e8aac0a9f202e823d83d6f66d9f6cadbf74d397ec72590f` |
| Complete 29-file source ledger             | `04ed9be4f1df0e0013f84a13566a6c39bbaef7f3f74503ae4cc09696acca4091` |
| Original `Refpoly/Basic.lean`              | `a716a1508842b4f4ed1b24eaf978d43758efbdb87990b4a64e80c8795089dfc4` |
| Original `Refpoly/ReflexiveDuality.lean`   | `5389d21a9567b6cb0e22a10906b75f1592c488f7170d2c0af60301e96d3185ca` |
| Original `Refpoly/WeightSystem.lean`       | `c874f2101fb944637922b022b765a357f96f38ba1a1595c716e811df5ca5f85a` |
| Original `Refpoly/BoxReduction.lean`       | `85e9dbaef06e9702267cd8cf5f88b1b7339f026177f1cfde359e93291dacebfd` |
| Original `Refpoly/Algorithm.lean`          | `0071873dae87fa805377c5a24d4fddac291826300030bfb0f454a157c85b568d` |
| Original `Refpoly/WeightSystemFinite.lean` | `ee3474a8ede4ecaf80963191c95f1450a504f7cf6e62e60e1fb131c216f811f1` |
| Original `Refpoly/GeometricCWS.lean`       | `4ded4d265dc6341931d1de6b89badd45ac22fabe9fb9b1d68bbe61886b0b1be1` |
| Original `Refpoly/CircuitGeometry.lean`    | `4a290977596325ffd7cc1400b6edf1a9140caa9dcf47b60593f59d5a85ffc9de` |
| Original `Refpoly/CircuitInterior.lean`    | `fcc66c79d2b5751844b8533e7f32a52957a0d35815aa1889cd416a92b6d98678` |
| Original `Refpoly/CircuitSpan.lean`        | `db8fb5bf5303cbd72fbbb4c1c138cae0ba3a0bef8299c0ba3be8d9510f771d77` |
| Original `Refpoly/ReflexiveVolume.lean`    | `23d54cf6206a41b10bfaf206fde3e67bf532fb2c06207f785e3da3124781c589` |
| Original `Refpoly/Finiteness.lean`         | `0152ac3597310ff9783b30665faf7fc49c4ddf3e3ad8af5bc83b9f6c02a222af` |
| Original `Refpoly/MaxMin.lean`             | `cf4bf17f61a683564371093226ea904da42a5be6f9d0cf07183e0e826a74b6ef` |
| Original `Refpoly/Minimal.lean`            | `6b933a310872bb3757d60d49fc804594918d2aa09079877751cfb8de1951390e` |
| Original `Refpoly/Classification.lean`     | `7e7af6ebb7e278d80f8c20f0f8f78af880ac2a2ab3c4fab1345b576ebe908902` |
| Original `Refpoly/CWSBasis.lean`           | `4ded32847f3aa5fd23052d21985a0b07d432ff27bfac9f63e26ff9f428413170` |
| Original `Refpoly/CWSContainer.lean`       | `adc0f96f712f81cf184ffc3d6a9fce02aefa806685d22dfe3fbaf9eb818c1b85` |
| Original `Refpoly/CWSAssembly.lean`        | `6d103cf8d2a507c1568629e3afc41bd8fe32d30f84476acaf13cab0858f30cbd` |
| Original `Refpoly/DegreeBound.lean`        | `46819d084db5bf1ff03979773a72b7ae7ca9fc90c4bb6743cc93234909583103` |
| Original `Refpoly/Sylvester.lean`          | `ae1c4efb04ed58b3ec54cd6f8c7f596ee423c73d1eaf04f8520eeb7da92f2856` |
| Original `Refpoly/PyramidLemma.lean`       | `cb7603526f7d6b57f178346d2fc07d4c5d77b88890408aae1d9a97dc6b84027c` |
| Original `Refpoly/BoxReductionAudit.lean`  | `25bed54977bac2c617f752e3f80a54c4621c0fe3d79b688c363f477fbd5e97ea` |
| Original `Refpoly/CoverageAudit.lean`      | `891634c0dd7b8c00daf8980ddf2e421eb868a186a40be0c6ee59462384e9de59` |
| Full dependency manifest                   | `0d6def3ad257f03d2d0301868135497e764c1121813de38a7edbf81903510c1f` |

`Refpoly/Basic.lean` now ports the complete donor module after its original
introductory header, with every definition, theorem statement and proof body
unchanged. `Refpoly/ReflexiveDuality.lean` ports the complete donor source with
an attribution header added. The foundation chunk adds 24 `Basic` theorem bodies
and two duality theorem bodies beyond the initial extraction. Both modules are
imported and covered by the existing compiler-derived axiom audit.

`WeightSystem.lean` now ports the complete donor source after its original
introductory header. `BoxReduction.lean` ports the complete donor source with an
attribution header added. Their definitions, statements and proof bodies after
the first import are unchanged. This chunk adds 14 weight-system results and
16 box-reduction results; both modules are imported and compiler-audited.
`Algorithm`, `WeightSystemFinite` and `GeometricCWS` are now fully ported,
with 21 theorem/lemma bodies and their supporting definitions. All source after
the first import is unchanged; attribution headers qualify historical enumeration
prose. All three modules are imported and compiler-audited. The remaining donor
modules are not yet ported. Abstract reachability and classical finite sets do
not supply an executable search, and retained census/IP-check prose is not
verification of PALP.

Mathematical meanings and namespaces are retained. References to the donor's
`PROOF_PLAN.md` in original docstrings refer to the frozen donor package, not a
new specification in this checkout. Retained historical census prose is not a
certified count. The audit's restrictions on pseudoreflexivity, projection
lifting and existential box bounds continue to govern reuse.

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

The complete faithful `CircuitGeometry`, `CircuitInterior` and `CircuitSpan`
ports add 24 theorem/lemma bodies, including the private boundary helper.
Positive simplex circuits span the entire real relation kernel under the
specified intrinsic-interior hypothesis. All three modules are imported and
compiler-audited. Primitive integer normalization and the KS circuit contract
remain open; this port closes no KS obligation or scientific routine.

Every declaration and proof after the first import matches the frozen donor;
only attribution headers differ.

The faithful volume/finiteness and minimal-seed ports add `ReflexiveVolume`,
`Finiteness`, `MaxMin`, `Minimal` and `Classification`, with 35 source results.
The weight-finiteness → volume → box/finiteness dependency remains noncircular.
Bounds are existential and reflexive-only. Minimal structure gives one
step, without the new $2d$ seed bound. Legacy simplex signatures still omit
seed-to-weight witness connections; stronger geometric witnesses are required
for the new contracts. All modules are imported and compiler-audited; no KS
obligation or routine is closed.

Every declaration and proof after the first import matches the frozen donor.
Attribution headers qualify existential bounds, single-step structure and
legacy classification prose without changing statements or proofs.

The complete faithful `CWSBasis`, `CWSContainer` and `CWSAssembly` ports add
20 source results. They provide independent complete real equations, coordinate
coverage, explicit reached/padded blocks and forward-integral containers from a
finite abstract catalogue. Container maps do not prove inverse integrality;
noncomputable catalogues do not implement primitive integer search or lattice
realizations. All units 3 and 4 are now ported and compiler-audited. The eighteen
KS proof obligations and seven routines remain open. Unit 5 is next and needs
separate approval; clean donor blind-review closure remains outstanding.

Every declaration and proof after the first import matches the frozen donor.
Only attribution headers differ; they state the real-space, forward-integral
and noncomputable scope without changing the mathematical interfaces.

Unit 5 now ports the remaining five donor modules: `DegreeBound`, `Sylvester`,
`PyramidLemma`, `BoxReductionAudit` and `CoverageAudit`. Their 24 source results
and 50 axiom guards are imported and built. Degree bounds remain existential;
no sharp numerical certificate or general pyramid normalization is claimed.
The applicable donor library is now ported. Units 6–7 are authorized in the
same batch and remain in progress; this prerequisite port closes no KS hole.

All five modules match the frozen donor after the first import; only attribution
headers differ. Degree and pyramid scope restrictions remain explicit.
