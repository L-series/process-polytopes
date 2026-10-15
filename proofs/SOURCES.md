# Sources and licensing

The proof package uses the repository's [MIT license](../LICENSE).
Supporting modules in `Refpoly/` originate from `refpoly-5d`, source revision
`6d71721e24f4cb4320cbcd059e6a9dc9dbeb50d1` plus the source patch identified
below. Attribution is retained in the module headers. Their mathematical
definitions, theorem statements and proof bodies are preserved; documentation
comments provide self-contained navigation and scope qualifications.
`Refpoly/KS/` supplies the executable construction and its geometric/lattice
connections. See [STATEMENTS.md](STATEMENTS.md) for the result and
[proof guarantees](../docs/proof-audit.md) for supporting-result restrictions.

## Source identities

The source patch supplements the named revision. The complete source ledger is
SHA-256 over sorted UTF-8 lines
`<raw-file SHA256><two spaces><repository-relative path><LF>`.
Individual hashes identify the original source bytes, before attribution and
documentation comments were adapted.

| Artifact                       | SHA-256                                                            |
| ------------------------------ | ------------------------------------------------------------------ |
| Source patch                   | `f4acf9b4c35bae350e8aac0a9f202e823d83d6f66d9f6cadbf74d397ec72590f` |
| Complete 29-file source ledger | `04ed9be4f1df0e0013f84a13566a6c39bbaef7f3f74503ae4cc09696acca4091` |
| Dependency manifest            | `0d6def3ad257f03d2d0301868135497e764c1121813de38a7edbf81903510c1f` |

| Original module in `Refpoly/` | SHA-256                                                            |
| ----------------------------- | ------------------------------------------------------------------ |
| `Basic.lean`                  | `a716a1508842b4f4ed1b24eaf978d43758efbdb87990b4a64e80c8795089dfc4` |
| `ReflexiveDuality.lean`       | `5389d21a9567b6cb0e22a10906b75f1592c488f7170d2c0af60301e96d3185ca` |
| `WeightSystem.lean`           | `c874f2101fb944637922b022b765a357f96f38ba1a1595c716e811df5ca5f85a` |
| `BoxReduction.lean`           | `85e9dbaef06e9702267cd8cf5f88b1b7339f026177f1cfde359e93291dacebfd` |
| `Algorithm.lean`              | `0071873dae87fa805377c5a24d4fddac291826300030bfb0f454a157c85b568d` |
| `WeightSystemFinite.lean`     | `ee3474a8ede4ecaf80963191c95f1450a504f7cf6e62e60e1fb131c216f811f1` |
| `GeometricCWS.lean`           | `4ded4d265dc6341931d1de6b89badd45ac22fabe9fb9b1d68bbe61886b0b1be1` |
| `CircuitGeometry.lean`        | `4a290977596325ffd7cc1400b6edf1a9140caa9dcf47b60593f59d5a85ffc9de` |
| `CircuitInterior.lean`        | `fcc66c79d2b5751844b8533e7f32a52957a0d35815aa1889cd416a92b6d98678` |
| `CircuitSpan.lean`            | `db8fb5bf5303cbd72fbbb4c1c138cae0ba3a0bef8299c0ba3be8d9510f771d77` |
| `ReflexiveVolume.lean`        | `23d54cf6206a41b10bfaf206fde3e67bf532fb2c06207f785e3da3124781c589` |
| `Finiteness.lean`             | `0152ac3597310ff9783b30665faf7fc49c4ddf3e3ad8af5bc83b9f6c02a222af` |
| `MaxMin.lean`                 | `cf4bf17f61a683564371093226ea904da42a5be6f9d0cf07183e0e826a74b6ef` |
| `Minimal.lean`                | `6b933a310872bb3757d60d49fc804594918d2aa09079877751cfb8de1951390e` |
| `Classification.lean`         | `7e7af6ebb7e278d80f8c20f0f8f78af880ac2a2ab3c4fab1345b576ebe908902` |
| `CWSBasis.lean`               | `4ded32847f3aa5fd23052d21985a0b07d432ff27bfac9f63e26ff9f428413170` |
| `CWSContainer.lean`           | `adc0f96f712f81cf184ffc3d6a9fce02aefa806685d22dfe3fbaf9eb818c1b85` |
| `CWSAssembly.lean`            | `6d103cf8d2a507c1568629e3afc41bd8fe32d30f84476acaf13cab0858f30cbd` |
| `DegreeBound.lean`            | `46819d084db5bf1ff03979773a72b7ae7ca9fc90c4bb6743cc93234909583103` |
| `Sylvester.lean`              | `ae1c4efb04ed58b3ec54cd6f8c7f596ee423c73d1eaf04f8520eeb7da92f2856` |
| `PyramidLemma.lean`           | `cb7603526f7d6b57f178346d2fc07d4c5d77b88890408aae1d9a97dc6b84027c` |
| `BoxReductionAudit.lean`      | `25bed54977bac2c617f752e3f80a54c4621c0fe3d79b688c363f477fbd5e97ea` |
| `CoverageAudit.lean`          | `891634c0dd7b8c00daf8980ddf2e421eb868a186a40be0c6ee59462384e9de59` |

## Dependencies and references

Lean is pinned to `leanprover/lean4:v4.30.0`. The nine-package
`lake-manifest.json` pins Mathlib to
`c5ea00351c28e24afc9f0f84379aa41082b1188f` (`v4.30.0`).
Mathlib's Apache-2.0 license and transitive dependency notices remain in their
respective repositories. PALP retains GPLv3; this proof license does not make
PALP-linked distributions MIT-only.

Primary mathematical references, exact editions and document hashes are in the
[literature notes](../docs/learning/README.md). Downloaded papers, source archives
and dependency caches are not distributed with this checkout.
