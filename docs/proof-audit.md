# Proof audit and source-to-contract ledger

This is the tracked synthesis of the frozen T-A0 source audit and the revised
dependency plan for the dimension-generic reflexive KS programme. It records
support and dispositions; the initial synthesis chunk changed documentation
only. The subsequently approved foundation chunk faithfully ports the complete
`Basic` and `ReflexiveDuality` modules. Further ports, statement repairs,
implementations and independent checks require their own approved chunks.
The subsequently approved weight/box chunk also ports the complete `WeightSystem`
and `BoxReduction` modules, preserving their original mathematical scopes.

The target remains `Refpoly.KS.classifies_reflexive : Classifies procedures`:
for each natural dimension, an executable finite list of reflexive polytopes
covers every reflexive lattice polytope up to origin-fixed $GL(d,\mathbb Z)$.
Duplicates are allowed. Nonreflexive IP targets, exact normal forms, the native
PALP/CUDA refinement and a production census are separate guarantees.

## Evidence, provenance and limitations

The inventory baseline is root commit
`4cfabbf96f2aba6da258068ecb252b02f4225683` on `integration/cws-5d`, with PALP
`968bb4b3c6c42a0893c95340e81718548e13f12a`. Only untracked `plan.md` was present
at entry. The donor is the reviewed snapshot of sibling HEAD
`6d71721e24f4cb4320cbcd059e6a9dc9dbeb50d1` **plus reviewed working-tree changes**.
The dirty sibling is not an input to this synthesis.

Private evidence is under the session artifact root
`/home/ahat01/.copilot/session-state/83ef73b2-4517-446c-9ac2-9437ff014965/files`:
`refpoly-proof-snapshot/proofs/`, `c00-provenance/`, `t-a0-audit/round-2/` and
`proof-contract-scaffold/review-1/`. These paths are provenance references, not
repository dependencies. Frozen sources and private reports remain out of Git.

| Evidence                               | SHA-256 / verification                                                                             |
| -------------------------------------- | -------------------------------------------------------------------------------------------------- |
| Complete donor source ledger           | `04ed9be4f1df0e0013f84a13566a6c39bbaef7f3f74503ae4cc09696acca4091`; all 29 files match             |
| Reviewed donor patch                   | `f4acf9b4c35bae350e8aac0a9f202e823d83d6f66d9f6cadbf74d397ec72590f`                                 |
| Nine-package dependency manifest       | `0d6def3ad257f03d2d0301868135497e764c1121813de38a7edbf81903510c1f`; donor and active package agree |
| T-A0 compiler inventory                | `53968db8d359bc477db012b9c362f6cdda8d7ec6bc02af48e92073fcc7c4bbfa`                                 |
| Round-2 informed initial report        | `5bc30d9e826ef62e6620c2730a743815c44d7bf479cbf99922da4520bfb1367b`                                 |
| Round-2 attempted blind initial report | `39ce2602658c7b3a31b827649cf46cd59a87795a7176c56f3e422cdd99e71f7f`                                 |
| Round-2 attempted blind addendum       | `fec94308a39a31d04ecc5f10088ecf56faaa190d180ef9ba922117ea4aef514b`                                 |

The 2026-10-15 inventory verified the donor ledger, patch, dependency manifest
and every file in the T-A0 archive manifest. The separate scaffold packet and
accepted reports match the hashes in [STATEMENTS.md](../proofs/STATEMENTS.md).
At the synthesis baseline its frozen mathematical sources matched the active
sources; the foundation chunk adds faithful donor bodies to `Basic` without
changing existing definitions or signatures. Post-review changes
in `ci/proof_audit.py` lock explicit constant dependencies and reject direct
holes in derived compositions; `.gitignore` additionally ignores Python caches.

The archived donor build used Lean 4.30.0, prebuilt pinned dependency artifacts
and an empty project build directory: exit zero, 8,499 jobs, no warnings or
admissions. Its compiler export covers 475 project constants. The informed
review found only `propext`, `Classical.choice` and `Quot.sound` in their axiom
closures, including all 71 designated statements. These are historical donor
results, not evidence that the stronger active KS endpoint is closed.

Round 1 is superseded because its blind provenance was unconfirmed. Round 2
froze both initial reports before comparison, but its attempted blind context
received `AGENTS.md`, a memory index and Git status including learning-note
filenames. Its disclosure says it read only the formal packet and supplement;
that does not establish the required isolation. **Retain round 2 as qualified
historical comparison, not an accepted clean blind audit.** The integrator's
reconciliation is not a third independent review. No disagreement was recorded
among flagged items; the 55 severity-none rows were not all compared
line by line.

The scaffold's later isolated, tools-disabled blind check is separate evidence
for its designated interfaces, not a replacement blind check of S01–S71.
Before claiming full T-A0 semantic closure, repeat the affected donor readings
in a verified neutral context using only sanitized types, definitions, binders
and instances. Since the exposure was context-wide, all 71 donor readings need
clean coverage; reuse the existing informed report while its inputs remain
unchanged. Resolve the outstanding explicit-index exports listed below in that
packet. Do not repeat the literature programme or a completed faithful port
audit merely to obtain this missing isolation evidence.

## Statement findings and dispositions

IDs S01–S71 refer to the frozen statement map. All names are in `Refpoly`, with
subnamespaces retained. The source catalogue below records every ID. The
following dispositions govern reuse; no statement or docstring is edited here.

| Finding                                              | Exact support and limitation                                                                                                                              | Disposition / remaining action                                                                                                                                                                                                                            |
| ---------------------------------------------------- | --------------------------------------------------------------------------------------------------------------------------------------------------------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| S33(v), S44: missing weight witness connection; high | `algorithm_enumerates_reflexive` and `reflexive_simplex_weight_system` assert existence of normalized IP weights without tying them to the specified seed | Retire these as evidence for the headline. Preserve their already-proved scoped results in a faithful port; use S09/S13/S36 with explicit geometric witnesses. A stronger replacement statement needs separate approval and affected fresh two-way checks |
| S02: degree claim; medium                            | `max_degree_dim5` is existential, not a proof of 3,263,442                                                                                                | Port with existential scope. No empirical cutoff or sharp-bound claim                                                                                                                                                                                     |
| S03, S54: pyramid scope                              | S03 proves its stated height obstruction; S54 treats a normalized family, not arbitrary $GL(4,\mathbb Z)$ normalization                                   | Keep the dimension and normalization hypotheses. Reflexivity-failure prose and stronger normalization are not proof evidence                                                                                                                              |
| S05: pseudoreflexivity                               | The theorem has an explicit lattice-polytope input, without the intended connection to $\Delta_q$                                                         | Accept the actual conditional use; specialization to Newton hulls remains a separate bridge                                                                                                                                                               |
| S25, S33: enumeration prose                          | Abstract reachability is not executable enumeration or a certified census                                                                                 | Port faithful theorem bodies; correct overreaching documentation in the approved port chunk, without changing types                                                                                                                                       |
| S29, S64: box bounds                                 | Radii are existential; S64 is reflexive-only                                                                                                              | Reuse as supporting theory. Do not use classical finite choice as executable box search                                                                                                                                                                   |
| S31, S60: volume bound                               | Same reflexive-only proposition, not general fixed-interior-count Hensley                                                                                 | Keep attribution and scope explicit; no general $k$ or numerical bound follows                                                                                                                                                                            |
| S34: component IP                                    | `cws_ip_component` requires the stated real projection-lifting hypothesis                                                                                 | Retain that hypothesis. Arbitrary independent covering CWS rows do not imply it; use S09/S46 for geometric blocks                                                                                                                                         |
| S15, S35, S36, S45, S62, S71: lattice direction      | Complete real pairing range and/or forward-integral containment; no inverse-integrality guarantee                                                         | Accept these scopes. Prove full integer-kernel and image-lattice bridges separately; do not upgrade containment to lattice equivalence                                                                                                                    |
| S45, S71: unused selection binder                    | Finite catalogue membership does not depend on `sel`                                                                                                      | Preserve signatures in faithful ports. Use S36/S62 for reachability, not the unused binder                                                                                                                                                                |
| S62, S71: seed provenance                            | Their conclusions omit lattice membership and dual containment of the seed                                                                                | Obtain those witnesses upstream from S36/S47 and carry them into the new `ValidSeed`; do not infer them from the weaker conclusions                                                                                                                       |
| S65: structural step                                 | One-step decomposition; strict relative rank decrease is implicit via S49                                                                                 | Accept that step. An iteration and the $2d$ upper bound remain new work                                                                                                                                                                                   |
| S70: certificate wrapper                             | `degree_dim5_le_of_certificate` unfolds `DegreeBoundDim5`                                                                                                 | Accept as an API wrapper only. No numerical certificate is supplied                                                                                                                                                                                       |
| S42: redundant conjunct                              | Final containment follows from maximality                                                                                                                 | Accept unchanged                                                                                                                                                                                                                                          |
| Other severity-none/helper readings                  | Scoped support is indexed below; frozen compiler closures are standard                                                                                    | Reusable within their actual hypotheses, subject to the isolation qualification above and fresh build/audit when imported                                                                                                                                 |

The blind addendum resolved constructor meanings and the important `S` versus
`T` indices in `ReachedCircuit` and `simplexCircuitWeights`. Its section D
still lacks explicit-index exports for S04, S16, S33(v), S44, S48, S56, S62(c),
S65, S71(d), and `IsGoodSimplex`. These are open packet-completeness items, even
where only one finset is in scope. The next isolated packet must include them.

The current predicates preserve ambient origin interior, the $-1$ polar
convention, primitive normalized positive rows, whole-kernel spanning, and
two-sided lattice preservation for `GLEquiv`. Algebraic subgroup generation in
`coarsest` is not topological closure. `Eligible` means origin-IP plus integral
vertex-pairing matrix, not reflexivity on the finest ambient lattice. The coarse
endpoint uses actual vertices, not redundant code points. Containers need not
be maximal or reflexive.

## Contract map: eighteen proof obligations

All eighteen contract rows are **open** in the active package, including those
with some library prerequisites now ported. `O` below means
`proofs/Refpoly/KS/Obligations.lean`; `I` means
`proofs/Refpoly/KS/Implementation.lean`. Donor paths are relative to its
`proofs/Refpoly/`. A numbered work unit refers to the dependency ledger below.
Ownership is proposed for a single reusable porter after approval; the
integrator owns shared docs, audit integration and Git. Proposed helper paths
are not existing implementations or permission to create them.
`KS/` file paths in the maps are relative to `proofs/Refpoly/`; module labels
identify donor files, not additional Lean namespaces.

| Obligation                | Existing declarations and exact support                                                                                                                                             | Required bridge / genuinely missing result                                                                                                            | Prerequisites; proposed owned files; acceptance                                                                                                                                    |
| ------------------------- | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | ----------------------------------------------------------------------------------------------------------------------------------------------------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `seed_bound`              | `ReflexiveDuality`: `IsReflexive.polarDual_reflexive` (S24), `MaxMin`: `exists_minimalIPGen_subset` (S55), `Minimal`: `minimalIPGen_card_lower` (S50), `minimal_structure` (S65)    | Extract a lattice seed from a finite dual generating set; prove $N\le2d$, enumerate distinct points and translate deletion-minimality                 | Units 1–4 then 6; `KS/Seeds.lean`, O; exact `ValidSeed` for every positive-dimensional reflexive target, with both bounds and no target input to the algorithm                     |
| `circuit_basis`           | `CircuitSpan`: `simplex_circuits_span_kernel` (S67), `CWSBasis`: `exists_circuitBasis` (S12), `cover`, `coordinate_count` (S51), `GeometricCWS`: `reflexive_simplex_block_ip` (S09) | Rationality and primitive integer normalization of each geometric circuit; preserve full span/independence under scaling and exact padding/reindexing | Units 2–4, 6; `KS/Circuits.lean`, O; `ValidCWS` and every `GeometricBasis` witness, not arbitrary unrelated IP rows                                                                |
| `weights_sound`           | `WeightSystem` defines integer Newton-hull IP; `DegreeBound` relates primitive normalization and finite weights                                                                     | Prove exact implemented IP filtering and normalization sound                                                                                          | Units 2, 7 and exact arithmetic unit 8; `KS/WeightSearch.lean`, I, O; every returned ordered code satisfies `PrimitiveIP`                                                          |
| `weights_complete`        | `Algorithm`: `descent_lemma` (S27), `algorithm_complete` (S25), `WeightSystemFinite.nodes_finite`, `normalized_ip_weight_systems_finite` (S07)                                      | Replace real classical selection with rational computation; guarded finite branching/depth; primitive encoding and order preservation                 | Units 2, 7–8; same owner/files as soundness; all primitive IP codes occur, including small arities, with no empirical degree bound                                                 |
| `assembly_sound`          | `CWSAssembly`: `padWeight`, `candidateRows`, `candidateCWS` give finite abstract padded-row families                                                                                | Finite list assembly and exact integer/rational rank/coverage tests linked to `ValidCWS`                                                              | Units 4, 7–8; `KS/Assembly.lean`, I, O; retain `WeightsSound` premise and prove every returned row is generated                                                                    |
| `assembly_complete`       | `CWS.RowsReached.mem_catalogue` (S26), finite `cwsCatalogue` membership                                                                                                             | Enumerate every allowed $N,r,m$, embedding and ordered row tuple, including permutations; bridge real normalized rows to natural codes                | Same assembly owner/prerequisites; every valid generated CWS appears as the exact code, not just an equivalent matrix                                                              |
| `kernel_bases`            | `CircuitBasis.pairing_range` (S35) proves real range; `BoxReduction` supplies integral-matrix tools                                                                                 | Verified integer normal-form/basis algorithm covering the saturated kernel lattice, with rank/dimension transport                                     | Unit 8; `KS/IntegerKernel.lean`, I, O; injective real basis map, exact range and integral preimage of every integral kernel vector                                                 |
| `geometric_containers`    | `CircuitBasis.reflexive_container` (S15), `algorithm_enumerates_cws_container` (S36) give forward-integral containment                                                              | Compare primitive padded equations and computed integer basis; lift seed pairings integrally into its coordinates                                     | Units 4, 6, 8; `KS/Containers.lean`, O; forward-integral real equivalence and inclusion in the specified integer hull; no inverse-integrality assumption                           |
| `container_points`        | `Basic`: `finite_latticePoints_of_isBounded`, lattice-polytope compactness                                                                                                          | Effective bounds from positive coordinate coverage and an exact kernel basis; enumerate integer coordinates and prove hull/intersection equality      | Unit 8; `KS/Containers.lean`, I, O; list equals **all** container lattice points, not only generating vertices                                                                     |
| `subpolytope_recovery`    | `Basic`: `IsLatticePolytope` gives finite integral generators; convex-hull image tools; existing `subpolytopes` enumerates sublists                                                 | Select the images of generators from the covering list, handling repetitions and empty/degenerate cases                                               | Unit 1; `KS/Recovery.lean`, O; membership in an actual sublist and exact hull equality for the stated universal input                                                              |
| `image_eligible`          | Reflexive primal/dual lattice definitions, duality and integral dot products                                                                                                        | Transport interior, polar extreme points and pairings under real equivalence                                                                          | Units 1, 8; `KS/Lattices.lean`, O; prove origin-IP and integral VPM for the exact target image                                                                                     |
| `eligibility_check`       | `Eligible` and `IntegralVPM` are specifications only                                                                                                                                | Exact rational hull/polar vertices, ambient-IP decision and integral pairing test                                                                     | Unit 8; `KS/PolytopeChecks.lean`, I, O; Boolean equivalence for every code, including degenerate inputs; do not require finest-lattice reflexivity                                 |
| `target_lattice`          | `Basic`: `IsLatticePoint.dotProduct_int`, duality; no donor theorem states the new endpoints                                                                                        | Show actual vertices lie in the image lattice and all its elements pair integrally with polar vertices                                                | Units 1, 8; `KS/Lattices.lean`, O; coarse $\le f(\mathbb Z^d)\le$ fine, with the same image map and exact hull                                                                     |
| `finite_lattice_interval` | `BoxReduction` finite quotient/submodule arguments are reusable tools; KS00 Lemma 7 is literature support                                                                           | Full-rank discrete endpoints and finite quotient; finite intermediate additive subgroups under eligibility                                            | Unit 8 endpoint geometry; owned in unit 9, `KS/Lattices.lean`, O; finiteness for arbitrary compatible `AddSubgroup`s under exactly the stated hypotheses, including dimension zero |
| `realizations_sound`      | No donor result reconstructs all compatible lattices; container results only give inclusion                                                                                         | Verified finite lattice reconstruction; polar/primal integrality implies reflexivity; preserve exact hull and compatible image witnesses              | Units 8–9; `KS/Realizations.lean`, I, O; every output on eligible input has the required reflexivity and reconstruction witnesses                                                  |
| `realizations_complete`   | Finite abstract container catalogue is insufficient; KS00 Lemma 7 supplies the mathematical route                                                                                   | Computably enumerate the finite interval, choose integral coordinates effectively and prove exact image-lattice/hull equalities                       | Same realization owner/prerequisites; reconstruct **every** compatible lattice, without noncomputable selection or assumed completeness                                            |
| `realization_equivalence` | `Basic`: `GLEquiv`, `IsUnimodular`, equivalence lemmas                                                                                                                              | Compose $e^{-1}f$; equal image lattices supply integrality in both directions                                                                         | Unit 1; `KS/Recovery.lean`, O; exact `GLEquiv P R.hull` with no extra reflexivity/full-dimension premise                                                                           |
| `reflexivity_check`       | `IsReflexive` is the specification; duality gives supporting theory                                                                                                                 | Exact full-dimensional origin-IP and integral polar test, linked to real hull semantics                                                               | Unit 8; `KS/PolytopeChecks.lean`, I, O; Boolean equivalence for every `PolyCode`, including zero dimension and malformed/degenerate hulls                                          |

## Implementation map: seven scientific routines

All seven bodies are **open** and currently `noncomputable`. Their replacements
must be total executable functions on their full input types. Correctness
contracts restrict some inputs; an implementation must still terminate on
invalid CWS/candidates. A terminating invalid-input branch is allowed only with
a proof that no valid input or required output is discarded.

| Routine                  | Source support                                                                         | Required effective construction and termination                                                                                                                                                             | Ownership and acceptance                                                                                                                            |
| ------------------------ | -------------------------------------------------------------------------------------- | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | --------------------------------------------------------------------------------------------------------------------------------------------------- |
| `searchWeights`          | `Algorithm`, `WeightSystemFinite`, `DegreeBound`; abstract reachability and finiteness | Exact rational feasible-point selection, finite integer branches, bounded depth, primitive conversion and exact IP check; or a separately justified effective complete search with no guessed degree cutoff | Weight-search owner: `KS/WeightSearch.lean`, I, O; compile and discharge both weight contracts; keep all ordered codes required by the signature    |
| `assembleCWS`            | `CWSAssembly`; noncomputable finite catalogue                                          | Loop over dimension-bounded indices and finite supplied pools, enumerate embeddings/tuples, decide rank and coverage; every loop has an explicit finite bound                                               | Assembly owner: `KS/Assembly.lean`, I, O; both assembly contracts, no geometric-realizability filter that could lose valid codes                    |
| `integerKernelBasis`     | `BoxReduction` integral matrices; real kernel identities in `CWSBasis`                 | Integer row/column reduction with verified termination and inverse transformations; entire integer kernel rather than merely independent vectors                                                            | Integer-kernel owner: `KS/IntegerKernel.lean`, I, O; `KernelBasisCorrect` and executable compilation                                                |
| `containerLatticePoints` | Bounded lattice-point finiteness in `Basic`                                            | Compute bounds for valid CWS and basis, enumerate a finite box, test exact inequalities; connect integer feasible points to the container hull                                                              | Container owner: `KS/Containers.lean`, I, O; `ContainerPoints`, and explicit handling of invalid inputs                                             |
| `eligibleCheck`          | No donor executable check; existing `Eligible` specification                           | Finite rational hull/polar calculations and exact origin-interior/VPM tests                                                                                                                                 | Exact-check owner: `KS/PolytopeChecks.lean`, I, O; `EligibilityCheck`, with redundant code points excluded from actual-vertex endpoint calculations |
| `latticeRealizations`    | No donor implementation; finite quotient and KS00 Lemma 7 route                        | Finite subgroup or bounded integer-factorization enumeration, computed bases and reconstructed vertex codes; settle row/column residue conventions before implementation                                    | Realization owner: `KS/Realizations.lean`, I, O; soundness/completeness and exact hull/image-lattice equalities, executable on every input          |
| `reflexiveCheck`         | No donor executable check; `IsReflexive` specification                                 | Exact polar and interior computation, proving integral polar vertices characterize the specified predicate                                                                                                  | Exact-check owner: `KS/PolytopeChecks.lean`, I, O; `ReflexivityCheck`, including dimension zero                                                     |

These owner labels describe coupled responsibilities for **one** dependency-aware
porter reused across approved chunks, not seven parallel assignments. Shared
exact rational linear algebra and hull tools should be reused across routines.
Only bounded independent mapping/review tasks may be delegated with disjoint
outputs. The integrator owns imports, audit wiring, documentation and publication.

## Revised dependency ledger and acceptance boundaries

The donor import graph is acyclic. Preserve its noncircular weight-finiteness
$\to$ reflexive-volume $\to$ box/finiteness direction. Do not replace the
constructive KS endpoint by generic bounded-box enumeration. Abstract proofs
may use approved standard classical foundations; scientific outputs may not be
implemented by noncomputable choice.

| Unit                                      | Source modules / deliverable                                                                                                  | Prerequisites and acceptance                                                                                                                                                                   |
| ----------------------------------------- | ----------------------------------------------------------------------------------------------------------------------------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| 0: synthesis                              | This ledger and synchronized status docs                                                                                      | Current approved documentation chunk; all findings disposed, all 25 holes mapped, isolation debt explicit; no mathematical changes                                                             |
| 1: foundations (ported)                   | Complete `Basic` and `ReflexiveDuality` (T-P1)                                                                                | Unit 0; faithful bodies, unchanged existing definitions/types; both modules build from landed imports and are imported by the umbrella/compiler audit                                          |
| 2: weights and box tools (ported)         | Complete `WeightSystem` and `BoxReduction` (T-P2)                                                                             | Unit 1; both imported and compiler-audited; topology, component-lifting and normalization hypotheses preserved                                                                                 |
| 3a: search support                        | `Algorithm`, `WeightSystemFinite`, `GeometricCWS` (T-P3)                                                                      | Unit 2; distinguish reachability/finite sets from executable lists                                                                                                                             |
| 3b: circuits                              | `CircuitGeometry`, `CircuitInterior`, `CircuitSpan` (T-P4)                                                                    | Unit 2; independent of 3a until combined downstream; preserve full relation-kernel spanning                                                                                                    |
| 4: finite geometry and containers         | `ReflexiveVolume`, `Finiteness`, `MaxMin`, `Minimal`, `Classification`, `CWSBasis`, `CWSContainer`, `CWSAssembly` (T-P5–T-P7) | Units 2, 3a, 3b; serialize coherent landed modules, not one large commit; keep weak legacy headlines scoped and retain stronger witnesses                                                      |
| 5: scoped side results and donor guards   | `DegreeBound`, `Sylvester`, `PyramidLemma`, `BoxReductionAudit`, `CoverageAudit` (T-P8–T-P9)                                  | Side results require 2/3a; complete audit imports require unit 4 and all side results; preserve original target inventory and closed results                                                   |
| 6: seed/circuit bridges                   | Bounded indexed minimal seeds, primitive circuit normalization, exact code transport                                          | Applicable unit 4 results; new proofs with existing contracts unchanged; no assumption that the eighteen holes are independent                                                                 |
| 7: effective WS/CWS search                | Weight search and exhaustive assembly with their four proofs                                                                  | Applicable 3a/4/5 support, exact arithmetic/checking from unit 8; finite branching and stopping must be constructive. Split shared arithmetic prerequisites before search if needed            |
| 8: exact geometry and kernel algorithms   | Rational linear algebra/hull tools, integer kernel, container points, eligibility/reflexivity checks; image/endpoint proofs   | Applicable 1–4 support; kernel/point and hull/check portions can land separately; exact acceptance and full-lattice basis proofs; no dependency on completed unit 7 is required                |
| 9: compatible lattices and reconstruction | Finite interval, executable realizations, soundness/completeness                                                              | Unit 8 and endpoint geometry; finite-index proof and exact reconstruction, then compose with seed/search/recovery results                                                                      |
| 10: endpoint and final audits             | All supporting contracts, concrete executable, fresh semantic/axiom/execution evidence                                        | Units 5–9 and recovery/equivalence bridges from unit 1; no direct/transitive `sorryAx`, no unexpected axioms; fresh full build and same-published-commit conditional/strict/CPU remote success |

Units are dependency groups, not batch authorizations. Unit 4 and algorithm
groups must be split into one-to-three logical commits per approved chunk.
Every new module must be imported and included in compiler-derived auditing;
an unimported file is not completion evidence. Preserve the locked major target
inventory. New public interfaces, predicate meanings or changes to existing
statement types require explicit approval and fresh affected two-way checks;
never regenerate a lock merely to suppress a comparison failure.

Units 1 and 2 are now ported: complete faithful `Basic`, `ReflexiveDuality`,
`WeightSystem` and `BoxReduction` modules, with imports, compiler-audit coverage,
attribution and status updates. The proposed next chunk is **unit 3a only**:
`Algorithm`, `WeightSystemFinite` and `GeometricCWS`. Preserve the distinction
between reachability/finite sets and executable search, positive-feasibility
guards and the geometric simplex witnesses. Keep faithful proof-body reuse
separate from interface repairs or new executable behaviour.
Acceptance remains targeted builds from landed imports, full pinned build,
conditional gate, strict gate with known incompleteness reported, CPU/lint
checks and remote results. A clean donor blind-review packet remains a separate
semantic closure task; it is not completed by the synthesis or faithful port.

## Remaining scope decisions and audit debt

- **Required for the current endpoint:** the seed upper bound, geometric
  primitive normalization, effective weight search, saturated integer kernels,
  exact hull/polar decisions, all-container-point recovery, compatible-lattice
  finiteness, constructive reconstruction and both-direction lattice equivalence.
  The maps above name their proof and execution acceptance boundaries.
- **Clean donor semantic review:** repeat the context-affected S01–S71 blind
  readings with verified isolation and complete explicit-index exports. The
  tracked synthesis is complete; full semantic review closure is still open.
- **Catalogue and optimization:** T-C0 requires CPU C02's integrated descriptor
  port. T-O0 requires T-P9 and T-C0. Request a scope decision when those become
  the remaining barrier; do not start CPU integration or mark them complete.
- **Optional structural refinements:** iterated simplex structures, no-lonely
  pruning and an exhaustive geometric structure catalogue need additional
  proofs if used. The current contracts can overgenerate independent covering
  rows and do not require the archived 47 structures/46 descriptors as a
  geometric certificate.
- **Scoped side mathematics:** general fixed-$k\ge1$ Hensley/LZ theorems,
  affine/marked equivalence for $k>1$, general WPS well-formedness, Skarke's
  low-dimensional reflexivity theorem, a sharp 3,263,442 certificate, IP proofs
  for the special `qct6`/`qlt6` vectors and the stronger pyramid normalization
  are not established by the donor inventory. Preserve existing scoped results;
  do not require these stronger claims merely to implement conservative search.
- **Outside this theorem:** nonreflexive one-point classification, exact normal
  forms/deduplication, hash/serialization correctness, PALP/CUDA refinement and
  production census validation remain separate. SS18's intended Newton-hull
  pseudoreflexivity specialization is not supplied by S05 alone.

## Source catalogue

The following table is an integrator synthesis of the frozen map and scoped
readings, not a copy of either private report. It covers all 71 designated
declarations. `Reuse` means reuse within the stated scope, subject to the
isolation limitation; `limited` marks a boundary explained above, not a failed
Lean proof. Module names are donor filenames without `.lean`.

| ID  | Donor module and declaration                                                   | Support / disposition                                               |
| --- | ------------------------------------------------------------------------------ | ------------------------------------------------------------------- |
| S01 | `CWSContainer`: `CircuitBasis.rows_reached`                                    | Padded row reachability                                             |
| S02 | `Sylvester`: `max_degree_dim5`                                                 | Existential degree bound only                                       |
| S03 | `PyramidLemma`: `pyramid_counterexample_dim_five`                              | Specific five-dimensional height obstruction                        |
| S04 | `Minimal`: `exists_proper_goodSimplex_of_not_affineIndependent`                | Proper simplex extraction                                           |
| S05 | `WeightSystem`: `pseudoreflexive`                                              | Conditional lattice-hull result; Newton connection absent           |
| S06 | `CircuitGeometry`: `ip_positive_circuit_through`                               | Positive circuit retaining prescribed point                         |
| S07 | `WeightSystemFinite`: `WeightSystemFinite.normalized_ip_weight_systems_finite` | Finite normalized real IP weights; no executable list               |
| S08 | `Sylvester`: `qct6_sum`                                                        | Special vector sum only                                             |
| S09 | `GeometricCWS`: `reflexive_simplex_block_ip`                                   | IP of the same geometric simplex weights                            |
| S10 | `DegreeBound`: `isIPWeightSystem_smul_iff`                                     | Positive scaling invariance                                         |
| S11 | `CWSAssembly`: `candidateRows_finite`                                          | Finite abstract padded row set                                      |
| S12 | `CWSBasis`: `exists_circuitBasis`                                              | Independent complete real circuit basis                             |
| S13 | `GeometricCWS`: `algorithm_reaches_simplex_block`                              | Reachability of the same geometric simplex weights                  |
| S14 | `WeightSystem`: `not_ip_of_weight_gt_half`                                     | Half-weight obstruction under stated dimension bound                |
| S15 | `CWSContainer`: `CircuitBasis.reflexive_container`                             | Forward-integral real container inclusion                           |
| S16 | `Minimal`: `exists_maximal_dim_proper_relativeIP`                              | Maximal-rank proper relative-IP subset                              |
| S17 | `DegreeBound`: `primitive_ip_degrees_bounded`                                  | Existential primitive degree bound                                  |
| S18 | `Sylvester`: `qct6_pos`                                                        | Special vector positivity only                                      |
| S19 | `CWSContainer`: `CircuitBasis.blocks_reached`                                  | Reachability of supported basis blocks                              |
| S20 | `GeometricCWS`: `simplex_relation_kernel`                                      | Complete simplex relation kernel                                    |
| S21 | `Basic`: `polarDual_antitone`                                                  | Polar reverses inclusion                                            |
| S22 | `MaxMin`: `exists_minimal_subset`                                              | Minimal reflexive subset, not minimal IP seed                       |
| S23 | `CircuitInterior`: `exists_positive_relation_of_intrinsicInterior`             | Strict positive normalized relation                                 |
| S24 | `ReflexiveDuality`: `IsReflexive.polarDual_reflexive`                          | Reflexive dual closure                                              |
| S25 | `Algorithm`: `algorithm_complete`                                              | Target-path reachability, not whole executable search               |
| S26 | `CWSAssembly`: `CWS.RowsReached.mem_catalogue`                                 | Abstract finite catalogue membership                                |
| S27 | `Algorithm`: `descent_lemma`                                                   | Separating Newton point for target descent                          |
| S28 | `CircuitSpan`: `normalized_relation_mem_span_circuits`                         | Normalized relations lie in circuit span                            |
| S29 | `BoxReduction`: `BoxReduction.box_of_volume_bound`                             | Existential box radius                                              |
| S30 | `Sylvester`: `qlt6_pos`                                                        | Special vector positivity only                                      |
| S31 | `Finiteness`: `hensley_volume_bound`                                           | Reflexive-only volume bound                                         |
| S32 | `Minimal`: `IsMinimalIPGen.isMinimalIPRel`                                     | Ambient-minimal implies relative-minimal                            |
| S33 | `Classification`: `algorithm_enumerates_reflexive`                             | Simplicial weights lack seed relation; exclude as headline evidence |
| S34 | `WeightSystem`: `cws_ip_component`                                             | Component IP requires explicit lifting                              |
| S35 | `CWSBasis`: `CircuitBasis.pairing_range`                                       | Real pairing image, not integer-lattice surjectivity                |
| S36 | `CWSContainer`: `algorithm_enumerates_cws_container`                           | Geometric seed and forward container coverage                       |
| S37 | `Basic`: `interior_latticePoint_unique`                                        | Unique interior lattice point for reflexive input                   |
| S38 | `DegreeBound`: `primitive_ip_weight_systems_finite`                            | Finite primitive IP weights, not executable list                    |
| S39 | `Basic`: `bipolar`                                                             | Closed convex bipolar equality with origin hypothesis               |
| S40 | `DegreeBound`: `normalizedWeights_injective_on_primitive`                      | Primitive normalization is injective                                |
| S41 | `Minimal`: `exists_goodSimplex_through`                                        | Good simplex through a generator                                    |
| S42 | `Classification`: `maximal_reflexive_covered_by_minimalIP_container`           | Maximal-reflexive container coverage                                |
| S43 | `WeightSystem`: `ip_half_reduction`                                            | Half-weight equivalence with changed depth                          |
| S44 | `Classification`: `reflexive_simplex_weight_system`                            | Weights lack seed relation; exclude as headline evidence            |
| S45 | `CWSAssembly`: `finite_ws_cws_container_coverage`                              | Finite abstract containers; unused selection binder                 |
| S46 | `GeometricCWS`: `simplex_pairing_surjective`                                   | Real simplex pairing surjectivity                                   |
| S47 | `Classification`: `classification_coverage`                                    | Minimal dual seed provenance                                        |
| S48 | `Minimal`: `simplex_card`                                                      | Simplex cardinality from ambient IP                                 |
| S49 | `Minimal`: `proper_relativeIP_dim_lt`                                          | Proper relative-IP subset has smaller rank                          |
| S50 | `Minimal`: `minimalIPGen_card_lower`                                           | Ambient-IP cardinal lower bound                                     |
| S51 | `CWSBasis`: `CircuitBasis.coordinate_count`                                    | Coordinate count from full-dimensional IP                           |
| S52 | `WeightSystem`: `at_most_one_half`                                             | At-most-one-half obstruction under dimension hypotheses             |
| S53 | `CWSAssembly`: `candidateCWS_finite`                                           | Finite abstract CWS set                                             |
| S54 | `PyramidLemma`: `pyramid_lemma_dim_le_four`                                    | Normalized low-dimensional pyramid family only                      |
| S55 | `MaxMin`: `exists_minimalIPGen_subset`                                         | Deletion-minimal IP subset of finite generators                     |
| S56 | `Minimal`: `simplex_weights`                                                   | Real normalized simplex relation, not primitive encoding            |
| S57 | `CircuitInterior`: `positive_relation_intrinsicInterior`                       | Intrinsic interior from positive relation                           |
| S58 | `MaxMin`: `exists_maximal_superset`                                            | Maximal reflexive superset                                          |
| S59 | `CircuitInterior`: `finset_positive_relation_intrinsicInterior`                | Finset positive relation gives intrinsic interior                   |
| S60 | `ReflexiveVolume`: `ReflexiveVolume.volume_bound`                              | Reflexive-only volume bound                                         |
| S61 | `CircuitGeometry`: `positive_circuit_through`                                  | Positive affine circuit through a prescribed point                  |
| S62 | `CWSContainer`: `ws_cws_container_coverage`                                    | Forward container/reachability; seed provenance omitted             |
| S63 | `Finiteness`: `finitely_many_reflexive`                                        | Finite GL-cover, not executable list or unique representatives      |
| S64 | `Finiteness`: `lagarias_ziegler_box`                                           | Existential reflexive box bound                                     |
| S65 | `Minimal`: `minimal_structure`                                                 | One-step minimal decomposition, not iterated upper bound            |
| S66 | `MaxMin`: `maximal_iff_dual_minimal`                                           | Maximal/minimal reflexive polarity correspondence                   |
| S67 | `CircuitSpan`: `simplex_circuits_span_kernel`                                  | All simplex circuits span entire relation kernel                    |
| S68 | `Sylvester`: `qlt6_sum`                                                        | Special vector sum only                                             |
| S69 | `CircuitGeometry`: `caratheodory_step`                                         | Caratheodory coefficient exchange helper                            |
| S70 | `Sylvester`: `degree_dim5_le_of_certificate`                                   | Unfolds assumed certificate; no numerical certificate               |
| S71 | `CWSAssembly`: `ws_cws_catalogue_coverage`                                     | Finite catalogue coverage; unused selector and omitted provenance   |

Supporting declarations outside S01–S71 are not silently promoted to separately
audited statements: these include `branch_finite`, `average_pos`, `reach_step`,
`positiveSelection`, `nodes_finite`, `latticePoint_count_bound`,
`subset_iff_polarDual_subset`, `IsReflexive.bipolar_eq`,
`ip_simplex_iff_weights_pos`, `finset_positive_relation_interior`,
`conical_caratheodory`, projected-circuit helpers,
`nonnegative_relation_mem_span_circuits`, `CircuitBasis.nonneg`, `cover`,
`toCWS_mem`, `CWS.RowsReached.row_mem`, `max_degree_factorization`,
`prod_sylvester`, `sylvester_sum_inv`, the reflexive-volume helpers,
`BoxReductionAudit`, `CoverageAudit` and `GLEquiv` helper results. Their faithful
ports require landed imports, compilation and transitive axiom checks; any
additional semantic review must be scoped to their use and actual interfaces.

## Progress and validation record

At the synthesis baseline, eight designated targets are closed: lattice zero,
the three `GLEquiv` equivalence lemmas, `originCode_hull`, `reflexive_zero`,
`reflexive_zero_unique` and `classifies_of_contracts`. Eighteen proof holes and
seven routine holes remain. `procedures`, `contracts` and the concrete headline
inherit those holes. The initial documentation chunk closed **zero** Lean results.

The baseline's conditional and CPU CI passed; strict CI failed intentionally.
Historical run references are recorded in the uncommitted planning artifact.
Fresh local validation for this documentation chunk passed the conditional
gate, including all 19 audit/compiler regressions and the pinned project build;
strict failed on the same 25 direct and three transitive `sorryAx` targets.
All 20 exact CPU CWS/NF regressions, full native lint, documentation coverage/link
checks and whitespace checks passed. The new Markdown file was formatted and
checked explicitly as well as included in the final tracked-file lint.

Native tools were Python 3.10.12, clang-format 21.1.8, Prettier 3.6.2, shfmt
3.13.1 and Node 24.21.0; Lean/Mathlib pins remain unchanged. A bounded consistency
review corrected module/namespace notation and a self-referential prerequisite;
it was not a fresh independent statement audit. No new literature or TeX claims
changed, so no literature compilation was rebuilt. Remote run results are
reported at publication; none of these dispositions is a new mathematical
certificate.

### Foundation port: unit 1

The approved foundation chunk ports the remaining `Basic` module and the complete
`ReflexiveDuality` module. Compared with the initial extraction it adds 26
explicit donor theorem bodies: 24 in `Basic` and two in `ReflexiveDuality`.
Every definition, statement and proof from the first import onward is
byte-identical to the verified donor; only attribution/header comments differ.
The source ledger was checked again before reuse. A bounded source-fidelity
review also confirmed preservation and audit wiring; it is not a new blind
semantic check.

The newly available results include integral pairings, lattice-polytope
convexity/compactness/closedness, lattice-hull identities, convex-hull inequality
transport, polar order/closedness/convexity and bipolarity, linear-functional
helpers, bounded lattice-point finiteness, lattice-equivalence invariants,
unique interior lattice point, reflexive bipolarity and reflexive dual closure.
Their original hypotheses and dimensions are unchanged.
The designated donor readings S21, S24, S37 and S39 now have imported proof
bodies; the other S01–S71 declarations remain outside this foundation chunk.

`Refpoly.lean` imports `ReflexiveDuality`, which imports `Basic`. The existing
exporter consequently includes all 57 constants defined in these modules,
including generated helpers and structure projections. All have only approved
standard foundations in their compiler-derived axiom closures. All 37 exported
theorem types in these modules, including generated declarations, agree with
the frozen donor compiler inventory after whitespace normalization. No major
target, semantic lock, dependency pin, KS definition or procedure changed.
No lock regeneration or new mathematical statement was needed.

Targeted module builds, the full pinned project build and the conditional gate
pass, including the 19 audit/compiler regressions. The designated eight closed
targets remain closed. The same eighteen KS proof holes, seven routine holes
and three transitive targets remain; the foundation port supplies prerequisites
without discharging any of those holes. Strict remains an explicit failing
completeness gate: its local failure consists of exactly the known 25 direct
and three transitive `sorryAx` targets, with no other audit failure. All 20 CPU
regressions and full native lint pass. Remote results are reported with the
published chunk. Native tool versions are the same as the synthesis record.
No literature claim or compilation changed.

### Weight-system geometry and box reduction: unit 2

The approved weight/box port adds complete `WeightSystem` and `BoxReduction` modules,
including 14 weight-system theorem bodies, 16 box-reduction theorem/lemma bodies
and their supporting definitions. Both modules match the verified frozen source
byte-for-byte after the first import. The 29-file donor ledger was rechecked
before copying; attribution headers and source identities are recorded in
[SOURCES.md](../proofs/SOURCES.md). A bounded source-fidelity review found no
declaration or audit-wiring discrepancy; it is not a new blind semantic review.

The imported donor catalogue now additionally includes S05 (`pseudoreflexive`),
S14 (`not_ip_of_weight_gt_half`), S29 (`BoxReduction.box_of_volume_bound`),
S34 (`cws_ip_component`), S43 (`ip_half_reduction`) and S52 (`at_most_one_half`).
S05 remains an abstract lattice-hull theorem without a Newton-hull identification;
S34 retains its explicit real projection-lifting hypothesis. The half-weight
reduction retains positivity and total weight $1/2$, with depth-2 IP on the
truncated system; the obstruction lemmas retain their dimension hypotheses.
Interior remains in the specified kernel subtype.

Box reduction assumes a volume bound and ambient origin interior, includes
dimension zero, and returns an existential natural radius. The proof uses
classical choices of bounded-determinant lattice representatives. Its
noncomputable supporting definitions are proof-library objects, not executable
scientific routines. Neither a computed radius nor a sharp numerical estimate
is claimed. Historical source prose about census counts is not a new certified
count; the existing audit dispositions continue to apply.

The umbrella imports `BoxReduction`; the KS import chain already imports
`WeightSystem`. Compiler export includes all 84 constants defined in these two
modules, including generated helpers. Their transitive axiom closures use only
approved standard foundations, and all 54 exported theorem types agree with the
frozen donor compiler inventory after whitespace normalization. The existing
eight designated closed targets remain closed. Existing locked definitions,
target inventory, statement lock, pins and scientific routine bodies are
unchanged; no lock regeneration is needed for this faithful additive port.

Targeted and full pinned builds, the conditional gate, all 19 audit/compiler
regressions, all 20 exact CPU regressions and full native lint pass. Strict fails
only on the same 25 direct and three transitive `sorryAx` targets; remote results
are reported with the published chunk. The same eighteen KS
proof holes, seven routine holes and three transitive targets remain; this
chunk ports prerequisites without discharging them. The next proposed chunk is
unit 3a's abstract search and geometric simplex support. Native tool versions
remain as recorded above; no literature claim or compilation changed.

See [proof commands](../proofs/README.md),
[statement policy](../proofs/STATEMENTS.md),
[source attribution](../proofs/SOURCES.md),
[theory roadmap](learning/THEORY_ROADMAP.md), and
[agent isolation protocol](THEORY_AGENTS.md).
