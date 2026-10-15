# Proof guarantees and scope

The concrete theorem
`Refpoly.KS.classifies_reflexive : Classifies procedures`
covers every reflexive lattice polytope in each fixed dimension by a finite
executable list, up to origin-fixed $\mathrm{GL}(d,\mathbb Z)$ equivalence.
Every output is reflexive; duplicates are permitted.
The [statement guide](../proofs/STATEMENTS.md) specifies the eighteen supporting
contracts and seven computational routines.

## Compiler and execution guarantees

The proof gates build current sources and export declaration types,
definitions, explicit constant dependencies and transitive axiom closures.
Strict mode requires all designated targets and their dependencies to be
closed, rejects `sorryAx` and unexpected axioms, and compiles the concrete
pipeline. The only permitted foundational axioms are `propext`,
`Classical.choice` and `Quot.sound`.

The reviewed lock in `proofs/ci/statement-lock.json` protects signatures,
predicate/data meanings, dependencies and the target inventory. Changes that
weaken a hypothesis, discard a reconstruction witness or change a standard
instance must not be hidden by regenerating that lock. Semantic review is
required for mathematical interface changes.

Source-policy checks reject proof/execution escape hatches. Audit-policy
regressions exercise signature and definition drift, missing targets,
unregistered admissions, transitive admissions and unexpected axioms.
Executable suites cover exact kernels, points, hull/polar checks, weights,
assembly, congruence lattices, realizations and the concrete endpoint.
Finite examples are regressions, not the dimension-generic completeness proof.

CI caches only pinned dependencies, not current-project build products.
Dependency identities and source attribution are recorded in
[SOURCES.md](../proofs/SOURCES.md). This is not an independent verification of
Lean's implementation or Mathlib.

## Mathematical connections

| Connection                             | Main implementation and supporting results                                                                                                                                       |
| -------------------------------------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Bounded dual seeds                     | `KS/Seeds`, `KS/SeedBound`; reflexive duality, finite minimal IP subsets and the one-step minimal decomposition                                                                  |
| Primitive complete circuit rows        | `KS/WeightEncoding`, `KS/RationalWeights`, `KS/CircuitWeights`, `KS/Circuits`; `simplex_circuits_span_kernel`, `exists_circuitBasis`, `reflexive_simplex_block_ip`               |
| Complete finite weights and CWS        | `KS/RationalSelection`, `KS/WeightSearch`, `KS/Assembly`; exact positive selection, bounded branches, strict affine-rank growth, primitive normalization and exact IP acceptance |
| Saturated kernel and container         | `KS/IntegerKernel`, `KS/IntegerPoints`, `KS/Containers`; integer preimages, effective point bounds and the exact seed-pairing range                                              |
| Exact hull and polar decisions         | `KS/RationalSimplex`, `KS/RationalHull`, `KS/PolytopeChecks`; rational barycentric and active-constraint witnesses, actual extreme points and ambient origin interior            |
| Subpolytope and image-lattice recovery | `KS/Recovery`, `KS/Lattices`; exact hull images, compatible endpoints, finite quotient and two-sided lattice equivalence                                                         |
| Exhaustive lattice reconstruction      | `KS/CongruenceLattice`, `KS/LatticeCoordinates`, `KS/Realizations`; subgroup enumeration, whole integer bases and exact hull/image-lattice equalities                            |
| Classification                         | `KS/Composition`, `KS/Implementation`, `KS/Obligations`; the contracts applied to the concrete procedures                                                                        |

The full relation kernel and the saturated integer kernel are distinct
requirements. Forward-integral containment is not upgraded to inverse
integrality. The target's image lattice is retained explicitly and recovered
from the compatible interval. Eligibility uses actual primal/polar vertices,
not redundant generators, and does not demand reference-lattice reflexivity.

## Supporting-result restrictions

The imported source library supplies reusable geometry within its actual
hypotheses. Its stronger-looking names and comments do not enlarge those types.

| Supporting result                                                                  | Scope                                                                                                                                                                                                     |
| ---------------------------------------------------------------------------------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `algorithm_enumerates_reflexive`, `reflexive_simplex_weight_system`                | Their weight witnesses are not linked to the chosen seed. They are not evidence for the concrete headline; `GeometricBasis` and the explicit circuit/container connections supply the required witnesses. |
| `max_degree_dim5`, `primitive_ip_degrees_bounded`, `degree_dim5_le_of_certificate` | Existential bounds or certificate interfaces, not a proof of the sharp number 3,263,442. The executable search uses no such cutoff.                                                                       |
| `PyramidLemma`                                                                     | The height obstruction and normalized low-dimensional family retain their dimension and normalization hypotheses. No arbitrary-base normalization theorem is inferred.                                    |
| `pseudoreflexivity`                                                                | Retains its explicit lattice-polytope hypotheses; it is not automatically a theorem about an unrelated Newton hull.                                                                                       |
| `box_of_volume_bound`, `lagarias_ziegler_box`                                      | Existential radii; the latter is reflexive-specific. They do not supply executable numerical search bounds.                                                                                               |
| `volume_bound`, `hensley_volume_bound`                                             | Reflexive-only statements, not the general fixed-interior-count Hensley theorem or a numerical estimate.                                                                                                  |
| `cws_ip_component`                                                                 | Requires the explicit real projection-lifting hypothesis. Independent covering rows alone do not imply component IP.                                                                                      |
| `minimal_structure`                                                                | One-step relative decomposition. The general $2d$ seed bound is proved separately in `KS/SeedBound`.                                                                                                      |
| Abstract reachability and finite catalogues                                        | Do not themselves compute a list, prove native descriptor coverage or identify all compatible lattices.                                                                                                   |

## Computational boundary

The Lean algorithms use exact arithmetic and proved terminating searches.
Some searches enumerate integer boxes, support tuples, point subsets or
subgroups of finite quotients; no efficient high-dimensional runtime is claimed.
Proof-side real-linear maps and classical existence arguments do not select
the returned computational data.

PALP/CUDA refinement, the operational descriptor catalogue, exact normal forms,
serialization, collision-safe deduplication and production counts are separate
guarantees. Native CWS/NF tests compare exact accepted rows and primal normal
forms with multiplicities, but do not prove those additional guarantees.
Classification of nonreflexive IP final targets is outside the theorem.
