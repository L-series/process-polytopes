# Reflexive KS statement contracts

The final target is coverage of **all reflexive lattice polytopes in every fixed
dimension**, up to origin-fixed `GL(d,Z)` equivalence. Output duplicates are
permitted. This does not certify exact normal forms, the operational 46-type
catalogue, PALP/CUDA code, a production census, or nonreflexive IP targets.

`Classifies p` states that for every natural dimension, every integer-coordinate
code in `enumerate p d` decodes to a reflexive polytope, and every reflexive
polytope is lattice-equivalent to one of those outputs. The composition handles
dimension zero directly. In positive dimensions it uses these major contracts:

| Obligation                | Required connection                                                                                                                 |
| ------------------------- | ----------------------------------------------------------------------------------------------------------------------------------- |
| `seed_bound`              | Reflexive target to a minimal IP dual-lattice seed with `d+1 <= N <= 2d`.                                                           |
| `circuit_basis`           | Complete relation-kernel basis, independent rows, exact circuit supports, positive normalized zero relations and primitive IP rows. |
| `weights_sound`           | Every generated ordered primitive weight code has the integer Newton-hull IP property in its specified kernel.                      |
| `weights_complete`        | Every required primitive IP weight code is generated; no empirical degree cutoff.                                                   |
| `assembly_sound`          | Generated CWS have exact rank/dimension, coordinate coverage and generated padded rows.                                             |
| `assembly_complete`       | Every valid bounded CWS whose rows are generated appears in exhaustive assembly.                                                    |
| `kernel_bases`            | A computed integer basis covers the whole finest kernel lattice, not just its real span.                                            |
| `geometric_containers`    | Seed/basis witnesses give a forward-integral real bijection placing the target inside its CWS container.                            |
| `container_points`        | Computed points equal all lattice points of the container.                                                                          |
| `subpolytope_recovery`    | Subsets of those points recover every relevant target image exactly.                                                                |
| `image_eligible`          | The target image is origin-IP and has an integral vertex-pairing matrix.                                                            |
| `eligibility_check`       | Exact candidate check, without demanding reflexivity in the finest ambient lattice.                                                 |
| `target_lattice`          | The target image lattice lies between the vertex-generated coarse and polar-defined fine endpoints.                                 |
| `finite_lattice_interval` | Only finitely many compatible intermediate additive subgroups exist for an eligible candidate.                                      |
| `realizations_sound`      | Reconstructed eligible realizations are reflexive and retain their exact hull/image-lattice witnesses.                              |
| `realizations_complete`   | Every compatible lattice is reconstructed with a basis and exact image-lattice equality.                                            |
| `realization_equivalence` | Equal geometric images and equal image lattices imply two-sided lattice equivalence.                                                |
| `reflexivity_check`       | The final Boolean acceptance is equivalent to mathematical reflexivity.                                                             |

`GeometricBasis` retains both the zero-padded row equality and its barycentric
relation to the selected vertices. `KernelBasisCorrect` describes a basis of the
**finest** lattice. The target's own forward-integral map is deliberately not
asserted to map onto that entire lattice.

The lower endpoint uses the candidate's **actual vertices**, not all redundant
points in its code. The upper endpoint consists of real points pairing
integrally with vertices of its polar; it need not equal the standard lattice.
`AddSubgroup.closure` here is algebraic subgroup generation, not topological
closure. Finite lattice intervals are asserted only for eligible full-dimensional
origin-IP lattice hulls with integral vertex-pairing matrix.

## Pipeline and unfinished implementation

The list-valued orchestration is defined, not postulated:

```text
weights -> bounded CWS assembly -> all container lattice points
  -> hulls of every point subset -> eligible candidates
  -> all compatible lattice realizations -> exact reflexivity filter
```

The seven routines in `Implementation.lean` remain explicit data holes:
weight search, CWS assembly, integer kernel basis, container lattice points,
eligibility test, lattice realizations, and reflexivity test. They are not
executable empty-list fallbacks. They must eventually be total computable
implementations; executable compilation is a separate strict requirement.

`classifies_of_contracts` is a genuinely proved implication from `Contracts p`.
The instantiated `classifies_reflexive` invokes that proof and the actual
routine/obligation constants, with no `sorry` in its own body. It still inherits
`sorryAx` from those unfinished inputs and is **not a completed classification
theorem**.

## Interface review and change policy

The scoped review accepted the conditional interfaces, without establishing
their admitted inputs. The informed checker covered eight critical endpoint
connections; the isolated blind checker reconstructed all 36 designated
declaration types. Both successful initial reports were frozen before comparison.
Their seed/row witnesses, interior, rank, image-lattice and final-equivalence
readings agree on those connections. Standard casts/topological/vector-space
instances are the pinned Mathlib ones; constructor meanings are also confirmed
by the concrete source records. This is not an independent audit of Mathlib.

The first tool-launched blind checker refused automatic exposure to project
intent/prior-audit metadata without opening the packet. The larger initial
informed task was cancelled and supplied no report; neither is counted as
successful evidence. The accepted blind check used a new tools-disabled CLI
outside the repository with no custom instructions, memory, resume or fork.
The informed endpoint check ran separately in a fresh context.

| Frozen review artifact           | SHA-256                                                            |
| -------------------------------- | ------------------------------------------------------------------ |
| Neutral compiler-derived packet  | `c821686fe18a07d64f36436d5c32ace0fe1638147df812b936a447c911330874` |
| Isolated blind initial report    | `44cc77756816952925c6e453e49715e2849d9ddbbb4291abca93e85b8b425ae1` |
| Informed endpoint initial report | `a1cc1f3d9e522862dadc697b6b0cd124bc0d460b0b9850a1f8fa44f05268af53` |

The informed findings are a **HIGH completion gate** (eighteen admitted
supporting results) and a **MEDIUM implementation/termination limitation**
(seven unfinished routines). Both are acknowledged WIP, not interface
mismatches: the strict gate remains red, and no completed or executable KS
classification is claimed. Reports and source ledgers stay outside Git.
This scoped review does not complete the pending full T-A0 synthesis.

The lock records compiler-printed target signatures, predicate/data definitions,
their explicit constant dependencies (including elided instances), and the
designated target inventory. It detects accidental statement weakening,
lost witnesses, changed predicate meanings, and dropped targets. Editing a lock
is not semantic approval: changes to mathematical interfaces require an explicit
approved chunk and affected fresh two-way checks. Routine proof-body ports with
unchanged audited interfaces do not require a duplicate whole-chain audit.

CI inspects all imported project constants' transitive axiom closures. Standard
foundations are `propext`, `Classical.choice`, and `Quot.sound`; `sorryAx` is
permitted only at assigned open/derived targets in conditional mode, nowhere in
strict mode. Unexpected axioms fail both modes. Already-closed declarations
must not regress, and execution/proof escape hatches are rejected.
