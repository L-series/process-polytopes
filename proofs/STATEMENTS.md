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

## Concrete pipeline and compiler closure

The list-valued orchestration is defined, not postulated:

```text
weights -> bounded CWS assembly -> all container lattice points
  -> hulls of every point subset -> eligible candidates
  -> all compatible lattice realizations -> exact reflexivity filter
```

All seven routines in `Implementation.lean` have total computable implementations:
complete ordered primitive-IP weight search, CWS assembly, saturated integer
kernel bases, all container lattice points, eligibility checks,
compatible-lattice realizations, and reflexivity checks. All eighteen supporting
contracts are proved.
An input rejected by a total routine has an explicit invalid-input theorem;
no dummy output substitutes for valid-input coverage.

Exact hull/polar decisions distinguish actual vertices from redundant code
generators. Realizations enumerate subgroups of a finite determinant quotient,
test both compatibility endpoints, and reconstruct every accepted lattice with
exact hull and image-lattice equalities. All nine registered native suites and
the strict compiler closure gate pass; finite native fixtures supplement rather
than replace the dimension-generic proof.

`classifies_of_contracts` is a genuinely proved implication from `Contracts p`.
The instantiated `classifies_reflexive` invokes that proof and the actual
routine/obligation constants, with no `sorry` in its body or transitive
dependencies. The concrete theorem is **compiler-closed**, as are all 36
designated targets. The complete 1,308-constant project inventory has no
`sorryAx` or unexpected mathematical axioms.

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

The initial informed findings were a **HIGH completion gate** (eighteen admitted
supporting results) and a **MEDIUM implementation/termination limitation**
(seven unfinished routines). These described the acknowledged scaffold WIP, not interface
mismatches. All those supporting results and routines are now implemented, with
strict and conditional gates passing. Reports and source ledgers stay outside Git.
This scoped review is separate from the
[tracked T-A0 synthesis](../docs/proof-audit.md), which now records the complete
source-to-contract map and findings. The older round-2 donor report remains qualified because its context received
excluded metadata. A new isolated, tools-disabled formal reading covers all 71
unchanged donor statements, including the formerly missing explicit S/T indices.
Its initial report and the unchanged informed report were frozen before
comparison. All literal statement readings were reconciled; two low-severity
wording qualifications are recorded without editing either initial report.
See the audit ledger for hashes and the scope of this closure.

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
