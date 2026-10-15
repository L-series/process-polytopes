# Dimension-generic reflexive KS proofs

This package kernel-proves the concrete terminating KS WS/CWS construction
recovering all reflexive lattice polytopes
in every fixed dimension, with lattice realizations and subpolytope search.
Nonreflexive IP classification is excluded; IP weights and possibly nonreflexive
containers remain intermediate objects.

The package contains all applicable faithful donor modules, eighteen unchanged
major contracts, finite-list orchestration, and the closed conditional composition
and dimension-zero results. All seven scientific routines, eighteen supporting
contracts, and `classifies_reflexive : Classifies procedures` are implemented and
compiler-closed. The inventory contains 1,308 project constants and all 36
designated targets are closed, using only `propext`, `Classical.choice` and
`Quot.sound`. Local strict and conditional gates pass without changing the
reviewed statement lock, major target inventory, toolchain or dependency pins.
See [STATEMENTS.md](STATEMENTS.md) for the exact chain,
[SOURCES.md](SOURCES.md) for attribution and pins, and the
[audit ledger](../docs/proof-audit.md) for evidence and remaining semantic debt.

The new KS modules prove the general indexed $2d$ minimal-seed bound and exact
primitive circuit transport. They implement exhaustive assembly, saturated
integer kernels, all-container-point enumeration, exact rational simplex/hull/
polar operations, and eligibility/reflexivity decisions. Compatible-lattice
finiteness, exhaustive finite quotient subgroups, verified integer bases, exact
hull/image-lattice reconstruction and two-sided equivalence are proved.
Complete ordered primitive-IP search uses rational feasible selection, finite
exact branches and a proved rank-based stopping depth. No empirical weight-degree
cutoff or classical output selection is used. Nine native execution suites cover
the scientific routines, arity 0--3 weight outputs, compatible lattices and the
concrete pipeline in dimensions zero and one. These regressions are not the
dimension-generic completeness proof. A clean project rebuild passes both proof
gates. The final tools-disabled neutral donor reading covers all 71 source
statements and closes the historical isolation debt, with frozen hashes and two
explicit low-severity prose qualifications in the audit ledger.

## Faithful donor ports

The foundation port adds 26 donor theorem bodies beyond the initial extraction,
including compactness, bounded lattice-point finiteness, bipolarity, uniqueness
of the interior lattice point, lattice-equivalence invariants and reflexive
dual closure. `Refpoly.lean` imports the new duality module, so the existing
compiler exporter and axiom audit inspect both modules and all their helpers.
The eight designated closed targets and statement lock remain unchanged; none
of the eighteen KS proof holes or seven routine holes is discharged by this port.

The weight/box chunk adds 30 donor theorem/lemma bodies: 14 in `WeightSystem`
and 16 in `BoxReduction`, together with their supporting definitions. Kernel
subspace topology, half-weight depth changes and the explicit CWS component
lifting hypothesis are retained. Box reduction assumes a volume bound and
returns an existential radius; it is not an effective search bound. Both
modules are imported and covered by the existing compiler audit. No KS hole is
closed, and no routine becomes executable in this chunk.

The faithful `Algorithm`, `WeightSystemFinite` and `GeometricCWS` ports add 21
results and supporting definitions. Reachability remains abstract. The
normalized-IP finiteness proof uses positive-feasibility-guarded nodes and
classical selection; neither supplies executable search. Geometric simplex
IP/reachability retains the explicit affine independence, normalized positive
zero relation and dual
lattice-point witnesses, including lower-dimensional blocks. All three modules
are imported and compiler-audited; no KS obligation or routine is closed.

The complete faithful `CircuitGeometry`, `CircuitInterior` and `CircuitSpan`
ports add 24 theorem/lemma bodies, including the private boundary helper.
Positive simplex circuits span the entire real relation kernel under the
specified intrinsic-interior hypothesis. All three modules are imported and
compiler-audited. The port itself did not supply primitive integer normalization;
the new KS circuit modules now provide that separate bridge.

The faithful volume/finiteness and minimal-seed ports add `ReflexiveVolume`,
`Finiteness`, `MaxMin`, `Minimal` and `Classification`, with 35 source results.
The weight-finiteness → volume → box/finiteness dependency remains noncircular.
Bounds are existential and reflexive-only. Minimal structure gives one
step; the new $2d$ seed bound is proved separately in `KS/SeedBound`. Legacy simplex signatures still omit
seed-to-weight witness connections; stronger geometric witnesses are required
for the new contracts. All modules are imported and compiler-audited; no KS
obligation or routine is closed.

The complete faithful `CWSBasis`, `CWSContainer` and `CWSAssembly` ports add
20 source results. They provide independent complete real equations, coordinate
coverage, explicit reached/padded blocks and forward-integral containers from a
finite abstract catalogue. Container maps do not prove inverse integrality;
noncomputable catalogues do not implement primitive integer search or lattice
realizations. All units 3 and 4 are ported and compiler-audited; the executable
KS modules supply these additional connections. The final isolated donor
reading and frozen-report comparison are recorded in the audit ledger.

Unit 5 now ports the remaining five donor modules: `DegreeBound`, `Sylvester`,
`PyramidLemma`, `BoxReductionAudit` and `CoverageAudit`. Their 24 source results
and 50 axiom guards are imported and built. Degree bounds remain existential;
no sharp numerical certificate or general pyramid normalization is claimed.
The applicable donor library is now ported. The requester authorized finishing
the remaining proof units as one batch. The historical port counts above describe
donor support, not the current count of unfinished KS contracts.

## Pinned setup and commands

Install [elan](https://github.com/leanprover/elan) if needed. From the repository
root:

```bash
elan toolchain install leanprover/lean4:v4.30.0
cd proofs
lake exe cache get
lake build
```

The toolchain and full nine-package Mathlib lock are committed. Do not run
`lake update` or upgrade dependencies as part of filling a proof. The pinned
Mathlib commit is `c5ea00351c28e24afc9f0f84379aa41082b1188f`.

From the repository root:

```bash
./scripts/test-proofs.sh conditional
./scripts/test-proofs.sh strict
```

**Conditional** builds current sources, exports actual types/definitions/axiom
closures, checks pipeline orchestration and audit-policy regressions, and
executes the registered assembly, kernel, point, simplex, geometry and lattice
regressions. It compares the reviewed interface lock and allows registered unfinished targets,
but rejects unregistered holes, unexpected axioms and regressions in closed
results. Passing means the stated obligations compose—not that they are true.

**Strict** additionally rejects every direct or transitive `sorryAx` dependency
and requires `ci/Executable.lean` to compile the concrete pipeline. It now passes.
The scaffold's historical intentional failure is no longer the current status:
both proof gates must remain green. Do not add `continue-on-error`, reintroduce
admissions or describe a failed completeness check as success.

Proof CI runs both gates on every push and pull request, separately from the
existing CPU job. It caches only locked dependency repositories/artifacts, not
this project's `.lake/build`. Cached dependencies are not fresh proof evidence;
the current project sources are always built and their closures re-exported.
The CI elan bootstrap is fixed at v4.2.0 with its published SHA-256 digest.

## Working one obligation at a time

Agents receive one approved unit and disjoint write ownership. Preserve
definitions, statement types, pins, lattice direction and witness connections.
Replace the assigned holes with implementations/proofs; do not weaken the
headline, add assumptions, return silent dummy outputs or move holes into
helpers. Keep operating/status docs current in the same chunk.

The compiler inventory and derived progress are written under ignored
`ci-output/`. Build products, caches and frozen source/report archives stay out
of Git. Report actual conditional/strict and CPU results at the chunk check-in;
do not begin the next unit without approval.

The statement-lock generator is deliberately separate from normal gates:

```bash
cd proofs
python3 ci/proof_audit.py freeze --inventory ci-output/inventory.json
```

Use it only after an explicitly approved interface/inventory change and its
required semantic review. It must never regenerate a baseline to hide a failing
comparison. Exact normal forms and native PALP/CUDA refinements remain separate
future work.
