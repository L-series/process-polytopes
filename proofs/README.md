# Dimension-generic reflexive proof scaffold

This is a **work-in-progress proof-contract package**, not a completed
classification theorem or executable scientific classifier. It targets a
terminating KS WS/CWS construction recovering all reflexive lattice polytopes
in every fixed dimension, with lattice realizations and subpolytope search.
Nonreflexive IP classification is excluded; IP weights and possibly nonreflexive
containers remain intermediate objects.

The package currently contains extracted foundational definitions/equivalence
lemmas, eighteen major statement contracts, list-pipeline orchestration, genuine
conditional composition and dimension-zero proofs, seven data holes, and
eighteen supporting proof holes. See [STATEMENTS.md](STATEMENTS.md) for the exact
chain and [SOURCES.md](SOURCES.md) for attribution and dependency provenance.

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
compares the reviewed interface lock. It allows registered unfinished targets,
but rejects unregistered holes, unexpected axioms and regressions in closed
results. Passing means the stated obligations compose—not that they are true.

**Strict** additionally rejects every direct or transitive `sorryAx` dependency
and requires `ci/Executable.lean` to compile the concrete pipeline. It is
**intentionally failing** until the missing proofs and scientific routines are
implemented. Do not add `continue-on-error` or describe its failure as a green
proof result. This is the approved exception to the earlier all-green policy.

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
