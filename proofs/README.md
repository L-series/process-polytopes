# Reflexive lattice-polytope classification

This Lean package implements and proves a terminating Kreuzer-Skarke
weight-system/combined-weight-system construction for every fixed dimension.
Its main theorem is:

```lean
Refpoly.KS.classifies_reflexive : Classifies procedures
```

For every natural dimension $d$, the concrete procedure returns a finite list
of reflexive lattice polytopes covering every such polytope up to origin-fixed
$\mathrm{GL}(d,\mathbb Z)$ equivalence. Dimension zero is handled directly.
Duplicates are permitted.

These are exact, dimension-generic reference algorithms, **not verified ports
of PALP**. The theorem does not certify efficient high-dimensional execution,
normal forms, a duplicate-free catalogue, CUDA, or a production census.
Nonreflexive IP polytopes are excluded as final targets; IP weights and
possibly nonreflexive containers remain necessary intermediates.

## Build and check

Install [elan](https://github.com/leanprover/elan), then from the repository root:

```bash
cd proofs
lake exe cache get
lake build
cd ..
./scripts/test-proofs.sh strict
```

`lean-toolchain` pins Lean 4.30.0; `lake-manifest.json` pins Mathlib and its
dependencies. No Nix environment is required. Do not upgrade these dependencies
implicitly or regenerate reviewed interfaces to hide a regression.

Strict mode builds current sources, checks the statement/definition lock and
compiler-derived axiom closures, rejects every direct or transitive `sorryAx`,
and compiles the concrete pipeline. It also runs audit-policy regressions and
nine executable suites covering weights, assembly, kernels, congruence
lattices, points, simplexes, geometry, realizations and the endpoint.

`./scripts/test-proofs.sh conditional` runs the interface/composition gate
without the strict no-admissions requirement for registered targets. It is
useful diagnostically but is not a proof-completeness certificate. Both modes
run in CI; only pinned dependencies are cached, not this project's build.
The proved package uses only `propext`, `Classical.choice` and `Quot.sound`.
Finite execution examples supplement rather than establish generic completeness.

## Read the construction

The executable path is:

```text
primitive IP weights → bounded CWS assembly → saturated integer kernels
  → all container lattice points → hulls of every point subset
  → eligible candidates → compatible lattice realizations
  → exact reflexivity filter
```

The completeness proof starts with an arbitrary reflexive target and finds a
minimal dual seed with $d+1\le N\le2d$. Its circuit relations identify a CWS
included in the search. Container, subpolytope and image-lattice witnesses then
show that an equivalent target occurs in the output. This seed is a
proof-side witness, not an additional input to the enumeration algorithm.

| File or directory                | Purpose                                                     |
| -------------------------------- | ----------------------------------------------------------- |
| [STATEMENTS.md](STATEMENTS.md)   | Conventions, eighteen contracts and seven algorithms        |
| `Refpoly/KS/Model.lean`          | Integer codes, seeds, lattices and semantic predicates      |
| `Refpoly/KS/Contracts.lean`      | Supporting theorem interfaces                               |
| `Refpoly/KS/Pipeline.lean`       | Finite-list orchestration and `Classifies`                  |
| `Refpoly/KS/Composition.lean`    | Classification from the supporting contracts                |
| `Refpoly/KS/Implementation.lean` | Concrete computational routines                             |
| `Refpoly/KS/Obligations.lean`    | Proved contracts and the concrete headline theorem          |
| `Refpoly/*.lean`                 | Supporting geometry, duality, finiteness and circuit theory |
| `ci/`                            | Compiler audit, reviewed locks and executable examples      |

See [proof guarantees](../docs/proof-audit.md) for the trust boundary and
supporting-result restrictions, [SOURCES.md](SOURCES.md) for attribution and
dependency identities, and the [literature notes](../docs/learning/README.md)
for primary references. Build products, compiler inventories and downloaded
source documents are not tracked.
