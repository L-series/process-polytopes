# process-polytopes

Compute dimension-five weight systems and their IP/reflexive polytopes, with
exact PALP regressions and a dimension-generic Lean reference classifier.

The [proof package](proofs/README.md) implements and proves a terminating
Kreuzer-Skarke WS/CWS construction covering every reflexive lattice polytope in
each fixed dimension, up to origin-fixed $\mathrm{GL}(d,\mathbb Z)$ equivalence.
Its outputs may contain duplicates. These are independent Lean reference
algorithms, not a formal verification of PALP or CUDA, a normal-form
deduplication theorem, or a completed dimension-five census.

The native CPU commands operate on the supplied descriptor/input-pool universe
and PALP reference lattice. Their accepted IP rows and distinct normal forms
are separate quantities; native regression coverage is not the generic
classification theorem.

## CPU build and tests

Initialize the pinned PALP submodule:

```bash
git submodule update --init --recursive
```

With GCC, GNU Make and Python 3:

```bash
make -C PALP all-dims
./scripts/test-cws.sh
```

The pinned Nix environment supports `x86_64-linux`:

```bash
nix develop -i -c ./scripts/lint.sh
nix develop -i -c make -C PALP all-dims
nix develop -i -c ./scripts/test-cws.sh
```

Add `--case 4d` to the test command for one generation case. Tests compare exact
accepted CWS rows and vertex normal forms of the primal polytope, retaining
multiplicities. See [tests/README.md](tests/README.md) for fixture provenance and
the baseline-review policy.

## Lean proofs

With [elan](https://github.com/leanprover/elan) installed:

```bash
(cd proofs && lake exe cache get && lake build)
./scripts/test-proofs.sh strict
```

Lean and all Mathlib dependencies are pinned separately from Nix. Proof CI runs
strict and conditional checks on every push and pull request. Strict mode
rejects all admitted dependencies and requires executable compilation;
conditional mode alone is not a completeness certificate.

Start with the [proof README](proofs/README.md), then the
[statement and algorithm guide](proofs/STATEMENTS.md). Mathematical background
is in the [literature notes](docs/learning/README.md) and their
[LaTeX compilation](docs/learning/LITERATURE_REVIEW.tex). The separate
[single-weight paper](docs/learning/SINGLE_WS_5d.tex) concerns a different
scientific deliverable.

## Development and provenance

Use Conventional Commits; enable the message hook with
`git config core.hooksPath .githooks`. Native linting requires `clang-format`,
Prettier and `shfmt` matching [flake.nix](flake.nix).

- [Proof guarantees and scope](docs/proof-audit.md)
- [Proof sources and licenses](proofs/SOURCES.md)
- [Benchmark methodology](docs/BENCHMARKING.md)
- [Experiment evidence](docs/experiments/README.md)

The proof package uses MIT; Mathlib uses Apache-2.0. PALP retains GPLv3.
PALP-linked distributions must retain the applicable licenses.
