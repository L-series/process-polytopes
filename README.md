# process-polytopes

Compute, process, and classify dimension-five weight systems and the IP and
reflexive polytopes arising from them.

## Status

- **Single weight systems:** the scientific sector is complete. Its separate
  paper provides context and reference leads. Restoring its processing tools
  into the integration branch is planned.
- **Combined weight systems:** consolidation and the verified census are in
  progress. This checkout has PALP CPU generation and exact reference regressions;
  the consolidated classifier and CUDA pipeline are planned.
- **Theory:** seven literature notes and their LaTeX compilation are available.
  All applicable frozen donor modules and both axiom guards are faithfully
  ported. The [dimension-generic Lean package](proofs/README.md) now implements
  bounded minimal seeds, primitive circuit encoding, exhaustive CWS assembly,
  saturated integer kernels, exact hull/polar checks, all-container-point
  enumeration, and compatible-lattice reconstruction.
  All seven scientific routines and eighteen contracts are implemented.
  `Refpoly.KS.classifies_reflexive` is kernel-checked: for every fixed dimension,
  the concrete finite list covers every reflexive lattice polytope up to
  origin-fixed, two-sided integral equivalence. Duplicates are permitted.
  Local strict and conditional gates pass, with all 36 designated targets closed
  and no `sorryAx` or custom mathematical axioms. A fresh isolated reading closes
  the donor blind-review debt for all 71 source statements; see the
  [audit ledger](docs/proof-audit.md) for evidence and precise scope.

The [long-term mathematical goal](docs/learning/THEORY_ROADMAP.md) is complete
KS WS/CWS-based enumeration of **all reflexive polytopes** in every fixed
dimension with lattice realizations and subpolytope recovery accounted for.
Nonreflexive IP targets are excluded from that theory programme; IP weights and
containers remain intermediates. The existing single-weight paper is a
separate work, not the official manuscript for that development. The Lean theorem supplies the mathematical coverage endpoint, not a verified
native PALP/CUDA implementation or completed census.

The CWS delivery covers the implemented descriptor/input-pool universe on the
PALP reference lattice, retaining both IP-but-nonreflexive and reflexive classes.
It does not claim all lattice realizations or all five-dimensional reflexive
polytopes.

## Getting Started

### Initializing Git Submodules

After checking out the desired branch, initialize the pinned PALP submodule:

```bash
git submodule update --init --recursive
```

Alternatively, you can clone the repository with submodules in one step:

```bash
git clone --recurse-submodules <repository-url>
```

### Commit message hook

Enable the native Git hook with `git config core.hooksPath .githooks`.

### Reproducible build environments

Install [Nix](https://nix.dev/) with flakes enabled. The flake and lockfile pin the
current CPU development environment for `x86_64-linux`.

```bash
nix develop -i -c ./scripts/lint.sh
nix develop -i -c make -C PALP all-dims
```

Add `--fix` to the lint command to apply formatting. CUDA setup remains planned.
The Lean environment is separately pinned under `proofs/` and does not need Nix.

Nix is optional locally. With GCC, GNU Make, and Python 3 installed, the CPU
commands are:

```bash
make -C PALP all-dims
./scripts/test-cws.sh
```

For native linting, install `clang-format`, Prettier, and `shfmt` matching the
pinned environment, then run `./scripts/lint.sh`. CI retains the Nix commands.

### CWS regression tests

Run `nix develop -i -c ./scripts/test-cws.sh` to build the four required CPU executables and check CWS counts, combination types, malformed input, and vertex normal forms. Add `--case 4d` to run one generation case. See [tests/README.md](tests/README.md) for fixture provenance and coverage. CI runs these tests on every push and pull request; the full `all-dims` build retains its source-change gate.

These tests preserve exact CWS rows and vertex normal forms of P. They do not
independently certify mathematical completeness, and accepted CWS counts are not
distinct-polytope counts.

### Proof CI

With [elan](https://github.com/leanprover/elan) installed:

```bash
(cd proofs && lake exe cache get && lake build)
./scripts/test-proofs.sh conditional
./scripts/test-proofs.sh strict
```

The **conditional** gate checks that the precise major statements compose and
that reviewed interfaces/closed results do not regress. The **strict** gate
rejects all unproved dependencies and requires executable compilation. Both gates
now pass locally and must remain green for the completed proof programme.
A green conditional check alone is not proof completeness. Both jobs run on each push/PR; the
existing CPU checks remain independent. See the [statement chain](proofs/STATEMENTS.md).

## Documentation

- [Agent operating manual](AGENTS.md)
- [Permanent theory agents and assignment protocol](docs/THEORY_AGENTS.md)
- [Consolidation scope, provenance, and status](docs/PORTING_PLAN.md)
- [Learning notes and separate single-weight paper](docs/learning/README.md)
- [Consolidated literature review with basic definitions](docs/learning/LITERATURE_REVIEW.tex)
- [Reusable local literature sources](docs/learning/sources/README.md)
- [Experiment index](docs/experiments/README.md)
- [Benchmark contract](docs/BENCHMARKING.md)
- [Lean proof package and gate commands](proofs/README.md)
- [Proof audit, contract map and dependency ledger](docs/proof-audit.md)
- [Major formal contracts and classification theorem](proofs/STATEMENTS.md)
- [Proof source provenance](proofs/SOURCES.md)

The Lean package is in `proofs/`. Execution remains theory-first. The requester
authorized the remaining proof-completion units as one batch; other chunks retain
their approval check-ins. Original experiment branches remain
archival and must not be deleted or force-pushed.
