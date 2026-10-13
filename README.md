# process-polytopes

Compute, process, and classify dimension-five weight systems and the IP and
reflexive polytopes arising from them.

## Status

- **Single weight systems:** the scientific sector is complete; its manuscript
  supplies the reference conventions. Restoring its processing tools into the
  integration branch is planned.
- **Combined weight systems:** consolidation and the verified census are in
  progress. This checkout has PALP CPU generation and exact reference regressions;
  the consolidated classifier and CUDA pipeline are planned.
- **Theory:** the reviewed sibling proof source has been frozen. Literature
  notes, the fresh proof audit, and the pinned Lean port are planned.

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

Add `--fix` to the lint command to apply formatting. CUDA and Lean environments
will be documented when their setup chunks land; neither is available here yet.

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

## Documentation

- [Agent operating manual](AGENTS.md)
- [Consolidation scope, provenance, and status](docs/PORTING_PLAN.md)
- [Learning notes and single-weight manuscript](docs/learning/README.md)
- [Experiment index](docs/experiments/README.md)
- [Benchmark contract](docs/BENCHMARKING.md)

The Lean package will live in `proofs/`; that directory and its build instructions
are planned. Execution is theory-first after the operating bootstrap, with an
approval check-in after every small chunk. Original experiment branches remain
archival and must not be deleted or force-pushed.
