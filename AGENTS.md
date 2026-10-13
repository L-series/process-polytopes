# Repository operating manual

## Scope and layout

The project computes dimension-five weight systems and their IP/reflexive
polytopes. The single-weight scientific sector is complete; CWS consolidation
and its verified census are in progress. The current delivery covers the
implemented descriptor/input-pool universe on the PALP reference lattice, not
all lattice realizations or all five-dimensional reflexive polytopes.

- `PALP/`: pinned PALP submodule and current CPU executables.
- `scripts/`: lint, commit-message checks, build detection, and CWS tests.
- `tests/`: Python standard-library tests and exact CWS/NF reference fixtures.
- `docs/learning/`: single-weight manuscript and planned literature notes.
- `docs/experiments/`: experiment index; benchmark records are planned.
- `docs/PORTING_PLAN.md`: scope, source provenance, and execution status.
- `proofs/`: planned Lean package; it does not exist yet.

## Existing build and test commands

Initialize PALP with `git submodule update --init --recursive`. From the root:

```bash
nix develop -i -c ./scripts/lint.sh
nix develop -i -c make -C PALP all-dims
nix develop -i -c ./scripts/test-cws.sh
nix develop -i -c ./scripts/test-cws.sh --case 4d
```

The pinned flake currently targets `x86_64-linux`. Enable the commit hook with
`git config core.hooksPath .githooks`.

Nix is optional for local CPU validation. With GCC, GNU Make, and Python 3
available, run:

```bash
make -C PALP all-dims
./scripts/test-cws.sh
```

Native linting uses `./scripts/lint.sh` with `clang-format`, Prettier, and `shfmt`
on PATH; match the versions in the pinned environment. Record local tool versions
when using host tools. CI continues to use Nix.

Tests compare exact accepted CWS rows and vertex normal forms of P, retaining
multiplicities. They are PALP consistency regressions, not independent
completeness certificates. Never regenerate goldens to make a test pass; see
`tests/README.md` for the explicit baseline-review procedure.

CUDA builds, the consolidated classification pipeline, benchmark runners, and
`lake build` are planned, not available commands in this checkout. GPU acceptance
will require an approved real device, not silent CPU fallback. Document the
pinned Lean build command when the proof scaffold lands.

## Benchmark and validation policy

Follow `docs/BENCHMARKING.md`: use immutable inputs/builds and matching work
boundaries. Complete timings include GPU synchronization, CPU recovery, and
required output. Distinguish IP acceptance, reflexivity, accepted CWS counts, and
distinct normal forms.

Do not invent speedups or proof status. Missing measurements remain missing until
reproduced. Exact NF equality, not equal hashes, is required for class merging.
The manifest-backed benchmark runner and repeated-run protocol are planned.

## Git and execution policy

Use Conventional Commits as checked by `scripts/check-commit-message.sh`.
Separate refactors, behavior changes, CPU changes, and GPU changes. Keep tests and
relevant README/manual updates with the change. Do not add AI coauthor trailers.

Work on fresh `integration/cws-5d` branches in the root and PALP repositories.
Keep original main and production refs untouched during integration. Experiment
branches are archival: never delete, force-push, or rewrite them. Main promotion
or archival cleanup requires separate approval. Publish every new PALP commit
under a durable ref before publishing a root commit that pins it.

Execute one approved chunk at a time. Report commits, validation/CI, deviations,
and the proposed next chunk; stop until the requester approves continuing.
Theory-first execution starts with literature, then the audit and proof port.
The integrated descriptor cross-check still waits for the CPU descriptor port.

Keep datasets, checkpoints, build products, and frozen source archives out of
Git. Do not modify the dirty sibling proof checkout; use the reviewed frozen
snapshot. Retain source attribution and dependency licenses, including PALP's
GPLv3; do not label PALP-linked distributions MIT-only.
