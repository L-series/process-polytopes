# Repository operating manual

## Scope and layout

The project computes dimension-five weight systems and their IP/reflexive
polytopes. The single-weight scientific sector is complete; CWS consolidation
and its verified census are in progress. The native CWS delivery covers the
implemented descriptor/input-pool universe on the PALP reference lattice, not
all lattice realizations or all five-dimensional reflexive polytopes.

The separately approved theory programme targets all reflexive polytopes in
every fixed dimension. Nonreflexive IP targets are excluded, but IP weights and
possibly nonreflexive containers remain necessary intermediates. The concrete
Lean endpoint is now kernel-closed, with all supporting contracts and executable
routines implemented. Preserve that closure; conditional CI alone is not a
proof-completeness certificate.

- `PALP/`: pinned PALP submodule and current CPU executables.
- `scripts/`: lint, commit-message checks, build detection, and CWS tests.
- `tests/`: Python standard-library tests and exact CWS/NF reference fixtures.
- `docs/learning/`: separate single-weight paper, theory roadmap, learning notes,
  and the standalone `LITERATURE_REVIEW.tex` compilation.
- `docs/experiments/`: experiment index; benchmark records are planned.
- `docs/PORTING_PLAN.md`: scope, source provenance, and execution status.
- `.codex/agents/`: permanent theory-agent definitions; see `docs/THEORY_AGENTS.md`.
- `proofs/`: pinned Lean/Mathlib proofs, executable KS routines and CI audit tools.
  Complete donor `Basic`, `ReflexiveDuality`, `WeightSystem`, `BoxReduction`,
  `Algorithm`, `WeightSystemFinite`, `GeometricCWS`, `CircuitGeometry`,
  `CircuitInterior`, `CircuitSpan`, `ReflexiveVolume`, `Finiteness`, `MaxMin`,
  `Minimal`, `Classification`, `CWSBasis`, `CWSContainer` and `CWSAssembly`
  ports are imported, together with `DegreeBound`, `Sylvester`, `PyramidLemma`
  and both donor axiom-audit modules. All applicable donor modules are ported.
  New KS modules implement bounded seeds, primitive circuits, exact kernels,
  geometry, complete weight search and compatible-lattice reconstruction.
  All seven routines, eighteen contracts and the concrete headline are
  compiler-closed. Clean build and both proof gates pass; fresh isolated donor
  readings and frozen-report reconciliation close the historical semantic debt.
- `docs/proof-audit.md`: source-to-contract map, audit dispositions, dependency
  ledger and isolated statement-review provenance.

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

With elan installed, the proof package pins Lean 4.30.0 and the full Mathlib lock:

```bash
(cd proofs && lake exe cache get && lake build)
./scripts/test-proofs.sh conditional
./scripts/test-proofs.sh strict
```

See [proofs/README.md](proofs/README.md). Strict, conditional and CPU gates now
pass locally and must remain green. Never reintroduce admissions, weaken the
reviewed statement lock, hide a strict failure or treat a conditional build alone
as proof completeness.
CUDA builds, the native consolidated classifier and benchmark runners remain
planned. GPU acceptance requires an approved real device, not silent CPU fallback.

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

Before every commit, ensure affected documentation reflects the final changes.
When the user requests revisions to work in progress, update the affected
documentation alongside those revisions rather than leaving it describing an
earlier version. Check this manual, README, and relevant learning, experiment,
and proof documentation as applicable.

Work on fresh `integration/cws-5d` branches in the root and PALP repositories.
Keep original main and production refs untouched during integration. Experiment
branches are archival: never delete, force-push, or rewrite them. Main promotion
or archival cleanup requires separate approval. Publish every new PALP commit
under a durable ref before publishing a root commit that pins it.

Execute one approved chunk at a time. Report commits, validation/CI, deviations,
and the proposed next chunk; stop until the requester approves continuing.
Theory-first execution starts with literature, then the audit and proof port.
The integrated descriptor cross-check still waits for the CPU descriptor port.

Follow [`docs/learning/THEORY_ROADMAP.md`](docs/learning/THEORY_ROADMAP.md) for the
long-term mathematical goal and source roles. `SINGLE_WS_5d.tex` is a separate
single-weight paper, not the official manuscript or specification for this work;
use it for cross-checks/reference leads and verify primary sources. Keep the
global completeness goal distinct from the approved abstract port/native scope.

Proof CI checks reviewed statement/definition locks and compiler-derived
transitive axioms. Preserve the major target inventory, explicit reconstruction
witnesses and already-closed results. Do not weaken statements, add completeness
assumptions, replace routines with dummy outputs or update the lock merely to
make CI pass. Interface/meaning changes require explicit approval and affected
fresh two-way statement checks; proof bodies can be filled in approved chunks.

Keep the Markdown literature notes and `LITERATURE_REVIEW.tex` compilation in
sync. Its LuaLaTeX build instructions are in `docs/learning/README.md`; write PDF
and auxiliary outputs outside the checkout.
Use `$...$` for inline Markdown math and `$$...$$` for displays. Raw `\[...\]`
is not recognized as math by Prettier and can have its subscripts corrupted.

During approved literature work, cache primary PDFs and available TeX sources in
`docs/learning/sources/` following its README. Check cached versions before fetching,
retain source URLs/versions/hashes, and assign disjoint cache writes to researchers.
Cached documents stay out of Git; notes retain citations and provenance references.
The blind statement checker must not read cached literature.

Use the roles and assignment/isolation protocol in
[`docs/THEORY_AGENTS.md`](docs/THEORY_AGENTS.md) for approved theory work.
Definitions do not authorize execution. Run statement checkers in fresh contexts
with separate inputs and freeze both reports before comparison. The blind checker
receives only the prepared formal packet; do not send it plan/manuscript content.
Final proof optimization follows the completed port and catalogue check, preserves
audited statements and pins, and requires a clean build and axiom guards.

Keep datasets, checkpoints, build products, and frozen source archives out of
Git. Do not modify the dirty sibling proof checkout; use the reviewed frozen
snapshot. Retain source attribution and dependency licenses, including PALP's
GPLv3; do not label PALP-linked distributions MIT-only.
