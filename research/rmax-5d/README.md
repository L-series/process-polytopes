# Five-dimensional r-maximal polytope research

This bundle collects the mathematical arguments, reference implementation,
formalized coverage lemmas, historical 4D validation, preliminary workload
calculations, and actual 5D WS profiling from the research discussion.
Start with the [research note](../../docs/R_MAXIMAL_SEARCH.md).

**Result:** a finite, lattice-complete maximal-only reference algorithm under
the stated exact-geometry and input-coverage contracts. The 4D experiment
reproduces the historical 340 maximal polytopes. **A practical, complete 5D
classification has not been achieved.** The unrestricted HNF loop is
infeasible, and many bounded recursive and maximality tests remain unresolved.

## Contents

| Path                                                                       | Purpose                                                                                                               |
| -------------------------------------------------------------------------- | --------------------------------------------------------------------------------------------------------------------- |
| [Research note](../../docs/R_MAXIMAL_SEARCH.md)                            | Proofs, limitations, literature, workload models, profiling, independent WS/CWS searches, and minimal/maximal pruning |
| [Reference search](../../scripts/rmax_search.py)                           | Exact small-instance algorithm; an index cap is explicitly incomplete                                                 |
| [4D validation driver](../../scripts/validate_rmax4d.py)                   | Reconstructs and checks the historical 340 outputs                                                                    |
| [Lean lemmas](../../proofs/Refpoly/MaximalSearch.lean)                     | Fixed-lattice order/coverage results, not verified native geometry                                                    |
| [Focused regressions](../../tests/test_rmax_search.py)                     | Small-instance, lattice-change and parser checks                                                                      |
| [Portable profiling report](../../docs/data/ws-rmax-profile-20261104.json) | Measured costs, selected input weights and censored results                                                           |
| [Final 4D artifacts](artifacts/rmax4d-validation/)                         | Authoritative 340 normal forms, capped-index provenance and PALP outputs                                              |
| [Exploratory 4D artifacts](artifacts/exploratory-4d/)                      | Earlier intermediate runs; superseded by the final validation                                                         |
| [Profiling source and raw results](ws-profile/)                            | All retained profiling scripts, native helpers, measurements, failures and build logs                                 |
| [Workload calculator](workload_estimates.py)                               | Recomputes the initial assumptions and later measured scenarios                                                       |
| [Explicit examples](examples.json)                                         | The 496/491-point obstruction and the 243/270-point minimal-pruning counterexample                                    |
| [Supplied WS statistics](user-ws-statistics.json)                          | The partial vertex tables and distribution information supplied before profiling                                      |
| [Original artifact manifest](original-artifact-manifest.json)              | Sizes and SHA-256 hashes of retained session-generated artifacts                                                      |

The stored WS sample contains representative weights, so the scientific
geometry probes can be replayed **without downloading the 264 GB catalogues**.
The actual catalogues and downloaded literature PDFs are not generated
deliverables and are not bundled. Temporary point dumps and bytecode were
cleaned up; point dumps can be regenerated from the stored sample.

The raw scripts are retained as executed, including their original local
paths. The replay driver adapts those paths in a separate output directory;
it does not modify the archived sources or measurements. Hashes recorded
inside historical provenance refer to the source revision used by that run,
which can precede later additions to the profiling harness.
Archived files are excluded from automatic formatting and line-ending
conversion so their original byte hashes remain checkable.

## Reproduce the reference results

Initialize PALP and build its dimension-five tools:

```bash
git submodule update --init --recursive
(cd PALP && make poly-5d.x class-5d.x poly-4d.x class-4d.x)
python3 -m unittest discover -s tests -p test_rmax_search.py -v
python3 research/rmax-5d/check_examples.py
python3 research/rmax-5d/workload_estimates.py
(cd proofs && lake build Refpoly.MaximalSearch)
```

The 4D driver requires a **new** artifact directory:

```bash
python3 scripts/validate_rmax4d.py --work-dir /tmp/rmax4d-replay
```

The authoritative output is
[`maximal-340.jsonl`](artifacts/rmax4d-validation/maximal-340.jsonl), SHA-256
`b16528fe9471388b26b3877a193b7f16fb13dde46be176e4fa0856756efcbb1c`.
The discovery pass uses indices at most three plus complete unchanged-shape
lattice intervals. It reproduces the historical answer; it is not an
independent exhaustive execution of the unrestricted completeness algorithm.

## Replay bounded WS profiling

The default replay requires Python with NumPy, a C compiler, and the pinned
PALP submodule. The lattice-tree stage additionally needs SymPy. PyArrow is
required only to rerun the original catalogue sampling scripts.

```bash
python3 research/rmax-5d/replay_profile.py \
  --work-dir /tmp/ws-rmax-replay \
  --stages native,lattices
```

This builds the helpers from source, reconstructs all 538 stored probes, and
tests their 81,776 index-two/index-three lattice states. No catalogue access
is needed. Do not interpret that bounded pass as complete lattice coverage.

For all bounded stages, including recursive searches and maximality:

```bash
python3 research/rmax-5d/replay_profile.py \
  --work-dir /tmp/ws-rmax-full-replay \
  --stages all
```

Stages run in prerequisite order and each original phase has its own
timeouts/node caps. Expensive cases remain censored, not classified as
negative. Wall times and cap-limited node counts vary across machines.
The raw `summary.json`, `concise.json`, and
[`portable report`](../../docs/data/ws-rmax-profile-20261104.json) are the
historical measurements; replay creates new phase outputs separately and
does not overwrite them.

The included ELF executables and shared library are historical Linux builds,
not portable runtime dependencies. Prefer rebuilding with the replay driver.
The wider PALP snapshot changes the 5D vertex capacity to 128; it is a
profiling experiment, not a sufficient bound for all 5D geometry.

## Important completeness distinctions

- Input coverage is forward-integral; proper image sublattices cannot be ignored.
- Nonreflexive IP containers can contain r-maximal reflexive descendants.
- Stopping at a reflexive node is valid for a maximal cover, not for enumerating its r-minimal descendants.
- WS and CWS can be processed independently and consolidated, provided each preserves its lattice obligations.
- Being inside a known maximum does not discharge a minimal-first search.
- A stable candidate count, low index cap or failed heuristic containment test is not a completion certificate.
- The native geometry, catalogue deduplication and complete 5D execution are not Lean-verified.

PALP and PALP-linked executables retain GPLv3; see
[`COPYING.palp`](ws-profile/COPYING.palp), [`GPL-3.txt`](ws-profile/GPL-3.txt), and the pinned
[PALP source](../../PALP/). The wider build's corresponding source is included
in `ws-profile/backend128/`. The proof package retains its existing MIT
license; dependency licenses are unchanged.
