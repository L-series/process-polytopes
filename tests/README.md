# CWS regression tests

Run from the repository root:

```bash
nix develop -i -c ./scripts/test-cws.sh
nix develop -i -c ./scripts/test-cws.sh --case 4d
```

The runner builds `cws-4d.x`, `poly-4d.x`, `cws-5d.x`, and `poly-5d.x`. Python's standard-library `unittest` runs the checks; no additional test framework is required. `PALP_SOURCE_DIR=/path/to/clean/palp ./scripts/test-cws.sh` can test a separate build. The runner raises its stack limit for PALP's dimension-5 routines and disables core dumps for malformed-input tests.

## Fixtures and assertions

`fixtures/cws/manifest.json` records commands, the reference PALP commit, accepted row counts, combination-type histograms, and distinct normal-form counts. Each case has two checked-in files:

- `.cws`: every accepted row, including IP/reflexivity and lattice-point/vertex annotations, with whitespace normalized.
- `.nf`: the vertex normal form of **P**, computed by `poly -fN`, in the same order as the CWS rows. Each block starts with `dimension vertex_count` and has `dimension` integer matrix rows. Only whitespace and the informational `perm=` header field are omitted.

Every case checks the exact generated rows, their total count, the combination-type histogram, and every normal-form matrix. It also checks that each CWS has the requested dimension, valid degrees and weights, and one normal form. Repeated normal forms are retained: accepted CWS counts and distinct polytopes are different quantities. These are golden regressions against PALP, not independent certificates of mathematical correctness or classification completeness.

## Low-dimensional coverage

Bare `cws -c2` currently emits nothing. The 2D test explicitly combines two copies of `2 1 1`, producing the square with `M:9 4 N:5 4`. Dimensions 3 and 4 use the complete built-in `-c3` and `-c4` generators.

Here component dimension means the dimension of the simplex WS, not its number of weights.

| Dimension | Combination                                     | Accepted rows |
| --------- | ----------------------------------------------- | ------------: |
| 2         | Product of two 1D WS                            |             1 |
| 3         | Two 2D WS sharing one coordinate                |            17 |
| 3         | Product of 1D and 2D WS                         |             3 |
| 3         | Product of three 1D WS                          |             1 |
| 4         | Two 3D WS sharing two coordinates               |        16,040 |
| 4         | 2D and 3D WS sharing one coordinate             |         1,122 |
| 4         | Product of 1D and 3D WS                         |            95 |
| 4         | Product of two 2D WS                            |             6 |
| 4         | Three 2D WS sharing one common coordinate       |            36 |
| 4         | Two 2D WS sharing one coordinate, times a 1D WS |            17 |
| 4         | Product of 2D, 1D, and 1D WS                    |             3 |
| 4         | Product of four 1D WS                           |             1 |

Totals are 1, 21, and 17,320 CWS; their distinct vertex-normal-form counts are 1, 21, and 10,780.

## Supported 5D types

The bounded input pools exercise every implemented two-file and three-file overlap pattern. These exhaust their checked-in pools, not the full 5D corpus.

| Input files | `-t`    |                  Accepted examples |
| ----------: | ------- | ---------------------------------: |
|           2 | `0 0`   |                              1,000 |
|           2 | `1 1`   |                              1,539 |
|           2 | `2 2`   |                              1,080 |
|           3 | `0 0 0` |               101 across two cases |
|           3 | `1 1 0` |                              1,122 |
|           3 | `2 2 0` | 1,099, plus a four-row bug fixture |
|           3 | `1 1 1` |                              1,505 |
|           3 | `2 1 1` |                              1,401 |
|           3 | `2 2 1` |                              1,468 |
|           3 | `2 2 2` |                              1,622 |

For `0 0 0`, the component-dimension partitions of five are `(1,1,3)` and `(1,2,2)`. Exhausting the lower-dimensional IP WS produces only 95 + 6 examples, so 1,000 distinct input combinations are unavailable for that pattern.

The 1D and 2D pools contain the complete small IP WS lists. `ws-3d.txt` is the 95-row output of `cws -w3`, with annotations removed; the `firstN` files are prefixes in PALP's output order. `ws-4d-first1000.txt` is the first 1,000 WS from `cws -w4 5 40`; its smaller prefix is used for the `2 2` case. `ws-3d-swapped.txt` isolates the swapped selections of `5 1 1 1 2`.

Recording these fixtures exposed an offset bug in `Make_nno_CWS`: the swapped `2 2 0` branch omitted subtraction of the overlap size when positioning the third WS. That raised the resulting polytope dimension from five to seven and corrupted memory. The pinned PALP fix restores the third-row offset; the four-row swapped fixture and the larger `2 2 0` case cover this branch. The 2D–4D outputs remain identical to the preceding PALP revision.

## Combination signatures

The manifest's `rWS:c1,c2,...` keys encode column incidence. Bit `i` of a mask means equation `i` uses that coordinate; `cm` counts coordinates whose incidence mask is `m`, for masks `1` through `2^r-1`. The lexicographically smallest vector over equation permutations is used. This distinguishes component sizes and overlap topology while ignoring equation and coordinate order. It is more specific than counting equations alone.

## Malformed input

Five malformed fixtures cover missing weights, inconsistent degrees in either row, unequal row widths, and a negative weight. Both `poly -fN` and `cws -i -f` must reject them without producing a normal form or accepted CWS row. PALP currently reports several format errors with exit status zero; those tests require the rejection diagnostic. The negative-weight case currently aborts through an assertion and must exit nonzero. These tests do not claim that every malformed string is rejected gracefully.

## Reference and provenance gates

`./scripts/test-cws-pipeline.sh --suite <name>` runs gates that do not trust
PALP's own generation or point code. Only `generator` and `points` have landed;
`schema`, `classification`, and `merge` are planned, and any unlanded suite exits
with an error instead of passing. `--case NAME` limits the generator suite to one
legacy or descriptor case (for example `s12`), and `--jobs N` sets reference
parallelism.

`cws_reference.py` is an independent, standard-library oracle:

- Each manifest case lists explicit `reference.structures`: input pools and
  one-based coordinate blocks, in the same shape as PALP's dimension-5
  descriptors. The gate checks that their incidence signatures equal the case's
  recorded combination types. The 3D and 4D cases use pool files equal to PALP's
  built-in lists.
- Every assignment of pool entries to the blocks is enumerated without PALP's
  selection order or symmetry pruning. Only weights on shared coordinates are
  placed individually, since permuting private coordinates is a coordinate
  permutation. Combined weight systems are identified up to equation and
  coordinate permutations.
- Lattice points are all nonnegative integer solutions of $W x = d$. The point
  $(1,\ldots,1)$ is interior exactly when the points span the expected
  dimension and the vectors $s - 1$ have a strictly positive linear dependency,
  decided by an exact integer phase-I simplex method.
- A stable candidate ID is the structure index, one-based pool line numbers, and
  the values placed on shared coordinates. Each class keeps its minimal ID.
- Accounting requires classes to equal IP plus non-IP plus failures. A point
  budget or other undecided candidate is a failure, and failures never pass.

The `points` suite reproduces the complete 1D, 2D, and 3D IP weight lists (1, 3,
and 95 systems, every normalized system up to degree 2, 6, and 66), checks point
enumeration against brute force on random small CWS, checks budget failures, and
requires each golden `M:` point count to equal the reference count.

The `generator` suite verifies the recorded SHA-256 of every input pool and
golden `.cws`/`.nf` file, then runs live PALP for each case. Its accepted rows must
lie in the reference universe, none may be non-IP or undecided, the IP classes
missing from PALP must equal the recorded known defects, and the number of
repeated rows must equal `reference.palp_repeated_rows`. Self-tests confirm that
removing, duplicating, or changing a row is detected. The recorded summary
(`candidates`, `classes`, `ip_classes`, `non_ip_classes`, `failures`, and a
digest of every class, label, point count, and representative ID) detects oracle
drift.

## Dimension-five descriptors (V1)

PALP's `dim5_structures.inc` defines descriptor IDs 2-47 (ID 1, the single
six-coordinate simplex, is not a combination). The manifest's `descriptors` is a
reviewed copy; the gate requires PALP's include to equal it, the IDs to be exactly
2-47, every polytope dimension to be five, and each block's shared prefix to be
exactly its coordinates used by another block.

Each descriptor is compared with the oracle on bounded pools
(`descriptor_pools.explicit`: `ws-1d`, `ws-2d`, `ws-3d-first5`, and
`ws-4d-first6` by block size) through `cws -c5 -n# wf_1 ... wf_# -s#` in two
modes: one shared file per size, which exercises same-source pruning, and
byte-identical per-slot copies. Fourteen descriptors without five-weight blocks
also run builtin `cws -c5 -s#` against the complete 1D, 2D, and 3D lists. In every
run PALP must emit exactly the oracle's IP classes; only the recorded number of
repeated, permutation-equivalent rows may vary. Builtin five-weight blocks use
`cws -w4`'s 184,026 systems, which take about ten minutes to generate and are not
run here.

The 46 descriptors have 39 distinct incidence structures: 12 and 13; 16, 17, and
18; 25, 26, and 27; and 30, 31, and 32 differ only by relabeling coordinates. The
gate requires isomorphic descriptors to give identical IP class sets, so their
production runs generate the same CWS classes.

### Defects found and fixed

The reference found these defects. PALP `c4a565f` and `31cbe55` fix them, and the
known-defect list is now empty:

- The experimental descriptor generator's shared-prefix pruning sorted weights
  on two shared coordinates whenever earlier blocks agreed on them, even if a
  later block used only one. On the bounded pools it lost IP classes for
  descriptors 12, 13, 14, 20, 25-28, 40, 42, 43, and 45 (for example 4,859 of
  5,994 for descriptor 12). Historical runs of those descriptors are therefore
  not complete.
- Legacy `-t 2 1 1` compared only shared weight values to detect symmetry, and
  `-t 2 2 2` emitted three of four relative pair orientations. They missed 35
  and 128 IP classes on the regression pools; the recovered classes add 35 and
  124 distinct normal forms.
- Legacy `-t 2 1 1` and `-t 2 2 1` derived their same-file pruning flag from the
  wrong two input files. Depending on file order this pruned distinct inputs or
  emitted hundreds of repeated rows.

The fixed modes' goldens were rerecorded after this review: every earlier CWS
class is still emitted, and only `5d-3ws-211`, `5d-3ws-221`, and `5d-3ws-222`
changed. Several modes still emit repeated permutation-equivalent rows, recorded
exactly, so accepted-row counts overstate distinct CWS. The oracle is bounded to
these inputs and is not a completeness proof.

After an intentional change to inputs, goldens, or the oracle, review it and run:

```bash
python3 tests/record_cws_reference.py --accept-reference
```

The recorder runs PALP from committed sources and refuses any missing,
unexpected, non-IP, or undecided descriptor class. It writes only hashes,
reference summaries, repeated-row counts, and the legacy known-defect list; it
never edits golden rows, normal forms, or the descriptor table.

## Updating the reference

Regressions never regenerate their own expected results. To deliberately replace the baseline after reviewing a PALP change:

```bash
nix develop -i -c ./scripts/test-cws.sh
nix develop -i -c python3 tests/record_cws_baseline.py --accept-current-palp
```

The first command builds the binaries and compares the old baseline; after a changed result, review the failure before running the second command. The recorder requires committed PALP sources and updates the manifest and both fixture files for every case. Review their differences and rerun the tests before committing a new baseline.
