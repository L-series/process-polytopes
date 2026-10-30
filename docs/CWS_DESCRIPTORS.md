# Five-dimensional CWS descriptor generation

From the repository root:

```sh
make
ulimit -s unlimited
build/cws.x -n5 -s5 > type5.cws
```

`-sID` selects exactly one of the 46 historical descriptors, numbered **2–47**.
The single-weight simplex (historical entry 1) is excluded. Both `-s5` and
`-s 5` work. Build with `POLY_Dmax >= 5`; `build/cws.x` supplies this project-specific mode. Standalone PALP does not.
There is no implicit all-types run: some complete types have trillions of
candidates.

The default pool directory is `docs/data/single-weight-systems`, relative to the
working directory. Override it with `--weights-dir DIRECTORY`, for example when
running outside the repository or using bounded experimental pools. Each pool
has one sorted primitive positive weight tuple per line, without a degree:
`2d.txt`, `3d.txt`, and `4d.txt` supply three, four, and five weights respectively.
Only needed files are loaded. The unique two-weight system `(1,1)` is builtin.
The committed files contain the complete PALP reference pools; alternate files
change the enumeration universe. Their shape is checked, not their IP property
or completeness. Missing or malformed needed pools fail nonzero before output.

Standard output retains PALP's existing IP-accepted CWS format, including degrees,
zero padding and polytope/dual annotations. IP-but-nonreflexive results are retained.
Successful standard error reports `CWS candidates: N`: assignments submitted to
PALP's geometry routines after symmetry pruning, including rejected assignments.
This is the numerator used by the benchmark script. It is not the accepted-row
count or number of distinct normal forms. Output errors fail nonzero.

## Implementation and provenance

The historical catalogue lives in `src/cws/catalogue.c` as explicit coordinate
supports, with compatibility IDs 2–47. Its constructor counts coordinate
occurrences and emits shared coordinates first, then private coordinates.
Sizes, shared counts and mappings match the frozen original descriptor table.
It expands a fixed catalogue; it does not derive the catalogue mathematically.

`PALP/cws_generate.h` supplies a dimension-independent descriptor and enumeration
API. `PALP/cws.c` consumes caller-provided descriptors and weight pools,
then sends assembled candidates to a callback before geometry. The project
adapter `src/cws/main.c` handles catalogue selection and sends those candidates
to PALP's existing `PRINT_CWS`. Weight-file conventions live in
`src/cws/weight_pool.c`. See [the source boundary](../src/README.md) for the API,
capacity limits, process isolation and planned CPU/GPU integration.

The enumeration code is adapted from PALP `c4a565f`, including the descriptor work
from `d84b4ab` and slot-order correction `9e13543`. It reuses PALP's selections,
CWS construction, point enumeration and IP/dual routines. Legacy command paths
are unchanged. No caching, sharding, point-walk optimization or GPU code is added.

Two symmetry conditions prevent known omissions:

- Slot-index ordering is allowed only when exchanging those two slots preserves
  the descriptor's complete coordinate mapping. The supplied pool must also be the same stream for both slots. The project
  adapter shares one loaded stream for every equal-sized slot. This is deliberately more conservative than grouping all
  slots in automorphism orbits.
- Shared coordinates may be ordered only when earlier rows give them identical
  coefficients **and every later row uses both or neither**. Equal earlier
  coefficients alone are insufficient.

## Evidence and limits

Run the independent bounded oracle and the existing CWS/NF regressions:

```sh
make
python3 tests/test_dim5_descriptors.py
./scripts/test-cws.sh
```

The independent rational oracle is ported unchanged from C01/C02
(`origin/integration/cws-5d-cpu:tests/cws_reference.py`). It enumerates assignments
without native symmetry pruning, identifies equation/coordinate permutations,
enumerates exact integer points, and checks IP with exact rational arithmetic.
All 46 descriptors are compared using two small input systems per nontrivial
size. Every emitted point count and the complete IP-class set must agree.
The frozen descriptor fixture retains the separately reviewed C02 table.
A larger type-12 regression requires 5,994 IP classes from 9,072 submitted
candidates, matching the archived exact-oracle result. Deliberately restoring
either historical pruning error causes the bounded tests to fail.

The retained regression checks do not prove native implementation correctness or
catalogue completeness. No Lean/CBMC harness is included in this integration.
The existing [structure audit](learning/minimal-ip-circuits-and-types.md) also
records isomorphic historical entries and gaps in the archived catalogue's
geometric interpretation. Keeping its 46 IDs does not certify 46 distinct valid
unlabeled geometric types or completeness for all 5D polytopes.

See [the first benchmark record](experiments/cws-descriptors.md) for the measured
boundary and raw repeated-run data.
