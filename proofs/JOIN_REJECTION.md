# Join rejection: proof and implementation status

The dimension-independent geometric soundness argument and an executable
integer verdict model are kernel checked. **A complete formal refinement proof
of the hand-written CUDA implementation is not supplied here.** The source is
checked by differential tests and compilation; those checks are not a substitute
for that missing proof.

## Foundations

The proofs use the production `CWS`, `CWS.subspace` and `CWS.IsIP`
definitions; they do not introduce a replacement IP predicate. The axiom
audit of the new theorems excludes the implementation-obligation assumptions
elsewhere in the project.

## What is proved

`Refpoly.JoinRejection.cws_join_preserves_ip_all_dimensions` states:

* for any natural dimension `d`, row count `r`, and valid `C : CWS r (d+r)`;
* if `C.IsIP`;
* for any complete finite enumeration of its nonnegative integer solutions;
* for any finite list of integer coefficient vectors;
* the capped rejection model returns `Verdict.keep`, for every cap and coverage
  flag.

`cws_join_rejection_sound_all_dimensions` gives the contrapositive: a rejection
from that model implies `¬ C.IsIP`. `cws_exact_join_inconclusive` gives the
uncapped version used by the general join. `keep` always means inconclusive,
never IP acceptance. A kept non-IP candidate is allowed.

The enumeration completeness premise is explicit (`Function.Surjective p`).
The theorem does not assume that a CUDA-produced enumeration is complete.
Separate row/join theorems establish exactness for the mathematical algorithms;
relating the CUDA traversal to those algorithms is part of the outstanding
source refinement obligation.

| Module | Kernel-checked result |
| --- | --- |
| `JoinRejection` | A one-sided nonconstant functional excludes origin interior; IP balances the two strict sides; fewer than `d+1` points cannot be IP. |
| `JoinModel` | Executable verdict, fail-open capped/unsupported cases, sound rejection for an exact finite family; literal four-bit low/high category formulas. |
| `JoinEnumeration` | Natural joins and a recursive depth-first traversal enumerate exactly the simultaneous row extensions, for any supports and row order. |
| `JoinBlocks` | Disjoint-key Cartesian blocks have count `sum_s product_r count(r,s)`; occupancy is an OR across blocks; same-factor and distinct-factor pair rules. |
| `JoinRows` | Positive integer row enumeration is sound and complete; final-coordinate divisibility optimization is exact. |
| `JoinCWS` | Kernel dimension is `n-r`; `X -> X-1` covers exactly the production lattice generators; integer coefficient evaluation agrees with a real linear map; main all-dimensions theorems. |
| `JoinSummary` | Count/OR accumulation across blocks equals the flattened point summary; uncapped verdict preservation; guarded arithmetic; injectivity of four-position 16-bit natural-number packing. |

The functional argument works in the entire joint kernel. If a functional is
zero there, both strict-side bits are absent and XOR is zero. If it is not
constant and all generators lie on one closed side with a strict witness, its
image would put an open neighbourhood of zero inside a closed half-line, a
contradiction. This covers coordinates, pairs, and arbitrary integer linear
combinations without enumerating CWS types.

In `X` coordinates the tests are exactly:

* coordinate: some `X_i=0`, and some `X_i>=2`;
* pair: some `X_i+X_j<=1`, and some `X_i+X_j>=3`.

A mismatch rejects. Both absent and both present remain inconclusive.
For private coordinates in the same row, the joint predicate must come from
that row's stored pair bits. Private coordinates from distinct rows combine
independently only after all shared values are fixed and both factors are
nonempty. These nonemptiness conditions are explicit in the proofs.

## Source correspondence and remaining obligations

The implementation is `src/gpu/join.cuh` (general join) and
`src/gpu/join_type3.cuh` (optional fused structure-3 join). It is dimension five
with fixed array bounds. The proof is dimension independent; it does not claim
that the current CUDA arrays support arbitrary dimensions.

| Source component | Mathematical correspondence | Status |
| --- | --- | --- |
| `build_gjoin_table` recursive row enumeration | `rowSolutions_exact`, `last_coordinate_exact` | Mathematical algorithm proved; C++ loops, grouping, storage, and uploads not formally refined. |
| Packed key and prefix weight matching | `pack16_injective`, `equal_weight_permutation` | Arithmetic/symmetry lemmas proved; machine representation and matching loops still require refinement. |
| `build_gjoin_plan`, `device_gjoin_decide` DFS, lookup and position indexes | `dfsRows_exact`, `joinRows_equations` | Abstract DFS proved; explicit stack, binary searches, stale-value invariant, and index tables not formally refined. |
| Fibre products and count accumulation | `blocks_count`, `accumulateBlocks_exact`, checked arithmetic | Abstract operations proved; full machine-state simulation outstanding. |
| `device_join_lo`, `device_join_hi` | `cudaLo_correct`, `cudaHi_correct` | Literal formulas proved by kernel reduction, with nonempty-factor hypotheses for high. Their extraction into the Lean definitions is reviewed/manual. |
| `tz ^ t2`, same-row pair substitution, `lo ^ hi` | Functional balance and block occupancy | Verdict model proved; concrete mask construction and indexing not formally refined. |
| Capped fused structure-3 join | `decideJoin` | Overflow keeps the candidate; the CUDA loops are not formally refined. |
| Join filter routing/compaction (`pipeline.cuh`) | `decideExactJoin` for the decision | Buffer ownership, compaction, deferred delivery, census mode and CUDA scheduling are outside this proof. |

In particular, neither the `hp` completeness premise nor the row-table contract
in `joinRows_equations` should be cited as a theorem about the CUDA code. A
complete implementation proof must discharge these obligations from an
operational semantics or a checked translation of the actual source. There is
no axiom named "CUDA is correct" hiding them in the new theorem closures.

Source fixes from this work, present in `src/gpu/join.cuh`: the planner's
best score starts at the minimum integer, so rows with negative scores are
still selected; invalid sizes, zero fibre counts and unrepresentable products
or sums fail open (keep, `np = 0`); range upper bounds compare the leading
value, avoiding the `(65535+1)<<48` wrap.

## Reproduce the checks

With the pinned Lean/Mathlib dependencies available (`cd proofs && lake exe cache get`):

```sh
python3 scripts/test-gpu-join-proofs.py
python3 scripts/test-gpu-join-source.py
ASAN_OPTIONS=detect_leaks=0 python3 scripts/test-gpu-join-source.py --sanitize
```

The first builds the seven join modules and asks Lean for the axiom closures
of the 19 main theorems; only `propext`, `Classical.choice` and `Quot.sound`
are permitted, and the finite category proofs use `decide`, not
`native_decide`. The second extracts the actual table builders, planner,
general join and fused structure-3 decision from `src/gpu`, replaces CUDA
allocation/copy operations and annotations with host equivalents, and
compares exact counts and verdicts with an independent global-coordinate
point enumerator on 552 cases spanning all 46 descriptors (shared-prefix
permutations and repeated weights included), plus the fused decision and its
capped keep path. It also exhausts the 256 category-mask pairs and tests zero
counts, multiplication overflow, unsupported layouts and the four-position
maximum-value range edge. These are host tests, not GPU execution tests.
