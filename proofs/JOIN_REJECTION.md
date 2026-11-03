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
| `JoinStack` | Explicit row-frame advance/push/pop traversal preserves the complete ordered key-tuple list; Cartesian filtering, no duplicates and exact fibre-product sum; depth-indexed slot decoder refines frame steps; bound snapshots explain mutable stale values. |
| `JoinSearch` | Binary lower-bound termination and partition invariants for arbitrary interval lengths; exact packed-value ranges; lookup completeness and uniqueness for sorted keys. |
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

## Integration continuation: checked table contracts and bounded search

The host builders now check the following contracts before their tables are
used for rejection:

* Row weights are positive, and row size and degree agree with the entry.
* Enumerated row points are distinct, ordered, nonnegative and satisfy the row
  equation. Canonical fibres partition **that enumerated list**, have increasing
  packed keys and positive exact counts, and carry the exact category/pair ORs.
* Each position-sorted index is a strictly sorted permutation of its entry's
  canonical indices, with the indicated coordinate packed first.
* The plan's row order is a permutation; shared/private ownership, same-row
  masks, cumulative bound masks and lookup flags agree with its coordinates.
* Pool/canonical indices fit the representation consumed by the device.
  In the fused builder, presence masks and Bloom filters contain every stored
  key; false positives are allowed, false negatives abort the build.

Both table views now carry explicit per-entry coverage bytes. An entry whose
shared-coordinate bound exceeds 65535 is not enumerated into a packed table.
A fibre count or aggregate canonical-index width that cannot be represented
also marks the entry uncovered. Both device decisions check coverage before
using keys and return `keep, np=0`; the pipeline must CPU-defer it. An empty
key interval is therefore never used to reject an unsupported entry. Host
source tests exercise this distinction in both general and fused paths.

The fused builder uses the same fibre validator after converting its record
format. Both builders now avoid the signed overflow in the former
`v * weight <= remaining` loop condition, check 16-bit shared values and
32-bit fibre counts, and propagate worker errors to the host caller. Private
pair sums use 64-bit arithmetic. Checks run whenever these tables are built;
they are not just test assertions. They add host work during table construction,
not device work per candidate; large-pool construction overhead is not measured.

These checks do **not** detect a point omitted by the row enumerator itself.
Nor do they prove CUDA uploads, subsequent device memory contents, or the
machine-state refinement of the traversal. The abstract row-enumeration proof
and its relationship to the C++ loops remain distinct obligations.

`device_gjoin_begin_level` is the former range-initialization lambda extracted
as a device function so CBMC can parse the actual search bodies. The bounded
checks extract those bodies verbatim, replace CUDA annotations/intrinsics and
the null-pointer spelling, and give functions C linkage to avoid commas in
CBMC loop identifiers. The harnesses provide the table/plan preconditions;
they do not assume the search result. Bounds, pointers, signed overflow,
division and loop unwinding are checked. A timeout is a failure to verify.

The completed bounded checks cover:

* Three rows, zero or one key per row, two shared positions per row with values
  in `{0,1}`, and independent fibre counts in `1..3`. Four support patterns
  exercise a cycle with a lookup, scans at entry position zero or one, a chain,
  and a disconnected prefix followed by a lookup. The comparison is the exact
  weighted count of compatible tuples from a separate Cartesian-product oracle.
  Category masks are suppressed: this is a search/count check, not a geometric
  rejection proof.
* Two rows, zero to two keys per row, values in `{0,1}` and independent
  counts in `1..3`, with canonical-position scans, position-one scans and
  disconnected row products. These additionally exercise sibling iteration
  and backtracking in the symbolic check.
* Range initialization on zero to two canonical keys, with **arbitrary full
  64-bit words** (four 16-bit coordinates), an arbitrary 16-bit target and
  bound position zero or two. Every matching key, and only such a key, lies in
  the returned interval. This covers the maximum-value upper-bound edge.
* Nonempty-input witnesses for the search and range checks. Deleting a scan
  range in the extracted source makes the count assertion fail; the mutation
  is never written to production source.

The larger three-row/two-key symbolic check timed out at 180 seconds, including
with unit counts. It is available as `--max-keys 2`, **not verified** by the
smaller result. Exhaustive host execution instead checks all 1,203,052
combinations of up to two keys chosen from `{0,1}²` with fibre counts `1..3`,
over the four support patterns. This covers sibling traversal, backtracking
with stale unbound values, empty matches, position-index scans and lookups.
It passes under AddressSanitizer and UndefinedBehaviorSanitizer, alongside the
existing independent global-coordinate tests for all 46 descriptors. Host
exhaustion still trusts the host compiler and does not generalize to arbitrary
table lengths.

`JoinSearch.lean` now proves the unbounded mathematical binary-search
argument as well. Its recursive loop uses the source's midpoint
`(lo + hi) / 2`, the same `lo = mid + 1` / `hi = mid` updates, and a decreasing
interval-length bound. `packed_range_exact` proves that the two searches
isolate exactly the requested leading coordinate, for arbitrary sorted table
lengths, radix and target. The second search starts at the first result, as in
CUDA; it never forms the overflowing successor of the maximum target.
`search_lookup_exact` and `search_lookup_unique` discharge the sorted-table
lookup argument. These are Nat algorithms: translating the source's pointer
accesses, shifts and fixed-width arithmetic into this model is still a source
correspondence obligation.

`JoinStack.lean` now proves exact ordered enumeration for a source-shaped
row-frame machine: advance the current cursor, skip a mismatch, push a child,
emit at the last row, and pop on exhaustion. Its natural work measure decreases
by one per step. The resulting key-tuple list equals an independent filtered
Cartesian product; unique row keys give unique tuples, and its sum of fibre
products is exact. A depth-indexed slot-store decoder proves each indexed
update/push/pop refines that frame transition, including the capacity bound
for child writes. Bound-snapshot theorems prove that failed mutable matches
preserve earlier frames and successful matches implement pure extension.

The actual `device_gjoin_match_key` is extracted and CBMC-checked for the
**entire supported local domain**: arities 0..4, distinct coordinate injections
into 0..9, all 64-bit key words, all ten-bit bound masks, and arbitrary signed
32-bit stale values. Exact success, bound preservation even on failure,
outside-row preservation and installation on success all pass, as does a
nonempty-input witness.

The remaining source refinement obligation is to decode the actual packed
key/index ranges, `chosen` identities and mutable value array jointly into
those indexed frames throughout the complete CUDA loop, and to discharge the
machine correspondence for the proved search loops. The proved slot-store
model carries decoded row-frame data; it is not an assertion that a C++ parser
has already translated every pointer access and every machine instruction.
Actual single-row enumeration, upload/storage and fused traversal refinement
also remain distinct obligations. Bounded verification
and host exhaustion must not be cited as discharging it.

## Reproduce the checks

With the pinned Lean/Mathlib dependencies available (`cd proofs && lake exe cache get`):

```sh
python3 scripts/test-gpu-proofs.py
python3 scripts/test-gpu-join-source.py
ASAN_OPTIONS=detect_leaks=0 python3 scripts/test-gpu-join-source.py --sanitize
ASAN_OPTIONS=detect_leaks=0 python3 scripts/test-gpu-join-source.py --sanitize --exhaustive-search
```

With CBMC/goto-cc available (`nix develop .#gpu-verify`):

```sh
python3 scripts/test-gpu-join-search.py --weighted
python3 scripts/test-gpu-join-search.py --rows 2 --max-keys 2 --weighted --shape 0 --shape 1 --shape 3
python3 scripts/test-gpu-join-search.py --range
python3 scripts/test-gpu-join-search.py --weighted --shape 0 --nonvacuity
python3 scripts/test-gpu-join-search.py --range --nonvacuity
python3 scripts/test-gpu-join-search.py --weighted --mutation
python3 scripts/test-gpu-join-match.py
python3 scripts/test-gpu-join-match.py --nonvacuity
```

The first builds the join and GPU proof modules and audits their theorem closures; only `propext`, `Classical.choice` and `Quot.sound`
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
