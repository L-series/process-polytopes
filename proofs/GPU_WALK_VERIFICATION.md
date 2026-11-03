# Lattice walk certificate and completeness boundary

The FP64 root box is a performance heuristic. Its error bound is not assumed
in the proof. Instead, a candidate can leave the point stage only after the
walk has produced valid distinct lattice points and the output count equals
the exact join count. The finite-set theorem `checked_walk_complete` then
proves coverage of every point. Missing a root interval, numerical failure,
capacity exhaustion, or failure of any certificate defers the candidate.

## Production checks

`src/gpu/lattice.cuh` now performs these checks:

* Signed 64-bit basis arithmetic uses checked add, subtract, multiply, divide
  and remainder; failure aborts the GPU walk. Basis entries are checked before
  narrowing to 32 bits. LLL row updates use 64-bit intermediates and checked
  narrowing. Nonfinite/degenerate Gram–Schmidt calculations defer.
* After reduction, every basis row is checked against every CWS weight row.
  A mod-2 Gaussian elimination proposes pivot columns and a left-inverse
  witness. The witness is checked independently against the original rows.
  A failed mod-2 rank check is inconclusive even when integer rank might be
  five; it is never a non-IP verdict.
* Root endpoints are checked for finite representable conversion. A conservative
  exact bound checks `sum_j |basis[j,A]| * max(|Lo[j]|,|Hi[j]|)` against
  `INT_MAX/8 - 2`, and coordinate upper bounds against `INT_MAX/4`. These bounds
  keep all subsequent int32 interval numerators, products, partial sums and
  endpoint increments representable. Failure defers.
* Every emitted ambient coordinate is checked against its exact nonnegative
  box. Reduced coefficient tuples are sorted and checked for duplicates.
  Every final tuple is then independently mapped back with checked arithmetic
  and verified against every exact CWS row equation.
* Coordinate/pair certificate masks are recomputed from that checked final
  buffer. Thus masks and the cardinality check refer to the same family.
  The point-stage caller must compare this count with the exact join count
  before trusting either the masks or an IP result.

The IP checker receives the reduced coefficients directly. The transform matrix
`U` is removed: it is unnecessary for IP and introduced additional unchecked
integer arithmetic. An invertible real coordinate map suffices; an integral
or unimodular map is not required once complete point coverage is established.
A potentially unsaturated basis may miss points, but cannot pass the exact
count check unless its valid distinct output still covers the entire point set.

## Kernel-checked results

`Refpoly/GpuWalk.lean` proves nine audited theorem closures:

* `checked_walk_complete`: finite-set membership, injectivity and exact count
  imply complete coverage. No floating-point assumption is present.
* `integer_det_ne_zero_of_mod2_left_inverse`, `real_minor_independent` and
  `real_embedding_injective`: the checked mod-2 witness certifies an independent
  integer minor and an injective real row embedding.
* `distinct_embedded_points`: distinct coefficient tuples have distinct
  ambient images under that embedding.
* `reduced_kernel_map_bijective`, `exists_reduced_coordinates`: rows in the
  actual production CWS kernel and the rank certificate give the full real
  coordinate isomorphism, using the proved `cws_dimension` for a valid CWS.
* `ip_preserved_by_coordinates`, `cws_ip_iff_reduced_hull`: origin-interior of
  the hull is invariant under this coordinate change, tied to production
  `CWS.IsIP` and its actual generator set.

These proofs apply in arbitrary dimension. The current CUDA arrays and rank
checker implement dimension five. The Lean certificate premises are explicit;
they are not an axiom declaring the CUDA validator correct. Refinement of the
C++ validity/sort/arithmetic checker into these mathematical premises remains
an operational source-correspondence obligation.

## Source checks and reproducibility

```sh
python3 scripts/test-gpu-walk-proofs.py
python3 scripts/test-gpu-walk-source.py
ASAN_OPTIONS=detect_leaks=0 python3 scripts/test-gpu-walk-source.py --sanitize
```

The source harness compiles the actual `lattice.cuh` with CUDA annotations and
intrinsics replaced by host shims. It checks 250,405 arithmetic operations,
including signed extrema, against `__int128`; verifies rank/kernel/duplicate/
invalid-point guards; checks capacity and extreme-degree deferral; and compares
exact ambient point sets and coordinate/pair certificates against an independent
ambient enumerator on 184 examples covering all 46 descriptors. All passed,
also with AddressSanitizer and UndefinedBehaviorSanitizer.

With CBMC and goto-cc on PATH (`nix develop .#gpu-verify`):

```sh
python3 scripts/test-gpu-walk-rank.py
python3 scripts/test-gpu-walk-rank.py --nonvacuity
```

The first extracts the actual rank checker and verifies that acceptance excludes
a zero nontrivial row combination over all 5×10 parity matrices, with symbolic
nonzero coefficient masks. This quantifies the full 50-bit rank domain rather
than sampling it. Kernel dot validation is isolated out of this particular
query. It checks bounds, pointers, signed overflow, division and unwinding.
The witness check is therefore supported both by the mathematical determinant
argument and source-level finite machine checking. Neither CBMC nor the host
checks prove CUDA compilation, hardware behavior, or the full pipeline.
