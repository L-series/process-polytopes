# CWS equations, containers, and lattice realizations

This review addresses how simplex weights become combined equations, how the equations recover a real pairing space, and what is additionally needed to recover every lattice realization. It belongs to the approved consolidated literature batch. Its long-term reference point is a terminating KS enumeration in each fixed dimension; its current formal evidence is the reviewed frozen source, not a new build or an implementation certificate. The [minimal-seed note](minimal-ip-circuits-and-types.md) treats decomposition, circuits, and the archived structure list separately.

## Primary source and provenance

**[KS00]** Maximilian Kreuzer and Harald Skarke, _Reflexive polyhedra, weights and toric Calabi-Yau fibrations_, Reviews in Mathematical Physics **14**, no. 4 (April 2002), 343–374, [DOI 10.1142/S0129055X0200120X](https://doi.org/10.1142/S0129055X0200120X). The DOI, authors, journal, issue, pages, and publication date were verified against the publisher-deposited [Crossref record](https://api.crossref.org/works/10.1142/S0129055X0200120X). Publisher HTML could not be fetched; the paper actually read was the primary [arXiv math/0001106v1](https://arxiv.org/abs/math/0001106v1), submitted 19 January 2000, [PDF](https://arxiv.org/pdf/math/0001106v1).

The reused original PDF is `docs/learning/sources/kreuzer-skarke-math-0001106/v1/paper.pdf`, SHA-256 `7d400220d9bb4f0e0e2a121d46d75a7aad0b0a98ebbc1d6ad45e33407d0b01c1`, independently checked against local `provenance.json`. Retrieval was 2026-10-14 UTC; `paper.txt` is a derived reading aid. Source-license status is unverified. No duplicate/shared-cache write was made. Pagination below is the paper's printed pagination, not its PDF leaf index or journal pagination.

The central sources are **§2.2**, pages 9–12, and **§2.3**, pages 13–17. In particular, the lattice classification algorithm is Lemma 7 in §2.3, pages 13–15; §3 starts the geometric interpretation of classification results on page 17. Confusing the section number would direct a later implementation review to the wrong material.

Lean paths below are relative to the read-only frozen root:

`/home/ahat01/.copilot/session-state/83ef73b2-4517-446c-9ac2-9437ff014965/files/refpoly-proof-snapshot/proofs`.

`GeometricCWS.lean`, `CWSBasis.lean`, `CWSContainer.lean`, `CWSAssembly.lean`, and the relevant definitions in `WeightSystem.lean`/`Algorithm.lean` were inspected directly. All declarations are in namespace `Refpoly`; filenames below identify source locations rather than extra namespaces. No sibling checkout, proof, dependency, code, archival ref, or commit was modified.

## Equations determine a space only when the relation family is complete

Let `v_1,...,v_N` span an `n`-dimensional real vector space. Let `A : R^N -> R^n` take coefficients to `sum_i c_i v_i`. The pairing map `F : (R^n)* -> R^N` takes `x` to the coordinates `(<v_i,x>)_i`. Its image is the annihilator of `ker A`. If rows of a matrix `W` are relations among the vertices, then `im F` is contained in `ker W`; equality requires their row span to equal **all** of `ker A`.

For any real `k`-by-`N` matrix, rank-nullity gives

`dim(ker W) = N - rank(W)`.

If the rows are independent, this is `N-k`. An arbitrary list of `k` blocks does not establish independence. Coordinate coverage does not establish it either, and independent rows need not constitute every relation of an independently supplied vertex configuration. Redundant blocks must be removed or handled with rank rather than raw row count.

[KS00] §2.2 defines a CWS from the positive normalized relations of an **IP simplex structure**, padding each row with zeros off its support. Its Lemma 4, page 10, assumes a minimal polytope and its corresponding simplex-structure CWS. It identifies the image of the pairing map with the simultaneous equation subspace, identifies the polar with coordinate inequalities `x_i >= -1`, and identifies the finest lattice when the CWS is rational. These conclusions do not apply to an arbitrary nonnegative matrix merely called a CWS. The IP simplex-structure hypotheses carry geometric information.

The formal `WeightSystem.lean` structure `CWS k N` explicitly stores nonnegative rows, a positive occurrence for every coordinate, and `LinearIndependent R W`. It does not store a vertex realization, affine independence of supports, rationality, primitive integral row normalization, or a complete relation-kernel witness. `CWS.subspace` is the intersection of row kernels; `CWS.IsIP` says that origin is interior, in that subspace, to the convex hull of its integer points satisfying the coordinate inequalities. Thus full ambient interior in `R^N` is neither intended nor possible for nonzero equations.

`CWSBasis.lean` separately supplies completeness. `CircuitBasis S` has independent positive simplex rows whose span equals `ker(vertexCombination S)`. `exists_circuitBasis` obtains such a basis abstractly from intrinsic-IP geometry. `CircuitBasis.pairing_range` then proves equality with the joint row kernel; `coordinate_count` assumes ambient IP and proves `S.card = n + B.k`. `CWSContainer.lean` reindexes this basis with the noncomputable `coeffEquiv`, constructs `B.toCWS h0`, and proves that reindexing preserves membership in the equations. These conclusions use the stronger basis object, not the bare `CWS` fields.

## Component IP and the projection hypothesis

[KS00] §2.2, page 11, gives the corollary that an IP CWS has IP constituent weight systems. The argument projects its lattice hull to one support: the projected hull is contained in the corresponding one-row hull. For origin to remain interior in the **entire one-row solution space**, the restricted projection must be surjective onto that space. Containment alone establishes interior only relative to the image.

In `WeightSystem.lean`, `cws_ip_component` assumes an injective support map `e`, the exact positive support of a row, agreement with the component weights `q'`, and the explicit lifting hypothesis

`for every z with q' dot z = 0, there exists y in the joint kernel with y(e b)=z(b)`.

It returns `IsIPWeightSystem q'`. That lifting hypothesis is an essential part of the actual theorem; a docstring's broad description must not hide it.

An independent elementary counterexample shows why a positive joint dimension and independent covering rows do not suffice. Take `N=3`, `k=2`, with normalized rows

`q=(1,1,3)/5`, `r=(1,1,0)/2`.

They are nonnegative, independent, and cover all coordinates. The joint kernel is `{(t,-t,0)}`. Its permitted integer points are `(-1,1,0)`, `(0,0,0)`, `(1,-1,0)`, so its lattice hull has origin in its one-dimensional interior. The full-support component `q`, however, is not IP: its integer generators satisfy `x_1+x_2+3x_3=0` and `x_i>=-1`; any positive integer `x_3` would require `x_1+x_2<=-3`, contradicting `x_1+x_2>=-2`. Thus every generator has `x_3<=0`, so origin cannot be interior in the two-dimensional component hyperplane. The block projection has one-dimensional image and is not onto. This is a counterexample to omitting the lifting hypothesis for arbitrary matrices, not a counterexample to the geometric KS corollary.

The frozen geometric development supplies a different route. `GeometricCWS.lean` proves `simplex_relation_kernel` from affine independence, normalization, and the circuit equation. `simplex_pairing_surjective` uses those same hypotheses to lift every vector in the component hyperplane to a pairing with simplex vertices, even for a lower-dimensional simplex. No ambient full-dimensionality of the simplex is required. `reflexive_simplex_block_ip` additionally assumes a reflexive polytope, positive weights, integral simplex vertices, and their membership in the dual polytope; its proof maps lattice generators into the component hull and uses the surjective real map's openness to obtain IP. For a complete global circuit basis, the global pairing image and this local pairing surjectivity together provide the geometric lifting interpretation. Surjectivity on **real spaces** is enough for this interior proof; it must not be confused with surjectivity on integer lattices.

## Finest, coarsest, image, and saturated lattices

Assume rational/lattice vertices spanning `N_R`. Let `N_0` be their integer span, the paper's `N_coarsest`, and let `M_f=N_0*`, the paper's `M_finest`. Rationality and full span make these full-rank discrete lattices. If the vertices are integral in an ambient lattice `N`, then

`N_0 <= N`, hence `M=N* <= M_f`.

The pairing embedding identifies `M_f` with `L_f = ker(W) intersect Z^N` under the hypotheses of [KS00] Lemma 4(c). Integral coordinate pairings characterize membership in the dual of `N_0`, which is why this is the **finest** lattice. The image of an originally chosen lattice `M` can be a proper finite-index sublattice of `L_f`.

The distinction can be stated using saturation. In the rational joint solution space, `L_f` is saturated in the coordinate lattice: if `a z` lies in the real kernel for a nonzero integer `a`, then `z` lies there too. A full-rank image lattice can nevertheless be unsaturated within `L_f`. Rational equations, an integral kernel basis, and primitive/saturated image data must be tracked separately from a real basis. The finite quotient `L_f/F(M)` records the missing index data; it is not encoded just by row support or real row span.

Here is a concrete example satisfying the geometric container setting. In `R^2` with lattice `Z^2`, let

`P=conv(±e_1,±e_2)`.

This reflexive diamond has dual square `[-1,1]^2`. Choose the vertex-minimal IP seed in that square consisting of `±(1,1)` and `±(1,-1)`. Two positive segment circuits give rows `(1,1,0,0)/2` and `(0,0,1,1)/2`. The pairing map is

`F(x,y)=(x+y,-x-y,x-y,-x+y)`.

Its real image is the CWS space `{(a,-a,b,-b)}`, and it is bijective onto that space. Integer `(x,y)` give integer coordinates, but the image lattice has the extra parity condition `a=b mod 2` and index two in the full coordinate lattice of the subspace. The integer vector `(1,-1,0,0)` has inverse `(1/2,1/2)`, which is not integral. In fact `F(P)` equals the real square container `|a|<=1`, `|b|<=1`; the missing assertion concerns its lattice, not the real inclusion or hull.

This independent example explains exactly why the signatures of `CircuitBasis.reflexive_container`, `algorithm_enumerates_cws_container`, and the assembly coverage theorems cannot be read as unimodular equivalence. They exhibit a real linear bijection, forward preservation of lattice points, and `F '' P` contained in the CWS lattice hull. They do not assert that every integer point of the target subspace has an integral preimage. Requiring that extra assertion indiscriminately would exclude valid examples; the correct completeness route records the image sublattice and later explores lattice realizations.

Duality reverses the lattice inclusions as well as polar inclusions. Refining `N_0` to a larger lattice `N` corresponds to replacing its dual `M_f` by a smaller sublattice `M`. Depending on which side is described, the associated finite quotient may be presented as a quotient/orbifold condition or a sublattice restriction. An implementation must state the side, the actual integer embedding, and the pairing compatibility; a real change of coordinates alone is insufficient.

## Effective finite lattice choices: the exact scope of KS Lemma 7

[KS00] §2.3 begins by stating that reflexive polytopes are recovered as subpolytopes of suitable CWS containers, possibly on sublattices of `M_finest`. For a **fixed full-dimensional IP real polytope** `Delta` and its polar, form the vertex-pairing matrix `X`. If `X` is integral, both vertex-generated coarsest lattices pair integrally, so

`M_coarsest <= M <= M_finest`

for any reflexive realization. This is now a bounded lattice interval: its endpoints have the same rank and their quotient is finite. Full span and integrality of the VPM matter. Arbitrarily choosing all sublattices of `M_finest` without the lower endpoint would give infinitely many choices.

Lemma 7, page 13, diagonalizes `X` by unimodular integer row and column operations: `X=W D U`, with `D` an `n`-by-`n` positive diagonal integer matrix after discarding zero rows/columns. It describes compatible lattices by integer upper-triangular factorizations `D=T S`. The columns of `S U` give primal vertices in lattice coordinates; the rows of `W T` give dual vertices. The finest/coarsest realizations correspond to `D U`/`U` on the primal side and `W D`/`W` on the dual side. This is an actual lattice-coordinate reconstruction, stronger than a real forward-integral embedding.

The proof on page 14 selects diagonal divisors of `D`, bounded off-diagonal representatives, and solves the triangular equations `T S=D` for integral entries. There are finitely many divisor choices and finitely many residue choices, giving an effective finite enumeration for the fixed integral VPM. Equivalently, one may enumerate subgroups of the finite quotient between the two endpoint lattices and recover intermediate lattices; this equivalence is a mathematical interpretation of the bounded interval, not a claim that the frozen package implements it.

An indexing caution must accompany a faithful transcription: the extracted v1 statement/final proof inequality reads `0<=T_{ji}<T_{ii}`, while the worked discussion specifies `0<=T_{12}<T_{11}`. A future algorithm port must settle a consistent row/column Hermite-style normalization and check the PDF/source convention. The finite-factorization principle and integral reconstruction are clear, but this note does not silently resolve the residue-bound indexing or assert a canonical implementation from the extraction.

The next paragraph, page 15, says to construct all subpolytopes with integral VPM of the CWS containers and apply Lemma 7. It supplies no uniform finite-index enumeration for unrestricted sublattices independently of a chosen polytope. The program needs a finite container lattice-point search, exact hull/facet/VPM computations, integral-VPM tests, and then the finite interval reconstruction. Source existence claims and an implementable fixed-matrix procedure are different parts of that chain.

The need is visible in the paper's own data: Proposition 2, page 17, identifies a three-dimensional reflexive polytope requiring a `Z_2` sublattice realization beyond the finest-lattice subpolytope list. This is historical evidence for the missing lattice stage, not a new census result or a benchmark.

## Reachability and finite assembly in the frozen source

| Declaration                                                          | Inspected conclusion                                                                                                                                                                | What it does not promise                                                                                                                   |
| -------------------------------------------------------------------- | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------ |
| `GeometricCWS.lean`: `algorithm_reaches_simplex_block`               | Each geometric dual-lattice circuit has a reachable WS node with selected weights equal to its normalized row; node size is bounded by support size plus one.                       | Actual native row ordering, pruning, integer representation, or execution trace.                                                           |
| `Algorithm.lean`: `SelectionRule`, `Reachable`                       | A rule picks from nonempty feasible regions; an inductive tree starts at the ones vector and inserts admissible nonnegative lattice points under a strict inequality.               | A supplied computable rule, exhaustive executable traversal, or equivalence with native stopping conditions.                               |
| `CWSContainer.lean`: `ReachedCircuit`, `CircuitBasis.blocks_reached` | Each basis row has a genuine simplex support and a WS reachability witness; length is at most `n+1`, or at most `n` for nonsimplicial minimal seeds.                                | A chosen optimized overlap descriptor or complete catalogue of geometric types.                                                            |
| `CWSContainer.lean`: `CWS.RowsReached`, `CircuitBasis.rows_reached`  | Every row is reached and injectively zero-padded with exact support/off-support equations.                                                                                          | A native assembler's completeness or an independently checked relation-rank computation.                                                   |
| `CWSAssembly.lean`: `candidateRows_finite`, `candidateCWS_finite`    | Bounded row length and coordinate/row counts give finite mathematical candidate sets, using normalized IP weight finiteness.                                                        | An explicit degree cutoff or executable finite list.                                                                                       |
| `cwsCatalogue`, `dimensionCWSCatalogue`                              | Noncomputable finite-set choices from those finiteness proofs.                                                                                                                      | A computed output, canonical representatives, or proven equality with the 46 descriptors.                                                  |
| `ws_cws_catalogue_coverage`, `finite_ws_cws_container_coverage`      | Every reflexive polytope has a forward-integral real bijective embedding into an IP container in the abstract catalogue, with an existential dimension bound in the latter theorem. | Inverse integrality, all image sublattices, all lattice realizations, maximality, subpolytope enumeration, or terminating end-to-end code. |

The existence of finite row catalogues is valuable: it avoids an empirical numerical degree bound in the abstract coverage statement. It still must be connected to effective weight generation and the actual native input pools. The current approved delivery covers its implemented descriptor/input-pool universe on the PALP reference lattice, not the union of every possible lattice realization.

## Remaining obligations and target boundary

| Obligation                                     | Supported evidence                                          | Missing refinement / limitation                                                                                                                    |
| ---------------------------------------------- | ----------------------------------------------------------- | -------------------------------------------------------------------------------------------------------------------------------------------------- |
| Complete equations and expected real dimension | Circuit basis, pairing-range equality, coordinate count     | Bare nonnegative covering matrices require stronger hypotheses; concrete row ranks need exact checks.                                              |
| IP component projection                        | Explicit `hlift`, or geometric simplex pairing surjectivity | Coordinate containment alone fails; real surjectivity must be established in the intended support semantics.                                       |
| Finest lattice identification                  | Primary Lemma 4(c), rational geometric CWS                  | Formal endpoint lattice and integer preimage characterization are absent from the inspected container theorem signatures.                          |
| Original image lattice                         | Forward integrality in frozen coverage                      | Record an integral basis/index/saturation witness; inverse integrality is neither proved nor generally true.                                       |
| Compatible finite lattice realizations         | Primary Lemma 7, fixed integral VPM                         | Formal finite quotient, constructive integer normal forms, consistent normalization, reconstruction correctness, and exhaustive finite traversal.  |
| Finite subpolytope recovery                    | Primary container strategy                                  | Implement/prove lattice-point bounds, hull search, target tests, and recovery on every relevant lattice.                                           |
| Maximal starting objects                       | Separate minimality refinements in KS §2.3                  | IP containers need not be reflexive or maximal; Lemma 8's automatic reflexivity is explicitly limited to `n<=4`.                                   |
| Native WS/CWS enumeration                      | Abstract reachability and finite catalogue                  | Executable branching/stopping/pruning, representation, input-pool and descriptor refinement, and termination proof.                                |
| One representative per class                   | Lattice reconstruction and eventual equivalence relation    | Correct lattice normal forms and exact equivalence/deduplication require their own theorem; equal hashes are insufficient.                         |
| General nonreflexive one-point IP route        | Real circuit geometry survives                              | Reflexive dual-lattice containment and integral-VPM recovery are not automatically available; rational polar extension or another route is needed. |

For reflexive targets, a minimal seed in the integral polar yields a container; Lemma 7 describes finite lattice choices after a candidate integral VPM is fixed. For general one-point lattice polytopes, the polar can be nonintegral, so the same dual-lattice circuit and pairing argument cannot simply be reused. The long-term theorem should state a concrete terminating procedure covering the declared target up to the declared lattice equivalence, including both subpolytope and lattice stages. The present source supplies substantial abstract coverage ingredients and exposes the remaining lattice information; it does not establish that complete executable theorem.

## Evidence checklist

- Verified exact primary preprint version, journal DOI/metadata, cache hash, and printed theorem locations.
- Read KS §2.2–§2.3, including component projection, Lemmas 4 and 7–9, and the sublattice example in Proposition 2.
- Inspected actual CWS, projection-lifting, circuit-basis, geometric container, reachability, and noncomputable catalogue declarations.
- Labeled the projection and parity examples as independent deductions; no theorem number or computational result was invented for them.
- No fresh build, global completeness, inverse-integrality theorem, native refinement, or new scientific count is claimed.
