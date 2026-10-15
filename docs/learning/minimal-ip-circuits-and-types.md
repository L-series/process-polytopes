# Minimal IP seeds, positive circuits, and finite structure data

This note explains the minimal-seed and circuit geometry behind the dimension-generic Kreuzer–Skarke construction, and distinguishes it from archived five-dimensional structure lists. The [formal endpoint](../../proofs/STATEMENTS.md) proves reflexive coverage with lattice realizations and subpolytope recovery; equality with a native descriptor catalogue is a separate claim.

## Primary references and evidence scope

**[KS97]** Maximilian Kreuzer and Harald Skarke, _On the Classification of Reflexive Polyhedra_, Communications in Mathematical Physics **185** (1997), 495–508, [journal DOI](https://doi.org/10.1007/s002200050100). The inspected primary preprint is [hep-th/9512204v2](https://arxiv.org/abs/hep-th/9512204v2), submitted 4 February 1997; its cover records December 1995 and a November 1996 revision. References below use the printed pages of the 13-page [v2 PDF](https://arxiv.org/pdf/hep-th/9512204v2), not guessed journal pagination. The geometric statements are in §2, especially printed page 4; the example continues on page 5. The small-dimensional lists are in §4, pages 9–10.

**[KS00]** Maximilian Kreuzer and Harald Skarke, _Reflexive polyhedra, weights and toric Calabi-Yau fibrations_, Reviews in Mathematical Physics **14** (2002), 343–374. The inspected primary version is [math/0001106v1](https://arxiv.org/abs/math/0001106v1), submitted 19 January 2000, [36-page PDF](https://arxiv.org/pdf/math/0001106v1). Section 2.1, pages 7–9, reformulates the structural argument using IP simplex structures; §2.2, page 10, states the CWS pairing-space and lattice result as Lemma 4. Publication metadata was checked against the arXiv records. The publication year and preprint year intentionally differ.

The consulted primary PDF editions have the following original-file hashes. Retrieval date is 2026-10-14 UTC; cache paths are relative to `docs/learning/sources/`:

| Source/version         | Original cached PDF                          | SHA-256                                                            |
| ---------------------- | -------------------------------------------- | ------------------------------------------------------------------ |
| KS97, hep-th/9512204v2 | `kreuzer-skarke-hep-th-9512204/v2/paper.pdf` | `c703beefd69d2f203c32cfafcf46dc971b7721b8ee23b831738123cdcf896075` |
| KS00, math/0001106v1   | `kreuzer-skarke-math-0001106/v1/paper.pdf`   | `7d400220d9bb4f0e0e2a121d46d75a7aad0b0a98ebbc1d6ad45e33407d0b01c1` |

Each directory has the primary URL and retrieval timestamp in `provenance.json`; `paper.txt` is a derived reading aid. Source licenses are unverified, so this is a local reading cache rather than a redistribution claim. `SINGLE_WS_5d.tex` is a separate single-weight paper, not the specification or proof-status authority for this construction.

Formal declaration paths below are relative to `proofs/Refpoly/`; [SOURCES.md](../../proofs/SOURCES.md) identifies the original editions. Archived catalogue evidence is fixed to root-repository ref `gpu-run-43types`, commit `452fbe8e1b296bf75fa9e8becf6c34d8d48bf00a`, at `lean/MinimalPolytopes5D/Profiles.lean`, `lean/MinimalPolytopes5D/Structures.lean`, `paper/minimal_polytopes_5d.tex`, and `paper/minimal_polytopes_5d_table.tex`.

## Definitions that control the interpretation

Fix a real vector space of dimension `n`, a finite generating set `S`, and origin `0`. The IP property means that `0` lies in the interior of `conv(S)` in the stated ambient space. It does **not** assert that there is exactly one interior lattice point. A lower-dimensional component has empty ambient interior, so its appropriate property is interior relative to its affine span. If the hull contains zero, that affine span is the corresponding linear span, which makes quotient arguments legitimate.

A vertex-minimal full-dimensional IP set retains IP itself and loses full-dimensional IP after removing any generator. For a finite set, testing all proper subsets and testing each one-point deletion are equivalent by monotonicity of convex hull and interior. This is distinct from inclusion-minimal reflexivity, minimality among all lattice subpolytopes, or a globally smallest vertex count across unrelated polytopes. No reflexivity or lattice hypothesis is needed for the real geometric decomposition.

A **good simplex**, or **IP simplex**, has affinely independent vertices and origin in its intrinsic interior. Equivalently, its unique normalized barycentric relation has strictly positive coefficients:

`q_i > 0`, `sum q_i = 1`, `sum q_i v_i = 0`.

The vertex vectors of such a nontrivial simplex are linearly dependent: the positive relation is exactly that dependence. Affine independence instead says that any relation whose coefficients also sum to zero is trivial. Subtracting suitable multiples of the normalized positive relation shows that its linear-relation space is one-dimensional. In the homogenized coordinates `(v_i,1)`, the same vertices are linearly independent. One must specify the representation before calling simplex vertices “independent.”

The positive relation guarantees simplex IP in its own span. An IP **weight system** in the enumeration sense requires a separate integer Newton-hull condition. Merely exhibiting a positive circuit does not establish that condition. Real weights also do not automatically give primitive integral weights: for lattice/rational vertices, rationality and normalization require their own bridge.

## The structural reduction and what follows from it

### A single step

[KS97] §2, Lemma 1, page 4, concerns an `n`-dimensional minimal polytope with `k` vertices. It either is a simplex, or has a lower-dimensional minimal subpolytope `M'` with dimension `n'` and `k'` vertices, together with a good simplex `T`. The simplex uses all vertices outside `M'` and possibly some old vertices; their union is the original vertex set. The quantitative conclusions are

`k - k' = n - n' + 1` and `dim(T) <= n'`.

The quotient by the span of `M'` is crucial: the new vertices form a simplex there. This is stronger than finding an arbitrary proper subset whose hull contains zero. It is also stronger than covering each vertex by some positive circuit. Maximality of the proper relative-IP component, positive lifting from the quotient, and minimality supply the dimension and counting assertions.

[KS00] §2.1, Lemma 1, pages 7–8, gives the same reduction in real spaces. Thus the geometry used here is not restricted to a particular integral lattice realization.

### Iteration, vertices, and supports

[KS97] Corollary 1, page 4, iterates Lemma 1 into an ordered sequence of good simplices and partial minimal polytopes `M_mu`. If `k_mu` counts the vertices present through stage `mu`, then

`dim(M_mu) = k_mu - mu`.

For a final `n`-dimensional object with `k` vertices, the number of stages is therefore `lambda = k - n`. Each stage adds `m_mu >= 2` vertices, contributes `m_mu - 1` new dimensions, and the dimension increments sum to `n`. [KS00] §2.1, page 8, explicitly defines IP simplex structures with partial minimality and at least two newly introduced vertices, and proves existence in its unnumbered corollary.

[KS97] Corollary 2, page 4, gives `n + 1 <= k <= 2n` for full-dimensional vertex-minimal IP polytopes in positive dimension. Hence `1 <= lambda <= n`. Every good-simplex support has at most `n + 1` vertices; for a non-simplex step the stronger bound `dim(T) <= n'` constrains its overlap with the old component.

In dimension five, the resulting sorted **new-vertex** profiles are

`[6]`, `[5,2]`, `[4,3]`, `[4,2,2]`, `[3,3,2]`, `[3,2,2,2]`, `[2,2,2,2,2]`.

These entries count vertices introduced at each stage. They are not simplex support sizes: an attached simplex may additionally use old vertices. Sorting the increments is an arithmetic operation, not a proof that every reordered support family is still a KS decomposition.

The upper vertex bound gives a finite abstract structural search space in every fixed dimension: finitely many supports among at most `2n` labeled vertices, finitely many finite support families, and finitely many relevant orderings. This observation is a finiteness route for combinatorial candidates. It does not imply finitely many arbitrary real shapes, finitely many unrestricted IP lattice polytopes, effective weight bounds, or a geometrically sound list returned by a particular program.

### No-lonely restriction and decomposition ambiguity

[KS97] Lemma 2, page 4, excludes a simplex support with **exactly one** vertex occurring in none of the other supports of a spanning good-simplex family. [KS00] Lemma 2, page 8, states the condition for an IP simplex structure. Zero exclusive vertices are allowed. The five-dimensional example on [KS97] pages 4–5 and [KS00] pages 8–9 shows a simplex whose vertices are all covered by other blocks and shows nonunique choices of decomposition.

This is a necessary geometric restriction, not an if-and-only-if characterization of arbitrary support hypergraphs. Passing the no-lonely filter does not prove affine independence, positive realizability, full span, partial minimality, independence of equations, or equivalence-class correctness. A simplex entirely covered by the **union** of other blocks is possible. A good-simplex support properly contained in **one** other good-simplex support is impossible: the smaller support gives barycentric coordinates with zeros on the extra vertices, whereas intrinsic interior in the larger affinely independent simplex requires every coordinate strictly positive. Uniqueness of barycentric coordinates gives a contradiction.

## Supporting seed and circuit declarations

The namespace is `Refpoly`, and `V n` is the real coordinate space used by the package. The table describes supporting-library statements; the concrete `KS` modules supply primitive encodings, bounded seeds and executable enumeration.

| Source/declaration                                                                                             | Actual hypotheses and conclusion                                                                                                                                                                                                                                     | Scope consequence                                                                                                                             |
| -------------------------------------------------------------------------------------------------------------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | --------------------------------------------------------------------------------------------------------------------------------------------- |
| `MaxMin.lean`: `IsMinimalIPGen`, `exists_minimalIPGen_subset`                                                  | Finite hull has ambient interior origin; every proper subset loses it. Every finite IP set contains such a subset.                                                                                                                                                   | Finite descent, without lattice/reflexivity assumptions.                                                                                      |
| `Minimal.lean`: `IsGoodSimplex`                                                                                | Affine independence together with intrinsic-interior origin.                                                                                                                                                                                                         | Correct relative notion for lower-dimensional simplex pieces.                                                                                 |
| `Minimal.lean`: `IsMinimalIPRel`                                                                               | Intrinsic-interior origin, and no proper subset with the same affine span retaining intrinsic interior.                                                                                                                                                              | Minimality is measured in the fixed parent span; deleting to a smaller span is not prohibited.                                                |
| `CircuitGeometry.lean`: `positive_circuit_through`                                                             | A normalized nonnegative zero relation with a prescribed point's coefficient positive yields an affinely independent supported positive relation retaining that point.                                                                                               | Prescribed-point Carathéodory reduction.                                                                                                      |
| `CircuitGeometry.lean`: `ip_positive_circuit_through`; `Minimal.lean`: `exists_goodSimplex_through`            | Ambient IP and membership of a chosen generator yield a good simplex through it.                                                                                                                                                                                     | Circuit coverage requires neither lattice points nor minimality.                                                                              |
| `CircuitInterior.lean`: `positive_relation_intrinsicInterior`, `exists_positive_relation_of_intrinsicInterior` | Strict positive normalized zero relations imply intrinsic interior; finite intrinsic-IP sets admit such relations.                                                                                                                                                   | Both directions are present; affine independence is unnecessary here.                                                                         |
| `CircuitInterior.lean`: `finset_positive_relation_interior`                                                    | Positive normalized relation **and full linear span** imply ambient IP.                                                                                                                                                                                              | Positivity alone does not establish ambient interior.                                                                                         |
| `CircuitGeometry.lean`: `conical_caratheodory`                                                                 | A nonnegative conical combination has a linearly independent positive support.                                                                                                                                                                                       | This concerns nonzero conical representations, not an IP simplex's positive zero relation.                                                    |
| `CircuitGeometry.lean`: `projected_positive_circuit`, `projected_relation_coefficients`                        | Circuit representatives in a linear projection, and proportionality of every linear relation on an affinely independent projected simplex to its normalized relation.                                                                                                | Supplies quotient geometry and relation uniqueness.                                                                                           |
| `Minimal.lean`: `minimalIPGen_card_lower`, `simplex_card`                                                      | Ambient IP gives `n+1 <= S.card`; adding affine independence gives equality.                                                                                                                                                                                         | Lower bound does not need minimality.                                                                                                         |
| `Minimal.lean`: `simplex_weights`                                                                              | Affine independence and ambient IP give an enumeration by `Fin(n+1)` and positive normalized real zero relation.                                                                                                                                                     | The existential signature does not itself assert rationality, primitive integral normalization, or Newton-hull IP.                            |
| `Minimal.lean`: `minimal_structure`                                                                            | Minimal ambient-IP set that is not affinely independent yields `S'` proper, `T` contained, union equal to `S`, `IsMinimalIPRel S'`, `IsGoodSimplex T`, `S.card = S'.card + (n - finrank(vectorSpan S')) + 1`, and `finrank(vectorSpan T) <= finrank(vectorSpan S')`. | A proof body for the one-step KS statement is present. Relative rank equals the relevant dimension because origin lies in the component hull. |

`Minimal.lean` also contains maximal-dimension proper relative-IP selection and lifting lemmas used in its decomposition proof. The general upper cardinality bound is proved in `KS/SeedBound.lean`.

### A complete relation basis is stronger than circuit coverage

Define `A : R^S -> R^n` by `A(q) = sum q_x x`. `CircuitSpan.lean` defines `simplexCircuitWeights S` as normalized positive affine-simplex relations, extended by zero outside their support, and `vertexCombination S` as `A`.

`normalized_relation_mem_span_circuits` and `nonnegative_relation_mem_span_circuits` first place nonnegative relations in the real span of those circuits. `simplex_circuits_span_kernel` then proves, assuming intrinsic-IP,

`span(simplexCircuitWeights S) = ker(vertexCombination S)`.

The final step uses a strictly positive relation on all coordinates: any signed relation can be shifted by a sufficiently large multiple of it into the nonnegative cone, then recovered by subtraction. This is why coverage of coordinates by arbitrary rows is not a substitute for kernel spanning.

`CWSBasis.lean` packages `CircuitBasis S` with a finite row index `Fin B.k`, linearly independent rows, a positive-simplex witness for every row, and an equality between their span and the complete kernel. `exists_circuitBasis` assumes intrinsic-IP and returns `Nonempty (CircuitBasis S)` using an abstract independent-subset choice. It is not a computed basis extraction routine. `CircuitBasis.nonneg` gives nonnegative padded rows; `CircuitBasis.cover` shows every coordinate occurs positively somewhere; the converse is not asserted.

`CircuitBasis.pairing_range` identifies the range of the vertex-pairing matrix with the joint kernel of the selected row equations. `CircuitBasis.coordinate_count` assumes ambient IP and proves `S.card = n + B.k`. The latter theorem does not require minimality despite its descriptive comment. These are real linear-space statements. In [KS00] Lemma 4, page 10, the corresponding finest lattice statement additionally uses rational CWS data and the coarsest lattice generated by the vertices. Forward integrality or real bijectivity alone does not certify a two-sided unimodular lattice equivalence.

`CWSAssembly.lean` contains abstract finite row/CWS candidate sets and a **noncomputable** `cwsCatalogue`, followed by a noncomputable dimension catalogue. `finite_ws_cws_container_coverage` states that for each dimension and family of selection rules there is a bound and a finite catalogue supplying IP containers for every reflexive polytope. Its displayed map is a real linear bijection with a forward lattice-point preservation property and container inclusion. It does not claim a computable bound, the optimized 47-entry structure list, every lattice realization, exact normal forms, or a terminating native end-to-end enumeration. Its dimension bound uses a separate lattice-point-count theorem rather than the unported structural `2n` bound.

## The archived 47 entries and the concrete 46 CWS descriptors

`Profiles.lean` computes lists with atoms `[6,5,4,3,2]`, lengths one through five, a nonincreasing predicate, and `sum(m-1)=5`. Its `admissibleProfile_complete` requires membership in that explicit search space. It verifies the resulting seven arithmetic profiles; it does not derive those hypotheses from a real minimal polytope.

`Structures.lean` hard-codes four `known3` and nine `known4` bases. The primary small-dimensional classification is stated in [KS97] §4 and [KS00] §2.1, Lemma 3, page 9 (the four-dimensional structures refer to its appendix table). Those papers are the provenance for the intended bases, but the archived declarations contain no geometric interpretation theorem for them.

`extendTo5` extends the supplied bases with two new vertices for `n'=4`, or three for `n'=3`, attaches allowable old labels, and filters only with `lemma2Ok`. `rawFiveDimensionalStructures` adds the simplex. `canonical` permutes the simplex list, then relabels vertices in first-occurrence order; `insertStructure` removes duplicate resulting lists. The native-decide theorems verify that this defined computation has 47 entries, with vertex-count distribution `(6,1),(7,6),(8,25),(9,13),(10,2)`, and the stated profile counts. No type mentions real vertices, affine independence, intrinsic interior, or minimality.

Two concrete distinctions are essential:

1. **Geometric soundness:** numbered row 8 of `paper/minimal_polytopes_5d_table.tex` contains `{1,2}` and `{1,2,3,4}` as purported good-simplex supports, plus `{5,6,7,8}`. Proper containment of the first support in the second is impossible for two good simplices of the same vertex realization, by the barycentric uniqueness argument above. No-lonely passes because the first support has zero exclusive vertices and the larger block has two. Thus satisfying that filter does not supply geometric soundness. Other numbered entries likewise have contained blocks; they may be redundant equations or overgenerated candidates, but require separate interpretation rather than being called minimal-IP simplex decompositions.
2. **Unlabeled classification:** numbered rows 16, 17, and 18 have `{1,2}`, `{3,4,5}`, and `{j,6,7,8}` for `j=3,4,5`. Swapping labels 3 and 4 or 3 and 5 while keeping the other labels fixed identifies their support hypergraphs. First-occurrence relabeling without all internal vertex permutations does not prove canonicalization under arbitrary vertex relabeling. Consequently 47 is an exact count of this computation's output lists, not a verified count of distinct unlabeled overlap hypergraphs.

These are direct deductions from inspected table entries and definitions, not results attributed to KS. The archived paper's assertion that passing Lemma 2 is sufficient for a valid structure, and its claim that this canonicalization quotients exactly by vertex relabeling, need separate justification and are contradicted by the examples above under the stated good-simplex/unlabeled interpretations. This review does not alter the archival source or propose a replacement count.

The 47-entry list includes the single-weight simplex. The project's 46 genuine CWS descriptors omit that single-weight case; subtraction alone does not prove correspondence. Descriptor labels, overlaps, redundant rows, orderings, row independence, component input pools and accepted lattice realizations require a separate native-refinement argument. Neither source arithmetic nor a container theorem proves that concrete census complete beyond its declared input universe.

## Connection to the concrete classification

| Connection                           | Formal guarantee                                                                                                                                            |
| ------------------------------------ | ----------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Bounded minimal seed                 | `KS/SeedBound` combines minimal decomposition and relative-coordinate transport to prove $n+1\le N\le2n$.                                                   |
| Complete primitive circuit rows      | `KS/Circuits` preserves supports, weighted-zero relations, independence and full relation-kernel span.                                                      |
| Weight search and row assembly       | `KS/WeightSearch` and `KS/Assembly` enumerate every required ordered primitive IP row and bounded independent covering CWS.                                 |
| Compatible lattices and subpolytopes | `KS/Recovery`, `KS/Lattices` and `KS/Realizations` prove exact hull recovery, finite lattice choices and reconstruction.                                    |
| All reflexive targets                | `Refpoly.KS.classifies_reflexive` in `KS/Obligations` composes the executable routines into terminating coverage up to two-sided lattice equivalence.       |
| Additional promises                  | Ordered simplex-decomposition catalogues, no-lonely pruning, native descriptor refinement and exact deduplication are not supplied by the headline theorem. |

The concrete enumeration conservatively assembles bounded relation rows; it need not identify every output row family with a unique minimal-IP simplex decomposition. The archived 47-entry computation is not used as its completeness certificate. Nonreflexive one-point targets remain outside the theorem.
