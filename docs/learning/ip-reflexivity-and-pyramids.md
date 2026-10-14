# IP, reflexivity, polarity, and the pyramid boundary

This note records the approved T-L1–T-L3 literature review. The mathematical target is the roadmap's eventual complete enumeration, while the current delivery is the implemented descriptor/input-pool universe on the PALP reference lattice and the audited abstract port. The separate `SINGLE_WS_5d.tex` supplied reference leads and cross-checks; its prose and proof-status comments are not the specification. Only the reviewed frozen snapshot was read; no proof build or proof edit was performed.

## 1. Fix the lattice, interior, and equivalence first

Let M be a rank-d lattice, M_R its real extension, and N = Hom(M,Z) the dual lattice. A lattice polytope is the convex hull of finitely many points of M. Throughout, an IP polytope is full dimensional and contains the chosen origin in its ambient interior. A one-point polytope has exactly one interior lattice point. These predicates differ: [-2,2] in Z has IP and three interior lattice points. For a one-point polytope, translate its unique interior lattice point to zero before comparing by GL(d,Z); without this normalization, the natural unmarked equivalence is affine unimodular equivalence.

KS [K00], §2, printed pp. 5–6, defines the lattice, IP property, and polar separately. The origin in a lower-dimensional simplex belongs to its intrinsic interior in its span, not the ambient interior. The lower-dimensional simplices in the seed decomposition therefore require a relative-interior predicate. Positive simplex barycentric coordinates alone do not say that the _integer Newton hull_ constructed from a weight system has IP.

For a positive normalized rational weight vector q of length l, the exponent-space region is

`Q = {a ∈ R^l : a_i ≥ 0, Σ q_i a_i = 1}`,

with integer Newton hull `Δ(q) = conv(Q ∩ Z^l)` and distinguished point `1 = (1,…,1)`. Interior here is intrinsic to the equation hyperplane and relative to its affine lattice. After translation a=x+1, the region lies in `Σ q_i x_i=0`, `x_i≥−1`. For a full-dimensional Newton hull with 1 in its intrinsic interior, any interior lattice point has positive integer coordinates. Since each a_i≥1 and Σq_i=1 with all q_i>0, the equation forces a=1. Thus IP Newton hulls in this positive-weight construction are one-point polytopes; this does not make arbitrary IP polytopes one-point. For CWS, the analogous argument needs the equations to cover every coordinate by a positive coefficient; uncovered coordinates must not be silently admitted. Skarke [S96], §2, p. 3, gives this exponent-coordinate argument and explicitly acknowledges lattice ambiguity.

## 2. Polar duality: what remains integral

Use the sign convention

`P* = {y ∈ N_R : <y,x> ≥ −1 for every x ∈ P}`.

For compact convex full-dimensional P with 0 in its interior, $P^{*}$ is compact, convex, and IP, and $P^{**}$=P. For arbitrary sets P⊆Q, $Q^{*}$⊆$P^{*}$. The reverse implication holds when Q is closed, convex, and contains 0: dualizing again and using P⊆$P^{**}$ yields P⊆Q. These are real convex-geometric facts; integrality is additional.

A full-dimensional IP lattice polytope is reflexive precisely when $P^{*}$ is also a lattice polytope in the _dual_ lattice. Equivalently, every facet has primitive lattice distance one from 0. If the facet is `<u,x>≥−h` with u primitive in N and h a positive integer, its polar vertex is u/h. For h>1 this is rational and nonintegral. A rational polar is therefore well defined without being an object of the same lattice-polytope class.

Batyrev [B94], Definition 4.1.1, printed p. 17, defines polarity; Definitions 4.1.4–4.1.5, p. 18, define primitive integral distance and reflexive pairs. Theorem 4.1.6, p. 18, states that `(P,M)` reflexive implies `(P*,N)` reflexive. Proposition 4.1.7, p. 19, reverses face incidence and sends an l-face to a (d−l−1)-face. Its face equality uses pairing −1. The v1 proof of Theorem 4.1.6 has a sign inconsistency: it writes outward inequalities ≤1 and then identifies their normals, rather than their negatives, with vertices of the ≥−1 polar. Correct the normal orientation when reconstructing this proof; the theorem and defining convention agree after this correction.

Reflexivity implies a unique interior lattice point. An interior lattice point pairs strictly above −1 with every integral polar vertex, hence at least 0. If it were nonzero, polarity and bipolarity would put an unbounded positive ray in the compact primal polytope. The converse fails; Section 5 supplies an explicit one-point Newton hull with a distance-two facet.

## 3. Toric interpretation and its hypotheses

Batyrev [B94], Theorem 4.1.9, printed p. 19, assumes an n-dimensional integral polyhedron Δ, its projective toric variety P_Δ, and the family of projective **Δ-regular** hypersurfaces. It equates the Calabi–Yau property (with the canonical-singularity convention of Definition 4.1.8) with the associated ample sheaf being anticanonical, and with Δ having a unique interior lattice point m0 such that `(Δ−m0,M)` is reflexive. This is not a theorem about every polynomial supported in Δ. The regularity hypothesis cannot be omitted, and a generic toric Calabi–Yau hypersurface is not asserted smooth in every dimension.

Proposition 4.1.10 and Corollary 4.1.11, printed p. 20, identify the anticanonical polyhedra and recover the toric fan from cones over the faces of $\Delta^{*}$. The dual pair is the basis of Batyrev's mirror construction; no mirror-symmetry theorem for arbitrary nonreflexive one-point polytopes follows. None of these toric objects is represented in the inspected Lean definitions.

## 4. Minimality and the class being dualized

KS [K00], §2.1, printed p. 7, calls an IP polytope vertex-minimal if removing any vertex destroys IP. Its §2.3, printed p. 15, distinguishes lattice-point minimality, very minimality, and r-maximal reflexive polytopes with their r-minimal duals. A vertex-minimal IP polytope need not be reflexive. An r-minimal reflexive polytope can have a proper IP subpolytope that is nonreflexive. Neither predicate can replace the other.

On a fixed dual pair of lattices, reflexive polarity is an involution that reverses inclusion. Consequently P is inclusion-maximal **among reflexive lattice polytopes** iff $P^{*}$ is inclusion-minimal among reflexive lattice polytopes in the dual lattice. Real antitonicity alone proves no such assertion for a class not closed under polarity. The comparison also fixes embeddings and origins; it is not an inclusion relation between bare GL-equivalence classes.

KS [K00], §2.3, printed pp. 13–15, starts with reflexive targets, constructs CWS containers on the finest lattice, and requires suitable sublattices and subpolytopes. Lemma 7 assumes an integral vertex pairing matrix and describes the intermediate-lattice recovery; a rational pairing matrix does not meet its hypothesis. A nonreflexive IP container can still contain reflexive targets. This is different from a claim that all nonreflexive one-point targets are covered by the same completeness argument: their rational polars need not admit integral minimal seeds in the fixed dual lattice. Clearing denominators changes lattice data and primitive distances and requires a new correctness/completeness argument. Rational polarity does not supply the missing argument, lattice enumeration, termination, or output guarantees.

## 5. Skarke's theorem and two distinct counterexamples

### The actual low-dimensional theorem

Skarke [S96], §3, **Lemma 1**, printed pp. 6–7, assumes an integer pyramid of lattice height h≥2 in a rank-four lattice, and its double-height pyramid with the same apex and shape. The double contains a lattice point outside the original pyramid and outside the doubled base. The remarks on p. 7 extend the proof to dimensions below four and give a failure in dimension five.

The **unnumbered Theorem** on p. 7 states that maximal Newton polyhedra of dimension at most four with 1 in their interior are reflexive. “Maximal Newton” means the entire integer hull of the prescribed nonnegative exponent region; it does not mean inclusion-maximal among all reflexive polytopes. The proof uses this full integer hull: the doubling lies in the region with exponent coordinates ≥−1, and a lattice point away from the doubled base has nonnegative coordinates and hence belongs to the original Newton hull. The following remark includes constructions defined by several weight equations. KS [K00], Lemma 8, printed p. 15, restates the conclusion as `n≤4` and `Δ(q)` IP implies reflexive. Neither statement is “every IP lattice polytope of dimension ≤4 is reflexive.”

There is a normalization obligation in the source-to-Lean bridge. Skarke triangulates the base and writes its vertices as 0,2e1,2e2,2e3. An arbitrary lattice tetrahedron is not necessarily unimodular: a real change making its edges coordinate vectors need not preserve the ambient lattice or keep the apex coordinates integral. The frozen theorem checks only the displayed normalized family. A proof of the general reduction must track the transformed lattice or supply additional lattice geometry; the comment asserting a general GL(4,Z) reduction is not itself that proof. This note does not conclude that Skarke's stated lemma is false; it identifies an obligation not discharged by the inspected theorem type.

### The five-dimensional height example

The doubled pyramid has base `conv(0,2e1,2e2,2e3,2e4)` at height zero and apex `(2,2,2,2,4)`. Its doubled height is 4; the associated original height parameter is h=2. At height 1 its first four coordinates are each at least 1/2. A lattice point must therefore have each at least 1. The cap inequality

`(x1+x2+x3+x4)/2 − 3x5/4 ≤ 1`

would then give 5/4≤1, a contradiction. This is exactly a failure of the intermediate-height lemma. The pyramid has 0 on its boundary and is not an IP weight system. The failure of one sufficient proof mechanism does not logically imply that a nonreflexive IP weight system exists.

### An actual nonreflexive IP weight system: an independent derivation

Take weights `(1,1,1,1,1,2)` and total degree 7. Write t=a6. Its complete integer Newton hull is

`H = {a_i≥0 : a1+…+a5+2t=7, 0≤t≤3}`.

To verify equality, all integer exponent solutions lie in H. Conversely H is the convex hull of the two endpoint simplices: their vertices are `7e_i` at t=0 and `e_i+3e6` at t=3, for i=1,…,5. These ten vertices are integer solutions, so the complete hull is H. Its dimension is five, and 1 has all coordinates positive and 0<t<3, hence belongs to its intrinsic interior. Every interior integer point has all six coordinates ≥1; the degree equation then forces every coordinate to equal 1. Thus H is one-point.

The face t=3 is a four-dimensional facet. On the translated rank-five lattice

`L = {x∈Z^6 : x1+…+x5+2x6=0}`,

the coordinate functional x6 is primitive: `e6−2e1∈L` has value 1. That facet has lattice distance `3−1=2` from the interior point. The corresponding polar vertex is `−x6/2` as a functional on L and is nonintegral because it takes value −1/2 on `e6−2e1`. Hence this IP Newton hull is not reflexive.

This is an explicit original exact-arithmetic derivation, independently inspectable from the displayed vertices and primitive lattice vector. It is not attributed to Skarke, is not a checked Lean theorem, and is not a claim about census counts. It separately establishes the existence which the height example alone cannot establish.

## 6. Frozen Lean support: actual types, not comments

Read-only snapshot: `/home/ahat01/.copilot/session-state/83ef73b2-4517-446c-9ac2-9437ff014965/files/refpoly-proof-snapshot/proofs/Refpoly/`. The table records inspected declarations and their scope; it is not a new clean-build or axiom audit.

| File / declaration                                                                    | Actual scope                                                                                                              | Boundary                                                                                                |
| ------------------------------------------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------- | ------------------------------------------------------------------------------------------------------- |
| `Basic.lean`: `V`, `IsLatticePoint`, `IsLatticePolytope`                              | `V n = Fin n → R`; coordinate integrality; finite lattice generating hull                                                 | A chosen Z^n model, not an arbitrary embedded lattice with a supplied basis                             |
| `Basic.lean`: `IsIP`                                                                  | `0 ∈ interior P`                                                                                                          | Ambient interior; no one-point count                                                                    |
| `Basic.lean`: `IsReflexive`                                                           | Lattice polytope, IP, and polar lattice polytope                                                                          | Definition uses dual integrality; primitive facet-distance equivalence not supplied by this declaration |
| `Basic.lean`: `polarDual_antitone`                                                    | `P⊆Q → polarDual Q⊆polarDual P` for arbitrary sets                                                                        | No lattice premise or class closure                                                                     |
| `Basic.lean`: `bipolar`                                                               | Convex P, closed P, `0∈P` imply $P^{**}$=P                                                                                | Origin need not be interior; no polytope premise required                                               |
| `Basic.lean`: `interior_latticePoint_unique`                                          | `IsReflexive P → interior P ∩ latticePoints n = {0}`                                                                      | No converse                                                                                             |
| `Basic.lean`: `IsUnimodular`, `GLEquiv`                                               | Real linear equivalence preserving integers forwards **and backwards**; exact image equality                              | Forward integrality alone is insufficient; no translation parameter                                     |
| `ReflexiveDuality.lean`: `IsReflexive.bipolar_eq`, `IsReflexive.polarDual_reflexive`  | Reflexive P has $P^{**}$=P and reflexive $P^{*}$                                                                          | Assumes the primal reflexivity structure; does not prove all IP objects reflexive                       |
| `MaxMin.lean`: `subset_iff_polarDual_subset`                                          | Closed convex Q containing 0 gives `P⊆Q ↔ Q*⊆P*`                                                                         | A real order statement                                                                                  |
| `MaxMin.lean`: `IsMaximalReflexive`, `IsMinimalReflexive`, `maximal_iff_dual_minimal` | Quantifies over reflexive sets on the same Z^n model; equivalence assumes reflexive P                                     | Does not apply to all one-point polytopes                                                               |
| `MaxMin.lean`: `IsMinimalIPGen`                                                       | A finite generating set whose hull is ambient IP and every proper subset fails IP                                         | Distinct from r-minimality; no lattice condition in the predicate                                       |
| `Minimal.lean`: `IsGoodSimplex`, `IsMinimalIPRel`                                     | Affine independence and intrinsic interior; relative minimality tested in the fixed parent span                           | A correct distinction from ambient IP                                                                   |
| `PyramidLemma.lean`: `pyramid_lemma_dim_le_four`                                      | For integers x,y,z,h with h≥2, produces an integral p in **V 4**, in `conv(0,2e1,2e2,2e3,(2x,2y,2z,2h))`, with `1≤p4≤h−1` | Not dimension-parametric and no arbitrary-base/lattice normalization theorem in this type               |
| `PyramidLemma.lean`: `pyramid_counterexample_dim_five`                                | Integral m in `conv(pyramid5)` implies `m5≠1`                                                                             | Height obstruction only; no WS, IP, or nonreflexivity conclusion                                        |

The normalized pyramid existence and counterexample have proof bodies in the snapshot. The actual low-dimensional integer Newton-hull reflexivity theorem is deliberately not declared in `PyramidLemma.lean`. Its facet-distance and relative-lattice infrastructure remains a separate obligation. Existing comments saying that the height example makes automatic WS reflexivity fail are broader than the counterexample's type.

## 7. Computational consequences and gaps

| Obligation                                                      | Evidence                                                              | Remaining boundary                                                                          |
| --------------------------------------------------------------- | --------------------------------------------------------------------- | ------------------------------------------------------------------------------------------- |
| Separate IP from reflexivity in dimension five                  | Explicit WS derivation above; low-dimensional source theorem has n≤4  | Native tests must use the actual lattice and separate accepted IP/reflexive counts          |
| Recover all reflexive targets from CWS containers               | [K00] §2.3 requires suitable sublattices and integer-VPM subpolytopes | The implemented descriptor catalogue/reference lattice is not global completeness           |
| Cover all nonreflexive one-point targets                        | Real polarity exists but may be nonintegral                           | Need a separate theorem or rational-dual extension preserving lattice data                  |
| Transfer Skarke's pyramid theorem to the normalized Lean family | [S96] §3 and inspected exact type                                     | General base normalization/lattice tracking and lower-dimensional specialization need proof |
| Automatic reflexivity of n≤4 IP Newton hulls                    | [S96] unnumbered theorem; [K00] Lemma 8                               | Not present as a theorem in the inspected pyramid file                                      |
| Toric interpretation                                            | [B94] Theorem 4.1.9 with Δ-regularity                                 | No toric formalization in these declarations                                                |
| Minimal/maximal equivalence                                     | Frozen `maximal_iff_dual_minimal`                                     | Class and lattice fixed; vertex-minimal seeds are a different predicate                     |
| Source-to-proof confidence                                      | Exact source versions and declaration bodies inspected                | T-A0 statement comparison and existing build/axiom evidence must supply formal validation   |

PALP's accepted CWS rows and exact normal forms are consistency regression evidence. They do not certify the missing coverage or lattice-realization steps. No numerical speedup, census, or independent completeness certificate is claimed here.

## 8. Bibliography and cached source evidence

All page references above are **printed preprint page numbers**, not journal pagination. Cached bytes are local ignored artifacts; notes retain retrieval information. PDFs and available TeX were read from the approved cache. SHA-256 values identify the original downloads.

- **[B94]** Victor V. Batyrev, _Dual Polyhedra and Mirror Symmetry for Calabi-Yau Hypersurfaces in Toric Varieties_, Journal of Algebraic Geometry **3** (1994), 493–535. [Primary arXiv record](https://arxiv.org/abs/alg-geom/9310003), [v1 PDF](https://arxiv.org/pdf/alg-geom/9310003v1), [v1 author source](https://arxiv.org/src/alg-geom/9310003v1). Version v1, 5 October 1993; arXiv DOI `10.48550/arXiv.alg-geom/9310003` (not asserted as a journal DOI). Cache `sources/batyrev-1994/v1/paper.pdf`, SHA-256 `ebcd9bc32414d1a9f4b52f0256d78144e371ee7bedc477387dcad017077cad10`; original gzip author source `source.download`, SHA-256 `3e724a0c2b93b3fb69c2b9cc944b0b187b5ab558bd721281b4776fc8d6fe2e38`. `tex/paper.tex` is supplementary gzip extraction. Retrieved 2026-10-14; provenance in the same directory. The license grant beyond arXiv distribution was not independently verified.
- **[S96]** Harald Skarke, _Weight systems for toric Calabi–Yau varieties and reflexivity of Newton polyhedra_, Modern Physics Letters A **11** (1996), 1637–1652, DOI [10.1142/S0217732396001636](https://doi.org/10.1142/S0217732396001636). [Primary v1](https://arxiv.org/abs/alg-geom/9603007v1), 7 March 1996. Read shared cache `sources/skarke-alg-geom-9603007/v1/paper.pdf`, SHA-256 `ee4357a5eadd242e9689546a7991a9e5c291bb7b4627674e59865a7e80698800`; provenance records retrieval 2026-10-14. §2 pp. 3–4 for exponent lattice and hull IP; §3 pp. 6–7 for Lemma 1 and the unnumbered reflexivity theorem. Shared author-source retrieval belongs to the integrator; no duplicate download was made.
- **[K00]** Maximilian Kreuzer and Harald Skarke, _Reflexive polyhedra, weights and toric Calabi–Yau fibrations_, Reviews in Mathematical Physics **14(4)** (April 2002), 343–374, DOI [10.1142/S0129055X0200120X](https://doi.org/10.1142/S0129055X0200120X). Journal volume/pages verified in the primary arXiv metadata; DOI/issue/date verified against the primary Crossref record by the CWS researcher and integrator. [Primary arXiv record](https://arxiv.org/abs/math/0001106), [v1 PDF](https://arxiv.org/pdf/math/0001106v1), v1, 19 January 2000. Read shared cache `sources/kreuzer-skarke-math-0001106/v1/paper.pdf`, SHA-256 `7d400220d9bb4f0e0e2a121d46d75a7aad0b0a98ebbc1d6ad45e33407d0b01c1`; provenance records retrieval 2026-10-14. §2 pp. 5–6 definitions; §2.1 p. 7 minimality; §2.3 pp. 13–15 lattice recovery and refined minimality. This note cites the checked preprint edition; publisher full text was not read and the journal DOI does not identify a separately checked edition.

Source-evidence checklist: assigned cache checked before fetching; original Batyrev PDF and gzip source retained with hashes; shared Skarke/KS PDFs reused and their hashes checked; actual source statements and surrounding arguments read; exact frozen definitions and theorem types inspected; original WS example kept separate from source and Lean claims; no shared-document, proof, code, or sibling-checkout edits and no commits performed.
