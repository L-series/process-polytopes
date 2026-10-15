# Finiteness, volume bounds, and reduction to a finite lattice box

This note concerns full-dimensional lattice polytopes in a fixed lattice,
first with a fixed positive number of interior lattice points, then with the
additional reflexivity hypothesis. The supporting Lean declarations provide
abstract volume and box results. The [concrete KS endpoint](../../proofs/STATEMENTS.md)
uses a separately proved executable construction; neither generic theorem is
a native descriptor census.

## Definitions and equivalence conventions

Let the dimension be `n ≥ 1`, let `M ≅ Z^n`, and let `P` be the convex hull of
finitely many points of `M`, with affine span `M_R`. Put

$$
k(P)=\#(\operatorname{int}P\cap M),\qquad
J(P)=\#(P\cap M).
$$

Here interior is ambient interior in `M_R`. For a lower-dimensional polytope,
one must instead specify its intrinsic lattice and relative interior before
applying a theorem in that smaller dimension. Ambient interior is empty for a
proper affine subspace.

The **IP property** means `0 ∈ int P`. It specifies an interior origin and
implies `k(P) ≥ 1`; it does not imply `k(P) = 1`. For example,
`[-r,r]^n`, with positive integral `r`, has the IP property and unbounded
volume as `r` grows. A **one-point lattice polytope** has exactly one interior
lattice point. Translating that point to zero makes it IP. A **reflexive
polytope** is an IP lattice polytope whose polar is a lattice polytope in the
dual lattice. It is a one-point lattice polytope, but the converse need not
hold.

Unmarked polytopes are compared by affine lattice isomorphisms
`x ↦ Ux + t`, with `U ∈ GL(n,Z)` and `t ∈ Z^n`. Origin-marked polytopes
are compared by `x ↦ Ux`. For `k = 1`, every affine equivalence sends the
unique interior lattice point to the unique such point; after centering both
polytopes it has zero translation. For `k > 1`, there is no distinguished
interior point. Marking one gives at most `k` possibilities per affine class,
so marked GL finiteness follows, but the marked and unmarked classifications
are different. Arbitrarily translating polytopes without requiring an
interior point at zero does not produce a finite GL classification.

Volume below is ordinary Euclidean volume in lattice coordinates: a fundamental
parallelepiped has volume one, and the standard lattice simplex has volume
`1/n!`. For a lattice of covolume `c`, first divide Euclidean volume by `c`.
The integer-valued normalized volume convention is
`Vol_Z(P) = n! vol(P)`. These conventions differ by `n!`, which must be
carried through both the volume and box bounds.

## Verified primary results

### Hensley's positive-interior-point bound

Douglas Hensley, _Lattice vertex polytopes with interior lattice points_,
**Pacific Journal of Mathematics 105** (1) (1983), 183–191,
[publisher record](https://msp.org/pjm/1983/105-1/p10.xhtml),
[journal PDF](https://msp.org/pjm/1983/105-1/pjm-v105-n1-p10-s.pdf),
[DOI 10.2140/pjm.1983.105.183](https://doi.org/10.2140/pjm.1983.105.183).
The article byline is Douglas Hensley; the cover/index and publisher record
give Douglas Austin Hensley. The scanned issue says September 1983, whereas
the current publisher record says 1 March 1983. The year, volume, issue,
pagination and DOI are verified; no resolution of this month discrepancy is
assumed.

Hensley constructs positive constants `ε(k,n)` in §2, pp. 184–186: Lemma 2.3
treats `k = 1`, and Lemma 2.4 treats `k ≥ 2`. Theorem 3.1, p. 186, bounds
the barycentric coordinates of interior lattice points of a lattice simplex
away from zero using these constants. Corollary 3.2, pp. 186–187, transfers
the relevant coefficient bound to a lattice pyramid. Lemma 3.3, p. 187,
explains why a simplex's relative interior lies in a containing convex body's
ambient interior when their relative interiors meet there; relative and
ambient interiors are not being identified unconditionally.

Theorem 3.4, p. 187, gives for a lattice simplex `S` with exactly `k ≥ 1`
interior lattice points

$$
\operatorname{vol}(S)\le \frac{k}{n!}\varepsilon(k,n)^{-n}.
$$

Theorem 3.6, p. 188, applies to **any convex lattice polytope** `P` in
`R^n` with `k = #(int P ∩ Z^n) ≥ 1` and gives

$$
\operatorname{vol}(P)\le k\varepsilon(k,n)^{-n}.
$$

Corollary 3.7 on that same page gives

$$
J(P)\le n+k\,n!\,\varepsilon(k,n)^{-n}.
$$

The general-polytope argument controls asymmetry about an interior lattice
point and invokes a generalized Mahler bound; it is not just the simplex
formula with the same factorial. The scanned statements include `k = 1`.
OCR repeatedly renders `≥` as `>` or `<`; the hypotheses, inequalities and
factorials above were checked against the scanned page. No sharp estimate
or later resolution of Hensley's conjectures is needed here.

### Lagarias–Ziegler: explicit volume and box bounds

Jeffrey C. Lagarias and Günter M. Ziegler, _Bounds for Lattice Polytopes
Containing a Fixed Number of Interior Points in a Sublattice_,
**Canadian Journal of Mathematics 43** (5) (1991), 1022–1035,
[publisher record](https://www.cambridge.org/core/journals/canadian-journal-of-mathematics/article/bounds-for-lattice-polytopes-containing-a-fixed-number-of-interior-points-in-a-sublattice/0D4E5B45AB64DC6069A9BD7DD45B450C),
[DOI 10.4153/CJM-1991-058-4](https://doi.org/10.4153/CJM-1991-058-4).
Print issue date is 1 October 1991; Cambridge's online publication date,
20 November 2018, is not the paper's research publication year.

Use `s ≥ 1` for their integer sublattice multiplier, called `d` in the paper,
to avoid confusing it with dimension. Theorem 1, p. 1023, assumes lattice
vertices in `Z^n` and exactly `k ≥ 1` interior points belonging to `s Z^n`.
Its formulas (1.1)–(1.2) are

$$
V(n,k,s)\le k s^n\,[7(ks+1)]^{\,n2^{n+1}},
\qquad
J(n,k,s)\le n+n! k s^n\,[7(ks+1)]^{\,n2^{n+1}}.
$$

`J` counts **all** `Z^n` points, not only sublattice points. The exponent is
`n · 2^(n+1)`, and the base starts with `7`, not an OCR letter `l`. Both
were visually verified on p. 1023. With `s = 1`, define the explicit integer
bound

$$
C(n,k)=k[7(k+1)]^{\,n2^{n+1}}.
$$

Thus every full-dimensional lattice polytope with exactly `k ≥ 1` interior
lattice points has Euclidean volume at most `C(n,k)`, normalized volume at
most `n! C(n,k)`, and total lattice-point count at most `n+n! C(n,k)`.
The proof is in §2, pp. 1025–1031: one-sided Diophantine approximation,
barycentric/asymmetry bounds, then a convex-body volume argument. This is
an effective bound, without a practicality claim.

Theorem 2, p. 1024, states that a lattice polytope of volume at most `V`
can be sent by a **linear** integral unimodular transformation into a lattice
cube of side at most `n · n! · V`. The cube need not be based at the origin.
The paragraph immediately after the theorem uses an **integer translation**
to put the cube in `0 ≤ x_i ≤ n · n! · V` and obtains finite affine integral
equivalence classes. If preserving `s Z^n` is required, it uses translations
in `s Z^n` and the enlarged bound `n · n! · V + s`. The affine group and its
sublattice-preserving subgroup are defined earlier on p. 1024.

The theorem's proof in §3, p. 1032, reduces a lattice simplex by Hermite
normal form and then covers a general polytope using a maximum-volume
simplex. Theorem 3(a), p. 1025, for a closed bounded convex body `K` with
nonempty interior and any maximum-volume contained simplex `S`, centroid
`b`, gives

$$
K\subseteq -nS+(n+1)b,
\qquad K\subseteq (n+2)S-(n+1)b.
$$

Theorem 3(b) allows `S` to use vertices of a polytope `K`; when `K` is
lattice, the two enlarged simplices are lattice because `(n+1)b` is a sum
of lattice vertices. Proof: §4, pp. 1032–1034. This maximum-volume simplex
is not an inclusion-maximal member of the fixed-`k` target class.

The sublattice extension (1.3)–(1.4), pp. 1023–1024, is for a fixed
full-rank sublattice `Λ ⊆ Z^n`. Its reduction uses an integral map with
determinant `s^n/det Λ`; that map need not be unimodular. A change of lattice
coordinates for deriving a volume bound is therefore not itself an equivalence
witness for a fixed-lattice classification.

## Consequences for termination and maximal containers

These paragraphs are deductions from the verified results, not additional
numbered theorems in the papers.

For fixed `(n,k)` with `k ≥ 1`, put `L=n · n! · C(n,k)`. There are finitely
many lattice points in `[0,L]^n`. Enumerating their subsets, taking convex
hulls, retaining full dimension and testing the exact interior count produces
a terminating finite list covering every affine class. The list has duplicates
until an exact equivalence procedure is supplied. Exact rational polytope
geometry can implement these predicates, but neither paper provides a proof
that this particular repository implements that enumeration.

If `0 ∈ int P`, the _linear_ map in Theorem 2 preserves zero. A cube
containing its image contains zero, so each coordinate of that cube lies
within `[-L,L]`. Therefore the same explicit `L` gives a centered box without
adding a translation to the origin-marked object. This applies to every
marked fixed-`k` polytope, not just reflexive ones. With integer normalized
volume bound `B`, the corresponding cube side is `n B`, not `n · n! · B`.

The positive-count restriction is essential. For `n ≥ 2`, boxes
`[0,m] × [0,1]^(n-1)` have no interior lattice points and volume `m`.
Their distinct volumes preclude affine equivalence. Hence there is no general
finite classification at `k = 0` in these dimensions by this route. In
dimension one, an empty-interior lattice interval has length one up to affine
equivalence; this exception does not salvage a uniform all-dimension claim.
Nor is the union over all positive `k` a finite list for fixed dimension.

Fix a class `C_{n,k}` of full-dimensional lattice polytopes on the same
lattice, with exactly `k` interior points; optionally require a marked origin.
The uniform bound on `J` proves that every `P` in this class has an
**inclusion-maximal superset within this class**: maximize the integer `J(Q)`
among members containing `P`. The attainable values form a nonempty bounded
set of natural numbers. If lattice polytopes `Q ⊊ R`, then
`J(Q) < J(R)`, because each is the convex hull of its lattice points and
equal point sets would give equal polytopes. A maximizer therefore admits no
proper class-preserving extension. This argument uses a numerical invariant
on actual embeddings; it does not order arbitrarily chosen class representatives
by literal inclusion.

There are only finitely many equivalence classes of these maximal objects.
Choose representatives `M_1,…,M_t`. The correct covering statement is
`g(P) ⊆ M_i` for some allowed equivalence `g` and some `i`. It is generally
false that every original `P` is literally contained in one of the fixed
representatives without a change of coordinates. For unmarked objects `g`
is affine; for centered reflexive or marked objects it is origin-fixed GL.

Every lattice subpolytope of a bounded lattice container `M_i` is generated
by a subset of the finite set `M_i ∩ M`. Thus finite subpolytope enumeration
can recover all targets, once the covering containers have actually been
constructed. It must allow **all lattice points** of the container as
generators. Subsets of only the container's vertices need not recover a
subpolytope with vertices at other lattice points. Container existence does
not identify the KS WS/CWS construction or supply an executable list of
containers.

## Supporting formal volume and box statements

Paths are relative to `proofs/Refpoly/`; [SOURCES.md](../../proofs/SOURCES.md)
identifies the original editions and hashes. Names below are in namespace
`Refpoly`, except explicitly qualified subnamespaces. Line locations identify
the original source edition, not the current checkout.

| File/declaration                                                                                      | Actual supported statement and limit                                                                                                                                                                                                                                                 |
| ----------------------------------------------------------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| `Basic.lean`, `IsIP`, `IsReflexive`, `IsUnimodular`, `GLEquiv`                                        | `IsIP` uses ambient `interior`; reflexivity requires lattice primal and polar. `GLEquiv` is a real linear equivalence with **both** forward and inverse lattice preservation. It has no translation parameter.                                                                       |
| `Basic.lean`, `interior_latticePoint_unique`                                                          | `IsReflexive P → interior P ∩ latticePoints n = {0}`. This bridges reflexivity to `k=1`; it does not classify all one-point polytopes as reflexive.                                                                                                                                  |
| `BoxReduction.lean:295`, `BoxReduction.box_of_volume_bound`                                           | For any `n` and real `C`, there exists a natural radius `R`; every lattice polytope with origin in its interior and `(volume P).toReal ≤ C` has a `GLEquiv` image in `[-R,R]^n`. This is general conditional box reduction, including dimension zero, but its radius is existential. |
| `ReflexiveVolume.lean:112`, `ReflexiveVolume.volume_bound`; `Finiteness.lean`, `hensley_volume_bound` | `∃ C : R, ∀ P, IsReflexive P → (volume P).toReal ≤ C`. The name does **not** mean the general fixed-`k` Hensley theorem is formalized, or that its numerical estimate is supplied.                                                                                                   |
| `Finiteness.lean`, `lagarias_ziegler_box`, `latticePoint_count_bound`                                 | Existential dimension-dependent centered radius and total lattice-point bound, specialized to reflexive polytopes.                                                                                                                                                                   |
| `Finiteness.lean`, `finitely_many_reflexive`                                                          | `∃ T, T.Finite ∧ ∀ P, IsReflexive P → ∃ Q ∈ T, GLEquiv P Q`. The proof takes **all lattice polytopes in a box** as `T`. The type neither requires every `Q ∈ T` to be reflexive nor promises one representative per class.                                                           |
| `Finiteness.lean`, `ncard_lt_of_ssubset`                                                              | Strict inclusion of two lattice polytopes strictly increases their total lattice-point `ncard`; no reflexivity needed for this lemma.                                                                                                                                                |
| `MaxMin.lean`, `IsMaximalReflexive`, `exists_maximal_superset`                                        | Maximality is under literal inclusion on the same ambient lattice. For reflexive `P`, obtains `∃ Δ, P ⊆ Δ ∧ IsMaximalReflexive Δ`. Its proof maximizes the bounded point count. This is not an executable enumeration of maximal representatives.                                    |
| `MaxMin.lean`, `IsMinimalReflexive`, `maximal_iff_dual_minimal`, `exists_minimal_subset`              | Inclusion-minimal **reflexive** objects and their polarity correspondence. These types do not extend this correspondence to general nonreflexive one-point objects.                                                                                                                  |
| `MaxMin.lean`, `IsMinimalIPGen`, `exists_minimalIPGen_subset`                                         | Minimality of a finite **generating set** under deletion while preserving origin interior. This differs from inclusion-minimal reflexivity.                                                                                                                                          |

The conditional box proof uses a maximum-determinant generator tuple, bounds
its determinant by volume via an anchored scaled parallelepiped, proves that
only finitely many row lattices have bounded nonzero determinant, and chooses
one representative matrix per row lattice. `bounded_matrix_reduction` uses
classical finite choices. Thus supplying an explicit input `C` does not turn
its conclusion into the LZ numerical radius or into code computing `R`.
Compactness ensures the volume is finite before the conversions from extended
nonnegative reals to `toReal` are used.

The frozen reflexive volume proof uses a different route from Hensley's
general theorem:

1. `WeightSystemFinite.normalized_ip_weight_systems_finite` proves finiteness
   of normalized IP weights from finite positive branching and
   `algorithm_complete` in `Algorithm.lean`. `positiveSelection` is noncomputable.
   Its import chain starts with `Algorithm → WeightSystem → Basic`, rather
   than with `Finiteness` or a volume bound.
2. `ReflexiveVolume.uniform_weight_lower_bound` takes a common positive
   lower bound for all coefficients of those finite weight sets with
   supports of size at most `n+1`.
3. `dual_point_ip_circuit` obtains a positive simplex circuit through each
   dual generator. `reflexive_simplex_block_ip` in `GeometricCWS.lean`
   uses the integral primal/dual pairings and a surjective open real-linear
   map to prove **integer Newton-hull IP** in `wsHyperplane q`.
   Positivity of simplex coefficients alone would only establish simplex
   relative interior. The map's forward integral pairing property is not a
   proof that it is a lattice isomorphism onto every hyperplane lattice.
4. `dual_generator_pairing_bound` bounds pairings above by `ε⁻¹`; polar
   inequalities bound them below by `-1`. Independent integral dual normals
   send `P` into a fixed box. Their determinant has absolute value at least
   one, which suffices to bound the **volume** of `P`; this map need not be
   unimodular.
5. The separate conditional box theorem then supplies actual GL representatives;
   box counting gives finiteness and terminates ascending reflexive chains.

The inspected dependency route avoids using reflexive-polytope finiteness to
prove the finite weights from which its volume bound is derived. One must
preserve that direction: replacing weight finiteness by an argument that
assumes the same polytope finiteness would create a circular justification.
The compiler-derived dependency and axiom checks preserve that direction.

## Role in the concrete classification

| Result                                 | Scope and connection                                                                                                                                                                       |
| -------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| Fixed `(n,k≥1)` volume/count bounds    | Hensley and LZ treat the general positive-interior-count problem. The supporting Lean volume theorem is reflexive-specific; the broader theorem is not needed for the classifier's target. |
| Equivalence and marking                | LZ uses affine lattice maps. The formal classifier fixes the origin and proves two-sided integral `GLEquiv`; these conventions must not be conflated for general `k>1`.                    |
| Effective boxes                        | The supporting box radii are existential. Executable `KS/IntegerPoints` derives its own bounds from CWS weights and a certified kernel basis, not an unevaluated LZ radius.                |
| Maximal containers                     | Abstract maximality existence does not construct a KS container. `KS/Containers` supplies explicit seed-pairing witnesses; its containers need not be maximal.                             |
| Subpolytope and lattice recovery       | `KS/Recovery` proves exact hull recovery from enumerated container points; `KS/Realizations` exhausts compatible lattices.                                                                 |
| Polarity for broader one-point targets | A nonreflexive one-point polytope may have a rational polar. Such final targets are outside the theorem.                                                                                   |
| WS/CWS coverage                        | `KS/WeightSearch`, `KS/Assembly` and the composition theorem supply the concrete terminating reflexive enumeration. Native pruning and descriptor refinement are separate guarantees.      |
| Unique representatives                 | Output duplicates are permitted. Exact normal forms and collision-safe class merging are not implied by volume or point-count bounds.                                                      |

## Source evidence and reusable provenance

Both original publisher PDFs were downloaded, read through their relevant
proof sections, and retained without byte changes in the ignored cache.
Mathematical formulas were checked on embedded scanned pages after observing
OCR errors. Each directory contains `provenance.json` with primary URL,
edition, retrieval date **2026-10-14**, copyright observations and SHA-256.
Derived `paper.txt` files are reading aids, not original source editions.
No author TeX source was linked by the consulted publisher records; none is
claimed to have been cached.

| Source/version                                               | Cache path relative to `docs/learning`            | Original PDF SHA-256                                               |
| ------------------------------------------------------------ | ------------------------------------------------- | ------------------------------------------------------------------ |
| Hensley 1983, published journal scan                         | `sources/hensley-1983/journal/paper.pdf`          | `db6d885105160df9c53be90f4e5f468d25a36319dccef69dc5dfbfe5813e271c` |
| Lagarias–Ziegler 1991, published journal scan, publisher PDF | `sources/lagarias-ziegler-1991/journal/paper.pdf` | `5d3b5a7c0bc2616eb08b176c569c46cbb868f6c7e9e29f9185038fe75698ac63` |

The LZ PDF download URL is the publisher's
[journal PDF](https://www.cambridge.org/core/services/aop-cambridge-core/content/view/0D4E5B45AB64DC6069A9BD7DD45B450C/S0008414X00007483a.pdf/bounds-for-lattice-polytopes-containing-a-fixed-number-of-interior-points-in-a-sublattice.pdf).
The cached Hensley publisher HTML record additionally verifies the DOI;
its original-byte hash is recorded in the local provenance file. Cached
literature is not an input to the Lean build.

The main inspected Lean file hashes identify the source evidence independently
of declaration names:

| Snapshot file in `Refpoly/` | SHA-256                                                            |
| --------------------------- | ------------------------------------------------------------------ |
| `Basic.lean`                | `a716a1508842b4f4ed1b24eaf978d43758efbdb87990b4a64e80c8795089dfc4` |
| `BoxReduction.lean`         | `85e9dbaef06e9702267cd8cf5f88b1b7339f026177f1cfde359e93291dacebfd` |
| `ReflexiveVolume.lean`      | `23d54cf6206a41b10bfaf206fde3e67bf532fb2c06207f785e3da3124781c589` |
| `Finiteness.lean`           | `0152ac3597310ff9783b30665faf7fc49c4ddf3e3ad8af5bc83b9f6c02a222af` |
| `MaxMin.lean`               | `cf4bf17f61a683564371093226ea904da42a5be6f9d0cf07183e0e826a74b6ef` |
