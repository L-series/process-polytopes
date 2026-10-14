# Weight enumeration, positive selection, and degree bounds

This T-L1–T-L3 literature note follows the weight-system part of the proposed
terminating, complete KS classification in every fixed dimension. Weight
enumeration is one ingredient: CWS assembly, required lattice realizations,
subpolytope search and exact equivalence still need their own coverage proofs.
The papers reviewed here target the reflexive construction. Their nonreflexive
Newton polytopes do not establish coverage of all nonreflexive one-point lattice
polytopes.

## Primary sources and conventions

The main sources actually read are:

- Harald Skarke, _Weight systems for toric Calabi–Yau varieties and reflexivity
  of Newton polyhedra_, **Modern Physics Letters A 11** (1996), 1637–1652,
  [DOI 10.1142/S0217732396001636](https://doi.org/10.1142/S0217732396001636).
  Consulted edition: [arXiv:alg-geom/9603007v1](https://arxiv.org/abs/alg-geom/9603007v1),
  7 March 1996. Locations below use its printed preprint page numbers.
- Friedrich Schöller and Harald Skarke, _All Weight Systems for Calabi–Yau
  Fourfolds from Reflexive Polyhedra_, **Communications in Mathematical Physics
  372** (2019), 657–678,
  [publisher record](https://link.springer.com/article/10.1007/s00220-019-03331-9),
  [DOI 10.1007/s00220-019-03331-9](https://doi.org/10.1007/s00220-019-03331-9).
  Consulted edition: [arXiv:1808.02422v1](https://arxiv.org/abs/1808.02422v1),
  7 August 2018, abbreviated SS18. The journal record verifies online publication
  on 12 February 2019 and December 2019 issue date. References to SS18 pages
  use the preprint's printed numbering; PDF page is printed page plus one.
- K. Soundararajan, _Approximating 1 from below using n Egyptian fractions_,
  [arXiv:math/0502247v1](https://arxiv.org/abs/math/0502247v1), 11 February 2005,
  [arXiv DOI 10.48550/arXiv.math/0502247](https://doi.org/10.48550/arXiv.math/0502247).
  This is a two-page author note, described by its arXiv record as not intended
  for publication, not a journal article.

Let `n` denote **number of weights**, so a single weight system normally
describes a polytope of dimension `n-1`. Skarke 1996 instead uses `l` for
number of weights and `n` for polytope dimension. Write positive normalized
weights `q_i` with `∑q_i=1`, and primitive positive integral weights `w_i`,
`gcd(w_i)=1`, degree `D=∑w_i`, with `q_i=w_i/D`.

In SS18 §3, p. 5, equations (5)–(7), the integer Newton hull is

$$
\Delta_q=\operatorname{conv}\{x\in\mathbb Z_{\ge0}^n:x\cdot q=1\}.
$$

Its IP condition is that `ones=(1,…,1)` lies in its **relative** interior in
`{x:x·q=1}`. In shifted coordinates `y=x-ones`, the equivalent condition is
origin interior in the lattice hull with `q·y=0` and `y_i≥-1`, using the
topology of `W_q=ker(q·−)`. Ambient interior in `R^n` would be empty.
Positive weights put `ones` in the real intercept simplex; they do not alone
put it in the integer Newton hull's relative interior. IP checking remains
an exact separate predicate.

## The finite search argument and its hypotheses

Skarke 1996 §2, pp. 4–5, describes the basic separating-functional search,
candidate generation and subsequent IP filtering. SS18 §3, pp. 5–7, states
the improved recursion precisely. In the normalization `r=1`, a finite set
of chosen nonnegative lattice points `S`, initially `{ones}`, determines

$$
Q_S=\{p:p_i\ge0,\ \sum p_i=1,\ x\cdot p=1\ (x\in S)\}.
$$

This is a bounded rational polytope because its equations have integer
coefficients and the sum constraint bounds coordinates. Empty feasibility
terminates a branch. For a viable node, the candidate must be **strictly
positive**, not just feasible with nonnegative entries, to obtain finite
branching. SS18 takes the arithmetic mean of all vertices of `Q_S`
(equation (8), p. 6). If `Q_S` contains a point positive in every coordinate,
then for each coordinate at least one vertex has positive value, and every
vertex is nonnegative. Their mean is consequently strictly positive. The
vertices and this mean are rational, permitting exact arithmetic.

For positive `p`, the next-point set

$$
B_p=\{x\in\mathbb Z_{\ge0}^n:x\cdot p<1\}
$$

is finite: `0≤x_i<1/p_i` for every coordinate. If `p_j=0`, all multiples
of `e_j` lie in `B_p`; nonnegative feasibility alone does not justify this
finiteness claim. A rational positive selection rule gives computable finite
integer coordinate bounds; an arbitrary real-valued choice does not itself
provide an algorithm for comparisons or enumeration.

The completeness argument is the **descent lemma** on SS18 p. 6. Fix an IP
target `q` whose Newton points include `S`. If the selected normalized `p`
differs from `q`, there must be an integer Newton point `x` of `q` with
`x·p<1`. Otherwise `p·−` would attain its minimum at the interior point
`ones` of `Δ_q`, forcing it to be constant on the target's affine span;
normalization would then give `p=q`. Retaining all such children therefore
retains a path to each target.

Every feasible step adds an independent equation. All old points satisfy
`z·p=1`, while the new point does not, so it lies outside their affine span.
Because that old affine span is in a hyperplane not containing zero, affine
independence there corresponds to linear independence of the point vectors.
At `n` linearly independent points, the equations determine a unique `q`.
SS18 stops the branch at `k+1=n`, with branching levels `k=0,…,n-2`.
Recording an early IP candidate does not justify discarding all descendants:
other targets can still satisfy the accumulated equations when the feasible
region has positive dimension.

The **whole-tree termination** conclusion needs bounded depth, finite branching,
and stopping on empty/nonpositive feasibility. Target-path descent by itself
does not prove that an arbitrary tree exploration terminates. A complete
finite implementation also requires exact IP decisions on its candidates.
Soundness is the assertion that the list records only candidates passing that
predicate; it needs a link to the implemented checker, not merely the comment
that PALP checks IP.

A normalized IP vector is rational even if initially defined over the reals:
its integer Newton hull spans the affine hyperplane containing `ones`, so
one can choose `n` linearly independent integer Newton points and solve an
invertible integer linear system `Xq=ones`. This rationality argument is a
mathematical deduction from full relative dimension, not a claim that the
frozen files contain that declaration. Clearing denominators and making the
weights primitive gives a unique integral encoding of such a rational ray.

## Half-weight reduction and the domains of pruning rules

SS18 §3, pp. 5–6, gives necessary conditions in its positive-dimensional
setting (`n≥2`) for normalized IP weights: `q_i≤1/2`; for `n>2` at most
one entry is `1/2`. If an entry is larger,
Newton points have that coordinate at most one, so `ones` lies on a supporting
coordinate hyperplane. The two-equal-halves restriction uses positivity of
the remaining weights and normalization; it excludes no general `n=2`
exception, where `(1/2,1/2)` is possible.

For `q=(q',1/2)` with `∑q'_i=1/2`, the reduced integer hull uses
`x'·q'=1` and interior point `(2,…,2)`. Thus the split is a six-weight
`r=1` run plus a five-weight `r=1/2` run, appending the half-weight at the
end. It is not an ordinary depth-one IP problem for the truncated weights.
In shifted coordinates `z=x'-2ones`, it is the **depth-two** hull
`q'·z=0`, `z_i≥-2`. Positively scaling `q'` to sum one preserves this
kernel but does not change depth two into depth one. In the reduced `r=1/2`
run, feasibility is encoded by `x^(0)=2ones`, while the right-hand side of
the Newton equation remains one.

SS18 §3, p. 7, lists seven rejection rules, organized by cost: coordinate and
total-sum exclusions; orbit representatives under permutations stabilizing
old points; lexicographic violations of previous minimum choices; a line
extension test; failure of positive feasibility; and the corresponding
minimum-choice test over integer points of the accumulated affine span.
The canonical path chooses the Newton point minimizing the current candidate
pairing, breaking ties by the lexicographically largest point. This supplies
the rationale for retaining a path while reducing duplicates, but requires
separate proofs for the actual tests and their domains. SS18 §5, p. 11,
also says some redundancy checks were turned off near the bottom of the tree
and remaining duplicates removed by sorting. The algorithm is not claimed
to generate each target once before that final sorting.

Two scope details matter for a faithful port:

- The stabilizer/orbit rule assumes a permutation-compatible search, including
  its candidate choices. It cannot automatically be attached to a completely
  arbitrary selection rule whose behavior breaks permutation symmetry.
- The total-sum rule `∑x_i≤2` uses the split search where remaining candidate
  weights are below `1/2`. In the unrestricted normalized feasible region,
  `(2,0,…,0)` **does** admit a positive solution with `q_1=1/2`.
  Therefore SS18's observation that this rule implies failed positive
  feasibility must retain the split-domain restrictions. It is not a lemma
  about the frozen unrestricted `qPolytope` alone.

## Actual frozen Lean declarations

The source inspected is the reviewed read-only snapshot at
`/home/ahat01/.copilot/session-state/83ef73b2-4517-446c-9ac2-9437ff014965/files/refpoly-proof-snapshot/proofs`.
Names below lie in `Refpoly`, except the explicit `WeightSystemFinite`
subnamespace. These are verified type/definition observations, not a new
build or independent axiom audit.

| Declaration                                                                 | Exact scope and limit                                                                                                                                                                                                                                                                                       |
| --------------------------------------------------------------------------- | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `WeightSystem.lean`, `IsIPWeightSystemAt`, `IsIPWeightSystem`               | Strict positivity plus origin interior of an integer convex hull **in the subtype** `wsHyperplane q`; ordinary IP uses depth one.                                                                                                                                                                           |
| `Algorithm.lean`, `qPolytope`, `SelectionRule`                              | Feasible vectors are nonnegative with sum one; selection membership is required only when feasibility is nonempty. Strict positivity and rationality are not fields of `SelectionRule`.                                                                                                                     |
| `Algorithm.lean`, `Reachable`                                               | Root `{ones}` and insertion of any nonnegative lattice `x` with `x·sel(S)<1`. The inductive relation has no guard requiring positive feasibility or nonempty child feasibility.                                                                                                                             |
| `average_pos`                                                               | Positivity of a mean of a supplied finite family of nonnegative vectors, provided every coordinate is positive somewhere. It does not construct all feasible-polytope vertices or prove they are rational.                                                                                                  |
| `branch_finite`                                                             | Requires `∀i,0<p_i`; proves the branch set is finite. It does not prove finite branching for every `SelectionRule`.                                                                                                                                                                                         |
| `descent_lemma`                                                             | For normalized IP `q` and normalized `p≠q`, produces `x∈wsPoints q` with `x·p<1`; no positivity hypothesis on `p` is needed for this target-path existence.                                                                                                                                                 |
| `reach_step`, `notMem_affineSpan_of_dot_lt`, `finrank_vectorSpan_lt_insert` | Target-compatible reachable nodes either select the target or have a target-compatible child of larger affine rank.                                                                                                                                                                                         |
| `algorithm_complete`                                                        | For **every** `SelectionRule R` and normalized IP target `q`, obtains `∃ S, Reachable R S ∧ R.sel S=q ∧ S.card≤n+1`. This finite witness/cardinality statement does not return a list or prove the full unrestricted tree finite. The printed bound is looser than the paper's `n`-point stopping argument. |
| `WeightSystemFinite.PositiveFeasible`, `positiveSelection`                  | Positive feasibility is existence of a strictly positive member of `qPolytope S`; `positiveSelection` chooses one by classical choice, with fallback feasible choices or zero. It is `noncomputable`.                                                                                                       |
| `WeightSystemFinite.nodes`, `nodes_finite`                                  | A depth-indexed finite set union branches only at positively feasible parents; the chosen candidate is then positive. This is a guarded finite catalogue construction in `Set`, not executable enumeration.                                                                                                 |
| `normalized_ip_weight_systems_finite`                                       | `{q : V n : ∑q_i=1 ∧ IsIPWeightSystem q}.Finite`, obtained by covering targets with the finite image of candidates in `nodes n n`. It does not use Hensley or an assumed numerical degree cutoff.                                                                                                           |
| `WeightSystem.lean`, `ip_half_reduction`                                    | For positive `q'` with sum `1/2`, `IsIPWeightSystem (snoc q' (1/2)) ↔ IsIPWeightSystemAt q' 2`. This includes the integer-hull and interior geometry, not just weight positivity.                                                                                                                          |
| `DegreeBound.lean`, `normalizedWeights_injective_on_primitive`              | Equal normalized values imply equal natural weight vectors when both have gcd one and positive total degree.                                                                                                                                                                                                |
| `primitive_ip_weight_systems_finite`, `primitive_ip_degrees_bounded`        | Fixed-length positive primitive integral IP vectors form a finite set, hence `∃D, ∀w, … → ∑w_i≤D`. This is existential boundedness without a supplied numerical value.                                                                                                                                      |

For example, a selection rule may pick a vertex with a zero coordinate at
the root. Its branch relation can then have infinitely many children. At
an infeasible node the rule is unconstrained, so `Reachable` can also include
arbitrary further insertions. Such behavior does not refute `algorithm_complete`:
that theorem only needs one bounded witness path for each target. It does
refute reading its source comment as a theorem about total executable
termination for all selection rules.

The audited port should preserve the supported abstract theorem while placing
the claims about a runnable, terminating list in a separate refinement theorem:
computable positive rational choice; decidable feasibility and IP tests;
finite enumeration of branches; stopping; safe pruning; and exact output
normalization/permutation handling. The current PALP/reference-lattice native
scope is narrower still than all-dimensional KS coverage.

## Census numbers and the status of 3,263,442

SS18 abstract and §6, pp. 12–13, report **322,383,760,930** six-weight IP
systems, of which **185,269,499,015** directly determine reflexive polytopes
and **137,114,261,915** determine nonreflexive IP polytopes. The reflexive
systems yield **532,600,483** distinct Hodge-data sets. These are published
enumeration results, not independently reproduced measurements here.
SS18 p. 13 explicitly says distinct weight systems can define the same
polytope and that their frequency was not checked. None of these weight
counts is the number of distinct five-dimensional reflexive polytopes, and
the Hodge count is neither a polytope nor an equivalence-class count.

SS18 §5, pp. 11–12, distinguishes the **337,301,206,339 candidate** systems
from the accepted IP census and describes cross-checks against the degree-300
list and the transverse-weight list. These support the published census but
do not substitute for the requested native-algorithm refinement proof.

The Sylvester sequence starts `a_1=2` and satisfies
`a_(j+1)=1+∏_(i≤j)a_i=a_j(a_j-1)+1`. SS18 §6, p. 14,
equations (23)–(27), constructs the two Fermat candidates

$$
q_{ct}^{(6)}=(1/2,1/3,1/7,1/43,1/1807,1/3263442),
\qquad
q_{lt}^{(6)}=(1/2,1/3,1/7,1/43,1/3612,1/3612).
$$

The paper gives their normalization and their location at the central and
left tips of its Hodge plot; it identifies Fermat weights as IP. Indeed,
when all `q_i=1/b_i` and their sum is one, the integral intercept vertices
`b_i e_i` are present, and `ones=∑q_i(b_i e_i)` is a strict convex combination
inside the full relative-dimensional intercept simplex. This proves IP for
these particular Newton hulls. It does not prove a universal degree upper
bound.

The frozen `Sylvester.lean` proves `prod_sylvester`, `sylvester_sum_inv`,
the positive-entry and sum-one theorems `qct6_pos`, `qct6_sum`, `qlt6_pos`,
`qlt6_sum`, and the arithmetic factorization `3263442=1806·1807`.
Those theorem types do **not** assert IP, reflexivity, primitive degree,
Hodge extremality or maximal degree. `max_degree_dim5` proves only
`∃D, DegreeBoundDim5 D`. `degree_dim5_le_of_certificate` assumes an explicit
certificate; none for `D=3263442` is supplied.

There is also a citation discrepancy: the frozen header attributes a maximum
degree of 3,263,442 to SS18 **§2**, but that section of the consulted v1 does
not state it. The number appears in **§6** as the Fermat tip candidate.
The reviewed sources therefore support that candidate and arithmetic,
not that particular attribution of a universally sharp cutoff. If a complete
computed catalogue elsewhere reports 3,263,442 as its observed maximum,
that report still needs catalogue coverage/provenance or a mathematical
bound to serve as a certified universal cutoff. This note did not independently
recompute or verify the global observed maximum.

### Unit fractions: a verified theorem and a restricted consequence

D. R. Curtiss, _On Kellogg's Diophantine Problem_, **American Mathematical
Monthly 29** (10) (November–December 1922), 380–387,
[JSTOR primary record](https://www.jstor.org/stable/2299023),
[DOI 10.2307/2299023](https://doi.org/10.2307/2299023), is a verified
bibliographic lead. Its attempted PDF download returned a robot-validation
HTML document; the original text was **not accessible/read**, so no theorem
number, exact statement or proof in Curtiss is asserted as directly verified.

The actually read substitute is Soundararajan's note, p. 1, opening statement
and equation (1): for positive integral denominators `b_1≤…≤b_k` with
`∑1/b_i<1`,

$$
\sum_{i=1}^k\frac1{b_i}\le\sum_{i=1}^k\frac1{a_i}
=1-\frac1{\prod_{i=1}^k a_i},
$$

with equality only for the Sylvester denominators. Repeated denominators are
allowed. The proof spans pp. 1–2, using its proposition comparing products
and sums of decreasing positive real sequences; this is an independently
consulted primary proof, not confirmation of the inaccessible Curtiss proof.

As a deduction, for `n` genuine unit fractions summing to one, delete the
term with the largest denominator. The remaining `n-1` terms sum below one,
so the deleted fraction is at least `1/∏_(i<n)a_i`. Every denominator is
therefore at most `∏_(i<n)a_i`; for six fractions this is 3,263,442, attained
by the displayed Sylvester completion. This only bounds denominators in
the **Fermat/unit-fraction subclass**. General primitive IP weights have
`q_i=w_i/D` with numerators not necessarily one. Moreover their primitive
degree in the Fermat subclass is `lcm(b_i)`, not in general the largest
denominator. The denominator bound alone yields, for example, the coarse
effective degree bound `lcm(1,…,3263442)` for that subclass, not automatically
the sharp degree cutoff 3,263,442 for all IP weights. For the displayed
`q_ct`, the last denominator is divisible by the others, so its degree does
equal 3,263,442 after primitive clearing. That arithmetic fact about one
example must not be promoted to a universal theorem.

## Remaining obligations

| Obligation                             | Verified support                                                           | Missing connection                                                                                          |
| -------------------------------------- | -------------------------------------------------------------------------- | ----------------------------------------------------------------------------------------------------------- |
| Complete normalized target coverage    | Paper descent argument; frozen reachable witness for any selector          | Computable candidate/branch enumeration and terminating output list.                                        |
| Finite branching                       | Requires strictly positive candidate; guarded frozen finite sets           | Positive rational selector and exact feasibility test; arbitrary selector is insufficient.                  |
| Rank/depth stopping                    | Paper linear independence; frozen affine-rank increase and bounded witness | Refine concrete recursion, empty/nonpositive exits and final stopping.                                      |
| Soundness and pruning                  | Exact IP filtering described; seven paper pruning rules                    | Prove actual checker and every enabled native pruning rule in its correct search domain.                    |
| Half-weight split                      | Frozen depth-two equivalence supported                                     | Refine reduced-run normalization and reconstruction; do not use ordinary depth-one IP on truncated weights. |
| Primitive degree bounds                | Existential frozen bound; particular Sylvester arithmetic                  | Numerical universal certificate; observed maximum and Fermat denominator bound are different claims.        |
| Census interpretation                  | Published weights, acceptance split and Hodge counts verified              | Exact distinct-polytope catalogue and lattice-equivalence deduplication.                                    |
| Full all-dimensional KS classification | Weight generation supplies necessary ingredients                           | Exhaustive CWS structures, lattice realization, container/subpolytope coverage and native refinement.       |
| Nonreflexive extension                 | Nonreflexive Newton hulls are included in the published WS census          | Coverage of arbitrary nonreflexive one-point polytopes needs a separate theorem.                            |

## Cache and source evidence

Shared PDFs were reused and their recorded SHA-256 values checked before
reading. The new Soundararajan PDF and available author TeX were cached in
the uniquely assigned ignored directory; original bytes are preserved.
Each local provenance file records URLs, versions and retrieval date
**2026-10-14**. No shared cache record was edited.

| Source/version          | Cache path relative to `docs/learning`             | Original PDF SHA-256                                               |
| ----------------------- | -------------------------------------------------- | ------------------------------------------------------------------ |
| Skarke 1996 v1          | `sources/skarke-alg-geom-9603007/v1/paper.pdf`     | `ee4357a5eadd242e9689546a7991a9e5c291bb7b4627674e59865a7e80698800` |
| Schöller–Skarke 2018 v1 | `sources/schoeller-skarke-1808-02422/v1/paper.pdf` | `1272b6535b727824744d5bd733b640f246e03a5e374a7ea0cdd428677001cc57` |
| Soundararajan 2005 v1   | `sources/soundararajan-math-0502247/v1/paper.pdf`  | `96f1716ee0f84ed613eb8118fa9199a47f66fe93ea70d0741038da55ca8e1d32` |

The Soundararajan source download is
`https://arxiv.org/src/math/0502247v1`, saved as `source-original` (gzip
compressed `Egyptian.tex`) with a supplementary `source-extracted.tex`.
Its original gzip SHA-256 is
`4c9625625c2f3cd6f2619f82e98b7650f215f8607e71209ce0f30361f25f8c51`.
The Curtiss directory retains `download-response.html` and provenance explicitly
marking **no PDF/text verified**. Download success alone was not accepted as
evidence of a readable PDF. No author-source availability is inferred from
the absence of a journal PDF.

Snapshot file hashes for the inspected declarations:

| File in `Refpoly/`        | SHA-256                                                            |
| ------------------------- | ------------------------------------------------------------------ |
| `Algorithm.lean`          | `0071873dae87fa805377c5a24d4fddac291826300030bfb0f454a157c85b568d` |
| `WeightSystemFinite.lean` | `ee3474a8ede4ecaf80963191c95f1450a504f7cf6e62e60e1fb131c216f811f1` |
| `WeightSystem.lean`       | `c874f2101fb944637922b022b765a357f96f38ba1a1595c716e811df5ca5f85a` |
| `DegreeBound.lean`        | `46819d084db5bf1ff03979773a72b7ae7ca9fc90c4bb6743cc93234909583103` |
| `Sylvester.lean`          | `ae1c4efb04ed58b3ec54cd6f8c7f596ee423c73d1eaf04f8520eeb7da92f2856` |

No numerical census or maximum degree was recomputed, and no proof file,
native code, dataset, shared plan or Git ref was changed by this assignment.
