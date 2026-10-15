/-
Classification faithfully ported from the reviewed refpoly-5d snapshot.
See SOURCES.md for attribution and exact source identities.
Legacy simplex signatures forget the seed-to-weight witness connection.
They are retained as scoped results, not evidence for the new headline.
Historical enumeration prose below does not verify PALP or census counts.
Use GeometricCWS and CWSContainer for explicit geometric weight witnesses.
-/
import Refpoly.Minimal
import Refpoly.Algorithm

open Set Module

namespace Refpoly

variable {n : ℕ}

/-! ## T9.1: coverage — every reflexive polytope sits below a minimal IP set -/

/-- **T9.1 (coverage of the classification, [KS95] §2).**  Every reflexive
polytope `P` admits a finite set `S` of lattice points such that

1. `S ⊆ P*` (the dual polytope's lattice points),
2. `S` is a minimal IP generating set (D3.1), and
3. `P ⊆ [conv(S)*]` — `P` is a lattice subpolytope of the maximal polytope
   determined by `S`.

Hence the two-stage strategy — enumerate minimal IP sets, then take all
reflexive lattice subpolytopes of their dual maximal polytopes — reaches
every reflexive polytope.

*Proof.*  `P*` is reflexive (T3.3), so it is the hull of its lattice points
`T`; the origin is interior to it, so `T` contains a minimal IP generating
subset `S` (T3.5).  Then `conv(S) ⊆ P*` gives, by antitonicity and the
bipolar theorem (L0.7), `P = P** ⊆ conv(S)*`; since `P` is a lattice
polytope, `P = [P] ⊆ [conv(S)*]`. -/
theorem classification_coverage {P : Set (V n)} (hP : IsReflexive P) :
    ∃ S : Finset (V n), (∀ x ∈ S, IsLatticePoint x) ∧
      (S : Set (V n)) ⊆ polarDual P ∧ IsMinimalIPGen S ∧
      P ⊆ latticeHull (polarDual (convexHull ℝ (S : Set (V n)))) := by
  have hdual := hP.polarDual_reflexive
  obtain ⟨T, hTlat, hTeq⟩ := hdual.latticePolytope
  have h0T : (0 : V n) ∈ interior (convexHull ℝ (T : Set (V n))) := by
    rw [← hTeq]
    exact hdual.ip
  obtain ⟨S, hsub, hmin⟩ := exists_minimalIPGen_subset T h0T
  refine ⟨S, fun x hx => hTlat x (hsub hx), ?_, hmin, ?_⟩
  · -- S ⊆ T ⊆ conv T = P*
    intro x hx
    rw [hTeq]
    exact subset_convexHull ℝ _ (hsub hx)
  · -- the coverage chain P = P** = [P**] ⊆ [conv(S)*]
    have hSsub : convexHull ℝ (S : Set (V n)) ⊆ polarDual P := by
      rw [hTeq]
      exact convexHull_mono (Finset.coe_subset.mpr hsub)
    have h1 : P ⊆ polarDual (convexHull ℝ (S : Set (V n))) := by
      rw [← hP.bipolar_eq]
      exact polarDual_antitone hSsub
    calc P = latticeHull P := hP.latticePolytope.eq_latticeHull
    _ ⊆ latticeHull (polarDual (convexHull ℝ (S : Set (V n)))) :=
        latticeHull_mono h1

/-! ## T9.3: the weight system of a reflexive polytope's dual simplex is IP -/

/-- **T9.3 (the simplicial bridge to the enumeration; [SS18] §2.2/§3).**
Let `P` be reflexive and `S` an affinely independent set of lattice points of
`P*` whose hull has `0` in its interior (the simplicial case of T9.1; for
non-simplicial `S`, [KS95] Lemma 1 decomposes into such simplices, one weight
system per block).  Then the barycentric weight system `q` of `S` is a
*normalized positive IP weight system* — exactly the kind of object the
enumeration of `Algorithm.lean` produces.

*Proof (the y-coordinates of [SS18] eq. (3.1)).*  Enumerate
`S = {V₁, …, V_{n+1}}` and let `q` be the barycentric coordinates of `0`
(`simplex_weights`).  The pairing map `Φ(x)ᵢ = ⟨Vᵢ, x⟩` is a surjective, hence
open, linear map onto the hyperplane `W_q`; it sends lattice points of `P` to
integer points of `W_q` with coordinates `≥ −1` (because `Vᵢ ∈ P*`), so
`Φ(P) ⊆ Δ_q` and `0 ∈ int P` maps to `0 ∈ int Δ_q`.  This is the
full-dimensional case of `reflexive_simplex_block_ip`. -/
theorem reflexive_simplex_weight_system {P : Set (V n)} (hP : IsReflexive P)
    {S : Finset (V n)} (hSlat : ∀ x ∈ S, IsLatticePoint x)
    (hSdual : (S : Set (V n)) ⊆ polarDual P)
    (hind : AffineIndependent ℝ ((↑) : ↥S → V n))
    (h0 : (0 : V n) ∈ interior (convexHull ℝ (S : Set (V n)))) :
    ∃ q : V (n + 1), (∀ i, 0 < q i) ∧ (∑ i, q i = 1) ∧ IsIPWeightSystem q := by
  obtain ⟨v, q, hrange, hv, hqpos, hqsum, hcirc⟩ := simplex_weights hind h0
  have hvS : ∀ i, v i ∈ (S : Set (V n)) := fun i => hrange ▸ Set.mem_range_self i
  exact ⟨q, hqpos, hqsum, reflexive_simplex_block_ip hP v q hv hqpos hqsum hcirc
    (fun i => hSlat _ (hvS i)) (fun i => hSdual (hvS i))⟩

/-! ## T9.4: the headline — the algorithm enumerates all reflexive polytopes -/

/-! ## Maximal-reflexive consequence

The next theorem is the precise statement needed for the `r`-maximal
pipeline.  It intentionally does **not** assume that the Newton container
`[conv(S)*]` is reflexive.  In dimension five this matters: an IP (combined)
weight-system container can fail the facet-distance-one test, but it may still
contain reflexive subpolytopes.  A maximal reflexive polytope cannot be enlarged
inside any such container by another reflexive polytope, so enumerating
reflexive subpolytopes of all minimal-IP containers and then keeping the
maximal ones reaches every `r`-maximal polytope.
-/

/-- **Maximal-reflexive coverage by minimal-IP containers.**

Every maximal reflexive polytope `P` occurs as a reflexive subpolytope of a
container `[conv(S)*]` coming from a minimal IP generating set `S ⊆ P*`.
Moreover, inside that container there is no strictly larger reflexive
subpolytope containing `P`.  The statement deliberately imposes no reflexivity
hypothesis on the container itself, which is the formal reason that
IP-but-not-reflexive Newton containers cannot be discarded before the
reflexive-subpolytope search. -/
theorem maximal_reflexive_covered_by_minimalIP_container {P : Set (V n)}
    (hP : IsMaximalReflexive P) :
    ∃ S : Finset (V n), (∀ x ∈ S, IsLatticePoint x) ∧
      (S : Set (V n)) ⊆ polarDual P ∧ IsMinimalIPGen S ∧
      let C := latticeHull (polarDual (convexHull ℝ (S : Set (V n))))
      P ⊆ C ∧ ∀ Q : Set (V n), IsReflexive Q → P ⊆ Q → Q ⊆ C → Q = P := by
  obtain ⟨S, hSlat, hSdual, hSmin, hcover⟩ := classification_coverage hP.1
  refine ⟨S, hSlat, hSdual, hSmin, ?_⟩
  refine ⟨hcover, ?_⟩
  intro Q hQ hPQ _hQC
  exact hP.2 Q hQ hPQ

/-- **T9.4 (correctness of the classification pipeline).**  For every
reflexive polytope `P ⊂ ℝⁿ` and every selection rule `R` (e.g. the vertex
average of [SS18] §3):

1. there is a minimal IP generating set `S` of lattice points of `P*` whose
   maximal polytope `[conv(S)*]` contains `P` (T9.1) — so `P` appears in the
   subpolytope search below `S`; and
2. whenever `S` is simplicial (the non-simplicial case decomposes into
   simplicial blocks via [KS95] Lemma 1 = L4.3), its barycentric weight
   system `q` is a normalized positive IP weight system and **the
   enumeration tree of `Algorithm.lean`, started at `(1,…,1)`, reaches a
   node where the selected candidate is exactly `q`, within `n + 1`
   branchings** (T7.5/T7.6).

In dimension `n = 5` this is the formal statement that the [SS18] run —
which enumerated all 322 383 760 930 IP weight systems with 6 weights and
kept the 185 269 499 015 reflexive ones — misses no reflexive 5-polytope. -/
theorem algorithm_enumerates_reflexive (R : SelectionRule (n + 1))
    {P : Set (V n)} (hP : IsReflexive P) :
    ∃ S : Finset (V n),
      (∀ x ∈ S, IsLatticePoint x) ∧ (S : Set (V n)) ⊆ polarDual P ∧
      IsMinimalIPGen S ∧
      P ⊆ latticeHull (polarDual (convexHull ℝ (S : Set (V n)))) ∧
      (AffineIndependent ℝ ((↑) : ↥S → V n) →
        ∃ q : V (n + 1), (∀ i, 0 < q i) ∧ (∑ i, q i = 1) ∧
          IsIPWeightSystem q ∧
          ∃ N : Finset (V (n + 1)), Reachable R N ∧ R.sel N = q ∧
            N.card ≤ n + 2) := by
  obtain ⟨S, hSlat, hSdual, hSmin, hcover⟩ := classification_coverage hP
  refine ⟨S, hSlat, hSdual, hSmin, hcover, ?_⟩
  intro hind
  obtain ⟨q, hqpos, hqsum, hqIP⟩ :=
    reflexive_simplex_weight_system hP hSlat hSdual hind hSmin.1
  obtain ⟨N, hreach, hsel, hcard⟩ := algorithm_complete R hqpos hqsum hqIP
  exact ⟨q, hqpos, hqsum, hqIP, N, hreach, hsel, by omega⟩

end Refpoly
