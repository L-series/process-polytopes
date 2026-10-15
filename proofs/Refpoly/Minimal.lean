/-
Minimal faithfully ported from the reviewed refpoly-5d snapshot.
See SOURCES.md for attribution and exact source identities.
The structure theorem is one decomposition step.
The dimension-generic 2d seed bound is proved in KS/SeedBound.lean.
-/
import Refpoly.MaxMin
import Refpoly.CircuitInterior

open Set Module

namespace Refpoly

variable {n : ℕ}

/-! ## D4.1: good simplices -/

/-- **D4.1 (good simplex, [KS95] §2).**  A finite point set is a *good
simplex* if it is affinely independent and the origin lies in the *relative*
(intrinsic) interior of its hull.  The faces appearing in the decomposition
of a non-simplicial minimal polytope are good simplices. -/
def IsGoodSimplex (T : Finset (V n)) : Prop :=
  AffineIndependent ℝ ((↑) : ↥T → V n) ∧
    (0 : V n) ∈ intrinsicInterior ℝ (convexHull ℝ (T : Set (V n)))

/-- Relative (intrinsic-interior) variant of `IsMinimalIPGen`, the notion of
minimality appropriate for the lower-dimensional pieces `M'` in [KS95]
Lemma 1. Proper subsets are tested in the parent affine span: only subsets
with the same span can retain interior there. -/
def IsMinimalIPRel (S : Finset (V n)) : Prop :=
  (0 : V n) ∈ intrinsicInterior ℝ (convexHull ℝ (S : Set (V n))) ∧
    ∀ S' ⊂ S, affineSpan ℝ (S' : Set (V n)) = affineSpan ℝ (S : Set (V n)) →
      (0 : V n) ∉ intrinsicInterior ℝ (convexHull ℝ (S' : Set (V n)))

/-- A full-dimensional minimal IP generating set also contains the origin in
its intrinsic interior.  This is the only direct bridge from the
full-dimensional definition `IsMinimalIPGen` to the relative notion used for
lower-dimensional pieces: relative minimality of a proper subset is an
additional structure theorem, not a consequence of `IsMinimalIPGen` alone. -/
theorem IsMinimalIPGen.origin_mem_intrinsicInterior {S : Finset (V n)}
    (hS : IsMinimalIPGen S) :
    (0 : V n) ∈ intrinsicInterior ℝ (convexHull ℝ (S : Set (V n))) :=
  interior_subset_intrinsicInterior hS.1

/-- A finite set whose convex hull contains the origin as a genuine interior
point affinely spans the ambient space. -/
theorem IsMinimalIPGen.affineSpan_eq_top {S : Finset (V n)}
    (hS : IsMinimalIPGen S) :
    affineSpan ℝ (S : Set (V n)) = ⊤ :=
  interior_convexHull_nonempty_iff_affineSpan_eq_top.mp ⟨0, hS.1⟩

/-- Full-dimensional minimality implies relative minimality when proper
subsets are tested in the fixed parent affine span. -/
theorem IsMinimalIPGen.isMinimalIPRel {S : Finset (V n)}
    (hS : IsMinimalIPGen S) : IsMinimalIPRel S := by
  refine ⟨interior_subset_intrinsicInterior hS.1, ?_⟩
  intro S' hsub hspan h0
  refine hS.2 S' hsub (mem_interior_of_mem_intrinsicInterior ?_ h0)
  rw [affineSpan_convexHull, hspan, hS.affineSpan_eq_top]

/-! ## C4.5 (lower bound): an IP generating set has ≥ n + 1 points -/

/-- **C4.5, lower bound ([KS95] Corollary 2).**  A set whose hull has the
origin as a (full-dimensional) interior point must affinely span `ℝⁿ`, so it
has at least `n + 1` elements.  (The upper bound `≤ 2n` for *minimal* sets
needs L4.3 and is not formalized.) -/
theorem minimalIPGen_card_lower {S : Finset (V n)}
    (h0 : (0 : V n) ∈ interior (convexHull ℝ (S : Set (V n)))) :
    n + 1 ≤ S.card := by
  classical
  have hspan : affineSpan ℝ (S : Set (V n)) = ⊤ :=
    interior_convexHull_nonempty_iff_affineSpan_eq_top.mp ⟨0, h0⟩
  have hne : S.Nonempty := by
    rcases S.eq_empty_or_nonempty with rfl | h
    · simp at h0
    · exact h
  have hpos : 0 < S.card := Finset.card_pos.mpr hne
  obtain ⟨m, hm⟩ : ∃ m, S.card = m + 1 := ⟨S.card - 1, by omega⟩
  have hbound := finrank_vectorSpan_image_finset_le (k := ℝ) id S hm
  rw [Finset.image_id] at hbound
  have htop : vectorSpan ℝ (S : Set (V n)) = ⊤ := by
    rw [← direction_affineSpan, hspan]
    exact AffineSubspace.direction_top ℝ _ _
  have hfr : finrank ℝ (vectorSpan ℝ (S : Set (V n))) = n := by
    rw [htop, finrank_top]
    exact Module.finrank_fin_fun ℝ
  omega

/-- An *affinely independent* IP generating set has exactly `n + 1` points:
it is the vertex set of a non-degenerate `n`-simplex containing `0` in its
interior. -/
theorem simplex_card {S : Finset (V n)}
    (hind : AffineIndependent ℝ ((↑) : ↥S → V n))
    (h0 : (0 : V n) ∈ interior (convexHull ℝ (S : Set (V n)))) :
    S.card = n + 1 := by
  have hlower := minimalIPGen_card_lower h0
  have hupper := hind.card_le_finrank_succ
  rw [Fintype.card_coe, Subtype.range_coe_subtype, Finset.setOf_mem] at hupper
  have hle : finrank ℝ (vectorSpan ℝ (S : Set (V n))) ≤ n :=
    (Submodule.finrank_le _).trans_eq (Module.finrank_fin_fun ℝ)
  omega

/-! ## The weight system of a minimal simplex (L4.2′ / [KS95] §3) -/

/-- **The barycentric weight system of an IP simplex.**  If `S` is affinely
independent and its hull contains `0` in its interior, then — enumerating
`S` as `v : Fin (n+1) → V n` — there is a weight vector `q` with

* the `vᵢ` are affinely independent,
* `qᵢ > 0` (positivity: `0` is *interior*, L5.2),
* `Σ qᵢ = 1` (normalization), and
* `Σ qᵢ • vᵢ = 0` (the circuit relation `Σ qᵢ Vᵢ = 0` of [KS95] §3 /
  [SS18] §2.2: the weights are the barycentric coordinates of the origin).

This is the object handed to the enumeration algorithm; `Classification.lean`
upgrades it to an `IsIPWeightSystem` when `S` supports a reflexive polytope. -/
theorem simplex_weights {S : Finset (V n)}
    (hind : AffineIndependent ℝ ((↑) : ↥S → V n))
    (h0 : (0 : V n) ∈ interior (convexHull ℝ (S : Set (V n)))) :
    ∃ (v : Fin (n + 1) → V n) (q : V (n + 1)),
      Set.range v = (S : Set (V n)) ∧ AffineIndependent ℝ v ∧ (∀ i, 0 < q i) ∧
      (∑ i, q i = 1) ∧ ∑ i, q i • v i = 0 := by
  classical
  have hcard := simplex_card hind h0
  have hScard : Fintype.card (↥S) = n + 1 := by
    rw [Fintype.card_coe]; exact hcard
  -- enumerate the vertices
  let e : ↥S ≃ Fin (n + 1) := Fintype.equivFinOfCardEq hScard
  let v : Fin (n + 1) → V n := fun i => ((e.symm i : ↥S) : V n)
  have hrange : Set.range v = (S : Set (V n)) := by
    have hcomp : v = ((↑) : ↥S → V n) ∘ e.symm := rfl
    rw [hcomp, Set.range_comp, Equiv.range_eq_univ, Set.image_univ,
      Subtype.range_coe_subtype, Finset.setOf_mem]
  -- the vertices form an affine basis
  have hv_ind : AffineIndependent ℝ v := hind.comp_embedding e.symm.toEmbedding
  have hspan : affineSpan ℝ (Set.range v) = ⊤ := by
    rw [hrange]
    exact interior_convexHull_nonempty_iff_affineSpan_eq_top.mp ⟨0, h0⟩
  let b : AffineBasis (Fin (n + 1)) ℝ (V n) := ⟨v, hv_ind, hspan⟩
  have hb_coe : ⇑b = v := rfl
  refine ⟨v, fun i => b.coord i 0, hrange, hv_ind, ?_, b.sum_coord_apply_eq_one 0, ?_⟩
  · -- positivity: barycentric coordinates of an interior point (L5.2)
    have h0' : (0 : V n) ∈ interior (convexHull ℝ (Set.range ⇑b)) := by
      rw [hb_coe, hrange]; exact h0
    rw [b.interior_convexHull] at h0'
    exact h0'
  · -- the circuit relation: the affine combination with these weights is 0
    have hself := b.affineCombination_coord_eq_self (0 : V n)
    rwa [Finset.univ.affineCombination_eq_linear_combination _ _
      (b.sum_coord_apply_eq_one 0), hb_coe] at hself

/-! ## Good simplices through individual generators -/

/-- Every generator of an IP polytope lies in a good simplex formed from
its generators. No lattice or minimality hypothesis is required. -/
theorem exists_goodSimplex_through {S : Finset (V n)}
    (h0 : (0 : V n) ∈ interior (convexHull ℝ (S : Set (V n))))
    (a : V n) (ha : a ∈ S) :
    ∃ T : Finset (V n), T ⊆ S ∧ a ∈ T ∧ IsGoodSimplex T := by
  obtain ⟨T, hTS, haT, hind, w, hw, hsum, hrel⟩ :=
    ip_positive_circuit_through S a ha h0
  exact ⟨T, hTS, haT, hind,
    finset_positive_relation_intrinsicInterior T w hw hsum hrel⟩

/-- A non-simplicial IP generating set contains a proper good simplex. -/
theorem exists_proper_goodSimplex_of_not_affineIndependent {S : Finset (V n)}
    (h0 : (0 : V n) ∈ interior (convexHull ℝ (S : Set (V n))))
    (hns : ¬ AffineIndependent ℝ ((↑) : S → V n)) :
    ∃ T : Finset (V n), T ⊂ S ∧ IsGoodSimplex T := by
  classical
  obtain ⟨a, ha⟩ := S.nonempty_iff_ne_empty.2 (by rintro rfl; simp at h0)
  obtain ⟨T, hTS, _, hgood⟩ := exists_goodSimplex_through h0 a ha
  have hne : T ≠ S := by intro h; subst T; exact hns hgood.1
  exact ⟨T, Finset.ssubset_iff_subset_ne.mpr ⟨hTS, hne⟩, hgood⟩

/-- Every proper relative-IP subset of a minimal full-dimensional IP set
has strictly smaller dimension. -/
theorem proper_relativeIP_dim_lt {S T : Finset (V n)} (hS : IsMinimalIPGen S)
    (hT : T ⊂ S)
    (h0 : (0 : V n) ∈ intrinsicInterior ℝ (convexHull ℝ (T : Set (V n)))) :
    finrank ℝ (vectorSpan ℝ (T : Set (V n))) < n := by
  have h0A : (0 : V n) ∈ affineSpan ℝ (T : Set (V n)) :=
    convexHull_subset_affineSpan _ (intrinsicInterior_subset h0)
  have hne : vectorSpan ℝ (T : Set (V n)) ≠ ⊤ := by
    intro htop
    have hA : affineSpan ℝ (T : Set (V n)) = ⊤ :=
      (AffineSubspace.direction_eq_top_iff_of_nonempty ⟨0, h0A⟩).mp
        (by rwa [direction_affineSpan])
    exact hS.isMinimalIPRel.2 T hT (hA.trans hS.affineSpan_eq_top.symm) h0
  simpa [V] using Submodule.finrank_lt hne

/-- The first choice in the KS structure proof: a proper relative-IP subset
of maximal dimension. The quotient and lifting steps remain separate. -/
theorem exists_maximal_dim_proper_relativeIP {S : Finset (V n)}
    (hS : IsMinimalIPGen S)
    (hns : ¬ AffineIndependent ℝ ((↑) : S → V n)) :
    ∃ T : Finset (V n), T ⊂ S ∧
      (0 : V n) ∈ intrinsicInterior ℝ (convexHull ℝ (T : Set (V n))) ∧
      finrank ℝ (vectorSpan ℝ (T : Set (V n))) < n ∧
      ∀ U : Finset (V n), U ⊂ S →
        (0 : V n) ∈ intrinsicInterior ℝ (convexHull ℝ (U : Set (V n))) →
        finrank ℝ (vectorSpan ℝ (U : Set (V n))) ≤
          finrank ℝ (vectorSpan ℝ (T : Set (V n))) := by
  classical
  let C : Set (Finset (V n)) := {T | T ⊂ S ∧
    (0 : V n) ∈ intrinsicInterior ℝ (convexHull ℝ (T : Set (V n)))}
  have hC : C.Finite := S.powerset.finite_toSet.subset
    (fun T hT => Finset.mem_powerset.mpr hT.1.subset)
  obtain ⟨T, hT, hgood⟩ := exists_proper_goodSimplex_of_not_affineIndependent hS.1 hns
  obtain ⟨M, hM, hmax⟩ := Set.exists_max_image C
    (fun T => finrank ℝ (vectorSpan ℝ (T : Set (V n)))) hC ⟨T, hT, hgood.2⟩
  exact ⟨M, hM.1, hM.2, proper_relativeIP_dim_lt hS hM.1 hM.2,
    fun U hU h0 => hmax U ⟨hU, h0⟩⟩

/-- A relative-IP subset strictly extending the span of a maximal-dimensional
proper relative-IP subset must be the whole minimal generating set. -/
theorem maximal_relativeIP_forces_union {S M U : Finset (V n)}
    (_hMS : M ⊆ S)
    (hM0 : (0 : V n) ∈ intrinsicInterior ℝ (convexHull ℝ (M : Set (V n))))
    (hmax : ∀ T : Finset (V n), T ⊂ S →
      (0 : V n) ∈ intrinsicInterior ℝ (convexHull ℝ (T : Set (V n))) →
      finrank ℝ (vectorSpan ℝ (T : Set (V n))) ≤ finrank ℝ (vectorSpan ℝ (M : Set (V n))))
    (hUS : U ⊆ S)
    (hU0 : (0 : V n) ∈ intrinsicInterior ℝ (convexHull ℝ (U : Set (V n))))
    (hstrict : Submodule.span ℝ (M : Set (V n)) < Submodule.span ℝ (U : Set (V n))) :
    U = S := by
  classical
  by_contra hne
  have hbound := hmax U (Finset.ssubset_iff_subset_ne.mpr ⟨hUS, hne⟩) hU0
  rw [vectorSpan_eq_span_of_intrinsicInterior U hU0,
    vectorSpan_eq_span_of_intrinsicInterior M hM0] at hbound
  exact (Submodule.finrank_lt_finrank_of_lt hstrict).not_ge hbound

/-- Relative minimality is inherited by a relative-IP component whose
positive quotient relation completes a minimal full-dimensional set. -/
theorem relative_minimal_of_minimalIP_union {S M C : Finset (V n)}
    (hS : IsMinimalIPGen S) (hMC : M ∪ C = S) (hdisj : Disjoint M C)
    (hM0 : (0 : V n) ∈ intrinsicInterior ℝ (convexHull ℝ (M : Set (V n))))
    (q : V n → ℝ) (hq : ∀ x ∈ C, 0 < q x)
    (hcancel : -(∑ x ∈ C, q x • x) ∈ Submodule.span ℝ (M : Set (V n))) :
    IsMinimalIPRel M := by
  classical
  refine ⟨hM0, ?_⟩
  intro M' hM' hA h0
  obtain ⟨w', hw', hsum', hrel'⟩ := exists_positive_relation_of_intrinsicInterior M' h0
  have hM'ne : M'.Nonempty := M'.nonempty_iff_ne_empty.2 (by rintro rfl; simp at hsum')
  have hspan : Submodule.span ℝ (M' : Set (V n)) = Submodule.span ℝ (M : Set (V n)) := by
    rw [← vectorSpan_eq_span_of_intrinsicInterior M' h0,
      ← vectorSpan_eq_span_of_intrinsicInterior M hM0, ← direction_affineSpan,
      ← direction_affineSpan, hA]
  have hdisj' : Disjoint M' C := Finset.disjoint_of_subset_left hM'.subset hdisj
  obtain ⟨f, hf, hsumf, hrelf⟩ := positive_relation_union_of_cancel M' C hM'ne hdisj'
    w' hw' hrel' q hq (hspan ▸ hcancel)
  have htop : Submodule.span ℝ ((M' ∪ C : Finset (V n)) : Set (V n)) = ⊤ := by
    rw [Finset.coe_union, Submodule.span_union, hspan, ← Submodule.span_union,
      ← Finset.coe_union, hMC]
    exact BoxReduction.span_vertices_eq_top S hS.1
  have hint := finset_positive_relation_interior (M' ∪ C) f hf hsumf hrelf htop
  have hsub : M' ∪ C ⊆ S := by rw [← hMC]; exact Finset.union_subset_union_left hM'.subset
  have heq : M' ∪ C = S := by
    by_contra hne
    exact hS.2 (M' ∪ C) (Finset.ssubset_iff_subset_ne.mpr ⟨hsub, hne⟩) hint
  have hMsub : M ⊆ M' := by
    intro x hx
    have hxU : x ∈ M' ∪ C := by rw [heq, ← hMC]; exact Finset.mem_union_left C hx
    exact (Finset.mem_union.mp hxU).resolve_right
      (fun hxC => Finset.disjoint_left.mp hdisj hx hxC)
  exact hM'.ne (Finset.Subset.antisymm hM'.subset hMsub)

/-! ## L4.3: the non-simplicial case ([KS95] Lemma 1) — statement only -/

/-- A first Carathéodory reduction for the non-simplicial case.

If a full-dimensional IP generating set is not affinely independent, then the
origin already lies in the convex hull of a proper subset.  The full
Kreuzer--Skarke structure lemma strengthens this from membership in the hull
to a maximal relative-interior subset and then analyzes the quotient. -/
theorem exists_proper_subset_convexHull_of_not_affineIndependent {S : Finset (V n)}
    (hS : IsMinimalIPGen S)
    (hns : ¬ AffineIndependent ℝ ((↑) : ↥S → V n)) :
    ∃ S' : Finset (V n), S' ⊂ S ∧
      (0 : V n) ∈ convexHull ℝ (S' : Set (V n)) := by
  classical
  have h0conv : (0 : V n) ∈ convexHull ℝ (S : Set (V n)) :=
    interior_subset hS.1
  obtain ⟨y, hy⟩ := Caratheodory.mem_convexHull_erase hns h0conv
  exact ⟨S.erase y, Finset.erase_ssubset y.2, hy⟩

/-- **L4.3 ([KS95] Lemma 1), proved.**

A minimal IP generating set that is *not* a simplex splits: there are
`S' ⊂ S` and `T ⊆ S` with `S' ∪ T = S` such that `S'` generates a
lower-dimensional minimal polytope (relative minimality, in its own span),
`T` is a good simplex, and the cardinalities satisfy [KS95]'s
`k̄ − k̄' = n − n' + 1` with `dim T ≤ n'` (`n' = dim span S'`).

*Paper proof sketch ([KS95]):* choose `S'` maximal among proper subsets whose
hull still has `0` in its relative interior; pass to the quotient
`ℝⁿ / span(S')`; the image of `S` is a minimal IP set in the quotient with
trivial proper sub-minimal sets, hence a simplex by the maximality of `S'`;
pulling back its vertices and adjoining suitable points `R ⊆ S'` yields the
good simplex `T`; minimality forces the cardinality identity.

This statement is what generates *combined* weight systems (`CWS`): iterating
it gives the block decomposition (KS95 Cor. 1), one weight system per block.
The weight-system completeness theorem does not use this lemma, but coverage
of all reflexive polytopes also needs a complete equation family.
`CWSContainer.lean` supplies this via an independent basis of positive
simplex circuits and the exhaustive row-assembly specification. -/
theorem minimal_structure {S : Finset (V n)} (hS : IsMinimalIPGen S)
    (hns : ¬ AffineIndependent ℝ ((↑) : ↥S → V n)) :
    ∃ S' T : Finset (V n), S' ⊂ S ∧ T ⊆ S ∧ S' ∪ T = S ∧
      IsMinimalIPRel S' ∧ IsGoodSimplex T ∧
      S.card = S'.card + (n - finrank ℝ (vectorSpan ℝ ((S' : Set (V n))))) + 1 ∧
      finrank ℝ (vectorSpan ℝ ((T : Set (V n))))
        ≤ finrank ℝ (vectorSpan ℝ ((S' : Set (V n)))) := by
  classical
  obtain ⟨M, hMS, hM0, hMdim, hmax⟩ := exists_maximal_dim_proper_relativeIP hS hns
  obtain ⟨wM, hwM, hsumM, hrelM⟩ := exists_positive_relation_of_intrinsicInterior M hM0
  have hMne : M.Nonempty := M.nonempty_iff_ne_empty.2 (by rintro rfl; simp at hsumM)
  let K := Submodule.span ℝ (M : Set (V n))
  have hKM : vectorSpan ℝ (M : Set (V n)) = K :=
    vectorSpan_eq_span_of_intrinsicInterior M hM0
  have hKdim : finrank ℝ K < n := by rwa [hKM] at hMdim
  have hspanS : Submodule.span ℝ (S : Set (V n)) = ⊤ :=
    BoxReduction.span_vertices_eq_top S hS.1
  let d := finrank ℝ (V n ⧸ K)
  let e : (V n ⧸ K) ≃ₗ[ℝ] V d := (Module.finBasis ℝ (V n ⧸ K)).equivFun
  let p : V n →ₗ[ℝ] V d := e.toLinearMap.comp K.mkQ
  have hpsurj : Function.Surjective p := e.surjective.comp K.mkQ_surjective
  have hker : LinearMap.ker p = K := by
    simp only [p, LinearEquiv.ker_comp, Submodule.ker_mkQ]
  have hpM : ∀ x ∈ M, p x = 0 := by
    intro x hx
    exact LinearMap.mem_ker.mp (hker ▸ Submodule.subset_span hx)
  have ha : ∃ a ∈ S, a ∉ K := by
    by_contra! h
    have hle : Submodule.span ℝ (S : Set (V n)) ≤ K := Submodule.span_le.mpr h
    rw [hspanS] at hle
    have ht : K = ⊤ := top_le_iff.mp hle
    rw [ht, finrank_top, Module.finrank_fin_fun] at hKdim
    omega
  obtain ⟨a, haS, haK⟩ := ha
  have hpa : p a ≠ 0 := by
    intro hz
    exact haK (hker ▸ LinearMap.mem_ker.mpr hz)
  have h0proj : (0 : V d) ∈ interior
      (convexHull ℝ ((S.image p : Finset (V d)) : Set (V d))) := by
    rw [Finset.coe_image, ← LinearMap.image_convexHull]
    exact (LinearMap.isOpenMap_of_finiteDimensional p hpsurj).image_interior_subset
      (convexHull ℝ (S : Set (V n))) ⟨0, hS.1, map_zero p⟩
  obtain ⟨C, hCS, hCne, hiC, q, hq, hqsum, hqrel, hpC⟩ :=
    projected_positive_circuit S p h0proj a haS hpa
  have hdisj : Disjoint M C := Finset.disjoint_left.mpr
    (fun x hxM hxC => hpC x hxC (hpM x hxM))
  have hcancel : -(∑ x ∈ C, q x • x) ∈ K := by
    rw [← hker, LinearMap.mem_ker]
    simp only [map_neg, map_sum, map_smul, hqrel, neg_zero]
  obtain ⟨fU, hfU, hsumU, hrelU⟩ := positive_relation_union_of_cancel M C hMne hdisj
    wM hwM hrelM q hq hcancel
  have hU0 := finset_positive_relation_intrinsicInterior (M ∪ C) fU hfU hsumU hrelU
  obtain ⟨c, hcC⟩ := hCne
  have hcK : c ∉ K := fun hc => hpC c hcC (LinearMap.mem_ker.mp (hker ▸ hc))
  have hstrict : K < Submodule.span ℝ ((M ∪ C : Finset (V n)) : Set (V n)) :=
    SetLike.lt_iff_le_and_exists.2
      ⟨Submodule.span_mono (Finset.coe_subset.mpr Finset.subset_union_left),
        c, Submodule.subset_span (Finset.mem_union_right M hcC), hcK⟩
  have hMC : M ∪ C = S := maximal_relativeIP_forces_union hMS.subset hM0 hmax
    (Finset.union_subset hMS.subset hCS) hU0 hstrict
  have hMmin : IsMinimalIPRel M := relative_minimal_of_minimalIP_union hS hMC hdisj hM0 q hq hcancel
  -- A good simplex through one quotient generator necessarily contains all of C.
  obtain ⟨T, hTS, hcT, hiT, wT, hwT, hsumT, hrelT⟩ :=
    ip_positive_circuit_through S c (hCS hcC) hS.1
  let b : V n → ℝ := fun x => if x ∈ T then wT x else 0
  have hrelTp : ∑ x ∈ T, wT x • p x = 0 := by
    have h := congrArg p hrelT
    simpa only [map_sum, map_smul, map_zero] using h
  have hrelS : ∑ x ∈ S, b x • p x = 0 := by
    calc
      _ = ∑ x ∈ T, b x • p x := (Finset.sum_subset hTS
        (fun x _ hx => by simp [b, hx])).symm
      _ = ∑ x ∈ T, wT x • p x := Finset.sum_congr rfl (fun x hx => by simp [b, hx])
      _ = 0 := hrelTp
  have hbrel : ∑ x ∈ C, b x • p x = 0 := by
    rw [← hMC, Finset.sum_union hdisj] at hrelS
    have hMsum : ∑ x ∈ M, b x • p x = 0 := Finset.sum_eq_zero (fun x hx => by rw [hpM x hx, smul_zero])
    simpa only [hMsum, zero_add] using hrelS
  have hcoeff := projected_relation_coefficients C p hiC q hqsum hqrel b hbrel
  have hbsum : 0 < ∑ x ∈ C, b x := by
    have hbpos : 0 < b c := by simpa [b, hcT] using hwT c hcT
    have heq := hcoeff c hcC
    by_contra! hneg
    have hn := mul_nonpos_of_nonpos_of_nonneg hneg (hq c hcC).le
    linarith
  have hCT : C ⊆ T := by
    intro x hx
    have hpos : 0 < b x := (hcoeff x hx).symm ▸ mul_pos hbsum (hq x hx)
    by_contra hnot
    simp [b, hnot] at hpos
  have hT0 := finset_positive_relation_intrinsicInterior T wT hwT hsumT hrelT
  have hTproper : T ⊂ S := Finset.ssubset_iff_subset_ne.mpr ⟨hTS,
    fun heq => hns (heq ▸ hiT)⟩
  have hMT : M ∪ T = S := Finset.Subset.antisymm
    (Finset.union_subset hMS.subset hTS)
    (by rw [← hMC]; exact Finset.union_subset_union_right hCT)
  have hTdim := hmax T hTproper hT0
  -- The quotient circuit spans the quotient, giving the cardinality identity.
  have hrange : Set.range (fun x : C => p x) = ((C.image p : Finset (V d)) : Set (V d)) := by
    ext y
    simp only [Set.mem_range, Finset.mem_coe, Finset.mem_image, Subtype.exists, exists_prop]
  have hspanProj : Submodule.span ℝ ((C.image p : Finset (V d)) : Set (V d)) = ⊤ := by
    have hle : Submodule.map p (Submodule.span ℝ (S : Set (V n))) ≤
        Submodule.span ℝ ((C.image p : Finset (V d)) : Set (V d)) := by
      rw [Submodule.map_span]
      apply Submodule.span_le.mpr
      rintro _ ⟨x, hx, rfl⟩
      rw [← hMC] at hx
      rcases Finset.mem_union.mp hx with hxM | hxC
      · rw [hpM x hxM]; exact Submodule.zero_mem _
      · exact Submodule.subset_span (Finset.mem_image.mpr ⟨x, hxC, rfl⟩)
    rw [hspanS, Submodule.map_top, LinearMap.range_eq_top.mpr hpsurj] at hle
    exact top_le_iff.mp hle
  have hqsum' : ∑ x : C, q x = 1 := (Finset.sum_coe_sort C q).trans hqsum
  have hqrel' : ∑ x : C, q x • p x = 0 :=
    (Finset.sum_coe_sort C fun x => q x • p x).trans hqrel
  have hzeroProj : (0 : V d) ∈ convexHull ℝ ((C.image p : Finset (V d)) : Set (V d)) := by
    rw [← hrange]
    have h := (Finset.univ : Finset C).centerMass_mem_convexHull
      (w := fun x : C => q x) (z := fun x : C => p x)
      (s := Set.range (fun x : C => p x)) (fun x _ => (hq x x.property).le)
      (hqsum'.symm ▸ zero_lt_one) (fun x _ => Set.mem_range_self x)
    rwa [Finset.centerMass_eq_of_sum_1 _ _ hqsum', hqrel'] at h
  have hAproj : affineSpan ℝ ((C.image p : Finset (V d)) : Set (V d)) = ⊤ := by
    rw [affineSpan_eq_span_of_zero_mem_convexHull (C.image p) hzeroProj, hspanProj]
    rfl
  have hVproj : vectorSpan ℝ (Set.range (fun x : C => p x)) = ⊤ := by
    rw [← direction_affineSpan, hrange, hAproj]
    exact AffineSubspace.direction_top ℝ _ _
  letI : Nonempty C := ⟨⟨c, hcC⟩⟩
  have hcardC : C.card = d + 1 := by
    have h := hiC.finrank_vectorSpan_add_one
    rw [hVproj, finrank_top, Module.finrank_fin_fun, Fintype.card_coe] at h
    exact h.symm
  have hqdim : d + finrank ℝ (vectorSpan ℝ (M : Set (V n))) = n := by
    rw [hKM]
    exact (K.finrank_quotient_add_finrank.trans (Module.finrank_fin_fun ℝ))
  have hcardS : S.card = M.card + C.card := by
    rw [← hMC]
    exact Finset.card_union_of_disjoint hdisj
  refine ⟨M, T, hMS, hTS, hMT, hMmin, ⟨hiT, hT0⟩, ?_, hTdim⟩
  rw [hcardS, hcardC]
  omega

end Refpoly
