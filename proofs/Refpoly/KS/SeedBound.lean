import Refpoly.KS.Contracts
import Refpoly.KS.Seeds

open Set Module

namespace Refpoly

private theorem origin_intrinsicInterior_image {n m : ℕ} (S : Finset (V n))
    (f : V n →ₗ[ℝ] V m) (g : V m → V n)
    (hgf : ∀ x ∈ S, g (f x) = x)
    (h0 : (0 : V n) ∈ intrinsicInterior ℝ (convexHull ℝ (S : Set (V n)))) :
    (0 : V m) ∈ intrinsicInterior ℝ
      (convexHull ℝ ((S.image f : Finset (V m)) : Set (V m))) := by
  classical
  have hinj : Set.InjOn f (S : Set (V n)) := by
    intro x hx y hy h
    simpa only [hgf x hx, hgf y hy] using congrArg g h
  obtain ⟨w, hw, hsum, hrel⟩ := exists_positive_relation_of_intrinsicInterior S h0
  apply finset_positive_relation_intrinsicInterior (S.image f) (fun y => w (g y))
  · intro y hy
    obtain ⟨x, hx, rfl⟩ := Finset.mem_image.mp hy
    rw [hgf x hx]
    exact hw x hx
  · rw [Finset.sum_image hinj]
    exact (Finset.sum_congr rfl (fun x hx => congrArg w (hgf x hx))).trans hsum
  · rw [Finset.sum_image hinj]
    calc
      (∑ x ∈ S, w (g (f x)) • f x) = ∑ x ∈ S, w x • f x :=
        Finset.sum_congr rfl (fun x hx => by rw [hgf x hx])
      _ = f (∑ x ∈ S, w x • x) := by simp only [map_sum, map_smul]
      _ = 0 := by rw [hrel, map_zero]

/-- Relative minimality becomes full-dimensional minimality in coordinates of
the component's span, including when that span has dimension zero. -/
theorem IsMinimalIPRel.exists_minimalIPGen_coordinates {n : ℕ}
    {M : Finset (V n)} (hM : IsMinimalIPRel M) :
    ∃ T : Finset (V (finrank ℝ (vectorSpan ℝ (M : Set (V n))))),
      IsMinimalIPGen T ∧ T.card = M.card := by
  classical
  rw [vectorSpan_eq_span_of_intrinsicInterior M hM.1]
  let K := Submodule.span ℝ (M : Set (V n))
  let r := finrank ℝ K
  let e : K ≃ₗ[ℝ] V r := (Module.finBasis ℝ K).equivFun
  let f : V r →ₗ[ℝ] V n := K.subtype.comp e.symm.toLinearMap
  have hfi : Function.Injective f := Subtype.val_injective.comp e.symm.injective
  obtain ⟨p, hp⟩ := f.exists_leftInverse_of_injective (LinearMap.ker_eq_bot.mpr hfi)
  have hpf : ∀ y, p (f y) = y := fun y => LinearMap.congr_fun hp y
  have hfK : ∀ x : K, f (e x) = (x : V n) := by
    intro x
    simp [f]
  have hfp : ∀ x ∈ K, f (p x) = x := by
    intro x hx
    have h := hpf (e ⟨x, hx⟩)
    rw [hfK] at h
    exact (congrArg f h).trans (hfK ⟨x, hx⟩)
  have hpinj : Set.InjOn p (M : Set (V n)) := by
    intro x hx y hy h
    calc
      x = f (p x) := (hfp x (Submodule.subset_span hx)).symm
      _ = f (p y) := congrArg f h
      _ = y := hfp y (Submodule.subset_span hy)
  have hrangef : LinearMap.range f = K := by
    ext x
    constructor
    · rintro ⟨y, rfl⟩
      exact (e.symm y).property
    · intro hx
      exact ⟨e ⟨x, hx⟩, hfK ⟨x, hx⟩⟩
  let T := M.image p
  have hTI : T.image f = M := by
    calc
      T.image f = M.image (fun x => x) := by
        rw [Finset.image_image]
        exact Finset.image_congr (fun x hx => hfp x (Submodule.subset_span hx))
      _ = M := Finset.image_id'
  have hspanT : Submodule.span ℝ (T : Set (V r)) = ⊤ := by
    dsimp only [T]
    rw [Finset.coe_image, ← Submodule.map_span]
    apply top_le_iff.mp
    intro y _
    exact ⟨f y, (e.symm y).property, hpf y⟩
  have h0T : (0 : V r) ∈ intrinsicInterior ℝ (convexHull ℝ (T : Set (V r))) :=
    origin_intrinsicInterior_image M p f
      (fun x hx => hfp x (Submodule.subset_span hx)) hM.1
  have hAT : affineSpan ℝ (T : Set (V r)) = ⊤ := by
    rw [affineSpan_eq_span_of_zero_mem_convexHull T (intrinsicInterior_subset h0T), hspanT]
    rfl
  refine ⟨T, ⟨mem_interior_of_mem_intrinsicInterior
    (by rwa [affineSpan_convexHull]) h0T, ?_⟩, Finset.card_image_of_injOn hpinj⟩
  intro U hU hU0
  have hUrel := origin_intrinsicInterior_image U f p (fun x _ => hpf x)
    (interior_subset_intrinsicInterior hU0)
  have hproper : U.image f ⊂ M := by
    rw [← hTI]
    exact (Finset.image_ssubset_image hfi).mpr hU
  have hspanU : Submodule.span ℝ ((U.image f : Finset (V n)) : Set (V n)) = K := by
    rw [Finset.coe_image, ← Submodule.map_span,
      BoxReduction.span_vertices_eq_top U hU0, Submodule.map_top, hrangef]
  have hAU : affineSpan ℝ ((U.image f : Finset (V n)) : Set (V n)) =
      affineSpan ℝ (M : Set (V n)) := by
    rw [affineSpan_eq_span_of_zero_mem_convexHull (U.image f)
      (intrinsicInterior_subset hUrel),
      affineSpan_eq_span_of_zero_mem_convexHull M (intrinsicInterior_subset hM.1), hspanU]
  exact hM.2 (U.image f) hproper hAU hUrel

/-- A vertex-minimal origin-IP generating set in positive dimension has at
most twice the ambient dimension many points. -/
theorem minimalIPGen_card_upper {d : ℕ} (hd : 0 < d) {S : Finset (V d)}
    (hS : IsMinimalIPGen S) : S.card ≤ 2 * d := by
  classical
  induction d using Nat.strong_induction_on with
  | h d ih =>
    by_cases hind : AffineIndependent ℝ ((↑) : S → V d)
    · have hc := simplex_card hind hS.1
      omega
    · obtain ⟨M, T, hMS, _, hunion, hM, hT, hcard, hTdim⟩ :=
        minimal_structure hS hind
      have hrlt := proper_relativeIP_dim_lt hS hMS hM.1
      have hMspan := vectorSpan_eq_span_of_intrinsicInterior M hM.1
      have hTspan := vectorSpan_eq_span_of_intrinsicInterior T hT.2
      have hdim := Submodule.finrank_sup_add_finrank_inf_eq
        (Submodule.span ℝ (M : Set (V d))) (Submodule.span ℝ (T : Set (V d)))
      rw [← Submodule.span_union, ← Finset.coe_union, hunion,
        BoxReduction.span_vertices_eq_top S hS.1, finrank_top,
        Module.finrank_fin_fun, ← hMspan, ← hTspan] at hdim
      have hdim_le : d ≤ finrank ℝ (vectorSpan ℝ (M : Set (V d))) +
          finrank ℝ (vectorSpan ℝ (T : Set (V d))) :=
        (Nat.le_add_right d _).trans_eq hdim
      have hrpos : 0 < finrank ℝ (vectorSpan ℝ (M : Set (V d))) := by omega
      obtain ⟨M', hM', hMc⟩ := hM.exists_minimalIPGen_coordinates
      have hbound := ih _ hrlt hrpos hM'
      rw [hMc] at hbound
      omega

namespace KS

/-- The audited seed contract, with its general cardinality upper bound. -/
theorem seed_bound_proved : SeedBound := by
  intro d hd P hP
  obtain ⟨T, hlat, hdual, hmin, _⟩ := classification_coverage hP
  exact ⟨seedOfFinset T,
    validSeed_of_minimal T hlat hdual hmin (minimalIPGen_card_upper hd hmin)⟩

end KS
end Refpoly
