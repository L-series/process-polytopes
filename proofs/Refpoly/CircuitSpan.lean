/-
CircuitSpan faithfully ported from the reviewed refpoly-5d snapshot.
See SOURCES.md for attribution and exact source identities.
-/
import Refpoly.CircuitInterior

/-! Positive simplex circuits span every linear relation of an IP set. -/
open Set Finset Module
namespace Refpoly

/-- Coordinates of a supported weight vector in the coefficient space of S. -/
noncomputable def supportedWeights {n : ℕ} (S T : Finset (V n)) (w : V n → ℝ) : S → ℝ :=
  fun x => if (x : V n) ∈ T then w x else 0

/-- Normalized positive simplex relations, extended by zero to all coordinates. -/
def simplexCircuitWeights {n : ℕ} (S : Finset (V n)) : Set (S → ℝ) :=
  {q | ∃ T : Finset (V n), T ⊆ S ∧ AffineIndependent ℝ ((↑) : T → V n) ∧
    ∃ w : V n → ℝ, (∀ x ∈ T, 0 < w x) ∧ (∑ x ∈ T, w x = 1) ∧
      (∑ x ∈ T, w x • x = 0) ∧ q = supportedWeights S T w}

/-- Move a strictly positive normalized relation to the boundary of its
coefficient simplex along a nonzero sum-zero relation. -/
private theorem relation_boundary {n : ℕ} (T : Finset (V n))
    (f g : V n → ℝ) (hf : ∀ x ∈ T, 0 < f x)
    (hfsum : ∑ x ∈ T, f x = 1) (hfrel : ∑ x ∈ T, f x • x = 0)
    (hgsum : ∑ x ∈ T, g x = 0) (hgrel : ∑ x ∈ T, g x • x = 0)
    (hgne : ∃ x ∈ T, g x ≠ 0) :
    ∃ b : V n, b ∈ T ∧ ∃ t : ℝ, 0 < t ∧
      (∀ x ∈ T, 0 ≤ f x - t * g x) ∧ f b - t * g b = 0 ∧
      (∑ x ∈ (T.erase b), (f x - t * g x) = 1) ∧
      ∑ x ∈ (T.erase b), (f x - t * g x) • x = 0 := by
  obtain ⟨b, hb, hgb, t, -, hbzero, hnonneg⟩ := caratheodory_step T f g
    (fun x hx => (hf x hx).le) (exists_pos_of_sum_zero_of_exists_nonzero g hgsum hgne)
  have ht : 0 < t := by nlinarith [hf b hb]
  refine ⟨b, hb, t, ht, hnonneg, hbzero, ?_, ?_⟩
  · rw [Finset.sum_erase_eq_sub hb, hbzero, sub_zero]
    simp only [Finset.sum_sub_distrib, ← Finset.mul_sum, hfsum, hgsum, mul_zero, sub_zero]
  · rw [Finset.sum_erase_eq_sub hb, hbzero, zero_smul, sub_zero]
    simp only [sub_smul, mul_smul, Finset.sum_sub_distrib, ← Finset.smul_sum,
      hfrel, hgrel, smul_zero, sub_zero]

/-- Every normalized nonnegative relation is in the span of positive circuits. -/
theorem normalized_relation_mem_span_circuits {n : ℕ} (S T : Finset (V n))
    (hTS : T ⊆ S) (f : V n → ℝ) (hf : ∀ x ∈ T, 0 ≤ f x)
    (hfsum : ∑ x ∈ T, f x = 1) (hfrel : ∑ x ∈ T, f x • x = 0) :
    supportedWeights S T f ∈ Submodule.span ℝ (simplexCircuitWeights S) := by
  classical
  induction T using Finset.strongInductionOn generalizing f
  rename_i T ih
  by_cases hpos : ∀ x ∈ T, 0 < f x
  · by_cases hi : AffineIndependent ℝ ((↑) : T → V n)
    · exact Submodule.subset_span ⟨T, hTS, hi, f, hpos, hfsum, hfrel, rfl⟩
    · obtain ⟨g, hgrel, hgsum, hgne⟩ :=
        exists_nontrivial_relation_sum_zero_of_not_affine_ind hi
      obtain ⟨b, hb, t, ht, hminus, hbzero, hminusSum, hminusRel⟩ :=
        relation_boundary T f g hpos hfsum hfrel hgsum hgrel hgne
      have hngsum : ∑ x ∈ T, (-g) x = 0 := by simp [Pi.neg_apply, hgsum]
      have hngrel : ∑ x ∈ T, (-g) x • x = 0 := by simp [Pi.neg_apply, neg_smul, hgrel]
      have hngne : ∃ x ∈ T, (-g) x ≠ 0 := by
        obtain ⟨x,hx,h⟩ := hgne; exact ⟨x,hx,by simpa using h⟩
      obtain ⟨c, hc, s, hs, hplus, hczero, hplusSum, hplusRel⟩ :=
        relation_boundary T f (-g) hpos hfsum hfrel hngsum hngrel hngne
      let fm : V n → ℝ := fun x => f x - t * g x
      let fp : V n → ℝ := fun x => f x - s * (-g) x
      have hm := ih (T.erase b) (Finset.erase_ssubset hb)
        ((Finset.erase_subset _ _).trans hTS) fm
        (fun x hx => hminus x (Finset.mem_of_mem_erase hx)) hminusSum hminusRel
      have hp := ih (T.erase c) (Finset.erase_ssubset hc)
        ((Finset.erase_subset _ _).trans hTS) fp
        (fun x hx => hplus x (Finset.mem_of_mem_erase hx)) hplusSum hplusRel
      have hmeq : supportedWeights S (T.erase b) fm = supportedWeights S T fm := by
        funext x
        by_cases hxb : (x : V n) = b
        · subst b
          have hh : fm (x : V n) = 0 := hbzero
          simp [supportedWeights, hb, hh]
        · simp [supportedWeights, Finset.mem_erase, hxb]
      have hpeq : supportedWeights S (T.erase c) fp = supportedWeights S T fp := by
        funext x
        by_cases hxc : (x : V n) = c
        · subst c
          have hh : fp (x : V n) = 0 := hczero
          simp [supportedWeights, hc, hh]
        · simp [supportedWeights, Finset.mem_erase, hxc]
      rw [hmeq] at hm
      rw [hpeq] at hp
      have hidentity : supportedWeights S T f =
          (s/(t+s)) • supportedWeights S T fm + (t/(t+s)) • supportedWeights S T fp := by
        funext x
        by_cases hx : (x : V n) ∈ T
        · simp only [supportedWeights, hx, if_true, Pi.add_apply, Pi.smul_apply,
            smul_eq_mul, fm, fp, Pi.neg_apply]
          have hden : t+s ≠ 0 := ne_of_gt (add_pos ht hs)
          field_simp
          ring
        · simp [supportedWeights, hx]
      rw [hidentity]
      exact Submodule.add_mem _ (Submodule.smul_mem _ _ hm) (Submodule.smul_mem _ _ hp)
  · push Not at hpos
    obtain ⟨b, hb, hfb⟩ := hpos
    have hzero : f b = 0 := le_antisymm hfb (hf b hb)
    have hsum : ∑ x ∈ T.erase b, f x = 1 := by
      rw [Finset.sum_erase_eq_sub hb, hzero, sub_zero]; exact hfsum
    have hrel : ∑ x ∈ T.erase b, f x • x = 0 := by
      rw [Finset.sum_erase_eq_sub hb, hzero, zero_smul, sub_zero]; exact hfrel
    have h := ih (T.erase b) (Finset.erase_ssubset hb)
      ((Finset.erase_subset _ _).trans hTS) f
      (fun x hx => hf x (Finset.mem_of_mem_erase hx)) hsum hrel
    have heq : supportedWeights S (T.erase b) f = supportedWeights S T f := by
      funext x
      by_cases hxb : (x : V n) = b
      · subst b; simp [supportedWeights, hzero, hb]
      · simp [supportedWeights, Finset.mem_erase, hxb]
    rwa [heq] at h
/-- Linear combination of the generators with the given coefficient vector. -/
noncomputable def vertexCombination {n : ℕ} (S : Finset (V n)) : (S → ℝ) →ₗ[ℝ] V n :=
  ∑ x : S, (LinearMap.proj x).smulRight (x : V n)

@[simp] theorem vertexCombination_apply {n : ℕ} (S : Finset (V n)) (q : S → ℝ) :
    vertexCombination S q = ∑ x : S, q x • (x : V n) := by
  simp [vertexCombination]

/-- A supported sum computed in S is the original sum computed in T. -/
theorem supportedWeights_combination {n : ℕ} (S T : Finset (V n)) (hTS : T ⊆ S)
    (w : V n → ℝ) : vertexCombination S (supportedWeights S T w) = ∑ x ∈ T, w x • x := by
  classical
  rw [vertexCombination_apply]
  simp only [supportedWeights, ite_smul, zero_smul]
  rw [Finset.sum_coe_sort S fun x => if x ∈ T then w x • x else 0]
  calc
    _ = ∑ x ∈ T, (if x ∈ T then w x • x else 0) :=
      (Finset.sum_subset hTS (fun x _ hx => by simp [hx])).symm
    _ = ∑ x ∈ T, w x • x := Finset.sum_congr rfl (fun x hx => by simp [hx])

/-- All nonnegative relations are in the span of positive simplex circuits. -/
theorem nonnegative_relation_mem_span_circuits {n : ℕ} (S : Finset (V n))
    (f : S → ℝ) (hf : ∀ x, 0 ≤ f x) (hrel : vertexCombination S f = 0) :
    f ∈ Submodule.span ℝ (simplexCircuitWeights S) := by
  classical
  let D := ∑ x : S, f x
  have hD : 0 ≤ D := Finset.sum_nonneg (fun x _ => hf x)
  by_cases hDz : D = 0
  · have hfzero : f = 0 := by
      funext x
      exact Finset.sum_eq_zero_iff_of_nonneg (fun y _ => hf y) |>.mp hDz x (Finset.mem_univ x)
    rw [hfzero]; exact Submodule.zero_mem _
  · have hDp : 0 < D := lt_of_le_of_ne hD (Ne.symm hDz)
    let w : V n → ℝ := fun x => if h : x ∈ S then f ⟨x,h⟩ / D else 0
    have hw : ∀ x ∈ S, 0 ≤ w x := by
      intro x hx; simp only [w, dif_pos hx]; exact div_nonneg (hf _) hD
    have hsum : ∑ x ∈ S, w x = 1 := by
      rw [← Finset.sum_attach (f := w)]
      simp only [w, Finset.coe_mem, dif_pos, ← Finset.sum_div]
      exact div_self hDz
    have hrelw : ∑ x ∈ S, w x • x = 0 := by
      rw [← Finset.sum_attach (f := fun x : V n => w x • x)]
      simp only [w, Finset.coe_mem, dif_pos, div_eq_mul_inv, mul_comm _ D⁻¹,
        mul_smul, ← Finset.smul_sum]
      rw [← Finset.univ_eq_attach]
      rw [vertexCombination_apply] at hrel
      simpa only [smul_zero] using congrArg (fun v : V n => D⁻¹ • v) hrel
    have h := normalized_relation_mem_span_circuits S S (Finset.Subset.refl _) w hw hsum hrelw
    have heq : D • supportedWeights S S w = f := by
      funext x
      simp only [Pi.smul_apply, smul_eq_mul, supportedWeights, x.property, if_true, w, dif_pos]
      field_simp
    rw [← heq]
    exact Submodule.smul_mem _ _ h

/-- Positive simplex circuits span the entire relation kernel of an IP set. -/
theorem simplex_circuits_span_kernel {n : ℕ} (S : Finset (V n))
    (h0 : (0 : V n) ∈ intrinsicInterior ℝ (convexHull ℝ (S : Set (V n)))) :
    Submodule.span ℝ (simplexCircuitWeights S) = LinearMap.ker (vertexCombination S) := by
  classical
  apply le_antisymm
  · apply Submodule.span_le.mpr
    rintro q ⟨T, hTS, _, w, _, _, hrel, rfl⟩
    change vertexCombination S (supportedWeights S T w) = 0
    rw [supportedWeights_combination S T hTS w, hrel]
  · obtain ⟨w, hw, _, hrelw⟩ := exists_positive_relation_of_intrinsicInterior S h0
    let wS : S → ℝ := fun x => w x
    have hwS : ∀ x : S, 0 < wS x := fun x => hw x x.property
    have hwrel : vertexCombination S wS = 0 := by
      rw [vertexCombination_apply]
      exact (Finset.sum_coe_sort S fun x => w x • x).trans hrelw
    have hwspan := nonnegative_relation_mem_span_circuits S wS (fun x => (hwS x).le) hwrel
    intro f hf
    obtain ⟨N, hN⟩ := (Set.finite_range (fun x : S => -f x / wS x)).bddAbove
    let g : S → ℝ := f + N • wS
    have hg : ∀ x, 0 ≤ g x := by
      intro x
      have hb := (div_le_iff₀ (hwS x)).mp (hN ⟨x,rfl⟩)
      change 0 ≤ f x + N * wS x
      linarith
    have hgrel : vertexCombination S g = 0 := by
      rw [map_add, map_smul, LinearMap.mem_ker.mp hf, hwrel, smul_zero, add_zero]
    have hgspan := nonnegative_relation_mem_span_circuits S g hg hgrel
    have heq : f = g - N • wS := by simp [g]
    rw [heq]
    exact Submodule.sub_mem _ hgspan (Submodule.smul_mem _ _ hwspan)

end Refpoly
