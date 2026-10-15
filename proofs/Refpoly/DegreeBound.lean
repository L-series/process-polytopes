/-
DegreeBound faithfully ported from the reviewed refpoly-5d snapshot.
See SOURCES.md for attribution and exact source identities.
The degree bound is existential; it supplies no executable cutoff.
-/
import Refpoly.WeightSystemFinite

/-! Primitive integral IP weight systems have a uniform degree bound.
This proves existence of a bound; it does not certify a sharp numerical value. -/

open Set Matrix Finset
namespace Refpoly

/-- Positive rescaling preserves the IP polytope and the IP property. -/
theorem isIPWeightSystem_smul_iff {n : ℕ} (q : V n) {c : ℝ} (hc : 0 < c) :
    IsIPWeightSystem (c • q) ↔ IsIPWeightSystem q := by
  have hW : wsHyperplane (c • q) = wsHyperplane q := by
    ext y
    simp [mem_wsHyperplane, smul_dotProduct, smul_eq_mul, hc.ne']
  unfold IsIPWeightSystem IsIPWeightSystemAt wsPolytopeAt wsLatticeGensAt
  simp only [Pi.smul_apply, smul_eq_mul, mul_pos_iff_of_pos_left hc]
  apply and_congr Iff.rfl
  exact iff_of_eq (congrArg (fun K : Submodule ℝ (V n) =>
    (0 : K) ∈ interior (convexHull ℝ
      {y : K | IsLatticePoint (y : V n) ∧ ∀ i, -(1 : ℝ) ≤ (y : V n) i})) hW)

/-- Normalize an integral weight system by its degree. -/
noncomputable def normalizedWeights {n : ℕ} (w : Fin n → ℕ) : V n :=
  fun i => (w i : ℝ) / (∑ j, w j : ℕ)

/-- Primitive weights are uniquely determined by their normalized values. -/
theorem normalizedWeights_injective_on_primitive {n : ℕ}
    {w u : Fin n → ℕ} (hw : Finset.univ.gcd w = 1) (hu : Finset.univ.gcd u = 1)
    (hwD : 0 < ∑ i, w i) (huD : 0 < ∑ i, u i)
    (heq : normalizedWeights w = normalizedWeights u) : w = u := by
  have hwD' : (∑ i, w i : ℕ) ≠ 0 := hwD.ne'
  have huD' : (∑ i, u i : ℕ) ≠ 0 := huD.ne'
  have hcross (i : Fin n) : (∑ j, u j) * w i = (∑ j, w j) * u i := by
    have h := congrFun heq i
    change (w i : ℝ) / (∑ j, w j : ℕ) = (u i : ℝ) / (∑ j, u j : ℕ) at h
    have hr := (div_eq_div_iff (by exact_mod_cast hwD') (by exact_mod_cast huD')).mp h
    exact_mod_cast (by nlinarith [hr] :
      ((∑ j, u j : ℕ) : ℝ) * w i = ((∑ j, w j : ℕ) : ℝ) * u i)
  have hdwu : (∑ i, w i) ∣ (∑ i, u i) := by
    have h : (∑ i, w i) ∣ Finset.univ.gcd (fun i => (∑ j, u j) * w i) :=
      Finset.dvd_gcd_iff.mpr (fun i _ => by rw [hcross i]; exact dvd_mul_right _ _)
    simpa [Finset.gcd_mul_left, hw] using h
  have hduw : (∑ i, u i) ∣ (∑ i, w i) := by
    have h : (∑ i, u i) ∣ Finset.univ.gcd (fun i => (∑ j, w j) * u i) :=
      Finset.dvd_gcd_iff.mpr (fun i _ => by rw [← hcross i]; exact dvd_mul_right _ _)
    simpa [Finset.gcd_mul_left, hu] using h
  have hd := Nat.dvd_antisymm hdwu hduw
  funext i
  have h := hcross i
  rw [← hd] at h
  exact Nat.eq_of_mul_eq_mul_left hwD h

/-- There are finitely many primitive integral IP weight systems in each
fixed length, without assuming any sharp degree bound. -/
theorem primitive_ip_weight_systems_finite (n : ℕ) :
    {w : Fin n → ℕ | (∀ i, 0 < w i) ∧ Finset.univ.gcd w = 1 ∧
      IsIPWeightSystem (fun i => (w i : ℝ))}.Finite := by
  classical
  let A : Set (Fin n → ℕ) := {w | (∀ i, 0 < w i) ∧ Finset.univ.gcd w = 1 ∧
    IsIPWeightSystem (fun i => (w i : ℝ))}
  have hdegree {w : Fin n → ℕ} (hw : w ∈ A) : 0 < ∑ i, w i := by
    have hne : (Finset.univ : Finset (Fin n)).Nonempty :=
      Finset.univ.nonempty_iff_ne_empty.2 fun h => by simpa [h] using hw.2.1
    exact Finset.sum_pos (fun i _ => hw.1 i) hne
  have himage : (normalizedWeights '' A).Finite := by
    apply (WeightSystemFinite.normalized_ip_weight_systems_finite n).subset
    rintro _ ⟨w, hw, rfl⟩
    have hD : 0 < ((∑ i, w i : ℕ) : ℝ) := by exact_mod_cast hdegree hw
    have hnorm : normalizedWeights w = ((∑ i, w i : ℕ) : ℝ)⁻¹ •
        (fun i => (w i : ℝ)) := by ext i; simp [normalizedWeights, div_eq_mul_inv, mul_comm]
    refine ⟨?_, ?_⟩
    · simp only [normalizedWeights, ← Finset.sum_div, ← Nat.cast_sum]
      exact div_self hD.ne'
    · rw [hnorm, isIPWeightSystem_smul_iff _ (inv_pos.mpr hD)]
      exact hw.2.2
  apply himage.of_finite_image
  intro w hw u hu heq
  exact normalizedWeights_injective_on_primitive hw.2.1 hu.2.1 (hdegree hw) (hdegree hu) heq

/-- Every fixed weight-system length has a uniform integral degree bound. -/
theorem primitive_ip_degrees_bounded (n : ℕ) :
    ∃ D : ℕ, ∀ w : Fin n → ℕ, (∀ i, 0 < w i) → Finset.univ.gcd w = 1 →
      IsIPWeightSystem (fun i => (w i : ℝ)) → ∑ i, w i ≤ D := by
  obtain ⟨D, hD⟩ := ((primitive_ip_weight_systems_finite n).image
    (fun w => ∑ i, w i)).bddAbove
  exact ⟨D, fun w hp hg hi => hD ⟨w, ⟨hp, hg, hi⟩, rfl⟩⟩

end Refpoly
