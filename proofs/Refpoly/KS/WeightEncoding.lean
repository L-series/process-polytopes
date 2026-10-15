import Refpoly.DegreeBound
import Refpoly.KS.Model

open Set Matrix
namespace Refpoly.KS

/-- Primitive natural codes have nonzero total weight, including empty arities. -/
theorem weight_degree_pos {m : ℕ} {w : WeightCode m} (hg : Finset.univ.gcd w = 1) :
    0 < ∑ i, w i := by
  by_contra hn
  have hz : ∑ i, w i = 0 := Nat.eq_zero_of_not_pos hn
  have hw : w = 0 := by
    funext i
    exact Finset.sum_eq_zero_iff.mp hz i (Finset.mem_univ i)
  have hgz : Finset.univ.gcd w = 0 := Finset.gcd_eq_zero_iff.mpr (by simp [hw])
  omega

/-- The rational code normalization is exactly the donor real normalization. -/
theorem normalized_cast_eq {m : ℕ} (w : WeightCode m) :
    (fun i => (normalized w i : ℝ)) = normalizedWeights w := by
  ext i
  simp [normalized, normalizedWeights]

/-- Scaling transport connects the new primitive predicate to the donor predicate. -/
theorem primitiveIP_iff {m : ℕ} (w : WeightCode m) :
    PrimitiveIP w ↔ Finset.univ.gcd w = 1 ∧ IsIPWeightSystem (fun i => (w i : ℝ)) := by
  unfold PrimitiveIP
  constructor
  · rintro ⟨hg, hi⟩
    refine ⟨hg, ?_⟩
    have hD : (0 : ℝ) < (∑ i, w i : ℕ) := by exact_mod_cast weight_degree_pos hg
    have heq : (fun i => (normalized w i : ℝ)) =
        ((∑ i, w i : ℕ) : ℝ)⁻¹ • (fun i => (w i : ℝ)) := by
      ext i
      simp [normalized, div_eq_mul_inv, mul_comm]
    rw [heq] at hi
    exact (isIPWeightSystem_smul_iff _ (inv_pos.mpr hD)).mp hi
  · rintro ⟨hg, hi⟩
    refine ⟨hg, ?_⟩
    have hD : (0 : ℝ) < (∑ i, w i : ℕ) := by exact_mod_cast weight_degree_pos hg
    have heq : (fun i => (normalized w i : ℝ)) =
        ((∑ i, w i : ℕ) : ℝ)⁻¹ • (fun i => (w i : ℝ)) := by
      ext i
      simp [normalized, div_eq_mul_inv, mul_comm]
    rw [heq]
    exact (isIPWeightSystem_smul_iff _ (inv_pos.mpr hD)).mpr hi

theorem primitiveIP_positive {m : ℕ} {w : WeightCode m} (hw : PrimitiveIP w) :
    ∀ i, 0 < w i := by
  intro i
  have h := ((primitiveIP_iff w).mp hw).2.1 i
  change (0 : ℝ) < (w i : ℝ) at h
  exact_mod_cast h

/-- This is abstract finiteness, not an executable stopping rule. -/
theorem primitiveIP_finite (m : ℕ) : {w : WeightCode m | PrimitiveIP w}.Finite := by
  apply (primitive_ip_weight_systems_finite m).subset
  intro w hw
  exact ⟨primitiveIP_positive hw, (primitiveIP_iff w).mp hw⟩

/-- The donor degree theorem transports exactly, without supplying a cutoff. -/
theorem primitiveIP_degrees_bounded (m : ℕ) :
    ∃ D : ℕ, ∀ w : WeightCode m, PrimitiveIP w → ∑ i, w i ≤ D := by
  obtain ⟨D, hD⟩ := primitive_ip_degrees_bounded m
  exact ⟨D, fun w hw => hD w (primitiveIP_positive hw)
    ((primitiveIP_iff w).mp hw).1 ((primitiveIP_iff w).mp hw).2⟩

/-- Exact support values are preserved by the natural-code padding operation. -/
theorem pad_on_support {m N : ℕ} (e : Fin m ↪ Fin N) (w : WeightCode m) (i : Fin m) :
    pad e w (e i) = w i := by
  simp only [pad, e.injective.eq_iff]
  rw [Finset.filter_eq' ]
  simp

theorem pad_off_support {m N : ℕ} (e : Fin m ↪ Fin N) (w : WeightCode m)
    (j : Fin N) (hj : ∀ i, e i ≠ j) : pad e w j = 0 := by
  simp [pad, hj]

theorem sum_pad {m N : ℕ} (e : Fin m ↪ Fin N) (w : WeightCode m) :
    ∑ j, pad e w j = ∑ i, w i := by
  simp only [pad, Finset.sum_filter]
  rw [Finset.sum_comm]
  simp

theorem pad_injective {m N : ℕ} (e : Fin m ↪ Fin N) :
    Function.Injective (pad e) := by
  intro w u h
  funext i
  have hi := congrFun h (e i)
  simpa only [pad_on_support] using hi

/-- Zero padding preserves the geometric weighted relation exactly. -/
theorem pad_relation {m N d : ℕ} (e : Fin m ↪ Fin N) (w : WeightCode m)
    (v : Fin N → V d) :
    ∑ j, (pad e w j : ℝ) • v j = ∑ i, (w i : ℝ) • v (e i) := by
  simp only [pad, Nat.cast_sum, Finset.sum_smul, Finset.sum_filter]
  rw [Finset.sum_comm]
  simp

/-- Normalized geometric relations use the same support and the same total weight. -/
theorem normalized_pad_relation {m N d : ℕ} (e : Fin m ↪ Fin N) (w : WeightCode m)
    (v : Fin N → V d) :
    ∑ j, (normalized (pad e w) j : ℝ) • v j =
      ∑ i, (normalized w i : ℝ) • v (e i) := by
  simp only [normalized, sum_pad]
  push_cast
  simp only [div_eq_mul_inv, mul_comm _ ((∑ i, (w i : ℝ))⁻¹),
    mul_smul, ← Finset.smul_sum]
  rw [pad_relation]

end Refpoly.KS
