import Refpoly.KS.WeightEncoding
import Mathlib.LinearAlgebra.Matrix.Integer

namespace Refpoly.KS

/-- Clear rational denominators and then divide by the common natural gcd. -/
def rationalWeightCode {m : ℕ} (q : Fin m → ℚ) : WeightCode m :=
  let A : Matrix (Fin m) (Fin 1) ℚ := fun i _ => q i
  let w : WeightCode m := fun i => (A.num i 0).toNat
  fun i => w i / Finset.univ.gcd w

theorem rationalWeightCode_correct {m : ℕ} (q : Fin m → ℚ)
    (hq : ∀ i, 0 < q i) (hsum : ∑ i, q i = 1) :
    Finset.univ.gcd (rationalWeightCode q) = 1 ∧ normalized (rationalWeightCode q) = q := by
  let A : Matrix (Fin m) (Fin 1) ℚ := fun i _ => q i
  let w : WeightCode m := fun i => (A.num i 0).toNat
  have hD : (0 : ℚ) < A.den := by exact_mod_cast Nat.pos_of_ne_zero A.den_ne_zero
  have hn (i : Fin m) : 0 < A.num i 0 := by
    have h := A.num_div_den i 0
    change (A.num i 0 : ℚ) / A.den = q i at h
    have hp := (div_pos_iff_of_pos_right hD).mp (h.symm ▸ hq i)
    exact_mod_cast hp
  have hwcast (i : Fin m) : (w i : ℚ) = (A.num i 0 : ℚ) := by
    dsimp [w]
    rw [← Int.cast_natCast, Int.toNat_of_nonneg (hn i).le]
  have hwq (i : Fin m) : (w i : ℚ) = (A.den : ℚ) * q i := by
    have h := (div_eq_iff (ne_of_gt hD)).mp (A.num_div_den i 0)
    rw [hwcast]
    simpa [A, mul_comm] using h
  have hsumw : ((∑ i, w i : ℕ) : ℚ) = A.den := by
    simp only [Nat.cast_sum, hwq, ← Finset.mul_sum, hsum, mul_one]
  have hnormalized : normalized w = q := by
    ext i
    simp only [normalized, hsumw, hwq]
    exact mul_div_cancel_left₀ (q i) (ne_of_gt hD)
  have hex : ∃ i, w i ≠ 0 := by
    by_contra! h
    have hz : ∑ i, w i = 0 := Finset.sum_eq_zero (fun i _ => h i)
    rw [hz, Nat.cast_zero] at hsumw
    exact (ne_of_gt hD) hsumw.symm
  obtain ⟨i, hi⟩ := hex
  let g := Finset.univ.gcd w
  have hg : g ≠ 0 := by
    exact mt Finset.gcd_eq_zero_iff.mp (fun h => hi (h i (Finset.mem_univ i)))
  have hmul (j : Fin m) : g * (w j / g) = w j :=
    Nat.mul_div_cancel' (Finset.gcd_dvd (Finset.mem_univ j))
  have htotal : ∑ j, w j = g * ∑ j, w j / g := by
    rw [Finset.mul_sum]
    exact Finset.sum_congr rfl (fun j _ => (hmul j).symm)
  constructor
  · exact Finset.gcd_div_eq_one (Finset.mem_univ i) hi
  · suffices heq : normalized (rationalWeightCode q) = normalized w from heq.trans hnormalized
    ext j
    change ((w j / g : ℕ) : ℚ) / (∑ k, w k / g : ℕ) =
      (w j : ℚ) / (∑ k, w k : ℕ)
    conv_rhs => rw [htotal, ← hmul j]
    push_cast
    exact (mul_div_mul_left _ _ (by exact_mod_cast hg : (g : ℚ) ≠ 0)).symm

/-- IP and the actual rational weights are retained by primitive encoding. -/
theorem rationalWeightCode_primitiveIP {m : ℕ} (q : Fin m → ℚ)
    (hq : ∀ i, 0 < q i) (hsum : ∑ i, q i = 1)
    (hIP : IsIPWeightSystem (fun i => (q i : ℝ))) :
    PrimitiveIP (rationalWeightCode q) := by
  obtain ⟨hg, heq⟩ := rationalWeightCode_correct q hq hsum
  exact ⟨hg, heq.symm ▸ hIP⟩

end Refpoly.KS
