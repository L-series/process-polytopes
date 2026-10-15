import Refpoly.KS.RationalWeights
import Mathlib.LinearAlgebra.LinearIndependent.BaseChange

open Set Matrix
namespace Refpoly.KS

/-- An integral affine simplex has rational normalized zero-relation coefficients. -/
theorem exists_rational_simplex_weights {d m : ℕ} (v : Fin m → V d) (q : V m)
    (hlat : ∀ i, IsLatticePoint (v i)) (hind : AffineIndependent ℝ v)
    (hsum : ∑ i, q i = 1) (hrel : ∑ i, q i • v i = 0) :
    ∃ p : Fin m → ℚ, ∀ i, (p i : ℝ) = q i := by
  classical
  choose Z hZ using hlat
  let vQ : Fin m → Fin d → ℚ := fun i j => (Z i j : ℚ)
  have hv : (fun i => algebraMap ℚ ℝ ∘ vQ i) = v := by
    ext i j
    simpa [vQ] using (hZ i j).symm
  have hnreal : ¬LinearIndependent ℝ v := by
    intro hi
    have hz := (Fintype.linearIndependent_iff.mp hi) q hrel
    have : ∑ i, q i = 0 := Finset.sum_eq_zero (fun i _ => hz i)
    linarith
  have hnQ : ¬LinearIndependent ℚ vQ := by
    intro hi
    apply hnreal
    rw [← hv]
    exact linearIndependent_algebraMap_comp_iff.mpr hi
  obtain ⟨a, ha, i, hi⟩ := Fintype.not_linearIndependent_iff.mp hnQ
  have hreal : ∑ j, (a j : ℝ) • v j = 0 := by
    ext k
    have h := congrFun ha k
    simp only [Finset.sum_apply, Pi.smul_apply, smul_eq_mul, Pi.zero_apply] at h
    have hc := congrArg (fun t : ℚ => (t : ℝ)) h
    push_cast at hc
    simpa [← hv, Function.comp_def] using hc
  have htot : (∑ j, a j) ≠ 0 := by
    intro hz
    have hsumzero : ∑ j, (a j : ℝ) = 0 := by exact_mod_cast hz
    have hai := hind.eq_zero_of_sum_eq_zero hsumzero hreal i (Finset.mem_univ i)
    exact hi (by exact_mod_cast hai)
  let p : Fin m → ℚ := fun j => a j / ∑ k, a k
  have hpsum : ∑ j, p j = 1 := by
    dsimp [p]
    rw [← Finset.sum_div, div_self htot]
  have hpsumreal : ∑ j, (p j : ℝ) = 1 := by exact_mod_cast hpsum
  have hpreal : ∑ j, (p j : ℝ) • v j = 0 := by
    dsimp [p]
    push_cast
    simp only [div_eq_mul_inv, mul_comm _ ((∑ k, (a k : ℝ))⁻¹),
      mul_smul, ← Finset.smul_sum, hreal, smul_zero]
  exact ⟨p, fun j => hind.eq_of_sum_eq_sum (hpsumreal.trans hsum.symm)
    (hpreal.trans hrel.symm) j (Finset.mem_univ j)⟩

/-- Primitive encoding preserves the same geometric simplex weights, not an unrelated witness. -/
theorem exists_primitive_simplex_weights {d m : ℕ} (v : Fin m → V d) (q : V m)
    (hlat : ∀ i, IsLatticePoint (v i)) (hind : AffineIndependent ℝ v)
    (hsum : ∑ i, q i = 1) (hrel : ∑ i, q i • v i = 0)
    (hIP : IsIPWeightSystem q) :
    ∃ w : WeightCode m, PrimitiveIP w ∧ (fun i => (normalized w i : ℝ)) = q := by
  obtain ⟨p, hp⟩ := exists_rational_simplex_weights v q hlat hind hsum hrel
  have hpos : ∀ i, 0 < p i := by
    intro i
    have h := hIP.1 i
    rw [← hp i] at h
    exact_mod_cast h
  have hpsum : ∑ i, p i = 1 := by
    have h : ∑ i, (p i : ℝ) = 1 := by simpa only [hp] using hsum
    exact_mod_cast h
  have hpeq : (fun i => (p i : ℝ)) = q := funext hp
  refine ⟨rationalWeightCode p, rationalWeightCode_primitiveIP p hpos hpsum ?_, ?_⟩
  · rwa [hpeq]
  · rw [(rationalWeightCode_correct p hpos hpsum).2]
    exact hpeq

end Refpoly.KS
