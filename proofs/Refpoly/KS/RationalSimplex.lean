import Refpoly.KS.CircuitWeights
import Refpoly.KS.IntegerLinear

open Set Matrix

namespace Refpoly.KS

def homogeneousColumns {d m : ℕ} (v : Fin m → IntPoint d) :
    Matrix (Option (Fin d)) (Fin m) ℤ :=
  fun j i => j.elim 1 (fun k => v i k)

def simplexGram {d m : ℕ} (v : Fin m → IntPoint d) : Matrix (Fin m) (Fin m) ℤ :=
  (homogeneousColumns v)ᵀ * homogeneousColumns v

def simplexNumerators {d m : ℕ} (v : Fin m → IntPoint d) : Fin m → ℤ :=
  (simplexGram v).adjugate *ᵥ ((homogeneousColumns v)ᵀ *ᵥ Pi.single none 1)

def simplexWeights {d m : ℕ} (v : Fin m → IntPoint d) : Fin m → ℚ :=
  fun i => (simplexNumerators v i : ℚ) / (simplexGram v).det

def positiveSimplexCheck {d m : ℕ} (v : Fin m → IntPoint d) : Bool :=
  decide ((simplexGram v).det ≠ 0 ∧ (∀ i, 0 < simplexWeights v i) ∧
    (∑ i, simplexWeights v i) = 1 ∧
    ∀ j, (∑ i, simplexWeights v i * (v i j : ℚ)) = 0)

theorem homogeneous_injective_iff {d m : ℕ} (v : Fin m → IntPoint d) :
    Function.Injective
        (Matrix.mulVecLin ((homogeneousColumns v).map (Int.castRingHom ℝ))) ↔
      AffineIndependent ℝ (fun i => ofInt (v i)) := by
  let H := (homogeneousColumns v).map (Int.castRingHom ℝ)
  constructor
  · intro hi
    apply (affineIndependent_iff_of_fintype ℝ _).mpr
    intro w hsum hweighted i
    have hrel : ∑ i, w i • ofInt (v i) = 0 := by
      rwa [Finset.univ.weightedVSub_eq_linear_combination hsum] at hweighted
    have hz : Matrix.mulVecLin H w = 0 := by
      ext j
      cases j with
      | none => simpa [H, homogeneousColumns, Matrix.mulVec, dotProduct] using hsum
      | some j =>
        have h := congrFun hrel j
        simpa [H, homogeneousColumns, ofInt, Matrix.mulVec, dotProduct,
          Finset.sum_apply, Pi.smul_apply, smul_eq_mul, mul_comm] using h
    have hw : w = 0 := hi (hz.trans (map_zero (Matrix.mulVecLin H)).symm)
    exact congrFun hw i
  · intro hi
    apply LinearMap.ker_eq_bot.mp
    apply LinearMap.ker_eq_bot'.mpr
    intro w hw
    have hsum : ∑ i, w i = 0 := by
      have h := congrFun hw none
      simpa [H, homogeneousColumns, Matrix.mulVec, dotProduct] using h
    have hrel : ∑ i, w i • ofInt (v i) = 0 := by
      ext j
      have h := congrFun hw (some j)
      simpa [H, homogeneousColumns, ofInt, Matrix.mulVec, dotProduct,
        Finset.sum_apply, Pi.smul_apply, smul_eq_mul, mul_comm] using h
    exact funext fun i => hi.eq_zero_of_sum_eq_zero hsum hrel i (Finset.mem_univ i)

theorem simplexGram_det_iff {d m : ℕ} (v : Fin m → IntPoint d) :
    (simplexGram v).det ≠ 0 ↔ AffineIndependent ℝ (fun i => ofInt (v i)) :=
  (integer_gram_det_ne_zero_iff (homogeneousColumns v)).trans
    (homogeneous_injective_iff v)

theorem simplexWeights_unique {d m : ℕ} (v : Fin m → IntPoint d) (q : V m)
    (hi : AffineIndependent ℝ (fun i => ofInt (v i)))
    (hsum : ∑ i, q i = 1) (hrel : ∑ i, q i • ofInt (v i) = 0) :
    ∀ i, (simplexWeights v i : ℝ) = q i := by
  let H := (homogeneousColumns v).map (Int.castRingHom ℝ)
  let G := (simplexGram v).map (Int.castRingHom ℝ)
  have hH : Matrix.mulVecLin H q = Pi.single none 1 := by
    ext j
    cases j with
    | none => simpa [H, homogeneousColumns, Matrix.mulVec, dotProduct] using hsum
    | some j =>
      have h := congrFun hrel j
      simpa [H, homogeneousColumns, ofInt, Matrix.mulVec, dotProduct,
        Finset.sum_apply, Pi.smul_apply, smul_eq_mul, mul_comm] using h
  have hG : G = Hᵀ * H := by
    ext i j
    simp [G, H, simplexGram, Matrix.mul_apply]
  have hdet : ((simplexGram v).det : ℝ) ≠ 0 := by
    exact_mod_cast (simplexGram_det_iff v).mpr hi
  have hn : ∀ i, (simplexNumerators v i : ℝ) = (simplexGram v).det * q i := by
    have h := congrArg (fun x => G.adjugate *ᵥ (Hᵀ *ᵥ x)) hH
    change G.adjugate *ᵥ (Hᵀ *ᵥ (H *ᵥ q)) =
      G.adjugate *ᵥ (Hᵀ *ᵥ Pi.single none 1) at h
    rw [Matrix.mulVec_mulVec q Hᵀ H, ← hG, Matrix.mulVec_mulVec q G.adjugate G,
      Matrix.adjugate_mul] at h
    intro i
    have hc := congrFun h i
    have hmap : G.adjugate = (simplexGram v).adjugate.map (Int.castRingHom ℝ) := by
      exact ((Int.castRingHom ℝ).map_adjugate (simplexGram v)).symm
    rw [hmap] at hc
    have hd : G.det = ((simplexGram v).det : ℝ) :=
      ((Int.castRingHom ℝ).map_det (simplexGram v)).symm
    rw [hd] at hc
    simp only [Matrix.smul_mulVec, Matrix.one_mulVec, Pi.smul_apply, smul_eq_mul] at hc
    simpa [simplexNumerators, H, Matrix.mulVec, dotProduct] using hc.symm
  intro i
  simp only [simplexWeights, Rat.cast_div, Rat.cast_intCast, hn]
  exact mul_div_cancel_left₀ (q i) hdet

theorem positiveSimplexCheck_iff {d m : ℕ} (v : Fin m → IntPoint d) :
    positiveSimplexCheck v = true ↔
      AffineIndependent ℝ (fun i => ofInt (v i)) ∧
        ∃ q : V m, (∀ i, 0 < q i) ∧ (∑ i, q i = 1) ∧
          ∑ i, q i • ofInt (v i) = 0 := by
  simp only [positiveSimplexCheck, decide_eq_true_eq]
  constructor
  · rintro ⟨hdet, hpos, hsum, hrel⟩
    refine ⟨(simplexGram_det_iff v).mp hdet,
      fun i => (simplexWeights v i : ℝ), ?_, ?_, ?_⟩
    · intro i
      change (0 : ℝ) < (simplexWeights v i : ℝ)
      exact_mod_cast hpos i
    · change ∑ i, (simplexWeights v i : ℝ) = 1
      exact_mod_cast hsum
    · ext j
      have h := hrel j
      have hc := congrArg (fun x : ℚ => (x : ℝ)) h
      push_cast at hc
      simpa [ofInt, Finset.sum_apply, Pi.smul_apply, smul_eq_mul] using hc
  · rintro ⟨hi, q, hpos, hsum, hrel⟩
    have heq := simplexWeights_unique v q hi hsum hrel
    refine ⟨(simplexGram_det_iff v).mpr hi, ?_, ?_, ?_⟩
    · intro i
      have h := hpos i
      rw [← heq i] at h
      exact_mod_cast h
    · have h : ∑ i, (simplexWeights v i : ℝ) = 1 := by simpa only [heq] using hsum
      exact_mod_cast h
    · intro j
      have h := congrFun hrel j
      simp only [Finset.sum_apply, Pi.smul_apply, smul_eq_mul, ofInt, Pi.zero_apply] at h
      simp only [← heq] at h
      exact_mod_cast h

end Refpoly.KS
