import Refpoly.KS.Model

open Matrix

namespace Refpoly.KS

theorem integer_gram_det_ne_zero_iff {ι κ : Type} [Fintype ι] [Fintype κ]
    [DecidableEq ι] [DecidableEq κ] (B : Matrix ι κ ℤ) :
    (Bᵀ * B).det ≠ 0 ↔
      Function.Injective (Matrix.mulVecLin (B.map (Int.castRingHom ℝ))) := by
  let A := B.map (Int.castRingHom ℝ)
  have hcast : (((Bᵀ * B).det : ℤ) : ℝ) = (Aᵀ * A).det := by
    have heq : (Bᵀ * B).map (Int.castRingHom ℝ) = Aᵀ * A := by
      ext i j
      simp [Matrix.mul_apply, A]
    rw [← heq]
    exact (Int.castRingHom ℝ).map_det (Bᵀ * B)
  have hker : LinearMap.ker (Matrix.mulVecLin (Aᵀ * A)) =
      LinearMap.ker (Matrix.mulVecLin A) :=
    Matrix.ker_mulVecLin_transpose_mul_self A
  have hdet : (Aᵀ * A).det ≠ 0 ↔ Function.Injective (Matrix.mulVecLin A) := by
    rw [← LinearMap.ker_eq_bot, ← hker, LinearMap.ker_eq_bot']
    constructor
    · intro h x hx
      exact Matrix.eq_zero_of_mulVec_eq_zero h hx
    · intro h hz
      obtain ⟨x, hx, hzero⟩ := Matrix.exists_mulVec_eq_zero_iff.mpr hz
      exact hx (h x hzero)
  rw [← hdet, ← hcast]
  exact_mod_cast Iff.rfl

end Refpoly.KS
