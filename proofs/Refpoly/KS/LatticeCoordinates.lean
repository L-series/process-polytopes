import Refpoly.KS.CongruenceLattice
import Refpoly.KS.Recovery

open Set Matrix

namespace Refpoly.KS

theorem realMatrix_det_ne_zero {d : ℕ} (B : Matrix (Fin d) (Fin d) ℤ)
    (hB : B.det ≠ 0) : (BoxReduction.realMatrix B).det ≠ 0 := by
  change ((Int.castRingHom ℝ).mapMatrix B).det ≠ 0
  rw [← (Int.castRingHom ℝ).map_det B]
  change (B.det : ℝ) ≠ 0
  exact_mod_cast hB

noncomputable def integerMatrixEquiv {d : ℕ} (B : Matrix (Fin d) (Fin d) ℤ)
    (hB : B.det ≠ 0) : V d ≃ₗ[ℝ] V d :=
  LinearEquiv.ofBijective (Matrix.mulVecLin (BoxReduction.realMatrix B))
    ⟨Matrix.mulVec_injective_of_isUnit
      ((Matrix.isUnit_iff_isUnit_det _).mpr
        (isUnit_iff_ne_zero.mpr (realMatrix_det_ne_zero B hB))),
      Matrix.mulVec_surjective_iff_isUnit.mpr
        ((Matrix.isUnit_iff_isUnit_det _).mpr
          (isUnit_iff_ne_zero.mpr (realMatrix_det_ne_zero B hB)))⟩

theorem integerMatrixEquiv_ofInt {d : ℕ} (B : Matrix (Fin d) (Fin d) ℤ)
    (hB : B.det ≠ 0) (x : IntPoint d) :
    integerMatrixEquiv B hB (ofInt x) = ofInt (B *ᵥ x) := by
  ext i
  simp [integerMatrixEquiv, BoxReduction.realMatrix, ofInt, Matrix.mulVec, dotProduct]

def integerCoordinates {d : ℕ} (B : Matrix (Fin d) (Fin d) ℤ)
    (z : IntPoint d) : IntPoint d :=
  fun i => (B.adjugate *ᵥ z) i / B.det

theorem integerCoordinates_mulVec {d : ℕ} (B : Matrix (Fin d) (Fin d) ℤ)
    (hB : B.det ≠ 0) (x : IntPoint d) :
    integerCoordinates B (B *ᵥ x) = x := by
  have h : B.adjugate *ᵥ (B *ᵥ x) = B.det • x := by
    rw [Matrix.mulVec_mulVec x B.adjugate B, Matrix.adjugate_mul,
      Matrix.smul_mulVec, Matrix.one_mulVec]
  ext i
  dsimp only [integerCoordinates]
  rw [h]
  exact Int.mul_ediv_cancel_left (x i) hB

theorem integerCoordinates_exact {d : ℕ} (B : Matrix (Fin d) (Fin d) ℤ)
    (hB : B.det ≠ 0) {z : IntPoint d} (hz : ∃ x : IntPoint d, B *ᵥ x = z) :
    B *ᵥ integerCoordinates B z = z := by
  obtain ⟨x, rfl⟩ := hz
  rw [integerCoordinates_mulVec B hB x]

def integralSubgroup {d : ℕ} (M : Submodule ℤ (IntPoint d)) : AddSubgroup (V d) :=
  M.toAddSubgroup.map (ofIntHom d)

theorem integerMatrixEquiv_imageLattice {d : ℕ} (B : Matrix (Fin d) (Fin d) ℤ)
    (hB : B.det ≠ 0) (M : Submodule ℤ (IntPoint d))
    (hM : ∀ z : IntPoint d, z ∈ M ↔ ∃ x : IntPoint d, B *ᵥ x = z) :
    imageLattice (integerMatrixEquiv B hB) = integralSubgroup M := by
  apply AddSubgroup.ext
  intro y
  change y ∈ (integerLattice d).map (integerMatrixEquiv B hB).toLinearMap.toAddMonoidHom ↔
    y ∈ M.toAddSubgroup.map (ofIntHom d)
  simp only [AddSubgroup.mem_map]
  constructor
  · rintro ⟨x, hx, hxy⟩
    obtain ⟨z, hz⟩ : ∃ z : IntPoint d, ofInt z = x := by
      choose z hz using hx
      exact ⟨z, funext fun i => (hz i).symm⟩
    refine ⟨B *ᵥ z, (hM _).mpr ⟨z, rfl⟩, ?_⟩
    change integerMatrixEquiv B hB x = y at hxy
    rw [← hz, integerMatrixEquiv_ofInt] at hxy
    exact hxy
  · rintro ⟨z, hz, rfl⟩
    obtain ⟨x, hx⟩ := (hM z).mp hz
    refine ⟨ofInt x, fun i => ⟨x i, rfl⟩, ?_⟩
    change integerMatrixEquiv B hB (ofInt x) = ofInt z
    rw [integerMatrixEquiv_ofInt, hx]

def latticeCoordinateCode {d : ℕ} (B : Matrix (Fin d) (Fin d) ℤ)
    (zs : List (IntPoint d)) : PolyCode d :=
  ⟨zs.map (integerCoordinates B)⟩

theorem latticeCoordinateCode_image {d : ℕ} (B : Matrix (Fin d) (Fin d) ℤ)
    (hB : B.det ≠ 0) (zs : List (IntPoint d))
    (hz : ∀ z ∈ zs, ∃ x : IntPoint d, B *ᵥ x = z) :
    integerMatrixEquiv B hB '' (latticeCoordinateCode B zs).hull =
      convexHull ℝ (pointSet zs) := by
  have heq (z : IntPoint d) (h : z ∈ zs) :
      integerMatrixEquiv B hB (ofInt (integerCoordinates B z)) = ofInt z := by
    rw [integerMatrixEquiv_ofInt, integerCoordinates_exact B hB (hz z h)]
  have hpoints : integerMatrixEquiv B hB '' pointSet (zs.map (integerCoordinates B)) =
      pointSet zs := by
    ext y
    constructor
    · rintro ⟨_, ⟨_, hmem, rfl⟩, rfl⟩
      obtain ⟨z, hz, rfl⟩ := List.mem_map.mp hmem
      exact ⟨z, hz, (heq z hz).symm⟩
    · rintro ⟨z, hz, rfl⟩
      exact ⟨ofInt (integerCoordinates B z),
        ⟨integerCoordinates B z, List.mem_map.mpr ⟨z, hz, rfl⟩, rfl⟩, heq z hz⟩
  change (integerMatrixEquiv B hB).toLinearMap ''
    convexHull ℝ (pointSet (zs.map (integerCoordinates B))) = _
  rw [LinearMap.image_convexHull]
  change convexHull ℝ (integerMatrixEquiv B hB ''
    pointSet (zs.map (integerCoordinates B))) = _
  rw [hpoints]

end Refpoly.KS
