import Refpoly.CWSBasis
import Refpoly.KS.Contracts

open Set Matrix Module

namespace Refpoly.KS

def seedPairing {d : ℕ} (S : Seed d) : V d →ₗ[ℝ] V S.N :=
  Matrix.mulVecLin (fun j i => S.vertices j i)

theorem seedPairing_range {d : ℕ} {S : Seed d} (B : RelationBasis S)
    (hind : LinearIndependent ℝ (realRows B.code)) (hB : GeometricBasis B) :
    LinearMap.range (seedPairing S) = kernelSpace B.code := by
  let A : Matrix (Fin d) (Fin S.N) ℝ := fun i j => S.vertices j i
  let W := realRows B.code
  have hrowrange : LinearMap.range (Matrix.mulVecLin Wᵀ) =
      Submodule.span ℝ (range W) := by
    rw [Matrix.range_mulVecLin]
    rfl
  have hdimrows : finrank ℝ (Submodule.span ℝ (range W)) = B.r := by
    simpa using finrank_span_eq_card hind
  have hdimker : finrank ℝ (LinearMap.ker (Matrix.mulVecLin A)) = B.r := by
    rw [← hB.1]
    exact hdimrows
  have hdA := LinearMap.finrank_range_add_finrank_ker (Matrix.mulVecLin A)
  rw [hdimker, Module.finrank_fin_fun] at hdA
  have htA := Matrix.rank_transpose A
  change finrank ℝ (LinearMap.range (Matrix.mulVecLin Aᵀ)) =
    finrank ℝ (LinearMap.range (Matrix.mulVecLin A)) at htA
  rw [← htA] at hdA
  have hdW := LinearMap.finrank_range_add_finrank_ker (Matrix.mulVecLin W)
  have htW := Matrix.rank_transpose W
  change finrank ℝ (LinearMap.range (Matrix.mulVecLin Wᵀ)) =
    finrank ℝ (LinearMap.range (Matrix.mulVecLin W)) at htW
  rw [← htW, hrowrange, hdimrows, Module.finrank_fin_fun] at hdW
  change B.r + finrank ℝ (LinearMap.ker (Matrix.mulVecLin W)) = S.N at hdW
  apply Submodule.eq_of_le_of_finrank_eq
  · rintro _ ⟨x, rfl⟩
    change Matrix.mulVecLin (realRows B.code) (seedPairing S x) = 0
    ext a
    have hr : Matrix.mulVecLin A (realRows B.code a) = 0 := by
      have hrow : realRows B.code a ∈ Submodule.span ℝ (range (realRows B.code)) :=
        Submodule.subset_span (mem_range_self a)
      rw [hB.1] at hrow
      exact LinearMap.mem_ker.mp hrow
    have hsum : ∑ j, realRows B.code a j • S.vertices j = 0 := by
      ext i
      have h := congrFun hr i
      simpa [A, Matrix.mulVecLin_apply, Matrix.mulVec, dotProduct,
        Finset.sum_apply, Pi.smul_apply, smul_eq_mul, mul_comm] using h
    have h := congrArg (fun y : V d => y ⬝ᵥ x) hsum
    dsimp only at h
    rw [sum_dotProduct, zero_dotProduct] at h
    simp only [smul_dotProduct, smul_eq_mul] at h
    exact h
  · change finrank ℝ (LinearMap.range (Matrix.mulVecLin Aᵀ)) =
      finrank ℝ (LinearMap.ker (Matrix.mulVecLin W))
    exact Nat.add_right_cancel (hdA.trans (by simpa only [Nat.add_comm] using hdW.symm))

theorem seedPairing_injective {d : ℕ} {P : Set (V d)} (S : Seed d)
    (hS : ValidSeed P S) : Function.Injective (seedPairing S) := by
  classical
  apply LinearMap.ker_eq_bot.mp
  apply LinearMap.ker_eq_bot'.mpr
  intro x hx
  let T := Finset.univ.image S.vertices
  have hT : (T : Set (V d)) = range S.vertices := by simp [T]
  have hspan : Submodule.span ℝ (range S.vertices) = ⊤ := by
    rw [← hT]
    apply BoxReduction.span_vertices_eq_top T
    rw [hT]
    exact hS.2.2.1
  have hle : Submodule.span ℝ (range S.vertices) ≤ LinearMap.ker (dotLin x) := by
    apply Submodule.span_le.mpr
    rintro _ ⟨j, rfl⟩
    have h := congrFun hx j
    change x ⬝ᵥ S.vertices j = 0
    simpa [seedPairing, Matrix.mulVecLin_apply, Matrix.mulVec, dotProduct,
      mul_comm] using h
  rw [hspan] at hle
  ext i
  have h := LinearMap.mem_ker.mp (hle (show Pi.single i 1 ∈ (⊤ : Submodule ℝ (V d))
    from Submodule.mem_top))
  simpa [dotLin, dotProduct_single] using h

theorem geometricContainers_proof (p : Procedures) : GeometricContainers p := by
  classical
  intro d _ P hP S hS B hC hB hb
  let b := p.kernelBasis B.code
  let K := kernelSpace B.code
  let Φ := seedPairing S
  have hΦrange : LinearMap.range Φ = K := seedPairing_range B hC.2.2.2.1 hB
  have hΦmem : ∀ x, Φ x ∈ K := by
    intro x
    rw [← hΦrange]
    exact LinearMap.mem_range_self Φ x
  let F := Φ.codRestrict K hΦmem
  have hFbij : Function.Bijective F := by
    constructor
    · intro x y h
      exact seedPairing_injective S hS (congrArg Subtype.val h)
    · rintro ⟨y, hy⟩
      rw [← hΦrange] at hy
      obtain ⟨x, hx⟩ := hy
      exact ⟨x, Subtype.ext hx⟩
  let E := LinearEquiv.ofBijective F hFbij
  have hKmem : ∀ x, basisMap b x ∈ K := by
    intro x
    change basisMap b x ∈ kernelSpace B.code
    rw [← hb.2.1]
    exact LinearMap.mem_range_self (basisMap b) x
  let G := (basisMap b).codRestrict K hKmem
  have hGbij : Function.Bijective G := by
    constructor
    · intro x y h
      exact hb.1 (congrArg Subtype.val h)
    · rintro ⟨y, hy⟩
      change y ∈ kernelSpace B.code at hy
      rw [← hb.2.1] at hy
      obtain ⟨x, hx⟩ := hy
      exact ⟨x, Subtype.ext hx⟩
  let H := LinearEquiv.ofBijective G hGbij
  let f := E.trans H.symm
  have hfcoord (x : V d) : basisMap b (f x) = Φ x := by
    exact congrArg Subtype.val (H.apply_symm_apply (E x))
  have hf : ForwardIntegral f := by
    intro x hx
    have hlat : IsLatticePoint (Φ x) := by
      intro j
      exact (hS.2.1 j).1.dotProduct_int hx
    obtain ⟨z, hz, hzx⟩ := hb.2.2 (Φ x) (hΦmem x) hlat
    have hzf : z = f x := hb.1 (hzx.trans (hfcoord x).symm)
    exact hzf ▸ hz
  refine ⟨f, hf, ?_⟩
  obtain ⟨T, hTlat, hTeq⟩ := hP.latticePolytope
  rw [hTeq]
  change f.toLinearMap '' convexHull ℝ (T : Set (V d)) ⊆
    convexHull ℝ {x : V d | IsLatticePoint x ∧ ∀ j, -1 ≤ basisMap b x j}
  rw [f.toLinearMap.image_convexHull]
  apply convexHull_mono
  rintro _ ⟨x, hx, rfl⟩
  refine ⟨hf x (hTlat x hx), fun j => ?_⟩
  change -1 ≤ basisMap b (f x) j
  rw [congrFun (hfcoord x) j]
  exact (hS.2.1 j).2 x (by rw [hTeq]; exact subset_convexHull ℝ _ hx)

end Refpoly.KS
