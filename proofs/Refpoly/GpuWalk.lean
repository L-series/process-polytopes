import Refpoly.JoinCWS
import Mathlib.Data.ZMod.Basic
import Mathlib.LinearAlgebra.Matrix.Nondegenerate
import Mathlib.LinearAlgebra.Matrix.ToLin

/-! Certificate interface for the lattice-point walk. Count equality is sound
only after exact validity and distinctness have been established. The floating
root box need not be complete when these certificates are checked. -/
namespace Refpoly.GpuWalk

/-- Exact cardinality plus valid distinct output points proves complete
coverage. No geometric or numerical assumption on the walk is needed. -/
theorem checked_walk_complete {α : Type*} [DecidableEq α] (P : Finset α)
    {n : Nat} (walk : Fin n → α) (distinct : Function.Injective walk)
    (valid : ∀ i, walk i ∈ P) (count : n = P.card) :
    ∀ x, x ∈ P ↔ ∃ i, walk i = x := by
  have subset : Finset.univ.image walk ⊆ P := by
    intro x hx
    obtain ⟨i, _, rfl⟩ := Finset.mem_image.mp hx
    exact valid i
  have equal : Finset.univ.image walk = P := by
    apply Finset.eq_of_subset_of_card_le subset
    simpa [Finset.card_image_of_injective _ distinct] using count.ge
  intro x
  rw [← equal]
  simp

/-- The independently validated mod-2 left-inverse witness implies a nonzero
integer determinant. Elimination itself need not be trusted to produce a
correct witness. -/
theorem integer_det_ne_zero_of_mod2_left_inverse {n : Nat}
    (minor : Matrix (Fin n) (Fin n) Int)
    (witness : Matrix (Fin n) (Fin n) (ZMod 2))
    (checked : witness * minor.map (Int.castRingHom (ZMod 2)) = 1) :
    minor.det ≠ 0 := by
  intro hzero
  have h := congrArg Matrix.det checked
  rw [Matrix.det_mul, Matrix.det_one] at h
  have hm : (minor.map (Int.castRingHom (ZMod 2))).det = 0 := by
    rw [← RingHom.mapMatrix_apply, ← RingHom.map_det]
    simp [hzero]
  rw [hm, mul_zero] at h
  exact zero_ne_one h

/-- The same certificate gives real row independence, so the reduced
coefficient map is injective over the reals as well as over the integers. -/
theorem real_minor_independent {n : Nat}
    (minor : Matrix (Fin n) (Fin n) Int)
    (witness : Matrix (Fin n) (Fin n) (ZMod 2))
    (checked : witness * minor.map (Int.castRingHom (ZMod 2)) = 1) :
    LinearIndependent ℝ (fun i => fun j => (minor i j : ℝ)) := by
  have hz := integer_det_ne_zero_of_mod2_left_inverse minor witness checked
  apply Matrix.linearIndependent_rows_of_det_ne_zero
  change (minor.map (Int.castRingHom ℝ)).det ≠ 0
  rw [← RingHom.mapMatrix_apply, ← RingHom.map_det]
  change (minor.det : ℝ) ≠ 0
  exact Int.cast_ne_zero.mpr hz

/-- A full rectangular row embedding inherits injectivity from any certified
square minor, irrespective of the other ambient coordinates. -/
theorem real_embedding_injective {d n : Nat}
    (basis : Matrix (Fin d) (Fin n) Int) (pivot : Fin d → Fin n)
    (witness : Matrix (Fin d) (Fin d) (ZMod 2))
    (checked : witness * (basis.submatrix id pivot).map (Int.castRingHom (ZMod 2)) = 1) :
    Function.Injective (Matrix.vecMul · (basis.map (Int.castRingHom ℝ))) := by
  have independent := real_minor_independent (basis.submatrix id pivot) witness checked
  have injective := Matrix.vecMul_injective_iff.mpr independent
  intro x y hxy
  apply injective
  funext j
  exact congrFun hxy (pivot j)

/-- Valid distinct reduced tuples remain distinct ambient points under the
certified embedding. This is the missing hypothesis in a count-only check. -/
theorem distinct_embedded_points {I A B : Type*} (embed : A → B)
    (embedding : Function.Injective embed) (points : I → A)
    (distinct : Function.Injective points) :
    Function.Injective (embed ∘ points) := embedding.comp distinct

/-- The checked integer rows define a linear map into the actual joint
kernel, not a replacement polytope or an ambient-relative interior. -/
def reducedKernelMap {d r : Nat} (C : CWS r (d + r))
    (basis : Matrix (Fin d) (Fin (d + r)) Int)
    (kernel : ∀ i, (fun j => (basis i j : ℝ)) ∈ C.subspace) :
    (Fin d → ℝ) →ₗ[ℝ] C.subspace :=
  (basis.map (Int.castRingHom ℝ)).vecMulLinear.codRestrict C.subspace (by
    intro x
    have heq : Matrix.vecMul x (basis.map (Int.castRingHom ℝ)) =
        ∑ i, x i • (fun j => (basis i j : ℝ)) := by
      ext j
      simp [Matrix.vecMul, dotProduct, Finset.sum_apply]
    change Matrix.vecMul x (basis.map (Int.castRingHom ℝ)) ∈ C.subspace
    rw [heq]
    exact Submodule.sum_mem _ (fun i _ => Submodule.smul_mem _ _ (kernel i)))

/-- Valid CWS row rank, the kernel-row check, and the checked mod-2 minor
supply the full real coordinate isomorphism. Unimodularity is unnecessary. -/
theorem reduced_kernel_map_bijective {d r : Nat} (C : CWS r (d + r))
    (basis : Matrix (Fin d) (Fin (d + r)) Int)
    (kernel : ∀ i, (fun j => (basis i j : ℝ)) ∈ C.subspace)
    (pivot : Fin d → Fin (d + r))
    (witness : Matrix (Fin d) (Fin d) (ZMod 2))
    (checked : witness * (basis.submatrix id pivot).map (Int.castRingHom (ZMod 2)) = 1) :
    Function.Bijective (reducedKernelMap C basis kernel) := by
  have injective : Function.Injective (reducedKernelMap C basis kernel) := by
    intro x y heq
    exact real_embedding_injective basis pivot witness checked
      (congrArg Subtype.val heq)
  have dimension : Module.finrank ℝ (Fin d → ℝ) = Module.finrank ℝ C.subspace := by
    rw [Refpoly.JoinRejection.cws_dimension, Module.finrank_fin_fun]
    omega
  exact ⟨injective, (LinearMap.injective_iff_surjective_of_finrank_eq_finrank dimension).mp injective⟩

/-- The certificate conditions construct the continuous coordinate change
used by the IP checker on the reduced coefficients. -/
theorem exists_reduced_coordinates {d r : Nat} (C : CWS r (d + r))
    (basis : Matrix (Fin d) (Fin (d + r)) Int)
    (kernel : ∀ i, (fun j => (basis i j : ℝ)) ∈ C.subspace)
    (pivot : Fin d → Fin (d + r))
    (witness : Matrix (Fin d) (Fin d) (ZMod 2))
    (checked : witness * (basis.submatrix id pivot).map (Int.castRingHom (ZMod 2)) = 1) :
    ∃ coordinates : (Fin d → ℝ) ≃L[ℝ] C.subspace,
      ∀ x, (coordinates x).val = Matrix.vecMul x (basis.map (Int.castRingHom ℝ)) := by
  let coordinates := (LinearEquiv.ofBijective (reducedKernelMap C basis kernel)
    (reduced_kernel_map_bijective C basis kernel pivot witness checked)).toContinuousLinearEquiv
  exact ⟨coordinates, fun _ => rfl⟩

/-- Origin-interior is invariant under an invertible continuous linear map;
reduced coordinates therefore need no integral/unimodular transform back. -/
theorem ip_preserved_by_coordinates {E F : Type*}
    [NormedAddCommGroup E] [NormedSpace ℝ E]
    [NormedAddCommGroup F] [NormedSpace ℝ F]
    (coordinates : E ≃L[ℝ] F) (points : Set E) :
    (0 : E) ∈ interior (convexHull ℝ points) ↔
      (0 : F) ∈ interior (convexHull ℝ (coordinates '' points)) := by
  have hull : convexHull ℝ (coordinates '' points) = coordinates '' convexHull ℝ points :=
    (coordinates.toLinearMap.image_convexHull points).symm
  have inside : interior (coordinates '' convexHull ℝ points) =
      coordinates '' interior (convexHull ℝ points) :=
    (coordinates.toHomeomorph.image_interior (convexHull ℝ points)).symm
  rw [hull, inside]
  constructor
  · intro h
    exact ⟨0, h, coordinates.map_zero⟩
  · rintro ⟨x, hx, heq⟩
    have : x = 0 := coordinates.injective (heq.trans coordinates.map_zero.symm)
    simpa [this] using hx

/-- Exact walk coverage and the certified coordinate isomorphism relate the
reduced-coordinate hull to the production CWS IP predicate. -/
theorem cws_ip_iff_reduced_hull {d r m : Nat} (C : CWS r (d + r))
    (coordinates : (Fin d → ℝ) ≃L[ℝ] C.subspace) (walk : Fin m → Fin d → ℝ)
    (coverage : coordinates '' Set.range walk = Refpoly.JoinRejection.generators C) :
    C.IsIP ↔ (0 : Fin d → ℝ) ∈ interior (convexHull ℝ (Set.range walk)) := by
  have h := ip_preserved_by_coordinates coordinates (Set.range walk)
  rw [coverage] at h
  exact h.symm

end Refpoly.GpuWalk
