/-
GeometricCWS faithfully ported from the reviewed refpoly-5d snapshot.
See SOURCES.md for attribution and exact source identities.
-/
import Refpoly.Algorithm
import Mathlib.LinearAlgebra.Matrix.Rank

/-!
# Geometric simplex blocks and weight-system enumeration

This module proves the IP property and enumeration reachability of every
simplex circuit formed from lattice points of a reflexive polytope's dual.
The simplex need not span the ambient space. `CircuitSpan.lean`,
`CWSBasis.lean`, and `CWSContainer.lean` construct a complete independent
circuit family and prove coverage by exhaustive zero-padded CWS row assembly.
-/

open Set Module Matrix
namespace Refpoly

/-- A normalized circuit on an affinely independent family spans its entire
space of linear relations. This also applies to lower-dimensional simplices. -/
theorem simplex_relation_kernel {n m : ℕ} (v : Fin m → V n) (q : V m)
    (hind : AffineIndependent ℝ v) (hqsum : ∑ i, q i = 1)
    (hcirc : ∑ i, q i • v i = 0) :
    LinearMap.ker (Matrix.mulVecLin (Matrix.transpose v)) = ℝ ∙ q := by
  classical
  ext a
  constructor
  · intro ha
    have haz : ∑ i, a i • v i = 0 := by
      ext j
      have h := congrFun (LinearMap.mem_ker.mp ha) j
      simpa [Matrix.mulVecLin_apply, Matrix.mulVec, dotProduct, Matrix.transpose,
        Finset.sum_apply, Pi.smul_apply, smul_eq_mul, mul_comm] using h
    have hsum : ∑ i, (a i - (∑ j, a j) * q i) = 0 := by
      rw [Finset.sum_sub_distrib, ← Finset.mul_sum, hqsum, mul_one, sub_self]
    have hrel : ∑ i, (a i - (∑ j, a j) * q i) • v i = 0 := by
      simp_rw [sub_smul, mul_smul]
      rw [Finset.sum_sub_distrib, ← Finset.smul_sum, haz, hcirc, smul_zero, sub_self]
    have heq := hind.eq_zero_of_sum_eq_zero hsum hrel
    have hae : a = (∑ j, a j) • q := by
      ext i
      exact sub_eq_zero.mp (heq i (Finset.mem_univ i))
    rw [hae]
    exact Submodule.smul_mem _ _ (Submodule.mem_span_singleton_self q)
  · intro ha
    obtain ⟨c, rfl⟩ := Submodule.mem_span_singleton.mp ha
    rw [LinearMap.mem_ker, map_smul]
    have hq : Matrix.mulVecLin (Matrix.transpose v) q = 0 := by
      ext j
      have h := congrFun hcirc j
      simpa [Matrix.mulVecLin_apply, Matrix.mulVec, dotProduct, Matrix.transpose,
        Finset.sum_apply, Pi.smul_apply, smul_eq_mul, mul_comm] using h
    rw [hq, smul_zero]

/-- The pairings of any vector with the vertices of a circuit are orthogonal
to the circuit weights. -/
theorem circuit_pairings_mem_wsHyperplane {n m : ℕ} (v : Fin m → V n) (q : V m)
    (hcirc : ∑ i, q i • v i = 0) (x : V n) :
    (fun i => v i ⬝ᵥ x) ∈ wsHyperplane q := by
  rw [mem_wsHyperplane]
  have h : (∑ i, q i • v i) ⬝ᵥ x = 0 := by rw [hcirc, zero_dotProduct]
  simpa only [sum_dotProduct, smul_dotProduct, smul_eq_mul] using h

/-- Every vector orthogonal to a simplex's circuit lifts to a pairing with
its vertices. No full-dimensionality assumption on the simplex is needed. -/
theorem simplex_pairing_surjective {n m : ℕ} (v : Fin m → V n) (q : V m)
    (hind : AffineIndependent ℝ v) (hqsum : ∑ i, q i = 1)
    (hcirc : ∑ i, q i • v i = 0) :
    ∀ z : V m, q ⬝ᵥ z = 0 → ∃ x : V n, ∀ i, v i ⬝ᵥ x = z i := by
  classical
  have hqne : q ≠ 0 := by intro h; simp [h] at hqsum
  have hker := simplex_relation_kernel v q hind hqsum hcirc
  have hdim := LinearMap.finrank_range_add_finrank_ker (Matrix.mulVecLin vᵀ)
  rw [hker, finrank_span_singleton hqne, Module.finrank_fin_fun] at hdim
  have hW : finrank ℝ (wsHyperplane q) + 1 = m := by
    have hr : LinearMap.range (dotLin q) = ⊤ := by
      rw [LinearMap.range_eq_top]
      intro c
      refine ⟨c • ones m, ?_⟩
      rw [map_smul, dotLin_apply, dotProduct_ones, hqsum, smul_eq_mul, mul_one]
    have h := LinearMap.finrank_range_add_finrank_ker (dotLin q)
    rw [hr, finrank_top, Module.finrank_self, Module.finrank_fin_fun] at h
    change 1 + finrank ℝ (wsHyperplane q) = m at h
    omega
  have hsub : LinearMap.range (Matrix.mulVecLin v) ≤ wsHyperplane q := by
    rintro _ ⟨x, rfl⟩
    exact circuit_pairings_mem_wsHyperplane v q hcirc x
  have heq : LinearMap.range (Matrix.mulVecLin v) = wsHyperplane q := by
    apply Submodule.eq_of_le_of_finrank_eq hsub
    have hrank := Matrix.rank_transpose v
    change finrank ℝ (LinearMap.range (Matrix.mulVecLin vᵀ)) =
      finrank ℝ (LinearMap.range (Matrix.mulVecLin v)) at hrank
    rw [hrank] at hdim
    exact Nat.add_right_cancel (hdim.trans hW.symm)
  intro z hz
  have hmem : z ∈ LinearMap.range (Matrix.mulVecLin v) := by
    rw [heq, mem_wsHyperplane]; exact hz
  obtain ⟨x, hx⟩ := hmem
  exact ⟨x, fun i => congrFun hx i⟩

/-- A positive simplex circuit made of dual lattice points is an IP weight
system, including when the simplex has smaller dimension than the polytope. -/
theorem reflexive_simplex_block_ip {n m : ℕ} {P : Set (V n)}
    (hP : IsReflexive P) (v : Fin m → V n) (q : V m)
    (hind : AffineIndependent ℝ v) (hqpos : ∀ i, 0 < q i)
    (hqsum : ∑ i, q i = 1) (hcirc : ∑ i, q i • v i = 0)
    (hvlat : ∀ i, IsLatticePoint (v i)) (hvdual : ∀ i, v i ∈ polarDual P) :
    IsIPWeightSystem q := by
  classical
  let Φ : V n →ₗ[ℝ] V m := LinearMap.pi (fun i => dotLin (v i))
  have hΦmem : ∀ x, Φ x ∈ wsHyperplane q := circuit_pairings_mem_wsHyperplane v q hcirc
  let F : V n →ₗ[ℝ] wsHyperplane q := Φ.codRestrict _ hΦmem
  have hFsurj : Function.Surjective F := by
    rintro ⟨z, hz⟩
    obtain ⟨x, hx⟩ := simplex_pairing_surjective v q hind hqsum hcirc z
      (mem_wsHyperplane.mp hz)
    exact ⟨x, Subtype.ext (funext hx)⟩
  have hFopen : IsOpenMap F := LinearMap.isOpenMap_of_finiteDimensional F hFsurj
  obtain ⟨T, hTlat, hTeq⟩ := hP.latticePolytope
  have hTP : (T : Set (V n)) ⊆ P := by
    rw [hTeq]; exact subset_convexHull ℝ _
  have hgens : F '' (T : Set (V n)) ⊆ wsLatticeGensAt q 1 := by
    rintro _ ⟨x, hx, rfl⟩
    refine ⟨fun i => ?_, fun i => ?_⟩
    · exact (hvlat i).dotProduct_int (hTlat x hx)
    · exact hvdual i x (hTP hx)
  have hsub : F '' P ⊆ wsPolytopeAt q 1 := by
    rw [hTeq, LinearMap.image_convexHull]
    exact convexHull_mono hgens
  refine ⟨hqpos, interior_mono hsub ?_⟩
  exact hFopen.image_interior_subset P ⟨0, hP.ip, map_zero F⟩

/-- The weight-system enumeration reaches every geometric simplex block of a
reflexive polytope, without a separate block-projection lifting hypothesis. -/
theorem algorithm_reaches_simplex_block {n m : ℕ} {P : Set (V n)}
    (hP : IsReflexive P) (v : Fin m → V n) (q : V m)
    (hind : AffineIndependent ℝ v) (hqpos : ∀ i, 0 < q i)
    (hqsum : ∑ i, q i = 1) (hcirc : ∑ i, q i • v i = 0)
    (hvlat : ∀ i, IsLatticePoint (v i)) (hvdual : ∀ i, v i ∈ polarDual P)
    (sel : SelectionRule m) :
    ∃ S : Finset (V m), Reachable sel S ∧ sel.sel S = q ∧ S.card ≤ m + 1 := by
  exact algorithm_complete sel hqpos hqsum
    (reflexive_simplex_block_ip hP v q hind hqpos hqsum hcirc hvlat hvdual)

end Refpoly
