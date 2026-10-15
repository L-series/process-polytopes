/-
CWSBasis faithfully ported from the reviewed refpoly-5d snapshot.
See SOURCES.md for attribution and exact source identities.
Kernel and pairing identities are over the reals, not an integer kernel basis.
-/
import Refpoly.CircuitSpan
import Refpoly.GeometricCWS
import Refpoly.BoxReduction

/-! Independent positive simplex equations for the complete CWS pairing space. -/
open Set Finset Module Matrix
namespace Refpoly

/-- A basis of the relation space made of positive simplex circuits. -/
structure CircuitBasis {n : ℕ} (S : Finset (V n)) where
  k : ℕ
  row : Fin k → S → ℝ
  indep : LinearIndependent ℝ row
  circuit : ∀ a, row a ∈ simplexCircuitWeights S
  spans : Submodule.span ℝ (Set.range row) = LinearMap.ker (vertexCombination S)

/-- Every IP generating set has an independent complete family of simplex
circuits; redundant circuits need not be kept as CWS equations. -/
theorem exists_circuitBasis {n : ℕ} (S : Finset (V n))
    (h0 : (0 : V n) ∈ intrinsicInterior ℝ (convexHull ℝ (S : Set (V n)))) :
    Nonempty (CircuitBasis S) := by
  classical
  obtain ⟨I, hIC, hspan, hi⟩ := exists_linearIndependent ℝ (simplexCircuitWeights S)
  letI : Fintype I := hi.set_finite_of_isNoetherian.fintype
  let e : Fin (Fintype.card I) ≃ I := (Fintype.equivFin I).symm
  have hrange : Set.range (fun a => (e a : S → ℝ)) = I := by
    rw [show (fun a => (e a : S → ℝ)) = Subtype.val ∘ e from rfl, Set.range_comp,
      e.range_eq_univ, Set.image_univ, Subtype.range_coe]
  refine ⟨⟨Fintype.card I, fun a => e a, hi.comp e e.injective,
    fun a => hIC (e a).property, ?_⟩⟩
  rw [hrange, hspan]
  exact simplex_circuits_span_kernel S h0

theorem CircuitBasis.nonneg {n : ℕ} {S : Finset (V n)} (B : CircuitBasis S) :
    ∀ a x, 0 ≤ B.row a x := by
  intro a x
  obtain ⟨T, _, _, w, hw, _, _, heq⟩ := B.circuit a
  rw [heq]
  dsimp [supportedWeights]
  split_ifs with hx
  · exact (hw x hx).le
  · exact le_rfl

/-- Completeness of the equation basis forces every coordinate to occur. -/
theorem CircuitBasis.cover {n : ℕ} {S : Finset (V n)} (B : CircuitBasis S)
    (h0 : (0 : V n) ∈ intrinsicInterior ℝ (convexHull ℝ (S : Set (V n)))) :
    ∀ x : S, ∃ a, 0 < B.row a x := by
  classical
  obtain ⟨w, hw, _, hrel⟩ := exists_positive_relation_of_intrinsicInterior S h0
  let wS : S → ℝ := fun x => w x
  have hwrel : wS ∈ LinearMap.ker (vertexCombination S) := by
    rw [LinearMap.mem_ker, vertexCombination_apply]
    exact (Finset.sum_coe_sort S fun x => w x • x).trans hrel
  intro x
  by_contra! hn
  have hz : ∀ a, B.row a x = 0 := fun a => le_antisymm (hn a) (B.nonneg a x)
  have hle : Submodule.span ℝ (Set.range B.row) ≤ LinearMap.ker (LinearMap.proj x) := by
    apply Submodule.span_le.mpr
    rintro _ ⟨a,rfl⟩
    exact LinearMap.mem_ker.mpr (hz a)
  rw [B.spans] at hle
  have hzero := LinearMap.mem_ker.mp (hle hwrel)
  exact (hw x x.property).ne' hzero

end Refpoly

namespace Refpoly
open Set Finset Module Matrix

/-- `vertexCombination` is the action of the transposed vertex matrix. -/
theorem vertexCombination_eq_mulVecLin {n : ℕ} (S : Finset (V n)) :
    vertexCombination S = Matrix.mulVecLin (Matrix.transpose (fun x : S => (x : V n))) := by
  ext q i
  simp [vertexCombination_apply, Matrix.mulVec, Matrix.transpose, dotProduct,
    Finset.sum_apply, Pi.smul_apply, smul_eq_mul, mul_comm]

/-- The joint solution space of all independent simplex equations. -/
noncomputable def CircuitBasis.subspace {n : ℕ} {S : Finset (V n)} (B : CircuitBasis S) :
    Submodule ℝ (S → ℝ) := LinearMap.ker (Matrix.mulVecLin B.row)

/-- Pairing with every generator lands in, and surjects onto, the complete
combined-weight-system solution space. -/
theorem CircuitBasis.pairing_range {n : ℕ} {S : Finset (V n)} (B : CircuitBasis S) :
    LinearMap.range (Matrix.mulVecLin (fun x : S => (x : V n))) = B.subspace := by
  classical
  let v : Matrix S (Fin n) ℝ := fun x => (x : V n)
  have hv : Matrix.mulVecLin vᵀ = vertexCombination S := (vertexCombination_eq_mulVecLin S).symm
  have hspan : LinearMap.range (Matrix.mulVecLin B.rowᵀ) =
      Submodule.span ℝ (Set.range B.row) := by
    rw [Matrix.range_mulVecLin]
    rfl
  have hdimV := LinearMap.finrank_range_add_finrank_ker (Matrix.mulVecLin vᵀ)
  rw [hv, ← B.spans, finrank_span_eq_card B.indep, Fintype.card_fin,
    Module.finrank_pi] at hdimV
  have hrankV := Matrix.rank_transpose v
  change finrank ℝ (LinearMap.range (Matrix.mulVecLin vᵀ)) =
    finrank ℝ (LinearMap.range (Matrix.mulVecLin v)) at hrankV
  rw [← hv, hrankV] at hdimV
  have hdimW := LinearMap.finrank_range_add_finrank_ker (Matrix.mulVecLin B.row)
  have hrankW := Matrix.rank_transpose B.row
  change finrank ℝ (LinearMap.range (Matrix.mulVecLin B.rowᵀ)) =
    finrank ℝ (LinearMap.range (Matrix.mulVecLin B.row)) at hrankW
  rw [← hrankW, hspan, finrank_span_eq_card B.indep, Fintype.card_fin,
    Module.finrank_pi] at hdimW
  apply Submodule.eq_of_le_of_finrank_eq
  · rintro _ ⟨x,rfl⟩
    rw [CircuitBasis.subspace, LinearMap.mem_ker]
    funext a
    have hr : vertexCombination S (B.row a) = 0 := by
      apply LinearMap.mem_ker.mp
      rw [← B.spans]
      exact Submodule.subset_span (Set.mem_range_self a)
    have hd := congrArg (fun y : V n => y ⬝ᵥ x) hr
    dsimp only at hd
    rw [vertexCombination_apply, sum_dotProduct, zero_dotProduct] at hd
    simp only [smul_dotProduct, smul_eq_mul] at hd
    change ∑ y : S, B.row a y * ((y : V n) ⬝ᵥ x) = 0
    exact hd
  · change finrank ℝ (LinearMap.range (Matrix.mulVecLin v)) =
      finrank ℝ (LinearMap.ker (Matrix.mulVecLin B.row))
    omega

end Refpoly

namespace Refpoly
open Set Finset Module Matrix

/-- A full-dimensional minimal container has N = n+k coordinates, with
exactly k independent CWS equations. -/
theorem CircuitBasis.coordinate_count {n : ℕ} {S : Finset (V n)} (B : CircuitBasis S)
    (h0 : (0 : V n) ∈ interior (convexHull ℝ (S : Set (V n)))) :
    S.card = n + B.k := by
  classical
  have hrange : LinearMap.range (vertexCombination S) =
      Submodule.span ℝ (S : Set (V n)) := by
    rw [vertexCombination_eq_mulVecLin, Matrix.range_mulVecLin]
    change Submodule.span ℝ (Set.range ((↑) : S → V n)) = _
    rw [Subtype.range_coe_subtype]
    rfl
  rw [BoxReduction.span_vertices_eq_top S h0] at hrange
  have hd := LinearMap.finrank_range_add_finrank_ker (vertexCombination S)
  rw [hrange, finrank_top, Module.finrank_fin_fun, ← B.spans,
    finrank_span_eq_card B.indep, Fintype.card_fin, Module.finrank_pi] at hd
  simpa only [Fintype.card_coe] using hd.symm

end Refpoly
