import Refpoly.JoinRows

/-! Bridge from CUDA's nonnegative `X` coordinates and integer linear
functionals to the production CWS kernel and its `y = X-1` IP predicate. -/
namespace Refpoly.JoinRejection

/-- Valid CWS row independence gives precisely the advertised dimension. -/
theorem cws_dimension {r n : Nat} (C : CWS r n) :
    Module.finrank ℝ C.subspace = n - r := by
  have hrank : Matrix.rank C.W = r := by
    simpa only [Fintype.card_fin] using C.indep.rank_matrix
  have hker : C.subspace = LinearMap.ker (Matrix.mulVecLin C.W) := by
    ext x
    simp [CWS.subspace, LinearMap.mem_ker, dotLin_apply, Matrix.mulVec, funext_iff]
  have h := LinearMap.finrank_range_add_finrank_ker (Matrix.mulVecLin C.W)
  change Matrix.rank C.W + Module.finrank ℝ (LinearMap.ker (Matrix.mulVecLin C.W)) =
    Module.finrank ℝ (V n) at h
  rw [hrank, ← hker, Module.finrank_fin_fun] at h
  exact Nat.eq_sub_of_add_eq' h

/-- The simultaneous nonnegative integer row equations, with row degree equal
to the sum of weights, exactly as for a valid CWS candidate. -/
def CWSPoint {r n : Nat} (C : CWS r n) :=
  {x : Fin n → Nat // ∀ a, ∑ i, C.W a i * (x i : ℝ) = ∑ i, C.W a i}

def center {r n : Nat} (C : CWS r n) (x : CWSPoint C) : C.subspace :=
  ⟨fun i => (x.val i : ℝ) - 1, by
    simp only [CWS.subspace, Submodule.mem_iInf, LinearMap.mem_ker, dotLin_apply,
      dotProduct]
    intro a
    simp_rw [mul_sub, mul_one, Finset.sum_sub_distrib]
    exact sub_eq_zero.mpr (x.property a)⟩

theorem center_generator {r n : Nat} (C : CWS r n) (x : CWSPoint C) :
    center C x ∈ generators C := by
  constructor
  · intro i
    exact ⟨(x.val i : Int) - 1, by simp [center]⟩
  · intro i
    change -1 ≤ (x.val i : ℝ) - 1
    have := Nat.cast_nonneg (α := ℝ) (x.val i)
    linarith

/-- Every lattice generator has a nonnegative integer `X` preimage. This
prevents a relative-interior or shifted-coordinate change of specification. -/
theorem center_surjective {r n : Nat} (C : CWS r n) (y : C.subspace)
    (hy : y ∈ generators C) : ∃ x : CWSPoint C, center C x = y := by
  classical
  obtain ⟨hlat, hbound⟩ := hy
  choose z hz using hlat
  have hn (i : Fin n) : 0 ≤ z i + 1 := by
    have hi := hbound i
    rw [hz i] at hi
    exact_mod_cast (show (0 : ℝ) ≤ (z i : ℝ) + 1 by linarith)
  let x : Fin n → Nat := fun i => (z i + 1).toNat
  have hx (i : Fin n) : (x i : ℝ) = (y : V n) i + 1 := by
    change (((z i + 1).toNat : Nat) : ℝ) = (y : V n) i + 1
    calc
      (((z i + 1).toNat : Nat) : ℝ) = ((z i + 1 : Int) : ℝ) := by
        exact_mod_cast Int.toNat_of_nonneg (hn i)
      _ = (y : V n) i + 1 := by simp [hz i]
  have heq : ∀ a, ∑ i, C.W a i * (x i : ℝ) = ∑ i, C.W a i := by
    intro a
    have hyker := y.property
    simp only [CWS.subspace, Submodule.mem_iInf, LinearMap.mem_ker, dotLin_apply,
      dotProduct] at hyker
    simp_rw [hx, mul_add, mul_one, Finset.sum_add_distrib]
    rw [hyker a, zero_add]
  refine ⟨⟨x, heq⟩, Subtype.ext ?_⟩
  funext i
  change (x i : ℝ) - 1 = (y : V n) i
  rw [hx]
  ring

/-- Arbitrary integer coefficient certificates, including coordinates and
pairs. Their thresholds in `X` coordinates are the sums of the coefficients. -/
def integerEval {r n : Nat} {C : CWS r n} (a : Fin n → Int) (x : CWSPoint C) : Int :=
  ∑ i, a i * ((x.val i : Int) - 1)

noncomputable def integerFunctional {r n : Nat} (C : CWS r n) (a : Fin n → Int) :
    C.subspace →ₗ[ℝ] ℝ := ∑ i, (a i : ℝ) • coordinate C i

theorem integerEval_correct {r n : Nat} (C : CWS r n) (a : Fin n → Int)
    (x : CWSPoint C) : integerFunctional C a (center C x) = (integerEval a x : ℝ) := by
  simp [integerFunctional, integerEval, coordinate, center]

/-- Complete final theorem for arbitrary dimension, valid CWS type, and finite
family of integer functional tests. Exact join enumeration supplies `hp`.
Every IP candidate receives `keep` (inconclusive), regardless of the cap. -/
theorem cws_integer_model_inconclusive {r n m : Nat} (C : CWS r n) (hip : C.IsIP)
    (p : Fin m → CWSPoint C) (hp : Function.Surjective p)
    (coefficients : List (Fin n → Int)) (cap : Nat) (covered : Bool) :
    decideJoin (Module.finrank ℝ C.subspace) cap m covered
      ((coefficients.map (fun a i => integerEval a (p i))).map strictSides) = .keep := by
  apply cws_model_inconclusive C hip (fun i => center C (p i))
  · ext y
    constructor
    · rintro ⟨i, rfl⟩
      exact center_generator C (p i)
    · intro hy
      obtain ⟨x, hx⟩ := center_surjective C y hy
      obtain ⟨i, rfl⟩ := hp x
      exact ⟨i, hx⟩
  · intro f hf
    obtain ⟨a, _, rfl⟩ := List.mem_map.mp hf
    exact ⟨integerFunctional C a, fun i => integerEval_correct C a (p i)⟩

/-- The headline with dimension explicitly quantified, rather than inferred
from the kernel. There is no catalogue restriction on the valid CWS type. -/
theorem cws_join_preserves_ip_all_dimensions {d r m : Nat}
    (C : CWS r (d + r)) (hip : C.IsIP)
    (p : Fin m → CWSPoint C) (hp : Function.Surjective p)
    (coefficients : List (Fin (d + r) → Int)) (cap : Nat) (covered : Bool) :
    decideJoin d cap m covered
      ((coefficients.map (fun a i => integerEval a (p i))).map strictSides) = .keep := by
  simpa only [cws_dimension, Nat.add_sub_cancel_right] using
    cws_integer_model_inconclusive C hip p hp coefficients cap covered

theorem cws_join_rejection_sound_all_dimensions {d r m : Nat}
    (C : CWS r (d + r)) (p : Fin m → CWSPoint C) (hp : Function.Surjective p)
    (coefficients : List (Fin (d + r) → Int)) (cap : Nat) (covered : Bool)
    (hreject : decideJoin d cap m covered
      ((coefficients.map (fun a i => integerEval a (p i))).map strictSides) ≠ .keep) :
    ¬ C.IsIP := by
  intro hip
  exact hreject (cws_join_preserves_ip_all_dimensions C hip p hp coefficients cap covered)

/-- Literal integer thresholds used by CUDA coordinate and pair occupancy. -/
theorem coordinate_thresholds (x : Nat) :
    (((x : Int) - 1 < 0) ↔ x = 0) ∧ ((0 < (x : Int) - 1) ↔ 2 ≤ x) := by
  omega

theorem pair_thresholds (x y : Nat) :
    (((x : Int) + (y : Int) - 2 < 0) ↔ x + y ≤ 1) ∧
      ((0 < (x : Int) + (y : Int) - 2) ↔ 3 ≤ x + y) := by
  omega

end Refpoly.JoinRejection
