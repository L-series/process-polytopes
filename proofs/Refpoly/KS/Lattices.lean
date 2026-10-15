import Refpoly.BoxReduction
import Refpoly.KS.Contracts
import Mathlib.LinearAlgebra.Matrix.Dual

open Set Matrix Module

namespace Refpoly.KS

theorem PolyCode.isLatticePolytope {d : ℕ} (Q : PolyCode d) :
    IsLatticePolytope Q.hull := by
  classical
  have hfinite : (pointSet Q.points).Finite := Q.points.finite_toSet.image ofInt
  refine ⟨hfinite.toFinset, ?_, ?_⟩
  · intro x hx
    obtain ⟨z, _, rfl⟩ := hfinite.mem_toFinset.mp hx
    exact fun i => ⟨z i, rfl⟩
  · rw [hfinite.coe_toFinset]
    rfl

theorem polarDual_bounded_of_ip {d : ℕ} {P : Set (V d)} (hP : IsIP P) :
    Bornology.IsBounded (polarDual P) := by
  obtain ⟨ε, hε, hball⟩ := Metric.isOpen_iff.mp isOpen_interior 0 hP
  refine isBounded_iff_forall_norm_le.mpr ⟨2 / ε, fun y hy => ?_⟩
  apply (pi_norm_le_iff_of_nonneg (by positivity)).mpr
  intro i
  let x : V d := Pi.single i (ε / 2)
  have hxnorm : ‖x‖ < ε := by
    rw [show x = Pi.single i (ε / 2) from rfl, Pi.norm_single, Real.norm_eq_abs,
      abs_of_pos (by positivity : 0 < ε / 2)]
    linarith
  have hx : x ∈ P := interior_subset (hball (by simpa using hxnorm))
  have hnx : -x ∈ P := interior_subset (hball (by simpa using hxnorm))
  have hplus := hy x hx
  have hminus := hy (-x) hnx
  simp [x, dotProduct_neg] at hplus hminus
  rw [Real.norm_eq_abs, abs_le]
  constructor
  · have h : -y i ≤ 2 / ε := by
      apply (le_div_iff₀ hε).mpr
      nlinarith
    linarith
  · apply (le_div_iff₀ hε).mpr
    nlinarith

theorem polarDual_ip_of_bounded {d : ℕ} {P : Set (V d)}
    (hP : Bornology.IsBounded P) (h0 : (0 : V d) ∈ P) : IsIP (polarDual P) := by
  obtain ⟨R, hR⟩ := hP.exists_norm_le
  have hR0 : 0 ≤ R := by simpa using hR 0 h0
  let ε : ℝ := 1 / (d * R + 1)
  have hε : 0 < ε := by dsimp [ε]; positivity
  apply interior_maximal (t := Metric.ball 0 ε) _ Metric.isOpen_ball
    (Metric.mem_ball_self hε)
  intro y hy x hx
  have hyNorm : ‖y‖ < ε := by simpa using hy
  have hbound : |y ⬝ᵥ x| ≤ d * (ε * R) := by
    calc
      _ ≤ ∑ i, |y i * x i| := Finset.abs_sum_le_sum_abs _ _
      _ ≤ ∑ _ : Fin d, ε * R := by
        apply Finset.sum_le_sum
        intro i _
        rw [abs_mul]
        have hyi : |y i| ≤ ε :=
          (Real.norm_eq_abs _ ▸ norm_le_pi_norm y i).trans hyNorm.le
        have hxi : |x i| ≤ R :=
          (Real.norm_eq_abs _ ▸ norm_le_pi_norm x i).trans (hR x hx)
        exact mul_le_mul hyi hxi (abs_nonneg _) hε.le
      _ = _ := by simp [Finset.sum_const, mul_comm]
  have hlt : d * (ε * R) < 1 := by
    change (d : ℝ) * (1 / (d * R + 1) * R) < 1
    rw [show (d : ℝ) * (1 / (d * R + 1) * R) =
      d * R / (d * R + 1) by ring, div_lt_one (by positivity)]
    linarith
  linarith [(abs_le.mp hbound).1]

theorem span_extremePoints_of_compact_ip {d : ℕ} {P : Set (V d)}
    (hcompact : IsCompact P) (hconvex : Convex ℝ P) (hP : IsIP P) :
    Submodule.span ℝ (P.extremePoints ℝ) = ⊤ := by
  let K := Submodule.span ℝ (P.extremePoints ℝ)
  have hsub : P ⊆ (K : Set (V d)) := by
    rw [← closure_convexHull_extremePoints hcompact hconvex]
    exact closure_minimal (convexHull_min Submodule.subset_span K.convex)
      K.closed_of_finiteDimensional
  exact K.eq_top_of_nonempty_interior' ⟨0, interior_mono hsub hP⟩

theorem exists_basis_in_spanning_set {d : ℕ} (s : Set (V d))
    (hs : Submodule.span ℝ s = ⊤) :
    ∃ b : Basis (Fin d) ℝ (V d), ∀ i, b i ∈ s := by
  classical
  obtain ⟨t, hts, htspan, htind⟩ := exists_linearIndependent ℝ s
  let b : Basis t ℝ (V d) := Basis.mk htind (by simpa [hs] using htspan.ge)
  letI := FiniteDimensional.fintypeBasisIndex b
  have hcard : Fintype.card t = d := by
    rw [← Module.finrank_eq_card_basis b]
    simp [V]
  let e : t ≃ Fin d := Fintype.equivFinOfCardEq hcard
  refine ⟨b.reindex e, fun i => ?_⟩
  rw [Basis.reindex_apply, Basis.mk_apply]
  exact hts (e.symm i).property

def ofIntHom (d : ℕ) : IntPoint d →+ V d where
  toFun := ofInt
  map_zero' := by ext i; simp [ofInt]
  map_add' := by intro x y; ext i; simp [ofInt]

def integralImage {d : ℕ} (f : V d ≃ₗ[ℝ] V d) (L : AddSubgroup (V d)) :
    Submodule ℤ (IntPoint d) :=
  (L.comap (f.symm.toLinearMap.toAddMonoidHom.comp (ofIntHom d))).toIntSubmodule

theorem integralImage_injOn {d : ℕ} (f : V d ≃ₗ[ℝ] V d)
    (F : AddSubgroup (V d)) (hF : ∀ x ∈ F, IsLatticePoint (f x)) :
    Set.InjOn (integralImage f) {L : AddSubgroup (V d) | L ≤ F} := by
  intro L hL M hM hLM
  have hle (L M : AddSubgroup (V d)) (hL : L ≤ F)
      (heq : integralImage f L = integralImage f M) : L ≤ M := by
    intro x hx
    obtain ⟨z, hz⟩ : ∃ z : IntPoint d, ofInt z = f x := by
      choose z hz using hF x (hL hx)
      exact ⟨z, funext fun i => (hz i).symm⟩
    have hback : f.symm (ofInt z) = x := by rw [hz]; simp
    have hmem : z ∈ integralImage f L := by
      change f.symm (ofInt z) ∈ L
      rwa [hback]
    rw [heq] at hmem
    change f.symm (ofInt z) ∈ M at hmem
    rwa [hback] at hmem
  exact le_antisymm (hle L M hL hLM) (hle M L hM hLM.symm)

theorem finite_integral_overgroups {d : ℕ} (A : BoxReduction.ZMat d)
    (hA : A.det ≠ 0) :
    {L : Submodule ℤ (IntPoint d) | BoxReduction.rowSpan A ≤ L}.Finite := by
  classical
  letI : NeZero A.det.natAbs := ⟨Int.natAbs_ne_zero.mpr hA⟩
  apply (Set.finite_range (Submodule.comap (BoxReduction.modMap d A.det.natAbs))).subset
  intro L hL
  refine ⟨Submodule.map (BoxReduction.modMap d A.det.natAbs) L, ?_⟩
  exact Submodule.comap_map_eq_self ((BoxReduction.ker_modMap_le_rowSpan A).trans hL)

theorem finiteLatticeInterval_proof : FiniteLatticeInterval := by
  classical
  intro d Q hQ
  have hcompact := Q.isLatticePolytope.isCompact
  obtain ⟨v, hv⟩ := exists_basis_in_spanning_set (Q.hull.extremePoints ℝ)
    (span_extremePoints_of_compact_ip hcompact Q.isLatticePolytope.convex hQ.1)
  have hpolarCompact : IsCompact (polarDual Q.hull) :=
    Metric.isCompact_of_isClosed_isBounded (polarDual_isClosed Q.hull)
      (polarDual_bounded_of_ip hQ.1)
  have hpolarIP : IsIP (polarDual Q.hull) :=
    polarDual_ip_of_bounded hcompact.isBounded (interior_subset hQ.1)
  obtain ⟨u, hu⟩ := exists_basis_in_spanning_set ((polarDual Q.hull).extremePoints ℝ)
    (span_extremePoints_of_compact_ip hpolarCompact (polarDual_convex Q.hull) hpolarIP)
  let f : V d ≃ₗ[ℝ] V d :=
    (dotProductEquiv ℝ (Fin d)).trans u.dualBasis.equivFun
  have hf (x : V d) (i : Fin d) : f x i = u i ⬝ᵥ x := by
    change u.dualBasis.equivFun (dotProductEquiv ℝ (Fin d) x) i = _
    rw [u.dualBasis_equivFun]
    exact dotProduct_comm x (u i)
  have hcoarse : coarsest Q.hull ≤ finest Q.hull :=
    (AddSubgroup.closure_le (finest Q.hull)).mpr hQ.2
  have hF : ∀ x ∈ finest Q.hull, IsLatticePoint (f x) := by
    intro x hx i
    obtain ⟨z, hz⟩ := hx (u i) (hu i)
    exact ⟨z, (hf x i).trans hz⟩
  choose A hA using fun i => hF (v i) (hcoarse (AddSubgroup.subset_closure (hv i)))
  have heq : BoxReduction.realMatrix A = fun i => f (v i) := by
    ext i j
    exact (hA i j).symm
  have hind : LinearIndependent ℝ (BoxReduction.realMatrix A) := by
    rw [heq]
    exact v.linearIndependent.map' f.toLinearMap (LinearMap.ker_eq_bot.mpr f.injective)
  have hdet : (BoxReduction.realMatrix A).det ≠ 0 :=
    isUnit_iff_ne_zero.mp ((Matrix.isUnit_iff_isUnit_det _).mp
      (Matrix.linearIndependent_rows_iff_isUnit.mp hind))
  have hdetInt : Matrix.det A ≠ 0 := by
    intro hz
    apply hdet
    change ((Int.castRingHom ℝ).mapMatrix (A : BoxReduction.ZMat d)).det = 0
    rw [← (Int.castRingHom ℝ).map_det A, hz]
    exact map_zero _
  have hmaps : MapsTo (integralImage f) (LatticeInterval Q)
      {L : Submodule ℤ (IntPoint d) | BoxReduction.rowSpan A ≤ L} := by
    intro L hL
    apply Submodule.span_le.mpr
    rintro _ ⟨i, rfl⟩
    change f.symm (ofInt (A i)) ∈ L
    have hvInt : ofInt (A i) = f (v i) := by ext j; exact (hA i j).symm
    rw [hvInt, f.symm_apply_apply]
    exact hL.1 (AddSubgroup.subset_closure (hv i))
  exact Set.Finite.of_injOn hmaps
    ((integralImage_injOn f (finest Q.hull) hF).mono (fun _ hL => hL.2))
    (finite_integral_overgroups A hdetInt)

end Refpoly.KS
