/-
Reflexive duality faithfully ported from the reviewed refpoly-5d snapshot.
See SOURCES.md for attribution and exact source identities.
-/
import Refpoly.Basic

open Set Matrix
namespace Refpoly
variable {n : ℕ}

/-! ## Reflexive polytopes are closed under duality (T3.3) -/

/-- For a reflexive polytope, `P** = P`. -/
theorem IsReflexive.bipolar_eq {P : Set (V n)} (hP : IsReflexive P) :
    polarDual (polarDual P) = P :=
  bipolar hP.latticePolytope.convex hP.latticePolytope.isClosed
    (interior_subset hP.ip)

/-- The polar dual of a reflexive polytope is reflexive ([SS18] §2.1:
"the dual of an IP polytope is itself an IP polytope with Δ** = Δ"). -/
theorem IsReflexive.polarDual_reflexive {P : Set (V n)} (hP : IsReflexive P) :
    IsReflexive (polarDual P) where
  latticePolytope := hP.dual_latticePolytope
  ip := by
    -- 0 is interior to P*: P is bounded, so a small ball pairs ≥ -1 with P.
    obtain ⟨R, hR⟩ := hP.latticePolytope.isCompact.isBounded.exists_norm_le
    have hR0 : (0 : ℝ) ≤ R := by
      simpa using hR 0 (interior_subset hP.ip)
    set ε : ℝ := 1 / (n * R + 1) with hε
    have hεpos : 0 < ε := by positivity
    have hball : Metric.ball (0 : V n) ε ⊆ polarDual P := by
      intro y hy x hx
      rw [Metric.mem_ball, dist_zero_right] at hy
      have hbound : |y ⬝ᵥ x| ≤ n * (ε * R) := by
        calc |y ⬝ᵥ x| ≤ ∑ i, |y i * x i| := Finset.abs_sum_le_sum_abs _ _
          _ ≤ ∑ _i : Fin n, ε * R := by
              apply Finset.sum_le_sum
              intro i _
              rw [abs_mul]
              have hy1 : |y i| ≤ ε := by
                calc |y i| = ‖y i‖ := (Real.norm_eq_abs _).symm
                  _ ≤ ‖y‖ := norm_le_pi_norm y i
                  _ ≤ ε := hy.le
              have hx1 : |x i| ≤ R := by
                calc |x i| = ‖x i‖ := (Real.norm_eq_abs _).symm
                  _ ≤ ‖x‖ := norm_le_pi_norm x i
                  _ ≤ R := hR x hx
              exact mul_le_mul hy1 hx1 (abs_nonneg _) hεpos.le
          _ = n * (ε * R) := by simp [Finset.sum_const, mul_comm]
      have hlt : n * (ε * R) < 1 := by
        rw [hε]
        rw [show (n : ℝ) * (1 / (n * R + 1) * R) = n * R / (n * R + 1) by ring]
        rw [div_lt_one (by positivity)]
        linarith
      have := abs_le.1 hbound
      linarith
    exact interior_maximal hball Metric.isOpen_ball (Metric.mem_ball_self hεpos)
  dual_latticePolytope := by
    rw [hP.bipolar_eq]
    exact hP.latticePolytope

end Refpoly
