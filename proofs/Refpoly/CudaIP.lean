import Refpoly.KS.PolytopeChecks
import Refpoly.JoinRejection

/-!
# IP-checker geometric obligations

These results use the production ambient-origin IP predicate. In particular,
capacity exhaustion is not a geometric rejection. The source-linked evaluator
and the finite equation-stack model live in separate modules.
-/
namespace Refpoly.CudaIP
open Set

structure Equation (d : Nat) where
  normal : Fin d → Int
  constant : Int
  deriving DecidableEq, Repr

def eval {d : Nat} (e : Equation d) (x : Fin d → Int) : Int :=
  e.constant + ∑ i, e.normal i * x i

noncomputable def linear {d : Nat} (e : Equation d) : V d →ₗ[ℝ] ℝ :=
  ∑ i, (e.normal i : ℝ) • LinearMap.proj i

noncomputable def realEval {d : Nat} (e : Equation d) (x : V d) : ℝ :=
  linear e x + (e.constant : ℝ)

theorem eval_cast {d : Nat} (e : Equation d) (x : Fin d → Int) :
    realEval e (KS.ofInt x) = (eval e x : ℝ) := by
  simp [realEval, linear, eval, KS.ofInt, add_comm]

theorem linear_ne_zero {d : Nat} (e : Equation d) (hn : e.normal ≠ 0) :
    linear e ≠ 0 := by
  classical
  intro hz
  have hi (i : Fin d) : e.normal i = 0 := by
    have h := LinearMap.congr_fun hz (Pi.single i 1)
    simpa [linear, Pi.single_apply] using h
  exact hn (funext hi)

/-- A genuine supporting inequality through or beyond the origin rules out
ambient IP. A zero normal does not supply this certificate. -/
theorem supporting_rejection {d : Nat} (S : Set (V d)) (e : Equation d)
    (hn : e.normal ≠ 0) (hc : e.constant ≤ 0)
    (hs : ∀ x ∈ S, 0 ≤ realEval e x) :
    ¬ IsIP (convexHull ℝ S) := by
  let l := linear e
  have hl : l ≠ 0 := linear_ne_zero e hn
  have hex : ∃ x, l x ≠ 0 := by
    by_contra h
    push Not at h
    exact hl (LinearMap.ext h)
  obtain ⟨x, hx⟩ := hex
  have hsurj : Function.Surjective l := by
    intro a
    exact ⟨(a / l x) • x, by simp [hx]⟩
  have hsub : convexHull ℝ S ⊆ l ⁻¹' Ici (-(e.constant : ℝ)) := by
    apply convexHull_min _ ((convex_Ici (-(e.constant : ℝ))).linear_preimage l)
    intro x hx
    have := hs x hx
    change 0 ≤ l x + (e.constant : ℝ) at this
    change -(e.constant : ℝ) ≤ l x
    linarith
  intro hip
  have hi := (LinearMap.isOpenMap_of_finiteDimensional l hsurj).image_interior_subset
    (convexHull ℝ S) ⟨0, hip, map_zero l⟩
  have hImage : l '' convexHull ℝ S ⊆ Ici (-(e.constant : ℝ)) := by
    rintro _ ⟨y, hy, rfl⟩
    exact hsub hy
  have hh := interior_mono hImage hi
  have hpos : (0 : ℝ) < e.constant := by simpa using hh
  have hnonpos : (e.constant : ℝ) ≤ 0 := by exact_mod_cast hc
  linarith

/-- Integer form of the rejection rule in `device_ip_search_bad_eq`. -/
theorem integer_supporting_rejection {d : Nat} (xs : List (Fin d → Int))
    (e : Equation d) (hn : e.normal ≠ 0) (hc : e.constant < 1)
    (hs : ∀ x ∈ xs, 0 ≤ eval e x) : ¬ IsIP (KS.PolyCode.hull ⟨xs⟩) := by
  apply supporting_rejection (KS.pointSet xs) e hn (by omega)
  rintro _ ⟨x, hx, rfl⟩
  rw [eval_cast]
  exact_mod_cast hs x hx

/-- Strict positive distance to a finite halfspace description proves IP.
The inclusion premise must be obtained from the hull invariant: valid
inequalities alone are insufficient to prove acceptance. -/
theorem halfspace_acceptance {d k : Nat} (S : Set (V d)) (es : Fin k → Equation d)
    (hc : ∀ i, 0 < (es i).constant)
    (hcover : {x | ∀ i, 0 ≤ realEval (es i) x} ⊆ convexHull ℝ S) :
    IsIP (convexHull ℝ S) := by
  let U : Set (V d) := {x | ∀ i, 0 < realEval (es i) x}
  have hopen : IsOpen U := by
    have hU : U = ⋂ i, {x | 0 < realEval (es i) x} := by ext x; simp [U]
    rw [hU]
    apply isOpen_iInter_of_finite
    intro i
    apply isOpen_lt continuous_const
    exact (linear (es i)).continuous_of_finiteDimensional.add continuous_const
  have hzero : (0 : V d) ∈ U := by
    intro i
    simpa [realEval] using (show (0 : ℝ) < (es i).constant by exact_mod_cast hc i)
  have hsub : U ⊆ convexHull ℝ S := fun x hx => hcover (fun i => (hx i).le)
  exact interior_mono hsub (by rwa [hopen.interior_eq])

/-- A deficient affine span is non-IP, regardless of whether its affine
hyperplane passes through the origin. -/
theorem deficient_span_rejection {d : Nat} (S : Set (V d))
    (h : affineSpan ℝ S ≠ ⊤) : ¬ IsIP (convexHull ℝ S) := by
  intro hip
  exact h (affineSpan_eq_top_of_nonempty_interior ⟨0, hip⟩)

/-- Equation generation cancels at the new vertex before gcd normalization. -/
theorem eev_cancellation {d : Nat} (a b : Equation d) (v : Fin d → Int) :
    eval ⟨fun i => eval b v * a.normal i - eval a v * b.normal i,
      eval b v * a.constant - eval a v * b.constant⟩ v = 0 := by
  simp only [eval, sub_mul, Finset.sum_sub_distrib, mul_assoc,
    ← Finset.mul_sum]
  ring

end Refpoly.CudaIP
