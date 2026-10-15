/-
Sylvester faithfully ported from the reviewed refpoly-5d snapshot.
See SOURCES.md for attribution and exact source identities.
Only scoped arithmetic and existential degree results are proved.
No sharp numerical bound or IP certificate for qct6/qlt6 is supplied.
-/
import Refpoly.DegreeBound

open Finset

namespace Refpoly

/-! ## The Sylvester sequence -/

/-- **D8.1: the Sylvester sequence** `2, 3, 7, 43, 1807, 3263443, …`,
`y₀ = 2`, `y_{k+1} = y_k (y_k − 1) + 1` (equivalently `y_{k+1} = y_k² − y_k + 1`).
[Sk96] §4, [SS18] §2. -/
def sylvester : ℕ → ℕ
  | 0 => 2
  | k + 1 => sylvester k * (sylvester k - 1) + 1

@[simp] theorem sylvester_zero : sylvester 0 = 2 := rfl
theorem sylvester_one : sylvester 1 = 3 := rfl
theorem sylvester_two : sylvester 2 = 7 := rfl
theorem sylvester_three : sylvester 3 = 43 := rfl
theorem sylvester_four : sylvester 4 = 1807 := rfl
theorem sylvester_five : sylvester 5 = 3263443 := rfl

/-- Every Sylvester number is at least 2 (so in particular positive, and
subtraction of 1 is well-behaved). -/
theorem sylvester_two_le (k : ℕ) : 2 ≤ sylvester k := by
  induction k with
  | zero => simp [sylvester]
  | succ k ih =>
    have h1 : 1 ≤ sylvester k - 1 := by omega
    have : 2 * 1 ≤ sylvester k * (sylvester k - 1) :=
      Nat.mul_le_mul ih h1
    simp only [sylvester]
    omega

theorem sylvester_pos (k : ℕ) : 0 < sylvester k :=
  (sylvester_two_le k).trans_lt' two_pos

/-- **L8.2 (telescoping product):** `y₀ y₁ ⋯ y_{k−1} = y_k − 1`.
This is the key identity behind the Egyptian-fraction property; it follows
since `y_{k+1} − 1 = y_k (y_k − 1)`. -/
theorem prod_sylvester (k : ℕ) :
    ∏ i ∈ Finset.range k, sylvester i = sylvester k - 1 := by
  induction k with
  | zero => simp [sylvester]
  | succ k ih =>
    rw [Finset.prod_range_succ, ih]
    show (sylvester k - 1) * sylvester k = sylvester (k + 1) - 1
    simp only [sylvester]
    rw [Nat.add_sub_cancel]
    exact Nat.mul_comm _ _

/-- **L8.3 (Egyptian fractions / unit-fraction identity):**
`∑_{i<k} 1/yᵢ = 1 − 1/(y_k − 1)`.  In particular the reciprocals of the
Sylvester numbers greedily approach 1 from below, and replacing the last term
`1/y_{k}` by `1/(y_k − 1)` gives exactly 1.  [Sk96] §4. -/
theorem sylvester_sum_inv (k : ℕ) :
    ∑ i ∈ Finset.range k, (1 : ℚ) / sylvester i
      = 1 - 1 / ((sylvester k : ℚ) - 1) := by
  induction k with
  | zero => norm_num [sylvester]
  | succ k ih =>
    rw [Finset.sum_range_succ, ih]
    have h2 : 2 ≤ sylvester k := sylvester_two_le k
    have hcast : ((sylvester (k + 1) : ℚ)) - 1
        = (sylvester k : ℚ) * ((sylvester k : ℚ) - 1) := by
      simp only [sylvester]
      push_cast [Nat.cast_sub (by omega : 1 ≤ sylvester k)]
      ring
    rw [hcast]
    have hy : (1 : ℚ) < (sylvester k : ℚ) := by
      exact_mod_cast (by omega : 1 < sylvester k)
    have hne : (sylvester k : ℚ) ≠ 0 := by linarith
    have hne1 : (sylvester k : ℚ) - 1 ≠ 0 := by linarith
    field_simp
    ring

/-! ## The extremal weight systems in dimension 5 ([SS18] §2) -/

/-- The maximal-degree IP weight system for `n = 6`, in normalized
(`∑ qᵢ = 1`) form: `q = (1/2, 1/3, 1/7, 1/43, 1/1807, 1/3263442)`.
Note the *last* entry is `1/(y₅ − 1) = 1/3263442`, not `1/y₅`: the identity
L8.3 with `k = 5` makes the sum exactly 1.  Clearing denominators gives the
integer weight system of degree `d = lcm = 3263442 = 1806·1807`. -/
noncomputable def qct6 : V 6 :=
  ![1/2, 1/3, 1/7, 1/43, 1/1807, 1/3263442]

/-- `3263442 = 1806 · 1807` — the maximal degree factors through the
Sylvester sequence (`1806 = y₄ − 1 = 2·3·7·43`, `1807 = y₄`). -/
theorem max_degree_factorization : 3263442 = 1806 * 1807 := by norm_num

theorem qct6_pos : ∀ i, 0 < qct6 i := by
  intro i; fin_cases i <;> norm_num [qct6]

/-- **L8.5: the candidate weight system is normalized**, `∑ qᵢ = 1`
(this is L8.3 with `k = 5`, verified directly by rational arithmetic). -/
theorem qct6_sum : ∑ i, qct6 i = 1 := by
  norm_num [qct6, Fin.sum_univ_succ, Matrix.cons_val_zero, Matrix.cons_val_succ]

/-- The second extremal weight system of [SS18] (maximal degree among weight
systems with the last two weights equal): `(1/2, 1/3, 1/7, 1/43, 1/3612, 1/3612)`,
of degree `3612 = 2·1806`. -/
noncomputable def qlt6 : V 6 :=
  ![1/2, 1/3, 1/7, 1/43, 1/3612, 1/3612]

theorem qlt6_pos : ∀ i, 0 < qlt6 i := by
  intro i; fin_cases i <;> norm_num [qlt6]

theorem qlt6_sum : ∑ i, qlt6 i = 1 := by
  norm_num [qlt6, Fin.sum_univ_succ, Matrix.cons_val_zero, Matrix.cons_val_succ]

/-- A proposed numerical degree bound. This is a proposition to be certified,
not an axiom or a theorem asserting the classification data are complete. -/
def DegreeBoundDim5 (D : ℕ) : Prop :=
  ∀ w : Fin 6 → ℕ, (∀ i, 0 < w i) → Finset.univ.gcd w = 1 →
    IsIPWeightSystem (fun i => (w i : ℝ)) → ∑ i, w i ≤ D

/-- Primitive dimension-five IP weights have a uniform degree bound.
The proof uses enumeration finiteness and uniqueness of primitive
normalization. It does not identify the bound with the reported sharp value. -/
theorem max_degree_dim5 : ∃ D : ℕ, DegreeBoundDim5 D :=
  primitive_ip_degrees_bounded 6

/-- Applying a numerical bound requires an explicit completeness certificate. -/
theorem degree_dim5_le_of_certificate {D : ℕ} (hcert : DegreeBoundDim5 D)
    (w : Fin 6 → ℕ) (hw : ∀ i, 0 < w i) (hgcd : Finset.univ.gcd w = 1)
    (hIP : IsIPWeightSystem (fun i => (w i : ℝ))) : ∑ i, w i ≤ D :=
  hcert w hw hgcd hIP

end Refpoly
