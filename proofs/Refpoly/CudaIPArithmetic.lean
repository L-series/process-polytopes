import Refpoly.CudaIPExpr

namespace Refpoly.CudaIP

def min64 : Int := -(2^63)
def max64 : Int := 2^63 - 1

def addOverflow (a b : Int) : Prop :=
  (0 < b ∧ max64 - b < a) ∨ (b < 0 ∧ a < min64 - b)

def subOverflow (a b : Int) : Prop :=
  (b < 0 ∧ max64 + b < a) ∨ (0 < b ∧ a < min64 + b)

def mulOverflow (a b : Int) : Prop :=
  (0 < a ∧ ((0 < b ∧ max64.tdiv b < a) ∨ (b < 0 ∧ b < min64.tdiv a))) ∨
  (a < 0 ∧ ((0 < b ∧ a < min64.tdiv b) ∨ (b < 0 ∧ a < max64.tdiv b)))

def divInvalid (a b : Int) : Prop := b = 0 ∨ (a = min64 ∧ b = -1)

theorem addOverflow_exact (a b : Int) (ha : fits64 a) (hb : fits64 b) :
    ¬ addOverflow a b ↔ fits64 (a + b) := by
  unfold addOverflow min64 max64 fits64 at *
  omega

theorem subOverflow_exact (a b : Int) (ha : fits64 a) (hb : fits64 b) :
    ¬ subOverflow a b ↔ fits64 (a - b) := by
  unfold subOverflow min64 max64 fits64 at *
  omega

private theorem pos_tdiv_bound (n b a : Int) (hn : 0 ≤ n) (hb : 0 < b) :
    a ≤ n.tdiv b ↔ a * b ≤ n := by
  rw [Int.tdiv_eq_ediv_of_nonneg hn, Int.le_ediv_iff_mul_le hb]

private theorem neg_tdiv_bound (n b a : Int) (hn : 0 ≤ n) (hb : 0 < b) :
    (-n).tdiv b ≤ a ↔ -n ≤ a * b := by
  rw [Int.neg_tdiv]
  have h := pos_tdiv_bound n b (-a) hn hb
  simp only [neg_mul] at h
  omega

private theorem tdiv_neg_bound (n b a : Int) (hn : 0 ≤ n) (hb : b < 0) :
    n.tdiv b ≤ a ↔ a * b ≤ n := by
  have hdiv : n.tdiv b = -(n.tdiv (-b)) := by simp
  rw [hdiv]
  have h := pos_tdiv_bound n (-b) (-a) hn (by omega)
  simp only [neg_mul, mul_neg, neg_neg] at h
  omega

/-- Literal multiplication guard, including C++ truncation toward zero for
negative quotients. No product is evaluated by the source guard. -/
theorem mulOverflow_exact (a b : Int) : ¬ mulOverflow a b ↔ fits64 (a * b) := by
  by_cases haz : a = 0
  · subst a; simp [mulOverflow, fits64]
  by_cases hbz : b = 0
  · subst b; simp [mulOverflow, fits64]
  have hn : (0 : Int) ≤ 2^63 := by norm_num
  have hm : (0 : Int) ≤ max64 := by norm_num [max64]
  by_cases ha : 0 < a
  · by_cases hb : 0 < b
    · have hq := pos_tdiv_bound max64 b a hm hb
      have hp : 0 ≤ a*b := mul_nonneg ha.le hb.le
      simp only [mulOverflow, ha, hb, not_lt.mpr ha.le, not_lt.mpr hb.le,
        false_and, or_false, true_and, not_lt]
      unfold fits64 max64 at *
      omega
    · have hbn : b < 0 := by omega
      have hq := neg_tdiv_bound (2^63) a b hn ha
      rw [mul_comm b a] at hq
      have hp : a*b ≤ 0 := mul_nonpos_of_nonneg_of_nonpos ha.le hbn.le
      simp only [mulOverflow, ha, hb, hbn, not_lt.mpr ha.le,
        false_and, or_false, true_and, false_or, not_lt]
      unfold fits64 min64 at *
      omega
  · have han : a < 0 := by omega
    by_cases hb : 0 < b
    · have hq := neg_tdiv_bound (2^63) b a hn hb
      have hp : a*b ≤ 0 := mul_nonpos_of_nonpos_of_nonneg han.le hb.le
      simp only [mulOverflow, ha, han, hb, not_lt.mpr hb.le,
        false_and, false_or, or_false, true_and, not_lt]
      unfold fits64 min64 at *
      omega
    · have hbn : b < 0 := by omega
      have hq := tdiv_neg_bound max64 b a hm hbn
      have hp : 0 ≤ a*b := mul_nonneg_of_nonpos_of_nonpos han.le hbn.le
      simp only [mulOverflow, ha, han, hb, hbn, false_and, false_or,
        true_and, not_lt]
      unfold fits64 max64 at *
      omega

/-- The fast small-operand multiplication path cannot overflow int64. -/
theorem small_mul_fits (a b : Int)
    (ha : -(2^31) ≤ a ∧ a < 2^31) (hb : -(2^31) ≤ b ∧ b < 2^31) :
    fits64 (a*b) := by
  unfold fits64
  constructor <;> nlinarith

end Refpoly.CudaIP
