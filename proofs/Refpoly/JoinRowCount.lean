import Refpoly.JoinRows

/-! Exact solution count of one weight row by the in-place coin-change DP of the
host table builders (`row_solution_count`, src/gpu/join.cuh). A list of distinct
valid solutions as long as that count is the complete solution set, so the
builders check the row enumeration for omissions. -/

namespace Refpoly.JoinRejection

/-- One in-place pass `for r = w..degree: dp[r] += dp[r - w]` (forward order):
the new value at `r` adds the already updated value at `r - w`. -/
def dpStep (w : ℕ) (f : ℕ → ℕ) : ℕ → ℕ
  | r => if _ : 0 < w ∧ w ≤ r then f r + dpStep w f (r - w) else f r
termination_by r => r
decreasing_by omega

/-- Passes over the weights from last to first, starting from `dp = [1, 0, 0, ...]`. -/
def dpCount : List ℕ → ℕ → ℕ
  | [] => fun r => if r = 0 then 1 else 0
  | w :: ws => dpStep w (dpCount ws)

theorem range_peel (g : ℕ → ℕ) (n : ℕ) :
    ((List.range (n + 1)).map g).sum = g 0 + ((List.range n).map fun x => g (x + 1)).sum := by
  rw [List.range_succ_eq_map, List.map_cons, List.sum_cons, List.map_map]
  rfl

theorem dpStep_sum (w : ℕ) (hw : 0 < w) (f : ℕ → ℕ) :
    ∀ r, dpStep w f r = ((List.range (r / w + 1)).map fun x => f (r - x * w)).sum := by
  intro r
  induction r using Nat.strong_induction_on with
  | _ r ih =>
    rw [dpStep]
    by_cases hr : w ≤ r
    · rw [dif_pos ⟨hw, hr⟩, ih (r - w) (by omega)]
      conv_rhs => rw [Nat.div_eq_sub_div hw hr, range_peel]
      simp only [Nat.zero_mul, Nat.sub_zero]
      congr 1
      congr 1
      apply List.map_congr_left
      intro x _
      congr 1
      rw [Nat.succ_mul]; omega
    · rw [dif_neg (by omega)]
      have : r / w = 0 := Nat.div_eq_of_lt (by omega)
      simp [this]

/-- The DP counts exactly the solutions the proved row enumeration lists. -/
theorem dpCount_exact (ws : List ℕ) (hw : ∀ w ∈ ws, 0 < w) :
    ∀ r, dpCount ws r = (rowSolutions ws r).length := by
  induction ws with
  | nil => intro r; by_cases h : r = 0 <;> simp [dpCount, rowSolutions, h]
  | cons w ws ih =>
    intro r
    have hw0 : 0 < w := hw w (by simp)
    rw [dpCount, dpStep_sum w hw0, rowSolutions, List.length_flatMap]
    congr 1
    apply List.map_congr_left
    intro x _
    rw [ih (fun v hv => hw v (by simp [hv])), List.length_map]

/-- `rowSolutions` lists each solution once. -/
theorem rowSolutions_nodup (ws : List ℕ) : ∀ r, (rowSolutions ws r).Nodup := by
  induction ws with
  | nil => intro r; by_cases h : r = 0 <;> simp [rowSolutions, h]
  | cons w ws ih =>
    intro r
    rw [rowSolutions, List.nodup_flatMap]
    refine ⟨fun x _ => (ih _).map (fun _ _ h => List.cons.inj h |>.2), ?_⟩
    refine List.Pairwise.imp_of_mem ?_ List.nodup_range
    intro a b _ _ hab
    rw [Function.onFun, List.disjoint_left]
    intro l h1 h2
    obtain ⟨_, _, rfl⟩ := List.mem_map.mp h1
    obtain ⟨_, _, he⟩ := List.mem_map.mp h2
    exact hab (List.cons.inj he).1.symm

/-- A list of distinct valid row solutions as long as the DP count is the
complete solution set: the table builder's completeness check. -/
theorem enumerated_complete (ws : List ℕ) (hw : ∀ w ∈ ws, 0 < w) (r : ℕ) (pts : List (List ℕ))
    (hnodup : pts.Nodup) (hvalid : ∀ x ∈ pts, x.length = ws.length ∧ rowDot ws x = r)
    (hcount : pts.length = dpCount ws r) :
    ∀ x, x ∈ pts ↔ x.length = ws.length ∧ rowDot ws x = r := by
  intro x
  rw [← rowSolutions_exact ws hw r x]
  have hsub : pts ⊆ rowSolutions ws r := fun y hy => (rowSolutions_exact ws hw r y).mpr (hvalid y hy)
  have hlen : (rowSolutions ws r).length ≤ pts.length := by rw [hcount, dpCount_exact ws hw]
  have hperm := (List.subperm_of_subset hnodup hsub).perm_of_length_le hlen
  exact hperm.mem_iff

end Refpoly.JoinRejection
