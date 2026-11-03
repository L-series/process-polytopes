import Refpoly.JoinBlocks

/-! Complete enumeration of a single positive integer weight row. This is the
recursive bounded loop used by the host table builders, before grouping by keys. -/
namespace Refpoly.JoinRejection

def rowDot : List Nat → List Nat → Nat
  | w :: ws, x :: xs => x * w + rowDot ws xs
  | _, _ => 0

def rowSolutions : List Nat → Nat → List (List Nat)
  | [], degree => if degree = 0 then [[]] else []
  | w :: ws, degree => (List.range (degree / w + 1)).flatMap fun x =>
      (rowSolutions ws (degree - x * w)).map (fun xs => x :: xs)

/-- Completeness and soundness of every recursion level, without degree or
row-size limits. Positive weights justify the loop bound and termination. -/
theorem rowSolutions_exact (ws : List Nat) (hw : ∀ w ∈ ws, 0 < w)
    (degree : Nat) (xs : List Nat) :
    xs ∈ rowSolutions ws degree ↔ xs.length = ws.length ∧ rowDot ws xs = degree := by
  induction ws generalizing degree xs with
  | nil =>
    cases xs <;> simp [rowSolutions, rowDot, eq_comm]
  | cons w ws ih =>
    have hw0 : 0 < w := hw w (by simp)
    have hws : ∀ v ∈ ws, 0 < v := fun v hv => hw v (by simp [hv])
    cases xs with
    | nil => simp [rowSolutions]
    | cons x xs =>
      constructor
      · intro h
        obtain ⟨v, hv, hmem⟩ := List.mem_flatMap.mp h
        obtain ⟨ys, hys, heq⟩ := List.mem_map.mp hmem
        obtain ⟨rfl, rfl⟩ := List.cons.inj heq
        have hbound : v * w ≤ degree := (Nat.le_div_iff_mul_le hw0).mp
          (by have := List.mem_range.mp hv; omega)
        obtain ⟨hlen, hdot⟩ := (ih hws _ _).mp hys
        simp only [List.length_cons, rowDot]
        constructor
        · omega
        · omega
      · rintro ⟨hlen, hdot⟩
        simp only [List.length_cons] at hlen
        simp only [rowDot] at hdot
        have hbound : x * w ≤ degree := by omega
        have hdiv : x ≤ degree / w := (Nat.le_div_iff_mul_le hw0).mpr hbound
        apply List.mem_flatMap.mpr
        refine ⟨x, List.mem_range.mpr (by omega), List.mem_map.mpr ?_⟩
        refine ⟨xs, (ih hws _ _).mpr ⟨by omega, by omega⟩, rfl⟩

/-- CUDA's last-coordinate divisibility optimization has exactly the same
solutions as the recursive specification. -/
theorem last_coordinate_exact (w degree x : Nat) (hw : 0 < w) :
    (degree % w = 0 ∧ x = degree / w) ↔ x * w = degree := by
  constructor
  · rintro ⟨hmod, rfl⟩
    exact Nat.div_mul_cancel (Nat.dvd_of_mod_eq_zero hmod)
  · intro h
    constructor
    · rw [← h, Nat.mul_mod_left]
    · rw [← h, Nat.mul_div_cancel x hw]

end Refpoly.JoinRejection
