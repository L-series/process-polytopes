import Refpoly.JoinSummary

/-!
Count arithmetic of `device_gjoin_leaf` in src/gpu/join.cuh. Source guards
are pinned by scripts/check_join_leaf_model.py. Naturals represent unsigned
words before wrap; successful guards prove every operation stays in range.
The mask CBMC query fixes counts to one because masks never read counts.
-/
namespace Refpoly.JoinRejection

/-- The leaf's ordered multiplication loop, including rejection of empty fibres. -/
def leafProduct (max : Nat) : List Nat → Nat → Option Nat
  | [], cnt => some cnt
  | b :: bs, cnt => do
      let next ← checkedMul max cnt b
      leafProduct max bs next

/-- Literal final guard (the subtrahend has already passed the product guard). -/
def leafCount (max np : Nat) (counts : List Nat) : Option Nat := do
  let cnt ← leafProduct max counts 1
  if max - cnt < np then none else some (np + cnt)

theorem checkedMul_iff {max a b v : Nat} :
    checkedMul max a b = some v ↔ b ≠ 0 ∧ a * b ≤ max ∧ v = a * b := by
  unfold checkedMul
  by_cases hb : b = 0
  · simp [hb]
  · have hp : 0 < b := Nat.pos_of_ne_zero hb
    have hd : a ≤ max / b ↔ a * b ≤ max := Nat.le_div_iff_mul_le hp
    split_ifs with h
    · simp only [false_iff, not_and]
      intro _ hm
      have := hd.mpr hm
      omega
    · constructor
      · intro he
        exact ⟨hb, hd.mp (by omega), (Option.some.inj he).symm⟩
      · rintro ⟨_, _, rfl⟩; rfl

theorem leafProduct_exact {max cnt v : Nat} {counts : List Nat}
    (hcnt : cnt ≤ max) (h : leafProduct max counts cnt = some v) :
    v = cnt * counts.prod ∧ v ≤ max ∧ ∀ b ∈ counts, b ≠ 0 := by
  induction counts generalizing cnt with
  | nil =>
    have hv : cnt = v := Option.some.inj h
    subst v
    simp [hcnt]
  | cons b bs ih =>
    simp only [leafProduct] at h
    cases hm : checkedMul max cnt b with
    | none => simp [hm] at h
    | some next =>
      simp [hm] at h
      obtain ⟨hb, hn, rfl⟩ := checkedMul_iff.mp hm
      obtain ⟨hv, hvmax, hne⟩ := ih hn h
      exact ⟨by simpa [List.prod_cons, Nat.mul_assoc] using hv, hvmax,
        by simpa only [List.mem_cons, forall_eq_or_imp] using And.intro hb hne⟩

/-- Any nonempty factors with a representable final product pass every guard. -/
theorem leafProduct_complete {max cnt : Nat} {counts : List Nat}
    (hne : ∀ b ∈ counts, b ≠ 0) (hbound : cnt * counts.prod ≤ max) :
    leafProduct max counts cnt = some (cnt * counts.prod) := by
  induction counts generalizing cnt with
  | nil => simp [leafProduct]
  | cons b bs ih =>
    have hb := hne b (by simp)
    have hbs : ∀ b ∈ bs, b ≠ 0 := fun b hm => hne b (by simp [hm])
    have hp : 0 < bs.prod := List.prod_pos (fun b hm => Nat.pos_of_ne_zero (hbs b hm))
    have hn : cnt * b ≤ max := le_trans (Nat.le_mul_of_pos_right _ hp)
      (by simpa [List.prod_cons, Nat.mul_assoc] using hbound)
    have hm : checkedMul max cnt b = some (cnt * b) := checkedMul_iff.mpr ⟨hb, hn, rfl⟩
    simp only [leafProduct, hm]
    dsimp
    simpa [List.prod_cons, Nat.mul_assoc] using ih (cnt := cnt * b) hbs (by
      simpa [List.prod_cons, Nat.mul_assoc] using hbound)

/-- Success is equivalent to nonempty factors and an exactly representable sum.
This covers zero counts, multiplication overflow and final addition overflow. -/
theorem leafCount_iff {max np v : Nat} {counts : List Nat} (hmax : 1 ≤ max) :
    leafCount max np counts = some v ↔
      (∀ b ∈ counts, b ≠ 0) ∧ np + counts.prod ≤ max ∧ v = np + counts.prod := by
  constructor
  · intro h
    unfold leafCount at h
    cases hm : leafProduct max counts 1 with
    | none => simp [hm] at h
    | some cnt =>
      obtain ⟨hc, hbound, hne⟩ := leafProduct_exact hmax hm
      simp only [one_mul] at hc
      subst cnt
      simp only [hm] at h
      dsimp at h
      split_ifs at h with hg
      exact ⟨hne, by omega, (Option.some.inj h).symm⟩
  · rintro ⟨hne, hbound, rfl⟩
    have hm := leafProduct_complete (cnt := 1) hne (show 1 * counts.prod ≤ max by omega)
    simp only [one_mul] at hm
    simp [leafCount, hm, show ¬ max - counts.prod < np by omega]

end Refpoly.JoinRejection
