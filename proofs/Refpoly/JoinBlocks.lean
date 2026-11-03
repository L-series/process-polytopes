import Refpoly.JoinEnumeration

/-! Exact counts and occupancy in a union of Cartesian fibres. Shared keys are
kept explicitly, so different fibres cannot identify the same point. -/
namespace Refpoly.JoinRejection

section Blocks
variable {S R : Type*} [DecidableEq R] [Fintype R]
  {P : R → Type*}

def blocks (keys : Finset S) (fibres : S → (r : R) → Finset (P r)) :
    Finset ((_ : S) × ((r : R) → P r)) :=
  keys.sigma (fun s => Fintype.piFinset (fibres s))

/-- The point count computed by the join: sum of products of row fibre counts. -/
theorem blocks_count (keys : Finset S) (fibres : S → (r : R) → Finset (P r)) :
    (blocks keys fibres).card = ∑ s ∈ keys, ∏ r, (fibres s r).card := by
  simp [blocks, Finset.card_sigma, Fintype.card_piFinset]

/-- Every global occupancy bit is the OR of the exact bits of its blocks. -/
theorem blocks_occupancy (keys : Finset S) (fibres : S → (r : R) → Finset (P r))
    (q : ((_ : S) × ((r : R) → P r)) → Prop) :
    (∃ z ∈ blocks keys fibres, q z) ↔
      ∃ s ∈ keys, ∃ p, (∀ r, p r ∈ fibres s r) ∧ q ⟨s, p⟩ := by
  simp only [blocks, Finset.mem_sigma, Fintype.mem_piFinset]
  constructor
  · rintro ⟨⟨s, p⟩, ⟨hs, hp⟩, hq⟩
    exact ⟨s, hs, p, hp, hq⟩
  · rintro ⟨s, hs, p, hp, hq⟩
    exact ⟨⟨s, p⟩, ⟨hs, hp⟩, hq⟩

/-- A predicate involving just one row uses its joint stored summary. This
includes a pair of private coordinates of that same row. -/
theorem one_factor_occupancy (F : (r : R) → Finset (P r))
    (hne : ∀ r, (F r).Nonempty) (a : R) (q : P a → Prop) :
    (∃ p ∈ Fintype.piFinset F, q (p a)) ↔ ∃ x ∈ F a, q x := by
  classical
  constructor
  · rintro ⟨p, hp, hq⟩
    exact ⟨p a, (Fintype.mem_piFinset.mp hp) a, hq⟩
  · rintro ⟨x, hx, hq⟩
    choose z hz using hne
    refine ⟨Function.update z a x, Fintype.mem_piFinset.mpr ?_, by simpa using hq⟩
    intro r
    by_cases hr : r = a
    · subst r; simpa using hx
    · simpa [Function.update, hr] using hz r

/-- Distinct rows vary independently once all shared coordinates are fixed.
Only here is it sound to combine two marginal category masks. -/
theorem two_factor_occupancy (F : (r : R) → Finset (P r))
    (hne : ∀ r, (F r).Nonempty) (a b : R) (hab : a ≠ b)
    (q : P a → P b → Prop) :
    (∃ p ∈ Fintype.piFinset F, q (p a) (p b)) ↔
      ∃ x ∈ F a, ∃ y ∈ F b, q x y := by
  classical
  constructor
  · rintro ⟨p, hp, hq⟩
    exact ⟨p a, (Fintype.mem_piFinset.mp hp) a,
      p b, (Fintype.mem_piFinset.mp hp) b, hq⟩
  · rintro ⟨x, hx, y, hy, hq⟩
    choose z hz using hne
    refine ⟨Function.update (Function.update z a x) b y,
      Fintype.mem_piFinset.mpr ?_, by simpa [hab] using hq⟩
    intro r
    by_cases hb : r = b
    · subst r; simpa using hy
    · by_cases ha : r = a
      · subst r; simpa [hab] using hx
      · simpa [Function.update, hb, ha] using hz r
end Blocks

/-- Matching equal prefix weights by any bijection preserves the entire row
solution set, including its fibres. Repeated weights need no unique matching. -/
theorem equal_weight_permutation {I : Type*} [Fintype I]
    (w x : I → Nat) (e : I ≃ I) (hw : ∀ i, w (e i) = w i) :
    (∑ i, w i * x (e i)) = ∑ i, w i * x i := by
  simpa only [hw] using e.sum_comp (fun i => w i * x i)

end Refpoly.JoinRejection
