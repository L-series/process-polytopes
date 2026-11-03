import Refpoly.JoinType3Search

/-! Concrete word64 presence/Bloom expressions and the builder's final
all-key checks. Presence reordering is proved against the actual remapped
lookup word. Hash collisions never require an injectivity assumption. -/
namespace Refpoly.JoinType3

def presenceBits (masks : Fin 3 → ℕ) (key : ℕ) : Bool :=
  decide (∀ q, (masks q).testBit (min (fields key q) 63) = true)

def hash9 (multiplier key : ℕ) : ℕ := ((key*multiplier) % 2^64) / 2^55

theorem hash9_bound (multiplier key : ℕ) : hash9 multiplier key < 512 := by
  unfold hash9
  apply (Nat.div_lt_iff_lt_mul (by norm_num)).mpr
  have h := Nat.mod_lt (key*multiplier) (show 0<2^64 by norm_num)
  norm_num at h ⊢
  exact h

def bloomBits (bits : ℕ → ℕ) (key : ℕ) : Bool :=
  let h1 := hash9 0x9E3779B97F4A7C15 key
  let h2 := hash9 0xC2B2AE3D27D4EB4F key
  (bits (h1/64)).testBit (h1%64) && (bits (h2/64)).testBit (h2%64)

theorem bloom_indices (multiplier key : ℕ) :
    hash9 multiplier key / 64 < 8 ∧ hash9 multiplier key % 64 < 64 := by
  have := hash9_bound multiplier key
  constructor
  · omega
  · exact Nat.mod_lt _ (by omega)

def candidatePresenceLeft (p : Fin 3 ≃ Fin 3) (masks : Fin 3 → ℕ) (leftKey : ℕ) : Bool :=
  decide (∀ ci, (masks (p ci)).testBit (min (fields leftKey ci) 63) = true)

def candidatePresenceRight (p : Fin 3 ≃ Fin 3) (masks : Fin 3 → ℕ) (rightKey : ℕ) : Bool :=
  decide (∀ ci, (masks ci).testBit (min (fields rightKey (p ci)) 63) = true)

theorem left_presence_remap (p : Fin 3 ≃ Fin 3) (masks : Fin 3 → ℕ) (key : ℕ) :
    candidatePresenceLeft p masks key = presenceBits masks (remap p.symm key) := by
  apply decide_eq_decide.mpr
  constructor
  · intro h q
    simpa only [fields_remap,Equiv.apply_symm_apply] using h (p.symm q)
  · intro h q
    simpa only [fields_remap,Equiv.symm_apply_apply] using h (p q)

theorem right_presence_remap (p : Fin 3 ≃ Fin 3) (masks : Fin 3 → ℕ) (key : ℕ) :
    candidatePresenceRight p masks key = presenceBits masks (remap p key) := by
  simp [candidatePresenceRight,presenceBits,fields_remap]

/-- The builder's independently checked final presence/Bloom loops. Each
source throw corresponds to one false bit in this all-canonical-key check. -/
def checkedRangeFilters (keys : ℕ → ℕ) (b e : ℕ) (masks : Fin 3 → ℕ) (bits : ℕ → ℕ) : Bool :=
  (sourceIndices b e).all fun i => presenceBits masks (keys i) && bloomBits bits (keys i)

theorem checked_filters_complete (keys : ℕ → ℕ) (b e : ℕ) (masks : Fin 3 → ℕ) (bits : ℕ → ℕ)
    (h : checkedRangeFilters keys b e masks bits = true) :
    FiltersComplete keys b e (presenceBits masks) (bloomBits bits) := by
  intro j hb hj
  have hh := List.all_eq_true.mp h j ((sourceIndices_mem b e j).mpr ⟨hb,hj⟩)
  simpa only [Bool.and_eq_true] using hh
end Refpoly.JoinType3
