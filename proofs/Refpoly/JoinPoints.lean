import Mathlib

/-!
# Points of a joined CWS as a union of fibre blocks

Rows `r` place their positions `q` on coordinates `pos r q`; the first `k r`
positions are shared. A solution of row `r` has the shared prefix of exactly
one stored key `i` of its table (`prefixKey`), and the fibre of `i` holds the
solutions with that prefix. For a tuple of keys, one per row, its block is the
set of points whose row restrictions lie in the chosen fibres. This module
proves the facts the join's leaf accumulation relies on: the point set is the
disjoint union of the blocks of consistent tuples, and membership in a block
is exactly the fibre condition on every row.
-/
namespace Refpoly.JoinPoints

variable {N R : ℕ}

/-- The table view of a joined CWS. -/
structure Layout (N R : ℕ) where
  size : Fin R → ℕ
  pos : (r : Fin R) → Fin (size r) → Fin N
  sol : (r : Fin R) → Finset (Fin (size r) → ℕ)
  /-- Stored keys of row `r` and the key whose fibre contains a solution. -/
  keys : (r : Fin R) → Finset ℕ
  prefixKey : (r : Fin R) → (Fin (size r) → ℕ) → ℕ
  /-- Shared positions of row `r` and the key fields stored for them. -/
  k : Fin R → ℕ
  fields : (r : Fin R) → ℕ → Fin (size r) → ℕ

variable (L : Layout N R)

def restr (r : Fin R) (X : Fin N → ℕ) : Fin (L.size r) → ℕ := fun q => X (L.pos r q)

/-- Points: every row restriction is a solution of that row. -/
def IsPoint (X : Fin N → ℕ) : Prop := ∀ r, restr L r X ∈ L.sol r

def fibre (r : Fin R) (i : ℕ) : Finset (Fin (L.size r) → ℕ) := (L.sol r).filter (fun x => L.prefixKey r x = i)

/-- The key tuple of a point: each row's fibre. -/
def tupleOf (X : Fin N → ℕ) : Fin R → ℕ := fun r => L.prefixKey r (restr L r X)

def InBlock (t : Fin R → ℕ) (X : Fin N → ℕ) : Prop := ∀ r, restr L r X ∈ fibre L r (t r)

/-- Every point lies in the block of its own tuple, and in no other. -/
theorem point_block_unique (X : Fin N → ℕ) (t : Fin R → ℕ) :
    (IsPoint L X ∧ tupleOf L X = t) ↔ InBlock L t X := by
  constructor
  · rintro ⟨hp, rfl⟩ r
    simp [fibre, hp r, tupleOf]
  · intro h
    refine ⟨fun r => (Finset.mem_filter.mp (h r)).1, funext fun r => ?_⟩
    exact (Finset.mem_filter.mp (h r)).2

theorem blocks_disjoint (t t' : Fin R → ℕ) (h : t ≠ t') (X : Fin N → ℕ) :
    ¬ (InBlock L t X ∧ InBlock L t' X) := by
  rintro ⟨h1, h2⟩
  exact h (((point_block_unique L X t).mpr h1).2.symm.trans ((point_block_unique L X t').mpr h2).2)

/-- A point's tuple uses stored keys whenever every solution's prefix key is stored. -/
theorem tuple_keys (hkeys : ∀ r x, x ∈ L.sol r → L.prefixKey r x ∈ L.keys r) (X : Fin N → ℕ)
    (hX : IsPoint L X) : ∀ r, tupleOf L X r ∈ L.keys r := fun r => hkeys r _ (hX r)


/-! ## Gluing fibre members into points -/

/-- Fibre members of two different rows of a tuple agree on every coordinate
both rows use. -/
def Compatible (t : Fin R → ℕ) : Prop :=
  ∀ r r' q q', r ≠ r' → L.pos r q = L.pos r' q' → ∀ x ∈ fibre L r (t r), ∀ x' ∈ fibre L r' (t r'), x q = x' q'

/-- Every coordinate is used by some row. -/
def Covered : Prop := ∀ A : Fin N, ∃ r q, L.pos r q = A

/-- The point assembled from one member per row: each coordinate takes the
value of a chosen row position on it. -/
noncomputable def glue (hcov : Covered L) (f : (r : Fin R) → Fin (L.size r) → ℕ) : Fin N → ℕ :=
  fun A => f (Classical.choose (hcov A)) (Classical.choose (Classical.choose_spec (hcov A)))

theorem glue_restr (hinj : ∀ r, Function.Injective (L.pos r)) (hcov : Covered L) (t : Fin R → ℕ)
    (ht : Compatible L t) (f : (r : Fin R) → Fin (L.size r) → ℕ) (hf : ∀ r, f r ∈ fibre L r (t r))
    (r : Fin R) : restr L r (glue L hcov f) = f r := by
  funext q
  simp only [restr, glue]
  have hspec := Classical.choose_spec (Classical.choose_spec (hcov (L.pos r q)))
  by_cases hr : Classical.choose (hcov (L.pos r q)) = r
  · revert hspec
    generalize Classical.choose (Classical.choose_spec (hcov (L.pos r q))) = q'
    revert q'
    rw [hr]
    intro q' hq'
    rw [hinj r hq']
  · exact ht _ r _ q hr hspec _ (hf _) _ (hf r)

/-- A glued family of fibre members of a compatible tuple is a point of that block. -/
theorem glue_inBlock (hinj : ∀ r, Function.Injective (L.pos r)) (hcov : Covered L) (t : Fin R → ℕ)
    (ht : Compatible L t) (f : (r : Fin R) → Fin (L.size r) → ℕ) (hf : ∀ r, f r ∈ fibre L r (t r)) :
    InBlock L t (glue L hcov f) := fun r => by rw [glue_restr L hinj hcov t ht f hf r]; exact hf r

/-- Facts the builders and plan validation check: a fibre member's shared
positions hold its key's fields, and a coordinate used by two rows is a shared
position in both. -/
structure Wf : Prop where
  key_fields : ∀ r i x, x ∈ fibre L r i → ∀ q : Fin (L.size r), q.val < L.k r → x q = L.fields r i q
  shared_overlap : ∀ r r' (q : Fin (L.size r)) (q' : Fin (L.size r')), r ≠ r' → L.pos r q = L.pos r' q' →
    q.val < L.k r ∧ q'.val < L.k r'
  pos_inj : ∀ r, Function.Injective (L.pos r)

/-- Keys agreeing on every shared coordinate give a compatible tuple. -/
theorem compatible_of_fields (hw : Wf L) (t : Fin R → ℕ)
    (ht : ∀ r r' (q : Fin (L.size r)) (q' : Fin (L.size r')), q.val < L.k r → q'.val < L.k r' →
      L.pos r q = L.pos r' q' → L.fields r (t r) q = L.fields r' (t r') q') : Compatible L t := by
  intro r r' q q' hr hpos x hx x' hx'
  obtain ⟨hq, hq'⟩ := hw.shared_overlap r r' q q' hr hpos
  rw [hw.key_fields r _ x hx q hq, hw.key_fields r' _ x' hx' q' hq']
  exact ht r r' q q' hq hq' hpos

/-- A point's own tuple is compatible, and the point is the glue of its rows. -/
theorem point_glue (hw : Wf L) (hcov : Covered L) (X : Fin N → ℕ) (hX : IsPoint L X) :
    Compatible L (tupleOf L X) ∧ (∀ r, restr L r X ∈ fibre L r (tupleOf L X r)) ∧
      glue L hcov (fun r => restr L r X) = X := by
  have hin : ∀ r, restr L r X ∈ fibre L r (tupleOf L X r) := fun r => by simp [fibre, hX r, tupleOf]
  refine ⟨?_, hin, ?_⟩
  · apply compatible_of_fields L hw
    intro r r' q q' hq hq' hpos
    rw [← hw.key_fields r _ _ (hin r) q hq, ← hw.key_fields r' _ _ (hin r') q' hq']
    simp only [restr, hpos]
  · funext A
    simp only [glue, restr]
    rw [Classical.choose_spec (Classical.choose_spec (hcov A))]

/-! ## The enumeration indexed by tuples and fibre members -/

/-- Index set of the join's point enumeration: a tuple from `T` and one
fibre member per row. Its size is the join's `np`. -/
noncomputable def index (T : Finset (Fin R → ℕ)) : Finset ((_ : Fin R → ℕ) × ((r : Fin R) → Fin (L.size r) → ℕ)) :=
  T.sigma fun t => Fintype.piFinset fun r => fibre L r (t r)

theorem index_card (T : Finset (Fin R → ℕ)) :
    (index L T).card = ∑ t ∈ T, ∏ r, (fibre L r (t r)).card := by
  simp [index, Finset.card_sigma, Fintype.card_piFinset]

/-- With `T` the compatible tuples of stored keys, gluing the index is onto
the point set and lands in it. -/
theorem index_onto (hw : Wf L) (hcov : Covered L) (T : Finset (Fin R → ℕ))
    (hT : ∀ t ∈ T, Compatible L t) (hall : ∀ X, IsPoint L X → tupleOf L X ∈ T) :
    (∀ z ∈ index L T, IsPoint L (glue L hcov z.2)) ∧
      ∀ X, IsPoint L X → ∃ z ∈ index L T, glue L hcov z.2 = X := by
  constructor
  · rintro ⟨t, f⟩ hz
    simp only [index, Finset.mem_sigma, Fintype.mem_piFinset] at hz
    exact ((point_block_unique L _ t).mpr (glue_inBlock L hw.pos_inj hcov t (hT t hz.1) f hz.2)).1
  · intro X hX
    obtain ⟨_, hin, hg⟩ := point_glue L hw hcov X hX
    refine ⟨⟨tupleOf L X, fun r => restr L r X⟩, ?_, hg⟩
    simp only [index, Finset.mem_sigma, Fintype.mem_piFinset]
    exact ⟨hall X hX, hin⟩


/-! ## Occupancy of coordinate and pair functionals -/

/-- The row position `glue` reads coordinate `A` from. -/
noncomputable def owner (hcov : Covered L) (A : Fin N) : (r : Fin R) × Fin (L.size r) :=
  ⟨Classical.choose (hcov A), Classical.choose (Classical.choose_spec (hcov A))⟩

theorem glue_apply (hcov : Covered L) (f : (r : Fin R) → Fin (L.size r) → ℕ) (A : Fin N) :
    glue L hcov f A = f (owner L hcov A).1 (owner L hcov A).2 := rfl

/-- A coordinate predicate holds at some enumerated point iff some tuple's
fibre at the coordinate's owning position has a member satisfying it. -/
theorem coordinate_occupancy (hcov : Covered L) (T : Finset (Fin R → ℕ))
    (hne : ∀ t ∈ T, ∀ r, (fibre L r (t r)).Nonempty) (A : Fin N) (Q : ℕ → Prop) :
    (∃ z ∈ index L T, Q (glue L hcov z.2 A)) ↔
      ∃ t ∈ T, ∃ x ∈ fibre L (owner L hcov A).1 (t (owner L hcov A).1), Q (x (owner L hcov A).2) := by
  classical
  simp only [index, Finset.mem_sigma, glue_apply]
  constructor
  · rintro ⟨⟨t, f⟩, ⟨ht, hf⟩, hq⟩
    exact ⟨t, ht, f _, (Fintype.mem_piFinset.mp hf) _, hq⟩
  · rintro ⟨t, ht, x, hx, hq⟩
    obtain ⟨f, hf, hfa⟩ : ∃ f ∈ Fintype.piFinset (fun r => fibre L r (t r)),
        f (owner L hcov A).1 = x := by
      choose z hz using hne t ht
      refine ⟨Function.update z (owner L hcov A).1 x, Fintype.mem_piFinset.mpr fun r => ?_, by simp⟩
      by_cases hr : r = (owner L hcov A).1
      · subst hr; simpa using hx
      · simpa [Function.update, hr] using hz r
    exact ⟨⟨t, f⟩, ⟨ht, hf⟩, by dsimp only; rw [hfa]; exact hq⟩

/-- A pair predicate on two coordinates read from different rows: members of
the two fibres vary independently. -/
theorem pair_occupancy_rows (hcov : Covered L) (T : Finset (Fin R → ℕ))
    (hne : ∀ t ∈ T, ∀ r, (fibre L r (t r)).Nonempty) (A B : Fin N)
    (hAB : (owner L hcov A).1 ≠ (owner L hcov B).1) (Q : ℕ → ℕ → Prop) :
    (∃ z ∈ index L T, Q (glue L hcov z.2 A) (glue L hcov z.2 B)) ↔
      ∃ t ∈ T, ∃ x ∈ fibre L (owner L hcov A).1 (t (owner L hcov A).1),
        ∃ y ∈ fibre L (owner L hcov B).1 (t (owner L hcov B).1), Q (x (owner L hcov A).2) (y (owner L hcov B).2) := by
  classical
  simp only [index, Finset.mem_sigma, glue_apply]
  set a := (owner L hcov A).1
  set b := (owner L hcov B).1
  constructor
  · rintro ⟨⟨t, f⟩, ⟨ht, hf⟩, hq⟩
    exact ⟨t, ht, f a, (Fintype.mem_piFinset.mp hf) a, f b, (Fintype.mem_piFinset.mp hf) b, hq⟩
  · rintro ⟨t, ht, x, hx, y, hy, hq⟩
    choose z hz using hne t ht
    let f := Function.update (Function.update z a x) b y
    have hfa : f a = x := by simp [f, Function.update_of_ne hAB]
    have hfb : f b = y := by simp [f]
    refine ⟨⟨t, f⟩, ⟨ht, Fintype.mem_piFinset.mpr fun r => ?_⟩, by dsimp only; rw [hfa, hfb]; exact hq⟩
    by_cases hrb : r = b
    · subst hrb; simpa [f] using hy
    · by_cases hra : r = a
      · subst hra; simpa [f, Function.update_of_ne hrb] using hx
      · simpa [f, Function.update, hra, hrb] using hz r

theorem glue_at (hcov : Covered L) {A : Fin N} {r : Fin R} {qa : Fin (L.size r)}
    (h : owner L hcov A = ⟨r, qa⟩) (f : (r : Fin R) → Fin (L.size r) → ℕ) : glue L hcov f A = f r qa := by
  have key : ∀ o : (r : Fin R) × Fin (L.size r), o = ⟨r, qa⟩ → f o.1 o.2 = f r qa := by
    rintro o rfl; rfl
  exact key _ h

/-- A pair predicate on two coordinates read from the same row uses that row's
joint fibre members. -/
theorem pair_occupancy_row (hcov : Covered L) (T : Finset (Fin R → ℕ))
    (hne : ∀ t ∈ T, ∀ r, (fibre L r (t r)).Nonempty) (A B : Fin N) (r : Fin R) (qa qb : Fin (L.size r))
    (hA : owner L hcov A = ⟨r, qa⟩) (hB : owner L hcov B = ⟨r, qb⟩) (Q : ℕ → ℕ → Prop) :
    (∃ z ∈ index L T, Q (glue L hcov z.2 A) (glue L hcov z.2 B)) ↔
      ∃ t ∈ T, ∃ x ∈ fibre L r (t r), Q (x qa) (x qb) := by
  classical
  simp only [index, Finset.mem_sigma, glue_at L hcov hA, glue_at L hcov hB]
  constructor
  · rintro ⟨⟨t, f⟩, ⟨ht, hf⟩, hq⟩
    exact ⟨t, ht, f r, (Fintype.mem_piFinset.mp hf) r, hq⟩
  · rintro ⟨t, ht, x, hx, hq⟩
    choose z hz using hne t ht
    refine ⟨⟨t, Function.update z r x⟩, ⟨ht, Fintype.mem_piFinset.mpr fun r' => ?_⟩, by simpa using hq⟩
    by_cases hr : r' = r
    · subst hr; simpa using hx
    · simpa [Function.update, hr] using hz r'

end Refpoly.JoinPoints
