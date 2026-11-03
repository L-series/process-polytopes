import Refpoly.JoinPoints
import Refpoly.JoinCWS
import Refpoly.JoinLeafCount

/-! Finite point enumeration at the join leaf boundary. Unlike coverage alone,
injectivity ensures that the sum of fibre products is the actual point count.
The row-equation equivalence is an explicit interface premise; this module
does not claim that CUDA's weight matching has already established it. -/
namespace Refpoly.JoinPoints

variable {N R : ℕ} (L : Layout N R)

theorem index_injective (hw : Wf L) (hcov : Covered L)
    (T : Finset (Fin R → ℕ)) (hT : ∀ t ∈ T, Compatible L t) :
    Set.InjOn (fun z : (_ : Fin R → ℕ) × ((r : Fin R) → Fin (L.size r) → ℕ) =>
      glue L hcov z.2) {z | z ∈ index L T} := by
  rintro ⟨t, f⟩ hz ⟨t', f'⟩ hz' he
  change glue L hcov f = glue L hcov f' at he
  simp only [Set.mem_setOf_eq, index, Finset.mem_sigma, Fintype.mem_piFinset] at hz hz'
  have hblock := glue_inBlock L hw.pos_inj hcov t (hT t hz.1) f hz.2
  have hblock' := glue_inBlock L hw.pos_inj hcov t' (hT t' hz'.1) f' hz'.2
  have ht : t = t' := by
    have h1 := ((point_block_unique L _ t).mpr hblock).2
    have h2 := ((point_block_unique L _ t').mpr hblock').2
    rw [he] at h1
    exact h1.symm.trans h2
  have hf : f = f' := by
    funext r
    have hr := congrArg (restr L r) he
    rw [glue_restr L hw.pos_inj hcov t (hT t hz.1) f hz.2 r,
      glue_restr L hw.pos_inj hcov t' (hT t' hz'.1) f' hz'.2 r] at hr
    exact hr
  cases ht
  cases hf
  rfl

/-- The enumeration really is a bijection with the simultaneous row points. -/
noncomputable def pointEquiv (hw : Wf L) (hcov : Covered L)
    (T : Finset (Fin R → ℕ)) (hT : ∀ t ∈ T, Compatible L t)
    (hall : ∀ X, IsPoint L X → tupleOf L X ∈ T) :
    {z // z ∈ index L T} ≃ {X // IsPoint L X} := by
  let f : {z // z ∈ index L T} → {X // IsPoint L X} :=
    fun z => ⟨glue L hcov z.val.2, (index_onto L hw hcov T hT hall).1 z.val z.property⟩
  apply Equiv.ofBijective f
  constructor
  · intro a b he
    apply Subtype.ext
    exact index_injective L hw hcov T hT a.property b.property (congrArg Subtype.val he)
  · intro X
    obtain ⟨z, hz, he⟩ := (index_onto L hw hcov T hT hall).2 X.val X.property
    exact ⟨⟨z, hz⟩, Subtype.ext he⟩

/-- Exact `np`, including the absence of collisions between distinct blocks. -/
theorem point_card (hw : Wf L) (hcov : Covered L)
    (T : Finset (Fin R → ℕ)) (hT : ∀ t ∈ T, Compatible L t)
    (hall : ∀ X, IsPoint L X → tupleOf L X ∈ T) :
    Nat.card {X // IsPoint L X} = ∑ t ∈ T, ∏ r, (fibre L r (t r)).card := by
  rw [← Nat.card_congr (pointEquiv L hw hcov T hT hall)]
  simpa using index_card L T

/-- Exact successful machine count over a complete duplicate-free tuple list.
The order may be the source search order; no canonical order is needed. -/
theorem leafCounts_index {max np : ℕ} (hmax : 1 ≤ max)
    (leaves : List (Fin R → ℕ)) (hn : leaves.Nodup)
    (T : Finset (Fin R → ℕ)) (hT : leaves.toFinset = T)
    (h : Refpoly.JoinRejection.leafCounts max
      (leaves.map (fun t => List.ofFn (fun r => (fibre L r (t r)).card))) 0 = some np) :
    np = (index L T).card := by
  have he := (Refpoly.JoinRejection.leafCounts_exact hmax (Nat.zero_le _) h).1
  simp only [Nat.zero_add, List.map_map, Function.comp_def, List.prod_ofFn] at he
  rw [← List.sum_toFinset _ hn, hT, ← index_card L T] at he
  exact he

theorem leafCounts_points {max np : ℕ} (hmax : 1 ≤ max)
    (hw : Wf L) (hcov : Covered L) (leaves : List (Fin R → ℕ)) (hn : leaves.Nodup)
    (T : Finset (Fin R → ℕ)) (hT : leaves.toFinset = T)
    (hcompat : ∀ t ∈ T, Compatible L t)
    (hall : ∀ X, IsPoint L X → tupleOf L X ∈ T)
    (h : Refpoly.JoinRejection.leafCounts max
      (leaves.map (fun t => List.ofFn (fun r => (fibre L r (t r)).card))) 0 = some np) :
    np = Nat.card {X // IsPoint L X} := by
  rw [leafCounts_index L hmax leaves hn T hT h, index_card,
    point_card L hw hcov T hcompat hall]

/-- Coordinate occupancies of the index are exactly those of actual points. -/
theorem point_occupancy (hw : Wf L) (hcov : Covered L)
    (T : Finset (Fin R → ℕ)) (hT : ∀ t ∈ T, Compatible L t)
    (hall : ∀ X, IsPoint L X → tupleOf L X ∈ T) (Q : (Fin N → ℕ) → Prop) :
    (∃ z ∈ index L T, Q (glue L hcov z.2)) ↔ ∃ X, IsPoint L X ∧ Q X := by
  constructor
  · rintro ⟨z, hz, hq⟩
    exact ⟨_, (index_onto L hw hcov T hT hall).1 z hz, hq⟩
  · rintro ⟨X, hX, hq⟩
    obtain ⟨z, hz, rfl⟩ := (index_onto L hw hcov T hT hall).2 X hX
    exact ⟨z, hz, hq⟩

open Refpoly.JoinRejection in
/-- Feed the exact fibre enumeration into the all-dimensions CWS theorem.
`hrows` is deliberately visible: the candidate-to-table refinement must
establish it before this theorem can certify a CUDA rejection. -/
theorem leaf_rejection_sound {d : ℕ} (C : CWS R (d + R))
    (L : Layout (d + R) R) (hw : Wf L) (hcov : Covered L)
    (T : Finset (Fin R → ℕ)) (hT : ∀ t ∈ T, Compatible L t)
    (hall : ∀ X, IsPoint L X → tupleOf L X ∈ T)
    (hrows : ∀ X, IsPoint L X ↔ ∀ a, ∑ i, C.W a i * (X i : ℝ) = ∑ i, C.W a i)
    (coefficients : List (Fin (d + R) → Int))
    (hreject : ∀ (p : Fin (index L T).card → CWSPoint C), Function.Surjective p →
      decideJoin d (index L T).card (index L T).card true
        ((coefficients.map (fun a i => integerEval a (p i))).map strictSides) ≠ .keep) :
    ¬ C.IsIP := by
  classical
  let e := pointEquiv L hw hcov T hT hall
  let f : {z // z ∈ index L T} → CWSPoint C :=
    fun z => ⟨(e z).val, (hrows _).mp (e z).property⟩
  have hf : Function.Surjective f := by
    intro X
    obtain ⟨z, hz⟩ := e.surjective ⟨X.val, (hrows _).mpr X.property⟩
    refine ⟨z, Subtype.ext ?_⟩
    change (e z).val = X.val
    exact congrArg Subtype.val hz
  let enum : Fin (index L T).card ≃ {z // z ∈ index L T} :=
    (Fintype.equivFinOfCardEq (by simp)).symm
  let p := f ∘ enum
  have hp : Function.Surjective p := hf.comp enum.surjective
  exact cws_join_rejection_sound_all_dimensions C p hp coefficients (index L T).card true
    (hreject p hp)

end Refpoly.JoinPoints
