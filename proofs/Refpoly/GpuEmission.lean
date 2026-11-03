import Mathlib

/-!
# Order equivariance of candidate emission

The GPU emission recursion (`device_emit_small_prefixes` and the functions it
calls, src/gpu/generate.cuh) and PALP's `EnumeratePermutations` consult the
weights only through two primitives: the prefix permutation step and the
shared-prefix test; otherwise they copy weights (scripts/audit_generation_copies.py).
CBMC proves both primitives order-invariant (verification/gpu/order_invariance.cpp).

This module models such a recursion over slots and proves that it commutes
with every per-slot strictly increasing relabelling of positive weights. Hence
two such recursions that agree on every order type (values `1..m` in each slot,
checked exhaustively by verification/gpu/emission_host.cpp) agree on all
positive weights.
-/
namespace Refpoly.GpuEmission

variable {S : ℕ}

/-- Weights per slot. -/
abbrev Weights (S : ℕ) := Fin S → List ℤ

def mapW (φ : Fin S → ℤ → ℤ) (w : Weights S) : Weights S := fun s => (w s).map (φ s)

/-- A relabelling: strictly increasing and positive on positives in each slot. -/
def Relabel (φ : Fin S → ℤ → ℤ) : Prop := ∀ s, StrictMono (φ s) ∧ ∀ x, 0 < x → 0 < φ s x

def Positive (w : Weights S) : Prop := ∀ s, ∀ x ∈ w s, 0 < x

/-- The arrangements visited from `l` by repeated steps (at most `fuel`). -/
def orbit (next : List ℤ → Option (List ℤ)) : ℕ → List ℤ → List (List ℤ)
  | 0, _ => []
  | n + 1, l => l :: match next l with
    | none => []
    | some l' => orbit next n l'

/-- Emission over the slots `ss` in order: every visited arrangement of the
slot that passes the test continues with the next slot; the last slot emits. -/
def emit (next : List ℤ → Option (List ℤ)) (canon : Weights S → Fin S → Bool) (fuel : ℕ) :
    List (Fin S) → Weights S → List (Weights S)
  | [], w => [w]
  | s :: ss, w => (orbit next fuel (w s)).flatMap fun p =>
      if canon (Function.update w s p) s then emit next canon fuel ss (Function.update w s p) else []

/-- The step permutes values (each output value is an input value) and commutes
with strictly increasing maps: what the PERMUTATION CBMC queries establish. -/
structure StepSpec (next : List ℤ → Option (List ℤ)) : Prop where
  values : ∀ l l', next l = some l' → ∀ x ∈ l', x ∈ l
  natural : ∀ (f : ℤ → ℤ), StrictMono f → ∀ l, next (l.map f) = (next l).map (List.map f)

/-- The test depends only on the per-slot order type of positive weights: what
the PREFIX CBMC queries establish. -/
def CanonSpec (canon : Weights S → Fin S → Bool) : Prop :=
  ∀ φ, Relabel φ → ∀ w, Positive w → ∀ s, canon (mapW φ w) s = canon w s

theorem orbit_values {next} (h : StepSpec next) :
    ∀ n l, ∀ p ∈ orbit next n l, ∀ x ∈ p, x ∈ l := by
  intro n
  induction n with
  | zero => intro l p hp; simp [orbit] at hp
  | succ n ih =>
    intro l p hp x hx
    simp only [orbit, List.mem_cons] at hp
    rcases hp with rfl | hp
    · exact hx
    · cases hl : next l with
      | none => simp [hl] at hp
      | some l' =>
        simp only [hl] at hp
        exact h.values l l' hl x (ih l' p hp x hx)

theorem orbit_map {next} (h : StepSpec next) (f : ℤ → ℤ) (hf : StrictMono f) :
    ∀ n l, orbit next n (l.map f) = (orbit next n l).map (List.map f) := by
  intro n
  induction n with
  | zero => intro l; simp [orbit]
  | succ n ih =>
    intro l
    simp only [orbit, h.natural f hf l]
    cases next l with
    | none => simp
    | some l' => simp [ih l']

theorem update_map (φ : Fin S → ℤ → ℤ) (w : Weights S) (s : Fin S) (p : List ℤ) :
    Function.update (mapW φ w) s (p.map (φ s)) = mapW φ (Function.update w s p) := by
  funext t
  by_cases ht : t = s
  · subst ht; simp [mapW]
  · simp [mapW, Function.update_of_ne ht]

theorem update_positive {w : Weights S} (hw : Positive w) (s : Fin S) {p : List ℤ}
    (hp : ∀ x ∈ p, x ∈ w s) : Positive (Function.update w s p) := by
  intro t x hx
  by_cases ht : t = s
  · subst ht; simp at hx; exact hw t x (hp x hx)
  · rw [Function.update_of_ne ht] at hx; exact hw t x hx

/-- Emission commutes with every relabelling of positive weights. -/
theorem emit_equivariant {next canon} (hn : StepSpec next) (hc : CanonSpec (S := S) canon)
    (fuel : ℕ) (φ : Fin S → ℤ → ℤ) (hφ : Relabel φ) :
    ∀ ss w, Positive w → emit next canon fuel ss (mapW φ w) = (emit next canon fuel ss w).map (mapW φ) := by
  intro ss
  induction ss with
  | nil => intro w _; simp [emit]
  | cons s ss ih =>
    intro w hw
    simp only [emit]
    have ho : orbit next fuel (mapW φ w s) = (orbit next fuel (w s)).map (List.map (φ s)) :=
      orbit_map hn (φ s) (hφ s).1 fuel (w s)
    rw [ho, List.flatMap_map, List.map_flatMap]
    apply List.flatMap_congr
    intro p hp
    have hpos := update_positive hw s (orbit_values hn fuel (w s) p hp)
    rw [update_map, hc φ hφ _ hpos s]
    split_ifs
    · exact ih _ hpos
    · rfl

/-- Two order-invariant emissions that agree on a positive input agree on all
of its relabellings. -/
theorem agree_on_relabelling {nextG canonG nextC canonC} (hnG : StepSpec nextG)
    (hcG : CanonSpec (S := S) canonG) (hnC : StepSpec nextC) (hcC : CanonSpec (S := S) canonC)
    (fuel : ℕ) (ss : List (Fin S)) (t : Weights S) (ht : Positive t)
    (hagree : emit nextG canonG fuel ss t = emit nextC canonC fuel ss t)
    (φ : Fin S → ℤ → ℤ) (hφ : Relabel φ) :
    emit nextG canonG fuel ss (mapW φ t) = emit nextC canonC fuel ss (mapW φ t) := by
  rw [emit_equivariant hnG hcG fuel φ hφ ss t ht, emit_equivariant hnC hcC fuel φ hφ ss t ht, hagree]

/-- Rank among the distinct values of `l`: `1 + #{v ∈ l | v < x}`. -/
def rank (l : List ℤ) (x : ℤ) : ℤ := ((l.toFinset.filter (· < x)).card : ℤ) + 1

theorem rank_gap (l : List ℤ) {a b : ℤ} (hab : a ≤ b) : rank l b - rank l a ≤ b - a := by
  classical
  have hsub : (l.toFinset.filter (· < b)) ⊆ (l.toFinset.filter (· < a)) ∪ Finset.Ico a b := by
    intro v hv
    simp only [Finset.mem_filter, Finset.mem_union, Finset.mem_Ico] at hv ⊢
    by_cases h : v < a
    · exact Or.inl ⟨hv.1, h⟩
    · exact Or.inr ⟨by omega, hv.2⟩
  have hc := (Finset.card_le_card hsub).trans (Finset.card_union_le _ _)
  simp only [Int.card_Ico] at hc
  unfold rank
  omega

theorem rank_le_self (l : List ℤ) (hl : ∀ x ∈ l, 0 < x) (x : ℤ) (hx : 0 < x) : rank l x ≤ x := by
  classical
  have hsub : (l.toFinset.filter (· < x)) ⊆ Finset.Ico 1 x := by
    intro v hv
    simp only [Finset.mem_filter, List.mem_toFinset, Finset.mem_Ico] at hv ⊢
    exact ⟨hl v hv.1, hv.2⟩
  have hc := Finset.card_le_card hsub
  simp only [Int.card_Ico] at hc
  unfold rank
  omega

theorem rank_strict (l : List ℤ) {a b : ℤ} (ha : a ∈ l) (hab : a < b) : rank l a < rank l b := by
  classical
  have hsub : (l.toFinset.filter (· < a)) ⊂ (l.toFinset.filter (· < b)) := by
    refine Finset.ssubset_iff_of_subset (fun v hv => ?_) |>.mpr ⟨a, ?_, ?_⟩
    · simp only [Finset.mem_filter] at hv ⊢; exact ⟨hv.1, by omega⟩
    · simp [ha, hab]
    · simp
  have := Finset.card_lt_card hsub
  unfold rank
  omega

/-- The relabelling that takes ranks back to values. -/
def lift (l : List ℤ) (x : ℤ) : ℤ :=
  x + ((l.toFinset.filter (fun d => rank l d ≤ x)).sup (fun d => (d - rank l d).toNat) : ℕ)

theorem lift_strictMono (l : List ℤ) : StrictMono (lift l) := by
  classical
  intro a b hab
  unfold lift
  have : (l.toFinset.filter (fun d => rank l d ≤ a)).sup (fun d => (d - rank l d).toNat) ≤
      (l.toFinset.filter (fun d => rank l d ≤ b)).sup (fun d => (d - rank l d).toNat) := by
    apply Finset.sup_mono
    intro d hd
    simp only [Finset.mem_filter] at hd ⊢
    exact ⟨hd.1, by omega⟩
  omega

theorem lift_rank (l : List ℤ) (hl : ∀ x ∈ l, 0 < x) (d : ℤ) (hd : d ∈ l) : lift l (rank l d) = d := by
  classical
  unfold lift
  have hmem : d ∈ l.toFinset.filter (fun d' => rank l d' ≤ rank l d) := by simp [hd]
  have hge := Finset.le_sup (f := fun d => (d - rank l d).toNat) hmem
  have hle : (l.toFinset.filter (fun d' => rank l d' ≤ rank l d)).sup (fun d => (d - rank l d).toNat) ≤
      (d - rank l d).toNat := by
    apply Finset.sup_le
    intro d' hd'
    simp only [Finset.mem_filter, List.mem_toFinset] at hd'
    have hd'd : d' ≤ d := by
      by_contra h
      have := rank_strict l hd (show d < d' by omega)
      omega
    have := rank_gap l hd'd
    omega
  have hpos := rank_le_self l hl d (hl d hd)
  simp only at hge
  omega

/-- Every positive weight list is a relabelling of its order type, whose
values are ranks in `1..length`. -/
theorem exists_order_type (l : List ℤ) (hl : ∀ x ∈ l, 0 < x) :
    ∃ t : List ℤ, ∃ f : ℤ → ℤ, StrictMono f ∧ (∀ x, 0 < x → 0 < f x) ∧
      (∀ x ∈ t, 1 ≤ x ∧ x ≤ l.length) ∧ l = t.map f := by
  classical
  refine ⟨l.map (rank l), lift l, lift_strictMono l, fun x hx => ?_, fun x hx => ?_, ?_⟩
  · unfold lift; omega
  · obtain ⟨d, hd, rfl⟩ := List.mem_map.mp hx
    have hc : (l.toFinset.filter (· < d)).card < l.length := by
      calc (l.toFinset.filter (· < d)).card < l.toFinset.card := by
            apply Finset.card_lt_card
            refine Finset.ssubset_iff_of_subset (Finset.filter_subset _ _) |>.mpr ⟨d, by simp [hd], by simp⟩
        _ ≤ l.length := List.toFinset_card_le l
    unfold rank
    omega
  · rw [List.map_map]
    conv_lhs => rw [← List.map_id l]
    exact List.map_congr_left (fun d hd => (lift_rank l hl d hd).symm)

/-- Order types as enumerated by verification/gpu/emission_host.cpp: tuples
over `1..length` that are their own rank tuple. -/
def RankFixed (t : List ℤ) : Prop := (∀ x ∈ t, 1 ≤ x ∧ x ≤ t.length) ∧ t.map (rank t) = t

theorem rank_mono (l : List ℤ) {a b : ℤ} (hab : a ≤ b) : rank l a ≤ rank l b := by
  classical
  unfold rank
  have : (l.toFinset.filter (· < a)).card ≤ (l.toFinset.filter (· < b)).card :=
    Finset.card_le_card (fun v hv => by simp only [Finset.mem_filter] at hv ⊢; exact ⟨hv.1, by omega⟩)
  omega

/-- Ranking the rank tuple gives the same ranks. -/
theorem rank_of_rank (l : List ℤ) (d : ℤ) : rank (l.map (rank l)) (rank l d) = rank l d := by
  classical
  have himage : (l.map (rank l)).toFinset = l.toFinset.image (rank l) := by
    ext y; simp
  have hfilter : (l.toFinset.filter (fun d' => rank l d' < rank l d)) = l.toFinset.filter (· < d) := by
    ext d'
    simp only [Finset.mem_filter, List.mem_toFinset]
    constructor
    · rintro ⟨h1, h2⟩
      refine ⟨h1, ?_⟩
      by_contra h
      have := rank_mono l (show d ≤ d' by omega)
      omega
    · rintro ⟨h1, h2⟩
      exact ⟨h1, rank_strict l h1 h2⟩
  have hinj : Set.InjOn (rank l) ↑(l.toFinset.filter (· < d)) := by
    intro a ha b hb hab
    simp only [Finset.coe_filter, List.mem_toFinset, Set.mem_setOf_eq] at ha hb
    by_contra hne
    rcases lt_or_gt_of_ne hne with h | h
    · have := rank_strict l ha.1 h; omega
    · have := rank_strict l hb.1 h; omega
  have hcard : (((l.map (rank l)).toFinset.filter (· < rank l d)).card : ℤ) =
      ((l.toFinset.filter (· < d)).card : ℤ) := by
    rw [himage, Finset.filter_image, hfilter, Finset.card_image_of_injOn hinj]
  change (((l.map (rank l)).toFinset.filter (· < rank l d)).card : ℤ) + 1 = rank l d
  rw [hcard]
  rfl

/-- The rank tuple of any list is rank-fixed. -/
theorem rank_tuple_fixed (l : List ℤ) (hl : ∀ x ∈ l, 0 < x) : RankFixed (l.map (rank l)) := by
  classical
  obtain ⟨_, _, _, _, hbound, _⟩ := exists_order_type l hl
  refine ⟨fun x hx => ?_, ?_⟩
  · obtain ⟨d, hd, rfl⟩ := List.mem_map.mp hx
    have hc : (l.toFinset.filter (· < d)).card < l.length := by
      calc (l.toFinset.filter (· < d)).card < l.toFinset.card := by
            apply Finset.card_lt_card
            refine Finset.ssubset_iff_of_subset (Finset.filter_subset _ _) |>.mpr ⟨d, by simp [hd], by simp⟩
        _ ≤ l.length := List.toFinset_card_le l
    simp only [List.length_map]
    unfold rank
    omega
  · rw [List.map_map]
    conv_rhs => rw [← List.map_id (l.map (rank l))]
    rw [List.map_map]
    apply List.map_congr_left
    intro d hd
    simp only [Function.comp, id]
    exact rank_of_rank l d

/-- Emissions satisfying the primitive specifications that agree on every
per-slot rank-fixed tuple agree on all positive weights. -/
theorem agree_on_positive {nextG canonG nextC canonC} (hnG : StepSpec nextG)
    (hcG : CanonSpec (S := S) canonG) (hnC : StepSpec nextC) (hcC : CanonSpec (S := S) canonC)
    (fuel : ℕ) (ss : List (Fin S))
    (hcheck : ∀ t : Weights S, (∀ s, RankFixed (t s)) →
      emit nextG canonG fuel ss t = emit nextC canonC fuel ss t)
    (w : Weights S) (hw : Positive w) :
    emit nextG canonG fuel ss w = emit nextC canonC fuel ss w := by
  classical
  let t : Weights S := fun s => (w s).map (rank (w s))
  have ht : Positive t := by
    intro s x hx
    obtain ⟨d, _, rfl⟩ := List.mem_map.mp hx
    unfold rank; omega
  have hw' : w = mapW (fun s => lift (w s)) t := by
    funext s
    simp only [mapW, t, List.map_map]
    conv_lhs => rw [← List.map_id (w s)]
    exact List.map_congr_left (fun d hd => (lift_rank (w s) (hw s) d hd).symm)
  have hφ : Relabel (fun s => lift (w s)) := fun s =>
    ⟨lift_strictMono _, fun x hx => by show 0 < lift (w s) x; unfold lift; omega⟩
  rw [hw']
  exact agree_on_relabelling hnG hcG hnC hcC fuel ss t ht
    (hcheck t (fun s => rank_tuple_fixed (w s) (hw s))) _ hφ

end Refpoly.GpuEmission
