import Refpoly.JoinModel

/-! A natural join of arbitrary row tables. Partial assignments are used so this
covers arbitrary support patterns, disconnected products, cycles, and hubs.
The extension theorem does not assume row independence or any fixed dimension. -/
namespace Refpoly.JoinRejection

abbrev PartialPoint (n : Nat) := Fin n → Option Nat

def Extends {n : Nat} (p : PartialPoint n) (x : Fin n → Nat) : Prop :=
  ∀ i v, p i = some v → x i = v

def Compatible {n : Nat} (p q : PartialPoint n) : Prop :=
  ∀ i a b, p i = some a → q i = some b → a = b

instance {n : Nat} (p q : PartialPoint n) : Decidable (Compatible p q) := by
  let test := ∀ i, match p i, q i with
    | some a, some b => a = b
    | _, _ => True
  letI (i : Fin n) : Decidable (match p i, q i with
    | some a, some b => a = b
    | _, _ => True) := by
    cases p i <;> cases q i <;> infer_instance
  letI : Decidable test := inferInstanceAs (Decidable (∀ i, match p i, q i with
    | some a, some b => a = b
    | _, _ => True))
  apply decidable_of_iff test
  constructor
  · intro h i a b ha hb
    have hi := h i
    simpa [ha, hb] using hi
  · intro h i
    cases hp : p i <;> cases hq : q i <;> dsimp
    exact h i _ _ hp hq

def overlay {n : Nat} (p q : PartialPoint n) : PartialPoint n :=
  fun i => (p i).or (q i)

theorem compatible_of_extends {n : Nat} {p q : PartialPoint n} {x : Fin n → Nat}
    (hp : Extends p x) (hq : Extends q x) : Compatible p q := by
  intro i a b ha hb
  exact (hp i a ha).symm.trans (hq i b hb)

theorem extends_overlay {n : Nat} {p q : PartialPoint n} {x : Fin n → Nat}
    (hc : Compatible p q) : Extends (overlay p q) x ↔ Extends p x ∧ Extends q x := by
  constructor
  · intro h
    constructor
    · intro i v hi
      exact h i v (by simp [overlay, hi])
    · intro i v hi
      cases hp : p i with
      | none => exact h i v (by simp [overlay, hp, hi])
      | some a =>
        have ha := h i a (by simp [overlay, hp])
        exact ha.trans (hc i a v hp hi)
  · rintro ⟨hp, hq⟩ i v hi
    cases hpi : p i with
    | none => exact hq i v (by simpa [overlay, hpi] using hi)
    | some a =>
      have hv : a = v := by simpa [overlay, hpi] using hi
      exact (hp i a hpi).trans hv

/-- Enumerate compatible records and merge their assignments. Finsets remove
multiplicity; keys label fibres, never individual repeated points. -/
def naturalJoin {n : Nat} (L R : Finset (PartialPoint n)) : Finset (PartialPoint n) :=
  ((L ×ˢ R).filter (fun pq => Compatible pq.1 pq.2)).image
    (fun pq => overlay pq.1 pq.2)

theorem naturalJoin_exact {n : Nat} (L R : Finset (PartialPoint n)) (x : Fin n → Nat) :
    (∃ p ∈ naturalJoin L R, Extends p x) ↔
      (∃ p ∈ L, Extends p x) ∧ (∃ q ∈ R, Extends q x) := by
  constructor
  · rintro ⟨p, hp, hx⟩
    obtain ⟨⟨a, b⟩, hab, rfl⟩ := Finset.mem_image.mp hp
    obtain ⟨hab, hc⟩ := Finset.mem_filter.mp hab
    obtain ⟨ha, hb⟩ := Finset.mem_product.mp hab
    obtain ⟨hax, hbx⟩ := (extends_overlay hc).mp hx
    exact ⟨⟨a, ha, hax⟩, ⟨b, hb, hbx⟩⟩
  · rintro ⟨⟨a, ha, hax⟩, ⟨b, hb, hbx⟩⟩
    have hc := compatible_of_extends hax hbx
    refine ⟨overlay a b, Finset.mem_image.mpr ⟨(a,b), ?_, rfl⟩,
      (extends_overlay hc).mpr ⟨hax, hbx⟩⟩
    exact Finset.mem_filter.mpr ⟨Finset.mem_product.mpr ⟨ha, hb⟩, hc⟩

def joinRows {n : Nat} : List (Finset (PartialPoint n)) → Finset (PartialPoint n)
  | [] => {fun _ => none}
  | t :: ts => naturalJoin t (joinRows ts)

/-- Exactness of the complete join, for every row order and support pattern. -/
theorem joinRows_exact {n : Nat} (tables : List (Finset (PartialPoint n)))
    (x : Fin n → Nat) :
    (∃ p ∈ joinRows tables, Extends p x) ↔
      ∀ t ∈ tables, ∃ p ∈ t, Extends p x := by
  induction tables with
  | nil => simp [joinRows, Extends]
  | cons t ts ih => simp only [joinRows, naturalJoin_exact, ih, List.mem_cons,
      forall_eq_or_imp]

/-- Depth-first traversal with a partial assignment accumulator. This is the
control-flow model of the general CUDA join, with row keys represented by
partial assignments rather than packed machine words. -/
def dfsRows {n : Nat} (acc : PartialPoint n) :
    List (Finset (PartialPoint n)) → Finset (PartialPoint n)
  | [] => {acc}
  | t :: ts => t.biUnion fun row =>
      if Compatible acc row then dfsRows (overlay acc row) ts else ∅

/-- The depth-first model visits exactly the simultaneous extensions, for any
row order. Failed matches produce no leaf; all compatible matches are visited. -/
theorem dfsRows_exact {n : Nat} (acc : PartialPoint n)
    (tables : List (Finset (PartialPoint n))) (x : Fin n → Nat) :
    (∃ p ∈ dfsRows acc tables, Extends p x) ↔
      Extends acc x ∧ ∀ t ∈ tables, ∃ p ∈ t, Extends p x := by
  induction tables generalizing acc with
  | nil => simp [dfsRows]
  | cons t ts ih =>
    constructor
    · rintro ⟨p, hp, hx⟩
      obtain ⟨row, hr, hp⟩ := Finset.mem_biUnion.mp hp
      by_cases hc : Compatible acc row
      · rw [if_pos hc] at hp
        obtain ⟨he, ht⟩ := (ih (overlay acc row)).mp ⟨p, hp, hx⟩
        obtain ⟨ha, hrx⟩ := (extends_overlay hc).mp he
        exact ⟨ha, by simpa using And.intro (Exists.intro row ⟨hr, hrx⟩) ht⟩
      · simp [hc] at hp
    · rintro ⟨ha, ht⟩
      have ht' : (∃ row ∈ t, Extends row x) ∧
          ∀ u ∈ ts, ∃ row ∈ u, Extends row x := by simpa using ht
      obtain ⟨⟨row, hr, hrx⟩, hts⟩ := ht'
      have hc := compatible_of_extends ha hrx
      obtain ⟨p, hp, hpx⟩ := (ih (overlay acc row)).mpr
        ⟨(extends_overlay hc).mpr ⟨ha, hrx⟩, hts⟩
      refine ⟨p, Finset.mem_biUnion.mpr ⟨row, hr, ?_⟩, hpx⟩
      simpa [hc] using hp

/-- Instantiate the join with exact row tables. This hypothesis concerns each
single row, never the global join or the desired rejection conclusion. -/
theorem joinRows_equations {n : Nat} (weights : List (Fin n → Nat))
    (tables : List (Finset (PartialPoint n)))
    (hrows : List.Forall₂ (fun w t => ∀ x : Fin n → Nat,
      (∃ p ∈ t, Extends p x) ↔ ∑ i, w i * x i = ∑ i, w i) weights tables)
    (x : Fin n → Nat) :
    (∃ p ∈ joinRows tables, Extends p x) ↔
      ∀ w ∈ weights, ∑ i, w i * x i = ∑ i, w i := by
  rw [joinRows_exact]
  induction hrows with
  | nil => simp
  | cons h hs ih =>
    simpa only [List.mem_cons, forall_eq_or_imp, h x] using and_congr Iff.rfl ih

end Refpoly.JoinRejection
