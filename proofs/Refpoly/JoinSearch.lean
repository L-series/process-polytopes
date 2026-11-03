import Refpoly.JoinSummary

/-! Unbounded interval invariants for the general join's binary searches.
The machine-to-Nat correspondence (including memory and arithmetic bounds) is
separate; these definitions model the actual midpoint and endpoint updates. -/
namespace Refpoly.JoinRejection

/-- A predicate whose true indices form a prefix of the entry's interval. -/
def SearchPrefix (b e : Nat) (p : Nat → Prop) : Prop :=
  ∀ i j, b ≤ i → i ≤ j → j < e → p j → p i

structure SearchInvariant (b e : Nat) (p : Nat → Prop) (lo hi : Nat) : Prop where
  left : b ≤ lo
  ordered : lo ≤ hi
  right : hi ≤ e
  beforeLo : ∀ i, b ≤ i → i < lo → p i
  afterHi : ∀ i, hi ≤ i → i < e → ¬p i

/-- Fuel bounds the strictly decreasing interval length. There is no fixed
maximum table size in the definition or its correctness theorem. -/
def searchLower (p : Nat → Prop) [DecidablePred p] : Nat → Nat → Nat → Nat
  | 0, lo, _ => lo
  | fuel + 1, lo, hi =>
    if lo < hi then
      let m := (lo + hi) / 2
      if p m then searchLower p fuel (m + 1) hi else searchLower p fuel lo m
    else lo

theorem search_left_invariant {b e lo hi : Nat} {p : Nat → Prop}
    (h : SearchInvariant b e p lo hi) (hp : SearchPrefix b e p)
    (hlt : lo < hi) (hm : p ((lo + hi) / 2)) :
    SearchInvariant b e p ((lo + hi) / 2 + 1) hi := by
  have mid : lo ≤ (lo + hi) / 2 ∧ (lo + hi) / 2 < hi := by omega
  refine ⟨by have := h.left; omega, by omega, h.right, ?_, h.afterHi⟩
  intro i hbi him
  exact hp i _ hbi (by omega) (by have := h.right; omega) hm

theorem search_right_invariant {b e lo hi : Nat} {p : Nat → Prop}
    (h : SearchInvariant b e p lo hi) (hp : SearchPrefix b e p)
    (hlt : lo < hi) (hm : ¬p ((lo + hi) / 2)) :
    SearchInvariant b e p lo ((lo + hi) / 2) := by
  have mid : lo ≤ (lo + hi) / 2 ∧ (lo + hi) / 2 < hi := by omega
  refine ⟨h.left, by omega, by have := h.right; omega, h.beforeLo, ?_⟩
  intro i hmi hie hpi
  exact hm (hp _ i (by have := h.left; omega) hmi hie hpi)

theorem search_meet_exact {b e lo : Nat} {p : Nat → Prop}
    (h : SearchInvariant b e p lo lo) :
    b ≤ lo ∧ lo ≤ e ∧ ∀ i, b ≤ i → i < e → (p i ↔ i < lo) := by
  refine ⟨h.left, h.right, ?_⟩
  intro i hbi hie
  constructor
  · intro hpi
    by_contra hn
    exact h.afterHi i (by omega) hie hpi
  · exact h.beforeLo i hbi

/-- Exact lower bound, with the same two branch updates as the CUDA loops.
The fuel hypothesis is just a termination bound, not a correctness oracle. -/
theorem searchLower_exact {b e : Nat} {p : Nat → Prop} [DecidablePred p]
    (hp : SearchPrefix b e p) (fuel lo hi : Nat)
    (h : SearchInvariant b e p lo hi) (hf : hi - lo ≤ fuel) :
    let result := searchLower p fuel lo hi
    b ≤ result ∧ result ≤ e ∧ ∀ i, b ≤ i → i < e → (p i ↔ i < result) := by
  induction fuel generalizing lo hi with
  | zero =>
    have heq : hi = lo := by have := h.ordered; omega
    subst hi
    exact search_meet_exact h
  | succ fuel ih =>
    by_cases hlt : lo < hi
    · have mid : lo ≤ (lo + hi) / 2 ∧ (lo + hi) / 2 < hi := by omega
      by_cases hm : p ((lo + hi) / 2)
      · have next := search_left_invariant h hp hlt hm
        have len : hi - ((lo + hi) / 2 + 1) ≤ fuel := by omega
        simpa only [searchLower, if_pos hlt, if_pos hm] using ih _ _ next len
      · have next := search_right_invariant h hp hlt hm
        have len : (lo + hi) / 2 - lo ≤ fuel := by omega
        simpa only [searchLower, if_pos hlt, if_neg hm] using ih _ _ next len
    · have heq : hi = lo := by have := h.ordered; omega
      subst hi
      simpa only [searchLower, Nat.lt_irrefl, ↓reduceIte] using search_meet_exact h

/-- The two actual comparisons used by range initialization: full packed key
against `value * radix`, then leading-coordinate comparison against `value`.
Starting the second search at the first result is exact, including the largest
representable leading coordinate; no `(value + 1) * radix` is formed. -/
theorem packed_range_exact (keys : Nat → Nat) (b e radix value : Nat)
    (hbe : b ≤ e) (hradix : 0 < radix)
    (sorted : ∀ i j, b ≤ i → i ≤ j → j < e → keys i ≤ keys j) :
    let lower := searchLower (fun i => keys i < value * radix) (e - b) b e
    let upper := searchLower (fun i => keys i / radix ≤ value) (e - lower) lower e
    b ≤ lower ∧ lower ≤ upper ∧ upper ≤ e ∧
      ∀ i, b ≤ i → i < e →
        (lower ≤ i ∧ i < upper ↔ keys i / radix = value) := by
  let p : Nat → Prop := fun i => keys i < value * radix
  let q : Nat → Prop := fun i => keys i / radix ≤ value
  have hp : SearchPrefix b e p := by
    intro i j hbi hij hje hpj
    exact lt_of_le_of_lt (sorted i j hbi hij hje) hpj
  have hq : SearchPrefix b e q := by
    intro i j hbi hij hje hqj
    exact le_trans (Nat.div_le_div_right (sorted i j hbi hij hje)) hqj
  have initial : SearchInvariant b e p b e :=
    ⟨le_refl _, hbe, le_refl _, by intros; omega, by intros; omega⟩
  let lower := searchLower p (e - b) b e
  obtain ⟨hbl, hle, hl⟩ := searchLower_exact hp (e - b) b e initial (le_refl _)
  change b ≤ lower at hbl
  change lower ≤ e at hle
  have next : SearchInvariant b e q lower e := by
    refine ⟨hbl, hle, le_refl _, ?_, by intros; omega⟩
    intro i hbi hil
    have hie : i < e := by omega
    have hip : keys i < value * radix := (hl i hbi hie).mpr hil
    exact Nat.le_of_lt ((Nat.div_lt_iff_lt_mul hradix).mpr hip)
  let upper := searchLower q (e - lower) lower e
  obtain ⟨hbu, hue, hu⟩ := searchLower_exact hq (e - lower) lower e next (le_refl _)
  change b ≤ upper at hbu
  change upper ≤ e at hue
  have hlu : lower ≤ upper := by
    by_contra hn
    have hup : p upper := (hl upper hbu (by omega)).mpr (by omega)
    have huq : q upper := Nat.le_of_lt ((Nat.div_lt_iff_lt_mul hradix).mpr hup)
    have := (hu upper hbu (by omega)).mp huq
    omega
  refine ⟨hbl, hlu, hue, ?_⟩
  intro i hbi hie
  change lower ≤ i ∧ i < upper ↔ keys i / radix = value
  have lower_spec : keys i / radix < value ↔ i < lower := by
    rw [Nat.div_lt_iff_lt_mul hradix]
    exact hl i hbi hie
  have upper_spec : keys i / radix ≤ value ↔ i < upper := hu i hbi hie
  omega

/-- Binary lookup cannot miss a matching canonical key, for any table length. -/
theorem search_lookup_exact (keys : Nat → Nat) (b e target : Nat)
    (hbe : b ≤ e)
    (sorted : ∀ i j, b ≤ i → i ≤ j → j < e → keys i ≤ keys j) :
    let found := searchLower (fun i => keys i < target) (e - b) b e
    (found < e ∧ keys found = target) ↔
      ∃ i, b ≤ i ∧ i < e ∧ keys i = target := by
  let p : Nat → Prop := fun i => keys i < target
  have hp : SearchPrefix b e p := by
    intro i j hbi hij hje hpj
    exact lt_of_le_of_lt (sorted i j hbi hij hje) hpj
  have initial : SearchInvariant b e p b e :=
    ⟨le_refl _, hbe, le_refl _, by intros; omega, by intros; omega⟩
  let found := searchLower p (e - b) b e
  obtain ⟨hbf, hfe, hf⟩ := searchLower_exact hp (e - b) b e initial (le_refl _)
  change b ≤ found at hbf
  change found ≤ e at hfe
  change (found < e ∧ keys found = target) ↔ _
  constructor
  · rintro ⟨hft, hkey⟩
    exact ⟨found, hbf, hft, hkey⟩
  · rintro ⟨i, hbi, hie, hkey⟩
    have hfi : found ≤ i := by
      by_contra hn
      have hpi : p i := (hf i hbi hie).mpr (by omega)
      change keys i < target at hpi
      omega
    have hft : found < e := by omega
    have hge : target ≤ keys found := by
      have hnot : ¬p found := by
        intro hpf
        have := (hf found hbf hft).mp hpf
        omega
      change ¬keys found < target at hnot
      omega
    have hle : keys found ≤ target := (sorted found i hbf hfi hie).trans_eq hkey
    exact ⟨hft, by omega⟩

/-- Strictly increasing canonical keys give one lookup result, not several
representations of the same shared assignment. -/
theorem search_lookup_unique (keys : Nat → Nat) (b e target : Nat)
    (hbe : b ≤ e)
    (strict : ∀ i j, b ≤ i → i < j → j < e → keys i < keys j)
    (i : Nat) (hbi : b ≤ i) (hie : i < e) (hkey : keys i = target) :
    searchLower (fun j => keys j < target) (e - b) b e = i := by
  have sorted : ∀ i j, b ≤ i → i ≤ j → j < e → keys i ≤ keys j := by
    intro i j hbi hij hje
    rcases Nat.eq_or_lt_of_le hij with rfl | hlt
    · exact le_refl _
    · exact Nat.le_of_lt (strict i j hbi hlt hje)
  let found := searchLower (fun j => keys j < target) (e - b) b e
  obtain ⟨hfe, hfound⟩ := (search_lookup_exact keys b e target hbe sorted).mpr
    ⟨i, hbi, hie, hkey⟩
  have hbf : b ≤ found := by
    have initial : SearchInvariant b e (fun j => keys j < target) b e :=
      ⟨le_refl _, hbe, le_refl _, by intros; omega, by intros; omega⟩
    have hp : SearchPrefix b e (fun j => keys j < target) := by
      intro a j hba haj hje hpj
      exact lt_of_le_of_lt (sorted a j hba haj hje) hpj
    exact (searchLower_exact hp (e - b) b e initial (le_refl _)).1
  change found = i
  change found < e at hfe
  change keys found = target at hfound
  rcases lt_trichotomy found i with hlt | heq | hgt
  · have := strict found i hbf hlt hie
    omega
  · exact heq
  · have := strict i found hbi hgt hfe
    omega

end Refpoly.JoinRejection
