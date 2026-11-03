import Refpoly.CudaIPSource

/-! Exact equation-stack and minimum-search models. Arithmetic correspondence
requires the signed-definedness conditions in `CudaIPSource`. -/
namespace Refpoly.CudaIP

structure ScanResult (d : Nat) where
  pending : List (Equation d)
  facets : List (Equation d)
  rejected : Option (Equation d)
  exhausted : Bool
  truncated : Bool
  deriving DecidableEq, Repr

def hasNegative {d : Nat} (xs : List (Fin d → Int)) (e : Equation d) : Bool :=
  xs.any (fun x => decide (eval e x < 0))

def goodFacet {d : Nat} (xs : List (Fin d → Int)) (e : Equation d) : Prop :=
  0 < e.constant ∧ ∀ x ∈ xs, 0 ≤ eval e x

def hasNormal {d : Nat} (e : Equation d) : Bool := decide (∃ i, e.normal i ≠ 0)

theorem hasNormal_iff {d : Nat} (e : Equation d) : hasNormal e = true ↔ e.normal ≠ 0 := by
  simp [hasNormal, funext_iff]

def scanEquations {d : Nat} (xs : List (Fin d → Int)) (capacity : Nat) :
    List (Equation d) → List (Equation d) → Bool → ScanResult d
  | [], fs, flag => ⟨[], fs, none, true, flag⟩
  | e :: es, fs, flag =>
    if hasNegative xs e then ⟨e :: es, fs, none, false, flag⟩
    else if e.constant < 1 then
      if hasNormal e then ⟨e :: es, fs, some e, false, flag⟩
      else ⟨e :: es, fs, none, false, true⟩
    else if fs.length < capacity then scanEquations xs capacity es (fs ++ [e]) flag
    else scanEquations xs capacity es fs true

theorem hasNegative_false {d : Nat} (xs : List (Fin d → Int)) (e : Equation d) :
    hasNegative xs e = false ↔ ∀ x ∈ xs, 0 ≤ eval e x := by
  simp [hasNegative, List.any_eq_false]

/-- Rejection produces a genuine supporting inequality. -/
theorem scan_rejection_witness {d : Nat} (xs : List (Fin d → Int)) (cap : Nat)
    (pending fs : List (Equation d)) (flag : Bool) (e : Equation d)
    (h : (scanEquations xs cap pending fs flag).rejected = some e) :
    e ∈ pending ∧ e.normal ≠ 0 ∧ e.constant < 1 ∧ ∀ x ∈ xs, 0 ≤ eval e x := by
  induction pending generalizing fs flag with
  | nil => simp [scanEquations] at h
  | cons a rest ih =>
    simp only [scanEquations] at h
    split at h
    · simp at h
    · rename_i hn
      split at h
      · split at h
        · rename_i hnormal
          have he : a = e := Option.some.inj h
          subst a
          exact ⟨by simp, (hasNormal_iff e).mp hnormal, by assumption,
            (hasNegative_false xs e).mp (by simpa using hn)⟩
        · simp at h
      · split at h
        · obtain ⟨hm, hnorm, hc, hs⟩ := ih _ _ h
          exact ⟨by simp [hm], hnorm, hc, hs⟩
        · obtain ⟨hm, hnorm, hc, hs⟩ := ih _ _ h
          exact ⟨by simp [hm], hnorm, hc, hs⟩

theorem scan_rejection_sound {d : Nat} (xs : List (Fin d → Int)) (cap : Nat)
    (pending fs : List (Equation d)) (flag : Bool)
    (e : Equation d)
    (h : (scanEquations xs cap pending fs flag).rejected = some e) :
    ¬ IsIP (KS.PolyCode.hull ⟨xs⟩) := by
  obtain ⟨_, hn, hc, hs⟩ := scan_rejection_witness xs cap pending fs flag e h
  exact integer_supporting_rejection xs e hn hc hs

/-- Exhaustion cannot repair a previously truncated list. -/
theorem scan_truncation_sticky {d : Nat} (xs : List (Fin d → Int)) (cap : Nat)
    (pending fs : List (Equation d)) :
    (scanEquations xs cap pending fs true).truncated = true := by
  induction pending generalizing fs with
  | nil => rfl
  | cons e es ih =>
    simp only [scanEquations]
    split <;> try rfl
    split
    · split <;> rfl
    · split <;> exact ih _

theorem scan_facets_good {d : Nat} (xs : List (Fin d → Int)) (cap : Nat)
    (pending fs : List (Equation d)) (flag : Bool)
    (hf : ∀ e ∈ fs, goodFacet xs e) :
    ∀ e ∈ (scanEquations xs cap pending fs flag).facets, goodFacet xs e := by
  induction pending generalizing fs flag with
  | nil => exact hf
  | cons a rest ih =>
    simp only [scanEquations]
    split
    · exact hf
    · rename_i hn
      split
      · split <;> exact hf
      · rename_i hc
        split
        · apply ih
          intro e he
          rcases List.mem_append.mp he with he | he
          · exact hf e he
          · have heq : e = a := by simpa using he
            subst e
            exact ⟨by omega, (hasNegative_false xs a).mp (by simpa using hn)⟩
        · exact ih fs true hf

/-- Completed, untruncated scans lose no defining inequality. -/
theorem scan_preserves_equations {d : Nat} (xs : List (Fin d → Int)) (cap : Nat)
    (pending fs : List (Equation d)) (flag : Bool)
    (ht : (scanEquations xs cap pending fs flag).truncated = false)
    (hr : (scanEquations xs cap pending fs flag).rejected = none) (q : Equation d) :
    (q ∈ pending ∨ q ∈ fs) ↔
      (q ∈ (scanEquations xs cap pending fs flag).pending ∨
        q ∈ (scanEquations xs cap pending fs flag).facets) := by
  induction pending generalizing fs flag with
  | nil => simp [scanEquations]
  | cons a rest ih =>
    simp only [scanEquations] at ht hr ⊢
    by_cases hn : hasNegative xs a = true
    · simp [hn]
    · simp only [if_neg hn] at ht hr ⊢
      by_cases hc : a.constant < 1
      · by_cases hnormal : hasNormal a = true
        · simp [hc, hnormal] at hr
        · simp [hc, hnormal] at ht
      · simp only [if_neg hc] at ht hr ⊢
        by_cases hcap : fs.length < cap
        · simp only [if_pos hcap] at ht hr ⊢
          have hh := ih (fs ++ [a]) flag ht hr
          simpa [or_assoc, or_left_comm, or_comm] using hh
        · simp only [if_neg hcap] at ht hr ⊢
          rw [scan_truncation_sticky] at ht
          contradiction

theorem scan_exhausted_empty {d : Nat} (xs : List (Fin d → Int)) (cap : Nat)
    (pending fs : List (Equation d)) (flag : Bool)
    (h : (scanEquations xs cap pending fs flag).exhausted = true) :
    (scanEquations xs cap pending fs flag).pending = [] := by
  induction pending generalizing fs flag with
  | nil => rfl
  | cons a rest ih =>
    by_cases hn : hasNegative xs a = true
    · simp [scanEquations, hn] at h
    · by_cases hc : a.constant < 1
      · by_cases hnormal : hasNormal a = true <;>
          simp [scanEquations, hn, hc, hnormal] at h
      · by_cases hcap : fs.length < cap
        · simpa [scanEquations, hn, hc, hcap] using
            ih (fs ++ [a]) flag (by simpa [scanEquations, hn, hc, hcap] using h)
        · simpa [scanEquations, hn, hc, hcap] using
            ih fs true (by simpa [scanEquations, hn, hc, hcap] using h)

/-- The terminal scan's acceptance obligation, stated against the exact
halfspace representation of the selected-vertex hull. The geometric update
must preserve this representation; validity of individual facets is not enough. -/
theorem scan_terminal_acceptance {d : Nat} (xs : List (Fin d → Int)) (cap : Nat)
    (pending fs : List (Equation d)) (flag : Bool)
    (hf : ∀ e ∈ fs, goodFacet xs e)
    (ht : (scanEquations xs cap pending fs flag).truncated = false)
    (hr : (scanEquations xs cap pending fs flag).rejected = none)
    (he : (scanEquations xs cap pending fs flag).exhausted = true)
    (hcover : {x | ∀ e ∈ pending ++ fs, 0 ≤ realEval e x} ⊆
      convexHull ℝ (KS.pointSet xs)) : IsIP (KS.PolyCode.hull ⟨xs⟩) := by
  let out := scanEquations xs cap pending fs flag
  have hg : ∀ e ∈ out.facets, goodFacet xs e := scan_facets_good xs cap pending fs flag hf
  have hp : out.pending = [] := scan_exhausted_empty xs cap pending fs flag he
  apply halfspace_acceptance (KS.pointSet xs) (fun i : Fin out.facets.length => out.facets.get i)
  · intro i
    exact (hg _ (List.get_mem out.facets i)).1
  · intro x hx
    apply hcover
    intro e hem
    have hi : e ∈ pending ∨ e ∈ fs := List.mem_append.mp hem
    have ho := (scan_preserves_equations xs cap pending fs flag ht hr e).mp hi
    change e ∈ out.pending ∨ e ∈ out.facets at ho
    rcases ho with ho | ho
    · simp [hp] at ho
    · obtain ⟨i, hi⟩ := List.mem_iff_get.mp ho
      rw [← hi]
      exact hx i

/-- Exact minimum selection; tie-breaking cannot change the minimum value. -/
def pickMin {d : Nat} (e : Equation d) (prefer : (Fin d → Int) → (Fin d → Int) → Bool)
    (a b : Fin d → Int) : Fin d → Int :=
  if eval e a < eval e b then a
  else if eval e b < eval e a then b
  else if prefer a b then a else b

theorem pickMin_value {d : Nat} (e : Equation d) (prefer) (a b : Fin d → Int) :
    eval e (pickMin e prefer a b) = min (eval e a) (eval e b) := by
  unfold pickMin
  split_ifs <;> omega

theorem pickMin_member {d : Nat} (e : Equation d) (prefer) (a b : Fin d → Int) :
    pickMin e prefer a b = a ∨ pickMin e prefer a b = b := by
  unfold pickMin
  split_ifs <;> simp

theorem minimum_search_correct {d : Nat} (e : Equation d) (prefer)
    (xs : List (Fin d → Int)) (seed : Fin d → Int) :
    let selected := xs.foldl (pickMin e prefer) seed
    (selected = seed ∨ selected ∈ xs) ∧ eval e selected ≤ eval e seed ∧
      ∀ x ∈ xs, eval e selected ≤ eval e x := by
  induction xs generalizing seed with
  | nil => simp
  | cons a rest ih =>
    simp only [List.foldl_cons]
    obtain ⟨hm, hv, hrest⟩ := ih (pickMin e prefer seed a)
    have hval := pickMin_value e prefer seed a
    have hp := pickMin_member e prefer seed a
    refine ⟨?_, ?_, ?_⟩
    · rcases hm with hm | hm
      · rcases hp with hp | hp
        · exact Or.inl (hm.trans hp)
        · exact Or.inr (List.mem_cons.mpr (Or.inl (hm.trans hp)))
      · exact Or.inr (List.mem_cons_of_mem a hm)
    · omega
    · intro x hx
      rcases List.mem_cons.mp hx with hx | hx
      · subst x; omega
      · exact hrest x hx

end Refpoly.CudaIP
