import Refpoly.JoinRejection

/-! Executable integer specification for the join's verdict and category algebra.
`keep` means inconclusive; it is never an IP acceptance. -/
namespace Refpoly.JoinRejection

inductive Verdict where
  | degenerate | certified | keep
  deriving DecidableEq, Repr

abbrev Sides := Bool × Bool

/-- CUDA verdicts 0/1/2, with unsupported layouts and capped scans failing open. -/
def decideJoin (d cap np : Nat) (covered : Bool) (sides : List Sides) : Verdict :=
  if !covered || cap < np then .keep
  else if np < d + 1 then .degenerate
  else if sides.any (fun s => s.1 != s.2) then .certified
  else .keep

theorem decideJoin_keep (d cap np : Nat) (covered : Bool) (sides : List Sides)
    (hn : d + 1 ≤ np) (hs : ∀ s ∈ sides, s.1 = s.2) :
    decideJoin d cap np covered sides = .keep := by
  have ha : sides.any (fun s => s.1 != s.2) = false := by
    apply List.any_eq_false.mpr
    intro s hmem
    simp [hs s hmem]
  simp [decideJoin, Nat.not_lt.mpr hn, ha]

def strictSides {m : Nat} (f : Fin m → Int) : Sides :=
  (decide (∃ i, f i < 0), decide (∃ i, 0 < f i))

/-- The central preservation theorem for finite exact enumeration. The family
may be assembled by any join and the functional list is arbitrary. The only
interface facts are exact point coverage and integer evaluation correctness. -/
theorem cws_model_inconclusive {r n m : Nat} (C : CWS r n) (hip : C.IsIP)
    (p : Fin m → C.subspace) (hp : Set.range p = generators C)
    (fs : List (Fin m → Int))
    (hf : ∀ f ∈ fs, ∃ l : C.subspace →ₗ[ℝ] ℝ, ∀ i, l (p i) = (f i : ℝ))
    (cap : Nat) (covered : Bool) :
    decideJoin (Module.finrank ℝ C.subspace) cap m covered (fs.map strictSides) = .keep := by
  apply decideJoin_keep
  · apply point_count_bound p
    rw [hp]
    exact hip
  · intro s hs
    obtain ⟨f, hmem, rfl⟩ := List.mem_map.mp hs
    obtain ⟨l, hl⟩ := hf f hmem
    have hb := cws_functional_balance C hip l
    rw [← hp] at hb
    have hneg : (∃ y ∈ Set.range p, l y < 0) ↔ ∃ i, f i < 0 := by
      simp only [Set.exists_range_iff, hl]
      norm_cast
    have hpos : (∃ y ∈ Set.range p, 0 < l y) ↔ ∃ i, 0 < f i := by
      simp only [Set.exists_range_iff, hl]
      norm_cast
    simp only [strictSides]
    exact decide_eq_decide.mpr (hneg.symm.trans (hb.trans hpos))

/-- Thus a rejection from this model can only concern a non-IP CWS. -/
theorem cws_model_rejection_sound {r n m : Nat} (C : CWS r n)
    (p : Fin m → C.subspace) (hp : Set.range p = generators C)
    (fs : List (Fin m → Int))
    (hf : ∀ f ∈ fs, ∃ l : C.subspace →ₗ[ℝ] ℝ, ∀ i, l (p i) = (f i : ℝ))
    (cap : Nat) (covered : Bool)
    (hr : decideJoin (Module.finrank ℝ C.subspace) cap m covered
      (fs.map strictSides) ≠ .keep) : ¬ C.IsIP := by
  intro hip
  exact hr (cws_model_inconclusive C hip p hp fs hf cap covered)

/-- Saturating a nonnegative integer to its CUDA class retains exactly the
information needed for cross-factor pair tests. -/
def valueClass (x : Nat) : Nat := min x 3

theorem class_lo (x y : Nat) :
    valueClass x + valueClass y ≤ 1 ↔ x + y ≤ 1 := by
  unfold valueClass
  omega

theorem class_hi (x y : Nat) :
    3 ≤ valueClass x + valueClass y ↔ 3 ≤ x + y := by
  unfold valueClass
  omega

/-- Bit layout of `cat_a`, `cat_b`, and each nibble of `GJoinKey.cats`. -/
def encodeCats (a : Fin 4 → Bool) : Nat :=
  (if a 0 then 1 else 0) + (if a 1 then 2 else 0) +
  (if a 2 then 4 else 0) + (if a 3 then 8 else 0)

/-- Literal transcription of `device_join_lo`. -/
def cudaLo (a b : Nat) : Bool :=
  ((a &&& 1 != 0) && (b &&& 3 != 0)) || ((a &&& 2 != 0) && (b &&& 1 != 0))

/-- Literal transcription of `device_join_hi`. -/
def cudaHi (a b : Nat) : Bool :=
  (a &&& 8 != 0) || (b &&& 8 != 0) ||
  ((a &&& 4 != 0) && (b &&& 14 != 0)) ||
  ((a &&& 14 != 0) && (b &&& 4 != 0))

/-- Kernel-checked exhaustive proof of the actual four-bit low formula. -/
theorem cudaLo_correct : ∀ a b : Fin 4 → Bool,
    cudaLo (encodeCats a) (encodeCats b) =
      decide (∃ i j : Fin 4, a i = true ∧ b j = true ∧ i.val + j.val ≤ 1) := by
  decide

/-- The high formula requires both factors nonempty. This is why marginal
class bits may be combined only across nonempty Cartesian factors. -/
theorem cudaHi_correct : ∀ a b : Fin 4 → Bool,
    (∃ i, a i = true) → (∃ j, b j = true) →
    cudaHi (encodeCats a) (encodeCats b) =
      decide (∃ i j : Fin 4, a i = true ∧ b j = true ∧ 3 ≤ i.val + j.val) := by
  decide

/-- The XOR rule leaves both the constant and the straddling cases inconclusive. -/
theorem xor_rule : ∀ lo hi : Bool, (lo != hi) = true ↔
    (lo = true ∧ hi = false) ∨ (lo = false ∧ hi = true) := by
  decide

end Refpoly.JoinRejection
