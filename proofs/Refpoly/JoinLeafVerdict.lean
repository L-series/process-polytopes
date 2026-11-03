import Refpoly.JoinLeafPoints
import Refpoly.JoinLeafMasks

/-! Soundness boundary for the leaf/verdict specification. A caller supplies
the exact count and exact accumulated functional sides. The theorem converts
those two facts to the production all-dimensions CWS rejection theorem, at the
CUDA pipeline's dimension five. Source accumulation and validator refinement
must establish the interface facts; they are not axioms of this model. -/
namespace Refpoly.JoinPoints

open Refpoly.JoinRejection

def evalPoint {N : ℕ} (a : Fin N → Int) (X : Fin N → ℕ) : Int :=
  ∑ i, a i * ((X i : Int) - 1)

noncomputable def functionalSides {N R : ℕ} (L : Layout N R)
    (coefficients : List (Fin N → Int)) : List Sides := by
  classical
  exact coefficients.map fun a =>
    (decide (∃ X, IsPoint L X ∧ evalPoint a X < 0),
     decide (∃ X, IsPoint L X ∧ 0 < evalPoint a X))

theorem enumeration_sides {N R m : ℕ} (C : CWS R N) (L : Layout N R)
    (hrows : ∀ X, IsPoint L X ↔ ∀ a, ∑ i, C.W a i * (X i : ℝ) = ∑ i, C.W a i)
    (p : Fin m → CWSPoint C) (hp : Function.Surjective p)
    (coefficients : List (Fin N → Int)) :
    ((coefficients.map (fun a i => integerEval a (p i))).map strictSides) =
      functionalSides L coefficients := by
  classical
  simp only [List.map_map, functionalSides]
  apply List.map_congr_left
  intro a _
  have hex (Q : Int → Prop) : (∃ i, Q (integerEval a (p i))) ↔
      ∃ X, IsPoint L X ∧ Q (evalPoint a X) := by
    constructor
    · rintro ⟨i, hi⟩
      exact ⟨(p i).val, (hrows _).mpr (p i).property, hi⟩
    · rintro ⟨X, hX, hq⟩
      obtain ⟨i, hi⟩ := hp ⟨X, (hrows _).mp hX⟩
      exact ⟨i, by simpa [hi, integerEval, evalPoint] using hq⟩
  apply Prod.ext
  · exact decide_eq_decide.mpr (hex (· < 0))
  · exact decide_eq_decide.mpr (hex (0 < ·))

/-- A rejected exact leaf specification cannot reject an IP CWS. -/
theorem leaf_spec_rejection_sound {R : ℕ} (C : CWS R (5 + R))
    (L : Layout (5 + R) R) (hw : Wf L) (hcov : Covered L)
    (T : Finset (Fin R → ℕ)) (hT : ∀ t ∈ T, Compatible L t)
    (hall : ∀ X, IsPoint L X → tupleOf L X ∈ T)
    (hrows : ∀ X, IsPoint L X ↔ ∀ a, ∑ i, C.W a i * (X i : ℝ) = ∑ i, C.W a i)
    (np : ℕ) (sides : List Sides) (coefficients : List (Fin (5 + R) → Int))
    (hcount : np = (index L T).card) (hmasks : sides = functionalSides L coefficients)
    (hreject : leafVerdict np [] sides ≠ .keep) : ¬ C.IsIP := by
  subst np
  subst sides
  apply leaf_rejection_sound C L hw hcov T hT hall hrows coefficients
  intro p hp hkeep
  apply hreject
  rw [leafVerdict_model]
  change decideJoin 5 (index L T).card (index L T).card true
    (functionalSides L coefficients) = .keep
  rw [← enumeration_sides C L hrows p hp coefficients]
  exact hkeep

end Refpoly.JoinPoints
