import Refpoly.JoinPoints
import Refpoly.JoinSummary

/-! The per-coordinate specification in verification/gpu/join_leaf.cpp uses
four saturated classes. These lemmas prove that combining the classes retains
exactly the low/high pair predicates. The row occupancy lemmas supply the
independent fibre members; a same-row private pair must use its joint summary.
-/
namespace Refpoly.JoinRejection

/-- Plain Boolean formulas checked against the extracted leaf by CBMC. -/
def leafLo (a b : Fin 4 → Bool) : Bool :=
  (a 0 && (b 0 || b 1)) || (a 1 && b 0)

def leafHi (a b : Fin 4 → Bool) : Bool :=
  a 3 || (a 2 && (b 1 || b 2 || b 3)) || ((a 1 || a 2 || a 3) && b 2)

theorem leafLo_cuda : ∀ a b : Fin 4 → Bool,
    leafLo a b = cudaLo (encodeCats a) (encodeCats b) := by decide

/-- The verdict supplies the transposed >=3 bit omitted from each HI row. -/
theorem leafHi_cuda : ∀ a b : Fin 4 → Bool,
    (leafHi a b || b 3) = cudaHi (encodeCats a) (encodeCats b) := by decide

/-- Plain verdict specification. The CUDA production path has dimension five;
the all-dimensions soundness theorem is instantiated at five by the bridge. -/
def leafVerdict (np : ℕ) (coordinateSides pairSides : List Sides) : Verdict :=
  if np < 6 then .degenerate
  else if (coordinateSides ++ pairSides).any (fun s => s.1 != s.2) then .certified
  else .keep

theorem leafVerdict_model (np : ℕ) (coordinateSides pairSides : List Sides) :
    leafVerdict np coordinateSides pairSides =
      decideExactJoin 5 np true (coordinateSides ++ pairSides) := by
  simp [leafVerdict, decideExactJoin, decideJoin]

def classes {α : Type*} (F : Finset α) (read : α → ℕ) (c : Fin 4) : Bool :=
  decide (∃ x ∈ F, valueClass (read x) = c.val)

theorem classes_nonempty {α : Type*} (F : Finset α) (read : α → ℕ)
    (hne : F.Nonempty) : ∃ c, classes F read c = true := by
  obtain ⟨x, hx⟩ := hne
  have hc : valueClass (read x) < 4 := by unfold valueClass; omega
  exact ⟨⟨_, hc⟩, by simp [classes]; exact ⟨x, hx, rfl⟩⟩

theorem classes_lo {α β : Type*} (F : Finset α) (G : Finset β)
    (a : α → ℕ) (b : β → ℕ) :
    cudaLo (encodeCats (classes F a)) (encodeCats (classes G b)) =
      decide (∃ x ∈ F, ∃ y ∈ G, a x + b y ≤ 1) := by
  rw [cudaLo_correct]
  apply decide_eq_decide.mpr
  constructor
  · rintro ⟨i, j, hi, hj, hij⟩
    obtain ⟨x, hx, hxi⟩ := of_decide_eq_true hi
    obtain ⟨y, hy, hyj⟩ := of_decide_eq_true hj
    exact ⟨x, hx, y, hy, (class_lo _ _).mp (by omega)⟩
  · rintro ⟨x, hx, y, hy, hxy⟩
    have ha : valueClass (a x) < 4 := by unfold valueClass; omega
    have hb : valueClass (b y) < 4 := by unfold valueClass; omega
    refine ⟨⟨_, ha⟩, ⟨_, hb⟩, ?_, ?_, (class_lo _ _).mpr hxy⟩
    · exact decide_eq_true ⟨x, hx, rfl⟩
    · exact decide_eq_true ⟨y, hy, rfl⟩

theorem classes_hi {α β : Type*} (F : Finset α) (G : Finset β)
    (a : α → ℕ) (b : β → ℕ) (hF : F.Nonempty) (hG : G.Nonempty) :
    cudaHi (encodeCats (classes F a)) (encodeCats (classes G b)) =
      decide (∃ x ∈ F, ∃ y ∈ G, 3 ≤ a x + b y) := by
  rw [cudaHi_correct _ _ (classes_nonempty F a hF) (classes_nonempty G b hG)]
  apply decide_eq_decide.mpr
  constructor
  · rintro ⟨i, j, hi, hj, hij⟩
    obtain ⟨x, hx, hxi⟩ := of_decide_eq_true hi
    obtain ⟨y, hy, hyj⟩ := of_decide_eq_true hj
    exact ⟨x, hx, y, hy, (class_hi _ _).mp (by omega)⟩
  · rintro ⟨x, hx, y, hy, hxy⟩
    have ha : valueClass (a x) < 4 := by unfold valueClass; omega
    have hb : valueClass (b y) < 4 := by unfold valueClass; omega
    refine ⟨⟨_, ha⟩, ⟨_, hb⟩, ?_, ?_, (class_hi _ _).mpr hxy⟩
    · exact decide_eq_true ⟨x, hx, rfl⟩
    · exact decide_eq_true ⟨y, hy, rfl⟩

end Refpoly.JoinRejection
