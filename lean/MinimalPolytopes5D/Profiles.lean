import Lean.Elab.Tactic.Decide

namespace MinimalPolytopes5D

/-
This file formalizes the arithmetic core of the dimension-5 profile
classification used in the paper draft.

Geometrically, Corollary 1 of Kreuzer-Skarke reduces a 5-dimensional
minimal polytope to an ordered list of "new vertex counts" `m₁, ..., m_λ`
with

* each `mᵢ >= 2`,
* each `mᵢ <= 6`,
* `sum (mᵢ - 1) = 5`.

The geometric reduction is proved in the paper.  Here we verify the finite
arithmetic classification of the resulting profiles by exhaustive search.
-/

def atoms : List Nat := [6, 5, 4, 3, 2]

def listsOfLength : Nat → List (List Nat)
  | 0 => [[]]
  | n + 1 =>
      (listsOfLength n).foldr
        (fun xs acc => atoms.map (fun x => x :: xs) ++ acc)
        []

def searchSpace : List (List Nat) :=
  listsOfLength 1 ++
  listsOfLength 2 ++
  listsOfLength 3 ++
  listsOfLength 4 ++
  listsOfLength 5

def nonincreasing : List Nat → Bool
  | [] => true
  | [_] => true
  | x :: y :: xs => decide (x >= y) && nonincreasing (y :: xs)

def profileWeight : List Nat → Nat
  | [] => 0
  | x :: xs => (x - 1) + profileWeight xs

def admissibleProfile (xs : List Nat) : Bool :=
  nonincreasing xs && decide (profileWeight xs = 5)

def allowedProfiles : List (List Nat) :=
  [[6], [5, 2], [4, 3], [4, 2, 2], [3, 3, 2], [3, 2, 2, 2], [2, 2, 2, 2, 2]]

def enumeratedProfiles : List (List Nat) :=
  searchSpace.filter admissibleProfile

theorem enumerateProfiles :
    enumeratedProfiles =
      [[6], [4, 3], [5, 2], [3, 3, 2], [4, 2, 2], [3, 2, 2, 2], [2, 2, 2, 2, 2]] := by
  native_decide

theorem enumerateProfiles_actualOrder :
    enumeratedProfiles =
      [[6], [4, 3], [5, 2], [3, 3, 2], [4, 2, 2], [3, 2, 2, 2], [2, 2, 2, 2, 2]] := by
  simpa using enumerateProfiles

theorem admissibleProfile_complete {xs : List Nat}
    (hspace : xs ∈ searchSpace) (hadm : admissibleProfile xs = true) :
    xs ∈ allowedProfiles := by
  native_decide +revert

theorem allowedProfiles_sound {xs : List Nat} (hmem : xs ∈ allowedProfiles) :
    xs ∈ searchSpace ∧ admissibleProfile xs = true := by
  native_decide +revert

end MinimalPolytopes5D