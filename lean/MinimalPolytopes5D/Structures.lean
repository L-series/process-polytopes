import Lean.Elab.Tactic.Decide
import MinimalPolytopes5D.Profiles

namespace MinimalPolytopes5D

abbrev Simplex := List Nat
abbrev Structure := List Simplex

def insertNat (x : Nat) : List Nat -> List Nat
  | [] => [x]
  | y :: ys => if x <= y then x :: y :: ys else y :: insertNat x ys

def insertNatDesc (x : Nat) : List Nat -> List Nat
  | [] => [x]
  | y :: ys => if x >= y then x :: y :: ys else y :: insertNatDesc x ys

def sortNat : List Nat -> List Nat
  | [] => []
  | x :: xs => insertNat x (sortNat xs)

def sortNatDesc : List Nat -> List Nat
  | [] => []
  | x :: xs => insertNatDesc x (sortNatDesc xs)

def simplexLT : Simplex -> Simplex -> Bool
  | [], [] => false
  | [], _ :: _ => true
  | _ :: _, [] => false
  | x :: xs, y :: ys =>
      if x < y then true else if y < x then false else simplexLT xs ys

def structureLT : Structure -> Structure -> Bool
  | [], [] => false
  | [], _ :: _ => true
  | _ :: _, [] => false
  | s :: ss, t :: ts =>
      if simplexLT s t then true
      else if simplexLT t s then false
      else structureLT ss ts

def insertStructure (x : Structure) : List Structure -> List Structure
  | [] => [x]
  | y :: ys =>
      if x = y then y :: ys
      else if structureLT x y then x :: y :: ys
      else y :: insertStructure x ys

def bindList {alpha beta : Type} (xs : List alpha) (f : alpha -> List beta) : List beta :=
  xs.foldr (fun x acc => f x ++ acc) []

def selections {alpha : Type} : List alpha -> List (alpha × List alpha)
  | [] => []
  | x :: xs =>
      (x, xs) ::
        (selections xs).map (fun
          | (y, ys) => (y, x :: ys))

def insertEverywhere {alpha : Type} (x : alpha) : List alpha -> List (List alpha)
  | [] => [[x]]
  | y :: ys => (x :: y :: ys) :: (insertEverywhere x ys).map (fun zs => y :: zs)

def permutations {alpha : Type} : List alpha -> List (List alpha)
  | [] => [[]]
  | x :: xs => bindList (permutations xs) (insertEverywhere x)

def rangeFrom (start len : Nat) : List Nat :=
  (List.range len).map (fun i => start + i)

def choose {alpha : Type} : Nat -> List alpha -> List (List alpha)
  | 0, _ => [[]]
  | _ + 1, [] => []
  | n + 1, x :: xs =>
      (choose n xs).map (fun ys => x :: ys) ++ choose (n + 1) xs

def maxSimplex : Simplex -> Nat
  | [] => 0
  | x :: xs => xs.foldl Nat.max x

def maxVertex : Structure -> Nat
  | [] => 0
  | s :: ss => Nat.max (maxSimplex s) (maxVertex ss)

def occursIn (v : Nat) : Structure -> Nat
  | [] => 0
  | s :: ss => (if v ∈ s then 1 else 0) + occursIn v ss

def uniqueCount (cand : Structure) (s : Simplex) : Nat :=
  s.foldl (fun acc v => acc + if occursIn v cand = 1 then 1 else 0) 0

def lemma2Ok (cand : Structure) : Bool :=
  cand.foldl (fun acc s => acc && decide (uniqueCount cand s ≠ 1)) true

def appendIfNew (seen : List Nat) (v : Nat) : List Nat :=
  if v ∈ seen then seen else seen ++ [v]

def firstOccurrenceOrder (st : Structure) : List Nat :=
  st.foldl (fun seen s => s.foldl appendIfNew seen) []

def labelAux (v : Nat) : Nat -> List Nat -> Nat
  | _, [] => 0
  | n, x :: xs => if v = x then n else labelAux v (n + 1) xs

def labelOf (order : List Nat) (v : Nat) : Nat := labelAux v 1 order

def relabelStructure (st : Structure) : Structure :=
  let order := firstOccurrenceOrder st
  st.map (fun s => sortNat (s.map (labelOf order)))

def canonical (st : Structure) : Structure :=
  match ((permutations st).map relabelStructure) with
  | [] => []
  | x :: xs => xs.foldl (fun best cand => if structureLT cand best then cand else best) x

def known3 : List Structure :=
  [
    [[1, 2, 3, 4]],
    [[1, 2, 3], [4, 5]],
    [[1, 2, 3], [1, 4, 5]],
    [[1, 2], [3, 4], [5, 6]]
  ]

def known4 : List Structure :=
  [
    [[1, 2, 3, 4, 5]],
    [[1, 2, 3, 4], [1, 2, 5, 6]],
    [[1, 2, 3, 4], [1, 5, 6]],
    [[1, 2, 3, 4], [5, 6]],
    [[1, 2, 3], [4, 5, 6]],
    [[1, 2, 3], [4, 5], [6, 7]],
    [[1, 2, 3], [1, 4, 5], [6, 7]],
    [[1, 2, 3], [1, 4, 5], [1, 6, 7]],
    [[1, 2], [3, 4], [5, 6], [7, 8]]
  ]

def extendTo5 (nprime : Nat) (base : Structure) : List Structure :=
  let kprime := maxVertex base
  let m := 6 - nprime
  let newVs := rangeFrom (kprime + 1) m
  let maxR := nprime - (m - 1)
  bindList (List.range (maxR + 1)) fun r =>
    (choose r (rangeFrom 1 kprime)).filterMap fun rset =>
      let cand := base ++ [sortNat (rset ++ newVs)]
      if lemma2Ok cand then some cand else none

def rawFiveDimensionalStructures : List Structure :=
  [[[1, 2, 3, 4, 5, 6]]] ++
    (bindList known4 (extendTo5 4)) ++
    (bindList known3 (extendTo5 3))

def canonicalFiveDimensionalStructures : List Structure :=
  (rawFiveDimensionalStructures.map canonical).foldr insertStructure []

def vertexCount (st : Structure) : Nat := maxVertex st

def orderedProfile (st : Structure) : List Nat :=
  let step : (List Nat × List Nat) -> Simplex -> (List Nat × List Nat) :=
    fun (seen, counts) s =>
      let newCount := s.foldl (fun acc v => acc + if v ∈ seen then 0 else 1) 0
      let seen' := s.foldl appendIfNew seen
      (seen', counts ++ [newCount])
  (st.foldl step ([], [])).2

def sortedProfile (st : Structure) : List Nat :=
  sortNatDesc (orderedProfile st)

def canonicalProfiles : List (List Nat) :=
  canonicalFiveDimensionalStructures.map sortedProfile

def countsByVertex : List (Nat × Nat) :=
  [6, 7, 8, 9, 10].map fun k =>
    (k, (canonicalFiveDimensionalStructures.filter fun st => vertexCount st = k).length)

def countsByProfile : List (List Nat × Nat) :=
  [ [6], [5, 2], [4, 3], [4, 2, 2], [3, 3, 2], [3, 2, 2, 2], [2, 2, 2, 2, 2] ].map
    fun p => (p, (canonicalProfiles.filter fun q => q = p).length)

theorem canonicalFiveDimensionalStructures_length :
    canonicalFiveDimensionalStructures.length = 47 := by
  native_decide

theorem countsByVertex_correct :
    countsByVertex = [(6, 1), (7, 6), (8, 25), (9, 13), (10, 2)] := by
  native_decide

theorem countsByProfile_correct :
    countsByProfile =
      [([6], 1), ([5, 2], 2), ([4, 3], 4), ([4, 2, 2], 7),
       ([3, 3, 2], 18), ([3, 2, 2, 2], 13), ([2, 2, 2, 2, 2], 2)] := by
  native_decide

def countsByProfileTotal : Nat :=
  (countsByProfile.map Prod.snd).foldl Nat.add 0

theorem allSortedProfilesAccountedFor :
    countsByProfileTotal = canonicalFiveDimensionalStructures.length := by
  native_decide

theorem sortedProfile_allowed (st : Structure)
    (hmem : st ∈ canonicalFiveDimensionalStructures) :
  sortedProfile st ∈ allowedProfiles := by
  native_decide +revert

end MinimalPolytopes5D