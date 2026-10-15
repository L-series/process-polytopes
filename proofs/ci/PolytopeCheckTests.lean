import Refpoly.KS.PolytopeChecks

open Refpoly.KS Matrix

private def requireCheck (ok : Bool) (message : String) : IO Unit := do
  unless ok do
    throw <| IO.userError message

private def sameFiniteSet {α : Type} [DecidableEq α] (xs ys : List α) : Bool :=
  xs.all (fun x => decide (x ∈ ys)) && ys.all (fun y => decide (y ∈ xs))

private def checkCase {d : ℕ} (name : String) (Q : PolyCode d)
    (ip eligible reflexive : Bool) : IO Unit := do
  requireCheck (originIPCheckExact Q == ip) s!"{name}: incorrect ambient origin-IP result"
  requireCheck (eligibleCheckExact Q == eligible) s!"{name}: incorrect eligibility result"
  requireCheck (reflexiveCheckExact Q == reflexive) s!"{name}: incorrect reflexivity result"

private def checkSmallIntervals : IO Unit := do
  let values := (List.range 7).map (fun n => (n : ℤ) - 3)
  for a in values do
    for b in values do
      if a ≤ b then
        let Q : PolyCode 1 := ⟨[fun _ => a, fun _ => b,
          fun _ => a, fun _ => (a + b) / 2]⟩
        let ip := decide (a < 0 ∧ 0 < b)
        checkCase s!"interval [{a},{b}]" Q ip
          (ip && decide (a = -b)) (decide (a = -1 ∧ b = 1))
        requireCheck (sameFiniteSet (actualVerticesExact Q) [fun _ => a, fun _ => b])
          s!"interval [{a},{b}]: incorrect actual vertices"
        let expectedPolar : List (Fin 1 → ℚ) :=
          (if a < 0 then [fun (_ : Fin 1) => -1 / (a : ℚ)] else []) ++
          (if 0 < b then [fun (_ : Fin 1) => -1 / (b : ℚ)] else [])
        requireCheck (sameFiniteSet (polarVerticesExact Q) expectedPolar)
          s!"interval [{a},{b}]: incorrect polar vertices"
        for c in (List.range 9).map (fun n => (n : ℤ) - 4) do
          requireCheck (hullContainsExact Q.points (fun _ => c) == decide (a ≤ c ∧ c ≤ b))
            s!"interval [{a},{b}]: incorrect membership for {c}"

def main : IO Unit := do
  let empty0 : PolyCode 0 := ⟨[]⟩
  checkCase "empty dimension zero" empty0 false false false
  requireCheck (actualVerticesExact empty0).isEmpty "empty hull has a primal vertex"
  requireCheck ((polarVerticesExact empty0).length == 1)
    "dimension-zero polar must have its unique point, even for an empty primal hull"
  let zero0 : PolyCode 0 := ⟨[0, 0, 0]⟩
  checkCase "repeated dimension-zero origin" zero0 true true true
  requireCheck ((actualVerticesExact zero0).length == 1)
    "dimension-zero primal vertices were not deduplicated"
  requireCheck (hullContainsExact ([] : List (IntPoint 0)) 0 == false &&
    hullContainsExact [0] (0 : IntPoint 0))
    "zero-dimensional convex-hull membership is wrong"

  let empty1 : PolyCode 1 := ⟨[]⟩
  checkCase "empty positive dimension" empty1 false false false
  requireCheck (polarVerticesExact empty1).isEmpty "the real line has an extreme point"
  checkCase "singleton origin" (⟨[0, 0]⟩ : PolyCode 1) false false false
  checkCase "positive singleton" (⟨[fun _ => 2]⟩ : PolyCode 1) false false false
  checkCase "shifted segment" (⟨[fun _ => 1, fun _ => 2]⟩ : PolyCode 1)
    false false false
  let boundary1 : PolyCode 1 := ⟨[0, fun _ => 2, 0]⟩
  checkCase "origin on segment boundary" boundary1 false false false
  requireCheck (sameFiniteSet (polarVerticesExact boundary1) [fun _ => -1 / 2])
    "unbounded polar half-line has incorrect extreme points"

  let unit : PolyCode 1 := ⟨[fun _ => -1, 0, fun _ => 1, fun _ => -1]⟩
  checkCase "unit segment with duplicates" unit true true true
  requireCheck (sameFiniteSet (actualVerticesExact unit) [fun _ => -1, fun _ => 1])
    "unit segment retained a nonvertex"
  let redundant : PolyCode 1 :=
    ⟨[fun _ => -2, fun _ => 2, fun _ => 1, fun _ => 2, 0]⟩
  checkCase "eligible nonreflexive redundant segment" redundant true true false
  requireCheck (sameFiniteSet (actualVerticesExact redundant) [fun _ => -2, fun _ => 2])
    "redundant generator was incorrectly treated as an extreme point"
  requireCheck (sameFiniteSet (polarVerticesExact redundant)
    [fun _ => -1 / 2, fun _ => 1 / 2])
    "half-integral polar vertices were not computed exactly"
  requireCheck (decide ((ratIntPairing (fun (_ : Fin 1) => 1 / 2)
    (fun _ => 1)).den ≠ 1))
    "redundant-point integrality counterexample is not exercised"
  let asymmetric : PolyCode 1 := ⟨[fun _ => -2, fun _ => 3]⟩
  checkCase "IP with nonintegral actual vertex pairing" asymmetric true false false
  requireCheck (sameFiniteSet (polarVerticesExact asymmetric)
    [fun _ => -1 / 3, fun _ => 1 / 2])
    "asymmetric rational polar vertices are wrong"
  requireCheck (hullContainsExact asymmetric.points (fun _ => -2) &&
    hullContainsExact asymmetric.points (fun _ => 0) &&
    hullContainsExact asymmetric.points (fun _ => 3) &&
    hullContainsExact asymmetric.points (fun _ => 4) == false)
    "segment membership lost an endpoint or accepted an exterior point"

  let lower : PolyCode 2 := ⟨[![-1, 0], ![1, 0], ![0, 0], ![-1, 0]]⟩
  checkCase "relative-IP is not ambient-IP" lower false false false
  requireCheck (sameFiniteSet (actualVerticesExact lower) [![-1, 0], ![1, 0]] &&
    (polarVerticesExact lower).isEmpty)
    "lower-dimensional origin section has incorrect primal or polar vertices"
  let shiftedLower : PolyCode 2 := ⟨[![-1, 1], ![1, 1]]⟩
  checkCase "shifted lower-dimensional segment" shiftedLower false false false
  requireCheck (sameFiniteSet (polarVerticesExact shiftedLower) [![0, -1]])
    "pointed unbounded polar of a lower-dimensional hull is wrong"
  let boundary2 : PolyCode 2 := ⟨[![0, 0], ![1, 0], ![0, 1]]⟩
  checkCase "origin on triangle boundary" boundary2 false false false
  requireCheck (sameFiniteSet (polarVerticesExact boundary2) [![-1, -1]])
    "polar quadrant extreme point is wrong"

  let triangle : PolyCode 2 := ⟨[![1, 0], ![0, 1], ![-1, -1], ![0, 0]]⟩
  checkCase "reflexive triangle" triangle true true true
  requireCheck (sameFiniteSet (polarVerticesExact triangle)
    [![-1, -1], ![-1, 2], ![2, -1]])
    "reflexive triangle polar is wrong"
  let square : PolyCode 2 := ⟨[![-1, -1], ![-1, 1], ![1, -1], ![1, 1]]⟩
  checkCase "IP square without a full-dimensional positive simplex" square true true true
  requireCheck (sameFiniteSet (polarVerticesExact square)
    [![-1, 0], ![1, 0], ![0, -1], ![0, 1]])
    "unit square polar is wrong"
  let largeSquare : PolyCode 2 :=
    ⟨[![-2, -2], ![-2, 2], ![2, -2], ![2, 2], ![1, 0], ![0, 0]]⟩
  checkCase "eligible nonreflexive square with redundant points" largeSquare true true false
  requireCheck (sameFiniteSet (actualVerticesExact largeSquare)
    [![-2, -2], ![-2, 2], ![2, -2], ![2, 2]])
    "large square retained a redundant point"

  let tetrahedron : PolyCode 3 := ⟨[![1, 0, 0], ![0, 1, 0],
    ![0, 0, 1], ![-1, -1, -1]]⟩
  checkCase "reflexive tetrahedron" tetrahedron true true true
  requireCheck (sameFiniteSet (polarVerticesExact tetrahedron)
    [![-1, -1, -1], ![3, -1, -1], ![-1, 3, -1], ![-1, -1, 3]])
    "three-dimensional active-constraint enumeration is wrong"
  checkCase "dimension-five singleton" (⟨[0]⟩ : PolyCode 5) false false false
  checkSmallIntervals
  IO.println "Exact polytope execution checks passed (edge cases and all 28 small intervals)."
