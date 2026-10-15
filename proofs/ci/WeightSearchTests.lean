import Refpoly.KS.WeightSearch

open Refpoly Refpoly.KS

namespace WeightSearchTests

example (m : ℕ) (w : WeightCode m) :
    w ∈ searchWeightsExact m ↔ PrimitiveIP w :=
  ⟨searchWeightsExact_sound m w, searchWeightsExact_complete m w⟩

def asList {m : ℕ} (w : WeightCode m) : List ℕ :=
  (List.finRange m).map w

def assertCheck (ok : Bool) (message : String) : IO Unit := do
  unless ok do
    throw <| IO.userError message

/-- Independent exhaustive enumeration in shifted Newton coordinates. -/
def referenceNewtonPoints {m : ℕ} (w : WeightCode m) : List (IntPoint m) :=
  let D := ∑ i, w i
  ((tuples (List.range (D + 1)) m).filter fun x =>
    decide (∑ i, w i * x i = D)).map fun x i => (x i : ℤ) - 1

def primitivePositive {m : ℕ} (w : WeightCode m) : Bool :=
  decide (Finset.univ.gcd w = 1 ∧ ∀ i, 0 < w i)

def cross (x y : IntPoint 3) : ℤ :=
  x 0 * y 1 - x 1 * y 0

/-- In arities two and three, projection of the positive-weight kernel onto
its first m-1 coordinates is an isomorphism. The reference checks signs on
the line, or full planar span and both sides of every generator line.
Higher arities are explicitly outside this test oracle's domain. -/
def referenceSmallPrimitiveIP : {m : ℕ} → WeightCode m → Option Bool
  | 0, _ => some false
  | 1, w => some (w 0 == 1)
  | 2, w =>
      some (primitivePositive w && (
        let ys := referenceNewtonPoints w
        ys.any (fun y => y 0 < 0) && ys.any (fun y => 0 < y 0)))
  | 3, w =>
      some (primitivePositive w && (
        let ys := referenceNewtonPoints w
        ys.any (fun x => ys.any (fun y => cross x y != 0)) &&
          ys.all (fun x =>
            (x 0 == 0 && x 1 == 0) ||
              (ys.any (fun y => cross x y < 0) && ys.any (fun y => 0 < cross x y)))))
  | _ + 4, _ => none

def referenceWeights (m D : ℕ) : List (WeightCode m) :=
  (tuples (List.range (D + 1)) m).filter fun w =>
    decide ((∑ i, w i) ≤ D) && (referenceSmallPrimitiveIP w).getD false

def checkSelection : IO Unit := do
  assertCheck (decide (WeightSearch.selection [fun _ : Fin 2 => (1 : ℤ)] =
      ![(1 : ℚ) / 2, 1 / 2])) "root selection did not return exact half weights"
  assertCheck (decide (WeightSearch.selection
      [(![2, 0, 0] : IntPoint 3), fun _ => 1] = ![(1 : ℚ) / 2, 1 / 4, 1 / 4]))
    "positive feasible selection lost its exact rational coordinates"
  assertCheck (!WeightSearch.selectionCheck
      (WeightSearch.selection [(![1, 0, 0] : IntPoint 3), fun _ => 1]))
    "a boundary-only feasible set was treated as strictly positive"
  assertCheck (!WeightSearch.selectionCheck
      (WeightSearch.selection [(![0, 0] : IntPoint 2), fun _ => 1]))
    "an infeasible equation was accepted"
  assertCheck (!WeightSearch.selectionCheck (![-1, 2] : Fin 2 → ℚ))
    "a nonpositive rational candidate was accepted"
  assertCheck (decide (rationalWeightCode (![(1 : ℚ) / 2, 1 / 2]) = ![1, 1]))
    "half weights did not primitive-normalize exactly"
  assertCheck (decide (rationalWeightCode (![(1 : ℚ) / 2, 1 / 4, 1 / 4]) = ![2, 1, 1]))
    "primitive rational encoding changed coordinate order"
  let children := WeightSearch.branches (![(1 : ℚ) / 2, 1 / 2])
  assertCheck (decide (children.toFinset =
      ([(![0, 0] : IntPoint 2), ![0, 1], ![1, 0]] : List (IntPoint 2)).toFinset))
    "finite branching disagreed with the exact strict dot-product bound"
  IO.println "Rational selection, normalization, positivity, and exact branch checks passed."
  (← IO.getStdout).flush

def checkArity (m D expectedCount : ℕ) : IO (List (WeightCode m)) := do
  assertCheck (decide (m ≤ 3)) "reference oracle only covers arities at most three"
  IO.println s!"Starting complete exact search at arity {m}"
  (← IO.getStdout).flush
  let actual := searchWeightsExact m
  let expected := referenceWeights m D
  let actualKeys := actual.map asList
  let expectedKeys := expected.map asList
  assertCheck (decide (actualKeys.toFinset = expectedKeys.toFinset))
    s!"arity {m}: exact ordered codes disagree with independent degree-{D} enumeration"
  assertCheck (decide (actualKeys.length = actualKeys.toFinset.card))
    s!"arity {m}: repeated exact codes remain in the output"
  assertCheck (decide (actual.length = expectedCount))
    s!"arity {m}: expected {expectedCount} codes, obtained {actual.length}"
  IO.println s!"arity {m}: {actual.length} exact ordered codes match the independent bounded reference"
  IO.println s!"  {actualKeys}"
  (← IO.getStdout).flush
  return actual

def orderedThreeFamilies : List (WeightCode 3) :=
  [![1, 1, 1], ![1, 1, 2], ![1, 2, 1], ![2, 1, 1],
    ![1, 2, 3], ![1, 3, 2], ![2, 1, 3], ![2, 3, 1], ![3, 1, 2], ![3, 2, 1]]

def run : IO Unit := do
  checkSelection
  let _ ← checkArity 0 12 0
  let ws1 ← checkArity 1 12 1
  let ws2 ← checkArity 2 12 1
  let ws3 ← checkArity 3 12 10
  assertCheck (decide (ws1.map asList = [[1]]))
    "arity one did not return exactly its accepted ordered code"
  assertCheck (decide (ws2.map asList = [[1, 1]]))
    "arity two did not return exactly its accepted ordered code"
  assertCheck (decide ((ws3.map asList).toFinset =
      (orderedThreeFamilies.map asList).toFinset))
    "accepted arity-three output is not exactly the ten ordered codes of the three families"
  assertCheck (decide ((![(1 : ℕ)] : WeightCode 1) ∈ ws1))
    "the zero-dimensional primitive weight was omitted"
  assertCheck (decide ((![(1 : ℕ), 1] : WeightCode 2) ∈ ws2))
    "(1,1) was omitted"
  for w in [(![0, 1] : WeightCode 2), ![1, 0], ![2, 2], ![1, 3], ![3, 1]] do
    assertCheck (decide (w ∉ ws2)) s!"invalid arity-two code accepted: {asList w}"
    assertCheck (referenceSmallPrimitiveIP w == some false)
      s!"independent reference accepted invalid code {asList w}"
  for w in [(![1, 1, 2] : WeightCode 3), ![1, 2, 1], ![2, 1, 1],
      ![1, 2, 3], ![1, 3, 2], ![2, 1, 3], ![2, 3, 1], ![3, 1, 2], ![3, 2, 1]] do
    assertCheck (decide (w ∈ ws3)) s!"ordered half-weight/permutation omitted: {asList w}"
  for w in [(![0, 1, 1] : WeightCode 3), ![2, 2, 2], ![1, 1, 3]] do
    assertCheck (decide (w ∉ ws3)) s!"invalid arity-three code accepted: {asList w}"
  IO.println "All terminating primitive ordered IP-weight native execution checks passed."
  IO.println "Bounded reference comparisons are regression tests, not the completeness proof."

end WeightSearchTests

def main : IO Unit := WeightSearchTests.run
