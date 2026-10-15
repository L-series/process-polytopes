import Refpoly.KS.IntegerKernel

open Matrix Refpoly Refpoly.KS

namespace KernelTests

def row11 : CWSCode 1 := ⟨2, 1, fun _ => ![1, 1]⟩
def row123 : CWSCode 2 := ⟨3, 1, fun _ => ![1, 2, 3]⟩
def twoRows : CWSCode 2 := ⟨4, 2, ![![1, 1, 0, 0], ![0, 0, 1, 1]]⟩
def emptyRows : CWSCode 0 := ⟨0, 0, Fin.elim0⟩
def dimensionMismatch : CWSCode 1 := ⟨3, 1, fun _ => ![1, 1, 1]⟩
def oversizedDimension : CWSCode 3 := ⟨2, 1, fun _ => ![1, 1]⟩
def uncoveredCoordinate : CWSCode 2 := ⟨3, 1, fun _ => ![1, 1, 0]⟩
def dependentRows : CWSCode 2 := ⟨4, 2, fun _ _ => 1⟩

def rowsList {d : ℕ} (C : CWSCode d) (b : Fin C.N → Fin d → ℤ) : List (List ℤ) :=
  (List.finRange C.N).map fun j => (List.finRange d).map fun i => b j i

def minorsGcd {d : ℕ} (C : CWSCode d) (b : Fin C.N → Fin d → ℤ) : ℕ :=
  ((embeddings d C.N).map fun e =>
    (Matrix.det (Matrix.of fun i j : Fin d => b (e i) j)).natAbs).foldl Nat.gcd 0

def checkValid {d : ℕ} (name : String) (C : CWSCode d) (radius : ℕ)
    (heq : d + C.r = C.N) (y : Fin C.N → ℤ) : IO Unit := do
  IO.println s!"Starting {name}"
  (← IO.getStdout).flush
  unless validCWSCheck C do
    throw <| IO.userError s!"{name}: valid input rejected"
  -- Materialize entries once rather than repeating the search for every minor and preimage.
  let data := ((rowsList C (integerKernelBasisExact C)).map List.toArray).toArray
  let b : Fin C.N → Fin d → ℤ := fun j i => (data.getD j.val #[]).getD i.val 0
  unless decide (IntegerKernel.intRows C * Matrix.of b = 0) do
    throw <| IO.userError s!"{name}: columns do not annihilate the integer rows"
  unless minorsGcd C b == 1 do
    throw <| IO.userError s!"{name}: maximal minors do not certify saturation"
  unless decide (IntegerKernel.intRows C *ᵥ y = 0) do
    throw <| IO.userError s!"{name}: test vector is not in the integer kernel"
  unless (tuples (IntegerKernel.integerBox 8) d).any (fun x =>
      decide (Matrix.of b *ᵥ x = y)) do
    throw <| IO.userError s!"{name}: test vector has no integral preimage in [-8,8]^d"
  let hd : d ≤ C.N := by omega
  match IntegerKernel.boundedWitness C hd heq radius with
  | none =>
    throw <| IO.userError s!"{name}: no certificate within radius {radius}"
  | some U =>
    let det := U.det
    unless (det == 1 || det == -1) &&
        decide (IntegerKernel.intRows C * IntegerKernel.columns C hd U = 0) do
      throw <| IO.userError s!"{name}: bounded witness is not an exact unimodular certificate"
  IO.println s!"{name}: basis={rowsList C b}; annihilation, saturation, preimage, certificate passed"
  (← IO.getStdout).flush

def checkInvalid {d : ℕ} (name : String) (C : CWSCode d) : IO Unit := do
  unless validCWSCheck C == false do
    throw <| IO.userError s!"{name}: malformed input accepted"
  unless decide (integerKernelBasisExact C = 0) do
    throw <| IO.userError s!"{name}: malformed input did not return zero"
  IO.println s!"{name}: rejected and zero totalization checked"

def run : IO Unit := do
  checkValid "(1,1)" row11 1 (by decide) ![3, -3]
  checkValid "(1,2,3)" row123 2 (by decide) ![1, 1, -1]
  checkValid "independent two-row system" twoRows 1 (by decide) ![3, -3, -2, 2]
  checkValid "dimension zero / empty kernel" emptyRows 0 (by decide) Fin.elim0
  checkInvalid "dimension mismatch" dimensionMismatch
  checkInvalid "dimension larger than coordinate count" oversizedDimension
  checkInvalid "uncovered coordinate" uncoveredCoordinate
  checkInvalid "dependent covering rows" dependentRows
  IO.println "All exact integer-kernel native execution checks passed."

end KernelTests

def main : IO Unit := KernelTests.run
