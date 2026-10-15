import Refpoly.KS.Realizations

open Refpoly.KS

private def check {d : ℕ} (label : String) (Q : PolyCode d)
    (sizes : List ℕ) (eligible : Bool := true) : IO Unit := do
  IO.println s!"Starting {label}"
  unless eligibleCheckExact Q == eligible do
    throw <| IO.userError s!"{label}: eligibility mismatch"
  let Rs := latticeRealizationsExact Q
  unless Rs.length == sizes.length do
    throw <| IO.userError s!"{label}: missing or extra lattice realizations"
  for R in Rs do
    unless reflexiveCheckExact R do
      throw <| IO.userError s!"{label}: reconstructed output is not reflexive"
  if hc : eligibleCheckExact Q = true then
    let hQ := (eligibleCheckExact_correct Q).mp hc
    let actual := ((realizationResidues Q hQ).map fun S => S.1.length).mergeSort (· ≤ ·)
    unless actual == sizes.mergeSort (· ≤ ·) do
      throw <| IO.userError s!"{label}: wrong compatible subgroups"

def main : IO Unit := do
  check "zero dimension" (⟨[0]⟩ : PolyCode 0) [1]
  check "empty zero-dimensional hull" (⟨[]⟩ : PolyCode 0) [] false
  check "standard segment" (⟨[![-1], ![1]]⟩ : PolyCode 1) [1]
  check "nonreflexive eligible segment" (⟨[![-2], ![2]]⟩ : PolyCode 1) [1]
  check "redundant nonintegral pairing" (⟨[![-2], ![1], ![2], ![2]]⟩ : PolyCode 1) [1]
  check "ineligible asymmetric segment" (⟨[![-1], ![2]]⟩ : PolyCode 1) [] false
  check "lower-dimensional hull" (⟨[![-1, 0], ![1, 0]]⟩ : PolyCode 2) [] false
  check "square coarse and fine lattices"
    (⟨[![-1, -1], ![-1, 1], ![1, -1], ![1, 1]]⟩ : PolyCode 2) [2, 4]
  check "diamond coarse and fine lattices"
    (⟨[![-1, 0], ![0, -1], ![1, 0], ![0, 1]]⟩ : PolyCode 2) [2, 4]
  check "nonreflexive eligible square"
    (⟨[![-2, -2], ![-2, 2], ![2, -2], ![2, 2]]⟩ : PolyCode 2) [2, 4]
  IO.println "Exact lattice-realization execution checks passed."
