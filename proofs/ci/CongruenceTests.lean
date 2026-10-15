import Refpoly.KS.LatticeCoordinates

open Matrix Refpoly.KS

private def check {d n : ℕ} [NeZero n] (label : String)
    (S : List (ResiduePoint d n)) (hS : ValidResidues S) (index : ℕ) : IO Unit := do
  let p := congruenceBasisPair S hS
  unless decide (CongruenceBasisCert S p.1 p.2) && p.1.det.natAbs == index do
    throw <| IO.userError s!"{label}: wrong index or failed exact basis certificate"
  for z in integerBox d 3 do
    let member := decide (residueMap d n z ∈ S)
    let preimage := (integerBox d 8).any fun x => p.1 *ᵥ x == z
    let reconstructed := p.1 *ᵥ integerCoordinates p.1 z == z
    unless member == preimage && member == reconstructed do
      throw <| IO.userError s!"{label}: integral preimage mismatch"

def main : IO Unit := do
  check "standard lattice" ([0] : List (ResiduePoint 1 1)) (by decide) 1
  check "even integers" ([0] : List (ResiduePoint 1 2)) (by decide) 2
  check "full residue group" ([0, 1] : List (ResiduePoint 1 2)) (by decide) 1
  check "diagonal parity" ([0, 1] : List (ResiduePoint 2 2)) (by decide) 2
  check "zero dimension" ([0] : List (ResiduePoint 0 3)) (by decide) 1
  unless decide (ValidResidues ([1] : List (ResiduePoint 1 2))) == false do
    throw <| IO.userError "residue subset without zero accepted as a subgroup"
  unless (residueGroups 1 2).length == 2 && (residueGroups 2 2).length == 5 &&
      (residueGroups 0 3).length == 1 do
    throw <| IO.userError "finite quotient subgroup enumeration is incomplete"
  IO.println "Exact congruence-lattice basis execution checks passed."
