import Refpoly.KS.IntegerPoints

open Refpoly.KS

def main : IO Unit := do
  let segment : CWSCode 1 := ⟨2, 1, fun _ _ => 1⟩
  let segmentBasis : Fin 2 → Fin 1 → ℤ := fun j _ => if j = 0 then 1 else -1
  let segmentPoints := pointsForBasis segment segmentBasis
  unless segmentPoints.map (fun x => x 0) == [-1, 0, 1] do
    throw <| IO.userError "segment enumeration lost an endpoint or interior point"
  let narrow : CWSCode 1 := ⟨2, 1, fun _ j => if j = 0 then 2 else 3⟩
  let narrowBasis : Fin 2 → Fin 1 → ℤ := fun j _ => if j = 0 then 3 else -2
  unless (pointsForBasis narrow narrowBasis).map (fun x => x 0) == [0] do
    throw <| IO.userError "non-IP container enumeration is inexact"
  let square : CWSCode 2 := ⟨4, 2, fun a j => if j.val / 2 = a.val then 1 else 0⟩
  let squareBasis : Fin 4 → Fin 2 → ℤ :=
    fun j i => if j.val / 2 = i.val then (if j.val % 2 = 0 then 1 else -1) else 0
  let squarePoints := pointsForBasis square squareBasis
  unless squarePoints == tuples [-1, 0, 1] 2 do
    throw <| IO.userError "square enumeration lost boundary or nonvertex lattice points"
  let zero : CWSCode 0 := ⟨0, 0, Fin.elim0⟩
  unless (pointsForBasis zero Fin.elim0).length == 1 do
    throw <| IO.userError "zero-dimensional container missing"
  unless (integerBox 2 0).length == 1 && (integerBox 1 2).map (fun x => x 0) == [-2, -1, 0, 1, 2] do
    throw <| IO.userError "integer box boundaries are wrong"
  IO.println "Exact container-point execution checks passed."
