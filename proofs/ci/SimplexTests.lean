import Refpoly.KS.RationalSimplex

open Refpoly.KS

def main : IO Unit := do
  let segment : Fin 2 → IntPoint 1 := fun i _ => if i = 0 then -2 else 3
  unless positiveSimplexCheck segment &&
      simplexWeights segment 0 == 3 / 5 && simplexWeights segment 1 == 2 / 5 do
    throw <| IO.userError "exact nonuniform segment relation failed"
  let shifted : Fin 2 → IntPoint 1 := fun i _ => if i = 0 then 1 else 2
  unless positiveSimplexCheck shifted == false do
    throw <| IO.userError "simplex outside the origin accepted"
  let duplicate : Fin 2 → IntPoint 2 := fun _ _ => 0
  unless positiveSimplexCheck duplicate == false do
    throw <| IO.userError "affinely dependent simplex accepted"
  let embedded : Fin 2 → IntPoint 2 :=
    fun i j => if j = 0 then (if i = 0 then -1 else 1) else 0
  unless positiveSimplexCheck embedded do
    throw <| IO.userError "relative-IP segment rejected"
  let triangle : Fin 3 → IntPoint 2 :=
    fun i j => if i = 2 then -1 else if i.val = j.val then 1 else 0
  unless positiveSimplexCheck triangle && (List.finRange 3).all
      (fun i => simplexWeights triangle i == 1 / 3) do
    throw <| IO.userError "triangle relation is inexact"
  unless positiveSimplexCheck (fun (_ : Fin 1) (_ : Fin 0) => (0 : ℤ)) &&
      positiveSimplexCheck (Fin.elim0 : Fin 0 → IntPoint 0) == false do
    throw <| IO.userError "zero-dimensional or empty simplex case failed"
  IO.println "Exact rational-simplex execution checks passed."
