import Refpoly

open Refpoly.KS

def main : IO Unit := do
  let zero := enumerate procedures 0
  unless zero.length == 1 && zero.all (fun R => reflexiveCheck R) do
    throw <| IO.userError "concrete zero-dimensional endpoint is incorrect"
  let segments := enumerate procedures 1
  unless !segments.isEmpty do
    throw <| IO.userError "concrete one-dimensional endpoint missed the reflexive segment"
  for R in segments do
    unless reflexiveCheck R do
      throw <| IO.userError "concrete endpoint emitted a nonreflexive code"
    let vertices := ((actualVerticesExact R).map fun v => v 0).mergeSort (· ≤ ·)
    unless vertices == [-1, 1] do
      throw <| IO.userError "concrete endpoint lost the exact one-dimensional hull"
  IO.println "Concrete KS endpoint execution checks passed in dimensions zero and one."
