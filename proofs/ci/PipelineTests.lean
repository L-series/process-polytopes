import Refpoly.KS.Pipeline

open Refpoly.KS

-- Synthetic routines check orchestration only; they do not satisfy Contracts.
def mockCWS (d : ℕ) : CWSCode d :=
  ⟨0, 0, fun a => Fin.elim0 a⟩

def mockProcedures : Procedures where
  weights _ := []
  assemble d _ := [mockCWS d]
  kernelBasis _C := fun _ _ => 0
  points _ := [fun _ => -1, fun _ => 0, fun _ => 1]
  eligible Q := Q.points.length == 2
  realize Q := [⟨Q.points ++ Q.points⟩, Q]
  reflexive R := R.points.length == 4

#guard (subpolytopes ([fun _ : Fin 1 => -1, fun _ => 0, fun _ => 1])).length == 8
#guard (candidates mockProcedures 1).length == 6
#guard ((enumerate mockProcedures 1).map fun R => R.points.length) == [4, 4, 4]
#guard ((enumerate mockProcedures 0).map fun R => R.points.length) == [1]
