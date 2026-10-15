import Refpoly

-- This must compile without a noncomputable escape hatch when the routines land.
def ksExecutable (d : ℕ) : List (Refpoly.KS.PolyCode d) :=
  Refpoly.KS.enumerate Refpoly.KS.procedures d
