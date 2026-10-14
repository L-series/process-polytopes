import Refpoly.KS.Model

namespace Refpoly.KS

-- These are explicit data holes, not executable empty-list fallbacks.
noncomputable def searchWeights (m : ℕ) : List (WeightCode m) := by sorry

noncomputable def assembleCWS (d : ℕ) (ws : (m : ℕ) → List (WeightCode m)) :
    List (CWSCode d) := by sorry

noncomputable def integerKernelBasis {d : ℕ} (C : CWSCode d) :
    Fin C.N → Fin d → ℤ := by sorry

noncomputable def containerLatticePoints {d : ℕ} (C : CWSCode d) :
    List (IntPoint d) := by sorry

noncomputable def eligibleCheck {d : ℕ} (Q : PolyCode d) : Bool := by sorry

noncomputable def latticeRealizations {d : ℕ} (Q : PolyCode d) :
    List (PolyCode d) := by sorry

noncomputable def reflexiveCheck {d : ℕ} (R : PolyCode d) : Bool := by sorry

noncomputable def procedures : Procedures :=
  ⟨searchWeights, assembleCWS, integerKernelBasis, containerLatticePoints,
    eligibleCheck, latticeRealizations, reflexiveCheck⟩

end Refpoly.KS
