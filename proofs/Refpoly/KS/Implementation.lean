import Refpoly.KS.Assembly
import Refpoly.KS.IntegerKernel
import Refpoly.KS.IntegerPoints
import Refpoly.KS.PolytopeChecks
import Refpoly.KS.Realizations
import Refpoly.KS.WeightSearch

namespace Refpoly.KS

def searchWeights (m : ℕ) : List (WeightCode m) := searchWeightsExact m

def assembleCWS (d : ℕ) (ws : (m : ℕ) → List (WeightCode m)) :
    List (CWSCode d) := assemble d ws

def integerKernelBasis {d : ℕ} (C : CWSCode d) :
    Fin C.N → Fin d → ℤ := integerKernelBasisExact C

def containerLatticePoints {d : ℕ} (C : CWSCode d) :
    List (IntPoint d) := pointsForBasis C (integerKernelBasis C)

def eligibleCheck {d : ℕ} (Q : PolyCode d) : Bool := eligibleCheckExact Q

def latticeRealizations {d : ℕ} (Q : PolyCode d) :
    List (PolyCode d) := latticeRealizationsExact Q

def reflexiveCheck {d : ℕ} (R : PolyCode d) : Bool := reflexiveCheckExact R

def procedures : Procedures :=
  ⟨searchWeights, assembleCWS, integerKernelBasis, containerLatticePoints,
    eligibleCheck, latticeRealizations, reflexiveCheck⟩

end Refpoly.KS
