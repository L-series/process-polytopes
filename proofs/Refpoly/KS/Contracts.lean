import Refpoly.KS.Model

open Set

namespace Refpoly.KS

def SeedBound : Prop :=
  ∀ d, 0 < d → ∀ P : Set (V d), IsReflexive P → ∃ S : Seed d, ValidSeed P S

def CircuitBasis : Prop :=
  ∀ d, 0 < d → ∀ P : Set (V d), IsReflexive P →
    ∀ S : Seed d, ValidSeed P S →
      ∃ B : RelationBasis S, ValidCWS B.code ∧ GeometricBasis B

def WeightsSound (p : Procedures) : Prop :=
  ∀ m, ∀ w ∈ p.weights m, PrimitiveIP w

def WeightsComplete (p : Procedures) : Prop :=
  ∀ m, ∀ w : WeightCode m, PrimitiveIP w → w ∈ p.weights m

def AssemblySound (p : Procedures) : Prop :=
  WeightsSound p → ∀ d, ∀ C ∈ p.assemble d p.weights,
    ValidCWS C ∧ GeneratedRows p.weights C

def AssemblyComplete (p : Procedures) : Prop :=
  ∀ d, ∀ C : CWSCode d, ValidCWS C → GeneratedRows p.weights C →
    C ∈ p.assemble d p.weights

def KernelBases (p : Procedures) : Prop :=
  ∀ d, ∀ C : CWSCode d, ValidCWS C → KernelBasisCorrect C (p.kernelBasis C)

def GeometricContainers (p : Procedures) : Prop :=
  ∀ d, 0 < d → ∀ P : Set (V d), IsReflexive P →
    ∀ S : Seed d, ValidSeed P S → ∀ B : RelationBasis S,
      ValidCWS B.code → GeometricBasis B →
      KernelBasisCorrect B.code (p.kernelBasis B.code) →
      ∃ f : V d ≃ₗ[ℝ] V d, ForwardIntegral f ∧
        f '' P ⊆ container (p.kernelBasis B.code)

def ContainerPoints (p : Procedures) : Prop :=
  ∀ d, ∀ C : CWSCode d, ValidCWS C →
    pointSet (p.points C) = container (p.kernelBasis C) ∩ latticePoints d

def SubpolytopeRecovery : Prop :=
  ∀ d, ∀ xs : List (IntPoint d), ∀ P : Set (V d), IsLatticePolytope P →
    ∀ f : V d ≃ₗ[ℝ] V d, ForwardIntegral f →
      (f '' P ∩ latticePoints d ⊆ pointSet xs) →
      ∃ Q ∈ subpolytopes xs, Q.hull = f '' P

def ImageEligible : Prop :=
  ∀ d, ∀ P : Set (V d), IsReflexive P →
    ∀ f : V d ≃ₗ[ℝ] V d, ForwardIntegral f →
      ∀ Q : PolyCode d, Q.hull = f '' P → Eligible Q

def EligibilityCheck (p : Procedures) : Prop :=
  ∀ d, ∀ Q : PolyCode d, p.eligible Q = true ↔ Eligible Q

def TargetLattice : Prop :=
  ∀ d, ∀ P : Set (V d), IsReflexive P →
    ∀ f : V d ≃ₗ[ℝ] V d, ForwardIntegral f →
      ∀ Q : PolyCode d, Q.hull = f '' P → Compatible Q (imageLattice f)

def FiniteLatticeInterval : Prop :=
  ∀ d, ∀ Q : PolyCode d, Eligible Q → (LatticeInterval Q).Finite

def RealizationsSound (p : Procedures) : Prop :=
  ∀ d, ∀ Q : PolyCode d, Eligible Q → ∀ R ∈ p.realize Q,
    IsReflexive R.hull ∧
      ∃ e : V d ≃ₗ[ℝ] V d, e '' R.hull = Q.hull ∧ Compatible Q (imageLattice e)

def RealizationsComplete (p : Procedures) : Prop :=
  ∀ d, ∀ Q : PolyCode d, Eligible Q → (LatticeInterval Q).Finite →
    ∀ L : AddSubgroup (V d), Compatible Q L →
      ∃ R ∈ p.realize Q, ∃ e : V d ≃ₗ[ℝ] V d,
        e '' R.hull = Q.hull ∧ imageLattice e = L

def RealizationEquivalence : Prop :=
  ∀ d, ∀ P : Set (V d), ∀ R : PolyCode d,
    ∀ f e : V d ≃ₗ[ℝ] V d, e '' R.hull = f '' P →
      imageLattice e = imageLattice f → GLEquiv P R.hull

def ReflexivityCheck (p : Procedures) : Prop :=
  ∀ d, ∀ R : PolyCode d, p.reflexive R = true ↔ IsReflexive R.hull

structure Contracts (p : Procedures) : Prop where
  seedBound : SeedBound
  circuitBasis : CircuitBasis
  weightsSound : WeightsSound p
  weightsComplete : WeightsComplete p
  assemblySound : AssemblySound p
  assemblyComplete : AssemblyComplete p
  kernelBases : KernelBases p
  geometricContainers : GeometricContainers p
  containerPoints : ContainerPoints p
  subpolytopeRecovery : SubpolytopeRecovery
  imageEligible : ImageEligible
  eligibilityCheck : EligibilityCheck p
  targetLattice : TargetLattice
  finiteLatticeInterval : FiniteLatticeInterval
  realizationsSound : RealizationsSound p
  realizationsComplete : RealizationsComplete p
  realizationEquivalence : RealizationEquivalence
  reflexivityCheck : ReflexivityCheck p

end Refpoly.KS
