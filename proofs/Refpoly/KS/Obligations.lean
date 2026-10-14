import Refpoly.KS.Composition
import Refpoly.KS.Implementation

namespace Refpoly.KS

theorem seed_bound : SeedBound := by sorry
theorem circuit_basis : CircuitBasis := by sorry
theorem weights_sound : WeightsSound procedures := by sorry
theorem weights_complete : WeightsComplete procedures := by sorry
theorem assembly_sound : AssemblySound procedures := by sorry
theorem assembly_complete : AssemblyComplete procedures := by sorry
theorem kernel_bases : KernelBases procedures := by sorry
theorem geometric_containers : GeometricContainers procedures := by sorry
theorem container_points : ContainerPoints procedures := by sorry
theorem subpolytope_recovery : SubpolytopeRecovery := by sorry
theorem image_eligible : ImageEligible := by sorry
theorem eligibility_check : EligibilityCheck procedures := by sorry
theorem target_lattice : TargetLattice := by sorry
theorem finite_lattice_interval : FiniteLatticeInterval := by sorry
theorem realizations_sound : RealizationsSound procedures := by sorry
theorem realizations_complete : RealizationsComplete procedures := by sorry
theorem realization_equivalence : RealizationEquivalence := by sorry
theorem reflexivity_check : ReflexivityCheck procedures := by sorry

theorem contracts : Contracts procedures :=
  ⟨seed_bound, circuit_basis, weights_sound, weights_complete, assembly_sound,
    assembly_complete, kernel_bases, geometric_containers, container_points,
    subpolytope_recovery, image_eligible, eligibility_check, target_lattice,
    finite_lattice_interval, realizations_sound, realizations_complete,
    realization_equivalence, reflexivity_check⟩

theorem classifies_reflexive : Classifies procedures :=
  classifies_of_contracts procedures contracts

end Refpoly.KS
