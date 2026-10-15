import Refpoly.KS.Composition
import Refpoly.KS.Implementation
import Refpoly.KS.SeedBound
import Refpoly.KS.Circuits
import Refpoly.KS.Containers
import Refpoly.KS.Recovery
import Refpoly.KS.Lattices

namespace Refpoly.KS

theorem seed_bound : SeedBound := seed_bound_proved
theorem circuit_basis : CircuitBasis := circuitBasis_of_validSeed
theorem weights_sound : WeightsSound procedures := by
  intro m w hw
  exact searchWeightsExact_sound m w hw
theorem weights_complete : WeightsComplete procedures := by
  intro m w hw
  exact searchWeightsExact_complete m w hw
theorem assembly_sound : AssemblySound procedures := by
  intro _ d C hC
  exact (mem_assemble C).mp hC
theorem assembly_complete : AssemblyComplete procedures := by
  intro d C hvalid hrows
  exact (mem_assemble C).mpr ⟨hvalid, hrows⟩
theorem kernel_bases : KernelBases procedures := by
  intro d C hC
  exact integerKernelBasisExact_correct C hC
theorem geometric_containers : GeometricContainers procedures :=
  geometricContainers_proof procedures
theorem container_points : ContainerPoints procedures := by
  intro d C hC
  exact pointsForBasis_exact C hC (integerKernelBasis C) (kernel_bases d C hC)
theorem subpolytope_recovery : SubpolytopeRecovery := subpolytopeRecovery_proof
theorem image_eligible : ImageEligible := imageEligible_proof
theorem eligibility_check : EligibilityCheck procedures := by
  intro d Q
  exact eligibleCheckExact_correct Q
theorem target_lattice : TargetLattice := targetLattice_proof
theorem finite_lattice_interval : FiniteLatticeInterval := finiteLatticeInterval_proof
theorem realizations_sound : RealizationsSound procedures := by
  intro d Q hQ R hR
  exact latticeRealizationsExact_sound Q hQ R hR
theorem realizations_complete : RealizationsComplete procedures := by
  intro d Q hQ _ L hL
  exact latticeRealizationsExact_complete Q hQ L hL
theorem realization_equivalence : RealizationEquivalence := realizationEquivalence_proof
theorem reflexivity_check : ReflexivityCheck procedures := by
  intro d R
  exact reflexiveCheckExact_correct R

theorem contracts : Contracts procedures :=
  ⟨seed_bound, circuit_basis, weights_sound, weights_complete, assembly_sound,
    assembly_complete, kernel_bases, geometric_containers, container_points,
    subpolytope_recovery, image_eligible, eligibility_check, target_lattice,
    finite_lattice_interval, realizations_sound, realizations_complete,
    realization_equivalence, reflexivity_check⟩

theorem classifies_reflexive : Classifies procedures :=
  classifies_of_contracts procedures contracts

end Refpoly.KS
