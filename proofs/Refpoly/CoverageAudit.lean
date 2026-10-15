/-
CoverageAudit faithfully ported from the reviewed refpoly-5d snapshot.
See SOURCES.md for attribution and exact source identities.
-/
import Refpoly.GeometricCWS
import Refpoly.WeightSystemFinite
import Refpoly.PyramidLemma
import Refpoly.ReflexiveVolume
import Refpoly.Minimal
import Refpoly.DegreeBound
import Refpoly.Sylvester
import Refpoly.CWSContainer
import Refpoly.CWSAssembly

/-! Axiom regression checks: these results must remain independent of the
uncertified numerical degree claims. -/

/-- info: 'Refpoly.pyramid_lemma_dim_le_four' depends on axioms: [propext, Classical.choice, Quot.sound] -/
#guard_msgs (whitespace := lax) in
#print axioms Refpoly.pyramid_lemma_dim_le_four

/-- info: 'Refpoly.WeightSystemFinite.normalized_ip_weight_systems_finite' depends on axioms: [propext, Classical.choice, Quot.sound] -/
#guard_msgs (whitespace := lax) in
#print axioms Refpoly.WeightSystemFinite.normalized_ip_weight_systems_finite

/-- info: 'Refpoly.simplex_relation_kernel' depends on axioms: [propext, Classical.choice, Quot.sound] -/
#guard_msgs (whitespace := lax) in
#print axioms Refpoly.simplex_relation_kernel

/-- info: 'Refpoly.simplex_pairing_surjective' depends on axioms: [propext, Classical.choice, Quot.sound] -/
#guard_msgs (whitespace := lax) in
#print axioms Refpoly.simplex_pairing_surjective

/-- info: 'Refpoly.reflexive_simplex_block_ip' depends on axioms: [propext, Classical.choice, Quot.sound] -/
#guard_msgs (whitespace := lax) in
#print axioms Refpoly.reflexive_simplex_block_ip

/-- info: 'Refpoly.algorithm_reaches_simplex_block' depends on axioms: [propext, Classical.choice, Quot.sound] -/
#guard_msgs (whitespace := lax) in
#print axioms Refpoly.algorithm_reaches_simplex_block

/-- info: 'Refpoly.IsMinimalIPGen.isMinimalIPRel' depends on axioms: [propext, Classical.choice, Quot.sound] -/
#guard_msgs (whitespace := lax) in
#print axioms Refpoly.IsMinimalIPGen.isMinimalIPRel

/-- info: 'Refpoly.positive_circuit_through' depends on axioms: [propext, Classical.choice, Quot.sound] -/
#guard_msgs (whitespace := lax) in
#print axioms Refpoly.positive_circuit_through

/-- info: 'Refpoly.ip_positive_circuit_through' depends on axioms: [propext, Classical.choice, Quot.sound] -/
#guard_msgs (whitespace := lax) in
#print axioms Refpoly.ip_positive_circuit_through

/-- info: 'Refpoly.ReflexiveVolume.volume_bound' depends on axioms: [propext, Classical.choice, Quot.sound] -/
#guard_msgs (whitespace := lax) in
#print axioms Refpoly.ReflexiveVolume.volume_bound

/-- info: 'Refpoly.hensley_volume_bound' depends on axioms: [propext, Classical.choice, Quot.sound] -/
#guard_msgs (whitespace := lax) in
#print axioms Refpoly.hensley_volume_bound

/-- info: 'Refpoly.positive_relation_intrinsicInterior' depends on axioms: [propext, Classical.choice, Quot.sound] -/
#guard_msgs (whitespace := lax) in
#print axioms Refpoly.positive_relation_intrinsicInterior

/-- info: 'Refpoly.finset_positive_relation_intrinsicInterior' depends on axioms: [propext, Classical.choice, Quot.sound] -/
#guard_msgs (whitespace := lax) in
#print axioms Refpoly.finset_positive_relation_intrinsicInterior

/-- info: 'Refpoly.exists_goodSimplex_through' depends on axioms: [propext, Classical.choice, Quot.sound] -/
#guard_msgs (whitespace := lax) in
#print axioms Refpoly.exists_goodSimplex_through

/-- info: 'Refpoly.exists_proper_goodSimplex_of_not_affineIndependent' depends on axioms: [propext, Classical.choice, Quot.sound] -/
#guard_msgs (whitespace := lax) in
#print axioms Refpoly.exists_proper_goodSimplex_of_not_affineIndependent

/-- info: 'Refpoly.proper_relativeIP_dim_lt' depends on axioms: [propext, Classical.choice, Quot.sound] -/
#guard_msgs (whitespace := lax) in
#print axioms Refpoly.proper_relativeIP_dim_lt

/-- info: 'Refpoly.exists_maximal_dim_proper_relativeIP' depends on axioms: [propext, Classical.choice, Quot.sound] -/
#guard_msgs (whitespace := lax) in
#print axioms Refpoly.exists_maximal_dim_proper_relativeIP

/-- info: 'Refpoly.exists_maximal_superset' depends on axioms: [propext, Classical.choice, Quot.sound] -/
#guard_msgs (whitespace := lax) in
#print axioms Refpoly.exists_maximal_superset

/-- info: 'Refpoly.minimal_structure' depends on axioms: [propext, Classical.choice, Quot.sound] -/
#guard_msgs (whitespace := lax) in
#print axioms Refpoly.minimal_structure

/-- info: 'Refpoly.ip_half_reduction' depends on axioms: [propext, Classical.choice, Quot.sound] -/
#guard_msgs (whitespace := lax) in
#print axioms Refpoly.ip_half_reduction

/-- info: 'Refpoly.isIPWeightSystem_smul_iff' depends on axioms: [propext, Classical.choice, Quot.sound] -/
#guard_msgs (whitespace := lax) in
#print axioms Refpoly.isIPWeightSystem_smul_iff

/-- info: 'Refpoly.normalizedWeights_injective_on_primitive' depends on axioms: [propext, Classical.choice, Quot.sound] -/
#guard_msgs (whitespace := lax) in
#print axioms Refpoly.normalizedWeights_injective_on_primitive

/-- info: 'Refpoly.primitive_ip_weight_systems_finite' depends on axioms: [propext, Classical.choice, Quot.sound] -/
#guard_msgs (whitespace := lax) in
#print axioms Refpoly.primitive_ip_weight_systems_finite

/-- info: 'Refpoly.primitive_ip_degrees_bounded' depends on axioms: [propext, Classical.choice, Quot.sound] -/
#guard_msgs (whitespace := lax) in
#print axioms Refpoly.primitive_ip_degrees_bounded

/-- info: 'Refpoly.max_degree_dim5' depends on axioms: [propext, Classical.choice, Quot.sound] -/
#guard_msgs (whitespace := lax) in
#print axioms Refpoly.max_degree_dim5

/-- info: 'Refpoly.normalized_relation_mem_span_circuits' depends on axioms: [propext, Classical.choice, Quot.sound] -/
#guard_msgs (whitespace := lax) in
#print axioms Refpoly.normalized_relation_mem_span_circuits

/-- info: 'Refpoly.simplex_circuits_span_kernel' depends on axioms: [propext, Classical.choice, Quot.sound] -/
#guard_msgs (whitespace := lax) in
#print axioms Refpoly.simplex_circuits_span_kernel

/-- info: 'Refpoly.exists_circuitBasis' depends on axioms: [propext, Classical.choice, Quot.sound] -/
#guard_msgs (whitespace := lax) in
#print axioms Refpoly.exists_circuitBasis

/-- info: 'Refpoly.CircuitBasis.pairing_range' depends on axioms: [propext, Classical.choice, Quot.sound] -/
#guard_msgs (whitespace := lax) in
#print axioms Refpoly.CircuitBasis.pairing_range

/-- info: 'Refpoly.CircuitBasis.reflexive_container' depends on axioms: [propext, Classical.choice, Quot.sound] -/
#guard_msgs (whitespace := lax) in
#print axioms Refpoly.CircuitBasis.reflexive_container

/-- info: 'Refpoly.CircuitBasis.blocks_reached' depends on axioms: [propext, Classical.choice, Quot.sound] -/
#guard_msgs (whitespace := lax) in
#print axioms Refpoly.CircuitBasis.blocks_reached

/-- info: 'Refpoly.algorithm_enumerates_cws_container' depends on axioms: [propext, Classical.choice, Quot.sound] -/
#guard_msgs (whitespace := lax) in
#print axioms Refpoly.algorithm_enumerates_cws_container

/-- info: 'Refpoly.CircuitBasis.rows_reached' depends on axioms: [propext, Classical.choice, Quot.sound] -/
#guard_msgs (whitespace := lax) in
#print axioms Refpoly.CircuitBasis.rows_reached

/-- info: 'Refpoly.ws_cws_container_coverage' depends on axioms: [propext, Classical.choice, Quot.sound] -/
#guard_msgs (whitespace := lax) in
#print axioms Refpoly.ws_cws_container_coverage

/-- info: 'Refpoly.CircuitBasis.coordinate_count' depends on axioms: [propext, Classical.choice, Quot.sound] -/
#guard_msgs (whitespace := lax) in
#print axioms Refpoly.CircuitBasis.coordinate_count

/-- info: 'Refpoly.candidateRows_finite' depends on axioms: [propext, Classical.choice, Quot.sound] -/
#guard_msgs (whitespace := lax) in
#print axioms Refpoly.candidateRows_finite

/-- info: 'Refpoly.candidateCWS_finite' depends on axioms: [propext, Classical.choice, Quot.sound] -/
#guard_msgs (whitespace := lax) in
#print axioms Refpoly.candidateCWS_finite

/-- info: 'Refpoly.CWS.RowsReached.mem_catalogue' depends on axioms: [propext, Classical.choice, Quot.sound] -/
#guard_msgs (whitespace := lax) in
#print axioms Refpoly.CWS.RowsReached.mem_catalogue

/-- info: 'Refpoly.ws_cws_catalogue_coverage' depends on axioms: [propext, Classical.choice, Quot.sound] -/
#guard_msgs (whitespace := lax) in
#print axioms Refpoly.ws_cws_catalogue_coverage

/-- info: 'Refpoly.finite_ws_cws_container_coverage' depends on axioms: [propext, Classical.choice, Quot.sound] -/
#guard_msgs (whitespace := lax) in
#print axioms Refpoly.finite_ws_cws_container_coverage

/-- info: 'Refpoly.descent_lemma' depends on axioms: [propext, Classical.choice, Quot.sound] -/
#guard_msgs (whitespace := lax) in
#print axioms Refpoly.descent_lemma

/-- info: 'Refpoly.algorithm_complete' depends on axioms: [propext, Classical.choice, Quot.sound] -/
#guard_msgs (whitespace := lax) in
#print axioms Refpoly.algorithm_complete

/-- info: 'Refpoly.classification_coverage' depends on axioms: [propext, Classical.choice, Quot.sound] -/
#guard_msgs (whitespace := lax) in
#print axioms Refpoly.classification_coverage

/-- info: 'Refpoly.reflexive_simplex_weight_system' depends on axioms: [propext, Classical.choice, Quot.sound] -/
#guard_msgs (whitespace := lax) in
#print axioms Refpoly.reflexive_simplex_weight_system

/-- info: 'Refpoly.algorithm_enumerates_reflexive' depends on axioms: [propext, Classical.choice, Quot.sound] -/
#guard_msgs (whitespace := lax) in
#print axioms Refpoly.algorithm_enumerates_reflexive

/-- info: 'Refpoly.maximal_reflexive_covered_by_minimalIP_container' depends on axioms: [propext, Classical.choice, Quot.sound] -/
#guard_msgs (whitespace := lax) in
#print axioms Refpoly.maximal_reflexive_covered_by_minimalIP_container

/-- info: 'Refpoly.caratheodory_step' depends on axioms: [propext, Classical.choice, Quot.sound] -/
#guard_msgs (whitespace := lax) in
#print axioms Refpoly.caratheodory_step
