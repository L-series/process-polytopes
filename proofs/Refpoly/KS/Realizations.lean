import Refpoly.KS.LatticeCoordinates
import Refpoly.KS.PolytopeChecks

open Set Matrix Module

namespace Refpoly.KS

def inverseFrame {d : ℕ} (U : Matrix (Fin d) (Fin d) ℚ) :
    Matrix (Fin d) (Fin d) ℚ := U.det⁻¹ • U.adjugate

theorem inverseFrame_mul {d : ℕ} (U : Matrix (Fin d) (Fin d) ℚ)
    (hU : U.det ≠ 0) : inverseFrame U * U = 1 := by
  rw [inverseFrame, Matrix.smul_mul, Matrix.adjugate_mul, smul_smul,
    inv_mul_cancel₀ hU, one_smul]

theorem mul_inverseFrame {d : ℕ} (U : Matrix (Fin d) (Fin d) ℚ)
    (hU : U.det ≠ 0) : U * inverseFrame U = 1 := by
  rw [inverseFrame, Matrix.mul_smul, Matrix.mul_adjugate, smul_smul,
    inv_mul_cancel₀ hU, one_smul]

theorem inverseFrame_real_left {d : ℕ} (U : Matrix (Fin d) (Fin d) ℚ)
    (hU : U.det ≠ 0) :
    (inverseFrame U).map (algebraMap ℚ ℝ) * U.map (algebraMap ℚ ℝ) = 1 := by
  rw [← Matrix.map_mul, inverseFrame_mul U hU]
  exact Matrix.map_one _ (map_zero _) (map_one _)

theorem inverseFrame_real_right {d : ℕ} (U : Matrix (Fin d) (Fin d) ℚ)
    (hU : U.det ≠ 0) :
    U.map (algebraMap ℚ ℝ) * (inverseFrame U).map (algebraMap ℚ ℝ) = 1 := by
  rw [← Matrix.map_mul, mul_inverseFrame U hU]
  exact Matrix.map_one _ (map_zero _) (map_one _)

noncomputable def rationalFrameEquiv {d : ℕ} (U : Matrix (Fin d) (Fin d) ℚ)
    (hU : U.det ≠ 0) : V d ≃ₗ[ℝ] V d :=
  Matrix.toLin'OfInv (inverseFrame_real_left U hU) (inverseFrame_real_right U hU)

theorem rationalFrameEquiv_apply {d : ℕ} (U : Matrix (Fin d) (Fin d) ℚ)
    (hU : U.det ≠ 0) (x : V d) :
    rationalFrameEquiv U hU x = U.map (algebraMap ℚ ℝ) *ᵥ x := rfl

theorem rationalFrameEquiv_symm_apply {d : ℕ} (U : Matrix (Fin d) (Fin d) ℚ)
    (hU : U.det ≠ 0) (x : V d) :
    (rationalFrameEquiv U hU).symm x =
      (inverseFrame U).map (algebraMap ℚ ℝ) *ᵥ x := rfl

theorem rationalFrameEquiv_ofRat {d : ℕ} (U : Matrix (Fin d) (Fin d) ℚ)
    (hU : U.det ≠ 0) (x : Fin d → ℚ) :
    rationalFrameEquiv U hU (ofRat x) = ofRat (U *ᵥ x) := by
  ext i
  simp [rationalFrameEquiv_apply, ofRat, Matrix.mulVec, dotProduct]

theorem rationalFrameEquiv_symm_ofRat {d : ℕ} (U : Matrix (Fin d) (Fin d) ℚ)
    (hU : U.det ≠ 0) (x : Fin d → ℚ) :
    (rationalFrameEquiv U hU).symm (ofRat x) = ofRat (inverseFrame U *ᵥ x) := by
  ext i
  simp [rationalFrameEquiv_symm_apply, ofRat, Matrix.mulVec, dotProduct]

theorem rationalFrameEquiv_pairing {d : ℕ} (U : Matrix (Fin d) (Fin d) ℚ)
    (hU : U.det ≠ 0) (x : V d) (i : Fin d) :
    rationalFrameEquiv U hU x i = ofRat (U i) ⬝ᵥ x := by
  simp [rationalFrameEquiv_apply, ofRat, Matrix.mulVec, dotProduct]

theorem matrix_det_ne_zero_of_real_independent {R : Type} [CommRing R] {d : ℕ}
    (φ : R →+* ℝ) (A : Matrix (Fin d) (Fin d) R)
    (hA : LinearIndependent ℝ (A.map φ)) : A.det ≠ 0 := by
  have hd : (A.map φ).det ≠ 0 :=
    isUnit_iff_ne_zero.mp ((Matrix.isUnit_iff_isUnit_det _).mp
      (Matrix.linearIndependent_rows_iff_isUnit.mp hA))
  intro hz
  apply hd
  calc
    (A.map φ).det = φ A.det := (φ.map_det A).symm
    _ = 0 := by rw [hz, map_zero]

def primalFrames {d : ℕ} (Q : PolyCode d) : List (Matrix (Fin d) (Fin d) ℤ) :=
  (tuples (actualVerticesExact Q) d).filter fun v => decide (Matrix.det v ≠ 0)

def polarFrames {d : ℕ} (Q : PolyCode d) : List (Matrix (Fin d) (Fin d) ℚ) :=
  (tuples (polarVerticesExact Q) d).filter fun u => decide (Matrix.det u ≠ 0)

theorem mem_primalFrames {d : ℕ} (Q : PolyCode d) (v : Matrix (Fin d) (Fin d) ℤ) :
    v ∈ primalFrames Q ↔ (∀ i, v i ∈ actualVerticesExact Q) ∧ v.det ≠ 0 := by
  constructor
  · intro h
    obtain ⟨hmem, hdet⟩ := List.mem_filter.mp h
    exact ⟨(mem_tuples _ _ _).mp hmem, of_decide_eq_true hdet⟩
  · rintro ⟨hmem, hdet⟩
    exact List.mem_filter.mpr ⟨(mem_tuples _ _ _).mpr hmem, decide_eq_true hdet⟩

theorem mem_polarFrames {d : ℕ} (Q : PolyCode d) (u : Matrix (Fin d) (Fin d) ℚ) :
    u ∈ polarFrames Q ↔ (∀ i, u i ∈ polarVerticesExact Q) ∧ u.det ≠ 0 := by
  constructor
  · intro h
    obtain ⟨hmem, hdet⟩ := List.mem_filter.mp h
    exact ⟨(mem_tuples _ _ _).mp hmem, of_decide_eq_true hdet⟩
  · rintro ⟨hmem, hdet⟩
    exact List.mem_filter.mpr ⟨(mem_tuples _ _ _).mpr hmem, decide_eq_true hdet⟩

theorem primalFrames_nonempty {d : ℕ} (Q : PolyCode d) (hQ : IsIP Q.hull) :
    primalFrames Q ≠ [] := by
  classical
  have hs : Submodule.span ℝ (pointSet (actualVerticesExact Q)) = ⊤ := by
    rw [actualVerticesExact_correct Q]
    exact span_extremePoints_of_compact_ip Q.isLatticePolytope.isCompact
      Q.isLatticePolytope.convex hQ
  obtain ⟨b, hb⟩ := exists_basis_in_spanning_set _ hs
  choose v hv heq using hb
  have hm : (Matrix.of v).map (Int.castRingHom ℝ) = fun i => b i := by
    ext i j
    exact congrFun (heq i) j
  have hi : LinearIndependent ℝ ((Matrix.of v).map (Int.castRingHom ℝ)) := by
    rw [hm]
    exact b.linearIndependent
  exact List.ne_nil_of_mem ((mem_primalFrames Q v).mpr
    ⟨hv, matrix_det_ne_zero_of_real_independent (Int.castRingHom ℝ) _ hi⟩)

theorem polarFrames_nonempty {d : ℕ} (Q : PolyCode d) (hQ : IsIP Q.hull) :
    polarFrames Q ≠ [] := by
  classical
  have hc : IsCompact (polarDual Q.hull) :=
    Metric.isCompact_of_isClosed_isBounded (polarDual_isClosed _)
      (polarDual_bounded_of_ip hQ)
  have hi := polarDual_ip_of_bounded Q.isLatticePolytope.isCompact.isBounded (interior_subset hQ)
  have hs : Submodule.span ℝ (ratPointSet (polarVerticesExact Q)) = ⊤ := by
    rw [polarVerticesExact_correct Q]
    exact span_extremePoints_of_compact_ip hc (polarDual_convex _) hi
  obtain ⟨b, hb⟩ := exists_basis_in_spanning_set _ hs
  choose u hu heq using hb
  have hm : (Matrix.of u).map (algebraMap ℚ ℝ) = fun i => b i := by
    ext i j
    exact congrFun (heq i) j
  have hind : LinearIndependent ℝ ((Matrix.of u).map (algebraMap ℚ ℝ)) := by
    rw [hm]
    exact b.linearIndependent
  exact List.ne_nil_of_mem ((mem_polarFrames Q u).mpr
    ⟨hu, matrix_det_ne_zero_of_real_independent (algebraMap ℚ ℝ) _ hind⟩)

def chosenPrimalFrame {d : ℕ} (Q : PolyCode d) (hQ : IsIP Q.hull) :
    Matrix (Fin d) (Fin d) ℤ := (primalFrames Q).head (primalFrames_nonempty Q hQ)

def chosenPolarFrame {d : ℕ} (Q : PolyCode d) (hQ : IsIP Q.hull) :
    Matrix (Fin d) (Fin d) ℚ := (polarFrames Q).head (polarFrames_nonempty Q hQ)

theorem chosenPrimalFrame_spec {d : ℕ} (Q : PolyCode d) (hQ : IsIP Q.hull) :
    (∀ i, chosenPrimalFrame Q hQ i ∈ actualVerticesExact Q) ∧
      (chosenPrimalFrame Q hQ).det ≠ 0 :=
  (mem_primalFrames Q _).mp (List.head_mem _)

theorem chosenPolarFrame_spec {d : ℕ} (Q : PolyCode d) (hQ : IsIP Q.hull) :
    (∀ i, chosenPolarFrame Q hQ i ∈ polarVerticesExact Q) ∧
      (chosenPolarFrame Q hQ).det ≠ 0 :=
  (mem_polarFrames Q _).mp (List.head_mem _)

def frameCoordinates {d : ℕ} (U : Matrix (Fin d) (Fin d) ℚ) (z : IntPoint d) :
    IntPoint d := fun i => (U *ᵥ fun j => (z j : ℚ)) i |>.num

theorem frameCoordinates_exact {d : ℕ} (U : Matrix (Fin d) (Fin d) ℚ)
    (hU : U.det ≠ 0) (z : IntPoint d)
    (hz : IsLatticePoint (rationalFrameEquiv U hU (ofInt z))) :
    ofInt (frameCoordinates U z) = rationalFrameEquiv U hU (ofInt z) := by
  ext i
  obtain ⟨a, ha⟩ := hz i
  have heq : ((U *ᵥ fun j => (z j : ℚ)) i : ℝ) =
      rationalFrameEquiv U hU (ofInt z) i := by
    have h := congrFun (rationalFrameEquiv_ofRat U hU (fun j => (z j : ℚ))) i
    simpa [ofRat, ofInt] using h.symm
  have hrat : (U *ᵥ fun j => (z j : ℚ)) i = (a : ℚ) := by
    exact_mod_cast heq.trans ha
  change (((U *ᵥ fun j => (z j : ℚ)) i).num : ℝ) = _
  rw [hrat]
  simpa using ha.symm

theorem coarsest_le_finest_of_eligible {d : ℕ} (Q : PolyCode d) (hQ : Eligible Q) :
    coarsest Q.hull ≤ finest Q.hull :=
  (AddSubgroup.closure_le (finest Q.hull)).mpr hQ.2

theorem frame_finest_integral {d : ℕ} (Q : PolyCode d)
    (U : Matrix (Fin d) (Fin d) ℚ) (hU : U.det ≠ 0)
    (hu : ∀ i, U i ∈ polarVerticesExact Q) :
    ∀ x ∈ finest Q.hull, IsLatticePoint (rationalFrameEquiv U hU x) := by
  intro x hx i
  have hvertex : ofRat (U i) ∈ (polarDual Q.hull).extremePoints ℝ := by
    rw [← polarVerticesExact_correct Q]
    exact ⟨U i, hu i, rfl⟩
  obtain ⟨z, hz⟩ := hx (ofRat (U i)) hvertex
  exact ⟨z, (rationalFrameEquiv_pairing U hU x i).trans hz⟩

theorem frame_vertex_integral {d : ℕ} (Q : PolyCode d) (hQ : Eligible Q)
    (U : Matrix (Fin d) (Fin d) ℚ) (hU : U.det ≠ 0)
    (hu : ∀ i, U i ∈ polarVerticesExact Q) (v : IntPoint d)
    (hv : v ∈ actualVerticesExact Q) :
    IsLatticePoint (rationalFrameEquiv U hU (ofInt v)) := by
  apply frame_finest_integral Q U hU hu
  apply coarsest_le_finest_of_eligible Q hQ
  apply AddSubgroup.subset_closure
  rw [← actualVerticesExact_correct Q]
  exact ⟨v, hv, rfl⟩

noncomputable def realizationFrameEquiv {d : ℕ} (Q : PolyCode d) (hQ : Eligible Q) :
    V d ≃ₗ[ℝ] V d :=
  rationalFrameEquiv (chosenPolarFrame Q hQ.1) (chosenPolarFrame_spec Q hQ.1).2

def framedVertices {d : ℕ} (Q : PolyCode d) (hQ : Eligible Q) : List (IntPoint d) :=
  let U := (polarFrames Q).head (polarFrames_nonempty Q hQ.1)
  let coordinates := (actualVerticesExact Q).map fun v => Vector.ofFn (frameCoordinates U v)
  coordinates.map Vector.get

theorem framedVertices_eq {d : ℕ} (Q : PolyCode d) (hQ : Eligible Q) :
    framedVertices Q hQ =
      (actualVerticesExact Q).map (frameCoordinates (chosenPolarFrame Q hQ.1)) := by
  simp [framedVertices, chosenPolarFrame, List.map_map, Function.comp_def]
  intro v _
  ext i
  exact Vector.get_ofFn _ i

theorem framed_vertex_exact {d : ℕ} (Q : PolyCode d) (hQ : Eligible Q)
    (v : IntPoint d) (hv : v ∈ actualVerticesExact Q) :
    ofInt (frameCoordinates (chosenPolarFrame Q hQ.1) v) =
      realizationFrameEquiv Q hQ (ofInt v) :=
  frameCoordinates_exact _ _ v (frame_vertex_integral Q hQ _ _
    (chosenPolarFrame_spec Q hQ.1).1 v hv)

theorem framedVertices_points {d : ℕ} (Q : PolyCode d) (hQ : Eligible Q) :
    pointSet (framedVertices Q hQ) =
      realizationFrameEquiv Q hQ '' pointSet (actualVerticesExact Q) := by
  ext y
  constructor
  · rintro ⟨z, hz, rfl⟩
    rw [framedVertices_eq] at hz
    obtain ⟨v, hv, rfl⟩ := List.mem_map.mp hz
    exact ⟨ofInt v, ⟨v, hv, rfl⟩, (framed_vertex_exact Q hQ v hv).symm⟩
  · rintro ⟨_, ⟨v, hv, rfl⟩, rfl⟩
    exact ⟨frameCoordinates (chosenPolarFrame Q hQ.1) v,
      by rw [framedVertices_eq]; exact List.mem_map.mpr ⟨v, hv, rfl⟩,
      framed_vertex_exact Q hQ v hv⟩

theorem framedVertices_hull {d : ℕ} (Q : PolyCode d) (hQ : Eligible Q) :
    convexHull ℝ (pointSet (framedVertices Q hQ)) =
      realizationFrameEquiv Q hQ '' Q.hull := by
  rw [framedVertices_points, actualVerticesExact_hull Q]
  exact ((realizationFrameEquiv Q hQ).toLinearMap.image_convexHull _).symm

def framePrimalMatrix {d : ℕ} (Q : PolyCode d) (hQ : Eligible Q) :
    Matrix (Fin d) (Fin d) ℤ :=
  fun i => frameCoordinates (chosenPolarFrame Q hQ.1) (chosenPrimalFrame Q hQ.1 i)

theorem framePrimalMatrix_exact {d : ℕ} (Q : PolyCode d) (hQ : Eligible Q) (i : Fin d) :
    ofInt (framePrimalMatrix Q hQ i) =
      realizationFrameEquiv Q hQ (ofInt (chosenPrimalFrame Q hQ.1 i)) :=
  framed_vertex_exact Q hQ _ ((chosenPrimalFrame_spec Q hQ.1).1 i)

theorem framePrimalMatrix_det {d : ℕ} (Q : PolyCode d) (hQ : Eligible Q) :
    (framePrimalMatrix Q hQ).det ≠ 0 := by
  have hv : LinearIndependent ℝ (BoxReduction.realMatrix (chosenPrimalFrame Q hQ.1)) :=
    Matrix.linearIndependent_rows_iff_isUnit.mpr
      ((Matrix.isUnit_iff_isUnit_det _).mpr
        (isUnit_iff_ne_zero.mpr (realMatrix_det_ne_zero _
          (chosenPrimalFrame_spec Q hQ.1).2)))
  have hm : (framePrimalMatrix Q hQ).map (Int.castRingHom ℝ) =
      fun i => realizationFrameEquiv Q hQ (ofInt (chosenPrimalFrame Q hQ.1 i)) := by
    ext i j
    exact congrFun (framePrimalMatrix_exact Q hQ i) j
  apply matrix_det_ne_zero_of_real_independent (Int.castRingHom ℝ)
  rw [hm]
  exact hv.map' (realizationFrameEquiv Q hQ).toLinearMap
    (LinearMap.ker_eq_bot.mpr (realizationFrameEquiv Q hQ).injective)

def realizationModulus {d : ℕ} (Q : PolyCode d) (hQ : Eligible Q) : ℕ :=
  (framePrimalMatrix Q hQ).det.natAbs

instance realizationModulus_neZero {d : ℕ} (Q : PolyCode d) (hQ : Eligible Q) :
    NeZero (realizationModulus Q hQ) :=
  ⟨Int.natAbs_ne_zero.mpr (framePrimalMatrix_det Q hQ)⟩

noncomputable def frameLattice {d : ℕ} (Q : PolyCode d) (hQ : Eligible Q)
    (M : Submodule ℤ (IntPoint d)) : AddSubgroup (V d) :=
  (integralSubgroup M).map (realizationFrameEquiv Q hQ).symm.toLinearMap.toAddMonoidHom

theorem mem_frameLattice {d : ℕ} (Q : PolyCode d) (hQ : Eligible Q)
    (M : Submodule ℤ (IntPoint d)) (x : V d) :
    x ∈ frameLattice Q hQ M ↔
      ∃ z ∈ M, (realizationFrameEquiv Q hQ).symm (ofInt z) = x := by
  change x ∈ (M.toAddSubgroup.map (ofIntHom d)).map
    (realizationFrameEquiv Q hQ).symm.toLinearMap.toAddMonoidHom ↔ _
  simp only [AddSubgroup.mem_map]
  constructor
  · rintro ⟨_, ⟨z, hz, rfl⟩, rfl⟩
    exact ⟨z, hz, rfl⟩
  · rintro ⟨z, hz, rfl⟩
    exact ⟨ofInt z, ⟨z, hz, rfl⟩, rfl⟩

theorem mem_frameLattice_symm_ofInt {d : ℕ} (Q : PolyCode d) (hQ : Eligible Q)
    (M : Submodule ℤ (IntPoint d)) (z : IntPoint d) :
    (realizationFrameEquiv Q hQ).symm (ofInt z) ∈ frameLattice Q hQ M ↔ z ∈ M := by
  rw [mem_frameLattice]
  constructor
  · rintro ⟨w, hw, heq⟩
    have hcast := (realizationFrameEquiv Q hQ).symm.injective heq
    have hz : w = z := by
      ext i
      have hi := congrFun hcast i
      change (w i : ℝ) = (z i : ℝ) at hi
      exact_mod_cast hi
    rwa [hz] at hw
  · intro hz
    exact ⟨z, hz, rfl⟩

theorem rowSpan_framePrimalMatrix_le {d : ℕ} (Q : PolyCode d) (hQ : Eligible Q) :
    BoxReduction.rowSpan (framePrimalMatrix Q hQ) ≤
      integralImage (realizationFrameEquiv Q hQ) (coarsest Q.hull) := by
  apply Submodule.span_le.mpr
  rintro _ ⟨i, rfl⟩
  change (realizationFrameEquiv Q hQ).symm (ofInt (framePrimalMatrix Q hQ i)) ∈
    coarsest Q.hull
  rw [framePrimalMatrix_exact, LinearEquiv.symm_apply_apply]
  apply AddSubgroup.subset_closure
  rw [← actualVerticesExact_correct Q]
  exact ⟨chosenPrimalFrame Q hQ.1 i, (chosenPrimalFrame_spec Q hQ.1).1 i, rfl⟩

theorem realization_period_finest {d : ℕ} (Q : PolyCode d) (hQ : Eligible Q)
    (z : IntPoint d) (hz : residueMap d (realizationModulus Q hQ) z = 0) :
    (realizationFrameEquiv Q hQ).symm (ofInt z) ∈ finest Q.hull := by
  have hker : z ∈ LinearMap.ker (BoxReduction.modMap d (realizationModulus Q hQ)) := hz
  have hrow := BoxReduction.ker_modMap_le_rowSpan (framePrimalMatrix Q hQ) hker
  have hc := rowSpan_framePrimalMatrix_le Q hQ hrow
  exact coarsest_le_finest_of_eligible Q hQ hc

theorem integralImage_frameLattice {d : ℕ} (Q : PolyCode d) (hQ : Eligible Q)
    (L : AddSubgroup (V d)) (hL : L ≤ finest Q.hull) :
    frameLattice Q hQ (integralImage (realizationFrameEquiv Q hQ) L) = L := by
  apply AddSubgroup.ext
  intro x
  rw [mem_frameLattice]
  constructor
  · rintro ⟨z, hz, rfl⟩
    exact hz
  · intro hx
    have hint := frame_finest_integral Q (chosenPolarFrame Q hQ.1)
      (chosenPolarFrame_spec Q hQ.1).2 (chosenPolarFrame_spec Q hQ.1).1 x (hL hx)
    obtain ⟨z, hz⟩ : ∃ z : IntPoint d, ofInt z = realizationFrameEquiv Q hQ x := by
      choose z hz using hint
      exact ⟨z, funext fun i => (hz i).symm⟩
    have hback : (realizationFrameEquiv Q hQ).symm (ofInt z) = x := by
      rw [hz]
      exact LinearEquiv.symm_apply_apply _ _
    exact ⟨z, by change (realizationFrameEquiv Q hQ).symm (ofInt z) ∈ L; rwa [hback],
      hback⟩

def pullbackPairing {d : ℕ} (U : Matrix (Fin d) (Fin d) ℚ)
    (u : Fin d → ℚ) (z : IntPoint d) : ℚ :=
  u ⬝ᵥ (inverseFrame U *ᵥ fun i => (z i : ℚ))

theorem pullbackPairing_cast {d : ℕ} (U : Matrix (Fin d) (Fin d) ℚ)
    (hU : U.det ≠ 0) (u : Fin d → ℚ) (z : IntPoint d) :
    (pullbackPairing U u z : ℝ) =
      ofRat u ⬝ᵥ (rationalFrameEquiv U hU).symm (ofInt z) := by
  rw [rationalFrameEquiv_symm_apply]
  simp [pullbackPairing, ofRat, ofInt, Matrix.mulVec, dotProduct]

theorem pullbackPairings_integral_iff {d : ℕ} (Q : PolyCode d)
    (U : Matrix (Fin d) (Fin d) ℚ) (hU : U.det ≠ 0) (z : IntPoint d) :
    (rationalFrameEquiv U hU).symm (ofInt z) ∈ finest Q.hull ↔
      ∀ u ∈ polarVerticesExact Q, (pullbackPairing U u z).den = 1 := by
  constructor
  · intro hz u hu
    have hvertex : ofRat u ∈ (polarDual Q.hull).extremePoints ℝ := by
      rw [← polarVerticesExact_correct Q]
      exact ⟨u, hu, rfl⟩
    obtain ⟨a, ha⟩ := hz (ofRat u) hvertex
    exact (rat_den_one_iff_real_integral _).mpr
      ⟨a, (pullbackPairing_cast U hU u z).trans ha⟩
  · intro hz u hu
    rw [← polarVerticesExact_correct Q] at hu
    obtain ⟨q, hq, rfl⟩ := hu
    obtain ⟨a, ha⟩ := (rat_den_one_iff_real_integral _).mp (hz q hq)
    exact ⟨a, (pullbackPairing_cast U hU q z).symm.trans ha⟩

def CompatibleResidues {d : ℕ} (Q : PolyCode d) (hQ : Eligible Q)
    (S : List (ResiduePoint d (realizationModulus Q hQ))) : Prop :=
  (∀ z ∈ framedVertices Q hQ, residueMap d (realizationModulus Q hQ) z ∈ S) ∧
    ∀ s ∈ S, ∀ u ∈ polarVerticesExact Q,
      (pullbackPairing (chosenPolarFrame Q hQ.1) u (residueLift s)).den = 1

instance {d : ℕ} (Q : PolyCode d) (hQ : Eligible Q)
    (S : List (ResiduePoint d (realizationModulus Q hQ))) :
    Decidable (CompatibleResidues Q hQ S) := by
  unfold CompatibleResidues
  infer_instance

theorem coarsest_le_frameLattice_iff {d : ℕ} (Q : PolyCode d) (hQ : Eligible Q)
    (S : List (ResiduePoint d (realizationModulus Q hQ))) (hS : ValidResidues S) :
    coarsest Q.hull ≤ frameLattice Q hQ (congruenceSpace S hS) ↔
      ∀ z ∈ framedVertices Q hQ, residueMap d (realizationModulus Q hQ) z ∈ S := by
  constructor
  · intro hL z hz
    rw [framedVertices_eq] at hz
    obtain ⟨v, hv, rfl⟩ := List.mem_map.mp hz
    apply (mem_congruenceSpace S hS _).mp
    apply (mem_frameLattice_symm_ofInt Q hQ _ _).mp
    rw [framed_vertex_exact Q hQ v hv, LinearEquiv.symm_apply_apply]
    apply hL
    apply AddSubgroup.subset_closure
    rw [← actualVerticesExact_correct Q]
    exact ⟨v, hv, rfl⟩
  · intro hL
    apply (AddSubgroup.closure_le _).mpr
    intro v hv
    rw [← actualVerticesExact_correct Q] at hv
    obtain ⟨z, hz, rfl⟩ := hv
    apply (mem_frameLattice Q hQ _ _).mpr
    refine ⟨frameCoordinates (chosenPolarFrame Q hQ.1) z,
      (mem_congruenceSpace S hS _).mpr
        (hL _ (by rw [framedVertices_eq]; exact List.mem_map.mpr ⟨z, hz, rfl⟩)), ?_⟩
    rw [framed_vertex_exact Q hQ z hz, LinearEquiv.symm_apply_apply]

theorem frameLattice_le_finest_iff {d : ℕ} (Q : PolyCode d) (hQ : Eligible Q)
    (S : List (ResiduePoint d (realizationModulus Q hQ))) (hS : ValidResidues S) :
    frameLattice Q hQ (congruenceSpace S hS) ≤ finest Q.hull ↔
      ∀ s ∈ S, ∀ u ∈ polarVerticesExact Q,
        (pullbackPairing (chosenPolarFrame Q hQ.1) u (residueLift s)).den = 1 := by
  constructor
  · intro hL s hs
    apply (pullbackPairings_integral_iff Q _ _ _).mp
    apply hL
    apply (mem_frameLattice_symm_ofInt Q hQ _ _).mpr
    apply (mem_congruenceSpace S hS _).mpr
    rwa [residueMap_lift]
  · intro hL x hx
    obtain ⟨z, hz, rfl⟩ := (mem_frameLattice Q hQ _ x).mp hx
    let s := residueMap d (realizationModulus Q hQ) z
    have hs : s ∈ S := (mem_congruenceSpace S hS _).mp hz
    have hlift : (realizationFrameEquiv Q hQ).symm (ofInt (residueLift s)) ∈
        finest Q.hull :=
      (pullbackPairings_integral_iff Q _ _ _).mpr (hL s hs)
    have hzero : residueMap d (realizationModulus Q hQ) (z - residueLift s) = 0 := by
      rw [map_sub, residueMap_lift]
      exact sub_self _
    have hperiod := realization_period_finest Q hQ _ hzero
    have heq : (realizationFrameEquiv Q hQ).symm (ofInt z) =
        (realizationFrameEquiv Q hQ).symm (ofInt (z - residueLift s)) +
          (realizationFrameEquiv Q hQ).symm (ofInt (residueLift s)) := by
      rw [← map_add]
      congr 1
      ext i
      simp [ofInt]
    rw [heq]
    exact (finest Q.hull).add_mem hperiod hlift

theorem compatibleResidues_iff {d : ℕ} (Q : PolyCode d) (hQ : Eligible Q)
    (S : List (ResiduePoint d (realizationModulus Q hQ))) (hS : ValidResidues S) :
    CompatibleResidues Q hQ S ↔ Compatible Q (frameLattice Q hQ (congruenceSpace S hS)) :=
  and_congr (coarsest_le_frameLattice_iff Q hQ S hS).symm
    (frameLattice_le_finest_iff Q hQ S hS).symm

theorem reflexive_realization_of_eligible {d : ℕ} (Q R : PolyCode d)
    (hQ : Eligible Q) (e : V d ≃ₗ[ℝ] V d) (he : e '' R.hull = Q.hull)
    (hL : Compatible Q (imageLattice e)) : IsReflexive R.hull := by
  classical
  have hIP : IsIP R.hull := by
    have h : IsIP (e.symm '' Q.hull) := by
      exact e.symm.toContinuousLinearEquiv.toHomeomorph.isOpenMap.image_interior_subset _
        ⟨0, hQ.1, map_zero e.symm⟩
    have heq : e.symm '' (e '' R.hull) = R.hull := by
      simp [Set.image_image]
    rwa [← he, heq] at h
  have hdual : polarDual R.hull = dualTransport e '' polarDual Q.hull := by
    rw [← he]
    exact (dualTransport_polar e R.hull).symm
  have hfinite : (dualTransport e '' ratPointSet (polarVerticesExact Q)).Finite :=
    ((polarVerticesExact Q).finite_toSet.image ofRat).image (dualTransport e)
  have hlat : ∀ u ∈ dualTransport e '' ratPointSet (polarVerticesExact Q),
      IsLatticePoint u := by
    rintro _ ⟨_, ⟨q, hq, rfl⟩, rfl⟩ i
    have hvertex : ofRat q ∈ (polarDual Q.hull).extremePoints ℝ := by
      rw [← polarVerticesExact_correct Q]
      exact ⟨q, hq, rfl⟩
    have hbasis : IsLatticePoint (Pi.single i (1 : ℝ) : V d) := by
      intro j
      by_cases h : i = j
      · exact ⟨1, by simp [Pi.single_apply, h]⟩
      · exact ⟨0, by simp [h]⟩
    have heL : e (Pi.single i (1 : ℝ)) ∈ imageLattice e :=
      AddSubgroup.mem_map.mpr ⟨Pi.single i 1, hbasis, rfl⟩
    obtain ⟨z, hz⟩ := hL.2 heL (ofRat q) hvertex
    refine ⟨z, ?_⟩
    have h := (dualTransport_pairing e (ofRat q) (Pi.single i 1)).trans hz
    simpa using h
  refine ⟨R.isLatticePolytope, hIP, hfinite.toFinset, ?_, ?_⟩
  · intro u hu
    exact hlat u (hfinite.mem_toFinset.mp hu)
  · rw [hfinite.coe_toFinset, hdual, polarVerticesExact_hull Q hQ.1]
    exact (dualTransport e).toLinearMap.image_convexHull _

def realizationResidues {d : ℕ} (Q : PolyCode d) (hQ : Eligible Q) :
    List {S : List (ResiduePoint d (realizationModulus Q hQ)) //
      S ∈ residueGroups d (realizationModulus Q hQ)} :=
  (residueGroups d (realizationModulus Q hQ)).attach.filter
    fun S => decide (CompatibleResidues Q hQ S.1)

theorem mem_realizationResidues {d : ℕ} (Q : PolyCode d) (hQ : Eligible Q)
    (S : {S : List (ResiduePoint d (realizationModulus Q hQ)) //
      S ∈ residueGroups d (realizationModulus Q hQ)}) :
    S ∈ realizationResidues Q hQ ↔ CompatibleResidues Q hQ S.1 := by
  simp [realizationResidues]

def realizationCode {d : ℕ} (Q : PolyCode d) (hQ : Eligible Q)
    (S : List (ResiduePoint d (realizationModulus Q hQ))) (hS : ValidResidues S) :
    PolyCode d :=
  let basis := congruenceBasisPair S hS
  latticeCoordinateCode basis.1 (framedVertices Q hQ)

noncomputable def realizationMap {d : ℕ} (Q : PolyCode d) (hQ : Eligible Q)
    (S : List (ResiduePoint d (realizationModulus Q hQ))) (hS : ValidResidues S) :
    V d ≃ₗ[ℝ] V d :=
  (integerMatrixEquiv (congruenceBasis S hS) (congruenceBasis_correct S hS).1).trans
    (realizationFrameEquiv Q hQ).symm

theorem imageLattice_trans {d : ℕ} (f g : V d ≃ₗ[ℝ] V d) :
    imageLattice (f.trans g) = (imageLattice f).map g.toLinearMap.toAddMonoidHom := by
  unfold imageLattice
  rw [AddSubgroup.map_map]
  rfl

theorem realizationMap_imageLattice {d : ℕ} (Q : PolyCode d) (hQ : Eligible Q)
    (S : List (ResiduePoint d (realizationModulus Q hQ))) (hS : ValidResidues S) :
    imageLattice (realizationMap Q hQ S hS) =
      frameLattice Q hQ (congruenceSpace S hS) := by
  unfold realizationMap frameLattice
  rw [imageLattice_trans, integerMatrixEquiv_imageLattice _ _
    (congruenceSpace S hS) (congruenceBasis_correct S hS).2]

theorem realizationCode_hull {d : ℕ} (Q : PolyCode d) (hQ : Eligible Q)
    (S : List (ResiduePoint d (realizationModulus Q hQ))) (hS : ValidResidues S)
    (hC : CompatibleResidues Q hQ S) :
    realizationMap Q hQ S hS '' (realizationCode Q hQ S hS).hull = Q.hull := by
  have hp : ∀ z ∈ framedVertices Q hQ,
      ∃ x : IntPoint d, congruenceBasis S hS *ᵥ x = z := by
    intro z hz
    exact ((congruenceBasis_correct S hS).2 z).mp
      ((mem_congruenceSpace S hS z).mpr (hC.1 z hz))
  calc
    realizationMap Q hQ S hS '' (realizationCode Q hQ S hS).hull =
        (realizationFrameEquiv Q hQ).symm ''
          (integerMatrixEquiv (congruenceBasis S hS) (congruenceBasis_correct S hS).1 ''
            (latticeCoordinateCode (congruenceBasis S hS) (framedVertices Q hQ)).hull) := by
      rw [Set.image_image]
      rfl
    _ = (realizationFrameEquiv Q hQ).symm ''
        convexHull ℝ (pointSet (framedVertices Q hQ)) := by
      rw [latticeCoordinateCode_image _ _ _ hp]
    _ = (realizationFrameEquiv Q hQ).symm ''
        (realizationFrameEquiv Q hQ '' Q.hull) := by rw [framedVertices_hull]
    _ = Q.hull := by simp [Set.image_image]

theorem realizationCode_reflexive {d : ℕ} (Q : PolyCode d) (hQ : Eligible Q)
    (S : List (ResiduePoint d (realizationModulus Q hQ))) (hS : ValidResidues S)
    (hC : CompatibleResidues Q hQ S) :
    IsReflexive (realizationCode Q hQ S hS).hull :=
  reflexive_realization_of_eligible Q _ hQ (realizationMap Q hQ S hS)
    (realizationCode_hull Q hQ S hS hC)
    (by rw [realizationMap_imageLattice]; exact (compatibleResidues_iff Q hQ S hS).mp hC)

def latticeRealizationsExact {d : ℕ} (Q : PolyCode d) : List (PolyCode d) :=
  if hc : eligibleCheckExact Q = true then
    let hQ := (eligibleCheckExact_correct Q).mp hc
    (realizationResidues Q hQ).map fun S =>
      realizationCode Q hQ S.1 ((mem_residueGroups _).mp S.2).2
  else []

theorem latticeRealizationsExact_of_ineligible {d : ℕ} (Q : PolyCode d)
    (hQ : ¬Eligible Q) : latticeRealizationsExact Q = [] := by
  have hc : eligibleCheckExact Q ≠ true := fun h => hQ ((eligibleCheckExact_correct Q).mp h)
  simp [latticeRealizationsExact, hc]

theorem latticeRealizationsExact_sound {d : ℕ} (Q : PolyCode d) (hQ : Eligible Q)
    (R : PolyCode d) (hR : R ∈ latticeRealizationsExact Q) :
    IsReflexive R.hull ∧ ∃ e : V d ≃ₗ[ℝ] V d,
      e '' R.hull = Q.hull ∧ Compatible Q (imageLattice e) := by
  have hc := (eligibleCheckExact_correct Q).mpr hQ
  simp only [latticeRealizationsExact, dif_pos hc] at hR
  obtain ⟨S, hmem, rfl⟩ := List.mem_map.mp hR
  have hS := ((mem_residueGroups _).mp S.2).2
  have hC := (mem_realizationResidues Q hQ S).mp hmem
  exact ⟨realizationCode_reflexive Q hQ S.1 hS hC,
    realizationMap Q hQ S.1 hS, realizationCode_hull Q hQ S.1 hS hC,
    by rw [realizationMap_imageLattice]; exact (compatibleResidues_iff Q hQ S.1 hS).mp hC⟩

theorem latticeRealizationsExact_complete {d : ℕ} (Q : PolyCode d) (hQ : Eligible Q)
    (L : AddSubgroup (V d)) (hL : Compatible Q L) :
    ∃ R ∈ latticeRealizationsExact Q, ∃ e : V d ≃ₗ[ℝ] V d,
      e '' R.hull = Q.hull ∧ imageLattice e = L := by
  classical
  have hrow : BoxReduction.rowSpan (framePrimalMatrix Q hQ) ≤
      integralImage (realizationFrameEquiv Q hQ) L := by
    intro z hz
    exact hL.1 (rowSpan_framePrimalMatrix_le Q hQ hz)
  obtain ⟨S, hmem, hS, heq⟩ :=
    integer_overgroup_residues (framePrimalMatrix Q hQ) (framePrimalMatrix_det Q hQ)
      (integralImage (realizationFrameEquiv Q hQ) L) hrow
  have hlat : frameLattice Q hQ (congruenceSpace S hS) = L := by
    rw [heq]
    exact integralImage_frameLattice Q hQ L hL.2
  have hC : CompatibleResidues Q hQ S := by
    apply (compatibleResidues_iff Q hQ S hS).mpr
    exact hlat.symm ▸ hL
  let a : {S : List (ResiduePoint d (realizationModulus Q hQ)) //
      S ∈ residueGroups d (realizationModulus Q hQ)} := ⟨S, hmem⟩
  refine ⟨realizationCode Q hQ S hS, ?_, realizationMap Q hQ S hS,
    realizationCode_hull Q hQ S hS hC, ?_⟩
  · have hc := (eligibleCheckExact_correct Q).mpr hQ
    simp only [latticeRealizationsExact, dif_pos hc]
    exact List.mem_map.mpr ⟨a, (mem_realizationResidues Q hQ a).mpr hC, rfl⟩
  · exact (realizationMap_imageLattice Q hQ S hS).trans hlat

end Refpoly.KS
