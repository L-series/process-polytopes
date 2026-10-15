import Refpoly.KS.Contracts

open Set Matrix

namespace Refpoly.KS

theorem mem_imageLattice_iff {d : ℕ} (f : V d ≃ₗ[ℝ] V d) (y : V d) :
    y ∈ imageLattice f ↔ IsLatticePoint (f.symm y) := by
  change (∃ x, IsLatticePoint x ∧ f x = y) ↔ _
  constructor
  · rintro ⟨x, hx, rfl⟩
    simpa using hx
  · intro hy
    exact ⟨f.symm y, hy, f.apply_symm_apply y⟩

theorem lattice_extremePoint {d : ℕ} {P : Set (V d)} (hP : IsLatticePolytope P)
    {x : V d} (hx : x ∈ P.extremePoints ℝ) : IsLatticePoint x := by
  obtain ⟨S, hS, rfl⟩ := hP
  exact hS x (extremePoints_convexHull_subset hx)

theorem subpolytopeRecovery_proof : SubpolytopeRecovery := by
  classical
  intro d xs P hP f hf hpoints
  obtain ⟨S, hS, rfl⟩ := hP
  let ys := xs.filter fun z => ofInt z ∈ f '' (S : Set (V d))
  have hset : pointSet ys = f '' (S : Set (V d)) := by
    ext y
    constructor
    · rintro ⟨z, hz, rfl⟩
      exact of_decide_eq_true (List.mem_filter.mp hz).2
    · rintro ⟨x, hx, rfl⟩
      have hp := hpoints ⟨mem_image_of_mem f (subset_convexHull ℝ _ hx), hf x (hS x hx)⟩
      obtain ⟨z, hz, heq⟩ := hp
      refine ⟨z, List.mem_filter.mpr ⟨hz, ?_⟩, heq⟩
      rw [heq]
      exact decide_eq_true (mem_image_of_mem f hx)
  refine ⟨⟨ys⟩, ?_, ?_⟩
  · exact List.mem_map.mpr ⟨ys, List.mem_sublists.mpr List.filter_sublist, rfl⟩
  · change convexHull ℝ (pointSet ys) = f '' convexHull ℝ (S : Set (V d))
    rw [hset]
    exact (f.toLinearMap.image_convexHull _).symm

noncomputable def dualTransport {d : ℕ} (f : V d ≃ₗ[ℝ] V d) : V d ≃ₗ[ℝ] V d :=
  (dotProductEquiv ℝ (Fin d)).trans
    (f.dualMap.trans (dotProductEquiv ℝ (Fin d)).symm)

theorem dualTransport_pairing {d : ℕ} (f : V d ≃ₗ[ℝ] V d) (y x : V d) :
    dualTransport f y ⬝ᵥ x = y ⬝ᵥ f x := by
  let E := dotProductEquiv ℝ (Fin d)
  change E (E.symm (f.dualMap (E y))) x = _
  rw [E.apply_symm_apply]
  rfl

theorem dualTransport_polar {d : ℕ} (f : V d ≃ₗ[ℝ] V d) (P : Set (V d)) :
    dualTransport f '' polarDual (f '' P) = polarDual P := by
  ext y
  constructor
  · rintro ⟨u, hu, rfl⟩ x hx
    rw [dualTransport_pairing]
    exact hu (f x) (mem_image_of_mem f hx)
  · intro hy
    refine ⟨(dualTransport f).symm y, ?_, (dualTransport f).apply_symm_apply y⟩
    rintro z ⟨x, hx, rfl⟩
    rw [← dualTransport_pairing, (dualTransport f).apply_symm_apply]
    exact hy x hx

theorem transported_dual_vertex_integral {d : ℕ} {P : Set (V d)}
    (hP : IsReflexive P) (f : V d ≃ₗ[ℝ] V d) {u : V d}
    (hu : u ∈ (polarDual (f '' P)).extremePoints ℝ) :
    IsLatticePoint (dualTransport f u) := by
  apply lattice_extremePoint hP.dual_latticePolytope
  rw [← dualTransport_polar f P, ← image_extremePoints (dualTransport f)]
  exact mem_image_of_mem (dualTransport f) hu

theorem imageEligible_proof : ImageEligible := by
  intro d P hP f _ Q hQ
  rw [Eligible, hQ]
  constructor
  · exact (LinearMap.isOpenMap_of_finiteDimensional f.toLinearMap f.surjective).image_interior_subset
      P ⟨0, hP.ip, map_zero f⟩
  · intro v hv u hu
    rw [← image_extremePoints f] at hv
    obtain ⟨x, hx, rfl⟩ := hv
    rw [← dualTransport_pairing]
    exact (transported_dual_vertex_integral hP f hu).dotProduct_int
      (lattice_extremePoint hP.latticePolytope hx)

theorem targetLattice_proof : TargetLattice := by
  intro d P hP f _ Q hQ
  rw [Compatible, hQ]
  constructor
  · apply (AddSubgroup.closure_le (imageLattice f)).mpr
    intro y hy
    rw [← image_extremePoints f] at hy
    obtain ⟨x, hx, rfl⟩ := hy
    exact (mem_imageLattice_iff f (f x)).mpr (by
      simpa using lattice_extremePoint hP.latticePolytope hx)
  · intro y hy u hu
    have hlat := (mem_imageLattice_iff f y).mp hy
    obtain ⟨z, hz⟩ := (transported_dual_vertex_integral hP f hu).dotProduct_int hlat
    refine ⟨z, ?_⟩
    rw [← f.apply_symm_apply y, ← dualTransport_pairing]
    exact hz

theorem realizationEquivalence_proof : RealizationEquivalence := by
  intro d P R f e hhull hlattice
  let g := f.trans e.symm
  have hgforward : ∀ x, IsLatticePoint x → IsLatticePoint (g x) := by
    intro x hx
    have hmem : f x ∈ imageLattice e := by
      rw [hlattice]
      exact (mem_imageLattice_iff f (f x)).mpr (by simpa using hx)
    exact (mem_imageLattice_iff e (f x)).mp hmem
  have hgbackward : ∀ x, IsLatticePoint x → IsLatticePoint (g.symm x) := by
    intro x hx
    have hmem : e x ∈ imageLattice f := by
      rw [← hlattice]
      exact (mem_imageLattice_iff e (e x)).mpr (by simpa using hx)
    exact (mem_imageLattice_iff f (e x)).mp hmem
  refine ⟨g, ⟨hgforward, hgbackward⟩, ?_⟩
  change (e.symm ∘ f) '' P = R.hull
  rw [image_comp, ← hhull, ← image_comp]
  simp

end Refpoly.KS
