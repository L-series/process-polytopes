import Refpoly.Classification
import Refpoly.KS.Model

open Set
namespace Refpoly.KS

/-- Index a finite geometric seed; this proof object is not a scientific output. -/
noncomputable def seedOfFinset {d : ℕ} (T : Finset (V d)) : Seed d :=
  ⟨T.card, fun i => (T.equivFin.symm i : V d)⟩

theorem seedOfFinset_injective {d : ℕ} (T : Finset (V d)) :
    Function.Injective (seedOfFinset T).vertices :=
  Subtype.val_injective.comp T.equivFin.symm.injective

theorem seedOfFinset_range {d : ℕ} (T : Finset (V d)) :
    range (seedOfFinset T).vertices = (T : Set (V d)) := by
  ext x
  constructor
  · rintro ⟨i, rfl⟩
    exact (T.equivFin.symm i).property
  · intro hx
    exact ⟨T.equivFin ⟨x, hx⟩, by simp [seedOfFinset]⟩

/-- Exact finite-set/indexed minimality transport; the upper bound is explicit. -/
theorem validSeed_of_minimal {d : ℕ} {P : Set (V d)} (T : Finset (V d))
    (hlat : ∀ x ∈ T, IsLatticePoint x) (hdual : (T : Set (V d)) ⊆ polarDual P)
    (hmin : IsMinimalIPGen T) (hupper : T.card ≤ 2 * d) :
    ValidSeed P (seedOfFinset T) := by
  classical
  have hi := seedOfFinset_injective T
  refine ⟨hi, ?_, ?_, ?_, minimalIPGen_card_lower hmin.1, hupper⟩
  · intro i
    exact ⟨hlat _ (T.equivFin.symm i).property, hdual (T.equivFin.symm i).property⟩
  · rw [seedOfFinset_range]
    exact hmin.1
  · intro U hU hIP
    let T' := U.image (seedOfFinset T).vertices
    have hsub : T' ⊆ T := by
      intro x hx
      obtain ⟨i, _, rfl⟩ := Finset.mem_image.mp hx
      exact (T.equivFin.symm i).property
    have hproper : T' ⊂ T := by
      refine Finset.ssubset_iff_subset_ne.mpr ⟨hsub, ?_⟩
      intro heq
      apply hU
      apply Finset.eq_univ_of_forall
      intro i
      have hx : (seedOfFinset T).vertices i ∈ T' := by
        rw [heq]
        exact (T.equivFin.symm i).property
      obtain ⟨j, hj, hji⟩ := Finset.mem_image.mp hx
      exact hi hji ▸ hj
    apply hmin.2 T' hproper
    have hset : (T' : Set (V d)) = (seedOfFinset T).vertices '' (U : Set (Fin (seedOfFinset T).N)) := by
      exact Finset.coe_image
    rwa [hset]

/-- The full seed contract follows for a minimal dual simplex in positive dimension. -/
theorem validSeed_of_minimal_simplex {d : ℕ} (hd : 0 < d) {P : Set (V d)}
    (T : Finset (V d)) (hlat : ∀ x ∈ T, IsLatticePoint x)
    (hdual : (T : Set (V d)) ⊆ polarDual P) (hmin : IsMinimalIPGen T)
    (hind : AffineIndependent ℝ ((↑) : T → V d)) : ValidSeed P (seedOfFinset T) := by
  apply validSeed_of_minimal T hlat hdual hmin
  have hc := simplex_card hind hmin.1
  omega

/-- Every reflexive target has a minimal indexed dual seed with a dimension lower bound.
The new upper bound remains a separate obligation; no algorithm consumes the target. -/
theorem exists_indexed_minimal_seed {d : ℕ} {P : Set (V d)} (hP : IsReflexive P) :
    ∃ T : Finset (V d), (∀ x ∈ T, IsLatticePoint x) ∧
      (T : Set (V d)) ⊆ polarDual P ∧ IsMinimalIPGen T ∧
      Function.Injective (seedOfFinset T).vertices ∧ d + 1 ≤ (seedOfFinset T).N := by
  obtain ⟨T, hlat, hdual, hmin, _⟩ := classification_coverage hP
  exact ⟨T, hlat, hdual, hmin, seedOfFinset_injective T, minimalIPGen_card_lower hmin.1⟩

end Refpoly.KS
