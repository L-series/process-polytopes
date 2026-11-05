import Refpoly.MaxMin

/-!
Order-theoretic certificates for maximal-only searches, in one fixed lattice.
These results do not verify lattice enumeration, facet branching or PALP.
-/

open Set

namespace Refpoly

variable {d : ℕ}

def CoversReflexiveIn (Q : Set (V d)) (outputs : Set (Set (V d))) : Prop :=
  ∀ P, IsReflexive P → P ⊆ Q → ∃ R ∈ outputs, IsReflexive R ∧ P ⊆ R

theorem reflexive_stop_covers {Q : Set (V d)} (hQ : IsReflexive Q) :
    CoversReflexiveIn Q {Q} := by
  intro P _ hPQ
  exact ⟨Q, by simp, hQ, hPQ⟩

theorem non_ip_contains_no_reflexive {Q : Set (V d)} (hQ : ¬ IsIP Q) :
    CoversReflexiveIn Q ∅ := by
  intro P hP hPQ
  exact (hQ (interior_mono hPQ hP.ip)).elim

theorem branch_covers {Q : Set (V d)} {children : Set (Set (V d))}
    {outputs : Set (Set (V d))}
    (hbranch : ∀ P, IsReflexive P → P ⊆ Q → ∃ C ∈ children, P ⊆ C)
    (hchildren : ∀ C ∈ children, CoversReflexiveIn C outputs) :
    CoversReflexiveIn Q outputs := by
  intro P hP hPQ
  obtain ⟨C, hC, hPC⟩ := hbranch P hP hPQ
  exact hchildren C hC P hP hPC

theorem maximal_in_cover_occurs {Q P : Set (V d)} {outputs : Set (Set (V d))}
    (hcover : CoversReflexiveIn Q outputs) (hP : IsMaximalReflexive P)
    (hPQ : P ⊆ Q) : P ∈ outputs := by
  obtain ⟨R, hR, hRref, hPR⟩ := hcover P hP.1 hPQ
  have heq : R = P := hP.2 R hRref hPR
  rwa [heq] at hR

theorem no_reflexive_superset_of_polar_latticeHull {Q : Set (V d)}
    (hQ : ∀ R, IsReflexive R → R ⊆ latticeHull (polarDual Q) → False) :
    ∀ P, IsReflexive P → Q ⊆ P → False := by
  intro P hP hQP
  apply hQ (polarDual P) hP.polarDual_reflexive
  calc
    polarDual P = latticeHull (polarDual P) :=
      hP.polarDual_reflexive.latticePolytope.eq_latticeHull
    _ ⊆ latticeHull (polarDual Q) := latticeHull_mono (polarDual_antitone hQP)

theorem complete_cover_maximal_iff {outputs : Set (Set (V d))}
    (hcover : ∀ P, IsReflexive P → ∃ R ∈ outputs, IsReflexive R ∧ P ⊆ R)
    {P : Set (V d)} :
    IsMaximalReflexive P ↔
      P ∈ outputs ∧ IsReflexive P ∧
        ∀ R ∈ outputs, IsReflexive R → P ⊆ R → R = P := by
  constructor
  · intro hP
    obtain ⟨R, hR, hRref, hPR⟩ := hcover P hP.1
    have heq : R = P := hP.2 R hRref hPR
    exact ⟨heq ▸ hR, hP.1, fun R _ hRref hPR => hP.2 R hRref hPR⟩
  · rintro ⟨_, hP, hmax⟩
    refine ⟨hP, fun Q hQ hPQ => ?_⟩
    obtain ⟨R, hR, hRref, hQR⟩ := hcover Q hQ
    have heq : R = P := hmax R hR hRref (hPQ.trans hQR)
    exact Subset.antisymm (heq ▸ hQR) hPQ

end Refpoly
