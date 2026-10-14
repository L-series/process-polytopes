import Refpoly.KS.Contracts
import Refpoly.KS.Pipeline

open Set

namespace Refpoly.KS

theorem originCode_hull (d : ℕ) : (originCode d).hull = {0} := by
  have hcast : ofInt (0 : IntPoint d) = (0 : V d) := by
    funext i
    simp [ofInt]
  simp [PolyCode.hull, originCode, pointSet, hcast]

theorem reflexive_zero : IsReflexive (originCode 0).hull := by
  have hV : ∀ x : V 0, x = 0 := fun x => funext fun i => Fin.elim0 i
  have hset : ({0} : Set (V 0)) = univ := by ext x; simp [hV x]
  have hlat : IsLatticePolytope ({0} : Set (V 0)) := by
    refine ⟨{0}, ?_, ?_⟩
    · intro x hx
      have : x = 0 := Finset.mem_singleton.1 hx
      subst x
      exact isLatticePoint_zero
    · rw [Finset.coe_singleton, convexHull_singleton]
  rw [originCode_hull]
  refine ⟨hlat, ?_, ?_⟩
  · change (0 : V 0) ∈ interior ({0} : Set (V 0))
    rw [hset, interior_univ]
    exact mem_univ _
  · have hpolar : polarDual ({0} : Set (V 0)) = {0} := by
      ext x
      simp [polarDual, hV x]
    rw [hpolar]
    exact hlat

theorem reflexive_zero_unique {P : Set (V 0)} (hP : IsReflexive P) :
    P = (originCode 0).hull := by
  rw [originCode_hull]
  refine Subset.antisymm (fun x _ => ?_) ?_
  · have : x = 0 := funext fun i => Fin.elim0 i
    simpa using this
  · rintro x rfl
    exact interior_subset hP.ip

theorem classifies_of_contracts (p : Procedures) (h : Contracts p) : Classifies p := by
  intro d
  by_cases hd : d = 0
  · subst d
    constructor
    · intro R hR
      have : R = originCode 0 := by simpa [enumerate] using hR
      simpa [this] using reflexive_zero
    · intro P hP
      refine ⟨originCode 0, by simp [enumerate], ?_⟩
      rw [reflexive_zero_unique hP]
      exact GLEquiv.refl _
  · have hdpos : 0 < d := Nat.pos_of_ne_zero hd
    constructor
    · intro R hR
      have hfilter := List.mem_filter.1 (show R ∈ (candidates p d).filter p.reflexive by
        simpa [enumerate, hd] using hR)
      exact (h.reflexivityCheck d R).1 hfilter.2
    · intro P hP
      obtain ⟨S, hS⟩ := h.seedBound d hdpos P hP
      obtain ⟨B, hC, hB⟩ := h.circuitBasis d hdpos P hP S hS
      have hrows : GeneratedRows p.weights B.code := by
        intro a
        obtain ⟨m, hm, e, w, hw, hrow, -, -⟩ := hB.2 a
        exact ⟨m, hm, e, w, h.weightsComplete m w hw, hrow⟩
      have hmemC := h.assemblyComplete d B.code hC hrows
      have hvalid := (h.assemblySound h.weightsSound d B.code hmemC).1
      obtain ⟨f, hf, hcontain⟩ := h.geometricContainers d hdpos P hP S hS B
        hvalid hB (h.kernelBases d B.code hvalid)
      have hpoints : f '' P ∩ latticePoints d ⊆ pointSet (p.points B.code) := by
        rw [h.containerPoints d B.code hvalid]
        exact inter_subset_inter_left _ hcontain
      obtain ⟨Q, hQ, hQeq⟩ := h.subpolytopeRecovery d (p.points B.code) P
        hP.latticePolytope f hf hpoints
      have hEligible := h.imageEligible d P hP f hf Q hQeq
      have hCheck := (h.eligibilityCheck d Q).2 hEligible
      obtain ⟨R, hR, e, he, hL⟩ := h.realizationsComplete d Q hEligible
        (h.finiteLatticeInterval d Q hEligible) (imageLattice f)
        (h.targetLattice d P hP f hf Q hQeq)
      have hGL : GLEquiv P R.hull := h.realizationEquivalence d P R f e
        (he.trans hQeq) hL
      have hReflexive : IsReflexive R.hull :=
        (h.realizationsSound d Q hEligible R hR).1
      refine ⟨R, ?_, hGL⟩
      simp only [enumerate, hd, ↓reduceIte, List.mem_filter]
      refine ⟨?_, (h.reflexivityCheck d R).2 hReflexive⟩
      simp only [candidates, List.mem_flatMap]
      refine ⟨B.code, hmemC, Q, hQ, ?_⟩
      simpa [hCheck] using hR

end Refpoly.KS
