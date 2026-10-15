/-
MaxMin faithfully ported from the reviewed refpoly-5d snapshot.
See SOURCES.md for attribution and exact source identities.
These are existence and polarity results, not executable seed enumeration.
-/
import Refpoly.Finiteness
import Refpoly.ReflexiveDuality

open Set Matrix

namespace Refpoly

variable {n : ℕ}

/-! ## Duality reverses inclusion (L3.2) -/

/-- **L3.2.**  For closed convex `Q ∋ 0`, inclusion of polytopes is equivalent
to the reversed inclusion of their duals.  [SS18] §2.2: "duality inverts
subset relations, Δ ⊆ Δ̃ ⇔ Δ* ⊇ Δ̃*". -/
theorem subset_iff_polarDual_subset {P Q : Set (V n)} (hQconv : Convex ℝ Q)
    (hQcl : IsClosed Q) (hQ0 : (0 : V n) ∈ Q) :
    P ⊆ Q ↔ polarDual Q ⊆ polarDual P := by
  constructor
  · exact polarDual_antitone
  · intro h
    calc P ⊆ polarDual (polarDual P) := subset_bipolar P
      _ ⊆ polarDual (polarDual Q) := polarDual_antitone h
      _ = Q := bipolar hQconv hQcl hQ0

/-! ## Maximal and minimal reflexive polytopes (D2.1, D3.1) -/

/-- **D2.1.**  A reflexive polytope is *maximal* if it is not properly
contained in another reflexive polytope on the same lattice. -/
def IsMaximalReflexive (P : Set (V n)) : Prop :=
  IsReflexive P ∧ ∀ Q, IsReflexive Q → P ⊆ Q → Q = P

/-- A reflexive polytope is *minimal* (order-dual of D2.1) if it contains no
proper reflexive subpolytope.  (This is the order-theoretic notion; for the
vertex-economical notion `IsMinimalIPGen` see below and Part 4.) -/
def IsMinimalReflexive (P : Set (V n)) : Prop :=
  IsReflexive P ∧ ∀ Q, IsReflexive Q → Q ⊆ P → Q = P

/-! ## Every reflexive polytope embeds in a maximal one (T2.2) -/

/-- **T2.2.**  Every reflexive polytope is contained in a *maximal* reflexive
polytope.  Proof: among reflexive polytopes containing `P`, the lattice point
count takes values in a finite range (T1.2); pick one realizing the maximal
count; strict inclusion would force a strictly larger count (L1.5). -/
theorem exists_maximal_superset {P : Set (V n)} (hP : IsReflexive P) :
    ∃ Δ : Set (V n), P ⊆ Δ ∧ IsMaximalReflexive Δ := by
  obtain ⟨N, hN⟩ := latticePoint_count_bound n
  set A : Set ℕ :=
    {m | ∃ Q : Set (V n), IsReflexive Q ∧ P ⊆ Q ∧ (Q ∩ latticePoints n).ncard = m}
    with hA
  have hAne : A.Nonempty := ⟨_, P, hP, subset_rfl, rfl⟩
  have hAbdd : BddAbove A := by
    refine ⟨N, ?_⟩
    rintro m ⟨Q, hQ, -, rfl⟩
    exact hN Q hQ
  obtain ⟨Δ, hΔ, hPΔ, hcard⟩ : sSup A ∈ A := Nat.sSup_mem hAne hAbdd
  refine ⟨Δ, hPΔ, hΔ, fun Q hQ hΔQ => ?_⟩
  by_contra hne
  have hss : Δ ⊂ Q := hΔQ.ssubset_of_ne (fun h => hne h.symm)
  have hlt : (Δ ∩ latticePoints n).ncard < (Q ∩ latticePoints n).ncard :=
    ncard_lt_of_ssubset hΔ.latticePolytope hQ.latticePolytope hss
  have hmem : (Q ∩ latticePoints n).ncard ∈ A :=
    ⟨Q, hQ, hPΔ.trans hΔQ, rfl⟩
  have hle : (Q ∩ latticePoints n).ncard ≤ sSup A := le_csSup hAbdd hmem
  omega

/-! ## Max/min duality (T3.4) -/

/-- **T3.4.**  A reflexive polytope is maximal iff its polar dual is minimal.
This is the strategic pivot of the classification ([SS18] §2.2): instead of
hunting for the big maximal polytopes one classifies the small minimal ones
and dualizes. -/
theorem maximal_iff_dual_minimal {P : Set (V n)} (hP : IsReflexive P) :
    IsMaximalReflexive P ↔ IsMinimalReflexive (polarDual P) := by
  constructor
  · rintro ⟨-, hmax⟩
    refine ⟨hP.polarDual_reflexive, fun Q hQ hQsub => ?_⟩
    -- Q ⊆ P* ⇒ P = P** ⊆ Q*, so by maximality Q* = P, hence Q = Q** = P*.
    have h1 : P ⊆ polarDual Q := by
      rw [← hP.bipolar_eq]
      exact polarDual_antitone hQsub
    have h2 : polarDual Q = P := hmax _ hQ.polarDual_reflexive h1
    calc Q = polarDual (polarDual Q) := hQ.bipolar_eq.symm
      _ = polarDual P := by rw [h2]
  · rintro ⟨-, hmin⟩
    refine ⟨hP, fun Q hQ hPQ => ?_⟩
    have h1 : polarDual Q ⊆ polarDual P := polarDual_antitone hPQ
    have h2 : polarDual Q = polarDual P := hmin _ hQ.polarDual_reflexive h1
    calc Q = polarDual (polarDual Q) := hQ.bipolar_eq.symm
      _ = polarDual (polarDual P) := by rw [h2]
      _ = P := hP.bipolar_eq

/-- Dual form of T2.2: every reflexive polytope *contains* a minimal reflexive
polytope.  [SS18] §2.2: "every reflexive polytope must then contain at least
one of Δ₁*, Δ₂*, …". -/
theorem exists_minimal_subset {P : Set (V n)} (hP : IsReflexive P) :
    ∃ Nb : Set (V n), Nb ⊆ P ∧ IsMinimalReflexive Nb := by
  obtain ⟨Δ, hsub, hmax⟩ := exists_maximal_superset hP.polarDual_reflexive
  refine ⟨polarDual Δ, ?_, ?_⟩
  · calc polarDual Δ ⊆ polarDual (polarDual P) := polarDual_antitone hsub
      _ = P := hP.bipolar_eq
  · exact (maximal_iff_dual_minimal hmax.1).1 hmax

/-! ## Minimal IP generating sets (T3.5)

The bridge to Part 4: the *vertex-economical* notion of minimality, which is
the one classified by weight systems.  [SS18] §2.2: "a minimal polytope
∇ ⊂ N_ℝ [is] a polytope that has the IP property, whereas the convex hull of
any proper subset of the set of its vertices fails to have it."  We work
with finite generating sets instead of vertex sets; an inclusion-minimal
generating set consists exactly of the vertices. -/

/-- **D3.1 (vertex-economical minimality).**  A finite set `S` is a *minimal
IP generating set* if its hull contains the origin in its interior while the
hull of every proper subset does not. -/
def IsMinimalIPGen (S : Finset (V n)) : Prop :=
  ((0 : V n) ∈ interior (convexHull ℝ (S : Set (V n)))) ∧
    ∀ S' ⊂ S, (0 : V n) ∉ interior (convexHull ℝ (S' : Set (V n)))

/-- **T3.5.**  Every finite IP generating set contains a minimal IP generating
subset.  (Finite descent; this is how [KS95] §2 passes from the vertex set of
`Q*` to a minimal polytope `M`.) -/
theorem exists_minimalIPGen_subset (S : Finset (V n))
    (h : (0 : V n) ∈ interior (convexHull ℝ (S : Set (V n)))) :
    ∃ S' ⊆ S, IsMinimalIPGen S' := by
  classical
  induction S using Finset.strongInduction with
  | _ S ih =>
    by_cases hall : ∀ S' ⊂ S, (0 : V n) ∉ interior (convexHull ℝ (S' : Set (V n)))
    · exact ⟨S, subset_rfl, h, hall⟩
    · push Not at hall
      obtain ⟨S', hS', hint⟩ := hall
      obtain ⟨S'', hsub, hmin⟩ := ih S' hS' hint
      exact ⟨S'', hsub.trans hS'.subset, hmin⟩

end Refpoly
