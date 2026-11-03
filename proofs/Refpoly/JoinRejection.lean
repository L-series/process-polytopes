import Refpoly.CWSContainer

/-! Sound join rejection in every dimension. Interiors are in the CWS joint
kernel, exactly as in `CWS.IsIP`. A functional constant on that kernel must
remain inconclusive: both strict sides are absent. -/
namespace Refpoly.JoinRejection
open Set

section Functional
variable {E : Type*} [NormedAddCommGroup E] [NormedSpace ℝ E]


/-- A one-sided, nonconstant linear functional excludes origin interior. -/
theorem not_ip_of_nonnegative (S : Set E) (l : E →ₗ[ℝ] ℝ)
    (hn : ∀ x ∈ S, 0 ≤ l x) (hp : ∃ x ∈ S, 0 < l x) :
    (0 : E) ∉ interior (convexHull ℝ S) := by
  obtain ⟨x, hx, hpos⟩ := hp
  have hsurj : Function.Surjective l := by
    intro a
    refine ⟨(a / l x) • x, ?_⟩
    simp [hpos.ne']
  have hsub : convexHull ℝ S ⊆ l ⁻¹' Ici 0 :=
    convexHull_min hn ((convex_Ici (0 : ℝ)).linear_preimage l)
  have himage : l '' convexHull ℝ S ⊆ Ici 0 := by
    rintro _ ⟨y, hy, rfl⟩
    exact hsub hy
  intro h
  have hi := (LinearMap.isOpenMap_of_finiteDimensional l hsurj).image_interior_subset
    (convexHull ℝ S) ⟨0, h, map_zero l⟩
  have := interior_mono himage hi
  simp at this

/-- IP forces both strict sides to occur, or neither. -/
theorem functional_balance (S : Set E) (h : (0 : E) ∈ interior (convexHull ℝ S))
    (l : E →ₗ[ℝ] ℝ) :
    (∃ x ∈ S, l x < 0) ↔ (∃ x ∈ S, 0 < l x) := by
  constructor
  · intro hn
    by_contra hp
    push Not at hp
    apply not_ip_of_nonnegative S (-l) (fun x hx => by simpa using neg_nonneg.mpr (hp x hx))
      (by obtain ⟨x, hx, hl⟩ := hn; exact ⟨x, hx, by simpa using neg_pos.mpr hl⟩) h
  · intro hp
    by_contra hn
    push Not at hn
    exact not_ip_of_nonnegative S l hn hp h
/-- Fewer than `d+1` generators cannot have ambient interior in dimension `d`.
Multiplicity in the input family is harmless for this lower bound. -/
theorem point_count_bound [FiniteDimensional ℝ E] {m : ℕ} (p : Fin m → E)
    (h : (0 : E) ∈ interior (convexHull ℝ (Set.range p))) :
    Module.finrank ℝ E + 1 ≤ m := by
  have hm : 0 < m := by
    by_contra hn
    have hz : m = 0 := by omega
    subst m
    simp at h
  letI : Nonempty (Fin m) := ⟨⟨0, hm⟩⟩
  have ha := affineSpan_eq_top_of_nonempty_interior ⟨0, h⟩
  have hv : vectorSpan ℝ (Set.range p) = ⊤ := by
    rw [← direction_affineSpan, ha, AffineSubspace.direction_top]
  have hc := finrank_vectorSpan_range_add_one_le ℝ p
  rw [hv, finrank_top, Fintype.card_fin] at hc
  exact hc

end Functional

/-- The exact generator set used by the classification's CWS IP predicate. -/
def generators {r n : ℕ} (C : CWS r n) : Set C.subspace :=
  {y | IsLatticePoint (y : V n) ∧ ∀ i, -1 ≤ (y : V n) i}

/-- All valid CWS types, with any number of rows and coordinates. -/
theorem cws_functional_balance {r n : ℕ} (C : CWS r n) (hip : C.IsIP)
    (l : C.subspace →ₗ[ℝ] ℝ) :
    (∃ y ∈ generators C, l y < 0) ↔ (∃ y ∈ generators C, 0 < l y) :=
  functional_balance (generators C) hip l

/-- `X_i = y_i+1` compares against one; pair sums compare against two. -/
def coordinate {r n : ℕ} (C : CWS r n) (i : Fin n) : C.subspace →ₗ[ℝ] ℝ :=
  (LinearMap.proj i).comp C.subspace.subtype

theorem cws_coordinate_balance {r n : ℕ} (C : CWS r n) (hip : C.IsIP) (i : Fin n) :
    (∃ y ∈ generators C, (y : V n) i < 0) ↔
      (∃ y ∈ generators C, 0 < (y : V n) i) :=
  cws_functional_balance C hip (coordinate C i)

theorem cws_pair_balance {r n : ℕ} (C : CWS r n) (hip : C.IsIP) (i j : Fin n) :
    (∃ y ∈ generators C, (y : V n) i + (y : V n) j < 0) ↔
      (∃ y ∈ generators C, 0 < (y : V n) i + (y : V n) j) :=
  cws_functional_balance C hip (coordinate C i + coordinate C j)

end Refpoly.JoinRejection
