/-
WeightSystemFinite faithfully ported from the reviewed refpoly-5d snapshot.
See SOURCES.md for attribution and exact source identities.
-/
import Refpoly.Algorithm

/-!
# Finiteness from weight-system enumeration

A selection rule choosing strictly positive feasible weights has finite
branching on every node that can lead to an IP weight system. Completeness
and the depth bound put all normalized IP weights in a finite image. This
proof does not depend on Hensley or on the sharp dimension-five degree bound.
-/

open Set Matrix Module
namespace Refpoly
namespace WeightSystemFinite
variable {n : ℕ}

def PositiveFeasible (S : Finset (V n)) : Prop :=
  ∃ q ∈ qPolytope S, ∀ i, 0 < q i

noncomputable def positiveSelection (n : ℕ) : SelectionRule n := by
  classical
  let f (S : Finset (V n)) : V n :=
    if h : PositiveFeasible S then h.choose
    else if h : (qPolytope S).Nonempty then h.choose else 0
  refine ⟨f, fun S hS => ?_⟩
  dsimp [f]
  split_ifs with hp
  · exact hp.choose_spec.1
  · exact hS.choose_spec

lemma positiveSelection_pos {S : Finset (V n)} (h : PositiveFeasible S) :
    ∀ i, 0 < (positiveSelection n).sel S i := by
  classical
  simpa [positiveSelection, h] using h.choose_spec.2

lemma positiveFeasible_of_insert {S : Finset (V n)} {x : V n}
    (h : PositiveFeasible (insert x S)) : PositiveFeasible S := by
  obtain ⟨q, hq, hp⟩ := h
  exact ⟨q, ⟨hq.1, hq.2.1, fun y hy => hq.2.2 y (Finset.mem_insert_of_mem hy)⟩, hp⟩

noncomputable def nodes (n : ℕ) : ℕ → Set (Finset (V n))
  | 0 => {{ones n}}
  | k + 1 => nodes n k ∪ ⋃ (S ∈ nodes n k) (_ : PositiveFeasible S),
      (fun x => insert x S) '' {x : V n | IsLatticePoint x ∧
        (∀ i, 0 ≤ x i) ∧ x ⬝ᵥ (positiveSelection n).sel S < 1}

lemma nodes_finite (n k : ℕ) : (nodes n k).Finite := by
  classical
  induction k with
  | zero => exact Set.finite_singleton _
  | succ k ih =>
    apply ih.union
    apply ih.biUnion
    intro S hS
    apply Set.finite_iUnion
    intro hp
    exact (branch_finite (positiveSelection_pos hp)).image _

lemma nodes_mono (n : ℕ) : Monotone (nodes n) := by
  apply monotone_nat_of_le_succ
  intro k
  exact Set.subset_union_left

lemma reachable_mem_nodes {S : Finset (V n)} (hr : Reachable (positiveSelection n) S)
    (hp : PositiveFeasible S) : S ∈ nodes n (S.card - 1) := by
  classical
  induction hr with
  | base => simp [nodes]
  | @step S x hr hlat hx0 hxlt ih =>
    have hpos := positiveFeasible_of_insert hp
    have hS := ih hpos
    have hfeas := (positiveSelection n).mem S ⟨hpos.choose, hpos.choose_spec.1⟩
    have hxnot : x ∉ S := by
      intro hx
      have heq := hfeas.2.2 x hx
      linarith
    have hSne : S.Nonempty := by
      induction hr with
      | base => exact Finset.singleton_nonempty _
      | step => exact Finset.insert_nonempty _ _
    have hcard : (insert x S).card - 1 = (S.card - 1) + 1 := by
      rw [Finset.card_insert_of_notMem hxnot]
      have := Finset.card_pos.mpr hSne
      omega
    rw [hcard]
    apply Set.mem_union_right
    exact Set.mem_iUnion₂.mpr ⟨S, hS, Set.mem_iUnion.mpr
      ⟨hpos, ⟨x, ⟨hlat, hx0, hxlt⟩, rfl⟩⟩⟩

/-- Normalized IP weight systems form a finite set, without any volume bound. -/
theorem normalized_ip_weight_systems_finite (n : ℕ) :
    {q : V n | (∑ i, q i) = 1 ∧ IsIPWeightSystem q}.Finite := by
  classical
  apply ((nodes_finite n n).image (positiveSelection n).sel).subset
  rintro q ⟨hsum, hIP⟩
  obtain ⟨S, hr, heq, hcard⟩ := algorithm_complete (positiveSelection n) hIP.1 hsum hIP
  have hnonempty : (qPolytope S).Nonempty := by
    by_contra hn
    have hp : ¬ PositiveFeasible S := fun h => hn ⟨h.choose, h.choose_spec.1⟩
    have hz : (positiveSelection n).sel S = 0 := by simp [positiveSelection, hp, hn]
    have hqzero : q = 0 := heq.symm.trans hz
    simp [hqzero] at hsum
  have hfeas : PositiveFeasible S :=
    ⟨q, heq ▸ (positiveSelection n).mem S hnonempty, hIP.1⟩
  refine ⟨S, ?_, heq⟩
  exact nodes_mono n (by omega : S.card - 1 ≤ n) (reachable_mem_nodes hr hfeas)
end WeightSystemFinite
end Refpoly
