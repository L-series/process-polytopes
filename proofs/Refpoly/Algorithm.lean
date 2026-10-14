/-
Algorithm faithfully ported from the reviewed refpoly-5d snapshot.
See SOURCES.md for attribution and exact source identities.
This is abstract reachability, not executable enumeration or a certified census.
Branch finiteness requires positive candidates; historical prose below supplies
no verification of PALP or its IP test.
-/
import Refpoly.WeightSystem

open Set Matrix Module

namespace Refpoly

variable {n : ℕ}

/-! ## The q-space polytope and selection rules -/

/-- **D7.1**: the feasible region in `q`-space after choosing the points `S`
([SS18] eq. (qpol)), in the normalization `Σᵢ pᵢ = 1`. -/
def qPolytope (S : Finset (V n)) : Set (V n) :=
  {p | (∀ i, 0 ≤ p i) ∧ (∑ i, p i) = 1 ∧ ∀ x ∈ S, x ⬝ᵥ p = 1}

/-- A *selection rule*: any way of choosing a candidate weight system from a
nonempty feasible region.  The [SS18] implementation takes the average of
the vertices of `Q_S` (cf. `average_pos`); completeness holds for every
rule. -/
structure SelectionRule (n : ℕ) where
  sel : Finset (V n) → V n
  mem : ∀ S : Finset (V n), (qPolytope S).Nonempty → sel S ∈ qPolytope S

/-- The search tree of the algorithm ([SS18] §3): the root is `{(1,…,1)}`;
from a node `S` one may pass to `insert x S` for any nonnegative lattice
point with `x ⬝ᵥ q̃ < 1`, where `q̃ = R.sel S` is the current candidate. -/
inductive Reachable (R : SelectionRule n) : Finset (V n) → Prop
  | base : Reachable R {ones n}
  | step {S : Finset (V n)} {x : V n} : Reachable R S → IsLatticePoint x →
      (∀ i, 0 ≤ x i) → x ⬝ᵥ R.sel S < 1 → Reachable R (insert x S)

/-! ## L7.2: positivity of the vertex average -/

/-- **L7.2.**  If among finitely many nonnegative vectors (the vertices of
the q-space polytope) every coordinate is positive on at least one of them,
then their average is strictly positive in every coordinate.  This is why
the [SS18] implementation's vertex-average candidate `q̃` has a strictly
positive entry wherever the feasible region allows one — which in turn makes
the branching of step 3 finite (`branch_finite`). -/
theorem average_pos {ι : Type*} [Fintype ι] [Nonempty ι] (v : ι → V n)
    (hnn : ∀ a i, 0 ≤ v a i) (hpos : ∀ i, ∃ a, 0 < v a i) (i : Fin n) :
    0 < ((Fintype.card ι : ℝ)⁻¹ • ∑ a, v a) i := by
  have hsum : 0 < (∑ a, v a) i := by
    rw [Finset.sum_apply]
    obtain ⟨a, ha⟩ := hpos i
    exact Finset.sum_pos' (fun b _ => hnn b i) ⟨a, Finset.mem_univ a, ha⟩
  have hcard : (0 : ℝ) < (Fintype.card ι : ℝ) := by
    exact_mod_cast Fintype.card_pos
  simp only [Pi.smul_apply, smul_eq_mul]
  positivity

/-! ## L7.3: finiteness of the branching set -/

/-- **L7.3.**  For a strictly positive candidate `p`, the branching set
`{x ∈ ℤⁿ_{≥0} : x ⬝ᵥ p < 1}` of step 3 is finite.  (Each coordinate is
bounded: `pᵢxᵢ ≤ x·p < 1`.) -/
theorem branch_finite {p : V n} (hp : ∀ i, 0 < p i) :
    {x : V n | IsLatticePoint x ∧ (∀ i, 0 ≤ x i) ∧ x ⬝ᵥ p < 1}.Finite := by
  set B := {x : V n | IsLatticePoint x ∧ (∀ i, 0 ≤ x i) ∧ x ⬝ᵥ p < 1} with hB
  set R : ℝ := ∑ i, 1 / p i with hR
  have hbdd : Bornology.IsBounded B := by
    apply Bornology.IsBounded.subset
      (Metric.isBounded_closedBall (x := (0 : V n)) (r := R))
    rintro x ⟨-, hx0, hxp⟩
    rw [Metric.mem_closedBall, dist_zero_right]
    have hcoord : ∀ i, x i ≤ R := by
      intro i
      -- pᵢxᵢ ≤ Σⱼ pⱼxⱼ = x·p < 1, hence xᵢ < 1/pᵢ ≤ R
      have hterm : x i * p i ≤ x ⬝ᵥ p :=
        Finset.single_le_sum (fun j _ => mul_nonneg (hx0 j) (hp j).le) (Finset.mem_univ i)
      have hxi : x i < 1 / p i := by
        rw [lt_div_iff₀ (hp i)]
        linarith
      have hone : 1 / p i ≤ R :=
        Finset.single_le_sum (fun j _ => (one_div_pos.2 (hp j)).le) (Finset.mem_univ i)
      linarith
    have hRnn : 0 ≤ R := by
      rw [hR]
      exact Finset.sum_nonneg fun i _ => (div_pos one_pos (hp i)).le
    rw [pi_norm_le_iff_of_nonneg hRnn]
    intro i
    rw [Real.norm_eq_abs, abs_of_nonneg (hx0 i)]
    exact hcoord i
  apply (finite_latticePoints_of_isBounded hbdd).subset
  rintro x hx
  exact ⟨hx, hx.1⟩

/-! ## L7.4: the descent lemma -/

/-- Helper: membership in `wsPoints` from a depth-1 generator. -/
theorem add_ones_mem_wsPoints {q : V n} {y : wsHyperplane q}
    (hy : y ∈ wsLatticeGensAt q 1) : (y : V n) + ones n ∈ wsPoints q := by
  obtain ⟨hlat, hge⟩ := hy
  refine ⟨?_, ?_, ?_⟩
  · intro i
    obtain ⟨m, hm⟩ := hlat i
    refine ⟨m + 1, ?_⟩
    simp only [Pi.add_apply, ones, hm]
    push_cast
    ring
  · intro i
    have := hge i
    simp only [Pi.add_apply, ones]
    linarith
  · have h0 : q ⬝ᵥ (y : V n) = 0 := mem_wsHyperplane.1 y.2
    rw [add_dotProduct, dotProduct_comm (y : V n) q, h0, zero_add,
      dotProduct_comm (ones n) q, dotProduct_ones]

/-- **L7.4 (descent lemma — the heart of the algorithm's completeness).**

Let `q` be a normalized IP weight system and `p ≠ q` any normalized
vector satisfying the constraints accumulated so far (in
particular `(1,…,1) ⬝ᵥ p = Σp = 1`, the constraint from `x⁽⁰⁾`).  Then `Δ_q`
contains a lattice point `x` with `x ⬝ᵥ p < 1`.

[SS18] §3: "note that x⁽⁰⁾ (which satisfies x⁽⁰⁾·q̃ = 1) cannot be interior
to Δ_q for q ≠ q̃ unless Δ_q contains points satisfying x·q̃ < 1."

Proof: if all lattice points of `Δ_q` had `x ⬝ᵥ p ≥ 1`, then in `y`-space
the functional `y ↦ p ⬝ᵥ y` would be `≥ 0` on `Δ_q` and `= 0` at the origin,
i.e. minimized at an interior point; by `eq_zero_of_le_on_interior` it would
vanish on all of `W_q = ker(q ⬝ᵥ ·)`.  A functional vanishing on the kernel
of another is proportional to it (`exists_smul_of_ker_le_ker`), so `p = c·q`;
evaluating at `(1,…,1)` gives `c = 1`, contradicting `p ≠ q`. -/
theorem descent_lemma {q p : V n} (hqsum : ∑ i, q i = 1)
    (hIP : IsIPWeightSystem q) (hpsum : ∑ i, p i = 1)
    (hne : p ≠ q) :
    ∃ x ∈ wsPoints q, x ⬝ᵥ p < 1 := by
  by_contra hcon
  push Not at hcon
  -- the functional y ↦ p ⬝ᵥ y on the hyperplane W_q
  set f : wsHyperplane q →L[ℝ] ℝ :=
    ((dotLin p).toContinuousLinearMap).comp (wsHyperplane q).subtypeL with hf
  have hf_apply : ∀ y : wsHyperplane q, f y = p ⬝ᵥ (y : V n) := by
    intro y
    simp [hf, dotLin]
  -- f ≥ 0 on the generators of Δ_q …
  have hgen : ∀ y ∈ wsLatticeGensAt q 1, (0 : ℝ) ≤ f y := by
    intro y hy
    have hx := hcon ((y : V n) + ones n) (add_ones_mem_wsPoints hy)
    have hexp : ((y : V n) + ones n) ⬝ᵥ p = p ⬝ᵥ (y : V n) + 1 := by
      rw [add_dotProduct, dotProduct_comm, dotProduct_comm (ones n) p,
        dotProduct_ones, hpsum]
    rw [hf_apply]
    rw [hexp] at hx
    linarith
  -- … hence ≥ 0 on Δ_q; the origin is interior and attains the minimum 0
  have hmin : ∀ z ∈ wsPolytopeAt q 1, (-f) z ≤ (-f) 0 := by
    intro z hz
    have h0 : (-f) (0 : wsHyperplane q) = 0 := by simp
    rw [h0]
    have hsub : wsPolytopeAt q 1 ⊆ {w : wsHyperplane q | (-f) w ≤ 0} := by
      refine convexHull_min ?_ ?_
      · intro y hy
        rw [mem_setOf_eq]
        have := hgen y hy
        simp only [ContinuousLinearMap.neg_apply]
        linarith
      · exact convex_halfSpace_le ((-f).toLinearMap.isLinear) 0
    exact hsub hz
  have hzero : (-f) = 0 := eq_zero_of_le_on_interior (-f) hIP.2 hmin
  have hfzero : ∀ y : wsHyperplane q, f y = 0 := by
    intro y
    have := congrFun (congrArg DFunLike.coe hzero) y
    simp only [ContinuousLinearMap.neg_apply, ContinuousLinearMap.zero_apply] at this
    linarith
  -- so p ⬝ᵥ · vanishes on ker (q ⬝ᵥ ·)
  have hker : LinearMap.ker (dotLin q) ≤ LinearMap.ker (dotLin p) := by
    intro y hy
    rw [LinearMap.mem_ker]
    have := hfzero ⟨y, hy⟩
    rw [hf_apply] at this
    simpa using this
  obtain ⟨c, hc⟩ := exists_smul_of_ker_le_ker hker
  -- evaluate at (1,…,1): c = 1, so p = q
  have hc1 : c = 1 := by
    have h1 := hc (ones n)
    simp only [dotLin_apply, dotProduct_ones, hqsum, hpsum, mul_one] at h1
    linarith
  have hpq : p = q := by
    funext i
    have h := hc (Pi.single i 1)
    simp only [dotLin_apply, dotProduct_single, mul_one, hc1, one_mul] at h
    exact h
  exact hne hpq

/-! ## The hyperplane `{z | z ⬝ᵥ p = c}` as an affine subspace

Used for the termination measure: all chosen points satisfy `z ⬝ᵥ q̃ = 1`,
so a new point with `x ⬝ᵥ q̃ < 1` lies outside their affine span and the
affine rank strictly increases. -/

/-- The affine hyperplane `{z | z ⬝ᵥ p = c}`. -/
def dotAffineSubspace (p : V n) (c : ℝ) : AffineSubspace ℝ (V n) where
  carrier := {z | z ⬝ᵥ p = c}
  smul_vsub_vadd_mem t {z₁ z₂ z₃} h₁ h₂ h₃ := by
    simp only [mem_setOf_eq, vsub_eq_sub, vadd_eq_add] at *
    rw [add_dotProduct, smul_dotProduct, sub_dotProduct, h₁, h₂, h₃]
    simp

theorem mem_dotAffineSubspace {p z : V n} {c : ℝ} :
    z ∈ dotAffineSubspace p c ↔ z ⬝ᵥ p = c := Iff.rfl

/-- A point strictly below a hyperplane containing `S` is outside the affine
span of `S`. -/
theorem notMem_affineSpan_of_dot_lt {S : Finset (V n)} {p x : V n}
    (hS : ∀ z ∈ S, z ⬝ᵥ p = 1) (hx : x ⬝ᵥ p < 1) :
    x ∉ affineSpan ℝ (S : Set (V n)) := by
  intro hmem
  have hle : affineSpan ℝ (S : Set (V n)) ≤ dotAffineSubspace p 1 := by
    rw [affineSpan_le]
    intro z hz
    exact hS z hz
  have := hle hmem
  rw [mem_dotAffineSubspace] at this
  linarith

/-- Inserting a point outside the affine span strictly increases the rank of
the direction space. -/
theorem finrank_vectorSpan_lt_insert {S : Finset (V n)} {x : V n}
    (hS : S.Nonempty) (hx : x ∉ affineSpan ℝ (S : Set (V n))) :
    finrank ℝ (vectorSpan ℝ (S : Set (V n))) <
      finrank ℝ (vectorSpan ℝ (insert x (S : Set (V n)))) := by
  obtain ⟨s₀, hs₀⟩ := hS
  have hle : vectorSpan ℝ (S : Set (V n)) ≤ vectorSpan ℝ (insert x (S : Set (V n))) :=
    vectorSpan_mono ℝ (subset_insert x _)
  have hne : vectorSpan ℝ (S : Set (V n)) ≠ vectorSpan ℝ (insert x (S : Set (V n))) := by
    intro heq
    apply hx
    have hdiff : x - s₀ ∈ vectorSpan ℝ (S : Set (V n)) := by
      rw [heq]
      have := vsub_mem_vectorSpan ℝ (mem_insert x (S : Set (V n)))
        (mem_insert_of_mem x (Finset.mem_coe.mpr hs₀))
      simpa using this
    have hs₀span : s₀ ∈ affineSpan ℝ (S : Set (V n)) :=
      subset_affineSpan ℝ _ (Finset.mem_coe.mpr hs₀)
    have hdir : x - s₀ ∈ (affineSpan ℝ (S : Set (V n))).direction := by
      rw [direction_affineSpan]
      exact hdiff
    have := AffineSubspace.vadd_mem_of_mem_direction hdir hs₀span
    simpa using this
  exact Submodule.finrank_lt_finrank_of_lt (lt_of_le_of_ne hle hne)

/-! ## T7.5 + T7.6: completeness and termination -/

/-- One step of the search: at a reachable node `S ∋ (1,…,1)` whose points lie
in `Δ_q`, either the selection rule already picks `q`, or the descent lemma
supplies a child node, still inside `Δ_q`, of strictly larger affine rank. -/
theorem reach_step (R : SelectionRule n) {q : V n} (hq : ∀ i, 0 < q i)
    (hqsum : ∑ i, q i = 1) (hIP : IsIPWeightSystem q) {S : Finset (V n)}
    (hreach : Reachable R S) (hones : ones n ∈ S) (hsub : (S : Set (V n)) ⊆ wsPoints q) :
    R.sel S = q ∨ ∃ x, Reachable R (insert x S) ∧ x ∈ wsPoints q ∧
      finrank ℝ (vectorSpan ℝ (S : Set (V n))) <
        finrank ℝ (vectorSpan ℝ (insert x (S : Set (V n)))) := by
  have hqfeas : q ∈ qPolytope S :=
    ⟨fun i => (hq i).le, hqsum, fun x hx => by rw [(hsub hx).2.2, hqsum]⟩
  have hsel := R.mem S ⟨q, hqfeas⟩
  refine or_iff_not_imp_left.2 fun hne => ?_
  obtain ⟨x, hxmem, hxlt⟩ := descent_lemma hqsum hIP hsel.2.1 hne
  exact ⟨x, Reachable.step hreach hxmem.1 hxmem.2.1 hxlt, hxmem,
    finrank_vectorSpan_lt_insert ⟨ones n, hones⟩ (notMem_affineSpan_of_dot_lt hsel.2.2 hxlt)⟩

/-- The inductive engine for completeness: from any reachable node `S` whose
points all lie in `Δ_q` (and contain the root), within `k` further steps the
algorithm selects `q`, provided `k` bounds the remaining co-rank
`n - rank(S)`.  Each descent step strictly increases the affine rank, which
is bounded by `n`; at full rank the feasible region is `{q}` and the
selection rule has no choice. -/
theorem reach_aux (R : SelectionRule n) {q : V n} (hq : ∀ i, 0 < q i)
    (hqsum : ∑ i, q i = 1) (hIP : IsIPWeightSystem q) :
    ∀ (k : ℕ) (S : Finset (V n)), Reachable R S → ones n ∈ S →
      ((S : Set (V n)) ⊆ wsPoints q) →
      n - finrank ℝ (vectorSpan ℝ (S : Set (V n))) ≤ k →
      ∃ S', Reachable R S' ∧ R.sel S' = q ∧ S'.card ≤ S.card + k := by
  classical
  intro k
  induction k with
  | zero =>
    intro S hreach hones hsub hrank
    refine (reach_step R hq hqsum hIP hreach hones hsub).elim
      (fun h => ⟨S, hreach, h, le_rfl⟩) fun ⟨x, _, _, hlt⟩ => ?_
    -- the rank is already maximal, so it cannot increase
    have := Submodule.finrank_le (vectorSpan ℝ (insert x (S : Set (V n))))
    simp only [finrank_fin_fun] at this
    omega
  | succ k ih =>
    intro S hreach hones hsub hrank
    refine (reach_step R hq hqsum hIP hreach hones hsub).elim
      (fun h => ⟨S, hreach, h, by omega⟩) fun ⟨x, hstep, hxmem, hlt⟩ => ?_
    obtain ⟨S', h1, h2, h3⟩ := ih (insert x S) hstep
      (Finset.mem_insert_of_mem hones)
      (by rw [Finset.coe_insert]; exact insert_subset_iff.mpr ⟨hxmem, hsub⟩)
      (by rw [Finset.coe_insert]; omega)
    exact ⟨S', h1, h2, by have := Finset.card_insert_le x S; omega⟩

/-- **T7.5 + T7.6 (completeness and termination of the enumeration;
[SS18] §3).**  For every selection rule `R` and every normalized IP weight
system `q`, the search tree rooted at `{(1,…,1)}` contains a node `S` at
which the selected candidate is exactly `q`; moreover `S` is reached within
`n` branching steps (`|S| ≤ n + 1`).

Together with the explicit IP check performed on every candidate (soundness)
this says: *the [SS18] algorithm enumerates precisely the IP weight systems*
— in dimension 5, the 322 383 760 930 systems of which 185 269 499 015 give
reflexive polytopes. -/
theorem algorithm_complete (R : SelectionRule n) {q : V n}
    (hq : ∀ i, 0 < q i) (hqsum : ∑ i, q i = 1) (hIP : IsIPWeightSystem q) :
    ∃ S : Finset (V n), Reachable R S ∧ R.sel S = q ∧ S.card ≤ n + 1 := by
  have hroot : ((({ones n} : Finset (V n))) : Set (V n)) ⊆ wsPoints q := by
    simpa using ones_mem_wsPoints q
  obtain ⟨S, h1, h2, h3⟩ := reach_aux R hq hqsum hIP n {ones n}
    Reachable.base (Finset.mem_singleton_self _) hroot (by omega)
  refine ⟨S, h1, h2, ?_⟩
  rw [Finset.card_singleton] at h3
  omega

end Refpoly
