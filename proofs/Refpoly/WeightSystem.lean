/-
WeightSystem faithfully ported from the reviewed refpoly-5d snapshot.
See SOURCES.md for attribution and exact source identities.
-/
import Refpoly.Basic

open Set Matrix

namespace Refpoly

variable {n : ℕ}

/-! ## The hyperplane of a weight system -/

/-- The linear functional `y ↦ q ⬝ᵥ y`. -/
def dotLin (q : V n) : V n →ₗ[ℝ] ℝ where
  toFun y := q ⬝ᵥ y
  map_add' x y := by simp [dotProduct_add]
  map_smul' c x := by simp [dotProduct_smul]

@[simp] theorem dotLin_apply (q y : V n) : dotLin q y = q ⬝ᵥ y := rfl

/-- `W_q = {y | Σ qᵢyᵢ = 0}`: the image of `M_ℝ` inside `ℝⁿ` under the
embedding `X ↦ (⟨Vᵢ,X⟩)ᵢ` of [SS18] eq. (3.1). -/
def wsHyperplane (q : V n) : Submodule ℝ (V n) := LinearMap.ker (dotLin q)

theorem mem_wsHyperplane {q y : V n} : y ∈ wsHyperplane q ↔ q ⬝ᵥ y = 0 :=
  LinearMap.mem_ker

/-- The generating lattice points of `Δ_q` at depth `c` (in `y`-coordinates):
integer points of `W_q` with all coordinates `≥ -c`.  The standard case is
`c = 1` ([SS18] eq. (3.2)); `c = 2` arises in the `q_n = ½` reduction. -/
def wsLatticeGensAt (q : V n) (c : ℝ) : Set (wsHyperplane q) :=
  {y | IsLatticePoint (y : V n) ∧ ∀ i, -c ≤ (y : V n) i}

/-- `Δ_q` (at depth `c`) as a subset of the hyperplane `W_q`. -/
def wsPolytopeAt (q : V n) (c : ℝ) : Set (wsHyperplane q) :=
  convexHull ℝ (wsLatticeGensAt q c)

/-- Generalized IP property at depth `c`.  (Positivity of the weights is part
of the definition of a weight system, [SS18] §2.2.) -/
def IsIPWeightSystemAt (q : V n) (c : ℝ) : Prop :=
  (∀ i, 0 < q i) ∧ (0 : wsHyperplane q) ∈ interior (wsPolytopeAt q c)

/-- **D5.4: the IP property of a weight system** ([SS18] §3): `0` is in the
interior — within the hyperplane `W_q`, i.e. within `M_ℝ` — of the convex
hull of the lattice points `y ∈ ℤⁿ ∩ W_q` with `yᵢ ≥ -1`. -/
def IsIPWeightSystem (q : V n) : Prop := IsIPWeightSystemAt q 1

/-! ## Shifted (`x`-space) coordinates -/

/-- The all-ones vector: the interior point `𝟙` of [KS95]/[Sk96], the
`x⁽⁰⁾ = (1,…,1)` of [SS18] §3. -/
def ones (n : ℕ) : V n := fun _ => 1

theorem isLatticePoint_ones : IsLatticePoint (ones n) := fun _ => ⟨1, by simp [ones]⟩

@[simp] theorem dotProduct_ones (q : V n) : q ⬝ᵥ ones n = ∑ i, q i := by
  simp [dotProduct, ones]

/-- The lattice points of `Δ_q` in shifted coordinates ([SS18] eq. (ipws)):
nonnegative integer vectors `x` with `Σ xᵢqᵢ = Σ qᵢ` (i.e. `x·q` equals its
value at the all-ones point; for normalized `Σq = 1` this is `x·q = 1`). -/
def wsPoints (q : V n) : Set (V n) :=
  {x | IsLatticePoint x ∧ (∀ i, 0 ≤ x i) ∧ x ⬝ᵥ q = ∑ i, q i}

theorem ones_mem_wsPoints (q : V n) : ones n ∈ wsPoints q :=
  ⟨isLatticePoint_ones, fun _ => by norm_num [ones],
    by rw [dotProduct_comm]; exact dotProduct_ones q⟩

/-- The change of variables `y = x - 𝟙` identifies `x`-space lattice points
with the depth-1 generators of `Δ_q` ([SS18] §3: "Upon passing to new
coordinates xᵢ = yᵢ + 1 …"). -/
theorem wsPoints_shift {q x : V n} :
    x ∈ wsPoints q ↔
      ∃ hy : x - ones n ∈ wsHyperplane q,
        (⟨x - ones n, hy⟩ : wsHyperplane q) ∈ wsLatticeGensAt q 1 := by
  constructor
  · rintro ⟨hlat, hpos, hdeg⟩
    have hy : x - ones n ∈ wsHyperplane q := by
      rw [mem_wsHyperplane, dotProduct_sub, dotProduct_ones,
        dotProduct_comm q x, hdeg, sub_self]
    refine ⟨hy, ?_, ?_⟩
    · intro i
      obtain ⟨m, hm⟩ := hlat i
      exact ⟨m - 1, by simp [ones, hm]⟩
    · intro i
      have := hpos i
      simp only [Pi.sub_apply, ones]
      linarith
  · rintro ⟨hy, hlat, hge⟩
    refine ⟨?_, ?_, ?_⟩
    · intro i
      obtain ⟨m, hm⟩ := hlat i
      simp only [Pi.sub_apply, ones] at hm
      exact ⟨m + 1, by push_cast; linarith⟩
    · intro i
      have := hge i
      simp only [Pi.sub_apply, ones] at this
      linarith
    · have h0 : q ⬝ᵥ (x - ones n) = 0 := mem_wsHyperplane.1 hy
      rw [dotProduct_sub, dotProduct_ones, sub_eq_zero] at h0
      rw [dotProduct_comm]
      exact h0

/-! ## The IP criterion for simplices (L5.2)

[KS95] §3: "We denote by the weights qᵢ … the barycentric coordinates of 𝟙̄";
[Sk96] §2: "If all of the barycentric coordinates are positive … 𝟙 is in the
interior of the simplex."  The Mathlib form: the interior of the hull of an
affine basis is the locus of positive barycentric coordinates. -/

/-- **L5.2 (interior point criterion for simplices).**  Let `b` be an affine
basis (a non-degenerate simplex's vertex family) of `ℝⁿ` and `w` weights with
`Σ wᵢ = 1` and `Σ wᵢ • b i = 0` (the circuit relation `Σ qᵢVᵢ = 0` of
[SS18] §2.2 in normalized form).  Then `0` is interior to the simplex iff all
weights are positive.  The `wᵢ` are exactly the barycentric coordinates of
the origin. -/
theorem ip_simplex_iff_weights_pos {ι : Type*} [Fintype ι]
    (b : AffineBasis ι ℝ (V n)) (w : ι → ℝ) (hw : ∑ i, w i = 1)
    (hcomb : ∑ i, w i • b i = (0 : V n)) :
    (0 : V n) ∈ interior (convexHull ℝ (Set.range b)) ↔ ∀ i, 0 < w i := by
  have hbar : ∀ i, b.coord i 0 = w i := by
    intro i
    have hac : Finset.univ.affineCombination ℝ b w = (0 : V n) := by
      rw [Finset.univ.affineCombination_eq_linear_combination b w hw]
      exact hcomb
    rw [← hac]
    exact b.coord_apply_combination_of_mem (Finset.mem_univ i) hw
  rw [b.interior_convexHull]
  simp only [mem_setOf_eq, hbar]

/-! ## Necessary bounds on IP weight systems (L5.6, L5.7) -/

/-- Generators of `Δ_q` have `j`-th coordinate `≤ 0` when `q j` exceeds half
the total weight: from `q·y = 0`, `yᵢ ≥ -1` one gets
`qⱼyⱼ = -Σ_{i≠j} qᵢyᵢ ≤ Σ_{i≠j} qᵢ < qⱼ`, so `yⱼ < 1`, and integrality gives
`yⱼ ≤ 0`.  ([SS18] §3: "if qᵢ > 1/2 then xᵢ ∈ {0,1} for all x ∈ Δ_q".) -/
theorem gens_coord_nonpos_of_gt_half {q : V n} (hq : ∀ i, 0 < q i)
    {j : Fin n} (hj : ∑ i, q i < 2 * q j) {y : wsHyperplane q}
    (hy : y ∈ wsLatticeGensAt q 1) : (y : V n) j ≤ 0 := by
  obtain ⟨hlat, hge⟩ := hy
  have h0 : ∑ i, q i * (y : V n) i = 0 := by simpa [dotProduct] using mem_wsHyperplane.1 y.2
  -- every term `qᵢ(yᵢ + 1)` is nonnegative and they sum to `Σ qᵢ`
  have hterm : q j * ((y : V n) j + 1) ≤ ∑ i, q i * ((y : V n) i + 1) :=
    Finset.single_le_sum (f := fun i => q i * ((y : V n) i + 1))
      (fun i _ => mul_nonneg (hq i).le (by linarith [hge i]))
      (Finset.mem_univ j)
  simp only [mul_add, mul_one, Finset.sum_add_distrib, h0, zero_add] at hterm
  have hylt : (y : V n) j < 1 := by nlinarith [hq j]
  obtain ⟨m, hm⟩ := hlat j
  rw [hm] at hylt ⊢
  have hm1 : m < 1 := by exact_mod_cast hylt
  have hm0 : m ≤ 0 := by omega
  exact_mod_cast hm0

/-- **L5.6.**  An IP weight system satisfies `2 qⱼ ≤ Σᵢ qᵢ` for every `j`
(in normalized form: `qⱼ ≤ ½`).  [SS18] §3: "This can hold only if all
weights obey qᵢ ≤ 1/2."

Proof: if `2qⱼ > Σq`, the linear functional `y ↦ yⱼ` is `≤ 0` on `Δ_q` and
`= 0` at the origin, so the origin maximizes it; since the origin is interior
this forces the functional to vanish on all of `W_q`
(`eq_zero_of_le_on_interior`), which is false because `W_q` contains a vector
with `j`-th coordinate `q k > 0` (any `k ≠ j`). -/
theorem not_ip_of_weight_gt_half (hn : 2 ≤ n) {q : V n} {j : Fin n}
    (hj : ∑ i, q i < 2 * q j) : ¬ IsIPWeightSystem q := by
  rintro ⟨hq, hint⟩
  -- the coordinate functional on the hyperplane
  set f : wsHyperplane q →L[ℝ] ℝ :=
    (ContinuousLinearMap.proj j).comp (wsHyperplane q).subtypeL with hf
  have hf_apply : ∀ y : wsHyperplane q, f y = (y : V n) j := fun _ => rfl
  -- f ≤ 0 on Δ_q
  have hmax : ∀ z ∈ wsPolytopeAt q 1, f z ≤ f 0 := by
    intro z hz
    have h0 : f (0 : wsHyperplane q) = 0 := by simp
    rw [h0]
    have hsub : wsPolytopeAt q 1 ⊆ {w : wsHyperplane q | f w ≤ 0} := by
      refine convexHull_min ?_ ?_
      · intro y hy
        rw [mem_setOf_eq, hf_apply]
        exact gens_coord_nonpos_of_gt_half hq hj hy
      · exact convex_halfSpace_le (f.toLinearMap.isLinear) 0
    exact hsub hz
  -- so f vanishes identically …
  have hzero : f = 0 := eq_zero_of_le_on_interior f hint hmax
  -- … but W_q contains a vector with nonzero j-th coordinate.
  obtain ⟨k, hk⟩ : ∃ k : Fin n, k ≠ j := by
    haveI : Nontrivial (Fin n) := Fin.nontrivial_iff_two_le.mpr hn
    exact exists_ne j
  set w : V n := q k • Pi.single j 1 - q j • Pi.single k 1 with hw
  have hwmem : w ∈ wsHyperplane q := by
    rw [mem_wsHyperplane, hw, dotProduct_sub, dotProduct_smul, dotProduct_smul,
      dotProduct_single, dotProduct_single]
    simp [mul_comm]
  have hwj : w j = q k := by
    rw [hw]
    simp only [Pi.sub_apply, Pi.smul_apply, smul_eq_mul]
    rw [Pi.single_eq_same, Pi.single_eq_of_ne hk.symm]
    ring
  have := congrFun (congrArg DFunLike.coe hzero) ⟨w, hwmem⟩
  rw [hf_apply] at this
  simp only [ContinuousLinearMap.zero_apply] at this
  rw [hwj] at this
  exact absurd this (ne_of_gt (hq k))

/-- **L5.7.**  For `n ≥ 3`, at most one weight can equal half the total
weight ([SS18] §3: "For n > 2 at most one of the qᵢ can be equal to 1/2
(otherwise Σᵢ qᵢ > 1)"). -/
theorem at_most_one_half (hn : 3 ≤ n) {q : V n} (hq : ∀ i, 0 < q i)
    {j k : Fin n} (hjk : j ≠ k) (hj : 2 * q j = ∑ i, q i)
    (hk : 2 * q k = ∑ i, q i) : False := by
  classical
  -- a third coordinate exists and contributes positively
  obtain ⟨m, -, hm⟩ := Finset.exists_mem_notMem_of_card_lt_card
    (s := {j, k}) (t := Finset.univ)
    (by simpa using (Finset.card_le_two (a := j) (b := k)).trans_lt (by omega))
  simp only [Finset.mem_insert, Finset.mem_singleton, not_or] at hm
  have hsum : ∑ i ∈ {j, k, m}, q i ≤ ∑ i, q i :=
    Finset.sum_le_univ_sum_of_nonneg fun i => (hq i).le
  rw [Finset.sum_insert (by simp [hjk, Ne.symm hm.1]),
    Finset.sum_pair (Ne.symm hm.2)] at hsum
  have := hq m
  linarith

/-! ## Pseudoreflexivity of `Δ_q` (T5.11)

[SS18] §6 shows that the polytope of any IP weight system satisfies
`Δ = [Δ] = [[Δ*]*]` ("pseudoreflexive" / "almost reflexive"), even when it is
not reflexive — which for `d = 5` happens for 137 114 261 915 of the
322 383 760 930 IP weight systems.  We formalize the inclusion-chasing proof
verbatim, abstractly in `ℝⁿ`: `∇` is any lattice polytope containing the
origin and `Δ_q := [∇*]`. -/

/-- `[·]` is idempotent. -/
theorem latticeHull_idem (P : Set (V n)) :
    latticeHull (latticeHull P) = latticeHull P := by
  apply Subset.antisymm
  · exact latticeHull_subset_of_convex (convex_convexHull ℝ _)
  · apply convexHull_mono
    intro x hx
    exact ⟨subset_convexHull ℝ _ hx, hx.2⟩

/-- **T5.11 (pseudoreflexivity, [SS18] §6).**  For a lattice polytope `∇`
containing the origin, the polytope `Δ := [∇*]` satisfies `[[Δ*]*] = Δ`.

Paper proof (transcribed): `Δ = [∇*] ⊆ ∇*` hence `Δ* ⊇ ∇** ⊇ ∇`; since `∇`
is a lattice polytope this implies `[Δ*] ⊇ [∇] = ∇` and therefore
`[[Δ*]*] ⊆ [∇*] = Δ`.  Conversely `[Δ*] ⊆ Δ*` gives `[Δ*]* ⊇ Δ** ⊇ Δ`,
which implies `[[Δ*]*] ⊇ [Δ] = Δ` because `Δ` is (the hull of) lattice
points. -/
theorem pseudoreflexive {Nbl : Set (V n)} (hNbl : IsLatticePolytope Nbl) :
    latticeHull (polarDual (latticeHull (polarDual (latticeHull (polarDual Nbl))))) =
      latticeHull (polarDual Nbl) := by
  set Δ : Set (V n) := latticeHull (polarDual Nbl) with hΔ
  -- Δ ⊆ ∇*
  have h1 : Δ ⊆ polarDual Nbl := latticeHull_subset_of_convex (polarDual_convex Nbl)
  -- ∇ ⊆ Δ*
  have h2 : Nbl ⊆ polarDual Δ :=
    (subset_bipolar Nbl).trans (polarDual_antitone h1)
  -- ∇ = [∇] ⊆ [Δ*]
  have h3 : Nbl ⊆ latticeHull (polarDual Δ) := by
    rw [hNbl.eq_latticeHull]
    exact latticeHull_mono h2
  -- [[Δ*]*] ⊆ [∇*] = Δ
  have h4 : latticeHull (polarDual (latticeHull (polarDual Δ))) ⊆ Δ :=
    latticeHull_mono (polarDual_antitone h3)
  -- Δ ⊆ [Δ*]* :  [Δ*] ⊆ Δ* gives Δ ⊆ Δ** ⊆ [Δ*]*
  have h5 : Δ ⊆ polarDual (latticeHull (polarDual Δ)) :=
    (subset_bipolar Δ).trans
      (polarDual_antitone (latticeHull_subset_of_convex (polarDual_convex Δ)))
  -- Δ = [Δ] ⊆ [[Δ*]*]
  have h6 : Δ ⊆ latticeHull (polarDual (latticeHull (polarDual Δ))) := by
    conv_lhs => rw [hΔ, ← latticeHull_idem (polarDual Nbl)]
    exact latticeHull_mono h5
  exact Subset.antisymm h4 h6

/-! ## The `q_n = ½` reduction (L5.8) -/

/-- **L5.8 ([SS18] §3).** Appending a half-weight gives an IP system
exactly when the truncated system has the depth-2 IP property.

Projection `y ↦ y_rest + y_last • 𝟙` maps the full lattice hull into the
depth-2 hull and is surjective and open. Conversely, the full lattice hull
contains the affine image `z ↦ (z + 𝟙, -1)` of the depth-2 hull and the
apex `(-𝟙, 1)`. A neighborhood of zero in the base produces an open
neighborhood of zero inside this pyramid. -/
theorem ip_half_reduction {m : ℕ} (q' : V m) (hq' : ∀ i, 0 < q' i)
    (hsum : ∑ i, q' i = 1 / 2) :
    IsIPWeightSystem (Fin.snoc q' (1 / 2) : V (m + 1)) ↔
      IsIPWeightSystemAt q' 2 := by
  classical
  let Q : V (m+1) := Fin.snoc q' (1/2)
  let H := wsHyperplane Q
  let H' := wsHyperplane q'
  have snoc_dot (z : V m) (t : ℝ) :
      Q ⬝ᵥ Fin.snoc z t = q' ⬝ᵥ z + (1/2)*t := by
    simp [Q, dotProduct, Fin.sum_univ_castSucc]
  let E : H' →ₗ[ℝ] H :=
    { toFun := fun z => ⟨Fin.snoc (z : V m) 0, by
        rw [mem_wsHyperplane, snoc_dot]; simpa only [mul_zero, add_zero] using (mem_wsHyperplane.mp z.property)⟩
      map_add' := by
        intro x y; apply Subtype.ext; funext i
        refine Fin.lastCases ?_ (fun j => ?_) i <;> simp
      map_smul' := by
        intro t z; apply Subtype.ext; funext i
        refine Fin.lastCases ?_ (fun j => ?_) i <;> simp }
  let a : H := ⟨Fin.snoc (-ones m) 1, by
    rw [mem_wsHyperplane, snoc_dot, dotProduct_neg, dotProduct_ones, hsum]
    norm_num⟩
  let G : H →ₗ[ℝ] H' :=
    { toFun := fun y => ⟨(fun i => (y : V (m+1)) i.castSucc) +
          (y : V (m+1)) (Fin.last m) • ones m, by
        rw [mem_wsHyperplane, dotProduct_add, dotProduct_smul, dotProduct_ones, hsum]
        have hy := y.property
        change Q ⬝ᵥ (y : V (m+1)) = 0 at hy
        simp only [dotProduct, Fin.sum_univ_castSucc, Q, Fin.snoc_castSucc,
          Fin.snoc_last] at hy
        change (∑ i, q' i * (y : V (m+1)) i.castSucc) + _ = 0
        simp only [smul_eq_mul] at *
        linarith⟩
      map_add' := by intro x y; apply Subtype.ext; funext i; simp [ones]; ring
      map_smul' := by intro t y; apply Subtype.ext; funext i; simp [ones] }
  have hGE (z : H') : G (E z) = z := by
    apply Subtype.ext; funext i; simp [G, E]
  have hGsurj : Function.Surjective G := fun z => ⟨E z, hGE z⟩
  let F : H' × ℝ →ₗ[ℝ] H :=
    E.comp (LinearMap.fst ℝ H' ℝ) + (LinearMap.snd ℝ H' ℝ).smulRight a
  have hFsurj : Function.Surjective F := by
    intro y
    refine ⟨(G y, (y : V (m+1)) (Fin.last m)), ?_⟩
    apply Subtype.ext; funext i
    refine Fin.lastCases ?_ (fun j => ?_) i <;> simp [F, E, G, a, ones]
  let B : H' →ᵃ[ℝ] H := (AffineEquiv.constVAdd ℝ H (-a)).toAffineMap.comp E.toAffineMap
  have hB (z : H') : B z = E z - a := by
    simp [B, sub_eq_add_neg, add_comm]
  have ha : a ∈ wsPolytopeAt Q 1 := by
    apply subset_convexHull ℝ
    constructor
    · intro i
      refine Fin.lastCases ?_ (fun j => ?_) i
      · exact ⟨1, by simp [a]⟩
      · exact ⟨-1, by simp [a, ones]⟩
    · intro i
      refine Fin.lastCases ?_ (fun j => ?_) i <;> simp [a, ones]
  have hBgens : B '' wsLatticeGensAt q' 2 ⊆ wsLatticeGensAt Q 1 := by
    rintro _ ⟨z, hz, rfl⟩
    constructor
    · intro i
      refine Fin.lastCases ?_ (fun j => ?_) i
      · exact ⟨-1, by simp [hB, E, a]⟩
      · obtain ⟨k, hk⟩ := hz.1 j
        exact ⟨k+1, by simp [hB, E, a, ones, ← hk]⟩
    · intro i
      refine Fin.lastCases ?_ (fun j => ?_) i
      · simp [hB, E, a]
      · have hj := hz.2 j
        simpa [hB, E, a, ones] using (show -1 ≤ (z : V m) j + 1 by linarith)
  have hBpoly : B '' wsPolytopeAt q' 2 ⊆ wsPolytopeAt Q 1 := by
    rw [wsPolytopeAt, AffineMap.image_convexHull]
    exact convexHull_mono hBgens
  have hGgens : G '' wsLatticeGensAt Q 1 ⊆ wsLatticeGensAt q' 2 := by
    rintro _ ⟨y, hy, rfl⟩
    constructor
    · intro i
      obtain ⟨k, hk⟩ := hy.1 i.castSucc
      obtain ⟨l, hl⟩ := hy.1 (Fin.last m)
      exact ⟨k+l, by simp [G, ones, ← hk, ← hl]⟩
    · intro i
      have hi := hy.2 i.castSucc
      have hl := hy.2 (Fin.last m)
      change -2 ≤ (y : V (m+1)) i.castSucc + (y : V (m+1)) (Fin.last m) * 1
      simp only [mul_one]
      linarith
  have hGpoly : G '' wsPolytopeAt Q 1 ⊆ wsPolytopeAt q' 2 := by
    rw [wsPolytopeAt, LinearMap.image_convexHull]
    exact convexHull_mono hGgens
  constructor
  · intro h
    refine ⟨hq', ?_⟩
    apply interior_mono hGpoly
    exact (LinearMap.isOpenMap_of_finiteDimensional G hGsurj).image_interior_subset
      (wsPolytopeAt Q 1) ⟨0, h.2, map_zero G⟩
  · rintro ⟨_, h0⟩
    refine ⟨?_, ?_⟩
    · intro i
      refine Fin.lastCases ?_ (fun j => ?_) i <;> simp [hq']
    · obtain ⟨r, hr, hball⟩ := Metric.mem_nhds_iff.mp (mem_interior_iff_mem_nhds.mp h0)
      let U : Set (H' × ℝ) := Metric.ball 0 (r/4) ×ˢ Ioo (-1/2) (1/2)
      have hUopen : IsOpen U := Metric.isOpen_ball.prod isOpen_Ioo
      have hU0 : (0 : H' × ℝ) ∈ U := by
        constructor
        · simpa using (show 0 < r/4 by linarith)
        · constructor <;> norm_num
      have hFU : F '' U ⊆ wsPolytopeAt Q 1 := by
        rintro _ ⟨⟨z,t⟩, hzt, rfl⟩
        have hz : ‖z‖ < r/4 := by simpa [Metric.mem_ball, dist_zero_right] using hzt.1
        have ht := hzt.2
        have htpos : 0 < 1-t := by linarith [ht.2]
        let u : H' := (2/(1-t)) • z
        have hu : u ∈ wsPolytopeAt q' 2 := by
          apply hball
          rw [Metric.mem_ball, dist_zero_right]
          have hnorm : ‖u‖ = (2/(1-t))*‖z‖ := by
            change ‖(2/(1-t)) • z‖ = _
            rw [norm_smul, Real.norm_eq_abs, abs_of_pos (div_pos (by norm_num) htpos)]
          rw [hnorm]
          apply (div_mul_eq_mul_div _ _ _).trans_lt
          apply (div_lt_iff₀ htpos).mpr
          nlinarith [ht.2]
        have hb : B u ∈ wsPolytopeAt Q 1 := hBpoly ⟨u, hu, rfl⟩
        have hconv := convex_convexHull ℝ (wsLatticeGensAt Q 1) hb ha
          (show 0 ≤ (1-t)/2 by linarith) (show 0 ≤ (1+t)/2 by linarith [ht.1])
          (show (1-t)/2 + (1+t)/2 = 1 by ring)
        have heq : ((1-t)/2) • B u + ((1+t)/2) • a = F (z,t) := by
          apply Subtype.ext; funext i
          refine Fin.lastCases ?_ (fun j => ?_) i <;>
            simp [hB, E, a, F, u, ones] <;> field_simp <;> ring
        rwa [heq] at hconv
      have hFopen := (LinearMap.isOpenMap_of_finiteDimensional F hFsurj) U hUopen
      apply interior_mono hFU
      rw [hFopen.interior_eq]
      exact ⟨0, hU0, map_zero F⟩

/-! ## Combined weight systems (D5.9, T5.10) -/

/-- **D5.9: combined weight system** ([SS18] §2.2: "Minimal polytopes
consisting of more than one IP simplex are described by combined weight
systems (matrices of weights)").  `k` weight systems on a common coordinate
set `Fin n`; each row is nonnegative, supported on its block, and every
coordinate is used by some row ([KS95] Corollary 1: the blocks cover all
directions).  The associated subspace is the joint kernel — for a single
system (`k = 1`) this recovers `wsHyperplane`; in general `n = d + k`
([SS18] §3). -/
structure CWS (k n : ℕ) where
  /-- the weight matrix: row `a` is the `a`-th weight system, zero-padded -/
  W : Fin k → V n
  nonneg : ∀ a i, 0 ≤ W a i
  /-- every coordinate carries a nonzero weight of at least one system -/
  cover : ∀ i, ∃ a, 0 < W a i
  /-- the rows are linearly independent (the `k` defining equations of
  [SS18] §3 are independent) -/
  indep : LinearIndependent ℝ W

/-- The joint kernel `{y | W a ⬝ᵥ y = 0 ∀ a} ≅ M_ℝ`, of dimension `n - k`. -/
def CWS.subspace {k : ℕ} (C : CWS k n) : Submodule ℝ (V n) :=
  ⨅ a, LinearMap.ker (dotLin (C.W a))

/-- The IP property for a CWS: `0` interior (within the joint kernel) to the
hull of the integer points with all coordinates `≥ -1`. -/
def CWS.IsIP {k : ℕ} (C : CWS k n) : Prop :=
  (0 : C.subspace) ∈ interior (convexHull ℝ
    {y : C.subspace | IsLatticePoint (y : V n) ∧ ∀ i, -1 ≤ (y : V n) i})

/-- **T5.10 ([SS18] §2.2, citing math/0001106).**  "For a combined weight
system to have the IP property, it is necessary that every single weight
system occurring in it has this property."  Stated via the block-restriction
`e`: an injection of the support of row `a` into `Fin m` carrying `W a` to a
positive weight system `q'`.

*Proof (the paper's projection argument, made precise).*  The block
projection `π : y ↦ y ∘ e` (Lean `LinearMap.funLeft`) carries the joint kernel
`C.subspace` into `W_{q'}` (because `W a` is supported on the block, so
`q' ⬝ᵥ π y = (W a) ⬝ᵥ y = 0`) and carries the joint generators `{y : ℤ-pt,
yᵢ ≥ -1}` into the generators of `Δ_{q'}`.  As a *surjective* linear map
between the finite-dimensional subspaces `C.subspace ↠ W_{q'}` it is an **open
map** (`LinearMap.isOpenMap_of_finiteDimensional`), so it sends the interior
point `0 ∈ int Δ_C` to an interior point `0 = π 0 ∈ int Δ_{q'}`.

The surjectivity of the block projection (`hlift`: every `z ∈ W_{q'}` lifts to
a joint-kernel vector agreeing with `z` on the block) is the one piece of
genuine CWS geometry that the four abstract axioms of `CWS` do not by
themselves force — it can fail for *degenerate* combined systems where the
joint kernel collapses on a block (e.g. `k = n`, where `C.subspace = 0`).  For
every geometrically realized combined weight system (`n = d + k`, each block
contributing a positive-dimensional factor) it holds, and it is exactly the
hypothesis under which the paper's projection argument is valid; we therefore
take it as an explicit hypothesis rather than silently assuming non-degeneracy.
-/
theorem cws_ip_component {k m : ℕ} (C : CWS k n) (hC : C.IsIP) (a : Fin k)
    (e : Fin m → Fin n) (he : Function.Injective e)
    (hsupp : ∀ i, 0 < C.W a i ↔ ∃ b, e b = i) (q' : V m)
    (hq' : ∀ b, q' b = C.W a (e b))
    (hlift : ∀ z : V m, q' ⬝ᵥ z = 0 →
      ∃ y : V n, (∀ a' : Fin k, C.W a' ⬝ᵥ y = 0) ∧ ∀ b, y (e b) = z b) :
    IsIPWeightSystem q' := by
  classical
  -- coordinates off the block carry zero weight (nonneg + support description)
  have hzero : ∀ i, (¬ ∃ b, e b = i) → C.W a i = 0 := fun i hi =>
    le_antisymm (not_lt.1 (fun h => hi ((hsupp i).1 h))) (C.nonneg a i)
  -- the block projection  y ↦ y ∘ e  as a linear map  V n → V m
  set pLin : V n →ₗ[ℝ] V m := LinearMap.funLeft ℝ ℝ e with hpLin
  -- reindexing:  Σ_b (W a)(e b)·y(e b) = Σ_i (W a i)·y i   (terms off-block vanish)
  have reindex : ∀ y : V n,
      (∑ b, C.W a (e b) * y (e b)) = ∑ i, C.W a i * y i := by
    intro y
    refine Fintype.sum_of_injective e he _ _ (fun i hi => ?_) (fun _ => rfl)
    rw [hzero i hi, zero_mul]
  -- (A) the projection maps the joint kernel into W_{q'}
  have hA : ∀ y ∈ C.subspace, pLin y ∈ wsHyperplane q' := by
    intro y hy
    have hy' : y ∈ ⨅ a', LinearMap.ker (dotLin (C.W a')) := hy
    have hker : C.W a ⬝ᵥ y = 0 := by
      simpa using LinearMap.mem_ker.1 ((Submodule.mem_iInf _).1 hy' a)
    have hcalc : q' ⬝ᵥ pLin y = C.W a ⬝ᵥ y := by
      simp only [dotProduct, hpLin, LinearMap.funLeft_apply]
      rw [← reindex y]
      exact Finset.sum_congr rfl (fun b _ => by rw [hq' b])
    rw [mem_wsHyperplane, hcalc]; exact hker
  -- the restricted/corestricted projection  F : C.subspace → W_{q'}
  set F : C.subspace →ₗ[ℝ] wsHyperplane q' := pLin.restrict hA with hF
  -- positivity of the component weights
  have hpos : ∀ b, 0 < q' b := fun b => by rw [hq' b]; exact (hsupp (e b)).2 ⟨b, rfl⟩
  refine ⟨hpos, ?_⟩
  -- F is surjective …
  have hFsurj : Function.Surjective F := by
    rintro ⟨z, hz⟩
    rw [mem_wsHyperplane] at hz
    obtain ⟨y, hyker, hyz⟩ := hlift z hz
    have hy : y ∈ C.subspace :=
      (Submodule.mem_iInf _).2 (fun a' => by rw [LinearMap.mem_ker, dotLin_apply]; exact hyker a')
    refine ⟨⟨y, hy⟩, Subtype.ext (funext fun b => ?_)⟩
    simp only [hF, LinearMap.coe_restrict_apply, hpLin, LinearMap.funLeft_apply]
    exact hyz b
  -- … hence an open map between the finite-dimensional subspaces
  have hFopen : IsOpenMap F := LinearMap.isOpenMap_of_finiteDimensional F hFsurj
  -- the joint generators map into the generators of Δ_{q'}
  set GC : Set C.subspace :=
    {y | IsLatticePoint (y : V n) ∧ ∀ i, -1 ≤ (y : V n) i} with hGC
  have himg : F '' GC ⊆ wsLatticeGensAt q' 1 := by
    rintro _ ⟨w, hw, rfl⟩
    rw [hGC, Set.mem_setOf_eq] at hw
    refine ⟨fun b => ?_, fun b => ?_⟩
    · simp only [hF, LinearMap.coe_restrict_apply, hpLin, LinearMap.funLeft_apply]
      exact hw.1 (e b)
    · simp only [hF, LinearMap.coe_restrict_apply, hpLin, LinearMap.funLeft_apply]
      simpa using hw.2 (e b)
  -- interior chase:  0 = F 0 ∈ F '' int Δ_C ⊆ int (F '' Δ_C) ⊆ int Δ_{q'}
  have hsub : F '' (convexHull ℝ GC) ⊆ wsPolytopeAt q' 1 := by
    rw [LinearMap.image_convexHull]
    exact convexHull_mono himg
  have hCmem : (0 : C.subspace) ∈ interior (convexHull ℝ GC) := by rw [hGC]; exact hC
  have h0 : (0 : wsHyperplane q') ∈ F '' interior (convexHull ℝ GC) :=
    ⟨0, hCmem, map_zero F⟩
  exact interior_mono hsub (hFopen.image_interior_subset (convexHull ℝ GC) h0)

end Refpoly
