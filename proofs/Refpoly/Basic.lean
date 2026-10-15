/-
Lattice-polytope foundations faithfully ported from the reviewed refpoly-5d
snapshot, including polar duality and lattice-equivalence results.
See SOURCES.md for attribution and exact source identities.
-/
import Mathlib

open Set Matrix

namespace Refpoly

/-- The ambient real vector space `ℝⁿ`, housing both `M_ℝ` and (via the dot
product pairing) `N_ℝ`. -/
abbrev V (n : ℕ) := Fin n → ℝ

variable {n : ℕ}

/-! ## Lattice points and lattice polytopes -/

/-- A point of `ℝⁿ` is a *lattice point* if all its coordinates are integers
(D0.1). -/
def IsLatticePoint (x : V n) : Prop := ∀ i, ∃ m : ℤ, x i = m

/-- The lattice `ℤⁿ ⊂ ℝⁿ`. -/
def latticePoints (n : ℕ) : Set (V n) := {x | IsLatticePoint x}

theorem isLatticePoint_zero : IsLatticePoint (0 : V n) := fun _ => ⟨0, by simp⟩

/-- The dot product of two lattice points is an integer. -/
theorem IsLatticePoint.dotProduct_int {x y : V n} (hx : IsLatticePoint x)
    (hy : IsLatticePoint y) : ∃ m : ℤ, x ⬝ᵥ y = m := by
  choose a ha using hx
  choose b hb using hy
  refine ⟨∑ i, a i * b i, ?_⟩
  unfold dotProduct
  push_cast
  exact Finset.sum_congr rfl fun i _ => by rw [ha i, hb i]

/-- A *lattice polytope* is the convex hull of a finite set of lattice points
(D0.2).  [SS18] §2.1: "A lattice polytope Δ ⊂ M_ℝ is a polytope, i.e. the
convex hull of a finite number of points, with vertices in M." -/
def IsLatticePolytope (P : Set (V n)) : Prop :=
  ∃ S : Finset (V n), (∀ x ∈ S, IsLatticePoint x) ∧ P = convexHull ℝ (S : Set (V n))

/-- The *IP property* (D0.3): the origin is in the interior.  [SS18] §2.1:
"a polytope has the IP property if the origin is in its interior". -/
def IsIP (P : Set (V n)) : Prop := (0 : V n) ∈ interior P

/-- The polar dual (D0.4): `P* = {y | ⟨y,x⟩ + 1 ≥ 0 for all x ∈ P}`.
[KS95] eq. (1), [SS18] §2.1. -/
def polarDual (P : Set (V n)) : Set (V n) := {y | ∀ x ∈ P, -1 ≤ y ⬝ᵥ x}

/-- A polytope is *reflexive* (D0.5) if it is an IP lattice polytope and its
polar dual is again a lattice polytope.  [SS18] §2.1: "An IP polytope Δ is
called reflexive if both Δ and Δ* are lattice polytopes."
The equivalent facet-distance convention is discussed in [KS95]. -/
structure IsReflexive (P : Set (V n)) : Prop where
  latticePolytope : IsLatticePolytope P
  ip : IsIP P
  dual_latticePolytope : IsLatticePolytope (polarDual P)

/-- `[P] = conv(P ∩ ℤⁿ)`, the convex hull of the lattice points of `P`.
[SS18] §6: "a notation in which [Δ] stands for conv(Δ ∩ M)". -/
def latticeHull (P : Set (V n)) : Set (V n) :=
  convexHull ℝ (P ∩ latticePoints n)

/-! ## Elementary properties of lattice polytopes -/

theorem IsLatticePolytope.convex {P : Set (V n)} (h : IsLatticePolytope P) :
    Convex ℝ P := by
  obtain ⟨S, -, rfl⟩ := h
  exact convex_convexHull ℝ _

theorem IsLatticePolytope.isCompact {P : Set (V n)} (h : IsLatticePolytope P) :
    IsCompact P := by
  obtain ⟨S, -, rfl⟩ := h
  exact S.finite_toSet.isCompact_convexHull (𝕜 := ℝ)

theorem IsLatticePolytope.isClosed {P : Set (V n)} (h : IsLatticePolytope P) :
    IsClosed P := h.isCompact.isClosed

/-- A lattice polytope is the convex hull of its own lattice points:
`P = [P]`.  ([SS18] §6: "the first condition Δ = [Δ] just means that Δ is a
lattice polytope".) -/
theorem IsLatticePolytope.eq_latticeHull {P : Set (V n)} (h : IsLatticePolytope P) :
    P = latticeHull P := by
  obtain ⟨S, hS, rfl⟩ := h
  apply le_antisymm
  · apply convexHull_mono
    intro x hx
    exact ⟨subset_convexHull ℝ _ hx, hS x hx⟩
  · exact convexHull_min inter_subset_left (convex_convexHull ℝ _)

theorem latticeHull_mono {P Q : Set (V n)} (h : P ⊆ Q) :
    latticeHull P ⊆ latticeHull Q :=
  convexHull_mono (inter_subset_inter_left _ h)

theorem latticeHull_subset_of_convex {P : Set (V n)} (h : Convex ℝ P) :
    latticeHull P ⊆ P :=
  convexHull_min inter_subset_left h

/-! ## Half-space lemmas

A linear inequality valid on a generating set is valid on the convex hull.
Used throughout (IP-criteria, the descent lemma, L0.9). -/

theorem isLinearMap_dotProduct (w : V n) : IsLinearMap ℝ (fun x : V n => w ⬝ᵥ x) :=
  ⟨fun x y => dotProduct_add w x y, fun c x => dotProduct_smul c w x⟩

/-- If `⟨w, x⟩ ≥ c` for all generators, the inequality holds on the hull. -/
theorem le_dotProduct_of_mem_convexHull {S : Set (V n)} {w : V n} {c : ℝ}
    (h : ∀ x ∈ S, c ≤ w ⬝ᵥ x) {x : V n} (hx : x ∈ convexHull ℝ S) :
    c ≤ w ⬝ᵥ x :=
  convexHull_min h (convex_halfSpace_ge (isLinearMap_dotProduct w) c) hx

/-- If `⟨w, x⟩ ≤ c` for all generators, the inequality holds on the hull. -/
theorem dotProduct_le_of_mem_convexHull {S : Set (V n)} {w : V n} {c : ℝ}
    (h : ∀ x ∈ S, w ⬝ᵥ x ≤ c) {x : V n} (hx : x ∈ convexHull ℝ S) :
    w ⬝ᵥ x ≤ c :=
  convexHull_min h (convex_halfSpace_le (isLinearMap_dotProduct w) c) hx

/-! ## Polar duality: order theory (L0.6, L0.7) -/

/-- **L0.6** Polar duality reverses inclusion.  [SS18] §2.2: "duality inverts
subset relations". -/
theorem polarDual_antitone {P Q : Set (V n)} (h : P ⊆ Q) :
    polarDual Q ⊆ polarDual P :=
  fun _ hy x hx => hy x (h hx)

theorem zero_mem_polarDual (P : Set (V n)) : (0 : V n) ∈ polarDual P :=
  fun x _ => by simp

theorem polarDual_convex (P : Set (V n)) : Convex ℝ (polarDual P) := by
  intro y₁ h₁ y₂ h₂ a b ha hb hab x hx
  have e : (a • y₁ + b • y₂) ⬝ᵥ x = a * (y₁ ⬝ᵥ x) + b * (y₂ ⬝ᵥ x) := by
    simp [add_dotProduct, smul_dotProduct]
  rw [e]
  nlinarith [h₁ x hx, h₂ x hx]

theorem polarDual_isClosed (P : Set (V n)) : IsClosed (polarDual P) := by
  have : polarDual P = ⋂ x ∈ P, {y : V n | -1 ≤ y ⬝ᵥ x} := by
    ext y; simp [polarDual]
  rw [this]
  exact isClosed_biInter fun x _ =>
    isClosed_le continuous_const (continuous_id.dotProduct continuous_const)

/-- `P ⊆ P**` (half of the bipolar theorem; always true). -/
theorem subset_bipolar (P : Set (V n)) : P ⊆ polarDual (polarDual P) := by
  intro x hx y hy
  have := hy x hx
  rwa [dotProduct_comm] at this

/-- **L0.8, bipolar theorem.**  For a closed convex set containing the origin,
`P** = P`.  Proved by Hahn–Banach separation: a point `x ∉ P` is separated
from `P` by a functional `f` with `f < u < f x` on `P`; since `f 0 = 0 < u`,
the rescaled vector `y = -(1/u)·f` lies in `P*` and pairs `< -1` with `x`. -/
theorem bipolar {P : Set (V n)} (hconv : Convex ℝ P) (hcl : IsClosed P)
    (h0 : (0 : V n) ∈ P) : polarDual (polarDual P) = P := by
  refine Subset.antisymm ?_ (subset_bipolar P)
  intro x hx
  by_contra hxP
  obtain ⟨f, u, hfP, hfx⟩ := geometric_hahn_banach_closed_point hconv hcl hxP
  have hu : 0 < u := by simpa using hfP 0 h0
  -- Represent the functional `f` by the vector of its values on basis vectors.
  set w : V n := fun i => f (fun j => if i = j then (1 : ℝ) else 0) with hw
  have hfeq : ∀ z : V n, f z = w ⬝ᵥ z := by
    intro z
    have h := LinearMap.pi_apply_eq_sum_univ (f : (Fin n → ℝ) →ₗ[ℝ] ℝ) z
    simpa [dotProduct, hw, mul_comm] using h
  set y : V n := (-(1 / u)) • w with hy
  have hyP : y ∈ polarDual P := by
    intro a ha
    have h1 : f a < u := hfP a ha
    have e : y ⬝ᵥ a = -(1 / u) * f a := by
      rw [hy, smul_dotProduct, smul_eq_mul, hfeq]
    rw [e]
    rw [neg_mul, neg_le_neg_iff, div_mul_eq_mul_div, div_le_one hu, one_mul]
    exact h1.le
  have hpair : -1 ≤ x ⬝ᵥ y := hx y hyP
  have e : x ⬝ᵥ y = -(1 / u) * f x := by
    rw [dotProduct_comm, hy, smul_dotProduct, smul_eq_mul, hfeq]
  rw [e] at hpair
  have hgt : 1 < f x / u := (one_lt_div hu).2 hfx
  have he2 : -(1 / u) * f x = -(f x / u) := by ring
  rw [he2] at hpair
  linarith

/-! ## The "no maximum at an interior point" lemma

This single lemma powers the IP-criteria for weight systems (L5.6) and the
descent lemma of the enumeration algorithm (L7.4): a linear functional that
attains its maximum over a set at an *interior* point of that set must be
identically zero. -/

/-- If the continuous linear functional `f` satisfies `f x ≤ f x₀` for all
`x ∈ s` and `x₀ ∈ interior s`, then `f = 0`. -/
theorem eq_zero_of_le_on_interior {E : Type*} [NormedAddCommGroup E]
    [NormedSpace ℝ E] (f : E →L[ℝ] ℝ) {s : Set E} {x₀ : E}
    (hx₀ : x₀ ∈ interior s) (hmax : ∀ x ∈ s, f x ≤ f x₀) : f = 0 :=
  IsLocalMax.hasFDerivAt_eq_zero
    (Filter.mem_of_superset (mem_interior_iff_mem_nhds.1 hx₀) hmax) f.hasFDerivAt

/-- A linear functional vanishing on the kernel of another is a scalar multiple
of it.  (Used to conclude `q̃ = q` in the descent lemma: a functional vanishing
on `ker(q ⬝ᵥ ·)` is proportional to `q ⬝ᵥ ·`.) -/
theorem exists_smul_of_ker_le_ker {E : Type*} [AddCommGroup E] [Module ℝ E]
    {f g : E →ₗ[ℝ] ℝ} (h : LinearMap.ker f ≤ LinearMap.ker g) : ∃ c : ℝ, ∀ z, g z = c * f z := by
  have hg : g ∈ Submodule.span ℝ (range fun _ : Unit => f) :=
    mem_span_of_iInf_ker_le_ker (by simpa using h)
  obtain ⟨c, rfl⟩ := Submodule.mem_span_singleton.1 (by simpa [range_const] using hg)
  exact ⟨c, fun z => by simp⟩

/-! ## Finiteness of lattice points in bounded sets -/

/-- The set of lattice points in a bounded set is finite.  (Geometry-of-numbers
counting: integer points in a box are finite.) -/
theorem finite_latticePoints_of_isBounded {P : Set (V n)}
    (h : Bornology.IsBounded P) : (P ∩ latticePoints n).Finite := by
  obtain ⟨R, hR⟩ := h.exists_norm_le
  classical
  -- integer vectors in the box `[-⌈R⌉, ⌈R⌉]ⁿ`
  set B : Set (Fin n → ℤ) := Set.pi univ fun _ => Set.Icc (-⌈R⌉) ⌈R⌉ with hB
  have hBfin : B.Finite := Set.Finite.pi fun _ => Set.finite_Icc _ _
  have himg : (P ∩ latticePoints n) ⊆ (fun m : Fin n → ℤ => fun i => (m i : ℝ)) '' B := by
    rintro x ⟨hxP, hxL⟩
    choose m hm using hxL
    refine ⟨m, ?_, by funext i; exact (hm i).symm⟩
    intro i _
    have hxi : |x i| ≤ R := by
      calc |x i| = ‖x i‖ := (Real.norm_eq_abs _).symm
        _ ≤ ‖x‖ := norm_le_pi_norm x i
        _ ≤ R := hR x hxP
    have : |(m i : ℝ)| ≤ (⌈R⌉ : ℝ) := by
      rw [← hm i] at *
      exact hxi.trans (Int.le_ceil R)
    rw [← Int.cast_abs] at this
    have habs : |m i| ≤ ⌈R⌉ := by exact_mod_cast this
    exact Set.mem_Icc.mpr ⟨neg_le_of_abs_le habs, le_of_abs_le habs⟩
  exact (hBfin.image _).subset himg

/-- The lattice points of a lattice polytope form a finite set. -/
theorem IsLatticePolytope.finite_latticePoints {P : Set (V n)}
    (h : IsLatticePolytope P) : (P ∩ latticePoints n).Finite :=
  finite_latticePoints_of_isBounded h.isCompact.isBounded

/-! ## `GL(n,ℤ)` equivalence -/

/-- A real linear automorphism is *unimodular* if it and its inverse preserve
the integer lattice.  These are exactly the elements of `GL(n,ℤ)` acting on
`ℝⁿ`. -/
structure IsUnimodular (g : V n ≃ₗ[ℝ] V n) : Prop where
  fwd : ∀ x, IsLatticePoint x → IsLatticePoint (g x)
  bwd : ∀ x, IsLatticePoint x → IsLatticePoint (g.symm x)

/-- Two subsets of `ℝⁿ` are `GL(n,ℤ)`-equivalent if a unimodular automorphism
maps one onto the other.  ([KS95] §1: "equivalences of polyhedra that are
mapped to each other by GL(n,ℤ) transformations".) -/
def GLEquiv (P Q : Set (V n)) : Prop :=
  ∃ g : V n ≃ₗ[ℝ] V n, IsUnimodular g ∧ g '' P = Q

theorem GLEquiv.refl (P : Set (V n)) : GLEquiv P P :=
  ⟨LinearEquiv.refl ℝ (V n), ⟨fun _ h => h, fun _ h => h⟩, image_id P⟩

theorem GLEquiv.symm {P Q : Set (V n)} (h : GLEquiv P Q) : GLEquiv Q P := by
  obtain ⟨g, hg, rfl⟩ := h
  refine ⟨g.symm, ⟨hg.bwd, by simpa using hg.fwd⟩, ?_⟩
  rw [← image_comp]
  simp

theorem GLEquiv.trans {P Q R : Set (V n)} (h₁ : GLEquiv P Q) (h₂ : GLEquiv Q R) :
    GLEquiv P R := by
  obtain ⟨g₁, hg₁, rfl⟩ := h₁
  obtain ⟨g₂, hg₂, rfl⟩ := h₂
  refine ⟨g₁.trans g₂, ⟨fun x hx => hg₂.fwd _ (hg₁.fwd x hx),
    fun x hx => hg₁.bwd _ (hg₂.bwd x hx)⟩, ?_⟩
  rw [← image_comp]
  rfl

/-- A unimodular map restricts to a bijection on lattice points, hence
`GL(n,ℤ)`-equivalent sets have equinumerous lattice points.  (This is why the
lattice point count is an invariant used for the bounds of Part 1.) -/
theorem GLEquiv.image_lattice {P Q : Set (V n)} (h : GLEquiv P Q) :
    ∃ g : V n ≃ₗ[ℝ] V n, IsUnimodular g ∧
      g '' (P ∩ latticePoints n) = Q ∩ latticePoints n := by
  obtain ⟨g, hg, rfl⟩ := h
  refine ⟨g, hg, ?_⟩
  apply Subset.antisymm
  · rintro y ⟨x, ⟨hxP, hxL⟩, rfl⟩
    exact ⟨mem_image_of_mem g hxP, hg.fwd x hxL⟩
  · rintro y ⟨⟨x, hxP, rfl⟩, hyL⟩
    refine ⟨x, ⟨hxP, ?_⟩, rfl⟩
    have := hg.bwd _ hyL
    simpa using this

theorem GLEquiv.ncard_lattice_eq {P Q : Set (V n)} (h : GLEquiv P Q) :
    (P ∩ latticePoints n).ncard = (Q ∩ latticePoints n).ncard := by
  obtain ⟨g, _, himg⟩ := h.image_lattice
  rw [← himg]
  exact (Set.ncard_image_of_injective _ g.injective).symm

/-- The image of a lattice polytope under a unimodular map is a lattice
polytope. -/
theorem GLEquiv.isLatticePolytope {P Q : Set (V n)} (h : GLEquiv P Q)
    (hP : IsLatticePolytope P) : IsLatticePolytope Q := by
  classical
  obtain ⟨g, hg, rfl⟩ := h
  obtain ⟨S, hS, rfl⟩ := hP
  refine ⟨S.image g, fun y hy => ?_, ?_⟩
  · obtain ⟨x, hx, rfl⟩ := Finset.mem_image.1 hy
    exact hg.fwd x (hS x hx)
  · rw [Finset.coe_image]
    exact (g : V n →ₗ[ℝ] V n).image_convexHull _

/-! ## Uniqueness of the interior lattice point (L0.9) -/

/-- **L0.9.**  A reflexive polytope contains exactly one interior lattice
point, namely the origin.  [KS95] §2 / [Sk96] §2 ("**1** is the only integer
point in the interior of Q").

Proof: if `x ≠ 0` is an interior lattice point then `(1+ε)x ∈ P` for small
`ε > 0`, so every `y ∈ P*` pairs `> -1` with `x`; for *lattice* `y` the
pairing is an integer, hence `≥ 0`.  Since `P` is reflexive, `P*` is the hull
of its lattice points, so `⟨y, x⟩ ≥ 0` for *all* `y ∈ P*`; then the whole ray
`t·x (t>0)` lies in `P** = P`, contradicting compactness. -/
theorem interior_latticePoint_unique {P : Set (V n)} (hP : IsReflexive P) :
    interior P ∩ latticePoints n = {0} := by
  apply Subset.antisymm
  · rintro x ⟨hxInt, hxL⟩
    by_contra hx0
    have hxne : x ≠ 0 := fun h => hx0 (by simp [h])
    -- (1+ε)·x ∈ P for some ε > 0
    obtain ⟨δ, hδ, hball⟩ := Metric.isOpen_iff.1 isOpen_interior x hxInt
    have hxnorm : 0 < ‖x‖ := norm_pos_iff.2 hxne
    set ε : ℝ := δ / (2 * ‖x‖) with hε
    have hεpos : 0 < ε := by positivity
    have hxne' : ‖x‖ ≠ 0 := ne_of_gt hxnorm
    have hmem : (1 + ε) • x ∈ P := by
      apply interior_subset
      apply hball
      rw [Metric.mem_ball, dist_eq_norm]
      have hsub : (1 + ε) • x - x = ε • x := by module
      rw [hsub, norm_smul, Real.norm_eq_abs, abs_of_pos hεpos, hε]
      have h2 : δ / (2 * ‖x‖) * ‖x‖ = δ / 2 := by field_simp
      rw [h2]
      linarith
    -- every lattice point of P* pairs ≥ 0 with x
    have hpair : ∀ y ∈ polarDual P ∩ latticePoints n, 0 ≤ y ⬝ᵥ x := by
      rintro y ⟨hyP, hyL⟩
      have h1 : -1 ≤ y ⬝ᵥ ((1 + ε) • x) := hyP _ hmem
      have h2 : y ⬝ᵥ ((1 + ε) • x) = (1 + ε) * (y ⬝ᵥ x) := by
        rw [dotProduct_smul]; simp
      rw [h2] at h1
      have h3 : -1 < y ⬝ᵥ x := by nlinarith
      obtain ⟨m, hm⟩ := hyL.dotProduct_int hxL
      rw [hm] at h3 ⊢
      have hm' : (-1 : ℤ) < m := by exact_mod_cast h3
      have hm0 : (0 : ℤ) ≤ m := by omega
      exact_mod_cast hm0
    -- hence every y ∈ P* pairs ≥ 0 with x  (P* is the hull of its lattice pts)
    have hall : ∀ y ∈ polarDual P, 0 ≤ x ⬝ᵥ y := by
      intro y hy
      have hrw : polarDual P = convexHull ℝ (polarDual P ∩ latticePoints n) :=
        hP.dual_latticePolytope.eq_latticeHull
      rw [hrw] at hy
      have hgen : ∀ z ∈ polarDual P ∩ latticePoints n, (0 : ℝ) ≤ x ⬝ᵥ z := by
        intro z hz
        rw [dotProduct_comm]
        exact hpair z hz
      exact le_dotProduct_of_mem_convexHull hgen hy
    -- the ray t·x lies in P** = P, contradicting boundedness
    have hray : ∀ t : ℝ, 0 < t → t • x ∈ P := by
      intro t ht
      have hmem' : t • x ∈ polarDual (polarDual P) := by
        intro y hy
        have h0 : 0 ≤ (t • x) ⬝ᵥ y := by
          rw [smul_dotProduct, smul_eq_mul]
          exact mul_nonneg ht.le (hall y hy)
        linarith
      rwa [bipolar hP.latticePolytope.convex hP.latticePolytope.isClosed
        (interior_subset hP.ip)] at hmem'
    obtain ⟨R, hR⟩ := hP.latticePolytope.isCompact.isBounded.exists_norm_le
    have hR0 : (0 : ℝ) ≤ R := by simpa using hR 0 (interior_subset hP.ip)
    have htpos : 0 < (R + 1) / ‖x‖ + 1 := by
      have := div_nonneg (show (0 : ℝ) ≤ R + 1 by linarith) hxnorm.le
      linarith
    have hcontra := hR _ (hray ((R + 1) / ‖x‖ + 1) htpos)
    rw [norm_smul, Real.norm_eq_abs, abs_of_pos htpos] at hcontra
    have hexp : ((R + 1) / ‖x‖ + 1) * ‖x‖ = R + 1 + ‖x‖ := by
      rw [add_mul, one_mul, div_mul_cancel₀ _ hxne']
    rw [hexp] at hcontra
    linarith
  · rintro x rfl
    exact ⟨hP.ip, isLatticePoint_zero⟩

end Refpoly
