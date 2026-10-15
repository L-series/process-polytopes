/-
Finiteness faithfully ported from the reviewed refpoly-5d snapshot.
See SOURCES.md for attribution and exact source identities.
Bounds remain existential and reflexive-only; the finite GL-cover is not
an executable classification or a unique census.
-/
import Refpoly.BoxReduction
import Refpoly.ReflexiveVolume

open Set

namespace Refpoly

variable {n : ℕ}

/-- **T1.1 (uniform volume bound for reflexive polytopes).**

This proves the reflexive specialization of the Hensley/Lagarias--Ziegler
finiteness theorem, with an existential dimension-dependent constant. It does
not claim the explicit numerical estimate for general one-interior-point
lattice polytopes. Positive circuits through dual generators produce IP
weights; their finite set supplies a uniform lower bound on coefficients.
This bounds all generator pairings. Independent integral dual normals map
`P` into a fixed box, with determinant of absolute value at least one. -/
theorem hensley_volume_bound (n : ℕ) :
    ∃ C : ℝ, ∀ P : Set (V n), IsReflexive P →
      (MeasureTheory.volume P).toReal ≤ C := by
  exact ReflexiveVolume.volume_bound n

/-- **T1.3 (Lagarias–Ziegler box lemma) — derived from the volume bound.**

[LZ91] Theorem 2: a lattice polytope of volume `≤ V` with an interior lattice
point (placed at the origin) is mapped by some `GL(n,ℤ)` transformation into
the cube `[-R, R]ⁿ` where `R = R(n, V)` is explicit.  Combined with T1.1 this
puts every reflexive polytope, up to `GL(n,ℤ)`, inside one fixed finite box.

`BoxReduction.box_of_volume_bound` proves the geometric reduction using a
maximal-determinant tuple of lattice generators and the finiteness of row
lattices of bounded determinant. It introduces no unproved inputs. This
specialization uses only T1.1; the radius here is existential. -/
theorem lagarias_ziegler_box (n : ℕ) :
    ∃ R : ℕ, ∀ P : Set (V n), IsReflexive P →
      ∃ Q : Set (V n), GLEquiv P Q ∧ Q ⊆ {x | ∀ i, |x i| ≤ (R : ℝ)} := by
  obtain ⟨C, hC⟩ := hensley_volume_bound n
  obtain ⟨R, hR⟩ := BoxReduction.box_of_volume_bound n C
  exact ⟨R, fun P hP => hR P hP.latticePolytope hP.ip (hC P hP)⟩

/-- The box `[-R,R]ⁿ` is bounded. -/
theorem isBounded_box (R : ℕ) :
    Bornology.IsBounded {x : V n | ∀ i, |x i| ≤ (R : ℝ)} := by
  apply Bornology.IsBounded.subset
    (Metric.isBounded_closedBall (x := (0 : V n)) (r := R))
  intro x hx
  rw [Metric.mem_closedBall, dist_zero_right,
    pi_norm_le_iff_of_nonneg (by positivity)]
  intro i
  rw [Real.norm_eq_abs]
  exact hx i

/-- **T1.2 (lattice point count bound)** — *derived* from the box lemma: the
number of lattice points of a reflexive polytope is bounded by the number of
lattice points of the box `[-R,R]ⁿ`, since unimodular maps biject lattice
points (`GLEquiv.ncard_lattice_eq`). -/
theorem latticePoint_count_bound (n : ℕ) :
    ∃ N : ℕ, ∀ P : Set (V n), IsReflexive P →
      (P ∩ latticePoints n).ncard ≤ N := by
  obtain ⟨R, hR⟩ := lagarias_ziegler_box n
  set B : Set (V n) := {x | ∀ i, |x i| ≤ (R : ℝ)} with hB
  have hBfin : (B ∩ latticePoints n).Finite :=
    finite_latticePoints_of_isBounded (isBounded_box R)
  refine ⟨(B ∩ latticePoints n).ncard, fun P hP => ?_⟩
  obtain ⟨Q, hgl, hsub⟩ := hR P hP
  rw [hgl.ncard_lattice_eq]
  exact Set.ncard_le_ncard (inter_subset_inter_left _ hsub) hBfin

/-- Lattice polytopes with the same lattice points are equal. -/
theorem IsLatticePolytope.eq_of_latticePoints_eq {P Q : Set (V n)} (hP : IsLatticePolytope P)
    (hQ : IsLatticePolytope Q) (h : P ∩ latticePoints n = Q ∩ latticePoints n) : P = Q := by
  rw [hP.eq_latticeHull, hQ.eq_latticeHull, latticeHull, latticeHull, h]

/-- **L1.5 (strict monotonicity of the lattice point count).**  A lattice
polytope properly containing another has strictly more lattice points.  Key
step: a lattice polytope is the hull of its lattice points
(`IsLatticePolytope.eq_latticeHull`), so equal lattice point sets force equal
polytopes. -/
theorem ncard_lt_of_ssubset {P Q : Set (V n)} (hP : IsLatticePolytope P)
    (hQ : IsLatticePolytope Q) (h : P ⊂ Q) :
    (P ∩ latticePoints n).ncard < (Q ∩ latticePoints n).ncard := by
  refine Set.ncard_lt_ncard ?_ hQ.finite_latticePoints
  exact (inter_subset_inter_left _ h.subset).ssubset_of_ne
    fun he => h.ne (hP.eq_of_latticePoints_eq hQ he)

/-- **T1.4 (Finiteness; Lagarias–Ziegler).**  Up to `GL(n,ℤ)`, there are only
finitely many reflexive polytopes in each dimension: there is a *finite* set
`T` of polytopes such that every reflexive polytope is `GL(n,ℤ)`-equivalent
to a member of `T`.

Derivation: by the box lemma, every reflexive polytope is equivalent to a
lattice polytope inside `[-R,R]ⁿ`; lattice polytopes inside the box are
determined by their lattice point sets, which are subsets of the *finite* set
`[-R,R]ⁿ ∩ ℤⁿ`. -/
theorem finitely_many_reflexive (n : ℕ) :
    ∃ T : Set (Set (V n)), T.Finite ∧
      ∀ P : Set (V n), IsReflexive P → ∃ Q ∈ T, GLEquiv P Q := by
  obtain ⟨R, hR⟩ := lagarias_ziegler_box n
  set B : Set (V n) := {x | ∀ i, |x i| ≤ (R : ℝ)} with hB
  have hBfin : (B ∩ latticePoints n).Finite :=
    finite_latticePoints_of_isBounded (isBounded_box R)
  set T : Set (Set (V n)) := {Q | IsLatticePolytope Q ∧ Q ⊆ B} with hT
  refine ⟨T, ?_, ?_⟩
  · -- T is finite: `Q ↦ Q ∩ ℤⁿ` is injective on lattice polytopes and lands
    -- in the (finite) powerset of the finite set `B ∩ ℤⁿ`.
    have hinj : Set.InjOn (fun Q => Q ∩ latticePoints n) T :=
      fun _ h₁ _ h₂ he => h₁.1.eq_of_latticePoints_eq h₂.1 he
    apply Set.Finite.of_finite_image ?_ hinj
    apply Set.Finite.subset hBfin.finite_subsets
    rintro s ⟨Q, ⟨-, hQB⟩, rfl⟩
    exact inter_subset_inter_left _ hQB
  · intro P hP
    obtain ⟨Q, hgl, hsub⟩ := hR P hP
    exact ⟨Q, ⟨hgl.isLatticePolytope hP.latticePolytope, hsub⟩, hgl⟩

end Refpoly
