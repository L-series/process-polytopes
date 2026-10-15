import Refpoly.KS.RationalSimplex
import Refpoly.KS.Assembly
import Refpoly.KS.Lattices
import Refpoly.CircuitInterior

/-!
Finite exact convex-hull membership uses positive relations on integer-translated
affinely independent simplexes. Vertex extraction excludes a supplied point
precisely when it belongs to the hull of the other distinct supplied points.
-/

open Set Matrix

namespace Refpoly.KS

theorem ofInt_injective {d : ℕ} : Function.Injective (@ofInt d) := by
  intro x y h
  ext i
  have hi : (x i : ℝ) = (y i : ℝ) := congrFun h i
  exact_mod_cast hi

theorem pointSet_finite {d : ℕ} (xs : List (IntPoint d)) :
    (pointSet xs).Finite :=
  xs.finite_toSet.image ofInt

theorem ofInt_sub {d : ℕ} (x y : IntPoint d) :
    ofInt (x - y) = ofInt x - ofInt y := by
  ext i
  simp [ofInt]

theorem integer_simplex_witness {d : ℕ} (xs : List (IntPoint d)) {x : V d}
    (hx : x ∈ convexHull ℝ (pointSet xs)) :
    ∃ m, m ≤ d + 1 ∧ ∃ v : Fin m → IntPoint d,
      (∀ i, v i ∈ xs) ∧ AffineIndependent ℝ (fun i => ofInt (v i)) ∧
      ∃ q : V m, (∀ i, 0 < q i) ∧ (∑ i, q i = 1) ∧
        ∑ i, q i • ofInt (v i) = x := by
  classical
  obtain ⟨ι, _, z, w, hz, hi, hw, hsum, hrel⟩ :=
    eq_pos_convex_span_of_mem_convexHull hx
  let e := Fintype.equivFin ι
  obtain ⟨v, hv, hev⟩ :
      ∃ v : Fin (Fintype.card ι) → IntPoint d,
        (∀ i, v i ∈ xs) ∧ ∀ i, ofInt (v i) = z (e.symm i) := by
    have h (i : Fin (Fintype.card ι)) := hz (mem_range_self (e.symm i))
    choose v hv hev using h
    exact ⟨v, hv, hev⟩
  have hm : Fintype.card ι ≤ d + 1 := by
    apply hi.card_le_finrank_succ.trans
    apply Nat.add_le_add_right
    simpa [V] using Submodule.finrank_le (vectorSpan ℝ (range z))
  refine ⟨Fintype.card ι, hm, v, hv, ?_, fun i => w (e.symm i), ?_, ?_, ?_⟩
  · simpa only [hev] using hi.comp_embedding e.symm.toEmbedding
  · exact fun i => hw (e.symm i)
  · exact (e.symm.sum_comp w).trans hsum
  · simpa only [hev] using
      (e.symm.sum_comp (fun i => w i • z i)).trans hrel

/-- Exact convex-hull membership; all loops are finite, including on degenerate inputs. -/
def hullContainsExact {d : ℕ} (xs : List (IntPoint d)) (x : IntPoint d) : Bool :=
  (List.range (d + 2)).any fun m =>
    (tuples xs m).any fun v => positiveSimplexCheck (fun i => v i - x)

theorem hullContainsExact_correct {d : ℕ} (xs : List (IntPoint d)) (x : IntPoint d) :
    hullContainsExact xs x = true ↔ ofInt x ∈ convexHull ℝ (pointSet xs) := by
  simp only [hullContainsExact, List.any_eq_true, List.mem_range, mem_tuples,
    positiveSimplexCheck_iff]
  constructor
  · rintro ⟨m, _, v, hv, _, q, hq, hsum, hrel⟩
    have heq : ∑ i, q i • ofInt (v i) = ofInt x := by
      simp only [ofInt_sub, smul_sub, Finset.sum_sub_distrib, ← Finset.sum_smul,
        hsum, one_smul] at hrel
      exact sub_eq_zero.mp hrel
    rw [← heq]
    exact (convex_convexHull ℝ _).sum_mem
      (fun i _ => (hq i).le) hsum
      (fun i _ => subset_convexHull ℝ _ ⟨v i, hv i, rfl⟩)
  · intro hx
    obtain ⟨m, hm, v, hv, hi, q, hq, hsum, hrel⟩ := integer_simplex_witness xs hx
    refine ⟨m, by omega, v, hv, ?_, q, hq, hsum, ?_⟩
    · convert hi.vadd ℝ (v := -ofInt x) using 1
      ext i j
      simp [ofInt, sub_eq_add_neg, add_comm]
    · simp only [ofInt_sub, smul_sub, Finset.sum_sub_distrib, ← Finset.sum_smul,
        hsum, one_smul, hrel, sub_self]

theorem extremePoint_hull_iff {d : ℕ} (xs : List (IntPoint d)) (x : V d) :
    x ∈ (convexHull ℝ (pointSet xs)).extremePoints ℝ ↔
      x ∈ pointSet xs ∧ x ∉ convexHull ℝ (pointSet xs \ {x}) := by
  classical
  let s := pointSet xs
  have hfinite := pointSet_finite xs
  constructor
  · intro hx
    refine ⟨extremePoints_convexHull_subset hx, fun h => ?_⟩
    have hsub : convexHull ℝ (s \ {x}) ⊆ convexHull ℝ s :=
      convexHull_mono diff_subset
    have hext := inter_extremePoints_subset_extremePoints_of_subset hsub ⟨h, hx⟩
    exact (extremePoints_convexHull_subset hext).2 rfl
  · rintro ⟨hx, hnot⟩
    by_contra hext
    have hsub : (convexHull ℝ s).extremePoints ℝ ⊆ s \ {x} := by
      intro y hy
      refine ⟨extremePoints_convexHull_subset hy, ?_⟩
      rintro (rfl : y = x)
      exact hext hy
    have hP : convexHull ℝ s ⊆ convexHull ℝ (s \ {x}) := by
      rw [← closure_convexHull_extremePoints (hfinite.isCompact_convexHull ℝ)
        (convex_convexHull ℝ s)]
      exact closure_minimal (convexHull_mono hsub)
        ((hfinite.subset diff_subset).isClosed_convexHull ℝ)
    exact hnot (hP (subset_convexHull ℝ _ hx))

theorem pointSet_filter_ne {d : ℕ} (xs : List (IntPoint d)) (x : IntPoint d) :
    pointSet (xs.filter (fun y => decide (y ≠ x))) = pointSet xs \ {ofInt x} := by
  ext y
  constructor
  · rintro ⟨z, hz, rfl⟩
    obtain ⟨hz, hne⟩ := List.mem_filter.mp hz
    exact ⟨⟨z, hz, rfl⟩, fun h => of_decide_eq_true hne (ofInt_injective h)⟩
  · rintro ⟨⟨z, hz, rfl⟩, hne⟩
    exact ⟨z, List.mem_filter.mpr ⟨hz, decide_eq_true (fun h => hne (h ▸ rfl))⟩, rfl⟩

/-- Only actual extreme points are returned; repeated supplied points are deduplicated. -/
def actualVerticesExact {d : ℕ} (Q : PolyCode d) : List (IntPoint d) :=
  Q.points.dedup.filter fun x =>
    !(hullContainsExact (Q.points.filter (fun y => decide (y ≠ x))) x)

theorem actualVerticesExact_correct {d : ℕ} (Q : PolyCode d) :
    pointSet (actualVerticesExact Q) = Q.hull.extremePoints ℝ := by
  ext x
  constructor
  · rintro ⟨z, hz, rfl⟩
    obtain ⟨hz, hn⟩ := List.mem_filter.mp hz
    rw [PolyCode.hull, extremePoint_hull_iff]
    refine ⟨⟨z, List.mem_dedup.mp hz, rfl⟩, ?_⟩
    rw [← pointSet_filter_ne, ← hullContainsExact_correct]
    simpa using hn
  · rw [PolyCode.hull, extremePoint_hull_iff]
    rintro ⟨⟨z, hz, rfl⟩, hn⟩
    refine ⟨z, List.mem_filter.mpr ⟨List.mem_dedup.mpr hz, ?_⟩, rfl⟩
    rw [← pointSet_filter_ne, ← hullContainsExact_correct] at hn
    simpa using hn

theorem actualVerticesExact_hull {d : ℕ} (Q : PolyCode d) :
    Q.hull = convexHull ℝ (pointSet (actualVerticesExact Q)) := by
  rw [actualVerticesExact_correct]
  have hextfinite : (Q.hull.extremePoints ℝ).Finite :=
    (pointSet_finite Q.points).subset extremePoints_convexHull_subset
  calc
    Q.hull = closure (convexHull ℝ (Q.hull.extremePoints ℝ)) :=
      (closure_convexHull_extremePoints Q.isLatticePolytope.isCompact
        Q.isLatticePolytope.convex).symm
    _ = _ := (hextfinite.isClosed_convexHull ℝ).closure_eq

end Refpoly.KS
