/-
CircuitInterior faithfully ported from the reviewed refpoly-5d snapshot.
See SOURCES.md for attribution and exact source identities.
-/
import Refpoly.CircuitGeometry
import Refpoly.WeightSystem

open Set Module Matrix
namespace Refpoly

/-- If the origin lies in the affine span of `s`, that affine span is the
linear span of `s`. -/
theorem affineSpan_eq_span_of_zero_mem {n : ℕ} {s : Set (V n)}
    (h0 : (0 : V n) ∈ affineSpan ℝ s) :
    affineSpan ℝ s = (Submodule.span ℝ s).toAffineSubspace := by
  apply SetLike.coe_injective
  rw [← affineSpan_insert_eq_affineSpan ℝ h0, affineSpan_insert_zero]
  rfl

/-- In a full-dimensional set, intrinsic-interior points are interior points. -/
theorem mem_interior_of_mem_intrinsicInterior {n : ℕ} {s : Set (V n)} {x : V n}
    (hs : affineSpan ℝ s = ⊤) (hx : x ∈ intrinsicInterior ℝ s) : x ∈ interior s := by
  have hopen : IsOpen (affineSpan ℝ s : Set (V n)) := by rw [hs]; exact isOpen_univ
  exact interior_mono (Set.image_preimage_subset _ _)
    (hopen.isOpenMap_subtype_val.image_interior_subset _ hx)

/-- A strictly positive normalized relation puts the origin in the intrinsic
interior of the hull, even when the family is not affinely independent. -/
theorem positive_relation_intrinsicInterior {n m : ℕ} (v : Fin m → V n) (q : V m)
    (hqpos : ∀ i, 0 < q i) (hqsum : ∑ i, q i = 1)
    (hcirc : ∑ i, q i • v i = 0) :
    (0 : V n) ∈ intrinsicInterior ℝ (convexHull ℝ (Set.range v)) := by
  classical
  let K := Submodule.span ℝ (Set.range v)
  let L : V m →ₗ[ℝ] V n := Fintype.linearCombination ℝ v
  have hL (a : V m) : L a = ∑ i, a i • v i := rfl
  have hK : LinearMap.range L = K := Fintype.range_linearCombination ℝ v
  have h0conv : (0 : V n) ∈ convexHull ℝ (Set.range v) := by
    have h := Finset.univ.centerMass_mem_convexHull (z := v) (s := Set.range v)
      (fun i _ => (hqpos i).le)
      (hqsum.symm ▸ zero_lt_one) (fun i _ => Set.mem_range_self i)
    rwa [Finset.centerMass_eq_of_sum_1 _ _ hqsum, hcirc] at h
  have hAeq : affineSpan ℝ (Set.range v) = K.toAffineSubspace :=
    affineSpan_eq_span_of_zero_mem (convexHull_subset_affineSpan _ h0conv)
  let H := wsHyperplane (ones m)
  have hmem : ∀ a : H, L a ∈ K := fun a => hK ▸ LinearMap.mem_range_self L a
  let F : H →ₗ[ℝ] K := (L.domRestrict H).codRestrict K hmem
  have hFsurj : Function.Surjective F := by
    rintro ⟨x, hx⟩
    rw [← hK] at hx
    obtain ⟨a, ha⟩ := hx
    let b : V m := fun i => a i - (∑ j, a j) * q i
    have hb : b ∈ H := by
      rw [mem_wsHyperplane, dotProduct_comm, dotProduct_ones]
      simp only [b, Finset.sum_sub_distrib, ← Finset.mul_sum, hqsum, mul_one, sub_self]
    have hLb : L b = x := by
      rw [hL]
      simp only [b, sub_smul, mul_smul, Finset.sum_sub_distrib, ← Finset.smul_sum,
        hcirc, smul_zero, sub_zero]
      exact ha
    exact ⟨⟨b, hb⟩, Subtype.ext hLb⟩
  let U : Set H := {a | ∀ i, -q i < (a : V m) i}
  have hUopen : IsOpen U := by
    simp only [U, setOf_forall]
    apply isOpen_iInter_of_finite
    intro i
    exact isOpen_lt continuous_const ((continuous_apply i).comp continuous_subtype_val)
  have h0U : (0 : H) ∈ U := by intro i; simpa using hqpos i
  have hsub : F '' U ⊆ ((↑) ⁻¹' convexHull ℝ (Set.range v) : Set K) := by
    rintro _ ⟨a, ha, rfl⟩
    let w : V m := fun i => q i + (a : V m) i
    have hw : ∀ i, 0 ≤ w i := fun i => by have := ha i; dsimp [w]; linarith
    have hasum : ∑ i, (a : V m) i = 0 := by
      have h := a.property
      rwa [mem_wsHyperplane, dotProduct_comm, dotProduct_ones] at h
    have hwsum : ∑ i, w i = 1 := by
      simp only [w, Finset.sum_add_distrib, hqsum, hasum, add_zero]
    have hrel : ∑ i, w i • v i = L a := by
      simp only [w, add_smul, Finset.sum_add_distrib, hcirc, zero_add, hL]
    have h := Finset.univ.centerMass_mem_convexHull (z := v) (s := Set.range v)
      (fun i _ => hw i)
      (hwsum.symm ▸ zero_lt_one) (fun i _ => Set.mem_range_self i)
    rwa [Finset.centerMass_eq_of_sum_1 _ _ hwsum, hrel] at h
  have hopen : IsOpenMap F := LinearMap.isOpenMap_of_finiteDimensional F hFsurj
  have h0int : (0 : K) ∈ interior ((↑) ⁻¹' convexHull ℝ (Set.range v) : Set K) :=
    interior_maximal hsub (hopen U hUopen) ⟨0, h0U, map_zero F⟩
  unfold intrinsicInterior
  rw [affineSpan_convexHull, hAeq]
  exact ⟨⟨0, K.zero_mem⟩, h0int, rfl⟩

/-- Finset form of the positive-relation interior criterion. -/
theorem finset_positive_relation_intrinsicInterior {n : ℕ}
    (T : Finset (V n)) (w : V n → ℝ) (hw : ∀ x ∈ T, 0 < w x)
    (hwsum : ∑ x ∈ T, w x = 1) (hwrel : ∑ x ∈ T, w x • x = 0) :
    (0 : V n) ∈ intrinsicInterior ℝ (convexHull ℝ (T : Set (V n))) := by
  classical
  let e : T ≃ Fin T.card := Fintype.equivFinOfCardEq (by simp)
  let v : Fin T.card → V n := fun i => (e.symm i : V n)
  let q : V T.card := fun i => w (v i)
  have hqpos : ∀ i, 0 < q i := fun i => hw _ (e.symm i).property
  have hqsum : ∑ i, q i = 1 :=
    (e.symm.sum_comp (fun x : T => w x)).trans ((Finset.sum_coe_sort T w).trans hwsum)
  have hcirc : ∑ i, q i • v i = 0 :=
    (e.symm.sum_comp (fun x : T => w x • (x : V n))).trans
      ((Finset.sum_coe_sort T fun x => w x • x).trans hwrel)
  have hrange : Set.range v = (T : Set (V n)) := by
    ext x
    constructor
    · rintro ⟨i, rfl⟩; exact (e.symm i).property
    · intro hx; exact ⟨e ⟨x, hx⟩, by simp [v]⟩
  rw [← hrange]
  exact positive_relation_intrinsicInterior v q hqpos hqsum hcirc

/-- Converse to the positive-relation criterion for a finite generating set. -/
theorem exists_positive_relation_of_intrinsicInterior {n : ℕ} (S : Finset (V n))
    (h0 : (0 : V n) ∈ intrinsicInterior ℝ (convexHull ℝ (S : Set (V n)))) :
    ∃ w : V n → ℝ, (∀ x ∈ S, 0 < w x) ∧
      (∑ x ∈ S, w x = 1) ∧ ∑ x ∈ S, w x • x = 0 := by
  classical
  let A := affineSpan ℝ (S : Set (V n))
  have h0A : (0 : V n) ∈ A :=
    convexHull_subset_affineSpan _ (intrinsicInterior_subset h0)
  have hvertices : ∀ x ∈ S, x ∈ A.direction := by
    intro x hx
    have h := A.vsub_mem_direction (subset_affineSpan ℝ _ hx) h0A
    simpa using h
  let c : V n := ∑ x ∈ S, x
  have hc : c ∈ A.direction := Submodule.sum_mem _ (fun x hx => hvertices x hx)
  unfold intrinsicInterior at h0
  rw [affineSpan_convexHull] at h0
  obtain ⟨z, hz, hz0⟩ := h0
  obtain ⟨ε, hε, hball⟩ := Metric.mem_nhds_iff.mp (mem_interior_iff_mem_nhds.mp hz)
  let t : ℝ := ε / (2 * (‖c‖ + 1))
  have hd : 0 < 2 * (‖c‖ + 1) := by positivity
  have ht : 0 < t := div_pos hε hd
  have htc : -t • c ∈ A := by
    have h := A.vadd_mem_of_mem_direction (A.direction.smul_mem (-t) hc) h0A
    simpa using h
  have hpoint : -t • c ∈ convexHull ℝ (S : Set (V n)) := by
    apply hball (a := ⟨-t • c, htc⟩)
    rw [Metric.mem_ball, Subtype.dist_eq, hz0, dist_zero_right, norm_smul,
      Real.norm_eq_abs, abs_neg, abs_of_pos ht]
    dsimp [t]
    rw [div_mul_eq_mul_div, div_lt_iff₀ hd]
    nlinarith [norm_nonneg c]
  rw [Finset.convexHull_eq] at hpoint
  obtain ⟨f, hf, hsum, hrel⟩ := hpoint
  rw [Finset.centerMass_eq_of_sum_1 _ id hsum] at hrel
  simp only [id_eq] at hrel
  let D : ℝ := 1 + (S.card : ℝ) * t
  have hD : 0 < D := by dsimp [D]; positivity
  let w : V n → ℝ := fun x => D⁻¹ * f x + D⁻¹ * t
  refine ⟨w, ?_, ?_, ?_⟩
  · intro x hx
    exact add_pos_of_nonneg_of_pos
      (mul_nonneg (inv_nonneg.mpr hD.le) (hf x hx)) (mul_pos (inv_pos.mpr hD) ht)
  · simp only [w, Finset.sum_add_distrib, ← Finset.mul_sum, hsum, mul_one,
      Finset.sum_const, nsmul_eq_mul]
    dsimp [D]
    field_simp
  · simp only [w, add_smul, mul_smul, Finset.sum_add_distrib, ← Finset.smul_sum,
      hrel]
    rw [← Finset.smul_sum]
    change D⁻¹ • (-t • c) + D⁻¹ • (t • c) = 0
    rw [← smul_add, ← add_smul, neg_add_cancel, zero_smul, smul_zero]

/-- A positive zero relation makes the nonnegative cone equal to the span. -/
theorem nonnegative_combination_of_mem_span {n : ℕ} (S : Finset (V n))
    (w : V n → ℝ) (hw : ∀ y ∈ S, 0 < w y) (hrel : ∑ y ∈ S, w y • y = 0)
    (x : V n) (hx : x ∈ Submodule.span ℝ (S : Set (V n))) :
    ∃ f : V n → ℝ, (∀ y ∈ S, 0 ≤ f y) ∧ ∑ y ∈ S, f y • y = x := by
  classical
  obtain ⟨g, hg⟩ := Submodule.mem_span_finset'.mp hx
  obtain ⟨N, hN⟩ := (Set.finite_range (fun y : S => -g y / w y)).bddAbove
  let f : V n → ℝ := fun y => if h : y ∈ S then g ⟨y, h⟩ + N * w y else 0
  refine ⟨f, ?_, ?_⟩
  · intro y hy
    have h := (div_le_iff₀ (hw y hy)).mp (hN ⟨⟨y, hy⟩, rfl⟩)
    dsimp [f]; rw [dif_pos hy]; linarith
  · rw [← Finset.sum_attach (f := fun y : V n => f y • y)]
    simp only [f, Finset.coe_mem, dif_pos, add_smul, mul_smul,
      Finset.sum_add_distrib, ← Finset.smul_sum]
    have hrel' : ∑ y ∈ S.attach, w (y : V n) • (y : V n) = 0 := by
      rw [Finset.sum_attach (f := fun y : V n => w y • y)]; exact hrel
    rw [hrel', smul_zero, add_zero]
    simpa only [Finset.univ_eq_attach] using hg

/-- Affine span and linear span agree when the convex hull contains zero. -/
theorem affineSpan_eq_span_of_zero_mem_convexHull {n : ℕ} (S : Finset (V n))
    (h0 : (0 : V n) ∈ convexHull ℝ (S : Set (V n))) :
    affineSpan ℝ (S : Set (V n)) = (Submodule.span ℝ (S : Set (V n))).toAffineSubspace :=
  affineSpan_eq_span_of_zero_mem (convexHull_subset_affineSpan _ h0)

/-- Full-dimensional form of the positive-relation interior criterion. -/
theorem finset_positive_relation_interior {n : ℕ} (S : Finset (V n))
    (w : V n → ℝ) (hw : ∀ y ∈ S, 0 < w y)
    (hsum : ∑ y ∈ S, w y = 1) (hrel : ∑ y ∈ S, w y • y = 0)
    (hspan : Submodule.span ℝ (S : Set (V n)) = ⊤) :
    (0 : V n) ∈ interior (convexHull ℝ (S : Set (V n))) := by
  have hi := finset_positive_relation_intrinsicInterior S w hw hsum hrel
  have h0conv := intrinsicInterior_subset hi
  have hA : affineSpan ℝ (S : Set (V n)) = ⊤ := by
    rw [affineSpan_eq_span_of_zero_mem_convexHull S h0conv, hspan]
    rfl
  exact mem_interior_of_mem_intrinsicInterior (by rwa [affineSpan_convexHull]) hi

/-- Normalize a strictly positive zero relation. -/
theorem normalize_positive_relation {n : ℕ} (S : Finset (V n)) (hS : S.Nonempty)
    (f : V n → ℝ) (hf : ∀ x ∈ S, 0 < f x) (hrel : ∑ x ∈ S, f x • x = 0) :
    ∃ w : V n → ℝ, (∀ x ∈ S, 0 < w x) ∧
      (∑ x ∈ S, w x = 1) ∧ ∑ x ∈ S, w x • x = 0 := by
  classical
  have hsum : 0 < ∑ x ∈ S, f x := Finset.sum_pos' (fun x hx => (hf x hx).le)
    (by obtain ⟨x, hx⟩ := hS; exact ⟨x, hx, hf x hx⟩)
  refine ⟨fun x => (∑ y ∈ S, f y)⁻¹ * f x, fun x hx => mul_pos (inv_pos.mpr hsum) (hf x hx), ?_, ?_⟩
  · rw [← Finset.mul_sum, inv_mul_cancel₀ (ne_of_gt hsum)]
  · simp only [mul_smul, ← Finset.smul_sum, hrel, smul_zero]

/-- Lift a positive relation modulo a relative-IP subset into a positive
relation on the union of the two disjoint generating sets. -/
theorem positive_relation_union_of_cancel {n : ℕ} (M C : Finset (V n))
    (hM : M.Nonempty) (hdisj : Disjoint M C)
    (w : V n → ℝ) (hw : ∀ x ∈ M, 0 < w x) (hwrel : ∑ x ∈ M, w x • x = 0)
    (q : V n → ℝ) (hq : ∀ x ∈ C, 0 < q x)
    (hcancel : -(∑ x ∈ C, q x • x) ∈ Submodule.span ℝ (M : Set (V n))) :
    ∃ f : V n → ℝ, (∀ x ∈ M ∪ C, 0 < f x) ∧
      (∑ x ∈ M ∪ C, f x = 1) ∧ ∑ x ∈ M ∪ C, f x • x = 0 := by
  classical
  obtain ⟨r, hr, hrrel⟩ := nonnegative_combination_of_mem_span M w hw hwrel _ hcancel
  let f : V n → ℝ := fun x => if x ∈ M then r x + w x else q x
  have hf : ∀ x ∈ M ∪ C, 0 < f x := by
    intro x hx
    by_cases hxM : x ∈ M
    · simp only [f, if_pos hxM]; exact add_pos_of_nonneg_of_pos (hr x hxM) (hw x hxM)
    · simp only [f, if_neg hxM]
      exact hq x ((Finset.mem_union.mp hx).resolve_left hxM)
  have hrel : ∑ x ∈ M ∪ C, f x • x = 0 := by
    rw [Finset.sum_union hdisj]
    have hleft : ∑ x ∈ M, f x • x = -(∑ x ∈ C, q x • x) := by
      calc
        _ = ∑ x ∈ M, (r x + w x) • x := Finset.sum_congr rfl (fun x hx => by simp [f, hx])
        _ = _ := by simp only [add_smul, Finset.sum_add_distrib, hrrel, hwrel, add_zero]
    have hright : ∑ x ∈ C, f x • x = ∑ x ∈ C, q x • x :=
      Finset.sum_congr rfl (fun x hx => by
        have hxM : x ∉ M := fun hm => Finset.disjoint_left.mp hdisj hm hx
        simp [f, hxM])
    rw [hleft, hright, neg_add_cancel]
  exact normalize_positive_relation (M ∪ C) (hM.mono Finset.subset_union_left) f hf hrel

/-- For a finite relative-IP generating set, the vector span is its linear span. -/
theorem vectorSpan_eq_span_of_intrinsicInterior {n : ℕ} (S : Finset (V n))
    (h0 : (0 : V n) ∈ intrinsicInterior ℝ (convexHull ℝ (S : Set (V n)))) :
    vectorSpan ℝ (S : Set (V n)) = Submodule.span ℝ (S : Set (V n)) := by
  rw [← direction_affineSpan,
    affineSpan_eq_span_of_zero_mem_convexHull S (intrinsicInterior_subset h0),
    Submodule.toAffineSubspace_direction]

end Refpoly
