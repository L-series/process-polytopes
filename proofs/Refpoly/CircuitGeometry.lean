/-
CircuitGeometry faithfully ported from the reviewed refpoly-5d snapshot.
See SOURCES.md for attribution and exact source identities.
-/
import Refpoly.Basic

/-! Geometric positive circuits, with a prescribed point retained. -/

open Set Finset
namespace Refpoly

/-- The Carathéodory exchange step: subtracting the largest admissible multiple
of `g` (which has some positive coefficient) from nonnegative weights `f`
keeps them nonnegative and makes the weight of some `b` with `g b > 0` vanish. -/
theorem caratheodory_step {n : ℕ} (S : Finset (V n)) (f g : V n → ℝ)
    (hf : ∀ x ∈ S, 0 ≤ f x) (hpos : ∃ x ∈ S, 0 < g x) :
    ∃ b ∈ S, 0 < g b ∧ ∃ c : ℝ, 0 ≤ c ∧ f b - c * g b = 0 ∧
      ∀ x ∈ S, 0 ≤ f x - c * g x := by
  classical
  obtain ⟨x, hx, hgx⟩ := hpos
  obtain ⟨b, hbU, hbmin⟩ := (S.filter fun x => 0 < g x).exists_min_image
    (fun x => f x / g x) ⟨x, Finset.mem_filter.mpr ⟨hx, hgx⟩⟩
  obtain ⟨hb, hgb⟩ := Finset.mem_filter.mp hbU
  have hc : 0 ≤ f b / g b := div_nonneg (hf b hb) hgb.le
  refine ⟨b, hb, hgb, f b / g b, hc, by rw [div_mul_cancel₀ _ hgb.ne', sub_self],
    fun y hy => sub_nonneg.2 ?_⟩
  by_cases hgy : 0 < g y
  · rw [← le_div_iff₀ hgy]; exact hbmin y (Finset.mem_filter.mpr ⟨hy, hgy⟩)
  · exact (mul_nonpos_of_nonneg_of_nonpos hc (not_lt.1 hgy)).trans (hf y hy)

/-- Carathéodory reduction retaining a point whose coefficient is positive. -/
theorem positive_circuit_through {n : ℕ} (S : Finset (V n)) (a : V n)
    (ha : a ∈ S) (f : V n → ℝ) (hf : ∀ x ∈ S, 0 ≤ f x)
    (hfsum : ∑ x ∈ S, f x = 1) (hfrel : ∑ x ∈ S, f x • x = 0)
    (hfa : 0 < f a) :
    ∃ T : Finset (V n), T ⊆ S ∧ a ∈ T ∧
      AffineIndependent ℝ ((↑) : T → V n) ∧
      ∃ w : V n → ℝ, (∀ x ∈ T, 0 < w x) ∧
        (∑ x ∈ T, w x = 1) ∧ ∑ x ∈ T, w x • x = 0 := by
  classical
  induction S using Finset.strongInductionOn generalizing f
  rename_i S ih
  by_cases hind : AffineIndependent ℝ ((↑) : S → V n)
  · let T := S.filter (fun x => f x ≠ 0)
    have hT : T ⊆ S := Finset.filter_subset _ _
    refine ⟨T, hT, Finset.mem_filter.mpr ⟨ha, ne_of_gt hfa⟩,
      hind.comp_embedding ⟨_, inclusion_injective hT⟩, f, ?_, ?_, ?_⟩
    · intro x hx
      exact (hf x (Finset.mem_filter.mp hx).1).lt_of_ne
        (Finset.mem_filter.mp hx).2.symm
    · exact (Finset.sum_filter_ne_zero S).trans hfsum
    · rw [Finset.sum_filter_of_ne]
      · exact hfrel
      · intro x _ hx
        exact fun h => hx (by rw [h, zero_smul])
  · obtain ⟨g₀, hg₀rel, hg₀sum, hg₀ne⟩ :=
      exists_nontrivial_relation_sum_zero_of_not_affine_ind hind
    let g : V n → ℝ := if 0 < g₀ a then -g₀ else g₀
    have hgrel : ∑ x ∈ S, g x • x = 0 := by
      dsimp [g]; split_ifs
      · simp only [Pi.neg_apply, neg_smul, Finset.sum_neg_distrib, hg₀rel, neg_zero]
      · exact hg₀rel
    have hgsum : ∑ x ∈ S, g x = 0 := by
      dsimp [g]; split_ifs
      · simp only [Pi.neg_apply, Finset.sum_neg_distrib, hg₀sum, neg_zero]
      · exact hg₀sum
    have hga : g a ≤ 0 := by
      dsimp [g]; split_ifs with h
      · simp only [Pi.neg_apply]; linarith
      · exact le_of_not_gt h
    have hgne : ∃ x ∈ S, g x ≠ 0 := by
      obtain ⟨x, hx, hne⟩ := hg₀ne
      refine ⟨x, hx, ?_⟩
      dsimp [g]; split_ifs <;> simpa using hne
    obtain ⟨b, hb, hgb, c, hc, hwb, hw⟩ := caratheodory_step S f g hf
      (exists_pos_of_sum_zero_of_exists_nonzero g hgsum hgne)
    have hba : a ≠ b := by rintro rfl; linarith
    let w : V n → ℝ := fun x => f x - c * g x
    have hw_sum : ∑ x ∈ S.erase b, w x = 1 := by
      rw [Finset.sum_erase_eq_sub hb]
      simp only [w, hwb, Finset.sum_sub_distrib, ← Finset.mul_sum, hfsum, hgsum,
        mul_zero, sub_zero]
    have hw_rel : ∑ x ∈ S.erase b, w x • x = 0 := by
      rw [Finset.sum_erase_eq_sub hb]
      simp only [w, hwb, zero_smul, sub_zero, sub_smul, mul_smul, Finset.sum_sub_distrib,
        ← Finset.smul_sum, hfrel, hgrel, smul_zero]
    have hwa : 0 < w a := by
      have := mul_nonpos_of_nonneg_of_nonpos hc hga
      simp only [w]; linarith
    obtain ⟨T, hT, haT, hiT, hwT⟩ := ih (S.erase b) (Finset.erase_ssubset hb)
      (Finset.mem_erase.mpr ⟨hba, ha⟩) w (fun x hx => hw x (Finset.mem_of_mem_erase hx))
      hw_sum hw_rel hwa
    exact ⟨T, hT.trans (Finset.erase_subset _ _), haT, hiT, hwT⟩

/-- Every point of a finite IP generating set belongs to a positive circuit. -/
theorem ip_positive_circuit_through {n : ℕ} (S : Finset (V n)) (a : V n)
    (ha : a ∈ S) (h0 : (0 : V n) ∈ interior (convexHull ℝ (S : Set (V n)))) :
    ∃ T : Finset (V n), T ⊆ S ∧ a ∈ T ∧
      AffineIndependent ℝ ((↑) : T → V n) ∧
      ∃ w : V n → ℝ, (∀ x ∈ T, 0 < w x) ∧
        (∑ x ∈ T, w x = 1) ∧ ∑ x ∈ T, w x • x = 0 := by
  classical
  obtain ⟨ε, hε, hball⟩ := Metric.mem_nhds_iff.mp (mem_interior_iff_mem_nhds.mp h0)
  let t : ℝ := ε / (2 * (‖a‖ + 1))
  have hd : 0 < 2 * (‖a‖ + 1) := by positivity
  have ht : 0 < t := div_pos hε hd
  have ht1 : 0 < 1 + t := by linarith
  have hta : -t • a ∈ convexHull ℝ (S : Set (V n)) := by
    apply hball
    rw [Metric.mem_ball, dist_zero_right, norm_smul, Real.norm_eq_abs,
      abs_neg, abs_of_pos ht]
    dsimp [t]
    rw [div_mul_eq_mul_div, div_lt_iff₀ hd]
    nlinarith [norm_nonneg a]
  rw [Finset.convexHull_eq] at hta
  obtain ⟨f, hf, hsum, hrel⟩ := hta
  rw [Finset.centerMass_eq_of_sum_1 _ id hsum] at hrel
  simp only [id_eq] at hrel
  let w : V n → ℝ := fun x => (1 + t)⁻¹ * f x + if x = a then t / (1 + t) else 0
  have hw : ∀ x ∈ S, 0 ≤ w x := by
    intro x hx
    dsimp [w]
    apply add_nonneg (mul_nonneg (inv_nonneg.mpr (le_of_lt ht1)) (hf x hx))
    split_ifs <;> positivity
  have hwsum : ∑ x ∈ S, w x = 1 := by
    simp only [w, Finset.sum_add_distrib, ← Finset.mul_sum, hsum, mul_one,
      Finset.sum_ite_eq', ha, if_true]
    field_simp
  have hwrel : ∑ x ∈ S, w x • x = 0 := by
    simp only [w, add_smul, mul_smul, Finset.sum_add_distrib, ← Finset.smul_sum,
      hrel, ite_smul, zero_smul, Finset.sum_ite_eq', ha, if_true]
    rw [smul_smul, ← add_smul]
    have hz : (1 + t)⁻¹ * -t + t / (1 + t) = 0 := by ring
    rw [hz, zero_smul]
  have hwa : 0 < w a := by
    dsimp [w]; rw [if_pos rfl]
    exact add_pos_of_nonneg_of_pos
      (mul_nonneg (inv_nonneg.mpr (le_of_lt ht1)) (hf a ha)) (div_pos ht ht1)
  exact positive_circuit_through S a ha w hw hwsum hwrel hwa

/-- Conical Carathéodory reduction: a nonnegative linear combination can be
supported on a linearly independent subset, with positive remaining weights. -/
theorem conical_caratheodory {n : ℕ} (S : Finset (V n)) (x : V n)
    (f : V n → ℝ) (hf : ∀ y ∈ S, 0 ≤ f y) (hrel : ∑ y ∈ S, f y • y = x) :
    ∃ T : Finset (V n), T ⊆ S ∧ LinearIndependent ℝ ((↑) : T → V n) ∧
      ∃ w : V n → ℝ, (∀ y ∈ T, 0 < w y) ∧ ∑ y ∈ T, w y • y = x := by
  classical
  induction S using Finset.strongInductionOn generalizing f
  rename_i S ih
  by_cases hind : LinearIndependent ℝ ((↑) : S → V n)
  · let T := S.filter (fun y => f y ≠ 0)
    have hT : T ⊆ S := Finset.filter_subset _ _
    refine ⟨T, hT, hind.comp (inclusion hT) (inclusion_injective hT),
      f, ?_, ?_⟩
    · intro y hy
      exact (hf y (Finset.mem_filter.mp hy).1).lt_of_ne
        (Finset.mem_filter.mp hy).2.symm
    · rw [Finset.sum_filter_of_ne]
      · exact hrel
      · intro y _ hy; exact fun h => hy (by rw [h, zero_smul])
  · obtain ⟨gsub, hgsub, a, hga⟩ := Fintype.not_linearIndependent_iff.mp hind
    let g₀ : V n → ℝ := fun y => if h : y ∈ S then gsub ⟨y, h⟩ else 0
    have hg₀rel : ∑ y ∈ S, g₀ y • y = 0 := by
      rw [← Finset.sum_attach (f := fun y : V n => g₀ y • y)]
      simpa [g₀, Finset.univ_eq_attach] using hgsub
    have hg₀a : g₀ a = gsub a := by simp [g₀, a.property]
    let g : V n → ℝ := if 0 < gsub a then g₀ else -g₀
    have hgrel : ∑ y ∈ S, g y • y = 0 := by
      dsimp [g]; split_ifs
      · exact hg₀rel
      · simp only [Pi.neg_apply, neg_smul, Finset.sum_neg_distrib, hg₀rel, neg_zero]
    have hgap : 0 < g a := by
      dsimp [g]; split_ifs with h
      · rwa [hg₀a]
      · simp only [Pi.neg_apply, hg₀a]; exact neg_pos.mpr (lt_of_le_of_ne (le_of_not_gt h) hga)
    obtain ⟨b, hb, -, c, -, hwb, hw⟩ := caratheodory_step S f g hf ⟨a, a.property, hgap⟩
    let w : V n → ℝ := fun y => f y - c * g y
    have hwrel : ∑ y ∈ S.erase b, w y • y = x := by
      rw [Finset.sum_erase_eq_sub hb]
      simp only [w, hwb, zero_smul, sub_zero, sub_smul, mul_smul, Finset.sum_sub_distrib,
        ← Finset.smul_sum, hrel, hgrel, smul_zero]
    obtain ⟨T, hT, hiT, hwT⟩ := ih (S.erase b) (Finset.erase_ssubset hb) w
      (fun y hy => hw y (Finset.mem_of_mem_erase hy)) hwrel
    exact ⟨T, hT.trans (Finset.erase_subset _ _), hiT, hwT⟩

/-- A positive simplex circuit containing a nonzero point cannot contain zero. -/
theorem zero_not_mem_positive_circuit {n : ℕ} (S : Finset (V n))
    (hind : AffineIndependent ℝ ((↑) : S → V n))
    (w : V n → ℝ) (hw : ∀ x ∈ S, 0 < w x)
    (hsum : ∑ x ∈ S, w x = 1) (hrel : ∑ x ∈ S, w x • x = 0)
    (a : V n) (ha : a ∈ S) (ha0 : a ≠ 0) : (0 : V n) ∉ S := by
  classical
  intro hzero
  let g : V n → ℝ := fun x => w x - if x = 0 then 1 else 0
  have hgsum : ∑ x ∈ S, g x = 0 := by
    simp only [g, Finset.sum_sub_distrib, hsum, Finset.sum_ite_eq', hzero,
      if_true, sub_self]
  have hgrel : ∑ x ∈ S, g x • x = 0 := by
    simp only [g, sub_smul, Finset.sum_sub_distrib, hrel, ite_smul,
      zero_smul, Finset.sum_ite_eq', hzero, if_true, one_smul, sub_zero]
  have he := hind.eq_zero_of_sum_eq_zero_subtype hgsum hgrel a ha
  simp only [g, if_neg ha0, sub_zero] at he
  exact (ne_of_gt (hw a ha)) he

/-- Choose representatives in the original finite set for a positive simplex
circuit of its image, with projection injective on those representatives. -/
theorem projected_positive_circuit {n d : ℕ} (S : Finset (V n))
    (p : V n →ₗ[ℝ] V d)
    (h0 : (0 : V d) ∈ interior (convexHull ℝ ((S.image p : Finset (V d)) : Set (V d))))
    (a : V n) (ha : a ∈ S) (ha0 : p a ≠ 0) :
    ∃ C : Finset (V n), C ⊆ S ∧ C.Nonempty ∧
      AffineIndependent ℝ (fun x : C => p x) ∧
      ∃ q : V n → ℝ, (∀ x ∈ C, 0 < q x) ∧
        (∑ x ∈ C, q x = 1) ∧ (∑ x ∈ C, q x • p x = 0) ∧
        ∀ x ∈ C, p x ≠ 0 := by
  classical
  obtain ⟨B, hB, haB, hiB, w, hw, hsum, hrel⟩ := ip_positive_circuit_through
    (S.image p) (p a) (Finset.mem_image.mpr ⟨a, ha, rfl⟩) h0
  have hzero : (0 : V d) ∉ B := zero_not_mem_positive_circuit B hiB w hw hsum hrel _ haB ha0
  have hrep : ∀ b : B, ∃ x ∈ S, p x = b := fun b => Finset.mem_image.mp (hB b.property)
  choose f hfS hfp using hrep
  let C := Finset.univ.image f
  have hCS : C ⊆ S := by
    intro x hx; obtain ⟨b, _, rfl⟩ := Finset.mem_image.mp hx; exact hfS b
  have hpB : ∀ x ∈ C, p x ∈ B := by
    intro x hx; obtain ⟨b, _, rfl⟩ := Finset.mem_image.mp hx
    rw [hfp b]; exact b.property
  have hpinj : Set.InjOn p (C : Set (V n)) := by
    intro x hx y hy heq
    obtain ⟨b, _, rfl⟩ := Finset.mem_image.mp hx
    obtain ⟨c, _, rfl⟩ := Finset.mem_image.mp hy
    have hbc : b = c := Subtype.ext (by simpa only [hfp] using heq)
    rw [hbc]
  let e : C ≃ B := Equiv.ofBijective (fun x => ⟨p x, hpB x x.property⟩) ⟨
    (fun x y h => Subtype.ext (hpinj x.property y.property (congrArg Subtype.val h))),
    (fun b => ⟨⟨f b, Finset.mem_image.mpr ⟨b, Finset.mem_univ _, rfl⟩⟩,
      Subtype.ext (hfp b)⟩)⟩
  have hne : C.Nonempty := ⟨f ⟨p a, haB⟩,
    Finset.mem_image.mpr ⟨⟨p a, haB⟩, Finset.mem_univ _, rfl⟩⟩
  have hiC : AffineIndependent ℝ (fun x : C => p x) := hiB.comp_embedding e.toEmbedding
  -- `p` is a bijection from `C` onto `B`
  have hsurj : Set.SurjOn p C B := fun b hb =>
    ⟨f ⟨b, hb⟩, Finset.mem_image.mpr ⟨⟨b, hb⟩, Finset.mem_univ _, rfl⟩, hfp _⟩
  have hsumC : ∑ x ∈ C, w (p x) = 1 :=
    (Finset.sum_nbij p hpB hpinj hsurj fun _ _ => rfl).trans hsum
  have hrelC : ∑ x ∈ C, w (p x) • p x = 0 :=
    (Finset.sum_nbij p hpB hpinj hsurj fun _ _ => rfl).trans hrel
  exact ⟨C, hCS, hne, hiC, fun x => w (p x),
    fun x hx => hw _ (hpB x hx), hsumC, hrelC,
    fun x hx hzero' => hzero (hzero' ▸ hpB x hx)⟩

/-- All linear relations on a projected simplex are scalar multiples of its
normalized circuit relation. -/
theorem projected_relation_coefficients {n d : ℕ} (C : Finset (V n))
    (p : V n →ₗ[ℝ] V d) (hi : AffineIndependent ℝ (fun x : C => p x))
    (q : V n → ℝ) (hqsum : ∑ x ∈ C, q x = 1)
    (hqrel : ∑ x ∈ C, q x • p x = 0)
    (b : V n → ℝ) (hbrel : ∑ x ∈ C, b x • p x = 0) :
    ∀ x ∈ C, b x = (∑ y ∈ C, b y) * q x := by
  classical
  let g : V n → ℝ := fun x => b x - (∑ y ∈ C, b y) * q x
  have hsum : ∑ x ∈ C, g x = 0 := by
    simp only [g, Finset.sum_sub_distrib, ← Finset.mul_sum, hqsum, mul_one, sub_self]
  have hrel : ∑ x ∈ C, g x • p x = 0 := by
    simp only [g, sub_smul, mul_smul, Finset.sum_sub_distrib,
      ← Finset.smul_sum, hbrel, hqrel, smul_zero, sub_self]
  have hsum' : ∑ x : C, g x = 0 := (Finset.sum_coe_sort C g).trans hsum
  have hrel' : ∑ x : C, g x • p x = 0 :=
    (Finset.sum_coe_sort C fun x => g x • p x).trans hrel
  intro x hx
  exact sub_eq_zero.mp (hi.eq_zero_of_sum_eq_zero hsum' hrel' ⟨x, hx⟩ (Finset.mem_univ _))

end Refpoly
