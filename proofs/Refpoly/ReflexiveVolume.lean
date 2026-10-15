/-
ReflexiveVolume faithfully ported from the reviewed refpoly-5d snapshot.
See SOURCES.md for attribution and exact source identities.
The dimension-dependent bound is existential and reflexive-only.
-/
import Refpoly.GeometricCWS
import Refpoly.WeightSystemFinite
import Refpoly.CircuitGeometry
import Refpoly.BoxReduction
import Refpoly.ReflexiveDuality

/-! Uniform volume bounds from finite IP weights and integral dual pairings. -/

open Set Matrix Module
namespace Refpoly
namespace ReflexiveVolume

lemma finite_positive_lower_bound {s : Set ℝ} (hs : s.Finite)
    (hp : ∀ r ∈ s, 0 < r) : ∃ ε : ℝ, 0 < ε ∧ ∀ r ∈ s, ε ≤ r := by
  rcases s.eq_empty_or_nonempty with rfl | hne
  · exact ⟨1, one_pos, by simp⟩
  obtain ⟨r, hr, hmin⟩ := Set.exists_min_image s id hs hne
  exact ⟨r, hp r hr, hmin⟩

lemma uniform_weight_lower_bound (n : ℕ) :
    ∃ ε : ℝ, 0 < ε ∧ ∀ m ≤ n + 1, ∀ q : V m,
      (∑ i, q i = 1) → IsIPWeightSystem q → ∀ i, ε ≤ q i := by
  let B : Set ℝ := ⋃ m ∈ Set.Iic (n + 1),
    ⋃ q ∈ {q : V m | ∑ i, q i = 1 ∧ IsIPWeightSystem q}, Set.range q
  have hB : B.Finite := by
    apply (Set.finite_Iic (n + 1)).biUnion
    intro m hm
    apply (WeightSystemFinite.normalized_ip_weight_systems_finite m).biUnion
    intro q hq
    exact Set.finite_range q
  have hpos : ∀ r ∈ B, 0 < r := by
    intro r hr
    obtain ⟨m, hm, hr⟩ := Set.mem_iUnion₂.mp hr
    obtain ⟨q, hq, hr⟩ := Set.mem_iUnion₂.mp hr
    obtain ⟨i, rfl⟩ := hr
    exact hq.2.1 i
  obtain ⟨ε, hε, he⟩ := finite_positive_lower_bound hB hpos
  refine ⟨ε, hε, fun m hm q hsum hIP i => he _ ?_⟩
  exact Set.mem_iUnion₂.mpr ⟨m, hm, Set.mem_iUnion₂.mpr ⟨q, ⟨hsum, hIP⟩,
    Set.mem_range_self i⟩⟩

lemma dual_point_ip_circuit {n : ℕ} {P : Set (V n)} (hP : IsReflexive P)
    (S : Finset (V n)) (hSlat : ∀ x ∈ S, IsLatticePoint x)
    (hSD : convexHull ℝ (S : Set (V n)) = polarDual P) (a : V n) (ha : a ∈ S) :
    ∃ m ≤ n + 1, ∃ (v : Fin m → V n) (q : V m),
      (∃ i, v i = a) ∧ (∀ i, 0 < q i) ∧ (∑ i, q i = 1) ∧
      IsIPWeightSystem q ∧ (∑ i, q i • v i = 0) ∧
      ∀ i, v i ∈ polarDual P := by
  classical
  have h0 : (0 : V n) ∈ interior (convexHull ℝ (S : Set (V n))) := by
    rw [hSD]; exact hP.polarDual_reflexive.ip
  obtain ⟨T, hTS, haT, hiT, w, hw, hw_sum, hw_rel⟩ :=
    ip_positive_circuit_through S a ha h0
  have hcard : T.card ≤ n + 1 := by
    have h := hiT.card_le_finrank_succ
    rw [Fintype.card_coe] at h
    have hfr : finrank ℝ (vectorSpan ℝ (Set.range ((↑) : T → V n))) ≤ n := by
      simpa using Submodule.finrank_le (vectorSpan ℝ (Set.range ((↑) : T → V n)))
    omega
  let e : T ≃ Fin T.card := Fintype.equivFinOfCardEq (by simp)
  let v : Fin T.card → V n := fun i => (e.symm i : V n)
  let q : V T.card := fun i => w (v i)
  have hv : ∀ i, v i ∈ T := fun i => (e.symm i).property
  have hqpos : ∀ i, 0 < q i := fun i => hw _ (hv i)
  have hqsum : ∑ i, q i = 1 :=
    (e.symm.sum_comp (fun x : T => w x)).trans ((Finset.sum_coe_sort T w).trans hw_sum)
  have hcirc : ∑ i, q i • v i = 0 :=
    (e.symm.sum_comp (fun x : T => w x • (x : V n))).trans
      ((Finset.sum_coe_sort T fun x => w x • x).trans hw_rel)
  have hind : AffineIndependent ℝ v := hiT.comp_embedding e.symm.toEmbedding
  have hvlat : ∀ i, IsLatticePoint (v i) := fun i => hSlat _ (hTS (hv i))
  have hvdual : ∀ i, v i ∈ polarDual P := by
    intro i
    rw [← hSD]
    exact subset_convexHull ℝ _ (hTS (hv i))
  refine ⟨T.card, hcard, v, q, ⟨e ⟨a, haT⟩, ?_⟩, hqpos, hqsum,
    reflexive_simplex_block_ip hP v q hind hqpos hqsum hcirc hvlat hvdual,
    hcirc, hvdual⟩
  simp [v]

lemma dual_generator_pairing_bound {n : ℕ} {P : Set (V n)} (hP : IsReflexive P)
    (ε : ℝ) (hε : 0 < ε)
    (he : ∀ m ≤ n + 1, ∀ q : V m, (∑ i, q i = 1) →
      IsIPWeightSystem q → ∀ i, ε ≤ q i)
    (S : Finset (V n)) (hSlat : ∀ x ∈ S, IsLatticePoint x)
    (hSD : convexHull ℝ (S : Set (V n)) = polarDual P) :
    ∀ a ∈ S, ∀ x ∈ P, a ⬝ᵥ x ≤ ε⁻¹ := by
  intro a ha x hx
  obtain ⟨m, hm, v, q, ⟨i, hi⟩, hqpos, hqsum, hIP, hcirc, hvdual⟩ :=
    dual_point_ip_circuit hP S hSlat hSD a ha
  have hsum : ∑ j, q j * (v j ⬝ᵥ x + 1) = 1 := by
    simp_rw [mul_add, mul_one]
    rw [Finset.sum_add_distrib, hqsum]
    have h : ∑ j, q j * (v j ⬝ᵥ x) = 0 := by
      have h := congrArg (fun y => y ⬝ᵥ x) hcirc
      simpa [sum_dotProduct, smul_dotProduct, smul_eq_mul] using h
    rw [h, zero_add]
  have hnonneg : ∀ j, 0 ≤ q j * (v j ⬝ᵥ x + 1) := fun j =>
    mul_nonneg (hqpos j).le (by have := hvdual j x hx; linarith)
  have hle : q i * (a ⬝ᵥ x + 1) ≤ 1 := by
    have h := Finset.single_le_sum (fun j _ => hnonneg j) (Finset.mem_univ i)
    rwa [hsum, hi] at h
  have heq := he m hm q hqsum hIP i
  have hqa : 0 ≤ a ⬝ᵥ x + 1 := by have := hvdual i x hx; rw [hi] at this; linarith
  have hprod := mul_le_mul_of_nonneg_right heq hqa
  have hbound : ε * (a ⬝ᵥ x) ≤ 1 := by nlinarith
  rw [inv_eq_one_div]
  exact (le_div_iff₀ hε).mpr (by nlinarith [hbound])

/-- A dimension-uniform volume bound for reflexive polytopes. The constant is
existential; no numerical Hensley estimate is used. -/
theorem volume_bound (n : ℕ) :
    ∃ C : ℝ, ∀ P : Set (V n), IsReflexive P →
      (MeasureTheory.volume P).toReal ≤ C := by
  classical
  obtain ⟨ε, hε, he⟩ := uniform_weight_lower_bound n
  let R : ℝ := max 1 ε⁻¹
  have hR : 0 ≤ R := (by norm_num : (0 : ℝ) ≤ 1).trans (le_max_left _ _)
  let B : Set (V n) := Set.Icc (fun _ => -R) (fun _ => R)
  refine ⟨(MeasureTheory.volume B).toReal, fun P hP => ?_⟩
  obtain ⟨S, hSlat, hSD⟩ := hP.dual_latticePolytope
  have h0 : (0 : V n) ∈ interior (convexHull ℝ (S : Set (V n))) := by
    rw [← hSD]; exact hP.polarDual_reflexive.ip
  obtain ⟨A, hA, hv, _⟩ := BoxReduction.maximal_vertex_matrix S h0
  have hupper := dual_generator_pairing_bound hP ε hε he S hSlat hSD.symm
  let D := Aᵀ
  have hsub : D.mulVec '' P ⊆ B := by
    rintro _ ⟨x, hx, rfl⟩
    constructor
    · intro i
      have hdual : A.col i ∈ polarDual P := by
        rw [hSD]
        exact subset_convexHull ℝ _ (hv i)
      have hlower := hdual x hx
      change -R ≤ A.col i ⬝ᵥ x
      exact (neg_le_neg (le_max_left 1 ε⁻¹)).trans hlower
    · intro i
      change A.col i ⬝ᵥ x ≤ R
      exact (hupper _ (hv i) x hx).trans (le_max_right _ _)
  obtain ⟨Z, -, hdetcast⟩ := BoxReduction.exists_intMatrix (A := A) fun i j => hSlat _ (hv j) i
  have hZne : Z.det ≠ 0 := fun hz => hA (by rw [← hdetcast, hz, Int.cast_zero])
  have hdetlower : (1 : ℝ) ≤ |D.det| := by
    have hc : (1 : ℝ) ≤ |(Z.det : ℝ)| := by exact_mod_cast Int.one_le_abs hZne
    simpa [D, Matrix.det_transpose, hdetcast] using hc
  have hvol : MeasureTheory.volume (D.mulVec '' P) =
      ENNReal.ofReal |D.det| * MeasureTheory.volume P := by
    rw [← Matrix.coe_mulVecLin, ← Matrix.toLin'_apply',
      MeasureTheory.Measure.addHaar_image_linearMap, LinearMap.det_toLin']
  have hBfinite : MeasureTheory.volume B ≠ ⊤ := by
    apply ne_of_lt
    exact (isCompact_Icc : IsCompact B).measure_lt_top
  have hmono := ENNReal.toReal_mono hBfinite (MeasureTheory.measure_mono hsub)
  rw [hvol, ENNReal.toReal_mul, ENNReal.toReal_ofReal (abs_nonneg _)] at hmono
  exact (le_mul_of_one_le_left ENNReal.toReal_nonneg hdetlower).trans hmono

end ReflexiveVolume
end Refpoly
