/-
BoxReduction faithfully ported from the reviewed refpoly-5d snapshot.
See SOURCES.md for attribution and exact source identities.
-/
import Refpoly.Basic

/-!
# Uniform box reduction for lattice polytopes of bounded volume

This file proves the geometric input to the Lagarias–Ziegler box lemma.
`box_of_volume_bound` assumes an explicit volume bound; it does not use
Hensley's theorem or any other unproved geometry-of-numbers input.

The proof chooses a maximal-determinant tuple of lattice generators.
Replacing a column bounds every generator's coordinates in this basis by one.
A scaled anchored parallelepiped inside the polytope bounds the determinant
by its volume. Integral matrices with bounded nonzero determinant have only
finitely many row lattices: each contains the determinant times the standard
lattice and is determined by its image in a finite residue group. Choosing
one matrix per row lattice gives bounded representatives, and changes of
integral bases give the required unimodular real linear equivalences.

The radius is existential (classical finite choices), rather than the explicit
numerical radius in [LZ91, Theorem 2]. The proof includes dimension zero.
-/

open Set Matrix Module
namespace Refpoly
namespace BoxReduction

abbrev ZV (n : ℕ) := Fin n → ℤ
abbrev ZMat (n : ℕ) := Matrix (Fin n) (Fin n) ℤ

def rowSpan (A : ZMat n) : Submodule ℤ (ZV n) := Submodule.span ℤ (Set.range A)

lemma vecMul_mem_rowSpan (A : ZMat n) (x : ZV n) : x ᵥ* A ∈ rowSpan A := by
  classical
  have h : x ᵥ* A = ∑ i, x i • A i := by
    ext j
    simp [Matrix.vecMul, dotProduct, Finset.sum_apply]
  rw [h]
  exact Submodule.sum_mem _ fun i _ => Submodule.smul_mem _ _ (Submodule.subset_span ⟨i, rfl⟩)

lemma det_smul_mem_rowSpan (A : ZMat n) (x : ZV n) : A.det • x ∈ rowSpan A := by
  have h := vecMul_mem_rowSpan A (x ᵥ* A.adjugate)
  simpa [Matrix.vecMul_vecMul, Matrix.adjugate_mul, Matrix.vecMul_smul] using h

noncomputable def modMap (n d : ℕ) : ZV n →ₗ[ℤ] (Fin n → ZMod d) :=
  LinearMap.pi fun i => (Int.castAddHom (ZMod d)).toIntLinearMap.comp (LinearMap.proj i)

lemma modMap_apply (n d : ℕ) (x : ZV n) (i : Fin n) :
    modMap n d x i = (x i : ZMod d) := rfl

lemma ker_modMap_le_rowSpan (A : ZMat n) :
    LinearMap.ker (modMap n A.det.natAbs) ≤ rowSpan A := by
  intro x hx
  have hx0 : ∀ i, (x i : ZMod A.det.natAbs) = 0 := by
    intro i
    exact congrFun (LinearMap.mem_ker.mp hx) i
  have hdiv : ∀ i, A.det ∣ x i := by
    intro i
    have hi := (ZMod.intCast_zmod_eq_zero_iff_dvd (x i) A.det.natAbs).mp (hx0 i)
    simpa only [Int.natCast_natAbs, abs_dvd] using hi
  choose y hy using hdiv
  have hxy : x = A.det • y := by ext i; exact hy i
  rw [hxy]
  exact det_smul_mem_rowSpan A y

lemma finite_rowSpans_bounded_det (n K : ℕ) :
    {L : Submodule ℤ (ZV n) | ∃ A : ZMat n, A.det ≠ 0 ∧ A.det.natAbs ≤ K ∧ L = rowSpan A}.Finite := by
  classical
  have hfin (d : ℕ) (hd : 0 < d) :
      (Set.range (Submodule.comap (modMap n d))).Finite := by
    letI : NeZero d := ⟨Nat.ne_of_gt hd⟩
    exact Set.finite_range _
  apply Set.Finite.subset (Set.finite_Icc 1 K |>.biUnion fun d hd => hfin d hd.1)
  rintro L ⟨A, hA, hK, rfl⟩
  refine Set.mem_iUnion₂.mpr ⟨A.det.natAbs, ⟨?_, hK⟩, ?_⟩
  · exact Nat.one_le_iff_ne_zero.mpr (Int.natAbs_ne_zero.mpr hA)
  · exact ⟨Submodule.map (modMap n A.det.natAbs) (rowSpan A),
      Submodule.comap_map_eq_self (ker_modMap_le_rowSpan A)⟩

/-- Matrices with the same full-rank row lattice differ by an integral change of basis. -/
lemma rowSpan_change_basis (A B : ZMat n) (hA : A.det ≠ 0) (hB : B.det ≠ 0)
    (hAB : rowSpan A = rowSpan B) :
    ∃ U W : ZMat n, U * W = 1 ∧ W * U = 1 ∧ U * A = B := by
  classical
  let a : Basis (Fin n) ℤ (rowSpan A) :=
    Basis.span (Matrix.linearIndependent_rows_of_det_ne_zero hA)
  let b : Basis (Fin n) ℤ (rowSpan A) :=
    (Basis.span (Matrix.linearIndependent_rows_of_det_ne_zero hB)).map
      (LinearEquiv.ofEq (rowSpan B) (rowSpan A) hAB.symm)
  have ha (i : Fin n) : (a i : ZV n) = A i := by
    exact Basis.coe_span_apply _ i
  have hb (i : Fin n) : (b i : ZV n) = B i := by
    change ((Basis.span (Matrix.linearIndependent_rows_of_det_ne_zero hB)) i : ZV n) = B i
    exact Basis.coe_span_apply _ i
  refine ⟨(a.toMatrix b).transpose, (b.toMatrix a).transpose, ?_, ?_, ?_⟩
  · rw [← Matrix.transpose_mul, Basis.toMatrix_mul_toMatrix_flip, Matrix.transpose_one]
  · rw [← Matrix.transpose_mul, Basis.toMatrix_mul_toMatrix_flip, Matrix.transpose_one]
  · ext i j
    have h := congrArg (fun z : rowSpan A => (z : ZV n) j)
      (a.sum_toMatrix_smul_self b i)
    simpa only [Submodule.coe_sum, Submodule.coe_smul, Finset.sum_apply, Pi.smul_apply, smul_eq_mul, ha, hb, Matrix.mul_apply, Matrix.transpose_apply] using h

abbrev realMatrix (A : ZMat n) : Matrix (Fin n) (Fin n) ℝ :=
  A.map (Int.castRingHom ℝ)

lemma integral_mulVec (A : ZMat n) {x : V n} (hx : IsLatticePoint x) :
    IsLatticePoint ((realMatrix A).mulVec x) := by
  classical
  choose z hz using hx
  intro i
  refine ⟨∑ j, A i j * z j, ?_⟩
  simp [Matrix.mulVec, dotProduct, hz, realMatrix]

/-- A real matrix with integer entries comes from an integral matrix with the
same determinant. -/
lemma exists_intMatrix {A : Matrix (Fin n) (Fin n) ℝ} (h : ∀ i j, ∃ z : ℤ, A i j = z) :
    ∃ Z : ZMat n, realMatrix Z = A ∧ (Z.det : ℝ) = A.det := by
  choose z hz using h
  have hcast : realMatrix z = A := by ext i j; exact (hz i j).symm
  exact ⟨z, hcast, by rw [← hcast]; exact (Int.castRingHom ℝ).map_det z⟩

/-- An integral matrix with an integral inverse acts unimodularly on real coordinates. -/
lemma integral_linearEquiv (U W : ZMat n) (hUW : U * W = 1) (hWU : W * U = 1) :
    ∃ g : V n ≃ₗ[ℝ] V n, IsUnimodular g ∧
      ∀ x, g x = (realMatrix U).mulVec x := by
  have hrUW : realMatrix U * realMatrix W = 1 := by
    rw [← Matrix.map_mul, hUW]
    exact Matrix.map_one _ (map_zero _) (map_one _)
  have hrWU : realMatrix W * realMatrix U = 1 := by
    rw [← Matrix.map_mul, hWU]
    exact Matrix.map_one _ (map_zero _) (map_one _)
  let g := Matrix.toLin'OfInv hrWU hrUW
  refine ⟨g, ⟨?_, ?_⟩, fun x => rfl⟩
  · intro x hx
    exact integral_mulVec U hx
  · intro x hx
    exact integral_mulVec W hx

/-- A convex set containing zero contains subconvex combinations. -/
lemma convex_sum_mem_zero {P : Set (V n)} (hP : Convex ℝ P) (h0 : 0 ∈ P)
    (v : Fin n → V n) (hv : ∀ i, v i ∈ P) (t : Fin n → ℝ)
    (ht : ∀ i, 0 ≤ t i) (hs : ∑ i, t i ≤ 1) : (∑ i, t i • v i) ∈ P := by
  classical
  let w : Option (Fin n) → ℝ := fun i => match i with
    | none => 1 - ∑ j, t j
    | some j => t j
  let z : Option (Fin n) → V n := fun i => match i with
    | none => 0
    | some j => v j
  have hh := hP.sum_mem (t := Finset.univ) (w := w) (z := z)
    (by intro i _; cases i <;> simp [w]; exact hs; exact ht _)
    (by simp [w, Fintype.sum_option])
    (by intro i _; cases i <;> simp [z, h0, hv])
  simpa [w, z, Fintype.sum_option] using hh

lemma span_vertices_eq_top (S : Finset (V n))
    (hip : (0 : V n) ∈ interior (convexHull ℝ (S : Set (V n)))) :
    Submodule.span ℝ (S : Set (V n)) = ⊤ := by
  have hsub : convexHull ℝ (S : Set (V n)) ⊆
      (Submodule.span ℝ (S : Set (V n)) : Set (V n)) :=
    convexHull_min Submodule.subset_span (Submodule.convex _)
  exact Submodule.eq_top_of_nonempty_interior' _ ⟨0, interior_mono hsub hip⟩

/-- Select a vertex tuple with maximal absolute determinant. -/
lemma maximal_vertex_matrix (S : Finset (V n))
    (hip : (0 : V n) ∈ interior (convexHull ℝ (S : Set (V n)))) :
    ∃ A : Matrix (Fin n) (Fin n) ℝ, A.det ≠ 0 ∧
      (∀ j, A.col j ∈ S) ∧
      ∀ v : Fin n → V n, (∀ j, v j ∈ S) → |(Matrix.of v).transpose.det| ≤ |A.det| := by
  classical
  have hspan := span_vertices_eq_top S hip
  let I := (linearIndepOn_empty ℝ (id : V n → V n)).extend (Set.empty_subset (S : Set (V n)))
  let b : Basis I ℝ (V n) := Basis.ofSpan hspan.ge
  letI := Fintype.ofFinite I
  have hc : Fintype.card I = n := by
    simpa [V] using (Module.finrank_eq_card_basis b).symm
  let c : Basis (Fin n) ℝ (V n) := b.reindex (Fintype.equivFinOfCardEq hc)
  have hcmem (j : Fin n) : c j ∈ S :=
    Basis.ofSpan_subset hspan.ge ⟨(Fintype.equivFinOfCardEq hc).symm j, by simp only [c, Basis.reindex_apply, b]⟩
  let T : Set (Fin n → V n) := {v | ∀ j, v j ∈ S}
  have hTfin : T.Finite := by
    simpa [T, Set.pi] using (Set.Finite.pi (fun _ : Fin n => S.finite_toSet))
  obtain ⟨v, hv, hmax⟩ := Set.exists_max_image T (fun v => |(Matrix.of v).transpose.det|)
    hTfin ⟨c, hcmem⟩
  refine ⟨(Matrix.of v).transpose, ?_, hv, fun w hw => hmax w hw⟩
  have hcunit : IsUnit ((Matrix.of c).transpose : Matrix (Fin n) (Fin n) ℝ) :=
    Matrix.linearIndependent_cols_iff_isUnit.mp c.linearIndependent
  have hcne : (Matrix.of c).transpose.det ≠ 0 :=
    ((Matrix.isUnit_iff_isUnit_det _).mp hcunit).ne_zero
  have hh := hmax c hcmem
  exact fun he => (abs_pos.mpr hcne).not_ge (by simpa [he] using hh)

/-- Maximal determinant gives coefficients bounded by one in absolute value. -/
lemma maximal_matrix_coordinates (S : Finset (V n)) (A : Matrix (Fin n) (Fin n) ℝ)
    (hA : A.det ≠ 0)
    (hm : ∀ v : Fin n → V n, (∀ j, v j ∈ S) →
      |(Matrix.of v).transpose.det| ≤ |A.det|)
    (hv : ∀ j, A.col j ∈ S) {x : V n} (hx : x ∈ S) :
    ∃ t : V n, (∀ i, |t i| ≤ 1) ∧ A.mulVec t = x := by
  classical
  let e := Matrix.toLinearEquiv (Pi.basisFun ℝ (Fin n)) A (isUnit_iff_ne_zero.mpr hA)
  let t := e.symm x
  have ht : A.mulVec t = x := e.apply_symm_apply x
  refine ⟨t, fun i => ?_, ht⟩
  have hdet := Matrix.det_updateCol_sum A i t
  have hrep' : (fun k => ∑ j, t j • A k j) = x := by
    ext k
    simpa [Matrix.mulVec, dotProduct, smul_eq_mul, mul_comm] using congrFun ht k
  rw [hrep'] at hdet
  have hh := hm (Function.update A.col i x) (by
    intro j
    by_cases hj : j = i
    · subst j; simp [hx]
    · simpa [Function.update_of_ne hj] using hv j)
  have heq : (Matrix.of (Function.update A.col i x)).transpose = A.updateCol i x := by
    ext j k
    by_cases hk : k = i <;> simp [Matrix.updateCol, Matrix.transpose_apply, Matrix.of_apply, Matrix.col,
      hk]
  rw [heq, hdet, smul_eq_mul, abs_mul] at hh
  exact (mul_le_mul_iff_left₀ (abs_pos.mpr hA)).mp (by simpa using hh)

/-- An anchored vertex parallelepiped, scaled by `1/(n+1)`, lies in the polytope. -/
lemma vertex_matrix_volume_bound {P : Set (V n)} (hP : IsLatticePolytope P)
    (h0 : (0 : V n) ∈ P) (A : Matrix (Fin n) (Fin n) ℝ)
    (hv : ∀ j, A.col j ∈ P) :
    |A.det| * (((n : ℝ) + 1)⁻¹)^n ≤ (MeasureTheory.volume P).toReal := by
  classical
  let δ : ℝ := ((n : ℝ) + 1)⁻¹
  have hd : 0 < δ := by dsimp [δ]; positivity
  let B : Set (V n) := Set.Icc 0 (fun _ => δ)
  have hsub : A.mulVec '' B ⊆ P := by
    rintro _ ⟨t, ht, rfl⟩
    have ht0 (i : Fin n) : 0 ≤ t i := ht.1 i
    have htδ (i : Fin n) : t i ≤ δ := ht.2 i
    have hsum : ∑ i, t i ≤ 1 := by
      calc
        ∑ i, t i ≤ ∑ _ : Fin n, δ := Finset.sum_le_sum fun i _ => htδ i
        _ = (n : ℝ) * δ := by simp
        _ ≤ 1 := by
          dsimp [δ]
          rw [← div_eq_mul_inv]
          exact (div_le_one (by positivity)).mpr (by linarith)
    have hh := convex_sum_mem_zero hP.convex h0 A.col hv t ht0 hsum
    have heq : A.mulVec t = ∑ i, t i • A.col i := by
      ext j
      simp [Matrix.mulVec, dotProduct, mul_comm]
    rwa [heq]
  have hvol : MeasureTheory.volume (A.mulVec '' B) =
      ENNReal.ofReal |A.det| * ENNReal.ofReal (δ ^ n) := by
    rw [← Matrix.coe_mulVecLin, ← Matrix.toLin'_apply',
      MeasureTheory.Measure.addHaar_image_linearMap, LinearMap.det_toLin']
    congr 1
    simp [B, Real.volume_Icc_pi, ← ENNReal.ofReal_pow hd.le]
  have hmono := ENNReal.toReal_mono (hP.isCompact.measure_lt_top (μ := MeasureTheory.volume)).ne
    (MeasureTheory.measure_mono hsub)
  rw [hvol, ENNReal.toReal_mul, ENNReal.toReal_ofReal (abs_nonneg _),
    ENNReal.toReal_ofReal (pow_nonneg hd.le _)] at hmono
  exact hmono

/-- Bounded determinants admit uniformly bounded representatives under integral row operations. -/
lemma bounded_matrix_reduction (n K : ℕ) :
    ∃ R : ℕ, ∀ A : ZMat n, A.det ≠ 0 → A.det.natAbs ≤ K →
      ∃ U W B : ZMat n, U * W = 1 ∧ W * U = 1 ∧ U * A = B ∧
        ∀ t : V n, (∀ j, |t j| ≤ 1) → ∀ i, |(realMatrix B).mulVec t i| ≤ (R : ℝ) := by
  classical
  let Ls : Set (Submodule ℤ (ZV n)) :=
    {L | ∃ A : ZMat n, A.det ≠ 0 ∧ A.det.natAbs ≤ K ∧ L = rowSpan A}
  have hf : Ls.Finite := finite_rowSpans_bounded_det n K
  letI := hf.fintype
  have hex (L : Ls) : ∃ B : ZMat n, B.det ≠ 0 ∧ L.val = rowSpan B := by
    obtain ⟨B, hB, _, hL⟩ := L.property
    exact ⟨B, hB, hL⟩
  choose B hB hL using hex
  have hbfin : (Set.range (fun p : Ls × Fin n × Fin n => ((B p.1) p.2.1 p.2.2).natAbs)).Finite :=
    Set.finite_range _
  obtain ⟨r, hr⟩ := hbfin.bddAbove
  refine ⟨n * r, fun A hA hK => ?_⟩
  let L : Ls := ⟨rowSpan A, ⟨A, hA, hK, rfl⟩⟩
  obtain ⟨U, W, hUW, hWU, hUA⟩ := rowSpan_change_basis A (B L) hA (hB L) (hL L)
  refine ⟨U, W, B L, hUW, hWU, hUA, fun t ht i => ?_⟩
  have hb (j : Fin n) : |(realMatrix (B L)) i j| ≤ (r : ℝ) := by
    change |((B L) i j : ℝ)| ≤ (r : ℝ)
    rw [← Int.cast_abs, ← Nat.cast_natAbs]
    exact_mod_cast hr ⟨(L, i, j), rfl⟩
  calc
    |(realMatrix (B L)).mulVec t i| = |∑ j, (realMatrix (B L)) i j * t j| := rfl
    _ ≤ ∑ j, |(realMatrix (B L)) i j * t j| := Finset.abs_sum_le_sum_abs _ _
    _ ≤ ∑ _ : Fin n, (r : ℝ) := by
      apply Finset.sum_le_sum
      intro j _
      rw [abs_mul]
      exact (mul_le_mul_of_nonneg_left (ht j) (abs_nonneg _)).trans (by simpa using hb j)
    _ = ((n * r : ℕ) : ℝ) := by simp

/-- The geometric box lemma, with the volume bound kept as an explicit hypothesis. -/
theorem box_of_volume_bound (n : ℕ) (C : ℝ) :
    ∃ R : ℕ, ∀ P : Set (V n), IsLatticePolytope P → (0 : V n) ∈ interior P →
      (MeasureTheory.volume P).toReal ≤ C →
      ∃ Q : Set (V n), GLEquiv P Q ∧ Q ⊆ {x | ∀ i, |x i| ≤ (R : ℝ)} := by
  classical
  let δ : ℝ := ((n : ℝ) + 1)⁻¹
  have hd : 0 < δ ^ n := by dsimp [δ]; positivity
  obtain ⟨K, hK⟩ := exists_nat_ge (C / δ ^ n)
  obtain ⟨R, hR⟩ := bounded_matrix_reduction n K
  refine ⟨R, fun P hP hip hC => ?_⟩
  obtain ⟨S, hS, hSP⟩ := hP
  obtain ⟨A, hA, hv, hm⟩ := maximal_vertex_matrix S (hSP ▸ hip)
  have hvP (j : Fin n) : A.col j ∈ P := by
    rw [hSP]
    exact subset_convexHull ℝ _ (hv j)
  have hvol := vertex_matrix_volume_bound ⟨S, hS, hSP⟩ (interior_subset hip) A hvP
  have hdetbound : |A.det| ≤ (K : ℝ) :=
    ((le_div_iff₀ hd).mpr (hvol.trans hC)).trans hK
  obtain ⟨AZ, hcast, hdetcast⟩ := exists_intMatrix (A := A) fun i j => hS _ (hv j) i
  have hAZ : AZ.det ≠ 0 := fun he => hA (by rw [← hdetcast, he, Int.cast_zero])
  have hAZK : AZ.det.natAbs ≤ K := by
    have hnatcast : (AZ.det.natAbs : ℝ) = |A.det| := by
      rw [Nat.cast_natAbs, Int.cast_abs, hdetcast]
    exact_mod_cast (hnatcast ▸ hdetbound)
  obtain ⟨U, W, B, hUW, hWU, hUA, hB⟩ := hR AZ hAZ hAZK
  obtain ⟨g, hguni, hg⟩ := integral_linearEquiv U W hUW hWU
  let box : Set (V n) := {x | ∀ i, |x i| ≤ (R : ℝ)}
  have hbox : Convex ℝ box := by
    have heq : box = Set.Icc (fun _ => -(R : ℝ)) (fun _ => (R : ℝ)) := by
      ext x
      simp [box, Set.mem_Icc, Pi.le_def, abs_le, forall_and]
    rw [heq]
    exact convex_Icc _ _
  have hvertex : (S : Set (V n)) ⊆ g ⁻¹' box := by
    intro x hx
    obtain ⟨t, ht, hAt⟩ := maximal_matrix_coordinates S A hA hm hv hx
    have hgt : g x = (realMatrix B).mulVec t := by
      rw [hg, ← hAt, ← hcast, Matrix.mulVec_mulVec, ← Matrix.map_mul, hUA]
    change ∀ i, |g x i| ≤ (R : ℝ)
    rw [hgt]
    exact hB t ht
  have hsub : P ⊆ g ⁻¹' box := by
    rw [hSP]
    exact convexHull_min hvertex (hbox.linear_preimage g.toLinearMap)
  refine ⟨g '' P, ⟨g, hguni, rfl⟩, ?_⟩
  rintro _ ⟨x, hx, rfl⟩
  exact hsub hx

end BoxReduction
end Refpoly
