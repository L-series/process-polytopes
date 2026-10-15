import Refpoly.CWSBasis
import Refpoly.KS.CircuitWeights
import Refpoly.KS.Contracts

open Set Matrix Module
namespace Refpoly.KS

/-- Primitive integer encoding of a complete geometric circuit basis. -/
theorem circuitBasis_of_validSeed : CircuitBasis := by
  classical
  intro d _ P hP S hS
  let T : Finset (V d) := Finset.univ.image S.vertices
  have hT : (T : Set (V d)) = range S.vertices := by simp [T]
  let f : Fin S.N → T := fun i => ⟨S.vertices i, by simp [T]⟩
  have hf : Function.Bijective f := by
    constructor
    · intro i j hij
      exact hS.1 (congrArg Subtype.val hij)
    · intro x
      obtain ⟨i, _, hi⟩ := Finset.mem_image.mp x.property
      exact ⟨i, Subtype.ext hi⟩
  let eT : Fin S.N ≃ T := Equiv.ofBijective f hf
  let E : (T → ℝ) ≃ₗ[ℝ] V S.N := LinearEquiv.funCongrLeft ℝ ℝ eT
  have hlat : ∀ x ∈ T, IsLatticePoint x := by
    intro x hx
    have hx' : x ∈ range S.vertices := by rw [← hT]; exact hx
    obtain ⟨i, rfl⟩ := hx'
    exact (hS.2.1 i).1
  have hdual : (T : Set (V d)) ⊆ polarDual P := by
    rintro x hx
    obtain ⟨i, rfl⟩ := hT ▸ hx
    exact (hS.2.1 i).2
  have hfull : (0 : V d) ∈ interior (convexHull ℝ (T : Set (V d))) := by
    rw [hT]
    exact hS.2.2.1
  have h0 : (0 : V d) ∈ intrinsicInterior ℝ (convexHull ℝ (T : Set (V d))) :=
    interior_subset_intrinsicInterior hfull
  obtain ⟨B⟩ := exists_circuitBasis T h0
  have hrows : ∀ a, ∃ m, m ≤ d + 1 ∧ ∃ e : Fin m ↪ Fin S.N, ∃ w : WeightCode m,
      PrimitiveIP w ∧ AffineIndependent ℝ (fun i => S.vertices (e i)) ∧
      (∑ i, (normalized w i : ℝ) • S.vertices (e i)) = 0 ∧
      (fun j => (pad e w j : ℝ)) = ((∑ i, w i : ℕ) : ℝ) • E (B.row a) := by
    intro a
    obtain ⟨U, hUT, hiU, q, hq, hsum, hrel, heq⟩ := B.circuit a
    let e : Fin U.card ↪ Fin S.N :=
      { toFun := fun i => eT.symm ⟨(U.equivFin.symm i : V d), hUT (U.equivFin.symm i).property⟩
        inj' := by
          intro i j hij
          have h := congrArg Subtype.val (eT.symm.injective hij)
          exact U.equivFin.symm.injective (Subtype.ext h) }
    let v : Fin U.card → V d := fun i => (U.equivFin.symm i : V d)
    let qU : V U.card := fun i => q (v i)
    have hev (i : Fin U.card) : S.vertices (e i) = v i := by
      have h := congrArg Subtype.val (eT.apply_symm_apply
        ⟨(U.equivFin.symm i : V d), hUT (U.equivFin.symm i).property⟩)
      exact h
    have hv : AffineIndependent ℝ v := hiU.comp_embedding U.equivFin.symm.toEmbedding
    have hqpos : ∀ i, 0 < qU i := fun i => hq _ (U.equivFin.symm i).property
    have hqsum : ∑ i, qU i = 1 :=
      (U.equivFin.symm.sum_comp (fun x : U => q x)).trans ((Finset.sum_coe_sort U q).trans hsum)
    have hqrel : ∑ i, qU i • v i = 0 :=
      (U.equivFin.symm.sum_comp (fun x : U => q x • (x : V d))).trans
        ((Finset.sum_coe_sort U fun x => q x • x).trans hrel)
    have hvlat : ∀ i, IsLatticePoint (v i) :=
      fun i => hlat _ (hUT (U.equivFin.symm i).property)
    have hvdual : ∀ i, v i ∈ polarDual P :=
      fun i => hdual (hUT (U.equivFin.symm i).property)
    have hIP := reflexive_simplex_block_ip hP v qU hv hqpos hqsum hqrel hvlat hvdual
    obtain ⟨w, hw, hwq⟩ := exists_primitive_simplex_weights v qU hvlat hv hqsum hqrel hIP
    have hm : U.card ≤ d + 1 := by
      have h := hiU.card_le_finrank_succ
      rw [Fintype.card_coe] at h
      have hf := Submodule.finrank_le (vectorSpan ℝ (Set.range ((↑) : U → V d)))
      rw [Module.finrank_fin_fun] at hf
      omega
    refine ⟨U.card, hm, e, w, hw, ?_, ?_, ?_⟩
    · simpa only [hev] using hv
    · have hn : ∀ i, (normalized w i : ℝ) = qU i := congrFun hwq
      simpa only [hev, hn] using hqrel
    · have hD : (0 : ℝ) < (∑ i, w i : ℕ) := by
        exact_mod_cast weight_degree_pos hw.1
      ext j
      by_cases hj : (eT j : V d) ∈ U
      · let i := U.equivFin ⟨(eT j : V d), hj⟩
        have hei : e i = j := by
          apply eT.injective
          apply Subtype.ext
          simp [e, i]
        have hqi := congrFun hwq i
        have hwi : (w i : ℝ) = ((∑ k, w k : ℕ) : ℝ) * q (eT j) := by
          have hh : (normalized w i : ℝ) = q (eT j) := by simpa [qU, v, i] using hqi
          have hc : (w i : ℝ) / ((∑ k, w k : ℕ) : ℝ) = q (eT j) := by
            simpa [normalized] using hh
          nlinarith [(div_eq_iff (ne_of_gt hD)).mp hc]
        rw [← hei, pad_on_support]
        change (w i : ℝ) = ((∑ k, w k : ℕ) : ℝ) * B.row a (eT (e i))
        rw [hei, heq]
        simpa [supportedWeights, hj] using hwi
      · have hoff : ∀ i, e i ≠ j := by
          intro i hij
          apply hj
          have hvj := hev i
          have hpoint : (eT j : V d) = S.vertices j := rfl
          rw [hpoint, ← hij, hvj]
          exact (U.equivFin.symm i).property
        rw [pad_off_support e w j hoff]
        simp only [Nat.cast_zero, Pi.smul_apply, smul_eq_mul]
        change (0 : ℝ) = ((∑ k, w k : ℕ) : ℝ) * B.row a (eT j)
        rw [heq]
        simp [supportedWeights, hj]
  choose m hm e w hw hai hrel hscale using hrows
  let R : RelationBasis S := ⟨B.k, fun a => pad (e a) (w a)⟩
  have hc (a : Fin B.k) : (0 : ℝ) < (∑ i, w a i : ℕ) := by
    exact_mod_cast weight_degree_pos (hw a).1
  have hreal : realRows R.code = fun a => ((∑ i, w a i : ℕ) : ℝ) • E (B.row a) := by
    funext a
    exact hscale a
  have hind0 := B.indep.map' E.toLinearMap (LinearMap.ker_eq_bot.mpr E.injective)
  have hind : LinearIndependent ℝ (realRows R.code) := by
    rw [hreal]
    exact hind0.units_smul (fun a => Units.mk0 _ (ne_of_gt (hc a)))
  have hspan : Submodule.span ℝ (range (realRows R.code)) =
      Submodule.span ℝ (range (fun a => E (B.row a))) := by
    apply le_antisymm
    · apply Submodule.span_le.mpr
      rintro _ ⟨a, rfl⟩
      have hh : realRows R.code a ∈ Submodule.span ℝ (range (fun a => E (B.row a))) := by
        rw [congrFun hreal a]
        exact (Submodule.span ℝ (range (fun a => E (B.row a)))).smul_mem
          _ (Submodule.subset_span (mem_range_self a))
      exact hh
    · apply Submodule.span_le.mpr
      rintro _ ⟨a, rfl⟩
      have h := Submodule.subset_span (R := ℝ) (mem_range_self (f := realRows R.code) a)
      have heq : E (B.row a) = (((∑ i, w a i : ℕ) : ℝ)⁻¹) • realRows R.code a := by
        rw [congrFun hreal a]
        funext j
        change (E (B.row a)) j = (((∑ i, w a i : ℕ) : ℝ)⁻¹) *
          (((∑ i, w a i : ℕ) : ℝ) * (E (B.row a)) j)
        rw [← mul_assoc, inv_mul_cancel₀ (ne_of_gt (hc a)), one_mul]
      change E (B.row a) ∈ Submodule.span ℝ (range (realRows R.code))
      rw [heq]
      exact (Submodule.span ℝ (range (realRows R.code))).smul_mem _ h
  have hcomb (z : T → ℝ) :
      Matrix.mulVecLin (fun i j => S.vertices j i) (E z) = vertexCombination T z := by
    ext i
    simp only [Matrix.mulVecLin_apply, Matrix.mulVec, dotProduct, vertexCombination_apply]
    change (∑ j, S.vertices j i * z (eT j)) = (∑ x : T, z x • (x : V d)) i
    rw [Finset.sum_apply]
    simpa only [Pi.smul_apply, smul_eq_mul, mul_comm] using
      eT.sum_comp (fun x : T => z x * (x : V d) i)
  have hkernel : Submodule.span ℝ (range (realRows R.code)) =
      LinearMap.ker (Matrix.mulVecLin (fun i j => S.vertices j i)) := by
    rw [hspan]
    have hmap : Submodule.span ℝ (range (fun a => E (B.row a))) =
        Submodule.map E.toLinearMap (Submodule.span ℝ (range B.row)) := by
      rw [Submodule.map_span, ← Set.range_comp]
      rfl
    rw [hmap, B.spans]
    ext z
    constructor
    · rintro ⟨x, hx, rfl⟩
      change Matrix.mulVecLin (fun i j => S.vertices j i) (E x) = 0
      rw [hcomb]
      exact LinearMap.mem_ker.mp hx
    · intro hz
      refine ⟨E.symm z, ?_, E.apply_symm_apply z⟩
      change vertexCombination T (E.symm z) = 0
      rw [← hcomb, E.apply_symm_apply]
      exact LinearMap.mem_ker.mp hz
  have hcard : S.N = d + B.k := by
    have he := Fintype.card_congr eT
    simp only [Fintype.card_fin, Fintype.card_coe] at he
    exact he.trans (B.coordinate_count hfull)
  have hvalid : ValidCWS R.code := by
    refine ⟨hcard.symm, hS.2.2.2.2.2, ?_, hind, ?_⟩
    · have hn := hS.2.2.2.2.2
      change B.k ≤ d
      omega
    · intro j
      obtain ⟨a, ha⟩ := B.cover h0 (eT j)
      refine ⟨a, ?_⟩
      have hh := congrFun (hscale a) j
      have hp : (0 : ℝ) < (pad (e a) (w a) j : ℝ) := by
        rw [hh]
        exact mul_pos (hc a) ha
      exact_mod_cast hp
  refine ⟨R, hvalid, hkernel, fun a => ?_⟩
  exact ⟨m a, hm a, e a, w a, hw a, rfl, hai a, hrel a⟩

end Refpoly.KS
