/-
CWSContainer faithfully ported from the reviewed refpoly-5d snapshot.
See SOURCES.md for attribution and exact source identities.
Container maps are bijective over the reals and forward-integral; inverse
integrality and compatible lattice reconstruction are separate obligations.
-/
import Refpoly.CWSBasis
import Refpoly.Classification

/-! Unconditional geometric CWS containers, with each simplex block reached
by the proved weight-system enumeration. -/
open Set Finset Module Matrix
namespace Refpoly

noncomputable def CircuitBasis.coeffEquiv {n : ℕ} (S : Finset (V n)) :
    (S → ℝ) ≃ₗ[ℝ] V S.card := LinearEquiv.funCongrLeft ℝ ℝ S.equivFin.symm

/-- Zero-padded simplex rows form a genuine combined weight system. -/
noncomputable def CircuitBasis.toCWS {n : ℕ} {S : Finset (V n)} (B : CircuitBasis S)
    (h0 : (0 : V n) ∈ intrinsicInterior ℝ (convexHull ℝ (S : Set (V n)))) :
    CWS B.k S.card where
  W a := CircuitBasis.coeffEquiv S (B.row a)
  nonneg a i := B.nonneg a (S.equivFin.symm i)
  cover i := B.cover h0 (S.equivFin.symm i)
  indep := B.indep.map' (CircuitBasis.coeffEquiv S).toLinearMap
    (LinearMap.ker_eq_bot.mpr (CircuitBasis.coeffEquiv S).injective)

/-- Reindexing does not change the simultaneous circuit equations. -/
theorem CircuitBasis.toCWS_mem {n : ℕ} {S : Finset (V n)} (B : CircuitBasis S)
    (h0 : (0 : V n) ∈ intrinsicInterior ℝ (convexHull ℝ (S : Set (V n)))) (z : S → ℝ) :
    CircuitBasis.coeffEquiv S z ∈ (B.toCWS h0).subspace ↔ z ∈ B.subspace := by
  classical
  rw [CWS.subspace, Submodule.mem_iInf, CircuitBasis.subspace, LinearMap.mem_ker, funext_iff]
  refine forall_congr' fun a => ?_
  rw [LinearMap.mem_ker]
  change (∑ i : Fin S.card, B.row a (S.equivFin.symm i) * z (S.equivFin.symm i)) = 0 ↔ _
  rw [S.equivFin.symm.sum_comp (fun x : S => B.row a x * z x)]
  rfl

/-- The lattice hull associated with a CWS, in its joint solution space. -/
def CWS.polytope {k N : ℕ} (C : CWS k N) : Set C.subspace :=
  convexHull ℝ {y : C.subspace | IsLatticePoint (y : V N) ∧ ∀ i, -1 ≤ (y : V N) i}

/-- Complete circuit equations give an IP CWS container for a reflexive
polytope. Pairing sends all of P into the CWS lattice hull. -/
theorem CircuitBasis.reflexive_container {n : ℕ} {P : Set (V n)} (hP : IsReflexive P)
    {S : Finset (V n)} (B : CircuitBasis S)
    (h0 : (0 : V n) ∈ intrinsicInterior ℝ (convexHull ℝ (S : Set (V n))))
    (hfull : (0 : V n) ∈ interior (convexHull ℝ (S : Set (V n))))
    (hSlat : ∀ x ∈ S, IsLatticePoint x) (hSdual : (S : Set (V n)) ⊆ polarDual P) :
    (B.toCWS h0).IsIP ∧ ∃ F : V n →ₗ[ℝ] (B.toCWS h0).subspace,
      Function.Bijective F ∧
      (∀ x, IsLatticePoint x → IsLatticePoint (F x : V S.card)) ∧
      F '' P ⊆ (B.toCWS h0).polytope := by
  classical
  let L := CircuitBasis.coeffEquiv S
  let Φ : V n →ₗ[ℝ] V S.card :=
    L.toLinearMap.comp (Matrix.mulVecLin (fun x : S => (x : V n)))
  have hΦmem : ∀ x, Φ x ∈ (B.toCWS h0).subspace := by
    intro x
    change CircuitBasis.coeffEquiv S
      ((Matrix.mulVecLin (fun y : S => (y : V n))) x) ∈ (B.toCWS h0).subspace
    apply (B.toCWS_mem h0 _).mpr
    rw [← B.pairing_range]
    exact ⟨x,rfl⟩
  let F : V n →ₗ[ℝ] (B.toCWS h0).subspace := Φ.codRestrict _ hΦmem
  have hFsurj : Function.Surjective F := by
    rintro ⟨z,hz⟩
    have hzin : L.symm z ∈ B.subspace := by
      apply (B.toCWS_mem h0 _).mp
      change L (L.symm z) ∈ (B.toCWS h0).subspace
      rwa [LinearEquiv.apply_symm_apply]
    rw [← B.pairing_range] at hzin
    obtain ⟨x,hx⟩ := hzin
    refine ⟨x,Subtype.ext ?_⟩
    change L ((Matrix.mulVecLin (fun x : S => (x : V n))) x) = z
    rw [hx, LinearEquiv.apply_symm_apply]
  have hFinj : Function.Injective F := by
    rw [← LinearMap.ker_eq_bot, Submodule.eq_bot_iff]
    intro x hx
    have hraw : (Matrix.mulVecLin (fun y : S => (y : V n))) x = 0 := by
      apply L.injective
      have h := congrArg Subtype.val (LinearMap.mem_ker.mp hx)
      change L ((Matrix.mulVecLin (fun y : S => (y : V n))) x) = 0 at h
      simpa only [map_zero] using h
    have hle : Submodule.span ℝ (S : Set (V n)) ≤ LinearMap.ker (dotLin x) := by
      apply Submodule.span_le.mpr
      intro y hy
      apply LinearMap.mem_ker.mpr
      have h := congrFun hraw ⟨y,hy⟩
      change y ⬝ᵥ x = 0 at h
      simpa only [dotLin_apply, dotProduct_comm] using h
    have hspan := BoxReduction.span_vertices_eq_top S hfull
    have hxspan : x ∈ Submodule.span ℝ (S : Set (V n)) := by rw [hspan]; trivial
    exact dotProduct_self_eq_zero.mp (LinearMap.mem_ker.mp (hle hxspan))
  obtain ⟨T,hTlat,hTeq⟩ := hP.latticePolytope
  have hTP : (T : Set (V n)) ⊆ P := by rw [hTeq]; exact subset_convexHull ℝ _
  have hgens : F '' (T : Set (V n)) ⊆
      {y : (B.toCWS h0).subspace | IsLatticePoint (y : V S.card) ∧ ∀ i, -1 ≤ (y : V S.card) i} := by
    rintro _ ⟨x,hx,rfl⟩
    constructor
    · intro i
      exact (hSlat _ (S.equivFin.symm i).property).dotProduct_int (hTlat x hx)
    · intro i
      exact hSdual (S.equivFin.symm i).property x (hTP hx)
  have hsub : F '' P ⊆ (B.toCWS h0).polytope := by
    rw [hTeq, LinearMap.image_convexHull]
    exact convexHull_mono hgens
  have hFlat : ∀ x, IsLatticePoint x → IsLatticePoint (F x : V S.card) := by
    intro x hx i
    exact (hSlat _ (S.equivFin.symm i).property).dotProduct_int hx
  refine ⟨?_, F, ⟨hFinj,hFsurj⟩, hFlat, hsub⟩
  apply interior_mono hsub
  exact (LinearMap.isOpenMap_of_finiteDimensional F hFsurj).image_interior_subset P
    ⟨0,hP.ip,map_zero F⟩

end Refpoly

namespace Refpoly
open Set Finset Module Matrix

/-- A CWS row is obtained by zero-padding a positive simplex weight system
reached by the WS search. The support is an actual subset of the generators. -/
def ReachedCircuit {n : ℕ} (S : Finset (V n)) (row : S → ℝ)
    (sel : ∀ m : ℕ, SelectionRule m) : Prop :=
  ∃ T : Finset (V n), T ⊆ S ∧ T.card ≤ n + 1 ∧
    (¬ AffineIndependent ℝ ((↑) : S → V n) → T.card ≤ n) ∧
    ∃ w : V n → ℝ, row = supportedWeights S T w ∧
      (∀ x ∈ T, 0 < w x) ∧ (∑ x ∈ T, w x = 1) ∧
      ∃ A : Finset (V T.card), Reachable (sel T.card) A ∧
        IsIPWeightSystem ((sel T.card).sel A) ∧
        (sel T.card).sel A = (fun i => w (T.equivFin.symm i)) ∧ A.card ≤ T.card + 1

/-- Every row of the complete circuit basis is reached by WS enumeration,
including lower-dimensional blocks in the non-simplicial case. -/
theorem CircuitBasis.blocks_reached {n : ℕ} {P : Set (V n)} (hP : IsReflexive P)
    {S : Finset (V n)} (B : CircuitBasis S)
    (hSlat : ∀ x ∈ S, IsLatticePoint x) (hSdual : (S : Set (V n)) ⊆ polarDual P)
    (hS : IsMinimalIPGen S) (sel : ∀ m : ℕ, SelectionRule m) :
    ∀ a, ReachedCircuit S (B.row a) sel := by
  classical
  intro a
  obtain ⟨T,hTS,hi,w,hw,hsum,hrel,heq⟩ := B.circuit a
  have hTne : T.Nonempty := T.nonempty_iff_ne_empty.2 (by rintro rfl; simp at hsum)
  letI : Nonempty T := hTne.to_subtype
  -- a simplex on `T` has dimension `|T| - 1`
  have hd : finrank ℝ (vectorSpan ℝ (T : Set (V n))) + 1 = T.card := by
    have h := hi.finrank_vectorSpan_add_one
    rw [Subtype.range_coe_subtype, Fintype.card_coe] at h
    exact h
  have hcard : T.card ≤ n+1 := by
    have hb := Submodule.finrank_le (vectorSpan ℝ (T : Set (V n)))
    rw [Module.finrank_fin_fun] at hb
    omega
  have hsmall : ¬ AffineIndependent ℝ ((↑) : S → V n) → T.card ≤ n := by
    intro hns
    have hp : T ⊂ S := Finset.ssubset_iff_subset_ne.mpr ⟨hTS, fun he => hns (he ▸ hi)⟩
    have hdlt := proper_relativeIP_dim_lt hS hp
      (finset_positive_relation_intrinsicInterior T w hw hsum hrel)
    omega
  let v : Fin T.card → V n := fun i => (T.equivFin.symm i : V n)
  let q : V T.card := fun i => w (v i)
  have hv : AffineIndependent ℝ v := hi.comp_embedding T.equivFin.symm.toEmbedding
  have hq : ∀ i, 0 < q i := fun i => hw _ (T.equivFin.symm i).property
  have hqsum : ∑ i, q i = 1 :=
    (T.equivFin.symm.sum_comp (fun x : T => w x)).trans ((Finset.sum_coe_sort T w).trans hsum)
  have hqrel : ∑ i, q i • v i = 0 :=
    (T.equivFin.symm.sum_comp (fun x : T => w x • (x : V n))).trans
      ((Finset.sum_coe_sort T fun x => w x • x).trans hrel)
  have hlat : ∀ i, IsLatticePoint (v i) := fun i => hSlat _ (hTS (T.equivFin.symm i).property)
  have hdual : ∀ i, v i ∈ polarDual P := fun i => hSdual (hTS (T.equivFin.symm i).property)
  obtain ⟨A,hA,hqA,hcardA⟩ := algorithm_reaches_simplex_block hP v q hv hq hqsum hqrel
    hlat hdual (sel T.card)
  have hIP := reflexive_simplex_block_ip hP v q hv hq hqsum hqrel hlat hdual
  exact ⟨T,hTS,hcard,hsmall,w,heq,hw,hsum,A,hA,hqA.symm ▸ hIP,hqA,hcardA⟩

/-- Every reflexive polytope has an IP CWS container with a complete,
independent equation family, each of whose blocks is reached by WS search.
No simplicial assumption or sharp numerical degree bound is used. -/
theorem algorithm_enumerates_cws_container {n : ℕ} {P : Set (V n)}
    (hP : IsReflexive P) (sel : ∀ m : ℕ, SelectionRule m) :
    ∃ S : Finset (V n), (∀ x ∈ S, IsLatticePoint x) ∧
      (S : Set (V n)) ⊆ polarDual P ∧ IsMinimalIPGen S ∧
      ∃ B : CircuitBasis S, ∃ h0 : (0 : V n) ∈ intrinsicInterior ℝ (convexHull ℝ (S : Set (V n))),
        (B.toCWS h0).IsIP ∧
        (∃ F : V n →ₗ[ℝ] (B.toCWS h0).subspace,
          Function.Bijective F ∧
      (∀ x, IsLatticePoint x → IsLatticePoint (F x : V S.card)) ∧
      F '' P ⊆ (B.toCWS h0).polytope) ∧
        ∀ a, ReachedCircuit S (B.row a) sel := by
  obtain ⟨S,hSlat,hSdual,hS,_⟩ := classification_coverage hP
  have h0 : (0 : V n) ∈ intrinsicInterior ℝ (convexHull ℝ (S : Set (V n))) :=
    interior_subset_intrinsicInterior hS.1
  obtain ⟨B⟩ := exists_circuitBasis S h0
  obtain ⟨hIP,F,hF,hFlat,hsub⟩ := B.reflexive_container hP h0 hS.1 hSlat hSdual
  exact ⟨S,hSlat,hSdual,hS,B,h0,hIP,⟨F,hF,hFlat,hsub⟩,B.blocks_reached hP hSlat hSdual hS sel⟩

end Refpoly

namespace Refpoly
open Set Finset Module Matrix

/-- Mathematical specification of exhaustive CWS assembly: injectively
embed reached WS rows, zero-pad outside each support, and retain independent
covering equations as required by `CWS`. -/
def CWS.RowsReached {k N : ℕ} (C : CWS k N) (maxLength : ℕ)
    (sel : ∀ m : ℕ, SelectionRule m) : Prop :=
  ∀ a, ∃ m ≤ maxLength, ∃ e : Fin m ↪ Fin N, ∃ A : Finset (V m),
    Reachable (sel m) A ∧ A.card ≤ m+1 ∧ IsIPWeightSystem ((sel m).sel A) ∧
      (∀ j, 0 < (sel m).sel A j) ∧
      (∑ j, (sel m).sel A j = 1) ∧
      (∀ j, C.W a (e j) = (sel m).sel A j) ∧
      ∀ i, (∀ j, e j ≠ i) → C.W a i = 0

/-- Reached geometric circuits satisfy the explicit zero-padding and support
specification for CWS assembly. -/
theorem CircuitBasis.rows_reached {n : ℕ} {S : Finset (V n)} (B : CircuitBasis S)
    (h0 : (0 : V n) ∈ intrinsicInterior ℝ (convexHull ℝ (S : Set (V n))))
    (sel : ∀ m : ℕ, SelectionRule m) (hr : ∀ a, ReachedCircuit S (B.row a) sel) :
    (B.toCWS h0).RowsReached (n+1) sel ∧
      (¬ AffineIndependent ℝ ((↑) : S → V n) → (B.toCWS h0).RowsReached n sel) := by
  classical
  have hgeneric (bound : ℕ) (hb : bound = n+1 ∨
      (bound = n ∧ ¬ AffineIndependent ℝ ((↑) : S → V n))) :
      (B.toCWS h0).RowsReached bound sel := by
    intro a
    obtain ⟨T,hTS,hcard,hsmall,w,heq,hw,hsum,A,hA,hIP,hq,hcardA⟩ := hr a
    have hbound : T.card ≤ bound := by
      rcases hb with rfl | ⟨rfl,hns⟩
      · exact hcard
      · exact hsmall hns
    let e : Fin T.card ↪ Fin S.card :=
      { toFun := fun j => S.equivFin ⟨(T.equivFin.symm j : V n),hTS (T.equivFin.symm j).property⟩
        inj' := by
          intro i j h
          have h' := S.equivFin.injective h
          have h'' := congrArg Subtype.val h'
          exact T.equivFin.symm.injective (Subtype.ext h'') }
    have hrow (i : Fin S.card) : (B.toCWS h0).W a i =
        if (S.equivFin.symm i : V n) ∈ T then w (S.equivFin.symm i) else 0 := by
      change B.row a (S.equivFin.symm i) = _
      rw [heq]; rfl
    refine ⟨T.card,hbound,e,A,hA,hcardA,hIP,?_,?_,?_,?_⟩
    · intro j
      rw [hq]
      exact hw _ (T.equivFin.symm j).property
    · rw [hq, T.equivFin.symm.sum_comp (fun x : T => w x), Finset.sum_coe_sort T w, hsum]
    · intro j
      rw [hq, hrow]
      simp [e, (T.equivFin.symm j).property]
    · intro i hi
      have hnot : (S.equivFin.symm i : V n) ∉ T := by
        intro hx
        let j := T.equivFin ⟨(S.equivFin.symm i : V n),hx⟩
        apply hi j
        simp [e,j]
      rw [hrow,if_neg hnot]
  exact ⟨hgeneric (n+1) (Or.inl rfl),fun hn => hgeneric n (Or.inr ⟨rfl,hn⟩)⟩

/-- Unconditional WS+CWS container coverage under exhaustive row assembly.
In dimension five, non-simplicial containers use only WS blocks of length
at most five; simplicial containers use the single length-six WS case. -/
theorem ws_cws_container_coverage {n : ℕ} {P : Set (V n)} (hP : IsReflexive P)
    (sel : ∀ m : ℕ, SelectionRule m) :
    ∃ S : Finset (V n), IsMinimalIPGen S ∧
      ∃ B : CircuitBasis S, ∃ h0 : (0 : V n) ∈ intrinsicInterior ℝ (convexHull ℝ (S : Set (V n))),
        (B.toCWS h0).IsIP ∧ (B.toCWS h0).RowsReached (n+1) sel ∧
        (¬ AffineIndependent ℝ ((↑) : S → V n) → (B.toCWS h0).RowsReached n sel) ∧
        ∃ F : V n →ₗ[ℝ] (B.toCWS h0).subspace,
          Function.Bijective F ∧
      (∀ x, IsLatticePoint x → IsLatticePoint (F x : V S.card)) ∧
      F '' P ⊆ (B.toCWS h0).polytope := by
  obtain ⟨S,_,_,hS,B,h0,hIP,hF,hr⟩ := algorithm_enumerates_cws_container hP sel
  obtain ⟨hrows,hsmall⟩ := B.rows_reached h0 sel hr
  exact ⟨S,hS,B,h0,hIP,hrows,hsmall,hF⟩

end Refpoly
