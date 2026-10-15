import Refpoly.KS.Assembly
import Mathlib.LinearAlgebra.FreeModule.PID
import Mathlib.LinearAlgebra.Matrix.Basis

open Set Matrix Module

namespace Refpoly.KS

namespace IntegerKernel

def intRows {d : ℕ} (C : CWSCode d) : Matrix (Fin C.r) (Fin C.N) ℤ :=
  fun a j => C.rows a j

def columns {d : ℕ} (C : CWSCode d) (h : d ≤ C.N)
    (U : Matrix (Fin C.N) (Fin C.N) ℤ) : Matrix (Fin C.N) (Fin d) ℤ :=
  fun j i => U j (Fin.castLE h i)

/-- A unimodular completion, not merely a rationally independent kernel family. -/
def Certificate {d : ℕ} (C : CWSCode d) (h : d ≤ C.N)
    (U : Matrix (Fin C.N) (Fin C.N) ℤ) : Prop :=
  (U.det = 1 ∨ U.det = -1) ∧ intRows C * columns C h U = 0

/-- Laplace recursion avoids repeatedly enumerating permutations during the search. -/
def determinant : {n : ℕ} → Matrix (Fin n) (Fin n) ℤ → ℤ
  | 0, _ => 1
  | n + 1, A => ∑ j : Fin (n + 1),
      (-1 : ℤ) ^ (j : ℕ) * A 0 j * determinant (A.submatrix Fin.succ j.succAbove)

theorem determinant_eq_det {n : ℕ} (A : Matrix (Fin n) (Fin n) ℤ) :
    determinant A = A.det := by
  induction n with
  | zero => simp [determinant, Matrix.det_isEmpty]
  | succ n ih =>
    rw [Matrix.det_succ_row_zero]
    simp only [determinant, ih]

instance {d : ℕ} (C : CWSCode d) (h : d ≤ C.N)
    (U : Matrix (Fin C.N) (Fin C.N) ℤ) : Decidable (Certificate C h U) :=
  decidable_of_iff
    ((determinant U = 1 ∨ determinant U = -1) ∧ intRows C * columns C h U = 0)
    (by simp only [Certificate, determinant_eq_det])

theorem intRows_independent {d : ℕ} (C : CWSCode d)
    (h : LinearIndependent ℝ (realRows C)) : LinearIndependent ℤ (intRows C) := by
  let castLinear : (Fin C.N → ℤ) →ₗ[ℤ] (Fin C.N → ℝ) :=
    { toFun := fun x j => (x j : ℝ)
      map_add' := by intro x y; ext j; simp
      map_smul' := by intro a x; ext j; simp }
  apply LinearIndependent.of_comp castLinear
  simpa [castLinear, intRows, realRows, Function.comp_def] using h.restrict_scalars' ℤ

/-- SNF is used only to prove termination of the arithmetic search below. -/
theorem certificate_exists {d : ℕ} (C : CWSCode d) (hv : ValidCWS C)
    (hd : d ≤ C.N) :
    ∃ U : Matrix (Fin C.N) (Fin C.N) ℤ, Certificate C hd U := by
  classical
  let S := Submodule.span ℤ (range (intRows C))
  have hli := intRows_independent C hv.2.2.2.1
  obtain ⟨n, snf⟩ := S.smithNormalForm (Pi.basisFun ℤ (Fin C.N))
  have hn : n = C.r := by
    simpa using Fintype.card_congr (snf.bN.indexEquiv (Basis.span hli))
  subst n
  let q : Fin C.r → Fin C.N := fun a => Fin.cast hv.1 (Fin.natAdd d a)
  have hq : Function.Injective q :=
    (finCongr hv.1).injective.comp (Fin.natAdd_injective C.r d)
  obtain ⟨σ, hσ⟩ := Equiv.Perm.exists_extending_pair q snf.f hq snf.f.injective
  have hnot (i : Fin d) : σ (Fin.castLE hd i) ∉ range snf.f := by
    rintro ⟨a, ha⟩
    have hqi := σ.injective ((hσ a).trans ha)
    have hval := congrArg Fin.val hqi
    simp only [q, Fin.val_cast, Fin.val_natAdd, Fin.val_castLE] at hval
    omega
  let b := snf.bM.reindex σ.symm
  let e := Pi.basisFun ℤ (Fin C.N)
  let T := b.toMatrix e
  let U := Tᵀ
  have hunitT : IsUnit T := by
    letI := b.invertibleToMatrix e
    exact isUnit_of_invertible T
  have hunitU : IsUnit U := (Matrix.isUnit_transpose T).mpr hunitT
  refine ⟨U, Int.isUnit_iff.mp ((Matrix.isUnit_iff_isUnit_det U).mp hunitU), ?_⟩
  ext a i
  have hzero : b.repr (intRows C a) (Fin.castLE hd i) = 0 := by
    simpa [b, Basis.repr_reindex_apply] using
      snf.repr_eq_zero_of_notMem_range
        (⟨intRows C a, Submodule.subset_span (mem_range_self a)⟩ : S) (hnot i)
  have hrepr := congrFun (e.toMatrix_mulVec_repr b (intRows C a)) (Fin.castLE hd i)
  simpa [T, U, e, columns, Matrix.mul_apply, Matrix.mulVec, dotProduct,
    Pi.basisFun_repr, mul_comm] using hrepr.trans hzero

theorem kernel_finrank {d : ℕ} (C : CWSCode d) (hv : ValidCWS C) :
    Module.finrank ℝ (kernelSpace C) = d := by
  have hrank : (Matrix.of (realRows C)).rank = C.r := by
    simpa using hv.2.2.2.1.rank_matrix
  have h := (Matrix.mulVecLin (realRows C)).finrank_range_add_finrank_ker
  change (Matrix.of (realRows C)).rank + Module.finrank ℝ (kernelSpace C) = _ at h
  rw [hrank, Module.finrank_pi, Fintype.card_fin] at h
  have hdim := hv.1
  omega

theorem certificate_correct {d : ℕ} (C : CWSCode d) (hv : ValidCWS C)
    (hd : d ≤ C.N) (U : Matrix (Fin C.N) (Fin C.N) ℤ)
    (hc : Certificate C hd U) : KernelBasisCorrect C (columns C hd U) := by
  classical
  let cast := Int.castRingHom ℝ
  have hunit : IsUnit U.det := Int.isUnit_iff.mpr hc.1
  have hdet : (U.map cast).det ≠ 0 := by
    have heq : cast U.det = (U.map cast).det := cast.map_det U
    rw [← heq]
    exact (hunit.map cast).ne_zero
  have hli := (Matrix.linearIndependent_cols_of_det_ne_zero hdet).comp
    (Fin.castLE hd) (Fin.castLE_injective hd)
  have hinj : Function.Injective (basisMap (columns C hd U)) :=
    Matrix.mulVec_injective_iff.mpr hli
  have hmul : Matrix.of (realRows C) * (columns C hd U).map cast = 0 := by
    have hrows : (intRows C).map cast = Matrix.of (realRows C) := by
      ext a j
      simp [intRows, realRows, cast]
    have h := congrArg (fun A => A.map cast) hc.2
    dsimp only at h
    rw [Matrix.map_mul, hrows] at h
    simpa only [map_zero, Matrix.map_zero] using h
  have hle : LinearMap.range (basisMap (columns C hd U)) ≤ kernelSpace C := by
    rw [kernelSpace, LinearMap.range_le_ker_iff]
    change (Matrix.mulVecLin (realRows C)).comp
      (Matrix.mulVecLin ((columns C hd U).map cast)) = 0
    simpa only [Matrix.mulVecLin_mul, Matrix.mulVecLin_zero] using
      congrArg Matrix.mulVecLin hmul
  have hrange : LinearMap.range (basisMap (columns C hd U)) = kernelSpace C := by
    apply Submodule.eq_of_le_of_finrank_eq hle
    rw [LinearMap.finrank_range_of_inj hinj, kernel_finrank C hv]
    exact Module.finrank_fin_fun ℝ
  refine ⟨hinj, hrange, ?_⟩
  intro y hy hyl
  obtain ⟨x, hx⟩ := (show y ∈ LinearMap.range (basisMap (columns C hd U)) by
    rwa [hrange])
  refine ⟨x, ?_, hx⟩
  let W := U⁻¹
  have hWU : W * U = 1 := Matrix.nonsing_inv_mul U hunit
  have hWB : W * columns C hd U =
      (1 : Matrix (Fin C.N) (Fin C.N) ℤ).submatrix id (Fin.castLE hd) := by
    ext j i
    exact congrFun (congrFun hWU j) (Fin.castLE hd i)
  have hWBReal : W.map cast * (columns C hd U).map cast =
      fun j i => if j = Fin.castLE hd i then (1 : ℝ) else 0 := by
    rw [← Matrix.map_mul, hWB]
    ext j i
    simp [Matrix.one_apply, cast]
  have hcoord (i : Fin d) :
      ((W.map cast) *ᵥ basisMap (columns C hd U) x) (Fin.castLE hd i) = x i := by
    change ((W.map cast) *ᵥ ((columns C hd U).map cast *ᵥ x)) _ = _
    rw [Matrix.mulVec_mulVec, hWBReal]
    simp [Matrix.mulVec, dotProduct, Fin.castLE_inj]
  choose z hz using hyl
  intro i
  refine ⟨∑ j, W (Fin.castLE hd i) j * z j, ?_⟩
  rw [← hcoord i, hx]
  simp [Matrix.mulVec, dotProduct, hz, cast]

/-- The exhaustive integer interval; its radius will be found, not assumed. -/
def integerBox (B : ℕ) : List ℤ :=
  (List.range (2 * B + 1)).map (fun n : ℕ => (n : ℤ) - B)

theorem mem_integerBox {B : ℕ} {z : ℤ} :
    z ∈ integerBox B ↔ -(B : ℤ) ≤ z ∧ z ≤ B := by
  simp only [integerBox, List.mem_map, List.mem_range]
  constructor
  · rintro ⟨n, hn, rfl⟩
    omega
  · rintro ⟨hl, hu⟩
    refine ⟨(z + B).toNat, by omega, ?_⟩
    omega

def vectorBox (N B : ℕ) : List (Fin N → ℤ) :=
  tuples (integerBox B) N

def kernelBox {d : ℕ} (C : CWSCode d) (B : ℕ) : List (Fin C.N → ℤ) :=
  (vectorBox C.N B).filter (fun v => decide (intRows C *ᵥ v = 0))

def kernelTuples {d : ℕ} (C : CWSCode d) (B : ℕ) :
    List (Fin d → Fin C.N → ℤ) :=
  (tuples (kernelBox C B) d).filter fun k =>
    decide (Function.Injective k ∧ ∀ i, k i ≠ 0)

def completion {d : ℕ} (C : CWSCode d) (heq : d + C.r = C.N)
    (k : Fin d → Fin C.N → ℤ) (t : Fin C.r → Fin C.N → ℤ) :
    Matrix (Fin C.N) (Fin C.N) ℤ :=
  fun j l => Fin.append k t (Fin.cast heq.symm l) j

theorem completion_eta {d : ℕ} (C : CWSCode d) (heq : d + C.r = C.N)
    (hd : d ≤ C.N) (U : Matrix (Fin C.N) (Fin C.N) ℤ) :
    completion C heq (fun i j => U j (Fin.castLE hd i))
      (fun a j => U j (Fin.cast heq (Fin.natAdd d a))) = U := by
  have hleft (i : Fin d) :
      Fin.cast heq (Fin.castAdd C.r i) = Fin.castLE hd i := Fin.ext rfl
  have h : Fin.append (fun i j => U j (Fin.castLE hd i))
      (fun a j => U j (Fin.cast heq (Fin.natAdd d a))) =
      fun l j => U j (Fin.cast heq l) := by
    funext l
    refine Fin.addCases (fun i => ?_) (fun a => ?_) l
    · simp [hleft]
    · simp
  ext j l
  simp only [completion, h, Fin.cast_cast, Fin.cast_eq_self]

/-- Search kernel columns first, then their unimodular completions. Both loops are finite. -/
def boundedWitness {d : ℕ} (C : CWSCode d) (hd : d ≤ C.N)
    (heq : d + C.r = C.N) (B : ℕ) :
    Option (Matrix (Fin C.N) (Fin C.N) ℤ) :=
  (kernelTuples C B).findSome? fun k =>
    (tuples (vectorBox C.N B) C.r).findSome? fun t =>
      let U := completion C heq k t
      if Certificate C hd U then some U else none

theorem boundedWitness_sound {d : ℕ} (C : CWSCode d) (hd : d ≤ C.N)
    (heq : d + C.r = C.N) (B : ℕ) (U : Matrix (Fin C.N) (Fin C.N) ℤ)
    (h : boundedWitness C hd heq B = some U) : Certificate C hd U := by
  obtain ⟨_, k, _, _, hk, _⟩ := List.findSome?_eq_some_iff.mp h
  obtain ⟨_, t, _, _, ht, _⟩ := List.findSome?_eq_some_iff.mp hk
  dsimp only at ht
  split_ifs at ht with hc
  · cases ht
    exact hc

theorem boundedWitness_complete {d : ℕ} (C : CWSCode d) (hd : d ≤ C.N)
    (heq : d + C.r = C.N) (B : ℕ) (U : Matrix (Fin C.N) (Fin C.N) ℤ)
    (hc : Certificate C hd U) (hB : ∀ j i, U j i ∈ integerBox B) :
    (boundedWitness C hd heq B).isSome = true := by
  let k : Fin d → Fin C.N → ℤ := fun i j => U j (Fin.castLE hd i)
  let t : Fin C.r → Fin C.N → ℤ := fun a j => U j (Fin.cast heq (Fin.natAdd d a))
  have hk : k ∈ tuples (kernelBox C B) d := by
    apply (mem_tuples _ _ _).mpr
    intro i
    simp only [kernelBox, List.mem_filter, decide_eq_true_eq]
    refine ⟨(mem_tuples _ _ _).mpr (fun j => hB j _), ?_⟩
    ext a
    exact congrFun (congrFun hc.2 a) i
  have ht : t ∈ tuples (vectorBox C.N B) C.r :=
    (mem_tuples _ _ _).mpr fun a => (mem_tuples _ _ _).mpr fun j => hB j _
  have hli : LinearIndependent ℤ k :=
    (Matrix.linearIndependent_cols_of_det_ne_zero (Int.isUnit_iff.mpr hc.1).ne_zero).comp
      (Fin.castLE hd) (Fin.castLE_injective hd)
  have hk' : k ∈ kernelTuples C B := by
    simp only [kernelTuples, List.mem_filter, decide_eq_true_eq]
    exact ⟨hk, hli.injective, fun i => hli.ne_zero i⟩
  have hU : completion C heq k t = U := completion_eta C heq hd U
  apply Option.isSome_iff_ne_none.mpr
  intro hnone
  have hi := List.findSome?_eq_none_iff.mp hnone k hk'
  have hf := List.findSome?_eq_none_iff.mp hi t ht
  have hfalse : some U = none := by simpa only [hU, if_pos hc] using hf
  cases hfalse

theorem integerBox_bounds {N : ℕ} (U : Matrix (Fin N) (Fin N) ℤ) :
    ∃ B, ∀ j i, U j i ∈ integerBox B := by
  let B := Finset.univ.sup fun j => Finset.univ.sup fun i => (U j i).natAbs
  refine ⟨B, fun j i => mem_integerBox.mpr ?_⟩
  have habs : (U j i).natAbs ≤ B :=
    (Finset.le_sup (s := Finset.univ) (f := fun i => (U j i).natAbs)
      (Finset.mem_univ i)).trans
      (Finset.le_sup (s := Finset.univ)
        (f := fun j => Finset.univ.sup fun i => (U j i).natAbs) (Finset.mem_univ j))
  have hpos : U j i ≤ ((U j i).natAbs : ℤ) := Int.le_natAbs
  have hneg : -U j i ≤ ((U j i).natAbs : ℤ) := by
    simpa only [Int.natAbs_neg] using (Int.le_natAbs (a := -U j i))
  constructor <;> omega

theorem boundedWitness_exists {d : ℕ} (C : CWSCode d) (hv : ValidCWS C)
    (hd : d ≤ C.N) :
    ∃ B, (boundedWitness C hd hv.1 B).isSome = true := by
  obtain ⟨U, hc⟩ := certificate_exists C hv hd
  obtain ⟨B, hB⟩ := integerBox_bounds U
  exact ⟨B, boundedWitness_complete C hd hv.1 B U hc hB⟩

/-- Only finite arithmetic and `Nat.find` contribute to the returned data. -/
def certifiedCompletion {d : ℕ} (C : CWSCode d) (hv : ValidCWS C) :
    {U : Matrix (Fin C.N) (Fin C.N) ℤ //
      Certificate C (by have h := hv.1; omega) U} :=
  let hd : d ≤ C.N := by have h := hv.1; omega
  let hex := boundedWitness_exists C hv hd
  let B := Nat.find hex
  let o := boundedWitness C hd hv.1 B
  have ho : o.isSome = true := Nat.find_spec hex
  ⟨o.get ho, boundedWitness_sound C hd hv.1 B _ (Option.some_get ho).symm⟩

end IntegerKernel

/-- Exact saturated integer kernel basis on valid input; malformed inputs return zero. -/
def integerKernelBasisExact {d : ℕ} (C : CWSCode d) : Fin C.N → Fin d → ℤ :=
  if h : validCWSCheck C = true then
    let hv := (validCWSCheck_iff C).mp h
    IntegerKernel.columns C (by have heq := hv.1; omega)
      (IntegerKernel.certifiedCompletion C hv).val
  else 0

theorem integerKernelBasisExact_correct {d : ℕ} (C : CWSCode d) (hv : ValidCWS C) :
    KernelBasisCorrect C (integerKernelBasisExact C) := by
  rw [integerKernelBasisExact, dif_pos ((validCWSCheck_iff C).mpr hv)]
  exact IntegerKernel.certificate_correct C hv _ _
    (IntegerKernel.certifiedCompletion C _).property

theorem integerKernelBasisExact_of_invalid {d : ℕ} (C : CWSCode d) (hv : ¬ValidCWS C) :
    integerKernelBasisExact C = 0 := by
  have hcheck : validCWSCheck C ≠ true := fun h => hv ((validCWSCheck_iff C).mp h)
  simp only [integerKernelBasisExact, dif_neg hcheck]

end Refpoly.KS
