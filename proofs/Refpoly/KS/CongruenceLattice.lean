import Refpoly.KS.Lattices
import Refpoly.KS.IntegerPoints

open Set Matrix Module

namespace Refpoly.KS

abbrev ResiduePoint (d n : ℕ) := Fin d → ZMod n

def ValidResidues {d n : ℕ} (S : List (ResiduePoint d n)) : Prop :=
  0 ∈ S ∧ (∀ x ∈ S, ∀ y ∈ S, x + y ∈ S) ∧ ∀ x ∈ S, -x ∈ S

instance {d n : ℕ} (S : List (ResiduePoint d n)) : Decidable (ValidResidues S) := by
  unfold ValidResidues
  infer_instance

def residueGroup {d n : ℕ} (S : List (ResiduePoint d n)) (hS : ValidResidues S) :
    AddSubgroup (ResiduePoint d n) where
  carrier := {x | x ∈ S}
  zero_mem' := hS.1
  add_mem' := by intro x y hx hy; exact hS.2.1 x hx y hy
  neg_mem' := by intro x hx; exact hS.2.2 x hx

def residueMap (d n : ℕ) : IntPoint d →ₗ[ℤ] ResiduePoint d n :=
  LinearMap.pi fun i =>
    (Int.castAddHom (ZMod n)).toIntLinearMap.comp (LinearMap.proj i)

def residueLift {d n : ℕ} (x : ResiduePoint d n) : IntPoint d :=
  fun i => (x i).val

theorem residueMap_lift {d n : ℕ} [NeZero n] (x : ResiduePoint d n) :
    residueMap d n (residueLift x) = x := by
  ext i
  simp [residueMap, residueLift]

def congruenceSpace {d n : ℕ} (S : List (ResiduePoint d n)) (hS : ValidResidues S) :
    Submodule ℤ (IntPoint d) :=
  (residueGroup S hS).toIntSubmodule.comap (residueMap d n)

theorem mem_congruenceSpace {d n : ℕ} (S : List (ResiduePoint d n))
    (hS : ValidResidues S) (x : IntPoint d) :
    x ∈ congruenceSpace S hS ↔ residueMap d n x ∈ S := by
  unfold congruenceSpace
  change residueMap d n x ∈ ((residueGroup S hS).toIntSubmodule :
    Set (ResiduePoint d n)) ↔ _
  rw [AddSubgroup.coe_toIntSubmodule]
  rfl

theorem period_mem_congruenceSpace {d n : ℕ} (S : List (ResiduePoint d n))
    (hS : ValidResidues S) (x : IntPoint d) :
    (n : ℤ) • x ∈ congruenceSpace S hS := by
  have hz : residueMap d n ((n : ℤ) • x) = 0 := by
    ext i
    simp [residueMap]
  change residueMap d n ((n : ℤ) • x) ∈ residueGroup S hS
  rw [hz]
  exact (residueGroup S hS).zero_mem

theorem residueMap_mulVec {d n : ℕ} (B : Matrix (Fin d) (Fin d) ℤ)
    (x : IntPoint d) :
    residueMap d n (B *ᵥ x) =
      B.map (Int.castRingHom (ZMod n)) *ᵥ residueMap d n x := by
  ext i
  simp [residueMap, Matrix.mulVec, dotProduct]

theorem residueMap_zero_iff {d n : ℕ} (x : IntPoint d) :
    residueMap d n x = 0 ↔ ∃ y : IntPoint d, x = (n : ℤ) • y := by
  constructor
  · intro h
    have hdiv : ∀ i, (n : ℤ) ∣ x i := by
      intro i
      apply (ZMod.intCast_zmod_eq_zero_iff_dvd (x i) n).mp
      exact congrFun h i
    choose y hy using hdiv
    exact ⟨y, funext hy⟩
  · rintro ⟨y, rfl⟩
    ext i
    simp [residueMap]

-- Covering the period and every residue proves coverage of the entire lattice.
def CongruenceBasisCert {d n : ℕ} [NeZero n] (S : List (ResiduePoint d n))
    (B T : Matrix (Fin d) (Fin d) ℤ) : Prop :=
  (∀ j, residueMap d n (B.col j) ∈ S) ∧
    B * T = (n : ℤ) • 1 ∧
      ∀ s ∈ S, ∃ c : ResiduePoint d n, B.map (Int.castRingHom (ZMod n)) *ᵥ c = s

instance {d n : ℕ} [NeZero n] (S : List (ResiduePoint d n))
    (B T : Matrix (Fin d) (Fin d) ℤ) : Decidable (CongruenceBasisCert S B T) := by
  unfold CongruenceBasisCert
  infer_instance

theorem congruenceBasisCert_det {d n : ℕ} [NeZero n] (S : List (ResiduePoint d n))
    (B T : Matrix (Fin d) (Fin d) ℤ) (h : CongruenceBasisCert S B T) : B.det ≠ 0 := by
  have hd := congrArg Matrix.det h.2.1
  simp only [Matrix.det_mul, Matrix.det_smul, Matrix.det_one, Fintype.card_fin,
    mul_one] at hd
  intro hz
  rw [hz, zero_mul] at hd
  exact pow_ne_zero d (by exact_mod_cast NeZero.ne n) hd.symm

theorem congruenceBasisCert_correct {d n : ℕ} [NeZero n]
    (S : List (ResiduePoint d n)) (hS : ValidResidues S)
    (B T : Matrix (Fin d) (Fin d) ℤ) (h : CongruenceBasisCert S B T)
    (z : IntPoint d) :
    z ∈ congruenceSpace S hS ↔ ∃ x : IntPoint d, B *ᵥ x = z := by
  constructor
  · intro hz
    obtain ⟨c, hc⟩ := h.2.2 (residueMap d n z) ((mem_congruenceSpace S hS z).mp hz)
    let x := residueLift c
    have hzero : residueMap d n (z - B *ᵥ x) = 0 := by
      rw [map_sub, residueMap_mulVec, residueMap_lift, hc, sub_self]
    obtain ⟨y, hy⟩ := (residueMap_zero_iff _).mp hzero
    refine ⟨x + T *ᵥ y, ?_⟩
    rw [Matrix.mulVec_add, Matrix.mulVec_mulVec y B T, h.2.1,
      Matrix.smul_mulVec, Matrix.one_mulVec, ← hy]
    abel
  · rintro ⟨x, rfl⟩
    have heq : B *ᵥ x = ∑ j, x j • B.col j := by
      ext i
      simp [Matrix.mulVec, dotProduct, Matrix.col, Finset.sum_apply, mul_comm]
    rw [heq]
    exact Submodule.sum_mem _ fun j _ =>
      (congruenceSpace S hS).smul_mem (x j) ((mem_congruenceSpace S hS _).mpr (h.1 j))

theorem congruenceSpace_finrank {d n : ℕ} [NeZero n]
    (S : List (ResiduePoint d n)) (hS : ValidResidues S) :
    Module.finrank ℤ (congruenceSpace S hS) = Module.finrank ℤ (IntPoint d) := by
  let f := (LinearMap.lsmul ℤ (IntPoint d) (n : ℤ)).codRestrict
    (congruenceSpace S hS) (period_mem_congruenceSpace S hS)
  have hinj : Function.Injective f :=
    (LinearMap.lsmul_injective (by exact_mod_cast NeZero.ne n)).codRestrict _
  exact le_antisymm (Submodule.finrank_le _) (f.finrank_le_finrank_of_injective hinj)

theorem exists_congruenceBasisCert {d n : ℕ} [NeZero n]
    (S : List (ResiduePoint d n)) (hS : ValidResidues S) :
    ∃ B T : Matrix (Fin d) (Fin d) ℤ, CongruenceBasisCert S B T := by
  classical
  let M := congruenceSpace S hS
  let b : Basis (Fin d) ℤ M :=
    Submodule.smithNormalFormBotBasis (Pi.basisFun ℤ (Fin d)) (congruenceSpace_finrank S hS)
  let B : Matrix (Fin d) (Fin d) ℤ := fun i j => (b j : IntPoint d) i
  have hB (x : M) : B *ᵥ b.equivFun x = (x : IntPoint d) := by
    ext i
    have h := congrArg (fun y : M => (y : IntPoint d) i) (b.sum_repr x)
    simpa only [B, Matrix.mulVec, dotProduct, Basis.equivFun_apply, Submodule.coe_sum,
      Submodule.coe_smul, Finset.sum_apply, Pi.smul_apply, smul_eq_mul, mul_comm] using h
  let T : Matrix (Fin d) (Fin d) ℤ := fun i j =>
    b.equivFun ⟨(n : ℤ) • Pi.single j 1, period_mem_congruenceSpace S hS _⟩ i
  refine ⟨B, T, ?_, ?_, ?_⟩
  · intro j
    apply (mem_congruenceSpace S hS _).mp
    change (b j : IntPoint d) ∈ M
    exact (b j).property
  · ext i j
    have h := congrFun (hB ⟨(n : ℤ) • Pi.single j 1,
      period_mem_congruenceSpace S hS _⟩) i
    simpa [T, Matrix.mulVec, dotProduct, Matrix.mul_apply, Matrix.one_apply,
      Pi.single_apply, eq_comm] using h
  · intro s hs
    let z : M := ⟨residueLift s, (mem_congruenceSpace S hS _).mpr (by
      rwa [residueMap_lift])⟩
    refine ⟨residueMap d n (b.equivFun z), ?_⟩
    rw [← residueMap_mulVec, hB, residueMap_lift]

def integerMatrices (d R : ℕ) : List (Matrix (Fin d) (Fin d) ℤ) :=
  tuples (integerBox d R) d

theorem mem_integerMatrices {d R : ℕ} (B : Matrix (Fin d) (Fin d) ℤ) :
    B ∈ integerMatrices d R ↔ ∀ i j, |B i j| ≤ (R : ℤ) := by
  constructor
  · intro h i
    exact (mem_integerBox (B i)).mp ((mem_tuples (integerBox d R) d B).mp h i)
  · intro h
    exact (mem_tuples (integerBox d R) d B).mpr fun i =>
      (mem_integerBox (B i)).mpr (h i)

def matrixEntryBound {d : ℕ} (B : Matrix (Fin d) (Fin d) ℤ) : ℕ :=
  ∑ i, ∑ j, (B i j).natAbs

theorem matrixEntryBound_le {d : ℕ} (B : Matrix (Fin d) (Fin d) ℤ) (i j : Fin d) :
    |B i j| ≤ (matrixEntryBound B : ℤ) := by
  have h : (B i j).natAbs ≤ matrixEntryBound B :=
    (Finset.single_le_sum (f := fun j => (B i j).natAbs)
      (fun _ _ => Nat.zero_le _) (Finset.mem_univ j)).trans
      (Finset.single_le_sum (f := fun i => ∑ j, (B i j).natAbs)
        (fun _ _ => Nat.zero_le _) (Finset.mem_univ i))
  rw [← Int.natCast_natAbs]
  exact_mod_cast h

def periodMatrix {d : ℕ} (n : ℕ) (B : Matrix (Fin d) (Fin d) ℤ) :
    Matrix (Fin d) (Fin d) ℤ :=
  fun i j => ((n : ℤ) * B.adjugate i j) / B.det

theorem periodMatrix_eq {d n : ℕ} [NeZero n]
    (S : List (ResiduePoint d n)) (B T : Matrix (Fin d) (Fin d) ℤ)
    (h : CongruenceBasisCert S B T) : periodMatrix n B = T := by
  have hdet := congruenceBasisCert_det S B T h
  have heq := congrArg (fun A => B.adjugate * A) h.2.1
  change B.adjugate * (B * T) = B.adjugate * ((n : ℤ) • 1) at heq
  rw [← Matrix.mul_assoc, Matrix.adjugate_mul, Matrix.smul_mul, Matrix.one_mul,
    Matrix.mul_smul, Matrix.mul_one] at heq
  ext i j
  have hij := congrFun (congrFun heq i) j
  change B.det * T i j = (n : ℤ) * B.adjugate i j at hij
  dsimp only [periodMatrix]
  rw [← hij]
  exact Int.mul_ediv_cancel_left (T i j) hdet

-- The period matrix is unique, so only the lattice basis needs enumeration.
def congruenceCertificatesAt {d n : ℕ} [NeZero n]
    (S : List (ResiduePoint d n)) (R : ℕ) :
    List (Matrix (Fin d) (Fin d) ℤ × Matrix (Fin d) (Fin d) ℤ) :=
  ((integerMatrices d R).map fun B => (B, periodMatrix n B)).filter fun p =>
      decide (CongruenceBasisCert S p.1 p.2)

theorem mem_congruenceCertificatesAt {d n : ℕ} [NeZero n]
    (S : List (ResiduePoint d n)) (R : ℕ)
    (B T : Matrix (Fin d) (Fin d) ℤ) :
    (B, T) ∈ congruenceCertificatesAt S R ↔
      B ∈ integerMatrices d R ∧ periodMatrix n B = T ∧ CongruenceBasisCert S B T := by
  simp [congruenceCertificatesAt, List.mem_map, and_assoc]

theorem exists_nonempty_congruenceCertificatesAt {d n : ℕ} [NeZero n]
    (S : List (ResiduePoint d n)) (hS : ValidResidues S) :
    ∃ R, congruenceCertificatesAt S R ≠ [] := by
  obtain ⟨B, T, h⟩ := exists_congruenceBasisCert S hS
  let R := matrixEntryBound B
  have hB : B ∈ integerMatrices d R := by
    apply (mem_integerMatrices B).mpr
    intro i j
    exact matrixEntryBound_le B i j
  have hmem := (mem_congruenceCertificatesAt S R B T).mpr
    ⟨hB, periodMatrix_eq S B T h, h⟩
  exact ⟨R, List.ne_nil_of_mem hmem⟩

def congruenceBasisPair {d n : ℕ} [NeZero n]
    (S : List (ResiduePoint d n)) (hS : ValidResidues S) :
    Matrix (Fin d) (Fin d) ℤ × Matrix (Fin d) (Fin d) ℤ :=
  (congruenceCertificatesAt S (Nat.find (exists_nonempty_congruenceCertificatesAt S hS))).head
    (Nat.find_spec (exists_nonempty_congruenceCertificatesAt S hS))

theorem congruenceBasisPair_cert {d n : ℕ} [NeZero n]
    (S : List (ResiduePoint d n)) (hS : ValidResidues S) :
    CongruenceBasisCert S (congruenceBasisPair S hS).1 (congruenceBasisPair S hS).2 := by
  have hmem := List.head_mem (Nat.find_spec (exists_nonempty_congruenceCertificatesAt S hS))
  exact ((mem_congruenceCertificatesAt S _ _ _).mp hmem).2.2

def congruenceBasis {d n : ℕ} [NeZero n]
    (S : List (ResiduePoint d n)) (hS : ValidResidues S) :
    Matrix (Fin d) (Fin d) ℤ := (congruenceBasisPair S hS).1

theorem congruenceBasis_correct {d n : ℕ} [NeZero n]
    (S : List (ResiduePoint d n)) (hS : ValidResidues S) :
    (congruenceBasis S hS).det ≠ 0 ∧ ∀ z : IntPoint d,
      z ∈ congruenceSpace S hS ↔ ∃ x : IntPoint d, congruenceBasis S hS *ᵥ x = z := by
  have h := congruenceBasisPair_cert S hS
  exact ⟨congruenceBasisCert_det S _ _ h,
    congruenceBasisCert_correct S hS _ _ h⟩

def residuePoints (d n : ℕ) : List (ResiduePoint d n) :=
  tuples ((List.range n).map fun (i : ℕ) => (i : ZMod n)) d

theorem mem_residuePoints {d n : ℕ} [NeZero n] (x : ResiduePoint d n) :
    x ∈ residuePoints d n := by
  apply (mem_tuples _ _ _).mpr
  intro i
  exact List.mem_map.mpr ⟨(x i).val, List.mem_range.mpr (ZMod.val_lt (x i)), by simp⟩

def residueGroups (d n : ℕ) : List (List (ResiduePoint d n)) :=
  (residuePoints d n).sublists.filter fun S => decide (ValidResidues S)

theorem mem_residueGroups {d n : ℕ} (S : List (ResiduePoint d n)) :
    S ∈ residueGroups d n ↔ S.Sublist (residuePoints d n) ∧ ValidResidues S := by
  simp [residueGroups, List.mem_sublists]

theorem residueGroups_complete {d n : ℕ} [NeZero n]
    (H : AddSubgroup (ResiduePoint d n)) :
    ∃ S ∈ residueGroups d n, ∃ hS : ValidResidues S, residueGroup S hS = H := by
  classical
  let S := (residuePoints d n).filter fun x => decide (x ∈ H)
  have hmem (x : ResiduePoint d n) : x ∈ S ↔ x ∈ H := by
    simp [S, mem_residuePoints]
  have hS : ValidResidues S := by
    refine ⟨(hmem 0).mpr H.zero_mem, ?_, ?_⟩
    · intro x hx y hy
      exact (hmem _).mpr (H.add_mem ((hmem _).mp hx) ((hmem _).mp hy))
    · intro x hx
      exact (hmem _).mpr (H.neg_mem ((hmem _).mp hx))
  refine ⟨S, (mem_residueGroups S).mpr ⟨List.filter_sublist, hS⟩, hS, ?_⟩
  exact AddSubgroup.ext hmem

theorem integer_overgroup_residues {d : ℕ} (A : BoxReduction.ZMat d)
    (hA : A.det ≠ 0) (L : Submodule ℤ (IntPoint d)) (hL : BoxReduction.rowSpan A ≤ L) :
    letI : NeZero A.det.natAbs := ⟨Int.natAbs_ne_zero.mpr hA⟩
    ∃ S ∈ residueGroups d A.det.natAbs, ∃ hS : ValidResidues S,
      congruenceSpace S hS = L := by
  classical
  letI : NeZero A.det.natAbs := ⟨Int.natAbs_ne_zero.mpr hA⟩
  obtain ⟨S, hmem, hS, heq⟩ := residueGroups_complete
    (Submodule.map (residueMap d A.det.natAbs) L).toAddSubgroup
  refine ⟨S, hmem, hS, ?_⟩
  unfold congruenceSpace
  rw [heq, Submodule.toAddSubgroup_toIntSubmodule]
  apply Submodule.comap_map_eq_self
  exact (BoxReduction.ker_modMap_le_rowSpan A).trans hL

end Refpoly.KS
