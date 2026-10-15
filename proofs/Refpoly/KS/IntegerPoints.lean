import Refpoly.KS.Assembly
import Refpoly.KS.IntegerLinear

open Set Matrix

namespace Refpoly.KS

def integerBox (d R : ℕ) : List (IntPoint d) :=
  tuples ((List.range (2 * R + 1)).map fun (n : ℕ) => (n : ℤ) - R) d

theorem mem_integerBox {d R : ℕ} (x : IntPoint d) :
    x ∈ integerBox d R ↔ ∀ i, |x i| ≤ (R : ℤ) := by
  rw [integerBox, mem_tuples]
  constructor
  · intro h i
    obtain ⟨n, hn, heq⟩ := List.mem_map.mp (h i)
    rw [List.mem_range] at hn
    rw [← heq, abs_le]
    omega
  · intro h i
    refine List.mem_map.mpr ⟨(x i + R).toNat, List.mem_range.mpr ?_, ?_⟩
    · have hi := abs_le.mp (h i)
      omega
    · have hi := abs_le.mp (h i)
      rw [Int.toNat_of_nonneg (by omega)]
      omega

def integerBasisMap {d N : ℕ} (b : Matrix (Fin N) (Fin d) ℤ) (x : IntPoint d) : IntPoint N :=
  Matrix.mulVec b x

def basisGram {d N : ℕ} (b : Matrix (Fin N) (Fin d) ℤ) : Matrix (Fin d) (Fin d) ℤ :=
  bᵀ * b

def basisAdjugate {d N : ℕ} (b : Matrix (Fin N) (Fin d) ℤ) : Matrix (Fin d) (Fin N) ℤ :=
  (basisGram b).adjugate * bᵀ

def kernelPointBound {d : ℕ} (C : CWSCode d) (b : Matrix (Fin C.N) (Fin d) ℤ) : ℕ :=
  (1 + ∑ a, ∑ j, C.rows a j) * ∑ i, ∑ j, (basisAdjugate b i j).natAbs

def pointsForBasis {d : ℕ} (C : CWSCode d) (b : Matrix (Fin C.N) (Fin d) ℤ) :
    List (IntPoint d) :=
  (integerBox d (kernelPointBound C b)).filter fun x =>
    decide (∀ j, -1 ≤ integerBasisMap b x j)

theorem integerBasisMap_cast {d : ℕ} {C : CWSCode d}
    (b : Matrix (Fin C.N) (Fin d) ℤ) (x : IntPoint d) :
    ofInt (integerBasisMap b x) = basisMap b (ofInt x) := by
  ext j
  simp [ofInt, integerBasisMap, basisMap, Matrix.mulVec,
    dotProduct]

theorem basisGram_det_ne_zero {d : ℕ} {C : CWSCode d}
    (b : Matrix (Fin C.N) (Fin d) ℤ) (hb : Function.Injective (basisMap b)) :
    (basisGram b).det ≠ 0 := by
  exact (integer_gram_det_ne_zero_iff b).mpr hb

theorem basisAdjugate_mul {d N : ℕ} (b : Matrix (Fin N) (Fin d) ℤ) :
    basisAdjugate b * b = (basisGram b).det • (1 : Matrix (Fin d) (Fin d) ℤ) := by
  rw [basisAdjugate, Matrix.mul_assoc]
  exact Matrix.adjugate_mul (basisGram b)

theorem kernel_coordinates_bounded {d : ℕ} (C : CWSCode d) (hC : ValidCWS C)
    (y : IntPoint C.N) (hy : ∀ j, -1 ≤ y j)
    (hk : ∀ a, ∑ j, (C.rows a j : ℤ) * y j = 0) :
    ∀ j, |y j| ≤ ((1 + ∑ a, ∑ k, C.rows a k : ℕ) : ℤ) := by
  intro j
  obtain ⟨a, ha⟩ := hC.2.2.2.2 j
  have hnonneg : ∀ k, 0 ≤ (C.rows a k : ℤ) * (y k + 1) :=
    fun k => mul_nonneg (Int.natCast_nonneg _) (by have := hy k; omega)
  have hsum : ∑ k, (C.rows a k : ℤ) * (y k + 1) = ∑ k, (C.rows a k : ℤ) := by
    simp only [mul_add, mul_one, Finset.sum_add_distrib, hk, zero_add]
  have hterm := Finset.single_le_sum (fun k _ => hnonneg k) (Finset.mem_univ j)
  rw [hsum] at hterm
  have hw : (1 : ℤ) ≤ C.rows a j := by exact_mod_cast ha
  have hprod : y j + 1 ≤ (C.rows a j : ℤ) * (y j + 1) := by
    nlinarith [hy j]
  have htotal : ∑ k, (C.rows a k : ℤ) ≤ ∑ a, ∑ k, (C.rows a k : ℤ) :=
    Finset.single_le_sum (fun a _ => Finset.sum_nonneg fun _ _ => Int.natCast_nonneg _)
      (Finset.mem_univ a)
  rw [abs_le]
  push_cast
  constructor
  · have hnn : (0 : ℤ) ≤ ∑ a, ∑ k, (C.rows a k : ℤ) :=
      Finset.sum_nonneg fun _ _ => Finset.sum_nonneg fun _ _ => Int.natCast_nonneg _
    have := hy j
    omega
  · omega

theorem lattice_generators_bounded {d : ℕ} (C : CWSCode d) (hC : ValidCWS C)
    (b : Matrix (Fin C.N) (Fin d) ℤ) (hb : KernelBasisCorrect C b)
    (x : IntPoint d) (hx : ∀ j, -1 ≤ integerBasisMap b x j) :
    ∀ i, |x i| ≤ (kernelPointBound C b : ℤ) := by
  let y := integerBasisMap b x
  have hkernel : basisMap b (ofInt x) ∈ kernelSpace C := by
    rw [← hb.2.1]
    exact LinearMap.mem_range_self _ _
  have hk : ∀ a, ∑ j, (C.rows a j : ℤ) * y j = 0 := by
    intro a
    have h := congrFun (LinearMap.mem_ker.mp hkernel) a
    rw [← integerBasisMap_cast] at h
    change ∑ j, (C.rows a j : ℝ) * (y j : ℝ) = 0 at h
    exact_mod_cast h
  have hy := kernel_coordinates_bounded C hC y hx hk
  have hdet := basisGram_det_ne_zero b hb.1
  have hd : (1 : ℤ) ≤ |(basisGram b).det| := by
    have h := abs_pos.mpr hdet
    omega
  intro i
  have hcoord : ∑ j, basisAdjugate b i j * y j = (basisGram b).det * x i := by
    change (basisAdjugate b *ᵥ (b *ᵥ x)) i = (basisGram b).det * x i
    rw [Matrix.mulVec_mulVec, basisAdjugate_mul]
    simp
  have hbound : |∑ j, basisAdjugate b i j * y j| ≤
      ((1 + ∑ a, ∑ k, C.rows a k : ℕ) : ℤ) *
        ∑ j, |basisAdjugate b i j| := by
    calc
      _ ≤ ∑ j, |basisAdjugate b i j * y j| := Finset.abs_sum_le_sum_abs _ _
      _ ≤ ∑ j, ((1 + ∑ a, ∑ k, C.rows a k : ℕ) : ℤ) *
          |basisAdjugate b i j| := by
        apply Finset.sum_le_sum
        intro j _
        rw [abs_mul]
        exact (mul_le_mul_of_nonneg_left (hy j) (abs_nonneg _)).trans_eq (mul_comm _ _)
      _ = _ := (Finset.mul_sum _ _ _).symm
  have hsum : ∑ j, |basisAdjugate b i j| ≤ ∑ i, ∑ j, |basisAdjugate b i j| :=
    Finset.single_le_sum (f := fun k : Fin d => ∑ j, |basisAdjugate b k j|)
      (fun k _ => Finset.sum_nonneg fun _ _ => abs_nonneg _)
      (Finset.mem_univ i)
  have hR : (0 : ℤ) ≤ ((1 + ∑ a, ∑ k, C.rows a k : ℕ) : ℤ) :=
    Int.natCast_nonneg _
  have htotal := hbound.trans (mul_le_mul_of_nonneg_left hsum hR)
  rw [hcoord, abs_mul] at htotal
  have hxi : |x i| ≤ |(basisGram b).det| * |x i| := by
    nlinarith [abs_nonneg (x i)]
  apply (hxi.trans htotal).trans_eq
  simp [kernelPointBound]

theorem pointsForBasis_exact {d : ℕ} (C : CWSCode d) (hC : ValidCWS C)
    (b : Matrix (Fin C.N) (Fin d) ℤ) (hb : KernelBasisCorrect C b) :
    pointSet (pointsForBasis C b) = container b ∩ latticePoints d := by
  classical
  let G : Set (V d) := {x | IsLatticePoint x ∧ ∀ j, -1 ≤ basisMap b x j}
  have hset : pointSet (pointsForBasis C b) = G := by
    ext z
    constructor
    · rintro ⟨x, hx, rfl⟩
      obtain ⟨_, hineq⟩ := List.mem_filter.mp hx
      refine ⟨fun i => ⟨x i, rfl⟩, ?_⟩
      intro j
      have h := of_decide_eq_true hineq j
      have hcast := congrFun (integerBasisMap_cast b x) j
      rw [← hcast]
      change (-1 : ℝ) ≤ (integerBasisMap b x j : ℝ)
      exact_mod_cast h
    · rintro ⟨hz, hineq⟩
      choose x hx using hz
      have heq : ofInt x = z := funext fun i => (hx i).symm
      have hxi : ∀ j, -1 ≤ integerBasisMap b x j := by
        intro j
        have h := hineq j
        rw [← heq, ← integerBasisMap_cast] at h
        change (-1 : ℝ) ≤ (integerBasisMap b x j : ℝ) at h
        exact_mod_cast h
      refine ⟨x, List.mem_filter.mpr ⟨?_, decide_eq_true hxi⟩, heq⟩
      exact (mem_integerBox x).mpr (lattice_generators_bounded C hC b hb x hxi)
  rw [hset]
  apply Subset.antisymm
  · intro x hx
    exact ⟨subset_convexHull ℝ G hx, hx.1⟩
  · rintro x ⟨hx, hlat⟩
    refine ⟨hlat, fun j => ?_⟩
    have hsub : convexHull ℝ G ⊆ {x | -1 ≤ basisMap b x j} := by
      apply convexHull_min
      · intro y hy
        exact hy.2 j
      · exact convex_halfSpace_ge
          ((LinearMap.proj j).comp (basisMap b)).isLinear (-1)
    exact hsub hx

end Refpoly.KS
