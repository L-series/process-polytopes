import Refpoly.WeightSystem

open Set Matrix

namespace Refpoly.KS

abbrev IntPoint (d : ℕ) := Fin d → ℤ
abbrev WeightCode (m : ℕ) := Fin m → ℕ

def ofInt {d : ℕ} (x : IntPoint d) : V d := fun i => (x i : ℝ)

def pointSet {d : ℕ} (xs : List (IntPoint d)) : Set (V d) :=
  ofInt '' {x | x ∈ xs}

structure PolyCode (d : ℕ) where
  points : List (IntPoint d)

def PolyCode.hull {d : ℕ} (P : PolyCode d) : Set (V d) :=
  convexHull ℝ (pointSet P.points)

def originCode (d : ℕ) : PolyCode d := ⟨[0]⟩

def normalized {m : ℕ} (w : WeightCode m) : Fin m → ℚ :=
  fun i => (w i : ℚ) / (∑ j, w j : ℕ)

def PrimitiveIP {m : ℕ} (w : WeightCode m) : Prop :=
  Finset.univ.gcd w = 1 ∧ IsIPWeightSystem (fun i => (normalized w i : ℝ))

def pad {m N : ℕ} (e : Fin m ↪ Fin N) (w : WeightCode m) : WeightCode N :=
  fun j => ∑ i ∈ Finset.univ.filter (fun i => e i = j), w i

structure Seed (d : ℕ) where
  N : ℕ
  vertices : Fin N → V d

def ValidSeed {d : ℕ} (P : Set (V d)) (S : Seed d) : Prop :=
  Function.Injective S.vertices ∧
  (∀ i, IsLatticePoint (S.vertices i) ∧ S.vertices i ∈ polarDual P) ∧
  IsIP (convexHull ℝ (range S.vertices)) ∧
  (∀ T : Finset (Fin S.N), T ≠ Finset.univ →
    ¬IsIP (convexHull ℝ (S.vertices '' (T : Set (Fin S.N))))) ∧
  d + 1 ≤ S.N ∧ S.N ≤ 2 * d

structure CWSCode (d : ℕ) where
  N : ℕ
  r : ℕ
  rows : Fin r → WeightCode N

def realRows {d : ℕ} (C : CWSCode d) : Fin C.r → V C.N :=
  fun a j => (C.rows a j : ℝ)

def ValidCWS {d : ℕ} (C : CWSCode d) : Prop :=
  d + C.r = C.N ∧ C.N ≤ 2 * d ∧ C.r ≤ d ∧
  LinearIndependent ℝ (realRows C) ∧ (∀ j, ∃ a, 0 < C.rows a j)

def GeneratedRows {d : ℕ} (weights : (m : ℕ) → List (WeightCode m))
    (C : CWSCode d) : Prop :=
  ∀ a, ∃ m, m ≤ d + 1 ∧ ∃ e : Fin m ↪ Fin C.N,
    ∃ w ∈ weights m, C.rows a = pad e w

structure RelationBasis {d : ℕ} (S : Seed d) where
  r : ℕ
  rows : Fin r → WeightCode S.N

def RelationBasis.code {d : ℕ} {S : Seed d} (B : RelationBasis S) : CWSCode d :=
  ⟨S.N, B.r, B.rows⟩

def GeometricBasis {d : ℕ} {S : Seed d} (B : RelationBasis S) : Prop :=
  (Submodule.span ℝ (range (realRows B.code)) =
    LinearMap.ker (Matrix.mulVecLin (fun i j => S.vertices j i))) ∧
  ∀ a, ∃ m, m ≤ d + 1 ∧ ∃ e : Fin m ↪ Fin S.N, ∃ w : WeightCode m,
    PrimitiveIP w ∧ B.rows a = pad e w ∧
    AffineIndependent ℝ (fun i => S.vertices (e i)) ∧
    (∑ i, (normalized w i : ℝ) • S.vertices (e i)) = 0

def kernelSpace {d : ℕ} (C : CWSCode d) : Submodule ℝ (V C.N) :=
  LinearMap.ker (Matrix.mulVecLin (realRows C))

def basisMap {d : ℕ} {C : CWSCode d} (b : Fin C.N → Fin d → ℤ) :
    V d →ₗ[ℝ] V C.N :=
  Matrix.mulVecLin (fun j i => (b j i : ℝ))

def KernelBasisCorrect {d : ℕ} (C : CWSCode d) (b : Fin C.N → Fin d → ℤ) : Prop :=
  Function.Injective (basisMap b) ∧ LinearMap.range (basisMap b) = kernelSpace C ∧
  ∀ y ∈ kernelSpace C, IsLatticePoint y →
    ∃ x : V d, IsLatticePoint x ∧ basisMap b x = y

def container {d : ℕ} {C : CWSCode d} (b : Fin C.N → Fin d → ℤ) : Set (V d) :=
  convexHull ℝ {x : V d | IsLatticePoint x ∧ ∀ j, -1 ≤ basisMap b x j}

def ForwardIntegral {d : ℕ} (f : V d ≃ₗ[ℝ] V d) : Prop :=
  ∀ x, IsLatticePoint x → IsLatticePoint (f x)

def IntegralVPM {d : ℕ} (P : Set (V d)) : Prop :=
  ∀ v ∈ P.extremePoints ℝ, ∀ u ∈ (polarDual P).extremePoints ℝ,
    ∃ z : ℤ, u ⬝ᵥ v = z

def Eligible {d : ℕ} (Q : PolyCode d) : Prop :=
  IsIP Q.hull ∧ IntegralVPM Q.hull

def integerLattice (d : ℕ) : AddSubgroup (V d) where
  carrier := latticePoints d
  zero_mem' := isLatticePoint_zero
  add_mem' := by
    intro x y hx hy i
    obtain ⟨a, ha⟩ := hx i
    obtain ⟨b, hb⟩ := hy i
    exact ⟨a + b, by simp [ha, hb]⟩
  neg_mem' := by
    intro x hx i
    obtain ⟨a, ha⟩ := hx i
    exact ⟨-a, by simp [ha]⟩

def imageLattice {d : ℕ} (f : V d ≃ₗ[ℝ] V d) : AddSubgroup (V d) :=
  (integerLattice d).map f.toLinearMap.toAddMonoidHom

def coarsest {d : ℕ} (P : Set (V d)) : AddSubgroup (V d) :=
  AddSubgroup.closure (P.extremePoints ℝ)

def finest {d : ℕ} (P : Set (V d)) : AddSubgroup (V d) where
  carrier := {x | ∀ u ∈ (polarDual P).extremePoints ℝ, ∃ z : ℤ, u ⬝ᵥ x = z}
  zero_mem' := by intro u hu; exact ⟨0, by simp⟩
  add_mem' := by
    intro x y hx hy u hu
    obtain ⟨a, ha⟩ := hx u hu
    obtain ⟨b, hb⟩ := hy u hu
    exact ⟨a + b, by simp [dotProduct_add, ha, hb]⟩
  neg_mem' := by
    intro x hx u hu
    obtain ⟨a, ha⟩ := hx u hu
    exact ⟨-a, by simp [dotProduct_neg, ha]⟩

def Compatible {d : ℕ} (Q : PolyCode d) (L : AddSubgroup (V d)) : Prop :=
  coarsest Q.hull ≤ L ∧ L ≤ finest Q.hull

def LatticeInterval {d : ℕ} (Q : PolyCode d) : Set (AddSubgroup (V d)) :=
  {L | Compatible Q L}

def subpolytopes {d : ℕ} (xs : List (IntPoint d)) : List (PolyCode d) :=
  xs.sublists.map PolyCode.mk

structure Procedures where
  weights : (m : ℕ) → List (WeightCode m)
  assemble : (d : ℕ) → ((m : ℕ) → List (WeightCode m)) → List (CWSCode d)
  kernelBasis : {d : ℕ} → (C : CWSCode d) → Fin C.N → Fin d → ℤ
  points : {d : ℕ} → CWSCode d → List (IntPoint d)
  eligible : {d : ℕ} → PolyCode d → Bool
  realize : {d : ℕ} → PolyCode d → List (PolyCode d)
  reflexive : {d : ℕ} → PolyCode d → Bool

end Refpoly.KS
