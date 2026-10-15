import Refpoly.KS.Contracts
import Mathlib.LinearAlgebra.Matrix.Rank
import Mathlib.LinearAlgebra.Matrix.ToLinearEquiv

open Set Matrix
namespace Refpoly.KS

/-- All ordered tuples drawn from a supplied finite list; duplicates are retained. -/
def tuples {α : Type} (xs : List α) : (n : ℕ) → List (Fin n → α)
  | 0 => [Fin.elim0]
  | n + 1 => xs.flatMap fun x => (tuples xs n).map (Fin.cons x)

theorem mem_tuples {α : Type} (xs : List α) (n : ℕ) (f : Fin n → α) :
    f ∈ tuples xs n ↔ ∀ i, f i ∈ xs := by
  induction n with
  | zero => simp [tuples, Subsingleton.elim f Fin.elim0]
  | succ n ih =>
    simp only [tuples, List.mem_flatMap, List.mem_map]
    constructor
    · rintro ⟨x, hx, g, hg, rfl⟩ i
      exact Fin.cases hx (fun j => (ih g).mp hg j) i
    · intro h
      exact ⟨f 0, h 0, fun j => f j.succ, (ih _).mpr (fun j => h j.succ),
        by ext i; exact Fin.cases rfl (fun _ => rfl) i⟩

/-- Enumerate supports by checking injectivity of every finite coordinate map. -/
def embeddings (m N : ℕ) : List (Fin m ↪ Fin N) :=
  (tuples (List.finRange N) m).flatMap fun f =>
    if h : Function.Injective f then [⟨f, h⟩] else []

theorem mem_embeddings {m N : ℕ} (e : Fin m ↪ Fin N) : e ∈ embeddings m N := by
  simp only [embeddings, List.mem_flatMap]
  refine ⟨e, (mem_tuples _ _ _).mpr (fun _ => List.mem_finRange _), ?_⟩
  simp [e.injective]

/-- Every supplied row on every allowed support, with exact zero padding. -/
def assemblyRows (d N : ℕ) (ws : (m : ℕ) → List (WeightCode m)) :
    List (WeightCode N) :=
  (List.range (d + 2)).flatMap fun m =>
    (embeddings m N).flatMap fun e => (ws m).map (pad e)

theorem mem_assemblyRows {d N : ℕ} {ws : (m : ℕ) → List (WeightCode m)}
    {v : WeightCode N} : v ∈ assemblyRows d N ws ↔
    ∃ m, m ≤ d + 1 ∧ ∃ e : Fin m ↪ Fin N, ∃ w ∈ ws m, v = pad e w := by
  simp only [assemblyRows, List.mem_flatMap, List.mem_range, List.mem_map]
  constructor
  · rintro ⟨m, hm, e, _, w, hw, heq⟩
    exact ⟨m, by omega, e, w, hw, heq.symm⟩
  · rintro ⟨m, hm, e, w, hw, rfl⟩
    exact ⟨m, by omega, e, mem_embeddings e, w, hw, rfl⟩

/-- Integer Gram determinant: a finite exact test of real row independence. -/
def rowGram {d : ℕ} (C : CWSCode d) : Matrix (Fin C.r) (Fin C.r) ℤ :=
  fun a b => ∑ j, (C.rows a j : ℤ) * C.rows b j

theorem rowGram_cast {d : ℕ} (C : CWSCode d) :
    (rowGram C).map (Int.castRingHom ℝ) = Matrix.of (realRows C) * (Matrix.of (realRows C))ᵀ := by
  ext a b
  simp [rowGram, realRows, Matrix.mul_apply]

theorem rowGram_det_ne_zero_iff {d : ℕ} (C : CWSCode d) :
    (rowGram C).det ≠ 0 ↔ LinearIndependent ℝ (realRows C) := by
  have hcast : ((rowGram C).det : ℝ) = (Matrix.of (realRows C) * (Matrix.of (realRows C))ᵀ).det := by
    rw [← rowGram_cast C]
    exact (Int.castRingHom ℝ).map_det (rowGram C)
  have hker : LinearMap.ker (Matrix.of (realRows C) * (Matrix.of (realRows C))ᵀ).mulVecLin =
      LinearMap.ker ((Matrix.of (realRows C))ᵀ).mulVecLin := by
    simpa using Matrix.ker_mulVecLin_transpose_mul_self (Matrix.of (realRows C))ᵀ
  have hli : LinearIndependent ℝ (realRows C) ↔
      Function.Injective ((Matrix.of (realRows C))ᵀ).mulVecLin := by
    change LinearIndependent ℝ (Matrix.of (realRows C)).row ↔ _
    rw [← Matrix.vecMul_injective_iff]
    change Function.Injective (fun x => x ᵥ* Matrix.of (realRows C)) ↔
      Function.Injective (fun x => (Matrix.of (realRows C))ᵀ *ᵥ x)
    simp only [Matrix.mulVec_transpose]
  rw [hli, ← LinearMap.ker_eq_bot]
  have hd : (rowGram C).det ≠ 0 ↔ (Matrix.of (realRows C) * (Matrix.of (realRows C))ᵀ).det ≠ 0 := by
    rw [← hcast]
    exact_mod_cast Iff.rfl
  rw [hd, ← hker, LinearMap.ker_eq_bot']
  constructor
  · intro h x hx
    exact Matrix.eq_zero_of_mulVec_eq_zero h hx
  · intro h hz
    obtain ⟨x, hx, hzero⟩ := Matrix.exists_mulVec_eq_zero_iff.mpr hz
    exact hx (h x hzero)

/-- This check includes all validity conditions, using only integer arithmetic. -/
def validCWSCheck {d : ℕ} (C : CWSCode d) : Bool :=
  decide (d + C.r = C.N ∧ C.N ≤ 2 * d ∧ C.r ≤ d ∧
    (rowGram C).det ≠ 0 ∧ ∀ j, ∃ a, 0 < C.rows a j)

theorem validCWSCheck_iff {d : ℕ} (C : CWSCode d) :
    validCWSCheck C = true ↔ ValidCWS C := by
  simp only [validCWSCheck, decide_eq_true_eq, ValidCWS, rowGram_det_ne_zero_iff]

/-- Exhaustive finite CWS assembly; every loop is bounded on arbitrary inputs. -/
def assemble (d : ℕ) (ws : (m : ℕ) → List (WeightCode m)) : List (CWSCode d) :=
  ((List.range (2 * d + 1)).flatMap fun N =>
    (List.range (d + 1)).flatMap fun r =>
      (tuples (assemblyRows d N ws) r).map (fun rows => (⟨N, r, rows⟩ : CWSCode d)))
    |>.filter validCWSCheck

theorem mem_assemble {d : ℕ} {ws : (m : ℕ) → List (WeightCode m)}
    (C : CWSCode d) : C ∈ assemble d ws ↔ ValidCWS C ∧ GeneratedRows ws C := by
  simp only [assemble, List.mem_filter, validCWSCheck_iff, List.mem_flatMap,
    List.mem_range, List.mem_map]
  constructor
  · rintro ⟨⟨N, _, r, _, rows, hrows, hC⟩, hvalid⟩
    cases hC
    refine ⟨hvalid, fun a => ?_⟩
    exact mem_assemblyRows.mp ((mem_tuples _ _ _).mp hrows a)
  · rintro ⟨hv, hg⟩
    refine ⟨⟨C.N, by have := hv.2.1; omega, C.r, by have := hv.2.2.1; omega,
      C.rows, (mem_tuples _ _ _).mpr (fun a => mem_assemblyRows.mpr (hg a)), ?_⟩, hv⟩
    rfl

end Refpoly.KS
