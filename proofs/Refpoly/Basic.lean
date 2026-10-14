/-
Foundational definitions and equivalence lemmas extracted from the reviewed
refpoly-5d snapshot. See SOURCES.md for attribution and exact source identities.
The remainder of the original Basic module is not ported in this scaffold.
-/
import Mathlib

open Set Matrix

namespace Refpoly

abbrev V (n : ℕ) := Fin n → ℝ

variable {n : ℕ}

def IsLatticePoint (x : V n) : Prop := ∀ i, ∃ m : ℤ, x i = m

def latticePoints (n : ℕ) : Set (V n) := {x | IsLatticePoint x}

theorem isLatticePoint_zero : IsLatticePoint (0 : V n) := fun _ => ⟨0, by simp⟩

def IsLatticePolytope (P : Set (V n)) : Prop :=
  ∃ S : Finset (V n), (∀ x ∈ S, IsLatticePoint x) ∧ P = convexHull ℝ (S : Set (V n))

def IsIP (P : Set (V n)) : Prop := (0 : V n) ∈ interior P

def polarDual (P : Set (V n)) : Set (V n) := {y | ∀ x ∈ P, -1 ≤ y ⬝ᵥ x}

structure IsReflexive (P : Set (V n)) : Prop where
  latticePolytope : IsLatticePolytope P
  ip : IsIP P
  dual_latticePolytope : IsLatticePolytope (polarDual P)

def latticeHull (P : Set (V n)) : Set (V n) :=
  convexHull ℝ (P ∩ latticePoints n)

structure IsUnimodular (g : V n ≃ₗ[ℝ] V n) : Prop where
  fwd : ∀ x, IsLatticePoint x → IsLatticePoint (g x)
  bwd : ∀ x, IsLatticePoint x → IsLatticePoint (g.symm x)

def GLEquiv (P Q : Set (V n)) : Prop :=
  ∃ g : V n ≃ₗ[ℝ] V n, IsUnimodular g ∧ g '' P = Q

theorem GLEquiv.refl (P : Set (V n)) : GLEquiv P P :=
  ⟨LinearEquiv.refl ℝ (V n), ⟨fun _ h => h, fun _ h => h⟩, image_id P⟩

theorem GLEquiv.symm {P Q : Set (V n)} (h : GLEquiv P Q) : GLEquiv Q P := by
  obtain ⟨g, hg, rfl⟩ := h
  refine ⟨g.symm, ⟨hg.bwd, by simpa using hg.fwd⟩, ?_⟩
  rw [← image_comp]
  simp

theorem GLEquiv.trans {P Q R : Set (V n)} (h₁ : GLEquiv P Q) (h₂ : GLEquiv Q R) :
    GLEquiv P R := by
  obtain ⟨g₁, hg₁, rfl⟩ := h₁
  obtain ⟨g₂, hg₂, rfl⟩ := h₂
  refine ⟨g₁.trans g₂, ⟨fun x hx => hg₂.fwd _ (hg₁.fwd x hx),
    fun x hx => hg₁.bwd _ (hg₂.bwd x hx)⟩, ?_⟩
  rw [← image_comp]
  rfl

end Refpoly
