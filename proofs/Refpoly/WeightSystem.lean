/-
Weight-system definitions extracted from the reviewed refpoly-5d snapshot.
Interior is taken in the specified kernel subspace, not the hull's affine span.
See SOURCES.md; the original weight-system theorems remain to be ported.
-/
import Refpoly.Basic

open Set Matrix

namespace Refpoly

variable {n : ℕ}

def dotLin (q : V n) : V n →ₗ[ℝ] ℝ where
  toFun y := q ⬝ᵥ y
  map_add' x y := by simp [dotProduct_add]
  map_smul' c x := by simp [dotProduct_smul]

def wsHyperplane (q : V n) : Submodule ℝ (V n) := LinearMap.ker (dotLin q)

def wsLatticeGensAt (q : V n) (c : ℝ) : Set (wsHyperplane q) :=
  {y | IsLatticePoint (y : V n) ∧ ∀ i, -c ≤ (y : V n) i}

def wsPolytopeAt (q : V n) (c : ℝ) : Set (wsHyperplane q) :=
  convexHull ℝ (wsLatticeGensAt q c)

def IsIPWeightSystemAt (q : V n) (c : ℝ) : Prop :=
  (∀ i, 0 < q i) ∧ (0 : wsHyperplane q) ∈ interior (wsPolytopeAt q c)

def IsIPWeightSystem (q : V n) : Prop := IsIPWeightSystemAt q 1

end Refpoly
