/-
CWSAssembly faithfully ported from the reviewed refpoly-5d snapshot.
See SOURCES.md for attribution and exact source identities.
Catalogues are noncomputable finite mathematical objects of real weight rows.
They do not implement primitive integer search or lattice realizations.
Fixed-dimensional coverage uses an existential point bound, not the 2d bound;
the selector binder in catalogue coverage is unused.
-/
import Refpoly.CWSContainer
import Refpoly.WeightSystemFinite

/-! A finite exhaustive specification of CWS assembly from IP weight rows.
This is a mathematical catalogue, not a certification of native pruning code. -/
open Set Finset Module Matrix
namespace Refpoly

/-- Embed a WS into a chosen coordinate support, padding the other entries
with zero. -/
noncomputable def padWeight {m N : ℕ} (e : Fin m ↪ Fin N) (q : V m) : V N :=
  Function.extend e q 0

@[simp] theorem padWeight_on_support {m N : ℕ} (e : Fin m ↪ Fin N) (q : V m) (j : Fin m) :
    padWeight e q (e j) = q j :=
  e.injective.extend_apply q 0 j

theorem padWeight_off_support {m N : ℕ} (e : Fin m ↪ Fin N) (q : V m) (i : Fin N)
    (hi : ∀ j, e j ≠ i) : padWeight e q i = 0 :=
  Function.extend_apply' _ _ _ fun ⟨j, hj⟩ => hi j hj

/-- All zero-padded normalized IP weights with bounded block length. -/
def candidateRows (maxLength N : ℕ) : Set (V N) :=
  ⋃ m ∈ Set.Iic maxLength, ⋃ e : Fin m ↪ Fin N,
    padWeight e '' {q : V m | ∑ i, q i = 1 ∧ IsIPWeightSystem q}

/-- The row catalogue is finite without a numerical degree cutoff. -/
theorem candidateRows_finite (maxLength N : ℕ) : (candidateRows maxLength N).Finite := by
  apply (Set.finite_Iic maxLength).biUnion
  intro m _
  apply Set.finite_iUnion
  intro e
  exact (WeightSystemFinite.normalized_ip_weight_systems_finite m).image (padWeight e)

/-- Explicitly reached, zero-padded rows belong to the exhaustive catalogue. -/
theorem CWS.RowsReached.row_mem {k N maxLength : ℕ} {C : CWS k N}
    {sel : ∀ m : ℕ, SelectionRule m} (h : C.RowsReached maxLength sel) :
    ∀ a, C.W a ∈ candidateRows maxLength N := by
  classical
  intro a
  obtain ⟨m,hm,e,A,_,_,hIP,_,hsum,hsupp,hoff⟩ := h a
  have heq : C.W a = padWeight e ((sel m).sel A) := by
    funext i
    by_cases hi : ∃ j, e j = i
    · obtain ⟨j,rfl⟩ := hi
      rw [hsupp j, padWeight_on_support]
    · have hi' : ∀ j, e j ≠ i := by simpa only [not_exists] using hi
      rw [hoff i hi', padWeight_off_support e _ i hi']
  rw [candidateRows]
  exact Set.mem_iUnion₂.mpr ⟨m,hm,Set.mem_iUnion.mpr
    ⟨e,⟨(sel m).sel A,⟨hsum,hIP⟩,heq.symm⟩⟩⟩

/-- CWS candidates retain precisely the independent, nonnegative, covering
matrix conditions encoded in CWS, and take all rows from the finite catalogue. -/
def candidateCWS (k N maxLength : ℕ) : Set (CWS k N) :=
  {C | ∀ a, C.W a ∈ candidateRows maxLength N}

theorem candidateCWS_finite (k N maxLength : ℕ) : (candidateCWS k N maxLength).Finite := by
  have hfinite : ((fun C : CWS k N => C.W) '' candidateCWS k N maxLength).Finite := by
    apply (Set.Finite.pi' (fun _ : Fin k => candidateRows_finite maxLength N)).subset
    rintro _ ⟨C,hC,rfl⟩
    exact hC
  apply hfinite.of_finite_image
  intro C _ D _ h
  cases C
  cases D
  cases h
  rfl

/-- An exhaustive finite CWS catalogue, with no empirical degree cutoff. -/
noncomputable def cwsCatalogue (k N maxLength : ℕ) : Finset (CWS k N) :=
  (candidateCWS_finite k N maxLength).toFinset

theorem CWS.RowsReached.mem_catalogue {k N maxLength : ℕ} {C : CWS k N}
    {sel : ∀ m : ℕ, SelectionRule m} (h : C.RowsReached maxLength sel) :
    C ∈ cwsCatalogue k N maxLength := by
  exact (candidateCWS_finite k N maxLength).mem_toFinset.mpr h.row_mem

/-- Every reflexive polytope is contained, via a bijective lattice-preserving
linear map, in a container from a finite exhaustive WS+CWS catalogue.
Non-simplicial dimension-five containers already occur using block length ≤5. -/
theorem ws_cws_catalogue_coverage {n : ℕ} {P : Set (V n)} (hP : IsReflexive P)
    (sel : ∀ m : ℕ, SelectionRule m) :
    ∃ S : Finset (V n), IsMinimalIPGen S ∧
      ∃ B : CircuitBasis S, ∃ h0 : (0 : V n) ∈ intrinsicInterior ℝ (convexHull ℝ (S : Set (V n))),
        S.card = n + B.k ∧ (B.toCWS h0).IsIP ∧
        (B.toCWS h0) ∈ cwsCatalogue B.k S.card (n+1) ∧
        (¬ AffineIndependent ℝ ((↑) : S → V n) →
          (B.toCWS h0) ∈ cwsCatalogue B.k S.card n) ∧
        ∃ F : V n →ₗ[ℝ] (B.toCWS h0).subspace,
          Function.Bijective F ∧
          (∀ x, IsLatticePoint x → IsLatticePoint (F x : V S.card)) ∧
          F '' P ⊆ (B.toCWS h0).polytope := by
  obtain ⟨S,hS,B,h0,hIP,hrows,hsmall,hF⟩ := ws_cws_container_coverage hP sel
  exact ⟨S,hS,B,h0,B.coordinate_count hS.1,hIP,hrows.mem_catalogue,
    fun hn => (hsmall hn).mem_catalogue,hF⟩

end Refpoly

namespace Refpoly
open Set Finset Module Matrix

/-- A CWS together with its row and coordinate counts. -/
abbrev CWSCode := Σ k : ℕ, Σ N : ℕ, CWS k N

/-- A finite catalogue in fixed geometric dimension, using a uniform bound
on the number of dual lattice generators. -/
noncomputable def dimensionCWSCatalogue (n bound : ℕ) : Finset CWSCode := by
  classical
  exact (Finset.range (bound+1)).biUnion (fun k =>
    (Finset.range (bound+1)).biUnion (fun N =>
      (cwsCatalogue k N (n+1)).image (fun C => ⟨k,N,C⟩)))

/-- A single finite CWS catalogue supplies lattice-preserving containers
for all reflexive polytopes of a fixed dimension. No empirical degree bound,
simplicial hypothesis, or optimized structure catalogue is assumed. -/
theorem finite_ws_cws_container_coverage (n : ℕ) (sel : ∀ m : ℕ, SelectionRule m) :
    ∃ bound : ℕ, ∀ P : Set (V n), IsReflexive P →
      ∃ code : CWSCode, code ∈ dimensionCWSCatalogue n bound ∧ code.2.2.IsIP ∧
        ∃ F : V n →ₗ[ℝ] code.2.2.subspace, Function.Bijective F ∧
          (∀ x, IsLatticePoint x → IsLatticePoint (F x : V code.2.1)) ∧
          F '' P ⊆ code.2.2.polytope := by
  classical
  obtain ⟨bound,hbound⟩ := latticePoint_count_bound n
  refine ⟨bound,fun P hP => ?_⟩
  obtain ⟨S,hSlat,hSdual,hS,B,h0,hIP,hF,hr⟩ := algorithm_enumerates_cws_container hP sel
  have hdual := hP.polarDual_reflexive
  have hSsub : (S : Set (V n)) ⊆ polarDual P ∩ latticePoints n := by
    intro x hx; exact ⟨hSdual hx,hSlat x hx⟩
  have hN : S.card ≤ bound := by
    have h := Set.ncard_le_ncard hSsub hdual.latticePolytope.finite_latticePoints
    rw [Set.ncard_coe_finset] at h
    exact h.trans (hbound _ hdual)
  have hk : B.k ≤ bound := by
    have hc := B.coordinate_count hS.1
    omega
  have hrows := (B.rows_reached h0 sel hr).1
  let code : CWSCode := ⟨B.k,S.card,B.toCWS h0⟩
  refine ⟨code,?_,hIP,hF⟩
  apply Finset.mem_biUnion.mpr
  refine ⟨B.k,Finset.mem_range.mpr (by omega),?_⟩
  apply Finset.mem_biUnion.mpr
  refine ⟨S.card,Finset.mem_range.mpr (by omega),?_⟩
  exact Finset.mem_image.mpr ⟨B.toCWS h0,hrows.mem_catalogue,rfl⟩

end Refpoly
