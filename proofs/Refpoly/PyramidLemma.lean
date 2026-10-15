/-
PyramidLemma faithfully ported from the reviewed refpoly-5d snapshot.
See SOURCES.md for attribution and exact source identities.
The low-dimensional theorem concerns the stated normalized family.
The five-dimensional result is a height obstruction, not a reflexivity test.
Historical prose below supplies no general lattice-normalization proof.
-/
import Refpoly.BoxReduction

open Set

namespace Refpoly

/-! ## Vector notation helpers

Evaluating `![a,b,c,d,e] ⬝ᵥ x` against an *arbitrary* vector `x` needs the
`Matrix.cons_val_*` simp lemmas; we package the expansion once for each of
the two dimensions used in this file. -/

/-- Expand a dot product with an explicit 5-vector of coefficients. -/
theorem dot_lit₅ (a b c d e : ℝ) (x : V 5) :
    ![a, b, c, d, e] ⬝ᵥ x
      = a * x 0 + b * x 1 + c * x 2 + d * x 3 + e * x 4 := by
  unfold dotProduct
  rw [Fin.sum_univ_five]
  simp only [Matrix.cons_val_zero, Matrix.cons_val_one, Matrix.cons_val_two,
    Matrix.cons_val_three, Matrix.cons_val_four, Matrix.head_cons,
    Matrix.tail_cons]

/-- Evaluate `![…]`-literals against `![…]`-literals (used for the finitely
many generator checks below). -/
local macro "vec_norm" : tactic =>
  `(tactic| norm_num [Matrix.cons_val_zero, Matrix.cons_val_one,
      Matrix.cons_val_two, Matrix.cons_val_three, Matrix.cons_val_four,
      Matrix.head_cons, Matrix.tail_cons])

/-! ## L6.1: the pyramid lemma in dimension ≤ 4 -/

set_option maxHeartbeats 800000 in
/-- **L6.1 (pyramid lemma, [Sk96] Lemma 1) — fully proved.**

Normalized form (the general case reduces to this by a `GL(4,ℤ)` change of
coordinates and relabeling): let `Pyr_double ⊂ ℝ⁴` be the pyramid with base
`conv{0, 2e₀, 2e₁, 2e₂} × {0}` and lattice apex `a = (2x, 2y, 2z, 2h)` with
`h ≥ 2` (evenness of `a` is what makes the *half* pyramid `Pyr` — the part
above height `h` — an integer pyramid; `Pyr_double` is its double).  Then
`Pyr_double` contains a lattice point at height `t` with `1 ≤ t ≤ h − 1`,
i.e. a lattice point lying neither in `Pyr` (heights `≥ h`) nor in the base
(height `0`).

The proof sets `rₓ = x % h`, and similarly for `y,z`. If the residue sum
is at least `h + 1`, the point with coordinates `x/h + 1`, `y/h + 1`,
`z/h + 1` and height `1` has nonnegative barycentric coefficients summing
to at most one. Otherwise the point with coordinates `x - x/h`, `y - y/h`,
`z - z/h` and height `h - 1` does. Integer division here is floor division,
including for negative coordinates. Both membership witnesses are explicit. -/
theorem pyramid_lemma_dim_le_four (x y z h : ℤ) (hh : 2 ≤ h) :
    ∃ p : V 4, IsLatticePoint p ∧
      p ∈ convexHull ℝ ({0, ![2, 0, 0, 0], ![0, 2, 0, 0], ![0, 0, 2, 0],
        ![(2 * x : ℝ), 2 * y, 2 * z, 2 * h]} : Set (V 4)) ∧
      1 ≤ p 3 ∧ p 3 ≤ (h : ℝ) - 1 := by
  have hpos : 0 < h := by omega
  have hreal : (0 : ℝ) < h := by exact_mod_cast hpos
  have hreal2 : (2 : ℝ) ≤ h := by exact_mod_cast hh
  let rx := x % h
  let ry := y % h
  let rz := z % h
  have hrx : 0 ≤ rx ∧ rx < h := ⟨Int.emod_nonneg _ (by omega), Int.emod_lt_of_pos _ hpos⟩
  have hry : 0 ≤ ry ∧ ry < h := ⟨Int.emod_nonneg _ (by omega), Int.emod_lt_of_pos _ hpos⟩
  have hrz : 0 ≤ rz ∧ rz < h := ⟨Int.emod_nonneg _ (by omega), Int.emod_lt_of_pos _ hpos⟩
  have hex : (rx : ℝ) + (h : ℝ) * (x / h : ℤ) = x := by exact_mod_cast Int.emod_add_mul_ediv x h
  have hey : (ry : ℝ) + (h : ℝ) * (y / h : ℤ) = y := by exact_mod_cast Int.emod_add_mul_ediv y h
  have hez : (rz : ℝ) + (h : ℝ) * (z / h : ℤ) = z := by exact_mod_cast Int.emod_add_mul_ediv z h
  let vertices : Fin 4 → V 4 :=
    ![![2, 0, 0, 0], ![0, 2, 0, 0], ![0, 0, 2, 0], ![(2 * x : ℝ), 2 * y, 2 * z, 2 * h]]
  let P : Set (V 4) := convexHull ℝ ({0, ![2, 0, 0, 0], ![0, 2, 0, 0],
    ![0, 0, 2, 0], ![(2 * x : ℝ), 2 * y, 2 * z, 2 * h]} : Set (V 4))
  have hvertices : ∀ i, vertices i ∈ P := by
    intro i
    apply subset_convexHull ℝ _
    fin_cases i <;> simp [vertices]
  have h0 : (0 : V 4) ∈ P := subset_convexHull ℝ _ (by simp)
  have hcombo (t : Fin 4 → ℝ) (ht : ∀ i, 0 ≤ t i) (hs : ∑ i, t i ≤ 1) :
      (∑ i, t i • vertices i) ∈ P :=
    BoxReduction.convex_sum_mem_zero (convex_convexHull ℝ _) h0 vertices hvertices t ht hs
  have hrx0 : (0 : ℝ) ≤ rx := by exact_mod_cast hrx.1
  have hry0 : (0 : ℝ) ≤ ry := by exact_mod_cast hry.1
  have hrz0 : (0 : ℝ) ≤ rz := by exact_mod_cast hrz.1
  have hrxh : (rx : ℝ) < h := by exact_mod_cast hrx.2
  have hryh : (ry : ℝ) < h := by exact_mod_cast hry.2
  have hrzh : (rz : ℝ) < h := by exact_mod_cast hrz.2
  by_cases hsum : rx + ry + rz ≥ h + 1
  · let p : V 4 := ![((x / h + 1 : ℤ) : ℝ), ((y / h + 1 : ℤ) : ℝ),
      ((z / h + 1 : ℤ) : ℝ), 1]
    let t : Fin 4 → ℝ := ![((h : ℝ) - rx) / (2 * h),
      ((h : ℝ) - ry) / (2 * h), ((h : ℝ) - rz) / (2 * h), 1 / (2 * h)]
    have ht : ∀ i, 0 ≤ t i := by
      intro i
      fin_cases i <;> dsimp [t] <;> apply div_nonneg <;> linarith
    have hs : ∑ i, t i ≤ 1 := by
      have hs' : (h : ℝ) + 1 ≤ rx + ry + rz := by exact_mod_cast hsum
      simp only [Fin.sum_univ_four, t, Matrix.cons_val_zero, Matrix.cons_val_one,
        Matrix.cons_val_two, Matrix.cons_val_three, Matrix.head_cons, Matrix.tail_cons]
      rw [← add_div, ← add_div, ← add_div]
      apply (div_le_one (by positivity : (0 : ℝ) < 2 * h)).mpr
      linarith
    refine ⟨p, ?_, ?_, ?_, ?_⟩
    · intro i
      fin_cases i
      · exact ⟨x / h + 1, rfl⟩
      · exact ⟨y / h + 1, rfl⟩
      · exact ⟨z / h + 1, rfl⟩
      · exact ⟨1, by norm_num [p]⟩
    · change p ∈ P
      have heq : (∑ i, t i • vertices i) = p := by
        ext i
        fin_cases i <;> simp [Fin.sum_univ_four, t, vertices, p]
        all_goals field_simp [ne_of_gt hreal]
        all_goals nlinarith [hex, hey, hez]
      rw [← heq]
      exact hcombo t ht hs
    · simp [p]
    · simp [p]; linarith
  · let p : V 4 := ![((x - x / h : ℤ) : ℝ), ((y - y / h : ℤ) : ℝ),
      ((z - z / h : ℤ) : ℝ), ((h - 1 : ℤ) : ℝ)]
    let t : Fin 4 → ℝ := ![(rx : ℝ) / (2 * h), (ry : ℝ) / (2 * h),
      (rz : ℝ) / (2 * h), ((h : ℝ) - 1) / (2 * h)]
    have ht : ∀ i, 0 ≤ t i := by
      intro i
      fin_cases i <;> dsimp [t] <;> apply div_nonneg <;> linarith
    have hs : ∑ i, t i ≤ 1 := by
      have hs' : (rx : ℝ) + ry + rz ≤ h := by exact_mod_cast (show rx + ry + rz ≤ h by omega)
      simp only [Fin.sum_univ_four, t, Matrix.cons_val_zero, Matrix.cons_val_one,
        Matrix.cons_val_two, Matrix.cons_val_three, Matrix.head_cons, Matrix.tail_cons]
      rw [← add_div, ← add_div, ← add_div]
      apply (div_le_one (by positivity : (0 : ℝ) < 2 * h)).mpr
      linarith
    refine ⟨p, ?_, ?_, ?_, ?_⟩
    · intro i
      fin_cases i
      · exact ⟨x - x / h, rfl⟩
      · exact ⟨y - y / h, rfl⟩
      · exact ⟨z - z / h, rfl⟩
      · exact ⟨h - 1, rfl⟩
    · change p ∈ P
      have heq : (∑ i, t i • vertices i) = p := by
        ext i
        fin_cases i <;> simp [Fin.sum_univ_four, t, vertices, p]
        all_goals field_simp [ne_of_gt hreal]
        all_goals nlinarith [hex, hey, hez]
      rw [← heq]
      exact hcombo t ht hs
    · simp [p]; linarith
    · simp [p]

/-! ## R6.3: failure of the pyramid lemma in dimension 5 -/

/-- The five-dimensional pyramid witnessing the failure: base
`conv{0, 2e₀, 2e₁, 2e₂, 2e₃} × {0}`, apex `(2,2,2,2,4)` — the `n = 5`,
`h = 2` instance of the pyramid-lemma setup. -/
def pyramid5 : Set (V 5) :=
  {0, ![2, 0, 0, 0, 0], ![0, 2, 0, 0, 0], ![0, 0, 2, 0, 0],
    ![0, 0, 0, 2, 0], ![2, 2, 2, 2, 4]}

/-- Check a linear inequality on each of the six generators of `pyramid5`. -/
local macro "pyramid_cert" : tactic =>
  `(tactic| (intro v hv
             simp only [pyramid5, Set.mem_insert_iff, Set.mem_singleton_iff] at hv
             rcases hv with h | h | h | h | h | h <;> subst h <;> (rw [dot_lit₅]; vec_norm)))

/-- **R6.3 (counterexample in dimension 5, [Sk96]) — fully proved.**

`pyramid5` contains **no** lattice point at height `1` — precisely the height
range `[1, h−1] = {1}` where L6.1 would guarantee one for `n ≤ 4`.  Hence the
pyramid lemma, and with it the automatic reflexivity of `Δ_q` (T6.2), fails
in dimension 5; reflexivity must be checked per weight system.

*Proof by separation certificates.*  Suppose `m` is a lattice point of the
pyramid with `m 4 = 1`.
1.  For each `i < 4` the functional `ψᵢ = (…, ½ at slot i, …, −¼ at slot 4)`
    is `≥ 0` on all six vertices (values `0, 1, 0, 0, 0, 0`), hence `≥ 0`
    on the hull; evaluating at `m` gives `m i ≥ m 4 / 2 = ½`, and
    integrality bumps this to `m i ≥ 1`.
2.  The functional `φ = (½, ½, ½, ½, −¾)` is `≤ 1` on all six vertices
    (values `0, 1, 1, 1, 1, 1`), hence `≤ 1` on the hull; but step 1 gives
    `φ(m) ≥ 4·½ − ¾ = 5/4 > 1`.  Contradiction. -/
theorem pyramid_counterexample_dim_five {m : V 5}
    (hm : IsLatticePoint m) (hmem : m ∈ convexHull ℝ pyramid5) :
    m 4 ≠ 1 := by
  intro h4
  -- any functional that is ≥ 0 on the six vertices is ≥ 0 at m
  have key : ∀ w : V 5, (∀ v ∈ pyramid5, 0 ≤ w ⬝ᵥ v) → 0 ≤ w ⬝ᵥ m :=
    fun w hw => le_dotProduct_of_mem_convexHull hw hmem
  -- the four side certificates ψ₀, …, ψ₃
  have hpsi0 := key ![1/2, 0, 0, 0, -(1/4)] (by pyramid_cert)
  have hpsi1 := key ![0, 1/2, 0, 0, -(1/4)] (by pyramid_cert)
  have hpsi2 := key ![0, 0, 1/2, 0, -(1/4)] (by pyramid_cert)
  have hpsi3 := key ![0, 0, 0, 1/2, -(1/4)] (by pyramid_cert)
  rw [dot_lit₅, h4] at hpsi0 hpsi1 hpsi2 hpsi3
  -- integrality: a coordinate that is ≥ ½ is ≥ 1
  have hint : ∀ i : Fin 5, (1 : ℝ) / 2 ≤ m i → 1 ≤ m i := by
    intro i hi
    obtain ⟨z, hz⟩ := hm i
    rw [hz] at hi ⊢
    have hz1 : (1 : ℤ) ≤ z := by
      have : (0 : ℝ) < (z : ℝ) := by linarith
      have : (0 : ℤ) < z := by exact_mod_cast this
      omega
    exact_mod_cast hz1
  have hm0 : (1 : ℝ) ≤ m 0 := hint 0 (by linarith)
  have hm1 : (1 : ℝ) ≤ m 1 := hint 1 (by linarith)
  have hm2 : (1 : ℝ) ≤ m 2 := hint 2 (by linarith)
  have hm3 : (1 : ℝ) ≤ m 3 := hint 3 (by linarith)
  -- the cap certificate φ
  have hcap : (![1/2, 1/2, 1/2, 1/2, -(3/4)] : V 5) ⬝ᵥ m ≤ 1 := by
    apply dotProduct_le_of_mem_convexHull ?_ hmem
    pyramid_cert
  rw [dot_lit₅, h4] at hcap
  linarith

end Refpoly
