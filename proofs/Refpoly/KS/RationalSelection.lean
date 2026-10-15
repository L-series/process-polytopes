import Refpoly.KS.RationalSimplex
import Refpoly.KS.Assembly

open Set Matrix

namespace Refpoly.KS.RationalSelection

def Feasible {d m : ℕ} (v : Fin m → V d) (q : V m) : Prop :=
  (∀ i, 0 ≤ q i) ∧ (∑ i, q i) = 1 ∧ ∑ i, q i • v i = 0

noncomputable def support {m : ℕ} (q : V m) : Finset (Fin m) :=
  Finset.univ.filter fun i => 0 < q i

@[simp] theorem mem_support {m : ℕ} (q : V m) (i : Fin m) :
    i ∈ support q ↔ 0 < q i := by
  simp [support]

theorem sum_on_support {m : ℕ} {q : V m} (hq : ∀ i, 0 ≤ q i) :
    ∑ i ∈ support q, q i = ∑ i, q i := by
  apply Finset.sum_subset (Finset.subset_univ _)
  intro i _ hi
  exact le_antisymm (le_of_not_gt (by simpa using hi)) (hq i)

theorem relation_on_support {d m : ℕ} {q : V m} (hq : ∀ i, 0 ≤ q i)
    (v : Fin m → V d) :
    ∑ i ∈ support q, q i • v i = ∑ i, q i • v i := by
  apply Finset.sum_subset (Finset.subset_univ _)
  intro i _ hi
  have hz : q i = 0 := le_antisymm (le_of_not_gt (by simpa using hi)) (hq i)
  simp [hz]

def extend {m : ℕ} (T : Finset (Fin m)) (r : T → ℝ) : V m :=
  fun i => if hi : i ∈ T then r ⟨i, hi⟩ else 0

@[simp] theorem extend_on {m : ℕ} (T : Finset (Fin m)) (r : T → ℝ) (i : T) :
    extend T r i = r i := by
  simp [extend, i.property]

theorem sum_extend {m : ℕ} (T : Finset (Fin m)) (r : T → ℝ) :
    ∑ i, extend T r i = ∑ i, r i := by
  calc
    _ = ∑ i ∈ T, extend T r i := by
      symm
      apply Finset.sum_subset (Finset.subset_univ _)
      intro i _ hi
      simp [extend, hi]
    _ = _ := by
      rw [← Finset.sum_attach T]
      simp only [Finset.attach_eq_univ, extend_on]

theorem relation_extend {d m : ℕ} (T : Finset (Fin m)) (r : T → ℝ)
    (v : Fin m → V d) :
    ∑ i, extend T r i • v i = ∑ i : T, r i • v i := by
  calc
    _ = ∑ i ∈ T, extend T r i • v i := by
      symm
      apply Finset.sum_subset (Finset.subset_univ _)
      intro i _ hi
      simp [extend, hi]
    _ = _ := by
      rw [← Finset.sum_attach T]
      simp only [Finset.attach_eq_univ, extend_on]

theorem smaller_support {d m : ℕ} (v : Fin m → V d) (q : V m)
    (hq : Feasible v q) (j : Fin m) (hj : 0 < q j)
    (hind : ¬AffineIndependent ℝ (fun i : support q => v i)) :
    ∃ u : V m, Feasible v u ∧ 0 < u j ∧ support u ⊂ support q := by
  classical
  rw [affineIndependent_iff_of_fintype] at hind
  simp only [not_forall, exists_prop] at hind
  obtain ⟨r₀, hr₀sum, hr₀sub, i₀, hi₀⟩ := hind
  have hr₀rel : ∑ i : support q, r₀ i • v i = 0 := by
    rwa [Finset.univ.weightedVSub_eq_linear_combination hr₀sum] at hr₀sub
  let r₁ := extend (support q) r₀
  have hr₁sum : ∑ i, r₁ i = 0 := (sum_extend _ _).trans hr₀sum
  have hr₁rel : ∑ i, r₁ i • v i = 0 := (relation_extend _ _ _).trans hr₀rel
  have hr₁ne : r₁ (i₀ : Fin m) ≠ 0 := by
    simpa [r₁] using hi₀
  let r : V m := if r₁ j ≤ 0 then r₁ else -r₁
  have hrsum : ∑ i, r i = 0 := by
    dsimp [r]
    split_ifs <;> simp [hr₁sum]
  have hrrel : ∑ i, r i • v i = 0 := by
    dsimp [r]
    split_ifs
    · exact hr₁rel
    · simp only [Pi.neg_apply, neg_smul, Finset.sum_neg_distrib, hr₁rel, neg_zero]
  have hrj : r j ≤ 0 := by
    dsimp [r]
    split_ifs with h
    · exact h
    · simpa using (neg_nonpos.mpr (le_of_not_ge h))
  have hrne : r (i₀ : Fin m) ≠ 0 := by
    dsimp [r]
    split_ifs <;> simpa using hr₁ne
  have hroff (i : Fin m) (hi : i ∉ support q) : r i = 0 := by
    dsimp [r]
    split_ifs <;> simp [r₁, extend, hi]
  obtain ⟨i, _, hri⟩ := Finset.exists_pos_of_sum_zero_of_exists_nonzero r hrsum
    ⟨i₀, Finset.mem_univ _, hrne⟩
  let T := Finset.univ.filter fun i => 0 < r i
  have hT : T.Nonempty := ⟨i, by simp [T, hri]⟩
  obtain ⟨k, hk, hmin⟩ := T.exists_min_image (fun i => q i / r i) hT
  have hrk : 0 < r k := (Finset.mem_filter.mp hk).2
  have hqk : 0 < q k := by
    by_contra h
    have := hroff k (by simpa using h)
    linarith
  let c := q k / r k
  have hc : 0 < c := div_pos hqk hrk
  let u : V m := fun i => q i - c * r i
  have hunonneg : ∀ i, 0 ≤ u i := by
    intro i
    by_cases hi : 0 < r i
    · have hle : c ≤ q i / r i := hmin i (by simp [T, hi])
      have := (le_div_iff₀ hi).mp hle
      dsimp [u]
      linarith
    · have hmul : c * r i ≤ 0 := mul_nonpos_of_nonneg_of_nonpos hc.le (le_of_not_gt hi)
      dsimp [u]
      linarith [hq.1 i]
  have hsub : support u ⊆ support q := by
    intro i hi
    by_contra hn
    have hz : q i = 0 := le_antisymm (le_of_not_gt (by simpa using hn)) (hq.1 i)
    have hu : u i = 0 := by simp [u, hz, hroff i hn]
    have := (mem_support u i).mp hi
    linarith
  have huk : u k = 0 := by
    simp [u, c, div_mul_cancel₀ _ hrk.ne']
  refine ⟨u, ⟨hunonneg, ?_, ?_⟩, ?_, ?_⟩
  · simp only [u, Finset.sum_sub_distrib, ← Finset.mul_sum, hrsum, mul_zero,
      sub_zero, hq.2.1]
  · simp only [u, sub_smul, mul_smul, Finset.sum_sub_distrib, ← Finset.smul_sum,
      hrrel, smul_zero, sub_zero, hq.2.2]
  · dsimp [u]
    have := mul_nonpos_of_nonneg_of_nonpos hc.le hrj
    linarith
  · apply Finset.ssubset_iff_subset_ne.mpr ⟨hsub, ?_⟩
    intro heq
    have : k ∈ support u := heq ▸ ((mem_support q k).mpr hqk)
    simp [huk] at this

theorem independent_feasible {d m : ℕ} (v : Fin m → V d) (q : V m)
    (hq : Feasible v q) (j : Fin m) (hj : 0 < q j) :
    ∃ u : V m, Feasible v u ∧ 0 < u j ∧
      AffineIndependent ℝ (fun i : support u => v i) := by
  classical
  generalize hn : (support q).card = n
  induction n using Nat.strong_induction_on generalizing q with
  | h n ih =>
    by_cases hi : AffineIndependent ℝ (fun i : support q => v i)
    · exact ⟨q, hq, hj, hi⟩
    · obtain ⟨u, hu, huj, hsub⟩ := smaller_support v q hq j hj hi
      exact ih _ (by simpa [hn] using Finset.card_lt_card hsub) u hu huj rfl

def spread {R : Type} [AddCommMonoid R] {k m : ℕ} (e : Fin k ↪ Fin m)
    (q : Fin k → R) : Fin m → R :=
  fun j => ∑ i ∈ Finset.univ.filter (fun i => e i = j), q i

@[simp] theorem spread_on {R : Type} [AddCommMonoid R] {k m : ℕ}
    (e : Fin k ↪ Fin m) (q : Fin k → R) (i : Fin k) :
    spread e q (e i) = q i := by
  simp only [spread, e.injective.eq_iff]
  rw [Finset.filter_eq']
  simp

theorem spread_off {R : Type} [AddCommMonoid R] {k m : ℕ}
    (e : Fin k ↪ Fin m) (q : Fin k → R) (j : Fin m)
    (hj : ∀ i, e i ≠ j) : spread e q j = 0 := by
  simp [spread, hj]

theorem sum_spread {R : Type} [AddCommMonoid R] {k m : ℕ}
    (e : Fin k ↪ Fin m) (q : Fin k → R) : ∑ j, spread e q j = ∑ i, q i := by
  simp only [spread, Finset.sum_filter]
  rw [Finset.sum_comm]
  simp

theorem spread_relation {d k m : ℕ} (e : Fin k ↪ Fin m) (q : V k)
    (v : Fin m → V d) :
    ∑ j, spread e q j • v j = ∑ i, q i • v (e i) := by
  simp only [spread, Finset.sum_smul, Finset.sum_filter]
  rw [Finset.sum_comm]
  simp

theorem spread_cast {k m : ℕ} (e : Fin k ↪ Fin m) (q : Fin k → ℚ) :
    (fun j => ((spread (R := ℚ) e q j : ℚ) : ℝ)) =
      spread e (fun i => (q i : ℝ)) := by
  ext j
  simp [spread]

def freeze {R : Type} {m : ℕ} (q : Fin m → R) : Fin m → R :=
  let a := Array.ofFn q
  fun i => a[i.val]'(by simp [a])

@[simp] theorem freeze_eq {R : Type} {m : ℕ} (q : Fin m → R) : freeze q = q := by
  ext i
  simp [freeze]

/-- Every support is considered in its original coordinate order. -/
def candidates {d m : ℕ} (v : Fin m → IntPoint d) : Finset (Fin m → ℚ) :=
  ((List.range (m + 1)).flatMap fun k =>
    (embeddings k m).flatMap fun e =>
      if positiveSimplexCheck (fun i => v (e i)) then
        [freeze (spread e (simplexWeights (fun i => v (e i))))] else []).toFinset

theorem mem_candidates {d m : ℕ} (v : Fin m → IntPoint d) (p : Fin m → ℚ) :
    p ∈ candidates v ↔ ∃ k, k ≤ m ∧ ∃ e : Fin k ↪ Fin m,
      positiveSimplexCheck (fun i => v (e i)) = true ∧
        p = spread e (simplexWeights (fun i => v (e i))) := by
  simp only [candidates, freeze_eq, List.mem_toFinset, List.mem_flatMap, List.mem_range]
  constructor
  · rintro ⟨k, hk, e, _, hp⟩
    split_ifs at hp with hc
    · exact ⟨k, by omega, e, hc, (List.mem_singleton.mp hp)⟩
    · simp at hp
  · rintro ⟨k, hk, e, hc, rfl⟩
    exact ⟨k, by omega, e, mem_embeddings e, by simp [hc]⟩

theorem candidates_feasible {d m : ℕ} (v : Fin m → IntPoint d)
    (p : Fin m → ℚ) (hp : p ∈ candidates v) :
    Feasible (fun i => ofInt (v i)) (fun i => (p i : ℝ)) := by
  obtain ⟨k, _, e, hc, rfl⟩ := (mem_candidates v p).mp hp
  have hc' := of_decide_eq_true hc
  change (simplexGram (fun i => v (e i))).det ≠ 0 ∧
    (∀ i, 0 < simplexWeights (fun i => v (e i)) i) ∧
    (∑ i, simplexWeights (fun i => v (e i)) i) = 1 ∧
    ∀ j, (∑ i, simplexWeights (fun i => v (e i)) i * (v (e i) j : ℚ)) = 0 at hc'
  rw [spread_cast]
  refine ⟨fun j => Finset.sum_nonneg (fun i _ => ?_), ?_, ?_⟩
  · exact (Rat.cast_nonneg.mpr (hc'.2.1 i).le)
  · rw [sum_spread]
    exact_mod_cast hc'.2.2.1
  · rw [spread_relation]
    ext j
    have h := congrArg (fun x : ℚ => (x : ℝ)) (hc'.2.2.2 j)
    push_cast at h
    simpa [ofInt, Finset.sum_apply, Pi.smul_apply, smul_eq_mul] using h

theorem independent_candidate {d m : ℕ} (v : Fin m → IntPoint d) (q : V m)
    (hq : Feasible (fun i => ofInt (v i)) q)
    (hi : AffineIndependent ℝ (fun i : support q => ofInt (v i))) :
    ∃ p ∈ candidates v, (fun i => (p i : ℝ)) = q := by
  classical
  let T := support q
  let k := Fintype.card T
  let f : Fin k ≃ T := (Fintype.equivFin T).symm
  let e : Fin k ↪ Fin m := ⟨fun i => (f i : Fin m), Subtype.val_injective.comp f.injective⟩
  have hk : k ≤ m := by
    simpa [k] using Fintype.card_le_of_injective (fun i : T => (i : Fin m))
      Subtype.val_injective
  have hpos (i : Fin k) : 0 < q (e i) := by
    exact (mem_support q _).mp (f i).property
  have hsum : ∑ i : Fin k, q (e i) = 1 := by
    calc
      _ = ∑ i : T, q i := f.sum_comp (fun i : T => q i)
      _ = ∑ i ∈ support q, q i := Finset.sum_coe_sort _ _
      _ = 1 := (sum_on_support hq.1).trans hq.2.1
  have hrel : ∑ i : Fin k, q (e i) • ofInt (v (e i)) = 0 := by
    calc
      _ = ∑ i : T, q i • ofInt (v i) := f.sum_comp (fun i : T => q i • ofInt (v i))
      _ = ∑ i ∈ support q, q i • ofInt (v i) :=
        Finset.sum_coe_sort T (fun i : Fin m => q i • ofInt (v i))
      _ = 0 := (relation_on_support hq.1 _).trans hq.2.2
  have hind : AffineIndependent ℝ (fun i : Fin k => ofInt (v (e i))) :=
    hi.comp_embedding f.toEmbedding
  have hcheck := (positiveSimplexCheck_iff (fun i => v (e i))).mpr
    ⟨hind, fun i => q (e i), hpos, hsum, hrel⟩
  let p := spread e (simplexWeights (fun i => v (e i)))
  refine ⟨p, (mem_candidates v p).mpr ⟨k, hk, e, hcheck, rfl⟩, ?_⟩
  have heq := simplexWeights_unique (fun i => v (e i)) (fun i => q (e i))
    hind hsum hrel
  ext j
  by_cases hj : j ∈ T
  · obtain ⟨i, hi⟩ := f.surjective ⟨j, hj⟩
    have hei : e i = j := congrArg Subtype.val hi
    rw [← hei]
    simpa [p] using heq i
  · have hoff : ∀ i, e i ≠ j := by
      intro i he
      apply hj
      exact he ▸ (f i).property
    have hzero : q j = 0 :=
      le_antisymm (le_of_not_gt (by simpa [T] using hj)) (hq.1 j)
    simp [p, spread_off e _ j hoff, hzero]

/-- An explicit finite average of all rational basic feasible solutions. -/
def select {d m : ℕ} (v : Fin m → IntPoint d) : Fin m → ℚ :=
  let F := candidates v
  freeze (fun i => (∑ p ∈ F, p i) / F.card)

theorem select_spec {d m : ℕ} (v : Fin m → IntPoint d) (q : V m)
    (hq : Feasible (fun i => ofInt (v i)) q) (hpos : ∀ i, 0 < q i) :
    (∀ i, 0 < select v i) ∧ (∑ i, select v i) = 1 ∧
      ∑ i, (select v i : ℝ) • ofInt (v i) = 0 := by
  classical
  let F := candidates v
  have hF (p) (hp : p ∈ F) := candidates_feasible v p hp
  have hcover (i : Fin m) : ∃ p ∈ F, (0 : ℝ) < p i := by
    obtain ⟨u, hu, hui, hi⟩ := independent_feasible (fun i => ofInt (v i)) q hq i (hpos i)
    obtain ⟨p, hp, heq⟩ := independent_candidate v u hu hi
    exact ⟨p, hp, by simpa only [congrFun heq] using hui⟩
  have hFn : F.Nonempty := by
    have hex : ∃ j, 0 < q j := by
      by_contra! hn
      have := Finset.sum_nonpos (fun i (_ : i ∈ Finset.univ) => hn i)
      linarith [hq.2.1]
    obtain ⟨j, hj⟩ := hex
    obtain ⟨p, hp, _⟩ := hcover j
    exact ⟨p, hp⟩
  have hcard : (0 : ℝ) < F.card := by exact_mod_cast Finset.card_pos.mpr hFn
  have hcast (i : Fin m) : (select v i : ℝ) =
      (F.card : ℝ)⁻¹ * ∑ p ∈ F, (p i : ℝ) := by
    simp [select, F, div_eq_mul_inv, mul_comm]
  have hsum : ∑ i, (select v i : ℝ) = 1 := by
    simp only [hcast, ← Finset.mul_sum]
    rw [Finset.sum_comm]
    have heach : ∑ p ∈ F, ∑ i, (p i : ℝ) = F.card := by
      calc
        _ = ∑ p ∈ F, (1 : ℝ) := Finset.sum_congr rfl (fun p hp => (hF p hp).2.1)
        _ = _ := by simp
    rw [heach, inv_mul_cancel₀ hcard.ne']
  refine ⟨?_, by exact_mod_cast hsum, ?_⟩
  · intro i
    have hs : (0 : ℝ) < ∑ p ∈ F, (p i : ℝ) :=
      Finset.sum_pos' (fun p hp => (hF p hp).1 i) (hcover i)
    have hp : (0 : ℝ) < select v i := by
      rw [hcast]
      exact mul_pos (inv_pos.mpr hcard) hs
    exact_mod_cast hp
  · simp only [hcast, mul_smul, ← Finset.smul_sum, Finset.sum_smul]
    rw [Finset.sum_comm]
    have hz : ∑ p ∈ F, ∑ i, (p i : ℝ) • ofInt (v i) = 0 :=
      Finset.sum_eq_zero (fun p hp => (hF p hp).2.2)
    rw [hz, smul_zero]

end Refpoly.KS.RationalSelection
