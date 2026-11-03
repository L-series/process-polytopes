import Mathlib

/-!
# Selection-index decoding of the GPU generator

`src/gpu/generate.cuh` turns a linear index into a selection tuple in two ways.

* `device_decode_tuple`: mixed radix over the slot pool sizes, slot 0 most
  significant, processed from the last slot (`remainder % count`, then divide).
* `device_pair_left_from_linear` (binary search) and
  `device_pair_left_closed` (floating-point estimate, then integer correction):
  the pairs `left ≤ right < n` of two exchangeable slots, where pair index
  `p` lies in the block of `left` iff `first left ≤ p < first (left + 1)`, with
  `first l = l * n - l * (l - 1) / 2`.

Both are proved to be bijections onto the tuples they stand for. The
correction loops reach the block from every start, so the floating-point
estimate affects only speed. The C code works in `unsigned long long`; with
`n ≤ 2^32` every intermediate below fits, so the natural-number statements
apply.
-/

namespace Refpoly.GpuDecode

/-! ## Mixed radix -/

/-- `device_decode_tuple`: the digits of `r` in slot order and the final
quotient. The last slot is reduced first. -/
def decode : List ℕ → ℕ → List ℕ × ℕ
  | [], r => ([], r)
  | c :: cs, r => ((decode cs r).2 % c :: (decode cs r).1, (decode cs r).2 / c)

/-- Slot 0 most significant. -/
def encode : List ℕ → List ℕ → ℕ
  | _ :: cs, d :: ds => d * cs.prod + encode cs ds
  | _, _ => 0

theorem decode_length (cs : List ℕ) (r : ℕ) : (decode cs r).1.length = cs.length := by
  induction cs with
  | nil => rfl
  | cons c cs ih => simp [decode, ih]

theorem decode_spec (cs : List ℕ) (r : ℕ) :
    encode cs (decode cs r).1 + (decode cs r).2 * cs.prod = r := by
  induction cs with
  | nil => simp [decode, encode]
  | cons c cs ih =>
      simp only [decode, encode, List.prod_cons]
      have h := Nat.mod_add_div (decode cs r).2 c
      calc (decode cs r).2 % c * cs.prod + encode cs (decode cs r).1 + (decode cs r).2 / c * (c * cs.prod)
          = encode cs (decode cs r).1 + ((decode cs r).2 % c + c * ((decode cs r).2 / c)) * cs.prod := by ring
        _ = r := by rw [h, ih]

theorem decode_digits (cs : List ℕ) (r : ℕ) (hc : ∀ c ∈ cs, 0 < c) :
    ∀ i (h : i < cs.length), (decode cs r).1[i]'(by rw [decode_length]; exact h) < cs[i] := by
  induction cs with
  | nil => intro i h; simp at h
  | cons c cs ih =>
      intro i h
      cases i with
      | zero => simp [decode]; exact Nat.mod_lt _ (hc c (by simp))
      | succ i =>
          simp only [decode, List.getElem_cons_succ]
          exact ih (fun c' h' => hc c' (by simp [h'])) i (by simpa using h)

/-- Every index below the selection product decodes to in-range digits that
re-encode to it: `device_decode_tuple` is a bijection from `[0, prod)` onto
the digit tuples. -/
theorem decode_bijective (cs : List ℕ) (r : ℕ) (hc : ∀ c ∈ cs, 0 < c) (hr : r < cs.prod) :
    encode cs (decode cs r).1 = r ∧ (decode cs r).2 = 0 := by
  have h := decode_spec cs r
  have hp : 0 < cs.prod := List.prod_pos (fun c h => hc c h)
  have hq : (decode cs r).2 = 0 := by
    by_contra hne
    have : cs.prod ≤ (decode cs r).2 * cs.prod := Nat.le_mul_of_pos_left _ (Nat.pos_of_ne_zero hne)
    omega
  exact ⟨by rw [hq] at h; simpa using h, hq⟩

/-! ## Pairs `left ≤ right < n` -/

/-- Pairs with a smaller left: `sum_{i < l} (n - i)`. -/
def first (n l : ℕ) : ℕ := ∑ i ∈ Finset.range l, (n - i)

theorem first_succ (n l : ℕ) : first n (l + 1) = first n l + (n - l) := by
  simp [first, Finset.sum_range_succ]

theorem first_twice (n l : ℕ) (hl : l ≤ n) : 2 * first n l + l * (l - 1) = 2 * (l * n) := by
  induction l with
  | zero => simp [first]
  | succ l ih =>
      have h := ih (by omega)
      rw [first_succ]
      zify [show l ≤ n by omega] at h ⊢
      cases l with
      | zero => push_cast at h ⊢; ring_nf at h ⊢; linarith
      | succ m => push_cast at h ⊢; ring_nf at h ⊢; nlinarith

/-- The C expression `l * n - l * (l - 1) / 2` equals `first n l` for `l ≤ n`. -/
theorem first_formula (n l : ℕ) (hl : l ≤ n) : l * n - l * (l - 1) / 2 = first n l := by
  have h := first_twice n l hl
  have hev : 2 * (l * (l - 1) / 2) = l * (l - 1) := by
    have : (l * (l - 1)) % 2 = 0 := by
      rcases Nat.even_or_odd l with ⟨k, hk⟩ | ⟨k, hk⟩
      · subst hk; simp [Nat.mul_mod, show (k + k) % 2 = 0 by omega]
      · subst hk; simp [show 2 * k + 1 - 1 = 2 * k by omega, Nat.mul_mod]
    omega
  omega

theorem first_mono (n : ℕ) {a b : ℕ} (h : a ≤ b) : first n a ≤ first n b := by
  induction b with
  | zero => simp_all
  | succ b ih =>
      rcases Nat.lt_or_ge a (b + 1) with h' | h'
      · have := ih (by omega); rw [first_succ]; omega
      · rw [show a = b + 1 by omega]

theorem first_strict (n : ℕ) {a b : ℕ} (h : a < b) (hb : b ≤ n) : first n a < first n b := by
  induction b with
  | zero => omega
  | succ b ih =>
      rw [first_succ]
      rcases Nat.lt_or_ge a b with h' | h'
      · have := ih h' (by omega); omega
      · rw [show a = b by omega]; omega

/-- The pair blocks: every `p < first n n` lies in exactly one block `l < n`,
and its right index `l + (p - first n l)` is in `[l, n)`. -/
theorem block_unique (n p : ℕ) (hp : p < first n n) :
    ∃! l, l < n ∧ first n l ≤ p ∧ p < first n (l + 1) := by
  -- existence: the largest l ≤ n with first l ≤ p
  have hex : ∃ l, l < n ∧ first n l ≤ p ∧ p < first n (l + 1) := by
    induction n generalizing p with
    | zero => simp [first] at hp
    | succ m _ =>
        let S := (Finset.range (m + 1)).filter (fun l => first (m + 1) l ≤ p)
        have hS : (0 : ℕ) ∈ S := by simp [S, first]
        let l := S.max' ⟨0, hS⟩
        have hl : l ∈ S := S.max'_mem _
        simp only [S, Finset.mem_filter, Finset.mem_range] at hl
        refine ⟨l, hl.1, hl.2, ?_⟩
        rcases Nat.lt_or_ge p (first (m + 1) (l + 1)) with hlt' | hge
        · exact hlt'
        rcases Nat.lt_or_ge (l + 1) (m + 1) with hlt | hge2
        · have : l + 1 ∈ S := by simp [S, hlt, hge]
          have := S.le_max' _ this
          omega
        · rw [show l + 1 = m + 1 by omega] at hge; omega
  obtain ⟨l, hl⟩ := hex
  refine ⟨l, hl, fun l' hl' => ?_⟩
  by_contra hne
  rcases Nat.lt_or_gt_of_ne hne with h | h
  · have := first_mono n (show l' + 1 ≤ l by omega); omega
  · have := first_mono n (show l + 1 ≤ l' by omega); omega

theorem right_in_range (n p l : ℕ) (hl : l < n) (h1 : first n l ≤ p) (h2 : p < first n (l + 1)) :
    l ≤ l + (p - first n l) ∧ l + (p - first n l) < n := by
  rw [first_succ] at h2; omega

/-- Every pair `l ≤ r < n` has an index in `[0, first n n)`, in block `l`, so
the pair decoding is onto as well as one-to-one. -/
theorem pair_surjective (n l r : ℕ) (hlr : l ≤ r) (hr : r < n) :
    first n l + (r - l) < first n n ∧ first n l ≤ first n l + (r - l) ∧
      first n l + (r - l) < first n (l + 1) := by
  have h1 := first_succ n l
  have h2 := first_mono n (show l + 1 ≤ n by omega)
  omega

/-- `triangular_count(n)` (halving the even factor first, so no intermediate
exceeds the result) is the size `first n n` of the pair space. -/
theorem triangular_formula (n : ℕ) :
    (if n % 2 = 0 then n / 2 * (n + 1) else n * ((n + 1) / 2)) = first n n := by
  have h := first_twice n n le_rfl
  have h2 : 2 * first n n = n * (n + 1) := by
    cases n with
    | zero => simp [first]
    | succ m => simp only [Nat.add_sub_cancel] at h; nlinarith
  split_ifs with he
  · obtain ⟨k, rfl⟩ : ∃ k, n = 2 * k := ⟨n / 2, by omega⟩
    rw [show 2 * k / 2 = k by omega]; nlinarith
  · obtain ⟨k, rfl⟩ : ∃ k, n = 2 * k + 1 := ⟨n / 2, by omega⟩
    rw [show (2 * k + 1 + 1) / 2 = k + 1 by omega]; nlinarith

/-! ### The binary search of `device_pair_left_from_linear`

`low = 0, high = n; while (low + 1 < high) { mid = low + (high - low) / 2;
if (first(mid) <= p) low = mid; else high = mid; } return low;` -/

def search (n p low high : ℕ) : ℕ :=
  if low + 1 < high then
    if first n (low + (high - low) / 2) ≤ p then search n p (low + (high - low) / 2) high
    else search n p low (low + (high - low) / 2)
  else low
termination_by high - low
decreasing_by all_goals omega

/-- Invariant `first low ≤ p < first high` is kept; at exit `high = low + 1`. -/
theorem search_block (n p : ℕ) : ∀ k low high, high - low ≤ k → low < high → high ≤ n →
    first n low ≤ p → p < first n high →
    search n p low high < n ∧ first n (search n p low high) ≤ p ∧ p < first n (search n p low high + 1) := by
  intro k
  induction k with
  | zero => intro low high hk hlh; omega
  | succ k ih =>
      intro low high hk hlh hhn h1 h2
      rw [search]
      split_ifs with hlt hle
      · exact ih (low + (high - low) / 2) high (by omega) (by omega) hhn hle h2
      · exact ih low (low + (high - low) / 2) (by omega) (by omega) (by omega) h1 (by omega)
      · exact ⟨by omega, h1, by rw [show low + 1 = high by omega]; exact h2⟩

/-- `device_pair_left_from_linear(p, n)` returns the block of `p`. -/
theorem search_correct (n p : ℕ) (hn : 0 < n) (hp : p < first n n) :
    search n p 0 n < n ∧ first n (search n p 0 n) ≤ p ∧ p < first n (search n p 0 n + 1) :=
  search_block n p n 0 n (by omega) hn le_rfl (by simp [first]) hp

/-! ### The correction loops of `device_pair_left_closed`

From any start `l0 ≤ n - 1`: `while (l > 0 && first(l) > p) --l;` then
`while (l + 1 < n && first(l + 1) <= p) ++l;`. -/

def down (n p : ℕ) : ℕ → ℕ
  | 0 => 0
  | l + 1 => if first n (l + 1) > p then down n p l else l + 1

def up (n p : ℕ) (l : ℕ) : ℕ :=
  if h : l + 1 < n ∧ first n (l + 1) ≤ p then up n p (l + 1) else l
termination_by n - l

theorem down_le (n p l : ℕ) : down n p l ≤ l := by
  induction l with
  | zero => simp [down]
  | succ l ih => unfold down; split <;> omega

theorem down_spec (n p l : ℕ) : first n (down n p l) ≤ p := by
  induction l with
  | zero => simp [down, first]
  | succ l ih => unfold down; split <;> omega

theorem up_spec (n p : ℕ) : ∀ k l, n - l ≤ k → l < n → first n l ≤ p → p < first n n →
    up n p l < n ∧ first n (up n p l) ≤ p ∧ p < first n (up n p l + 1) := by
  intro k
  induction k with
  | zero => intro l hk hl; omega
  | succ k ih =>
      intro l hk hl h1 hp
      rw [up]
      split_ifs with h
      · exact ih _ (by omega) h.1 h.2 hp
      · refine ⟨hl, h1, ?_⟩
        rcases Nat.lt_or_ge p (first n (l + 1)) with hlt | hge
        · exact hlt
        rcases Nat.lt_or_ge (l + 1) n with hlt | hge2
        · exact absurd ⟨hlt, hge⟩ h
        · rw [show l + 1 = n by omega] at hge; omega

/-- `device_pair_left_closed(p, n)` returns the block of `p` for every
floating-point estimate `l0 < n`. -/
theorem closed_correct (n p l0 : ℕ) (hl0 : l0 < n) (hp : p < first n n) :
    up n p (down n p l0) < n ∧ first n (up n p (down n p l0)) ≤ p ∧
      p < first n (up n p (down n p l0) + 1) :=
  up_spec n p n _ (by omega) (lt_of_le_of_lt (down_le n p l0) hl0) (down_spec n p l0) hp

end Refpoly.GpuDecode
