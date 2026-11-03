-- Source-function SHA256: adede5de7411ca5a6c9b4d6d4718a2ed52231ccbd9e1d3beeee07a973a8354a8
import Refpoly.CudaIPAcceptance
import Refpoly.CudaIPArithmeticSource

namespace Refpoly.CudaIP

/-- Checked signed 64-bit addition and multiplication of 64-bit operands, as
the source helpers compute them (source_add_guard_correct,
source_mul_guard_correct): the exact result, or failure. -/
def chkAdd (a b : ℤ) : Option ℤ := if fits64 (a + b) then some (a + b) else none
def chkMul (a b : ℤ) : Option ℤ := if fits64 (a * b) then some (a * b) else none

/-- `value = 0; for each (a, b): value = add(value, mul(a, b))`. -/
def dotChecked (l : List (ℤ × ℤ)) : Option ℤ :=
  l.foldl (fun acc ab => acc.bind fun v => (chkMul ab.1 ab.2).bind (chkAdd v)) (some 0)

/-- `total = 0; for each a: total = add(total, a)`. -/
def sumChecked (l : List ℤ) : Option ℤ :=
  l.foldl (fun acc a => acc.bind (chkAdd · a)) (some 0)

theorem dotChecked_exact_aux (l : List (ℤ × ℤ)) :
    ∀ v w, l.foldl (fun acc ab => acc.bind fun v => (chkMul ab.1 ab.2).bind (chkAdd v)) (some v) = some w →
      w = v + (l.map fun ab => ab.1 * ab.2).sum := by
  induction l with
  | nil => intro v w h; simp at h; simp [h]
  | cons ab l ih =>
    intro v w h
    simp only [List.foldl_cons, Option.bind_some] at h
    cases hm : chkMul ab.1 ab.2 with
    | none =>
      rw [hm] at h
      exfalso
      have : ∀ l' : List (ℤ × ℤ), l'.foldl (fun acc ab => acc.bind fun v => (chkMul ab.1 ab.2).bind (chkAdd v))
          none = none := by
        intro l'; induction l' with
        | nil => rfl
        | cons _ _ ih' => simpa using ih'
      simp [this] at h
    | some p =>
      have hp : p = ab.1 * ab.2 := by
        unfold chkMul at hm; split_ifs at hm; simp at hm; omega
      rw [hm] at h
      cases ha : chkAdd v p with
      | none =>
        exfalso
        have : ∀ l' : List (ℤ × ℤ), l'.foldl (fun acc ab => acc.bind fun v => (chkMul ab.1 ab.2).bind (chkAdd v))
            none = none := by
          intro l'; induction l' with
          | nil => rfl
          | cons _ _ ih' => simpa using ih'
        simp [ha, this] at h
      | some q =>
        have hq : q = v + p := by
          unfold chkAdd at ha; split_ifs at ha; simp at ha; omega
        simp only [ha, Option.bind_some] at h
        have := ih q w h
        simp only [List.map_cons, List.sum_cons]
        rw [this, hq, hp]; ring

theorem dotChecked_exact (l : List (ℤ × ℤ)) (w : ℤ) (h : dotChecked l = some w) :
    w = (l.map fun ab => ab.1 * ab.2).sum := by
  simpa using dotChecked_exact_aux l 0 w h

theorem sumChecked_exact_aux (l : List ℤ) :
    ∀ v w, l.foldl (fun acc a => acc.bind (chkAdd · a)) (some v) = some w → w = v + l.sum := by
  induction l with
  | nil => intro v w h; simp at h; simp [h]
  | cons a l ih =>
    intro v w h
    simp only [List.foldl_cons, Option.bind_some] at h
    cases ha : chkAdd v a with
    | none =>
      exfalso
      have : ∀ l' : List ℤ, l'.foldl (fun acc a => acc.bind (chkAdd · a)) none = none := by
        intro l'; induction l' with
        | nil => rfl
        | cons _ _ ih' => simpa using ih'
      simp [ha, this] at h
    | some q =>
      have hq : q = v + a := by unfold chkAdd at ha; split_ifs at ha; simp at ha; omega
      rw [ha] at h
      rw [ih q w h, hq, List.sum_cons]; ring

theorem sumChecked_exact (l : List ℤ) (w : ℤ) (h : sumChecked l = some w) : w = l.sum := by
  simpa using sumChecked_exact_aux l 0 w h

/-- The certificate fields of `DeviceAcceptance5`. -/
structure Acceptance where
  b : Fin 5 → ℤ
  adj : Fin 5 → Fin 5 → ℤ
  d : ℤ
  idx : Fin 5 → ℤ
  mu : Fin 5 → ℤ
  k : ℤ

/-- `points[i * 5 + j]` for the point list `xs`. -/
def pt (xs : List (Fin 5 → ℤ)) (i : ℤ) : Fin 5 → ℤ := xs.getD i.toNat 0

/-- Model of `device_ip_acceptance_verified`: it returns 1 exactly when this holds. -/
def sourceAcceptance (c : Acceptance) (xs : List (Fin 5 → ℤ)) : Prop :=
  c.d ≠ 0 ∧ 0 < c.k ∧
  (∀ r, 0 ≤ c.b r ∧ c.b r < xs.length ∧ 0 ≤ c.idx r ∧ c.idx r < xs.length ∧ 0 ≤ c.mu r) ∧
  (∃ s, sumChecked (List.ofFn c.mu) = some s ∧ 0 < s) ∧
  (∀ i j, dotChecked (List.ofFn fun t => (c.adj i t, pt xs (c.b t) j)) = some (if i = j then c.d else 0)) ∧
  (∀ j, dotChecked ((List.ofFn fun r => [(c.mu r, pt xs (c.idx r) j), (c.k, pt xs (c.b r) j)]).flatten) = some 0)

theorem pt_mem (xs : List (Fin 5 → ℤ)) (i : ℤ) (h0 : 0 ≤ i) (h1 : i < xs.length) : pt xs i ∈ xs := by
  unfold pt
  rw [List.getD_eq_getElem _ _ (by omega)]
  exact List.getElem_mem _

/-- A passing source checker proves the point set IP. -/
theorem sourceAcceptance_sound (c : Acceptance) (xs : List (Fin 5 → ℤ))
    (h : sourceAcceptance c xs) : IsIP (KS.PolyCode.hull ⟨xs⟩) := by
  obtain ⟨hd, hk, hidx, ⟨s, hs, hspos⟩, hadj, hzero⟩ := h
  let B : Fin 5 → KS.IntPoint 5 := fun r => pt xs (c.b r)
  let P : Fin 5 → KS.IntPoint 5 := fun r => pt xs (c.idx r)
  have hA : Matrix.of c.adj * Matrix.of B = c.d • (1 : Matrix (Fin 5) (Fin 5) ℤ) := by
    ext i j
    have := dotChecked_exact _ _ (hadj i j)
    simp only [List.map_ofFn, List.sum_ofFn, Function.comp] at this
    simp only [Matrix.mul_apply, Matrix.of_apply, Matrix.smul_apply, Matrix.one_apply, smul_eq_mul, B]
    rw [← this]; split_ifs <;> simp
  have hmu : ∀ r, 0 ≤ c.mu r := fun r => (hidx r).2.2.2.2
  have hsum : s = ∑ r, c.mu r := by
    rw [sumChecked_exact _ _ hs, List.sum_ofFn]
  have hz : ∑ r, ((c.mu r).toNat : ℤ) • P r + ((c.k.toNat : ℕ) : ℤ) • ∑ r, B r = 0 := by
    funext j
    have := dotChecked_exact _ _ (hzero j)
    simp only [List.map_flatten, List.map_ofFn] at this
    rw [List.sum_flatten, List.map_ofFn, List.sum_ofFn] at this
    simp only [Finset.sum_apply, Pi.add_apply, Pi.smul_apply, smul_eq_mul, Pi.zero_apply,
      Int.toNat_of_nonneg (hmu _), Int.toNat_of_nonneg hk.le, Finset.mul_sum, ← Finset.sum_add_distrib]
    rw [this]
    simp [P, B]
  apply checked_acceptance xs B (fun r => pt_mem xs _ (hidx r).1 (hidx r).2.1) (Matrix.of c.adj) c.d hd hA P
    (fun r => pt_mem xs _ (hidx r).2.2.1 (hidx r).2.2.2.1) (fun r => (c.mu r).toNat)
    (by
      have : (0 : ℤ) < ∑ r, ((c.mu r).toNat : ℤ) := by
        simp only [Int.toNat_of_nonneg (hmu _)]; rw [← hsum]; exact hspos
      exact_mod_cast this)
    c.k.toNat (by omega) hz

end Refpoly.CudaIP
