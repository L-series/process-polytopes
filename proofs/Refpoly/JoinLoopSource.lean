import Refpoly.JoinLoop
import Refpoly.JoinSearch

/-!
# Transcription of the CUDA join search loop

Lean transcription of `device_gjoin_begin_level`, `device_gjoin_match_key` and
the search loop of `device_gjoin_decide` (src/gpu/join.cuh, pinned by
scripts/check_join_loop_model.py). Values are naturals: table keys are 64-bit
unsigned words, shared values 16-bit fields, indices below 2^32; `x >> s` is
`x / 2^s` and `x & 0xFFFF` is `x % 2^16`.

`cstep` is one iteration of the loop's inner scan: try the next position of the
current level's range; on a match descend (`++depth; begin_level`) or emit a
leaf at the last level; when the range is exhausted pop (`--depth`). The outer
`while (depth >= 0)` with its inner `while (it < end)` is a sequence of these.

`cstep_refines` proves that decoding the source state into frames turns each
`cstep` into one `JoinLoop.step`, so the source loop emits exactly
`JoinLoop.leavesFrom` (`crun_exact`).
-/
namespace Refpoly.JoinLoopSource

open Refpoly.JoinLoop Refpoly.JoinRejection

/-- The plan and tables as the loop reads them (indexed by depth `j`, slot `s`,
shared position `p`, coordinate `A`, global key index `i`, index position). -/
structure Env where
  R : ℕ
  slot : ℕ → ℕ
  lookup : ℕ → Bool
  bound : ℕ → ℕ → Bool
  k : ℕ → ℕ
  coord : ℕ → ℕ → ℕ
  key : ℕ → ℕ → ℕ
  pkey : ℕ → ℕ → ℕ → ℕ
  pidx : ℕ → ℕ → ℕ → ℕ
  kb : ℕ → ℕ
  ke : ℕ → ℕ

/-- `(key >> (16 * (k - 1 - p))) & 0xFFFF`. -/
def field (k p key : ℕ) : ℕ := key / 2 ^ (16 * (k - 1 - p)) % 2 ^ 16

/-- `device_gjoin_match_key` from position `p` on: a bound coordinate must
equal the key's value (else fail at once, keeping writes made so far), an
unbound one is written. -/
def matchFrom (key k : ℕ) (coords : ℕ → ℕ) (bnd : ℕ → Bool) : ℕ → (ℕ → ℕ) → Bool × (ℕ → ℕ)
  | p, val =>
    if h : p < k then
      let A := coords p
      if bnd A then
        if val A ≠ field k p key then (false, val) else matchFrom key k coords bnd (p + 1) val
      else matchFrom key k coords bnd (p + 1) (Function.update val A (field k p key))
    else (true, val)
termination_by p => k - p

def matchKey (key k : ℕ) (coords : ℕ → ℕ) (bnd : ℕ → Bool) (val : ℕ → ℕ) : Bool × (ℕ → ℕ) :=
  matchFrom key k coords bnd 0 val

/-- `want = (want << 16) | val[coord p]` for `p < k`. -/
def pack (k : ℕ) (f : ℕ → ℕ) : ℕ := (List.range k).foldl (fun want p => want * 2 ^ 16 + f p) 0

/-- `while (p < k && !bound(coord p)) ++p;` -/
def firstBound (k : ℕ) (coords : ℕ → ℕ) (bnd : ℕ → Bool) : ℕ → Option ℕ
  | p => if h : p < k then (if bnd (coords p) then some p else firstBound k coords bnd (p + 1)) else none
termination_by p => k - p

/-- `device_gjoin_begin_level`: the level's range `[it, end)` and `pstar`. -/
def beginLevel (E : Env) (j : ℕ) (val : ℕ → ℕ) : ℕ × ℕ × Option ℕ :=
  let s := E.slot j
  let k := E.k s
  let b := E.kb s
  let e := E.ke s
  if E.lookup j then
    let want := pack k (fun p => val (E.coord s p))
    let lo := searchLower (fun i => E.key s i < want) (e - b) b e
    (lo, if lo < e ∧ E.key s lo = want then lo + 1 else lo, none)
  else match firstBound k (E.coord s) (E.bound j) 0 with
    | none => (b, e, none)
    | some p =>
      let v := val (E.coord s p)
      let sh := 16 * (k - 1)
      if p = 0 then
        let lo := searchLower (fun i => E.key s i < v * 2 ^ sh) (e - b) b e
        (lo, searchLower (fun i => E.key s i / 2 ^ sh ≤ v) (e - lo) lo e, some 0)
      else
        let lo := searchLower (fun i => E.pkey s p i < v * 2 ^ sh) (e - b) b e
        (lo, searchLower (fun i => E.pkey s p i / 2 ^ sh ≤ v) (e - lo) lo e, some p)

/-- `kk = px ? px[pos].idx : pos` with `px` the index of `pstar > 0`. -/
def keyIndex (E : Env) (j : ℕ) (ps : Option ℕ) (pos : ℕ) : ℕ :=
  match ps with
  | some p => if 0 < p then E.pidx (E.slot j) p pos else pos
  | none => pos

/-- Source loop state; `active` is `depth + 1`. -/
structure CState where
  active : ℕ
  it : ℕ → ℕ
  en : ℕ → ℕ
  ps : ℕ → Option ℕ
  chosen : ℕ → ℕ
  val : ℕ → ℕ

/-- One iteration of the scan at the current level. -/
def cstep (E : Env) (st : CState) : List (List ℕ) × CState :=
  match st.active with
  | 0 => ([], st)
  | d + 1 =>
    let s := E.slot d
    if st.it d < st.en d then
      let pos := st.it d
      let kk := keyIndex E d (st.ps d) pos
      let m := matchKey (E.key s kk) (E.k s) (E.coord s) (E.bound d) st.val
      let it1 := Function.update st.it d (pos + 1)
      if m.1 then
        let ch := Function.update st.chosen d kk
        if d + 1 < E.R then
          let lv := beginLevel E (d + 1) m.2
          ([], ⟨d + 2, Function.update it1 (d + 1) lv.1, Function.update st.en (d + 1) lv.2.1,
                Function.update st.ps (d + 1) lv.2.2, ch, m.2⟩)
        else ([(List.range (d + 1)).map ch], ⟨d + 1, it1, st.en, st.ps, ch, m.2⟩)
      else ([], ⟨d + 1, it1, st.en, st.ps, st.chosen, m.2⟩)
    else ([], ⟨d, st.it, st.en, st.ps, st.chosen, st.val⟩)

def crun (E : Env) : ℕ → CState → List (List ℕ)
  | 0, _ => []
  | fuel + 1, st => match st.active with
    | 0 => []
    | _ + 1 => (cstep E st).1 ++ crun E fuel (cstep E st).2

/-! ## The abstract instance and the decoding -/

def restrict (bnd : ℕ → Bool) (val : ℕ → ℕ) : ℕ → ℕ := fun A => if bnd A then val A else 0

/-- Matching level `j` against a bound state; the next state is the values
bound below `j + 1`. -/
def extendA (E : Env) (j : ℕ) (state : ℕ → ℕ) (kk : ℕ) : Option (ℕ → ℕ) :=
  let s := E.slot j
  let m := matchKey (E.key s kk) (E.k s) (E.coord s) (E.bound j) state
  if m.1 then some (restrict (E.bound (j + 1)) m.2) else none

def candA (E : Env) (j : ℕ) (state : ℕ → ℕ) : List ℕ :=
  let lv := beginLevel E j state
  (List.range' lv.1 (lv.2.1 - lv.1)).map (keyIndex E j lv.2.2)

def frameOf (E : Env) (st : CState) (j : ℕ) : Frame (ℕ → ℕ) ℕ :=
  ⟨j, restrict (E.bound j) st.val, (List.range' (st.it j) (st.en j - st.it j)).map (keyIndex E j (st.ps j)),
   (List.range j).map st.chosen, E.R - 1 - j⟩

/-- Active levels, deepest first (the machine's stack order). -/
def decode (E : Env) (st : CState) : List (Frame (ℕ → ℕ) ℕ) :=
  ((List.range st.active).reverse).map (frameOf E st)


/-! ## The matcher and range initialization read only bound values -/

/-- Induction up to a bound, for loops `p = p0, p0 + 1, ..., k`. -/
theorem upto_ind (k : ℕ) {P : ℕ → Prop} (base : ∀ p, k ≤ p → P p)
    (step : ∀ p, p < k → P (p + 1) → P p) : ∀ p, P p := by
  intro p
  obtain ⟨n, hn⟩ : ∃ n, k - p = n := ⟨_, rfl⟩
  induction n generalizing p with
  | zero => exact base p (by omega)
  | succ n ih => exact step p (by omega) (ih (p + 1) (by omega))

theorem matchFrom_bound (key k : ℕ) (coords : ℕ → ℕ) (bnd : ℕ → Bool) :
    ∀ p val A, bnd A = true → (matchFrom key k coords bnd p val).2 A = val A := by
  refine upto_ind k (fun p hp => ?_) (fun p hp ih => ?_)
  · intro val A _; rw [matchFrom, dif_neg (by omega)]
  · intro val A hA
    rw [matchFrom, dif_pos hp]
    simp only
    split_ifs with hb hne
    · rfl
    · exact ih val A hA
    · rw [ih _ A hA, Function.update_of_ne]
      intro h; subst h; simp_all

theorem matchFrom_ok_congr (key k : ℕ) (coords : ℕ → ℕ) (bnd : ℕ → Bool) :
    ∀ p val₁ val₂, (∀ A, bnd A = true → val₁ A = val₂ A) →
      (matchFrom key k coords bnd p val₁).1 = (matchFrom key k coords bnd p val₂).1 := by
  refine upto_ind k (fun p hp => ?_) (fun p hp ih => ?_)
  · intro _ _ _
    conv_lhs => rw [matchFrom, dif_neg (by omega)]
    conv_rhs => rw [matchFrom, dif_neg (by omega)]
  · intro val₁ val₂ hv
    conv_lhs => rw [matchFrom, dif_pos hp]
    conv_rhs => rw [matchFrom, dif_pos hp]
    simp only
    by_cases hb : bnd (coords p) = true
    · rw [if_pos hb, if_pos hb, hv _ hb]
      split_ifs
      · rfl
      · exact ih _ _ hv
    · rw [if_neg hb, if_neg hb]
      apply ih
      intro A hA
      by_cases hAp : A = coords p
      · subst hAp; exact absurd hA hb
      · rw [Function.update_of_ne hAp, Function.update_of_ne hAp]; exact hv A hA

/-- A coordinate not met again from position `r` on keeps its value. -/
theorem matchFrom_keep (key k : ℕ) (coords : ℕ → ℕ) (bnd : ℕ → Bool) (A : ℕ) :
    ∀ r val, (∀ q, r ≤ q → q < k → coords q ≠ A) → (matchFrom key k coords bnd r val).2 A = val A := by
  refine upto_ind k (fun r hr => ?_) (fun r hr ih => ?_)
  · intro val _; rw [matchFrom, dif_neg (by omega)]
  · intro val hne
    rw [matchFrom, dif_pos hr]
    simp only
    have hr' : coords r ≠ A := hne r le_rfl hr
    have hnext : ∀ q, r + 1 ≤ q → q < k → coords q ≠ A := fun q h1 h2 => hne q (by omega) h2
    split_ifs
    · rfl
    · exact ih val hnext
    · rw [ih _ hnext, Function.update_of_ne hr'.symm]

/-- On success every remaining position's coordinate holds the key's value. -/
theorem matchFrom_installs (key k : ℕ) (coords : ℕ → ℕ) (bnd : ℕ → Bool)
    (inj : ∀ p q, p < k → q < k → coords p = coords q → p = q) :
    ∀ p val, (matchFrom key k coords bnd p val).1 = true →
      ∀ q, p ≤ q → q < k → (matchFrom key k coords bnd p val).2 (coords q) = field k q key := by
  refine upto_ind k (fun p hp => ?_) (fun p hp ih => ?_)
  · intro val _ q hq hqk; omega
  · intro val hok q hpq hqk
    rw [matchFrom, dif_pos hp] at hok ⊢
    simp only at hok ⊢
    have later : ∀ r, p + 1 ≤ r → r < k → coords r ≠ coords p :=
      fun r h1 h2 h => by have := inj r p h2 hp h; omega
    by_cases hb : bnd (coords p) = true
    · rw [if_pos hb] at hok ⊢
      by_cases hne : val (coords p) ≠ field k p key
      · rw [if_pos hne] at hok; simp at hok
      · rw [if_neg hne] at hok ⊢
        rcases Nat.eq_or_lt_of_le hpq with rfl | hlt
        · rw [matchFrom_bound key k coords bnd (p + 1) val _ hb]; exact not_not.mp hne
        · exact ih val hok q hlt hqk
    · rw [if_neg hb] at hok ⊢
      rcases Nat.eq_or_lt_of_le hpq with rfl | hlt
      · rw [matchFrom_keep key k coords bnd _ (p + 1) _ later, Function.update_self]
      · exact ih _ hok q hlt hqk

theorem pack_congr (k : ℕ) (f g : ℕ → ℕ) (h : ∀ p, p < k → f p = g p) : pack k f = pack k g := by
  unfold pack
  have : ∀ (l : List ℕ) (acc : ℕ), (∀ p ∈ l, f p = g p) →
      l.foldl (fun want p => want * 2 ^ 16 + f p) acc = l.foldl (fun want p => want * 2 ^ 16 + g p) acc := by
    intro l
    induction l with
    | nil => intros; rfl
    | cons x l ih => intro acc hl; simp only [List.foldl_cons]; rw [hl x (by simp)]; exact ih _ (fun p hp => hl p (by simp [hp]))
  exact this _ 0 (fun p hp => h p (List.mem_range.mp hp))

theorem firstBound_spec (k : ℕ) (coords : ℕ → ℕ) (bnd : ℕ → Bool) :
    ∀ p q, firstBound k coords bnd p = some q → q < k ∧ bnd (coords q) = true := by
  refine upto_ind k (fun p hp => ?_) (fun p hp ih => ?_)
  · intro q h; rw [firstBound, dif_neg (by omega)] at h; simp at h
  · intro q h
    rw [firstBound, dif_pos hp] at h
    split_ifs at h with hb
    · simp at h; subst h; exact ⟨hp, hb⟩
    · exact ih q h

/-- Plan facts checked by `validate_gjoin_plan`. -/
structure PlanOK (E : Env) : Prop where
  bound_succ : ∀ j A, E.bound (j + 1) A = true ↔
    (E.bound j A = true ∨ ∃ p, p < E.k (E.slot j) ∧ E.coord (E.slot j) p = A)
  lookup_bound : ∀ j, E.lookup j = true → ∀ p, p < E.k (E.slot j) → E.bound j (E.coord (E.slot j) p) = true
  inj : ∀ s p q, p < E.k s → q < E.k s → E.coord s p = E.coord s q → p = q

theorem bound_mono {E : Env} (hP : PlanOK E) : ∀ i j A, i ≤ j → E.bound i A = true → E.bound j A = true := by
  intro i j A hij hA
  induction j, hij using Nat.le_induction with
  | base => exact hA
  | succ j _ ih => exact (hP.bound_succ j A).mpr (Or.inl ih)

theorem beginLevel_congr {E : Env} (hP : PlanOK E) (j : ℕ) (val₁ val₂ : ℕ → ℕ)
    (hv : ∀ A, E.bound j A = true → val₁ A = val₂ A) : beginLevel E j val₁ = beginLevel E j val₂ := by
  unfold beginLevel
  simp only
  by_cases hl : E.lookup j = true
  · rw [if_pos hl, if_pos hl,
      pack_congr _ _ _ (fun p hp => hv _ (hP.lookup_bound j hl p hp))]
  · rw [if_neg hl, if_neg hl]
    cases hf : firstBound (E.k (E.slot j)) (E.coord (E.slot j)) (E.bound j) 0 with
    | none => rfl
    | some p =>
      have := firstBound_spec _ _ _ 0 p hf
      simp only [hv _ this.2]

theorem restrict_matchKey {E : Env} (hP : PlanOK E) (d kk : ℕ) (val : ℕ → ℕ)
    (hok : (matchKey (E.key (E.slot d) kk) (E.k (E.slot d)) (E.coord (E.slot d)) (E.bound d) val).1 = true) :
    restrict (E.bound (d + 1))
        (matchKey (E.key (E.slot d) kk) (E.k (E.slot d)) (E.coord (E.slot d)) (E.bound d) val).2 =
      restrict (E.bound (d + 1))
        (matchKey (E.key (E.slot d) kk) (E.k (E.slot d)) (E.coord (E.slot d)) (E.bound d)
          (restrict (E.bound d) val)).2 := by
  have hok' : (matchKey (E.key (E.slot d) kk) (E.k (E.slot d)) (E.coord (E.slot d)) (E.bound d)
      (restrict (E.bound d) val)).1 = true := by
    rw [← hok]; unfold matchKey
    exact (matchFrom_ok_congr _ _ _ _ 0 _ _ (fun A hA => by simp [restrict, hA]))
  generalize hv0 : restrict (E.bound d) val = v0 at hok' ⊢
  funext A
  by_cases hA : E.bound (d + 1) A = true
  · simp only [restrict, hA, if_true]
    rcases (hP.bound_succ d A).mp hA with hb | ⟨p, hp, rfl⟩
    · unfold matchKey
      rw [matchFrom_bound _ _ _ _ 0 _ _ hb, matchFrom_bound _ _ _ _ 0 _ _ hb, ← hv0]
      simp [restrict, hb]
    · unfold matchKey at hok hok' ⊢
      rw [matchFrom_installs _ _ _ _ (hP.inj _) 0 _ hok p (Nat.zero_le _) hp,
        matchFrom_installs _ _ _ _ (hP.inj _) 0 _ hok' p (Nat.zero_le _) hp]
  · simp [restrict, hA]


/-! ## One source iteration is one machine step -/

theorem decode_succ (E : Env) (st : CState) (d : ℕ) (h : st.active = d + 1) :
    decode E st = frameOf E st d :: ((List.range d).reverse).map (frameOf E st) := by
  simp [decode, h, List.range_succ]

theorem frameOf_congr (E : Env) (st st' : CState) (j : ℕ) (hit : st'.it j = st.it j) (hen : st'.en j = st.en j)
    (hps : st'.ps j = st.ps j) (hch : ∀ i, i < j → st'.chosen i = st.chosen i)
    (hval : ∀ A, E.bound j A = true → st'.val A = st.val A) : frameOf E st' j = frameOf E st j := by
  unfold frameOf
  have hr : restrict (E.bound j) st'.val = restrict (E.bound j) st.val := by
    funext A; unfold restrict; split_ifs with hA
    · exact hval A hA
    · rfl
  have hc : (List.range j).map st'.chosen = (List.range j).map st.chosen :=
    List.map_congr_left (fun i hi => hch i (List.mem_range.mp hi))
  rw [hit, hen, hps, hr, hc]

theorem frames_congr (E : Env) (st st' : CState) (d : ℕ)
    (h : ∀ j, j < d → frameOf E st' j = frameOf E st j) :
    ((List.range d).reverse).map (frameOf E st') = ((List.range d).reverse).map (frameOf E st) :=
  List.map_congr_left (fun j hj => h j (List.mem_range.mp (List.mem_reverse.mp hj)))

theorem range'_cons (a n : ℕ) (h : 0 < n) : List.range' a n = a :: List.range' (a + 1) (n - 1) := by
  obtain ⟨m, rfl⟩ : ∃ m, n = m + 1 := ⟨n - 1, by omega⟩
  simp [List.range'_succ]

theorem cstep_refines {E : Env} (hP : PlanOK E) (st : CState) (hR : st.active ≤ E.R) :
    (cstep E st).1 = (step (candA E) (extendA E) (decode E st)).1 ∧
      decode E (cstep E st).2 = (step (candA E) (extendA E) (decode E st)).2 ∧
      (cstep E st).2.active ≤ E.R := by
  rcases st with ⟨active, it, en, ps, chosen, val⟩
  cases active with
  | zero => simp [cstep, decode, step]
  | succ d =>
    rw [decode_succ E _ d rfl]
    simp only at hR
    let s := E.slot d
    let kI := keyIndex E d (ps d)
    by_cases hlt : it d < en d
    · -- try the candidate at position it d
      set kk := kI (it d) with hkk
      set m := matchKey (E.key s kk) (E.k s) (E.coord s) (E.bound d) val with hm
      have hpend : (frameOf E ⟨d + 1, it, en, ps, chosen, val⟩ d).pending =
          kk :: (List.range' (it d + 1) (en d - (it d + 1))).map kI := by
        simp only [frameOf]
        rw [range'_cons _ _ (by omega)]
        simp only [List.map_cons, hkk, kI, Nat.sub_sub]
      have hm1 : (matchKey (E.key s kk) (E.k s) (E.coord s) (E.bound d) (restrict (E.bound d) val)).1 = m.1 := by
        unfold matchKey
        exact matchFrom_ok_congr _ _ _ _ 0 _ _ (fun A hA => by simp [restrict, hA])
      have hkeep : ∀ j, j ≤ d → ∀ A, E.bound j A = true → m.2 A = val A := fun j hj A hA =>
        matchFrom_bound _ _ _ _ 0 _ A (bound_mono hP j d A hj hA)
      have hrest : ∀ (st' : CState), (∀ j, j < d → st'.it j = it j) → (∀ j, j < d → st'.en j = en j) →
          (∀ j, j < d → st'.ps j = ps j) → (∀ i, i < d → st'.chosen i = chosen i) → st'.val = m.2 →
          ((List.range d).reverse).map (frameOf E st') =
            ((List.range d).reverse).map (frameOf E ⟨d + 1, it, en, ps, chosen, val⟩) := by
        intro st' h1 h2 h3 h4 h5
        apply frames_congr
        intro j hj
        exact frameOf_congr E _ _ j (h1 j hj) (h2 j hj) (h3 j hj) (fun i hi => h4 i (by omega))
          (fun A hA => by rw [h5]; exact hkeep j (by omega) A hA)
      have hstate : restrict (E.bound d) m.2 = restrict (E.bound d) val := by
        funext A; unfold restrict; split_ifs with hA
        · exact hkeep d le_rfl A hA
        · rfl
      have hframe : (frameOf E ⟨d + 1, it, en, ps, chosen, val⟩ d) =
          ⟨d, restrict (E.bound d) val, kk :: (List.range' (it d + 1) (en d - (it d + 1))).map kI,
           (List.range d).map chosen, E.R - 1 - d⟩ := by
        rw [← hpend]; rfl
      by_cases hok : m.1 = true
      · have hext : extendA E d (restrict (E.bound d) val) kk = some (restrict (E.bound (d + 1)) m.2) := by
          simp only [extendA]
          rw [show (matchKey (E.key (E.slot d) kk) (E.k (E.slot d)) (E.coord (E.slot d)) (E.bound d)
              (restrict (E.bound d) val)).1 = true from hm1.trans hok, if_pos rfl]
          rw [restrict_matchKey hP d kk val hok]
        by_cases hnext : d + 1 < E.R
        · set lv := beginLevel E (d + 1) m.2 with hlv
          have hc : cstep E ⟨d + 1, it, en, ps, chosen, val⟩ =
              ([], ⟨d + 2, Function.update (Function.update it d (it d + 1)) (d + 1) lv.1,
                Function.update en (d + 1) lv.2.1, Function.update ps (d + 1) lv.2.2,
                Function.update chosen d kk, m.2⟩) := by
            simp only [cstep, if_pos hlt]
            simp only [← hkk, ← hm, s, kI, if_pos hok, if_pos hnext, ← hlv]
          have hbelow : E.R - 1 - d = (E.R - 2 - d) + 1 := by omega
          have hcand : candA E (d + 1) (restrict (E.bound (d + 1)) m.2) =
              (List.range' lv.1 (lv.2.1 - lv.1)).map (keyIndex E (d + 1) lv.2.2) := by
            simp only [candA]
            rw [beginLevel_congr hP (d + 1) (restrict (E.bound (d + 1)) m.2) m.2
              (fun A hA => by simp [restrict, hA]), ← hlv]
          rw [hc]
          refine ⟨?_, ?_, by simp; omega⟩
          · rw [hframe]; simp [step, hext, hbelow]
          · rw [hframe, decode_succ E _ (d + 1) rfl]
            simp only [List.range_succ, List.reverse_append, List.reverse_cons, List.reverse_nil,
              List.nil_append, List.cons_append, List.map_cons]
            simp only [step, hext, hbelow]
            refine congrArg₂ List.cons ?_ (congrArg₂ List.cons ?_ ?_)
            · simp only [frameOf, Function.update_self]
              have hch : (List.range (d + 1)).map (Function.update chosen d kk) = (List.range d).map chosen ++ [kk] := by
                rw [List.range_succ, List.map_append]
                simp only [List.map_cons, List.map_nil, Function.update_self]
                congr 1
                exact List.map_congr_left (fun i hi => Function.update_of_ne (by
                  have := List.mem_range.mp hi; omega) _ _)
              rw [hch, hcand]
              congr 1
              omega
            · simp only [frameOf, Function.update_of_ne (show d ≠ d + 1 by omega), Function.update_self, hstate]
              have hch : (List.range d).map (Function.update chosen d kk) = (List.range d).map chosen :=
                List.map_congr_left (fun i hi => Function.update_of_ne (by
                  have := List.mem_range.mp hi; omega) _ _)
              rw [hch, hbelow]
            · apply hrest
              · intro j hj; simp [Function.update_of_ne (show j ≠ d + 1 by omega), Function.update_of_ne (show j ≠ d by omega)]
              · intro j hj; simp [Function.update_of_ne (show j ≠ d + 1 by omega)]
              · intro j hj; simp [Function.update_of_ne (show j ≠ d + 1 by omega)]
              · intro i hi; simp [Function.update_of_ne (show i ≠ d by omega)]
              · rfl
        · -- last level: emit the leaf
          have hc : cstep E ⟨d + 1, it, en, ps, chosen, val⟩ =
              ([(List.range (d + 1)).map (Function.update chosen d kk)],
                ⟨d + 1, Function.update it d (it d + 1), en, ps, Function.update chosen d kk, m.2⟩) := by
            simp only [cstep, if_pos hlt]
            simp only [← hkk, ← hm, s, kI, if_pos hok, if_neg hnext]
          have hbelow : E.R - 1 - d = 0 := by omega
          have hch : (List.range d).map (Function.update chosen d kk) = (List.range d).map chosen :=
            List.map_congr_left (fun i hi => Function.update_of_ne (by
              have := List.mem_range.mp hi; omega) _ _)
          rw [hc]
          refine ⟨?_, ?_, by simpa using hR⟩
          · rw [hframe]
            simp only [step, hext, hbelow, List.range_succ, List.map_append, List.map_cons, List.map_nil,
              Function.update_self, hch]
          · rw [hframe, decode_succ E _ d rfl]
            simp only [step, hext, hbelow]
            refine congrArg₂ List.cons ?_ ?_
            · simp only [frameOf, Function.update_self, hstate, hch]
              rw [hbelow]
            · apply hrest
              · intro j hj; simp [Function.update_of_ne (show j ≠ d by omega)]
              · intro j _; rfl
              · intro j _; rfl
              · intro i hi; simp [Function.update_of_ne (show i ≠ d by omega)]
              · rfl
      · -- mismatch: stay at this level with the next candidate
        have hok' : m.1 = false := by simpa using hok
        have hext : extendA E d (restrict (E.bound d) val) kk = none := by
          simp only [extendA]
          rw [show (matchKey (E.key (E.slot d) kk) (E.k (E.slot d)) (E.coord (E.slot d)) (E.bound d)
              (restrict (E.bound d) val)).1 = false from hm1.trans hok']
          rfl
        have hc : cstep E ⟨d + 1, it, en, ps, chosen, val⟩ =
            ([], ⟨d + 1, Function.update it d (it d + 1), en, ps, chosen, m.2⟩) := by
          simp only [cstep, if_pos hlt]
          simp only [← hkk, ← hm, s, kI, hok', Bool.false_eq_true, if_false]
        rw [hc]
        refine ⟨?_, ?_, by simpa using hR⟩
        · rw [hframe]; simp [step, hext]
        · rw [hframe, decode_succ E _ d rfl]
          simp only [step, hext]
          refine congrArg₂ List.cons ?_ ?_
          · simp only [frameOf, Function.update_self, hstate]
            rfl
          · apply hrest
            · intro j hj; simp [Function.update_of_ne (show j ≠ d by omega)]
            · intro j _; rfl
            · intro j _; rfl
            · intro i _; rfl
            · rfl
    · -- range exhausted: pop
      have hpend : (frameOf E ⟨d + 1, it, en, ps, chosen, val⟩ d).pending = [] := by
        simp [frameOf, show en d - it d = 0 by omega]
      have hc : cstep E ⟨d + 1, it, en, ps, chosen, val⟩ = ([], ⟨d, it, en, ps, chosen, val⟩) := by
        simp [cstep, hlt]
      rw [hc]
      refine ⟨?_, ?_, by simp; omega⟩
      · simp [step, hpend]
      · simp only [step, hpend]
        simp only [decode, List.range_succ, List.reverse_append, List.reverse_cons, List.reverse_nil,
          List.nil_append, List.cons_append, List.map_cons]
        exact List.map_congr_left (fun a _ => rfl)


/-- The source loop emits what the machine emits from the decoded state. -/
theorem crun_refines {E : Env} (hP : PlanOK E) :
    ∀ fuel st, st.active ≤ E.R → crun E fuel st = run (candA E) (extendA E) fuel (decode E st) := by
  intro fuel
  induction fuel with
  | zero => intro st _; rfl
  | succ fuel ih =>
    intro st hR
    rcases st with ⟨active, it, en, ps, chosen, val⟩
    cases active with
    | zero => simp [crun, decode, run]
    | succ d =>
      obtain ⟨h1, h2, h3⟩ := cstep_refines hP ⟨d + 1, it, en, ps, chosen, val⟩ hR
      rw [decode_succ E ⟨d + 1, it, en, ps, chosen, val⟩ d rfl] at h1 h2 ⊢
      simp only [crun, run]
      rw [h1, ih _ h3, h2]

/-- The loop's start: `depth = 0` after `device_gjoin_begin_level(0, ...)`. -/
def initial (E : Env) (val : ℕ → ℕ) (it en : ℕ → ℕ) (ps : ℕ → Option ℕ) (chosen : ℕ → ℕ) : CState :=
  let lv := beginLevel E 0 val
  ⟨1, Function.update it 0 lv.1, Function.update en 0 lv.2.1, Function.update ps 0 lv.2.2, chosen, val⟩

/-- **The CUDA search loop is exact.** From its start, with enough iterations,
it emits precisely the recursive specification `leavesFrom` over `R` levels,
in order (whatever the initial array contents). -/
theorem crun_exact {E : Env} (hP : PlanOK E) (hR : 0 < E.R) (val : ℕ → ℕ) (it en : ℕ → ℕ)
    (ps : ℕ → Option ℕ) (chosen : ℕ → ℕ) (fuel : ℕ)
    (enough : stackWork (candA E) (extendA E) (decode E (initial E val it en ps chosen)) ≤ fuel) :
    crun E fuel (initial E val it en ps chosen) =
      leavesFrom (candA E) (extendA E) E.R 0 (restrict (E.bound 0) val) := by
  rw [crun_refines hP _ _ (by simp [initial]; omega), run_exact _ _ _ _ enough]
  have hd : decode E (initial E val it en ps chosen) =
      [⟨0, restrict (E.bound 0) val, candA E 0 (restrict (E.bound 0) val), [], E.R - 1⟩] := by
    simp only [decode, initial, List.range_one, List.reverse_singleton, List.map_cons, List.map_nil]
    simp only [frameOf, Function.update_self, List.range_zero, List.map_nil, candA]
    rw [beginLevel_congr hP 0 (restrict (E.bound 0) val) val (fun A hA => by simp [restrict, hA]), Nat.sub_zero]
  rw [hd]
  obtain ⟨r, hr⟩ : ∃ r, E.R = r + 1 := ⟨E.R - 1, by omega⟩
  simp [stackLeaves, frameLeaves, leavesFrom, hr]


/-! ## Narrowed ranges keep every compatible key once -/

/-- A successful match passed every bound coordinate's check. -/
theorem matchFrom_checks (key k : ℕ) (coords : ℕ → ℕ) (bnd : ℕ → Bool) :
    ∀ p val, (matchFrom key k coords bnd p val).1 = true →
      ∀ q, p ≤ q → q < k → bnd (coords q) = true → val (coords q) = field k q key := by
  refine upto_ind k (fun p hp => ?_) (fun p hp ih => ?_)
  · intro val _ q hq hqk; omega
  · intro val hok q hpq hqk hb
    rw [matchFrom, dif_pos hp] at hok
    simp only at hok
    rcases Nat.eq_or_lt_of_le hpq with rfl | hlt
    · rw [if_pos hb] at hok
      by_contra hne
      rw [if_pos hne] at hok; exact absurd hok (by simp)
    · by_cases hb' : bnd (coords p) = true
      · rw [if_pos hb'] at hok
        by_cases hne : val (coords p) ≠ field k p key
        · rw [if_pos hne] at hok; exact absurd hok (by simp)
        · rw [if_neg hne] at hok; exact ih val hok q hlt hqk hb
      · rw [if_neg hb'] at hok
        have := ih _ hok q hlt hqk hb
        rwa [Function.update_of_ne] at this
        intro h; rw [h] at hb; exact hb' hb

/-- With every coordinate bound the match only checks, and succeeds exactly
when all fields agree. -/
theorem matchFrom_allbound (key k : ℕ) (coords : ℕ → ℕ) (bnd : ℕ → Bool)
    (hall : ∀ q, q < k → bnd (coords q) = true) :
    ∀ p val, (matchFrom key k coords bnd p val).1 = true ↔ ∀ q, p ≤ q → q < k → val (coords q) = field k q key := by
  refine upto_ind k (fun p hp => ?_) (fun p hp ih => ?_)
  · intro val; rw [matchFrom, dif_neg (by omega)]; simp; intro q h1 h2; omega
  · intro val
    rw [matchFrom, dif_pos hp]
    simp only [if_pos (hall p hp)]
    constructor
    · intro h q hpq hqk
      by_cases hne : val (coords p) ≠ field k p key
      · rw [if_pos hne] at h; exact absurd h (by simp)
      · rw [if_neg hne] at h
        rcases Nat.eq_or_lt_of_le hpq with rfl | hlt
        · exact not_not.mp hne
        · exact (ih val).mp h q hlt hqk
    · intro h
      rw [if_neg (by simpa using h p le_rfl hp)]
      exact (ih val).mpr (fun q h1 h2 => h q (by omega) h2)

theorem field_succ (k p key : ℕ) (hp : p < k) : field (k + 1) p key = field k p (key / 2 ^ 16) := by
  unfold field
  rw [Nat.div_div_eq_div_mul, ← pow_add]
  congr 3
  omega

theorem field_last (k key : ℕ) : field (k + 1) k key = key % 2 ^ 16 := by
  simp [field]

/-- A key below `2^(16k)` is the packing of its own fields. -/
theorem pack_fields : ∀ (k key : ℕ), key < 2 ^ (16 * k) → pack k (fun p => field k p key) = key := by
  intro k
  induction k with
  | zero => intro key h; simp at h; simp [pack, h]
  | succ k ih =>
    intro key h
    have hsplit : pack (k + 1) (fun p => field (k + 1) p key) =
        pack k (fun p => field (k + 1) p key) * 2 ^ 16 + field (k + 1) k key := by
      simp [pack, List.range_succ, List.foldl_append]
    rw [hsplit, pack_congr k _ (fun p => field k p (key / 2 ^ 16)) (fun p hp => field_succ k p key hp),
      ih (key / 2 ^ 16) (by rw [Nat.div_lt_iff_lt_mul (by positivity), ← pow_add]; rwa [show 16 * k + 16 = 16 * (k + 1) by ring]),
      field_last]
    exact Nat.div_add_mod' key (2 ^ 16)

/-- Table facts checked by `validate_gjoin_fibres` and `validate_gjoin_index`
for the entries a candidate uses. -/
structure TablesOK (E : Env) : Prop where
  ordered : ∀ s, E.kb s ≤ E.ke s
  strict : ∀ s i i', E.kb s ≤ i → i < i' → i' < E.ke s → E.key s i < E.key s i'
  width : ∀ s i, E.kb s ≤ i → i < E.ke s → E.key s i < 2 ^ (16 * E.k s)
  index_range : ∀ s p pos, 0 < p → p < E.k s → E.kb s ≤ pos → pos < E.ke s →
    E.kb s ≤ E.pidx s p pos ∧ E.pidx s p pos < E.ke s
  index_inj : ∀ s p pos pos', 0 < p → p < E.k s → E.kb s ≤ pos → pos < E.ke s → E.kb s ≤ pos' →
    pos' < E.ke s → E.pidx s p pos = E.pidx s p pos' → pos = pos'
  index_sorted : ∀ s p pos pos', 0 < p → p < E.k s → E.kb s ≤ pos → pos ≤ pos' → pos' < E.ke s →
    E.pkey s p pos ≤ E.pkey s p pos'
  index_leading : ∀ s p pos, 0 < p → p < E.k s → E.kb s ≤ pos → pos < E.ke s →
    E.pkey s p pos / 2 ^ (16 * (E.k s - 1)) = field (E.k s) p (E.key s (E.pidx s p pos))

/-- The full row of level `j`: the canonical keys of its entry. -/
def row (E : Env) (j : ℕ) : List ℕ := List.range' (E.kb (E.slot j)) (E.ke (E.slot j) - E.kb (E.slot j))

theorem compatible_checks {E : Env} (j : ℕ) (state : ℕ → ℕ) (kk : ℕ)
    (h : compatible (extendA E) j state kk = true) :
    ∀ q, q < E.k (E.slot j) → E.bound j (E.coord (E.slot j) q) = true →
      state (E.coord (E.slot j) q) = field (E.k (E.slot j)) q (E.key (E.slot j) kk) := by
  intro q hq hb
  simp only [compatible, extendA] at h
  split_ifs at h with hm
  · exact matchFrom_checks _ _ _ _ 0 state hm q (Nat.zero_le _) hq hb
  · simp at h

theorem filter_range'_window (f : ℕ → Bool) (b e lo hi : ℕ) (hb : b ≤ lo) (hl : lo ≤ hi) (he : hi ≤ e)
    (outside : ∀ i, b ≤ i → i < e → f i = true → lo ≤ i ∧ i < hi) :
    (List.range' lo (hi - lo)).filter f = (List.range' b (e - b)).filter f := by
  have hsplit : List.range' b (e - b) = List.range' b (lo - b) ++ List.range' lo (hi - lo) ++ List.range' hi (e - hi) := by
    have e1 : List.range' b (lo - b) ++ List.range' lo (hi - lo) = List.range' b (hi - b) := by
      have := @List.range'_append_1 b (lo - b) (hi - lo)
      rwa [show b + (lo - b) = lo by omega, show lo - b + (hi - lo) = hi - b by omega] at this
    have e2 : List.range' b (hi - b) ++ List.range' hi (e - hi) = List.range' b (e - b) := by
      have := @List.range'_append_1 b (hi - b) (e - hi)
      rwa [show b + (hi - b) = hi by omega, show hi - b + (e - hi) = e - b by omega] at this
    rw [e1, e2]
  rw [hsplit, List.filter_append, List.filter_append]
  have h1 : (List.range' b (lo - b)).filter f = [] := by
    rw [List.filter_eq_nil_iff]
    intro i hi' hf
    rw [List.mem_range'_1] at hi'
    have := outside i (by omega) (by omega) hf
    omega
  have h2 : (List.range' hi (e - hi)).filter f = [] := by
    rw [List.filter_eq_nil_iff]
    intro i hi' hf
    rw [List.mem_range'_1] at hi'
    have := outside i (by omega) (by omega) hf
    omega
  rw [h1, h2]; simp


theorem keyIndex_none (E : Env) (j : ℕ) : keyIndex E j none = id := rfl
theorem keyIndex_zero (E : Env) (j : ℕ) : keyIndex E j (some 0) = id := by funext pos; simp [keyIndex]
theorem keyIndex_pos (E : Env) (j p : ℕ) (h : 0 < p) : keyIndex E j (some p) = E.pidx (E.slot j) p := by
  funext pos; simp [keyIndex, h]

theorem compatible_eq (E : Env) (j : ℕ) (state : ℕ → ℕ) (kk : ℕ) :
    compatible (extendA E) j state kk =
      (matchKey (E.key (E.slot j) kk) (E.k (E.slot j)) (E.coord (E.slot j)) (E.bound j) state).1 := by
  simp only [compatible, extendA]
  split_ifs with h <;> simp [h]

theorem filter_single {l : List ℕ} (hl : l.Nodup) (i : ℕ) (hi : i ∈ l) (f : ℕ → Bool)
    (h : ∀ x ∈ l, f x = true → x = i) : l.filter f = [i].filter f := by
  induction l with
  | nil => simp at hi
  | cons x l ih =>
    rcases List.nodup_cons.mp hl with ⟨hx, hl'⟩
    by_cases hxi : x = i
    · subst hxi
      have : l.filter f = [] := List.filter_eq_nil_iff.mpr (fun y hy hf => hx (h y (by simp [hy]) hf ▸ hy))
      by_cases hf : f x = true <;> simp [hf, this]
    · have hi' : i ∈ l := by simpa [Ne.symm hxi] using hi
      have hfx : f x = false := by
        by_contra hc; exact hxi (h x (by simp) (by simpa using hc))
      simp only [List.filter_cons, hfx, Bool.false_eq_true, if_false]
      rw [ih hl' hi' (fun y hy hf => h y (by simp [hy]) hf)]
      cases hfi : f i <;> simp [hfi]

theorem map_perm_range' (f : ℕ → ℕ) (b n : ℕ) (hrange : ∀ x, b ≤ x → x < b + n → b ≤ f x ∧ f x < b + n)
    (hinj : ∀ x y, b ≤ x → x < b + n → b ≤ y → y < b + n → f x = f y → x = y) :
    ((List.range' b n).map f).Perm (List.range' b n) := by
  have hnodup : ((List.range' b n).map f).Nodup := by
    apply List.Nodup.map_on _ List.nodup_range'
    intro x hx y hy hxy
    rw [List.mem_range'_1] at hx hy
    exact hinj x y hx.1 hx.2 hy.1 hy.2 hxy
  have hsub : (List.range' b n).map f ⊆ List.range' b n := by
    intro y hy
    obtain ⟨x, hx, rfl⟩ := List.mem_map.mp hy
    rw [List.mem_range'_1] at hx ⊢
    exact hrange x hx.1 hx.2
  exact (List.subperm_of_subset hnodup hsub).perm_of_length_le (by simp)

/-- **Every narrowed level keeps exactly the compatible keys of its full row.** -/
theorem candA_narrowed {E : Env} (hP : PlanOK E) (hT : TablesOK E) (j : ℕ) (state : ℕ → ℕ) :
    ((candA E j state).filter (compatible (extendA E) j state)).Perm
      ((row E j).filter (compatible (extendA E) j state)) := by
  set s := E.slot j with hs
  set k := E.k s with hk
  set b := E.kb s with hb
  set e := E.ke s with he
  set comp := compatible (extendA E) j state with hcomp
  have hbe : b ≤ e := hT.ordered s
  have sorted : ∀ i i', b ≤ i → i ≤ i' → i' < e → E.key s i ≤ E.key s i' := by
    intro i i' h1 h2 h3
    rcases Nat.eq_or_lt_of_le h2 with rfl | hlt
    · exact le_rfl
    · exact Nat.le_of_lt (hT.strict s i i' h1 hlt h3)
  have checks := compatible_checks (E := E) j state
  have hrow : row E j = List.range' b (e - b) := rfl
  unfold candA beginLevel
  simp only [← hs, ← hk, ← hb, ← he]
  by_cases hl : E.lookup j = true
  · -- lookup: the one key whose fields are the bound values
    rw [if_pos hl]
    simp only [keyIndex_none, List.map_id]
    set want := pack k (fun p => state (E.coord s p)) with hwant
    have hall : ∀ q, q < k → E.bound j (E.coord s q) = true := hP.lookup_bound j hl
    have comp_key : ∀ i, b ≤ i → i < e → comp i = true → E.key s i = want := by
      intro i h1 h2 hc
      rw [hwant, ← pack_fields k (E.key s i) (hT.width s i h1 h2)]
      exact (pack_congr k _ _ (fun q hq => (checks i hc q hq (hall q hq)).symm))
    have hsearch := search_lookup_exact (E.key s) b e want hbe sorted
    simp only at hsearch
    set lo := searchLower (fun i => E.key s i < want) (e - b) b e with hlo
    by_cases hex : ∃ i, b ≤ i ∧ i < e ∧ E.key s i = want
    · obtain ⟨i, hi1, hi2, hik⟩ := hex
      have hloi : lo = i := search_lookup_unique (E.key s) b e want hbe
        (fun a c h1 h2 h3 => hT.strict s a c h1 h2 h3) i hi1 hi2 hik
      have hfound : lo < e ∧ E.key s lo = want := by rw [hloi]; exact ⟨hi2, hik⟩
      have honly : ∀ x ∈ List.range' b (e - b), comp x = true → x = i := by
        intro x hx hc
        rw [List.mem_range'_1] at hx
        have := comp_key x hx.1 (by omega) hc
        by_contra hne
        rcases lt_or_gt_of_ne hne with h | h
        · have := hT.strict s x i hx.1 h hi2; omega
        · have := hT.strict s i x hi1 h (by omega); omega
      rw [if_pos hfound, hloi, show i + 1 - i = 1 by omega, hrow,
        filter_single List.nodup_range' i (List.mem_range'_1.mpr ⟨hi1, by omega⟩) comp honly]
      simp
    · have hnf : ¬(lo < e ∧ E.key s lo = want) := fun h => hex (hsearch.mp h)
      have hnil : (List.range' b (e - b)).filter comp = [] := by
        apply List.filter_eq_nil_iff.mpr
        intro x hx hc
        rw [List.mem_range'_1] at hx
        exact hex ⟨x, hx.1, by omega, comp_key x hx.1 (by omega) hc⟩
      rw [if_neg hnf, Nat.sub_self, hrow, hnil]
      simp
  · rw [if_neg hl]
    cases hf : firstBound k (E.coord s) (E.bound j) 0 with
    | none => simp only [keyIndex_none, List.map_id]; rw [hrow]
    | some p =>
      obtain ⟨hpk, hpb⟩ := firstBound_spec _ _ _ 0 p hf
      simp only
      set v := state (E.coord s p) with hv
      set sh := 16 * (k - 1) with hsh
      have hradix : 0 < 2 ^ sh := by positivity
      by_cases hp0 : p = 0
      · subst hp0
        rw [if_pos rfl]
        simp only [keyIndex_zero, List.map_id]
        obtain ⟨h1, h2, h3, hwin⟩ := packed_range_exact (E.key s) b e (2 ^ sh) v hbe hradix sorted
        rw [hrow]
        apply List.Perm.of_eq
        apply filter_range'_window _ _ _ _ _ h1 h2 h3
        intro i hi1 hi2 hc
        apply (hwin i hi1 hi2).mpr
        have hfield := checks i hc 0 hpk hpb
        have hw := hT.width s i hi1 hi2
        have hlt : E.key s i / 2 ^ sh < 2 ^ 16 := by
          rw [Nat.div_lt_iff_lt_mul hradix, ← pow_add]
          calc E.key s i < 2 ^ (16 * k) := hw
            _ = 2 ^ (16 + sh) := by congr 1; omega
        rw [hv, hfield]
        simp only [field, Nat.sub_zero]
        rw [Nat.mod_eq_of_lt (by simpa [hsh] using hlt)]
      · rw [if_neg hp0]
        have hp_pos : 0 < p := Nat.pos_of_ne_zero hp0
        rw [keyIndex_pos E j p hp_pos]
        have psorted : ∀ i i', b ≤ i → i ≤ i' → i' < e → E.pkey s p i ≤ E.pkey s p i' :=
          fun i i' h1 h2 h3 => hT.index_sorted s p i i' hp_pos hpk h1 h2 h3
        obtain ⟨h1, h2, h3, hwin⟩ := packed_range_exact (E.pkey s p) b e (2 ^ sh) v hbe hradix psorted
        set lo := searchLower (fun i => E.pkey s p i < v * 2 ^ sh) (e - b) b e
        set hi := searchLower (fun i => E.pkey s p i / 2 ^ sh ≤ v) (e - lo) lo e
        rw [List.filter_map]
        have hwindow : (List.range' lo (hi - lo)).filter (comp ∘ E.pidx s p) =
            (List.range' b (e - b)).filter (comp ∘ E.pidx s p) := by
          apply filter_range'_window _ _ _ _ _ h1 h2 h3
          intro pos hp1 hp2 hc
          apply (hwin pos hp1 hp2).mpr
          have hr := hT.index_range s p pos hp_pos hpk hp1 hp2
          rw [hT.index_leading s p pos hp_pos hpk hp1 hp2, hv]
          exact (checks _ hc p hpk hpb).symm
        rw [hwindow, ← List.filter_map, hrow]
        apply List.Perm.filter
        apply map_perm_range' _ b (e - b)
        · intro x h1 h2
          have ho := hT.ordered s
          simp only [hb, he] at h1 h2 ⊢
          have := hT.index_range s p x hp_pos hpk h1 (by omega)
          simp only [← hs] at this ⊢
          omega
        · intro x y hx1 hx2 hy1 hy2 hxy
          have ho := hT.ordered s
          simp only [hb, he] at hx1 hx2 hy1 hy2
          simp only [← hs] at hxy
          exact hT.index_inj s p x y hp_pos hpk hx1 (by omega) hy1 (by omega) hxy


/-- **The CUDA join search is complete and duplicate-free.** With the plan
and table facts the builders validate, the source loop's leaves are a
permutation of every compatible tuple of canonical keys, one per row. -/
theorem crun_perm_full {E : Env} (hP : PlanOK E) (hT : TablesOK E) (hR : 0 < E.R) (val : ℕ → ℕ)
    (it en : ℕ → ℕ) (ps : ℕ → Option ℕ) (chosen : ℕ → ℕ) (fuel : ℕ)
    (enough : stackWork (candA E) (extendA E) (decode E (initial E val it en ps chosen)) ≤ fuel) :
    (crun E fuel (initial E val it en ps chosen)).Perm
      (fullLeaves (row E) (extendA E) E.R 0 (restrict (E.bound 0) val)) := by
  rw [crun_exact hP hR val it en ps chosen fuel enough]
  exact leavesFrom_perm_full _ _ _ (candA_narrowed hP hT) _ _ _

end Refpoly.JoinLoopSource
