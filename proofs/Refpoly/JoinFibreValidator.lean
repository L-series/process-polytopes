import Refpoly.JoinRowCount
import Refpoly.JoinLeafState

/-! A source-pinned model of `validate_gjoin_fibres` in `src/gpu/join.cuh`.

The source consumes one contiguous chunk per canonical fibre, with exactly
`g.count` points, and a final cursor equality. `chunks` below is the direct
`take`/`drop` transcription of that scan. The source checks adjacent array
order; `sourceAdjacent` states that same chain, while `adjacent_pairwise`
derives global distinctness using transitivity of lexicographic order. The
builder's `std::array<int, 5> x{}` establishes the fixed zero suffix recorded
by `pointShape`; the source pin script also checks that call-site fact.

Naturals model successful nonnegative signed-int fields. `rowScan` keeps the
source's sequential `remaining / weight` guard, rather than assuming only the
final dot product. The uint64 row-count DP is already modeled in
`JoinRowCount.lean`; the validator's final equality uses that exact count.
-/
namespace Refpoly.JoinFibreValidator

open Refpoly.JoinLoopSource Refpoly.JoinRejection

abbrev Point := List ℕ

structure Entry where
  weights : List ℕ
  degree : ℕ

structure Fibre where
  key : ℕ
  count : ℕ
  cats : ℕ
  pairs : ℕ
  deriving DecidableEq

def sourceLex (a b : Point) : Prop := List.Lex Nat.lt a b

/-- The source's adjacent strict-order check on `points[cursor - 1]` and
`points[cursor]`. -/
def Adjacent {α : Type} (r : α → α → Prop) : List α → Prop
  | [] => True
  | [_] => True
  | a :: b :: rest => r a b ∧ Adjacent r (b :: rest)

def sourceAdjacent (points : List Point) : Prop := Adjacent sourceLex points

theorem sourceLex_trans {a b c : Point} (hab : sourceLex a b) (hbc : sourceLex b c) :
    sourceLex a c := by
  induction hab generalizing c with
  | nil =>
      cases hbc with
      | rel _ => exact .nil
      | cons _ => exact .nil
  | rel h =>
      cases hbc with
      | rel hbc => exact .rel (Nat.lt_trans h hbc)
      | cons _ => exact .rel h
  | cons h ih =>
      cases hbc with
      | rel hbc => exact .rel hbc
      | cons hbc => exact .cons (ih hbc)

theorem sourceLex_irrefl (a : Point) : ¬ sourceLex a a := by
  intro h
  have hlt : a < a := List.lex_lt.mp h
  exact (lt_irrefl a) hlt

theorem adjacent_pairwise (xs : List Point) (h : sourceAdjacent xs) :
    List.Pairwise sourceLex xs := by
  induction xs with
  | nil => simp [sourceAdjacent, Adjacent]
  | cons a xs ih =>
      cases xs with
      | nil => simp [sourceAdjacent, Adjacent]
      | cons b xs =>
          simp only [sourceAdjacent, Adjacent] at h
          rcases h with ⟨hab, htail⟩
          rw [List.pairwise_cons]
          refine ⟨?_, ih htail⟩
          intro z hz
          rcases List.mem_cons.mp hz with rfl | hz
          · exact hab
          · have htailPair := ih htail
            have hbz : sourceLex b z := (List.pairwise_cons.mp htailPair).1 z hz
            exact sourceLex_trans hab hbz

theorem adjacent_pairwise_trans {α : Type} {r : α → α → Prop}
    (htrans : ∀ a b c, r a b → r b c → r a c) (xs : List α)
    (h : Adjacent r xs) : List.Pairwise r xs := by
  induction xs with
  | nil => simp [Adjacent]
  | cons a xs ih =>
      cases xs with
      | nil => simp [Adjacent]
      | cons b xs =>
          simp only [Adjacent] at h
          rcases h with ⟨hab, htail⟩
          rw [List.pairwise_cons]
          refine ⟨?_, ih htail⟩
          intro z hz
          rcases List.mem_cons.mp hz with rfl | hz
          · exact hab
          · have htailPair := ih htail
            have hbz : r b z := (List.pairwise_cons.mp htailPair).1 z hz
            exact htrans _ _ _ hab hbz

theorem adjacent_keys_nodup (fibres : List Fibre)
    (h : Adjacent (fun a b : Fibre => a.key < b.key) fibres) : fibres.Nodup := by
  apply List.nodup_iff_pairwise_ne.mpr
  apply List.Pairwise.imp (R := fun a b : Fibre => a.key < b.key)
  · intro a b hab heq
    cases heq
    exact Nat.lt_irrefl _ hab
  · exact adjacent_pairwise_trans
      (fun a b c hab hbc => show a.key < c.key from Nat.lt_trans hab hbc) fibres h

theorem equal_key_mem_eq (fibres : List Fibre)
    (h : Adjacent (fun a b : Fibre => a.key < b.key) fibres)
    {a b : Fibre} (ha : a ∈ fibres) (hb : b ∈ fibres) (hab : a.key = b.key) : a = b := by
  have hp := adjacent_pairwise_trans
    (fun a b c hab hbc => show a.key < c.key from Nat.lt_trans hab hbc) fibres h
  obtain ⟨i, hi⟩ := List.mem_iff_get.mp ha
  obtain ⟨j, hj⟩ := List.mem_iff_get.mp hb
  by_cases hij : i = j
  · subst j
    calc
      a = fibres.get i := hi.symm
      _ = b := hj
  · rcases lt_or_gt_of_ne hij with hij' | hji
    · have hlt := (List.pairwise_iff_get.mp hp) i j hij'
      rw [hi, hj] at hlt
      omega
    · have hlt := (List.pairwise_iff_get.mp hp) j i hji
      rw [hj, hi] at hlt
      omega

theorem pairProjection_injective {α β : Type} {chunks : List (α × β)}
    (h : (chunks.map Prod.fst).Nodup) :
    ∀ z ∈ chunks, ∀ z' ∈ chunks, z.1 = z'.1 → z = z' := by
  have hn : chunks.Nodup := List.Nodup.of_map Prod.fst h
  have hinj := (List.nodup_map_iff_inj_on hn).mp h
  exact hinj

theorem sourceAdjacent_nodup (xs : List Point) (h : sourceAdjacent xs) : xs.Nodup := by
  rw [List.nodup_iff_pairwise_ne]
  apply List.Pairwise.imp (R := sourceLex)
  · intro a b hab heq
    subst b
    exact sourceLex_irrefl a hab
  · exact adjacent_pairwise xs h

def rowScan : List ℕ → List ℕ → ℕ → Option ℕ
  | [], [], remaining => some remaining
  | w :: ws, x :: xs, remaining =>
      if 0 < w ∧ x ≤ remaining / w then
        rowScan ws xs (remaining - x * w)
      else none
  | _, _, _ => none

theorem rowScan_sound (weights values : List ℕ) (degree remainder : ℕ)
    (hscan : rowScan weights values degree = some remainder) :
    values.length = weights.length ∧
      (∀ w ∈ weights, 0 < w) ∧ rowDot weights values + remainder = degree := by
  induction weights generalizing values degree remainder with
  | nil =>
      cases values with
      | nil => simp [rowScan] at hscan; simp [rowDot, hscan]
      | cons x xs => simp [rowScan] at hscan
  | cons w ws ih =>
      cases values with
      | nil => simp [rowScan] at hscan
      | cons x xs =>
          simp only [rowScan] at hscan
          split_ifs at hscan with hguard
          · obtain ⟨hlen, hpos, hdot⟩ := ih xs (degree - x * w) remainder hscan
            rcases hguard with ⟨hw, hx⟩
            have hmul : x * w ≤ degree := (Nat.le_div_iff_mul_le hw).mp hx
            simp only [List.length_cons, rowDot]
            refine ⟨by omega, ?_, ?_⟩
            · intro v hv
              rcases List.mem_cons.mp hv with rfl | hv
              · exact hw
              · exact hpos v hv
            · omega

def rowValues (size : ℕ) (x : Point) : List ℕ := x.take size

/-- The caller constructs each source `std::array<int, 5>` from `x{}` and only
writes the first `size` positions. -/
def pointShape (size : ℕ) (x : Point) : Prop :=
  x.length = 5 ∧ x = x.take size ++ List.replicate (5 - size) 0

def packPoint (shared : ℕ) (x : Point) : ℕ :=
  pack shared (fun p => x.getD p 0)

def pointCats (size shared : ℕ) (x : Point) : ℕ :=
  (List.range (size - shared)).foldl (fun cats t =>
    cats ||| (((1 : ℕ) <<< min (x.getD (shared + t) 0) 3) <<< (4 * t))) 0

def privatePairs (size shared : ℕ) : List (ℕ × ℕ) :=
  let n := size - shared
  (List.range n).flatMap fun t1 =>
    (List.range (n - (t1 + 1))).map fun dt => (shared + t1, shared + (t1 + 1 + dt))

def pairPointContribution (x : Point) (u : ℕ) (ab : ℕ × ℕ) : ℕ :=
  let sum := x.getD ab.1 0 + x.getD ab.2 0
  (if sum ≤ 1 then (1 : ℕ) <<< (2 * u) else 0) |||
    (if 3 ≤ sum then (1 : ℕ) <<< (2 * u + 1) else 0)

def pointPairs (size shared : ℕ) (x : Point) : ℕ :=
  let ps := privatePairs size shared
  (List.range ps.length).foldl (fun pairs u =>
    pairs ||| pairPointContribution x u (ps.getD u (0, 0))) 0

def catsSummary (size shared : ℕ) (rows : List Point) : ℕ :=
  rows.foldl (fun cats x => cats ||| pointCats size shared x) 0

def pairsSummary (size shared : ℕ) (rows : List Point) : ℕ :=
  rows.foldl (fun pairs x => pairs ||| pointPairs size shared x) 0

theorem foldl_or_testBit {α : Type} (f : α → ℕ) (xs : List α) (bit init : ℕ) :
    (xs.foldl (fun acc x => acc ||| f x) init).testBit bit =
      (init.testBit bit || xs.any (fun x => (f x).testBit bit)) := by
  induction xs generalizing init with
  | nil => simp
  | cons x xs ih =>
      simp only [List.foldl_cons, Nat.testBit_lor, ih, List.any_cons]
      simp [Bool.or_assoc, Bool.or_comm]

theorem oneShift_testBit (i j : ℕ) : (1 <<< i).testBit j = decide (i = j) := by
  rw [Nat.shiftLeft_eq]
  simpa using (Nat.testBit_two_pow (n := i) (m := j))

theorem conditionalShift_testBit (P : Prop) [Decidable P] (i j : ℕ) :
    (if P then (1 : ℕ) <<< i else 0).testBit j = decide (P ∧ i = j) := by
  by_cases h : P
  · rw [if_pos h, oneShift_testBit]
    simp [h]
  · rw [if_neg h, Nat.zero_testBit]
    simp [h]

theorem pointCats_nibble (size shared : ℕ) (x : Point) (t c : ℕ)
    (ht : t < size - shared) (hc : c < 4) :
    (pointCats size shared x).testBit (4 * t + c) =
      decide (min (x.getD (shared + t) 0) 3 = c) := by
  rw [pointCats, foldl_or_testBit]
  simp only [Nat.zero_testBit, Bool.false_or]
  apply Bool.eq_iff_iff.mpr
  simp only [List.any_eq_true, decide_eq_true_iff]
  constructor
  · rintro ⟨u, hu, hbit⟩
    have hu' : u < size - shared := List.mem_range.mp hu
    have heq : min (x.getD (shared + u) 0) 3 + 4 * u = 4 * t + c := by
      have h := oneShift_testBit (min (x.getD (shared + u) 0) 3 + 4 * u) (4 * t + c)
      rw [← Nat.shiftLeft_add, h] at hbit
      exact of_decide_eq_true hbit
    have hut : u = t := by omega
    subst u
    omega
  · intro hclass
    refine ⟨t, List.mem_range.mpr ht, ?_⟩
    have heq : min (x.getD (shared + t) 0) 3 + 4 * t = 4 * t + c := by omega
    have hshift := oneShift_testBit (min (x.getD (shared + t) 0) 3 + 4 * t) (4 * t + c)
    rw [← Nat.shiftLeft_add, hshift]
    exact decide_eq_true heq

theorem catsSummary_nibble (size shared : ℕ) (rows : List Point) (t c : ℕ)
    (ht : t < size - shared) (hc : c < 4) :
    (catsSummary size shared rows).testBit (4 * t + c) = true ↔
      ∃ x ∈ rows, min (x.getD (shared + t) 0) 3 = c := by
  rw [catsSummary, foldl_or_testBit]
  simp only [Nat.zero_testBit, Bool.false_or, List.any_eq_true]
  constructor
  · rintro ⟨x, hx, hbit⟩
    have hclass := pointCats_nibble size shared x t c ht hc
    rw [hclass] at hbit
    exact ⟨x, hx, of_decide_eq_true hbit⟩
  · rintro ⟨x, hx, hclass⟩
    refine ⟨x, hx, ?_⟩
    rw [pointCats_nibble size shared x t c ht hc]
    exact decide_eq_true hclass

theorem pairPointContribution_low (x : Point) (u v : ℕ) (ab : ℕ × ℕ) :
    (pairPointContribution x v ab).testBit (2 * u) =
      decide (v = u ∧ x.getD ab.1 0 + x.getD ab.2 0 ≤ 1) := by
  unfold pairPointContribution
  rw [Nat.testBit_lor, conditionalShift_testBit, conditionalShift_testBit]
  have hcross : 2 * v + 1 ≠ 2 * u := by omega
  by_cases hvu : v = u
  · subst v
    simp [hcross]
  · have heven : 2 * v ≠ 2 * u := by omega
    simp [hvu, heven, hcross]

theorem pairPointContribution_high (x : Point) (u v : ℕ) (ab : ℕ × ℕ) :
    (pairPointContribution x v ab).testBit (2 * u + 1) =
      decide (v = u ∧ 3 ≤ x.getD ab.1 0 + x.getD ab.2 0) := by
  unfold pairPointContribution
  rw [Nat.testBit_lor, conditionalShift_testBit, conditionalShift_testBit]
  have hcross : 2 * v ≠ 2 * u + 1 := by omega
  by_cases hvu : v = u
  · subst v
    simp [hcross]
  · have hodd : 2 * v + 1 ≠ 2 * u + 1 := by omega
    simp [hvu, hodd, hcross]

theorem pointPairs_low (size shared : ℕ) (x : Point) (u : ℕ)
    (hu : u < (privatePairs size shared).length) :
    (pointPairs size shared x).testBit (2 * u) =
      decide (let ab := (privatePairs size shared).getD u (0, 0);
        x.getD ab.1 0 + x.getD ab.2 0 ≤ 1) := by
  unfold pointPairs
  rw [foldl_or_testBit]
  simp only [Nat.zero_testBit, Bool.false_or]
  apply Bool.eq_iff_iff.mpr
  simp only [List.any_eq_true, decide_eq_true_iff]
  let ps := privatePairs size shared
  let ab := ps.getD u (0, 0)
  change (∃ v ∈ List.range ps.length,
      (pairPointContribution x v (ps.getD v (0, 0))).testBit (2 * u) = true) ↔
      x.getD ab.1 0 + x.getD ab.2 0 ≤ 1
  constructor
  · rintro ⟨v, hv, hbit⟩
    have hcontrib := pairPointContribution_low x u v (ps.getD v (0, 0))
    rw [hcontrib] at hbit
    have hvu : v = u := (of_decide_eq_true hbit).1
    subst v
    simpa [ab, ps] using (of_decide_eq_true hbit).2
  · intro hsum
    refine ⟨u, List.mem_range.mpr hu, ?_⟩
    have hcontrib := pairPointContribution_low x u u (ps.getD u (0, 0))
    rw [hcontrib]
    exact decide_eq_true ⟨rfl, by simpa [ab, ps] using hsum⟩

theorem pointPairs_high (size shared : ℕ) (x : Point) (u : ℕ)
    (hu : u < (privatePairs size shared).length) :
    (pointPairs size shared x).testBit (2 * u + 1) =
      decide (let ab := (privatePairs size shared).getD u (0, 0);
        3 ≤ x.getD ab.1 0 + x.getD ab.2 0) := by
  unfold pointPairs
  rw [foldl_or_testBit]
  simp only [Nat.zero_testBit, Bool.false_or]
  apply Bool.eq_iff_iff.mpr
  simp only [List.any_eq_true, decide_eq_true_iff]
  let ps := privatePairs size shared
  let ab := ps.getD u (0, 0)
  change (∃ v ∈ List.range ps.length,
      (pairPointContribution x v (ps.getD v (0, 0))).testBit (2 * u + 1) = true) ↔
      3 ≤ x.getD ab.1 0 + x.getD ab.2 0
  constructor
  · rintro ⟨v, hv, hbit⟩
    have hcontrib := pairPointContribution_high x u v (ps.getD v (0, 0))
    rw [hcontrib] at hbit
    have hvu : v = u := (of_decide_eq_true hbit).1
    subst v
    simpa [ab, ps] using (of_decide_eq_true hbit).2
  · intro hsum
    refine ⟨u, List.mem_range.mpr hu, ?_⟩
    have hcontrib := pairPointContribution_high x u u (ps.getD u (0, 0))
    rw [hcontrib]
    exact decide_eq_true ⟨rfl, by simpa [ab, ps] using hsum⟩

theorem pairsSummary_low (size shared : ℕ) (rows : List Point) (u : ℕ)
    (hu : u < (privatePairs size shared).length) :
    (pairsSummary size shared rows).testBit (2 * u) = true ↔
      ∃ x ∈ rows, let ab := (privatePairs size shared).getD u (0, 0);
        x.getD ab.1 0 + x.getD ab.2 0 ≤ 1 := by
  rw [pairsSummary, foldl_or_testBit]
  simp only [Nat.zero_testBit, Bool.false_or, List.any_eq_true]
  constructor
  · rintro ⟨x, hx, hbit⟩
    rw [pointPairs_low size shared x u hu] at hbit
    exact ⟨x, hx, of_decide_eq_true hbit⟩
  · rintro ⟨x, hx, hsum⟩
    refine ⟨x, hx, ?_⟩
    rw [pointPairs_low size shared x u hu]
    exact decide_eq_true hsum

theorem pairsSummary_high (size shared : ℕ) (rows : List Point) (u : ℕ)
    (hu : u < (privatePairs size shared).length) :
    (pairsSummary size shared rows).testBit (2 * u + 1) = true ↔
      ∃ x ∈ rows, let ab := (privatePairs size shared).getD u (0, 0);
        3 ≤ x.getD ab.1 0 + x.getD ab.2 0 := by
  rw [pairsSummary, foldl_or_testBit]
  simp only [Nat.zero_testBit, Bool.false_or, List.any_eq_true]
  constructor
  · rintro ⟨x, hx, hbit⟩
    rw [pointPairs_high size shared x u hu] at hbit
    exact ⟨x, hx, of_decide_eq_true hbit⟩
  · rintro ⟨x, hx, hsum⟩
    refine ⟨x, hx, ?_⟩
    rw [pointPairs_high size shared x u hu]
    exact decide_eq_true hsum

def pointOK (entry : Entry) (size shared : ℕ) (x : Point) : Prop :=
  pointShape size x ∧ rowScan entry.weights (rowValues size x) entry.degree = some 0 ∧
    ∀ p, p < shared → x.getD p 0 ≤ 65535

/-- The finite-list form of the source's mutable cursor. For each fibre it
checks nonzero count, consumes exactly that many points, and recurses on the
remaining suffix. The empty/empty case is the final `cursor == points.size`
check; an empty fibre list with remaining points is the final throw. -/
def sourceChunks : List Fibre → List Point → Option (List (Fibre × List Point))
  | [], [] => some []
  | [], _ :: _ => none
  | g :: gs, points =>
      if 0 < g.count ∧ g.count ≤ points.length then
        (sourceChunks gs (points.drop g.count)).map
          (fun rest => (g, points.take g.count) :: rest)
      else none

theorem sourceChunks_sound (fibres : List Fibre) (points : List Point)
    (chunks : List (Fibre × List Point))
    (h : sourceChunks fibres points = some chunks) :
    chunks.map Prod.fst = fibres ∧
      (chunks.map Prod.snd).flatten = points ∧
      (∀ z ∈ chunks, z.1.count = z.2.length ∧ 0 < z.1.count) := by
  induction fibres generalizing points chunks with
  | nil =>
      cases points with
      | nil => simp [sourceChunks] at h; subst chunks; simp
      | cons p ps => simp [sourceChunks] at h
  | cons g gs ih =>
      simp only [sourceChunks] at h
      split_ifs at h with hguard
      · simp only [Option.map_eq_some_iff] at h
        rcases h with ⟨tail, htail, rfl⟩
        obtain ⟨hfst, hflat, hrows⟩ := ih (points.drop g.count) tail htail
        have htake : (points.take g.count).length = g.count := by
          rw [List.length_take]
          omega
        refine ⟨?_, ?_, ?_⟩
        · simp [hfst]
        · change (List.map Prod.snd ((g, points.take g.count) :: tail)).flatten = points
          simp only [List.map_cons, Prod.snd, List.flatten_cons, hflat]
          exact List.take_append_drop g.count points
        · intro z hz
          simp only [List.mem_cons] at hz
          rcases hz with rfl | hz
          · rcases hguard with ⟨hc, _⟩
            simpa using And.intro htake hc
          · exact hrows z hz

def fibreOK (entry : Entry) (size shared : ℕ) (g : Fibre) (rows : List Point) : Prop :=
  g.key < 2 ^ 64 ∧ g.count < 2 ^ 32 ∧ g.cats < 2 ^ 32 ∧ g.pairs < 2 ^ 32 ∧
    (shared < 4 → g.key < 2 ^ (16 * shared)) ∧
    (∀ x ∈ rows, pointOK entry size shared x ∧ packPoint shared x = g.key) ∧
    g.cats = catsSummary size shared rows ∧ g.pairs = pairsSummary size shared rows

/-- All checks in the validator, plus the small caller invariants supplied by
`build_gjoin_table`: supported dimensions, positive table weights, and the
zero-filled suffix of its five-slot point arrays. -/
def Accepts (entry : Entry) (size shared : ℕ) (points : List Point)
    (fibres : List Fibre) : Prop :=
  1 ≤ size ∧ size ≤ 5 ∧ shared ≤ size ∧ shared ≤ 4 ∧
  entry.weights.length = size ∧ (∀ w ∈ entry.weights, 0 < w) ∧
  sourceAdjacent points ∧
  Adjacent (fun a b : Fibre => a.key < b.key) fibres ∧
  ∃ chunks, sourceChunks fibres points = some chunks ∧
    (∀ z ∈ chunks, fibreOK entry size shared z.1 z.2) ∧
    dpCount entry.weights entry.degree = points.length

theorem sourceChunks_rows {entry : Entry} {size shared : ℕ} {points : List Point}
    {fibres : List Fibre} (h : Accepts entry size shared points fibres) :
    ∃ chunks, sourceChunks fibres points = some chunks ∧
      (chunks.map Prod.snd).flatten = points ∧
      (∀ z ∈ chunks, z.1.count = z.2.length ∧ 0 < z.1.count ∧
        fibreOK entry size shared z.1 z.2) := by
  rcases h with ⟨_, _, _, _, _, _, _, _, chunks, hchunks, hok, _⟩
  obtain ⟨_, hflat, hcounts⟩ := sourceChunks_sound fibres points chunks hchunks
  exact ⟨chunks, hchunks, hflat, fun z hz => ⟨(hcounts z hz).1, (hcounts z hz).2,
    hok z hz⟩⟩

theorem pointOK_row_solution {entry : Entry} {size shared : ℕ} {x : Point}
    (h : pointOK entry size shared x) :
    (rowValues size x).length = entry.weights.length ∧
      rowDot entry.weights (rowValues size x) = entry.degree := by
  obtain ⟨_, hscan, _⟩ := h
  have hs := rowScan_sound entry.weights (rowValues size x) entry.degree 0 hscan
  exact ⟨hs.1, by simpa [rowDot] using hs.2.2⟩

theorem map_rowValues_injective {size : ℕ} {points : List Point}
    (hshape : ∀ x ∈ points, pointShape size x) :
    ∀ x ∈ points, ∀ y ∈ points, rowValues size x = rowValues size y → x = y := by
  intro x hx y hy hxy
  obtain ⟨_, hxform⟩ := hshape x hx
  obtain ⟨_, hyform⟩ := hshape y hy
  change x.take size = y.take size at hxy
  calc
    x = x.take size ++ List.replicate (5 - size) 0 := hxform
    _ = y.take size ++ List.replicate (5 - size) 0 := by rw [hxy]
    _ = y := hyform.symm

/-- Accepted validation proves that the emitted rows are exactly all
nonnegative solutions of the row equation, each once. This is the interface
used to identify a stored chunk with the corresponding `JoinPoints.fibre`. -/
theorem accepted_exact_solution_set {entry : Entry} {size shared : ℕ}
    {points : List Point} {fibres : List Fibre}
    (h : Accepts entry size shared points fibres) :
    ∀ xs, xs ∈ points.map (rowValues size) ↔
      xs.length = entry.weights.length ∧ rowDot entry.weights xs = entry.degree := by
  rcases h with ⟨hsize0, hsize, hshared, hshared4, hwlen, hwpos, horder, hkeys,
    chunks, hchunks, hOK, hcount⟩
  have hnd : points.Nodup := sourceAdjacent_nodup points horder
  have hchunksSound := sourceChunks_sound fibres points chunks hchunks
  have hflat := hchunksSound.2.1
  have hshape : ∀ x ∈ points, pointShape size x := by
    intro x hx
    have hx' : x ∈ (chunks.map Prod.snd).flatten := by simpa [hflat] using hx
    rcases List.mem_flatten.mp hx' with ⟨rows, hrows, hrow⟩
    rcases List.mem_map.mp hrows with ⟨⟨g, row⟩, hg, rfl⟩
    rcases hOK (g, row) hg with ⟨_, _, _, _, _, hpoints, _, _⟩
    exact (hpoints x hrow).1.1
  have hrownd : (points.map (rowValues size)).Nodup :=
    List.Nodup.map_on (map_rowValues_injective hshape) hnd
  have hvalid : ∀ x ∈ points.map (rowValues size),
      x.length = entry.weights.length ∧ rowDot entry.weights x = entry.degree := by
    intro x hx
    obtain ⟨p, hp, rfl⟩ := List.mem_map.mp hx
    have hpChunk : p ∈ (chunks.map Prod.snd).flatten := by simpa [hflat] using hp
    rcases List.mem_flatten.mp hpChunk with ⟨rows, hrows, hrow⟩
    rcases List.mem_map.mp hrows with ⟨⟨g, row⟩, hg, rfl⟩
    rcases hOK (g, row) hg with ⟨_, _, _, _, _, hpoints, _, _⟩
    exact pointOK_row_solution (hpoints p hrow).1
  have hlen : (points.map (rowValues size)).length = dpCount entry.weights entry.degree := by
    simp [hcount]
  intro xs
  exact enumerated_complete entry.weights hwpos entry.degree (points.map (rowValues size))
    hrownd hvalid hlen xs

/-- The concrete rows assigned to one stored key by the count-driven scan. -/
structure CheckedChunk (entry : Entry) (size shared : ℕ) (g : Fibre)
    (rows : List Point) : Prop where
  count_eq : rows.length = g.count
  nonempty : 0 < g.count
  point_valid : ∀ x ∈ rows, pointOK entry size shared x
  packed_key : ∀ x ∈ rows, packPoint shared x = g.key
  key_width : g.key < 2 ^ (16 * shared)
  cats_exact : g.cats = catsSummary size shared rows
  pairs_exact : g.pairs = pairsSummary size shared rows

theorem accepted_chunk_facts {entry : Entry} {size shared : ℕ}
    {points : List Point} {fibres : List Fibre}
    (h : Accepts entry size shared points fibres) (g : Fibre) (hg : g ∈ fibres) :
    ∃ chunks rows, sourceChunks fibres points = some chunks ∧
      (g, rows) ∈ chunks ∧ CheckedChunk entry size shared g rows := by
  rcases h with ⟨hsize0, hsize, hshared, hshared4, hwlen, hwpos, horder, hkeys,
    chunks, hchunks, hOK, hcount⟩
  have hscan := sourceChunks_sound fibres points chunks hchunks
  have hg' : g ∈ chunks.map Prod.fst := by rw [hscan.1]; exact hg
  rcases List.mem_map.mp hg' with ⟨⟨f, rows⟩, hz, hfg⟩
  simp only [Prod.fst] at hfg
  subst f
  have hcountRows := hscan.2.2 (g, rows) hz
  rcases hOK (g, rows) hz with
    ⟨hkey64, hcount32, hcats32, hpairs32, hkeywidth, hpoints, hcats, hpairs⟩
  have hkeyWidth' : g.key < 2 ^ (16 * shared) := by
    by_cases hsmall : shared < 4
    · exact hkeywidth hsmall
    · have heq : shared = 4 := by omega
      rw [heq]
      norm_num at hkey64 ⊢
      exact hkey64
  refine ⟨chunks, rows, hchunks, hz, ?_⟩
  exact ⟨hcountRows.1.symm, hcountRows.2, fun x hx => (hpoints x hx).1,
    fun x hx => (hpoints x hx).2, hkeyWidth', hcats, hpairs⟩

theorem point_chunk_member {fibres : List Fibre} {points : List Point}
    {chunks : List (Fibre × List Point)}
    (hchunks : sourceChunks fibres points = some chunks) {x : Point} (hx : x ∈ points) :
    ∃ g rows, (g, rows) ∈ chunks ∧ x ∈ rows := by
  have hs := sourceChunks_sound fibres points chunks hchunks
  have hxflat : x ∈ (chunks.map Prod.snd).flatten := by rw [hs.2.1]; exact hx
  rcases List.mem_flatten.mp hxflat with ⟨rows, hrows, hxrow⟩
  rcases List.mem_map.mp hrows with ⟨z, hz, hzs⟩
  cases z with
  | mk g rows' =>
      simp only [Prod.snd] at hzs
      subst rows'
      exact ⟨g, rows, hz, hxrow⟩

/-- A canonical chunk is exactly the accepted row points with that packed key.
The source count partitions all points, each point's key matches its chunk,
and strict canonical key ordering makes the chunk for a key unique. -/
theorem accepted_chunk_rows_exact {entry : Entry} {size shared : ℕ}
    {points : List Point} {fibres : List Fibre}
    (h : Accepts entry size shared points fibres) (g : Fibre) (hg : g ∈ fibres) :
    ∃ chunks rows, sourceChunks fibres points = some chunks ∧
      (g, rows) ∈ chunks ∧ CheckedChunk entry size shared g rows ∧
      ∀ x, x ∈ rows ↔ x ∈ points ∧ packPoint shared x = g.key := by
  rcases h with ⟨hsize0, hsize, hshared, hshared4, hwlen, hwpos, horder, hkeys,
    chunks, hchunks, hOK, hcount⟩
  have hscan := sourceChunks_sound fibres points chunks hchunks
  have hg' : g ∈ chunks.map Prod.fst := by rw [hscan.1]; exact hg
  rcases List.mem_map.mp hg' with ⟨⟨f, rows⟩, hz, hfg⟩
  simp only [Prod.fst] at hfg
  subst f
  have hcountRows := hscan.2.2 (g, rows) hz
  rcases hOK (g, rows) hz with
    ⟨hkey64, hcount32, hcats32, hpairs32, hkeywidth, hpoints, hcats, hpairs⟩
  have hkeyWidth' : g.key < 2 ^ (16 * shared) := by
    by_cases hsmall : shared < 4
    · exact hkeywidth hsmall
    · have heq : shared = 4 := by omega
      rw [heq]
      norm_num at hkey64 ⊢
      exact hkey64
  have hchecked : CheckedChunk entry size shared g rows :=
    ⟨hcountRows.1.symm, hcountRows.2, fun x hx => (hpoints x hx).1,
      fun x hx => (hpoints x hx).2, hkeyWidth', hcats, hpairs⟩
  have hfibresNodup : fibres.Nodup := adjacent_keys_nodup fibres hkeys
  have hfirstNodup : (chunks.map Prod.fst).Nodup := by
    rw [hscan.1]
    exact hfibresNodup
  refine ⟨chunks, rows, hchunks, hz, hchecked, ?_⟩
  intro x
  constructor
  · intro hx
    refine ⟨?_, hchecked.packed_key x hx⟩
    have hRows : rows ∈ chunks.map Prod.snd :=
      List.mem_map.mpr ⟨(g, rows), hz, rfl⟩
    have hxflat : x ∈ (chunks.map Prod.snd).flatten :=
      List.mem_flatten.mpr ⟨rows, hRows, hx⟩
    rw [hscan.2.1] at hxflat
    exact hxflat
  · rintro ⟨hx, hkey⟩
    obtain ⟨f, otherRows, hother, hxother⟩ := point_chunk_member hchunks hx
    have hf' : f ∈ fibres := by
      have hfmap : f ∈ chunks.map Prod.fst :=
        List.mem_map.mpr ⟨(f, otherRows), hother, rfl⟩
      rw [hscan.1] at hfmap
      exact hfmap
    rcases hOK (f, otherRows) hother with ⟨_, _, _, _, _, hotherPoints, _, _⟩
    have hotherKey := (hotherPoints x hxother).2
    have hfgKey : f.key = g.key := hotherKey.symm.trans hkey
    have hfg : f = g := equal_key_mem_eq fibres hkeys hf' hg hfgKey
    subst f
    have hpEq := pairProjection_injective hfirstNodup (g, otherRows) hother
      (g, rows) hz rfl
    have hrowsEq : otherRows = rows := by cases hpEq; rfl
    simpa [hrowsEq] using hxother

/-- Every accepted shared coordinate is the key's decoded 16-bit field. -/
theorem checkedChunk_shared_field {entry : Entry} {size shared : ℕ}
    {g : Fibre} {rows : List Point}
    (h : CheckedChunk entry size shared g rows) (x : Point) (hx : x ∈ rows)
    (p : ℕ) (hp : p < shared) : x.getD p 0 = field shared p g.key := by
  apply fibre_key_fields shared g.key (fun q => x.getD q 0)
  · intro q hq
    have hbound := (h.point_valid x hx).2.2 q hq
    norm_num at hbound ⊢
    omega
  · exact h.key_width
  · simpa [packPoint] using h.packed_key x hx
  · exact hp

theorem checkedChunk_cat_class {entry : Entry} {size shared : ℕ}
    {g : Fibre} {rows : List Point} (h : CheckedChunk entry size shared g rows)
    (t c : ℕ) (ht : t < size - shared) (hc : c < 4) :
    g.cats.testBit (4 * t + c) = true ↔
      ∃ x ∈ rows, min (x.getD (shared + t) 0) 3 = c := by
  rw [h.cats_exact, catsSummary_nibble size shared rows t c ht hc]

theorem checkedChunk_pair_low {entry : Entry} {size shared : ℕ}
    {g : Fibre} {rows : List Point} (h : CheckedChunk entry size shared g rows)
    (u : ℕ) (hu : u < (privatePairs size shared).length) :
    g.pairs.testBit (2 * u) = true ↔
      ∃ x ∈ rows, let ab := (privatePairs size shared).getD u (0, 0);
        x.getD ab.1 0 + x.getD ab.2 0 ≤ 1 := by
  rw [h.pairs_exact, pairsSummary_low size shared rows u hu]

theorem checkedChunk_pair_high {entry : Entry} {size shared : ℕ}
    {g : Fibre} {rows : List Point} (h : CheckedChunk entry size shared g rows)
    (u : ℕ) (hu : u < (privatePairs size shared).length) :
    g.pairs.testBit (2 * u + 1) = true ↔
      ∃ x ∈ rows, let ab := (privatePairs size shared).getD u (0, 0);
        3 ≤ x.getD ab.1 0 + x.getD ab.2 0 := by
  rw [h.pairs_exact, pairsSummary_high size shared rows u hu]

end Refpoly.JoinFibreValidator
