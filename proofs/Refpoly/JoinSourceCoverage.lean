import Refpoly.JoinTupleBridge

/-! Coverage of a particular simultaneous row point by the source join walk.

The table/layout translation supplies an index in each row whose shared key
fields equal that point's shared values. This file proves those indices are
accepted successively by the real `extendA` matcher and therefore occur in
`fullLeaves`, then uses `crun_perm_full` to get the tuple into the actual
source loop output. No global coverage or compatibility conclusion is assumed.
-/
namespace Refpoly.JoinLoopSource

open Refpoly.JoinLoop

def BoundCorrect (E : Env) (depth : ℕ) (state point : ℕ → ℕ) : Prop :=
  ∀ A, E.bound depth A = true → state A = point A

def RowFieldsCorrect (E : Env) (depth index : ℕ) (point : ℕ → ℕ) : Prop :=
  ∀ q, q < E.k (E.slot depth) →
    field (E.k (E.slot depth)) q (E.key (E.slot depth) index) =
      point (E.coord (E.slot depth) q)

/-- A row match succeeds if all already-bound shared values agree with the
selected key. Unbound positions are written by the matcher and do not block
the match. -/
theorem matchFrom_ok_of_bound_agree (key k : ℕ) (coords : ℕ → ℕ) (bnd : ℕ → Bool)
    (inj : ∀ p q, p < k → q < k → coords p = coords q → p = q) :
    ∀ p val, (∀ q, p ≤ q → q < k → bnd (coords q) = true →
      val (coords q) = field k q key) →
      (matchFrom key k coords bnd p val).1 = true := by
  refine upto_ind k (fun p hp => ?_) (fun p hp ih => ?_)
  · intro val _
    rw [matchFrom, dif_neg (by omega)]
  · intro val hagree
    rw [matchFrom, dif_pos hp]
    simp only
    by_cases hb : bnd (coords p) = true
    · rw [if_pos hb]
      have hval := hagree p le_rfl hp hb
      rw [if_neg (by simpa [hval])]
      apply ih val
      intro q hpq hq hbq
      exact hagree q (by omega) hq hbq
    · rw [if_neg hb]
      apply ih (Function.update val (coords p) (field k p key))
      intro q hpq hq hbq
      have hne : coords q ≠ coords p := by
        intro heq
        have h := inj q p hq hp heq
        omega
      rw [Function.update_of_ne hne]
      exact hagree q (by omega) hq hbq

theorem extendA_preserves_point {E : Env} (hP : PlanOK E)
    (depth index : ℕ) (state point : ℕ → ℕ)
    (hstate : BoundCorrect E depth state point)
    (hfields : RowFieldsCorrect E depth index point) :
    ∃ next, extendA E depth state index = some next ∧
      BoundCorrect E (depth + 1) next point := by
  let s := E.slot depth
  let k := E.k s
  let coords := E.coord s
  let bnd := E.bound depth
  let key := E.key s index
  have hagree : ∀ q, 0 ≤ q → q < k → bnd (coords q) = true →
      state (coords q) = field k q key := by
    intro q _ hq hb
    rw [hstate (coords q) hb]
    exact (hfields q hq).symm
  have hmatch : (matchKey key k coords bnd state).1 = true := by
    unfold matchKey
    exact matchFrom_ok_of_bound_agree key k coords bnd (hP.inj s) 0 state
      (fun q h0 hq hb => hagree q h0 hq hb)
  have hsuccess : extendA E depth state index =
      some (restrict (E.bound (depth + 1)) (matchKey key k coords bnd state).2) := by
    simp [extendA, s, k, coords, bnd, key, hmatch]
  refine ⟨restrict (E.bound (depth + 1)) (matchKey key k coords bnd state).2,
    hsuccess, ?_⟩
  intro A hnext
  obtain hprev | ⟨q, hq, hcoord⟩ := (hP.bound_succ depth A).mp hnext
  · have hkeep := matchFrom_bound key k coords bnd 0 state A hprev
    simp [restrict, s, k, coords, bnd, key, hnext, matchKey, hkeep]
    exact hstate A hprev
  · have hin := matchFrom_installs key k coords bnd (hP.inj s) 0 state hmatch q
      (Nat.zero_le _) hq
    have hcoord' : coords q = A := by simpa [coords, s] using hcoord
    rw [← hcoord']
    have hbq : E.bound (depth + 1) (coords q) = true := by rw [hcoord']; exact hnext
    simp [restrict, hbq, matchKey, hin]
    simpa [k, key, coords, s] using hfields q hq

/-- The selected source indices in levels `j` through `j+n-1`. -/
def keysFrom (u : ℕ → ℕ) : ℕ → ℕ → List ℕ
  | _, 0 => []
  | j, n + 1 => u j :: keysFrom u (j + 1) n

/-- Apply a fixed sequence of selected indices, returning the final matcher
state if every extension succeeds. -/
def followIndices (E : Env) : ℕ → (ℕ → ℕ) → List ℕ → Option (ℕ → ℕ)
  | _, state, [] => some state
  | depth, state, index :: rest => do
      let next ← extendA E depth state index
      followIndices E (depth + 1) next rest

theorem followIndices_point {E : Env} (hP : PlanOK E) (point u : ℕ → ℕ) :
    ∀ n depth state, depth + n ≤ E.R → BoundCorrect E depth state point →
      (∀ t, t < n → RowFieldsCorrect E (depth + t) (u (depth + t)) point) →
      ∃ final, followIndices E depth state (keysFrom u depth n) = some final ∧
        BoundCorrect E (depth + n) final point := by
  intro n
  induction n with
  | zero =>
      intro depth state hdepth hstate _
      exact ⟨state, rfl, by simpa using hstate⟩
  | succ n ih =>
      intro depth state hdepth hstate hfields
      have hfirst := extendA_preserves_point hP depth (u depth) state point hstate
        (hfields 0 (by omega))
      rcases hfirst with ⟨next, hnext, hnextState⟩
      obtain ⟨final, hfollow, hfinal⟩ := ih (depth + 1) next (by omega) hnextState
        (by
          intro t ht
          have := hfields (t + 1) (by omega)
          simpa [Nat.add_assoc, Nat.add_comm, Nat.add_left_comm] using this)
      refine ⟨final, ?_, ?_⟩
      · change (do
          let next ← extendA E depth state (u depth)
          followIndices E (depth + 1) next (keysFrom u (depth + 1) n)) = some final
        rw [hnext]
        exact hfollow
      · simpa [Nat.add_assoc, Nat.add_comm, Nat.add_left_comm] using hfinal

/-- If the chosen indices are in their full rows, successful extension at each
level puts the whole tuple in `fullLeaves`. -/
theorem keysFrom_mem_fullLeaves (E : Env) (u : ℕ → ℕ) :
    ∀ n depth state final, followIndices E depth state (keysFrom u depth n) = some final →
      (∀ t, t < n → u (depth + t) ∈ row E (depth + t)) →
      keysFrom u depth n ∈ fullLeaves (row E) (extendA E) n depth state := by
  intro n
  induction n with
  | zero => intro depth state final hfollow _; simp [keysFrom, fullLeaves]
  | succ n ih =>
      intro depth state final hfollow hrow
      simp only [keysFrom, followIndices] at hfollow
      cases hext : extendA E depth state (u depth) with
      | none => simp [hext] at hfollow
      | some next =>
          simp only [hext] at hfollow
          have hrest : keysFrom u (depth + 1) n ∈ fullLeaves (row E) (extendA E) n (depth + 1) next :=
            ih (depth + 1) next final hfollow (by
              intro t ht
              simpa [Nat.add_assoc, Nat.add_comm, Nat.add_left_comm] using hrow (t + 1) (by omega))
          apply List.mem_flatMap.mpr
          refine ⟨u depth, hrow 0 (by omega), ?_⟩
          simp only [fullLeaves, hext]
          exact List.mem_map.mpr ⟨keysFrom u (depth + 1) n, hrest, rfl⟩

/-- Every tuple with per-level keys and fields matching one point is in the
actual `crun` output. The false initial bound is explicit: at depth zero no
coordinate value is required from the uninitialized caller buffer. -/
theorem crun_contains_point_indices {E : Env} (hP : PlanOK E) (hT : TablesOK E)
    (hR : 0 < E.R) (hbound0 : ∀ A, E.bound 0 A = false)
    (val it en : ℕ → ℕ) (ps : ℕ → Option ℕ) (chosen : ℕ → ℕ)
    (fuel : ℕ)
    (enough : Refpoly.JoinLoop.stackWork (candA E) (extendA E)
      (decode E (initial E val it en ps chosen)) ≤ fuel)
    (point u : ℕ → ℕ)
    (hindices : ∀ j, j < E.R → E.kb (E.slot j) ≤ u j ∧ u j < E.ke (E.slot j))
    (hfields : ∀ j, j < E.R → RowFieldsCorrect E j (u j) point) :
    keysFrom u 0 E.R ∈ crun E fuel (initial E val it en ps chosen) := by
  have hstate0 : BoundCorrect E 0 (restrict (E.bound 0) val) point := by
    intro A hA
    rw [hbound0 A] at hA
    simp at hA
  obtain ⟨final, hfollow, _⟩ := followIndices_point hP point u E.R 0
    (restrict (E.bound 0) val) (by omega) hstate0
    (fun t ht => by simpa only [Nat.zero_add] using hfields t ht)
  have hrows : ∀ t, t < E.R → u t ∈ row E t := by
    intro t ht
    rw [row, List.mem_range'_1]
    rcases hindices t ht with ⟨hb, he⟩
    exact ⟨hb, by have := hT.ordered (E.slot t); omega⟩
  have hfull := keysFrom_mem_fullLeaves E u E.R 0 (restrict (E.bound 0) val) final hfollow
    (by intro t ht; simpa using hrows t ht)
  have hperm := crun_perm_full hP hT hR val it en ps chosen fuel enough
  exact hperm.mem_iff.mpr hfull

/-- The layout key set is the image of the source's normal array range. -/
def SourceKeysExact {N : ℕ} (E : Env) (L : JoinPoints.Layout N E.R) : Prop :=
  ∀ s, L.keys s =
    (List.range' (E.kb s.val) (E.ke s.val - E.kb s.val)).toFinset.image
      (E.key s.val)

/-- Inverting the validator's key-set representation gives a concrete source
array index, with its actual row bounds. -/
theorem source_index_of_layout_key {N : ℕ} {E : Env} {L : JoinPoints.Layout N E.R}
    (hT : TablesOK E) (hkeys : SourceKeysExact E L) (s : Fin E.R) (key : ℕ)
    (hkey : key ∈ L.keys s) :
    ∃ i, E.kb s.val ≤ i ∧ i < E.ke s.val ∧ E.key s.val i = key := by
  rw [hkeys s] at hkey
  rcases Finset.mem_image.mp hkey with ⟨i, hi, hik⟩
  have hirange := List.mem_toFinset.mp hi
  rw [List.mem_range'_1] at hirange
  refine ⟨i, hirange.1, ?_, hik⟩
  have ho := hT.ordered s.val
  omega

/-- Source and Layout shared positions use the same coordinate and prefix
length. `coord` is stated over naturals so its finite proof is discharged by
the caller's row-size fact. -/
structure SourceLayoutGeometry {N : ℕ} (E : Env) (L : JoinPoints.Layout N E.R)
    (O : SlotOrder E) : Prop where
  shared_size : ∀ j : Fin E.R, E.k (E.slot j.val) = L.k (O.order j)
  row_size : ∀ s : Fin E.R, L.k s ≤ L.size s
  coord : ∀ (j : Fin E.R) (q : ℕ) (hq : q < E.k (E.slot j.val)),
    E.coord (E.slot j.val) q =
      (L.pos (O.order j) ⟨q, by
        have hqL : q < L.k (O.order j) := by rw [← shared_size j]; exact hq
        exact Nat.lt_of_lt_of_le hqL (row_size (O.order j))⟩).val

def sourceIndicesNat {E : Env} (u : Fin E.R → ℕ) (j : ℕ) : ℕ :=
  if h : j < E.R then u ⟨j, h⟩ else 0

def sourcePointValue {N : ℕ} (X : Fin N → ℕ) (A : ℕ) : ℕ :=
  if h : A < N then X ⟨A, h⟩ else 0

/-- Every point's own Layout tuple has source array indices, and those indices
are accepted by the real CUDA matcher at every depth. The key-set equality is
the normal array decoder; `hprefix` is the row validator fact that every
solution's packed shared prefix is represented among those keys. -/
theorem crun_covers_layout_point {N : ℕ} {E : Env} {L : JoinPoints.Layout N E.R}
    {O : SlotOrder E} {count : Fin E.R → ℕ → ℕ}
    (hP : PlanOK E) (hT : TablesOK E) (hR : 0 < E.R)
    (hbound0 : ∀ A, E.bound 0 A = false)
    (val it en : ℕ → ℕ) (ps : ℕ → Option ℕ) (chosen : ℕ → ℕ)
    (fuel : ℕ)
    (henough : Refpoly.JoinLoop.stackWork (candA E) (extendA E)
      (decode E (initial E val it en ps chosen)) ≤ fuel)
    (hI : FibreLayoutInterface E L O count)
    (hkeys : SourceKeysExact E L)
    (hprefix : ∀ s x, x ∈ L.sol s → L.prefixKey s x ∈ L.keys s)
    (hw : JoinPoints.Wf L) (hgeo : SourceLayoutGeometry E L O)
    (X : Fin N → ℕ) (hX : JoinPoints.IsPoint L X) :
    ∃ u : Fin E.R → ℕ, SourceIndicesValid O u ∧
      sourceTableTuple O u = JoinPoints.tupleOf L X ∧
      keysFrom (sourceIndicesNat u) 0 E.R ∈ crun E fuel (initial E val it en ps chosen) := by
  classical
  have hfind : ∀ s : Fin E.R, ∃ i, E.kb s.val ≤ i ∧ i < E.ke s.val ∧
      E.key s.val i = JoinPoints.tupleOf L X s := by
    intro s
    apply source_index_of_layout_key hT hkeys s (JoinPoints.tupleOf L X s)
    exact hprefix s (JoinPoints.restr L s X) (hX s)
  choose idx hidx using hfind
  let u : Fin E.R → ℕ := fun j => idx (O.order j)
  have hu : SourceIndicesValid O u := by
    intro j
    simpa [u] using And.intro (hidx (O.order j)).1 (hidx (O.order j)).2.1
  have htuple : sourceTableTuple O u = JoinPoints.tupleOf L X := by
    funext s
    simp only [sourceTableTuple, JoinPoints.tableTuple]
    simpa [u] using (hidx s).2.2
  have hindices : ∀ j, j < E.R →
      E.kb (E.slot j) ≤ sourceIndicesNat u j ∧
        sourceIndicesNat u j < E.ke (E.slot j) := by
    intro j hj
    let jf : Fin E.R := ⟨j, hj⟩
    have hs := hidx (O.order jf)
    have huj : sourceIndicesNat u j = idx (O.order jf) := by
      change (if h : j < E.R then u ⟨j, h⟩ else 0) = idx (O.order jf)
      rw [dif_pos hj]
    constructor
    · rw [O.slot_eq jf, huj]
      exact hs.1
    · rw [O.slot_eq jf, huj]
      exact hs.2.1
  have hfields : ∀ j, j < E.R →
      RowFieldsCorrect E j (sourceIndicesNat u j) (sourcePointValue X) := by
    intro j hj
    let jf : Fin E.R := ⟨j, hj⟩
    let s : Fin E.R := O.order jf
    have hs := hidx s
    have huj : sourceIndicesNat u j = idx s := by
      change (if h : j < E.R then u ⟨j, h⟩ else 0) = idx s
      rw [dif_pos hj]
    have hk : E.k (E.slot j) = L.k s := by simpa [jf, s] using hgeo.shared_size jf
    intro q hq
    let qf : Fin (L.size s) := ⟨q, Nat.lt_of_lt_of_le (by rw [← hk]; exact hq) (hgeo.row_size s)⟩
    have hmember : JoinPoints.restr L s X ∈ JoinPoints.fibre L s (JoinPoints.tupleOf L X s) := by
      simp [JoinPoints.fibre, JoinPoints.tupleOf, hX s]
    have hfields := hw.key_fields s (JoinPoints.tupleOf L X s)
      (JoinPoints.restr L s X) hmember qf (by rw [← hk]; exact hq)
    have hkeyfield := hI.key_fields s (idx s) qf hs.1 hs.2.1 (by rw [← hk]; exact hq)
    calc
      field (E.k (E.slot j)) q (E.key (E.slot j) (sourceIndicesNat u j)) =
          field (E.k s.val) q (E.key s.val (idx s)) := by
            rw [O.slot_eq jf, huj]
      _ = L.fields s (JoinPoints.tupleOf L X s) qf := by
            rw [← hs.2.2]
            exact hkeyfield.symm
      _ = X (L.pos s qf) := hfields.symm
      _ = sourcePointValue X (E.coord (E.slot j) q) := by
            have hcoord := hgeo.coord jf q (by simpa [jf] using hq)
            have hlt : E.coord (E.slot j) q < N := by
              rw [hcoord]
              exact (L.pos s qf).isLt
            unfold sourcePointValue
            rw [dif_pos hlt]
            apply congrArg X
            exact Fin.ext hcoord.symm
  have hrun := crun_contains_point_indices hP hT hR hbound0 val it en ps chosen
    fuel henough (sourcePointValue X) (sourceIndicesNat u) hindices hfields
  exact ⟨u, hu, htuple, hrun⟩

end Refpoly.JoinLoopSource
