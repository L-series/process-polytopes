import Refpoly.JoinLeafPoints
import Refpoly.JoinLeafState

/-! A small refinement interface between the source join's depth-indexed
choices and the slot-indexed table layout.  The source validators still have
to provide the explicitly stated row/table facts below; this file derives the
finite reindexing, fibre product counts, and row-equation equivalence from
those facts. -/
namespace Refpoly.JoinPoints

variable {N R : ℕ}

/-- The explicit position map used to extend a prefix permutation. -/
def prefixPrivateFun {k n : ℕ} (hk : k ≤ n) (e : Fin k ≃ Fin k) (q : Fin n) : Fin n :=
  if h : q.val < k then Fin.castLE hk (e ⟨q.val, h⟩) else q

theorem prefixPrivateFun_left_inv {k n : ℕ} (hk : k ≤ n) (e : Fin k ≃ Fin k) :
    Function.LeftInverse (prefixPrivateFun hk e.symm) (prefixPrivateFun hk e) := by
  intro q
  by_cases hq : q.val < k
  · simp [prefixPrivateFun, hq]
  · simp [prefixPrivateFun, hq, Nat.le_of_not_gt hq]

theorem prefixPrivateFun_right_inv {k n : ℕ} (hk : k ≤ n) (e : Fin k ≃ Fin k) :
    Function.RightInverse (prefixPrivateFun hk e.symm) (prefixPrivateFun hk e) := by
  intro q
  by_cases hq : q.val < k
  · simp [prefixPrivateFun, hq]
  · simp [prefixPrivateFun, hq, Nat.le_of_not_gt hq]

/-- Extend a prefix permutation by the identity on private positions. -/
def prefixPrivateEquiv {k n : ℕ} (hk : k ≤ n) (e : Fin k ≃ Fin k) : Fin n ≃ Fin n where
  toFun := prefixPrivateFun hk e
  invFun := prefixPrivateFun hk e.symm
  left_inv := prefixPrivateFun_left_inv hk e
  right_inv := prefixPrivateFun_right_inv hk e

/-- On the shared prefix, the extension is the given prefix permutation. -/
theorem prefixPrivateEquiv_prefix {k n : ℕ} (hk : k ≤ n) (e : Fin k ≃ Fin k)
    (q : Fin n) (hq : q.val < k) :
    prefixPrivateEquiv hk e q = Fin.castLE hk (e ⟨q.val, hq⟩) := by
  simp [prefixPrivateEquiv, prefixPrivateFun, hq]

/-- On private positions, the extension is exactly the identity. -/
theorem prefixPrivateEquiv_private {k n : ℕ} (hk : k ≤ n) (e : Fin k ≃ Fin k)
    (q : Fin n) (hq : k ≤ q.val) : prefixPrivateEquiv hk e q = q := by
  simp [prefixPrivateEquiv, prefixPrivateFun, Nat.not_lt.mpr hq]

/-- A full coordinate-position match follows from the checked prefix bijection
and the unchanged private positions. -/
theorem prefixPrivate_weightMatch {k n : ℕ} (hk : k ≤ n)
    (e : Fin k ≃ Fin k) (candidate entry : Fin n → ℕ)
    (hshared : ∀ q : Fin k, candidate (Fin.castLE hk q) = entry (Fin.castLE hk (e q)))
    (hprivate : ∀ q : Fin n, k ≤ q.val → candidate q = entry q) :
    ∀ q, candidate q = entry (prefixPrivateEquiv hk e q) := by
  intro q
  by_cases hq : q.val < k
  · let q₀ : Fin k := ⟨q.val, hq⟩
    have hqcast : Fin.castLE hk q₀ = q := by
      apply Fin.ext
      rfl
    have hEq : prefixPrivateEquiv hk e (Fin.castLE hk q₀) = Fin.castLE hk (e q₀) := by
      exact prefixPrivateEquiv_prefix hk e (Fin.castLE hk q₀) (by simpa using q₀.isLt)
    rw [← hqcast, hEq]
    exact hshared q₀
  · have hq' : k ≤ q.val := Nat.le_of_not_gt hq
    rw [prefixPrivateEquiv_private hk e q hq']
    exact hprivate q hq'

/-- A depth-indexed tuple is reindexed into descriptor-slot order.  `order`
maps each source depth to the slot visited at that depth. -/
def slotsOfDepth {α : Type*} (order : Fin R ≃ Fin R) (u : Fin R → α) : Fin R → α :=
  fun s => u (order.symm s)

theorem slotsOfDepth_injective {α : Type*} (order : Fin R ≃ Fin R) :
    Function.Injective (slotsOfDepth order (α := α)) := by
  intro u v h
  funext j
  have hj := congrFun h (order j)
  simpa [slotsOfDepth] using hj

/-- Reindexing a finite source product changes only its order. -/
theorem depth_product_eq_slots {α : Type*} (order : Fin R ≃ Fin R)
    (count : Fin R → α → ℕ) (u : Fin R → α) :
    (∏ j, count (order j) (u j)) =
      ∏ s, count s (slotsOfDepth order u s) := by
  simpa [slotsOfDepth] using order.prod_comp (fun s => count s (u (order.symm s)))

/-- Construct the full table-key tuple from the source's depth-indexed key
indices. The index lookup is by slot, even when the search order differs. -/
def tableTuple {α : Type*} (order : Fin R ≃ Fin R) (key : Fin R → α → ℕ)
    (u : Fin R → α) : Fin R → ℕ := fun s => key s (u (order.symm s))

/-- The representation map is injective when each row's stored entry-index to
key map is injective. This is the finite slot/entry-coordinate fact needed to
preserve duplicate-freedom of source leaves. -/
theorem tableTuple_injective {α : Type*} (order : Fin R ≃ Fin R)
    (key : Fin R → α → ℕ) (hkey : ∀ s a b, key s a = key s b → a = b) :
    Function.Injective (tableTuple order key) := by
  intro u v huv
  funext j
  have h := congrFun huv (order j)
  have h' : key (order j) (u j) = key (order j) (v j) := by
    simpa [tableTuple] using h
  exact hkey (order j) (u j) (v j) h'

/-- A source leaf's product of table counts is exactly the corresponding
Layout fibre product, provided each table count is the validated fibre size. -/
theorem tableTuple_count_product {L : Layout N R} (order : Fin R ≃ Fin R)
    (key : Fin R → ℕ → ℕ) (count : Fin R → ℕ → ℕ)
    (u : Fin R → ℕ)
    (hcount : ∀ s, (fibre L s (key s (u (order.symm s)))).card =
      count s (u (order.symm s))) :
    (∏ j, count (order j) (u j)) =
      ∏ s, (fibre L s (tableTuple order key u s)).card := by
  rw [depth_product_eq_slots order count u]
  apply Finset.prod_congr rfl
  intro s _
  simpa [tableTuple] using (hcount s).symm

/-- Turn a tuple list into its `Fin R`-indexed view. This definition is total;
the `length` premise below identifies all actual source leaves with the
corresponding finite tuple. -/
def tupleView (l : List ℕ) : Fin R → ℕ := fun j =>
  if h : l.length = R then l.get (Fin.cast h.symm j) else 0

theorem tupleView_injective {l l' : List ℕ} (hl : l.length = R) (hl' : l'.length = R)
    (h : tupleView (R := R) l = tupleView l') : l = l' := by
  apply List.ext_get (by rw [hl, hl'])
  intro i hi hi'
  have hh := congrFun h ⟨i, by simpa [hl] using hi⟩
  simpa [tupleView, hl, hl'] using hh

end Refpoly.JoinPoints

namespace Refpoly.JoinPoints

/-- Reindex the candidate row over its descriptor support to the table's
position order. The support and coefficient facts below are source-interface
conditions; equation equivalence itself is derived here. -/
theorem candidate_row_sum_reindex {N R : ℕ} (C : CWS R N) (L : Layout N R)
    (r : Fin R) (basePos : Fin (L.size r) → Fin N)
    (candidateWeight : Fin (L.size r) → ℕ)
    (hpos : Function.Injective basePos)
    (hweight : ∀ q, C.W r (basePos q) = (candidateWeight q : ℝ))
    (hsupport : ∀ i, (∀ q, basePos q ≠ i) → C.W r i = 0)
    (X : Fin N → ℝ) :
    (∑ i, C.W r i * X i) = ∑ q, (candidateWeight q : ℝ) * X (basePos q) := by
  classical
  let im := Finset.univ.image basePos
  have hsub : im ⊆ (Finset.univ : Finset (Fin N)) := by intro i hi; simp
  have hzero : ∀ i ∈ (Finset.univ : Finset (Fin N)), i ∉ im → C.W r i * X i = 0 := by
    intro i _ hi
    have hno : ∀ q, basePos q ≠ i := by
      intro q hq
      apply hi
      exact Finset.mem_image.mpr ⟨q, Finset.mem_univ q, hq⟩
    rw [hsupport i hno, zero_mul]
  calc
    (∑ i, C.W r i * X i) = ∑ i ∈ im, C.W r i * X i := by
      simpa [im] using (Finset.sum_subset hsub hzero).symm
    _ = ∑ q, C.W r (basePos q) * X (basePos q) := by
      exact Finset.sum_image (s := (Finset.univ : Finset (Fin (L.size r))))
        (by intro a _ b _ hab; exact hpos hab)
    _ = ∑ q, (candidateWeight q : ℝ) * X (basePos q) := by
      apply Finset.sum_congr rfl
      intro q _
      rw [hweight]

/-- The row equation seen by the candidate is equivalent to the table row
equation. `e` is the checked prefix matching extended by identity on private
positions. The point-coordinate map is the inverse permutation, so the
weight-preserving bijection acts on coefficients while preserving each
coordinate's value. -/
theorem candidate_table_row_equation {N R : ℕ} (C : CWS R N) (L : Layout N R)
    (r : Fin R) (basePos : Fin (L.size r) → Fin N)
    (entryWeight candidateWeight : Fin (L.size r) → ℕ)
    (e : Fin (L.size r) ≃ Fin (L.size r))
    (hpos_inj : Function.Injective basePos)
    (hweight : ∀ q, C.W r (basePos q) = (candidateWeight q : ℝ))
    (hsupport : ∀ i, (∀ q, basePos q ≠ i) → C.W r i = 0)
    (hmatch : ∀ q, candidateWeight q = entryWeight (e q))
    (hLayoutPos : ∀ p, L.pos r p = basePos (e.symm p))
    (hsol : ∀ x, x ∈ L.sol r ↔
      (∑ p, (entryWeight p : ℝ) * (x p : ℝ)) = ∑ p, (entryWeight p : ℝ))
    (X : Fin N → ℕ) :
    ((∑ i, C.W r i * (X i : ℝ)) = ∑ i, C.W r i) ↔
      (restr L r X ∈ L.sol r) := by
  have hcoeff (x : Fin (L.size r) → ℕ) :=
    Refpoly.JoinRejection.matched_weights_equation entryWeight candidateWeight x e hmatch
  have htotal := hcoeff (fun _ => 1)
  have hval := hcoeff (fun q => X (basePos q))
  have hglobal := candidate_row_sum_reindex C L r basePos candidateWeight
    hpos_inj hweight hsupport
  have hsum : (∑ i, C.W r i * (X i : ℝ)) =
      ∑ p, (entryWeight p : ℝ) * ((restr L r X p : ℕ) : ℝ) := by
    rw [hglobal]
    have hvalR :
        (∑ q, (candidateWeight q : ℝ) * (X (basePos q) : ℝ)) =
          ∑ p, (entryWeight p : ℝ) * (X (basePos (e.symm p)) : ℝ) := by
      exact_mod_cast hval
    calc
      (∑ q, (candidateWeight q : ℝ) * (X (basePos q) : ℝ)) =
          ∑ p, (entryWeight p : ℝ) * (X (basePos (e.symm p)) : ℝ) := hvalR
      _ = ∑ p, (entryWeight p : ℝ) * ((restr L r X p : ℕ) : ℝ) := by
        apply Finset.sum_congr rfl
        intro p _
        rw [← hLayoutPos p]
        rfl
  have htotalR : (∑ q, (candidateWeight q : ℝ)) = ∑ p, (entryWeight p : ℝ) := by
    have htotalNat : (∑ q, candidateWeight q) = ∑ p, entryWeight p := by
      simpa only [mul_one] using htotal
    exact_mod_cast htotalNat
  have hdegreeR : (∑ i, C.W r i) = ∑ p, (entryWeight p : ℝ) := by
    have hglobal1 := candidate_row_sum_reindex C L r basePos candidateWeight
      hpos_inj hweight hsupport (fun _ => 1)
    calc
      (∑ i, C.W r i) = ∑ i, C.W r i * (1 : ℝ) := by simp
      _ = ∑ q, (candidateWeight q : ℝ) * (1 : ℝ) := hglobal1
      _ = ∑ p, (entryWeight p : ℝ) := by
        simp only [mul_one]
        exact htotalR
  rw [hsum, hdegreeR, hsol]

/-- The rowwise bridge assembles into the exact `hrows` interface expected by
`leaf_rejection_sound`; no row-equivalence conclusion is taken as a premise. -/
theorem candidate_layout_rows {N R : ℕ} (C : CWS R N) (L : Layout N R)
    (basePos : ∀ r, Fin (L.size r) → Fin N)
    (entryWeight candidateWeight : ∀ r, Fin (L.size r) → ℕ)
    (e : ∀ r, Fin (L.size r) ≃ Fin (L.size r))
    (hpos_inj : ∀ r, Function.Injective (basePos r))
    (hweight : ∀ r q, C.W r (basePos r q) = (candidateWeight r q : ℝ))
    (hsupport : ∀ r i, (∀ q, basePos r q ≠ i) → C.W r i = 0)
    (hmatch : ∀ r q, candidateWeight r q = entryWeight r (e r q))
    (hLayoutPos : ∀ r p, L.pos r p = basePos r ((e r).symm p))
    (hsol : ∀ r x, x ∈ L.sol r ↔
      (∑ p, (entryWeight r p : ℝ) * (x p : ℝ)) = ∑ p, (entryWeight r p : ℝ)) :
    ∀ X, IsPoint L X ↔ ∀ r, ∑ i, C.W r i * (X i : ℝ) = ∑ i, C.W r i := by
  intro X
  simp only [IsPoint]
  constructor
  · intro hX r
    exact (candidate_table_row_equation C L r (basePos r) (entryWeight r) (candidateWeight r)
      (e r) (hpos_inj r) (hweight r) (hsupport r) (hmatch r) (hLayoutPos r)
      (hsol r) X).2 (hX r)
  · intro hrows r
    exact (candidate_table_row_equation C L r (basePos r) (entryWeight r) (candidateWeight r)
      (e r) (hpos_inj r) (hweight r) (hsupport r) (hmatch r) (hLayoutPos r)
      (hsol r) X).1 (hrows r)

/-- The complete row interface from the weight-prefix matcher. Shared matches
are exactly the CBMC-checked bijection; private positions keep their original
indices. The full row permutation and `hrows` are both constructed. -/
theorem candidate_layout_rows_prefix {N R : ℕ} (C : CWS R N) (L : Layout N R)
    (k : Fin R → ℕ) (hk : ∀ r, k r ≤ L.size r)
    (basePos : ∀ r, Fin (L.size r) → Fin N)
    (entryWeight candidateWeight : ∀ r, Fin (L.size r) → ℕ)
    (prePerm : ∀ r, Fin (k r) ≃ Fin (k r))
    (hpos_inj : ∀ r, Function.Injective (basePos r))
    (hweight : ∀ r q, C.W r (basePos r q) = (candidateWeight r q : ℝ))
    (hsupport : ∀ r i, (∀ q, basePos r q ≠ i) → C.W r i = 0)
    (hshared : ∀ r q, candidateWeight r (Fin.castLE (hk r) q) =
      entryWeight r (Fin.castLE (hk r) (prePerm r q)))
    (hprivate : ∀ r q, k r ≤ q.val → candidateWeight r q = entryWeight r q)
    (hLayoutPos : ∀ r p,
      L.pos r p = basePos r ((prefixPrivateEquiv (hk r) (prePerm r)).symm p))
    (hsol : ∀ r x, x ∈ L.sol r ↔
      (∑ p, (entryWeight r p : ℝ) * (x p : ℝ)) = ∑ p, (entryWeight r p : ℝ)) :
    ∀ X, IsPoint L X ↔ ∀ r, ∑ i, C.W r i * (X i : ℝ) = ∑ i, C.W r i := by
  apply candidate_layout_rows C L basePos entryWeight candidateWeight
    (fun r => prefixPrivateEquiv (hk r) (prePerm r)) hpos_inj hweight hsupport
  · intro r q
    exact prefixPrivate_weightMatch (hk r) (prePerm r) (candidateWeight r) (entryWeight r)
      (hshared r) (hprivate r) q
  · exact hLayoutPos
  · exact hsol

end Refpoly.JoinPoints

namespace Refpoly.JoinLoopSource

/-- A validated finite permutation of search depths onto the descriptor slots. -/
structure SlotOrder (E : Env) where
  order : Fin E.R ≃ Fin E.R
  slot_eq : ∀ j, E.slot j.val = (order j).val

/-- Source choices are table indices by depth; this is their actual table
index validity condition. -/
def SourceIndicesValid {E : Env} (O : SlotOrder E) (u : Fin E.R → ℕ) : Prop :=
  ∀ j, E.kb (O.order j).val ≤ u j ∧ u j < E.ke (O.order j).val

/-- Convert a depth-indexed source leaf to slot-indexed table key words. -/
def sourceTableTuple {E : Env} (O : SlotOrder E) (u : Fin E.R → ℕ) : Fin E.R → ℕ :=
  JoinPoints.tableTuple O.order (fun s i => E.key s.val i) u

/-- The table's strict key ordering makes source index decoding injective on
valid entry ranges, even if values outside the selected table range are junk. -/
theorem sourceTableTuple_injOn {E : Env} (O : SlotOrder E) (hT : TablesOK E) :
    Set.InjOn (sourceTableTuple O) {u | SourceIndicesValid O u} := by
  intro u hu v hv heq
  funext j
  have hkey : E.key (O.order j).val (u j) = E.key (O.order j).val (v j) := by
    have h := congrFun heq (O.order j)
    simpa [sourceTableTuple, JoinPoints.tableTuple] using h
  have hu' : E.kb (O.order j).val ≤ u j ∧ u j < E.ke (O.order j).val := by
    exact hu j
  have hv' : E.kb (O.order j).val ≤ v j ∧ v j < E.ke (O.order j).val := by
    exact hv j
  by_contra hne
  rcases lt_or_gt_of_ne hne with hlt | hgt
  · have hs := hT.strict (O.order j).val (u j) (v j) hu'.1 hlt hv'.2
    omega
  · have hs := hT.strict (O.order j).val (v j) (u j) hv'.1 hgt hu'.2
    omega

/-- The chosen list emitted at a source leaf has exactly one entry per depth;
`crun_leaf_values` supplies the source-state witness and key fields. -/
theorem crun_leaf_length {E : Env} (hP : PlanOK E) (hR : 0 < E.R)
    (val it en : ℕ → ℕ) (ps : ℕ → Option ℕ) (chosen : ℕ → ℕ)
    (fuel : ℕ) (tuple : List ℕ)
    (ht : tuple ∈ crun E fuel (initial E val it en ps chosen)) : tuple.length = E.R := by
  obtain ⟨leaf, _, htuple, _⟩ := crun_leaf_values hP hR val it en ps chosen fuel tuple ht
  rw [htuple]
  simp

/-- The explicit index-to-fibre facts that a source-pinned table validator
must provide for a `JoinPoints.Layout`. -/
structure FibreLayoutInterface {N : ℕ} (E : Env) (L : JoinPoints.Layout N E.R)
    (O : SlotOrder E) (count : Fin E.R → ℕ → ℕ) : Prop where
  key_mem : ∀ s i, E.kb s.val ≤ i → i < E.ke s.val →
    E.key s.val i ∈ L.keys s
  fibre_count : ∀ s i, E.kb s.val ≤ i → i < E.ke s.val →
    (JoinPoints.fibre L s (E.key s.val i)).card = count s i
  key_fields : ∀ s i (p : Fin (L.size s)), E.kb s.val ≤ i → i < E.ke s.val →
    p.val < L.k s → L.fields s (E.key s.val i) p = field (E.k s.val) p.val (E.key s.val i)

/-- Once validator counts are identified with Layout fibre cardinalities, a
source leaf's checked product is the exact Layout block size. -/
theorem source_leaf_count_product {N : ℕ} {E : Env} {L : JoinPoints.Layout N E.R}
    (O : SlotOrder E) (count : Fin E.R → ℕ → ℕ)
    (hI : FibreLayoutInterface E L O count) (u : Fin E.R → ℕ)
    (hvalid : SourceIndicesValid O u)
    :
    (∏ j, count (O.order j) (u j)) =
      ∏ s, (JoinPoints.fibre L s (sourceTableTuple O u s)).card := by
  apply JoinPoints.tableTuple_count_product O.order (fun s i => E.key s.val i) count u
  intro s
  have hv := hvalid (O.order.symm s)
  have hslot : E.slot (O.order.symm s).val = s.val := by
    rw [O.slot_eq]
    simp
  have hv' : E.kb s.val ≤ u (O.order.symm s) ∧ u (O.order.symm s) < E.ke s.val := by
    simpa [hslot] using hv
  exact hI.fibre_count s _ hv'.1 hv'.2

/-- Every decoded source leaf tuple uses stored Layout keys, under the
validated table-range condition. -/
theorem source_leaf_keys_mem {N : ℕ} {E : Env} {L : JoinPoints.Layout N E.R}
    (O : SlotOrder E) (count : Fin E.R → ℕ → ℕ)
    (hI : FibreLayoutInterface E L O count) (u : Fin E.R → ℕ)
    (hu : SourceIndicesValid O u) : ∀ s, sourceTableTuple O u s ∈ L.keys s := by
  intro s
  have hv := hu (O.order.symm s)
  have hslot : E.slot (O.order.symm s).val = s.val := by
    rw [O.slot_eq]
    simp
  have hv' : E.kb s.val ≤ u (O.order.symm s) ∧ u (O.order.symm s) < E.ke s.val := by
    simpa [hslot] using hv
  exact hI.key_mem s _ hv'.1 hv'.2

end Refpoly.JoinLoopSource
