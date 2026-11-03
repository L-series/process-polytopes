import Refpoly.JoinSearch

/-! Exact explicit-continuation traversal and mutable-value invariants.

This module removes the bounded-table restriction from the stack *model*.
The concrete CUDA arrays and loop control still require a machine-state
simulation into this model; no such simulation is assumed as an axiom. -/
namespace Refpoly.JoinRejection

inductive JoinSearchTree (α : Type*) where
  | empty
  | leaf (value : α)
  | fork (first rest : JoinSearchTree α)

def JoinSearchTree.leaves {α : Type*} : JoinSearchTree α → List α
  | .empty => []
  | .leaf a => [a]
  | .fork first rest => first.leaves ++ rest.leaves

def JoinSearchTree.work {α : Type*} : JoinSearchTree α → Nat
  | .empty => 1
  | .leaf _ => 1
  | .fork first rest => 1 + first.work + rest.work

def joinStackWork {α : Type*} (stack : List (JoinSearchTree α)) : Nat :=
  (stack.map JoinSearchTree.work).sum

def joinStackLeaves {α : Type*} (stack : List (JoinSearchTree α)) : List α :=
  stack.flatMap JoinSearchTree.leaves

/-- Pop an exhausted branch, emit a leaf, or push the next branch before its
remaining siblings. This is an explicit stack, not recursive tree traversal. -/
def runJoinStack {α : Type*} : Nat → List (JoinSearchTree α) → List α
  | 0, _ => []
  | _ + 1, [] => []
  | fuel + 1, .empty :: stack => runJoinStack fuel stack
  | fuel + 1, .leaf a :: stack => a :: runJoinStack fuel stack
  | fuel + 1, .fork first rest :: stack => runJoinStack fuel (first :: rest :: stack)

/-- Every leaf is emitted exactly as often, and in the same order, as in the
recursive specification. Equality of lists preserves identities/multiplicity;
this is stronger than equality of counts or finite sets. -/
theorem runJoinStack_exact {α : Type*} (fuel : Nat) (stack : List (JoinSearchTree α))
    (enough : joinStackWork stack ≤ fuel) :
    runJoinStack fuel stack = joinStackLeaves stack := by
  induction fuel generalizing stack with
  | zero =>
    cases stack with
    | nil => rfl
    | cons tree stack =>
      cases tree <;> simp [joinStackWork, JoinSearchTree.work] at enough
  | succ fuel ih =>
    cases stack with
    | nil => rfl
    | cons tree stack =>
      cases tree with
      | empty =>
        have next : joinStackWork stack ≤ fuel := by
          simp [joinStackWork, JoinSearchTree.work] at enough ⊢
          omega
        simpa [runJoinStack, joinStackLeaves, JoinSearchTree.leaves] using ih stack next
      | leaf a =>
        have next : joinStackWork stack ≤ fuel := by
          simp [joinStackWork, JoinSearchTree.work] at enough ⊢
          omega
        simpa [runJoinStack, joinStackLeaves, JoinSearchTree.leaves] using
          congrArg (List.cons a) (ih stack next)
      | fork first rest =>
        have next : joinStackWork (first :: rest :: stack) ≤ fuel := by
          simp [joinStackWork, JoinSearchTree.work] at enough ⊢
          omega
        simpa [runJoinStack, joinStackLeaves, JoinSearchTree.leaves, List.append_assoc] using
          ih (first :: rest :: stack) next

/-- Exact leaf identities immediately give exact weighted accumulation, for
arbitrary (unbounded) natural fibre products. Machine overflow guards remain
an additional obligation. -/
theorem runJoinStack_weighted_exact {α : Type*} (fuel : Nat)
    (stack : List (JoinSearchTree α)) (weight : α → Nat)
    (enough : joinStackWork stack ≤ fuel) :
    ((runJoinStack fuel stack).map weight).sum =
      ((joinStackLeaves stack).map weight).sum := by
  rw [runJoinStack_exact fuel stack enough]

/-- Change emitted values without changing branch structure or order. -/
def JoinSearchTree.map {α β : Type*} (f : α → β) : JoinSearchTree α → JoinSearchTree β
  | .empty => .empty
  | .leaf a => .leaf (f a)
  | .fork first rest => .fork (first.map f) (rest.map f)

theorem joinTree_leaves_map {α β : Type*} (f : α → β) (tree : JoinSearchTree α) :
    (tree.map f).leaves = tree.leaves.map f := by
  induction tree with
  | empty => rfl
  | leaf a => rfl
  | fork first rest ih₁ ih₂ => simp [JoinSearchTree.map, JoinSearchTree.leaves, ih₁, ih₂]

/-- A row-key DFS: `extend` performs compatibility checking and installs the
row's newly bound values. Keys retain their identities in the emitted tuple. -/
def joinChoiceTree {σ α : Type*} (extend : σ → α → Option σ) :
    σ → List (List α) → JoinSearchTree (List α)
  | _, [] => .leaf []
  | state, row :: rows => row.foldr (fun key siblings =>
      .fork (match extend state key with
        | none => .empty
        | some next => (joinChoiceTree extend next rows).map (List.cons key)) siblings) .empty

def joinChoiceList {σ α : Type*} (extend : σ → α → Option σ) :
    σ → List (List α) → List (List α)
  | _, [] => [[]]
  | state, row :: rows => row.flatMap fun key =>
      match extend state key with
      | none => []
      | some next => (joinChoiceList extend next rows).map (List.cons key)

/-- Defunctionalization into the stack tree preserves the entire ordered list
of compatible row-key tuples, including their multiplicity. -/
theorem joinChoiceTree_exact {σ α : Type*} (extend : σ → α → Option σ)
    (state : σ) (rows : List (List α)) :
    (joinChoiceTree extend state rows).leaves = joinChoiceList extend state rows := by
  induction rows generalizing state with
  | nil => rfl
  | cons row rows ih =>
    simp only [joinChoiceTree, joinChoiceList]
    induction row with
    | nil => rfl
    | cons key row ihr =>
      cases he : extend state key with
      | none => simpa [List.foldr_cons, List.flatMap_cons, JoinSearchTree.leaves, he] using ihr
      | some next =>
        simpa [List.foldr_cons, List.flatMap_cons, JoinSearchTree.leaves, he,
          joinTree_leaves_map, ih] using congrArg
            (fun xs => (joinChoiceList extend next rows).map (List.cons key) ++ xs) ihr

/-- End-to-end theorem for the explicit stack model, at arbitrary row/table
sizes. Its assumptions concern fuel only, not enumeration completeness. -/
theorem joinChoiceStack_exact {σ α : Type*} (extend : σ → α → Option σ)
    (state : σ) (rows : List (List α)) :
    runJoinStack (joinChoiceTree extend state rows).work [joinChoiceTree extend state rows] =
      joinChoiceList extend state rows := by
  rw [runJoinStack_exact]
  · simpa [joinStackLeaves] using joinChoiceTree_exact extend state rows
  · simp [joinStackWork]


def cartesianJoinChoices {α : Type*} : List (List α) → List (List α)
  | [] => [[]]
  | row :: rows => row.flatMap fun key => (cartesianJoinChoices rows).map (List.cons key)

def validJoinChoices {σ α : Type*} (extend : σ → α → Option σ) : σ → List α → Bool
  | _, [] => true
  | state, key :: keys => match extend state key with
    | none => false
    | some next => validJoinChoices extend next keys

/-- The recursive specification is independently characterized as filtering
all Cartesian row-key tuples. In particular it cannot omit a compatible tuple. -/
theorem joinChoiceList_cartesian {σ α : Type*} (extend : σ → α → Option σ)
    (state : σ) (rows : List (List α)) :
    joinChoiceList extend state rows =
      (cartesianJoinChoices rows).filter (validJoinChoices extend state) := by
  induction rows generalizing state with
  | nil => rfl
  | cons row rows ih =>
    simp only [joinChoiceList, cartesianJoinChoices, List.filter_flatMap]
    apply List.flatMap_congr
    intro key hkey
    cases he : extend state key with
    | none => simp [List.filter_map, validJoinChoices, he]
    | some next => simp [List.filter_map, validJoinChoices, he, ih, Function.comp_def]

theorem cartesianJoinChoices_nodup {α : Type*} (rows : List (List α))
    (distinct : ∀ row ∈ rows, row.Nodup) : (cartesianJoinChoices rows).Nodup := by
  induction rows with
  | nil => simp [cartesianJoinChoices]
  | cons row rows ih =>
    have hr := distinct row (by simp)
    have ht := ih (fun r h => distinct r (by simp [h]))
    have prod := hr.product ht
    have injective : Function.Injective (fun pair : α × List α => pair.1 :: pair.2) := by
      intro x y h
      exact Prod.ext (List.cons.inj h).1 (List.cons.inj h).2
    have mapped := prod.map injective
    have representation : (row ×ˢ cartesianJoinChoices rows).map
        (fun pair : α × List α => pair.1 :: pair.2) = cartesianJoinChoices (row :: rows) := by
      simp only [cartesianJoinChoices]
      clear distinct hr prod mapped
      induction row with
      | nil => rfl
      | cons key tail iht =>
        simpa [List.product_cons, List.flatMap_cons, List.map_map, Function.comp_def] using
          congrArg (fun xs => (cartesianJoinChoices rows).map (List.cons key) ++ xs) iht
    rw [← representation]
    exact mapped

/-- With unique row keys, the complete DFS tuple list has no duplicates. -/
theorem joinChoiceList_nodup {σ α : Type*} (extend : σ → α → Option σ)
    (state : σ) (rows : List (List α)) (distinct : ∀ row ∈ rows, row.Nodup) :
    (joinChoiceList extend state rows).Nodup := by
  rw [joinChoiceList_cartesian]
  exact (cartesianJoinChoices_nodup rows distinct).filter _


/-- Source-shaped row frame: row iterator suffix, remaining rows, previously
selected key identities and the already bound assignment projection. -/
structure JoinRowFrame (σ α : Type*) where
  state : σ
  pending : List α
  rows : List (List α)
  chosen : List α

def joinFrameLeaves {σ α : Type*} (extend : σ → α → Option σ)
    (frame : JoinRowFrame σ α) : List (List α) :=
  frame.pending.flatMap fun key => match extend frame.state key with
    | none => []
    | some next => (joinChoiceList extend next frame.rows).map
        (fun suffix => frame.chosen ++ key :: suffix)

def joinFrameStackLeaves {σ α : Type*} (extend : σ → α → Option σ)
    (stack : List (JoinRowFrame σ α)) : List (List α) :=
  stack.flatMap (joinFrameLeaves extend)

/-- One outer-loop transition: pop on exhaustion, otherwise advance the
current cursor exactly once, then skip, emit, or push a child row frame. -/
def stepJoinFrames {σ α : Type*} (extend : σ → α → Option σ) :
    List (JoinRowFrame σ α) → List (List α) × List (JoinRowFrame σ α)
  | [] => ([], [])
  | frame :: stack => match frame.pending with
    | [] => ([], stack)
    | key :: pending =>
      let parent := { frame with pending := pending }
      match extend frame.state key with
      | none => ([], parent :: stack)
      | some next => match frame.rows with
        | [] => ([frame.chosen ++ [key]], parent :: stack)
        | row :: rows =>
          ([], ⟨next, row, rows, frame.chosen ++ [key]⟩ :: parent :: stack)

/-- The source-shaped advance/push/pop transition preserves the complete
remaining tuple list. This is an identity theorem, not a counter equality. -/
theorem stepJoinFrames_exact {σ α : Type*} (extend : σ → α → Option σ)
    (stack : List (JoinRowFrame σ α)) :
    (stepJoinFrames extend stack).1 ++
      joinFrameStackLeaves extend (stepJoinFrames extend stack).2 =
        joinFrameStackLeaves extend stack := by
  cases stack with
  | nil => rfl
  | cons frame stack =>
    rcases frame with ⟨state, pending, rows, chosen⟩
    cases pending with
    | nil => rfl
    | cons key pending =>
      cases he : extend state key with
      | none => simp [stepJoinFrames, joinFrameStackLeaves, joinFrameLeaves, he]
      | some next =>
        cases rows with
        | nil => simp [stepJoinFrames, joinFrameStackLeaves, joinFrameLeaves, joinChoiceList, he]
        | cons row rows =>
          simp [stepJoinFrames, joinFrameStackLeaves, joinFrameLeaves, joinChoiceList, he,
            List.map_flatMap, List.append_assoc]
          apply List.flatMap_congr
          intro key hkey
          cases extend next key <;> simp [List.map_map, Function.comp_def]

def joinFrameWork {σ α : Type*} (extend : σ → α → Option σ) :
    List (List α) → σ → List α → Nat
  | [], _, pending => 1 + pending.length
  | row :: rows, state, pending => 1 + (pending.map fun key =>
      match extend state key with
      | none => 1
      | some next => 1 + joinFrameWork extend rows next row).sum

def joinFrameStackWork {σ α : Type*} (extend : σ → α → Option σ)
    (stack : List (JoinRowFrame σ α)) : Nat :=
  (stack.map fun frame => joinFrameWork extend frame.rows frame.state frame.pending).sum

/-- Every nonterminal source-shaped transition decreases a natural work
measure by exactly one, independently of weights or table sizes. -/
theorem stepJoinFrames_work {σ α : Type*} (extend : σ → α → Option σ)
    (frame : JoinRowFrame σ α) (stack : List (JoinRowFrame σ α)) :
    joinFrameStackWork extend (stepJoinFrames extend (frame :: stack)).2 + 1 =
      joinFrameStackWork extend (frame :: stack) := by
  rcases frame with ⟨state, pending, rows, chosen⟩
  cases pending with
  | nil => cases rows <;> simp [stepJoinFrames, joinFrameStackWork, joinFrameWork, Nat.add_comm]
  | cons key pending =>
    cases he : extend state key with
    | none =>
      cases rows <;> simp [stepJoinFrames, joinFrameStackWork, joinFrameWork, he,
        Nat.add_assoc, Nat.add_comm, Nat.add_left_comm]
      all_goals omega
    | some next =>
      cases rows <;> simp [stepJoinFrames, joinFrameStackWork, joinFrameWork, he,
        Nat.add_assoc, Nat.add_comm, Nat.add_left_comm]
      all_goals omega

def runJoinFrames {σ α : Type*} (extend : σ → α → Option σ) :
    Nat → List (JoinRowFrame σ α) → List (List α)
  | 0, _ => []
  | _ + 1, [] => []
  | fuel + 1, frame :: stack =>
    let step := stepJoinFrames extend (frame :: stack)
    step.1 ++ runJoinFrames extend fuel step.2

/-- Complete ordered enumeration by the source-shaped iterative frame machine,
for arbitrary finite row tables, with no table-length restriction. -/
theorem runJoinFrames_exact {σ α : Type*} (extend : σ → α → Option σ)
    (fuel : Nat) (stack : List (JoinRowFrame σ α))
    (enough : joinFrameStackWork extend stack ≤ fuel) :
    runJoinFrames extend fuel stack = joinFrameStackLeaves extend stack := by
  induction fuel generalizing stack with
  | zero =>
    cases stack with
    | nil => rfl
    | cons frame stack =>
      rcases frame with ⟨state, pending, rows, chosen⟩
      cases rows <;> simp [joinFrameStackWork, joinFrameWork] at enough
  | succ fuel ih =>
    cases stack with
    | nil => rfl
    | cons frame stack =>
      have decreases := stepJoinFrames_work extend frame stack
      have next : joinFrameStackWork extend (stepJoinFrames extend (frame :: stack)).2 ≤ fuel := by omega
      simpa only [runJoinFrames, ih _ next] using stepJoinFrames_exact extend (frame :: stack)


/-- Initializing the first row frame enumerates exactly the Cartesian-product
compatibility specification proved independently above. -/
theorem initialJoinFrames_exact {σ α : Type*} (extend : σ → α → Option σ)
    (state : σ) (row : List α) (rows : List (List α)) :
    let initial : JoinRowFrame σ α := ⟨state, row, rows, []⟩
    runJoinFrames extend (joinFrameStackWork extend [initial]) [initial] =
      (cartesianJoinChoices (row :: rows)).filter (validJoinChoices extend state) := by
  dsimp only
  rw [runJoinFrames_exact extend _ _ (le_refl _)]
  rw [← joinChoiceList_cartesian]
  simp [joinFrameStackLeaves, joinFrameLeaves, joinChoiceList]

theorem initialJoinFrames_nodup {σ α : Type*} (extend : σ → α → Option σ)
    (state : σ) (row : List α) (rows : List (List α))
    (distinct : ∀ r ∈ row :: rows, r.Nodup) :
    let initial : JoinRowFrame σ α := ⟨state, row, rows, []⟩
    (runJoinFrames extend (joinFrameStackWork extend [initial]) [initial]).Nodup := by
  dsimp only
  rw [initialJoinFrames_exact, ← joinChoiceList_cartesian]
  exact joinChoiceList_nodup extend state (row :: rows) distinct

/-- Exact sum of fibre products for the source-shaped frame machine. No
cancellation between omitted and repeated leaves is hidden by this theorem:
`initialJoinFrames_exact` establishes equality of complete key-identity lists. -/
theorem initialJoinFrames_count {σ α : Type*} (extend : σ → α → Option σ)
    (state : σ) (row : List α) (rows : List (List α)) (fibreCount : α → Nat) :
    let initial : JoinRowFrame σ α := ⟨state, row, rows, []⟩
    ((runJoinFrames extend (joinFrameStackWork extend [initial]) [initial]).map
      (fun keys => (keys.map fibreCount).prod)).sum =
    (((cartesianJoinChoices (row :: rows)).filter (validJoinChoices extend state)).map
      (fun keys => (keys.map fibreCount).prod)).sum := by
  dsimp only
  rw [initialJoinFrames_exact]


/-- Decode the concrete indexed frame slots in active-depth order. The source
uses depth=-1 to terminate; `active` here is its nonnegative `depth+1`. -/
def decodeJoinSlots {σ α : Type*} (slots : Nat → JoinRowFrame σ α) (active : Nat) :
    List (JoinRowFrame σ α) := ((List.range active).reverse).map slots

theorem decodeJoinSlots_succ {σ α : Type*} (slots : Nat → JoinRowFrame σ α) (depth : Nat) :
    decodeJoinSlots slots (depth + 1) = slots depth :: decodeJoinSlots slots depth := by
  simp [decodeJoinSlots, List.range_succ]

theorem decodeJoinSlots_update_above {σ α : Type*} (slots : Nat → JoinRowFrame σ α)
    (active index : Nat) (value : JoinRowFrame σ α) (above : active ≤ index) :
    decodeJoinSlots (Function.update slots index value) active = decodeJoinSlots slots active := by
  simp only [decodeJoinSlots]
  apply List.map_congr_left
  intro i hi
  have hir : i < active := List.mem_range.mp (List.mem_reverse.mp hi)
  exact Function.update_of_ne (by omega) _ _

theorem decodeJoinSlots_push {σ α : Type*} (slots : Nat → JoinRowFrame σ α)
    (active : Nat) (child : JoinRowFrame σ α) :
    decodeJoinSlots (Function.update slots active child) (active + 1) =
      child :: decodeJoinSlots slots active := by
  rw [decodeJoinSlots_succ]
  simp only [Function.update_self]
  rw [decodeJoinSlots_update_above slots active active child (le_refl _)]

/-- Concrete slot/depth control, using the same indexed parent update, child
write and depth increment/decrement as the source. Iterator advancement and
compatibility are represented by the verified row suffix/extend interface. -/
def stepJoinSlots {σ α : Type*} (extend : σ → α → Option σ)
    (slots : Nat → JoinRowFrame σ α) : Nat → List (List α) × (Nat → JoinRowFrame σ α) × Nat
  | 0 => ([], slots, 0)
  | depth + 1 =>
    let frame := slots depth
    match frame.pending with
    | [] => ([], slots, depth)
    | key :: pending =>
      let parent := {frame with pending := pending}
      let advanced := Function.update slots depth parent
      match extend frame.state key with
      | none => ([], advanced, depth + 1)
      | some next => match frame.rows with
        | [] => ([frame.chosen ++ [key]], advanced, depth + 1)
        | row :: rows => ([], Function.update advanced (depth + 1)
            ⟨next, row, rows, frame.chosen ++ [key]⟩, depth + 2)

/-- Per-branch concrete array-slot control refinement. No enumeration or
rejection conclusion is assumed; this is equality with the stack transition. -/
theorem stepJoinSlots_refines {σ α : Type*} (extend : σ → α → Option σ)
    (slots : Nat → JoinRowFrame σ α) (active : Nat) :
    let concrete := stepJoinSlots extend slots active
    (concrete.1, decodeJoinSlots concrete.2.1 concrete.2.2) =
      stepJoinFrames extend (decodeJoinSlots slots active) := by
  cases active with
  | zero => rfl
  | succ depth =>
    rw [decodeJoinSlots_succ]
    generalize hf : slots depth = frame
    rcases frame with ⟨state, pending, rows, chosen⟩
    cases pending with
    | nil => simp [stepJoinSlots, hf, stepJoinFrames]
    | cons key pending =>
      cases he : extend state key with
      | none => simp [stepJoinSlots, hf, stepJoinFrames, he, decodeJoinSlots_push]
      | some next =>
        cases rows with
        | nil => simp [stepJoinSlots, hf, stepJoinFrames, he, decodeJoinSlots_push]
        | cons row rows =>
          simp [stepJoinSlots, hf, stepJoinFrames, he, decodeJoinSlots_push]


/-- The frame rows remaining at depth d determine the fixed source capacity. -/
def JoinSlotsValid {σ α : Type*} (capacity : Nat)
    (slots : Nat → JoinRowFrame σ α) (active : Nat) : Prop :=
  active ≤ capacity ∧ ∀ d, d < active → (slots d).rows.length + d + 1 = capacity

/-- A child-slot write stays inside the fixed allocation: nonempty remaining
rows imply a strictly smaller depth than the row count. -/
theorem stepJoinSlots_capacity {σ α : Type*} (extend : σ → α → Option σ)
    (slots : Nat → JoinRowFrame σ α) (active capacity : Nat)
    (valid : JoinSlotsValid capacity slots active) :
    (stepJoinSlots extend slots active).2.2 ≤ capacity := by
  obtain ⟨bound, rowsLeft⟩ := valid
  cases active with
  | zero => simp [stepJoinSlots]
  | succ depth =>
    have remaining := rowsLeft depth (by omega)
    generalize hf : slots depth = frame at *
    rcases frame with ⟨state, pending, rows, chosen⟩
    cases pending with
    | nil => simp [stepJoinSlots, hf]; omega
    | cons key pending =>
      cases he : extend state key with
      | none => simpa [stepJoinSlots, hf, he] using bound
      | some next =>
        cases rows with
        | nil => simpa [stepJoinSlots, hf, he] using bound
        | cons row rows =>
          simp only [List.length_cons] at remaining
          simp [stepJoinSlots, hf, he]
          omega

/-- Mutable row matching, including the source's short-circuit failure and the
partial writes to unbound coordinates before that failure. -/
def matchRowFields {n : Nat} {β : Type*} [DecidableEq β] (bound : Finset (Fin n)) :
    List (Fin n × β) → (Fin n → β) → Bool × (Fin n → β)
  | [], values => (true, values)
  | (a, value) :: fields, values =>
    if a ∈ bound then
      if values a = value then matchRowFields bound fields values else (false, values)
    else matchRowFields bound fields (Function.update values a value)

/-- Every bound coordinate survives both successful and unsuccessful matches.
This is the invariant that makes stale unbound values harmless on backtracking. -/
theorem matchRowFields_bound_preserved {n : Nat} {β : Type*} [DecidableEq β]
    (bound : Finset (Fin n)) (fields : List (Fin n × β)) (values : Fin n → β)
    (a : Fin n) (ha : a ∈ bound) :
    (matchRowFields bound fields values).2 a = values a := by
  induction fields generalizing values with
  | nil => rfl
  | cons field fields ih =>
    obtain ⟨b, value⟩ := field
    by_cases hb : b ∈ bound
    · by_cases hv : values b = value
      · simpa [matchRowFields, hb, hv] using ih values
      · simp [matchRowFields, hb, hv]
    · have hab : a ≠ b := by rintro rfl; exact hb ha
      simpa [matchRowFields, hb, Function.update_of_ne hab] using
        ih (Function.update values b value)

/-- Coordinates not mentioned by the row are unchanged, even on failure. -/
theorem matchRowFields_unwritten {n : Nat} {β : Type*} [DecidableEq β]
    (bound : Finset (Fin n)) (fields : List (Fin n × β)) (values : Fin n → β)
    (a : Fin n) (ha : a ∉ fields.map Prod.fst) :
    (matchRowFields bound fields values).2 a = values a := by
  induction fields generalizing values with
  | nil => rfl
  | cons field fields ih =>
    obtain ⟨b, value⟩ := field
    have hsplit : a ≠ b ∧ a ∉ fields.map Prod.fst := by simpa using ha
    obtain ⟨hab, hat⟩ := hsplit
    by_cases hb : b ∈ bound
    · by_cases hv : values b = value
      · simpa [matchRowFields, hb, hv] using ih values hat
      · simp [matchRowFields, hb, hv]
    · simpa [matchRowFields, hb, Function.update_of_ne hab] using
        ih (Function.update values b value) hat

def BoundFieldsAgree {n : Nat} {β : Type*} (bound : Finset (Fin n))
    (values : Fin n → β) : List (Fin n × β) → Prop
  | [] => True
  | (a, value) :: fields => (a ∈ bound → values a = value) ∧ BoundFieldsAgree bound values fields

theorem boundFieldsAgree_update_unbound {n : Nat} {β : Type*} [DecidableEq β]
    (bound : Finset (Fin n)) (fields : List (Fin n × β)) (values : Fin n → β)
    (a : Fin n) (value : β) (ha : a ∉ bound) :
    BoundFieldsAgree bound (Function.update values a value) fields ↔
      BoundFieldsAgree bound values fields := by
  induction fields with
  | nil => rfl
  | cons field fields ih =>
    obtain ⟨b, v⟩ := field
    by_cases hb : b ∈ bound
    · have hba : b ≠ a := by rintro rfl; exact ha hb
      simp [BoundFieldsAgree, Function.update_of_ne hba, ih]
    · simp [BoundFieldsAgree, hb, ih]

/-- Success tests only already bound values: arbitrary stale values in new
coordinates cannot suppress a compatible row. -/
theorem matchRowFields_success_iff {n : Nat} {β : Type*} [DecidableEq β]
    (bound : Finset (Fin n)) (fields : List (Fin n × β)) (values : Fin n → β) :
    (matchRowFields bound fields values).1 = true ↔ BoundFieldsAgree bound values fields := by
  induction fields generalizing values with
  | nil => simp [matchRowFields, BoundFieldsAgree]
  | cons field fields ih =>
    obtain ⟨a, value⟩ := field
    by_cases ha : a ∈ bound
    · by_cases hv : values a = value
      · simpa [matchRowFields, BoundFieldsAgree, ha, hv] using ih values
      · simp [matchRowFields, BoundFieldsAgree, ha, hv]
    · simpa [matchRowFields, BoundFieldsAgree, ha,
        boundFieldsAgree_update_unbound bound fields values a value ha] using
        ih (Function.update values a value)

/-- On success, every coordinate of a row with distinct coordinate positions
has its exact key value. A later failed match cannot affect earlier bound rows. -/
theorem matchRowFields_installs_row {n : Nat} {β : Type*} [DecidableEq β]
    (bound : Finset (Fin n)) (fields : List (Fin n × β)) (values : Fin n → β)
    (distinct : (fields.map Prod.fst).Nodup)
    (ok : (matchRowFields bound fields values).1 = true) :
    ∀ a value, (a, value) ∈ fields → (matchRowFields bound fields values).2 a = value := by
  induction fields generalizing values with
  | nil => simp
  | cons field fields ih =>
    obtain ⟨b, v⟩ := field
    obtain ⟨hnot, hdistinct⟩ := List.nodup_cons.mp distinct
    by_cases hb : b ∈ bound
    · by_cases hv : values b = v
      · have tailOk : (matchRowFields bound fields values).1 = true := by
          simpa [matchRowFields, hb, hv] using ok
        intro a value hmem
        rcases List.mem_cons.mp hmem with heq | htail
        · cases heq
          simpa [matchRowFields, hb, hv] using
            (matchRowFields_unwritten bound fields values b hnot).trans hv
        · simpa [matchRowFields, hb, hv] using ih values hdistinct tailOk a value htail
      · simp [matchRowFields, hb, hv] at ok
    · have tailOk : (matchRowFields bound fields (Function.update values b v)).1 = true := by
        simpa [matchRowFields, hb] using ok
      intro a value hmem
      rcases List.mem_cons.mp hmem with heq | htail
      · cases heq
        simpa [matchRowFields, hb] using
          matchRowFields_unwritten bound fields (Function.update values b v) b hnot
      · simpa [matchRowFields, hb] using
          ih (Function.update values b v) hdistinct tailOk a value htail

/-- Reading only the bound projection makes a row's accept/skip decision
independent of every stale value left by a sibling or deeper search. -/
theorem matchRowFields_stale_independent {n : Nat} {β : Type*} [DecidableEq β]
    (bound : Finset (Fin n)) (fields : List (Fin n × β)) (left right : Fin n → β)
    (same : ∀ a ∈ bound, left a = right a) :
    (matchRowFields bound fields left).1 = (matchRowFields bound fields right).1 := by
  apply Bool.eq_iff_iff.mpr
  simp only [matchRowFields_success_iff]
  induction fields with
  | nil => rfl
  | cons field fields ih =>
    obtain ⟨a, value⟩ := field
    by_cases ha : a ∈ bound
    · simp [BoundFieldsAgree, ha, same a ha, ih]
    · simp [BoundFieldsAgree, ha, ih]

/-- The source stores all coordinates in one mutable array. The decoded frame
state stores only coordinates bound at that depth, erasing every stale value. -/
def boundSnapshot {n : Nat} {β : Type*} (bound : Finset (Fin n))
    (values : Fin n → β) : Fin n → Option β :=
  fun a => if a ∈ bound then some (values a) else none

theorem matchRowFields_snapshot_preserved {n : Nat} {β : Type*} [DecidableEq β]
    (bound : Finset (Fin n)) (fields : List (Fin n × β)) (values : Fin n → β) :
    boundSnapshot bound (matchRowFields bound fields values).2 = boundSnapshot bound values := by
  funext a
  by_cases ha : a ∈ bound
  · simp [boundSnapshot, ha, matchRowFields_bound_preserved bound fields values a ha]
  · simp [boundSnapshot, ha]

/-- Decode a successful mutable row match as the pure partial-assignment
extension used by the stack model. `row` is just the row-key field encoding. -/
theorem matchRowFields_snapshot_extension {n : Nat} {β : Type*} [DecidableEq β]
    (bound : Finset (Fin n)) (fields : List (Fin n × β)) (values : Fin n → β)
    (row : Fin n → Option β) (encoding : ∀ a value, row a = some value ↔ (a, value) ∈ fields)
    (distinct : (fields.map Prod.fst).Nodup)
    (ok : (matchRowFields bound fields values).1 = true) :
    boundSnapshot (bound ∪ (fields.map Prod.fst).toFinset) (matchRowFields bound fields values).2 =
      fun a => (boundSnapshot bound values a).or (row a) := by
  funext a
  by_cases ha : a ∈ bound
  · simp [boundSnapshot, ha, matchRowFields_bound_preserved bound fields values a ha]
  · by_cases hr : a ∈ fields.map Prod.fst
    · obtain ⟨⟨b, value⟩, hmem, hcoord⟩ := List.mem_map.mp hr
      dsimp at hcoord
      subst b
      have hrow := (encoding a value).mpr hmem
      have hvalue := matchRowFields_installs_row bound fields values distinct ok a value hmem
      simp [boundSnapshot, ha, hr, hrow, hvalue]
    · have hrow : row a = none := by
        cases he : row a with
        | none => rfl
        | some value =>
          have hm := (encoding a value).mp he
          exact False.elim (hr (List.mem_map.mpr ⟨(a, value), hm, rfl⟩))
      simp [boundSnapshot, ha, hr, hrow]

end Refpoly.JoinRejection
