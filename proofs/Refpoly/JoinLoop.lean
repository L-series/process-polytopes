import Refpoly.JoinStack

/-!
# The join search loop with state-dependent candidate lists

`device_gjoin_decide` (src/gpu/join.cuh) does not scan a fixed key list per
row: `device_gjoin_begin_level` narrows level `j` to the keys whose first bound
shared value matches (a range of a position-sorted index), and a level whose
shared values are all bound is a single lookup. The candidate list of a level
therefore depends on the values bound by earlier levels.

This module generalizes the frame machine of `JoinStack.lean` to candidate
lists `cand j σ` that depend on the level and the bound state, proves the
iterative machine enumerates exactly the recursive specification, and that the
specification is a permutation of the filtered Cartesian product whenever each
narrowed list keeps exactly the compatible keys of the full row (once each).
-/
namespace Refpoly.JoinLoop

variable {σ α : Type*}

/-- Recursive specification: the compatible key tuples from level `j` on.
`fuel` counts the remaining levels (the source's `R - j`). -/
def leavesFrom (cand : ℕ → σ → List α) (extend : ℕ → σ → α → Option σ) :
    ℕ → ℕ → σ → List (List α)
  | 0, _, _ => [[]]
  | fuel + 1, j, state => (cand j state).flatMap fun key =>
      match extend j state key with
      | none => []
      | some next => (leavesFrom cand extend fuel (j + 1) next).map (List.cons key)

/-- One active level of the source loop: its depth, the state its keys are
matched against, the not yet tried candidates, the chosen keys of the levels
above, and the number of levels below (`R - 1 - depth`). -/
structure Frame (σ α : Type*) where
  depth : ℕ
  state : σ
  pending : List α
  chosen : List α
  below : ℕ

def frameLeaves (cand : ℕ → σ → List α) (extend : ℕ → σ → α → Option σ) (f : Frame σ α) :
    List (List α) :=
  f.pending.flatMap fun key => match extend f.depth f.state key with
    | none => []
    | some next => (leavesFrom cand extend f.below (f.depth + 1) next).map
        (fun suffix => f.chosen ++ key :: suffix)

def stackLeaves (cand : ℕ → σ → List α) (extend : ℕ → σ → α → Option σ) (stack : List (Frame σ α)) :
    List (List α) :=
  stack.flatMap (frameLeaves cand extend)

/-- One iteration of the source loop: pop an exhausted level (`--depth`),
otherwise try the next candidate; a mismatch stays at the level, a match at the
last level emits a leaf, and a match above it pushes the next level
(`++depth; begin_level`) with its state-dependent candidates. -/
def step (cand : ℕ → σ → List α) (extend : ℕ → σ → α → Option σ) :
    List (Frame σ α) → List (List α) × List (Frame σ α)
  | [] => ([], [])
  | f :: stack => match f.pending with
    | [] => ([], stack)
    | key :: pending =>
      let parent := { f with pending := pending }
      match extend f.depth f.state key with
      | none => ([], parent :: stack)
      | some next => match f.below with
        | 0 => ([f.chosen ++ [key]], parent :: stack)
        | below + 1 =>
          ([], ⟨f.depth + 1, next, cand (f.depth + 1) next, f.chosen ++ [key], below⟩ :: parent :: stack)

theorem step_exact (cand : ℕ → σ → List α) (extend : ℕ → σ → α → Option σ) (stack : List (Frame σ α)) :
    (step cand extend stack).1 ++ stackLeaves cand extend (step cand extend stack).2 =
      stackLeaves cand extend stack := by
  cases stack with
  | nil => rfl
  | cons f stack =>
    rcases f with ⟨depth, state, pending, chosen, below⟩
    cases pending with
    | nil => rfl
    | cons key pending =>
      cases he : extend depth state key with
      | none => simp [step, stackLeaves, frameLeaves, he]
      | some next =>
        cases below with
        | zero => simp [step, stackLeaves, frameLeaves, leavesFrom, he]
        | succ below =>
          simp [step, stackLeaves, frameLeaves, leavesFrom, he, List.map_flatMap, List.append_assoc]
          apply List.flatMap_congr
          intro k _
          cases extend (depth + 1) next k <;> simp [List.map_map, Function.comp_def]

/-- Work measure: one unit per candidate tried and per pop. -/
def work (cand : ℕ → σ → List α) (extend : ℕ → σ → α → Option σ) : ℕ → ℕ → σ → List α → ℕ
  | 0, _, _, pending => 1 + pending.length
  | below + 1, j, state, pending => 1 + (pending.map fun key =>
      match extend j state key with
      | none => 1
      | some next => 1 + work cand extend below (j + 1) next (cand (j + 1) next)).sum

def stackWork (cand : ℕ → σ → List α) (extend : ℕ → σ → α → Option σ) (stack : List (Frame σ α)) : ℕ :=
  (stack.map fun f => work cand extend f.below f.depth f.state f.pending).sum

theorem step_work (cand : ℕ → σ → List α) (extend : ℕ → σ → α → Option σ)
    (f : Frame σ α) (stack : List (Frame σ α)) :
    stackWork cand extend (step cand extend (f :: stack)).2 + 1 = stackWork cand extend (f :: stack) := by
  rcases f with ⟨depth, state, pending, chosen, below⟩
  cases pending with
  | nil => cases below <;> simp [step, stackWork, work, Nat.add_comm]
  | cons key pending =>
    cases he : extend depth state key with
    | none =>
      cases below <;> simp [step, stackWork, work, he, Nat.add_assoc, Nat.add_comm, Nat.add_left_comm]
      all_goals omega
    | some next =>
      cases below <;> simp [step, stackWork, work, he, Nat.add_assoc, Nat.add_comm, Nat.add_left_comm]
      all_goals omega

def run (cand : ℕ → σ → List α) (extend : ℕ → σ → α → Option σ) : ℕ → List (Frame σ α) → List (List α)
  | 0, _ => []
  | _ + 1, [] => []
  | fuel + 1, f :: stack =>
    let s := step cand extend (f :: stack)
    s.1 ++ run cand extend fuel s.2

theorem run_exact (cand : ℕ → σ → List α) (extend : ℕ → σ → α → Option σ)
    (fuel : ℕ) (stack : List (Frame σ α)) (enough : stackWork cand extend stack ≤ fuel) :
    run cand extend fuel stack = stackLeaves cand extend stack := by
  induction fuel generalizing stack with
  | zero =>
    cases stack with
    | nil => rfl
    | cons f stack =>
      rcases f with ⟨depth, state, pending, chosen, below⟩
      cases below <;> simp [stackWork, work] at enough
  | succ fuel ih =>
    cases stack with
    | nil => rfl
    | cons f stack =>
      have decreases := step_work cand extend f stack
      have next : stackWork cand extend (step cand extend (f :: stack)).2 ≤ fuel := by omega
      simpa only [run, ih _ next] using step_exact cand extend (f :: stack)

/-- The loop started at level 0 with `R = rows + 1` levels emits exactly the
recursive specification, in order. -/
theorem run_initial (cand : ℕ → σ → List α) (extend : ℕ → σ → α → Option σ) (rows : ℕ) (state : σ) :
    let initial : Frame σ α := ⟨0, state, cand 0 state, [], rows⟩
    run cand extend (stackWork cand extend [initial]) [initial] = leavesFrom cand extend (rows + 1) 0 state := by
  dsimp only
  rw [run_exact cand extend _ _ le_rfl]
  simp [stackLeaves, frameLeaves, leavesFrom]

/-! ## Narrowed candidates versus full rows -/

/-- Filtered Cartesian product of the full rows from level `j` on. -/
def fullLeaves (row : ℕ → List α) (extend : ℕ → σ → α → Option σ) : ℕ → ℕ → σ → List (List α)
  | 0, _, _ => [[]]
  | fuel + 1, j, state => (row j).flatMap fun key =>
      match extend j state key with
      | none => []
      | some next => (fullLeaves row extend fuel (j + 1) next).map (List.cons key)

def compatible (extend : ℕ → σ → α → Option σ) (j : ℕ) (state : σ) (key : α) : Bool :=
  (extend j state key).isSome

theorem flatMap_filter_of_nil {β : Type*} (l : List α) (p : α → Bool) (f : α → List β)
    (h : ∀ x, p x = false → f x = []) : (l.filter p).flatMap f = l.flatMap f := by
  induction l with
  | nil => rfl
  | cons x l ih => by_cases hx : p x <;> simp [hx, ih, h]

/-- Keys whose match fails contribute nothing, so only the compatible
candidates matter. -/
theorem leavesFrom_filter (cand : ℕ → σ → List α) (extend : ℕ → σ → α → Option σ) (fuel j : ℕ) (state : σ) :
    leavesFrom cand extend (fuel + 1) j state =
      ((cand j state).filter (compatible extend j state)).flatMap fun key =>
        match extend j state key with
        | none => []
        | some next => (leavesFrom cand extend fuel (j + 1) next).map (List.cons key) := by
  rw [flatMap_filter_of_nil]
  · rfl
  · intro key hk
    simp only [compatible, Option.isSome_eq_false_iff, Option.isNone_iff_eq_none] at hk
    simp [hk]

theorem fullLeaves_filter (row : ℕ → List α) (extend : ℕ → σ → α → Option σ) (fuel j : ℕ) (state : σ) :
    fullLeaves row extend (fuel + 1) j state =
      ((row j).filter (compatible extend j state)).flatMap fun key =>
        match extend j state key with
        | none => []
        | some next => (fullLeaves row extend fuel (j + 1) next).map (List.cons key) := by
  rw [flatMap_filter_of_nil]
  · rfl
  · intro key hk
    simp only [compatible, Option.isSome_eq_false_iff, Option.isNone_iff_eq_none] at hk
    simp [hk]

/-- If every narrowed list holds the compatible keys of the full row, each
once (a permutation after filtering), the source enumeration is a permutation
of the filtered Cartesian product: no compatible tuple is lost or repeated. -/
theorem leavesFrom_perm_full (cand : ℕ → σ → List α) (row : ℕ → List α) (extend : ℕ → σ → α → Option σ)
    (narrowed : ∀ j state, ((cand j state).filter (compatible extend j state)).Perm
      ((row j).filter (compatible extend j state))) :
    ∀ fuel j state, (leavesFrom cand extend fuel j state).Perm (fullLeaves row extend fuel j state) := by
  intro fuel
  induction fuel with
  | zero => intro j state; simp [leavesFrom, fullLeaves]
  | succ fuel ih =>
    intro j state
    rw [leavesFrom_filter, fullLeaves_filter]
    refine (List.Perm.flatMap_left _ (fun key _ => ?_)).trans (List.Perm.flatMap_right _ (narrowed j state))
    cases extend j state key with
    | none => rfl
    | some next => exact (ih (j + 1) next).map _

end Refpoly.JoinLoop
