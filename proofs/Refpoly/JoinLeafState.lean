import Refpoly.JoinLoopSource
import Refpoly.JoinBlocks

/-! Shared-value invariant at a successful tuple extension, using the actual
source matcher. Candidate/entry position permutations preserve row equations.
The matching loop must still be shown to construct the permutation used by
`matched_weights_equation`; the interface is explicit rather than assumed
from the presence of equal weights. -/
namespace Refpoly.JoinLoopSource

/-- Values installed by the chosen keys at all levels below `depth`. -/
def PrefixFields (E : Env) (chosen val : ℕ → ℕ) (depth : ℕ) : Prop :=
  ∀ i, i < depth → ∀ p, p < E.k (E.slot i) →
    val (E.coord (E.slot i) p) = field (E.k (E.slot i)) p (E.key (E.slot i) (chosen i))

theorem matchKey_extends_prefix {E : Env} (hP : PlanOK E)
    (depth kk : ℕ) (chosen val : ℕ → ℕ) (hpref : PrefixFields E chosen val depth)
    (hok : (matchKey (E.key (E.slot depth) kk) (E.k (E.slot depth))
      (E.coord (E.slot depth)) (E.bound depth) val).1 = true) :
    PrefixFields E (Function.update chosen depth kk)
      (matchKey (E.key (E.slot depth) kk) (E.k (E.slot depth))
        (E.coord (E.slot depth)) (E.bound depth) val).2 (depth + 1) := by
  intro i hi p hpk
  by_cases hid : i = depth
  · subst i
    rw [Function.update_self]
    exact matchFrom_installs _ _ _ _ (hP.inj _) 0 val hok p (Nat.zero_le _) hpk
  · have hlt : i < depth := by omega
    have hb : E.bound depth (E.coord (E.slot i) p) = true := by
      apply bound_mono hP (i + 1) depth _ (by omega)
      exact (hP.bound_succ i _).mpr (Or.inr ⟨p, hpk, rfl⟩)
    rw [Function.update_of_ne hid, matchKey, matchFrom_bound _ _ _ _ 0 val _ hb]
    exact hpref i hlt p hpk

theorem PrefixFields.mono {E : Env} {chosen val : ℕ → ℕ} {a b : ℕ}
    (h : PrefixFields E chosen val b) (hab : a ≤ b) : PrefixFields E chosen val a :=
  fun i hi p hp => h i (by omega) p hp

/-- A failed match may write stale unbound values, but never a prefix field. -/
theorem matchKey_keeps_prefix {E : Env} (hP : PlanOK E)
    (depth kk : ℕ) (chosen val : ℕ → ℕ) (hpref : PrefixFields E chosen val depth) :
    PrefixFields E chosen
      (matchKey (E.key (E.slot depth) kk) (E.k (E.slot depth))
        (E.coord (E.slot depth)) (E.bound depth) val).2 depth := by
  intro i hi p hp
  have hb : E.bound depth (E.coord (E.slot i) p) = true := by
    apply bound_mono hP (i + 1) depth _ (by omega)
    exact (hP.bound_succ i _).mpr (Or.inr ⟨p, hp, rfl⟩)
  rw [matchKey, matchFrom_bound _ _ _ _ 0 val _ hb]
  exact hpref i hi p hp

def FieldsInvariant (E : Env) (st : CState) : Prop :=
  st.active ≤ E.R ∧ PrefixFields E st.chosen st.val (st.active - 1)

theorem cstep_fields {E : Env} (hP : PlanOK E) (st : CState)
    (hinv : FieldsInvariant E st) : FieldsInvariant E (cstep E st).2 := by
  obtain ⟨hcap, hpref⟩ := hinv
  cases ha : st.active with
  | zero => simpa [cstep, ha, FieldsInvariant] using And.intro hcap hpref
  | succ d =>
    have hp : PrefixFields E st.chosen st.val d := by simpa [ha] using hpref
    simp only [cstep, ha]
    split_ifs with hscan hmatch hdesc
    · constructor
      · dsimp only; omega
      · exact matchKey_extends_prefix hP d _ st.chosen st.val hp hmatch
    · constructor
      · dsimp only; omega
      · exact (matchKey_extends_prefix hP d _ st.chosen st.val hp hmatch).mono (by dsimp only; omega)
    · constructor
      · dsimp only; omega
      · exact matchKey_keeps_prefix hP d _ st.chosen st.val hp
    · exact ⟨by dsimp only; omega, hp.mono (by dsimp only; omega)⟩

/-- Each source emission carries every chosen key's fields in its post-state. -/
theorem cstep_leaf_fields {E : Env} (hP : PlanOK E) (st : CState)
    (hinv : FieldsInvariant E st) (tuple : List ℕ) (ht : tuple ∈ (cstep E st).1) :
    tuple = (List.range E.R).map (cstep E st).2.chosen ∧
      PrefixFields E (cstep E st).2.chosen (cstep E st).2.val E.R := by
  obtain ⟨hcap, hpref⟩ := hinv
  cases ha : st.active with
  | zero => simp [cstep, ha] at ht
  | succ d =>
    have hp : PrefixFields E st.chosen st.val d := by simpa [ha] using hpref
    simp only [cstep, ha] at ht ⊢
    split_ifs at ht ⊢ with hscan hmatch hdesc <;> simp only [List.not_mem_nil] at ht
    have heq : d + 1 = E.R := by omega
    simp only [List.mem_singleton] at ht
    constructor
    · simpa [heq] using ht
    · simpa [heq] using matchKey_extends_prefix hP d _ st.chosen st.val hp hmatch

/-- The same source run, retaining the post-state beside each emitted tuple. -/
def crunWitness (E : Env) : ℕ → CState → List (List ℕ × CState)
  | 0, _ => []
  | fuel + 1, st => match st.active with
    | 0 => []
    | _ + 1 => ((cstep E st).1.map (fun t => (t, (cstep E st).2))) ++
        crunWitness E fuel (cstep E st).2

theorem crunWitness_tuples (E : Env) (fuel : ℕ) (st : CState) :
    (crunWitness E fuel st).map Prod.fst = crun E fuel st := by
  induction fuel generalizing st with
  | zero => rfl
  | succ fuel ih =>
    cases ha : st.active with
    | zero => simp [crunWitness, crun, ha]
    | succ d => simp [crunWitness, crun, ha, List.map_map, ih, Function.comp_def]

theorem crunWitness_fields {E : Env} (hP : PlanOK E) (fuel : ℕ) (st : CState)
    (hinv : FieldsInvariant E st) : ∀ event ∈ crunWitness E fuel st,
      event.1 = (List.range E.R).map event.2.chosen ∧
        PrefixFields E event.2.chosen event.2.val E.R := by
  induction fuel generalizing st with
  | zero => simp [crunWitness]
  | succ fuel ih =>
    cases ha : st.active with
    | zero => simp [crunWitness, ha]
    | succ d =>
      intro event he
      simp only [crunWitness, ha, List.mem_append, List.mem_map] at he
      rcases he with ⟨t, ht, rfl⟩ | he
      · exact cstep_leaf_fields hP st hinv t ht
      · exact ih _ (cstep_fields hP st hinv) _ he

theorem initial_fields {E : Env} (hR : 0 < E.R) (val it en : ℕ → ℕ)
    (ps : ℕ → Option ℕ) (chosen : ℕ → ℕ) :
    FieldsInvariant E (initial E val it en ps chosen) := by
  constructor
  · simpa [initial] using hR
  · simp [initial, PrefixFields]

/-- Every tuple emitted by the source search has an associated actual leaf
state whose shared values equal every chosen key's fields. -/
theorem crun_leaf_values {E : Env} (hP : PlanOK E) (hR : 0 < E.R)
    (val it en : ℕ → ℕ) (ps : ℕ → Option ℕ) (chosen : ℕ → ℕ)
    (fuel : ℕ) (tuple : List ℕ)
    (ht : tuple ∈ crun E fuel (initial E val it en ps chosen)) :
    ∃ leaf, (tuple, leaf) ∈ crunWitness E fuel (initial E val it en ps chosen) ∧
      tuple = (List.range E.R).map leaf.chosen ∧ PrefixFields E leaf.chosen leaf.val E.R := by
  rw [← crunWitness_tuples] at ht
  obtain ⟨⟨t, leaf⟩, he, htuple⟩ := List.mem_map.mp ht
  change t = tuple at htuple
  subst t
  exact ⟨leaf, he, crunWitness_fields hP fuel _ (initial_fields hR val it en ps chosen) _ he⟩

/-- Injectivity of the literal 16-bit prefix packing at any prefix length. -/
theorem pack_injective_fields (k : ℕ) (f g : ℕ → ℕ)
    (hf : ∀ p, p < k → f p < 2^16) (hg : ∀ p, p < k → g p < 2^16)
    (he : pack k f = pack k g) : ∀ p, p < k → f p = g p := by
  induction k with
  | zero => omega
  | succ k ih =>
    have he' : pack k f * 65536 + f k = pack k g * 65536 + g k := by
      simpa [pack, List.range_succ, List.foldl_append] using he
    have hfk := hf k (by omega)
    have hgk := hg k (by omega)
    norm_num at hfk hgk
    have hpack : pack k f = pack k g := by omega
    intro p hp
    by_cases hpk : p = k
    · subst p; omega
    · exact ih (fun p hp => hf p (by omega)) (fun p hp => hg p (by omega)) hpack p (by omega)

/-- The fibre validator checks exactly these premises: bounded shared values,
their packed word equals the fibre key, and the key fits the prefix width.
Thus its accepted points hold precisely the fields that the matcher installs. -/
theorem fibre_key_fields (k key : ℕ) (x : ℕ → ℕ)
    (hx : ∀ p, p < k → x p < 2^16) (hw : key < 2^(16*k))
    (hkey : pack k x = key) : ∀ p, p < k → x p = field k p key := by
  apply pack_injective_fields k x (fun p => field k p key) hx
  · intro p _
    exact Nat.mod_lt _ (by norm_num)
  · rw [hkey, pack_fields k key hw]

end Refpoly.JoinLoopSource

namespace Refpoly.JoinRejection

/-- A bijective weight matching gives the exact table row equation, including
repeated weights. This also applies to the shared-prefix permutation extended
by the identity on private positions. -/
theorem matched_weights_equation {n : ℕ} (entry candidate x : Fin n → ℕ)
    (e : Fin n ≃ Fin n) (hmatch : ∀ q, candidate q = entry (e q)) :
    (∑ q, candidate q * x q) = ∑ p, entry p * x (e.symm p) := by
  simpa only [hmatch, Equiv.symm_apply_apply] using
    (e.sum_comp (fun p => entry p * x (e.symm p)))

theorem matched_weights_solutions {n : ℕ} (entry candidate x : Fin n → ℕ)
    (e : Fin n ≃ Fin n) (hmatch : ∀ q, candidate q = entry (e q)) (degree : ℕ) :
    (∑ q, candidate q * x q) = degree ↔
      (∑ p, entry p * x (e.symm p)) = degree := by
  rw [matched_weights_equation entry candidate x e hmatch]

end Refpoly.JoinRejection
