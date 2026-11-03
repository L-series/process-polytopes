import Refpoly.JoinCWS

/-! Completeness-only contracts. Output accounting is necessary but cannot
replace proofs that generated candidates cover the target and that every
rejection is sound. Deferred candidates are never counted as rejected. -/
namespace Refpoly.GpuPipeline
open Set

variable {α : Type*}

def Retains (IP : α → Prop) (before after : Set α) : Prop :=
  ∀ c ∈ before, IP c → c ∈ after

theorem retains_compose {IP : α → Prop} {a b c : Set α}
    (hab : Retains IP a b) (hbc : Retains IP b c) : Retains IP a c :=
  fun x hx hip => hbc x (hab x hx hip) hip

/-- A rejection needs a proof of non-IP; incomplete work remains in `deferred`. -/
theorem gpu_retains_ip {IP : α → Prop} (generated accepted rejected deferred : Set α)
    (hcover : generated ⊆ accepted ∪ rejected ∪ deferred)
    (hreject : ∀ c ∈ rejected, ¬ IP c) : Retains IP generated (accepted ∪ deferred) := by
  intro c hc hip
  rcases hcover hc with (ha | hr) | hd
  · exact Or.inl ha
  · exact False.elim (hreject c hr hip)
  · exact Or.inr hd

/-- Finishing every deferred record closes the preservation chain. GPU false
acceptances do not threaten this completeness statement. -/
theorem gpu_cpu_no_omissions {IP : α → Prop}
    (targets generated accepted rejected deferred cpuAccepted : Set α)
    (hgeneration : Retains IP targets generated)
    (hcover : generated ⊆ accepted ∪ rejected ∪ deferred)
    (hreject : ∀ c ∈ rejected, ¬ IP c)
    (hcpu : Retains IP deferred cpuAccepted) :
    Retains IP targets (accepted ∪ cpuAccepted) := by
  intro c hc hip
  have hg := hgeneration c hc hip
  rcases gpu_retains_ip generated accepted rejected deferred hcover hreject c hg hip with ha | hd
  · exact Or.inl ha
  · exact Or.inr (hcpu c hd hip)

/-- Same counts do not establish coverage. This rules out using the execution
ledger as a substitute for a source-level no-omissions proof. -/
theorem equal_counts_do_not_prove_coverage :
    ([0,1] : List Nat).length = ([0,0] : List Nat).length ∧
      (1 : Nat) ∈ [0,1] ∧ (1 : Nat) ∉ [0,0] := by decide

/-- The host's nonoverflowing quotient/remainder formula. -/
def boundary (total shards index : Nat) : Nat :=
  (total / shards) * index + ((total % shards) * index) / shards

theorem boundary_exact (total shards index : Nat) (hn : 0 < shards) :
    boundary total shards index = total * index / shards := by
  unfold boundary
  have h := Nat.mod_add_div total shards
  have he : total * index = total % shards * index + (total / shards * index) * shards := by
    nlinarith
  rw [he, Nat.add_mul_div_right _ _ hn]
  omega

theorem boundary_mono (total shards : Nat) (hn : 0 < shards) :
    Monotone (boundary total shards) := by
  intro i j hij
  rw [boundary_exact total shards i hn, boundary_exact total shards j hn]
  exact Nat.div_le_div_right (Nat.mul_le_mul_left total hij)

/-- Every selection lies in one shard. There are no gaps, including when the
selection count is not divisible by the number of GPUs. -/
theorem shard_coverage (total shards : Nat) (hn : 0 < shards) (i : Nat) (hi : i < total) :
    ∃ k, k < shards ∧ boundary total shards k ≤ i ∧ i < boundary total shards (k+1) := by
  have hend : boundary total shards shards = total := by
    rw [boundary_exact total shards shards hn, Nat.mul_div_cancel total hn]
  have hex : ∃ k, k ≤ shards ∧ i < boundary total shards k :=
    ⟨shards, le_rfl, by simpa [hend] using hi⟩
  let k := Nat.find hex
  have hk : k ≤ shards ∧ i < boundary total shards k := Nat.find_spec hex
  have hpos : 0 < k := by
    by_contra h
    have hz : k = 0 := by omega
    simp [hz, boundary] at hk
  have hp := Nat.find_min hex (show k-1 < k by omega)
  have hlow : boundary total shards (k-1) ≤ i := by
    have hle : k-1 ≤ shards := by omega
    simp only [not_and, not_lt] at hp
    exact hp hle
  refine ⟨k-1, by omega, hlow, ?_⟩
  simpa [Nat.sub_add_cancel (show 1 ≤ k by omega)] using hk.2

/-- Two distinct shard ranges never own the same selection. -/
theorem shard_disjoint (total shards : Nat) (hn : 0 < shards)
    (a b i : Nat) (ha : boundary total shards a ≤ i ∧ i < boundary total shards (a+1))
    (hb : boundary total shards b ≤ i ∧ i < boundary total shards (b+1)) : a = b := by
  have hm := boundary_mono total shards hn
  rcases lt_trichotomy a b with h | h | h
  · have hh := hm (show a+1 ≤ b by omega)
    omega
  · exact h
  · have hh := hm (show b+1 ≤ a by omega)
    omega

/-- Every intermediate in the host formula fits uint64 for its signed-int
shard API. This includes totals near UINT64_MAX, where total*index would wrap. -/
theorem boundary_intermediates_fit (total shards index : Nat)
    (ht : total ≤ 2^64-1) (hn : 0 < shards) (hs : shards ≤ 2^31-1) (hi : index ≤ shards) :
    total / shards * index ≤ 2^64-1 ∧
      total % shards * index ≤ 2^64-1 ∧ boundary total shards index ≤ 2^64-1 := by
  have hq := (Nat.mul_le_mul_left (total / shards) hi).trans (Nat.div_mul_le_self total shards)
  have hr : total % shards ≤ shards := (Nat.mod_lt total hn).le
  have hp := (Nat.mul_le_mul hr hi).trans (Nat.mul_le_mul hs hs)
  have hb := boundary_mono total shards hn hi
  have hend : boundary total shards shards = total := by
    rw [boundary_exact total shards shards hn, Nat.mul_div_cancel total hn]
  rw [hend] at hb
  constructor
  · exact hq.trans ht
  · constructor
    · norm_num at hp ⊢
      omega
    · exact hb.trans ht

/-- Projection of the concrete ownership states during the final output audit:
accepted/deferred states project to pending; both stored states project to stored.
The rejected state remains separate. -/
inductive AuditRoute where
  | pending | rejected | stored
  deriving DecidableEq

structure TaggedOutput (n : Nat) (α : Type*) where
  id : Fin n
  payload : α

def auditWrite {n : Nat} [DecidableEq α] (original : Fin n → α)
    (state : Fin n → AuditRoute) (row : TaggedOutput n α) : Option (Fin n → AuditRoute) :=
  if state row.id = .pending ∧ row.payload = original row.id then
    some (Function.update state row.id .stored)
  else none

def auditOutputs {n : Nat} [DecidableEq α] (original : Fin n → α) :
    List (TaggedOutput n α) → (Fin n → AuditRoute) → Option (Fin n → AuditRoute)
  | [], state => some state
  | row :: rows, state => (auditWrite original state row).bind (auditOutputs original rows)

/-- A successful audit transition checks payload identity, not just a tag or
counter. An already stored/rejected identity cannot be written again. -/
theorem auditWrite_exact {n : Nat} [DecidableEq α] {original : Fin n → α}
    {state next : Fin n → AuditRoute} {row : TaggedOutput n α}
    (h : auditWrite original state row = some next) :
    state row.id = .pending ∧ row.payload = original row.id ∧
      next = Function.update state row.id .stored := by
  unfold auditWrite at h
  split_ifs at h with guard
  exact ⟨guard.1, guard.2, (Option.some.inj h).symm⟩

/-- Every stored identity has a matching output witness, unless it was already
stored before this batch. This holds for arbitrary record order and multiplicity. -/
theorem auditOutputs_witness {n : Nat} [DecidableEq α] (original : Fin n → α)
    (rows : List (TaggedOutput n α)) (state final : Fin n → AuditRoute)
    (h : auditOutputs original rows state = some final) (i : Fin n)
    (hi : final i = .stored) :
    state i = .stored ∨ ∃ row ∈ rows, row.id = i ∧ row.payload = original i := by
  induction rows generalizing state with
  | nil =>
    have hf : state = final := Option.some.inj h
    exact Or.inl (hf ▸ hi)
  | cons row rows ih =>
    cases hs : auditWrite original state row with
    | none => simp [auditOutputs, hs] at h
    | some next =>
      have hrest : auditOutputs original rows next = some final := by
        simpa only [auditOutputs, hs, Option.bind_some] using h
      rcases ih next hrest with hn | ⟨r, hr, hid, hp⟩
      · obtain ⟨_, hp, hnext⟩ := auditWrite_exact hs
        by_cases heq : row.id = i
        · exact Or.inr ⟨row, by simp, heq, heq ▸ hp⟩
        · left
          simpa [hnext, Function.update_of_ne (Ne.symm heq)] using hn
      · exact Or.inr ⟨r, by simp [hr], hid, hp⟩

/-- A stored identity cannot occur again in a subsequently successful output
batch: the concrete CAS guard must see a pending state. -/
theorem auditOutputs_stored_absent {n : Nat} [DecidableEq α] (original : Fin n → α)
    (rows : List (TaggedOutput n α)) (state final : Fin n → AuditRoute)
    (h : auditOutputs original rows state = some final) (i : Fin n)
    (hi : state i = .stored) : ∀ row ∈ rows, row.id ≠ i := by
  induction rows generalizing state with
  | nil => simp
  | cons row rows ih =>
    cases hs : auditWrite original state row with
    | none => simp [auditOutputs, hs] at h
    | some next =>
      have hrest : auditOutputs original rows next = some final := by
        simpa only [auditOutputs, hs, Option.bind_some] using h
      obtain ⟨hguard, _, hn⟩ := auditWrite_exact hs
      have hne : row.id ≠ i := by
        intro heq
        rw [heq, hi] at hguard
        cases hguard
      have hni : next i = .stored := by
        simpa [hn, Function.update_of_ne (Ne.symm hne)] using hi
      intro r hr
      rcases List.mem_cons.mp hr with rfl | hr
      · exact hne
      · exact ih next hrest hni r hr

/-- Successful output auditing enforces uniqueness of tags, including across
accepted/deferred buffers when their audits are composed. -/
theorem auditOutputs_unique {n : Nat} [DecidableEq α] (original : Fin n → α)
    (rows : List (TaggedOutput n α)) (state final : Fin n → AuditRoute)
    (h : auditOutputs original rows state = some final) : (rows.map TaggedOutput.id).Nodup := by
  induction rows generalizing state with
  | nil => simp
  | cons row rows ih =>
    cases hs : auditWrite original state row with
    | none => simp [auditOutputs, hs] at h
    | some next =>
      have hrest : auditOutputs original rows next = some final := by
        simpa only [auditOutputs, hs, Option.bind_some] using h
      obtain ⟨_, _, hn⟩ := auditWrite_exact hs
      have hni : next row.id = .stored := by simp [hn]
      have habsent := auditOutputs_stored_absent original rows next final hrest row.id hni
      simp only [List.map_cons, List.nodup_cons]
      constructor
      · intro hmem
        obtain ⟨r, hr, heq⟩ := List.mem_map.mp hmem
        exact habsent r hr heq
      · exact ih next hrest

/-- The terminal-state scan plus successful identity audit establishes delivery
of every non-rejected original payload. Cardinality equality is not a premise. -/
theorem audited_output_coverage {n : Nat} [DecidableEq α] (original : Fin n → α)
    (rows : List (TaggedOutput n α)) (initial final : Fin n → AuditRoute)
    (hstart : ∀ i, initial i ≠ .stored)
    (haudit : auditOutputs original rows initial = some final)
    (hterminal : ∀ i, final i = .rejected ∨ final i = .stored) :
    ∀ i, final i = .rejected ∨ ∃ row ∈ rows, row.id = i ∧ row.payload = original i := by
  intro i
  rcases hterminal i with hr | hs
  · exact Or.inl hr
  · right
    rcases auditOutputs_witness original rows initial final haudit i hs with hbad | hw
    · exact False.elim (hstart i hbad)
    · exact hw

end Refpoly.GpuPipeline
