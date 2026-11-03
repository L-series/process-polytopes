import Refpoly.JoinLeafVerdict
import Refpoly.JoinSearch
import Refpoly.JoinLeafState

/-! Source model for the fused type-3 join. The finite certificate operations
refine the general leaf by extracted-source CBMC. This module proves the
unbounded lookup, no-false-negative filter, exact-match enumeration and capped
summary scan. Unsigned count bounds are proved before using Nat arithmetic. -/
namespace Refpoly.JoinType3
open Refpoly.JoinRejection

/-- The previous uint32 `(lo+hi)>>1` could leave its valid interval. -/
theorem legacy_midpoint_counterexample :
    ((2^31 + (2^31+1)) % 2^32) / 2 = 0 ∧ (0 : ℕ) < 2^31 := by norm_num

/-- Safe source midpoint is the mathematical midpoint without uint32 wrap. -/
theorem midpoint_exact (lo hi : ℕ) (h : lo ≤ hi) : lo + (hi-lo)/2 = (lo+hi)/2 := by omega

theorem midpoint_u32 (lo hi : ℕ) (h : lo ≤ hi) (hw : hi < 2^32) :
    lo + (hi-lo)/2 < 2^32 := by omega

/-- At every iteration prior np is at most a nonnegative signed-int cap. Two
uint32 fibre counts leave enough headroom for that prior count in uint64. -/
theorem count_u64 (np a b cap : ℕ) (hn : np ≤ cap) (hc : cap ≤ 2^31-1)
    (ha : a ≤ 2^32-1) (hb : b ≤ 2^32-1) : np+a*b < 2^64 := by
  have hm := Nat.mul_le_mul ha hb
  norm_num at hm hc ⊢
  omega

noncomputable def find (keys : ℕ → ℕ) (b e want : ℕ) : Option ℕ := by
  classical
  let f := searchLower (fun i => keys i < want) (e-b) b e
  exact if f < e ∧ keys f = want then some f else none

theorem find_iff (keys : ℕ → ℕ) (b e want : ℕ) (hbe : b ≤ e)
    (strict : ∀ i j, b ≤ i → i < j → j < e → keys i < keys j) (j : ℕ) :
    find keys b e want = some j ↔ b ≤ j ∧ j < e ∧ keys j = want := by
  classical
  constructor
  · intro h
    unfold find at h
    dsimp only at h
    split_ifs at h with hg
    have he := Option.some.inj h
    have sorted : ∀ i j, b ≤ i → i ≤ j → j < e → keys i ≤ keys j := by
      intro i j hb hij hj; rcases Nat.eq_or_lt_of_le hij with rfl | hlt
      · rfl
      · exact (strict i j hb hlt hj).le
    have initial : SearchInvariant b e (fun i => keys i < want) b e :=
      ⟨le_rfl,hbe,le_rfl,by intros; omega,by intros; omega⟩
    have hp : SearchPrefix b e (fun i => keys i < want) := by
      intro i j hb hij hj hkey; exact lt_of_le_of_lt (sorted i j hb hij hj) hkey
    have hb := (searchLower_exact hp (e-b) b e initial (le_refl _)).1
    subst j
    exact ⟨hb,hg⟩
  · rintro ⟨hb,hj,hkey⟩
    have he := search_lookup_unique keys b e want hbe strict j hb hj hkey
    simp [find, he, hj, hkey]

/-- The builder checks these bits for every stored canonical key. Hashes use
uint64 wrap before taking bits 55..63; collisions only add possible matchedPairs. -/
def FiltersComplete (keys : ℕ → ℕ) (b e : ℕ) (presence bloom : ℕ → Bool) : Prop :=
  ∀ j, b ≤ j → j < e → presence (keys j) = true ∧ bloom (keys j) = true

noncomputable def filteredFind (keys : ℕ → ℕ) (b e want : ℕ)
    (presence bloom : ℕ → Bool) : Option ℕ :=
  if presence want && bloom want then find keys b e want else none

theorem filteredFind_iff (keys : ℕ → ℕ) (b e want : ℕ) (hbe : b ≤ e)
    (strict : ∀ i j, b ≤ i → i < j → j < e → keys i < keys j)
    (presence bloom : ℕ → Bool) (hfilters : FiltersComplete keys b e presence bloom) (j : ℕ) :
    filteredFind keys b e want presence bloom = some j ↔ b ≤ j ∧ j < e ∧ keys j = want := by
  classical
  constructor
  · intro h
    unfold filteredFind at h
    split_ifs at h
    exact (find_iff keys b e want hbe strict j).mp h
  · rintro ⟨hb,hj,hkey⟩
    have hf := hfilters j hb hj
    rw [hkey] at hf
    simp [filteredFind, hf.1, hf.2, (find_iff keys b e want hbe strict j).mpr ⟨hb,hj,hkey⟩]

/-- The source's increasing scan of the shorter interval, retaining one
successful target index at most per source index. Choosing the other side
merely swaps the pair's two components. -/
noncomputable def matchedPairs (indices : List ℕ) (lookup : ℕ → Option ℕ) : List (ℕ × ℕ) :=
  indices.filterMap fun i => (lookup i).map (fun j => (i,j))

theorem matches_mem (indices : List ℕ) (lookup : ℕ → Option ℕ) (i j : ℕ) :
    (i,j) ∈ matchedPairs indices lookup ↔ i ∈ indices ∧ lookup i = some j := by
  simp only [matchedPairs, List.mem_filterMap, Option.map_eq_some_iff]
  constructor
  · rintro ⟨k,hk,j',hj,he⟩
    have hk' : k=i := congrArg Prod.fst he
    have hj' : j'=j := congrArg Prod.snd he
    subst k; subst j'; exact ⟨hk,hj⟩
  · rintro ⟨hi,hj⟩; exact ⟨i,hi,j,hj,rfl⟩

theorem matches_nodup (indices : List ℕ) (lookup : ℕ → Option ℕ) (hn : indices.Nodup) :
    (matchedPairs indices lookup).Nodup := by
  induction indices with
  | nil => simp [matchedPairs]
  | cons i rest ih =>
    obtain ⟨hnot,hr⟩ := List.nodup_cons.mp hn
    cases he : lookup i with
    | none => simpa [matchedPairs,he] using ih hr
    | some j =>
      have hm : (i,j) ∉ matchedPairs rest lookup := by
        intro h; exact hnot ((matches_mem rest lookup i j).mp h).1
      simpa [matchedPairs,he] using List.nodup_cons.mpr ⟨hm,ih hr⟩

/-- Complete and duplicate-free matched pairs, regardless of which side is
shorter. Presence/Bloom filters cannot suppress a real matching stored key. -/
theorem matches_exact (indices : List ℕ) (hn : indices.Nodup) (want keys : ℕ → ℕ)
    (b e : ℕ) (hbe : b ≤ e)
    (strict : ∀ i j, b ≤ i → i < j → j < e → keys i < keys j)
    (presence bloom : ℕ → Bool) (hf : FiltersComplete keys b e presence bloom) :
    (matchedPairs indices (fun i => filteredFind keys b e (want i) presence bloom)).Nodup ∧
      ∀ i j, (i,j) ∈ matchedPairs indices (fun i => filteredFind keys b e (want i) presence bloom) ↔
        i ∈ indices ∧ b ≤ j ∧ j < e ∧ keys j = want i := by
  exact ⟨matches_nodup indices _ hn, fun i j => (matches_mem indices _ i j).trans
    (and_congr_right fun _ => filteredFind_iff keys b e (want i) hbe strict presence bloom hf j)⟩

inductive ScanResult (k : ℕ) where
  | capped (np : ℕ)
  | complete (summary : Summary k)

/-- The cap branch precedes certificate accumulation, exactly as in CUDA. -/
def scan {k : ℕ} (cap : ℕ) : List (Summary k) → Summary k → ScanResult k
  | [], acc => .complete acc
  | leaf :: leaves, acc =>
    if cap < acc.count + leaf.count then .capped (acc.count+leaf.count)
    else scan cap leaves (combineSummary acc leaf)

def total {k : ℕ} : List (Summary k) → Summary k → Summary k
  | [], acc => acc
  | leaf :: leaves, acc => total leaves (combineSummary acc leaf)

theorem scan_complete {k : ℕ} (cap : ℕ) (leaves : List (Summary k)) (acc out : Summary k)
    (h : scan cap leaves acc = .complete out) : out = total leaves acc := by
  induction leaves generalizing acc with
  | nil => exact ScanResult.complete.inj h.symm
  | cons leaf leaves ih =>
    simp only [scan] at h
    split_ifs at h
    exact ih _ h

theorem scan_capped {k : ℕ} (cap : ℕ) (leaves : List (Summary k)) (acc : Summary k) (np : ℕ)
    (h : scan cap leaves acc = .capped np) : cap < np := by
  induction leaves generalizing acc with
  | nil => cases h
  | cons leaf leaves ih =>
    simp only [scan] at h
    split_ifs at h with hc
    · cases h; exact hc
    · exact ih _ h

/-- The capped branch always returns keep and cannot produce rejection. -/
def verdict {k : ℕ} (result : ScanResult k) : Verdict :=
  match result with
  | .capped _ => .keep
  | .complete s => leafVerdict s.count [] ((List.ofFn fun i => (s.lower i,s.upper i)))

theorem capped_keep {k : ℕ} (np : ℕ) : verdict (.capped (k:=k) np) = .keep := rfl


open Refpoly.JoinLoopSource

theorem pack_lt (k : ℕ) (f : ℕ → ℕ) (hf : ∀ i, i < k → f i < 2^16) :
    pack k f < 2^(16*k) := by
  induction k with
  | zero => simp [pack]
  | succ k ih =>
    have hprev := ih (fun i hi => hf i (by omega))
    have hlast := hf k (by omega)
    have hp : 2^(16*(k+1)) = 2^(16*k) * 65536 := by
      rw [show 16*(k+1)=16*k+16 by omega, pow_add]; norm_num
    rw [hp]
    have he : pack (k+1) f = pack k f * 65536 + f k := by
      simp [pack,List.range_succ,List.foldl_append]
    rw [he]
    norm_num at hlast
    omega

def fields (key : ℕ) (i : Fin 3) : ℕ := field 3 i.val key

def encode (v : Fin 3 → ℕ) : ℕ := pack 3 (fun i => v ⟨i%3, Nat.mod_lt _ (by omega)⟩)

theorem encode_lt (v : Fin 3 → ℕ) (hv : ∀ i, v i < 2^16) : encode v < 2^48 := by
  exact pack_lt 3 _ (fun i _ => hv _)

theorem fields_encode (v : Fin 3 → ℕ) (hv : ∀ i, v i < 2^16) (i : Fin 3) :
    fields (encode v) i = v i := by
  have he := fibre_key_fields 3 (encode v) (fun i => v ⟨i%3,Nat.mod_lt _ (by omega)⟩)
    (fun i _ => hv _) (encode_lt v hv) rfl i.val i.isLt
  simpa [fields,Nat.mod_eq_of_lt i.isLt] using he.symm

theorem encode_fields (key : ℕ) (hw : key < 2^48) : encode (fields key) = key := by
  rw [encode]
  rw [pack_congr 3 _ (fun i => field 3 i key) (by
    intro i hi
    simp [fields,Nat.mod_eq_of_lt hi])]
  exact pack_fields 3 key hw

def remap (p : Fin 3 ≃ Fin 3) (key : ℕ) : ℕ := encode (fun i => fields key (p i))

theorem fields_lt (key : ℕ) (i : Fin 3) : fields key i < 2^16 := Nat.mod_lt _ (by norm_num)

theorem remap_lt (p : Fin 3 ≃ Fin 3) (key : ℕ) : remap p key < 2^48 :=
  encode_lt _ (fun i => fields_lt _ _)

theorem fields_remap (p : Fin 3 ≃ Fin 3) (key : ℕ) (i : Fin 3) :
    fields (remap p key) i = fields key (p i) := fields_encode _ (fun i => fields_lt _ _) i

theorem fields_injective {a b : ℕ} (ha : a < 2^48) (hb : b < 2^48)
    (he : fields a = fields b) : a = b := by
  rw [← encode_fields a ha, ← encode_fields b hb, he]

theorem remap_inverse (p : Fin 3 ≃ Fin 3) (key : ℕ) (hw : key < 2^48) :
    remap p.symm (remap p key) = key := by
  apply fields_injective (remap_lt _ _) hw
  funext i
  simp only [fields_remap,Equiv.apply_symm_apply]

/-- Left iteration searches for the inverse-permuted key; right iteration
searches for the forward-permuted key. They enumerate the same relation. -/
theorem remap_matching (p : Fin 3 ≃ Fin 3) (left right : ℕ)
    (hl : left < 2^48) (hr : right < 2^48) :
    right = remap p.symm left ↔ left = remap p right := by
  constructor
  · intro h; rw [h]; exact (remap_inverse p.symm left hl).symm
  · intro h; rw [h]; exact (remap_inverse p right hr).symm

/-- Candidate-order mask checks are simply a permutation of all entry-order
checks, including the saturation-to-63 bit used by the presence tables. -/
theorem presence_permutation (p : Fin 3 ≃ Fin 3) (P : Fin 3 → Prop) :
    (∀ i, P (p i)) ↔ ∀ i, P i := by
  constructor
  · intro h i; simpa using h (p.symm i)
  · intro h i; exact h _


/-- Literal source scan: filtering and lookup happen before the cap and bits. -/
def sourceScan {k : ℕ} (cap : ℕ) (lookup : ℕ → Option ℕ) (leaf : ℕ × ℕ → Summary k) :
    List ℕ → Summary k → ScanResult k
  | [], acc => .complete acc
  | i :: rest, acc => match lookup i with
    | none => sourceScan cap lookup leaf rest acc
    | some j =>
      let s := leaf (i,j)
      if cap < acc.count+s.count then .capped (acc.count+s.count)
      else sourceScan cap lookup leaf rest (combineSummary acc s)

theorem sourceScan_eq {k : ℕ} (cap : ℕ) (lookup : ℕ → Option ℕ)
    (leaf : ℕ × ℕ → Summary k) (indices : List ℕ) (acc : Summary k) :
    sourceScan cap lookup leaf indices acc = scan cap ((matchedPairs indices lookup).map leaf) acc := by
  induction indices generalizing acc with
  | nil => rfl
  | cons i rest ih =>
    cases he : lookup i with
    | none => simpa [sourceScan,matchedPairs,he] using ih acc
    | some j => simp [sourceScan,matchedPairs,he,scan,ih]

theorem total_count {k : ℕ} (leaves : List (Summary k)) (acc : Summary k) :
    (total leaves acc).count = acc.count + (leaves.map Summary.count).sum := by
  induction leaves generalizing acc with
  | nil => simp [total]
  | cons s rest ih => simp [total,ih,combineSummary,Nat.add_assoc]

theorem total_lower {k : ℕ} (leaves : List (Summary k)) (acc : Summary k) (i : Fin k) :
    (total leaves acc).lower i = (acc.lower i || leaves.any (fun s => s.lower i)) := by
  induction leaves generalizing acc with
  | nil => simp [total]
  | cons s rest ih => simp [total,ih,combineSummary,Bool.or_assoc]

theorem total_upper {k : ℕ} (leaves : List (Summary k)) (acc : Summary k) (i : Fin k) :
    (total leaves acc).upper i = (acc.upper i || leaves.any (fun s => s.upper i)) := by
  induction leaves generalizing acc with
  | nil => simp [total]
  | cons s rest ih => simp [total,ih,combineSummary,Bool.or_assoc]

theorem sourceScan_complete {k : ℕ} (cap : ℕ) (lookup : ℕ → Option ℕ)
    (leaf : ℕ × ℕ → Summary k) (indices : List ℕ) (out : Summary k)
    (h : sourceScan cap lookup leaf indices (emptySummary k) = .complete out) :
    out.count = (((matchedPairs indices lookup).map leaf).map Summary.count).sum ∧
      (∀ i, out.lower i = (matchedPairs indices lookup).any (fun p => (leaf p).lower i)) ∧
      (∀ i, out.upper i = (matchedPairs indices lookup).any (fun p => (leaf p).upper i)) := by
  rw [sourceScan_eq] at h
  have he := scan_complete cap _ _ out h
  rw [he]
  simp [total_count,total_lower,total_upper,emptySummary,List.any_map,Function.comp_def]

theorem sentinel_distinct (j e : ℕ) (hj : j < e) (he : e ≤ 2^32-1) : j ≠ 2^32-1 := by omega
end Refpoly.JoinType3
