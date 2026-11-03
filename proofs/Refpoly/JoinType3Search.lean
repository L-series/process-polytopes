import Refpoly.JoinType3
import Refpoly.GpuDecode

/-! Search-side bridge for the fused type-3 join. This records the exact
half-open source ranges, translates candidate-order presence checks to the
stored entry order, models the two uint64 Bloom hashes, and characterizes the
word pairs emitted by either shorter-side scan. -/
namespace Refpoly.JoinType3

open Refpoly.GpuDecode

/-- The source `for (k = b; k < e; ++k)` interval. -/
def sourceIndices (b e : ℕ) : List ℕ := List.range' b (e-b)

theorem sourceIndices_mem (b e i : ℕ) : i ∈ sourceIndices b e ↔ b ≤ i ∧ i < e := by
  simp [sourceIndices, List.mem_range'_1]
  omega

theorem sourceIndices_nodup (b e : ℕ) : (sourceIndices b e).Nodup := by
  exact List.nodup_range'

theorem sourceIndices_length (b e : ℕ) (hbe : b ≤ e) :
    (sourceIndices b e).length = e-b := by simp [sourceIndices]

/-- Every source offset in a finite thread-strided loop has one unique
`(thread, iteration)` counter pair. -/
theorem thread_assignment_unique (stride offset : ℕ) (hstride : 0 < stride) :
    ∃! tm : ℕ × ℕ, tm.1 < stride ∧ tm.1 + stride * tm.2 = offset := by
  refine ⟨(offset % stride, offset / stride), ?_, ?_⟩
  · constructor
    · exact Nat.mod_lt _ hstride
    · exact Nat.mod_add_div offset stride
  · intro tm htm
    rcases htm with ⟨ht, heq⟩
    have hfirst : tm.1 = offset % stride := by
      have hm := congrArg (fun x => x % stride) heq
      change (tm.1 + stride * tm.2) % stride = offset % stride at hm
      rw [Nat.add_mul_mod_self_left, Nat.mod_eq_of_lt ht] at hm
      exact hm
    have hlo : tm.2 * stride ≤ offset := by
      rw [Nat.mul_comm tm.2 stride, ← heq]
      exact Nat.le_add_left _ _
    have hhi : offset < (tm.2 + 1) * stride := by
      rw [← heq]
      calc
        tm.1 + stride * tm.2 < stride + stride * tm.2 := Nat.add_lt_add_right ht _
        _ = (tm.2 + 1) * stride := by ring
    have hquot : offset / stride = tm.2 := Nat.div_eq_of_lt_le hlo hhi
    apply Prod.ext
    · simpa [hfirst]
    · exact hquot.symm

/-- A strict source-table order gives injective key words on the indices that
the source can visit. -/
theorem strict_key_injective {keys : ℕ → ℕ} {b e : ℕ}
    (strict : ∀ i j, b ≤ i → i < j → j < e → keys i < keys j) :
    ∀ i ∈ sourceIndices b e, ∀ j ∈ sourceIndices b e, keys i = keys j → i = j := by
  intro i hi j hj heq
  have ⟨hbi, hie⟩ := (sourceIndices_mem b e i).mp hi
  have ⟨hbj, hje⟩ := (sourceIndices_mem b e j).mp hj
  by_contra hne
  rcases lt_or_gt_of_ne hne with hlt | hgt
  · have h := strict i j hbi hlt hje
    omega
  · have h := strict j i hbj hgt hie
    omega

/-- Linear index of a pair in the source's triangular pool enumeration. -/
def pairCode (n left right : ℕ) : ℕ := first n left + (right-left)

/-- Source semantics of `device_pair_left_closed` followed by the right index
calculation. The closed-form floating estimate is supplied as `guess`; the two
integer correction loops make every in-range guess produce the same pair. -/
def sourcePairAt (n p guess : ℕ) : ℕ × ℕ :=
  let left := up n p (down n p guess)
  (left, left + (p - first n left))

theorem sourcePairAt_correct (n p guess : ℕ) (hguess : guess < n)
    (hp : p < first n n) :
    let pair := sourcePairAt n p guess
    pair.1 < n ∧ pair.1 ≤ pair.2 ∧ pair.2 < n ∧ pairCode n pair.1 pair.2 = p := by
  unfold sourcePairAt pairCode
  obtain ⟨hl, hlo, hhi⟩ := closed_correct n p guess hguess hp
  have hrange := right_in_range n p (up n p (down n p guess)) hl hlo hhi
  refine ⟨hl, hrange.1, hrange.2, ?_⟩
  dsimp
  omega

/-- The fused kernel's finite pair interval is a duplicate-free list of
source row-index pairs. -/
noncomputable def sourcePairRange (n start count : ℕ) (guess : ℕ → ℕ) : List (ℕ × ℕ) :=
  (List.range' start count).map (fun p => sourcePairAt n p (guess p))

theorem sourcePairAt_injective {n : ℕ} (guess : ℕ → ℕ)
    (p q : ℕ) (hguessP : guess p < n) (hguessQ : guess q < n)
    (hp : p < first n n) (hq : q < first n n)
    (he : sourcePairAt n p (guess p) = sourcePairAt n q (guess q)) : p = q := by
  have hpc := (sourcePairAt_correct n p (guess p) hguessP hp).2.2.2
  have hqc := (sourcePairAt_correct n q (guess q) hguessQ hq).2.2.2
  rw [he] at hpc
  exact hpc.symm.trans hqc

theorem sourcePairRange_nodup {n start count : ℕ} (hend : start + count ≤ first n n)
    (guess : ℕ → ℕ)
    (hguess : ∀ p, start ≤ p → p < start + count → guess p < n) :
    (sourcePairRange n start count guess).Nodup := by
  classical
  apply (List.nodup_map_iff_inj_on List.nodup_range').2
  intro p hp q hq he
  rw [List.mem_range'_1] at hp hq
  exact sourcePairAt_injective guess p q (hguess p hp.1 (by omega))
    (hguess q hq.1 (by omega)) (by omega) (by omega) he

theorem sourcePairRange_mem {n start count : ℕ}
    (guess : ℕ → ℕ) (pair : ℕ × ℕ) :
    pair ∈ sourcePairRange n start count guess ↔
      ∃ p, start ≤ p ∧ p < start + count ∧ sourcePairAt n p (guess p) = pair := by
  classical
  unfold sourcePairRange
  rw [List.mem_map]
  constructor
  · rintro ⟨p,hp,hpair⟩
    rw [List.mem_range'_1] at hp
    exact ⟨p,hp.1,by omega,hpair⟩
  · rintro ⟨p,hp1,hp2,hpair⟩
    exact ⟨p,List.mem_range'_1.mpr ⟨hp1, by omega⟩,hpair⟩

/-- The left-shorter source scan maps each source key to the exact target
index whose word is its inverse-remapped candidate-order word. -/
noncomputable def leftLookup (p : Fin 3 ≃ Fin 3) (sourceKeys targetKeys : ℕ → ℕ)
    (b e : ℕ) (presence bloom : ℕ → Bool) : ℕ → Option ℕ :=
  fun i => filteredFind targetKeys b e (remap p.symm (sourceKeys i)) presence bloom

/-- The source pairs, represented as their canonical packed words in left,
right entry order. -/
noncomputable def leftWordPairs (p : Fin 3 ≃ Fin 3) (sourceKeys targetKeys : ℕ → ℕ)
    (indices : List ℕ) (b e : ℕ) (presence bloom : ℕ → Bool) : List (ℕ × ℕ) :=
  ((matchedPairs indices (leftLookup p sourceKeys targetKeys b e presence bloom)).map
    (fun ij => (sourceKeys ij.1, targetKeys ij.2)))

/-- Exact membership for the actual left iteration: source and target indices
stay in their half-open table ranges, and the words satisfy the key-remap
relation. -/
theorem leftWordPairs_mem (p : Fin 3 ≃ Fin 3) (sourceKeys targetKeys : ℕ → ℕ)
    (indices : List ℕ) (b e : ℕ) (hbe : b ≤ e)
    (targetStrict : ∀ i j, b ≤ i → i < j → j < e → targetKeys i < targetKeys j)
    (presence bloom : ℕ → Bool)
    (hfilters : FiltersComplete targetKeys b e presence bloom)
    (left right : ℕ) :
    (left, right) ∈ leftWordPairs p sourceKeys targetKeys indices b e presence bloom ↔
      ∃ i, i ∈ indices ∧ sourceKeys i = left ∧
        ∃ j, b ≤ j ∧ j < e ∧ targetKeys j = right ∧
          right = remap p.symm left := by
  classical
  unfold leftWordPairs leftLookup
  rw [List.mem_map]
  constructor
  · rintro ⟨⟨i,j⟩, hpair, heq⟩
    have hfst : sourceKeys i = left := congrArg Prod.fst heq
    have hsnd : targetKeys j = right := congrArg Prod.snd heq
    have ⟨hi, hlookup⟩ := (matches_mem indices _ i j).mp hpair
    have ⟨hbj,hje,hkey⟩ := (filteredFind_iff targetKeys b e (remap p.symm (sourceKeys i))
      hbe targetStrict presence bloom hfilters j).mp hlookup
    refine ⟨i,hi,hfst,j,hbj,hje,hsnd,?_⟩
    rw [← hfst, ← hsnd]
    exact hkey
  · rintro ⟨i,hi,rfl,j,hbj,hje,rfl,hrel⟩
    refine ⟨(i,j), (matches_mem indices _ i j).mpr ⟨hi, ?_⟩, rfl⟩
    exact (filteredFind_iff targetKeys b e (remap p.symm (sourceKeys i))
      hbe targetStrict presence bloom hfilters j).mpr ⟨hbj,hje,hrel⟩

/-- Word-pair outputs stay duplicate-free when source and target table words
are injective on their scanned intervals. -/
theorem leftWordPairs_nodup (p : Fin 3 ≃ Fin 3) (sourceKeys targetKeys : ℕ → ℕ)
    (indices : List ℕ) (hindices : indices.Nodup) (b e : ℕ) (hbe : b ≤ e)
    (sourceInj : ∀ i ∈ indices, ∀ j ∈ indices, sourceKeys i = sourceKeys j → i = j)
    (targetStrict : ∀ i j, b ≤ i → i < j → j < e → targetKeys i < targetKeys j)
    (presence bloom : ℕ → Bool)
    (hfilters : FiltersComplete targetKeys b e presence bloom) :
    (leftWordPairs p sourceKeys targetKeys indices b e presence bloom).Nodup := by
  classical
  apply (List.nodup_map_iff_inj_on (matches_nodup indices
    (leftLookup p sourceKeys targetKeys b e presence bloom) hindices)).mpr
  intro a ha a' ha' heq
  rcases a with ⟨i,j⟩
  rcases a' with ⟨i',j'⟩
  have hmem := (matches_mem indices _ i j).mp ha
  have hmem' := (matches_mem indices _ i' j').mp ha'
  have hkey := (filteredFind_iff targetKeys b e (remap p.symm (sourceKeys i))
    hbe targetStrict presence bloom hfilters j).mp hmem.2
  have hkey' := (filteredFind_iff targetKeys b e (remap p.symm (sourceKeys i'))
    hbe targetStrict presence bloom hfilters j').mp hmem'.2
  have hsource : sourceKeys i = sourceKeys i' := congrArg Prod.fst heq
  have hi : i = i' := sourceInj i hmem.1 i' hmem'.1 hsource
  subst i'
  have htarget : targetKeys j = targetKeys j' := congrArg Prod.snd heq
  have hj : j = j' := by
    by_contra hne
    rcases lt_or_gt_of_ne hne with hlt | hgt
    · have hs := targetStrict j j' hkey.1 hlt hkey'.2.1
      omega
    · have hs := targetStrict j' j hkey'.1 hgt hkey.2.1
      omega
  subst j'
  rfl

/-- The right-shorter scan visits right-entry words and looks up their forward
remap in the left table. -/
noncomputable def rightLookup (p : Fin 3 ≃ Fin 3) (rightKeys leftKeys : ℕ → ℕ)
    (b e : ℕ) (presence bloom : ℕ → Bool) : ℕ → Option ℕ :=
  fun i => filteredFind leftKeys b e (remap p (rightKeys i)) presence bloom

/-- Right iteration still emits word pairs in conventional left/right order. -/
noncomputable def rightWordPairs (p : Fin 3 ≃ Fin 3) (rightKeys leftKeys : ℕ → ℕ)
    (indices : List ℕ) (b e : ℕ) (presence bloom : ℕ → Bool) : List (ℕ × ℕ) :=
  ((matchedPairs indices (rightLookup p rightKeys leftKeys b e presence bloom)).map
    (fun ij => (leftKeys ij.2, rightKeys ij.1)))

/-- Exact membership for right iteration, translated back to the same
`right = remap p.symm left` relation as left iteration. -/
theorem rightWordPairs_mem (p : Fin 3 ≃ Fin 3) (rightKeys leftKeys : ℕ → ℕ)
    (indices : List ℕ) (b e : ℕ) (hbe : b ≤ e)
    (leftStrict : ∀ i j, b ≤ i → i < j → j < e → leftKeys i < leftKeys j)
    (leftBound : ∀ j, b ≤ j → j < e → leftKeys j < 2^48)
    (rightBound : ∀ i, i ∈ indices → rightKeys i < 2^48)
    (presence bloom : ℕ → Bool)
    (hfilters : FiltersComplete leftKeys b e presence bloom)
    (left right : ℕ) :
    (left, right) ∈ rightWordPairs p rightKeys leftKeys indices b e presence bloom ↔
      ∃ i, i ∈ indices ∧ rightKeys i = right ∧
        ∃ j, b ≤ j ∧ j < e ∧ leftKeys j = left ∧
          right = remap p.symm left := by
  classical
  unfold rightWordPairs rightLookup
  rw [List.mem_map]
  constructor
  · rintro ⟨⟨i,j⟩, hpair, heq⟩
    have hleft : leftKeys j = left := congrArg Prod.fst heq
    have hright : rightKeys i = right := congrArg Prod.snd heq
    have ⟨hi,hlookup⟩ := (matches_mem indices _ i j).mp hpair
    have ⟨hbj,hje,hkey⟩ := (filteredFind_iff leftKeys b e (remap p (rightKeys i))
      hbe leftStrict presence bloom hfilters j).mp hlookup
    have hrel : right = remap p.symm left := by
      have hrel' := (remap_matching p (leftKeys j) (rightKeys i)
        (leftBound j hbj hje) (rightBound i hi)).mpr hkey
      simpa [hleft, hright] using hrel'
    exact ⟨i,hi,hright,j,hbj,hje,hleft,hrel⟩
  · rintro ⟨i,hi,rfl,j,hbj,hje,rfl,hrel⟩
    refine ⟨(i,j), (matches_mem indices _ i j).mpr ⟨hi, ?_⟩, rfl⟩
    have hkey : leftKeys j = remap p (rightKeys i) :=
      (remap_matching p (leftKeys j) (rightKeys i)
        (leftBound j hbj hje) (rightBound i hi)).mp hrel
    exact (filteredFind_iff leftKeys b e (remap p (rightKeys i))
      hbe leftStrict presence bloom hfilters j).mpr ⟨hbj,hje,hkey⟩

/-- Right scan results are duplicate-free under the corresponding source and
target key injectivity facts. -/
theorem rightWordPairs_nodup (p : Fin 3 ≃ Fin 3) (rightKeys leftKeys : ℕ → ℕ)
    (indices : List ℕ) (hindices : indices.Nodup) (b e : ℕ) (hbe : b ≤ e)
    (sourceInj : ∀ i ∈ indices, ∀ j ∈ indices, rightKeys i = rightKeys j → i = j)
    (leftStrict : ∀ i j, b ≤ i → i < j → j < e → leftKeys i < leftKeys j)
    (presence bloom : ℕ → Bool)
    (hfilters : FiltersComplete leftKeys b e presence bloom) :
    (rightWordPairs p rightKeys leftKeys indices b e presence bloom).Nodup := by
  classical
  apply (List.nodup_map_iff_inj_on (matches_nodup indices
    (rightLookup p rightKeys leftKeys b e presence bloom) hindices)).mpr
  intro a ha a' ha' heq
  rcases a with ⟨i,j⟩
  rcases a' with ⟨i',j'⟩
  have hmem := (matches_mem indices _ i j).mp ha
  have hmem' := (matches_mem indices _ i' j').mp ha'
  have hkey := (filteredFind_iff leftKeys b e (remap p (rightKeys i))
    hbe leftStrict presence bloom hfilters j).mp hmem.2
  have hkey' := (filteredFind_iff leftKeys b e (remap p (rightKeys i'))
    hbe leftStrict presence bloom hfilters j').mp hmem'.2
  have hsource : rightKeys i = rightKeys i' := congrArg Prod.snd heq
  have hi : i = i' := sourceInj i hmem.1 i' hmem'.1 hsource
  subst i'
  have htarget : leftKeys j = leftKeys j' := congrArg Prod.fst heq
  have hj : j = j' := by
    by_contra hne
    rcases lt_or_gt_of_ne hne with hlt | hgt
    · have hs := leftStrict j j' hkey.1 hlt hkey'.2.1
      omega
    · have hs := leftStrict j' j hkey'.1 hgt hkey.2.1
      omega
  subst j'
  rfl

/-- A presence bit uses the source's saturated coordinate index: all values
at least 63 share bit 63. -/
def presenceBit (mask value : ℕ) : Prop := Nat.testBit mask (min value 63) = true

theorem presenceIndex_eq_source_clamp (value : ℕ) :
    min value 63 = if value < 63 then value else 63 := by
  by_cases h : value < 63 <;> simp [h, Nat.min_def] <;> omega

/-- The three entry-order mask checks for a packed word. -/
def entryPresenceCheck (masks : Fin 3 → ℕ) (key : ℕ) : Prop :=
  ∀ q, presenceBit (masks q) (fields key q)

/-- The actual candidate-order checks in the left scan: candidate coordinate
`ci` reads the right table's mask at `p ci`. -/
def leftCandidatePresenceCheck (p : Fin 3 ≃ Fin 3) (masks : Fin 3 → ℕ)
    (leftKey : ℕ) : Prop :=
  ∀ ci, presenceBit (masks (p ci)) (fields leftKey ci)

/-- The actual candidate-order checks in the right scan: source coordinate
`p ci` is tested against the left table's entry-order mask `ci`. -/
def rightCandidatePresenceCheck (masks : Fin 3 → ℕ) (p : Fin 3 ≃ Fin 3)
    (rightKey : ℕ) : Prop :=
  ∀ ci, presenceBit (masks ci) (fields rightKey (p ci))

/-- Permuted right-entry mask reads are exactly the entry-order checks for the
inverse-remapped left key. -/
theorem leftCandidatePresenceCheck_eq (p : Fin 3 ≃ Fin 3)
    (masks : Fin 3 → ℕ) (leftKey : ℕ) :
    leftCandidatePresenceCheck p masks leftKey =
      entryPresenceCheck masks (remap p.symm leftKey) := by
  apply propext
  unfold leftCandidatePresenceCheck entryPresenceCheck
  constructor
  · intro h q
    have hh := h (p.symm q)
    simpa [presenceBit, fields_remap, Equiv.apply_symm_apply] using hh
  · intro h ci
    have hh := h (p ci)
    simpa [presenceBit, fields_remap, Equiv.symm_apply_apply] using hh

/-- The right scan's permuted source fields are exactly the entry-order
checks for its forward-remapped target key. -/
theorem rightCandidatePresenceCheck_eq (p : Fin 3 ≃ Fin 3)
    (masks : Fin 3 → ℕ) (rightKey : ℕ) :
    rightCandidatePresenceCheck masks p rightKey =
      entryPresenceCheck masks (remap p rightKey) := by
  apply propext
  unfold rightCandidatePresenceCheck entryPresenceCheck
  constructor
  · intro h q
    simpa [presenceBit, fields_remap] using h q
  · intro h ci
    simpa [presenceBit, fields_remap] using h ci

noncomputable def leftCandidatePresence (p : Fin 3 ≃ Fin 3)
    (masks : Fin 3 → ℕ) (leftKey : ℕ) : Bool := by
  classical
  exact decide (leftCandidatePresenceCheck p masks leftKey)

noncomputable def rightCandidatePresence (masks : Fin 3 → ℕ)
    (p : Fin 3 ≃ Fin 3) (rightKey : ℕ) : Bool := by
  classical
  exact decide (rightCandidatePresenceCheck masks p rightKey)

/-- The two source Bloom multipliers, interpreted with the uint64 wrap that
the C++ multiplication performs before extracting bits 55 through 63. -/
def bloomHash (mult key : ℕ) : ℕ := (key * mult % 2^64) / 2^55

theorem bloomHash_lt_512 (mult key : ℕ) : bloomHash mult key < 512 := by
  have hmod : key * mult % 2^64 < 2^64 := Nat.mod_lt _ (by norm_num)
  have hmul : 2^64 = 512 * 2^55 := by norm_num
  unfold bloomHash
  apply (Nat.div_lt_iff_lt_mul (by norm_num : 0 < 2^55)).2
  rw [hmul]
  omega

theorem bloomHash_bucket_lt_8 (mult key : ℕ) : bloomHash mult key / 64 < 8 := by
  have h := bloomHash_lt_512 mult key
  omega

theorem bloomHash_bit_lt_64 (mult key : ℕ) : bloomHash mult key % 64 < 64 :=
  Nat.mod_lt _ (by norm_num)

/-- One source Bloom lookup: the high-nine-bit hash chooses one of eight
64-bit words, then the low six bits select its bit. -/
def bloomProbe (words : ℕ → ℕ) (mult key : ℕ) : Bool :=
  Nat.testBit (words (bloomHash mult key / 64)) (bloomHash mult key % 64)

def bloomPresent (words : ℕ → ℕ) (key : ℕ) : Bool :=
  bloomProbe words 0x9E3779B97F4A7C15 key &&
    bloomProbe words 0xC2B2AE3D27D4EB4F key

/-- Concrete source-side completeness condition for the masks and Bloom
words built from a sorted table interval. The C++ builder's final validation
loops check these bits for every stored key. -/
def SourceFilterBitsComplete (keys : ℕ → ℕ) (b e : ℕ)
    (masks : Fin 3 → ℕ) (words : ℕ → ℕ) : Prop :=
  ∀ j, b ≤ j → j < e →
    entryPresenceCheck masks (keys j) ∧
      bloomProbe words 0x9E3779B97F4A7C15 (keys j) = true ∧
      bloomProbe words 0xC2B2AE3D27D4EB4F (keys j) = true

noncomputable def sourceMaskPresence (masks : Fin 3 → ℕ) : ℕ → Bool :=
  by
    classical
    exact fun key => decide (entryPresenceCheck masks key)

theorem leftCandidatePresence_eq (p : Fin 3 ≃ Fin 3)
    (masks : Fin 3 → ℕ) (leftKey : ℕ) :
    leftCandidatePresence p masks leftKey =
      sourceMaskPresence masks (remap p.symm leftKey) := by
  classical
  unfold leftCandidatePresence sourceMaskPresence
  rw [leftCandidatePresenceCheck_eq]

theorem rightCandidatePresence_eq (p : Fin 3 ≃ Fin 3)
    (masks : Fin 3 → ℕ) (rightKey : ℕ) :
    rightCandidatePresence masks p rightKey =
      sourceMaskPresence masks (remap p rightKey) := by
  classical
  unfold rightCandidatePresence sourceMaskPresence
  rw [rightCandidatePresenceCheck_eq]

def sourceBloomPresence (words : ℕ → ℕ) : ℕ → Bool := bloomPresent words

/-- The builder-certified mask and Bloom bits supply exactly the
`FiltersComplete` condition needed by binary search. -/
theorem sourceFilterBits_filtersComplete (keys : ℕ → ℕ) (b e : ℕ)
    (masks : Fin 3 → ℕ) (words : ℕ → ℕ)
    (h : SourceFilterBitsComplete keys b e masks words) :
    FiltersComplete keys b e (sourceMaskPresence masks) (sourceBloomPresence words) := by
  intro j hb hj
  obtain ⟨hm,hb1,hb2⟩ := h j hb hj
  have hmask : sourceMaskPresence masks (keys j) = true := by
    simp [sourceMaskPresence, hm]
  have hbloom : sourceBloomPresence words (keys j) = true := by
    simp [sourceBloomPresence, bloomPresent, hb1, hb2]
  exact ⟨hmask,hbloom⟩

/-- The relation selected by the type-3 key remap, as a finite product of the
two table key sets. -/
def remapPairSet (p : Fin 3 ≃ Fin 3) (left right : Finset ℕ) : Finset (ℕ × ℕ) :=
  (left ×ˢ right).filter (fun pair => pair.2 = remap p.symm pair.1)

/-- The left iteration's word-pair list has exactly the table relation as its
`toFinset`; the range membership is derived from the actual source interval
and `filteredFind_iff`. -/
theorem leftWordPairs_toFinset (p : Fin 3 ≃ Fin 3)
    (sourceKeys targetKeys : ℕ → ℕ) (indices : List ℕ)
    (b e : ℕ) (hbe : b ≤ e) (targetStrict : ∀ i j, b ≤ i → i < j → j < e → targetKeys i < targetKeys j)
    (presence bloom : ℕ → Bool) (hfilters : FiltersComplete targetKeys b e presence bloom) :
    (leftWordPairs p sourceKeys targetKeys indices b e presence bloom).toFinset =
      remapPairSet p (indices.map sourceKeys).toFinset ((sourceIndices b e).map targetKeys).toFinset := by
  classical
  ext pair
  rcases pair with ⟨left,right⟩
  conv_lhs => rw [List.mem_toFinset]
  rw [leftWordPairs_mem p sourceKeys targetKeys indices b e hbe targetStrict
    presence bloom hfilters]
  rw [remapPairSet, Finset.mem_filter]
  change (∃ i, i ∈ indices ∧ sourceKeys i = left ∧
      ∃ j, b ≤ j ∧ j < e ∧ targetKeys j = right ∧ right = remap p.symm left) ↔
    ((left,right) ∈ ((indices.map sourceKeys).toFinset ×ˢ
      ((sourceIndices b e).map targetKeys).toFinset) ∧ right = remap p.symm left)
  rw [Finset.mem_product, List.mem_toFinset, List.mem_map,
    List.mem_toFinset, List.mem_map]
  constructor
  · intro h
    obtain ⟨i,hi,hsi,j,hbj,hje,htj,hrel⟩ := h
    exact ⟨⟨⟨i,hi,hsi⟩,⟨j,(sourceIndices_mem b e j).mpr ⟨hbj,hje⟩,htj⟩⟩,hrel⟩
  · rintro ⟨⟨⟨i,hi,hsi⟩,⟨j,hj,htj⟩⟩,hrel⟩
    have ⟨hbj,hje⟩ := (sourceIndices_mem b e j).mp hj
    exact ⟨i,hi,hsi,j,hbj,hje,htj,hrel⟩

/-- The right iteration has the same conventional left/right relation as the
left iteration, despite scanning the opposite side. -/
theorem rightWordPairs_toFinset (p : Fin 3 ≃ Fin 3)
    (rightKeys leftKeys : ℕ → ℕ) (indices : List ℕ)
    (b e : ℕ) (hbe : b ≤ e) (leftStrict : ∀ i j, b ≤ i → i < j → j < e → leftKeys i < leftKeys j)
    (leftBound : ∀ j, b ≤ j → j < e → leftKeys j < 2^48)
    (rightBound : ∀ i, i ∈ indices → rightKeys i < 2^48)
    (presence bloom : ℕ → Bool) (hfilters : FiltersComplete leftKeys b e presence bloom) :
    (rightWordPairs p rightKeys leftKeys indices b e presence bloom).toFinset =
      remapPairSet p ((sourceIndices b e).map leftKeys).toFinset (indices.map rightKeys).toFinset := by
  classical
  ext pair
  rcases pair with ⟨left,right⟩
  conv_lhs => rw [List.mem_toFinset]
  rw [rightWordPairs_mem p rightKeys leftKeys indices b e hbe leftStrict leftBound rightBound
    presence bloom hfilters]
  rw [remapPairSet, Finset.mem_filter]
  change (∃ i, i ∈ indices ∧ rightKeys i = right ∧
      ∃ j, b ≤ j ∧ j < e ∧ leftKeys j = left ∧ right = remap p.symm left) ↔
    ((left,right) ∈ ((sourceIndices b e).map leftKeys).toFinset ×ˢ
      (indices.map rightKeys).toFinset ∧ right = remap p.symm left)
  rw [Finset.mem_product, List.mem_toFinset, List.mem_map,
    List.mem_toFinset, List.mem_map]
  constructor
  · intro h
    obtain ⟨i,hi,hri,j,hbj,hje,hlj,hrel⟩ := h
    exact ⟨⟨⟨j,(sourceIndices_mem b e j).mpr ⟨hbj,hje⟩,hlj⟩,⟨i,hi,hri⟩⟩,hrel⟩
  · rintro ⟨⟨⟨j,hj,hlj⟩,⟨i,hi,hri⟩⟩,hrel⟩
    have ⟨hbj,hje⟩ := (sourceIndices_mem b e j).mp hj
    exact ⟨i,hi,hri,j,hbj,hje,hlj,hrel⟩

end Refpoly.JoinType3
