import Refpoly.JoinType3Filters
import Refpoly.JoinType3Geometry
import Refpoly.JoinType3Search
import Refpoly.JoinType3Sound
import Refpoly.JoinCandidateCertificate

/-! Type-3 model assembly. Geometry, key-word matching and row facts derive
coverage, compatibility, values, model np and rejection soundness. These are
valid conditional model theorems. The public scan still uses `eventSummary`,
whose count is a Layout fibre cardinality; refinement of the *actual stored*
JoinKey.count/cats/pairs to this summary is not yet composed here. In particular
FibreFacts.count_bound is a bound, not the missing stored-count equality. See
GPU_PIPELINE_STATUS.md before treating this as a complete CUDA source proof. -/
namespace Refpoly.JoinType3
open Refpoly.JoinPoints Refpoly.JoinRejection Refpoly.JoinLoopSource

structure FibreFacts (L : Layout 7 2) : Prop where
  fields : ∀ r i x, x ∈ fibre L r i → ∀ q : Fin (L.size r), q.val < L.k r → x q = L.fields r i q
  decoded : ∀ r i (q : Fin (L.size r)), q.val < L.k r → L.fields r i q = field 3 q.val i
  keys : ∀ r x, x ∈ L.sol r → L.prefixKey r x ∈ L.keys r
  width : ∀ r i, i ∈ L.keys r → i < 2^48
  count_bound : ∀ r i, i ∈ L.keys r → (fibre L r i).card < 2^32
  nonempty : ∀ r i, i ∈ L.keys r → (fibre L r i).Nonempty

variable {m : ℕ} (C : CWS 2 7) (L : Layout 7 2) (p : Fin 3 ≃ Fin 3)

/-- Composition theorem for either source iteration direction. Exact word-pair
membership is supplied by leftWordPairs_mem/rightWordPairs_mem, not an assumed
point-covering enumeration. -/
theorem word_scan_rejection_sound (g : Geometry L p) (f : FibreFacts L) (rows : RowCertificate C L)
    (tests : Fin m → Test 7) (cap : ℕ) (lookup : ℕ → Option ℕ) (indices : List ℕ)
    (eventOf : ℕ × ℕ → LeafEvent 7 2) (words : List (ℕ × ℕ))
    (hn : words.Nodup)
    (hwords : ∀ l r, (l,r) ∈ words ↔ l ∈ L.keys 0 ∧ r ∈ L.keys 1 ∧ r = remap p.symm l)
    (hevents : wordEvents words = (matchedPairs indices lookup).map eventOf)
    (out : Summary m)
    (hsource : sourceScan cap lookup (fun ij => eventSummary L tests (eventOf ij)) indices
      (emptySummary m) = .complete out)
    (hreject : verdict (.complete out) ≠ .keep) : ¬ C.IsIP := by
  let hw := geometry_wf L p g f.fields
  apply fused_rejection_sound C L hw (geometry_covered L p g) (sourceTuples L p)
    (sourceTuples_compatible L p g hw f.decoded) (sourceTuples_nonempty L p f.nonempty)
    (sourceTuples_onto L p g hw f.decoded f.keys f.width) rows.equations tests cap lookup indices
    eventOf (wordEvents words) out hevents hsource (wordEvents_nodup words hn)
    (wordEvents_tuples L p words hwords)
  · exact wordEvents_values L p g f.decoded words hwords
  · exact hreject

/-- The same complete scan's output is the exact number of simultaneous points,
which is the cardinality the downstream walk certificate must match. -/
theorem word_scan_count_exact (g : Geometry L p) (f : FibreFacts L)
    (tests : Fin m → Test 7) (cap : ℕ) (lookup : ℕ → Option ℕ) (indices : List ℕ)
    (eventOf : ℕ × ℕ → LeafEvent 7 2) (words : List (ℕ × ℕ)) (hn : words.Nodup)
    (hwords : ∀ l r, (l,r) ∈ words ↔ l ∈ L.keys 0 ∧ r ∈ L.keys 1 ∧ r = remap p.symm l)
    (hevents : wordEvents words = (matchedPairs indices lookup).map eventOf) (out : Summary m)
    (hsource : sourceScan cap lookup (fun ij => eventSummary L tests (eventOf ij)) indices
      (emptySummary m) = .complete out) : out.count = Nat.card {X // IsPoint L X} := by
  have hc := complete_count_eq_index L tests cap lookup indices eventOf (wordEvents words) out
    hevents hsource (sourceTuples L p) (wordEvents_nodup words hn) (wordEvents_tuples L p words hwords)
  let hw := geometry_wf L p g f.fields
  rw [hc,index_card,point_card L hw (geometry_covered L p g) (sourceTuples L p)
    (sourceTuples_compatible L p g hw f.decoded)
    (sourceTuples_onto L p g hw f.decoded f.keys f.width)]

/-- Concrete left-shorter source entry point: only sorted table ranges,
validated key-set representations and builder-checked filters are required. -/
theorem left_source_rejection_sound (g : Geometry L p) (f : FibreFacts L) (rows : RowCertificate C L)
    (leftKeys rightKeys : ℕ → ℕ) (lb le rb re : ℕ) (hl : lb ≤ le) (hr : rb ≤ re)
    (ls : ∀ i j, lb ≤ i → i < j → j < le → leftKeys i < leftKeys j)
    (rs : ∀ i j, rb ≤ i → i < j → j < re → rightKeys i < rightKeys j)
    (hleft : L.keys 0 = (sourceIndices lb le).toFinset.image leftKeys)
    (hright : L.keys 1 = (sourceIndices rb re).toFinset.image rightKeys)
    (presence bloom : ℕ → Bool) (hf : FiltersComplete rightKeys rb re presence bloom)
    (tests : Fin m → Test 7) (cap : ℕ) (out : Summary m)
    (hs : sourceScan cap (leftLookup p leftKeys rightKeys rb re presence bloom)
      (fun ij => eventSummary L tests (eventOfWord (leftKeys ij.1,rightKeys ij.2)))
      (sourceIndices lb le) (emptySummary m) = .complete out)
    (hreject : verdict (.complete out) ≠ .keep) : ¬ C.IsIP := by
  classical
  let words := leftWordPairs p leftKeys rightKeys (sourceIndices lb le) rb re presence bloom
  have hn : words.Nodup := leftWordPairs_nodup p leftKeys rightKeys _ (sourceIndices_nodup lb le)
    rb re hr (strict_key_injective ls) rs presence bloom hf
  have hm : ∀ l r, (l,r) ∈ words ↔ l ∈ L.keys 0 ∧ r ∈ L.keys 1 ∧ r = remap p.symm l := by
    intro l r
    rw [leftWordPairs_mem p leftKeys rightKeys _ rb re hr rs presence bloom hf]
    rw [hleft,hright]
    simp only [Finset.mem_image,List.mem_toFinset]
    constructor
    · rintro ⟨i,hi,he,j,hbj,hje,hkey,hrel⟩
      exact ⟨⟨i,hi,he⟩,⟨j,(sourceIndices_mem rb re j).mpr ⟨hbj,hje⟩,hkey⟩,hrel⟩
    · rintro ⟨⟨i,hi,he⟩,⟨j,hj,hkey⟩,hrel⟩
      have hj' := (sourceIndices_mem rb re j).mp hj
      exact ⟨i,hi,he,j,hj'.1,hj'.2,hkey,hrel⟩
  apply word_scan_rejection_sound C L p g f rows tests cap _ _ _ words hn hm
    (by simp [wordEvents,words,leftWordPairs,List.map_map,Function.comp_def]) out hs hreject



/-- Concrete right-shorter entry point, preserving the same left/right tuple
meaning and covering the exact same simultaneous point set. -/
theorem right_source_rejection_sound (g : Geometry L p) (f : FibreFacts L) (rows : RowCertificate C L)
    (leftKeys rightKeys : ℕ → ℕ) (lb le rb re : ℕ) (hl : lb ≤ le) (hr : rb ≤ re)
    (ls : ∀ i j, lb ≤ i → i < j → j < le → leftKeys i < leftKeys j)
    (rs : ∀ i j, rb ≤ i → i < j → j < re → rightKeys i < rightKeys j)
    (hleft : L.keys 0 = (sourceIndices lb le).toFinset.image leftKeys)
    (hright : L.keys 1 = (sourceIndices rb re).toFinset.image rightKeys)
    (presence bloom : ℕ → Bool) (hf : FiltersComplete leftKeys lb le presence bloom)
    (tests : Fin m → Test 7) (cap : ℕ) (out : Summary m)
    (hs : sourceScan cap (rightLookup p rightKeys leftKeys lb le presence bloom)
      (fun ij => eventSummary L tests (eventOfWord (leftKeys ij.2,rightKeys ij.1)))
      (sourceIndices rb re) (emptySummary m) = .complete out)
    (hreject : verdict (.complete out) ≠ .keep) : ¬ C.IsIP := by
  classical
  let words := rightWordPairs p rightKeys leftKeys (sourceIndices rb re) lb le presence bloom
  have hn : words.Nodup := rightWordPairs_nodup p rightKeys leftKeys _ (sourceIndices_nodup rb re)
    lb le hl (strict_key_injective rs) ls presence bloom hf
  have lw : ∀ j, lb ≤ j → j < le → leftKeys j < 2^48 := by
    intro j hb hj
    apply f.width 0
    rw [hleft]
    exact Finset.mem_image.mpr ⟨j,List.mem_toFinset.mpr ((sourceIndices_mem _ _ _).mpr ⟨hb,hj⟩),rfl⟩
  have rwid : ∀ i ∈ sourceIndices rb re, rightKeys i < 2^48 := by
    intro i hi
    apply f.width 1
    rw [hright]
    exact Finset.mem_image.mpr ⟨i,List.mem_toFinset.mpr hi,rfl⟩
  have hm : ∀ l r, (l,r) ∈ words ↔ l ∈ L.keys 0 ∧ r ∈ L.keys 1 ∧ r = remap p.symm l := by
    intro l r
    rw [rightWordPairs_mem p rightKeys leftKeys _ lb le hl ls lw rwid presence bloom hf]
    rw [hleft,hright]
    simp only [Finset.mem_image,List.mem_toFinset]
    constructor
    · rintro ⟨i,hi,he,j,hbj,hje,hkey,hrel⟩
      exact ⟨⟨j,(sourceIndices_mem lb le j).mpr ⟨hbj,hje⟩,hkey⟩,⟨i,hi,he⟩,hrel⟩
    · rintro ⟨⟨j,hj,hkey⟩,⟨i,hi,he⟩,hrel⟩
      have hj' := (sourceIndices_mem lb le j).mp hj
      exact ⟨i,hi,he,j,hj'.1,hj'.2,hkey,hrel⟩
  apply word_scan_rejection_sound C L p g f rows tests cap _ _ _ words hn hm
    (by simp [wordEvents,words,rightWordPairs,List.map_map,Function.comp_def]) out hs hreject


/-- The exact count is for the production CWS points, not a replacement point
specification: the primitive row certificate derives their identity. -/
theorem word_scan_count_cws (g : Geometry L p) (f : FibreFacts L) (rows : RowCertificate C L)
    (tests : Fin m → Test 7) (cap : ℕ) (lookup : ℕ → Option ℕ) (indices : List ℕ)
    (eventOf : ℕ × ℕ → LeafEvent 7 2) (words : List (ℕ × ℕ)) (hn : words.Nodup)
    (hwords : ∀ l r, (l,r) ∈ words ↔ l ∈ L.keys 0 ∧ r ∈ L.keys 1 ∧ r = remap p.symm l)
    (hevents : wordEvents words = (matchedPairs indices lookup).map eventOf) (out : Summary m)
    (hsource : sourceScan cap lookup (fun ij => eventSummary L tests (eventOf ij)) indices
      (emptySummary m) = .complete out) : out.count = Nat.card (CWSPoint C) := by
  have hc := word_scan_count_exact L p g f tests cap lookup indices eventOf words hn hwords hevents out hsource
  let e : {X // IsPoint L X} ≃ CWSPoint C :=
    { toFun := fun X => ⟨X.val,(rows.equations X.val).mp X.property⟩
      invFun := fun X => ⟨X.val,(rows.equations X.val).mpr X.property⟩
      left_inv := fun _ => rfl
      right_inv := fun _ => rfl }
  exact hc.trans (Nat.card_congr e)


/-- Every actual typed source count operation is exact in uint64. The cap's
Fin type is precisely a nonnegative int32; row bounds come from uint32 counts
accepted by the row validator, not from an assumed exact join count. -/
theorem typed_step_safe (f : FibreFacts L) (cap : Fin (2^31)) (np left right : ℕ)
    (hnp : np ≤ cap.val) (hl : left ∈ L.keys 0) (hr : right ∈ L.keys 1) :
    np + (fibre L 0 left).card * (fibre L 1 right).card < 2^64 := by
  apply count_u64 np _ _ cap.val hnp
  · have := cap.isLt; omega
  · have := f.count_bound 0 left hl; omega
  · have := f.count_bound 1 right hr; omega

/-- Public left-side entry point with the literal checked mask/Bloom data.
The normal-return source builder establishes checkedRangeFilters. -/
theorem left_checked_source_rejection_sound (g : Geometry L p) (f : FibreFacts L) (rows : RowCertificate C L)
    (leftKeys rightKeys : ℕ → ℕ) (lb le rb re : ℕ) (hl : lb ≤ le) (hr : rb ≤ re)
    (ls : ∀ i j, lb ≤ i → i < j → j < le → leftKeys i < leftKeys j)
    (rs : ∀ i j, rb ≤ i → i < j → j < re → rightKeys i < rightKeys j)
    (hleft : L.keys 0 = (sourceIndices lb le).toFinset.image leftKeys)
    (hright : L.keys 1 = (sourceIndices rb re).toFinset.image rightKeys)
    (masks : Fin 3 → ℕ) (bits : ℕ → ℕ)
    (hc : checkedRangeFilters rightKeys rb re masks bits = true)
    (tests : Fin m → Test 7) (cap : Fin (2^31)) (out : Summary m)
    (hs : sourceScan cap.val (leftLookup p leftKeys rightKeys rb re (presenceBits masks) (bloomBits bits))
      (fun ij => eventSummary L tests (eventOfWord (leftKeys ij.1,rightKeys ij.2)))
      (sourceIndices lb le) (emptySummary m) = .complete out)
    (hreject : verdict (.complete out) ≠ .keep) : ¬ C.IsIP :=
  left_source_rejection_sound C L p g f rows leftKeys rightKeys lb le rb re hl hr ls rs hleft hright
    (presenceBits masks) (bloomBits bits) (checked_filters_complete rightKeys rb re masks bits hc)
    tests cap.val out hs hreject

/-- Public right-side entry point, with the same literal builder checks. -/
theorem right_checked_source_rejection_sound (g : Geometry L p) (f : FibreFacts L) (rows : RowCertificate C L)
    (leftKeys rightKeys : ℕ → ℕ) (lb le rb re : ℕ) (hl : lb ≤ le) (hr : rb ≤ re)
    (ls : ∀ i j, lb ≤ i → i < j → j < le → leftKeys i < leftKeys j)
    (rs : ∀ i j, rb ≤ i → i < j → j < re → rightKeys i < rightKeys j)
    (hleft : L.keys 0 = (sourceIndices lb le).toFinset.image leftKeys)
    (hright : L.keys 1 = (sourceIndices rb re).toFinset.image rightKeys)
    (masks : Fin 3 → ℕ) (bits : ℕ → ℕ)
    (hc : checkedRangeFilters leftKeys lb le masks bits = true)
    (tests : Fin m → Test 7) (cap : Fin (2^31)) (out : Summary m)
    (hs : sourceScan cap.val (rightLookup p rightKeys leftKeys lb le (presenceBits masks) (bloomBits bits))
      (fun ij => eventSummary L tests (eventOfWord (leftKeys ij.2,rightKeys ij.1)))
      (sourceIndices rb re) (emptySummary m) = .complete out)
    (hreject : verdict (.complete out) ≠ .keep) : ¬ C.IsIP :=
  right_source_rejection_sound C L p g f rows leftKeys rightKeys lb le rb re hl hr ls rs hleft hright
    (presenceBits masks) (bloomBits bits) (checked_filters_complete leftKeys lb le masks bits hc)
    tests cap.val out hs hreject
end Refpoly.JoinType3
