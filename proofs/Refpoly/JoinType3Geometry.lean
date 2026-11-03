import Refpoly.JoinType3

/-! The checked type-3 descriptor's two rows, including the entry-order
permutation of the right shared prefix. This constructs coverage and the
shared/private geometry; it does not assume compatibility or point coverage. -/
namespace Refpoly.JoinType3
open Refpoly.JoinPoints Refpoly.JoinLoopSource

structure Geometry (L : Layout 7 2) (p : Fin 3 ≃ Fin 3) : Prop where
  size : ∀ r, L.size r = 5
  shared : ∀ r, L.k r = 3
  left : ∀ q : Fin (L.size 0), (L.pos 0 q).val = q.val
  right : ∀ q : Fin (L.size 1), (L.pos 1 q).val =
    if h : q.val < 3 then (p.symm ⟨q.val,h⟩).val else q.val+2

variable (L : Layout 7 2) (p : Fin 3 ≃ Fin 3)

theorem geometry_covered (g : Geometry L p) : Covered L := by
  intro A
  by_cases ha : A.val < 5
  · let q : Fin (L.size 0) := ⟨A.val, by rw [g.size]; exact ha⟩
    exact ⟨0,q,Fin.ext (by simpa [q] using g.left q)⟩
  · let q : Fin (L.size 1) := ⟨A.val-2,by rw [g.size]; have := A.isLt; omega⟩
    refine ⟨1,q,Fin.ext ?_⟩
    rw [g.right]
    have hq : ¬q.val < 3 := by dsimp [q]; omega
    rw [dif_neg hq]
    dsimp [q]
    omega

theorem geometry_pos_inj (g : Geometry L p) : ∀ r, Function.Injective (L.pos r) := by
  intro r
  fin_cases r
  · change Function.Injective (L.pos 0)
    intro a b he; apply Fin.ext
    have h := congrArg Fin.val he
    rw [g.left a,g.left b] at h
    exact h
  · change Function.Injective (L.pos 1)
    intro a b he
    have h := congrArg Fin.val he
    rw [g.right a,g.right b] at h
    by_cases ha : a.val < 3 <;> by_cases hb : b.val < 3
    · simp only [dif_pos ha,dif_pos hb] at h
      have hh := p.symm.injective (Fin.ext h)
      have hv : a.val = b.val := congrArg (fun q : Fin 3 => q.val) hh
      exact Fin.ext hv
    · simp only [dif_pos ha,dif_neg hb] at h
      have hl := (p.symm ⟨a.val,ha⟩).isLt
      omega
    · simp only [dif_neg ha,dif_pos hb] at h
      have hl := (p.symm ⟨b.val,hb⟩).isLt
      omega
    · simp only [dif_neg ha,dif_neg hb] at h
      exact Fin.ext (by omega)

theorem geometry_overlap (g : Geometry L p) : ∀ r s (qa : Fin (L.size r)) (qb : Fin (L.size s)),
    r ≠ s → L.pos r qa = L.pos s qb → qa.val < L.k r ∧ qb.val < L.k s := by
  intro r s qa qb hne he
  fin_cases r <;> fin_cases s <;> try contradiction
  · have ha := qa.isLt; have hb := qb.isLt
    change qa.val < L.size 0 at ha
    change qb.val < L.size 1 at hb
    have hs0 := g.size 0
    have hs1 := g.size 1
    have h := congrArg Fin.val he
    change (L.pos 0 qa).val = (L.pos 1 qb).val at h
    rw [g.left qa,g.right qb] at h
    by_cases hqb : qb.val < 3
    · rw [dif_pos hqb] at h
      have hbound := (p.symm ⟨qb.val,hqb⟩).isLt
      simpa [g.shared] using And.intro (show qa.val<3 by omega) hqb
    · rw [dif_neg hqb] at h
      omega
  · have ha := qa.isLt; have hb := qb.isLt
    change qa.val < L.size 1 at ha
    change qb.val < L.size 0 at hb
    have hs0 := g.size 0
    have hs1 := g.size 1
    have h := congrArg Fin.val he
    change (L.pos 1 qa).val = (L.pos 0 qb).val at h
    rw [g.right qa,g.left qb] at h
    by_cases hqa : qa.val < 3
    · rw [dif_pos hqa] at h
      have hbound := (p.symm ⟨qa.val,hqa⟩).isLt
      simpa [g.shared] using And.intro hqa (show qb.val<3 by omega)
    · rw [dif_neg hqa] at h
      omega

theorem geometry_wf (g : Geometry L p)
    (hfields : ∀ r i x, x ∈ fibre L r i → ∀ q : Fin (L.size r), q.val < L.k r →
      x q = L.fields r i q) : Wf L :=
  ⟨hfields,geometry_overlap L p g,geometry_pos_inj L p g⟩

def keyTuple (left right : ℕ) : Fin 2 → ℕ := fun r => if r=0 then left else right

def leftValues (key : ℕ) (A : Fin 7) : ℕ := field 3 A.val key

/-- The actual fused shared values are decoded from the left key. Matching
against the inverse-permuted right key establishes every row's shared value. -/
theorem geometry_values (g : Geometry L p) (left right : ℕ) (hmatch : right = remap p.symm left)
    (hfields : ∀ r i (q : Fin (L.size r)), q.val < L.k r →
      L.fields r i q = field 3 q.val i) :
    ∀ r (q : Fin (L.size r)), q.val < L.k r →
      leftValues left (L.pos r q) = L.fields r (keyTuple left right r) q := by
  intro r q hq
  rw [g.shared] at hq
  rw [hfields r _ q (by simpa [g.shared] using hq)]
  fin_cases r
  · change field 3 (L.pos 0 q).val left = field 3 q.val left
    rw [g.left q]
  · change field 3 (L.pos 1 q).val left = field 3 q.val right
    rw [g.right q,dif_pos hq,hmatch]
    exact (fields_remap p.symm left ⟨q.val,hq⟩).symm

/-- Normalized table prefix keys of any simultaneous point satisfy the exact
relation used by the fused lookup, so the search cannot omit that point. -/
theorem point_keys_match (g : Geometry L p) (hw : Wf L)
    (hfields : ∀ r i (q : Fin (L.size r)), q.val < L.k r → L.fields r i q = field 3 q.val i)
    (hwidth : ∀ X, IsPoint L X → ∀ r, tupleOf L X r < 2^48)
    (X : Fin 7 → ℕ) (hX : IsPoint L X) :
    tupleOf L X 1 = remap p.symm (tupleOf L X 0) := by
  apply fields_injective (hwidth X hX 1) (remap_lt _ _)
  funext i
  have hin : InBlock L (tupleOf L X) X := (point_block_unique L X _).mp ⟨hX,rfl⟩
  let qr : Fin (L.size 1) := ⟨i.val,by rw [g.size]; have := i.isLt; omega⟩
  let ql : Fin (L.size 0) := ⟨(p.symm i).val,by rw [g.size]; have := (p.symm i).isLt; omega⟩
  have hr := hw.key_fields 1 _ _ (hin 1) qr (by simp [qr,g.shared])
  have hl := hw.key_fields 0 _ _ (hin 0) ql (by simp [ql,g.shared])
  rw [hfields 1 _ qr (by simp [qr,g.shared])] at hr
  rw [hfields 0 _ ql (by simp [ql,g.shared])] at hl
  have hpos : L.pos 1 qr = L.pos 0 ql := by
    apply Fin.ext
    rw [g.right qr,g.left ql,dif_pos (show qr.val<3 by exact i.isLt)]
  rw [fields_remap]
  change field 3 i.val (tupleOf L X 1) = field 3 (p.symm i).val (tupleOf L X 0)
  rw [← hr,← hl]
  change X (L.pos 1 qr) = X (L.pos 0 ql)
  rw [hpos]


theorem keyTuple_eta (t : Fin 2 → ℕ) : keyTuple (t 0) (t 1) = t := by
  funext r
  fin_cases r
  · change keyTuple (t 0) (t 1) 0 = t 0; simp [keyTuple]
  · change keyTuple (t 0) (t 1) 1 = t 1; simp [keyTuple]

theorem keyTuple_injective : Function.Injective (fun lr : ℕ × ℕ => keyTuple lr.1 lr.2) := by
  intro a b he
  apply Prod.ext
  · simpa [keyTuple] using congrFun he 0
  · simpa [keyTuple] using congrFun he 1

noncomputable def sourceTuples : Finset (Fin 2 → ℕ) := by
  classical
  exact (((L.keys 0).product (L.keys 1)).filter (fun lr => lr.2 = remap p.symm lr.1)).image
    (fun lr => keyTuple lr.1 lr.2)

theorem sourceTuples_mem (t : Fin 2 → ℕ) : t ∈ sourceTuples L p ↔
    t 0 ∈ L.keys 0 ∧ t 1 ∈ L.keys 1 ∧ t 1 = remap p.symm (t 0) := by
  classical
  constructor
  · intro ht
    obtain ⟨⟨l,r⟩,hp,rfl⟩ := Finset.mem_image.mp ht
    obtain ⟨hm,he⟩ := Finset.mem_filter.mp hp
    have hh := Finset.mem_product.mp hm
    simpa [keyTuple] using And.intro hh.1 (And.intro hh.2 he)
  · rintro ⟨hl,hr,hm⟩
    exact Finset.mem_image.mpr ⟨(t 0,t 1), Finset.mem_filter.mpr
      ⟨Finset.mem_product.mpr ⟨hl,hr⟩,hm⟩,keyTuple_eta t⟩

theorem sourceTuples_compatible (g : Geometry L p) (hw : Wf L)
    (hfields : ∀ r i (q : Fin (L.size r)), q.val < L.k r → L.fields r i q = field 3 q.val i) :
    ∀ t ∈ sourceTuples L p, Compatible L t := by
  intro t ht
  obtain ⟨_,_,hrel⟩ := (sourceTuples_mem L p t).mp ht
  have hv := geometry_values L p g (t 0) (t 1) hrel hfields
  rw [keyTuple_eta] at hv
  apply compatible_of_fields L hw
  intro r s qa qb hqa hqb hpos
  rw [← hv r qa hqa,← hv s qb hqb,hpos]

theorem sourceTuples_onto (g : Geometry L p) (hw : Wf L)
    (hfields : ∀ r i (q : Fin (L.size r)), q.val < L.k r → L.fields r i q = field 3 q.val i)
    (hkeys : ∀ r x, x ∈ L.sol r → L.prefixKey r x ∈ L.keys r)
    (hwidth : ∀ r i, i ∈ L.keys r → i < 2^48) :
    ∀ X, IsPoint L X → tupleOf L X ∈ sourceTuples L p := by
  intro X hX
  have hk := tuple_keys L hkeys X hX
  apply (sourceTuples_mem L p _).mpr
  refine ⟨hk 0,hk 1,?_⟩
  exact point_keys_match L p g hw hfields
    (fun Y hY r => hwidth r _ (tuple_keys L hkeys Y hY r)) X hX

theorem sourceTuples_nonempty (hne : ∀ r i, i ∈ L.keys r → (fibre L r i).Nonempty) :
    ∀ t ∈ sourceTuples L p, ∀ r, (fibre L r (t r)).Nonempty := by
  intro t ht r
  have hk := (sourceTuples_mem L p t).mp ht
  fin_cases r
  · exact hne 0 _ hk.1
  · exact hne 1 _ hk.2.1

def eventOfWord (lr : ℕ × ℕ) : (Fin 2 → ℕ) × (Fin 7 → ℕ) :=
  (keyTuple lr.1 lr.2,leftValues lr.1)

def wordEvents (words : List (ℕ × ℕ)) := words.map eventOfWord

theorem wordEvents_nodup (words : List (ℕ × ℕ)) (hn : words.Nodup) :
    ((wordEvents words).map Prod.fst).Nodup := by
  simp only [wordEvents,List.map_map,Function.comp_def,eventOfWord]
  exact hn.map keyTuple_injective

theorem wordEvents_tuples (words : List (ℕ × ℕ))
    (hwords : ∀ l r, (l,r) ∈ words ↔ l ∈ L.keys 0 ∧ r ∈ L.keys 1 ∧ r = remap p.symm l) :
    ((wordEvents words).map Prod.fst).toFinset = sourceTuples L p := by
  classical
  ext t
  simp only [List.mem_toFinset,List.mem_map,wordEvents]
  constructor
  · rintro ⟨e,⟨⟨l,r⟩,hw,he⟩,ht⟩
    subst e
    change keyTuple l r = t at ht
    subst t
    exact (sourceTuples_mem L p _).mpr (by simpa [keyTuple] using (hwords l r).mp hw)
  · intro ht
    obtain ⟨hl,hr,hm⟩ := (sourceTuples_mem L p t).mp ht
    refine ⟨eventOfWord (t 0,t 1),⟨(t 0,t 1),(hwords _ _).mpr ⟨hl,hr,hm⟩,rfl⟩,?_⟩
    exact keyTuple_eta t

theorem wordEvents_values (g : Geometry L p)
    (hfields : ∀ r i (q : Fin (L.size r)), q.val < L.k r → L.fields r i q = field 3 q.val i)
    (words : List (ℕ × ℕ))
    (hwords : ∀ l r, (l,r) ∈ words ↔ l ∈ L.keys 0 ∧ r ∈ L.keys 1 ∧ r = remap p.symm l) :
    ∀ e ∈ wordEvents words, ∀ r (q : Fin (L.size r)), q.val < L.k r →
      e.2 (L.pos r q) = L.fields r (e.1 r) q := by
  rintro e he
  obtain ⟨⟨l,r⟩,hw,rfl⟩ := List.mem_map.mp he
  exact geometry_values L p g l r ((hwords l r).mp hw).2.2 hfields
end Refpoly.JoinType3
