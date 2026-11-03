import Refpoly.JoinLeafVerdict

/-! The source's shared/private class union and joint private pair summaries,
assembled from row fibres. Exact-mask premises are derived, not assumed. -/
namespace Refpoly.JoinPoints
open Refpoly.JoinRejection
variable {N R : ℕ} (L : Layout N R) (hcov : Covered L)
noncomputable def family (t : Fin R → ℕ) := Fintype.piFinset (fun r => fibre L r (t r))
def Shared (A : Fin N) : Prop := ∃ r, ∃ q : Fin (L.size r), q.val < L.k r ∧ L.pos r q = A
def SamePrivate (A B : Fin N) : Prop :=
  ∃ r, ∃ qa qb : Fin (L.size r), L.k r ≤ qa.val ∧ L.k r ≤ qb.val ∧ L.pos r qa = A ∧ L.pos r qb = B
def Values (t : Fin R → ℕ) (val : Fin N → ℕ) : Prop :=
  ∀ r (q : Fin (L.size r)), q.val < L.k r → val (L.pos r q) = L.fields r (t r) q
def SourceCoordinate (t : Fin R → ℕ) (val : Fin N → ℕ) (A : Fin N) (Q : ℕ → Prop) : Prop :=
  (Shared L A ∧ Q (val A)) ∨ ∃ r, ∃ q : Fin (L.size r), L.k r ≤ q.val ∧ L.pos r q = A ∧
    ∃ x ∈ fibre L r (t r), Q (x q)
def SourceJoint (t : Fin R → ℕ) (A B : Fin N) (Q : ℕ → ℕ → Prop) : Prop :=
  ∃ r, ∃ qa qb : Fin (L.size r), L.k r ≤ qa.val ∧ L.k r ≤ qb.val ∧
    L.pos r qa = A ∧ L.pos r qb = B ∧ ∃ x ∈ fibre L r (t r), Q (x qa) (x qb)
theorem family_at (hw : Wf L) (t : Fin R → ℕ) (ht : Compatible L t)
    (f) (hf : f ∈ family L t) (r : Fin R) (q : Fin (L.size r)) :
    glue L hcov f (L.pos r q) = f r q :=
  congrFun (glue_restr L hw.pos_inj hcov t ht f (Fintype.mem_piFinset.mp hf) r) q
theorem family_one (t : Fin R → ℕ) (hne : ∀ r, (fibre L r (t r)).Nonempty)
    (r : Fin R) (Q : (Fin (L.size r) → ℕ) → Prop) :
    (∃ f ∈ family L t, Q (f r)) ↔ ∃ x ∈ fibre L r (t r), Q x :=
  one_factor_occupancy _ hne r Q

theorem sourceCoordinate_exact (hw : Wf L) (t : Fin R → ℕ) (ht : Compatible L t)
    (hne : ∀ r, (fibre L r (t r)).Nonempty) (val : Fin N → ℕ) (hv : Values L t val)
    (A : Fin N) (Q : ℕ → Prop) :
    SourceCoordinate L t val A Q ↔ ∃ f ∈ family L t, Q (glue L hcov f A) := by
  classical
  constructor
  · rintro (⟨⟨r, q, hq, hpos⟩, hQ⟩ | ⟨r, q, _, hpos, x, hx, hQ⟩)
    · choose f hf using hne
      refine ⟨f, Fintype.mem_piFinset.mpr hf, ?_⟩
      rw [← hpos, family_at L hcov hw t ht f (Fintype.mem_piFinset.mpr hf) r q,
        hw.key_fields r _ _ (hf r) q hq, ← hv r q hq]
      simpa [hpos] using hQ
    · obtain ⟨f, hf, he⟩ := (family_one L t hne r (fun y => y = x)).mpr ⟨x, hx, rfl⟩
      refine ⟨f, hf, ?_⟩
      rw [← hpos, family_at L hcov hw t ht f hf r q, he]
      exact hQ
  · rintro ⟨f, hf, hQ⟩
    obtain ⟨r, q, hpos⟩ := hcov A
    have hfr := (Fintype.mem_piFinset.mp hf) r
    by_cases hq : q.val < L.k r
    · left
      refine ⟨⟨r, q, hq, hpos⟩, ?_⟩
      rw [← hpos, family_at L hcov hw t ht f hf r q, hw.key_fields r _ _ hfr q hq,
        ← hv r q hq] at hQ
      simpa [hpos] using hQ
    · right
      refine ⟨r, q, by omega, hpos, f r, hfr, ?_⟩
      rw [← hpos, family_at L hcov hw t ht f hf r q] at hQ
      exact hQ

theorem sourceJoint_exact (hw : Wf L) (t : Fin R → ℕ) (ht : Compatible L t)
    (hne : ∀ r, (fibre L r (t r)).Nonempty) (A B : Fin N)
    (hsame : SamePrivate L A B) (Q : ℕ → ℕ → Prop) :
    SourceJoint L t A B Q ↔ ∃ f ∈ family L t, Q (glue L hcov f A) (glue L hcov f B) := by
  constructor
  · rintro ⟨r, qa, qb, _, _, hA, hB, x, hx, hQ⟩
    obtain ⟨f, hf, he⟩ := (family_one L t hne r (fun y => y = x)).mpr ⟨x, hx, rfl⟩
    refine ⟨f, hf, ?_⟩
    rw [← hA, ← hB, family_at L hcov hw t ht f hf r qa,
      family_at L hcov hw t ht f hf r qb, he]
    exact hQ
  · rintro ⟨f, hf, hQ⟩
    obtain ⟨r, qa, qb, hqa, hqb, hA, hB⟩ := hsame
    refine ⟨r, qa, qb, hqa, hqb, hA, hB, f r, (Fintype.mem_piFinset.mp hf) r, ?_⟩
    rw [← hA, ← hB, family_at L hcov hw t ht f hf r qa,
      family_at L hcov hw t ht f hf r qb] at hQ
    exact hQ

theorem constant_factor_pair {α : Type*} (F : Finset α) (a b : α → ℕ)
    (hc : (∀ x ∈ F, ∀ y ∈ F, a x = a y) ∨ (∀ x ∈ F, ∀ y ∈ F, b x = b y))
    (Q : ℕ → ℕ → Prop) :
    (∃ x ∈ F, Q (a x) (b x)) ↔ ∃ x ∈ F, ∃ y ∈ F, Q (a x) (b y) := by
  constructor
  · rintro ⟨x, hx, hq⟩; exact ⟨x, hx, x, hx, hq⟩
  · rintro ⟨x, hx, y, hy, hq⟩
    rcases hc with ha | hb
    · exact ⟨y, hy, by rw [← ha x hx y hy]; exact hq⟩
    · exact ⟨x, hx, by rw [hb x hx y hy]; exact hq⟩

theorem independent_pair (hw : Wf L) (t : Fin R → ℕ) (ht : Compatible L t)
    (hne : ∀ r, (fibre L r (t r)).Nonempty) (A B : Fin N)
    (hnot : ¬ SamePrivate L A B) (Q : ℕ → ℕ → Prop) :
    (∃ f ∈ family L t, Q (glue L hcov f A) (glue L hcov f B)) ↔
      ∃ x ∈ fibre L (owner L hcov A).1 (t (owner L hcov A).1),
        ∃ y ∈ fibre L (owner L hcov B).1 (t (owner L hcov B).1),
          Q (x (owner L hcov A).2) (y (owner L hcov B).2) := by
  classical
  cases ha : owner L hcov A with
  | mk r qa =>
    cases hb : owner L hcov B with
    | mk s qb =>
      have hA : L.pos r qa = A := by
        have h := Classical.choose_spec (Classical.choose_spec (hcov A))
        change L.pos (owner L hcov A).1 (owner L hcov A).2 = A at h
        rw [ha] at h
        exact h
      have hB : L.pos s qb = B := by
        have h := Classical.choose_spec (Classical.choose_spec (hcov B))
        change L.pos (owner L hcov B).1 (owner L hcov B).2 = B at h
        rw [hb] at h
        exact h
      by_cases hrs : r = s
      · subst s
        have hshared : qa.val < L.k r ∨ qb.val < L.k r := by
          by_contra hn
          push_neg at hn
          exact hnot ⟨r, qa, qb, hn.1, hn.2, hA, hB⟩
        have hc : (∀ x ∈ fibre L r (t r), ∀ y ∈ fibre L r (t r), x qa = y qa) ∨
            (∀ x ∈ fibre L r (t r), ∀ y ∈ fibre L r (t r), x qb = y qb) := by
          rcases hshared with hqa | hqb
          · left; intro x hx y hy; rw [hw.key_fields r _ _ hx qa hqa, hw.key_fields r _ _ hy qa hqa]
          · right; intro x hx y hy; rw [hw.key_fields r _ _ hx qb hqb, hw.key_fields r _ _ hy qb hqb]
        have he := family_one L t hne r (fun x => Q (x qa) (x qb))
        have hg : (∃ f ∈ family L t, Q (glue L hcov f A) (glue L hcov f B)) ↔
            ∃ x ∈ fibre L r (t r), Q (x qa) (x qb) := by
          simpa only [glue_at L hcov ha, glue_at L hcov hb] using he
        simpa only [ha, hb] using hg.trans (constant_factor_pair _ _ _ hc Q)
      · have he := two_factor_occupancy (fun r => fibre L r (t r)) hne r s hrs
          (fun x y => Q (x qa) (y qb))
        simpa only [family, glue_at L hcov ha, glue_at L hcov hb, ha, hb] using he


theorem family_coordinate (t : Fin R → ℕ) (hne : ∀ r, (fibre L r (t r)).Nonempty)
    (A : Fin N) (Q : ℕ → Prop) :
    (∃ f ∈ family L t, Q (glue L hcov f A)) ↔
      ∃ x ∈ fibre L (owner L hcov A).1 (t (owner L hcov A).1), Q (x (owner L hcov A).2) :=
  family_one L t hne _ (fun x => Q (x (owner L hcov A).2))

noncomputable def sourceClass (t : Fin R → ℕ) (val : Fin N → ℕ) (A : Fin N) (c : Fin 4) : Bool := by
  classical
  exact decide (SourceCoordinate L t val A (fun x => valueClass x = c.val))

theorem sourceClass_eq (hw : Wf L) (t : Fin R → ℕ) (ht : Compatible L t)
    (hne : ∀ r, (fibre L r (t r)).Nonempty) (val : Fin N → ℕ) (hv : Values L t val) (A : Fin N) :
    sourceClass L t val A = classes (fibre L (owner L hcov A).1 (t (owner L hcov A).1))
      (fun x => x (owner L hcov A).2) := by
  funext c
  exact decide_eq_decide.mpr ((sourceCoordinate_exact L hcov hw t ht hne val hv A _).trans
    (family_coordinate L hcov t hne A (fun x => valueClass x = c.val)))

noncomputable def coordinateSides (t : Fin R → ℕ) (val : Fin N → ℕ) (A : Fin N) : Sides :=
  (sourceClass L t val A 0, sourceClass L t val A 2 || sourceClass L t val A 3)

noncomputable def pairSides (t : Fin R → ℕ) (val : Fin N → ℕ) (A B : Fin N) : Sides := by
  classical
  exact if SamePrivate L A B then
    (decide (SourceJoint L t A B (fun a b => a + b ≤ 1)),
     decide (SourceJoint L t A B (fun a b => 3 ≤ a + b)))
  else (leafLo (sourceClass L t val A) (sourceClass L t val B),
    leafHi (sourceClass L t val A) (sourceClass L t val B) || sourceClass L t val B 3)

theorem coordinateSides_exact (hw : Wf L) (t : Fin R → ℕ) (ht : Compatible L t)
    (hne : ∀ r, (fibre L r (t r)).Nonempty) (val : Fin N → ℕ) (hv : Values L t val) (A : Fin N) :
    coordinateSides L t val A =
      (decide (∃ f ∈ family L t, glue L hcov f A = 0),
       decide (∃ f ∈ family L t, 2 ≤ glue L hcov f A)) := by
  classical
  apply Prod.ext
  · change decide _ = decide _
    apply decide_eq_decide.mpr
    rw [sourceCoordinate_exact L hcov hw t ht hne val hv]
    simp [valueClass]
  · change (decide _ || decide _) = decide _
    rw [← Bool.decide_or]
    apply decide_eq_decide.mpr
    rw [sourceCoordinate_exact L hcov hw t ht hne val hv,
      sourceCoordinate_exact L hcov hw t ht hne val hv]
    constructor
    · rintro (⟨f, hf, hc⟩ | ⟨f, hf, hc⟩) <;> exact ⟨f, hf, by unfold valueClass at hc; omega⟩
    · rintro ⟨f, hf, hc⟩
      by_cases he : glue L hcov f A = 2
      · left; exact ⟨f, hf, by simp [valueClass, he]⟩
      · right; exact ⟨f, hf, by unfold valueClass; omega⟩

theorem pairSides_exact (hw : Wf L) (t : Fin R → ℕ) (ht : Compatible L t)
    (hne : ∀ r, (fibre L r (t r)).Nonempty) (val : Fin N → ℕ) (hv : Values L t val) (A B : Fin N) :
    pairSides L t val A B =
      (decide (∃ f ∈ family L t, glue L hcov f A + glue L hcov f B ≤ 1),
       decide (∃ f ∈ family L t, 3 ≤ glue L hcov f A + glue L hcov f B)) := by
  classical
  unfold pairSides
  split_ifs with hs
  · apply Prod.ext <;> apply decide_eq_decide.mpr
    · exact sourceJoint_exact L hcov hw t ht hne A B hs _
    · exact sourceJoint_exact L hcov hw t ht hne A B hs _
  · rw [sourceClass_eq L hcov hw t ht hne val hv A, sourceClass_eq L hcov hw t ht hne val hv B]
    apply Prod.ext
    · rw [leafLo_cuda, classes_lo]
      exact decide_eq_decide.mpr (independent_pair L hcov hw t ht hne A B hs (fun a b => a + b ≤ 1)).symm
    · rw [leafHi_cuda, classes_hi _ _ _ _ (hne _) (hne _)]
      exact decide_eq_decide.mpr (independent_pair L hcov hw t ht hne A B hs (fun a b => 3 ≤ a + b)).symm

inductive Test (N : ℕ) where
  | coordinate (a : Fin N)
  | pair (a b : Fin N)

def Test.coefficients : Test N → Fin N → Int
  | .coordinate a => fun i => if i = a then 1 else 0
  | .pair a b => fun i => (if i = a then 1 else 0) + (if i = b then 1 else 0)

def Test.eval : Test N → (Fin N → ℕ) → Int
  | .coordinate a, X => (X a : Int) - 1
  | .pair a b, X => (X a : Int) + (X b : Int) - 2

theorem Test.eval_coefficients (test : Test N) (X : Fin N → ℕ) :
    evalPoint test.coefficients X = test.eval X := by
  cases test <;> simp [evalPoint, Test.coefficients, Test.eval, add_mul, Finset.sum_add_distrib] <;> ring

noncomputable def testSides (t : Fin R → ℕ) (val : Fin N → ℕ) : Test N → Sides
  | .coordinate a => coordinateSides L t val a
  | .pair a b => pairSides L t val a b

theorem testSides_exact (hw : Wf L) (t : Fin R → ℕ) (ht : Compatible L t)
    (hne : ∀ r, (fibre L r (t r)).Nonempty) (val : Fin N → ℕ) (hv : Values L t val) (test : Test N) :
    testSides L t val test =
      (decide (∃ f ∈ family L t, test.eval (glue L hcov f) < 0),
       decide (∃ f ∈ family L t, 0 < test.eval (glue L hcov f))) := by
  classical
  cases test with
  | coordinate a =>
    rw [testSides, coordinateSides_exact L hcov hw t ht hne val hv]
    simp only [Test.eval, (coordinate_thresholds _).1, (coordinate_thresholds _).2]
  | pair a b =>
    rw [testSides, pairSides_exact L hcov hw t ht hne val hv]
    simp only [Test.eval, (pair_thresholds _ _).1, (pair_thresholds _ _).2]

/-! ## Accumulation over source leaf events

One event represents a tuple and its checked source key fields. For each event,
`testSides_exact` has already quantified over the full product of row fibres.
Accumulating event verdicts with Boolean OR therefore gives existential
occupancy over the whole point set when the event tuples cover `T`.
-/

abbrev LeafEvent (N R : ℕ) := (Fin R → ℕ) × (Fin N → ℕ)

noncomputable def propBool (p : Prop) : Bool := by
  classical
  exact decide p

theorem propBool_true (p : Prop) : propBool p = true ↔ p := by
  classical
  simp [propBool]

noncomputable def accumulatedTestSides (events : List (LeafEvent N R))
    (test : Test N) : Sides :=
  (events.any fun e => (testSides L e.1 e.2 test).1,
   events.any fun e => (testSides L e.1 e.2 test).2)

noncomputable def accumulatedSourceMasks (events : List (LeafEvent N R))
    (tests : List (Test N)) : List Sides :=
  tests.map (accumulatedTestSides L events)

theorem event_tuple_mem (events : List (LeafEvent N R))
    (T : Finset (Fin R → ℕ)) (hEvents : (events.map Prod.fst).toFinset = T)
    {e : LeafEvent N R} (he : e ∈ events) : e.1 ∈ T := by
  have hm : e.1 ∈ (events.map Prod.fst).toFinset := by
    apply List.mem_toFinset.mpr
    exact List.mem_map.mpr ⟨e, he, rfl⟩
  rw [hEvents] at hm
  exact hm

theorem event_for_tuple_unique (events : List (LeafEvent N R))
    (T : Finset (Fin R → ℕ)) (hnodup : (events.map Prod.fst).Nodup)
    (hEvents : (events.map Prod.fst).toFinset = T) (t : Fin R → ℕ) (ht : t ∈ T) :
    ∃! e, e ∈ events ∧ e.1 = t := by
  have ht' : t ∈ (events.map Prod.fst).toFinset := by rw [hEvents]; exact ht
  have heMap : t ∈ events.map Prod.fst := List.mem_toFinset.mp ht'
  obtain ⟨e, he, hfst⟩ := List.mem_map.mp heMap
  refine ⟨e, ⟨he, hfst⟩, ?_⟩
  intro e' he'
  rcases he' with ⟨he', hfst'⟩
  exact List.inj_on_of_nodup_map hnodup he' he (hfst'.trans hfst.symm)

theorem event_family_occupancy (hw : Wf L) (T : Finset (Fin R → ℕ))
    (hT : ∀ t ∈ T, Compatible L t)
    (hall : ∀ X, IsPoint L X → tupleOf L X ∈ T)
    (events : List (LeafEvent N R))
    (hEvents : (events.map Prod.fst).toFinset = T)
    (Q : (Fin N → ℕ) → Prop) :
    (∃ e ∈ events, ∃ f ∈ family L e.1, Q (glue L hcov f)) ↔
      ∃ X, IsPoint L X ∧ Q X := by
  constructor
  · rintro ⟨e, he, f, hf, hQ⟩
    have ht : e.1 ∈ T := event_tuple_mem events T hEvents he
    have hblock := glue_inBlock L hw.pos_inj hcov e.1 (hT e.1 ht) f
      (Fintype.mem_piFinset.mp hf)
    have hpoint := (point_block_unique L (glue L hcov f) e.1).mpr hblock |>.1
    exact ⟨glue L hcov f, hpoint, hQ⟩
  · rintro ⟨X, hX, hQ⟩
    obtain ⟨_, hfib, hglue⟩ := point_glue L hw hcov X hX
    let t := tupleOf L X
    have ht : t ∈ T := hall X hX
    have ht' : t ∈ (events.map Prod.fst).toFinset := by rw [hEvents]; exact ht
    have hpMap : t ∈ events.map Prod.fst := List.mem_toFinset.mp ht'
    obtain ⟨e, he, hfst⟩ := List.mem_map.mp hpMap
    let f : (r : Fin R) → Fin (L.size r) → ℕ := fun r => restr L r X
    have hf : f ∈ family L t := by
      rw [family, Fintype.mem_piFinset]
      intro r
      simpa [f] using hfib r
    have hfe : f ∈ family L e.1 := by rw [hfst]; exact hf
    refine ⟨e, he, f, hfe, ?_⟩
    change Q (glue L hcov (fun r => restr L r X))
    rw [hglue]
    exact hQ

theorem accumulated_test_sides_exact (hcov : Covered L) (hw : Wf L)
    (T : Finset (Fin R → ℕ)) (hT : ∀ t ∈ T, Compatible L t)
    (hne : ∀ t ∈ T, ∀ r, (fibre L r (t r)).Nonempty)
    (hall : ∀ X, IsPoint L X → tupleOf L X ∈ T)
    (events : List (LeafEvent N R))
    (hEvents : (events.map Prod.fst).toFinset = T)
    (hvalues : ∀ e ∈ events, Values L e.1 e.2) (test : Test N) :
    accumulatedTestSides L events test =
      (propBool (∃ X, IsPoint L X ∧ test.eval X < 0),
       propBool (∃ X, IsPoint L X ∧ 0 < test.eval X)) := by
  classical
  apply Prod.ext
  · change events.any (fun e => (testSides L e.1 e.2 test).1) = _
    apply Bool.eq_iff_iff.mpr
    simp only [List.any_eq_true, propBool_true]
    constructor
    · rintro ⟨e, he, hs⟩
      have ht := event_tuple_mem events T hEvents he
      have hside := testSides_exact L hcov hw e.1 (hT e.1 ht)
        (fun r => hne e.1 ht r) e.2 (hvalues e he) test
      rw [hside] at hs
      have hf : ∃ f ∈ family L e.1, test.eval (glue L hcov f) < 0 := by
        simpa using hs
      exact (event_family_occupancy L hcov hw T hT hall events hEvents
        (fun X => test.eval X < 0)).mp ⟨e, he, hf⟩
    · intro hX
      obtain ⟨e, he, hf⟩ := (event_family_occupancy L hcov hw T hT hall events hEvents
        (fun X => test.eval X < 0)).mpr hX
      have hside := testSides_exact L hcov hw e.1
        (hT e.1 (event_tuple_mem events T hEvents he))
        (fun r => hne e.1 (event_tuple_mem events T hEvents he) r)
        e.2 (hvalues e he) test
      refine ⟨e, he, ?_⟩
      rw [hside]
      exact decide_eq_true hf
  · change events.any (fun e => (testSides L e.1 e.2 test).2) = _
    apply Bool.eq_iff_iff.mpr
    simp only [List.any_eq_true, propBool_true]
    constructor
    · rintro ⟨e, he, hs⟩
      have ht := event_tuple_mem events T hEvents he
      have hside := testSides_exact L hcov hw e.1 (hT e.1 ht)
        (fun r => hne e.1 ht r) e.2 (hvalues e he) test
      rw [hside] at hs
      have hf : ∃ f ∈ family L e.1, 0 < test.eval (glue L hcov f) := by
        simpa using hs
      exact (event_family_occupancy L hcov hw T hT hall events hEvents
        (fun X => 0 < test.eval X)).mp ⟨e, he, hf⟩
    · intro hX
      obtain ⟨e, he, hf⟩ := (event_family_occupancy L hcov hw T hT hall events hEvents
        (fun X => 0 < test.eval X)).mpr hX
      have hside := testSides_exact L hcov hw e.1
        (hT e.1 (event_tuple_mem events T hEvents he))
        (fun r => hne e.1 (event_tuple_mem events T hEvents he) r)
        e.2 (hvalues e he) test
      refine ⟨e, he, ?_⟩
      rw [hside]
      exact decide_eq_true hf

theorem accumulated_masks_exact (hcov : Covered L) (hw : Wf L)
    (T : Finset (Fin R → ℕ)) (hT : ∀ t ∈ T, Compatible L t)
    (hne : ∀ t ∈ T, ∀ r, (fibre L r (t r)).Nonempty)
    (hall : ∀ X, IsPoint L X → tupleOf L X ∈ T)
    (events : List (LeafEvent N R))
    (hnodup : (events.map Prod.fst).Nodup)
    (hEvents : (events.map Prod.fst).toFinset = T)
    (hvalues : ∀ e ∈ events, Values L e.1 e.2) (tests : List (Test N)) :
    accumulatedSourceMasks L events tests =
      functionalSides L (tests.map Test.coefficients) ∧
      ∀ t ∈ T, ∃! e, e ∈ events ∧ e.1 = t := by
  classical
  constructor
  · unfold accumulatedSourceMasks functionalSides
    rw [List.map_map]
    apply List.map_congr_left
    intro test htest
    rw [accumulated_test_sides_exact L hcov hw T hT hne hall events hEvents hvalues test]
    apply Prod.ext <;> simp [propBool, Test.eval_coefficients]
  · intro t ht
    exact event_for_tuple_unique events T hnodup hEvents t ht

/-- Source-mask rejection with the source count derived from leaf products and
the accumulated masks derived from the checked events. `hrows` remains the
explicit candidate/table equation bridge identified in the pipeline audit. -/
theorem source_rejection_sound {R : ℕ} (C : CWS R (5 + R))
    (L : Layout (5 + R) R) (hw : Wf L) (hcov : Covered L)
    (T : Finset (Fin R → ℕ)) (hT : ∀ t ∈ T, Compatible L t)
    (hne : ∀ t ∈ T, ∀ r, (fibre L r (t r)).Nonempty)
    (hall : ∀ X, IsPoint L X → tupleOf L X ∈ T)
    (hrows : ∀ X, IsPoint L X ↔ ∀ a, ∑ i, C.W a i * (X i : ℝ) = ∑ i, C.W a i)
    (events : List (LeafEvent (5 + R) R))
    (hnodup : (events.map Prod.fst).Nodup)
    (hEvents : (events.map Prod.fst).toFinset = T)
    (hvalues : ∀ e ∈ events, Values L e.1 e.2)
    (max np : ℕ) (hmax : 1 ≤ max)
    (hcounts : Refpoly.JoinRejection.leafCounts max
      ((events.map Prod.fst).map (fun t => List.ofFn (fun r => (fibre L r (t r)).card))) 0 = some np)
    (tests : List (Test (5 + R)))
    (hreject : leafVerdict np [] (accumulatedSourceMasks L events tests) ≠ .keep) :
    ¬ C.IsIP := by
  have hcount := leafCounts_index L hmax (events.map Prod.fst) hnodup T hEvents hcounts
  have hmasks := (accumulated_masks_exact L hcov hw T hT hne hall events hnodup
    hEvents hvalues tests).1
  exact leaf_spec_rejection_sound C L hw hcov T hT hall hrows np _
    (tests.map Test.coefficients) hcount hmasks hreject

end Refpoly.JoinPoints
