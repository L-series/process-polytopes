import Refpoly.KS.RationalHull

/-!
The origin-IP test combines full ambient span with positive circuits through all
generators. Polar vertices are explicit rational adjugate solutions of active
inequalities. Eligibility pairs only actual primal and polar extreme points;
reference-lattice reflexivity instead requires integral polar coordinates.

These are total finite-enumeration reference routines, not a fast hull backend:
tuple enumeration is exponential in dimension and retains polar multiplicities.
-/

open Set Matrix

namespace Refpoly.KS

def pointMatrix {d : ℕ} (v : Fin d → IntPoint d) : Matrix (Fin d) (Fin d) ℤ :=
  fun i j => v i j

theorem pointMatrix_det_iff {d : ℕ} (v : Fin d → IntPoint d) :
    (pointMatrix v).det ≠ 0 ↔ LinearIndependent ℝ (fun i => ofInt (v i)) := by
  let A := (pointMatrix v).map (Int.castRingHom ℝ)
  have hcast : A.det = ((pointMatrix v).det : ℝ) :=
    ((Int.castRingHom ℝ).map_det (pointMatrix v)).symm
  have hli : LinearIndependent ℝ (fun i => ofInt (v i)) ↔ A.det ≠ 0 := by
    change LinearIndependent ℝ A.row ↔ _
    rw [Matrix.linearIndependent_rows_iff_isUnit, Matrix.isUnit_iff_isUnit_det,
      isUnit_iff_ne_zero]
  rw [hli, hcast]
  exact_mod_cast Iff.rfl

theorem integer_basis_tuple {d : ℕ} (xs : List (IntPoint d)) (s : Set (V d))
    (hspan : Submodule.span ℝ s = ⊤) (hsub : s ⊆ pointSet xs) :
    ∃ v : Fin d → IntPoint d, (∀ i, v i ∈ xs) ∧
      (pointMatrix v).det ≠ 0 ∧ ∀ i, ofInt (v i) ∈ s := by
  classical
  obtain ⟨b, hb⟩ := exists_basis_in_spanning_set s hspan
  have h (i : Fin d) : ∃ z ∈ xs, ofInt z = b i := hsub (hb i)
  choose v hv hev using h
  refine ⟨v, hv, (pointMatrix_det_iff v).mpr ?_, ?_⟩
  · simpa only [hev] using b.linearIndependent
  · simpa only [hev] using hb

def fullSpanCheckExact {d : ℕ} (xs : List (IntPoint d)) : Bool :=
  (tuples xs d).any fun v => decide ((pointMatrix v).det ≠ 0)

theorem fullSpanCheckExact_correct {d : ℕ} (xs : List (IntPoint d)) :
    fullSpanCheckExact xs = true ↔ Submodule.span ℝ (pointSet xs) = ⊤ := by
  simp only [fullSpanCheckExact, List.any_eq_true, mem_tuples, decide_eq_true_eq]
  constructor
  · rintro ⟨v, hv, hd⟩
    have hsp := ((pointMatrix_det_iff v).mp hd).span_eq_top_of_card_eq_finrank'
      (by simp [V])
    apply top_unique
    rw [← hsp]
    exact Submodule.span_mono (by rintro _ ⟨i, rfl⟩; exact ⟨v i, hv i, rfl⟩)
  · intro hs
    obtain ⟨v, hv, hd, _⟩ := integer_basis_tuple xs (pointSet xs) hs Subset.rfl
    exact ⟨v, hv, hd⟩

def positiveCircuitThroughExact {d : ℕ} (xs : List (IntPoint d)) (a : IntPoint d) :
    Bool :=
  (List.range (d + 2)).any fun m =>
    (tuples xs m).any fun v =>
      positiveSimplexCheck v && decide (∃ i, v i = a)

theorem positiveCircuitThroughExact_spec {d : ℕ} (xs : List (IntPoint d))
    (a : IntPoint d) :
    positiveCircuitThroughExact xs a = true ↔
      ∃ m, m ≤ d + 1 ∧ ∃ v : Fin m → IntPoint d,
        (∀ i, v i ∈ xs) ∧ positiveSimplexCheck v = true ∧ ∃ i, v i = a := by
  simp only [positiveCircuitThroughExact, List.any_eq_true, List.mem_range,
    mem_tuples, Bool.and_eq_true, decide_eq_true_eq]
  constructor
  · rintro ⟨m, hm, v, hv, hp, hi⟩
    exact ⟨m, by omega, v, hv, hp, hi⟩
  · rintro ⟨m, hm, v, hv, hp, hi⟩
    exact ⟨m, by omega, v, hv, hp, hi⟩

theorem positiveCircuitThroughExact_of_ip {d : ℕ} (Q : PolyCode d)
    (hQ : IsIP Q.hull) (a : IntPoint d) (ha : a ∈ Q.points) :
    positiveCircuitThroughExact Q.points a = true := by
  classical
  let S := (pointSet_finite Q.points).toFinset
  have hS : (S : Set (V d)) = pointSet Q.points :=
    (pointSet_finite Q.points).coe_toFinset
  obtain ⟨T, hTS, haT, hi, w, hw, hsum, hrel⟩ :=
    ip_positive_circuit_through S (ofInt a)
      (by rw [← Finset.mem_coe, hS]; exact ⟨a, ha, rfl⟩)
      (by simpa only [hS] using hQ)
  let e := T.equivFin
  have h (i : Fin T.card) : ∃ z ∈ Q.points, ofInt z = (e.symm i : V d) := by
    have hm : (e.symm i : V d) ∈ (S : Set (V d)) := hTS (e.symm i).property
    rw [hS] at hm
    exact hm
  choose v hv hev using h
  have hind : AffineIndependent ℝ (fun i => ofInt (v i)) := by
    simpa only [hev] using hi.comp_embedding e.symm.toEmbedding
  have hm : T.card ≤ d + 1 := by
    have hcard := hind.card_le_finrank_succ
    simp only [Fintype.card_fin] at hcard
    exact hcard.trans (Nat.add_le_add_right (by
      simpa [V] using Submodule.finrank_le (vectorSpan ℝ (range (fun i => ofInt (v i)))) ) 1)
  apply (positiveCircuitThroughExact_spec Q.points a).mpr
  refine ⟨T.card, hm, v, hv, (positiveSimplexCheck_iff v).mpr
    ⟨hind, fun i => w (e.symm i), fun i => hw _ (e.symm i).property, ?_, ?_⟩, ?_⟩
  · exact (e.symm.sum_comp (fun x : T => w x)).trans ((Finset.sum_coe_sort T w).trans hsum)
  · simpa only [hev] using
      (e.symm.sum_comp (fun x : T => w x • (x : V d))).trans
        ((Finset.sum_coe_sort T (fun x => w x • x)).trans hrel)
  · refine ⟨e ⟨ofInt a, haT⟩, ofInt_injective ?_⟩
    simp [hev]

theorem positiveCircuitThroughExact_intrinsic {d : ℕ} (xs : List (IntPoint d))
    (a : IntPoint d) (ha : positiveCircuitThroughExact xs a = true) :
    ∃ T : Finset (V d), (T : Set (V d)) ⊆ pointSet xs ∧ ofInt a ∈ T ∧
      (0 : V d) ∈ intrinsicInterior ℝ (convexHull ℝ (T : Set (V d))) := by
  classical
  obtain ⟨m, _, v, hv, hp, i, hi⟩ := (positiveCircuitThroughExact_spec xs a).mp ha
  obtain ⟨_, q, hq, hsum, hrel⟩ := (positiveSimplexCheck_iff v).mp hp
  let T := Finset.univ.image (fun i => ofInt (v i))
  have hT : (T : Set (V d)) = range (fun i => ofInt (v i)) := by simp [T]
  refine ⟨T, ?_, ?_, ?_⟩
  · rw [hT]
    rintro x ⟨j, rfl⟩
    exact ⟨v j, hv j, rfl⟩
  · rw [← Finset.mem_coe, hT]
    exact ⟨i, congrArg ofInt hi⟩
  · rw [hT]
    exact positive_relation_intrinsicInterior _ q hq hsum hrel

theorem interior_of_positive_circuits {d : ℕ} (S : Finset (V d)) (hS : S.Nonempty)
    (hspan : Submodule.span ℝ (S : Set (V d)) = ⊤)
    (hc : ∀ a ∈ S, ∃ T : Finset (V d), T ⊆ S ∧ a ∈ T ∧
      (0 : V d) ∈ intrinsicInterior ℝ (convexHull ℝ (T : Set (V d)))) :
    (0 : V d) ∈ interior (convexHull ℝ (S : Set (V d))) := by
  classical
  have hwitness (a : S) :
      ∃ w : V d → ℝ, (∀ x ∈ S, 0 ≤ w x) ∧ 0 < w a ∧
        ∑ x ∈ S, w x • x = 0 := by
    obtain ⟨T, hTS, haT, h0⟩ := hc a a.property
    obtain ⟨w, hw, _, hrel⟩ := exists_positive_relation_of_intrinsicInterior T h0
    let f : V d → ℝ := fun x => if x ∈ T then w x else 0
    refine ⟨f, ?_, by simpa [f, haT] using hw a haT, ?_⟩
    · intro x _
      dsimp [f]
      split_ifs with hx
      · exact (hw x hx).le
      · exact le_rfl
    · calc
        ∑ x ∈ S, f x • x = ∑ x ∈ T, f x • x :=
          (Finset.sum_subset hTS (fun x _ hx => by simp [f, hx])).symm
        _ = ∑ x ∈ T, w x • x :=
          Finset.sum_congr rfl (fun x hx => by simp [f, hx])
        _ = 0 := hrel
  choose w hw hpos hrel using hwitness
  let f : V d → ℝ := fun x => ∑ a : S, w a x
  have hf : ∀ x ∈ S, 0 < f x := by
    intro x hx
    exact Finset.sum_pos' (fun a _ => hw a x hx)
      ⟨⟨x, hx⟩, Finset.mem_univ _, hpos ⟨x, hx⟩⟩
  have hfrel : ∑ x ∈ S, f x • x = 0 := by
    simp only [f, Finset.sum_smul]
    rw [Finset.sum_comm]
    simp only [hrel, Finset.sum_const_zero]
  obtain ⟨q, hq, hsum, hqrel⟩ := normalize_positive_relation S hS f hf hfrel
  exact finset_positive_relation_interior S q hq hsum hqrel hspan

/-- Ambient origin-interior check, not merely relative interior. -/
def originIPCheckExact {d : ℕ} (Q : PolyCode d) : Bool :=
  decide (Q.points ≠ []) && fullSpanCheckExact Q.points &&
    Q.points.all (positiveCircuitThroughExact Q.points)

theorem originIPCheckExact_correct {d : ℕ} (Q : PolyCode d) :
    originIPCheckExact Q = true ↔ IsIP Q.hull := by
  classical
  simp only [originIPCheckExact, Bool.and_eq_true, decide_eq_true_eq,
    fullSpanCheckExact_correct, List.all_eq_true]
  constructor
  · rintro ⟨⟨hne, hspan⟩, hc⟩
    let S := (pointSet_finite Q.points).toFinset
    have hS : (S : Set (V d)) = pointSet Q.points :=
      (pointSet_finite Q.points).coe_toFinset
    have hSne : S.Nonempty := by
      obtain ⟨a, ha⟩ := List.exists_mem_of_ne_nil Q.points hne
      exact ⟨ofInt a, by rw [← Finset.mem_coe, hS]; exact ⟨a, ha, rfl⟩⟩
    change (0 : V d) ∈ interior (convexHull ℝ (pointSet Q.points))
    rw [← hS]
    apply interior_of_positive_circuits S hSne (by rwa [hS])
    · intro x hx
      have hx' : x ∈ pointSet Q.points := by rw [← hS]; exact hx
      obtain ⟨a, ha, rfl⟩ := hx'
      obtain ⟨T, hT, haT, h0⟩ := positiveCircuitThroughExact_intrinsic Q.points a (hc a ha)
      exact ⟨T, by intro y hy; rw [← Finset.mem_coe, hS]; exact hT hy, haT, h0⟩
  · intro hQ
    refine ⟨⟨?_, ?_⟩, fun a ha => positiveCircuitThroughExact_of_ip Q hQ a ha⟩
    · intro he
      simp [IsIP, PolyCode.hull, pointSet, he] at hQ
    · let K := Submodule.span ℝ (pointSet Q.points)
      have hsub : Q.hull ⊆ (K : Set (V d)) :=
        convexHull_min Submodule.subset_span K.convex
      exact K.eq_top_of_nonempty_interior' ⟨0, interior_mono hsub hQ⟩

def ofRat {d : ℕ} (x : Fin d → ℚ) : V d := fun i => (x i : ℝ)

def ratPointSet {d : ℕ} (xs : List (Fin d → ℚ)) : Set (V d) :=
  ofRat '' {x | x ∈ xs}

theorem ratPointSet_finite {d : ℕ} (xs : List (Fin d → ℚ)) :
    (ratPointSet xs).Finite :=
  xs.finite_toSet.image ofRat

def ratIntPairing {d : ℕ} (u : Fin d → ℚ) (v : IntPoint d) : ℚ :=
  ∑ i, u i * (v i : ℚ)

theorem ratIntPairing_cast {d : ℕ} (u : Fin d → ℚ) (v : IntPoint d) :
    (ratIntPairing u v : ℝ) = ofRat u ⬝ᵥ ofInt v := by
  simp [ratIntPairing, ofRat, ofInt, dotProduct]

theorem mem_polar_hull_iff {d : ℕ} (Q : PolyCode d) (u : V d) :
    u ∈ polarDual Q.hull ↔ ∀ x ∈ Q.points, -1 ≤ u ⬝ᵥ ofInt x := by
  constructor
  · intro hu x hx
    exact hu _ (subset_convexHull ℝ _ ⟨x, hx, rfl⟩)
  · intro hu x hx
    exact le_dotProduct_of_mem_convexHull
      (by rintro y ⟨z, hz, rfl⟩; exact hu z hz) hx

def polarCandidateNumerators {d : ℕ} (v : Fin d → IntPoint d) : IntPoint d :=
  (pointMatrix v).adjugate *ᵥ (fun _ => -1)

def polarCandidate {d : ℕ} (v : Fin d → IntPoint d) : Fin d → ℚ :=
  fun i => (polarCandidateNumerators v i : ℚ) / (pointMatrix v).det

def polarCandidateCheck {d : ℕ} (Q : PolyCode d) (v : Fin d → IntPoint d) : Bool :=
  decide ((pointMatrix v).det ≠ 0 ∧
    (∀ i, ratIntPairing (polarCandidate v) (v i) = -1) ∧
    ∀ x ∈ Q.points, -1 ≤ ratIntPairing (polarCandidate v) x)

theorem polarCandidate_unique {d : ℕ} (v : Fin d → IntPoint d)
    (hd : (pointMatrix v).det ≠ 0) (u : V d)
    (hu : ∀ i, u ⬝ᵥ ofInt (v i) = -1) :
    ofRat (polarCandidate v) = u := by
  let A := (pointMatrix v).map (Int.castRingHom ℝ)
  have hmul : A *ᵥ u = fun _ => -1 := by
    ext i
    simpa [A, pointMatrix, Matrix.mulVec, dotProduct, ofInt, mul_comm] using hu i
  have hdet : ((pointMatrix v).det : ℝ) ≠ 0 := by exact_mod_cast hd
  have hn : ∀ i, (polarCandidateNumerators v i : ℝ) = (pointMatrix v).det * u i := by
    have h := congrArg (fun x => A.adjugate *ᵥ x) hmul
    change A.adjugate *ᵥ (A *ᵥ u) = A.adjugate *ᵥ (fun _ => -1) at h
    rw [Matrix.mulVec_mulVec, Matrix.adjugate_mul] at h
    have hmap : A.adjugate = (pointMatrix v).adjugate.map (Int.castRingHom ℝ) :=
      ((Int.castRingHom ℝ).map_adjugate (pointMatrix v)).symm
    have hcast : A.det = ((pointMatrix v).det : ℝ) :=
      ((Int.castRingHom ℝ).map_det (pointMatrix v)).symm
    rw [hmap, hcast] at h
    intro i
    have hi := congrFun h i
    simp only [Matrix.smul_mulVec, Matrix.one_mulVec, Pi.smul_apply, smul_eq_mul] at hi
    simpa [polarCandidateNumerators, Matrix.mulVec, dotProduct] using hi.symm
  ext i
  simp only [ofRat, polarCandidate, Rat.cast_div, Rat.cast_intCast, hn]
  exact mul_div_cancel_left₀ (u i) hdet

theorem polarCandidateCheck_spec {d : ℕ} (Q : PolyCode d)
    (v : Fin d → IntPoint d) :
    polarCandidateCheck Q v = true ↔
      (pointMatrix v).det ≠ 0 ∧
        (∀ i, ofRat (polarCandidate v) ⬝ᵥ ofInt (v i) = -1) ∧
        ofRat (polarCandidate v) ∈ polarDual Q.hull := by
  simp only [polarCandidateCheck, decide_eq_true_eq, mem_polar_hull_iff]
  constructor
  · rintro ⟨hd, ha, hf⟩
    refine ⟨hd, fun i => ?_, fun x hx => ?_⟩
    · rw [← ratIntPairing_cast]
      exact_mod_cast ha i
    · rw [← ratIntPairing_cast]
      exact_mod_cast hf x hx
  · rintro ⟨hd, ha, hf⟩
    refine ⟨hd, fun i => ?_, fun x hx => ?_⟩
    · have h := ha i
      rw [← ratIntPairing_cast] at h
      exact_mod_cast h
    · have h := hf x hx
      rw [← ratIntPairing_cast] at h
      exact_mod_cast h

theorem polar_extreme_of_active_tuple {d : ℕ} (Q : PolyCode d)
    (v : Fin d → IntPoint d) (hv : ∀ i, v i ∈ Q.points)
    (hd : (pointMatrix v).det ≠ 0) (u : V d)
    (hu : u ∈ polarDual Q.hull) (ha : ∀ i, u ⬝ᵥ ofInt (v i) = -1) :
    u ∈ (polarDual Q.hull).extremePoints ℝ := by
  refine ⟨hu, ?_⟩
  intro y hy z hz hseg
  obtain ⟨a, b, hab, hba, hsum, heq⟩ := hseg
  have hyact : ∀ i, y ⬝ᵥ ofInt (v i) = -1 := by
    intro i
    have hyi := (mem_polar_hull_iff Q y).mp hy (v i) (hv i)
    have hzi := (mem_polar_hull_iff Q z).mp hz (v i) (hv i)
    have he := congrArg (fun x => x ⬝ᵥ ofInt (v i)) heq
    simp only [add_dotProduct, smul_dotProduct, smul_eq_mul, ha] at he
    have hle : a * (y ⬝ᵥ ofInt (v i) + 1) ≤ 0 := by
      nlinarith [mul_nonneg hba.le (show 0 ≤ z ⬝ᵥ ofInt (v i) + 1 by linarith)]
    have hzero : a * (y ⬝ᵥ ofInt (v i) + 1) = 0 :=
      le_antisymm hle (mul_nonneg hab.le (by linarith))
    have hh := (mul_eq_zero.mp hzero).resolve_left (ne_of_gt hab)
    linarith
  let A := (pointMatrix v).map (Int.castRingHom ℝ)
  have hdet : A.det ≠ 0 := by
    rw [show A.det = ((pointMatrix v).det : ℝ) from
      ((Int.castRingHom ℝ).map_det (pointMatrix v)).symm]
    exact_mod_cast hd
  have hmul : A *ᵥ (y - u) = 0 := by
    ext i
    change ofInt (v i) ⬝ᵥ (y - u) = 0
    rw [dotProduct_sub, dotProduct_comm (ofInt (v i)) y,
      dotProduct_comm (ofInt (v i)) u, hyact, ha, sub_self]
  exact sub_eq_zero.mp (Matrix.eq_zero_of_mulVec_eq_zero hdet hmul)

theorem polar_extreme_active_span {d : ℕ} (Q : PolyCode d) (u : V d)
    (hu : u ∈ (polarDual Q.hull).extremePoints ℝ) :
    Submodule.span ℝ {x | x ∈ pointSet Q.points ∧ u ⬝ᵥ x = -1} = ⊤ := by
  classical
  let s : Set (V d) := {x | x ∈ pointSet Q.points ∧ u ⬝ᵥ x = -1}
  let K := Submodule.span ℝ s
  change K = ⊤
  by_contra hK
  have hdual : K.dualAnnihilator ≠ ⊥ := by
    intro he
    exact hK (Submodule.dualAnnihilator_eq_bot_iff.mp he)
  obtain ⟨f, hf, hfne⟩ := Submodule.exists_mem_ne_zero_of_ne_bot hdual
  let E := dotProductEquiv ℝ (Fin d)
  let w := E.symm f
  have hwf (x : V d) : w ⬝ᵥ x = f x :=
    congrArg (fun g : Module.Dual ℝ (V d) => g x) (E.apply_symm_apply f)
  have hw : w ≠ 0 := by
    intro hw
    apply hfne
    rw [← E.apply_symm_apply f]
    change E w = 0
    rw [hw, map_zero]
  have horth (x : V d) (hx : x ∈ s) : w ⬝ᵥ x = 0 := by
    rw [hwf]
    exact (Submodule.mem_dualAnnihilator f).mp hf x (Submodule.subset_span hx)
  let S := (pointSet_finite Q.points).toFinset
  have hS : (S : Set (V d)) = pointSet Q.points :=
    (pointSet_finite Q.points).coe_toFinset
  have hnear : ∀ᶠ t : ℝ in nhds 0, ∀ x ∈ S, -1 ≤ (u + t • w) ⬝ᵥ x := by
    apply (Filter.eventually_all_finset S).mpr
    intro x hx
    have hxS : x ∈ pointSet Q.points := by rw [← hS]; exact hx
    by_cases he : u ⬝ᵥ x = -1
    · have hzero := horth x ⟨hxS, he⟩
      exact Filter.Eventually.of_forall (fun t => by
        simp [add_dotProduct, smul_dotProduct, hzero, he])
    · have hlt : -1 < u ⬝ᵥ x :=
        lt_of_le_of_ne (hu.1 x (subset_convexHull ℝ _ hxS)) (Ne.symm he)
      have hc : Continuous (fun t : ℝ => (u + t • w) ⬝ᵥ x) := by fun_prop
      have hopen : IsOpen {t : ℝ | -1 < (u + t • w) ⬝ᵥ x} :=
        isOpen_lt continuous_const hc
      have hmem : {t : ℝ | -1 < (u + t • w) ⬝ᵥ x} ∈ nhds (0 : ℝ) :=
        hopen.mem_nhds (by simpa using hlt)
      exact Filter.Eventually.mono hmem (fun _ ht => ht.le)
  obtain ⟨ε, hε, hball⟩ := Metric.mem_nhds_iff.mp hnear
  have hplus : ε / 2 ∈ Metric.ball (0 : ℝ) ε := by
    rw [Metric.mem_ball, dist_zero_right, Real.norm_eq_abs, abs_of_pos (half_pos hε)]
    linarith
  have hminus : -ε / 2 ∈ Metric.ball (0 : ℝ) ε := by
    rw [Metric.mem_ball, dist_zero_right, Real.norm_eq_abs,
      neg_div, abs_neg, abs_of_pos (half_pos hε)]
    linarith
  have hfeas (t : ℝ) (ht : t ∈ Metric.ball (0 : ℝ) ε) :
      u + t • w ∈ polarDual Q.hull := by
    apply (mem_polar_hull_iff Q _).mpr
    intro x hx
    apply hball ht (ofInt x)
    rw [← Finset.mem_coe, hS]
    exact ⟨x, hx, rfl⟩
  have hseg : u ∈ openSegment ℝ (u + (ε / 2) • w) (u + (-ε / 2) • w) := by
    refine ⟨1 / 2, 1 / 2, by norm_num, by norm_num, by norm_num, ?_⟩
    ext i
    simp only [Pi.add_apply, Pi.smul_apply, smul_eq_mul]
    ring
  have heq := hu.2 (hfeas _ hplus) (hfeas _ hminus) hseg
  have hzero : (ε / 2) • w = 0 := add_left_cancel (heq.trans (add_zero u).symm)
  exact hw ((smul_eq_zero.mp hzero).resolve_left (ne_of_gt (half_pos hε)))

/-- Rational polar vertices obtained from all nonsingular active-inequality tuples. -/
def polarVerticesExact {d : ℕ} (Q : PolyCode d) : List (Fin d → ℚ) :=
  ((tuples Q.points d).filter (polarCandidateCheck Q)).map polarCandidate

theorem polarVerticesExact_correct {d : ℕ} (Q : PolyCode d) :
    ratPointSet (polarVerticesExact Q) = (polarDual Q.hull).extremePoints ℝ := by
  ext u
  constructor
  · rintro ⟨q, hq, rfl⟩
    obtain ⟨v, hv, rfl⟩ := List.mem_map.mp hq
    obtain ⟨hv, hcheck⟩ := List.mem_filter.mp hv
    obtain ⟨hd, ha, hf⟩ := (polarCandidateCheck_spec Q v).mp hcheck
    exact polar_extreme_of_active_tuple Q v ((mem_tuples _ _ _).mp hv) hd _ hf ha
  · intro hu
    obtain ⟨v, hv, hd, ha⟩ := integer_basis_tuple Q.points
      {x | x ∈ pointSet Q.points ∧ u ⬝ᵥ x = -1}
      (polar_extreme_active_span Q u hu) (fun _ hx => hx.1)
    have heq : ofRat (polarCandidate v) = u :=
      polarCandidate_unique v hd u (fun i => (ha i).2)
    refine ⟨polarCandidate v, List.mem_map.mpr ⟨v,
      List.mem_filter.mpr ⟨(mem_tuples _ _ _).mpr hv, ?_⟩, rfl⟩, heq⟩
    apply (polarCandidateCheck_spec Q v).mpr
    exact ⟨hd, by simpa only [heq] using fun i => (ha i).2, heq ▸ hu.1⟩

theorem polarVerticesExact_hull {d : ℕ} (Q : PolyCode d) (hQ : IsIP Q.hull) :
    polarDual Q.hull = convexHull ℝ (ratPointSet (polarVerticesExact Q)) := by
  have hcompact : IsCompact (polarDual Q.hull) :=
    Metric.isCompact_of_isClosed_isBounded (polarDual_isClosed Q.hull)
      (polarDual_bounded_of_ip hQ)
  have hfinite : ((polarDual Q.hull).extremePoints ℝ).Finite := by
    rw [← polarVerticesExact_correct]
    exact ratPointSet_finite _
  rw [polarVerticesExact_correct]
  calc
    polarDual Q.hull = closure (convexHull ℝ ((polarDual Q.hull).extremePoints ℝ)) :=
      (closure_convexHull_extremePoints hcompact (polarDual_convex Q.hull)).symm
    _ = _ := (hfinite.isClosed_convexHull ℝ).closure_eq

theorem rat_den_one_iff_real_integral (r : ℚ) :
    r.den = 1 ↔ ∃ z : ℤ, (r : ℝ) = z := by
  constructor
  · intro hr
    refine ⟨r.num, ?_⟩
    have h := (Rat.den_eq_one_iff r).mp hr
    exact_mod_cast h.symm
  · rintro ⟨z, hz⟩
    have hr : r = (z : ℚ) := by exact_mod_cast hz
    rw [hr, Rat.den_intCast]

theorem ofRat_lattice_iff {d : ℕ} (u : Fin d → ℚ) :
    IsLatticePoint (ofRat u) ↔ ∀ i, (u i).den = 1 := by
  simp only [IsLatticePoint, ofRat, rat_den_one_iff_real_integral]

def integralPairingsCheckExact {d : ℕ} (Q : PolyCode d) : Bool :=
  (actualVerticesExact Q).all fun v =>
    (polarVerticesExact Q).all fun u => decide ((ratIntPairing u v).den = 1)

theorem integralPairingsCheckExact_correct {d : ℕ} (Q : PolyCode d) :
    integralPairingsCheckExact Q = true ↔ IntegralVPM Q.hull := by
  simp only [integralPairingsCheckExact, List.all_eq_true, decide_eq_true_eq,
    rat_den_one_iff_real_integral]
  constructor
  · intro hp v hv u hu
    rw [← actualVerticesExact_correct] at hv
    obtain ⟨v, hv, rfl⟩ := hv
    rw [← polarVerticesExact_correct] at hu
    obtain ⟨u, hu, rfl⟩ := hu
    obtain ⟨z, hz⟩ := hp v hv u hu
    exact ⟨z, (ratIntPairing_cast u v).symm.trans hz⟩
  · intro hp v hv u hu
    have hv' : ofInt v ∈ Q.hull.extremePoints ℝ := by
      rw [← actualVerticesExact_correct]
      exact ⟨v, hv, rfl⟩
    have hu' : ofRat u ∈ (polarDual Q.hull).extremePoints ℝ := by
      rw [← polarVerticesExact_correct]
      exact ⟨u, hu, rfl⟩
    obtain ⟨z, hz⟩ := hp _ hv' _ hu'
    exact ⟨z, (ratIntPairing_cast u v).trans hz⟩

/-- Integral pairing is checked on actual vertices, never on redundant generators. -/
def eligibleCheckExact {d : ℕ} (Q : PolyCode d) : Bool :=
  if originIPCheckExact Q then integralPairingsCheckExact Q else false

theorem eligibleCheckExact_correct {d : ℕ} (Q : PolyCode d) :
    eligibleCheckExact Q = true ↔ Eligible Q := by
  have heq : eligibleCheckExact Q =
      (originIPCheckExact Q && integralPairingsCheckExact Q) := by
    cases h : originIPCheckExact Q <;> simp [eligibleCheckExact, h]
  rw [heq, Bool.and_eq_true, originIPCheckExact_correct,
    integralPairingsCheckExact_correct, Eligible]

def polarLatticeCheckExact {d : ℕ} (Q : PolyCode d) : Bool :=
  (polarVerticesExact Q).all fun u => decide (∀ i, (u i).den = 1)

theorem polarLatticeCheckExact_correct {d : ℕ} (Q : PolyCode d) (hQ : IsIP Q.hull) :
    polarLatticeCheckExact Q = true ↔ IsLatticePolytope (polarDual Q.hull) := by
  classical
  simp only [polarLatticeCheckExact, List.all_eq_true, decide_eq_true_eq,
    ← ofRat_lattice_iff]
  constructor
  · intro hlat
    let S := (ratPointSet_finite (polarVerticesExact Q)).toFinset
    have hS : (S : Set (V d)) = ratPointSet (polarVerticesExact Q) :=
      (ratPointSet_finite (polarVerticesExact Q)).coe_toFinset
    refine ⟨S, ?_, ?_⟩
    · intro x hx
      have hx' : x ∈ ratPointSet (polarVerticesExact Q) := by rw [← hS]; exact hx
      obtain ⟨u, hu, rfl⟩ := hx'
      exact hlat u hu
    · rw [hS]
      exact polarVerticesExact_hull Q hQ
  · rintro ⟨S, hS, hpoly⟩ u hu
    have hext : ofRat u ∈ (polarDual Q.hull).extremePoints ℝ := by
      rw [← polarVerticesExact_correct]
      exact ⟨u, hu, rfl⟩
    rw [hpoly] at hext
    exact hS _ (extremePoints_convexHull_subset hext)

/-- Reference-lattice reflexivity, including an independent ambient IP check. -/
def reflexiveCheckExact {d : ℕ} (Q : PolyCode d) : Bool :=
  if originIPCheckExact Q then polarLatticeCheckExact Q else false

theorem reflexiveCheckExact_correct {d : ℕ} (Q : PolyCode d) :
    reflexiveCheckExact Q = true ↔ IsReflexive Q.hull := by
  have heq : reflexiveCheckExact Q =
      (originIPCheckExact Q && polarLatticeCheckExact Q) := by
    cases h : originIPCheckExact Q <;> simp [reflexiveCheckExact, h]
  rw [heq, Bool.and_eq_true, originIPCheckExact_correct]
  constructor
  · rintro ⟨hip, hdual⟩
    exact ⟨Q.isLatticePolytope, hip, (polarLatticeCheckExact_correct Q hip).mp hdual⟩
  · intro h
    exact ⟨h.ip, (polarLatticeCheckExact_correct Q h.ip).mpr h.dual_latticePolytope⟩

end Refpoly.KS
