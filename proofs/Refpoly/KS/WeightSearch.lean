import Refpoly.KS.RationalSelection
import Refpoly.KS.IntegerPoints
import Refpoly.KS.IntegerKernel
import Refpoly.KS.PolytopeChecks

open Set Matrix Module

namespace Refpoly.KS.WeightSearch

def columns {m : ℕ} (S : List (IntPoint m)) (i : Fin m) : IntPoint S.length :=
  fun j => S.get j i - 1

def selection {m : ℕ} (S : List (IntPoint m)) : Fin m → ℚ :=
  RationalSelection.select (columns S)

theorem selection_spec {m : ℕ} (S : List (IntPoint m)) (q : V m)
    (hq : ∀ i, 0 < q i) (hsum : ∑ i, q i = 1)
    (hS : ∀ x ∈ S, ofInt x ⬝ᵥ q = 1) :
    (∀ i, 0 < selection S i) ∧ (∑ i, selection S i) = 1 ∧
      ∀ x ∈ S, ofInt x ⬝ᵥ (fun i => (selection S i : ℝ)) = 1 := by
  have hrel : ∑ i, q i • ofInt (columns S i) = 0 := by
    ext j
    have hx := hS (S.get j) (List.get_mem S j)
    change ∑ i, (S.get j i : ℝ) * q i = 1 at hx
    simpa [ofInt, columns, Finset.sum_apply, Pi.smul_apply, smul_eq_mul,
      mul_sub, Finset.sum_sub_distrib, hsum, mul_comm] using sub_eq_zero.mpr hx
  obtain ⟨hp, hpsum, hprel⟩ := RationalSelection.select_spec (columns S) q
    ⟨fun i => (hq i).le, hsum, hrel⟩ hq
  refine ⟨hp, hpsum, ?_⟩
  intro x hx
  obtain ⟨j, rfl⟩ := List.mem_iff_get.mp hx
  have h := congrFun hprel j
  have hpsumR : ∑ i, (selection S i : ℝ) = 1 := by exact_mod_cast hpsum
  change ∑ i, (S.get j i : ℝ) * (selection S i : ℝ) = 1
  have hzero : (∑ i, (selection S i : ℝ) * (S.get j i : ℝ)) - 1 = 0 := by
    simp only [columns, ofInt, Finset.sum_apply, Pi.smul_apply, smul_eq_mul,
      Int.cast_sub, Int.cast_one, mul_sub, mul_one, Finset.sum_sub_distrib,
      Pi.zero_apply] at h
    change (∑ i, (selection S i : ℝ) * (S.get j i : ℝ)) -
      (∑ i, (selection S i : ℝ)) = 0 at h
    rwa [hpsumR] at h
  simpa [mul_comm] using sub_eq_zero.mp hzero

def boundedTuples : {m : ℕ} → (Fin m → ℕ) → List (WeightCode m)
  | 0, _ => [Fin.elim0]
  | _ + 1, B =>
      (List.range (B 0 + 1)).flatMap fun x =>
        (boundedTuples (fun i => B i.succ)).map (Fin.cons x)

theorem mem_boundedTuples {m : ℕ} (B : Fin m → ℕ) (x : WeightCode m) :
    x ∈ boundedTuples B ↔ ∀ i, x i ≤ B i := by
  induction m with
  | zero => simp [boundedTuples, Subsingleton.elim x Fin.elim0]
  | succ m ih =>
      simp only [boundedTuples, List.mem_flatMap, List.mem_range, List.mem_map]
      constructor
      · rintro ⟨a, ha, u, hu, rfl⟩ i
        exact Fin.cases (by simpa using ha) (fun j => (ih _ u).mp hu j) i
      · intro h
        refine ⟨x 0, by have := h 0; omega, fun i => x i.succ,
          (ih _ _).mpr (fun i => h i.succ), ?_⟩
        ext i
        exact Fin.cases rfl (fun _ => rfl) i

def branches {m : ℕ} (p : Fin m → ℚ) : List (IntPoint m) :=
  ((boundedTuples (fun i => ⌈1 / p i⌉₊)).map (fun x i => (x i : ℤ))).filter fun x =>
    decide ((∑ i, (x i : ℚ) * p i) < 1)

theorem mem_branches {m : ℕ} (p : Fin m → ℚ) (hp : ∀ i, 0 < p i)
    (x : IntPoint m) (hx : ∀ i, 0 ≤ x i) (hxp : ∑ i, (x i : ℚ) * p i < 1) :
    x ∈ branches p := by
  have hcast (i) : ((x i).toNat : ℤ) = x i := Int.toNat_of_nonneg (hx i)
  refine List.mem_filter.mpr ⟨List.mem_map.mpr
    ⟨fun i => (x i).toNat, (mem_boundedTuples _ _).mpr ?_, funext hcast⟩,
    decide_eq_true hxp⟩
  intro i
  have hterm : (x i : ℚ) * p i ≤ ∑ j, (x j : ℚ) * p j :=
    Finset.single_le_sum
      (fun j _ => mul_nonneg (by exact_mod_cast hx j) (hp j).le) (Finset.mem_univ i)
  have hcoord : (x i : ℚ) < 1 / p i := (lt_div_iff₀ (hp i)).mpr (hterm.trans_lt hxp)
  have hceil : (x i : ℚ) ≤ (⌈1 / p i⌉₊ : ℕ) :=
    hcoord.le.trans (Nat.le_ceil _)
  have hi : x i ≤ (⌈1 / p i⌉₊ : ℕ) := by exact_mod_cast hceil
  omega

def selectionCheck {m : ℕ} (p : Fin m → ℚ) : Bool :=
  decide ((∀ i, 0 < p i) ∧ ∑ i, p i = 1)

def readVector {R : Type} [Zero R] {m : ℕ} (a : Array R) : Fin m → R :=
  fun i => a.getD i.val 0

@[simp] theorem readVector_ofFn {R : Type} [Zero R] {m : ℕ} (q : Fin m → R) :
    readVector (Array.ofFn q) = q := by
  ext i
  simp [readVector, Array.getD]

/-- The depth is structural; every branch coordinate has an arithmetic bound. -/
def enumerate {m : ℕ} : ℕ → List (IntPoint m) → List (WeightCode m)
  | 0, S =>
      let data := Array.ofFn (selection S)
      let p := readVector data
      if selectionCheck p then [rationalWeightCode p] else []
  | k + 1, S =>
      let data := Array.ofFn (selection S)
      let p := readVector data
      if selectionCheck p then
        rationalWeightCode p :: (branches p).flatMap (fun x => enumerate k (x :: S))
      else []

noncomputable def realNode {m : ℕ} (S : List (IntPoint m)) : Finset (V m) := by
  classical
  exact (S.map ofInt).toFinset

@[simp] theorem mem_realNode {m : ℕ} (S : List (IntPoint m)) (y : V m) :
    y ∈ realNode S ↔ ∃ x ∈ S, ofInt x = y := by
  classical
  simp [realNode]

@[simp] theorem realNode_cons {m : ℕ} (x : IntPoint m) (S : List (IntPoint m)) :
    realNode (x :: S) = insert (ofInt x) (realNode S) := by
  classical
  simp [realNode]

theorem latticePoint_intPoint {m : ℕ} {x : V m} (hx : IsLatticePoint x) :
    ∃ y : IntPoint m, ofInt y = x := by
  choose y hy using hx
  exact ⟨y, funext (fun i => (hy i).symm)⟩

theorem affineRank_le {m : ℕ} (q : V m) (hq : ∑ i, q i = 1)
    (S : Finset (V m)) (hS : ∀ x ∈ S, x ⬝ᵥ q = 1) :
    finrank ℝ (vectorSpan ℝ (S : Set (V m))) ≤ m - 1 := by
  have hqne : dotLin q ≠ 0 := by
    intro hz
    have h := congrArg (fun f : V m →ₗ[ℝ] ℝ => f (ones m)) hz
    simp [hq] at h
  have hdim := Module.Dual.finrank_ker_add_one_of_ne_zero hqne
  simp only [finrank_fin_fun] at hdim
  have hle : vectorSpan ℝ (S : Set (V m)) ≤ wsHyperplane q := by
    rw [vectorSpan_def]
    apply Submodule.span_le.mpr
    rintro _ ⟨x, hx, y, hy, rfl⟩
    change x - y ∈ wsHyperplane q
    rw [mem_wsHyperplane, dotProduct_sub,
      dotProduct_comm q x, dotProduct_comm q y, hS x hx, hS y hy, sub_self]
  have h := Submodule.finrank_mono hle
  change _ ≤ finrank ℝ (LinearMap.ker (dotLin q)) at h
  omega

theorem enumerate_complete {m : ℕ} (q : Fin m → ℚ) (hq : ∀ i, 0 < q i)
    (hsum : ∑ i, q i = 1) (hIP : IsIPWeightSystem (fun i => (q i : ℝ))) :
    ∀ (k : ℕ) (S : List (IntPoint m)), (realNode S).Nonempty →
      ((realNode S : Set (V m)) ⊆ wsPoints (fun i => (q i : ℝ))) →
      m - 1 - finrank ℝ (vectorSpan ℝ (realNode S : Set (V m))) ≤ k →
      rationalWeightCode q ∈ enumerate k S := by
  classical
  have hqR (i) : (0 : ℝ) < q i := by exact_mod_cast hq i
  have hsumR : ∑ i, (q i : ℝ) = 1 := by exact_mod_cast hsum
  intro k
  induction k with
  | zero =>
    intro S hne hsub hrank
    have hS : ∀ x ∈ S, ofInt x ⬝ᵥ (fun i => (q i : ℝ)) = 1 := by
      intro x hx
      exact (hsub ((mem_realNode _ _).mpr ⟨x, hx, rfl⟩)).2.2.trans hsumR
    obtain ⟨hp, hpsum, hprel⟩ := selection_spec S (fun i => (q i : ℝ)) hqR hsumR hS
    have hcheck : selectionCheck (selection S) = true := decide_eq_true ⟨hp, hpsum⟩
    by_cases heq : selection S = q
    · simp only [enumerate, readVector_ofFn, hcheck, ↓reduceIte, List.mem_singleton]
      exact congrArg rationalWeightCode heq.symm
    · have hneR : (fun i => (selection S i : ℝ)) ≠ (fun i => (q i : ℝ)) := by
        intro he
        apply heq
        ext i
        exact_mod_cast congrFun he i
      have hpsumR : ∑ i, (selection S i : ℝ) = 1 := by exact_mod_cast hpsum
      obtain ⟨x, hx, hxlt⟩ := descent_lemma hsumR hIP hpsumR hneR
      have hnode : ∀ z ∈ realNode S, z ⬝ᵥ (fun i => (selection S i : ℝ)) = 1 := by
        intro z hz
        obtain ⟨y, hy, rfl⟩ := (mem_realNode _ _).mp hz
        exact hprel y hy
      have hlt := finrank_vectorSpan_lt_insert hne (notMem_affineSpan_of_dot_lt hnode hxlt)
      have hle := affineRank_le (fun i => (q i : ℝ)) hsumR (insert x (realNode S))
        (by
          intro z hz
          rcases Finset.mem_insert.mp hz with rfl | hz
          · exact hx.2.2.trans hsumR
          · obtain ⟨y, hy, rfl⟩ := (mem_realNode _ _).mp hz
            exact hS y hy)
      rw [Finset.coe_insert] at hle
      omega
  | succ k ih =>
    intro S hne hsub hrank
    have hS : ∀ x ∈ S, ofInt x ⬝ᵥ (fun i => (q i : ℝ)) = 1 := by
      intro x hx
      exact (hsub ((mem_realNode _ _).mpr ⟨x, hx, rfl⟩)).2.2.trans hsumR
    obtain ⟨hp, hpsum, hprel⟩ := selection_spec S (fun i => (q i : ℝ)) hqR hsumR hS
    have hcheck : selectionCheck (selection S) = true := decide_eq_true ⟨hp, hpsum⟩
    by_cases heq : selection S = q
    · simp only [enumerate, readVector_ofFn, hcheck, ↓reduceIte, List.mem_cons]
      exact Or.inl (congrArg rationalWeightCode heq.symm)
    · have hneR : (fun i => (selection S i : ℝ)) ≠ (fun i => (q i : ℝ)) := by
        intro he
        apply heq
        ext i
        exact_mod_cast congrFun he i
      have hpsumR : ∑ i, (selection S i : ℝ) = 1 := by exact_mod_cast hpsum
      obtain ⟨x, hx, hxlt⟩ := descent_lemma hsumR hIP hpsumR hneR
      obtain ⟨y, rfl⟩ := latticePoint_intPoint hx.1
      have hy : ∀ i, 0 ≤ y i := fun i => by
        have h := hx.2.1 i
        change (0 : ℝ) ≤ (y i : ℝ) at h
        exact_mod_cast h
      have hylt : ∑ i, (y i : ℚ) * selection S i < 1 := by
        change ∑ i, (y i : ℝ) * (selection S i : ℝ) < 1 at hxlt
        exact_mod_cast hxlt
      have hybranch := mem_branches (selection S) hp y hy hylt
      have hnode : ∀ z ∈ realNode S, z ⬝ᵥ (fun i => (selection S i : ℝ)) = 1 := by
        intro z hz
        obtain ⟨t, ht, rfl⟩ := (mem_realNode _ _).mp hz
        exact hprel t ht
      have hlt := finrank_vectorSpan_lt_insert hne
        (notMem_affineSpan_of_dot_lt hnode hxlt)
      have hchildsub : (realNode (y :: S) : Set (V m)) ⊆
          wsPoints (fun i => (q i : ℝ)) := by
        rw [realNode_cons, Finset.coe_insert]
        exact insert_subset_iff.mpr ⟨hx, hsub⟩
      have hchild := ih (y :: S) (by simp) hchildsub (by
        rw [realNode_cons, Finset.coe_insert]
        omega)
      simp only [enumerate, readVector_ofFn, hcheck, ↓reduceIte, List.mem_cons, List.mem_flatMap]
      exact Or.inr ⟨y, hybranch, hchild⟩

theorem normalized_sum {m : ℕ} (w : WeightCode m) (hg : Finset.univ.gcd w = 1) :
    ∑ i, normalized w i = 1 := by
  have hD : (∑ i, w i : ℕ) ≠ 0 := (weight_degree_pos hg).ne'
  simp only [normalized, ← Finset.sum_div, ← Nat.cast_sum]
  exact div_self (by exact_mod_cast hD)

theorem rationalWeightCode_normalized {m : ℕ} (w : WeightCode m) (hw : PrimitiveIP w) :
    rationalWeightCode (normalized w) = w := by
  have hpos (i) : 0 < normalized w i := by
    have h := hw.2.1 i
    change (0 : ℝ) < (normalized w i : ℝ) at h
    exact_mod_cast h
  obtain ⟨hg, hn⟩ := rationalWeightCode_correct (normalized w) hpos (normalized_sum w hw.1)
  apply normalizedWeights_injective_on_primitive hg hw.1
    (weight_degree_pos hg) (weight_degree_pos hw.1)
  rw [← normalized_cast_eq, ← normalized_cast_eq, hn]

def candidateWeights (m : ℕ) : List (WeightCode m) :=
  let data : List (Array ℕ) := (enumerate (m := m) (m - 1) [fun _ => 1]).map Array.ofFn
  (data.map (readVector (m := m))).eraseDups

theorem candidateWeights_complete (m : ℕ) (w : WeightCode m) (hw : PrimitiveIP w) :
    w ∈ candidateWeights m := by
  classical
  have hpos (i) : 0 < normalized w i := by
    have h := hw.2.1 i
    change (0 : ℝ) < (normalized w i : ℝ) at h
    exact_mod_cast h
  have hroot : realNode [fun _ : Fin m => (1 : ℤ)] = {ones m} := by
    have ho : ofInt (fun _ : Fin m => (1 : ℤ)) = ones m := by
      ext i
      simp [ofInt, ones]
    simp [realNode, ho]
  have h := enumerate_complete (normalized w) hpos (normalized_sum w hw.1) hw.2 (m - 1)
    [fun _ => 1] (by rw [hroot]; exact Finset.singleton_nonempty _)
    (by rw [hroot]; simpa using ones_mem_wsPoints (fun i => (normalized w i : ℝ)))
    (by omega)
  have hwEnum : w ∈ enumerate (m - 1) [fun _ => 1] := by
    simpa only [rationalWeightCode_normalized w hw] using h
  apply List.mem_eraseDups.mpr
  exact List.mem_map.mpr ⟨Array.ofFn w,
    List.mem_map.mpr ⟨w, hwEnum, rfl⟩, readVector_ofFn w⟩

def oneRow {m : ℕ} (w : WeightCode m) : CWSCode (m - 1) :=
  ⟨m, 1, fun _ => w⟩

theorem oneRow_valid {m : ℕ} (hm : 2 ≤ m) (w : WeightCode m) (hw : ∀ i, 0 < w i) :
    ValidCWS (oneRow w) := by
  unfold ValidCWS
  dsimp only [oneRow, realRows]
  refine ⟨by omega, by omega, by omega, ?_, fun j => ⟨0, hw j⟩⟩
  change LinearIndependent ℝ (fun _ : Fin 1 => fun i : Fin m => (w i : ℝ))
  rw [linearIndependent_unique_iff]
  intro hz
  let i : Fin m := ⟨0, by omega⟩
  have h := congrFun hz i
  have hp := hw i
  change (w i : ℝ) = 0 at h
  have : w i = 0 := by exact_mod_cast h
  omega

theorem oneRow_kernel {m : ℕ} (w : WeightCode m) :
    kernelSpace (oneRow w) = wsHyperplane (fun i => (w i : ℝ)) := by
  ext y
  simp only [kernelSpace, LinearMap.mem_ker]
  change ((fun _ : Fin 1 => ∑ j, (w j : ℝ) * y j) = 0) ↔ _
  constructor
  · intro h
    exact congrFun h 0
  · intro h
    ext a
    exact h

theorem basisMap_lattice {d : ℕ} {C : CWSCode d}
    (b : Fin C.N → Fin d → ℤ) (x : V d) (hx : IsLatticePoint x) :
    IsLatticePoint (basisMap b x) := by
  choose z hz using hx
  intro j
  refine ⟨∑ i, b j i * z i, ?_⟩
  simp [basisMap, Matrix.mulVec, dotProduct, hz]

theorem pointsForBasis_hull {d : ℕ} (C : CWSCode d) (hC : ValidCWS C)
    (b : Fin C.N → Fin d → ℤ) (hb : KernelBasisCorrect C b) :
    (PolyCode.mk (pointsForBasis C b)).hull = container b := by
  rw [PolyCode.hull, pointsForBasis_exact C hC b hb]
  apply Subset.antisymm
  · exact convexHull_min inter_subset_left (convex_convexHull ℝ _)
  · apply convexHull_min
    · intro x hx
      exact subset_convexHull ℝ _ ⟨subset_convexHull ℝ _ hx, hx.1⟩
    · exact convex_convexHull ℝ _

def matrixData {N d : ℕ} (b : Fin N → Fin d → ℤ) : Array (Array ℤ) :=
  Array.ofFn fun j => Array.ofFn (b j)

def readMatrix {N d : ℕ} (a : Array (Array ℤ)) : Fin N → Fin d → ℤ :=
  fun j i => (a.getD j.val #[]).getD i.val 0

@[simp] theorem readMatrix_data {N d : ℕ} (b : Fin N → Fin d → ℤ) :
    readMatrix (matrixData b) = b := by
  ext j i
  simp [readMatrix, matrixData, Array.getD]

/-- The certificate is extracted before tabulating entries, so a curried
basis alias cannot repeat its unbounded certificate search per entry. -/
def integerKernelBasisDataExact {d : ℕ} (C : CWSCode d) : Array (Array ℤ) :=
  if h : validCWSCheck C = true then
    let hv := (validCWSCheck_iff C).mp h
    let hd : d ≤ C.N := by have heq := hv.1; omega
    let hex := IntegerKernel.boundedWitness_exists C hv hd
    let B := Nat.find hex
    let o := IntegerKernel.boundedWitness C hd hv.1 B
    have ho : o.isSome = true := Nat.find_spec hex
    let U := o.get ho
    matrixData (IntegerKernel.columns C hd U)
  else matrixData (0 : Fin C.N → Fin d → ℤ)

@[simp] theorem read_integerKernelBasisDataExact {d : ℕ} (C : CWSCode d) :
    readMatrix (integerKernelBasisDataExact C) = integerKernelBasisExact C := by
  unfold integerKernelBasisDataExact integerKernelBasisExact
  split_ifs
  · rw [readMatrix_data]
    rfl
  · exact readMatrix_data _

def newtonPolytope {m : ℕ} (w : WeightCode m) : PolyCode (m - 1) :=
  let C := oneRow w
  let data := integerKernelBasisDataExact C
  ⟨pointsForBasis C (readMatrix data)⟩

theorem newtonPolytope_eq {m : ℕ} (w : WeightCode m) :
    newtonPolytope w =
      ⟨pointsForBasis (oneRow w) (integerKernelBasisExact (oneRow w))⟩ := by
  simp only [newtonPolytope, read_integerKernelBasisDataExact]

/-- The saturated basis identifies the full integer Newton hull and its
subspace topology, without imposing reflexivity on that hull. -/
theorem newtonPolytope_ip_iff {m : ℕ} (hm : 2 ≤ m) (w : WeightCode m)
    (hw : ∀ i, 0 < w i) :
    IsIP (newtonPolytope w).hull ↔ IsIPWeightSystem (fun i => (w i : ℝ)) := by
  classical
  let C := oneRow w
  let b := integerKernelBasisExact C
  have hC : ValidCWS C := oneRow_valid hm w hw
  have hb : KernelBasisCorrect C b := integerKernelBasisExact_correct C hC
  have hkernel : kernelSpace C = wsHyperplane (fun i => (w i : ℝ)) := oneRow_kernel w
  let W := wsHyperplane (fun i => (w i : ℝ))
  have hmem : ∀ x, basisMap b x ∈ W := by
    intro x
    dsimp only [W]
    rw [← hkernel, ← hb.2.1]
    exact LinearMap.mem_range_self _ _
  let F := (basisMap b).codRestrict W hmem
  have hbij : Function.Bijective F := by
    constructor
    · intro x y h
      exact hb.1 (congrArg Subtype.val h)
    · rintro ⟨y, hy⟩
      dsimp only [W] at hy
      rw [← hkernel, ← hb.2.1] at hy
      obtain ⟨x, hx⟩ := hy
      exact ⟨x, Subtype.ext hx⟩
  have hgens : F '' {x : V (m - 1) | IsLatticePoint x ∧ ∀ j, -1 ≤ basisMap b x j} =
      wsLatticeGensAt (fun i => (w i : ℝ)) 1 := by
    apply Subset.antisymm
    · rintro _ ⟨x, hx, rfl⟩
      exact ⟨basisMap_lattice b x hx.1, hx.2⟩
    · intro y hy
      obtain ⟨x, hx, hxy⟩ := hb.2.2 (y : V m) (hkernel ▸ y.property) hy.1
      refine ⟨x, ⟨hx, ?_⟩, Subtype.ext hxy⟩
      intro j
      rw [hxy]
      exact hy.2 j
  have hpoly : F '' (newtonPolytope w).hull =
      wsPolytopeAt (fun i => (w i : ℝ)) 1 := by
    rw [newtonPolytope_eq]
    change F '' (PolyCode.mk (pointsForBasis C b)).hull = _
    rw [pointsForBasis_hull C hC b hb]
    change F '' convexHull ℝ _ = convexHull ℝ _
    rw [F.image_convexHull, hgens]
  have hpre : F ⁻¹' wsPolytopeAt (fun i => (w i : ℝ)) 1 = (newtonPolytope w).hull := by
    rw [← hpoly]
    exact Set.preimage_image_eq _ hbij.1
  constructor
  · intro h
    refine ⟨fun i => ?_, ?_⟩
    · change (0 : ℝ) < (w i : ℝ)
      exact_mod_cast hw i
    rw [← hpoly]
    exact (LinearMap.isOpenMap_of_finiteDimensional F hbij.2).image_interior_subset _
      ⟨0, h, map_zero F⟩
  · intro h
    have h0 : (0 : V (m - 1)) ∈ F ⁻¹' interior
        (wsPolytopeAt (fun i => (w i : ℝ)) 1) := by
      simpa only [Set.mem_preimage, map_zero] using h.2
    have ht := preimage_interior_subset_interior_preimage
      (LinearMap.continuous_of_finiteDimensional F) h0
    rwa [hpre] at ht

theorem primitiveIP_zero (w : WeightCode 0) : ¬ PrimitiveIP w := by
  intro h
  have hD := weight_degree_pos h.1
  simp at hD

theorem primitiveIP_one (w : WeightCode 1) :
    PrimitiveIP w ↔ w = fun _ => 1 := by
  constructor
  · intro hw
    have h : w 0 = 1 := by simpa using hw.1
    funext i
    simpa only [Fin.eq_zero i] using h
  · rintro rfl
    apply (primitiveIP_iff _).mpr
    refine ⟨by simp, fun i => by norm_num, ?_⟩
    let q : V 1 := fun _ => ((1 : ℕ) : ℝ)
    have hW : wsHyperplane q = ⊥ := by
      ext y
      simp only [mem_wsHyperplane, Submodule.mem_bot]
      change (∑ i : Fin 1, ((1 : ℕ) : ℝ) * y i) = 0 ↔ y = 0
      simp only [Nat.cast_one, Fin.sum_univ_one, one_mul]
      exact ⟨fun h => funext (fun i => by simpa only [Fin.eq_zero i] using h),
        fun h => by simp [h]⟩
    haveI : Subsingleton (wsHyperplane q) := by
      rw [hW]
      infer_instance
    have h0 : (0 : wsHyperplane q) ∈ wsPolytopeAt q 1 := by
      exact subset_convexHull ℝ _ ⟨isLatticePoint_zero, fun i => by norm_num⟩
    have hall : wsPolytopeAt q 1 = Set.univ := by
      apply Set.eq_univ_of_forall
      intro y
      simpa only [Subsingleton.elim y 0] using h0
    have hint : (0 : wsHyperplane q) ∈ interior (wsPolytopeAt q 1) := by
      rw [hall, interior_univ]
      trivial
    exact hint

end Refpoly.KS.WeightSearch

namespace Refpoly.KS

/-- Parameterization isolates the independently verified origin-interior
decision procedure; the search itself has no oracle or noncomputable data. -/
def searchWeightsWith
    (originIP : {d : ℕ} → PolyCode d → Bool) : (m : ℕ) → List (WeightCode m)
  | 0 => []
  | 1 => [fun _ => 1]
  | m + 2 =>
      (WeightSearch.candidateWeights (m + 2)).filter fun w =>
        decide (Finset.univ.gcd w = 1 ∧ ∀ i, 0 < w i) &&
          originIP (WeightSearch.newtonPolytope w)

theorem searchWeightsWith_sound
    (originIP : {d : ℕ} → PolyCode d → Bool)
    (hcheck : ∀ d (Q : PolyCode d), originIP Q = true ↔ IsIP Q.hull)
    (m : ℕ) (w : WeightCode m) :
    w ∈ searchWeightsWith originIP m → PrimitiveIP w := by
  match m with
  | 0 => simp [searchWeightsWith]
  | 1 =>
      intro hw
      apply (WeightSearch.primitiveIP_one w).mpr
      simpa [searchWeightsWith] using hw
  | m + 2 =>
      intro hw
      obtain ⟨_, h⟩ := List.mem_filter.mp hw
      obtain ⟨hcode, hpoly⟩ := Bool.and_eq_true_iff.mp h
      obtain ⟨hg, hp⟩ := of_decide_eq_true hcode
      exact (primitiveIP_iff w).mpr ⟨hg,
        (WeightSearch.newtonPolytope_ip_iff (by omega) w hp).mp
          ((hcheck _ _).mp hpoly)⟩

theorem searchWeightsWith_complete
    (originIP : {d : ℕ} → PolyCode d → Bool)
    (hcheck : ∀ d (Q : PolyCode d), originIP Q = true ↔ IsIP Q.hull)
    (m : ℕ) (w : WeightCode m) (hw : PrimitiveIP w) :
    w ∈ searchWeightsWith originIP m := by
  match m with
  | 0 => exact (WeightSearch.primitiveIP_zero w hw).elim
  | 1 =>
      rw [(WeightSearch.primitiveIP_one w).mp hw]
      simp [searchWeightsWith]
  | m + 2 =>
      have hp := primitiveIP_positive hw
      refine List.mem_filter.mpr ⟨WeightSearch.candidateWeights_complete _ w hw, ?_⟩
      apply Bool.and_eq_true_iff.mpr
      refine ⟨decide_eq_true ⟨hw.1, hp⟩, (hcheck _ _).mpr ?_⟩
      exact (WeightSearch.newtonPolytope_ip_iff (by omega) w hp).mpr
        ((primitiveIP_iff w).mp hw).2

/-- Complete terminating enumeration of primitive IP codes, retaining every
coordinate ordering and checking IP rather than reflexivity. -/
def searchWeightsExact (m : ℕ) : List (WeightCode m) :=
  searchWeightsWith originIPCheckExact m

theorem searchWeightsExact_sound (m : ℕ) (w : WeightCode m) :
    w ∈ searchWeightsExact m → PrimitiveIP w :=
  searchWeightsWith_sound originIPCheckExact
    (fun _ Q => originIPCheckExact_correct Q) m w

theorem searchWeightsExact_complete (m : ℕ) (w : WeightCode m) :
    PrimitiveIP w → w ∈ searchWeightsExact m :=
  searchWeightsWith_complete originIPCheckExact
    (fun _ Q => originIPCheckExact_correct Q) m w

end Refpoly.KS
