-- Source-function SHA256: 9d1dd8fc40cacd549cc60a682b7acf92b1b010f857f3f7b954f5c31a8ecdf1e8
import Refpoly.GpuWalk
import Refpoly.JoinCWS

/-! Source-pinned transcription of `device_walk_validate_points` and its
lexicographic helper in `src/gpu/lattice.cuh`.

The input list is the readable point-buffer slice of length `count`; negative
counts and invalid pointers are outside this normal-return model. `ord` is a
function into `Fin 5`, which encodes the helper's five successful range checks;
the source does not check that `ord` is a permutation. The list chain models
the adjacent `p`/`p-1` comparisons. `Finset` masks retain the active coordinate
bits: the `ffs`/clear-lowest-bit loops fold over those bits, `~0u` retains every
ambient coordinate, and `n ≤ 10` is the source array-width condition.

The successful-path theorem establishes the checks performed here. Weight
equations are obtained separately from the already-certified kernel rows and
origin equation (`GpuWalk.kernel_rows_preserve_weights`). Full-rank embedding
is an explicit mod-2 minor certificate, and the root box is the input to the
separate certified box check. Neither is inferred from an accepted buffer.
-/
namespace Refpoly.GpuWalkValidator

open Refpoly.GpuWalk
open Refpoly.JoinRejection

abbrev Reduced := Fin 5 → Int

/-- Exact ambient `X` coordinate recomputed by the validator. -/
def ambientPoint {n : Nat} (basis : Matrix (Fin 5) (Fin n) Int) (x : Reduced)
    (a : Fin n) : Int := 1 + ∑ j, x j * basis j a

/-- The five values inspected by `device_walk_lex_greater`, in source order. -/
def walkKey (ord : Fin 5 → Fin 5) (x : Reduced) : List Int :=
  List.ofFn (fun d : Fin 5 => x (ord d))

/-- `device_walk_lex_greater left right ord` is `key right < key left`. -/
def lexGreater (ord : Fin 5 → Fin 5) (left right : Reduced) : Bool :=
  decide (walkKey ord right < walkKey ord left)

theorem lexGreater_spec (ord : Fin 5 → Fin 5) (left right : Reduced) :
    lexGreater ord left right = true ↔ walkKey ord right < walkKey ord left := by
  simp [lexGreater]

def BoxPoint {n : Nat} (basis : Matrix (Fin 5) (Fin n) Int) (xu : Fin n → Int)
    (lo hi : Fin 5 → Int) (x : Reduced) : Prop :=
  (∀ j, lo j ≤ x j ∧ x j ≤ hi j) ∧
  (∀ a, 0 ≤ ambientPoint basis x a ∧ ambientPoint basis x a ≤ xu a)

/-- Exact mathematical form of the preceding `device_walk_int32_box` guard.
This is a caller certificate: the validator itself does not check it. The
coefficient budget bounds every product/partial sum, and the explicit ranges
record the source's int32 inputs and upper-coordinate limit. -/
def CertifiedInt32RootBox {n : Nat} (basis : Matrix (Fin 5) (Fin n) Int)
    (xu : Fin n → Int) (lo hi : Fin 5 → Int) : Prop :=
  (∀ j, -(2147483646 : Int) ≤ lo j ∧ lo j ≤ hi j ∧ hi j ≤ 2147483646) ∧
  (∀ a, 0 ≤ xu a ∧ xu a ≤ (2147483647 : Int) / 4) ∧
  (∀ j a, -(2147483648 : Int) ≤ basis j a ∧ basis j a ≤ 2147483647) ∧
  (∀ a, ∑ j, (basis j a).natAbs * max (lo j).natAbs (hi j).natAbs ≤
    (2147483647 : Nat) / 8 - 2)

def WalkOrdered (ord : Fin 5 → Fin 5) (points : List Reduced) : Prop :=
  List.IsChain (fun x y => walkKey ord x < walkKey ord y) points

/-! The next definitions are the finite-set abstraction of the source's
unsigned occupancy masks. `unionMap` is the repeated `|=` update; each row
below corresponds directly to the per-bit `ffs` loops in the pinned function.
-/

def unionMap {α β : Type*} [DecidableEq β] (f : α → Finset β) : List α → Finset β
  | [] => ∅
  | x :: xs => f x ∪ unionMap f xs

theorem mem_unionMap {α β : Type*} [DecidableEq β] (f : α → Finset β)
    (xs : List α) (b : β) :
    b ∈ unionMap f xs ↔ ∃ x ∈ xs, b ∈ f x := by
  classical
  induction xs with
  | nil => simp [unionMap]
  | cons x xs ih =>
      simp [unionMap, ih]

def zeroCoords {n : Nat} (basis : Matrix (Fin 5) (Fin n) Int) (x : Reduced) : Finset (Fin n) :=
  Finset.univ.filter (fun a => ambientPoint basis x a = 0)

def twoPlusCoords {n : Nat} (basis : Matrix (Fin 5) (Fin n) Int) (x : Reduced) : Finset (Fin n) :=
  Finset.univ.filter (fun a => 2 ≤ ambientPoint basis x a)

def smallCoords {n : Nat} (basis : Matrix (Fin 5) (Fin n) Int) (x : Reduced) : Finset (Fin n) :=
  Finset.univ.filter (fun a => ambientPoint basis x a = 0 ∨ ambientPoint basis x a = 1)

def highCoords {n : Nat} (basis : Matrix (Fin 5) (Fin n) Int) (x : Reduced) : Finset (Fin n) :=
  Finset.univ.filter (fun a => 1 ≤ ambientPoint basis x a)

def pairLoRow {n : Nat} (basis : Matrix (Fin 5) (Fin n) Int) (x : Reduced) (a : Fin n) :
    Finset (Fin n) :=
  if ambientPoint basis x a = 0 then smallCoords basis x else ∅

def pairHiRow {n : Nat} (basis : Matrix (Fin 5) (Fin n) Int) (x : Reduced) (a : Fin n) :
    Finset (Fin n) :=
  if 3 ≤ ambientPoint basis x a then Finset.univ
  else if ambientPoint basis x a = 2 then highCoords basis x
  else ∅

structure WalkOccupancy (n : Nat) where
  touchZero : Finset (Fin n)
  touchTwo : Finset (Fin n)
  pairLo : Fin n → Finset (Fin n)
  pairHi : Fin n → Finset (Fin n)

/-- The exact finite-set update represented by the source's `m0..m3`, touch
unions, `pair_lo` and `pair_hi` loops. -/
def sourceOccupancy {n : Nat} (basis : Matrix (Fin 5) (Fin n) Int) (points : List Reduced) :
    WalkOccupancy n :=
  { touchZero := unionMap (zeroCoords basis) points
    touchTwo := unionMap (twoPlusCoords basis) points
    pairLo := fun a => unionMap (fun x => pairLoRow basis x a) points
    pairHi := fun a => unionMap (fun x => pairHiRow basis x a) points }

/-- Normal return of the source transcription. `points` represents exactly
the `count` readable rows, and the source's order-index bounds are represented
by `ord : Fin 5 → Fin 5`. -/
noncomputable def validateModel {n : Nat} (basis : Matrix (Fin 5) (Fin n) Int) (xu : Fin n → Int)
    (ord : Fin 5 → Fin 5) (lo hi : Fin 5 → Int) (points : List Reduced) :
    Option (WalkOccupancy n) := by
  classical
  exact if (∀ x ∈ points, BoxPoint basis xu lo hi x) ∧ WalkOrdered ord points then
    some (sourceOccupancy basis points) else none

theorem validateModel_success {n : Nat} (basis : Matrix (Fin 5) (Fin n) Int) (xu : Fin n → Int)
    (ord : Fin 5 → Fin 5) (lo hi : Fin 5 → Int) (points : List Reduced)
    (occupancy : WalkOccupancy n)
    (h : validateModel basis xu ord lo hi points = some occupancy) :
    occupancy = sourceOccupancy basis points ∧
      (∀ x ∈ points, BoxPoint basis xu lo hi x) ∧ WalkOrdered ord points := by
  classical
  unfold validateModel at h
  split_ifs at h with hvalid
  · injection h with heq
    exact ⟨heq.symm, hvalid.1, hvalid.2⟩

theorem ordered_reduced_nodup {ord : Fin 5 → Fin 5} {points : List Reduced}
    (h : WalkOrdered ord points) : points.Nodup := by
  have hpair : List.Pairwise (fun x y => walkKey ord x < walkKey ord y) points :=
    List.isChain_iff_pairwise.mp h
  have hne : List.Pairwise (fun x y => x ≠ y) points := by
    apply hpair.imp
    intro x y hxy hEq
    subst y
    exact (lt_irrefl _ hxy)
  have toNodup : ∀ xs : List Reduced, List.Pairwise (fun x y => x ≠ y) xs → xs.Nodup := by
    intro xs
    induction xs with
    | nil => intro _; simp
    | cons x xs ih =>
        intro hp
        rw [List.pairwise_cons] at hp
        obtain ⟨hhead, htail⟩ := hp
        apply List.nodup_cons.mpr
        refine ⟨?_, ih htail⟩
        intro hx
        exact hhead x hx rfl
  exact toNodup points hne

theorem ordered_ambient_nodup {n : Nat} (basis : Matrix (Fin 5) (Fin n) Int)
    (pivot : Fin 5 → Fin n) (witness : Matrix (Fin 5) (Fin 5) (ZMod 2))
    (checked : witness * (basis.submatrix id pivot).map (Int.castRingHom (ZMod 2)) = 1)
    {ord : Fin 5 → Fin 5} {points : List Reduced} (h : WalkOrdered ord points) :
    (points.map (ambientPoint basis)).Nodup := by
  apply (ordered_reduced_nodup h).map
  intro x y heq
  have hlin (a : Fin n) : (∑ j, x j * basis j a) = ∑ j, y j * basis j a := by
    have ha := congrFun heq a
    dsimp [ambientPoint] at ha
    omega
  have hreal := real_embedding_injective basis pivot witness checked
  have hxy : Matrix.vecMul (fun j => (x j : ℝ)) (basis.map (Int.castRingHom ℝ)) =
      Matrix.vecMul (fun j => (y j : ℝ)) (basis.map (Int.castRingHom ℝ)) := by
    ext a
    simp [Matrix.vecMul, dotProduct]
    exact_mod_cast hlin a
  have hcast := hreal hxy
  funext j
  exact Int.cast_injective (congrFun hcast j)

/-- Accepted points satisfy the root box and all candidate coordinate bounds.
The equation component is discharged only from the certified kernel rows and
the separately checked `W 1 = degree` premise. -/
theorem accepted_points_valid {r n : Nat}
    (basis : Matrix (Fin 5) (Fin n) Int) (xu : Fin n → Int) (ord : Fin 5 → Fin 5)
    (lo hi : Fin 5 → Int) (points : List Reduced) (occupancy : WalkOccupancy n)
    (ambientWidth : n ≤ 10)
    (rootBoxCertified : CertifiedInt32RootBox basis xu lo hi)
    (W : Matrix (Fin r) (Fin n) Int) (degree : Fin r → Int)
    (kernel : ∀ i s, ∑ a, W s a * basis i a = 0)
    (origin : ∀ s, ∑ a, W s a = degree s)
    (h : validateModel basis xu ord lo hi points = some occupancy) :
    ∀ x ∈ points, (∀ j, lo j ≤ x j ∧ x j ≤ hi j) ∧
      (∀ a, 0 ≤ ambientPoint basis x a ∧ ambientPoint basis x a ≤ xu a) ∧
      (∀ s, ∑ a, W s a * ambientPoint basis x a = degree s) := by
  have _ambientWidth := ambientWidth
  have _rootBoxCertified := rootBoxCertified
  obtain ⟨_, hp, _⟩ := validateModel_success basis xu ord lo hi points occupancy h
  intro x hx
  obtain ⟨hroot, hcoord⟩ := hp x hx
  refine ⟨hroot, hcoord, ?_⟩
  intro s
  exact kernel_rows_preserve_weights W degree basis kernel origin x s

theorem accepted_points_ambient_distinct {n : Nat}
    (basis : Matrix (Fin 5) (Fin n) Int) (xu : Fin n → Int) (ord : Fin 5 → Fin 5)
    (lo hi : Fin 5 → Int) (points : List Reduced) (occupancy : WalkOccupancy n)
    (ambientWidth : n ≤ 10)
    (rootBoxCertified : CertifiedInt32RootBox basis xu lo hi)
    (pivot : Fin 5 → Fin n) (witness : Matrix (Fin 5) (Fin 5) (ZMod 2))
    (checked : witness * (basis.submatrix id pivot).map (Int.castRingHom (ZMod 2)) = 1)
    (h : validateModel basis xu ord lo hi points = some occupancy) :
    (points.map (ambientPoint basis)).Nodup := by
  have _ambientWidth := ambientWidth
  have _rootBoxCertified := rootBoxCertified
  obtain ⟨_, _, hordered⟩ := validateModel_success basis xu ord lo hi points occupancy h
  exact ordered_ambient_nodup basis pivot witness checked hordered

theorem occupancy_coordinate_exact {n : Nat} (basis : Matrix (Fin 5) (Fin n) Int)
    (points : List Reduced) (occupancy : WalkOccupancy n)
    (h : occupancy = sourceOccupancy basis points) (a : Fin n) :
    (a ∈ occupancy.touchZero ↔ ∃ x ∈ points, ambientPoint basis x a = 0) ∧
    (a ∈ occupancy.touchTwo ↔ ∃ x ∈ points, 2 ≤ ambientPoint basis x a) := by
  subst occupancy
  constructor <;> simp [sourceOccupancy, mem_unionMap, zeroCoords, twoPlusCoords]

theorem pairLoRow_exact {n : Nat} (basis : Matrix (Fin 5) (Fin n) Int)
    (x : Reduced) (a b : Fin n) (hnonneg : ∀ c, 0 ≤ ambientPoint basis x c) :
    (b ∈ pairLoRow basis x a) ↔
      ambientPoint basis x a = 0 ∧ ambientPoint basis x b ≤ 1 := by
  classical
  unfold pairLoRow
  by_cases ha : ambientPoint basis x a = 0
  · rw [if_pos ha]
    simp only [smallCoords, Finset.mem_filter, Finset.mem_univ, true_and]
    constructor
    · intro hb
      rcases hb with hb | hb
      · omega
      · omega
    · rintro ⟨_, hb⟩
      have hb0 := hnonneg b
      exact (show ambientPoint basis x b = 0 ∨ ambientPoint basis x b = 1 by omega)
  · rw [if_neg ha]
    simp [ha]

theorem pairHiRow_exact {n : Nat} (basis : Matrix (Fin 5) (Fin n) Int)
    (x : Reduced) (a b : Fin n) :
    (b ∈ pairHiRow basis x a) ↔
      3 ≤ ambientPoint basis x a ∨
        (ambientPoint basis x a = 2 ∧ 1 ≤ ambientPoint basis x b) := by
  classical
  unfold pairHiRow highCoords
  by_cases ha3 : 3 ≤ ambientPoint basis x a
  · simp [ha3]
  · by_cases ha2 : ambientPoint basis x a = 2
    · simp [ha2]
    · simp [ha3, ha2]

theorem occupancy_pair_low_exact {n : Nat} (basis : Matrix (Fin 5) (Fin n) Int)
    (points : List Reduced) (occupancy : WalkOccupancy n)
    (hocc : occupancy = sourceOccupancy basis points)
    (hvalid : ∀ x ∈ points, ∀ a, 0 ≤ ambientPoint basis x a)
    (a b : Fin n) :
    (b ∈ occupancy.pairLo a ∨ a ∈ occupancy.pairLo b) ↔
      ∃ x ∈ points, ambientPoint basis x a + ambientPoint basis x b ≤ 1 := by
  subst occupancy
  change ((b ∈ unionMap (fun x => pairLoRow basis x a) points) ∨
      (a ∈ unionMap (fun x => pairLoRow basis x b) points)) ↔ _
  simp only [mem_unionMap]
  constructor
  · rintro (⟨x, hx, hxb⟩ | ⟨x, hx, hxa⟩)
    · have hv := hvalid x hx
      have hr := (pairLoRow_exact basis x a b hv).mp hxb
      rcases hr with ⟨ha, hb⟩
      have hvA := hv a
      have hvB := hv b
      exact ⟨x, hx, by omega⟩
    · have hv := hvalid x hx
      have hr := (pairLoRow_exact basis x b a hv).mp hxa
      rcases hr with ⟨hb, ha⟩
      have hvA := hv a
      have hvB := hv b
      exact ⟨x, hx, by omega⟩
  · rintro ⟨x, hx, hab⟩
    have hv := hvalid x hx
    have hvA := hv a
    have hvB := hv b
    by_cases ha : ambientPoint basis x a = 0
    · left
      refine ⟨x, hx, (pairLoRow_exact basis x a b hv).mpr ⟨ha, ?_⟩⟩
      omega
    · have hb : ambientPoint basis x b = 0 := by omega
      right
      refine ⟨x, hx, (pairLoRow_exact basis x b a hv).mpr ⟨hb, ?_⟩⟩
      omega

theorem occupancy_pair_high_exact {n : Nat} (basis : Matrix (Fin 5) (Fin n) Int)
    (points : List Reduced) (occupancy : WalkOccupancy n)
    (hocc : occupancy = sourceOccupancy basis points)
    (hvalid : ∀ x ∈ points, ∀ a, 0 ≤ ambientPoint basis x a)
    (a b : Fin n) :
    (b ∈ occupancy.pairHi a ∨ a ∈ occupancy.pairHi b) ↔
      ∃ x ∈ points, 3 ≤ ambientPoint basis x a + ambientPoint basis x b := by
  subst occupancy
  change ((b ∈ unionMap (fun x => pairHiRow basis x a) points) ∨
      (a ∈ unionMap (fun x => pairHiRow basis x b) points)) ↔ _
  simp only [mem_unionMap]
  constructor
  · rintro (⟨x, hx, hxb⟩ | ⟨x, hx, hxa⟩)
    · have hv := hvalid x hx
      have hr := (pairHiRow_exact basis x a b).mp hxb
      have hvA := hv a
      have hvB := hv b
      rcases hr with h | ⟨ha, hb⟩ <;> exact ⟨x, hx, by omega⟩
    · have hv := hvalid x hx
      have hr := (pairHiRow_exact basis x b a).mp hxa
      have hvA := hv a
      have hvB := hv b
      rcases hr with h | ⟨hb, ha⟩ <;> exact ⟨x, hx, by omega⟩
  · rintro ⟨x, hx, hab⟩
    have hv := hvalid x hx
    have hvA := hv a
    have hvB := hv b
    by_cases ha3 : 3 ≤ ambientPoint basis x a
    · left
      exact ⟨x, hx, (pairHiRow_exact basis x a b).mpr (Or.inl ha3)⟩
    · by_cases hb3 : 3 ≤ ambientPoint basis x b
      · right
        exact ⟨x, hx, (pairHiRow_exact basis x b a).mpr (Or.inl hb3)⟩
      · have hcase :
            (ambientPoint basis x a = 2 ∧ 1 ≤ ambientPoint basis x b) ∨
            (ambientPoint basis x b = 2 ∧ 1 ≤ ambientPoint basis x a) := by omega
        rcases hcase with hcase | hcase
        · left
          exact ⟨x, hx, (pairHiRow_exact basis x a b).mpr (Or.inr hcase)⟩
        · right
          exact ⟨x, hx, (pairHiRow_exact basis x b a).mpr (Or.inr hcase)⟩

theorem occupancy_coordinate_thresholds {n : Nat} (basis : Matrix (Fin 5) (Fin n) Int)
    (points : List Reduced) (occupancy : WalkOccupancy n)
    (hocc : occupancy = sourceOccupancy basis points)
    (hvalid : ∀ x ∈ points, ∀ a, 0 ≤ ambientPoint basis x a) (a : Fin n) :
    (a ∈ occupancy.touchZero ↔ ∃ x ∈ points, ambientPoint basis x a - 1 < 0) ∧
    (a ∈ occupancy.touchTwo ↔ ∃ x ∈ points, 0 < ambientPoint basis x a - 1) := by
  obtain ⟨hz, ht⟩ := occupancy_coordinate_exact basis points occupancy hocc a
  constructor
  · rw [hz]
    constructor
    · rintro ⟨x, hx, hlt⟩
      have hv := hvalid x hx a
      exact ⟨x, hx, by omega⟩
    · rintro ⟨x, hx, hlt⟩
      have hv := hvalid x hx a
      exact ⟨x, hx, by omega⟩
  · rw [ht]
    constructor
    · rintro ⟨x, hx, hlt⟩
      exact ⟨x, hx, by omega⟩
    · rintro ⟨x, hx, hlt⟩
      exact ⟨x, hx, by omega⟩

theorem occupancy_pair_thresholds {n : Nat} (basis : Matrix (Fin 5) (Fin n) Int)
    (points : List Reduced) (occupancy : WalkOccupancy n)
    (hocc : occupancy = sourceOccupancy basis points)
    (hvalid : ∀ x ∈ points, ∀ a, 0 ≤ ambientPoint basis x a) (a b : Fin n) :
    ((b ∈ occupancy.pairLo a ∨ a ∈ occupancy.pairLo b) ↔
      ∃ x ∈ points, ambientPoint basis x a - 1 + (ambientPoint basis x b - 1) < 0) ∧
    ((b ∈ occupancy.pairHi a ∨ a ∈ occupancy.pairHi b) ↔
      ∃ x ∈ points, 0 < ambientPoint basis x a - 1 + (ambientPoint basis x b - 1)) := by
  have hlo := occupancy_pair_low_exact basis points occupancy hocc hvalid a b
  have hhi := occupancy_pair_high_exact basis points occupancy hocc hvalid a b
  constructor
  · rw [hlo]
    constructor
    · rintro ⟨x, hx, hlt⟩
      exact ⟨x, hx, by omega⟩
    · rintro ⟨x, hx, hlt⟩
      exact ⟨x, hx, by omega⟩
  · rw [hhi]
    constructor
    · rintro ⟨x, hx, hlt⟩
      exact ⟨x, hx, by omega⟩
    · rintro ⟨x, hx, hlt⟩
      exact ⟨x, hx, by omega⟩

end Refpoly.GpuWalkValidator
