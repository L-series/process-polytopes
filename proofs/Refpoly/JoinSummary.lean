import Refpoly.JoinCWS

/-! Accumulation and checked-arithmetic refinement of the executable join model. -/
namespace Refpoly.JoinRejection

structure Summary (k : Nat) where
  count : Nat
  lower : Fin k → Bool
  upper : Fin k → Bool
  deriving DecidableEq

def emptySummary (k : Nat) : Summary k := ⟨0, fun _ => false, fun _ => false⟩

def combineSummary {k : Nat} (a b : Summary k) : Summary k :=
  ⟨a.count + b.count, fun i => a.lower i || b.lower i,
    fun i => a.upper i || b.upper i⟩

def pointSummary {α : Type*} {k : Nat} (xs : List α) (f : Fin k → α → Int) : Summary k :=
  ⟨xs.length, fun i => xs.any (fun x => decide (f i x < 0)),
    fun i => xs.any (fun x => decide (0 < f i x))⟩

theorem pointSummary_append {α : Type*} {k : Nat} (xs ys : List α)
    (f : Fin k → α → Int) :
    pointSummary (xs ++ ys) f = combineSummary (pointSummary xs f) (pointSummary ys f) := by
  simp [pointSummary, combineSummary, List.any_append]

def accumulateBlocks {α : Type*} {k : Nat} (bs : List (List α)) (f : Fin k → α → Int) :
    Summary k := bs.foldr (fun xs s => combineSummary (pointSummary xs f) s) (emptySummary k)

/-- Summing counts and OR-ing exact block occupancies produces exactly the
summary of the complete point list. No ordering of keys or rows is assumed. -/
theorem accumulateBlocks_exact {α : Type*} {k : Nat} (bs : List (List α))
    (f : Fin k → α → Int) : accumulateBlocks bs f = pointSummary bs.flatten f := by
  induction bs with
  | nil => rfl
  | cons xs bs ih =>
    simp only [accumulateBlocks, List.foldr_cons, List.flatten_cons, pointSummary_append]
    rw [← ih]
    rfl

/-- General CUDA computes an uncapped exact verdict; only a kept candidate is
then routed by the cap. Specialized joins may instead stop early and keep. -/
def decideExactJoin (d np : Nat) (covered : Bool) (sides : List Sides) : Verdict :=
  decideJoin d np np covered sides

theorem cws_exact_join_inconclusive {d r m : Nat} (C : CWS r (d + r)) (hip : C.IsIP)
    (p : Fin m → CWSPoint C) (hp : Function.Surjective p)
    (coefficients : List (Fin (d + r) → Int)) (covered : Bool) :
    decideExactJoin d m covered
      ((coefficients.map (fun a i => integerEval a (p i))).map strictSides) = .keep :=
  cws_join_preserves_ip_all_dimensions C hip p hp coefficients m covered

/-- Arithmetic guards used by the CUDA accumulator, expressed without wrap.
An unsuccessful guard means inconclusive, never a rejection. -/
def checkedMul (max a b : Nat) : Option Nat :=
  if b = 0 ∨ max / b < a then none else some (a * b)

def checkedAdd (max a b : Nat) : Option Nat :=
  if max < b ∨ max - b < a then none else some (a + b)

theorem checkedMul_exact {max a b v : Nat} (h : checkedMul max a b = some v) :
    v = a * b ∧ v ≤ max := by
  unfold checkedMul at h
  split_ifs at h with hg
  have hv : a * b = v := Option.some.inj h
  have hb : 0 < b := by omega
  have hab : a ≤ max / b := by omega
  exact ⟨hv.symm, hv ▸ (Nat.le_div_iff_mul_le hb).mp hab⟩

theorem checkedAdd_exact {max a b v : Nat} (h : checkedAdd max a b = some v) :
    v = a + b ∧ v ≤ max := by
  unfold checkedAdd at h
  split_ifs at h with hg
  have hv : a + b = v := Option.some.inj h
  constructor
  · exact hv.symm
  · omega

/-- The literal key encoding preserves all four 16-bit positions. -/
def pack16 (a b c d : Nat) : Nat := ((a * 65536 + b) * 65536 + c) * 65536 + d

theorem pack16_injective {a b c d a' b' c' d' : Nat}
    (_ha : a < 65536) (hb : b < 65536) (hc : c < 65536) (hd : d < 65536)
    (_ha' : a' < 65536) (hb' : b' < 65536) (hc' : c' < 65536) (hd' : d' < 65536)
    (h : pack16 a b c d = pack16 a' b' c' d') :
    a = a' ∧ b = b' ∧ c = c' ∧ d = d' := by
  unfold pack16 at h
  omega

end Refpoly.JoinRejection
