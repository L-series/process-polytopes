import Refpoly.JoinFibreValidator
import Refpoly.JoinTupleBridge

/-! Convert the source fibre validator's padded five-slot rows into finite
function fibres used by `JoinPoints.Layout`. -/
namespace Refpoly.JoinValidatedFibres

open Refpoly.JoinFibreValidator
open Refpoly.JoinPoints
open Refpoly.JoinLoopSource
open Refpoly.JoinRejection

/-- Decode a projected row of exactly `size` natural entries as a finite
function. -/
def decode (size : ℕ) (x : List ℕ) : Fin size → ℕ := fun q => x.getD q.val 0

def decodePoint (size : ℕ) (x : Point) : Fin size → ℕ :=
  decode size (rowValues size x)

def rowSol (size : ℕ) (points : List Point) : Finset (Fin size → ℕ) :=
  (points.map (decodePoint size)).toFinset

def rowPrefix (size shared : ℕ) (x : Fin size → ℕ) : ℕ :=
  pack shared (fun p => if h : p < size then x ⟨p, h⟩ else 0)

theorem decode_ofFn {size : ℕ} (x : Fin size → ℕ) : decode size (List.ofFn x) = x := by
  funext q
  simp [decode]

theorem decode_injective_of_length {size : ℕ} {x y : List ℕ}
    (hx : x.length = size) (hy : y.length = size)
    (h : decode size x = decode size y) : x = y := by
  apply List.ext_get (by omega)
  intro i hxi hyi
  have hxy := congrFun h ⟨i, by omega⟩
  change x.getD i 0 = y.getD i 0 at hxy
  calc
    x.get ⟨i, hxi⟩ = x.getD i 0 := (List.getD_eq_get x 0 ⟨i, hxi⟩).symm
    _ = y.getD i 0 := hxy
    _ = y.get ⟨i, hyi⟩ := List.getD_eq_get y 0 ⟨i, hyi⟩

theorem ofFn_decode {size : ℕ} {x : List ℕ} (hx : x.length = size) :
    List.ofFn (decode size x) = x := by
  apply decode_injective_of_length (by simp) hx
  simp [decode_ofFn]

theorem rowValues_length {size : ℕ} {x : Point} (hsize : size ≤ 5)
    (hshape : pointShape size x) :
    (rowValues size x).length = size := by
  rcases hshape with ⟨hlen, _⟩
  simp [rowValues, List.length_take, hlen]
  omega

theorem decodePoint_injective {size : ℕ} {points : List Point}
    (hsize : size ≤ 5)
    (hshape : ∀ x ∈ points, pointShape size x) :
    ∀ x ∈ points, ∀ y ∈ points, decodePoint size x = decodePoint size y → x = y := by
  intro x hx y hy hxy
  apply map_rowValues_injective hshape x hx y hy
  exact decode_injective_of_length (rowValues_length hsize (hshape x hx))
    (rowValues_length hsize (hshape y hy)) hxy

theorem rowPrefix_decodePoint {size shared : ℕ} (hshared : shared ≤ size)
    (x : Point) : rowPrefix size shared (decodePoint size x) = packPoint shared x := by
  unfold rowPrefix packPoint decodePoint decode
  apply pack_congr
  intro p hp
  have hpsize : p < size := by omega
  simp [rowValues, hpsize]

theorem pointShape_of_accepts {entry : Entry} {size shared : ℕ}
    {points : List Point} {fibres : List Fibre}
    (h : Accepts entry size shared points fibres) {x : Point} (hx : x ∈ points) :
    pointShape size x := by
  rcases h with ⟨_, _, _, _, _, _, _, _, chunks, hchunks, hOK, _⟩
  have hflat := (sourceChunks_sound fibres points chunks hchunks).2.1
  have hxflat : x ∈ (chunks.map Prod.snd).flatten := by simpa [hflat] using hx
  rcases List.mem_flatten.mp hxflat with ⟨rows, hrows, hrow⟩
  rcases List.mem_map.mp hrows with ⟨⟨g, rows'⟩, hg, heq⟩
  simp only [Prod.snd] at heq
  subst rows'
  rcases hOK (g, rows) hg with ⟨_, _, _, _, _, hpoints, _, _⟩
  exact (hpoints x hrow).1.1

theorem rowSol_mem_iff {entry : Entry} {size shared : ℕ}
    {points : List Point} {fibres : List Fibre}
    (h : Accepts entry size shared points fibres)
    (f : Fin size → ℕ) :
    f ∈ rowSol size points ↔
      rowDot entry.weights (List.ofFn f) = entry.degree := by
  have hsize : entry.weights.length = size := h.2.2.2.2.1
  have hsize5 : size ≤ 5 := h.2.1
  have haccepted := accepted_exact_solution_set h
  rw [rowSol, List.mem_toFinset, List.mem_map]
  constructor
  · rintro ⟨x, hx, hdecode⟩
    have hlen := rowValues_length hsize5 (pointShape_of_accepts h hx)
    have hx' : rowValues size x = List.ofFn f := by
      apply decode_injective_of_length hlen (by simp)
      exact hdecode.trans (decode_ofFn f).symm
    have := (haccepted (List.ofFn f)).mp (List.mem_map.mpr ⟨x, hx, hx'⟩)
    exact this.2
  · intro hdot
    have hspec := (haccepted (List.ofFn f)).mpr ⟨by simpa [hsize], hdot⟩
    obtain ⟨x, hx, hrow⟩ := List.mem_map.mp hspec
    refine ⟨x, hx, ?_⟩
    change decode size (rowValues size x) = f
    rw [hrow, decode_ofFn]

def rowWeight {size : ℕ} (entry : Entry) (i : Fin size) : ℕ :=
  entry.weights.getD i.val 0

theorem rowDot_ofFn_eq_sum {size : ℕ} (weights : List ℕ) (hweights : weights.length = size)
    (x : Fin size → ℕ) :
    rowDot weights (List.ofFn x) = ∑ i : Fin size, weights.getD i.val 0 * x i := by
  induction size generalizing weights with
  | zero =>
      have hw : weights = [] := List.eq_nil_of_length_eq_zero hweights
      subst weights
      simp [rowDot]
  | succ size ih =>
      cases weights with
      | nil => simp at hweights
      | cons w ws =>
          have hlen : ws.length = size := by simp at hweights; omega
          rw [List.ofFn_succ]
          simp only [rowDot]
          rw [Fin.sum_univ_succ]
          simp only [List.getD_eq_get, List.getElem_cons_zero, List.getElem_cons_succ,
            Fin.val_zero, Fin.val_succ]
          rw [ih ws hlen (fun i => x i.succ)]
          simp [rowWeight, Nat.mul_comm]

theorem accepted_rowSol_equation {entry : Entry} {size shared : ℕ}
    {points : List Point} {fibres : List Fibre}
    (h : Accepts entry size shared points fibres)
    (hdegree : entry.degree = ∑ i : Fin size, rowWeight entry i)
    (x : Fin size → ℕ) :
    x ∈ rowSol size points ↔
      (∑ i : Fin size, (rowWeight entry i : ℝ) * (x i : ℝ)) =
        ∑ i : Fin size, (rowWeight entry i : ℝ) := by
  constructor
  · intro hx
    have hdot := (rowSol_mem_iff h x).mp hx
    have hsum : (∑ i : Fin size, rowWeight entry i * x i) = entry.degree := by
      rw [rowDot_ofFn_eq_sum (size := size) entry.weights h.2.2.2.2.1] at hdot
      simpa [rowWeight] using hdot
    have hnat : (∑ i : Fin size, rowWeight entry i * x i) =
        ∑ i : Fin size, rowWeight entry i := hsum.trans hdegree
    exact_mod_cast hnat
  · intro hreal
    have hnat : (∑ i : Fin size, rowWeight entry i * x i) =
        ∑ i : Fin size, rowWeight entry i := by exact_mod_cast hreal
    have hsum : (∑ i : Fin size, rowWeight entry i * x i) = entry.degree :=
      hnat.trans hdegree.symm
    apply (rowSol_mem_iff h x).mpr
    rw [rowDot_ofFn_eq_sum (size := size) entry.weights h.2.2.2.2.1]
    simpa [rowWeight] using hsum

end Refpoly.JoinValidatedFibres
