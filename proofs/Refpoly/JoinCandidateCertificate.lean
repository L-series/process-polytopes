import Refpoly.JoinTupleBridge

/-! Primitive candidate/entry facts supplied by generation, prefix matching,
and row-table validation. Their simultaneous CWS row equivalence is derived.
No count, occupancy or rejection-soundness fact is part of this certificate. -/
namespace Refpoly.JoinPoints

structure RowCertificate {N R : ℕ} (C : CWS R N) (L : Layout N R) where
  prefixBound : ∀ r, L.k r ≤ L.size r
  basePos : ∀ r, Fin (L.size r) → Fin N
  entryWeight : ∀ r, Fin (L.size r) → ℕ
  candidateWeight : ∀ r, Fin (L.size r) → ℕ
  prePerm : ∀ r, Fin (L.k r) ≃ Fin (L.k r)
  pos_inj : ∀ r, Function.Injective (basePos r)
  coefficient : ∀ r q, C.W r (basePos r q) = (candidateWeight r q : ℝ)
  support : ∀ r i, (∀ q, basePos r q ≠ i) → C.W r i = 0
  sharedMatch : ∀ r q, candidateWeight r (Fin.castLE (prefixBound r) q) =
    entryWeight r (Fin.castLE (prefixBound r) (prePerm r q))
  privateMatch : ∀ r q, L.k r ≤ q.val → candidateWeight r q = entryWeight r q
  positions : ∀ r q, L.pos r q =
    basePos r ((prefixPrivateEquiv (prefixBound r) (prePerm r)).symm q)
  solutions : ∀ r x, x ∈ L.sol r ↔
    (∑ p, (entryWeight r p : ℝ) * (x p : ℝ)) = ∑ p, (entryWeight r p : ℝ)

theorem RowCertificate.equations {N R : ℕ} {C : CWS R N} {L : Layout N R}
    (c : RowCertificate C L) :
    ∀ X, IsPoint L X ↔ ∀ r, ∑ i, C.W r i * (X i : ℝ) = ∑ i, C.W r i :=
  candidate_layout_rows_prefix C L L.k c.prefixBound c.basePos c.entryWeight c.candidateWeight
    c.prePerm c.pos_inj c.coefficient c.support c.sharedMatch c.privateMatch c.positions c.solutions
end Refpoly.JoinPoints
