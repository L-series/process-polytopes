import Refpoly.JoinLoopSource
import Refpoly.JoinBlocks

/-! Shared-value invariant at a successful tuple extension, using the actual
source matcher. Candidate/entry position permutations preserve row equations.
The matching loop must still be shown to construct the permutation used by
`matched_weights_equation`; the interface is explicit rather than assumed
from the presence of equal weights. -/
namespace Refpoly.JoinLoopSource

/-- Values installed by the chosen keys at all levels below `depth`. -/
def PrefixFields (E : Env) (chosen val : ℕ → ℕ) (depth : ℕ) : Prop :=
  ∀ i, i < depth → ∀ p, p < E.k (E.slot i) →
    val (E.coord (E.slot i) p) = field (E.k (E.slot i)) p (E.key (E.slot i) (chosen i))

theorem matchKey_extends_prefix {E : Env} (hP : PlanOK E)
    (depth kk : ℕ) (chosen val : ℕ → ℕ) (hpref : PrefixFields E chosen val depth)
    (hok : (matchKey (E.key (E.slot depth) kk) (E.k (E.slot depth))
      (E.coord (E.slot depth)) (E.bound depth) val).1 = true) :
    PrefixFields E (Function.update chosen depth kk)
      (matchKey (E.key (E.slot depth) kk) (E.k (E.slot depth))
        (E.coord (E.slot depth)) (E.bound depth) val).2 (depth + 1) := by
  intro i hi p hpk
  by_cases hid : i = depth
  · subst i
    rw [Function.update_self]
    exact matchFrom_installs _ _ _ _ (hP.inj _) 0 val hok p (Nat.zero_le _) hpk
  · have hlt : i < depth := by omega
    have hb : E.bound depth (E.coord (E.slot i) p) = true := by
      apply bound_mono hP (i + 1) depth _ (by omega)
      exact (hP.bound_succ i _).mpr (Or.inr ⟨p, hpk, rfl⟩)
    rw [Function.update_of_ne hid, matchKey, matchFrom_bound _ _ _ _ 0 val _ hb]
    exact hpref i hlt p hpk

end Refpoly.JoinLoopSource

namespace Refpoly.JoinRejection

/-- A bijective weight matching gives the exact table row equation, including
repeated weights. This also applies to the shared-prefix permutation extended
by the identity on private positions. -/
theorem matched_weights_equation {n : ℕ} (entry candidate x : Fin n → ℕ)
    (e : Fin n ≃ Fin n) (hmatch : ∀ q, candidate q = entry (e q)) :
    (∑ q, candidate q * x q) = ∑ p, entry p * x (e.symm p) := by
  simpa only [hmatch, Equiv.symm_apply_apply] using
    (e.sum_comp (fun p => entry p * x (e.symm p)))

theorem matched_weights_solutions {n : ℕ} (entry candidate x : Fin n → ℕ)
    (e : Fin n ≃ Fin n) (hmatch : ∀ q, candidate q = entry (e q)) (degree : ℕ) :
    (∑ q, candidate q * x q) = degree ↔
      (∑ p, entry p * x (e.symm p)) = degree := by
  rw [matched_weights_equation entry candidate x e hmatch]

end Refpoly.JoinRejection
