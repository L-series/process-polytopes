import Refpoly.CudaIP

/-! Semantics of the source evaluator's pure signed-integer expression fragment.
`checkedEval` represents C++ definedness: it does not add runtime overflow checks
to the source. It returns `none` precisely for an unrepresentable intermediate.
-/
namespace Refpoly.CudaIP

inductive EvalExpr where
  | constant
  | normal (i : Fin 5)
  | point (i : Fin 5)
  | add (a b : EvalExpr)
  | mul (a b : EvalExpr)
  deriving DecidableEq, Repr

def evalExpr (e : Equation 5) (x : Fin 5 → Int) : EvalExpr → Int
  | .constant => e.constant
  | .normal i => e.normal i
  | .point i => x i
  | .add a b => evalExpr e x a + evalExpr e x b
  | .mul a b => evalExpr e x a * evalExpr e x b

def fits64 (v : Int) : Prop := -(2^63) ≤ v ∧ v < 2^63
instance (v : Int) : Decidable (fits64 v) := inferInstanceAs (Decidable (_ ∧ _))

def checked64 (v : Int) : Option Int := if fits64 v then some v else none

def checkedEval (e : Equation 5) (x : Fin 5 → Int) : EvalExpr → Option Int
  | .constant => checked64 e.constant
  | .normal i => checked64 (e.normal i)
  | .point i => checked64 (x i)
  | .add a b => do
      let va ← checkedEval e x a
      let vb ← checkedEval e x b
      checked64 (va + vb)
  | .mul a b => do
      let va ← checkedEval e x a
      let vb ← checkedEval e x b
      checked64 (va * vb)

def expressionDefined (e : Equation 5) (x : Fin 5 → Int) (t : EvalExpr) : Prop :=
  fits64 (evalExpr e x t) ∧ match t with
    | .add a b | .mul a b => expressionDefined e x a ∧ expressionDefined e x b
    | _ => True

theorem checked64_exact (a v : Int) :
    checked64 a = some v ↔ fits64 a ∧ v = a := by
  unfold checked64
  split_ifs <;> simp_all [eq_comm]

/-- Signed multiplication and every left-associated intermediate addition must
fit. A bound on the final dot product alone would be insufficient. -/
theorem checkedEval_exact (e : Equation 5) (x : Fin 5 → Int) (t : EvalExpr) (v : Int) :
    checkedEval e x t = some v ↔ expressionDefined e x t ∧ v = evalExpr e x t := by
  induction t generalizing v with
  | constant => simp [checkedEval, expressionDefined, evalExpr, checked64_exact]
  | normal i => simp [checkedEval, expressionDefined, evalExpr, checked64_exact]
  | point i => simp [checkedEval, expressionDefined, evalExpr, checked64_exact]
  | add a b ha hb =>
    simp [checkedEval, Option.bind_eq_some_iff, ha, hb, checked64_exact,
      expressionDefined, evalExpr, and_assoc, and_left_comm, and_comm]
  | mul a b ha hb =>
    simp [checkedEval, Option.bind_eq_some_iff, ha, hb, checked64_exact,
      expressionDefined, evalExpr, and_assoc, and_left_comm, and_comm]

end Refpoly.CudaIP
