import Refpoly
import Lean.Util.CollectAxioms

open Lean Meta

set_option format.width 160
set_option pp.universes true
set_option pp.proofs false
set_option pp.funBinderTypes true
set_option pp.piBinderTypes true
set_option pp.fieldNotation false

run_meta do
  let env ← getEnv
  let entries := env.constants.toList.filter fun (name, _) =>
    name.toString.startsWith "Refpoly." || name.toString.startsWith "_private.Refpoly"
  let entries := entries.toArray.qsort fun a b => a.1.toString < b.1.toString
  let mut rows : Array Json := #[]
  for (name, info) in entries do
    let some moduleIndex := env.getModuleIdxFor? name
      | throwError "No defining module recorded for imported constant {name}"
    let moduleName := env.header.moduleNames[moduleIndex]!
    let type ← ppExpr info.type
    let axioms ← collectAxioms name
    let value := info.value? (allowOpaque := true)
    let dependencies := info.type.getUsedConstants ++
      (value.map Expr.getUsedConstants).getD #[]
    let body ← if info.isTheorem then pure "" else
      match value with
      | some expression => pure (← ppExpr expression).pretty
      | none => pure ""
    rows := rows.push <| Json.mkObj [
      ("name", toJson name.toString),
      ("module", toJson moduleName.toString),
      ("is_theorem", toJson info.isTheorem),
      ("type", toJson type.pretty),
      ("definition", toJson body),
      ("dependencies", toJson (dependencies.map Name.toString)),
      ("direct_sorry", toJson ((value.map Expr.getUsedConstants).getD #[]
        |>.contains ``sorryAx)),
      ("axioms", toJson (axioms.map Name.toString))]
  let output := (← IO.getEnv "PROOF_CI_OUTPUT").getD "ci-output/inventory.json"
  IO.FS.writeFile output (toJson rows).pretty
  logInfo m!"Exported {rows.size} project constants to {output}."
