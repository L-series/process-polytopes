import Refpoly.KS.Assembly

open Refpoly.KS

-- Compiled execution of finite assembly, including invalid inputs and duplicates.
def main : IO Unit := do
  let zero := assemble 0 (fun _ => [])
  unless zero.length == 1 do
    throw <| IO.userError "dimension-zero empty CWS missing"
  let empty := assemble 1 (fun _ => [])
  unless empty.isEmpty do
    throw <| IO.userError "empty pool produced a positive-dimensional CWS"
  let ones := assemble 1 (fun m => if m = 2 then [fun _ => 1] else [])
  unless ones.length == 2 && ones.all (fun C => C.N == 2 && C.r == 1) do
    throw <| IO.userError "ordered support enumeration lost duplicates or dimensions"
  let zeros := assemble 1 (fun m => if m = 2 then [fun _ => 0] else [])
  unless zeros.isEmpty do
    throw <| IO.userError "zero rows passed coverage/independence"
  let dependent : CWSCode 2 := ⟨4, 2, fun _ _ => 1⟩
  unless validCWSCheck dependent == false do
    throw <| IO.userError "dependent covering rows accepted"
  let independent : CWSCode 2 := ⟨4, 2, fun a j => if j.val / 2 = a.val then 1 else 0⟩
  unless validCWSCheck independent do
    throw <| IO.userError "independent covering rows rejected"
  IO.println "Assembly execution checks passed."
