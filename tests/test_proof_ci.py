#!/usr/bin/env python3
"""Regression tests for proof CI, including actual Lean axiom closures."""

import copy
import json
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "proofs/ci"))

from proof_audit import (  # noqa: E402
    check_axioms,
    check_lock,
    check_pins,
    check_sources,
    semantic_snapshot,
    source_tokens,
)


def targets(*, closed=(), obligations=()):
    return {
        "closed": list(closed),
        "obligations": list(obligations),
        "implementations": [],
        "derived": [],
    }


def row(name, *, axioms=(), direct=False, module="Fixture", definition=""):
    return {
        "name": name,
        "module": module,
        "type": "True",
        "is_theorem": not definition,
        "definition": definition,
        "dependencies": [],
        "direct_sorry": direct,
        "axioms": list(axioms),
    }


class AuditPolicyTests(unittest.TestCase):
    def test_standard_foundations_are_allowed(self):
        rows = [row("ok", axioms=["propext", "Classical.choice", "Quot.sound"])]
        self.assertEqual(check_axioms(rows, targets(closed=["ok"]), True), [])

    def test_closed_statement_cannot_regress(self):
        rows = [row("closed", axioms=["sorryAx"], direct=True)]
        self.assertTrue(check_axioms(rows, targets(closed=["closed"]), False))

    def test_missing_target_is_an_error(self):
        self.assertTrue(check_axioms([], targets(obligations=["missing"]), False))

    def test_unregistered_hole_fails_conditional_gate(self):
        self.assertTrue(check_axioms([row("hidden", axioms=["sorryAx"])], targets(), False))

    def test_headline_cannot_replace_composition_with_own_hole(self):
        inventory = targets()
        inventory["derived"] = ["headline"]
        rows = [row("headline", axioms=["sorryAx"], direct=True)]
        self.assertTrue(check_axioms(rows, inventory, False))

    def test_signature_weakening_is_detected(self):
        rows = [row("headline")]
        inventory = targets(obligations=["headline"])
        lock = semantic_snapshot(rows, inventory)
        changed = copy.deepcopy(rows)
        changed[0]["type"] = "False → True"
        self.assertTrue(check_lock(changed, inventory, lock))

    def test_definition_weakening_is_detected(self):
        rows = [row("Refpoly.IsReflexive", module="Refpoly.Basic", definition="precise")]
        lock = semantic_snapshot(rows, targets())
        changed = copy.deepcopy(rows)
        changed[0]["definition"] = "fun _ => True"
        self.assertTrue(check_lock(changed, targets(), lock))

    def test_elided_instance_dependency_changes_are_detected(self):
        rows = [row("Refpoly.IsIP", module="Refpoly.Basic", definition="interior")]
        rows[0]["dependencies"] = ["standardTopology"]
        lock = semantic_snapshot(rows, targets())
        changed = copy.deepcopy(rows)
        changed[0]["dependencies"] = ["differentTopology"]
        self.assertTrue(check_lock(changed, targets(), lock))

    def test_dropping_audit_targets_is_detected(self):
        rows = [row("headline")]
        inventory = targets(obligations=["headline"])
        lock = semantic_snapshot(rows, inventory)
        self.assertTrue(check_lock(rows, targets(), lock))

    def test_current_source_pins_are_exact(self):
        self.assertEqual(check_pins(ROOT / "proofs"), [])

    def test_dependency_pin_drift_is_detected(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            (root / "lean-toolchain").write_text("leanprover/lean4:stable\n")
            (root / "lake-manifest.json").write_text("{}\n")
            self.assertEqual(len(check_pins(root)), 2)

    def test_nested_comments_and_strings_do_not_trigger_source_policy(self):
        text = '/- sorry /- partial -/ unsafe -/\ndef x := "axiom \\"extern\\"" -- admit\n'
        self.assertEqual(source_tokens(text), frozenset({"def", "x"}))

    def test_actual_escape_hatches_remain_visible(self):
        self.assertIn("partial", source_tokens("partial def loop : Nat := loop"))
        self.assertIn("native_decide", source_tokens("theorem t : True := by native_decide"))

    def test_unterminated_source_is_rejected(self):
        with self.assertRaises(ValueError):
            source_tokens("/- unfinished")

    def test_current_proof_sources_obey_policy(self):
        self.assertEqual(check_sources(ROOT / "proofs"), [])


class CompilerClosureTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        with tempfile.TemporaryDirectory(prefix="proof-ci-fixtures-") as directory:
            root = Path(directory)
            output = root / "closures.json"
            source = root / "Fixtures.lean"
            source.write_text(
                """import Lean
import Lean.Util.CollectAxioms
open Lean Meta
namespace Fixture
theorem closed : True := True.intro
theorem direct_hole : True := by sorry
theorem transitive_hole : True := direct_hole
axiom unexpected_axiom : False
theorem unexpected : True := False.elim unexpected_axiom
end Fixture
run_meta do
  let env ← getEnv
  let mut rows : Array Json := #[]
  for name in #[``Fixture.closed, ``Fixture.direct_hole,
      ``Fixture.transitive_hole, ``Fixture.unexpected] do
    let info := env.constants.find! name
    let axioms ← collectAxioms name
    let value := info.value? (allowOpaque := true)
    rows := rows.push <| Json.mkObj [
      ("name", toJson name.toString),
      ("axioms", toJson (axioms.map Name.toString)),
      ("direct_sorry", toJson ((value.map Expr.getUsedConstants).getD #[]
        |>.contains ``sorryAx))]
  IO.FS.writeFile """
                + json.dumps(str(output))
                + " (toJson rows).pretty\n"
            )
            result = subprocess.run(
                ["lake", "env", "lean", str(source)],
                cwd=ROOT / "proofs",
                capture_output=True,
                text=True,
                check=False,
            )
            if result.returncode:
                raise RuntimeError("Lean audit-fixture failure:\n" + result.stdout + result.stderr)
            cls.rows = {entry["name"]: entry for entry in json.loads(output.read_text())}

    def test_actual_closed_proof_passes(self):
        name = "Fixture.closed"
        self.assertEqual(check_axioms([self.rows[name]], targets(closed=[name]), True), [])

    def test_actual_direct_hole_passes_only_conditionally(self):
        name = "Fixture.direct_hole"
        self.assertTrue(self.rows[name]["direct_sorry"])
        inventory = targets(obligations=[name])
        self.assertEqual(check_axioms([self.rows[name]], inventory, False), [])
        self.assertTrue(check_axioms([self.rows[name]], inventory, True))

    def test_actual_transitive_hole_is_detected(self):
        name = "Fixture.transitive_hole"
        self.assertFalse(self.rows[name]["direct_sorry"])
        self.assertIn("sorryAx", self.rows[name]["axioms"])
        self.assertTrue(check_axioms([self.rows[name]], targets(obligations=[name]), True))

    def test_actual_custom_axiom_fails_both_gates(self):
        name = "Fixture.unexpected"
        self.assertIn("Fixture.unexpected_axiom", self.rows[name]["axioms"])
        inventory = targets(obligations=[name])
        self.assertTrue(check_axioms([self.rows[name]], inventory, False))
        self.assertTrue(check_axioms([self.rows[name]], inventory, True))


if __name__ == "__main__":
    unittest.main(verbosity=2)
