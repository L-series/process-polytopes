/-
BoxReductionAudit faithfully ported from the reviewed refpoly-5d snapshot.
See SOURCES.md for attribution and exact source identities.
-/
import Refpoly.Finiteness

/-!
# Axiom regression checks for the box lemma

The geometric reduction must remain independent of unproved inputs.
The reflexive specialization and its finiteness corollary use the proved
reflexive volume bound. These checks run with `lake build`.
-/

/-- info: 'Refpoly.BoxReduction.box_of_volume_bound' depends on axioms: [propext, Classical.choice, Quot.sound] -/
#guard_msgs in
#print axioms Refpoly.BoxReduction.box_of_volume_bound

/-- info: 'Refpoly.lagarias_ziegler_box' depends on axioms: [propext, Classical.choice, Quot.sound] -/
#guard_msgs in
#print axioms Refpoly.lagarias_ziegler_box

/-- info: 'Refpoly.finitely_many_reflexive' depends on axioms: [propext, Classical.choice, Quot.sound] -/
#guard_msgs in
#print axioms Refpoly.finitely_many_reflexive
