import Refpoly.JoinType3
import Refpoly.JoinMaskAssembly

/-! Refinement boundary for the fused type-3 summary scan.

Each matched word pair contributes the exact fibre-product count and the
per-test occupancy of its tuple block. A completed source scan therefore has
the same count and accumulated masks as the ordinary join leaf specification.
Candidate/table row equivalence remains an explicit premise at the final CWS
theorem, as do source event values and coverage facts.
-/
namespace Refpoly.JoinType3

open Refpoly.JoinPoints
open Refpoly.JoinRejection

noncomputable def eventSummary {R m : ℕ} (L : Layout (5 + R) R)
    (tests : Fin m → Test (5 + R)) (event : LeafEvent (5 + R) R) : Summary m :=
  { count := ∏ r, (fibre L r (event.1 r)).card
    lower := fun i => (testSides L event.1 event.2 (tests i)).1
    upper := fun i => (testSides L event.1 event.2 (tests i)).2 }

theorem complete_count_eq_index {R m : ℕ} (L : Layout (5 + R) R)
    (tests : Fin m → Test (5 + R)) (cap : ℕ) (lookup : ℕ → Option ℕ)
    (indices : List ℕ) (eventOf : ℕ × ℕ → LeafEvent (5 + R) R)
    (events : List (LeafEvent (5 + R) R)) (out : Summary m)
    (hEventsScan : events = (matchedPairs indices lookup).map eventOf)
    (hsource : sourceScan cap lookup (fun pair => eventSummary L tests (eventOf pair))
      indices (emptySummary m) = .complete out)
    (T : Finset (Fin R → ℕ))
    (hnodup : (events.map Prod.fst).Nodup)
    (hEventsT : (events.map Prod.fst).toFinset = T) :
    out.count = (index L T).card := by
  have hscan := (sourceScan_complete cap lookup
    (fun pair => eventSummary L tests (eventOf pair)) indices out hsource).1
  have hprod : out.count =
      ((events.map Prod.fst).map (fun t => ∏ r, (fibre L r (t r)).card)).sum := by
    calc
      out.count = (((matchedPairs indices lookup).map
        (fun pair => eventSummary L tests (eventOf pair))).map Summary.count).sum := hscan
      _ = ((matchedPairs indices lookup).map
        (fun pair => ∏ r, (fibre L r ((eventOf pair).1 r)).card)).sum := by
          simp [List.map_map, eventSummary, Function.comp_def]
      _ = (events.map (fun e => ∏ r, (fibre L r (e.1 r)).card)).sum := by
          rw [hEventsScan]
          simp only [List.map_map, Function.comp_def]
      _ = ((events.map Prod.fst).map
        (fun t => ∏ r, (fibre L r (t r)).card)).sum := by
          simp [List.map_map, Function.comp_def]
  rw [← List.sum_toFinset _ hnodup, hEventsT, ← index_card L T] at hprod
  exact hprod

theorem complete_masks_eq_accumulated {R m : ℕ} (L : Layout (5 + R) R)
    (tests : Fin m → Test (5 + R)) (cap : ℕ) (lookup : ℕ → Option ℕ)
    (indices : List ℕ) (eventOf : ℕ × ℕ → LeafEvent (5 + R) R)
    (events : List (LeafEvent (5 + R) R)) (out : Summary m)
    (hEventsScan : events = (matchedPairs indices lookup).map eventOf)
    (hsource : sourceScan cap lookup (fun pair => eventSummary L tests (eventOf pair))
      indices (emptySummary m) = .complete out) :
    List.ofFn (fun i => (out.lower i, out.upper i)) =
      accumulatedSourceMasks L events (List.ofFn tests) := by
  have hscan := sourceScan_complete cap lookup
    (fun pair => eventSummary L tests (eventOf pair)) indices out hsource
  unfold accumulatedSourceMasks
  rw [List.map_ofFn]
  apply List.ofFn_inj.mpr
  funext i
  apply Prod.ext
  · have hlower := hscan.2.1 i
    change out.lower i =
      (events.any fun e => (testSides L e.1 e.2 (tests i)).1)
    rw [hEventsScan]
    simpa [eventSummary, List.any_map] using hlower
  · have hupper := hscan.2.2 i
    change out.upper i =
      (events.any fun e => (testSides L e.1 e.2 (tests i)).2)
    rw [hEventsScan]
    simpa [eventSummary, List.any_map] using hupper

theorem sourceScan_capped_keep {k : ℕ} (cap : ℕ) (lookup : ℕ → Option ℕ)
    (leaf : ℕ × ℕ → Summary k) (indices : List ℕ) (acc : Summary k) (np : ℕ)
    (h : sourceScan cap lookup leaf indices acc = .capped np) :
    verdict (sourceScan cap lookup leaf indices acc) = .keep := by
  rw [h]
  rfl

/-- A complete fused type-3 scan supplies the exact count and mask interfaces
to the all-dimensions CWS rejection theorem. The source-specific construction
of `eventOf`, key/table validity, candidate row equations and point coverage
remain visible premises for the caller to discharge.
-/
theorem fused_rejection_sound {R m : ℕ} (C : CWS R (5 + R))
    (L : Layout (5 + R) R) (hw : Wf L) (hcov : Covered L)
    (T : Finset (Fin R → ℕ)) (hT : ∀ t ∈ T, Compatible L t)
    (hne : ∀ t ∈ T, ∀ r, (fibre L r (t r)).Nonempty)
    (hall : ∀ X, IsPoint L X → tupleOf L X ∈ T)
    (hrows : ∀ X, IsPoint L X ↔ ∀ a, ∑ i, C.W a i * (X i : ℝ) = ∑ i, C.W a i)
    (tests : Fin m → Test (5 + R)) (cap : ℕ) (lookup : ℕ → Option ℕ)
    (indices : List ℕ) (eventOf : ℕ × ℕ → LeafEvent (5 + R) R)
    (events : List (LeafEvent (5 + R) R)) (out : Summary m)
    (hEventsScan : events = (matchedPairs indices lookup).map eventOf)
    (hsource : sourceScan cap lookup (fun pair => eventSummary L tests (eventOf pair))
      indices (emptySummary m) = .complete out)
    (hnodup : (events.map Prod.fst).Nodup)
    (hEventsT : (events.map Prod.fst).toFinset = T)
    (hvalues : ∀ e ∈ events, Values L e.1 e.2)
    (hreject : verdict (.complete out) ≠ .keep) :
    ¬ C.IsIP := by
  have hcount := complete_count_eq_index L tests cap lookup indices eventOf events out
    hEventsScan hsource T hnodup hEventsT
  have hsourceMasks := complete_masks_eq_accumulated L tests cap lookup indices eventOf
    events out hEventsScan hsource
  have haccumulated := (accumulated_masks_exact L hcov hw T hT hne hall events hnodup
    hEventsT hvalues (List.ofFn tests)).1
  have hmasks : List.ofFn (fun i => (out.lower i, out.upper i)) =
      functionalSides L ((List.ofFn tests).map Test.coefficients) :=
    hsourceMasks.trans haccumulated
  have hreject' : leafVerdict out.count []
      (List.ofFn (fun i => (out.lower i, out.upper i))) ≠ .keep := by
    simpa [verdict] using hreject
  exact leaf_spec_rejection_sound C L hw hcov T hT hall hrows out.count
    (List.ofFn (fun i => (out.lower i, out.upper i)))
    ((List.ofFn tests).map Test.coefficients) hcount hmasks hreject'

end Refpoly.JoinType3
