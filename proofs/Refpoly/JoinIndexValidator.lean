import Refpoly.JoinLoopSource

/-! Transcription of validate_gjoin_index (src/gpu/join.cuh). Array slices
are lists; the mutable seen vector is a finite set of absolute indices.
Normal return is `some visited`; throwing is `none`. Index words and keys
are naturals with the source's unsigned width bounds supplied by their types
at the interface. The decoded-field loop reads `position` first, followed by
the remaining positions in their original order. -/
namespace Refpoly.JoinIndexValidator

open Refpoly.JoinLoopSource

structure Record where
  target : ℕ
  key : ℕ

def positions (shared position : ℕ) : List ℕ :=
  position :: (List.range shared).filter (· ≠ position)

def RecordOK (keys : ℕ → ℕ) (begin finish shared position : ℕ) (g : Record) : Prop :=
  begin ≤ g.target ∧ g.target < finish ∧
    (∀ output, output < shared →
      field shared output g.key =
        field shared ((positions shared position).getD output 0) (keys g.target)) ∧
    (shared < 4 → g.key < 2^(16*shared))

def After (previous : Option ℕ) (key : ℕ) : Prop :=
  match previous with | none => True | some old => old < key

noncomputable def scan (keys : ℕ → ℕ) (begin finish shared position : ℕ) :
    List Record → Finset ℕ → Option ℕ → Option (Finset ℕ)
  | [], seen, _ => some seen
  | g :: rest, seen, previous => by
      classical
      exact if RecordOK keys begin finish shared position g ∧
          g.target ∉ seen ∧ After previous g.key then
        scan keys begin finish shared position rest (insert g.target seen) (some g.key)
      else none

theorem scan_sound (keys : ℕ → ℕ) (begin finish shared position : ℕ)
    (entries : List Record) (seen visited : Finset ℕ) (previous : Option ℕ)
    (h : scan keys begin finish shared position entries seen previous = some visited) :
    visited = seen ∪ (entries.map Record.target).toFinset ∧
      (entries.map Record.target).Nodup ∧
      (∀ g ∈ entries, RecordOK keys begin finish shared position g ∧
        g.target ∉ seen ∧ After previous g.key) ∧
      entries.Pairwise (fun a b => a.key < b.key) := by
  classical
  induction entries generalizing seen previous with
  | nil =>
    have he : seen = visited := Option.some.inj h
    subst visited
    simp
  | cons g rest ih =>
    rw [scan] at h
    split_ifs at h with hg
    obtain ⟨hout, hnodup, hrecords, hsorted⟩ := ih _ _ h
    obtain ⟨hok, hnew, hafter⟩ := hg
    have hnot : g.target ∉ rest.map Record.target := by
      intro hm
      obtain ⟨r, hr, he⟩ := List.mem_map.mp hm
      have hn := (hrecords r hr).2.1
      simp [he] at hn
    refine ⟨?_, ?_, ?_, ?_⟩
    · rw [hout]
      ext n
      simp only [List.map_cons, List.toFinset_cons, Finset.mem_union, Finset.mem_insert]
      tauto
    · simpa only [List.map_cons, List.nodup_cons] using And.intro hnot hnodup
    · intro r hr
      rcases List.mem_cons.mp hr with rfl | hr
      · exact ⟨hok, hnew, hafter⟩
      · obtain ⟨hrOK, hrnew, hrprev⟩ := hrecords r hr
        refine ⟨hrOK, ?_, ?_⟩
        · intro hm; exact hrnew (Finset.mem_insert_of_mem hm)
        · cases previous with
          | none => trivial
          | some old => exact Nat.lt_trans hafter hrprev
    · apply List.pairwise_cons.mpr
      exact ⟨fun r hr => (hrecords r hr).2.2, hsorted⟩

/-- A successful scan of a full slice is a permutation of its canonical
indices: bounds and distinctness plus equal cardinality give surjectivity. -/
theorem scan_complete (keys : ℕ → ℕ) (begin finish shared position : ℕ)
    (entries : List Record) (visited : Finset ℕ)
    (hlen : entries.length = finish - begin)
    (h : scan keys begin finish shared position entries ∅ none = some visited) :
    (entries.map Record.target).toFinset = Finset.Ico begin finish := by
  classical
  obtain ⟨_, hn, hr, _⟩ := scan_sound keys begin finish shared position entries ∅ visited none h
  apply Finset.eq_of_subset_of_card_le
  · intro n hnmem
    obtain ⟨g, hg, rfl⟩ := List.mem_map.mp (List.mem_toFinset.mp hnmem)
    exact Finset.mem_Ico.mpr ⟨(hr g hg).1.1, (hr g hg).1.2.1⟩
  · simp [List.toFinset_card_of_nodup hn, hlen]

noncomputable def validateIndex (keys : List ℕ) (entries : List Record)
    (begin finish shared position : ℕ) : Option (Finset ℕ) := by
  classical
  exact if begin ≤ finish ∧ finish ≤ keys.length ∧ finish ≤ entries.length ∧
      2 ≤ shared ∧ shared ≤ 4 ∧ 1 ≤ position ∧ position < shared then
    scan (fun i => keys.getD i 0) begin finish shared position
      ((entries.drop begin).take (finish - begin)) ∅ none
  else none

theorem validateIndex_complete (keys : List ℕ) (entries : List Record)
    (begin finish shared position : ℕ) (visited : Finset ℕ)
    (h : validateIndex keys entries begin finish shared position = some visited) :
    (((entries.drop begin).take (finish - begin)).map Record.target).toFinset =
      Finset.Ico begin finish := by
  classical
  unfold validateIndex at h
  split_ifs at h with hg
  exact scan_complete _ _ _ _ _ _ _ (by simp only [List.length_take, List.length_drop]; omega) h

end Refpoly.JoinIndexValidator
