import Refpoly.KS.Model

namespace Refpoly.KS

def candidates (p : Procedures) (d : ℕ) : List (PolyCode d) :=
  (p.assemble d p.weights).flatMap fun C =>
    (subpolytopes (p.points C)).flatMap fun Q =>
      if p.eligible Q then p.realize Q else []

def enumerate (p : Procedures) (d : ℕ) : List (PolyCode d) :=
  if d = 0 then [originCode d] else (candidates p d).filter p.reflexive

def Classifies (p : Procedures) : Prop :=
  ∀ d, (∀ R ∈ enumerate p d, IsReflexive R.hull) ∧
    ∀ P : Set (V d), IsReflexive P →
      ∃ R ∈ enumerate p d, GLEquiv P R.hull

end Refpoly.KS
