# Mathematical goal and proof organization

## Goal and source roles

The long-term goal is a terminating, complete Kreuzer-Skarke WS/CWS-based
enumeration of the target lattice polytopes in every fixed dimension, including
the necessary lattice realizations and subpolytope searches. This is a proposed
mathematical endpoint, not a claim that the frozen Lean package or current native
pipeline already proves or implements it.

[SINGLE_WS_5d.tex](SINGLE_WS_5d.tex) is the author's separate paper on the
single-weight-system part of the five-dimensional classification programme.
It is not the official manuscript for this development. Its rough proof chain
and references are useful for finding primary sources and cross-checking claims;
they are not the specification or proof-status authority for the new work.
The literature notes must verify the primary sources and the exact Lean types.
Editing this separate paper is not implicit in a literature-review assignment.

The user's proposed chain is: finiteness for fixed dimension and interior-point
count; finitely many lattice-equivalence classes; minimal/maximal objects under
inclusion; recovery from maximal containers by subpolytope enumeration; reversal
by polarity; finite minimal-seed structures; inner/outer containers; their KS
construction; complete WS/CWS generation; and recovery of all target polytopes
with lattice refinements accounted for.

## Definitions that must precede the chain

- Fix full dimension, the ambient lattice, a chosen interior origin and the
  equivalence relation. For unmarked polytopes, translations belong to affine
  unimodular equivalence. With a unique interior lattice point, translating it
  to zero gives the origin-fixed GL(d,Z) formulation. For k > 1, an interior
  point marking or an explicit translation convention is necessary.
- Distinguish the IP property (origin in the interior) from having exactly one
  interior lattice point. The k = 1 finiteness route covers one-point lattice
  polytopes, including reflexive ones. An unrestricted IP property does not
  bound the number of interior lattice points, so does not give finiteness in
  fixed dimension by itself. Record which meaning each theorem uses.
  If the endpoint instead includes arbitrary positive interior-point counts,
  formulate a terminating enumeration for each (d,k); their union need not be
  a finite list for fixed d.
- Fix the meaning of minimal: removing vertices, removing lattice points,
  inclusion-minimal in a stated class, or r-minimal among reflexive polytopes.
  These are different predicates and cannot be exchanged without a lemma.
- Distinguish positive real-linear equivalence, forward integrality and an
  actual integral lattice isomorphism. Record the lattice with each container.

## Suggested dependency structure

1. **Finiteness and effective bounds.** For fixed d and k >= 1, establish a
   volume/box bound and finiteness up to the specified lattice equivalence.
   Then distinguish an existential bound from a computable bound that can
   support a terminating enumeration. Hensley's general positive-interior-point
   setting is a primary starting point; general box/equivalence results require
   their own verified references and hypotheses.
2. **Extremal reduction and polarity.** State the class and embeddings used for
   inclusion. Establish existence of appropriate minimal/maximal representatives
   and finite subpolytope recovery. In the reflexive class, polarity reverses
   inclusion and stays in lattice polytopes. General one-point lattice polytopes
   can have nonintegral polars, so their completeness route needs a separate
   argument or a proved rational-dual extension.
3. **Minimal seed geometry.** Prove decomposition into positive simplex circuits,
   bounds on vertices/supports and a finite, exhaustive structural enumeration
   for each dimension. Finite does not mean computationally small. Distinguish
   these seeds from r-minimal reflexive polytopes and from the existing hard-coded
   descriptor catalogue.
4. **Weights, relations and containers.** Extract normalized weights and a
   complete independent relation basis from the seeds. Construct the relevant
   inner/outer containers and prove the needed containments with explicit
   dimension, rank and lattice hypotheses. Cover finite-index lattice choices
   and their effective enumeration; WS/CWS data alone do not specify every
   realization. A covering container need not itself be a maximal target object.
5. **WS/CWS search correctness.** Prove the actual search rules are well-defined,
   sound, complete and terminating, including branching, pruning and stopping.
   Connect every geometrically required container to generated data with explicit
   reconstruction witnesses. An existential reachable node or noncomputable
   finite catalogue does not establish an executable search's completeness.
6. **End-to-end enumeration.** Compose container generation, lattice realization,
   finite lattice-subpolytope search and exact target predicates. Prove that each
   output is in the target class and every target is attained up to the declared
   equivalence. Treat canonical representatives/deduplication as a separate
   guarantee if the endpoint promises one representative per class. State
   reflexive and general one-point completeness separately.

The organizing theorem should say that, for each fixed dimension, a specified
terminating algorithm returns a finite list of valid lattice polytopes covering
every member of the specified target class up to lattice equivalence. Proving
this algorithm is the KS WS/CWS construction requires the connections above;
generic bounded enumeration alone would not establish that identification.

## Relationship to approved integration work

T-L1 through T-L3 collect the literature supporting this structure. T-A0 compares
its obligations with the frozen Lean declarations using the independent
statement checkers, marking each as supported, conditional, missing or excluded
from the current delivery. T-P0 through T-P9 faithfully port the reviewed theory;
they do not silently add the missing global completeness proofs. T-C0 checks
the implemented descriptor catalogue; T-O0 reviews final proof clarity and
Mathlib reuse without changing statement scope.

The current consolidation delivery remains the implemented descriptor/input-pool
universe on the PALP reference lattice plus the audited abstract proof port.
New lattice-refinement enumeration, subpolytope search and native-algorithm
refinement proofs remain future work requiring separately approved chunks.
The audit must expose their distance from the long-term endpoint rather than
present abstract container coverage as the completed programme.

## Primary starting sources

- Douglas Hensley, _Lattice vertex polytopes with interior lattice points_,
  Pacific Journal of Mathematics 105 (1983), 183–191,
  [primary paper](https://msp.org/pjm/1983/105-1/pjm-v105-n1-p10-s.pdf).
  This addresses positive interior-point counts; the literature review must
  verify the exact bounding statements and subsequent equivalence/box steps.
- Maximilian Kreuzer and Harald Skarke, _Reflexive polyhedra, weights and toric
  Calabi-Yau fibrations_,
  [primary preprint](https://arxiv.org/pdf/math/0001106), especially sections 2–3.
  The minimal-seed and lattice-container strategy provides the reflexive route;
  it must not be assumed to establish the same route for nonreflexive IP objects.

This roadmap records the goal and proposed organization. It is not a completed
literature review, proof audit or authorization to start T-L1.
