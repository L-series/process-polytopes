# Mathematical goal and proof organization

## Goal and source roles

The approved mathematical goal is a terminating, sound and complete
Kreuzer-Skarke WS/CWS-based enumeration of **reflexive lattice polytopes in every
fixed dimension**, including compatible lattice realizations and subpolytope
recovery. Classification of nonreflexive IP targets is excluded. IP weight
systems and possibly nonreflexive IP containers remain necessary intermediates;
they cannot be discarded merely because they are not final reflexive outputs.

The [proof-contract scaffold](../../proofs/README.md) formalizes the major
interfaces and proves their conditional composition. Supporting proofs and
scientific routine bodies remain holes; the closed instantiated headline is
not yet established. The strict completeness job is intentionally red until
those dependencies and executable compilation are closed.

[SINGLE_WS_5d.tex](SINGLE_WS_5d.tex) is the author's separate paper on the
single-weight-system part of the five-dimensional classification programme.
It is not the official manuscript for this development. Its rough proof chain
and references are useful for finding primary sources and cross-checking claims;
they are not the specification or proof-status authority for the new work.
The literature notes must verify the primary sources and the exact Lean types.
Editing this separate paper is not implicit in a literature-review assignment.

The chosen constructive route is minimal dual seeds, bounded circuit supports,
complete weight generation, exhaustive CWS assembly, finest-lattice containers,
all-lattice-point subpolytope candidates, finite compatible lattice intervals,
exact reconstruction and reflexivity acceptance. The existing reflexive
finiteness/maximality results remain supporting theory, not a substitute for
the KS reconstruction chain.

## Definitions that must precede the chain

- Fix full dimension, the ambient lattice and the interior origin. Reflexive
  polytopes have a unique interior lattice point; centering it gives the
  origin-fixed GL(d,Z) formulation. Different lattice realizations must still
  be reconstructed and recorded before choosing integer-coordinate bases.
- Distinguish origin-IP, one-point and reflexive predicates. IP does not itself
  imply reflexivity, and arbitrary origin-IP polytopes have no finite
  fixed-dimension classification. They are not an additional final target.
- Fix the meaning of minimal: removing vertices, removing lattice points,
  inclusion-minimal in a stated class, or r-minimal among reflexive polytopes.
  These are different predicates and cannot be exchanged without a lemma.
- Distinguish positive real-linear equivalence, forward integrality and an
  actual integral lattice isomorphism. Record the lattice with each container.

## Suggested dependency structure

1. **Duality and bounded minimal seeds.** For a reflexive target, extract a
   minimal origin-IP seed of dual lattice points. In positive dimension prove
   $d+1\le N\le2d$; handle dimension zero separately. The seed is not the same
   predicate as r-minimal reflexivity.
2. **Complete circuit relations.** Extract independent positive simplex rows
   spanning the entire relation kernel, with $r=N-d\le d$ and support lengths
   at most $d+1$. Retain exact vertex/support/weight and normalized zero-relation
   witnesses. Conservative overgeneration is acceptable; the archived
   47-entry computation is not the general geometric completeness certificate.
3. **Finite executable WS/CWS generation.** Implement positive rational
   selection, bounded branching and rank-based stopping, with exact primitive
   IP acceptance. Assemble every bounded independent covering row family from
   generated supports. Do not assume an empirical degree cutoff or interpret
   abstract reachability/noncomputable finiteness as an executable list.
4. **Geometric containers and finest lattices.** Identify the integer kernel
   lattice with a computed basis and prove target containment via a
   forward-integral real bijection. Preserve the target's image lattice;
   forward integrality is not inverse integrality. Containers need not be
   reflexive or maximal.
5. **Subpolytopes and compatible lattice intervals.** Enumerate subsets of
   every container lattice point, not only its vertices. For origin-IP
   candidates with integral vertex-pairing matrix, prove finiteness of lattices
   between the vertex-generated coarse and polar-defined fine endpoints.
   Reconstruct all of them; unrestricted sublattice enumeration is not finite.
6. **Exact acceptance and global composition.** Prove candidate/reflexivity
   checks, lattice reconstruction and two-sided equivalence correct. Compose
   them into a total executable list covering every reflexive class. Exact
   deduplication is a separate guarantee; output duplicates are initially
   allowed.

The organizing theorem should say that, for each fixed dimension, a specified
terminating algorithm returns a finite list of valid lattice polytopes covering
every member of the specified target class up to lattice equivalence. Proving
this algorithm is the KS WS/CWS construction requires the connections above;
generic bounded enumeration alone would not establish that identification.

## Relationship to approved integration work

T-L1 through T-L3 supplied the literature. The frozen source's
[T-A0 tracked synthesis](../proof-audit.md) is complete, with source/contract
mappings, audit dispositions and a revised dependency ledger. Clean donor
blind-review closure remains outstanding because the historical round-2
context received excluded repository metadata. The synthesis closes no Lean
proofs or routines. The separately approved first proof-CI chunk adds
generic contracts, conditional composition and strict/conditional gates, with
fresh scoped statement checks; it does not discharge that donor isolation debt.

The complete `Basic` and `ReflexiveDuality` modules are now faithfully ported
and imported, with 26 additional donor theorem bodies covered by the existing
compiler-derived axiom audit. This supplies foundations and reflexive dual
closure; it closes none of the eighteen KS contracts or seven routines.
The complete faithful `WeightSystem` and `BoxReduction` ports add 30 further
results and their supporting definitions. The component-IP lifting hypothesis
and existential/noncomputable box radius remain explicit; these ports do not
supply effective search or lattice reconstruction.

The faithful `Algorithm`, `WeightSystemFinite` and `GeometricCWS` ports add 21
results and supporting definitions. Reachability remains abstract. The
normalized-IP finiteness proof uses positive-feasibility-guarded nodes and
classical selection; neither supplies executable search. Geometric simplex
IP/reachability retains the explicit affine independence, normalized positive
zero relation and dual
lattice-point witnesses, including lower-dimensional blocks. All three modules
are imported and compiler-audited; no KS obligation or routine is closed.

The complete faithful `CircuitGeometry`, `CircuitInterior` and `CircuitSpan`
ports add 24 theorem/lemma bodies, including the private boundary helper.
Positive simplex circuits span the entire real relation kernel under the
specified intrinsic-interior hypothesis. All three modules are imported and
compiler-audited. Primitive integer normalization and the KS circuit contract
remain open; this port closes no KS obligation or scientific routine.

The faithful volume/finiteness and minimal-seed ports add `ReflexiveVolume`,
`Finiteness`, `MaxMin`, `Minimal` and `Classification`, with 35 source results.
The weight-finiteness → volume → box/finiteness dependency remains noncircular.
Bounds are existential and reflexive-only. Minimal structure gives one
step, without the new $2d$ seed bound. Legacy simplex signatures still omit
seed-to-weight witness connections; stronger geometric witnesses are required
for the new contracts. All modules are imported and compiler-audited; no KS
obligation or routine is closed.

The complete faithful `CWSBasis`, `CWSContainer` and `CWSAssembly` ports add
20 source results. They provide independent complete real equations, coordinate
coverage, explicit reached/padded blocks and forward-integral containers from a
finite abstract catalogue. Container maps do not prove inverse integrality;
noncomputable catalogues do not implement primitive integer search or lattice
realizations. All units 3 and 4 are now ported and compiler-audited. The eighteen
KS proof obligations and seven routines remain open. Unit 5 is next and needs
separate approval; clean donor blind-review closure remains outstanding.

Subsequent approved chunks port the remaining applicable source proofs and implement
the missing search/lattice/recovery obligations. Keep faithful extraction,
statement repair and new behaviour separate. T-C0 still concerns the implemented
descriptor catalogue and does not prove global geometric completeness. Final
optimization preserves the eventual audited statements and pins.

The current consolidation delivery remains the implemented descriptor/input-pool
universe on the PALP reference lattice plus the audited abstract proof port.
The new proof scaffold specifies lattice-refinement and subpolytope recovery;
their implementation/proofs, and native-algorithm refinements, remain future
work requiring separately approved chunks.
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

Preserve the noncircular supporting dependency direction: finite weights,
reflexive volume bound, box reduction, then reflexive finiteness. A generic
bounded-box enumeration alone would not identify the algorithm as KS.
This roadmap is not a claim that the global theorem has been proved.
