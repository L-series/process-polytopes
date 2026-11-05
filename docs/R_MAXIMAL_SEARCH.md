# A lattice-complete, maximal-only search

## Result and limitations

There is a deterministic, terminating reduction from the supplied WS/CWS
containers to exactly the r-maximal reflexive polytopes. It need not enumerate
the reflexive census: **stop as soon as a search node is reflexive**, and
branch only at a nonunit facet. Lattice changes must be enumerated separately,
including changes that remove vertices and change the integer hull.

The accompanying reference experiment reproduces **340 distinct r-maximal
four-polytopes from all 201,346 WS/CWS records**, without generating the
473,800,776-polytope census. The seven changed-hull outputs agree exactly, up
to lattice normal form and polarity, with Table 8 of [KS-classification].

This is **not yet an established practical solution for twenty billion
five-dimensional containers**. The unrestricted algorithm below is complete,
but its lattice enumeration and nonreflexive residual search can still be
prohibitive. The 4D experiment uses an explicitly truncated discovery pass,
plus complete unchanged-shape lattice intervals; it is a historical regression,
not an exhaustive execution of the general algorithm or a new independent
proof that the 4D list is complete. Section 9 records bounded profiling of
the subsequently identified 5D WS datasets; no full 5D maximal search has
been performed.

## 1. Input contract and lattices

Use the polarity convention

$$
P^*=\{y\in N_{\mathbb R}:\langle y,x\rangle\geq-1
                         \text{ for every }x\in P\}.
$$

The original target lattice is $M$, with dual $N=M^\vee$. A dataset record is
a full-dimensional origin-IP lattice polytope $Q$ in a specified reference
lattice $\Lambda\cong\mathbb Z^d$. For a CWS record, $\Lambda$ is the saturated
integer kernel lattice, usually the finest lattice associated with its seed.
Write $Q^*\subset\Lambda^\vee_{\mathbb R}$ for its rational polar. Neither
$Q^*$ nor $Q$ is assumed reflexive.

The required coverage hypothesis is:

> For every reflexive $(P,M)$ there are a record $(Q,\Lambda)$ and a real
> linear isomorphism $F:M_{\mathbb R}\to\Lambda_{\mathbb R}$ such that
> $F(M)\subseteq\Lambda$ and $F(P)\subseteq Q$.

In general, $F(M)$ is a **proper finite-index sublattice**. This is precisely
why reference-lattice subpolytopes alone do not suffice. The production branch
is named `production/cws-5d` in this checkout. Its
[`STATEMENTS.md`](../proofs/STATEMENTS.md) distinguishes `geometric_containers`,
`target_lattice`, and `realizations_complete`; it does not assert inverse
integrality for every container embedding.

The supplied dataset's completeness is an assumption here. The Lean theorem
does not independently certify a native twenty-billion-record file. Records
must retain their reference-lattice coordinates; deduplication by rational
equivalence or vertex-pairing matrices alone is not lattice equivalence.

An r-maximal polytope is maximal among reflexive polytopes **on the same
lattice**. By polarity it is equivalent to an r-minimal dual. IP-minimal seeds
and r-minimal reflexive polytopes are different notions.

## 2. A finite lattice enumeration before any subpolytope search

For a fixed record set

$$
S=Q\cap\Lambda,\qquad
D(Q)=\max_{v_1,\ldots,v_d\in\operatorname{Vert}(Q)}
                  |\det_\Lambda(v_1,\ldots,v_d)|.
$$

Distinct vertices suffice; dependent tuples contribute zero.

**Lemma 1 (index bound).** If $L\subseteq\Lambda$ and
$\operatorname{conv}(Q\cap L)$ is full dimensional, then
$[\Lambda:L]\leq D(Q)$.

**Proof.** Choose linearly independent $x_1,\ldots,x_d\in Q\cap L$. Their
integer span $K$ satisfies

$$
0<|\det_\Lambda(x_1,\ldots,x_d)|
  =[\Lambda:L][L:K].
$$

The absolute determinant on $Q^d$ attains its maximum at a tuple of vertices:
fix all arguments but one, and maximize the absolute value of a linear
functional on $Q$, successively for each argument. Consequently the displayed
determinant is at most $D(Q)$. This proves the bound. $\square$

Enumerate every column Hermite normal form $H$ with
$1\leq\det H\leq D(Q)$. Our convention is upper triangular,
$H_{ii}>0$, and $0\leq H_{ij}<H_{ii}$ for $i<j$. Its lattice is
$L=H\mathbb Z^d$. Work thereafter in actual $L$-coordinates:

$$
S_H=\{H^{-1}x:x\in S,\ H^{-1}x\in\mathbb Z^d\},\qquad
Q_H=\operatorname{conv}(S_H).
$$

Discard a non-IP $Q_H$: every subsequent subpolytope is non-IP as well.
This enumerates finitely many lattices **without first choosing a
subpolytope**. It is an alternative to the existing, also correct
polytope-first compatible-lattice interval.

The bound is a completeness device, not a practical estimate. In particular,
enumerating all HNFs to this bound for every large root is not the proposed
production fast path.

**Why the unchanged-vertex interval is insufficient.** For a chosen real
polytope $C$ with integral vertex-pairing matrix, the compatible lattices form
the finite interval

$$
\langle\operatorname{Vert}(C)\rangle_{\mathbb Z}
\ \subseteq L\subseteq\
\langle\operatorname{Vert}(C^*)\rangle_{\mathbb Z}^{\vee}.
$$

This interval exhausts reflexive realizations of **that real shape**, not all
reflexive integer hulls obtained by restricting the lattice of its container.
A target may omit some vertices of $C$, so its image lattice need not contain
the left endpoint. Enumerating all those intervals only for the original
maximal shapes misses the historical last seven.

## 3. Covering reflexive subpolytopes, not enumerating them

Fix a lattice once and for all in this section. A node consists of all lattice
points $T=C\cap\mathbb Z^d$ of its current hull $C$.

**Lemma 2 (reflexive stopping rule).** If $C$ is reflexive, returning $C$
covers every reflexive subpolytope of $C$. If an r-maximal target $P$ lies in
$C$, then $P=C$.

This elementary rule is the main difference from a full census. It remains
valid even if $C$ is not globally maximal: in that case none of its proper
subpolytopes can be globally maximal either.

For a nonreflexive IP lattice hull choose a facet

$$
F=\{x:\langle a,x\rangle=-h\},\qquad
a\in(\mathbb Z^d)^\vee\text{ primitive},\quad h>1.
$$

Choose **only $d$ affinely independent vertices**
$B=\{b_1,\ldots,b_d\}$ on this facet. Since its hyperplane avoids zero,
these are also linearly independent.

**Lemma 3 (bad-facet obstruction).** Every reflexive $P\subseteq C$ omits at
least one point of $B$.

**Proof.** If it contained all of $B$, then $P\cap F$ would have dimension
$d-1$. Thus $F$ would also be a facet of $P$, with the same primitive
normal $a$ and lattice distance $h>1$. This contradicts reflexivity.
Equivalently, $a/h$ would be a nonintegral polar vertex of $P^*$. $\square$

Branch into the at most $d$ hulls

$$
C_i=\operatorname{conv}(T\setminus\{b_i\}).
$$

If $b_i\notin P$, every vertex of $P$ belongs to $T\setminus\{b_i\}$, so
$P\subseteq C_i$. These branches therefore cover every reflexive target.
Since each $b_i$ is an actual vertex of $C$, the lattice points of $C_i$ are
exactly $T\setminus\{b_i\}$: no missing nonvertex needs to be reinserted.
Memoization handles overlap; a kept/deleted partition can avoid duplicates
but is not needed for correctness.

```text
Cover(T):
    C := exact hull(T)
    if C is not origin-IP: return {}
    if C is reflexive: return {C}
    choose a nonunit facet and a deterministic d-vertex frame B
    return union over b in B of Cover(T minus {b})
```

Every recursive edge decreases $|T|$. Lemmas 2 and 3 prove by induction that
every reflexive subpolytope is contained in some returned reflexive leaf.
Choosing the lexicographically first normalized bad facet and first
independent frame makes the routine deterministic.

This refines the bad-facet deletion already present in
`PALP/Subpoly.c:Make_All_Subpolys`; it is not a claim that bad-facet pruning
itself is new. In five dimensions the frame gives five children rather than
all vertices on the bad facet.

## 4. Complete maximal-only algorithm

```text
for every input (Q, reference lattice Lambda):
    compute S and D(Q)
    for every HNF H with 1 <= det(H) <= D(Q):
        compute S_H in H-lattice coordinates
        for C in Cover(S_H):
            deduplicate by exact origin-fixed GL(d,Z) normal form
            retain C iff C* has no proper reflexive subpolytope
return the retained normal forms
```

The last step is an **existence test on the dual**, not enumeration or storage
of all descendants. Its exact implementation can use the existing PALP
r-minimality search or the same covering routine: delete each vertex of
$C^*$ from its full lattice-point set, and ask whether any resulting hull
has a reflexive leaf. Any proper subpolytope misses an original vertex, so
this test is complete. Stop on the first witness. Absence of witnesses is
the maximality certificate. The dual need not always be small; that is an
empirical optimization, not a hypothesis of the proof.

**Theorem (soundness and completeness).** Assuming the stated container
coverage and correct exact hull, lattice, normal-form and reflexivity
primitives, this finite algorithm outputs exactly the r-maximal reflexive
polytopes up to origin-fixed lattice equivalence.

**Proof.** Every retained output is reflexive and passes the equivalent
dual r-minimality test, hence is r-maximal.

Conversely, let $(P,M)$ be r-maximal. Choose $Q,F$ by coverage, and put
$L=F(M)$. Its index is bounded by Lemma 1 because $F(P)$ is full
dimensional and has vertices in $Q\cap L$. The enumeration includes its HNF
$H$. The map $H^{-1}F$ is now a lattice isomorphism from $M$ to
$\mathbb Z^d$, not merely a forward-integral real map. Its image $P'$ is
reflexive and r-maximal, and lies in $Q_H$.

The covering induction returns a reflexive $C\supseteq P'$. Maximality
forces $C=P'$. It survives the final test and normal-form deduplication.
Finally, every reflexive polytope has an r-maximal reflexive superset
([`exists_maximal_superset`](../proofs/Refpoly/MaxMin.lean)), so the outputs
also have the requested covering property. $\square$

No automatic IP-to-reflexive implication is used. In particular, a
nonreflexive 5D container cannot be discarded just because it is not an output.

## 5. Production reduction: certify the easy majority first

The unrestricted algorithm is an exact fallback, not a recommendation to
expand twenty billion independent HNF trees.

### 5.1 Reduce roots with a lattice-transport certificate

If there is a unimodular map $A:\Lambda_Q\to\Lambda_R$ with
$A(Q)\subseteq R$, discard $Q$ **provided $R$ will be processed with its
entire lattice-descendant search**. Every image lattice $L\subseteq\Lambda_Q$
transports to $A(L)\subseteq\Lambda_R$, and every reflexive target is preserved
with its actual lattice. Therefore this root reduction is lattice complete.
Chains of such inclusions give a finite coverage DAG; break equal-volume
equivalences by canonical representative.

Do not substitute the weaker rule "the fine-lattice $R$ is reflexive, so all
its sublattice targets are covered by this one realization." That is false.
Retain the lattice-descendant obligation at the surviving root.

A nonsingular integral embedding also gives a valid transport, with an
additional index factor, but unimodular certificates are simpler.

### 5.2 Search containments on the dual side

For a known reflexive candidate $R$ and fixed-lattice node $C$,

$$
C\subseteq A(R)\quad\Longleftrightarrow\quad
A^{-t}(R^*)\subseteq C^*,\qquad A\in\mathrm{GL}(d,\mathbb Z).
$$

The right side often involves many fewer lattice points than $C$. Fix
linearly independent integer vertices $u_1,\ldots,u_d$ of $R^*$. For each
ordered independent tuple $y_1,\ldots,y_d\in C^*\cap\mathbb Z^d$, form

$$
B=(y_1\ \cdots\ y_d)(u_1\ \cdots\ u_d)^{-1}.
$$

Require integer entries, determinant $\pm1$, and the exact inequalities
$\langle B u,v\rangle\geq-1$ for every vertex $u$ of $R^*$ and vertex $v$
of $C$. A successful $B$ is a checkable dominance certificate with
$A=B^{-t}$. This finite search is complete for unimodular containment of
$R^*$ in $C^*$; it does not just compare the two lists of vertices.

For nonreflexive CWS roots $R$, an integer frame in $R^*$ may be taken from
the seed, or from the integer polar points; all rational polar vertices must
still pass the final inequalities. A CWS seed supplies full span. This
extension must not assume arbitrary rational IP polars have spanning integer
points.

At a fixed lattice node, a successful containment in an already retained
reflexive $R$ discharges its whole maximal-only search. For root removal
across varying lattices, use the transport rule of 5.1 instead.

### 5.3 Deterministic work order and stopping criterion

Build an initial reflexive family from inexpensive records, minimizing
observed dual lattice-point and vertex counts, with normal-form tie breaks.
Use it to produce inclusion witnesses for the remaining records. Container
point counts alone do not establish coverage, and a small-vertex family is
not complete until the residual obligations are discharged.

Process each stream shard with exact canonical keys and successful witnesses.
Unsuccessful heuristic searches stay unresolved. Reduce surviving roots,
process their lattice descendants, and run bad-facet search only on their
nonreflexive residual hulls. New maximal candidates improve later dominance
tests; no candidate-dependent assumption enters the completeness proof.

Prime-index sublattice recursion with HNF deduplication is another exact
enumeration strategy. If an intermediate integer hull is non-IP, all its
sublattice descendants can be pruned. If it is reflexive, stop its **fixed
lattice** subpolytope search, not its lattice-descendant traversal.

Completion requires zero unresolved root, lattice and bad-facet obligations.
A plateau in the number of discovered maximal polytopes is not a stopping
certificate.

### 5.4 What must be measured before a 5D production commitment

The critical quantities are the number of surviving roots, the lattice
indices and quotient groups encountered, the dual-point counts used in
containment tests, and the number and depth of residual nonunit-facet nodes.
The supplied input size alone determines none of them.

There is no proved polynomial or subexponential runtime here. A memoized
bad-facet search can still visit exponentially many point subsets, and the
number of bounded-index HNFs grows rapidly. Thus an unrestricted worst-case
run may remain infeasible even though it avoids expanding every reflexive
node. Claiming a feasible 20B-container census before measuring this residual
would be unjustified.

## 6. Reproducible implementation and 4D experiment

[`scripts/rmax_search.py`](../scripts/rmax_search.py) implements the finite
HNF bound, exact lattice coordinates, frame branching, early reflexive
stopping, normal forms and final dual maximality checks. It is a standard
library Python **reference implementation**, using the existing PALP binaries
as geometry oracles. It does not implement the production coverage DAG,
dual containment index, external-memory scheduling, or a certified
arbitrary-precision hull backend.

For a small complete run, omit `--max-index`:

```bash
python3 scripts/rmax_search.py \
  --poly PALP/poly-5d.x --class PALP/class-5d.x input.cws
```

Alternatively `--format jsonl` accepts one object per line with a `vertices`
array of integer coordinate vectors in the record's reference-lattice basis.
Outputs are JSON Lines with canonical vertices, source vertices, root record
number and HNF lattice basis. The summary records whether lattice enumeration
was truncated. An explicit `--max-index` is **experimental and incomplete**;
finding no new outputs below that cap proves nothing about larger indices.

Run the historical regression with a new artifact directory:

```bash
python3 scripts/validate_rmax4d.py --work-dir /tmp/rmax4d-validation
python3 -m unittest discover -s tests -p test_rmax_search.py -v
(cd proofs && lake build Refpoly.MaximalSearch)
```

The regression executes these stages, not an import of the answers:

| Stage                                                                            | Input                               | Result                        |
| -------------------------------------------------------------------------------- | ----------------------------------- | ----------------------------- |
| Dual r-minimality filter                                                         | 184,026 WS + 17,320 CWS             | 308 r-minimal CWS             |
| All compatible lattices of the unchanged 308 shapes                              | PALP `-sv -mr`                      | Union has 333 maximal classes |
| Recompute integer hulls at every index-2 and index-3 sublattice of the 308 roots | 17,248 HNF nodes, including index 1 | Seven further shapes          |
| Canonical union and reflexivity/maximality recheck                               | All discoveries                     | 340 distinct classes          |

PALP's unchanged-shape binary export contains 175 maximal classes, including
150 of the original 308. Its union with the originals is 333, not
$308+175$. In the bounded-index pass, 11,729 hulls are non-IP and 5,519 are
reflexive; there are no bad-facet nodes in that 4D pass. It finds 334 maximal
classes on its own. Its union with the unchanged-shape stage gives 340.

The seven new witnesses occur at:

| Single weight system | Image-lattice index |
| -------------------- | ------------------- |
| $(1,1,1,1,1)$        | 2                   |
| $(1,1,1,1,2)$        | 2                   |
| $(1,1,1,1,3)$        | 2 and 3             |
| $(1,1,1,1,4)$        | 3                   |
| $(1,1,2,2,3)$        | 2                   |
| $(1,1,2,2,4)$        | 2                   |

The comparison coordinates in `validate_rmax4d.py` are the **duals** in
Table 8. They are used only after discovery to check equality, never as
search seeds or injected outputs.

Input SHA-256:

```text
b8c20654d50bc1bc78c8b1c2bca794f6cabc7bb02e952771dd8b73dfee9c97ee  docs/data/single-weight-systems/4d.txt
4662a36eed52a943992fb6b2f97e6bb54c9da5c5ad651aa3e82128f85b6429fd  tests/fixtures/cws/4d.cws
```

PALP commit: `ccc8e5e3f96ab2d424649f4bed3707a206c50bd2`.
The generated `summary.json` also records the canonical 340-record output
hash and elapsed times. The recorded run took 213.970 seconds, including
134.369 seconds for the initial WS/CWS reduction. Its output SHA-256 is
`b16528fe9471388b26b3877a193b7f16fb13dde46be176e4fa0856756efcbb1c`.
These timings are not a 5D scaling estimate. Downloaded paper files and
intermediate binaries are not repository inputs.

## 7. A 5D obstruction and an explicit maximal descendant

The WS $(1,1,1,1,1,2)$ of degree 7 gives the following container in
$M=\mathbb Z^5$:

$$
Q=\{x:x_i\geq-1\ (1\leq i\leq5),\
        x_1+x_2+x_3+x_4+2x_5\leq1,\ x_5\leq2\}.
$$

It is the integer hull of the same inequalities without the last bound.
Its two vertex layers have $x_5=-1$ and $x_5=2$, with five vertices each.
It is IP but not reflexive: the last facet has primitive normal $-e_5$
and distance two. Its polar is

$$
Q^*=\operatorname{conv}(e_1,\ldots,e_5,a,-e_5/2),
\qquad a=(-1,-1,-1,-1,-2).
$$

Put $S=\operatorname{conv}(e_1,\ldots,e_5,a)$. Directly,

$$
Q^*\cap N=\{0,e_1,\ldots,e_5,a\},\qquad [Q^*]_N=S.
$$

For example, every first-four coordinate lies in $[-1,1]$. A coordinate
equal to $1$ forces the corresponding $e_i$, and a coordinate equal to
$-1$ forces $a$. Otherwise all four integer coordinates are zero, and the
last coordinate lies between $-1/2$ and $1$, leaving only $0,e_5$.
Every IP lattice subpolytope of $S$ must contain all six nonzero points:
omitting any of them puts the origin on a supporting coordinate hyperplane.
But $S$ is not reflexive, since $S^*$ has the nonintegral vertex
$(-1,-1,-1,-1,5/2)$.

**Consequently $Q$ is contained in no reflexive polytope on this lattice.**
Indeed, if $Q\subseteq R$ with $R$ reflexive, then
$R^*\subseteq Q^*$, and integrality of $R^*$ gives $R^*\subseteq S$,
contradicting the preceding observation.

This rules out a universal strategy that merely tries to fit every
nonreflexive container into a family of known reflexive maximal containers,
even if that family were already complete. Such a family covers the
reflexive descendants, **not necessarily the container itself**.

Nevertheless a single unit-layer cut gives

$$
P=Q\cap\{x_5\leq1\}.
$$

This is not just reflexive; it is **r-maximal** on $M$. Its ten vertices are
the five points at $x_5=-1$ whose first four coordinates are all $-1$ or
have one coordinate $6$, and the five points at $x_5=1$ whose first four
coordinates are all $-1$ or have one coordinate $2$. All its defining
facet distances are one, and

$$
P^*=\operatorname{conv}(e_1,\ldots,e_5,a,-e_5).
$$

The lattice points of $P^*$ are exactly those seven vertices and zero.
The same coordinate argument applies; on the last coordinate axis the
intersection is now $[-e_5,e_5]$. Every IP lattice subpolytope still must
contain $e_1,\ldots,e_5,a$. Omitting $-e_5$ leaves the nonreflexive $S$;
including it gives all of $P^*$. Hence $P^*$ is r-minimal and $P$ is
r-maximal. This is a fixed-lattice argument, with no change of $M$ or $N$.

The integer-point counts follow from the layer formula

$$
|Q\cap M|=\sum_{z=-1}^{2}\binom{9-2z}{4}
         =330+126+35+5=496,\qquad
|P\cap M|=491.
$$

PALP independently confirms these hulls, point counts and the global
maximality of $P$. The reference bad-facet search on $[Q^*]_N$ terminates
with six nodes and no reflexive leaf, confirming the obstruction.

The unpruned reference covering search on $Q$ itself finds a first
reflexive leaf in approximately 0.5 seconds, but did **not** finish its
fixed-lattice covering search within a bounded 60-second experiment.
This is useful negative evidence: early stopping alone is not a demonstrated
practical 5D solution, even at 496 points. Certified dominance and stronger
residual reductions need to be measured, not assumed effective.

## 8. Formalization and verification boundary

[`proofs/Refpoly/MaximalSearch.lean`](../proofs/Refpoly/MaximalSearch.lean)
machine-checks:

- non-IP nodes contain no reflexive target;
- a reflexive node is a sufficient cover;
- covering child branches compose;
- every maximal target in a reflexive cover occurs in that cover;
- a complete reflexive cover can be filtered to the exact maximal set.
- absence of reflexive subpolytopes in $[Q^*]_N$ rules out a reflexive
  superpolytope of $Q$.

It uses the existing definitions of reflexivity and fixed-lattice maximality.
Its axiom closures contain only `propext`, `Classical.choice` and `Quot.sound`.
**The determinant index bound, facet-frame obstruction and lattice transport
are proved mathematically above, not yet formalized in Lean.** Nor does this
file verify Python, PALP, normal forms, the generated catalogue, or a native
5D run. No existing audited classifier contract has been changed.

The focused regressions include a complete 2D maximal search (three outputs),
an exhaustive small nonreflexive example checked against all point subsets,
an index-two changed-hull example, and a 5D nonunit-facet example where one
bad-facet expansion produces five children and only one reflexive leaf.

For independent production certificates, retain exact hull/facet witnesses,
HNF membership maps, the chosen bad-facet frame at each branch, lattice
transport matrices, normal-form equivalences and dual maximality search
certificates. A small checker should verify those artifacts separately from
the discovery engine. The prototype's provenance fields alone are not this
full certificate system.

## 9. Initial profiling of the actual 5D WS catalogues

The measurements below were made on 2026-11-04. The machine exposes 64 CPUs
on an AMD EPYC 9554P. Native kernels were compiled with `-O3` and
`POLY_Dmax=5`. Each phase used at most two workers; some phases overlapped.
These are shared-host, warm-kernel measurements, not isolated-host or
cold-storage benchmarks. The portable results, selected weights and
timeout/cap information are in
[`ws-rmax-profile-20261104.json`](data/ws-rmax-profile-20261104.json).

### 9.1 Inventory and sampling

Summing Parquet footer row counts verifies exactly:

- 11,469,405,637 nonreflexive IP WS records, in 21 files;
- 2,377,115,412 reflexive WS records, in one file.

The WS total is 13,846,521,049 records. Approximately one billion additional
CWS records would put the combined input near 14.85 billion, before any
WS/CWS cross-deduplication. The earlier twenty-billion figure was a planning
assumption, not the measured size of these files.

Records contain representative weights and geometry counts, not vertex
coordinates. The profile therefore includes reconstruction in the saturated
kernel lattice. It checks 538 records: 256 random probes per catalogue and
26 additional extreme/bin probes. The random probes use 64 independent
uniform record draws to select row-group clusters, with three further
uniform records in each selected cluster. Population summaries exclude the
deliberately biased probes and account for the clustered sampling.

All 538 reconstructions match their stored point, vertex, facet and
reflexivity data; reflexive dual point counts also match. Footer counting
does not independently validate every record or audit deduplication.

### 9.2 Native primitive costs

The following are means over the random probes, using the median of three
native repetitions after a discarded warm-up:

| Primitive                                | Nonreflexive IP WS | Reflexive WS |
| ---------------------------------------- | -----------------: | -----------: |
| Construct complete primal point set      |           0.098 ms |     0.080 ms |
| Hull/IP/reflexivity decision, all points |           0.301 ms |     0.235 ms |
| Hull/IP decision from vertices only      |           0.035 ms |     0.027 ms |
| Normal form, reusing hull data           |           0.033 ms |     0.027 ms |
| Enumerate complete dual point set        |     not applicable |     8.109 ms |

These are kernel costs, not Python subprocess costs. For example, the
222,426-point nonreflexive extreme takes approximately 3.17 ms for point
construction and 22.42 ms for its full-point hull; its vertex-only hull
takes approximately 0.012 ms. The 355,785-point reflexive extreme takes
approximately 3.13 ms and 10.42 ms respectively. The largest observed dual
enumeration takes approximately 319 ms.

Naively scaling just primal reconstruction and full-point hull checking
gives about 62 single-worker days for the WS inputs. This is not a forecast
for the maximal search: it excludes lattice changes, recursion, maximality,
I/O and orchestration. The sampled mean root costs have nominal
cluster-bootstrap 95% intervals of 0.329--0.474 ms for IP and
0.250--0.397 ms for reflexive records. These intervals do not bound an
unseen rare runtime tail.

### 9.3 Lattice enumeration: inexpensive tests, prohibitive raw multiplicity

All 31 index-two and 121 index-three sublattices were tested for every
sample: 81,776 nonidentity lattice states in total. The exact native filter
uses HNF congruences, changes to actual sublattice coordinates, rejects
obvious non-IP coordinate bounds, then calls the IP oracle when necessary.
Scalar exact-coordinate checks agree on 1,368 states, including the point
extremes.

For random roots, approximately 67% of these states fail the cheap
coordinate test. Only about 1.9% are IP after exact checking. The whole
152-state pass averages 4.48 ms per IP root and 4.35 ms per reflexive root.
It nevertheless implies roughly 702 billion geometry-oracle calls at WS
scale, including original roots, without root reduction. This is a bounded
index experiment, not a complete lattice search.

The literal determinant-bound loop is decisively infeasible. Exact bounds
for the two primal point extremes are:

| Root                    |  Points | Exact $D(Q)$ |
| ----------------------- | ------: | -----------: |
| Nonreflexive WS extreme | 222,426 |    2,038,071 |
| Reflexive WS extreme    | 355,785 |    3,261,636 |

At index $n$, at least $n^4$ five-dimensional HNFs occur. Consequently

$$
H_5(D)\geq\sum_{n=1}^{D}n^4
=\frac{D(D+1)(2D+1)(3D^2+3D-1)}{30}.
$$

The two roots alone require at least $7.03\,10^{30}$ and
$7.38\,10^{31}$ HNF visits under the unrestricted reference loop.

There is a cheap WS-specific substitute for computing every five-vertex
determinant. For six positive weights $w_i$, put $s=\sum_iw_i$ and
$g=\gcd(w_i)$. Then

$$
D(Q)\leq
\left\lfloor\frac{g\,s^4\max_iw_i}{\prod_iw_i}\right\rfloor.
$$

Indeed, $Q$ lies in the rational WS simplex, whose origin has barycentric
coordinates $w_i/s$. In saturated kernel-lattice units the determinant of
the five simplex vertices omitting vertex $i$ is
$g\,s^4w_i/\prod_jw_j$. Multilinearity bounds determinants inside that
simplex by the largest such value. Taking a floor is valid because $D(Q)$
is integral. This saves the vertex-frame loop, but does not make the ensuing
HNFs enumerable.

Prime-index traversal with non-IP pruning is much more promising. In
12 probes restricted to $2,3$-smooth indices through 36, 11 traversals
finished. Some tested only 153 lattices including the root, versus
13,481,086 in a blind smooth-index pass. However, the globally maximal
603-point root with weights $(1,1,1,2,3,4)$ generated 6,289 IP children
after only 100 parent expansions, with 6,190 IP states still queued.
Thus even an inexpensive, modest-point, reference-lattice maximum can have
a difficult lattice-descendant search. No other primes were covered by
this experiment.

### 9.4 Bad-facet recursion and maximality

An experimental in-process PALP adapter removes subprocess and text-matrix
overhead; it does not change the mathematical search. On a 27-point
nonreflexive root, the original 25-node search takes approximately 17 ms
to reach its first leaf, versus approximately 0.4 ms with the adapter.
The speedup is substantial, but does not remove state explosion.

Disjoint kept/deleted branching helps more than simply selecting the
largest-distance bad facet. For an ordered frame, branch $i$ deletes its
$i$th vertex and keeps all earlier vertices. A frame whose vertices are
all kept is an immediate obstruction. Completed partitioned searches
include:

- 27 points: 19 nodes, approximately 1 ms;
- 37 points: 15,716 nodes, approximately 1.20 s;
- 47 points: 17,681 nodes, approximately 1.54 s;
- 59 points: 9,905 nodes, approximately 0.81 s.

The overlapping search had not finished the latter three at 25,000 nodes.
Selecting only the largest-distance facet did not finish them at that
cap either. Fifteen of the 20 deliberately selected nonreflexive probes
remained unresolved with partitioning at 25,000 nodes or ten seconds.

More importantly, 63 of 64 uniformly drawn nonreflexive roots remained
unresolved at 1,000 nodes or two seconds. First leaves were found in
52 cases. Finding one leaf is therefore very different from completing
the covering search. Censored node counts are lower bounds, never
completed-search means. All three branching policies were additionally
checked against every reflexive subset of a 14-point 5D test instance.

Global maximality is also nontrivial. Of 78 reflexive probes, 53 eventually
finished and 25 remained unresolved after five seconds. Completed tests
range from approximately 4 ms to 4.80 s; their median, approximately
0.354 s, is selection-biased and is not an estimate of the full mean.
Separate instrumented repeats recorded up to 3,952 full/aided hull calls
for a completed test.
These probes are reflexive WS roots, not the as-yet unknown distribution
of candidates surviving a production sieve.

The default 8 MiB stack produced 62 failures. Raising the profiling
process's stack to 256 MiB allowed 47 completions, but 20 cases still
failed, mostly at the compiled 64-vertex limit in intermediate dual
subpolytopes. An isolated 128-vertex, 1 GiB-stack build converted all
remaining failures into completions or explicit timeouts. Neither
configuration is a proved sufficient bound for production 5D geometry.
No failed or timed-out oracle result was treated as a maximality decision.

### 9.5 Conditional workload and priorities

For original lattices and indices two and three only, scaling the random
probes gives approximately 34.8 billion nonreflexive starting states and
702 billion initial geometry calls. If $b$ is the still-unknown mean
additional recursive work per nonreflexive start, then

$$
G_{\leq3}\approx 7.025\,10^{11}+3.480\,10^{10}b.
$$

For $b=10,100,1000$, this gives approximately 1.05, 4.18 and 35.5 trillion
calls respectively. Larger indices, CWS, global maximality and normal
forms are excluded. The bounded recursion results show why even $b=1000$
must not be advertised as conservative.

Known-map containment certificates average approximately seven
microseconds to verify. A limited search over 3,840 signed permutations
against three anchor roots certified only 10 of 512 random inputs.
Failures are inconclusive; this is not a complete embedding search or
evidence of an effective production root sieve.

The highest-value directions are therefore certified root reduction with
retained lattice obligations, prime-index non-IP pruning, canonical reuse
of lattice-hull states, disjoint branching, and postponing dual maximality
until after substantial deduplication/pruning. Native in-process geometry
and reusing facets are worthwhile kernel optimizations, not substitutes
for those structural reductions.

## 10. Initial workload estimates and their assumptions

These were **planning scenarios before the actual WS measurements**, not
statistical forecasts. Initially we assumed $N=20\,000\,000\,000$ containers,
a point-count tail around 3,000, and vertices concentrated near 15. The
measured WS point maxima in section 9 are much larger; the early tail
assumption and hypothetical root timings must not replace the measured data.
The user-supplied distributions and partial vertex tables are retained in
[`user-ws-statistics.json`](../research/rmax-5d/user-ws-statistics.json).
Their selected-row sums are not catalogue totals, and the published
deduplication savings have already been realized in the unique inputs.

### 10.1 Lattice starts, geometry calls and recursive work

The exact number of index-$n$ sublattices of $\mathbb Z^d$ is

$$
a_1(n)=1,\qquad
a_d(n)=\sum_{k\mid n}k^{d-1}a_{d-1}(n/k),\qquad
H_d(K)=\sum_{n=1}^K a_d(n).
$$

The recurrence follows by choosing the first diagonal HNF entry $k$:
its $d-1$ off-diagonal residues give $k^{d-1}$ possibilities, while the
remaining block has determinant $n/k$.

| Index cap $K$ | Exact index $a_5(K)$ | Cumulative $H_5(K)$ |
| ------------- | -------------------: | ------------------: |
| 1             |                    1 |                   1 |
| 2             |                   31 |                  32 |
| 3             |                  121 |                 153 |
| 4             |                  651 |                 804 |
| 5             |                  781 |               1,585 |
| 10            |               24,211 |              55,170 |
| 100           |          330,988,581 |       4,606,849,681 |

At 20 billion roots, blind enumeration through index three produces
3.06 trillion lattice candidates; through five, 31.7 trillion; through
ten, $1.1034\times10^{15}$. These are **candidate states**, not necessarily
full hull calls, and none of these caps proves completeness.

The 496-point obstruction has $D(Q)=2401=7^4$ and

$$
H_5(2401)=35\,476\,236\,001\,762\,088.
$$

Even a hypothetical million candidates per second would require about
1,124 single-worker years for this one root. This is why the finite HNF
fallback is a completeness reference, not a production implementation.

A useful conditional model is

$$
G\approx N+Ns[(\ell-1)+\ell f b].
$$

Here $s$ is the fraction of roots surviving a **lattice-complete certified**
sieve, $\ell$ is an assumed effective number of geometry-triggering
lattice starts per retained root, $f$ is the fraction of those starts
that are nonreflexive IP, and $b$ is the mean additional recursive calls
per such start. The initial $N$ term assumes one new geometry decision
per input; reusing trusted stored metadata can reduce it.

With hypothetical $\ell=153$:

|    $s$ |  $f$ |    $b$ | Main geometry calls |
| -----: | ---: | -----: | ------------------: |
| 0.0001 | 0.10 |    100 |      23.364 billion |
| 0.0015 | 0.25 |  1,000 |    1.17206 trillion |
|   0.01 | 0.25 | 10,000 |    76.5504 trillion |
|      1 | 0.25 |  1,000 |     768.06 trillion |

The 4D reduction $308/201346\approx0.153\%$ motivated the second
scenario; it is **not a demonstrated 5D rate**. A hypothetical 1.6-fold
allowance for normal forms and maximality puts it near 1.9 trillion
calls, but neither that multiplier nor the underlying parameters was
measured. Counting 153 effective starts is also not a substitute for
handling higher-index lattices.

For a blind index-at-most-three pass, if only a fraction $\rho$ of
nonidentity candidates requires geometry, the root-plus-lattice count is
$N(1+152\rho)$. Taking $\rho=1,0.1,0.01,0.001$ gives 3.06 trillion,
324 billion, 50.4 billion and 23.04 billion calls. Section 9 measures
this fraction and the surviving IP starts for the actual WS sample.

### 10.2 Tail, determinant and timing sensitivities

A full five-child tree to depth ten has 12,207,031 nodes. If just 0.01%
of 20 billion roots each needs one million extra calls, that tail alone
adds two trillion calls. A heavily skewed point distribution therefore
does not establish a small mean completed-search cost.

There are $\binom{15}{5}=3003$ five-vertex frames at 15 vertices, versus
$\binom{50}{5}=2\,118\,760$ at 50. Computing all frames at all 20 billion
15-vertex roots would require 60.06 trillion determinant evaluations.
The WS simplex bound in section 9.3 avoids that loop, but not the
subsequent lattice-state multiplicity.

For scale only, one trillion calls at a hypothetical 10 ms each is
about 317 single-worker years, or 165 days on 1,000 cores at 70%
effective utilization. The measured warm root kernels are much faster
than 10 ms; recursive states, dual enumeration and global maximality
have different costs. I/O, memory, caching, parallel efficiency,
unseen tails and completed-search distributions remain separate
uncertainties. The reproducible
[`workload calculator`](../research/rmax-5d/workload_estimates.py)
preserves these assumptions alongside the updated WS scenarios.

## 11. Independent WS and CWS runs and consolidation

Yes: process WS and CWS independently, provided each run preserves all
of its image-lattice obligations. Let $\mathcal A_W,\mathcal A_C$ be the
resulting **finite reflexive covering families**, with lattice
realizations included. After placing comparable members in common
lattices or taking their origin-fixed unimodular classes,

$$
\operatorname{Max}\bigl(
 \operatorname{Max}\mathcal A_W\cup\operatorname{Max}\mathcal A_C
\bigr)
=\operatorname{Max}(\mathcal A_W\cup\mathcal A_C).
$$

For the nontrivial direction, any member discarded by a source's local
maximal-element pass lies below a retained member of that finite source.
Thus deleting local nonmaxima cannot delete a maximum of the union.
The union covers every reflexive polytope by the assumed complete
WS/CWS coverage theorem. Any globally r-maximal polytope equals a
reflexive member covering it; hence the maxima of that complete union
are precisely the global maxima.

If each source already applies an exact **global** maximality oracle,
the consolidation pass is just lattice-normal-form deduplication of
the union. If global tests are deferred, merge, deduplicate and retain
the maximal elements, or apply those tests to the merged candidates.
No new full subpolytope search or lattice enumeration of the outputs
is needed merely to consolidate two complete covering families.

WS can be run first to supply certificates and anchors for CWS. However,
a fixed-lattice containment witness does not discharge other lattice
realizations: removing a whole root requires retaining the appropriate
complete lattice-descendant search of its covering root. Splitting work
is an organizational and caching opportunity, not a solution to lattice
enumeration. The reported approximately one billion CWS inputs were
still loading; no CWS dataset profiling or complete 5D run was performed.

## 12. Why maximal-cover pruning is not minimal-first pruning

Suppose small containers have supplied some r-minimal reflexive
polytopes $(\Delta_{\min})_i\subset N_{\mathbb R}$. Their duals are
candidate r-maximal polytopes in $M_{\mathbb R}$. Knowing some such
maxima is useful, but **a container inside a known maximum can still
contain a new r-minimum**. Its dual maximum need not be inside the
known maximum, since polarity reverses inclusion and exchanges lattices.

### 12.1 A fixed-lattice counterexample in dimension five

Use standard dual bases to write both lattices as $\mathbb Z^5$. Let

$$
A=\operatorname{conv}(\pm e_1,\ldots,\pm e_5),
$$

$$
B=\operatorname{conv}(e_1,e_2,-e_1-e_2,\pm e_3,\pm e_4,\pm e_5).
$$

Both are reflexive and IP-minimal, hence r-minimal. Their only lattice
points are their vertices and the origin: 11 points for $A$ and ten for
$B$. Removing any vertex destroys the origin-IP property. They are
distinct minimal objects, not one containing a smaller minimal object
properly: distinct r-minima cannot be properly nested.

Their duals are the globally r-maximal polytopes

$$
R=A^*=[-1,1]^5,\qquad |R\cap\mathbb Z^5|=3^5=243,
$$

$$
U=B^*=
\{y_1,y_2\ge-1,\ y_1+y_2\le1\}\times[-1,1]^3,\qquad
|U\cap\mathbb Z^5|=10\cdot27=270.
$$

Yet $B\subset R$. Consequently, a minimal-first search that skips
everything inside the known maximum $R$ would miss $B$, and hence
possibly its dual maximum $U$. No unimodular image of $U$ fits inside
$R$, because unimodular transport preserves its 270 lattice points
and $R$ has only 243. The coordinates and exact PALP checks are saved
in [`examples.json`](../research/rmax-5d/examples.json), regenerated by
[`check_examples.py`](../research/rmax-5d/check_examples.py).

### 12.2 What can safely be skipped

For a **fixed-lattice maximal-cover search**, if

$$
P\subseteq Q\subseteq R,\qquad P\text{ globally r-maximal},\quad
R\text{ reflexive},
$$

then $P=R$. Thus there is no new global maximum strictly inside this
node $Q$, and it need not be enumerated on this lattice. In particular,
the cover algorithm can stop at a reflexive hull without enumerating
its r-minimal descendants. This is the coverage invariant formalized
by `reflexive_stop_covers` and `maximal_in_cover_occurs`.

This does not assert that $Q$ has no unknown minima. Their dual maxima
must have an undischargeable container/lattice realization elsewhere
in the universally complete catalogue: any reflexive cover containing
a genuinely missing maximum must equal it. A hybrid algorithm can
therefore use small-container minimal searches to seed candidates,
then switch to direct maximal-cover obligations. Its completeness
comes from processing those obligations, not from finding every
minimum inside every skipped container.

If the goal remains **minimal-first enumeration**, retain the full
minimal-descendant obligation of the covering root; a maximal-cover
certificate is insufficient. In either approach, compare actual common
lattices or transport unimodularly. A same-lattice containment decision
does not remove proper image-sublattice obligations, even when the
reference-lattice root is reflexive or already globally maximal.

## 13. Shareable artifacts and reproduction

The [research bundle](../research/rmax-5d/README.md) indexes every
retained generated source, measurement, failure diagnostic and validation
artifact. It includes the historical intermediate 4D outputs separately
from the authoritative final 340. Original artifact hashes are preserved.
The WS replay uses stored representative weights in a new directory,
without needing the original catalogues, and does not overwrite the
historical measurements. Published papers are cited below rather than
redistributed as downloaded PDFs.

## References actually consulted

**[KS-classification]** M. Kreuzer and H. Skarke,
_Complete classification of reflexive polyhedra in four dimensions_,
[hep-th/0002240v1](https://arxiv.org/abs/hep-th/0002240v1).
Section 2 describes the 201,346-to-308 reduction and lattice reconstruction.
Printed p. 11 gives $308+25+7=340$ and explains why unchanged-shape lattice
choices miss the last seven. Table 8, printed p. 18, lists their r-minimal
duals. The v1 PDF was retrieved and its text checked for this investigation.

**[KS-weights]** M. Kreuzer and H. Skarke,
_Reflexive polyhedra, weights and toric Calabi-Yau fibrations_,
[math/0001106v1](https://arxiv.org/abs/math/0001106v1), section 2.3.
Lemma 7 concerns the lattice interval for a **fixed** integral pairing matrix;
Lemma 8 is the low-dimensional CWS reflexivity result; Lemma 9 and
Proposition 3 distinguish seed minimality, r-minimal CWS and further lattice
realizations. This earlier paper reports the additional 25, not the eventual
complete 340.

**[Skarke]** H. Skarke, _Weight systems for toric Calabi-Yau varieties and
reflexivity of Newton polyhedra_,
[alg-geom/9603007](https://arxiv.org/abs/alg-geom/9603007).
The theorem on maximal Newton polyhedra is restricted to dimension at most
four. This is not a theorem that every IP polytope in those dimensions is
reflexive.

**[SS]** M. Scholler and H. Skarke, _All Weight Systems for Calabi-Yau
Fourfolds from Reflexive Polyhedra_,
[1808.02422](https://arxiv.org/abs/1808.02422), section 6.
The five-dimensional weight enumeration has essential nonreflexive IP
intermediates; its weight counts are not counts of distinct containers or
r-maximal polytopes.
