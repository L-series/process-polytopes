# Normal forms, exact equality, and classification output

A normal form can decide lattice equivalence only after the target lattice and equivalence relation are fixed. Its canonical integer matrix, its serialized bytes, and a hash of those bytes are three different objects. This note reviews the primary normal-form algorithm and the pinned/archived computational contracts. It does not certify a new implementation, run a dataset census, or change scientific counts. The approved current universe remains the implemented descriptor/input-pool universe on the PALP reference lattice; [lattice-realization coverage](cws-and-lattice-realizations.md) is a separate completeness obligation.

## Primary references and reusable evidence

**[GK13]** Roland Grinis and Alexander M. Kasprzyk, _Normal forms of convex lattice polytopes_, [arXiv:1301.6641v1](https://arxiv.org/abs/1301.6641v1), submitted 28 January 2013, 28 pages. The primary version inspected is the [PDF](https://arxiv.org/pdf/1301.6641v1) and author [TeX archive](https://arxiv.org/src/1301.6641v1). No journal publication metadata was present in the checked arXiv record, and none is inferred. Relevant statements are §3, pages 8–12; Definitions 3.1–3.2, pages 8–9; affine normal form in §3.3, page 11; PALP's extra column ordering in §3.4, pages 11–12; Appendix A, pages 16–25, especially A.8 on pages 24–25. Section 2 treats a different, more general face-graph approach, including rational and lower-dimensional polytopes.

**[PALP04]** Maximilian Kreuzer and Harald Skarke, _PALP: A Package for Analyzing Lattice Polytopes with Applications to Toric Geometry_, Computer Physics Communications **157** (2004), 87–106, [DOI 10.1016/S0010-4655(03)00491-0](https://doi.org/10.1016/S0010-4655%2803%2900491-0). Title/authors/publication metadata and DOI were verified against [arXiv:math/0204356v1](https://arxiv.org/abs/math/0204356v1), submitted 30 April 2002. The actual paper read is the primary [22-page v1 PDF](https://arxiv.org/pdf/math/0204356v1) and [author source](https://arxiv.org/src/math/0204356v1). Section 1, pages 1–2, states full-dimension and fixed-precision limitations; §2's `-N`/`-t` documentation, page 6, describes the isomorphism test and stages. The preprint year differs from the journal year.

Both owned source caches were created during this approved assignment. Original downloaded bytes were retained, SHA-256 recorded, and TeX/text extracted only as reading aids. Retrieval date is 2026-10-14 UTC. Paths below are relative to `docs/learning/sources/`:

| Source/version                  | Original PDF SHA-256                                               | Original source SHA-256                                            |
| ------------------------------- | ------------------------------------------------------------------ | ------------------------------------------------------------------ |
| `grinis-kasprzyk-1301-6641/v1/` | `0e2fb84a6bd81009924079ca02a39deca2f87ff575d942cbb05def37b045273d` | `fde1cb487f13d1d12d0ce992f71c90012e9ceafcb2e61bedb292c12385e4526a` |
| `palp-math-0204356/v1/`         | `5134b66baac3b0009b4f4cdd02aa642d958d7cef03f66f7bcccf2c127c30e247` | `627ca3515b570be5560bd939dc4db3ff69466995fd65befffb6a619f61c4bf51` |

Each directory contains `paper.pdf`, `source.tar.gz`, `provenance.json`, `paper.txt`, and supplementary `tex/` files. The PALP source download is a gzip-compressed **single TeX file**, despite the local archive filename; provenance records that format. Source-license status is unverified; these are local reading caches. Printed page numbers, rather than guessed journal pages, are used above.

Implementation evidence is fixed to PALP commit `968bb4b3c6c42a0893c95340e81718548e13f12a`, root archived `main` commit `7c4dc2e62da1ff8d539684d4977d4de9656d7a4f`, and `gpu-run-43types` commit `452fbe8e1b296bf75fa9e8becf6c34d8d48bf00a`. Archived files were read with `git show`; no donor checkout or ref was changed. Current regression evidence is `tests/README.md`, `tests/cws_support.py`, and `tests/test_cws.py`. Frozen formal definitions were inspected read-only at

`/home/ahat01/.copilot/session-state/83ef73b2-4517-446c-9ac2-9437ff014965/files/refpoly-proof-snapshot/proofs/Refpoly/Basic.lean`.

## Linear and affine equivalence are different contracts

For a fixed rank-`n` lattice, linear lattice isomorphism means `Q=g(P)` for `g` in `GL(n,Z)`. The origin is distinguished and fixed. Affine lattice equivalence allows `Q=g(P)+t` for an integral translation `t`. Neither is merely combinatorial equivalence or an arbitrary invertible real transformation.

A lattice polytope with exactly one interior lattice point has an intrinsic centering rule: translate that unique point to zero. Every affine lattice equivalence sends that point to the unique point of the image; after centering both polytopes, it becomes linear. This applies to reflexive and general one-point lattice polytopes. If several interior lattice points exist, choosing one arbitrarily can distinguish affine-equivalent but differently marked objects. For instance, intervals `[-1,3]` and `[-2,2]` are integral translates, both have origin interior, and are not related by `GL(1,Z)={+1,-1}`. A marked origin or a translation-invariant normal form must be specified.

[PALP04] describes ordinary normal form in the linear setting. [GK13] §3.3 separately defines an affine normal form by minimizing over vertex translations as well as the relevant vertex permutations. Its finite vertex-translation formula covers affine lattice equivalence of full-dimensional lattice polytopes. Neither origin interior nor reflexivity is required by the normal-form construction in §3. The full-dimensional **lattice** hypothesis is essential to its integer Hermite-normal-form step. The paper separately discusses lower-dimensional reductions in §3.2 and general rational isomorphism testing in §2; those extensions should not be silently assumed to be the pinned `poly -N` command's general input contract.

## The primary normal-form algorithm

With vertices arranged as columns of an integer matrix `V`, changing the lattice basis acts on the left by a unimodular matrix; vertex relabeling acts on the columns. Hermite normal form removes the first freedom for a fixed column order, but does not remove column-order dependence. Therefore choosing a single arbitrary vertex order and taking HNF is not sufficient.

[GK13] Definition 3.1 forms the vertex–facet **height** matrix

`PM_ij = <w_i,v_j> + c_i`,

where `w_i` is a primitive inward integral facet normal and its supporting facet has equation `<w_i,x>=-c_i`. This matrix is invariant under lattice automorphisms and translations, up to row and column permutations. For a nonreflexive lattice polytope the `c_i` need not equal one; no reflexive polar is needed. Its purpose is to organize candidate vertex orders, not to serve by itself as a complete lattice-equivalence invariant.

The construction takes the lexicographically **maximal** height matrix over facet/vertex permutations, retains the permutations attaining it (equivalently one maximizing permutation composed with all its matrix automorphisms), and then minimizes the HNF of the corresponding reordered vertex matrices. Definition 3.2 gives this ordinary normal form. PALP adds a deterministic column order based on each maximal height-matrix column's maximum entry, sum, and relative position, as explained in §3.4 and Appendix A.8. The final choice is the lexicographically **minimal** vertex HNF over the remaining candidate permutations. Matrix symmetries can exceed actual lattice automorphisms; retaining all the relevant candidates before HNF is part of the correctness argument.

Appendix A, page 16, explicitly states the canonical-output contract: for full-dimensional lattice polytopes in the specified lattice, equal PALP normal forms are equivalent to linear lattice isomorphism. This is an algorithmic description and correctness statement in the primary source, not a numbered Lean theorem or a proof that every future reimplementation matches it. Finite permutation sets and integer HNF give a terminating mathematical procedure; arithmetic range, capacities, and complete permutation handling in a C build remain implementation obligations. Definitions 3.1 and 3.2 are the relevant numbered items; this note does not invent a separate theorem number for that contract.

Determinism is strictly weaker. A routine could always return the same wrong matrix for the same input and be deterministic without being canonical. Canonicity needs both invariance under every allowed equivalence and separation of inequivalent inputs. Even a canonical matrix is not a collision-free finite-width digest.

## Pinned PALP and the actual regression target

At the pinned PALP commit, `Polynf.c` implements the expected stages: `Init_rVM_VPM` computes integer vertex coordinates and facet evaluations; `Make_VPM_NF` organizes the maximal pairing matrix and its admissible permutations; `New_pNF_Order` supplies the additional column ordering; `Aux_pNF_from_vNF`/`Aux_Make_Triang` compare triangularized vertex matrices; `GLZ_Make_Trian_NF` supplies the unimodular triangular/HNF step. `Aux_XltY_Poly_NF` compares entries row by row. `Init_rVM_VPM` records whether facet constants all equal one, but `Make_Poly_NF` computes the normal form regardless and returns that reflexivity flag separately. Therefore the normal-form routine is not restricted mathematically to reflexive objects.

`poly.c` invokes `Make_Poly_Sym_NF` on `_P` for option `N`, and `Polynf.c` labels the result **Normal form of vertices of P**. Option `A` calls the separate `Make_ANF` path; it temporarily adds an auxiliary coordinate and origin, obtains a linear normal form, and reduces/restores the affine representation. The root regression command is `poly -fN`, with no dual-selection flag. Its goldens are vertex normal forms of **P**, not of its polar.

`tests/cws_support.py` parses each normal form as `(dimension, vertex_count, tuple(matrix_rows))`. It requires the expected P header, exactly `dimension` rows, and exactly `vertex_count` integers per row. `serialize_forms` writes the shape and integer rows as normalized text. Only whitespace and the informational `perm=` annotation are removed. `tests/test_cws.py` compares every resulting matrix to its corresponding golden in sequence, requires one matrix per accepted CWS row, and separately checks `len(set(forms))` for the distinct count. Repeated NFs remain in the accepted-row list: their multiplicities are part of the regression contract.

These are exact PALP consistency regressions for the checked-in pools. They do not independently prove mathematical canonicity, all-dimensional completeness, equivalence with the abstract catalogue, or the correctness of another lattice realization. A changed golden requires the repository's explicit baseline-review procedure; regenerating it merely to pass a test would erase evidence. This assignment did not regenerate or run a new baseline.

## What the archived classifiers actually compare

Both inspected archived classifiers, `main:src/classify/classifier.cpp` and `gpu-run-43types:src/classify/classifier.cpp`, compute a 128-bit xxHash of the used NF entries, stored contiguously as native `Long` values. Their maps use `Hash128` as the key. On a key hit, counts are merged without comparing the full integer NF matrices. The payload stores counts and representative metadata, not the full matrix used for an exact equality check. The same digest-key logic appears in local-to-global merges. This is hash-based aggregation; no collision-resolution equality path was present in those inspected sections.

The archived `gpu-run-43types:src/classify/compare_polytope_outputs.cpp` compares sorted `(hash_lo,hash_hi,count)` records. Passing it shows equality of those records, not exact equality of the underlying NF matrices. `src/verify/proofs/nf_determinism.md` discusses same-input deterministic sorting and hashing, with assertions about other stages; it is an archived argument, not a formal proof of NF canonicity or hash injectivity. A total-order comparator addresses one sorting concern, while leaving the geometric canonicalization and arithmetic contracts distinct.

The archived `palp_api.h` wrappers construct P from CWS data, find its vertices/equations, sort its vertex indices, and call `Make_Poly_Sym_NF` on those points. `result.ok` is set after the IP pipeline succeeds; it is not, by that assignment alone, an independently asserted reflexivity flag. Again the returned matrix is primal P's NF.

The newer archived `PolytopeInfo` preserves first-CWS replay fields: schema version, structure/profile IDs, row count, coordinate count, source index, degrees, all padded weights, and a legacy six-weight projection, alongside geometric/Hodge metadata. These fields can help reconstruct a representative after aggregation, provided their input/lattice/build interpretation is pinned. They do not retain all members of a digest bucket or automatically detect an incorrect earlier merge.

`gpu-run-43types:src/classify/add_nf.cpp` replays those first-CWS inputs and appends `nf_vertices` as row-major native `int32` bytes. Its optional hash verification checks agreement with the stored digest, not equality against a separately preserved full canonical matrix. The conversion's range check is guarded by `first_block`, so the inspected check does not cover every later successful block per worker; the source's general expectation that values fit is not a proved universal bound. A lossless output contract must justify all converted entries or check them all. This review records the historical behavior without modifying it or inferring that any actual retained entry overflowed.

## Exact reduction and serialization obligations

For a mathematically exact class merge, the decisive object should include the declared equivalence/lattice convention, dimension, vertex count, and every integer NF entry. A digest may select a partition or candidate bucket. A hit must then compare complete canonical values, or replay enough immutable data to recover and compare them exactly. Distinct matrices in the same digest bucket must remain distinct classes. The counts of equivalent rows should be accumulated only after that equality is established.

A persistent binary encoding needs a defined signed-integer width, byte order, matrix layout, shape, and algorithm/schema version. The native `Long` hash buffer and native `int32` output are different encodings; equality of hashes computed from one does not prove lossless serialization into the other. Fixed platform/build assumptions may support replay within a pinned run, but should be recorded instead of being mistaken for a portable mathematical normal form. Shape and integer comparisons in the current text goldens avoid relying on native byte width or hash collision assumptions.

Exact replay also needs immutable CWS input, the relevant descriptor/support ordering, PALP commit/build capacities/arithmetic choices, and the lattice realization. The same real polytope on a different lattice can represent a different equivalence class, so a normal form only classifies the lattice coordinates it is actually given. Recording weights alone may be insufficient once the long-term programme includes finite-index realizations beyond the reference lattice.

## Formal support and remaining gaps

Frozen `Basic.lean` defines `IsUnimodular g` by **both** forward and inverse preservation of integer lattice points. `GLEquiv P Q` is the existence of a real linear equivalence with that unimodularity property carrying P onto Q. Its declarations prove reflexivity, symmetry, transitivity, lattice-image/cardinality preservation, and preservation of lattice-polytope structure. They give an abstract equivalence relation; they do not define NF, its serializer, a hash, or a native reduction theorem.

| Contract                                | Evidence available                                            | Missing or bounded scope                                                                                       |
| --------------------------------------- | ------------------------------------------------------------- | -------------------------------------------------------------------------------------------------------------- |
| Mathematical linear lattice equivalence | `Basic.IsUnimodular`, `GLEquiv`; primary NF contract          | No frozen theorem `NF(P)=NF(Q) iff GLEquiv P Q`.                                                               |
| Affine equivalence                      | GK §3.3 algorithm; unique-interior centering deduction        | No inspected formal affine-equivalence/NF bridge; multiple origins need a declared convention.                 |
| P versus polar target                   | Pinned PALP call/output and exact root parser                 | Switching targets requires an explicit contract and fixture change.                                            |
| General nonreflexive NF                 | GK full-dimensional lattice scope; PALP height matrix         | Rational/lower-dimensional reductions and native capacity bounds need separate verification.                   |
| Same-input determinism                  | Archived argument and finite algorithm structure              | Does not prove equivalent-input canonicity or separation.                                                      |
| Collision-safe class merging            | Required exact-NF equality policy                             | Inspected archived maps compare digest keys only; no full payload collision resolution.                        |
| Lossless serialization                  | Exact integer text goldens; archived row-major binary path    | Portable binary convention and complete width/range guarantee are not established by those wrappers.           |
| Replay                                  | First-CWS schema/degree/weight/source metadata                | Representative replay alone cannot recover collided classes; lattice/build/input provenance must be immutable. |
| Scientific output counts                | Regression distinguishes accepted rows and distinct exact NFs | Hash-count equality is insufficient to certify an exact geometric census.                                      |
| End-to-end completeness                 | Separate abstract container coverage                          | Native NF/refinement/dedup correctness and global lattice/subpolytope coverage remain separate.                |

A complete enumeration theorem can permit duplicates: every output must be valid and every target class represented, with termination. Duplicate-free output is an additional theorem only if one representative per equivalence class is promised. Normal forms are useful for that stronger promise and for exact distinct-class counts; they do not repair missing seed, lattice-realization, or subpolytope coverage. The planned native refinement and census must therefore keep coverage, correctness, and exact equivalence reduction as separate obligations.

## Evidence checklist

- Primary PDF/author-source versions read and cached with original-byte hashes and provenance.
- Verified algorithm assumptions, Definitions 3.1–3.2, affine extension, PALP permutation/HNF steps, and unnumbered Appendix A canonicity statement.
- Inspected pinned PALP P/affine entry points and current exact tuple-based golden tests retaining multiplicities.
- Inspected fixed archived classifiers, replay serialization, hash/count comparison, and determinism argument without mutation or large dataset scans.
- Inspected actual frozen `IsUnimodular`/`GLEquiv` definitions; no formal NF, hash-injectivity, or native-dedup theorem is claimed.
