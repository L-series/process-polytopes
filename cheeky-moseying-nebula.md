# Formal Verification Plan for Polytope Classifier & PALP

## Context

The polytope classification pipeline processes ~16.7B records across 67 checkpoint files. Correctness depends on a chain of operations: CWS parsing → PALP normal form computation → xxHash128 → deduplication via sorting/merging → final output. A bug anywhere in this chain could silently corrupt the scientific results (wrong polytope counts, missed duplicates, lost records). Formal verification targets the critical routines where testing alone cannot provide sufficient confidence.

---

## Part 1: Classifier Verification Targets

### 1.1 Strict Weak Ordering of `key_less` (classifier.cpp:126-129)

**Property:** `key_less` must be a strict weak ordering for use with `std::sort`, `std::lower_bound`, and the min-heap.
- Irreflexivity: `key_less(a, a)` is false
- Asymmetry: `key_less(a, b)` implies `!key_less(b, a)`
- Transitivity: `key_less(a, b) ∧ key_less(b, c) ⟹ key_less(a, c)`
- Equivalence consistency: `!key_less(a,b) ∧ !key_less(b,a) ⟺ a == b`

**Tool:** ESBMC (bounded model checker with C++ support)

**Approach:** Write a harness with 3 non-deterministic `Hash128` values and assert all four properties. ESBMC exhaustively checks all possible 128-bit combinations within the bound.

```cpp
// harness_key_less.cpp
#include "classifier_types.h"  // extract Hash128 + key_less

void harness() {
    Hash128 a, b, c;
    // ESBMC generates all possible values for a, b, c

    // Irreflexivity
    assert(!key_less(a, a));

    // Asymmetry
    if (key_less(a, b)) assert(!key_less(b, a));

    // Transitivity
    if (key_less(a, b) && key_less(b, c)) assert(key_less(a, c));

    // Equivalence = equality
    if (!key_less(a, b) && !key_less(b, a)) {
        assert(a.hi == b.hi && a.lo == b.lo);
    }
}
```

**Assumptions:** None — this is a complete proof over the function's domain.

**Impact:** If this property fails, `std::sort` has undefined behavior, and the entire merge pipeline is unsound. All downstream dedup results would be unreliable.

---

### 1.2 Hash128Hasher consistency with operator== (classifier.cpp:83-93)

**Property:** `a == b ⟹ Hash128Hasher(a) == Hash128Hasher(b)` (required for `std::unordered_map` correctness).

**Tool:** ESBMC or even compile-time proof (trivial by inspection: `operator==` checks both `lo` and `hi`, hasher returns `lo`; if `a == b` then `a.lo == b.lo` so hashes match).

**Approach:** A 2-line harness. Trivial, but worth stating explicitly as a verified lemma.

**Assumptions:** None.

---

### 1.3 `hash_normal_form` byte-layout correctness (classifier.cpp:135-148)

**Property:** The function hashes exactly `dim × nv` `Long` values from the normal form matrix, in row-major order, with no uninitialized bytes.

**What to verify:**
1. `k == dim * nv` after the loop (no off-by-one)
2. All accessed indices `nf[i][j]` satisfy `0 ≤ i < dim` and `0 ≤ j < nv`
3. `buf` has no uninitialized gaps (contiguous fill from `buf[0]` to `buf[k-1]`)
4. The bytes passed to `XXH3_128bits` are exactly `k * sizeof(Long)`

**Tool:** CBMC (C-compatible function, no STL)

**Approach:** Harness with non-deterministic `dim ∈ [1,5]` and `nv ∈ [1,64]`:
```c
void harness() {
    int dim, nv;
    __CPROVER_assume(dim >= 1 && dim <= POLY_Dmax);
    __CPROVER_assume(nv >= 1 && nv <= VERT_Nmax);
    Long nf[POLY_Dmax][VERT_Nmax];
    // Fill nf with non-deterministic values
    Hash128 h = hash_normal_form(nf, dim, nv);
    // CBMC checks: array bounds, k == dim*nv (via assertion)
}
```

**Assumptions:**
- `dim ∈ [1, POLY_Dmax]` and `nv ∈ [1, VERT_Nmax]` — these are guaranteed by PALP (verified separately in Part 2)
- xxHash itself is treated as a black box (its correctness is validated by the xxHash test suite / SMHasher; we verify only that we feed it the right bytes)

**Impact:** If wrong bytes are hashed, two identical polytopes could get different hashes (false negatives — missed dedup) or different polytopes could hash the same (already accepted as 10⁻²⁰ probability from xxHash128 properties).

---

### 1.4 MergeRecord binary layout (classifier.cpp:117-124)

**Property:** `sizeof(MergeRecord) == 72` with no padding, matching the on-disk checkpoint format. Hash128 at offset 0, PolytopeInfo at offset 16.

**Already partially verified:** `static_assert` at line 123 checks `sizeof(MergeRecord) == sizeof(Hash128) + sizeof(PolytopeInfo)`.

**Additional verification needed:**
- `offsetof(MergeRecord, key) == 0`
- `offsetof(MergeRecord, info) == 16`
- `sizeof(PolytopeInfo) == 56` (50 bytes of fields + 6 bytes padding after h13 to reach 8-byte alignment)

**Tool:** Static assertions (compile-time, no external tool needed)

```cpp
static_assert(offsetof(MergeRecord, key) == 0);
static_assert(offsetof(MergeRecord, info) == sizeof(Hash128));
static_assert(sizeof(Hash128) == 16);
static_assert(sizeof(PolytopeInfo) == 56);
static_assert(sizeof(MergeRecord) == 72);
```

**Assumptions:** Assumes the same compiler and platform for checkpoint writer and reader. Cross-platform portability (e.g., big-endian) is not verified and not needed (all processing happens on x86-64 Linux).

---

### 1.5 Count preservation in `merge_dedup_parallel` (classifier.cpp:659-766)

**Property:** The sum of all `info.count` values in the output equals the sum of all `info.count` values in both inputs (accumulator + shard). No counts are lost or duplicated.

Formally: `Σ(out[i].info.count) == Σ(acc[j].info.count) + Σ(shard[k].info.count)`

**Tool:** ESBMC with bounded harness

**Approach:** Create small sorted arrays (e.g., 4 elements each) with known counts, call `merge_dedup_parallel`, verify sum. ESBMC explores all possible key/count combinations within the bound.

```cpp
void harness() {
    // Small arrays (bounded for tractability)
    const int N = 4;
    std::vector<MergeRecord> acc(N), shard(N), out;
    // Non-deterministic but sorted keys and positive counts
    // ... (constrain: acc sorted by key_less, shard sorted by key_less)

    uint64_t sum_in = 0;
    for (auto &r : acc) sum_in += r.info.count;
    for (auto &r : shard) sum_in += r.info.count;

    uint64_t dups;
    merge_dedup_parallel(acc, shard, out, dups, 1 /*single thread*/);

    uint64_t sum_out = 0;
    for (auto &r : out) sum_out += r.info.count;

    assert(sum_out == sum_in);  // COUNT PRESERVATION
}
```

**Assumptions:**
- Input arrays are sorted (precondition; verified by the sort step)
- Single-threaded verification (multi-threaded version partitions into independent single-threaded merges; if each partition preserves counts, the whole does)
- `uint64_t` addition does not overflow (assumption: total count < 2⁶⁴; with ~16.7B records, this holds by many orders of magnitude)
- Bounded to small arrays; the algorithm is uniform (same code path regardless of array size), so bounded verification generalizes

**Impact:** If counts are lost during merge, the final polytope frequency data is wrong. This is the most scientifically important property to verify.

---

### 1.6 Count preservation in k-way heap merge (classifier.cpp:971-1048)

**Property:** Same as 1.5 but for the heap-based merge: sum of all output counts equals sum across all input files.

**Tool:** ESBMC

**Approach:** Harness with 3 small sorted input streams (3 elements each), verify total count sum.

**Additional property:** Output is sorted by `key_less` (each emitted record has key ≥ previous).

**Assumptions:**
- All input streams are sorted (precondition from phase 1)
- Heap comparator `operator>` is consistent with `key_less` (verified by inspection: both compare hi-first, lo-second)
- `SortedBinaryReader` correctly iterates all records (verified separately)

---

### 1.7 `SortedBinaryReader::advance()` correctness (classifier.cpp:782-809)

**Property:** The reader visits every record exactly once, in order, and sets `valid = false` exactly when all records are exhausted.

**What to verify:**
1. After construction, `current` holds the first record (or `valid == false` if file is empty)
2. Each `advance()` call moves to the next record
3. After exactly `n` calls to `advance()` (where `n` is the header count), `valid` becomes false
4. No buffer overrun: `buf_pos_ ≤ buf_size_` always

**Tool:** CBMC (pure C-style code, no STL dependency in the logic)

**Approach:** Model the file as an in-memory buffer of `n` records. Replace `file_.read()` with a mock that copies from the buffer. Verify the state machine with `n ∈ {0, 1, BUF_RECORDS-1, BUF_RECORDS, BUF_RECORDS+1}` edge cases.

**Assumptions:**
- File I/O is correct (file contains exactly `n` records as declared in header)
- No I/O errors (partial reads, disk corruption). This is an environment assumption, not a code property.

---

### 1.8 Checkpoint write/read round-trip (classifier.cpp:512-541)

**Property:** Writing a `PolytopeMap` to a checkpoint and reading it back produces an identical map (same keys, same counts, same info fields).

**Tool:** ESBMC with bounded harness

**Approach:** Create a small map (3 entries), write to in-memory buffer, read back, verify equality.

**Assumptions:**
- `MergeRecord` has no padding (verified in 1.4)
- Single write, single read (idempotency: reading the same checkpoint twice intentionally doubles counts — this is documented behavior, not a bug)

---

### 1.9 Accounting invariant (classifier.cpp:~1900-1915)

**Property:** `processed == unique + duplicate + failed`

**Tool:** Frama-C WP (ACSL contracts) or ESBMC

**Approach:** Annotate `process_batch` and `merge_maps` with ACSL-style pre/postconditions tracking the invariant. This requires reasoning about the interaction between the two functions.

**Assumptions:**
- Atomic counters are correctly ordered (relaxed ordering is sufficient for aggregate sums when checked after all threads join)
- No thread creates records outside the process_batch → merge_maps path

---

### 1.10 Scatter-sort bucket correctness in `fast_merge_checkpoints` (classifier.cpp:1073-1283)

**Property:** 
1. Every input record appears in exactly one bucket
2. Bucket `b` contains only records where `(key.hi >> 56) & 0xFF == b`
3. After sorting, records within each bucket are sorted by `key_less`
4. Buckets are in non-overlapping key ranges, so bucket-local dedup = global dedup

**Tool:** ESBMC for properties 1-2 (bounded), manual proof for 3-4

**Property 4 proof sketch:** If `x.hi >> 56 != y.hi >> 56`, then `x.hi != y.hi`, so `key_less` orders them by `hi`. All records in bucket `b` have `hi` values in `[b << 56, (b+1) << 56)`. Records in bucket `b` and `b+1` never have equal keys. Therefore dedup within each bucket catches all duplicates. QED.

**Assumptions:**
- Histogram scatter writes each record exactly once (two-pass: count then write; verified by prefix-sum construction)
- `std::sort` is correct (assumed; part of the C++ standard library)

---

## Part 2: PALP Verification Targets

### 2.1 Tool Selection: Frama-C

PALP is pure C with global arrays, macros, and pointer arithmetic. Frama-C is the best fit:
- **Eva plugin** (automatic): Detects buffer overflows, null dereferences, integer overflows without annotations
- **WP plugin** (deductive): Proves functional properties with ACSL annotations
- Handles global variables, macro-heavy code, and pointer arithmetic natively

### 2.2 Array Bounds in `Make_CWS_Points` (PALP/Coord.c)

**Property:** `P->np ≤ POINT_Nmax` after lattice point enumeration.

**Why it matters:** If `P->np` exceeds `POINT_Nmax` (2,000,000), subsequent array accesses `P->x[np][n]` corrupt memory. This was already a known issue (POINT_Nmax was raised from 200K).

**Tool:** Frama-C Eva

**Approach:** Run Eva on `Make_CWS_Points` with abstract inputs representing valid CWS weight systems. Eva will compute value ranges for `P->np` and flag any path where it exceeds `POINT_Nmax`.

**Assumptions:**
- CWS input is valid (non-negative weights, degree > 0). This is guaranteed by the Parquet input data.
- The `POINT_Nmax = 2,000,000` bound is sufficient for all 5D reflexive polytopes. **This is an empirical assumption, not formally provable** — it depends on the mathematical properties of the dataset.

**Impact of assumption failure:** If a CWS produces > 2M lattice points, PALP silently writes past the array bound. The result could be a corrupted normal form that hashes differently from the correct one.

---

### 2.3 Vertex count bounds in `Find_Equations` (PALP/Vertex.c:1089)

**Property:** `V->nv ≤ VERT_Nmax` (64) throughout the vertex discovery loop.

**Current protection:** `assert(V->nv < VERT_Nmax)` at Vertex.c:1082, compiled with `-DPALP_FAST_ASSERT` to prevent assert-elision.

**Tool:** Frama-C Eva + WP

**Approach:**
1. Eva: Run abstract interpretation on `Find_Equations` to compute value range of `V->nv`
2. WP: Add ACSL loop invariant `/*@ loop invariant 0 <= V->nv <= VERT_Nmax; */` and prove it holds

**Assumptions:**
- `-DPALP_FAST_ASSERT` is always enabled (verified in CMakeLists.txt)
- `VERT_Nmax = 64` is sufficient for all 5D reflexive polytopes. **Empirical assumption** (max observed: 47, margin of 17).

---

### 2.4 Side-effect-in-assert safety (PALP/Vertex.c, multiple locations)

**Property:** All `assert()` calls with side effects are preserved by `PALP_FAST_ASSERT`.

**Known locations with side-effect asserts:**
- Vertex.c:697 — `Vec_Greater_Than` result check
- Vertex.c:709 — `x != y` verification
- Vertex.c:786 — `EVAL_EQ` sign check (EVAL_EQ modifies state)
- Vertex.c:807 — vertex validation

**Tool:** Frama-C Eva (run twice: with and without `-DNDEBUG`, compare results)

**Approach:** 
1. Compile PALP with `PALP_FAST_ASSERT` and run Eva → baseline
2. Compile PALP with standard `assert` (NDEBUG) and run Eva → compare; Eva should report new potential errors on the disabled-assert paths
3. This validates that PALP_FAST_ASSERT is load-bearing

**Assumptions:** The PALP_FAST_ASSERT macro correctly evaluates the expression even when NDEBUG is defined. Verified by inspection of Global.h:8-9.

---

### 2.5 Normal form determinism in `Make_Poly_Sym_NF` (PALP/Polynf.c:880)

**Property:** For the same input (P, V, E), `Make_Poly_Sym_NF` always produces the same `NF[POLY_Dmax][VERT_Nmax]` output. This is the most critical property — if the normal form is non-deterministic, two identical polytopes could get different hashes.

**Tool:** Frama-C WP (hard) or differential testing (practical)

**Approach:** 
- **Formal (expensive):** Annotate `Make_Poly_Sym_NF` with ACSL postcondition that NF depends only on the mathematical content of (P, V, E), not on memory layout or allocation order. This requires deep annotations through the entire call chain.
- **Practical alternative:** Run the same CWS through `palp_compute_nf` 1000 times and verify identical hashes each time. This is not formal verification but provides high confidence.
- **Hybrid:** Use Frama-C Eva to verify that no uninitialized memory is read during NF computation (which would be a source of non-determinism). Then use WP to verify that the `qsort` in `Sort_VL` uses a deterministic comparator.

**Assumptions:**
- `qsort` is a stable sort or the comparator produces a unique ordering (if `qsort` breaks ties differently across calls, output could vary). **This is a real risk** — `qsort` is not required to be stable by the C standard.
- No use of uninitialized memory in the NF computation chain
- Thread-local workspaces are properly zeroed (verified: `calloc` in `palp_workspace_alloc`)

**Impact:** Non-deterministic normal forms would cause the same polytope to appear multiple times in the output with different hashes. This is a **silent correctness failure** — the output would have more "unique" polytopes than actually exist.

---

### 2.6 `palp_compute_nf` wrapper correctness (palp_api.h:82-128)

**Property:** The wrapper correctly populates the CWS struct, calls PALP functions in the right order, and extracts results.

**Specific checks:**
1. `C->nw = 1, C->N = 6` are correct for single-weight-system 5D CWS
2. `C->d[0] == sum(weights[0..5])` (degree computation)
3. `memset(C, 0, sizeof(CWS))` correctly zeros all fields before population
4. Return value of `Find_Equations` (ip) is correctly interpreted (0 = non-reflexive)
5. `result->dim = P->n` correctly captures the dimension

**Tool:** CBMC (pure C function, no PALP internals needed)

**Approach:** Harness with non-deterministic weights, verify CWS struct fields after population.

**Assumptions:** PALP's internal functions are correct (verified separately in 2.2-2.5).

---

## Part 3: Verification Strategy & Toolchain

### Recommended Multi-Tool Approach

| Layer | Tool | Target | Effort | What it proves |
|-------|------|--------|--------|----------------|
| **Compile-time** | `static_assert` | Struct layout (1.4) | 1 day | Layout matches I/O format |
| **Bounded MC** | ESBMC | key_less (1.1), count preservation (1.5, 1.6), hasher (1.2) | 1-2 weeks | Functional correctness of core algorithms |
| **Bounded MC** | CBMC | hash_normal_form (1.3), SortedBinaryReader (1.7), palp_compute_nf wrapper (2.6) | 1 week | Memory safety + byte-level correctness |
| **Abstract interp.** | Frama-C Eva | PALP array bounds (2.2, 2.3), assert side effects (2.4) | 1 week | No buffer overflows in PALP |
| **Deductive** | Frama-C WP | PALP NF determinism (2.5), loop invariants | 2-4 weeks | Functional correctness of PALP (hardest) |
| **Manual proof** | Paper/Coq | Bucket non-overlap (1.10 property 4) | 1 day | Global dedup = bucket-local dedup |

### What each tool handles

**ESBMC** (for C++ with STL):
- Install: `apt install esbmc` or build from source
- Strengths: Handles `std::vector`, `std::sort`, `std::priority_queue`
- Limitation: Bounded — verifies up to N elements, not arbitrary size. But the merge algorithms are uniform (same code path for all sizes), so bounded results generalize.
- Usage: `esbmc harness.cpp --unwind 10 --z3`

**CBMC** (for C and simple C++):
- Install: `apt install cbmc`
- Strengths: Bit-precise, excellent for C functions with array arithmetic
- Limitation: Cannot handle STL containers. Use only for C-style functions.
- Usage: `cbmc harness.c --function harness --unwind 256 --pointer-check --bounds-check`

**Frama-C** (for PALP C code):
- Install: `opam install frama-c` (requires OCaml toolchain)
- Eva: `frama-c -eva -eva-precision 3 palp_files.c -cpp-extra-args="-DPOLY_Dmax=5 -DPALP_FAST_ASSERT"`
- WP: Add ACSL annotations, then `frama-c -wp -wp-rte annotated_file.c`
- Strength: Best tool for legacy C with globals, macros, pointer arithmetic

### Assumption Summary

| Assumption | Type | Risk | Mitigation |
|------------|------|------|------------|
| `POINT_Nmax` sufficient for all 5D CWS | Empirical | Low (raised to 2M) | Runtime check + abort |
| `VERT_Nmax` sufficient | Empirical | Low (47 observed, 64 limit) | Existing assert |
| `xxHash128` has no collisions | Probabilistic (10⁻²⁰) | Negligible | Accept |
| `std::sort` is correct | Stdlib trust | Negligible | Trust compiler vendor |
| `qsort` in PALP is deterministic for given input | C stdlib property | **Medium** | Verify comparator produces total order (no ties) |
| No uint64_t overflow in count sums | Arithmetic | Low (16.7B << 2⁶⁴) | Assertion |
| `PALP_FAST_ASSERT` always enabled | Build config | Low | CMakeLists.txt enforces it |
| Checkpoint files not corrupted on disk | Environment | Low | Checksum (not implemented) |
| Same platform for write and read | Environment | Low | All x86-64 Linux |

### Implementation Order

**Phase 1 (immediate, 2-3 days):**
1. Add static_asserts for struct layout (1.4) — no tools needed
2. Write ESBMC harness for `key_less` strict weak ordering (1.1)
3. Write CBMC harness for `hash_normal_form` bounds (1.3)

**Phase 2 (1-2 weeks):**
4. Write ESBMC harness for `merge_dedup_parallel` count preservation (1.5)
5. Write ESBMC harness for k-way merge count preservation (1.6)
6. Run Frama-C Eva on PALP `Find_Equations` and `Make_CWS_Points` (2.2, 2.3)

**Phase 3 (2-4 weeks):**
7. Frama-C WP annotations for PALP normal form determinism (2.5)
8. Write paper proof for bucket non-overlap property (1.10)
9. Verify `SortedBinaryReader` state machine (1.7)

### Directory Structure
```
src/verify/
├── CMakeLists.txt              # Build harnesses
├── harness_key_less.cpp        # ESBMC: strict weak ordering
├── harness_hash_nf.c           # CBMC: hash byte layout
├── harness_merge_dedup.cpp     # ESBMC: count preservation
├── harness_kway_merge.cpp      # ESBMC: heap merge counts
├── harness_reader.c            # CBMC: SortedBinaryReader
├── harness_checkpoint_rt.cpp   # ESBMC: write/read round-trip
├── palp_eva.sh                 # Frama-C Eva runner for PALP
├── palp_wp/                    # ACSL-annotated PALP sources
│   ├── Vertex_annotated.c
│   └── Polynf_annotated.c
└── proofs/
    └── bucket_nonoverlap.md    # Manual proof
```

### Verification
- Each harness should be runnable independently: `esbmc harness_key_less.cpp --unwind 10 --z3`
- Add a `make verify` target that runs all harnesses and reports results
- CI integration: run bounded checks on every commit (fast harnesses: ~seconds each)
