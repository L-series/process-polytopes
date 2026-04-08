/**
 * harness_hash_nf.cpp — CBMC harness for hash_normal_form bounds (Plan 1.3)
 *
 * Verifies that hash_normal_form:
 *   1. Fills exactly dim * nv elements into buf (no off-by-one)
 *   2. All array accesses nf[i][j] satisfy 0 <= i < dim, 0 <= j < nv
 *   3. The byte count passed to the hash function is k * sizeof(Long)
 *
 * We use small symbolic bounds (DMAX=3, VMAX=4) to keep CBMC tractable.
 * The function's loop structure is uniform: it iterates dim*nv times
 * regardless of the actual values, so correctness at small bounds
 * generalises to POLY_Dmax=5, VERT_Nmax=64.
 *
 * Run: cbmc harness_hash_nf.cpp --function harness --cpp11 \
 *      --bounds-check --pointer-check --unwind 15
 *
 * Assumptions:
 *   - dim in [1, DMAX] and nv in [1, VMAX] (guaranteed by PALP)
 *   - Loop structure is identical to classifier.cpp's hash_normal_form
 */
#include <cassert>
#include <cstddef>
#include <cstdint>

/* Small bounds to keep CBMC memory-safe */
#define DMAX 3
#define VMAX 4

typedef long Long;

struct Hash128 {
    uint64_t lo, hi;
};

/* Stub for XXH3_128bits — records the size argument for verification */
static size_t xxh_last_len;

struct XXH128_hash_t {
    uint64_t low64, high64;
};

XXH128_hash_t XXH3_128bits(const void *data, size_t len) {
    xxh_last_len = len;
    XXH128_hash_t h;
    h.low64 = 0;
    h.high64 = 0;
    return h;
}

/*
 * Exact logic of hash_normal_form from classifier.cpp, using small bounds.
 * The code structure (nested for-loop, k counter, buf layout) is identical.
 */
static Hash128 hash_normal_form(const Long nf[DMAX][VMAX],
                                int dim, int nv) {
    Long buf[DMAX * VMAX];
    int k = 0;
    for (int i = 0; i < dim; i++)
        for (int j = 0; j < nv; j++)
            buf[k++] = nf[i][j];

    XXH128_hash_t h = XXH3_128bits(buf, k * sizeof(Long));
    Hash128 result;
    result.lo = h.low64;
    result.hi = h.high64;
    return result;
}

void harness() {
    int dim, nv;

    __CPROVER_assume(dim >= 1 && dim <= DMAX);
    __CPROVER_assume(nv >= 1 && nv <= VMAX);

    Long nf[DMAX][VMAX];

    Hash128 result = hash_normal_form(nf, dim, nv);

    /* Property: k == dim * nv (verified via the byte count passed to xxHash) */
    assert(xxh_last_len == (size_t)(dim * nv) * sizeof(Long));
}
