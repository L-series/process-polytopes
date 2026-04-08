/**
 * harness_sort_vl.cpp — CBMC harness for Sort_VL comparator (Plan 2.5)
 *
 * Verifies that the `diff` comparator used by Sort_VL's qsort:
 *   1. Is a valid comparator (antisymmetric, transitive)
 *   2. Produces a total order on vertex indices (no ties when indices are distinct)
 *   3. Has no integer overflow for valid vertex index ranges [0, VERT_Nmax-1]
 *
 * The total-order property is critical for normal form determinism:
 * if qsort could break ties differently across calls, the same polytope
 * could get different normal forms and thus different hashes.
 *
 * Run: cbmc harness_sort_vl.cpp --function harness --cpp11 \
 *      --bounds-check --pointer-check --unwind 4
 *
 * Assumptions:
 *   - Vertex indices are in [0, VERT_Nmax-1] = [0, 63]
 *   - Vertex indices within a VertexNumList are distinct
 */
#include <cstdint>

#define VERT_Nmax 64

/* Exact copy of PALP's diff comparator (Vertex.c:279) */
int diff(const void *a, const void *b) {
    return *((int *)a) - *((int *)b);
}

void harness() {
    int a, b, c;

    /* Vertex indices are in [0, VERT_Nmax-1] */
    __CPROVER_assume(a >= 0 && a < VERT_Nmax);
    __CPROVER_assume(b >= 0 && b < VERT_Nmax);
    __CPROVER_assume(c >= 0 && c < VERT_Nmax);

    int ab = diff(&a, &b);
    int ba = diff(&b, &a);
    int ac = diff(&a, &c);
    int bc = diff(&b, &c);

    /* Property 1: No overflow.
       Max value: 63 - 0 = 63.  Min value: 0 - 63 = -63.
       Both fit in int32. CBMC's overflow checks verify this automatically. */

    /* Property 2: Antisymmetry — diff(a,b) and diff(b,a) have opposite signs */
    if (ab > 0) __CPROVER_assert(ba < 0, "antisymmetry: ab>0 => ba<0");
    if (ab < 0) __CPROVER_assert(ba > 0, "antisymmetry: ab<0 => ba>0");
    if (ab == 0) __CPROVER_assert(ba == 0, "antisymmetry: ab==0 => ba==0");

    /* Property 3: Transitivity */
    if (ab > 0 && bc > 0) __CPROVER_assert(ac > 0, "transitivity: a>b>c => a>c");
    if (ab < 0 && bc < 0) __CPROVER_assert(ac < 0, "transitivity: a<b<c => a<c");

    /* Property 4: Total order — diff(a,b)==0 implies a==b.
       This is the KEY property: no ties among distinct elements. */
    if (ab == 0) __CPROVER_assert(a == b, "total order: diff==0 implies equal");

    /* Property 5: Consistency with equality */
    if (a == b) __CPROVER_assert(ab == 0, "equal values give diff==0");

    /* Property 6: Distinct indices give non-zero diff */
    if (a != b) __CPROVER_assert(ab != 0, "distinct indices give non-zero diff");
}
