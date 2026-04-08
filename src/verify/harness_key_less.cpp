/**
 * harness_key_less.cpp — CBMC harness for key_less strict weak ordering (Plan 1.1)
 *
 * Verifies that key_less is a strict weak ordering over Hash128:
 *   1. Irreflexivity:  !key_less(a, a)
 *   2. Asymmetry:      key_less(a, b) => !key_less(b, a)
 *   3. Transitivity:   key_less(a, b) && key_less(b, c) => key_less(a, c)
 *   4. Equivalence:    !key_less(a, b) && !key_less(b, a) <=> a == b
 *
 * Run: cbmc harness_key_less.cpp --function harness --cpp
 *
 * Assumptions: None. This is a complete proof over the function's domain.
 * CBMC explores all possible values of a, b, c (symbolic 128-bit values).
 */
#include <cassert>
#include "classifier_types.h"

void harness() {
    Hash128 a, b, c;
    /* CBMC assigns non-deterministic values to a, b, c */

    /* 1. Irreflexivity */
    assert(!key_less(a, a));

    /* 2. Asymmetry */
    if (key_less(a, b))
        assert(!key_less(b, a));

    /* 3. Transitivity */
    if (key_less(a, b) && key_less(b, c))
        assert(key_less(a, c));

    /* 4. Equivalence consistency: incomparable implies equal */
    if (!key_less(a, b) && !key_less(b, a))
        assert(a == b);
}
