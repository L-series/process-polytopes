/**
 * harness_hasher.cpp — CBMC harness for Hash128Hasher consistency (Plan 1.2)
 *
 * Verifies that equal Hash128 values produce equal hashes:
 *   a == b => Hash128Hasher()(a) == Hash128Hasher()(b)
 *
 * This is required for std::unordered_map correctness.
 *
 * Run: cbmc harness_hasher.cpp --function harness --cpp
 *
 * Assumptions: None.
 */
#include <cassert>
#include "classifier_types.h"

void harness() {
    Hash128 a, b;
    /* CBMC assigns non-deterministic values */

    if (a == b) {
        Hash128Hasher hasher;
        assert(hasher(a) == hasher(b));
    }
}
