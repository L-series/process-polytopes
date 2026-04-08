/**
 * harness_layout.cpp — CBMC harness for MergeRecord binary layout (Plan 1.4)
 *
 * Verifies struct layout assumptions that the checkpoint I/O relies on.
 *
 * Run: cbmc harness_layout.cpp --function harness --cpp11
 *
 * Note: Uses __CPROVER_assert directly instead of cassert to avoid
 * CBMC issues with __builtin_FILE/__builtin_LINE in libstdc++ assert.
 *
 * Assumptions: x86-64 Linux with standard alignment rules.
 */
#include <cstddef>
#include <cstdint>
#include "classifier_types.h"

void harness() {
    /* Size checks */
    __CPROVER_assert(sizeof(Hash128) == 16,
        "Hash128 must be 16 bytes");
    __CPROVER_assert(sizeof(MergeRecord) == sizeof(Hash128) + sizeof(PolytopeInfo),
        "MergeRecord has no inter-member padding");

    /* Offset checks via pointer arithmetic */
    MergeRecord r;
    char *base = (char *)&r;
    char *key_ptr = (char *)&r.key;
    char *info_ptr = (char *)&r.info;

    __CPROVER_assert(key_ptr == base,
        "MergeRecord::key is at offset 0");
    __CPROVER_assert(info_ptr == base + 16,
        "MergeRecord::info is at offset 16");
    __CPROVER_assert(info_ptr - key_ptr == 16,
        "info follows key with no gap");
}
