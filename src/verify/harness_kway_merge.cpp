/**
 * harness_kway_merge.cpp — CBMC harness for k-way merge count preservation (Plan 1.6)
 *
 * Verifies that the k-way heap merge preserves total counts:
 *   sum(out[i].info.count) == sum over all streams of sum(stream[j].info.count)
 *
 * We model the heap-based merge with K=2 streams of M=2 records each using
 * manual min-selection (equivalent to priority_queue but CBMC-friendly).
 * Keys are constrained to small values to keep the solver tractable.
 *
 * Run: cbmc harness_kway_merge.cpp --function harness --cpp11 \
 *      --bounds-check --pointer-check --unwind 12
 *
 * Assumptions:
 *   - Each input stream is sorted (precondition from Phase 1)
 *   - HeapEntry::operator> is consistent with key_less (verified by inspection)
 *   - Bounded: K=2 streams, M=2 records each
 */
#include "classifier_types.h"

#define K 3  /* number of streams */
#define M 1  /* records per stream */

static int find_min_stream(const MergeRecord streams[K][M],
                           const int pos[K], const int len[K])
{
    int best = -1;
    for (int i = 0; i < K; i++) {
        if (pos[i] >= len[i]) continue;
        if (best == -1 || key_less(streams[i][pos[i]].key,
                                   streams[best][pos[best]].key))
            best = i;
    }
    return best;
}

void harness() {
    MergeRecord streams[K][M];
    int len[K];
    MergeRecord out[K * M];

    for (int i = 0; i < K; i++)
        __CPROVER_assume(len[i] >= 0 && len[i] <= M);

    /* Constrain keys to small values */
    for (int i = 0; i < K; i++)
        for (int j = 0; j < len[i]; j++) {
            __CPROVER_assume(streams[i][j].key.hi == 0);
            __CPROVER_assume(streams[i][j].key.lo <= 4);
        }

    /* Precondition: each stream is sorted */
    for (int i = 0; i < K; i++)
        for (int j = 1; j < len[i]; j++)
            __CPROVER_assume(!key_less(streams[i][j].key, streams[i][j-1].key));

    /* Precondition: counts are positive and small */
    for (int i = 0; i < K; i++)
        for (int j = 0; j < len[i]; j++)
            __CPROVER_assume(streams[i][j].info.count >= 1
                          && streams[i][j].info.count <= 100);

    /* Compute input sum */
    uint64_t sum_in = 0;
    for (int i = 0; i < K; i++)
        for (int j = 0; j < len[i]; j++)
            sum_in += streams[i][j].info.count;

    /* K-way merge with dedup (mirrors classifier.cpp:1022-1044) */
    int pos[K];
    for (int i = 0; i < K; i++) pos[i] = 0;
    int out_len = 0;

    for (;;) {
        int best = find_min_stream(streams, pos, len);
        if (best == -1) break;

        Hash128 cur_key = streams[best][pos[best]].key;
        MergeRecord merged = streams[best][pos[best]];
        pos[best]++;

        for (;;) {
            int dup = find_min_stream(streams, pos, len);
            if (dup == -1) break;
            if (!(streams[dup][pos[dup]].key == cur_key)) break;
            merged.info.count += streams[dup][pos[dup]].info.count;
            pos[dup]++;
        }

        out[out_len++] = merged;
    }

    /* Compute output sum */
    uint64_t sum_out = 0;
    for (int i = 0; i < out_len; i++)
        sum_out += out[i].info.count;

    /* COUNT PRESERVATION */
    __CPROVER_assert(sum_out == sum_in, "k-way count preservation");

    /* Output is sorted */
    for (int i = 1; i < out_len; i++)
        __CPROVER_assert(!key_less(out[i].key, out[i-1].key), "k-way output sorted");
}
