#include <assert.h>

long nondet_long(void);
void __CPROVER_assume(int);

static int Dim5IndexBelongsToShard(long index, long shard_count,
                                   long shard_index) {
    if (shard_count <= 1)
        return 1;
    return ((index - 1) % shard_count) == shard_index;
}

void harness(void) {
    long index = nondet_long();
    long shard_count = nondet_long();
    long shard_index = nondet_long();
    long other_shard;
    int membership_count = 0;

    __CPROVER_assume(index >= 1 && index <= 64);
    __CPROVER_assume(shard_count >= 1 && shard_count <= 8);
    __CPROVER_assume(shard_index >= 0 && shard_index < shard_count);

    for (other_shard = 0; other_shard < shard_count; other_shard++)
        membership_count += Dim5IndexBelongsToShard(index, shard_count,
                                                    other_shard);

    assert(membership_count == 1);

    if (Dim5IndexBelongsToShard(index, shard_count, shard_index)) {
        for (other_shard = 0; other_shard < shard_count; other_shard++) {
            if (other_shard == shard_index)
                continue;
            assert(!Dim5IndexBelongsToShard(index, shard_count, other_shard));
        }
    }
}