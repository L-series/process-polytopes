// Included after verbatim production declarations; no join algorithm is copied.
// Arbitrary small row tables: sorted unique keys, positive fibre counts, and
// position indexes which are permutations of those keys. Compare every leaf
// against an independent Cartesian-product compatibility test.
extern "C" unsigned int nondet_uint();
extern "C" void __CPROVER_assume(bool);
extern "C" void __CPROVER_assert(bool, const char *);

int main() {
    GJoinPlan plan;
    plan.nrows = JOIN_ROWS;
    plan.ambient = 4;
    const int coords[3][2] = {
        {0, 1},
#if JOIN_SHAPE == 1
        {2, 1},
#elif JOIN_SHAPE == 3
        {2, 3},
#else
        {1, 2},
#endif
#if JOIN_SHAPE == 2
        {2, 3}
#elif JOIN_SHAPE == 3
        {1, 3}
#else
        {0, 2}
#endif
    };
    GJoinKey keys[3][2] = {};
    GJoinIdx px[3][2] = {};
    unsigned long long offsets[3][2] = {};
    SelectedEntry entries[3];
    DeviceCwsCandidate c;
    unsigned int bound = 0;
    for (int r = 0; r < JOIN_ROWS; ++r) {
        unsigned int n = nondet_uint() & (JOIN_MAX_KEYS == 1 ? 1u : 3u);
        __CPROVER_assume(n <= JOIN_MAX_KEYS);
        offsets[r][1] = n;
        plan.order[r] = r;
        plan.size[r] = plan.shared[r] = 2;
        plan.bound_before[r] = bound;
        unsigned int support = 0;
        for (int p = 0; p < 2; ++p) {
            plan.coord[r][p] = coords[r][p];
            support |= 1u << coords[r][p];
            entries[r].w[p] = p + 1;
            c.weights[r][coords[r][p]] = p + 1;
        }
        plan.lookup[r] = (support & ~bound) == 0;
        bound |= support;
        c.sel_index[r] = r;
        plan.table[r].covered = 0;
        plan.table[r].pool_offset = r;
        plan.table[r].pool_count = 1;
        plan.table[r].keys = keys[r];
        plan.table[r].offsets = offsets[r];
        plan.table[r].pidx[1] = px[r];
        for (int i = 0; i < JOIN_MAX_KEYS; ++i) {
            unsigned int a = nondet_uint() & 1u, b = nondet_uint() & 1u;
#ifdef JOIN_WEIGHTED
            unsigned int count = nondet_uint() & 3u;
            __CPROVER_assume(count >= 1 && count <= 3);
#else
            const unsigned int count = 1;
#endif
            keys[r][i].key = (static_cast<unsigned long long>(a) << 16) | b;
            keys[r][i].count = count;
            px[r][i].pkey = (static_cast<unsigned long long>(b) << 16) | a;
            px[r][i].idx = i;
        }
        __CPROVER_assume(n < 2 || keys[r][0].key < keys[r][1].key);
        if (n == 2 && px[r][0].pkey > px[r][1].pkey) {
            GJoinIdx tmp = px[r][0]; px[r][0] = px[r][1]; px[r][1] = tmp;
        }
    }
    // Summary masks are irrelevant here: the obligation is traversal/count.
    // Zero suppresses category work without changing search or accumulation.
    plan.shared_mask = 0;
    unsigned long long expected = 0;
    for (unsigned int i = 0; i < offsets[0][1]; ++i)
        for (unsigned int j = 0; j < offsets[1][1]; ++j)
            for (unsigned int k = 0; k < (JOIN_ROWS == 3 ? offsets[2][1] : 1); ++k) {
                const unsigned int index[3] = {i,j,k};
                int assignment[4] = {-1,-1,-1,-1};
                bool compatible = true;
                for (int r = 0; r < JOIN_ROWS; ++r)
                    for (int p = 0; p < 2; ++p) {
                        int v = (keys[r][index[r]].key >> (16 * (1-p))) & 65535ULL;
                        int a = coords[r][p];
                        if (assignment[a] >= 0 && assignment[a] != v) compatible = false;
                        assignment[a] = v;
                    }
                if (compatible)
                    expected += static_cast<unsigned long long>(keys[0][i].count)
                              * keys[1][j].count * (JOIN_ROWS == 3 ? keys[2][k].count : 1);
            }
    unsigned long long actual = 99;
    device_gjoin_decide(c, entries, plan, &actual);
    __CPROVER_assert(actual == expected, "join count equals brute-force compatible fibre product");
#ifdef JOIN_NONVACUITY
    __CPROVER_assert(expected == 0, "nonvacuity: a nonempty join is reachable");
#endif
}
