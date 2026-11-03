// Full-width packed keys: verify the actual binary range search independently
// of the bounded DFS harness. Both canonical and position-sorted access paths.
extern "C" unsigned long long nondet_ull();
extern "C" unsigned int nondet_uint();
extern "C" void __CPROVER_assume(bool);
extern "C" void __CPROVER_assert(bool, const char *);

int main() {
    GJoinPlan plan;
    plan.order[0] = 0;
    plan.lookup[0] = 0;
    plan.shared[0] = 4;
    const int position = JOIN_POSITION;
    int mapping[kMaxSlots][kMaxSize] = {};
    for (int p = 0; p < 4; ++p) mapping[0][p] = p;
    plan.bound_before[0] = 1u << position;
    GJoinKey keys[2] = {};
    GJoinIdx px[2] = {};
    unsigned int n = nondet_uint() & 3u;
    __CPROVER_assume(n <= 2);
    for (int i = 0; i < 2; ++i) {
        keys[i].key = nondet_ull();
        unsigned long long permuted = (keys[i].key >> (16 * (3-position))) & 65535ULL;
        for (int p = 0; p < 4; ++p)
            if (p != position) permuted = (permuted << 16) | ((keys[i].key >> (16 * (3-p))) & 65535ULL);
        px[i].pkey = permuted;
        px[i].idx = i;
    }
    __CPROVER_assume(n < 2 || keys[0].key < keys[1].key);
    if (n == 2 && px[0].pkey > px[1].pkey) {
        GJoinIdx tmp = px[0]; px[0] = px[1]; px[1] = tmp;
    }
    plan.table[0].keys = keys;
    plan.table[0].pidx[position] = px;
    int values[10] = {};
    values[position] = nondet_uint() & 65535u;
    unsigned long long begin[kMaxSlots] = {}, finish[kMaxSlots] = {n};
    unsigned long long it[kMaxSlots] = {}, end[kMaxSlots] = {};
    int pstar[kMaxSlots] = {};
    device_gjoin_begin_level(0, plan, mapping, values, begin, finish, it, end, pstar);
    __CPROVER_assert(it[0] <= end[0] && end[0] <= n && pstar[0] == position,
                     "range endpoints and selected position are valid");
    unsigned int count = 0;
    for (unsigned int i = 0; i < n; ++i) {
        unsigned long long key = position == 0 ? keys[i].key : keys[px[i].idx].key;
        bool matches = ((key >> (16 * (3-position))) & 65535ULL)
                     == static_cast<unsigned int>(values[position]);
        if (matches) ++count;
        __CPROVER_assert(matches == (it[0] <= i && i < end[0]),
                         "binary range contains every and only matching key");
    }
    __CPROVER_assert(end[0] - it[0] == count, "range length equals matching key count");
#ifdef JOIN_NONVACUITY
    __CPROVER_assert(count == 0, "nonvacuity: a nonempty join is reachable");
#endif
}
