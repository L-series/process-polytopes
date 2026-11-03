// All source-supported row arities, all coordinate injections into the ten
// ambient slots, all packed key bits, bound masks, and signed stale values.
extern "C" unsigned long long nondet_ull();
extern "C" unsigned int nondet_uint();
extern "C" int nondet_int();
extern "C" void __CPROVER_assume(bool);
extern "C" void __CPROVER_assert(bool, const char *);
int main() {
    int k = nondet_uint() & 7u;
    __CPROVER_assume(k <= 4);
    int coordinate[kMaxSize] = {};
    for (int p = 0; p < k; ++p) {
        coordinate[p] = nondet_uint() & 15u;
        __CPROVER_assume(coordinate[p] < 10);
        for (int q = 0; q < p; ++q) __CPROVER_assume(coordinate[p] != coordinate[q]);
    }
    unsigned int bound = nondet_uint() & 1023u;
    unsigned long long key = nondet_ull();
    int before[10], after[10];
    for (int a = 0; a < 10; ++a) before[a] = after[a] = nondet_int();
    bool expected = true;
    for (int p = 0; p < k; ++p) {
        int field = (key >> (16 * (k - 1 - p))) & 65535ULL;
        if (((bound >> coordinate[p]) & 1u) && before[coordinate[p]] != field) expected = false;
    }
    bool ok = device_gjoin_match_key(key, k, coordinate, bound, after);
    __CPROVER_assert(ok == expected, "matcher succeeds exactly when every bound field agrees");
    for (int a = 0; a < 10; ++a) {
        if ((bound >> a) & 1u)
            __CPROVER_assert(after[a] == before[a], "matcher preserves every bound coordinate even on failure");
        bool written = false;
        for (int p = 0; p < k; ++p) written |= coordinate[p] == a;
        if (!written)
            __CPROVER_assert(after[a] == before[a], "matcher preserves coordinates outside this row");
    }
    if (ok) for (int p = 0; p < k; ++p) {
        int field = (key >> (16 * (k - 1 - p))) & 65535ULL;
        __CPROVER_assert(after[coordinate[p]] == field, "successful matcher installs every row field");
    }
#ifdef JOIN_NONVACUITY
    __CPROVER_assert(!ok || k == 0, "nonvacuity: nonempty successful row is reachable");
#endif
}
