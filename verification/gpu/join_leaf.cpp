// CBMC queries for the join's leaf accumulation and verdict (src/gpu/join.cuh),
// against a plain per-coordinate specification (no bit tricks), on symbolic
// inputs: any plan layout of JOIN_ROWS rows over 10 coordinates, any
// chosen fibre masks, any shared values, any prior mask accumulator.
//  * LEAF: with unit counts, device_gjoin_leaf sets every checked mask bit to
//    prior OR this block's contribution; count/overflow is proved in Lean.
//  * VERDICT: device_gjoin_verdict equals the specified verdict.
// Count guards are separately transcribed and proved in JoinLeafCount.lean.
// Here counts are one and the prior count is zero: masks do not depend on counts.

int nondet_int();
unsigned int nondet_uint();
unsigned long long nondet_ull();

static bool bit(unsigned int m, int i) { return (m >> i) & 1u; }

#ifdef FFS
int main() {
    const unsigned int x = nondet_uint();
    const int r = __ffs(x);
    __CPROVER_assert((x == 0) == (r == 0), "ffs zero case");
    if (x) {
        __CPROVER_assert(r >= 1 && r <= 32, "ffs range");
        __CPROVER_assert(((x >> (r - 1)) & 1u) == 1u, "ffs selects a set bit");
        __CPROVER_assert((x & ((1u << (r - 1)) - 1u)) == 0, "ffs selects the least set bit");
    }
}
#endif

#ifdef LEAF
int main() {
    GJoinPlan plan;
    const int R = JOIN_ROWS;
    plan.nrows = R;
    plan.ambient = 10;
    plan.shared_mask = nondet_uint() & 0x3FFu;
    GJoinKey records[kMaxSlots];
    unsigned long long chosen[kMaxSlots];
    for (int j = 0; j < R; ++j) {
        plan.order[j] = j;
        const int size = nondet_int(), k = nondet_int();
        __CPROVER_assume(size >= 1 && size <= kMaxSize && k >= 0 && k <= size);
        plan.size[j] = size;
        plan.shared[j] = k;
        for (int p = 0; p < kMaxSize; ++p) {
            plan.coord[j][p] = nondet_int();
            __CPROVER_assume(plan.coord[j][p] >= 0 && plan.coord[j][p] < 10);
        }
        records[j].key = 0;
        records[j].count = 1;
        records[j].cats = nondet_uint();
        records[j].pairs = nondet_uint();
        plan.table[j].keys = records;
        chosen[j] = j;
    }
    int val[10];
    for (int A = 0; A < 10; ++A) { val[A] = nondet_int(); __CPROVER_assume(val[A] >= 0 && val[A] <= 65535); }
    GJoinAcc acc;
    acc.np = 0;
    acc.tz = nondet_uint(); acc.t2 = nondet_uint(); acc.m3_all = nondet_uint();
    for (int A = 0; A < 10; ++A) {
        acc.LO[A] = nondet_uint(); acc.HI[A] = nondet_uint(); acc.LS[A] = nondet_uint(); acc.HS[A] = nondet_uint();
    }
    const GJoinAcc before = acc;

    // Arbitrary output coordinates suffice for all bits; tracking just these
    // two avoids a symbolic write into a 10x4 specification array at every
    // private position. This is universal quantification, not sampling.
    const int A = nondet_int(), B = nondet_int();
    __CPROVER_assume(A >= 0 && A < 10 && B >= 0 && B < 10);
    bool pa[4], pb[4];
    for (int c = 0; c < 4; ++c) {
        pa[c] = bit(plan.shared_mask, A) && (val[A] < 3 ? val[A] == c : c == 3);
        pb[c] = bit(plan.shared_mask, B) && (val[B] < 3 ? val[B] == c : c == 3);
    }
    unsigned long long cnt = 1;
    bool ls = false, hs = false;
    for (int j = 0; j < R; ++j) {
        const GJoinKey &g = records[j];
        cnt *= g.count;
        const int k = plan.shared[j], npriv = plan.size[j] - k;
        for (int t = 0; t < npriv; ++t)
            for (int c = 0; c < 4; ++c)
                if (bit(g.cats, 4 * t + c)) {
                    if (plan.coord[j][k + t] == A) pa[c] = true;
                    if (plan.coord[j][k + t] == B) pb[c] = true;
                }
        int u = 0;
        for (int t1 = 0; t1 < npriv; ++t1)
            for (int t2 = t1 + 1; t2 < npriv; ++t2, ++u) {
                int a = plan.coord[j][k + t1], b = plan.coord[j][k + t2];
                if (a > b) { const int tmp = a; a = b; b = tmp; }
                if (a == A && b == B) {
                    if (bit(g.pairs, 2 * u)) ls = true;
                    if (bit(g.pairs, 2 * u + 1)) hs = true;
                }
            }
    }

    const bool ok = device_gjoin_leaf(plan, chosen, val, acc);
    __CPROVER_assert(ok, "unit counts cannot overflow");
#ifdef NONVACUITY
    __CPROVER_assert(!ok, "reachable conclusive leaf");
#endif
    if (ok) {
        __CPROVER_assert(acc.np == before.np + cnt, "np adds the product of the fibre counts");
        const bool ge1 = pa[1] || pa[2] || pa[3];
#if MASK_GROUP == 0
        __CPROVER_assert(bit(acc.tz, A) == (bit(before.tz, A) || pa[0]), "tz: some X_A == 0");
        __CPROVER_assert(bit(acc.t2, A) == (bit(before.t2, A) || pa[2] || pa[3]), "t2: some X_A >= 2");
        __CPROVER_assert(bit(acc.m3_all, A) == (bit(before.m3_all, A) || pa[3]), "m3_all: some X_A >= 3");
#endif
        const bool bge1 = pb[1] || pb[2] || pb[3];
        const bool lo = (pa[0] && (pb[0] || pb[1])) || (pa[1] && pb[0]);
        const bool hi = pa[3] || (pa[2] && bge1) || (ge1 && pb[2]);
#if MASK_GROUP == 1
        __CPROVER_assert(bit(acc.LO[A], B) == (bit(before.LO[A], B) || lo), "LO: classes with sum <= 1");
#elif MASK_GROUP == 2
        __CPROVER_assert(bit(acc.HI[A], B) == (bit(before.HI[A], B) || hi), "HI: classes with sum >= 3");
#elif MASK_GROUP == 3
        __CPROVER_assert(bit(acc.LS[A], B) == (bit(before.LS[A], B) || ls), "LS: same-row pair <= 1");
#elif MASK_GROUP == 4
        __CPROVER_assert(bit(acc.HS[A], B) == (bit(before.HS[A], B) || hs), "HS: same-row pair >= 3");
#endif
    }
    return 0;
}
#endif

#ifdef VERDICT
int main() {
    GJoinPlan plan;
    const int N = nondet_int();
    __CPROVER_assume(N >= 1 && N <= 10);
    plan.ambient = N;
    for (int A = 0; A < 10; ++A) plan.same_row[A] = nondet_uint();
    GJoinAcc acc;
    acc.np = nondet_ull();
    acc.tz = nondet_uint(); acc.t2 = nondet_uint(); acc.m3_all = nondet_uint();
    for (int A = 0; A < 10; ++A) {
        acc.LO[A] = nondet_uint(); acc.HI[A] = nondet_uint(); acc.LS[A] = nondet_uint(); acc.HS[A] = nondet_uint();
    }
    int expected = 2;
    bool one_sided = false;
    for (int A = 0; A < N; ++A) {
        if (bit(acc.tz, A) != bit(acc.t2, A)) one_sided = true;
        for (int B = A + 1; B < N; ++B) {
            const bool same = bit(plan.same_row[A], B);
            const bool lo = same ? bit(acc.LS[A], B) : bit(acc.LO[A], B);
            const bool hi = same ? bit(acc.HS[A], B) : (bit(acc.HI[A], B) || bit(acc.m3_all, B));
            if (lo != hi) one_sided = true;
        }
    }
    if (acc.np < 6) expected = 0;
    else if (one_sided) expected = 1;
    __CPROVER_assert(device_gjoin_verdict(acc, plan) == expected, "verdict equals the specification");
    return 0;
}
#endif
