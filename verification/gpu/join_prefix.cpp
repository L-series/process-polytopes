// Full-domain source prefix matching: at most four shared positions, arbitrary
// signed weights (including repeats), and arbitrary distinct coordinates.
int nondet_int();

int main() {
    const int k = nondet_int();
    __CPROVER_assume(k >= 0 && k <= 4);
    GJoinPlan plan;
    SelectedEntry E;
    DeviceCwsCandidate c;
    for (int a = 0; a < 10; ++a) c.weights[0][a] = nondet_int();
    for (int p = 0; p < 4; ++p) {
        E.w[p] = nondet_int();
        plan.coord[0][p] = nondet_int();
        __CPROVER_assume(plan.coord[0][p] >= 0 && plan.coord[0][p] < 10);
        for (int q = 0; q < p; ++q)
            __CPROVER_assume(plan.coord[0][p] != plan.coord[0][q]);
    }
    bool expected = true;
    for (int q = 0; q < k; ++q) {
        const int cw = c.weights[0][plan.coord[0][q]];
        int candidate_count = 0, entry_count = 0;
        for (int p = 0; p < k; ++p) {
            candidate_count += c.weights[0][plan.coord[0][p]] == cw;
            entry_count += E.w[p] == cw;
        }
        if (candidate_count != entry_count) expected = false;
    }
    int epos_coord[kMaxSlots][kMaxSize];
    unsigned int used_out = 0;
    const int result = match_prefix(plan, E, c, k, epos_coord, used_out);
    __CPROVER_assert((result == 0) == expected, "prefix success iff weight multisets agree");
    if (result == 0) {
        __CPROVER_assert(used_out == (1u << k) - 1u, "every entry prefix position is used");
        for (int p = 0; p < k; ++p) {
            const int a = epos_coord[0][p];
            __CPROVER_assert(a >= 0 && a < 10, "matched coordinate is in bounds");
            __CPROVER_assert(E.w[p] == c.weights[0][a], "matching preserves the weight at each position");
            bool present = false;
            for (int q = 0; q < k; ++q) present = present || a == plan.coord[0][q];
            __CPROVER_assert(present, "matching uses only candidate prefix coordinates");
            for (int q = 0; q < p; ++q)
                __CPROVER_assert(a != epos_coord[0][q], "matching is bijective, even with repeated weights");
        }
    }
}
