// Appended to the extracted src/gpu join functions by scripts/test-gpu-join-source.py.
// The oracle enumerates global coordinates directly, without row tables or joins.
struct Oracle {
    unsigned long long np = 0;
    unsigned int zero = 0, high = 0, lo[10] = {}, hi[10] = {};
    int verdict(int n) const {
        if (np < 6) return 0;
        if (zero ^ high) return 1;
        for (int a = 0; a < n; ++a) if (lo[a] ^ hi[a]) return 1;
        return 2;
    }
};

Oracle enumerate_direct(const DeviceCwsCandidate &c) {
    Oracle out;
    int x[10] = {}, rem[5] = {};
    for (int r = 0; r < c.nw; ++r) rem[r] = c.degree[r];
    std::function<void(int)> walk = [&](int a) {
        if (a == c.ambient_vertices) {
            for (int r = 0; r < c.nw; ++r) if (rem[r]) return;
            ++out.np;
            for (int i = 0; i < a; ++i) {
                if (x[i] == 0) out.zero |= 1u << i;
                if (x[i] >= 2) out.high |= 1u << i;
                for (int j = i + 1; j < a; ++j) {
                    if (x[i] + x[j] <= 1) out.lo[i] |= 1u << j;
                    if (x[i] + x[j] >= 3) out.hi[i] |= 1u << j;
                }
            }
            return;
        }
        int upper = std::numeric_limits<int>::max();
        for (int r = 0; r < c.nw; ++r)
            if (c.weights[r][a]) upper = std::min(upper, rem[r] / c.weights[r][a]);
        assert(upper != std::numeric_limits<int>::max());
        for (int v = 0; v <= upper; ++v) {
            x[a] = v;
            for (int r = 0; r < c.nw; ++r) rem[r] -= v * c.weights[r][a];
            // A completed row must have no remainder.
            bool possible = true;
            for (int r = 0; r < c.nw; ++r) {
                bool later = false;
                for (int b = a + 1; b < c.ambient_vertices; ++b) later |= c.weights[r][b] != 0;
                if (!later && rem[r]) possible = false;
            }
            if (possible) walk(a + 1);
            for (int r = 0; r < c.nw; ++r) rem[r] += v * c.weights[r][a];
        }
    };
    walk(0);
    return out;
}

template<class F> void must_reject_table(F operation) {
    bool failed = false;
    try { operation(); } catch (const std::runtime_error &) { failed = true; }
    assert(failed);
}

void test_table_checks() {
    SelectedEntry e;
    e.size = 2; e.w[0] = e.w[1] = 1; e.degree = 2;
    std::vector<std::array<int, 5>> pts = {{{0,2,0,0,0}}, {{1,1,0,0,0}}, {{2,0,0,0,0}}};
    std::vector<GJoinKey> fibres = {{0,1,4,0,0}, {1,1,2,0,0}, {2,1,1,0,0}};
    validate_gjoin_fibres(e, 2, 1, pts, fibres);
    for (int corruption = 0; corruption < 5; ++corruption) {
        auto bad = fibres;
        if (corruption == 0) ++bad[0].count;
        if (corruption == 1) bad[1].key = 0;
        if (corruption == 2) bad[0].cats = 1;
        if (corruption == 3) bad[0].pairs = 1;
        if (corruption == 4) bad.pop_back();
        must_reject_table([&] { validate_gjoin_fibres(e, 2, 1, pts, bad); });
    }
    // A consistent table that omits a solution is caught only by the DP count.
    std::vector<std::array<int, 5>> omitted = {{{0,2,0,0,0}}, {{2,0,0,0,0}}};
    std::vector<GJoinKey> omitted_fibres = {{0,1,4,0,0}, {2,1,1,0,0}};
    assert(row_solution_count(e, 2) == 3);
    must_reject_table([&] { validate_gjoin_fibres(e, 2, 1, omitted, omitted_fibres); });
    auto bad_points = pts; bad_points[1] = pts[0];
    must_reject_table([&] { validate_gjoin_fibres(e, 2, 1, bad_points, fibres); });
    bad_points = pts; ++bad_points[0][1];
    must_reject_table([&] { validate_gjoin_fibres(e, 2, 1, bad_points, fibres); });
    std::vector<GJoinKey> keys = {{2,1,0,0,0}, {65537,1,0,0,0}, {131072,1,0,0,0}};
    std::vector<GJoinIdx> index = {{2,2,0}, {65537,1,0}, {131072,0,0}};
    validate_gjoin_index(keys, index, 0, 3, 2, 1);
    for (int corruption = 0; corruption < 4; ++corruption) {
        auto bad = index;
        if (corruption == 0) bad[0].idx = 3;
        if (corruption == 1) bad[1].idx = bad[0].idx;
        if (corruption == 2) std::swap(bad[0], bad[1]);
        if (corruption == 3) ++bad[0].pkey;
        must_reject_table([&] { validate_gjoin_index(keys, bad, 0, 3, 2, 1); });
    }
    // Representation overflow is explicitly uncovered, rather than an empty
    // table that could be mistaken for a degenerate join.
    PoolInfo pool;
    e.w[1] = 65536; e.degree = 65537;
    pool.entries.push_back(e);
    HostGJoinTable table;
    build_gjoin_table(pool, 2, 1, table);
    assert(table.view.covered && !table.view.covered[0]);
    assert(table.view.offsets[0] == table.view.offsets[1]);
    GJoinPlan plan;
    plan.nrows = 1; plan.ambient = 2; plan.table[0] = table.view;
    DeviceCwsCandidate c;
    unsigned long long np = 99;
    assert(device_gjoin_decide(c, pool.entries.data(), plan, &np) == 2 && np == 0);
    std::free(table.d_keys); std::free(table.d_offsets); std::free(table.d_covered);
    e.size = 3; e.w[1] = e.w[2] = 65536; e.degree = 131073;
    std::vector<SelectedEntry> flat = {e};
    HostJoinTables legacy;
    build_join_table(flat, flat.data(), 0, 1, 3, 1, legacy);
    assert(legacy.view.covered && !legacy.view.covered[0]);
    int prefix[3] = {0,0,0};
    np = 99;
    assert(device_join_decide(legacy.view, 0, 0, prefix, prefix, 1024, &np) == 2 && np == 0);
    std::free(legacy.d_keys); std::free(legacy.d_offsets); std::free(legacy.d_covered);
    std::free(legacy.d_masks); std::free(legacy.d_bloom);
}

int main() {
    test_table_checks();
#ifdef CWS_JOIN_EXHAUSTIVE
    test_exhaustive_join_search();
#endif
    // Exhaust every category mask, including empty masks for the low formula.
    for (unsigned int a = 0; a < 16; ++a) for (unsigned int b = 0; b < 16; ++b) {
        bool lo = false, hi = false;
        for (int i = 0; i < 4; ++i) for (int j = 0; j < 4; ++j)
            if (((a >> i) & 1u) && ((b >> j) & 1u)) {
                lo |= i + j <= 1;
                hi |= i + j >= 3;
            }
        assert(bool(device_join_lo(a,b)) == lo);
        if (a && b) assert(bool(device_join_hi(a,b)) == hi);
    }
    for (int v = 0; v < 100; ++v)
        assert(device_join_cat(v) == (1u << std::min(v,3)));

    // All-private synthetic rows exercise count overflow and zero-count
    // fail-open guards independently of the production weight pools.
    {
        GJoinPlan plan;
        plan.nrows = 5;
        plan.ambient = 10;
        GJoinKey key{};
        key.count = std::numeric_limits<unsigned int>::max();
        unsigned long long offsets[2] = {0,1};
        DeviceCwsCandidate c;
        SelectedEntry entry;
        for (int r = 0; r < 5; ++r) {
            plan.order[r] = r;
            plan.lookup[r] = 1;
            plan.size[r] = 2;
            plan.coord[r][0] = 2*r;
            plan.coord[r][1] = 2*r+1;
            plan.table[r].keys = &key;
            plan.table[r].offsets = offsets;
            plan.table[r].pool_count = 1;
        }
        unsigned long long np = 99;
        assert(device_gjoin_decide(c,&entry,plan,&np) == 2 && np == 0);
        key.count = 0;
        assert(device_gjoin_decide(c,&entry,plan,&np) == 2 && np == 0);
        key.count = 1;
        assert(device_gjoin_decide(c,&entry,plan,&np) == 0 && np == 1);
        plan.nrows = 0;
        assert(device_gjoin_decide(c,&entry,plan,&np) == 2 && np == 0);
    }

    // Full-width shared key: the upper endpoint of the 65535 range is
    // 2^64, so an upper-bound implementation must avoid wrapping it to zero.
    // Exercise both canonical position 0 and a position-sorted index.
    for (int bound_position : {0, 2}) {
        GJoinPlan plan;
        plan.nrows = 2; plan.ambient = 5; plan.shared_mask = 15;
        plan.order[0] = 0; plan.order[1] = 1;
        plan.size[0] = plan.shared[0] = 1;
        plan.size[1] = 5; plan.shared[1] = 4;
        plan.bound_before[1] = 1;
        plan.coord[0][0] = 0;
        int next = 1;
        for (int p = 0; p < 4; ++p) plan.coord[1][p] = p == bound_position ? 0 : next++;
        plan.coord[1][4] = 4;
        GJoinKey keys[2] = {};
        keys[0].key = 65535; keys[0].count = 2;
        keys[1].key = 65535ULL << (16 * (3-bound_position)); keys[1].count = 3;
        keys[1].cats = 2;
        GJoinIdx index = {65535ULL << 48, 0, 0};
        unsigned long long offsets[2] = {0,1};
        for (int r = 0; r < 2; ++r) {
            plan.table[r].keys = &keys[r];
            plan.table[r].offsets = offsets;
            plan.table[r].pool_offset = r;
            plan.table[r].pool_count = 1;
        }
        if (bound_position) plan.table[1].pidx[bound_position] = &index;
        SelectedEntry entry[2];
        entry[0].w[0] = 1;
        DeviceCwsCandidate c;
        c.sel_index[1] = 1; c.weights[0][0] = 1;
        for (int p = 0; p < 4; ++p) {
            entry[1].w[p] = p+1;
            c.weights[1][plan.coord[1][p]] = p+1;
        }
        unsigned long long np = 0;
        const int verdict = device_gjoin_decide(c,entry,plan,&np);
        assert(np == 6 && verdict == 1);
    }
    PoolArray pools;
    std::vector<SelectedEntry> entries;
    for (int size = 2; size <= 5; ++size) for (int shared = 0; shared <= size; ++shared) {
        auto &pool = pools[pool_key(size, shared)];
        pool.device_offset = entries.size();
        for (int variant = 0; variant < 4; ++variant) {
            SelectedEntry e;
            e.size = size;
            e.index = entries.size();
            for (int p = 0; p < size; ++p) {
                e.w[p] = variant == 0 ? 1 : variant == 1 ? (p % 2 + 1) : variant == 2 ? (p % 3 + 1) : (p + 1);
                e.degree += e.w[p];
            }
            pool.entries.push_back(e);
            entries.push_back(e);
        }
    }
    std::mt19937 random(20261102);
    HostJoinTables legacy5;
    const auto &p5 = pools[pool_key(5,3)];
    build_join_table(entries, entries.data(), p5.device_offset, p5.entries.size(), 5, 3, legacy5);
    std::array<HostGJoinTable, kPoolKeyStride * kPoolKeyStride> tables;
    unsigned int cases = 0, specialised = 0, verdicts[3] = {};
    for (int id = 2; id <= 47; ++id) {
        GpuDescriptor d{};
        assert(GpuCatalogueDescriptor(id, &d));
        GJoinPlan plan = build_gjoin_plan(d, pools, tables);
        for (int corruption = 0; corruption < 4; ++corruption) {
            auto bad = plan;
            if (corruption == 0) bad.order[1] = bad.order[0];
            if (corruption == 1) bad.bound_before[0] = 1;
            if (corruption == 2) bad.lookup[0] = !bad.lookup[0];
            if (corruption == 3) bad.shared_mask ^= 1;
            must_reject_table([&] { validate_gjoin_plan(bad); });
        }
        unsigned int visited = 0;
        for (int depth = 0; depth < plan.nrows; ++depth) {
            assert(plan.order[depth] >= 0 && plan.order[depth] < plan.nrows);
            assert(!(visited & (1u << plan.order[depth])));
            visited |= 1u << plan.order[depth];
        }
        for (int trial = 0; trial < 12; ++trial) {
            DeviceCwsCandidate c;
            c.structure_id = d.id;
            c.nw = d.row_count;
            c.ambient_vertices = d.coordinate_count;
            for (int s = 0; s < c.nw; ++s) {
                const auto &pool = pools[pool_key(d.row_sizes[s], d.shared_counts[s])];
                c.sel_index[s] = pool.device_offset + (trial == 0 ? 0 : random() % pool.entries.size());
                const auto &e = entries[c.sel_index[s]];
                c.degree[s] = e.degree;
                std::vector<int> perm(d.shared_counts[s]);
                for (int p = 0; p < int(perm.size()); ++p) perm[p] = p;
                // The fused structure-3 kernel emits slot 0 in pool order only;
                // every other slot prefix may be permuted.
                if (!(s == 0 && d.id == 3))
                    std::shuffle(perm.begin(), perm.end(), random);
                for (int p = 0; p < e.size; ++p)
                    c.weights[s][d.mappings[s][p] - 1] = e.w[p < int(perm.size()) ? perm[p] : p];
            }
            const auto expected = enumerate_direct(c);
            unsigned long long np = 0;
            const int actual = device_gjoin_decide(c, entries.data(), plan, &np);
            if (np != expected.np || actual != expected.verdict(c.ambient_vertices)) {
                std::cerr << "Mismatch: structure " << d.id << " trial " << trial
                          << " np " << np << "/" << expected.np
                          << " verdict " << actual << "/" << expected.verdict(c.ambient_vertices) << '\n';
                return 1;
            }
            if (d.id == 3) {
                const auto &right = entries[c.sel_index[1]];
                int prefix[3] = {c.weights[1][0], c.weights[1][1], c.weights[1][2]};
                unsigned long long fused_np = 0;
                assert(device_join_decide(legacy5.view, c.sel_index[0]-p5.device_offset,
                    c.sel_index[1]-p5.device_offset, right.w, prefix,
                    std::numeric_limits<int>::max(), &fused_np) == expected.verdict(c.ambient_vertices));
                assert(fused_np == expected.np);
                assert(device_join_decide(legacy5.view, c.sel_index[0]-p5.device_offset,
                    c.sel_index[1]-p5.device_offset, right.w, prefix, 0, &fused_np) == 2);
                ++specialised;
            }
            ++cases;
            ++verdicts[actual];
            // Missing pool records and unmatched prefix weights must fail open.
            auto bad = c;
            bad.sel_index[0] = -1;
            np = 99;
            assert(device_gjoin_decide(bad, entries.data(), plan, &np) == 2 && np == 0);
        }
    }
    for (auto &t : tables) {
        std::free(t.d_keys);
        std::free(t.d_offsets);
        std::free(t.d_covered);
        for (auto *p : t.d_pidx) std::free(p);
    }
    std::free(legacy5.d_keys); std::free(legacy5.d_offsets); std::free(legacy5.d_covered);
    std::free(legacy5.d_masks); std::free(legacy5.d_bloom);
    std::cout << "Source join agrees with direct enumeration: " << cases << " cases, "
              << "all 46 descriptors; verdicts degenerate/certified/keep = "
              << verdicts[0] << '/' << verdicts[1] << '/' << verdicts[2]
              << "; specialised/cap checks = " << specialised << '\n';
}
