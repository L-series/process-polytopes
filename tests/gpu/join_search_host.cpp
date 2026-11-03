// Exhaust all <=2-record tables on {0,1}^2, independently weighting each
// nonempty fibre by 1, 2 or 3. This checks a larger finite search space than
// the default symbolic harness; it remains a host-compiler check.
void test_exhaustive_join_search() {
    struct Table {
        std::vector<GJoinKey> keys;
        std::vector<GJoinIdx> index;
    };
    std::vector<Table> tables(1);
    const unsigned long long key[4] = {0,1,65536,65537};
    for (int a = 0; a < 4; ++a)
        for (unsigned int ca = 1; ca <= 3; ++ca) {
            tables.push_back({{{key[a],ca,0,0,0}}, {}});
            for (int b = a + 1; b < 4; ++b)
                for (unsigned int cb = 1; cb <= 3; ++cb)
                    tables.push_back({{{key[a],ca,0,0,0}, {key[b],cb,0,0,0}}, {}});
        }
    assert(tables.size() == 67);
    for (auto &table : tables) {
        for (unsigned int i = 0; i < table.keys.size(); ++i) {
            auto word = table.keys[i].key;
            table.index.push_back({((word & 65535ULL) << 16) | (word >> 16),i,0});
        }
        std::sort(table.index.begin(), table.index.end(),
                  [](auto a, auto b) { return a.pkey < b.pkey; });
    }
    unsigned long long cases = 0;
    for (int shape = 0; shape < 4; ++shape) {
        int coords[3][2] = {{0,1}, {1,2}, {0,2}};
        if (shape == 1) { coords[1][0] = 2; coords[1][1] = 1; }
        if (shape == 2) { coords[2][0] = 2; coords[2][1] = 3; }
        if (shape == 3) { coords[1][0] = 2; coords[1][1] = 3; coords[2][0] = 1; coords[2][1] = 3; }
        GJoinPlan plan;
        plan.nrows = 3; plan.ambient = 4;
        SelectedEntry entries[3];
        DeviceCwsCandidate c;
        unsigned long long offsets[3][2] = {};
        unsigned int bound = 0;
        for (int r = 0; r < 3; ++r) {
            plan.order[r] = r;
            plan.size[r] = plan.shared[r] = 2;
            plan.bound_before[r] = bound;
            unsigned int support = 0;
            for (int p = 0; p < 2; ++p) {
                plan.coord[r][p] = coords[r][p]; support |= 1u << coords[r][p];
                entries[r].w[p] = p + 1; c.weights[r][coords[r][p]] = p + 1;
            }
            plan.lookup[r] = (support & ~bound) == 0;
            bound |= support;
            c.sel_index[r] = r;
            plan.table[r].pool_offset = r; plan.table[r].pool_count = 1;
            plan.table[r].offsets = offsets[r];
        }
        for (const auto &a : tables) for (const auto &b : tables) for (const auto &cc : tables) {
            const Table *rows[3] = {&a,&b,&cc};
            for (int r = 0; r < 3; ++r) {
                plan.table[r].keys = rows[r]->keys.data();
                plan.table[r].pidx[1] = rows[r]->index.data();
                offsets[r][1] = rows[r]->keys.size();
            }
            unsigned long long expected = 0;
            for (const auto &ka : a.keys) for (const auto &kb : b.keys) for (const auto &kc : cc.keys) {
                const GJoinKey tuple[3] = {ka,kb,kc};
                int assigned[4] = {-1,-1,-1,-1};
                bool compatible = true;
                for (int r = 0; r < 3; ++r) for (int p = 0; p < 2; ++p) {
                    int value = (tuple[r].key >> (16 * (1-p))) & 65535ULL;
                    int coordinate = coords[r][p];
                    if (assigned[coordinate] >= 0 && assigned[coordinate] != value) compatible = false;
                    assigned[coordinate] = value;
                }
                if (compatible) expected += static_cast<unsigned long long>(ka.count) * kb.count * kc.count;
            }
            unsigned long long actual = 99;
            device_gjoin_decide(c, entries, plan, &actual);
            if (actual != expected) {
                std::cerr << "Exhaustive search mismatch at shape " << shape << ", case " << cases << '\n';
                std::abort();
            }
            ++cases;
        }
    }
    std::cout << "Exhaustive small-table join search: " << cases << " weighted cases\n";
}
