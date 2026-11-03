template<class F> void launch(F kernel, int blocks = 1, int threads = 1) {
    gridDim.x = blocks; blockDim.x = threads;
    for (unsigned int b = 0; b < gridDim.x; ++b) {
        blockIdx.x = b;
        for (unsigned int t = 0; t < blockDim.x; ++t) {
            threadIdx.x = t;
            kernel();
        }
    }
}

struct Fixture {
    std::vector<DeviceCwsCandidate> original;
    std::vector<int> routes;
    std::vector<unsigned char> keep;
    int error = 0;
    explicit Fixture(int n, bool joined = false) : original(n), routes(n, -1), keep(n, 1) {
        for (int i = 0; i < n; ++i) {
            auto &c = original[i];
            c.structure_id = 2 + i % 46;
            c.nw = 2; c.ambient_vertices = 7;
            for (int r = 0; r < c.nw; ++r) {
                c.sel_index[r] = i * 2 + r;
                for (int a = 0; a < c.ambient_vertices; ++a) {
                    c.weights[r][a] = 1 + (a + r + i) % 3;
                    c.degree[r] += c.weights[r][a];
                }
            }
        }
        launch([&] { initialize_routes_kernel(original.data(), original.size(), joined, routes.data()); }, 3, 4);
        for (int i = 0; i < n; ++i) {
            assert(original[i].pipeline_id == static_cast<unsigned long long>(i));
            assert(routes[i] == (joined ? kRouteSurvivor : kRouteGenerated));
        }
    }
    void selected(const std::vector<DeviceCwsCandidate> &rows) {
        launch([&] { audit_selected_kernel(rows.data(), rows.size(), original.data(), original.size(),
                                           keep.data(), routes.data(), &error); }, 2, 3);
    }
    void compaction() {
        launch([&] { audit_compaction_kernel(keep.data(), routes.data(), original.size(), &error); }, 2, 3);
    }
    void output(const std::vector<DeviceCwsCandidate> &rows, bool accepted) {
        launch([&] { audit_output_kernel(rows.data(), rows.size(), original.data(), original.size(),
            accepted ? kRouteAccepted : kRouteDeferred,
            accepted ? kRouteAcceptedStored : kRouteDeferredStored, routes.data(), &error); }, 3, 2);
    }
    void terminal() {
        launch([&] { audit_terminal_kernel(routes.data(), original.size(), &error); }, 2, 2);
    }
};

unsigned int cases = 0;

void test_valid_routes() {
    std::mt19937 random(20261102);
    for (int n : {0,1,2,7,31,129}) for (int trial = 0; trial < 20; ++trial) {
        Fixture f(n);
        std::vector<DeviceCwsCandidate> selected, accepted, deferred;
        for (int i = 0; i < n; ++i) {
            const int outcome = random() % 4;
            if (outcome == 0) {
                f.keep[i] = 0;
                assert(device_route(f.original[i], kRouteGenerated, kRouteRejected,
                                    f.routes.data(), n, &f.error));
            } else if (outcome == 1) {
                f.keep[i] = 0;
                assert(device_route(f.original[i], kRouteGenerated, kRouteDeferred,
                                    f.routes.data(), n, &f.error));
                deferred.push_back(f.original[i]);
            } else selected.push_back(f.original[i]);
        }
        std::shuffle(selected.begin(), selected.end(), random);
        f.selected(selected); f.compaction(); assert(!f.error);
        std::vector<int> permutation(selected.size());
        std::iota(permutation.begin(), permutation.end(), 0);
        std::shuffle(permutation.begin(), permutation.end(), random);
        std::vector<DeviceCwsCandidate> gathered(selected.size());
        launch([&] { gather_candidates_kernel(selected.data(), permutation.data(), selected.size(),
                                               gathered.data(), f.routes.data(), n, &f.error); }, 3, 5);
        for (const auto &c : gathered) {
            assert(f.routes[c.pipeline_id] == kRoutePoint);
            const bool accept = random() % 2;
            assert(device_route(c, kRoutePoint, accept ? kRouteAccepted : kRouteDeferred,
                                f.routes.data(), n, &f.error));
            (accept ? accepted : deferred).push_back(c);
        }
        std::shuffle(accepted.begin(), accepted.end(), random);
        std::shuffle(deferred.begin(), deferred.end(), random);
        f.output(accepted, true); f.output(deferred, false); f.terminal(); assert(!f.error);
        ++cases;
    }
}

void test_compaction_faults() {
    // Every semantic CWS field and every identity participates in audit checks.
    for (int fault = 0; fault < 10; ++fault) {
        Fixture f(4);
        auto rows = f.original;
        switch (fault) {
        case 0: rows.pop_back(); break;
        case 1: rows[3] = rows[0]; break;
        case 2: ++rows[1].structure_id; break;
        case 3: ++rows[1].degree[0]; break;
        case 4: ++rows[1].weights[1][4]; break;
        case 5: ++rows[1].sel_index[0]; break;
        case 6: rows[1].pipeline_id = 999; break;
        case 7: f.keep[1] = 0; break;
        case 8: rows[1].nw = 0; break;
        case 9: rows[1].ambient_vertices = 11; break;
        }
        f.selected(rows); f.compaction(); assert(f.error);
        ++cases;
    }
    Fixture f(2);
    f.keep[1] = 0; f.routes[1] = kRoutePoint;
    f.selected({f.original[0]}); f.compaction(); assert(f.error);
    ++cases;
}

void test_sort_faults() {
    for (int fault = 0; fault < 4; ++fault) {
        Fixture f(4, true);
        std::vector<int> perm = {0,1,2,3};
        if (fault == 0) perm[1] = -1;
        if (fault == 1) perm[1] = 4;
        if (fault == 2) perm[1] = 0;
        if (fault == 3) f.original[1].pipeline_id = 99;
        std::vector<DeviceCwsCandidate> sorted(4);
        launch([&] { gather_candidates_kernel(f.original.data(), perm.data(), 4, sorted.data(),
                                               f.routes.data(), 4, &f.error); });
        assert(f.error);
        ++cases;
    }
}

void test_output_faults() {
    for (bool accepted : {false,true}) for (int fault = 0; fault < 5; ++fault) {
        Fixture f(3, true);
        std::fill(f.routes.begin(), f.routes.end(), accepted ? kRouteAccepted : kRouteDeferred);
        auto rows = f.original;
        if (fault == 0) rows.pop_back();
        if (fault == 1) rows[1] = rows[0];
        if (fault == 2) ++rows[1].weights[0][0];
        if (fault == 3) rows[1].pipeline_id = 55;
        if (fault == 4) f.routes[1] = kRouteRejected;
        f.output(rows, accepted); f.terminal(); assert(f.error);
        ++cases;
    }
    Fixture f(1, true);
    assert(device_route(f.original[0], kRouteSurvivor, kRoutePoint, f.routes.data(), 1, &f.error));
    assert(!device_route(f.original[0], kRouteSurvivor, kRoutePoint, f.routes.data(), 1, &f.error));
    assert(f.error && f.routes[0] == kRoutePoint);
    ++cases;
}

void test_exhaustive_small_compaction_and_sort() {
    // Independent finite oracle: selected IDs must be exactly the flagged
    // subset, each once, in any order. Exhaust every length<=4 sequence on 3 IDs.
    for (unsigned int mask = 0; mask < 8; ++mask) for (int length = 0; length <= 4; ++length) {
        unsigned int combinations = 1;
        for (int i = 0; i < length; ++i) combinations *= 3;
        for (unsigned int code = 0; code < combinations; ++code) {
            Fixture f(3);
            for (int i = 0; i < 3; ++i) {
                f.keep[i] = (mask >> i) & 1u;
                if (!f.keep[i]) f.routes[i] = i % 2 ? kRouteDeferred : kRouteRejected;
            }
            auto value = code;
            int seen[3] = {};
            bool expected = true;
            std::vector<DeviceCwsCandidate> rows;
            for (int j = 0; j < length; ++j) {
                const unsigned int id = value % 3; value /= 3;
                rows.push_back(f.original[id]);
                if (!f.keep[id] || ++seen[id] > 1) expected = false;
            }
            for (int i = 0; i < 3; ++i) if (f.keep[i] && seen[i] != 1) expected = false;
            f.selected(rows); f.compaction();
            assert(bool(f.error) == !expected); ++cases;
        }
    }
    // Exhaust every purported permutation, including negative and oversized
    // source indices, through the production gather and ownership CAS.
    for (int n = 0; n <= 4; ++n) {
        unsigned int combinations = 1;
        for (int i = 0; i < n; ++i) combinations *= n + 2;
        for (unsigned int code = 0; code < combinations; ++code) {
            Fixture f(n, true);
            auto value = code;
            std::vector<int> permutation(n), seen(n);
            std::vector<DeviceCwsCandidate> output(n);
            bool expected = true;
            for (int i = 0; i < n; ++i) {
                permutation[i] = static_cast<int>(value % (n + 2)) - 1;
                value /= n + 2;
                if (permutation[i] < 0 || permutation[i] >= n) expected = false;
                else if (++seen[permutation[i]] > 1) expected = false;
            }
            launch([&] { gather_candidates_kernel(f.original.data(), permutation.data(), n,
                output.data(), f.routes.data(), n, &f.error); });
            assert(bool(f.error) == !expected);
            if (expected) for (int i = 0; i < n; ++i)
                assert(device_same_candidate(output[i], f.original[permutation[i]]));
            ++cases;
        }
    }
}

void test_defer_capacity() {
    for (unsigned long long capacity : {0ULL,1ULL,2ULL}) {
        Fixture f(2, true);
        std::fill(f.routes.begin(), f.routes.end(), kRoutePoint);
        // One guarded slot on either side catches a speculative overflow write.
        std::vector<DeviceCwsCandidate> storage(capacity + 2);
        storage.front().structure_id = -991; storage.back().structure_id = -992;
        DeviceIpStats stats{};
        unsigned long long count = 0;
        for (const auto &c : f.original)
            device_defer(c, &stats, storage.data() + 1, &count, capacity,
                         f.routes.data(), 2, &f.error);
        assert(count == 2 && stats.deferred == 2);
        assert(storage.front().structure_id == -991 && storage.back().structure_id == -992);
        std::vector<DeviceCwsCandidate> written(storage.begin()+1, storage.begin()+1+std::min(capacity,count));
        f.output(written, false); f.terminal();
        assert(bool(f.error) == (capacity < count));
        ++cases;
    }
    Fixture f(1, true);
    f.routes[0] = kRoutePoint;
    DeviceIpStats stats{}; unsigned long long count = 0;
    DeviceCwsCandidate output[2];
    for (int repeat = 0; repeat < 2; ++repeat)
        device_defer(f.original[0], &stats, output, &count, 2, f.routes.data(), 1, &f.error);
    assert(f.error); ++cases;
}

void test_ip_routing() {
    for (int reason : {0,1,2,3,4,5,99,1000}) {
        Fixture f(1, true);
        f.routes[0] = kRoutePoint;
        std::vector<long long> points(30); points[0] = reason;
        int np = 6, index = 0;
        DeviceIpScratch scratch{}; DeviceIpStats stats{};
        DeviceCwsCandidate accepted[1], deferred[1];
        unsigned long long na = 0, nd = 0;
        launch([&] { ip_check_kernel(f.original.data(), points.data(), &np, 6, &index, 1,
            &scratch, &stats, accepted, &na, 1, deferred, &nd, 1, 1, f.routes.data(), 1, &f.error); });
        // Stub value 1000 accepts; all other values are false with that reason.
        if (reason == 1000) { assert(na == 1 && nd == 0); f.output({accepted[0]}, true); }
        else if (reason == 4) assert(na == 0 && nd == 0 && stats.rejected == 1);
        else if (reason == 1) assert(na == 0 && nd == 0 && stats.rank_deficient == 1);
        else { assert(na == 0 && nd == 1); f.output({deferred[0]}, false); }
        f.terminal(); assert(!f.error); ++cases;
    }
    for (int fault = 0; fault < 4; ++fault) {
        Fixture f(2, true);
        std::fill(f.routes.begin(), f.routes.end(), kRoutePoint);
        std::vector<long long> points(60, 3);  // stub reason 3: deferred
        int np[2] = {6,6}, index[2] = {0,1};
        if (fault == 0) index[1] = -1;
        if (fault == 1) index[1] = 2;
        if (fault == 2) index[1] = 0;
        if (fault == 3) np[1] = 5;
        DeviceIpScratch scratch{}; DeviceIpStats stats{};
        DeviceCwsCandidate accepted[2], deferred[2];
        unsigned long long na = 0, nd = 0;
        launch([&] { ip_check_kernel(f.original.data(), points.data(), np, 6, index, 2,
            &scratch, &stats, accepted, &na, 2, deferred, &nd, 2, 2, f.routes.data(), 2, &f.error); });
        assert(f.error);
        if (fault == 2) assert(na + nd == 1); // CAS blocks duplicate processing
        ++cases;
    }
    // Both IP output branches obey buffer capacity, with missing records caught
    // by terminal identity audit (and by the host's count/capacity check).
    for (long long outcome : {3LL,1000LL}) {
        Fixture f(1, true); f.routes[0] = kRoutePoint;
        std::vector<long long> points(30); points[0] = outcome;
        int np = 6, index = 0;
        DeviceIpScratch scratch{}; DeviceIpStats stats{};
        DeviceCwsCandidate accepted, deferred;
        accepted.structure_id = -771; deferred.structure_id = -772;
        unsigned long long na = 0, nd = 0;
        launch([&] { ip_check_kernel(f.original.data(), points.data(), &np, 6, &index, 1,
            &scratch, &stats, &accepted, &na, 0, &deferred, &nd, 0, 1,
            f.routes.data(), 1, &f.error); });
        assert(na + nd == 1 && accepted.structure_id == -771 && deferred.structure_id == -772);
        f.terminal(); assert(f.error); ++cases;
    }
    // A dropped IP selection leaves an unfinished identity for the terminal audit.
    Fixture f(1, true); f.routes[0] = kRoutePoint; f.terminal(); assert(f.error); ++cases;
}

int main() {
    test_valid_routes(); test_compaction_faults(); test_sort_faults();
    test_output_faults(); test_exhaustive_small_compaction_and_sort();
    test_defer_capacity(); test_ip_routing();
    std::cout << "Pipeline source routing: " << cases
              << " valid/fault-injection cases passed (sequential CUDA shims)\n";
}
