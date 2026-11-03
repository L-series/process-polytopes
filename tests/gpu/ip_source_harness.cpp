using Point = std::array<long long,5>;

int classify(const std::vector<Point> &points, int *reason = nullptr) {
    DeviceIpScratch scratch{};
    int local_reason = 0;
    const int result = device_ip_check(points.front().data(), points.size(), &scratch, &local_reason);
    if (!result && (local_reason == 4 || local_reason == 1)) {
        const auto &eq = scratch.rejecting_equation;
        bool nonzero = false;
        for (auto a : eq.a) nonzero |= a != 0;
        assert(nonzero && eq.c <= 0 && !scratch.arithmetic_failed);
        for (const auto &point : points) {
            __int128 value = eq.c;
            for (int i = 0; i < 5; ++i) value += __int128(eq.a[i]) * point[i];
            assert(value >= 0);
        }
    }
    if (reason) *reason = local_reason;
    return result;
}

void expect(const std::vector<Point> &points, int ip) {
    int reason = 0;
    const int actual = classify(points, &reason);
    if (actual != ip) {
        std::cerr << "Unexpected IP result " << actual << ", expected " << ip << ", reason " << reason << '\n';
        std::abort();
    }
}

int main() {
    // Exact independent 128-bit oracle for checked arithmetic, including every
    // pair of signed-limit boundary values and randomly sampled int64 pairs.
    std::mt19937_64 arithmetic_random(20261102);
    std::vector<long long> limits = {std::numeric_limits<long long>::min(),
        std::numeric_limits<long long>::min()+1, -2, -1, 0, 1, 2,
        std::numeric_limits<long long>::max()-1, std::numeric_limits<long long>::max()};
    auto check_pair = [&](long long a, long long b) {
        for (int op = 0; op < 3; ++op) {
            __int128 exact = op == 0 ? __int128(a)+b : op == 1 ? __int128(a)-b : __int128(a)*b;
            bool fits = exact >= std::numeric_limits<long long>::min() && exact <= std::numeric_limits<long long>::max();
            int failed = 0;
            long long got = op == 0 ? device_ip_add(a,b,&failed) : op == 1 ? device_ip_sub(a,b,&failed) : device_ip_mul(a,b,&failed);
            assert(bool(failed) == !fits);
            if (fits) assert(got == exact);
        }
        for (int op = 0; op < 2; ++op) {
            int failed = 0;
            bool valid = b != 0 && !(a == std::numeric_limits<long long>::min() && b == -1);
            long long got = op == 0 ? device_ip_div(a,b,&failed) : device_ip_rem(a,b,&failed);
            assert(bool(failed) == !valid);
            if (valid) assert(got == (op == 0 ? a/b : a%b));
        }
    };
    for (long long a : limits) for (long long b : limits) check_pair(a,b);
    for (int i = 0; i < 50000; ++i) check_pair(arithmetic_random(),arithmetic_random());

    // Acceptance certificate: found for the cross-polytope, and any tampering
    // with its multiplicities, scale, adjugate or indices fails the checker.
    {
        std::vector<Point> cross_points = {{0,0,0,0,0}};
        for (int i = 0; i < 5; ++i) for (int sign : {1,-1}) { Point p{}; p[i] = sign; cross_points.push_back(p); }
        DeviceAcceptance5 cert; int failed = 0;
        const long long *data = cross_points.front().data();
        const int count = static_cast<int>(cross_points.size());
        std::vector<int> all(count); for (int i = 0; i < count; ++i) all[i] = i;
        assert(device_ip_find_acceptance(data, count, all.data(), count, &cert, &failed));
        assert(device_ip_acceptance_verified(cert, data, count, &failed) && !failed);
        DeviceAcceptance5 bad = cert; bad.k += 1; assert(!device_ip_acceptance_verified(bad, data, count, &failed));
        bad = cert; bad.k = 0; assert(!device_ip_acceptance_verified(bad, data, count, &failed));
        bad = cert; bad.d = 0; assert(!device_ip_acceptance_verified(bad, data, count, &failed));
        bad = cert; bad.adj[0][0] += 1; assert(!device_ip_acceptance_verified(bad, data, count, &failed));
        bad = cert; for (auto &m : bad.mu) m = 0; assert(!device_ip_acceptance_verified(bad, data, count, &failed));
        bad = cert; bad.mu[0] = -bad.mu[0] - 1; assert(!device_ip_acceptance_verified(bad, data, count, &failed));
        bad = cert; bad.idx[0] = count; assert(!device_ip_acceptance_verified(bad, data, count, &failed));
        bad = cert; bad.b[1] = bad.b[0]; assert(!device_ip_acceptance_verified(bad, data, count, &failed));
        // Half of the cross-polytope: the origin is a vertex, no certificate exists.
        std::vector<Point> half = {{0,0,0,0,0}};
        for (int i = 0; i < 5; ++i) { Point p{}; p[i] = 1; half.push_back(p); }
        failed = 0;
        std::vector<int> half_all(half.size()); for (std::size_t i = 0; i < half.size(); ++i) half_all[i] = static_cast<int>(i);
        assert(!device_ip_find_acceptance(half.front().data(), static_cast<int>(half.size()), half_all.data(),
                                          static_cast<int>(half.size()), &cert, &failed));
    }

    // Rank deficient: a flat set through the origin is rejected (reason 1,
    // certificate rechecked by classify); a flat set missing the origin, whose
    // hyperplane constant is nonzero, is rejected only by a valid gate or deferred.
    {
        std::vector<Point> flat = {{0,0,0,0,0}};
        for (int i = 0; i < 4; ++i) for (int sign : {-1,1}) { Point p{}; p[i] = sign; flat.push_back(p); }
        flat.push_back({1,1,0,0,0});
        int reason = 0;
        assert(classify(flat, &reason) == 0 && reason == 1);
        std::vector<Point> shifted;
        for (Point p : flat) { p[4] = 1; shifted.push_back(p); }
        assert(classify(shifted, &reason) == 0 && (reason == 1 || reason == 3));
    }

    // Exact expected answers follow from explicit geometric constructions.
    std::vector<Point> cross;
    for (int i = 0; i < 5; ++i) for (int sign : {-1,1}) {
        Point p{}; p[i] = sign; cross.push_back(p);
    }
    std::vector<Point> simplex(6);
    for (int i = 0; i < 5; ++i) simplex[i][i] = 1;
    simplex[5].fill(-1);
    std::vector<Point> cube;
    for (int mask = 0; mask < 32; ++mask) {
        Point p{};
        for (int i = 0; i < 5; ++i) p[i] = (mask >> i & 1) ? 1 : -1;
        cube.push_back(p);
    }
#ifdef IP_OVERFLOW_PROBE
    auto transformed = cross;
    for (int step = 0; step < 10; ++step) {
        int a = step%5, b = (a+1)%5;
        for (auto &p : transformed) p[a] += 1000*p[b];
    }
    // This is an invertible integer shear of the IP cross polytope. Every
    // supplied coordinate fits in int64. Intermediate hull arithmetic may not.
    int reason = 0;
    assert(classify(transformed,&reason) == 0 && reason == 3);
    std::cout << "Large-basis arithmetic failure returns incomplete without signed overflow\n";
    return 0;
#endif
    // Public certificate gate rejects invalid/undefined certificates even when
    // the proposed hull equation itself is arbitrary.
    {
        DeviceIpScratch scratch{};
        int reason = 99;
        assert(!device_ip_check(nullptr, 0, &scratch, &reason) && reason == 3);
        assert(!device_ip_check(nullptr, 2147483647, &scratch, &reason) && reason == 3);
        DeviceEquation5 eq{};
        int failed = 0;
        assert(!device_ip_supporting_certificate(eq, cross.front().data(), cross.size(), &failed));
        eq.a[0] = 1;
        assert(!device_ip_supporting_certificate(eq, cross.front().data(), cross.size(), &failed));
        std::vector<Point> halfspace = cross;
        for (auto &p : halfspace) p[0] += 1;
        assert(device_ip_supporting_certificate(eq, halfspace.front().data(), halfspace.size(), &failed));
        eq.c = 1;
        assert(!device_ip_supporting_certificate(eq, halfspace.front().data(), halfspace.size(), &failed));
        eq.c = 0; eq.a[0] = std::numeric_limits<long long>::max();
        assert(!device_ip_supporting_certificate(eq, halfspace.front().data(), halfspace.size(), &failed));
        assert(failed);
        failed = 0;
        assert(!device_ip_supporting_certificate(eq, nullptr, 0, &failed));
        auto extreme = cross;
        extreme[0][0] = std::numeric_limits<long long>::min();
        reason = 0;
        assert(classify(extreme, &reason) == 0 && reason == 3);
    }
    expect(cross,1); expect(simplex,1); expect(cube,1);
    auto boundary = cube;
    for (auto &p : boundary) ++p[0];
    expect(boundary,0);
    auto separated = cube;
    for (auto &p : separated) p[0] += 2;
    expect(separated,0);
    auto flat = cube;
    for (auto &p : flat) p[0] = 0;
    expect(flat,0);
    flat = cube;
    for (auto &p : flat) p[0] = 1;
    expect(flat,0);

    // IP is invariant under invertible integer coordinate shears, scaling,
    // point reordering, duplicated points, and adding interior points.
    std::mt19937 random(20261102);
    unsigned int cases = 7;
    for (int trial = 0; trial < 400; ++trial) {
        for (const auto &seed : {cross, simplex, cube, boundary, separated, flat}) {
            auto points = seed;
            for (int step = 0; step < 4; ++step) {
                int a = random()%5, b = (a+1+random()%4)%5;
                int scale = int(random()%5)-2;
                for (auto &p : points) p[a] += scale*p[b];
            }
            for (auto &p : points) for (auto &v : p) v *= 1+trial%3;
            std::shuffle(points.begin(), points.end(), random);
            const bool is_ip = seed == cross || seed == simplex || seed == cube;
            if (is_ip) points.push_back(Point{});
            points.push_back(points.front());
            expect(points,is_ip);
            ++cases;
        }
    }
    // A complete candidate-equation scan promotes supporting positive-distance
    // facets, stops at the last violated equation, and preserves its stack top.
    {
        DeviceCEqList5 pending{};
        DeviceEqList5 facets{};
        unsigned long long pi[64] = {}, fi[64] = {};
        int ip = 1, truncated = 0;
        pending.ne = 2;
        pending.e[0].a[0] = 1; pending.e[0].c = 1;
        pending.e[1].a[1] = 1; pending.e[1].c = 1;
        assert(device_ip_search_bad_eq(&pending,&facets,pi,fi,cross.front().data(),cross.size(),&ip,&truncated) == 0);
        assert(pending.ne == -1 && facets.ne == 2 && ip && !truncated);
        pending.ne = 1; facets.ne = 64;
        pending.e[0].a[0] = 1; pending.e[0].c = 2;
        assert(device_ip_search_bad_eq(&pending,&facets,pi,fi,cross.front().data(),cross.size(),&ip,&truncated) == 0);
        assert(truncated && facets.ne == 64);
        pending.ne = 1; pending.e[0] = DeviceEquation5{};
        ip = 1; truncated = 0;
        assert(device_ip_search_bad_eq(&pending,&facets,pi,fi,cross.front().data(),cross.size(),&ip,&truncated) == 0);
        assert(ip == 1 && truncated == 1);
    }
    std::cout << "Serial IP source: " << cases << " exact-construction cases, independent reason-4 certificates and capacity regression passed\n";
}
