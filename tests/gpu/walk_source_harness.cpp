using namespace cwsgpu;
using AmbientPoint = std::array<int,10>;

std::set<AmbientPoint> direct_points(const DeviceCwsCandidate &candidate) {
    std::set<AmbientPoint> out;
    AmbientPoint x{};
    int remaining[5] = {};
    for (int r = 0; r < candidate.nw; ++r) remaining[r] = candidate.degree[r];
    std::function<void(int)> visit = [&](int a) {
        if (a == candidate.ambient_vertices) {
            for (int r = 0; r < candidate.nw; ++r) if (remaining[r]) return;
            out.insert(x); return;
        }
        int limit = std::numeric_limits<int>::max();
        for (int r = 0; r < candidate.nw; ++r)
            if (candidate.weights[r][a]) limit = std::min(limit, remaining[r] / candidate.weights[r][a]);
        assert(limit != std::numeric_limits<int>::max());
        for (int v = 0; v <= limit; ++v) {
            x[a] = v;
            for (int r = 0; r < candidate.nw; ++r) remaining[r] -= v * candidate.weights[r][a];
            bool possible = true;
            for (int r = 0; r < candidate.nw; ++r) {
                bool later = false;
                for (int b = a + 1; b < candidate.ambient_vertices; ++b) later |= candidate.weights[r][b] != 0;
                if (!later && remaining[r]) possible = false;
            }
            if (possible) visit(a + 1);
            for (int r = 0; r < candidate.nw; ++r) remaining[r] += v * candidate.weights[r][a];
        }
    };
    visit(0); return out;
}

bool direct_possible(const std::set<AmbientPoint> &points, int N) {
    for (int a = 0; a < N; ++a) {
        bool lo = false, hi = false;
        for (auto &x : points) { lo |= x[a] == 0; hi |= x[a] >= 2; }
        if (lo != hi) return false;
        for (int b = a + 1; b < N; ++b) {
            lo = hi = false;
            for (auto &x : points) { lo |= x[a] + x[b] <= 1; hi |= x[a] + x[b] >= 3; }
            if (lo != hi) return false;
        }
    }
    return true;
}

void test_arithmetic() {
    const long long values[] = {0,1,-1,2,-2,2147483647LL,-2147483648LL,
        9223372036854775807LL,-9223372036854775807LL-1};
    std::mt19937_64 random(20261102);
    unsigned long long checked = 0;
    for (int sample = 0; sample < 50081; ++sample) {
        long long a = sample < 81 ? values[sample/9] : static_cast<long long>(random());
        long long b = sample < 81 ? values[sample%9] : static_cast<long long>(random());
        for (int operation = 0; operation < 5; ++operation) {
            int ok = 1; long long actual = 0; __int128 expected = 0; bool valid = true;
            if (operation == 0) { expected = (__int128)a+b; actual = device_walk_add(a,b,&ok); }
            if (operation == 1) { expected = (__int128)a-b; actual = device_walk_sub(a,b,&ok); }
            if (operation == 2) { expected = (__int128)a*b; actual = device_walk_mul(a,b,&ok); }
            if (operation >= 3) {
                valid = b && !(a == std::numeric_limits<long long>::min() && b == -1);
                if (valid) expected = operation == 3 ? (__int128)a/b : (__int128)a%b;
                actual = operation == 3 ? device_walk_div(a,b,&ok) : device_walk_rem(a,b,&ok);
            }
            valid = valid && expected >= std::numeric_limits<long long>::min() && expected <= std::numeric_limits<long long>::max();
            assert(bool(ok) == valid);
            if (valid) assert(actual == expected);
            ++checked;
        }
    }
    std::cout << "Checked walk arithmetic: " << checked << " operations\n";
}

void test_certificates() {
    DeviceCwsCandidate c; c.nw = 1; c.ambient_vertices = 6; c.degree[0] = 6;
    for (int a = 0; a < 6; ++a) c.weights[0][a] = 1;
    int basis[5][10] = {};
    for (int j = 0; j < 5; ++j) { basis[j][j] = 1; basis[j][5] = -1; }
    assert(device_walk_basis_certificate(c,basis));
    basis[0][5] = 0; assert(!device_walk_basis_certificate(c,basis)); basis[0][5] = -1;
    for (int a = 0; a < 6; ++a) basis[0][a] = basis[1][a];
    assert(!device_walk_basis_certificate(c,basis));
    for (int a = 0; a < 6; ++a) basis[0][a] = 0;
    basis[0][0] = 2; basis[0][5] = -2;
    assert(!device_walk_basis_certificate(c,basis)); // mod 2 failure is inconclusive
    basis[0][0] = 1; basis[0][5] = -1;
    int xu[10] = {6,6,6,6,6,6};
    int lo[5] = {-2,-2,-2,-2,-2}, hi[5] = {2,2,2,2,2};
    assert(device_walk_int32_box(basis,6,lo,hi,xu));
    hi[0] = 2147483647; assert(!device_walk_int32_box(basis,6,lo,hi,xu)); hi[0] = 2;
    const int ord[5] = {0,1,2,3,4}, reversed[5] = {4,3,2,1,0};
    long long pts[15] = {-1,0,0,0,0, 0,0,0,0,0, 1,0,0,0,0};
    assert(device_walk_validate_points(c,basis,xu,ord,lo,hi,pts,3));
    long long tail_order[10] = {0,0,0,0,1, 1,0,0,0,0};   // increasing under ord, decreasing under reversed
    assert(device_walk_validate_points(c,basis,xu,ord,lo,hi,tail_order,2));
    assert(!device_walk_validate_points(c,basis,xu,reversed,lo,hi,tail_order,2));
    long long unordered[15] = {0,0,0,0,0, 1,0,0,0,0, -1,0,0,0,0};
    assert(!device_walk_validate_points(c,basis,xu,ord,lo,hi,unordered,3));
    for (int j = 0; j < 5; ++j) pts[5+j] = pts[j];
    assert(!device_walk_validate_points(c,basis,xu,ord,lo,hi,pts,3));
    long long invalid[5] = {7,0,0,0,0};
    assert(!device_walk_validate_points(c,basis,xu,ord,lo,hi,invalid,1));
    invalid[0] = std::numeric_limits<long long>::max();
    assert(!device_walk_validate_points(c,basis,xu,ord,lo,hi,invalid,1));
    long long buffer[10]; int count = 99, possible = 0;
    assert(device_make_points_fp(c,buffer,2,&count,&possible) < 0); // cap defers
    count = 99;
    assert(device_make_points_fp(c,buffer,0,&count,&possible) < 0 && count == 0);
    c.weights[0][0] = 2147483642; c.degree[0] = 2147483647;
    assert(device_make_points_fp(c,buffer,2,&count,&possible) < 0);
    c.nw = 0;
    assert(device_make_points_fp(c,buffer,2,&count,&possible) < 0);
}

int main() {
    test_arithmetic(); test_certificates();
    int completed = 0, deferred = 0;
    for (int id = 2; id <= 47; ++id) for (int trial = 0; trial < 4; ++trial) {
        GpuDescriptor d{}; assert(GpuCatalogueDescriptor(id,&d));
        DeviceCwsCandidate c; c.nw = d.row_count; c.ambient_vertices = d.coordinate_count;
        for (int r = 0; r < d.row_count; ++r) for (int p = 0; p < d.row_sizes[r]; ++p) {
            int weight = trial == 0 ? 1 : trial == 1 ? 1+(p%2) : trial == 2 ? 1+(p%3) : p+1;
            c.weights[r][d.mappings[r][p]-1] = weight; c.degree[r] += weight;
        }
        auto expected = direct_points(c);
        std::vector<long long> points(4096*5);
        int count = 0, possible = 1;
        int status = device_make_points_fp(c,points.data(),4096,&count,&possible);
        if (status <= 0 || count != static_cast<int>(expected.size())) { ++deferred; continue; }
        assert(bool(possible) == direct_possible(expected,c.ambient_vertices));
        long long basis64[5][10]; int dim = 0; assert(device_make_cws_basis(c,&dim,basis64));
        int basis[5][10]; long long xu64[10]; assert(device_candidate_basic_precheck(c,xu64));
        float weights[10];
        for (int a = 0; a < c.ambient_vertices; ++a) {
            float r = xu64[a] > 0 ? xu64[a]*.5f : .5f; weights[a] = 1.f/(r*r);
            for (int j = 0; j < 5; ++j) basis[j][a] = basis64[j][a];
        }
        assert(device_lll_f(basis,5,c.ambient_vertices,weights,.75f));
        std::set<AmbientPoint> actual;
        for (int p = 0; p < count; ++p) {
            AmbientPoint x{};
            for (int a = 0; a < c.ambient_vertices; ++a) {
                long long value = 1;
                for (int j = 0; j < 5; ++j) value += points[p*5+j]*basis[j][a];
                assert(value >= 0 && value <= xu64[a]); x[a] = value;
            }
            assert(actual.insert(x).second);
        }
        assert(actual == expected); ++completed;
    }
    assert(completed >= 100);
    std::cout << "Walk exact set/certificate agreement: " << completed << "; deferred: " << deferred
              << "; 184 cases, all 46 descriptors\n";
}
