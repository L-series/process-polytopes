// General join: decides most candidates from precomputed single-row tables
// before any lattice point is enumerated.
#pragma once

#include "descriptor.cuh"
#include "lattice.cuh"

#include <algorithm>
#include <array>
#include <chrono>
#include <exception>
#include <functional>
#include <thread>

namespace cwsgpu {

// A coordinate is shared if two or more rows use it. Once the values s of all
// shared coordinates are fixed, each row's private coordinates are constrained
// by that row alone, so
//     P = U_s  prod_r F_r(s|T_r),        np = sum_s prod_r c_r(s|T_r),
// and inside one block the private parts of different rows vary independently.
// Per pool (size, shared count k) and entry we store the entry's points grouped
// by the shared prefix (entry position order), with the count, the value
// classes 0/1/2/>=3 of each private position, and for each pair of private
// positions whether their sum reaches <= 1 / >= 3. A candidate's blocks are
// enumerated depth-first over the rows in a host-chosen order: a row whose
// shared coordinates are all bound by earlier rows is a binary-search lookup,
// otherwise its keys are range-scanned through a copy sorted by one bound
// position and the other bound positions filtered. The coordinate and pair
// certificates of the point walk then follow from the stored classes.
struct GJoinKey {
    unsigned long long key;  // shared prefix, 16 bits per position, position 0 highest
    unsigned int count;      // points of the entry with this prefix
    unsigned int cats;       // private position t: bits 4t..4t+3 = classes 0/1/2/>=3 present
    unsigned int pairs;      // private pair u (t1<t2, lexicographic): bit 2u sum <= 1, bit 2u+1 sum >= 3
    unsigned int pad;
};

// Per entry and shared position p >= 1, the entry's keys re-sorted with
// position p's value moved to the top, so a row whose position p is bound is
// scanned over one value range only (position 0 uses the canonical order).
struct GJoinIdx {
    unsigned long long pkey;  // v_p << 16(k-1) | the other positions in order
    unsigned int idx;         // index of the canonical GJoinKey
    unsigned int pad;
};

struct GJoinTable {
    const unsigned char *covered = nullptr;  // nullptr: all covered; 0 entry: defer, never an empty join
    const GJoinKey *keys = nullptr;
    const GJoinIdx *pidx[4] = {nullptr, nullptr, nullptr, nullptr};  // shared position 1..3
    const unsigned long long *offsets = nullptr;  // pool_count + 1
    int pool_offset = 0;
    int pool_count = 0;
};

struct GJoinPlan {
    int nrows = 0;
    int ambient = 0;
    int size[kMaxSlots] = {0, 0, 0, 0, 0};
    int shared[kMaxSlots] = {0, 0, 0, 0, 0};
    int coord[kMaxSlots][kMaxSize] = {};     // descriptor position -> coordinate (0-based)
    int order[kMaxSlots] = {0, 0, 0, 0, 0};  // slot visited at depth j
    int lookup[kMaxSlots] = {0, 0, 0, 0, 0}; // depth j: every shared coordinate already bound
    unsigned int bound_before[kMaxSlots] = {0, 0, 0, 0, 0};
    unsigned int shared_mask = 0;
    unsigned int same_row[10] = {};          // coordinate A: the other private coordinates of A's row
    GJoinTable table[kMaxSlots];
};

struct GJoinCensus {
    unsigned long long degenerate = 0, certified = 0, kept = 0, kept_over_cap = 0;
    unsigned long long max_np_all = 0;   // over every joined candidate
    unsigned long long max_np_kept = 0;  // over candidates no join rule rejects
};

// Position the iterator of depth j: a lookup starts fresh; a scan with a
// bound shared position covers only that value's range in the position's
// sorted copy; a scan with none covers all keys.
__device__ void device_gjoin_begin_level(
    int j, const GJoinPlan &plan, const int epos_coord[kMaxSlots][kMaxSize],
    const int val[10], const unsigned long long kb[kMaxSlots],
    const unsigned long long ke[kMaxSlots], unsigned long long it[kMaxSlots],
    unsigned long long end[kMaxSlots], int pstar[kMaxSlots]) {
    const int s = plan.order[j];
    const int k = plan.shared[s];
    it[j] = kb[s];
    end[j] = ke[s];
    pstar[j] = -1;
    if (plan.lookup[j]) {
        // Every shared value is bound: the level is the one key equal to them,
        // if present, as a range of length at most one.
        unsigned long long want = 0;
        for (int p = 0; p < k; ++p) want = (want << 16) | static_cast<unsigned long long>(val[epos_coord[s][p]]);
        const GJoinKey *keys = plan.table[s].keys;
        unsigned long long lo = kb[s], hi = ke[s];
        while (lo < hi) { const unsigned long long m = (lo + hi) >> 1; if (keys[m].key < want) lo = m + 1; else hi = m; }
        it[j] = lo;
        end[j] = (lo < ke[s] && keys[lo].key == want) ? lo + 1 : lo;
        return;
    }
    const unsigned int bound = plan.bound_before[j];
    int p = 0;
    while (p < k && !((bound >> epos_coord[s][p]) & 1u)) ++p;
    if (p == k) return;
    pstar[j] = p;
    const unsigned long long v = static_cast<unsigned long long>(val[epos_coord[s][p]]);
    const int sh = 16 * (k - 1);
    const unsigned long long want_lo = v << sh;
    unsigned long long lo = kb[s], hi = ke[s];
    if (p == 0) {
        const GJoinKey *keys = plan.table[s].keys;
        while (lo < hi) { const unsigned long long m = (lo + hi) >> 1; if (keys[m].key < want_lo) lo = m + 1; else hi = m; }
        it[j] = lo;
        hi = ke[s];
        // Comparing the leading value also handles k=4, v=65535;
        // forming (v+1)<<48 would wrap to zero in that case.
        while (lo < hi) { const unsigned long long m = (lo + hi) >> 1; if ((keys[m].key >> sh) <= v) lo = m + 1; else hi = m; }
        end[j] = lo;
    } else {
        const GJoinIdx *px = plan.table[s].pidx[p];
        while (lo < hi) { const unsigned long long m = (lo + hi) >> 1; if (px[m].pkey < want_lo) lo = m + 1; else hi = m; }
        it[j] = lo;
        hi = ke[s];
        while (lo < hi) { const unsigned long long m = (lo + hi) >> 1; if ((px[m].pkey >> sh) <= v) lo = m + 1; else hi = m; }
        end[j] = lo;
    }
}

// A failed row comparison may leave new (unbound) values behind. It must
// never alter the earlier rows' bound coordinates. Every successful match
// overwrites all coordinates introduced by this row before descent.
__device__ bool device_gjoin_match_key(unsigned long long key, int k,
                                       const int coordinates[kMaxSize],
                                       unsigned int bound, int val[10]) {
    for (int p = 0; p < k; ++p) {
        const int A = coordinates[p];
        const int v = static_cast<int>((key >> (16 * (k - 1 - p))) & 0xFFFFULL);
        if ((bound >> A) & 1u) { if (val[A] != v) return false; }
        else val[A] = v;
    }
    return true;
}

// Running sums and occupancy masks of the join's leaves (bit A: coordinate A).
// tz/t2: some point has X_A == 0 / X_A >= 2. LO[A]/HI[A] bit B: some point has
// X_A + X_B <= 1 / >= 3, from class combinations of different factors; LS/HS
// the same for two private coordinates of one row, from that row's fibre.
struct GJoinAcc {
    unsigned long long np;
    unsigned int tz, t2, m3_all;
    unsigned int LO[10], HI[10], LS[10], HS[10];
};

// One leaf: the block of the chosen keys, with the shared values in val.
// False when a count is zero or a sum cannot be represented (inconclusive).
__device__ bool device_gjoin_leaf(const GJoinPlan &plan, const unsigned long long chosen[kMaxSlots],
                                  const int val[10], GJoinAcc &acc) {
    const int R = plan.nrows;
    unsigned long long cnt = 1;
    unsigned int m0 = 0, m1 = 0, m2 = 0, m3 = 0;
    for (unsigned int sm = plan.shared_mask; sm; sm &= sm - 1) {
        const int A = __ffs(sm) - 1;
        const unsigned int b = 1u << A;
        const int v = val[A];
        m0 |= v == 0 ? b : 0u; m1 |= v == 1 ? b : 0u; m2 |= v == 2 ? b : 0u; m3 |= v >= 3 ? b : 0u;
    }
    for (int j = 0; j < R; ++j) {
        const int sj = plan.order[j];
        const GJoinKey g = plan.table[sj].keys[chosen[j]];
        // Zero counts violate the nonempty-factor invariant. Arithmetic
        // that cannot be represented must remain inconclusive as well.
        if (g.count == 0 || cnt > (~0ULL) / g.count) return false;
        cnt *= g.count;
        const int k = plan.shared[sj];
        const int npriv = plan.size[sj] - k;
        for (int t = 0; t < npriv; ++t) {
            const unsigned int b = 1u << plan.coord[sj][k + t];
            const unsigned int cl = (g.cats >> (4 * t)) & 15u;
            m0 |= (cl & 1u) ? b : 0u; m1 |= (cl & 2u) ? b : 0u; m2 |= (cl & 4u) ? b : 0u; m3 |= (cl & 8u) ? b : 0u;
        }
        int u = 0;
        for (int t1 = 0; t1 < npriv; ++t1)
            for (int t2i = t1 + 1; t2i < npriv; ++t2i, ++u) {
                int a = plan.coord[sj][k + t1], bb = plan.coord[sj][k + t2i];
                if (a > bb) { const int tmp = a; a = bb; bb = tmp; }
                if ((g.pairs >> (2 * u)) & 1u) acc.LS[a] |= 1u << bb;
                if ((g.pairs >> (2 * u + 1)) & 1u) acc.HS[a] |= 1u << bb;
            }
    }
    if (acc.np > (~0ULL) - cnt) return false;
    acc.np += cnt;
    acc.tz |= m0;
    acc.t2 |= m2 | m3;
    acc.m3_all |= m3;
    const unsigned int ge1 = m1 | m2 | m3;
    for (unsigned int x = m0; x; x &= x - 1) acc.LO[__ffs(x) - 1] |= m0 | m1;
    for (unsigned int x = m1; x; x &= x - 1) acc.LO[__ffs(x) - 1] |= m0;
    for (unsigned int x = m3; x; x &= x - 1) acc.HI[__ffs(x) - 1] = 0x3FFu;
    for (unsigned int x = m2; x; x &= x - 1) acc.HI[__ffs(x) - 1] |= ge1;
    for (unsigned int x = ge1; x; x &= x - 1) acc.HI[__ffs(x) - 1] |= m2;
    return true;
}

// 0 = degenerate (np < 6), 1 = a coordinate or pair functional is one-sided
// and non-constant (certified non-IP), 2 = keep.
__device__ int device_gjoin_verdict(const GJoinAcc &acc, const GJoinPlan &plan) {
    const int N = plan.ambient;
    if (acc.np < 6) return 0;
    const unsigned int all = (1u << N) - 1u;
    if ((acc.tz ^ acc.t2) & all) return 1;
    for (int A = 0; A < N; ++A) {
        const unsigned int upper = all & ~((2u << A) - 1u);   // B > A
        const unsigned int same = plan.same_row[A];
        const unsigned int lo = ((acc.LO[A] & ~same) | (acc.LS[A] & same)) & upper;
        const unsigned int hi = (((acc.HI[A] | acc.m3_all) & ~same) | (acc.HS[A] & same)) & upper;
        if (lo ^ hi) return 1;
    }
    return 2;
}

// Returns 0 = degenerate (np < 6), 1 = certified non-IP, 2 = keep; *np_out is
// the exact point count. Layouts the tables do not cover return 2 with
// *np_out = 0; callers must defer these candidates for exact CPU completion.
__device__ int device_gjoin_decide(const DeviceCwsCandidate &c, const SelectedEntry *entries,
                                   const GJoinPlan &plan, unsigned long long *np_out) {
    *np_out = 0;
    const int R = plan.nrows;
    const int N = plan.ambient;
    if (R < 1 || R > kMaxSlots || N < 1 || N > 10) return 2;
    int epos_coord[kMaxSlots][kMaxSize];  // slot, entry shared position -> coordinate
    unsigned long long kb[kMaxSlots], ke[kMaxSlots];
    for (int s = 0; s < R; ++s) {
        const GJoinTable &t = plan.table[s];
        const int local = c.sel_index[s] - t.pool_offset;
        if (local < 0 || local >= t.pool_count) return 2;
        if (t.covered && !t.covered[local]) return 2;
        const SelectedEntry &E = entries[c.sel_index[s]];
        const int k = plan.shared[s];
        // The candidate's shared prefix is a permutation of the entry's; match
        // positions by weight (equal weights are interchangeable, Lemma 3).
        unsigned int used = 0;
        for (int q = 0; q < k; ++q) {
            const int cq = plan.coord[s][q];
            const int cw = c.weights[s][cq];
            int p = 0;
            while (p < k && (((used >> p) & 1u) || E.w[p] != cw)) ++p;
            if (p == k) return 2;
            used |= 1u << p;
            epos_coord[s][p] = cq;
        }
        kb[s] = t.offsets[local];
        ke[s] = t.offsets[local + 1];
    }

    int val[10] = {0, 0, 0, 0, 0, 0, 0, 0, 0, 0};
    unsigned long long it[kMaxSlots], end[kMaxSlots];
    int pstar[kMaxSlots];
    unsigned long long chosen[kMaxSlots];
    GJoinAcc acc = {};

    int depth = 0;
    device_gjoin_begin_level(0, plan, epos_coord, val, kb, ke, it, end, pstar);
    while (depth >= 0) {
        const int s = plan.order[depth];
        const int k = plan.shared[s];
        const GJoinKey *keys = plan.table[s].keys;
        bool found = false;
        const unsigned int bound = plan.bound_before[depth];
        const GJoinIdx *px = pstar[depth] > 0 ? plan.table[s].pidx[pstar[depth]] : nullptr;
        while (it[depth] < end[depth]) {
            const unsigned long long pos = it[depth]++;
            const unsigned long long kk = px ? static_cast<unsigned long long>(px[pos].idx) : pos;
            const unsigned long long key = keys[kk].key;
            const bool ok = device_gjoin_match_key(key, k, epos_coord[s], bound, val);
            if (ok) { chosen[depth] = kk; found = true; break; }
        }
        if (!found) { --depth; continue; }
        if (depth + 1 < R) {
            ++depth;
            device_gjoin_begin_level(depth, plan, epos_coord, val, kb, ke, it, end, pstar);
            continue;
        }
        // Leaf: one block {s} x prod_r F_r(s).
        if (!device_gjoin_leaf(plan, chosen, val, acc)) return 2;
    }
    *np_out = acc.np;
    return device_gjoin_verdict(acc, plan);
}

// Device copy of one pool's table; freed with the process.
struct HostGJoinTable {
    GJoinTable view;
    GJoinIdx *d_pidx[4] = {nullptr, nullptr, nullptr, nullptr};
    GJoinKey *d_keys = nullptr;
    unsigned char *d_covered = nullptr;
    unsigned long long *d_offsets = nullptr;
    std::uint64_t key_count = 0;
    std::uint64_t point_count = 0;
};

// Check the representation consumed by the search, before uploading it. This
// does not establish completeness of the row enumerator: it checks that every
// enumerated point is valid/distinct and is represented by exactly one fibre,
// with its exact count and occupancy summary.
// Exact number of nonnegative solutions of sum_p w[p] x[p] = degree: the in-place
// coin-change DP over the positions from last to first (dpCount, JoinRowCount.lean).
inline std::uint64_t row_solution_count(const SelectedEntry &entry, int size) {
    if (entry.degree < 0) throw std::runtime_error("gjoin: negative row degree");
    std::vector<std::uint64_t> dp(static_cast<std::size_t>(entry.degree) + 1, 0);
    dp[0] = 1;
    for (int p = size - 1; p >= 0; --p) {
        if (entry.w[p] <= 0) throw std::runtime_error("gjoin: nonpositive row weight");
        for (int r = entry.w[p]; r <= entry.degree; ++r) dp[r] = checked_add(dp[r], dp[r - entry.w[p]]);
    }
    return dp[static_cast<std::size_t>(entry.degree)];
}

inline void validate_gjoin_fibres(const SelectedEntry &entry, int size, int shared,
                                 const std::vector<std::array<int, 5>> &points,
                                 const std::vector<GJoinKey> &fibres) {
    std::size_t cursor = 0;
    for (std::size_t f = 0; f < fibres.size(); ++f) {
        const GJoinKey &g = fibres[f];
        if (!g.count || (f && fibres[f - 1].key >= g.key) ||
            (shared < 4 && (g.key >> (16 * shared))))
            throw std::runtime_error("gjoin: invalid canonical fibre key/count");
        unsigned int cats = 0, pairs = 0;
        for (unsigned int j = 0; j < g.count; ++j, ++cursor) {
            if (cursor >= points.size() || (cursor && !(points[cursor - 1] < points[cursor])))
                throw std::runtime_error("gjoin: missing, repeated or unsorted row point");
            const auto &point = points[cursor];
            int remaining = entry.degree;
            unsigned long long key = 0;
            for (int p = 0; p < size; ++p) {
                if (entry.w[p] <= 0 || point[p] < 0 || point[p] > remaining / entry.w[p])
                    throw std::runtime_error("gjoin: invalid row point");
                remaining -= point[p] * entry.w[p];
                if (p < shared) {
                    if (point[p] > 65535) throw std::runtime_error("gjoin: shared value exceeds 16 bits");
                    key = (key << 16) | static_cast<unsigned int>(point[p]);
                } else {
                    cats |= (1u << std::min(point[p], 3)) << (4 * (p - shared));
                }
            }
            if (remaining || key != g.key) throw std::runtime_error("gjoin: incorrect row fibre");
            int pair = 0;
            for (int a = shared; a < size; ++a)
                for (int b = a + 1; b < size; ++b, ++pair) {
                    const long long sum = static_cast<long long>(point[a]) + point[b];
                    pairs |= (sum <= 1 ? 1u : 0u) << (2 * pair);
                    pairs |= (sum >= 3 ? 2u : 0u) << (2 * pair);
                }
        }
        if (cats != g.cats || pairs != g.pairs)
            throw std::runtime_error("gjoin: incorrect fibre occupancy summary");
    }
    if (cursor != points.size()) throw std::runtime_error("gjoin: unrepresented row points");
    // The points are valid, distinct and as many as all solutions: none is
    // missing (enumerated_complete, JoinRowCount.lean).
    if (row_solution_count(entry, size) != points.size())
        throw std::runtime_error("gjoin: row enumeration omitted solutions");
}

inline void validate_gjoin_index(const std::vector<GJoinKey> &keys,
                                const std::vector<GJoinIdx> &index,
                                std::size_t begin, std::size_t end, int shared, int position) {
    if (begin > end || end > keys.size() || end > index.size() ||
        shared < 2 || shared > 4 || position < 1 || position >= shared)
        throw std::runtime_error("gjoin: invalid position index shape");
    std::vector<bool> seen(end - begin, false);
    for (std::size_t i = begin; i < end; ++i) {
        const std::size_t target = index[i].idx;
        if (target < begin || target >= end || seen[target - begin] ||
            (i > begin && index[i - 1].pkey >= index[i].pkey))
            throw std::runtime_error("gjoin: position index is not a sorted permutation");
        seen[target - begin] = true;
        const unsigned long long key = keys[target].key;
        // Independently unpack the permuted word, rather than using the
        // builder's packing loop to validate itself.
        int output = 0;
        for (int p = -1; p < shared; ++p) {
            if (p == position) continue;
            const int original = p < 0 ? position : p;
            const auto value = (key >> (16 * (shared - 1 - original))) & 65535ULL;
            const auto actual = (index[i].pkey >> (16 * (shared - 1 - output))) & 65535ULL;
            if (value != actual) throw std::runtime_error("gjoin: incorrect position index key");
            ++output;
        }
        if (shared < 4 && (index[i].pkey >> (16 * shared)))
            throw std::runtime_error("gjoin: position index key exceeds width");
    }
}

inline void build_gjoin_table(const PoolInfo &pool, int size, int shared, HostGJoinTable &out) {
    if (pool.entries.size() > static_cast<std::size_t>(std::numeric_limits<int>::max()) ||
        pool.device_offset > static_cast<std::uint64_t>(std::numeric_limits<int>::max()) - pool.entries.size())
        throw std::runtime_error("gjoin: pool exceeds signed candidate index range");
    const int pool_count = static_cast<int>(pool.entries.size());
    const int npriv = size - shared;
    if (size < 1 || size > kMaxSize || shared < 0 || shared > size || shared > 4)
        throw std::runtime_error("gjoin: unsupported table shape");
    for (const auto &entry : pool.entries) {
        long long degree = 0;
        for (int p = 0; p < size; ++p) {
            if (entry.w[p] <= 0) throw std::runtime_error("gjoin: nonpositive row weight");
            degree += entry.w[p];
        }
        if (entry.size != size || degree != entry.degree)
            throw std::runtime_error("gjoin: inconsistent row size/degree");
    }
    std::vector<std::vector<GJoinKey>> per(static_cast<std::size_t>(pool_count));
    std::vector<unsigned char> covered(static_cast<std::size_t>(pool_count), 1);
    std::vector<std::uint64_t> points_per(static_cast<std::size_t>(pool_count), 0);
    auto work = [&](int begin, int end) {
        std::vector<std::array<int, 5>> pts;
        for (int e = begin; e < end; ++e) {
            const SelectedEntry &en = pool.entries[static_cast<std::size_t>(e)];
            const int *w = en.w;
            // A conservative coverage guard is enough: unsupported entries
            // retain their identity and are sent to exact CPU completion.
            bool fits = true;
            for (int p = 0; p < shared; ++p) fits &= en.degree / w[p] <= 65535;
            if (!fits) { covered[static_cast<std::size_t>(e)] = 0; continue; }
            pts.clear();
            std::array<int, 5> x{};
            std::function<void(int, int)> rec = [&](int pos, int rem) {
                if (pos == size - 1) {
                    if (rem % w[pos] == 0) { x[pos] = rem / w[pos]; pts.push_back(x); }
                    return;
                }
                // Avoid the overflowing multiply at the first excluded v and
                // the final increment when rem / w[pos] == INT_MAX.
                for (int v = 0, rest = rem;; ++v) {
                    x[pos] = v;
                    rec(pos + 1, rest);
                    if (rest < w[pos]) break;
                    rest -= w[pos];
                }
            };
            rec(0, en.degree);
            std::sort(pts.begin(), pts.end());
            points_per[static_cast<std::size_t>(e)] = pts.size();
            auto keyof = [&](const std::array<int, 5> &q) {
                unsigned long long k = 0;
                for (int i = 0; i < shared; ++i) {
                    if (q[i] > 0xFFFF) throw std::runtime_error("gjoin: point value exceeds 16 bits");
                    k = (k << 16) | static_cast<unsigned long long>(q[i]);
                }
                return k;
            };
            auto cls = [](int v) -> unsigned int { return v == 0 ? 1u : v == 1 ? 2u : v == 2 ? 4u : 8u; };
            std::vector<GJoinKey> &o = per[static_cast<std::size_t>(e)];
            bool representable = true;
            for (std::size_t i = 0; i < pts.size();) {
                GJoinKey g{};
                g.key = keyof(pts[i]);
                std::size_t j = i;
                for (; j < pts.size() && keyof(pts[j]) == g.key; ++j) {
                    if (g.count == std::numeric_limits<unsigned int>::max()) { representable = false; break; }
                    ++g.count;
                    for (int t = 0; t < npriv; ++t) g.cats |= cls(pts[j][shared + t]) << (4 * t);
                    int u = 0;
                    for (int t1 = 0; t1 < npriv; ++t1)
                        for (int t2 = t1 + 1; t2 < npriv; ++t2, ++u) {
                            const long long sum = static_cast<long long>(pts[j][shared + t1]) + pts[j][shared + t2];
                            if (sum <= 1) g.pairs |= 1u << (2 * u);
                            if (sum >= 3) g.pairs |= 1u << (2 * u + 1);
                        }
                }
                if (!representable) break;
                o.push_back(g);
                i = j;
            }
            if (!representable) {
                covered[static_cast<std::size_t>(e)] = 0;
                points_per[static_cast<std::size_t>(e)] = 0;
                o.clear();
                continue;
            }
            validate_gjoin_fibres(en, size, shared, pts, o);
        }
    };
    unsigned int nthreads = std::max(1u, std::thread::hardware_concurrency());
    std::vector<std::thread> threads;
    std::vector<std::exception_ptr> errors(nthreads);
    for (unsigned int t = 0; t < nthreads; ++t) {
        int b0 = static_cast<int>(static_cast<long long>(pool_count) * t / nthreads);
        int e0 = static_cast<int>(static_cast<long long>(pool_count) * (t + 1) / nthreads);
        threads.emplace_back([&, b0, e0, t] {
            try { work(b0, e0); }
            catch (...) { errors[t] = std::current_exception(); }
        });
    }
    for (auto &th : threads) th.join();
    for (const auto &error : errors) if (error) std::rethrow_exception(error);
    std::vector<unsigned long long> offsets(static_cast<std::size_t>(pool_count) + 1, 0);
    for (int e = 0; e < pool_count; ++e) {
        auto count = per[static_cast<std::size_t>(e)].size();
        if (count > 0xFFFFFFFFULL - offsets[e]) {
            covered[static_cast<std::size_t>(e)] = 0;
            points_per[static_cast<std::size_t>(e)] = 0;
            per[static_cast<std::size_t>(e)].clear();
            count = 0;
        }
        offsets[e + 1] = offsets[e] + count;
    }
    std::vector<GJoinKey> keys(static_cast<std::size_t>(offsets.back()));
    for (int e = 0; e < pool_count; ++e)
        std::copy(per[e].begin(), per[e].end(), keys.begin() + static_cast<std::ptrdiff_t>(offsets[e]));
    check_cuda(cudaMalloc(&out.d_keys, std::max<std::size_t>(keys.size(), 1) * sizeof(GJoinKey)), "gjoin keys");
    check_cuda(cudaMemcpy(out.d_keys, keys.data(), keys.size() * sizeof(GJoinKey), cudaMemcpyHostToDevice), "gjoin keys");
    check_cuda(cudaMalloc(&out.d_offsets, offsets.size() * sizeof(unsigned long long)), "gjoin offsets");
    check_cuda(cudaMemcpy(out.d_offsets, offsets.data(), offsets.size() * sizeof(unsigned long long),
                          cudaMemcpyHostToDevice), "gjoin offsets");
    // Position-sorted copies for shared positions 1..k-1 (global key indices).
    for (int p = 1; p < shared; ++p) {
        std::vector<GJoinIdx> px(keys.size());
        for (int e = 0; e < pool_count; ++e) {
            const std::size_t b = offsets[e], n = offsets[e + 1] - offsets[e];
            for (std::size_t i = 0; i < n; ++i) {
                const unsigned long long key = keys[b + i].key;
                unsigned long long pk = (key >> (16 * (shared - 1 - p))) & 0xFFFFULL;
                for (int q = 0; q < shared; ++q)
                    if (q != p) pk = (pk << 16) | ((key >> (16 * (shared - 1 - q))) & 0xFFFFULL);
                px[b + i].pkey = pk;
                px[b + i].idx = static_cast<unsigned int>(b + i);
                px[b + i].pad = 0;
            }
            std::sort(px.begin() + static_cast<std::ptrdiff_t>(b), px.begin() + static_cast<std::ptrdiff_t>(b + n),
                      [](const GJoinIdx &l, const GJoinIdx &r) { return l.pkey < r.pkey; });
            validate_gjoin_index(keys, px, b, b + n, shared, p);
        }
        check_cuda(cudaMalloc(&out.d_pidx[p], std::max<std::size_t>(px.size(), 1) * sizeof(GJoinIdx)), "gjoin pidx");
        check_cuda(cudaMemcpy(out.d_pidx[p], px.data(), px.size() * sizeof(GJoinIdx), cudaMemcpyHostToDevice), "gjoin pidx");
        out.view.pidx[p] = out.d_pidx[p];
    }
    check_cuda(cudaMalloc(&out.d_covered, std::max<std::size_t>(covered.size(), 1)), "gjoin coverage");
    check_cuda(cudaMemcpy(out.d_covered, covered.data(), covered.size(), cudaMemcpyHostToDevice), "gjoin coverage");
    out.view.covered = out.d_covered;
    out.view.keys = out.d_keys;
    out.view.offsets = out.d_offsets;
    out.view.pool_offset = static_cast<int>(pool.device_offset);
    out.view.pool_count = pool_count;
    out.key_count = keys.size();
    for (std::uint64_t p : points_per) out.point_count += p;
}

// Plan for one descriptor: shared coordinates, row order for the depth-first
// join, and the tables. Rows are visited greedily: first the row with the most
// shared coordinates, then always a row whose shared coordinates are all bound
// (a lookup) if one exists, else the row with the most bound ones.
inline void validate_gjoin_plan(const GJoinPlan &plan) {
    if (plan.nrows < 1 || plan.nrows > kMaxSlots || plan.ambient < 1 || plan.ambient > 10)
        throw std::runtime_error("gjoin: invalid plan dimensions");
    int uses[10] = {};
    unsigned int support[kMaxSlots] = {}, expected_pairs[10] = {};
    for (int r = 0; r < plan.nrows; ++r) {
        if (plan.size[r] < 1 || plan.size[r] > kMaxSize || plan.shared[r] < 0 ||
            plan.shared[r] > 4 || plan.shared[r] > plan.size[r])
            throw std::runtime_error("gjoin: invalid plan row shape");
        unsigned int seen = 0;
        for (int p = 0; p < plan.size[r]; ++p) {
            const int a = plan.coord[r][p];
            if (a < 0 || a >= plan.ambient || (seen & (1u << a)))
                throw std::runtime_error("gjoin: invalid or repeated plan coordinate");
            seen |= 1u << a;
            ++uses[a];
            if (p < plan.shared[r]) support[r] |= 1u << a;
            else for (int q = plan.shared[r]; q < plan.size[r]; ++q) {
                // q's coordinate is checked on its own iteration before the
                // masks below are consumed; validate it here before shifting.
                const int b = plan.coord[r][q];
                if (b < 0 || b >= plan.ambient) throw std::runtime_error("gjoin: invalid private coordinate");
                if (p != q) expected_pairs[a] |= 1u << b;
            }
        }
    }
    unsigned int shared = 0;
    for (int a = 0; a < plan.ambient; ++a) {
        if (!uses[a]) throw std::runtime_error("gjoin: uncovered plan coordinate");
        if (uses[a] > 1) shared |= 1u << a;
        if (expected_pairs[a] != plan.same_row[a]) throw std::runtime_error("gjoin: incorrect same-row pair mask");
    }
    if (shared != plan.shared_mask) throw std::runtime_error("gjoin: incorrect shared mask");
    for (int r = 0; r < plan.nrows; ++r)
        for (int p = 0; p < plan.size[r]; ++p)
            if ((uses[plan.coord[r][p]] > 1) != (p < plan.shared[r]))
                throw std::runtime_error("gjoin: row is not in shared-first layout");
    unsigned int visited = 0, bound = 0;
    for (int j = 0; j < plan.nrows; ++j) {
        const int r = plan.order[j];
        if (r < 0 || r >= plan.nrows || (visited & (1u << r)))
            throw std::runtime_error("gjoin: row order is not a permutation");
        if (plan.bound_before[j] != bound ||
            plan.lookup[j] != static_cast<int>((support[r] & ~bound) == 0))
            throw std::runtime_error("gjoin: incorrect bound/lookup plan");
        visited |= 1u << r;
        bound |= support[r];
    }
}

inline GJoinPlan build_gjoin_plan(const GpuDescriptor &d, const PoolArray &pools,
                                  std::array<HostGJoinTable, kPoolKeyStride * kPoolKeyStride> &tables) {
    GJoinPlan plan;
    plan.nrows = d.row_count;
    plan.ambient = d.coordinate_count;
    unsigned int row_shared[kMaxSlots] = {0, 0, 0, 0, 0};
    for (int s = 0; s < d.row_count; ++s) {
        plan.size[s] = d.row_sizes[s];
        plan.shared[s] = d.shared_counts[s];
        for (int p = 0; p < d.row_sizes[s]; ++p) {
            const int A = d.mappings[s][p] - 1;
            plan.coord[s][p] = A;
            if (p < d.shared_counts[s]) {  // shared-first layout, checked by catalogue_descriptor
                plan.shared_mask |= 1u << A;
                row_shared[s] |= 1u << A;
            }
        }
        for (int p = d.shared_counts[s]; p < d.row_sizes[s]; ++p)
            for (int q = d.shared_counts[s]; q < d.row_sizes[s]; ++q)
                if (q != p) plan.same_row[plan.coord[s][p]] |= 1u << plan.coord[s][q];
        HostGJoinTable &t = tables[pool_key(d.row_sizes[s], d.shared_counts[s])];
        if (!t.d_keys) build_gjoin_table(pools[pool_key(d.row_sizes[s], d.shared_counts[s])], d.row_sizes[s],
                                         d.shared_counts[s], t);
        plan.table[s] = t.view;
    }
    unsigned int bound = 0, done = 0;
    for (int j = 0; j < d.row_count; ++j) {
        // Rows of a product component have negative scores; still pick one.
        int best = -1, best_score = std::numeric_limits<int>::min();
        for (int s = 0; s < d.row_count; ++s) {
            if ((done >> s) & 1u) continue;
            const int nshared = __builtin_popcount(row_shared[s]);
            const int nbound = __builtin_popcount(row_shared[s] & bound);
            int score;
            if (j == 0) score = nshared * 16 - d.row_sizes[s];
            else if (nbound == nshared) score = 1000 + (nshared > 0 ? 100 : 0) - d.row_sizes[s];
            else score = nbound * 16 - d.row_sizes[s];
            if (score > best_score) { best_score = score; best = s; }
        }
        plan.order[j] = best;
        plan.bound_before[j] = bound;
        plan.lookup[j] = (row_shared[best] & ~bound) == 0;
        bound |= row_shared[best];
        done |= 1u << best;
    }
    validate_gjoin_plan(plan);
    return plan;
}

}  // namespace cwsgpu
