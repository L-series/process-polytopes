// General join: decides most candidates from precomputed single-row tables
// before any lattice point is enumerated.
#pragma once

#include "descriptor.cuh"
#include "lattice.cuh"

#include <algorithm>
#include <array>
#include <chrono>
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

// Returns 0 = degenerate (np < 6), 1 = certified non-IP, 2 = keep; *np_out is
// the exact point count. Layouts the tables do not cover return 2 with
// *np_out = 0, and the walk decides them as before.
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
    unsigned long long np = 0;
    unsigned int tz = 0, t2 = 0, m3_all = 0;
    unsigned int LO[10] = {}, HI[10] = {}, LS[10] = {}, HS[10] = {};

    // Position the iterator of depth j: a lookup starts fresh; a scan with a
    // bound shared position covers only that value's range in the position's
    // sorted copy; a scan with none covers all keys.
    auto begin_level = [&](int j) {
        const int s = plan.order[j];
        const int k = plan.shared[s];
        it[j] = kb[s];
        end[j] = ke[s];
        pstar[j] = -1;
        if (plan.lookup[j]) return;
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
    };

    int depth = 0;
    begin_level(0);
    while (depth >= 0) {
        const int s = plan.order[depth];
        const int k = plan.shared[s];
        const GJoinKey *keys = plan.table[s].keys;
        bool found = false;
        if (plan.lookup[depth]) {
            if (it[depth] == kb[s]) {
                unsigned long long want = 0;
                for (int p = 0; p < k; ++p) want = (want << 16) | static_cast<unsigned long long>(val[epos_coord[s][p]]);
                unsigned long long lo = kb[s], hi = ke[s];
                while (lo < hi) {
                    const unsigned long long m = (lo + hi) >> 1;
                    if (keys[m].key < want) lo = m + 1; else hi = m;
                }
                if (lo < ke[s] && keys[lo].key == want) { chosen[depth] = lo; found = true; }
                it[depth] = ke[s];
            }
        } else {
            const unsigned int bound = plan.bound_before[depth];
            const GJoinIdx *px = pstar[depth] > 0 ? plan.table[s].pidx[pstar[depth]] : nullptr;
            while (it[depth] < end[depth]) {
                const unsigned long long pos = it[depth]++;
                const unsigned long long kk = px ? static_cast<unsigned long long>(px[pos].idx) : pos;
                const unsigned long long key = keys[kk].key;
                bool ok = true;
                for (int p = 0; p < k; ++p) {
                    const int A = epos_coord[s][p];
                    const int v = static_cast<int>((key >> (16 * (k - 1 - p))) & 0xFFFFULL);
                    if ((bound >> A) & 1u) { if (val[A] != v) { ok = false; break; } }
                    else val[A] = v;
                }
                if (ok) { chosen[depth] = kk; found = true; break; }
            }
        }
        if (!found) { --depth; continue; }
        if (depth + 1 < R) {
            ++depth;
            begin_level(depth);
            continue;
        }
        // Leaf: one block {s} x prod_r F_r(s).
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
            if (g.count == 0 || cnt > (~0ULL) / g.count) return 2;
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
                    if ((g.pairs >> (2 * u)) & 1u) LS[a] |= 1u << bb;
                    if ((g.pairs >> (2 * u + 1)) & 1u) HS[a] |= 1u << bb;
                }
        }
        if (np > (~0ULL) - cnt) return 2;
        np += cnt;
        tz |= m0;
        t2 |= m2 | m3;
        m3_all |= m3;
        const unsigned int ge1 = m1 | m2 | m3;
        for (unsigned int x = m0; x; x &= x - 1) LO[__ffs(x) - 1] |= m0 | m1;
        for (unsigned int x = m1; x; x &= x - 1) LO[__ffs(x) - 1] |= m0;
        for (unsigned int x = m3; x; x &= x - 1) HI[__ffs(x) - 1] = 0x3FFu;
        for (unsigned int x = m2; x; x &= x - 1) HI[__ffs(x) - 1] |= ge1;
        for (unsigned int x = ge1; x; x &= x - 1) HI[__ffs(x) - 1] |= m2;
    }
    *np_out = np;
    if (np < 6) return 0;
    const unsigned int all = (1u << N) - 1u;
    if ((tz ^ t2) & all) return 1;
    for (int A = 0; A < N; ++A) {
        const unsigned int upper = all & ~((2u << A) - 1u);   // B > A
        const unsigned int same = plan.same_row[A];
        const unsigned int lo = ((LO[A] & ~same) | (LS[A] & same)) & upper;
        const unsigned int hi = (((HI[A] | m3_all) & ~same) | (HS[A] & same)) & upper;
        if (lo ^ hi) return 1;
    }
    return 2;
}

// Device copy of one pool's table; freed with the process.
struct HostGJoinTable {
    GJoinTable view;
    GJoinIdx *d_pidx[4] = {nullptr, nullptr, nullptr, nullptr};
    GJoinKey *d_keys = nullptr;
    unsigned long long *d_offsets = nullptr;
    std::uint64_t key_count = 0;
    std::uint64_t point_count = 0;
};

inline void build_gjoin_table(const PoolInfo &pool, int size, int shared, HostGJoinTable &out) {
    const int pool_count = static_cast<int>(pool.entries.size());
    const int npriv = size - shared;
    if (size < 1 || size > kMaxSize || shared < 0 || shared > size || shared > 4)
        throw std::runtime_error("gjoin: unsupported table shape");
    std::vector<std::vector<GJoinKey>> per(static_cast<std::size_t>(pool_count));
    std::vector<std::uint64_t> points_per(static_cast<std::size_t>(pool_count), 0);
    auto work = [&](int begin, int end) {
        std::vector<std::array<int, 5>> pts;
        for (int e = begin; e < end; ++e) {
            const SelectedEntry &en = pool.entries[static_cast<std::size_t>(e)];
            const int *w = en.w;
            pts.clear();
            std::array<int, 5> x{};
            std::function<void(int, int)> rec = [&](int pos, int rem) {
                if (pos == size - 1) {
                    if (rem % w[pos] == 0) { x[pos] = rem / w[pos]; pts.push_back(x); }
                    return;
                }
                for (int v = 0; v * w[pos] <= rem; ++v) { x[pos] = v; rec(pos + 1, rem - v * w[pos]); }
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
            for (std::size_t i = 0; i < pts.size();) {
                GJoinKey g{};
                g.key = keyof(pts[i]);
                std::size_t j = i;
                for (; j < pts.size() && keyof(pts[j]) == g.key; ++j) {
                    if (g.count == std::numeric_limits<unsigned int>::max())
                        throw std::runtime_error("gjoin: fibre count exceeds 32 bits");
                    ++g.count;
                    for (int t = 0; t < npriv; ++t) g.cats |= cls(pts[j][shared + t]) << (4 * t);
                    int u = 0;
                    for (int t1 = 0; t1 < npriv; ++t1)
                        for (int t2 = t1 + 1; t2 < npriv; ++t2, ++u) {
                            const int sum = pts[j][shared + t1] + pts[j][shared + t2];
                            if (sum <= 1) g.pairs |= 1u << (2 * u);
                            if (sum >= 3) g.pairs |= 1u << (2 * u + 1);
                        }
                }
                o.push_back(g);
                i = j;
            }
        }
    };
    unsigned int nthreads = std::max(1u, std::thread::hardware_concurrency());
    std::vector<std::thread> threads;
    for (unsigned int t = 0; t < nthreads; ++t) {
        int b0 = static_cast<int>(static_cast<long long>(pool_count) * t / nthreads);
        int e0 = static_cast<int>(static_cast<long long>(pool_count) * (t + 1) / nthreads);
        threads.emplace_back(work, b0, e0);
    }
    for (auto &th : threads) th.join();
    std::vector<unsigned long long> offsets(static_cast<std::size_t>(pool_count) + 1, 0);
    for (int e = 0; e < pool_count; ++e) offsets[e + 1] = offsets[e] + per[static_cast<std::size_t>(e)].size();
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
        }
        if (keys.size() > 0xFFFFFFFFULL) throw std::runtime_error("gjoin: table exceeds 32-bit indices");
        check_cuda(cudaMalloc(&out.d_pidx[p], std::max<std::size_t>(px.size(), 1) * sizeof(GJoinIdx)), "gjoin pidx");
        check_cuda(cudaMemcpy(out.d_pidx[p], px.data(), px.size() * sizeof(GJoinIdx), cudaMemcpyHostToDevice), "gjoin pidx");
        out.view.pidx[p] = out.d_pidx[p];
    }
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
    return plan;
}

}  // namespace cwsgpu
