// Optional fast path for structure 3 (compile with -DCWS_GPU_FUSED_TYPE3=1,
// `make gpu-type3`): generation fused with a join specialised to its two
// W5 rows sharing three coordinates, so only survivors are stored. Same
// mathematics as join.cuh; the general join remains the reference.
//
// For s the values on the shared coordinates, the point set is the union over
// s of {s} x L(s) x R(s), np = sum_s |L(s)| |R(s)|. Every pool entry's points
// are grouped once by s; a candidate is decided by joining two short sorted
// key lists. Per key we keep the count and, for the two private positions, the
// value classes (0 / 1 / 2 / >=3) and whether their sum reaches <= 1 / >= 3.
#pragma once

#include "generate.cuh"

#include <functional>
#include <thread>

namespace cwsgpu {

struct JoinKey {
    unsigned long long key;   // s0 << 32 | s1 << 16 | s2 (entry position order)
    unsigned int count;       // points of the row with this shared part
    unsigned char cat_a;      // value classes of entry position 3: bit0 0, bit1 1, bit2 2, bit3 >=3
    unsigned char cat_b;      // same for entry position 4
    unsigned char pair;       // bit0: some point has x3 + x4 <= 1, bit1: >= 3
    unsigned char pad;
};

struct JoinTables {
    const JoinKey *keys = nullptr;
    const unsigned long long *bloom = nullptr;  // 8 words (512 bits) per entry over its packed keys
    const unsigned long long *masks = nullptr;  // 3 per entry: bit v (v<63) / bit 63 (v>=63) = value present at shared position
    const unsigned long long *offsets = nullptr;  // pool_count + 1 entries
    const SelectedEntry *entries = nullptr;        // flattened pool (device)
    int pool_offset = 0;
    int pool_count = 0;
};

struct HostJoinTables {
    JoinTables view;
    JoinKey *d_keys = nullptr;
    unsigned long long *d_offsets = nullptr;
    unsigned long long *d_masks = nullptr;
    unsigned long long *d_bloom = nullptr;
    std::uint64_t key_count = 0;
};

__device__ __forceinline__ unsigned int device_join_cat(int v) {
    return v == 0 ? 1u : v == 1 ? 2u : v == 2 ? 4u : 8u;
}
// some point of {A-classes} x {B-classes} has X_A + X_B <= 1 / >= 3
__device__ __forceinline__ unsigned int device_join_lo(unsigned int a, unsigned int b) {
    return ((a & 1u) && (b & 3u)) || ((a & 2u) && (b & 1u));
}
__device__ __forceinline__ unsigned int device_join_hi(unsigned int a, unsigned int b) {
    return (a & 8u) || (b & 8u) || ((a & 4u) && (b & 14u)) || ((a & 14u) && (b & 4u));
}

__device__ __forceinline__ unsigned int device_join_find(const JoinKey *keys, unsigned int b, unsigned int e,
                                                         unsigned long long want) {
    unsigned int lo = b, hi = e;
    while (lo < hi) {
        unsigned int m = (lo + hi) >> 1;
        if (keys[m].key < want) lo = m + 1; else hi = m;
    }
    return (lo < e && keys[lo].key == want) ? lo : 0xFFFFFFFFu;
}

__device__ __forceinline__ unsigned long long device_join_pack(const int v[3]) {
    return (static_cast<unsigned long long>(v[0]) << 32) | (static_cast<unsigned long long>(v[1]) << 16) |
           static_cast<unsigned long long>(v[2]);
}

// Branch-free certificate bits. A 7x7 pair relation is a 49-bit
// matrix (bit 7A+B); spread(a) puts bit A of a 7-bit set at 7A, so
// spread(a) * b (b < 128) writes the row pattern b into every row in a.
__device__ __forceinline__ unsigned long long device_spread7(unsigned int a) {
    unsigned long long x = a;
    return (x & 1ULL) | ((x & 2ULL) << 6) | ((x & 4ULL) << 12) | ((x & 8ULL) << 18) |
           ((x & 16ULL) << 24) | ((x & 32ULL) << 30) | ((x & 64ULL) << 36);
}
// Accumulate one joined key {s} x L(s) x R(s): value classes per coordinate
// (shared exact, private as stored), pairs combine independently except the
// two same-row private pairs, which use the stored joint bits.
__device__ __forceinline__ void device_join_cert_bits(const JoinKey &kl, const JoinKey &kr,
                                                      unsigned int &tz, unsigned int &t2,
                                                      unsigned long long &LO, unsigned long long &HI,
                                                      unsigned int &p34, unsigned int &p56) {
    unsigned int m0 = 0, m1 = 0, m2 = 0, m3 = 0;
    const unsigned long long key = kl.key;
    #pragma unroll
    for (int ci = 0; ci < 3; ++ci) {
        const unsigned int v = static_cast<unsigned int>((key >> (32 - 16 * ci)) & 0xFFFF);
        const unsigned int bit = 1u << ci;
        m0 |= (v == 0) ? bit : 0u;
        m1 |= (v == 1) ? bit : 0u;
        m2 |= (v == 2) ? bit : 0u;
        m3 |= (v >= 3) ? bit : 0u;
    }
    const unsigned int c3 = kl.cat_a, c4 = kl.cat_b, c5 = kr.cat_a, c6 = kr.cat_b;
    m0 |= ((c3 & 1u) << 3) | ((c4 & 1u) << 4) | ((c5 & 1u) << 5) | ((c6 & 1u) << 6);
    m1 |= (((c3 >> 1) & 1u) << 3) | (((c4 >> 1) & 1u) << 4) | (((c5 >> 1) & 1u) << 5) | (((c6 >> 1) & 1u) << 6);
    m2 |= (((c3 >> 2) & 1u) << 3) | (((c4 >> 2) & 1u) << 4) | (((c5 >> 2) & 1u) << 5) | (((c6 >> 2) & 1u) << 6);
    m3 |= (((c3 >> 3) & 1u) << 3) | (((c4 >> 3) & 1u) << 4) | (((c5 >> 3) & 1u) << 5) | (((c6 >> 3) & 1u) << 6);
    tz |= m0;
    t2 |= m2 | m3;
    const unsigned int ge1 = m1 | m2 | m3;
    LO |= device_spread7(m0) * (m0 | m1) | device_spread7(m1) * m0;                       // X_A + X_B <= 1
    HI |= device_spread7(m3) * 0x7FULL | device_spread7(0x7F) * m3 |                      // X_A + X_B >= 3
          device_spread7(m2) * ge1 | device_spread7(ge1) * m2;
    p34 |= kl.pair;
    p56 |= kr.pair;
}

// Join of the left entry (shared values in candidate order) with the right
// entry (shared values permuted: candidate coordinate ci = right position
// perm[ci]). Iterates the shorter list and binary-searches the longer one
// (the joined set is symmetric), skipping keys via presence masks and the
// Bloom filter; certificate bits are accumulated inline for each joined key.

__device__ int device_join_decide(const JoinTables &jt, int L, int R, const int eRw[3],
                                  const int prefix[3], int np_cap, unsigned long long *np_out) {
    int perm[3] = {0, 1, 2};
    unsigned int used = 0;
    for (int ci = 0; ci < 3; ++ci)
        for (int pos = 0; pos < 3; ++pos)
            if (!((used >> pos) & 1u) && eRw[pos] == prefix[ci]) { perm[ci] = pos; used |= 1u << pos; break; }
    const unsigned int lb = static_cast<unsigned int>(jt.offsets[L]), le = static_cast<unsigned int>(jt.offsets[L + 1]);
    const unsigned int rb = static_cast<unsigned int>(jt.offsets[R]), re = static_cast<unsigned int>(jt.offsets[R + 1]);
    const bool iter_left = (le - lb) <= (re - rb);
    unsigned long long mS[3];
    for (int ci = 0; ci < 3; ++ci)
        mS[ci] = iter_left ? jt.masks[3 * R + perm[ci]] : jt.masks[3 * L + ci];
    const unsigned int ib = iter_left ? lb : rb, ie = iter_left ? le : re;
    const unsigned int sb = iter_left ? rb : lb, se = iter_left ? re : le;
    const unsigned long long *bl = jt.bloom + 8ULL * static_cast<unsigned long long>(iter_left ? R : L);
    unsigned long long np = 0;
    unsigned int tz = 0, t2 = 0, p34 = 0, p56 = 0;
    unsigned long long LO = 0, HI = 0;
    for (unsigned int k = ib; k < ie; ++k) {
        const unsigned long long key = jt.keys[k].key;
        const int v[3] = {static_cast<int>((key >> 32) & 0xFFFF), static_cast<int>((key >> 16) & 0xFFFF),
                          static_cast<int>(key & 0xFFFF)};
        int cv[3];   // candidate-order shared values of this key
        if (iter_left) { cv[0] = v[0]; cv[1] = v[1]; cv[2] = v[2]; }
        else { for (int ci = 0; ci < 3; ++ci) cv[ci] = v[perm[ci]]; }
        bool possible = true;
        for (int ci = 0; ci < 3; ++ci)
            possible = possible && ((mS[ci] >> (cv[ci] < 63 ? cv[ci] : 63)) & 1ULL);
        if (!possible) continue;
        int target[3];
        if (iter_left) { for (int ci = 0; ci < 3; ++ci) target[perm[ci]] = cv[ci]; }
        else { target[0] = cv[0]; target[1] = cv[1]; target[2] = cv[2]; }
        const unsigned long long want = device_join_pack(target);
        const unsigned h1 = static_cast<unsigned>((want * 0x9E3779B97F4A7C15ULL) >> 55);
        const unsigned h2 = static_cast<unsigned>((want * 0xC2B2AE3D27D4EB4FULL) >> 55);
        if (!((bl[h1 >> 6] >> (h1 & 63)) & 1ULL) || !((bl[h2 >> 6] >> (h2 & 63)) & 1ULL)) continue;
        const unsigned int f = device_join_find(jt.keys, sb, se, want);
        if (f == 0xFFFFFFFFu) continue;
        const JoinKey kl = jt.keys[iter_left ? k : f], kr = jt.keys[iter_left ? f : k];
        np += static_cast<unsigned long long>(kl.count) * kr.count;
        if (np > static_cast<unsigned long long>(np_cap)) { *np_out = np; return 2; }   // deferred by the walk
        device_join_cert_bits(kl, kr, tz, t2, LO, HI, p34, p56);
    }
    *np_out = np;
    if (np < 6) return 0;
    if (tz ^ t2) return 1;
    // upper triangle A < B, excluding the same-row private pairs (3,4) and (5,6)
    constexpr unsigned long long kUpper =
        (0x7EULL << 0) | (0x7CULL << 7) | (0x78ULL << 14) | (0x70ULL << 21) | (0x60ULL << 28) | (0x40ULL << 35);
    constexpr unsigned long long kGroupPairs = (1ULL << (7 * 3 + 4)) | (1ULL << (7 * 5 + 6));
    if ((LO ^ HI) & (kUpper & ~kGroupPairs)) return 1;
    if (((p34 & 1u) ^ ((p34 >> 1) & 1u)) || ((p56 & 1u) ^ ((p56 >> 1) & 1u))) return 1;
    return 2;
}

// Closed-form inverse of first_pair_for_left (largest l with
// l*n - l(l-1)/2 <= p): double-precision root, then exact integer correction.
__device__ __forceinline__ unsigned long long device_pair_left_closed(unsigned long long p, unsigned long long n) {
    double b = 2.0 * static_cast<double>(n) + 1.0;
    double disc = b * b - 8.0 * static_cast<double>(p);
    long long l = static_cast<long long>((b - sqrt(disc > 0.0 ? disc : 0.0)) * 0.5);
    if (l < 0) l = 0;
    if (l > static_cast<long long>(n) - 1) l = static_cast<long long>(n) - 1;
    while (l > 0 && device_first_pair_for_left(static_cast<unsigned long long>(l), n) > p) --l;
    while (static_cast<unsigned long long>(l) + 1 < n &&
           device_first_pair_for_left(static_cast<unsigned long long>(l) + 1, n) <= p) ++l;
    return static_cast<unsigned long long>(l);
}

// device_prefix_is_canonical for slot 1 of structure 3: slot 1 shares all three
// of slot 0's shared coordinates and no slot follows, so two of them may be
// ordered exactly when slot 0 gives them equal weights. Slot 0 itself is only
// emitted in pool order, the one arrangement its own test accepts here.
__device__ __forceinline__ bool device_type3_prefix_ok(const SelectedEntry &le, const int prefix[3]) {
    bool canonical = true;
    for (int i = 0; i < 2; ++i)
        for (int j = i + 1; j < 3; ++j)
            if (le.w[i] == le.w[j] && prefix[i] > prefix[j]) canonical = false;
    return canonical;
}

// Type-3 generation fused with the join. Same pair decoding and the
// same canonical prefix permutations as descriptor_pair_scan_kernel /
// device_emit_last_slot; every canonical candidate is decided by the join and
// only survivors are written (as device_store_candidate would write them).
__global__ void type3_join_scan_kernel(const SelectedEntry *entries,
                                       DeviceDescriptor descriptor,
                                       unsigned long long start_pair,
                                       unsigned long long pair_count,
                                       DeviceScanStats *stats,
                                       DeviceCwsCandidate *candidate_output,
                                       unsigned long long candidate_capacity,
                                       JoinTables jt, int np_cap) {
    unsigned long long local_selection = 0, local_prefix = 0, local_deg = 0, local_cert = 0;
    const unsigned long long global_thread = blockIdx.x * blockDim.x + threadIdx.x;
    const unsigned long long stride = blockDim.x * gridDim.x;
    const unsigned long long pool_count = descriptor.pool_counts[0];
    for (unsigned long long offset = global_thread; offset < pair_count; offset += stride) {
        const unsigned long long pair_index = start_pair + offset;
        const unsigned long long left = device_pair_left_closed(pair_index, pool_count);
        const unsigned long long right = left + (pair_index - device_first_pair_for_left(left, pool_count));
        const SelectedEntry le = entries[descriptor.pool_offsets[0] + left];
        const SelectedEntry re = entries[descriptor.pool_offsets[1] + right];
        ++local_selection;
        const int L = le.index - jt.pool_offset, R = re.index - jt.pool_offset;
        const int eRw[3] = {re.w[0], re.w[1], re.w[2]};
        int prefix[3] = {re.w[0], re.w[1], re.w[2]};
        do {
            if (!device_type3_prefix_ok(le, prefix)) continue;
            ++local_prefix;   // pool prefixes are sorted, so this visits every distinct permutation
            unsigned long long np = 0;
            const int verdict = device_join_decide(jt, L, R, eRw, prefix, np_cap, &np);
            if (verdict == 0) { ++local_deg; continue; }
            if (verdict == 1) { ++local_cert; continue; }
            const unsigned long long out_index = atomicAdd(&stats->stored_candidate_count, 1ULL);
            if (out_index >= candidate_capacity) continue;
            // Field-wise stores through a pointer, as device_store_candidate does:
            // a whole-struct assignment from a local here stored wrong bytes
            // (nvcc 13.3, sm_120).
            DeviceCwsCandidate *out = &candidate_output[out_index];
            out->structure_id = descriptor.id;
            out->nw = 2;
            out->ambient_vertices = 7;
            for (int slot = 0; slot < kMaxSlots; ++slot) {
                out->degree[slot] = 0;
                out->sel_index[slot] = 0;
                for (int coordinate = 0; coordinate < 10; ++coordinate) out->weights[slot][coordinate] = 0;
            }
            out->join_np = np <= static_cast<unsigned long long>(np_cap) ? static_cast<int>(np) : 0;
            out->degree[0] = le.degree;
            out->degree[1] = re.degree;
            for (int pos = 0; pos < 5; ++pos) out->weights[0][pos] = le.w[pos];
            out->weights[1][0] = prefix[0];
            out->weights[1][1] = prefix[1];
            out->weights[1][2] = prefix[2];
            out->weights[1][5] = re.w[3];
            out->weights[1][6] = re.w[4];
            out->sel_index[0] = le.index;
            out->sel_index[1] = re.index;
        } while (device_next_permutation(prefix, 3));
    }
    if (local_selection) {
        atomicAdd(&stats->selection_tuples, local_selection);
        atomicAdd(&stats->canonical_selection_tuples, local_selection);
    }
    if (local_prefix) atomicAdd(&stats->prefix_candidates, local_prefix);
    if (local_deg) atomicAdd(&stats->join_degenerate, local_deg);
    if (local_cert) atomicAdd(&stats->join_certified, local_cert);
}

inline void build_join_table(const std::vector<SelectedEntry> &flat, const SelectedEntry *device_entries,
                      int pool_offset, int pool_count, int size, int shared, HostJoinTables &out) {
    if (size - shared != 2 || shared < 1 || shared > 3)
        throw std::runtime_error("build_join_table: unsupported pool shape");
    std::vector<std::vector<JoinKey>> per(static_cast<std::size_t>(pool_count));
    auto work = [&](int begin, int end) {
        std::vector<std::array<int, 5>> pts;
        for (int e = begin; e < end; ++e) {
            const SelectedEntry &en = flat[static_cast<std::size_t>(pool_offset + e)];
            const int *w = en.w;
            const int d = en.degree;
            pts.clear();
            std::array<int, 5> x{};
            // all non-negative solutions of sum_i w_i x_i = d (last position by divisibility)
            std::function<void(int, int)> rec = [&](int pos, int rem) {
                if (pos == size - 1) {
                    if (rem % w[pos] == 0) { x[pos] = rem / w[pos]; pts.push_back(x); }
                    return;
                }
                for (int v = 0; v * w[pos] <= rem; ++v) { x[pos] = v; rec(pos + 1, rem - v * w[pos]); }
            };
            rec(0, d);
            std::sort(pts.begin(), pts.end());
            std::vector<JoinKey> &o = per[static_cast<std::size_t>(e)];
            auto keyof = [&](const std::array<int, 5> &q) {
                unsigned long long k = 0;
                for (int i = 0; i < 3; ++i) k = (k << 16) | static_cast<unsigned long long>(i < shared ? q[i] : 0);
                return k;
            };
            auto cls = [](int v) -> unsigned char { return v == 0 ? 1 : v == 1 ? 2 : v == 2 ? 4 : 8; };
            for (std::size_t i = 0; i < pts.size();) {
                std::size_t j = i;
                JoinKey k{};
                k.key = keyof(pts[i]);
                while (j < pts.size() && keyof(pts[j]) == k.key) {
                    ++k.count;
                    k.cat_a |= cls(pts[j][shared]);
                    k.cat_b |= cls(pts[j][shared + 1]);
                    int sum = pts[j][shared] + pts[j][shared + 1];
                    if (sum <= 1) k.pair |= 1;
                    if (sum >= 3) k.pair |= 2;
                    ++j;
                }
                o.push_back(k);
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
    std::vector<unsigned long long> masks(static_cast<std::size_t>(pool_count) * 3, 0);
    std::vector<unsigned long long> bloom(static_cast<std::size_t>(pool_count) * 8, 0);
    for (int e = 0; e < pool_count; ++e)
        for (const JoinKey &k : per[static_cast<std::size_t>(e)]) {
            for (int pos = 0; pos < 3; ++pos) {
                unsigned v = static_cast<unsigned>((k.key >> (32 - 16 * pos)) & 0xFFFF);
                masks[static_cast<std::size_t>(e) * 3 + pos] |= 1ULL << (v < 63 ? v : 63);
            }
            unsigned h1 = static_cast<unsigned>((k.key * 0x9E3779B97F4A7C15ULL) >> 55);
            unsigned h2 = static_cast<unsigned>((k.key * 0xC2B2AE3D27D4EB4FULL) >> 55);
            bloom[static_cast<std::size_t>(e) * 8 + (h1 >> 6)] |= 1ULL << (h1 & 63);
            bloom[static_cast<std::size_t>(e) * 8 + (h2 >> 6)] |= 1ULL << (h2 & 63);
        }
    std::vector<JoinKey> keys(static_cast<std::size_t>(offsets.back()));
    for (int e = 0; e < pool_count; ++e)
        std::copy(per[e].begin(), per[e].end(), keys.begin() + static_cast<std::ptrdiff_t>(offsets[e]));
    auto upload = [](auto **dst, const auto &v, const char *what) {
        check_cuda(cudaMalloc(dst, std::max<std::size_t>(v.size(), 1) * sizeof(v[0])), what);
        check_cuda(cudaMemcpy(*dst, v.data(), v.size() * sizeof(v[0]), cudaMemcpyHostToDevice), what);
    };
    upload(&out.d_bloom, bloom, "join bloom");
    upload(&out.d_masks, masks, "join masks");
    upload(&out.d_keys, keys, "join keys");
    upload(&out.d_offsets, offsets, "join offsets");
    out.view.keys = out.d_keys;
    out.view.masks = out.d_masks;
    out.view.bloom = out.d_bloom;
    out.view.offsets = out.d_offsets;
    out.view.entries = device_entries;
    out.view.pool_offset = pool_offset;
    out.view.pool_count = pool_count;
    out.key_count = keys.size();
}

// Generates the candidates of pair range [start, start + count) of structure 3,
// decides them by the join and stores the survivors.
inline HostScanResult scan_type3_fused(const DeviceDescriptor &descriptor, const SelectedEntry *device_entries,
                                       DeviceScanStats *device_stats, DeviceCwsCandidate *candidate_output,
                                       std::uint64_t candidate_capacity, int blocks, int threads,
                                       const JoinTables &tables, int np_cap, std::uint64_t start, std::uint64_t count) {
    check_cuda(cudaMemset(device_stats, 0, sizeof(DeviceScanStats)), "cudaMemset stats");
    auto t0 = std::chrono::steady_clock::now();
    type3_join_scan_kernel<<<blocks, threads>>>(device_entries, descriptor, start, count, device_stats,
                                                candidate_output, candidate_capacity, tables, np_cap);
    check_cuda(cudaGetLastError(), "type3_join_scan_kernel launch");
    check_cuda(cudaDeviceSynchronize(), "type3_join_scan_kernel");
    DeviceScanStats stats{};
    check_cuda(cudaMemcpy(&stats, device_stats, sizeof(stats), cudaMemcpyDeviceToHost), "cudaMemcpy stats");
    HostScanResult result;
    result.structure_id = descriptor.id;
    result.selection_product = descriptor.selection_product;
    result.scanned_tuples = stats.selection_tuples;
    result.canonical_selection_tuples = stats.canonical_selection_tuples;
    result.prefix_candidates = stats.prefix_candidates;
    result.stored_candidate_count = stats.stored_candidate_count;
    result.join_degenerate = stats.join_degenerate;
    result.join_certified = stats.join_certified;
    result.seconds = std::chrono::duration<double>(std::chrono::steady_clock::now() - t0).count();
    return result;
}

// Structure 3 layout assumed by the fused kernel: rows on coordinates {1..5} and
// {1,2,3,6,7}, the first three positions of each shared.
inline bool type3_layout_supported(const GpuDescriptor &d) {
    static const int rows[2][5] = {{1, 2, 3, 4, 5}, {1, 2, 3, 6, 7}};
    if (d.id != 3 || d.row_count != 2) return false;
    for (int s = 0; s < 2; ++s) {
        if (d.row_sizes[s] != 5 || d.shared_counts[s] != 3) return false;
        for (int p = 0; p < 5; ++p) if (d.mappings[s][p] != rows[s][p]) return false;
    }
    return true;
}

}  // namespace cwsgpu
