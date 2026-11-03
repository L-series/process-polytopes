// Exhaustive host check: with every candidate kept by the join, the fused
// structure-3 kernel (type3_join_scan_kernel, one thread) stores exactly the
// candidates of the general generator (device_emit_small_prefixes, which
// emission_host.cpp equates with PALP), in the same order, for every pair of
// slot weight order types (the reduction argued in emission_host.cpp). The
// join itself is replaced by a stub that keeps everything; its rejections are
// covered by the join proofs.
#define __global__
struct Dim { unsigned x; };
static const Dim blockIdx = {0}, threadIdx = {0}, blockDim = {1}, gridDim = {1};
template <class Tables>
int device_join_decide(const Tables &, int, int, const int *, const int *, int, unsigned long long *np) {
    *np = 0;
    return 2;
}
#include "gpu_extract.h"

#include <cstdio>
#include <vector>

static std::vector<std::vector<int>> order_types(int n) {
    std::vector<std::vector<int>> out;
    std::vector<int> t(n, 1);
    while (true) {
        int seen = 0;
        for (int v : t) seen |= 1 << v;
        int m = 0;
        while (seen & (1 << (m + 1))) ++m;
        if (seen == ((1 << (m + 1)) - 2)) out.push_back(t);
        int p = n - 1;
        while (p >= 0 && t[p] == n) t[p--] = 1;
        if (p < 0) break;
        ++t[p];
    }
    return out;
}

int main() {
    GpuDescriptor g;
    if (GpuCatalogueDescriptor(3, &g) != 1) return 1;
    DeviceDescriptor d = {};
    d.id = g.id;
    d.ambient_vertices = g.coordinate_count;
    d.simplex_count = g.row_count;
    for (int s = 0; s < g.row_count; ++s) {
        d.simplex_sizes[s] = g.row_sizes[s];
        d.shared_counts[s] = g.shared_counts[s];
        for (int p = 0; p < g.row_sizes[s]; ++p) d.mappings[s][p] = g.mappings[s][p];
    }
    d.pool_counts[0] = d.pool_counts[1] = 2;
    const std::vector<std::vector<int>> types = order_types(5);
    DeviceCwsCandidate general[64], fused[64];
    JoinTables jt = {};
    unsigned long long cases = 0, stored = 0;
    for (const std::vector<int> &a : types)
        for (const std::vector<int> &b : types) {
            // Pool entries hold their shared weights nondecreasing.
            if (a[0] > a[1] || a[1] > a[2] || b[0] > b[1] || b[1] > b[2]) continue;
            SelectedEntry e[2] = {};
            for (int s = 0; s < 2; ++s) {
                e[s].size = 5;
                e[s].index = s;
                e[s].degree = 1000 + s;
                for (int p = 0; p < 5; ++p) e[s].w[p] = (s ? b : a)[p];
            }
            DeviceScanStats gs = {}, fs = {};
            SelectedEntry tuple[kMaxSlots] = {e[0], e[1]};
            device_emit_small_prefixes(d, tuple, general, 64, &gs);
            type3_join_scan_kernel(e, d, 1, 1, &fs, fused, 64, jt, 8192);  // pair 1 = (0, 1)
            bool same = fs.stored_candidate_count == gs.stored_candidate_count && fs.prefix_candidates == gs.stored_candidate_count &&
                        fs.selection_tuples == 1 && gs.stored_candidate_count <= 64;
            for (unsigned long long i = 0; same && i < gs.stored_candidate_count; ++i) {
                same = fused[i].structure_id == general[i].structure_id && fused[i].nw == general[i].nw &&
                       fused[i].ambient_vertices == general[i].ambient_vertices;
                for (int s = 0; s < kMaxSlots; ++s) {
                    same = same && fused[i].degree[s] == general[i].degree[s];
                    if (s < 2) same = same && fused[i].sel_index[s] == general[i].sel_index[s];
                    for (int c = 0; c < 10; ++c) same = same && fused[i].weights[s][c] == general[i].weights[s][c];
                }
            }
            if (!same) {
                std::fprintf(stderr, "fused structure-3 generation differs (general %llu, fused %llu)\n",
                             gs.stored_candidate_count, fs.stored_candidate_count);
                return 1;
            }
            ++cases;
            stored += gs.stored_candidate_count;
        }
    std::printf("Fused structure-3 generation equals the general generator: %llu order-type pairs, %llu candidates\n",
                cases, stored);
    return 0;
}
