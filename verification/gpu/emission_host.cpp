// Exhaustive host check: for every descriptor, the GPU emits exactly the
// candidates PALP's EnumeratePermutations emits from slot 0, in the same order,
// and device_count_small_prefixes counts them.
//
// Reduction to a finite domain. Both sides compare weights only with each
// other within one slot (the prefix permutation step and w[i] > w[j] compare
// positions of the current slot; PrefixIsCanonical's earlier-slot test compares
// two weights of that earlier slot, or one with 0 when the slot lacks the
// coordinate) and otherwise copy them. Weights are positive, so a map that is
// strictly increasing on each slot's values separately commutes with both
// functions. Each slot's weights can therefore be replaced by their order
// type (its rank tuple). All rank-fixed tuples per slot, in every combination,
// are run; agree_on_positive (GpuEmission.lean) then covers all positive
// weights. Degrees are copied only and get sentinels.
#include "gpu_extract.h"

#include <cstdio>
#include <vector>

extern "C" int cpu_enumerate_buffers(int id, const void *weights, const void *degrees, void *out_degrees,
                                     void *out_weights, int capacity);

// Order types of n positions, as RankFixed in GpuEmission.lean: tuples over
// 1..n equal to their own rank tuple, rank(t, x) = 1 + #{distinct v in t : v < x}.
static std::vector<std::vector<int>> order_types(int n) {
    std::vector<std::vector<int>> out;
    std::vector<int> t(n, 1);
    while (true) {
        bool fixed = true;
        for (int i = 0; i < n && fixed; ++i) {
            int below = 0;
            for (int v = 1; v < t[i]; ++v) {
                bool present = false;
                for (int x : t) present |= x == v;
                below += present;
            }
            fixed = t[i] == below + 1;
        }
        if (fixed) out.push_back(t);
        int p = n - 1;
        while (p >= 0 && t[p] == n) t[p--] = 1;
        if (p < 0) break;
        ++t[p];
    }
    return out;
}

int main() {
    const int capacity = 1 << 16;
    std::vector<DeviceCwsCandidate> gpu_out(capacity);
    std::vector<long> cpu_degrees(capacity * 5), cpu_weights(capacity * 5 * 10);
    unsigned long long cases = 0, emitted = 0;
    for (int id = 2; id <= 47; ++id) {
        GpuDescriptor g;
        if (GpuCatalogueDescriptor(id, &g) != 1) { std::fprintf(stderr, "no descriptor %d\n", id); return 1; }
        DeviceDescriptor d = {};
        d.id = g.id;
        d.ambient_vertices = g.coordinate_count;
        d.simplex_count = g.row_count;
        for (int s = 0; s < g.row_count; ++s) {
            d.simplex_sizes[s] = g.row_sizes[s];
            d.shared_counts[s] = g.shared_counts[s];
            for (int p = 0; p < g.row_sizes[s]; ++p) d.mappings[s][p] = g.mappings[s][p];
        }
        std::vector<std::vector<std::vector<int>>> types(g.row_count);
        for (int s = 0; s < g.row_count; ++s) types[s] = order_types(g.row_sizes[s]);
        std::vector<std::size_t> pick(g.row_count, 0);
        while (true) {
            SelectedEntry entries[kMaxSlots] = {};
            long weights[5][6] = {}, degrees[5] = {};
            for (int s = 0; s < g.row_count; ++s) {
                entries[s].size = g.row_sizes[s];
                entries[s].index = s;
                entries[s].degree = degrees[s] = 1000 + s;
                for (int p = 0; p < g.row_sizes[s]; ++p) entries[s].w[p] = weights[s][p] = types[s][pick[s]][p];
            }
            DeviceScanStats stats = {};
            const unsigned long long counted = device_count_small_prefixes(d, entries);
            device_emit_small_prefixes(d, entries, gpu_out.data(), capacity, &stats);
            const int cpu = cpu_enumerate_buffers(id, weights, degrees, cpu_degrees.data(), cpu_weights.data(), capacity);
            bool same = cpu < capacity && stats.stored_candidate_count == (unsigned long long)cpu && counted == (unsigned long long)cpu;
            for (int i = 0; same && i < cpu; ++i)
                for (int s = 0; s < g.row_count; ++s) {
                    same = same && gpu_out[i].degree[s] == cpu_degrees[i * 5 + s];
                    for (int c = 0; c < 10; ++c) same = same && gpu_out[i].weights[s][c] == cpu_weights[(i * 5 + s) * 10 + c];
                }
            if (!same) {
                std::fprintf(stderr, "emission differs: structure %d (GPU %llu stored, %llu counted, CPU %d)\n", id,
                             stats.stored_candidate_count, counted, cpu);
                return 1;
            }
            ++cases;
            emitted += cpu;
            int s = g.row_count - 1;
            while (s >= 0 && ++pick[s] == types[s].size()) pick[s--] = 0;
            if (s < 0) break;
        }
    }
    std::printf("GPU emission equals EnumeratePermutations on all 46 descriptors: %llu order-type cases, %llu candidates\n",
                cases, emitted);
    return 0;
}
