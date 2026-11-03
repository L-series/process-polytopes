// CBMC query: for slot SLOT of historical descriptor CATALOGUE_ID and every
// assignment of positive int weights, the GPU shared-prefix test equals the
// verified CPU PrefixIsCanonical. gpu_extract.h holds the GPU functions copied
// verbatim from src/gpu by scripts/test-gpu-generation.sh.
#include "gpu_extract.h"

extern "C" int cpu_prefix_is_canonical(int id, int slot, const long weights[5][6]);
int nondet_int();

int main() {
    GpuDescriptor g;
    __CPROVER_assert(GpuCatalogueDescriptor(CATALOGUE_ID, &g) == 1, "catalogue entry exists");
    DeviceDescriptor d;
    d.id = g.id;
    d.ambient_vertices = g.coordinate_count;
    d.simplex_count = g.row_count;
    for (int s = 0; s < g.row_count; ++s) {
        d.simplex_sizes[s] = g.row_sizes[s];
        d.shared_counts[s] = g.shared_counts[s];
        for (int p = 0; p < g.row_sizes[s]; ++p) d.mappings[s][p] = g.mappings[s][p];
    }
    SelectedEntry entries[kMaxSlots];
    long weights[5][6] = {};
    for (int s = 0; s < g.row_count; ++s) {
        entries[s].size = g.row_sizes[s];
        for (int p = 0; p < g.row_sizes[s]; ++p) {
            const int w = nondet_int();
            __CPROVER_assume(w > 0);
            entries[s].w[p] = w;
            weights[s][p] = w;
        }
    }
    const int gpu = device_prefix_is_canonical(d, entries, SLOT);
#ifdef PROBE
    // Non-vacuity: on slots where pruning can apply this must FAIL.
    __CPROVER_assert(gpu == 1, "probe: the GPU test never rejects");
#else
    __CPROVER_assert(gpu == cpu_prefix_is_canonical(CATALOGUE_ID, SLOT, weights),
                     "GPU shared-prefix test equals CPU PrefixIsCanonical");
#endif
    return 0;
}
