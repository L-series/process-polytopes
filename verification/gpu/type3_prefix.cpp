// CBMC query: the fused structure-3 kernel's slot-1 prefix test equals the
// general device_prefix_is_canonical for structure 3, slot 1, for all
// positive weights.
#include "gpu_extract.h"

int nondet_int();

int main() {
    GpuDescriptor g;
    __CPROVER_assert(GpuCatalogueDescriptor(3, &g) == 1, "catalogue entry exists");
    DeviceDescriptor d;
    for (int s = 0; s < kMaxSlots; ++s) {
        d.simplex_sizes[s] = 0;
        d.shared_counts[s] = 0;
        for (int p = 0; p < kMaxSize; ++p) d.mappings[s][p] = 0;
    }
    d.id = 3;
    d.ambient_vertices = g.coordinate_count;
    d.simplex_count = g.row_count;
    for (int s = 0; s < g.row_count; ++s) {
        d.simplex_sizes[s] = g.row_sizes[s];
        d.shared_counts[s] = g.shared_counts[s];
        for (int p = 0; p < g.row_sizes[s]; ++p) d.mappings[s][p] = g.mappings[s][p];
    }
    SelectedEntry entries[kMaxSlots];
    for (int s = 0; s < 2; ++s) {
        entries[s].size = 5;
        for (int p = 0; p < 5; ++p) {
            entries[s].w[p] = nondet_int();
            __CPROVER_assume(entries[s].w[p] > 0);
        }
    }
    const int prefix[3] = {entries[1].w[0], entries[1].w[1], entries[1].w[2]};
    __CPROVER_assert(device_type3_prefix_ok(entries[0], prefix) == (device_prefix_is_canonical(d, entries, 1) == 1),
                     "fused slot-1 test equals the general test");
    return 0;
}
