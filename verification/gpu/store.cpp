// CBMC query: for descriptor CATALOGUE_ID and any positive row weights and
// degrees, the candidate the GPU stores has exactly the CWS rows PALP's
// EmitDescriptorCWS emits (EmbedWeightInCWS), zero elsewhere.
#include "gpu_extract.h"

extern "C" void cpu_embed(int id, const long weights[5][6], const long degrees[5], long out_degree[5],
                          long out_weights[5][10]);
int nondet_int();

int main() {
    GpuDescriptor g;
    __CPROVER_assert(GpuCatalogueDescriptor(CATALOGUE_ID, &g) == 1, "catalogue entry exists");
    DeviceDescriptor d;
    for (int s = 0; s < kMaxSlots; ++s) {
        d.simplex_sizes[s] = 0;
        d.shared_counts[s] = 0;
        for (int p = 0; p < kMaxSize; ++p) d.mappings[s][p] = 0;
    }
    d.id = g.id;
    d.ambient_vertices = g.coordinate_count;
    d.simplex_count = g.row_count;
    for (int s = 0; s < g.row_count; ++s) {
        d.simplex_sizes[s] = g.row_sizes[s];
        d.shared_counts[s] = g.shared_counts[s];
        for (int p = 0; p < g.row_sizes[s]; ++p) d.mappings[s][p] = g.mappings[s][p];
    }
    SelectedEntry entries[kMaxSlots];
    long weights[5][6] = {}, degrees[5] = {};
    for (int s = 0; s < g.row_count; ++s) {
        entries[s].size = g.row_sizes[s];
        entries[s].degree = nondet_int();
        __CPROVER_assume(entries[s].degree > 0);
        degrees[s] = entries[s].degree;
        entries[s].index = nondet_int();
        for (int p = 0; p < g.row_sizes[s]; ++p) {
            entries[s].w[p] = nondet_int();
            __CPROVER_assume(entries[s].w[p] > 0);
            weights[s][p] = entries[s].w[p];
        }
    }
    // CBMC does not apply default member initializers to locals: set the
    // fields the store reads explicitly.
    DeviceScanStats stats;
    stats.stored_candidate_count = 0;
    DeviceCwsCandidate out;
    device_store_candidate(d, entries, &out, 1, &stats);
    long cpu_degree[5], cpu_weights[5][10];
    cpu_embed(CATALOGUE_ID, weights, degrees, cpu_degree, cpu_weights);
    __CPROVER_assert(out.nw == g.row_count && out.ambient_vertices == g.coordinate_count, "row and coordinate counts");
    for (int s = 0; s < g.row_count; ++s) {
        __CPROVER_assert(out.degree[s] == cpu_degree[s], "row degree");
        for (int c = 0; c < g.coordinate_count; ++c) __CPROVER_assert(out.weights[s][c] == cpu_weights[s][c], "row weight");
        __CPROVER_assert(out.sel_index[s] == entries[s].index, "pool entry recorded");
    }
    return 0;
}
