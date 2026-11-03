// CBMC queries for the comparison-only reduction behind the exhaustive
// emission check (emission_host.cpp): on two inputs with the same order type
// the generation primitives agree.
//  * PERMUTATION (U values): one step of device_next_permutation continues on
//    both or neither, and maps each position to corresponding values: every
//    output pair (a'_i, b'_i) is an input pair (a_t, b_t).
//  * PREFIX (CATALOGUE_ID, SLOT): device_prefix_is_canonical gives the same
//    answer for positive weights with the same order type within each slot.
#include "gpu_extract.h"

int nondet_int();

#ifdef PERMUTATION
int main() {
    int a[5], b[5], a0[5], b0[5];
    for (int i = 0; i < U; ++i) { a[i] = a0[i] = nondet_int(); b[i] = b0[i] = nondet_int(); }
    for (int i = 0; i < U; ++i)
        for (int j = 0; j < U; ++j)
            __CPROVER_assume((a[i] < a[j]) == (b[i] < b[j]) && (a[i] == a[j]) == (b[i] == b[j]));
    const int ra = device_next_permutation(a, U), rb = device_next_permutation(b, U);
    __CPROVER_assert(ra == rb, "same continuation");
    for (int i = 0; i < U; ++i) {
        int corresponds = 0;
        for (int t = 0; t < U; ++t) corresponds |= a[i] == a0[t] && b[i] == b0[t];
        __CPROVER_assert(corresponds, "positions carry corresponding values");
    }
    return 0;
}
#else
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
    SelectedEntry e[kMaxSlots], f[kMaxSlots];
    for (int s = 0; s < g.row_count; ++s) {
        d.simplex_sizes[s] = g.row_sizes[s];
        d.shared_counts[s] = g.shared_counts[s];
        for (int p = 0; p < g.row_sizes[s]; ++p) d.mappings[s][p] = g.mappings[s][p];
        e[s].size = f[s].size = g.row_sizes[s];
        for (int p = 0; p < g.row_sizes[s]; ++p) {
            e[s].w[p] = nondet_int();
            f[s].w[p] = nondet_int();
            __CPROVER_assume(e[s].w[p] > 0 && f[s].w[p] > 0);
        }
        for (int p = 0; p < g.row_sizes[s]; ++p)
            for (int q = 0; q < g.row_sizes[s]; ++q)
                __CPROVER_assume((e[s].w[p] < e[s].w[q]) == (f[s].w[p] < f[s].w[q]) &&
                                 (e[s].w[p] == e[s].w[q]) == (f[s].w[p] == f[s].w[q]));
    }
    __CPROVER_assert(device_prefix_is_canonical(d, e, SLOT) == device_prefix_is_canonical(d, f, SLOT),
                     "same answer on the same order type");
    return 0;
}
#endif
