// Exhaustive host checks of the GPU generation inputs against the CPU
// generator, on the production pool files:
//  * every (size, shared count) selection pool equals PALP MakeSelections'
//    stream, entry by entry and in order, with each shared prefix
//    nondecreasing (the fused structure-3 kernel relies on it);
//  * on all 46 descriptors two slots are ordered by pool index on the GPU
//    exactly when CWSGenerate orders them (same pool stream and a
//    descriptor-preserving transposition).
#include "descriptor.cuh"

#include <cstdio>
#include <iostream>

extern "C" {
FILE *ProjectReadWeightPool(const char *directory, int size);
int cpu_slot_exchange(int id, int i, int j);
int cpu_read_weight(FILE *in, int *d, int *n, long w[7]);
void MakeSelections(FILE *in, FILE *out, int u);
}

using namespace cwsgpu;

int main(int argc, char **argv) {
    const std::string dir = argc > 1 ? argv[1] : "docs/data/single-weight-systems";
    std::uint64_t entries = 0;
    for (int size = 2; size <= kMaxSize; ++size) {
        const std::vector<BaseWeight> base = load_base_pool(dir, size);
        for (int shared = 0; shared <= size; ++shared) {
            const std::vector<SelectedEntry> gpu = build_selected_pool(base, shared);
            FILE *pool = ProjectReadWeightPool(dir.c_str(), size);
            FILE *out = std::tmpfile();
            MakeSelections(pool, out, shared);
            std::rewind(out);
            std::size_t i = 0;
            int d = 0, n = 0;
            long w[7] = {};
            while (cpu_read_weight(out, &d, &n, w)) {
                if (i >= gpu.size() || gpu[i].degree != d || gpu[i].size != n) {
                    std::cerr << "selection pool size " << size << " shared " << shared << " differs at entry " << i << '\n';
                    return 1;
                }
                for (int p = 0; p < n; ++p)
                    if (gpu[i].w[p] != w[p] || (p + 1 < shared && w[p] > w[p + 1])) {
                        std::cerr << "selection pool size " << size << " shared " << shared << " entry " << i
                                  << " position " << p << " differs or breaks the shared order\n";
                        return 1;
                    }
                ++i;
            }
            if (i != gpu.size()) {
                std::cerr << "selection pool size " << size << " shared " << shared << ": GPU " << gpu.size()
                          << " entries, CPU " << i << '\n';
                return 1;
            }
            entries += i;
            std::fclose(out);
            std::fclose(pool);
        }
    }
    const PoolArray pools = build_all_pools(dir);
    int pairs = 0, ordered = 0;
    for (int id = 2; id <= 47; ++id) {
        const GpuDescriptor g = catalogue_descriptor(id);
        const DeviceDescriptor d = make_device_descriptor(g, pools);
        for (int i = 0; i < g.row_count; ++i)
            for (int j = i + 1; j < g.row_count; ++j) {
                // The condition descriptor_scan_kernel prunes on (equal families
                // also share the pool, so use_pair_scan is the same relation).
                const bool gpu_orders = d.family_groups[i] == d.family_groups[j];
                const int cpu = cpu_slot_exchange(id, i, j);
                if (cpu < 0 || gpu_orders != (cpu == 1)) {
                    std::cerr << "slot order differs: structure " << id << " slots " << i << ',' << j << '\n';
                    return 1;
                }
                ++pairs;
                ordered += gpu_orders;
            }
    }
    std::cout << "Selection pools equal MakeSelections on all " << entries << " entries of the 24 (size, shared) pools; "
              << "slot order equals CWSGenerate on all " << pairs << " slot pairs (" << ordered << " ordered)\n";
    return 0;
}
