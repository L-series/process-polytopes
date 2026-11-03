// Weight pools: the single weight systems read from the project pool files,
// and for each (size, shared count) the list of distinct shared-position
// choices in shared-first layout (one entry per choice).
#pragma once

#include "common.cuh"

#include <array>
#include <cerrno>
#include <cstdlib>
#include <fstream>
#include <numeric>
#include <sstream>

namespace cwsgpu {

// Reads `<dir>/<size-1>d.txt` (sorted positive primitive weights per line, as
// used by the CPU generator); size 2 is the builtin (1,1).
inline std::vector<BaseWeight> load_base_pool(const std::string &dir, int size) {
    std::vector<BaseWeight> pool;
    if (size == 2) {
        BaseWeight entry;
        entry.size = 2; entry.degree = 2; entry.w[0] = 1; entry.w[1] = 1;
        pool.push_back(entry);
        return pool;
    }
    const std::string path = dir + "/" + std::to_string(size - 1) + "d.txt";
    std::ifstream input(path);
    if (!input) throw std::runtime_error("cannot open weight pool " + path + "; use --weights-dir DIRECTORY");
    std::string line;
    while (std::getline(input, line)) {
        std::istringstream fields(line);
        BaseWeight entry;
        entry.size = size;
        long degree = 0;
        int gcd = 0;
        for (int i = 0; i < size; ++i) {
            long value = 0;
            if (!(fields >> value) || value <= 0 || value > std::numeric_limits<int>::max() - degree ||
                (i && value < entry.w[i - 1]))
                throw std::runtime_error("invalid weight row in " + path + " (expected sorted positive integers)");
            entry.w[i] = static_cast<int>(value);
            degree += value;
            gcd = std::gcd(gcd, entry.w[i]);
        }
        std::string extra;
        if (fields >> extra || gcd != 1)
            throw std::runtime_error("invalid weight row in " + path + " (wrong width or nonprimitive)");
        entry.degree = static_cast<int>(degree);
        pool.push_back(entry);
    }
    if (pool.empty()) throw std::runtime_error("empty weight pool " + path);
    return pool;
}

inline void append_selected_weight(const BaseWeight &source,
                            const int selected_indices[kMaxSize],
                            int selected_count,
                            std::vector<SelectedEntry> &selected) {
    SelectedEntry entry;
    entry.degree = source.degree;
    entry.size = source.size;

    int selected_position = 0;
    int output_position = 0;
    for (int index = 0; index < selected_count; ++index) {
        entry.w[output_position++] = source.w[selected_indices[index]];
    }
    for (int index = 0; index < source.size; ++index) {
        if (selected_position < selected_count && index == selected_indices[selected_position]) {
            ++selected_position;
            continue;
        }
        entry.w[output_position++] = source.w[index];
    }
    selected.push_back(entry);
}

inline void enumerate_selections(const BaseWeight &source,
                          int selected_count,
                          int selected_indices[kMaxSize],
                          int selected_so_far,
                          std::vector<SelectedEntry> &selected) {
    if (selected_so_far == selected_count) {
        append_selected_weight(source, selected_indices, selected_count, selected);
        return;
    }

    int last_index = selected_indices[selected_so_far - 1];
    if (last_index == source.size - 1) return;

    if (source.w[last_index + 1] == source.w[last_index]) {
        selected_indices[selected_so_far] = last_index + 1;
        enumerate_selections(source, selected_count, selected_indices,
                             selected_so_far + 1, selected);
    }

    for (int index = last_index + 1; index < source.size; ++index) {
        if (source.w[index] > source.w[index - 1]) {
            selected_indices[selected_so_far] = index;
            enumerate_selections(source, selected_count, selected_indices,
                                 selected_so_far + 1, selected);
        }
    }
}

inline std::vector<SelectedEntry> build_selected_pool(const std::vector<BaseWeight> &base,
                                               int selected_count) {
    std::vector<SelectedEntry> selected;
    int selected_indices[kMaxSize] = {0, 0, 0, 0, 0};
    for (const BaseWeight &source : base) {
        if (selected_count == 0) {
            SelectedEntry entry;
            entry.degree = source.degree;
            entry.size = source.size;
            for (int coord = 0; coord < source.size; ++coord) entry.w[coord] = source.w[coord];
            selected.push_back(entry);
            continue;
        }

        selected_indices[0] = 0;
        enumerate_selections(source, selected_count, selected_indices, 1, selected);
        for (int index = 1; index < source.size; ++index) {
            if (source.w[index] > source.w[index - 1]) {
                selected_indices[0] = index;
                enumerate_selections(source, selected_count, selected_indices, 1, selected);
            }
        }
    }
    return selected;
}

inline int pool_key(int simplex_size, int shared_count) { return simplex_size * kPoolKeyStride + shared_count; }

using PoolArray = std::array<PoolInfo, kPoolKeyStride * kPoolKeyStride>;

// Every (size, shared count) pool, laid out back to back in one device array.
inline PoolArray build_all_pools(const std::string &weights_dir) {
    PoolArray pools;
    for (int simplex_size = 2; simplex_size <= kMaxSize; ++simplex_size) {
        const std::vector<BaseWeight> base = load_base_pool(weights_dir, simplex_size);
        for (int shared_count = 0; shared_count <= simplex_size; ++shared_count)
            pools[pool_key(simplex_size, shared_count)].entries = build_selected_pool(base, shared_count);
    }
    std::uint64_t offset = 0;
    for (PoolInfo &pool : pools) {
        pool.device_offset = offset;
        offset += static_cast<std::uint64_t>(pool.entries.size());
    }
    return pools;
}

inline std::vector<SelectedEntry> flatten_pools(const PoolArray &pools) {
    std::uint64_t total = 0;
    for (const PoolInfo &pool : pools) total += static_cast<std::uint64_t>(pool.entries.size());
    std::vector<SelectedEntry> flat;
    flat.reserve(static_cast<std::size_t>(total));
    for (const PoolInfo &pool : pools) {
        flat.insert(flat.end(), pool.entries.begin(), pool.entries.end());
    }
    for (std::size_t i = 0; i < flat.size(); ++i) flat[i].index = static_cast<int>(i);
    return flat;
}

}  // namespace cwsgpu
