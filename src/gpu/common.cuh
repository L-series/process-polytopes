// Shared limits, records and error handling of the CUDA CWS pipeline.
#pragma once

#include <cuda_runtime.h>

#include <cstdint>
#include <limits>
#include <stdexcept>
#include <string>
#include <vector>

namespace cwsgpu {

static constexpr int kMaxSlots = 5;        // weight rows per CWS
static constexpr int kMaxSize = 5;         // coordinates per weight row
static constexpr int kMaxCoordinates = 10; // ambient coordinates
static constexpr int kPoolKeyStride = 6;   // pool_key(size, shared count)

inline void check_cuda(cudaError_t status, const char *operation) {
    if (status != cudaSuccess) throw std::runtime_error(std::string(operation) + ": " + cudaGetErrorString(status));
}

inline std::uint64_t checked_mul(std::uint64_t left, std::uint64_t right) {
    if (right != 0 && left > std::numeric_limits<std::uint64_t>::max() / right)
        throw std::runtime_error("uint64 overflow while computing selection product");
    return left * right;
}

inline std::uint64_t checked_add(std::uint64_t left, std::uint64_t right) {
    if (left > std::numeric_limits<std::uint64_t>::max() - right)
        throw std::runtime_error("uint64 overflow while accumulating pipeline counts");
    return left + right;
}

// One weight system from a pool file; degree is the sum of the weights.
struct BaseWeight {
    int degree = 0;
    int size = 0;
    int w[kMaxSize] = {0, 0, 0, 0, 0};
};

// A weight system with a chosen set of shared positions moved to the front
// (shared-first layout), as one row of a descriptor slot.
struct SelectedEntry {
    int degree = 0;
    int size = 0;
    int w[kMaxSize] = {0, 0, 0, 0, 0};
    int index = 0;  // position in the flattened device pool
};

struct PoolInfo {
    std::vector<SelectedEntry> entries;
    std::uint64_t device_offset = 0;
};

struct DeviceDescriptor {
    int id = 0;
    int ambient_vertices = 0;
    int simplex_count = 0;
    int simplex_sizes[kMaxSlots] = {0, 0, 0, 0, 0};
    int shared_counts[kMaxSlots] = {0, 0, 0, 0, 0};
    int mappings[kMaxSlots][kMaxSize] = {};          // one-based coordinates, shared first
    int family_groups[kMaxSlots] = {0, 0, 0, 0, 0};  // slots that may be ordered by pool index
    std::uint64_t pool_offsets[kMaxSlots] = {0, 0, 0, 0, 0};
    std::uint64_t pool_counts[kMaxSlots] = {0, 0, 0, 0, 0};
    std::uint64_t selection_product = 0;
};

struct DeviceScanStats {
    unsigned long long selection_tuples = 0;
    unsigned long long canonical_selection_tuples = 0;
    unsigned long long prefix_candidates = 0;
    unsigned long long stored_candidate_count = 0;
    unsigned long long join_degenerate = 0;  // decided during generation (fused type-3 join)
    unsigned long long join_certified = 0;
};

// One generated CWS: degree and weights of every row in ambient coordinates.
struct DeviceCwsCandidate {
    int structure_id = 0;
    int nw = 0;
    int ambient_vertices = 0;
    int degree[kMaxSlots] = {0, 0, 0, 0, 0};
    int weights[kMaxSlots][kMaxCoordinates] = {};
    int sel_index[kMaxSlots] = {0, 0, 0, 0, 0};  // flattened pool entry per slot
    unsigned long long pipeline_id = 0; // chunk-local identity, audited through every routing stage
    int join_np = 0;  // exact point count from the join (0 = not joined)
};

struct HostScanResult {
    int structure_id = 0;
    std::uint64_t selection_product = 0;
    std::uint64_t scanned_tuples = 0;
    std::uint64_t canonical_selection_tuples = 0;
    std::uint64_t prefix_candidates = 0;
    std::uint64_t stored_candidate_count = 0;
    std::uint64_t join_degenerate = 0;
    std::uint64_t join_certified = 0;
    double seconds = 0.0;
};

}  // namespace cwsgpu
