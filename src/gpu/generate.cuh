// Candidate generation: every selection tuple of pool entries, the
// permutations of each slot's shared prefix, and the two symmetry prunings
// (slot order by pool index within an orbit; shared prefix order when the two
// coordinates are interchangeable). Mirrors PALP's CWSGenerate (cws.c,
// EnumerateWeights / EnumeratePermutations / PrefixIsCanonical).
#pragma once

#include "common.cuh"

#include <chrono>

namespace cwsgpu {

__device__ int device_next_permutation(int *values, int count) {
    if (count < 2) return 0;
    int left = count - 2;
    while (left >= 0 && values[left] >= values[left + 1]) --left;
    if (left < 0) return 0;
    int right = count - 1;
    while (values[left] >= values[right]) --right;
    int tmp = values[left];
    values[left] = values[right];
    values[right] = tmp;
    for (int swap_index = left + 1, end = count - 1; swap_index < end; ++swap_index, --end) {
        tmp = values[swap_index];
        values[swap_index] = values[end];
        values[end] = tmp;
    }
    return 1;
}

__device__ int device_weight_at_coordinate(const DeviceDescriptor &descriptor,
                                           const SelectedEntry entries[kMaxSlots],
                                           int slot,
                                           int coordinate) {
    for (int position = 0; position < entries[slot].size; ++position) {
        if (descriptor.mappings[slot][position] - 1 == coordinate) return entries[slot].w[position];
    }
    return 0;
}

__device__ int device_slot_uses_coordinate(const DeviceDescriptor &descriptor, int slot, int coordinate) {
    for (int position = 0; position < descriptor.simplex_sizes[slot]; ++position) {
        if (descriptor.mappings[slot][position] - 1 == coordinate) return 1;
    }
    return 0;
}

// Two shared coordinates are interchangeable only when every earlier slot
// assigns them equal weights and every later slot uses both or neither (PALP
// ccc8e5e). The transposition is then a coordinate permutation of every
// completion, so keeping only nondecreasing weights on them loses no CWS. The
// later-slot condition was missing here: on structures such as 28, 42 and 45
// a later slot using only one of the two coordinates made the pruning drop
// half of the candidates.
__device__ int device_prefix_is_canonical(const DeviceDescriptor &descriptor,
                                          const SelectedEntry entries[kMaxSlots],
                                          int slot) {
    int shared_count = descriptor.shared_counts[slot];
    if (shared_count < 2) return 1;

    for (int left = 0; left < shared_count - 1; ++left) {
        int left_coordinate = descriptor.mappings[slot][left] - 1;
        for (int right = left + 1; right < shared_count; ++right) {
            int right_coordinate = descriptor.mappings[slot][right] - 1;
            int equivalent = 1;
            for (int previous_slot = 0; previous_slot < slot; ++previous_slot) {
                if (device_weight_at_coordinate(descriptor, entries, previous_slot, left_coordinate) !=
                    device_weight_at_coordinate(descriptor, entries, previous_slot, right_coordinate)) {
                    equivalent = 0;
                    break;
                }
            }
            for (int later_slot = slot + 1; equivalent && later_slot < descriptor.simplex_count; ++later_slot) {
                if (device_slot_uses_coordinate(descriptor, later_slot, left_coordinate) !=
                    device_slot_uses_coordinate(descriptor, later_slot, right_coordinate))
                    equivalent = 0;
            }
            if (equivalent && entries[slot].w[left] > entries[slot].w[right]) return 0;
        }
    }
    return 1;
}

__device__ unsigned long long device_count_prefixes(const DeviceDescriptor &descriptor,
                                                    SelectedEntry entries[kMaxSlots],
                                                    int slot) {
    if (slot >= descriptor.simplex_count) return 1ULL;
    int shared_count = descriptor.shared_counts[slot];
    if (shared_count < 2) return device_count_prefixes(descriptor, entries, slot + 1);

    int prefix[kMaxSize] = {0, 0, 0, 0, 0};
    int original[kMaxSize] = {0, 0, 0, 0, 0};
    for (int index = 0; index < shared_count; ++index) {
        prefix[index] = entries[slot].w[index];
        original[index] = entries[slot].w[index];
    }

    unsigned long long total = 0;
    do {
        for (int index = 0; index < shared_count; ++index) entries[slot].w[index] = prefix[index];
        if (device_prefix_is_canonical(descriptor, entries, slot)) {
            total += device_count_prefixes(descriptor, entries, slot + 1);
        }
    } while (device_next_permutation(prefix, shared_count));

    for (int index = 0; index < shared_count; ++index) entries[slot].w[index] = original[index];
    return total;
}

__device__ unsigned long long device_count_last_slot(const DeviceDescriptor &descriptor,
                                                     SelectedEntry entries[kMaxSlots],
                                                     int slot) {
    int shared_count = descriptor.shared_counts[slot];
    if (shared_count < 2) return 1ULL;

    int prefix[kMaxSize] = {0, 0, 0, 0, 0};
    int original[kMaxSize] = {0, 0, 0, 0, 0};
    for (int index = 0; index < shared_count; ++index) {
        prefix[index] = entries[slot].w[index];
        original[index] = entries[slot].w[index];
    }

    unsigned long long total = 0;
    do {
        for (int index = 0; index < shared_count; ++index) entries[slot].w[index] = prefix[index];
        total += static_cast<unsigned long long>(device_prefix_is_canonical(descriptor, entries, slot));
    } while (device_next_permutation(prefix, shared_count));

    for (int index = 0; index < shared_count; ++index) entries[slot].w[index] = original[index];
    return total;
}

__device__ unsigned long long device_count_after_slot0(const DeviceDescriptor &descriptor,
                                                      SelectedEntry entries[kMaxSlots]) {
    if (descriptor.simplex_count == 2) return device_count_last_slot(descriptor, entries, 1);
    if (descriptor.simplex_count != 3) return device_count_prefixes(descriptor, entries, 1);

    int shared_count = descriptor.shared_counts[1];
    if (shared_count < 2) return device_count_last_slot(descriptor, entries, 2);

    int prefix[kMaxSize] = {0, 0, 0, 0, 0};
    int original[kMaxSize] = {0, 0, 0, 0, 0};
    for (int index = 0; index < shared_count; ++index) {
        prefix[index] = entries[1].w[index];
        original[index] = entries[1].w[index];
    }

    unsigned long long total = 0;
    do {
        for (int index = 0; index < shared_count; ++index) entries[1].w[index] = prefix[index];
        if (device_prefix_is_canonical(descriptor, entries, 1)) {
            total += device_count_last_slot(descriptor, entries, 2);
        }
    } while (device_next_permutation(prefix, shared_count));

    for (int index = 0; index < shared_count; ++index) entries[1].w[index] = original[index];
    return total;
}

__device__ __noinline__ void device_store_candidate(const DeviceDescriptor &descriptor,
                                                    const SelectedEntry entries[kMaxSlots],
                                                    DeviceCwsCandidate *candidate_output,
                                                    unsigned long long candidate_capacity,
                                                    DeviceScanStats *stats) {
    if (!candidate_output || candidate_capacity == 0ULL) return;
    unsigned long long output_index = atomicAdd(&stats->stored_candidate_count, 1ULL);
    if (output_index >= candidate_capacity) return;

    DeviceCwsCandidate *out = &candidate_output[output_index];
    out->structure_id = descriptor.id;
    out->nw = descriptor.simplex_count;
    out->ambient_vertices = descriptor.ambient_vertices;
    for (int slot = 0; slot < kMaxSlots; ++slot) {
        out->degree[slot] = 0;
        for (int coordinate = 0; coordinate < 10; ++coordinate) out->weights[slot][coordinate] = 0;
    }
    out->join_np = 0;

    for (int slot = 0; slot < descriptor.simplex_count; ++slot) {
        out->degree[slot] = entries[slot].degree;
        out->sel_index[slot] = entries[slot].index;
        for (int position = 0; position < descriptor.simplex_sizes[slot]; ++position) {
            int coordinate = descriptor.mappings[slot][position] - 1;
            if (coordinate >= 0 && coordinate < 10) out->weights[slot][coordinate] = entries[slot].w[position];
        }
    }
}

__device__ void device_emit_prefixes(const DeviceDescriptor &descriptor,
                                     SelectedEntry entries[kMaxSlots],
                                     int slot,
                                     DeviceCwsCandidate *candidate_output,
                                     unsigned long long candidate_capacity,
                                     DeviceScanStats *stats) {
    if (slot >= descriptor.simplex_count) {
        device_store_candidate(descriptor, entries, candidate_output, candidate_capacity, stats);
        return;
    }
    int shared_count = descriptor.shared_counts[slot];
    if (shared_count < 2) {
        device_emit_prefixes(descriptor, entries, slot + 1, candidate_output, candidate_capacity, stats);
        return;
    }

    int prefix[kMaxSize] = {0, 0, 0, 0, 0};
    int original[kMaxSize] = {0, 0, 0, 0, 0};
    for (int index = 0; index < shared_count; ++index) {
        prefix[index] = entries[slot].w[index];
        original[index] = entries[slot].w[index];
    }

    do {
        for (int index = 0; index < shared_count; ++index) entries[slot].w[index] = prefix[index];
        if (device_prefix_is_canonical(descriptor, entries, slot)) {
            device_emit_prefixes(descriptor, entries, slot + 1, candidate_output, candidate_capacity, stats);
        }
    } while (device_next_permutation(prefix, shared_count));

    for (int index = 0; index < shared_count; ++index) entries[slot].w[index] = original[index];
}

__device__ void device_emit_last_slot(const DeviceDescriptor &descriptor,
                                      SelectedEntry entries[kMaxSlots],
                                      int slot,
                                      DeviceCwsCandidate *candidate_output,
                                      unsigned long long candidate_capacity,
                                      DeviceScanStats *stats) {
    int shared_count = descriptor.shared_counts[slot];
    if (shared_count < 2) {
        device_store_candidate(descriptor, entries, candidate_output, candidate_capacity, stats);
        return;
    }

    int prefix[kMaxSize] = {0, 0, 0, 0, 0};
    int original[kMaxSize] = {0, 0, 0, 0, 0};
    for (int index = 0; index < shared_count; ++index) {
        prefix[index] = entries[slot].w[index];
        original[index] = entries[slot].w[index];
    }
    do {
        for (int index = 0; index < shared_count; ++index) entries[slot].w[index] = prefix[index];
        if (device_prefix_is_canonical(descriptor, entries, slot)) {
            device_store_candidate(descriptor, entries, candidate_output, candidate_capacity, stats);
        }
    } while (device_next_permutation(prefix, shared_count));
    for (int index = 0; index < shared_count; ++index) entries[slot].w[index] = original[index];
}

__device__ void device_emit_after_slot0(const DeviceDescriptor &descriptor,
                                        SelectedEntry entries[kMaxSlots],
                                        DeviceCwsCandidate *candidate_output,
                                        unsigned long long candidate_capacity,
                                        DeviceScanStats *stats) {
    if (descriptor.simplex_count == 2) {
        device_emit_last_slot(descriptor, entries, 1, candidate_output, candidate_capacity, stats);
        return;
    }
    if (descriptor.simplex_count != 3) {
        device_emit_prefixes(descriptor, entries, 1, candidate_output, candidate_capacity, stats);
        return;
    }

    int shared_count = descriptor.shared_counts[1];
    if (shared_count < 2) {
        device_emit_last_slot(descriptor, entries, 2, candidate_output, candidate_capacity, stats);
        return;
    }

    int prefix[kMaxSize] = {0, 0, 0, 0, 0};
    int original[kMaxSize] = {0, 0, 0, 0, 0};
    for (int index = 0; index < shared_count; ++index) {
        prefix[index] = entries[1].w[index];
        original[index] = entries[1].w[index];
    }
    do {
        for (int index = 0; index < shared_count; ++index) entries[1].w[index] = prefix[index];
        if (device_prefix_is_canonical(descriptor, entries, 1)) {
            device_emit_last_slot(descriptor, entries, 2, candidate_output, candidate_capacity, stats);
        }
    } while (device_next_permutation(prefix, shared_count));
    for (int index = 0; index < shared_count; ++index) entries[1].w[index] = original[index];
}

// Slot 0's shared prefix is permuted too, as in PALP's EnumeratePermutations
// from slot 0. Pool entries hold their shared weights in nondecreasing order,
// which is the only arrangement that passes device_prefix_is_canonical when no
// later slot tells slot 0's shared coordinates apart. Where one does (for
// example structure 28: slot 1 uses coordinate 1, slot 2 uses 2 and 3), the
// other arrangements are distinct CWS and were previously never generated.
__device__ unsigned long long device_count_small_prefixes(const DeviceDescriptor &descriptor,
                                                         SelectedEntry entries[kMaxSlots]) {
    const int shared_count = descriptor.shared_counts[0];
    if (shared_count < 2) return device_count_after_slot0(descriptor, entries);
    int prefix[kMaxSize] = {0, 0, 0, 0, 0};
    int original[kMaxSize] = {0, 0, 0, 0, 0};
    for (int index = 0; index < shared_count; ++index) {
        prefix[index] = entries[0].w[index];
        original[index] = entries[0].w[index];
    }
    unsigned long long total = 0;
    do {
        for (int index = 0; index < shared_count; ++index) entries[0].w[index] = prefix[index];
        if (device_prefix_is_canonical(descriptor, entries, 0)) total += device_count_after_slot0(descriptor, entries);
    } while (device_next_permutation(prefix, shared_count));
    for (int index = 0; index < shared_count; ++index) entries[0].w[index] = original[index];
    return total;
}

__device__ void device_emit_small_prefixes(const DeviceDescriptor &descriptor,
                                           SelectedEntry entries[kMaxSlots],
                                           DeviceCwsCandidate *candidate_output,
                                           unsigned long long candidate_capacity,
                                           DeviceScanStats *stats) {
    const int shared_count = descriptor.shared_counts[0];
    if (shared_count < 2) {
        device_emit_after_slot0(descriptor, entries, candidate_output, candidate_capacity, stats);
        return;
    }
    int prefix[kMaxSize] = {0, 0, 0, 0, 0};
    int original[kMaxSize] = {0, 0, 0, 0, 0};
    for (int index = 0; index < shared_count; ++index) {
        prefix[index] = entries[0].w[index];
        original[index] = entries[0].w[index];
    }
    do {
        for (int index = 0; index < shared_count; ++index) entries[0].w[index] = prefix[index];
        if (device_prefix_is_canonical(descriptor, entries, 0))
            device_emit_after_slot0(descriptor, entries, candidate_output, candidate_capacity, stats);
    } while (device_next_permutation(prefix, shared_count));
    for (int index = 0; index < shared_count; ++index) entries[0].w[index] = original[index];
}

__device__ unsigned long long device_first_pair_for_left(unsigned long long left,
                                                         unsigned long long count) {
    return left * count - (left * (left - 1ULL)) / 2ULL;
}

__device__ unsigned long long device_pair_left_from_linear(unsigned long long pair_index,
                                                           unsigned long long count) {
    unsigned long long low = 0;
    unsigned long long high = count;
    while (low + 1ULL < high) {
        unsigned long long mid = low + (high - low) / 2ULL;
        if (device_first_pair_for_left(mid, count) <= pair_index) low = mid;
        else high = mid;
    }
    return low;
}

inline std::uint64_t triangular_count(std::uint64_t count) {
    if ((count & 1ULL) == 0) return (count / 2ULL) * (count + 1ULL);
    return count * ((count + 1ULL) / 2ULL);
}

inline bool use_pair_scan(const DeviceDescriptor &descriptor) {
    return descriptor.simplex_count == 2 &&
           descriptor.family_groups[0] == descriptor.family_groups[1] &&
           descriptor.pool_offsets[0] == descriptor.pool_offsets[1] &&
           descriptor.pool_counts[0] == descriptor.pool_counts[1];
}

inline std::uint64_t descriptor_scan_space(const DeviceDescriptor &descriptor) {
    return use_pair_scan(descriptor)
        ? triangular_count(descriptor.pool_counts[0])
        : descriptor.selection_product;
}

inline std::uint64_t factorial_u64(int value) {
    std::uint64_t result = 1;
    for (int factor = 2; factor <= value; ++factor) result *= static_cast<std::uint64_t>(factor);
    return result;
}

inline std::uint64_t max_prefix_variants_per_selection(const DeviceDescriptor &descriptor) {
    std::uint64_t variants = 1;
    for (int slot = 0; slot < descriptor.simplex_count; ++slot) {
        variants = checked_mul(variants, factorial_u64(descriptor.shared_counts[slot]));
    }
    return std::max<std::uint64_t>(variants, 1);
}

inline void descriptor_shard_range(const DeviceDescriptor &descriptor,
                            int shard_count_total, int shard_index,
                            std::uint64_t *shard_start,
                            std::uint64_t *shard_count) {
    std::uint64_t scan_space = descriptor_scan_space(descriptor);
    std::uint64_t start =
        (scan_space * static_cast<std::uint64_t>(shard_index)) /
        static_cast<std::uint64_t>(shard_count_total);
    std::uint64_t end =
        (scan_space * static_cast<std::uint64_t>(shard_index + 1)) /
        static_cast<std::uint64_t>(shard_count_total);
    *shard_start = start;
    *shard_count = end - start;
}

// Mixed-radix decoding of a selection tuple index, slot 0 most significant:
// a bijection from [0, selection_product) onto the index tuples.
__device__ void device_decode_tuple(const DeviceDescriptor &descriptor, unsigned long long linear,
                                    unsigned long long selected_indices[kMaxSlots]) {
    unsigned long long remainder = linear;
    for (int slot = descriptor.simplex_count - 1; slot >= 0; --slot) {
        unsigned long long pool_count = descriptor.pool_counts[slot];
        selected_indices[slot] = remainder % pool_count;
        remainder /= pool_count;
    }
}

__global__ void descriptor_scan_kernel(const SelectedEntry *entries,
                                       DeviceDescriptor descriptor,
                                       unsigned long long start_tuple,
                                       unsigned long long tuple_count,
                                       DeviceScanStats *stats,
                                       DeviceCwsCandidate *candidate_output,
                                       unsigned long long candidate_capacity) {
    unsigned long long local_selection = 0;
    unsigned long long local_canonical = 0;
    unsigned long long local_prefix = 0;
    unsigned long long global_thread = blockIdx.x * blockDim.x + threadIdx.x;
    unsigned long long stride = blockDim.x * gridDim.x;

    for (unsigned long long offset = global_thread; offset < tuple_count; offset += stride) {
        unsigned long long selected_indices[kMaxSlots] = {0, 0, 0, 0, 0};
        SelectedEntry tuple_entries[kMaxSlots];
        device_decode_tuple(descriptor, start_tuple + offset, selected_indices);

        int canonical_selection = 1;
        for (int slot = 0; slot < descriptor.simplex_count; ++slot) {
            for (int previous = 0; previous < slot; ++previous) {
                if (descriptor.family_groups[previous] == descriptor.family_groups[slot] &&
                    selected_indices[previous] > selected_indices[slot]) {
                    canonical_selection = 0;
                }
            }
        }
        ++local_selection;
        if (!canonical_selection) continue;

        for (int slot = 0; slot < descriptor.simplex_count; ++slot) {
            tuple_entries[slot] = entries[descriptor.pool_offsets[slot] + selected_indices[slot]];
        }

        ++local_canonical;
        local_prefix += device_count_small_prefixes(descriptor, tuple_entries);
        if (candidate_output && candidate_capacity > 0ULL) {
            device_emit_small_prefixes(descriptor, tuple_entries,
                                       candidate_output, candidate_capacity, stats);
        }
    }

    if (local_selection) atomicAdd(&stats->selection_tuples, local_selection);
    if (local_canonical) atomicAdd(&stats->canonical_selection_tuples, local_canonical);
    if (local_prefix) atomicAdd(&stats->prefix_candidates, local_prefix);
}

__global__ void descriptor_pair_scan_kernel(const SelectedEntry *entries,
                                            DeviceDescriptor descriptor,
                                            unsigned long long start_pair,
                                            unsigned long long pair_count,
                                            DeviceScanStats *stats,
                                            DeviceCwsCandidate *candidate_output,
                                            unsigned long long candidate_capacity) {
    unsigned long long local_selection = 0;
    unsigned long long local_prefix = 0;
    unsigned long long global_thread = blockIdx.x * blockDim.x + threadIdx.x;
    unsigned long long stride = blockDim.x * gridDim.x;
    unsigned long long pool_count = descriptor.pool_counts[0];

    for (unsigned long long offset = global_thread; offset < pair_count; offset += stride) {
        unsigned long long pair_index = start_pair + offset;
        unsigned long long left = device_pair_left_from_linear(pair_index, pool_count);
        unsigned long long first_for_left = device_first_pair_for_left(left, pool_count);
        unsigned long long right = left + (pair_index - first_for_left);

        ++local_selection;
        SelectedEntry tuple_entries[kMaxSlots];
        tuple_entries[0] = entries[descriptor.pool_offsets[0] + left];
        tuple_entries[1] = entries[descriptor.pool_offsets[1] + right];
        local_prefix += device_count_small_prefixes(descriptor, tuple_entries);
        if (candidate_output && candidate_capacity > 0ULL) {
            device_emit_small_prefixes(descriptor, tuple_entries,
                                       candidate_output, candidate_capacity, stats);
        }
    }

    if (local_selection) {
        atomicAdd(&stats->selection_tuples, local_selection);
        atomicAdd(&stats->canonical_selection_tuples, local_selection);
    }
    if (local_prefix) atomicAdd(&stats->prefix_candidates, local_prefix);
}

// Generates (and, with an output buffer, stores) the candidates of the selection
// tuples [range_start, range_start + range_count). Counts are exact even when
// the buffer is too small; stored_candidate_count then exceeds the capacity.
inline HostScanResult scan_descriptor_range(const DeviceDescriptor &descriptor, const SelectedEntry *device_entries,
                                            DeviceScanStats *device_stats, DeviceCwsCandidate *candidate_output,
                                            std::uint64_t candidate_capacity, int blocks, int threads,
                                            std::uint64_t range_start, std::uint64_t range_count) {
    check_cuda(cudaMemset(device_stats, 0, sizeof(DeviceScanStats)), "cudaMemset stats");
    auto start = std::chrono::steady_clock::now();
    if (use_pair_scan(descriptor)) {
        descriptor_pair_scan_kernel<<<blocks, threads>>>(device_entries, descriptor, range_start, range_count,
                                                         device_stats, candidate_output, candidate_capacity);
    } else {
        descriptor_scan_kernel<<<blocks, threads>>>(device_entries, descriptor, range_start, range_count,
                                                    device_stats, candidate_output, candidate_capacity);
    }
    check_cuda(cudaGetLastError(), "descriptor scan launch");
    check_cuda(cudaDeviceSynchronize(), "descriptor scan");
    auto end = std::chrono::steady_clock::now();
    DeviceScanStats stats{};
    check_cuda(cudaMemcpy(&stats, device_stats, sizeof(stats), cudaMemcpyDeviceToHost), "cudaMemcpy stats");
    HostScanResult result;
    result.structure_id = descriptor.id;
    result.selection_product = descriptor.selection_product;
    result.scanned_tuples = stats.selection_tuples;
    result.canonical_selection_tuples = stats.canonical_selection_tuples;
    result.prefix_candidates = stats.prefix_candidates;
    result.stored_candidate_count = stats.stored_candidate_count;
    result.seconds = std::chrono::duration<double>(end - start).count();
    return result;
}

}  // namespace cwsgpu
