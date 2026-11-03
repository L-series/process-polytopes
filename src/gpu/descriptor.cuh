// Descriptors: the historical catalogue as used on the device, with the slot
// orbits that allow ordering two slots by pool index.
#pragma once

#include "catalogue_bridge.h"
#include "pools.cuh"

namespace cwsgpu {

inline GpuDescriptor catalogue_descriptor(int id) {
    GpuDescriptor d{};
    if (!GpuCatalogueDescriptor(id, &d)) throw std::runtime_error("unknown structure id " + std::to_string(id));
    // The generator treats exactly the leading shared_counts[slot] positions of
    // a slot as shared: they must be the coordinates used by another slot.
    int uses[kMaxCoordinates] = {0};
    for (int s = 0; s < d.row_count; ++s)
        for (int p = 0; p < d.row_sizes[s]; ++p) ++uses[d.mappings[s][p] - 1];
    for (int s = 0; s < d.row_count; ++s)
        for (int p = 0; p < d.row_sizes[s]; ++p)
            if ((uses[d.mappings[s][p] - 1] > 1) != (p < d.shared_counts[s]))
                throw std::runtime_error("structure " + std::to_string(id) + " does not list its shared coordinates first");
    return d;
}

// A slot permutation is an automorphism when it maps every slot to one of the
// same size and shared count and the induced coordinate map is a bijection.
inline int is_descriptor_automorphism(const GpuDescriptor &descriptor,
                               const int permutation[kMaxSlots]) {
    int coordinate_map[11] = {0};
    int inverse_coordinate_map[11] = {0};

    for (int slot = 0; slot < descriptor.row_count; ++slot) {
        int target_slot = permutation[slot];
        int simplex_size = descriptor.row_sizes[slot];
        if (simplex_size != descriptor.row_sizes[target_slot]) return 0;
        if (descriptor.shared_counts[slot] != descriptor.shared_counts[target_slot]) return 0;

        for (int position = 0; position < simplex_size; ++position) {
            int source_coordinate = descriptor.mappings[slot][position];
            int target_coordinate = descriptor.mappings[target_slot][position];
            if (coordinate_map[source_coordinate] == 0) {
                if (inverse_coordinate_map[target_coordinate] != 0 &&
                    inverse_coordinate_map[target_coordinate] != source_coordinate) {
                    return 0;
                }
                coordinate_map[source_coordinate] = target_coordinate;
                inverse_coordinate_map[target_coordinate] = source_coordinate;
            } else if (coordinate_map[source_coordinate] != target_coordinate) {
                return 0;
            }
        }
    }
    return 1;
}

inline void enumerate_slot_automorphisms(const GpuDescriptor &descriptor,
                                  int slot,
                                  int permutation[kMaxSlots],
                                  int used[kMaxSlots],
                                  int reachable[kMaxSlots][kMaxSlots]) {
    if (slot >= descriptor.row_count) {
        if (is_descriptor_automorphism(descriptor, permutation)) {
            for (int index = 0; index < descriptor.row_count; ++index) {
                reachable[index][permutation[index]] = 1;
            }
        }
        return;
    }

    for (int target_slot = 0; target_slot < descriptor.row_count; ++target_slot) {
        if (used[target_slot]) continue;
        if (descriptor.row_sizes[slot] != descriptor.row_sizes[target_slot]) continue;
        if (descriptor.shared_counts[slot] != descriptor.shared_counts[target_slot]) continue;
        permutation[slot] = target_slot;
        used[target_slot] = 1;
        enumerate_slot_automorphisms(descriptor, slot + 1, permutation, used, reachable);
        used[target_slot] = 0;
    }
}

inline void compute_slot_orbit_groups(const GpuDescriptor &descriptor,
                               int orbit_groups[kMaxSlots]) {
    int permutation[kMaxSlots] = {0, 0, 0, 0, 0};
    int used[kMaxSlots] = {0, 0, 0, 0, 0};
    int reachable[kMaxSlots][kMaxSlots] = {{0, 0, 0, 0, 0},
                                           {0, 0, 0, 0, 0},
                                           {0, 0, 0, 0, 0},
                                           {0, 0, 0, 0, 0},
                                           {0, 0, 0, 0, 0}};
    for (int index = 0; index < descriptor.row_count; ++index) reachable[index][index] = 1;
    enumerate_slot_automorphisms(descriptor, 0, permutation, used, reachable);

    for (int index = 0; index < kMaxSlots; ++index) orbit_groups[index] = 0;
    int next_group = 1;
    for (int index = 0; index < descriptor.row_count; ++index) {
        if (orbit_groups[index]) continue;
        orbit_groups[index] = next_group;
        for (int other = index + 1; other < descriptor.row_count; ++other) {
            if (reachable[index][other]) orbit_groups[other] = next_group;
        }
        ++next_group;
    }
}

inline DeviceDescriptor make_device_descriptor(
    const GpuDescriptor &descriptor,
    const PoolArray &pools) {
    DeviceDescriptor device;
    device.id = descriptor.id;
    device.ambient_vertices = descriptor.coordinate_count;
    device.simplex_count = descriptor.row_count;
    device.selection_product = 1;

    int orbit_groups[kMaxSlots] = {0, 0, 0, 0, 0};
    compute_slot_orbit_groups(descriptor, orbit_groups);

    for (int slot = 0; slot < kMaxSlots; ++slot) {
        device.simplex_sizes[slot] = descriptor.row_sizes[slot];
        device.shared_counts[slot] = descriptor.shared_counts[slot];
        device.family_groups[slot] = descriptor.row_sizes[slot] * (kMaxSlots + 1) + orbit_groups[slot];
        for (int position = 0; position < kMaxSize; ++position) {
            device.mappings[slot][position] = descriptor.mappings[slot][position];
        }
    }

    for (int slot = 0; slot < descriptor.row_count; ++slot) {
        int key = pool_key(descriptor.row_sizes[slot], descriptor.shared_counts[slot]);
        device.pool_offsets[slot] = pools[key].device_offset;
        device.pool_counts[slot] = static_cast<std::uint64_t>(pools[key].entries.size());
        device.selection_product = checked_mul(device.selection_product, device.pool_counts[slot]);
    }
    return device;
}

}  // namespace cwsgpu
