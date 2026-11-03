// The streaming pipeline: generate a chunk of candidates, enumerate their
// lattice points, IP-check the ones with 6..np_cap points, and write accepted
// and deferred CWS. Every candidate ends in exactly one counter; a deferred
// candidate (no GPU verdict) is written for the CPU and never dropped.
#pragma once

#include "generate.cuh"
#include "ip_check.cuh"

#include <cub/cub.cuh>

#include <algorithm>
#include <ostream>

namespace cwsgpu {

// Outcome of every candidate of a chunk. Their sum equals the candidate count.
struct DeviceIpStats {
    unsigned long long point_fail = 0;      // no kernel basis / point set (precheck or basis failure)
    unsigned long long degenerate = 0;      // fewer than 6 points: cannot be full-dimensional
    unsigned long long certified = 0;       // complete point set has a coordinate or pair certificate
    unsigned long long deferred = 0;        // written for the CPU: np > np_cap, walk limits, IP lists full
    unsigned long long rank_deficient = 0;  // IP check: points do not span 5 dimensions
    unsigned long long inci_fail = 0;       // IP check: initial simplex facet with < 5 incidences
    unsigned long long rejected = 0;        // IP check: a valid inequality with constant < 1
    unsigned long long accepted = 0;        // IP
};

struct HostIpResult {
    DeviceIpStats stats{};
    std::uint64_t candidates = 0;
    double seconds = 0.0;
};

inline void accumulate(HostIpResult *total, const HostIpResult &chunk) {
    const unsigned long long *from = &chunk.stats.point_fail;
    unsigned long long *to = &total->stats.point_fail;
    for (std::size_t i = 0; i < sizeof(DeviceIpStats) / sizeof(unsigned long long); ++i) to[i] += from[i];
    total->candidates += chunk.candidates;
    total->seconds += chunk.seconds;
}

inline std::uint64_t outcome_sum(const DeviceIpStats &s) {
    return s.point_fail + s.degenerate + s.certified + s.deferred + s.rank_deficient + s.inci_fail + s.rejected +
           s.accepted;
}

// np_out values written by the point stage.
static constexpr int kNpDecided = -1;  // counted already (deferred or certified); not IP-checked

__device__ void device_defer(const DeviceCwsCandidate &candidate, DeviceIpStats *stats, DeviceCwsCandidate *deferred,
                             unsigned long long *deferred_count, unsigned long long capacity) {
    atomicAdd(&stats->deferred, 1ULL);
    const unsigned long long slot = atomicAdd(deferred_count, 1ULL);
    if (slot < capacity) deferred[slot] = candidate;
}

// Point stage, one thread per candidate. Points go to a compact np_cap slot per
// candidate. 3 blocks of 256 per SM: the walk is latency bound, so the extra
// resident warps beat the small register spill.
__global__ void __launch_bounds__(256, 3) point_enum_kernel(const DeviceCwsCandidate *candidates,
                                                            unsigned long long candidate_count, int np_cap,
                                                            long long *points, int *np_out, DeviceIpStats *stats,
                                                            DeviceCwsCandidate *deferred,
                                                            unsigned long long *deferred_count,
                                                            unsigned long long deferred_capacity) {
    for (unsigned long long index = blockIdx.x * blockDim.x + threadIdx.x; index < candidate_count;
         index += blockDim.x * gridDim.x) {
        const DeviceCwsCandidate &candidate = candidates[index];
        long long *slot = points + index * static_cast<unsigned long long>(np_cap) * 5ULL;
        int np = 0;
        int ip_possible = 1;
        const int status = device_make_points_fp(candidate, slot, np_cap, &np, &ip_possible);
        if (status < 0) {
            np_out[index] = kNpDecided;
            device_defer(candidate, stats, deferred, deferred_count, deferred_capacity);
        } else if (status == 0) {
            np_out[index] = kNpDecided;
            atomicAdd(&stats->point_fail, 1ULL);
        } else if (np < 6) {
            np_out[index] = kNpDecided;
            atomicAdd(&stats->degenerate, 1ULL);
        } else if (!ip_possible) {
            np_out[index] = kNpDecided;
            atomicAdd(&stats->certified, 1ULL);
        } else {
            np_out[index] = np;
        }
    }
}

// Sort key = sum of bit lengths of the coordinate bounds (~ log2 box volume), so
// a warp's lanes run walks of similar length.
__global__ void vol_key_kernel(const DeviceCwsCandidate *candidates, unsigned long long candidate_count, int *keys) {
    for (unsigned long long index = blockIdx.x * blockDim.x + threadIdx.x; index < candidate_count;
         index += blockDim.x * gridDim.x) {
        long long x_upper[10] = {0};
        int key = 0;
        if (device_candidate_basic_precheck(candidates[index], x_upper)) {
            for (int c = 0; c < candidates[index].ambient_vertices; ++c)
                for (long long v = x_upper[c]; v > 0; v >>= 1) ++key;
        } else {
            key = -1;
        }
        keys[index] = key + 1;
    }
}
static constexpr int kVolKeyBits = 16;

inline int np_key_bits(int np_cap) {
    int bits = 1;
    while ((1 << bits) <= np_cap) ++bits;
    return bits;
}

__global__ void iota_kernel(int *out, unsigned long long count) {
    for (unsigned long long index = blockIdx.x * blockDim.x + threadIdx.x; index < count;
         index += blockDim.x * gridDim.x)
        out[index] = static_cast<int>(index);
}

// Sort key for the IP stage: np for candidates to check, 0 otherwise, so they
// form the tail of a stable ascending sort and a warp checks similar sizes.
__global__ void np_key_kernel(const int *np_out, unsigned long long count, int *keys,
                              unsigned long long *valid_count) {
    unsigned long long local = 0;
    for (unsigned long long index = blockIdx.x * blockDim.x + threadIdx.x; index < count;
         index += blockDim.x * gridDim.x) {
        const int np = np_out[index];
        keys[index] = np >= 6 ? np : 0;
        local += np >= 6 ? 1ULL : 0ULL;
    }
    if (local) atomicAdd(valid_count, local);
}

__global__ void gather_candidates_kernel(const DeviceCwsCandidate *src, const int *perm, unsigned long long count,
                                         DeviceCwsCandidate *dst) {
    for (unsigned long long index = blockIdx.x * blockDim.x + threadIdx.x; index < count;
         index += blockDim.x * gridDim.x)
        dst[index] = src[perm[index]];
}

// IP stage over the np-sorted candidates with 6..np_cap points. Scratch is per
// launched thread.
__global__ void ip_check_kernel(const DeviceCwsCandidate *candidates, const long long *points, const int *np_out,
                                int np_cap, const int *valid_index, unsigned long long valid_count,
                                DeviceIpScratch *scratch_workspace, DeviceIpStats *stats,
                                DeviceCwsCandidate *accepted, unsigned long long *accepted_count,
                                unsigned long long accepted_capacity, DeviceCwsCandidate *deferred,
                                unsigned long long *deferred_count, unsigned long long deferred_capacity) {
    const unsigned long long tid = blockIdx.x * blockDim.x + threadIdx.x;
    DeviceIpScratch *scratch = scratch_workspace + tid;
    for (unsigned long long j = tid; j < valid_count; j += blockDim.x * gridDim.x) {
        const unsigned long long index = static_cast<unsigned long long>(valid_index[j]);
        const long long *slot = points + index * static_cast<unsigned long long>(np_cap) * 5ULL;
        int reason = 0;
        if (device_ip_check(slot, np_out[index], scratch, &reason)) {
            atomicAdd(&stats->accepted, 1ULL);
            const unsigned long long a = atomicAdd(accepted_count, 1ULL);
            if (a < accepted_capacity) accepted[a] = candidates[index];
        } else if (reason == 1) {
            atomicAdd(&stats->rank_deficient, 1ULL);
        } else if (reason == 2) {
            atomicAdd(&stats->inci_fail, 1ULL);
        } else if (reason == 3) {
            device_defer(candidates[index], stats, deferred, deferred_count, deferred_capacity);
        } else {
            atomicAdd(&stats->rejected, 1ULL);
        }
    }
}

// PALP CWS format: per row its degree, then the weights of all coordinates.
inline void write_candidate_row(std::ostream &out, const DeviceCwsCandidate &candidate) {
    for (int slot = 0; slot < candidate.nw; ++slot) {
        if (slot) out << ' ';
        out << candidate.degree[slot];
        for (int coordinate = 0; coordinate < candidate.ambient_vertices; ++coordinate)
            out << ' ' << candidate.weights[slot][coordinate];
    }
    out << '\n';
}

struct PipelineOptions {
    int blocks = 0;
    int threads = 128;
    int np_cap = 1024;
    std::uint64_t chunk_capacity = 0;  // candidates per chunk
};

// Device buffers of one chunk, allocated once per run.
struct Workspace {
    std::uint64_t capacity = 0;
    long long *points = nullptr;
    int *np_out = nullptr, *valid_index = nullptr, *keys = nullptr, *perm = nullptr, *iota = nullptr;
    int *np_keys = nullptr, *keys_sorted = nullptr;
    DeviceCwsCandidate *sorted = nullptr, *accepted = nullptr, *deferred = nullptr;
    unsigned long long *counts = nullptr;  // [0] valid, [1] accepted, [2] deferred
    DeviceIpScratch *scratch = nullptr;
    DeviceIpStats *stats = nullptr;
    void *sort_temp = nullptr;
    std::size_t sort_temp_bytes = 0;

    Workspace(std::uint64_t chunk, const PipelineOptions &options) : capacity(chunk) {
        const std::uint64_t threads = static_cast<std::uint64_t>(options.blocks) * options.threads;
        check_cuda(cudaMalloc(&points, chunk * options.np_cap * 5ULL * sizeof(long long)), "cudaMalloc points");
        for (int **buffer : {&np_out, &valid_index, &keys, &perm, &iota, &np_keys, &keys_sorted})
            check_cuda(cudaMalloc(buffer, chunk * sizeof(int)), "cudaMalloc index buffer");
        for (DeviceCwsCandidate **buffer : {&sorted, &accepted, &deferred})
            check_cuda(cudaMalloc(buffer, chunk * sizeof(DeviceCwsCandidate)), "cudaMalloc candidate buffer");
        check_cuda(cudaMalloc(&counts, 3 * sizeof(unsigned long long)), "cudaMalloc counts");
        check_cuda(cudaMalloc(&scratch, threads * sizeof(DeviceIpScratch)), "cudaMalloc IP scratch");
        check_cuda(cudaMalloc(&stats, sizeof(DeviceIpStats)), "cudaMalloc stats");
        std::size_t vol_bytes = 0, np_bytes = 0;
        const int items = static_cast<int>(chunk);
        check_cuda(cub::DeviceRadixSort::SortPairs(nullptr, vol_bytes, keys, keys_sorted, iota, perm, items, 0,
                                                   kVolKeyBits), "cub sort size");
        check_cuda(cub::DeviceRadixSort::SortPairs(nullptr, np_bytes, np_keys, keys_sorted, iota, valid_index, items, 0,
                                                   np_key_bits(options.np_cap)), "cub sort size");
        sort_temp_bytes = std::max(vol_bytes, np_bytes);
        check_cuda(cudaMalloc(&sort_temp, sort_temp_bytes), "cudaMalloc sort temp");
    }
    ~Workspace() {
        cudaFree(sort_temp); cudaFree(stats); cudaFree(scratch); cudaFree(counts);
        cudaFree(deferred); cudaFree(accepted); cudaFree(sorted);
        cudaFree(keys_sorted); cudaFree(np_keys); cudaFree(iota); cudaFree(perm); cudaFree(keys);
        cudaFree(valid_index); cudaFree(np_out); cudaFree(points);
    }
    Workspace(const Workspace &) = delete;
    Workspace &operator=(const Workspace &) = delete;
};

inline void copy_rows(const DeviceCwsCandidate *device_rows, unsigned long long count, std::ostream &out) {
    if (!count) return;
    std::vector<DeviceCwsCandidate> rows(static_cast<std::size_t>(count));
    check_cuda(cudaMemcpy(rows.data(), device_rows, count * sizeof(DeviceCwsCandidate), cudaMemcpyDeviceToHost),
               "cudaMemcpy rows");
    for (const DeviceCwsCandidate &row : rows) write_candidate_row(out, row);
    if (!out) throw std::runtime_error("failed to write output rows");
}

// Points, certificates and IP check for `count` stored candidates.
inline HostIpResult classify_chunk(DeviceCwsCandidate *candidates, std::uint64_t count, const PipelineOptions &options,
                                   Workspace &ws, std::ostream &accepted_out, std::ostream &deferred_out) {
    HostIpResult result;
    result.candidates = count;
    if (count == 0) return result;
    const int blocks = options.blocks, threads = options.threads, items = static_cast<int>(count);
    auto start = std::chrono::steady_clock::now();
    check_cuda(cudaMemset(ws.stats, 0, sizeof(DeviceIpStats)), "cudaMemset stats");
    check_cuda(cudaMemset(ws.counts, 0, 3 * sizeof(unsigned long long)), "cudaMemset counts");

    iota_kernel<<<blocks, threads>>>(ws.iota, count);
    vol_key_kernel<<<blocks, threads>>>(candidates, count, ws.keys);
    std::size_t temp = ws.sort_temp_bytes;
    check_cuda(cub::DeviceRadixSort::SortPairs(ws.sort_temp, temp, ws.keys, ws.keys_sorted, ws.iota, ws.perm, items, 0,
                                               kVolKeyBits), "cub volume sort");
    gather_candidates_kernel<<<blocks, threads>>>(candidates, ws.perm, count, ws.sorted);

    point_enum_kernel<<<blocks, threads>>>(ws.sorted, count, options.np_cap, ws.points, ws.np_out, ws.stats,
                                           ws.deferred, ws.counts + 2, ws.capacity);
    check_cuda(cudaGetLastError(), "point_enum_kernel launch");
    np_key_kernel<<<blocks, threads>>>(ws.np_out, count, ws.np_keys, ws.counts);
    temp = ws.sort_temp_bytes;
    check_cuda(cub::DeviceRadixSort::SortPairs(ws.sort_temp, temp, ws.np_keys, ws.keys_sorted, ws.iota, ws.valid_index,
                                               items, 0, np_key_bits(options.np_cap)), "cub np sort");
    unsigned long long valid = 0;
    check_cuda(cudaMemcpy(&valid, ws.counts, sizeof(valid), cudaMemcpyDeviceToHost), "cudaMemcpy valid count");
    if (valid) {
        ip_check_kernel<<<blocks, threads>>>(ws.sorted, ws.points, ws.np_out, options.np_cap,
                                             ws.valid_index + (count - valid), valid, ws.scratch, ws.stats,
                                             ws.accepted, ws.counts + 1, ws.capacity, ws.deferred, ws.counts + 2,
                                             ws.capacity);
        check_cuda(cudaGetLastError(), "ip_check_kernel launch");
    }
    check_cuda(cudaDeviceSynchronize(), "classify chunk");
    result.seconds = std::chrono::duration<double>(std::chrono::steady_clock::now() - start).count();

    unsigned long long counts[3] = {0, 0, 0};
    check_cuda(cudaMemcpy(counts, ws.counts, sizeof(counts), cudaMemcpyDeviceToHost), "cudaMemcpy counts");
    check_cuda(cudaMemcpy(&result.stats, ws.stats, sizeof(DeviceIpStats), cudaMemcpyDeviceToHost), "cudaMemcpy stats");
    // Accounting: every candidate has exactly one outcome and every accepted or
    // deferred row fits its buffer, so nothing is lost silently.
    if (outcome_sum(result.stats) != count || counts[1] != result.stats.accepted || counts[2] != result.stats.deferred ||
        counts[1] > ws.capacity || counts[2] > ws.capacity)
        throw std::runtime_error("chunk accounting failed: outcomes do not add up to the candidate count");
    copy_rows(ws.accepted, counts[1], accepted_out);
    copy_rows(ws.deferred, counts[2], deferred_out);
    return result;
}

// Generates and classifies a shard in chunks. A chunk that would not fit the
// buffer is halved and retried, so no candidate is truncated.
inline HostIpResult run_shard(const DeviceDescriptor &descriptor, const SelectedEntry *device_entries,
                              DeviceScanStats *device_stats, std::uint64_t shard_start, std::uint64_t shard_size,
                              const PipelineOptions &options, std::ostream &accepted_out, std::ostream &deferred_out,
                              std::uint64_t *generated) {
    HostIpResult total;
    Workspace ws(options.chunk_capacity, options);
    DeviceCwsCandidate *candidates = nullptr;
    check_cuda(cudaMalloc(&candidates, options.chunk_capacity * sizeof(DeviceCwsCandidate)), "cudaMalloc candidates");
    std::uint64_t span = std::max<std::uint64_t>(1, options.chunk_capacity / max_prefix_variants_per_selection(descriptor));
    *generated = 0;
    for (std::uint64_t position = shard_start; position < shard_start + shard_size;) {
        const std::uint64_t count = std::min(span, shard_start + shard_size - position);
        const HostScanResult scan = scan_descriptor_range(descriptor, device_entries, device_stats, candidates,
                                                          options.chunk_capacity, options.blocks, options.threads,
                                                          position, count);
        if (scan.stored_candidate_count > options.chunk_capacity) {
            if (count == 1) {
                cudaFree(candidates);
                throw std::runtime_error("one selection exceeds the chunk capacity; raise --chunk-capacity");
            }
            span = std::max<std::uint64_t>(1, count / 2);
            continue;
        }
        if (scan.stored_candidate_count != scan.prefix_candidates) {
            cudaFree(candidates);
            throw std::runtime_error("generation stored a different number of candidates than it counted");
        }
        *generated += scan.prefix_candidates;
        accumulate(&total, classify_chunk(candidates, scan.stored_candidate_count, options, ws, accepted_out,
                                          deferred_out));
        position += count;
    }
    cudaFree(candidates);
    return total;
}

}  // namespace cwsgpu
