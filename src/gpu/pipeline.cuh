// The streaming pipeline: generate a chunk of candidates, enumerate their
// lattice points, IP-check the ones with 6..np_cap points, and write accepted
// and deferred CWS. Every candidate ends in exactly one counter; a deferred
// candidate (no GPU verdict) is written for the CPU and never dropped.
#pragma once

#include "generate.cuh"
#include "ip_check.cuh"
#include "join.cuh"
#if CWS_GPU_FUSED_TYPE3
#include "join_type3.cuh"
#endif

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
    unsigned long long np_mismatch = 0;     // of the deferred: walk count != join count (must stay 0)
};

struct HostIpResult {
    DeviceIpStats stats{};
    std::uint64_t candidates = 0;
    double seconds = 0.0;
    std::uint64_t scanned_selections = 0;
};

inline void accumulate(HostIpResult *total, const HostIpResult &chunk) {
#define CWS_ACCUMULATE(field) total->stats.field = checked_add(total->stats.field, chunk.stats.field)
    CWS_ACCUMULATE(point_fail); CWS_ACCUMULATE(degenerate); CWS_ACCUMULATE(certified);
    CWS_ACCUMULATE(deferred); CWS_ACCUMULATE(rank_deficient); CWS_ACCUMULATE(inci_fail);
    CWS_ACCUMULATE(rejected); CWS_ACCUMULATE(accepted); CWS_ACCUMULATE(np_mismatch);
#undef CWS_ACCUMULATE
    total->candidates = checked_add(total->candidates, chunk.candidates);
    total->scanned_selections = checked_add(total->scanned_selections, chunk.scanned_selections);
    total->seconds += chunk.seconds;
}

inline std::uint64_t outcome_sum(const DeviceIpStats &s) {  // np_mismatch is part of deferred
    std::uint64_t sum = 0;
    for (auto value : {s.point_fail, s.degenerate, s.certified, s.deferred, s.rank_deficient,
                       s.inci_fail, s.rejected, s.accepted}) sum = checked_add(sum, value);
    return sum;
}

// np_out values written by the point stage.
static constexpr int kNpDecided = -1;  // counted already (deferred or certified); not IP-checked

// Chunk-local ownership states. Auditing identities (not merely totals) makes
// lost/duplicated CUB selections, permutations and output records fail closed.
static constexpr int kRouteGenerated = 0, kRouteSurvivor = 1, kRouteDeferred = 2,
                     kRouteRejected = 3, kRoutePoint = 4, kRouteAccepted = 5,
                     kRouteAcceptedStored = 6, kRouteDeferredStored = 7, kRouteIp = 8;

__device__ bool device_same_candidate(const DeviceCwsCandidate &a, const DeviceCwsCandidate &b) {
    if (a.structure_id != b.structure_id || a.nw != b.nw || a.ambient_vertices != b.ambient_vertices)
        return false;
    if (a.nw < 1 || a.nw > kMaxSlots || a.ambient_vertices < 1 || a.ambient_vertices > 10) return false;
    for (int r = 0; r < a.nw; ++r) {
        if (a.degree[r] != b.degree[r] || a.sel_index[r] != b.sel_index[r]) return false;
        for (int c = 0; c < a.ambient_vertices; ++c)
            if (a.weights[r][c] != b.weights[r][c]) return false;
    }
    return true;
}

__device__ bool device_route(const DeviceCwsCandidate &c, int from, int to,
                             int *routes, unsigned long long original_count, int *error) {
    const auto id = c.pipeline_id;
    if (id >= original_count || atomicCAS(routes + id, from, to) != from) {
        atomicExch(error, 1);
        return false;
    }
    return true;
}

__global__ void initialize_routes_kernel(DeviceCwsCandidate *rows, unsigned long long count,
                                         bool joined, int *routes) {
    for (unsigned long long i = blockIdx.x * blockDim.x + threadIdx.x; i < count;
         i += blockDim.x * gridDim.x) {
        rows[i].pipeline_id = i;
        routes[i] = joined ? kRouteSurvivor : kRouteGenerated;
    }
}

__global__ void audit_selected_kernel(const DeviceCwsCandidate *rows, unsigned long long count,
                                      const DeviceCwsCandidate *original, unsigned long long original_count,
                                      const unsigned char *keep, int *routes, int *error) {
    for (unsigned long long i = blockIdx.x * blockDim.x + threadIdx.x; i < count;
         i += blockDim.x * gridDim.x) {
        const auto id = rows[i].pipeline_id;
        if (id >= original_count || !keep[id] || !device_same_candidate(rows[i], original[id])) {
            atomicExch(error, 1); continue;
        }
        device_route(rows[i], kRouteGenerated, kRouteSurvivor, routes, original_count, error);
    }
}

__global__ void audit_compaction_kernel(const unsigned char *keep, const int *routes,
                                        unsigned long long count, int *error) {
    for (unsigned long long i = blockIdx.x * blockDim.x + threadIdx.x; i < count;
         i += blockDim.x * gridDim.x)
        if ((keep[i] && routes[i] != kRouteSurvivor) ||
            (!keep[i] && routes[i] != kRouteRejected && routes[i] != kRouteDeferred)) atomicExch(error, 1);
}

__global__ void audit_output_kernel(const DeviceCwsCandidate *rows, unsigned long long count,
                                    const DeviceCwsCandidate *original, unsigned long long original_count,
                                    int from, int to, int *routes, int *error) {
    for (unsigned long long i = blockIdx.x * blockDim.x + threadIdx.x; i < count;
         i += blockDim.x * gridDim.x) {
        const auto id = rows[i].pipeline_id;
        if (id >= original_count || !device_same_candidate(rows[i], original[id])) {
            atomicExch(error, 1); continue;
        }
        device_route(rows[i], from, to, routes, original_count, error);
    }
}

__global__ void audit_terminal_kernel(const int *routes, unsigned long long count, int *error) {
    for (unsigned long long i = blockIdx.x * blockDim.x + threadIdx.x; i < count;
         i += blockDim.x * gridDim.x)
        if (routes[i] != kRouteRejected && routes[i] != kRouteAcceptedStored && routes[i] != kRouteDeferredStored)
            atomicExch(error, 1);
}

__device__ void device_defer(const DeviceCwsCandidate &candidate, DeviceIpStats *stats, DeviceCwsCandidate *deferred,
                             unsigned long long *deferred_count, unsigned long long capacity,
                             int *routes, unsigned long long original_count, int *error, int from = kRoutePoint) {
    device_route(candidate, from, kRouteDeferred, routes, original_count, error);
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
                                                            unsigned long long deferred_capacity, int *routes,
                                                            unsigned long long original_count, int *error) {
    for (unsigned long long index = blockIdx.x * blockDim.x + threadIdx.x; index < candidate_count;
         index += blockDim.x * gridDim.x) {
        const DeviceCwsCandidate &candidate = candidates[index];
        // The walk is complete only when it finds the join's exact point count;
        // without that count (np > np_cap in the fused join, or a layout the
        // join does not cover) there is nothing to check it against.
        if (candidate.join_np <= 0) {
            np_out[index] = kNpDecided;
            device_defer(candidate, stats, deferred, deferred_count, deferred_capacity, routes, original_count, error);
            continue;
        }
        long long *slot = points + index * static_cast<unsigned long long>(np_cap) * 5ULL;
        int np = 0;
        int ip_possible = 1;
        const int status = device_make_points_fp(candidate, slot, np_cap, &np, &ip_possible);
        const bool mismatch = status >= 0 && np != candidate.join_np;
        if (mismatch) atomicAdd(&stats->np_mismatch, 1ULL);
        if (status < 0 || mismatch) {
            np_out[index] = kNpDecided;
            device_defer(candidate, stats, deferred, deferred_count, deferred_capacity, routes, original_count, error);
        } else if (status == 0) {
            np_out[index] = kNpDecided;
            device_defer(candidate, stats, deferred, deferred_count, deferred_capacity, routes, original_count, error);
        } else if (np < 6) {
            np_out[index] = kNpDecided;
            device_route(candidate, kRoutePoint, kRouteRejected, routes, original_count, error);
            atomicAdd(&stats->degenerate, 1ULL);
        } else if (!ip_possible) {
            np_out[index] = kNpDecided;
            device_route(candidate, kRoutePoint, kRouteRejected, routes, original_count, error);
            atomicAdd(&stats->certified, 1ULL);
        } else {
            np_out[index] = np;
        }
    }
}

// Join stage: keep[i] = 1 for candidates the walk must still process. Decided
// candidates are counted here; kept ones carry their exact point count, and
// those with more than np_cap points are deferred. With a census only counts.
__global__ void join_filter_kernel(DeviceCwsCandidate *candidates, unsigned long long candidate_count,
                                   const SelectedEntry *entries, GJoinPlan plan, int np_cap, unsigned char *keep,
                                   DeviceIpStats *stats, DeviceCwsCandidate *deferred,
                                   unsigned long long *deferred_count, unsigned long long deferred_capacity,
                                   GJoinCensus *census, int *routes, unsigned long long original_count, int *error) {
    for (unsigned long long index = blockIdx.x * blockDim.x + threadIdx.x; index < candidate_count;
         index += blockDim.x * gridDim.x) {
        const DeviceCwsCandidate &c = candidates[index];
        keep[index] = 0;
        long long x_upper[10] = {0};
        if (!device_candidate_basic_precheck(c, x_upper)) {
            if (!census) device_defer(c, stats, deferred, deferred_count, deferred_capacity,
                                      routes, original_count, error, kRouteGenerated);
            else atomicAdd(&stats->point_fail, 1ULL);
            continue;
        }
        unsigned long long np = 0;
        const int verdict = device_gjoin_decide(c, entries, plan, &np);
        if (census) {
            atomicMax(&census->max_np_all, np);
            if (verdict == 0) atomicAdd(&census->degenerate, 1ULL);
            else if (verdict == 1) atomicAdd(&census->certified, 1ULL);
            else {
                atomicAdd(&census->kept, 1ULL);
                atomicMax(&census->max_np_kept, np);
                if (np > static_cast<unsigned long long>(np_cap)) atomicAdd(&census->kept_over_cap, 1ULL);
            }
        } else if (verdict == 0) {
            device_route(c, kRouteGenerated, kRouteRejected, routes, original_count, error);
            atomicAdd(&stats->degenerate, 1ULL);
        } else if (verdict == 1) {
            device_route(c, kRouteGenerated, kRouteRejected, routes, original_count, error);
            atomicAdd(&stats->certified, 1ULL);
        } else if (np > static_cast<unsigned long long>(np_cap)) {
            device_defer(c, stats, deferred, deferred_count, deferred_capacity, routes, original_count, error, kRouteGenerated);
        } else {
            keep[index] = 1;
            candidates[index].join_np = static_cast<int>(np);
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
    while (bits < 31 && (1u << bits) <= static_cast<unsigned int>(np_cap)) ++bits;
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
                                         DeviceCwsCandidate *dst, int *routes,
                                         unsigned long long original_count, int *error) {
    for (unsigned long long index = blockIdx.x * blockDim.x + threadIdx.x; index < count;
         index += blockDim.x * gridDim.x) {
        const int source = perm[index];
        if (source < 0 || static_cast<unsigned long long>(source) >= count) {
            atomicExch(error, 1); continue;
        }
        dst[index] = src[source];
        device_route(dst[index], kRouteSurvivor, kRoutePoint, routes, original_count, error);
    }
}

// IP stage over the np-sorted candidates with 6..np_cap points. Scratch is per
// launched thread.
__global__ void ip_check_kernel(const DeviceCwsCandidate *candidates, const long long *points, const int *np_out,
                                int np_cap, const int *valid_index, unsigned long long valid_count,
                                DeviceIpScratch *scratch_workspace, DeviceIpStats *stats,
                                DeviceCwsCandidate *accepted, unsigned long long *accepted_count,
                                unsigned long long accepted_capacity, DeviceCwsCandidate *deferred,
                                unsigned long long *deferred_count, unsigned long long deferred_capacity,
                                unsigned long long slice_count, int *routes, unsigned long long original_count,
                                int *error) {
    const unsigned long long tid = blockIdx.x * blockDim.x + threadIdx.x;
    DeviceIpScratch *scratch = scratch_workspace + tid;
    for (unsigned long long j = tid; j < valid_count; j += blockDim.x * gridDim.x) {
        const int selected = valid_index[j];
        if (selected < 0 || static_cast<unsigned long long>(selected) >= slice_count || np_out[selected] < 6 || np_out[selected] > np_cap) {
            atomicExch(error, 1); continue;
        }
        const unsigned long long index = static_cast<unsigned long long>(selected);
        if (!device_route(candidates[index], kRoutePoint, kRouteIp, routes, original_count, error)) continue;
        const long long *slot = points + index * static_cast<unsigned long long>(np_cap) * 5ULL;
        int reason = 0;
        if (device_ip_check(slot, np_out[index], scratch, &reason)) {
            device_route(candidates[index], kRouteIp, kRouteAccepted, routes, original_count, error);
            atomicAdd(&stats->accepted, 1ULL);
            const unsigned long long a = atomicAdd(accepted_count, 1ULL);
            if (a < accepted_capacity) accepted[a] = candidates[index];
        } else if (reason == 4 || reason == 1) {
            device_route(candidates[index], kRouteIp, kRouteRejected, routes, original_count, error);
            atomicAdd(reason == 4 ? &stats->rejected : &stats->rank_deficient, 1ULL);
        } else {
            device_defer(candidates[index], stats, deferred, deferred_count, deferred_capacity,
                         routes, original_count, error, kRouteIp);
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
    std::uint64_t chunk_capacity = 0;  // candidates generated per chunk
    std::uint64_t slice_capacity = 0;  // join survivors walked and IP-checked at once
    bool census = false;               // join only: exact point-count maxima, nothing written
};

struct JoinContext {
    const SelectedEntry *entries = nullptr;
    GJoinPlan plan;
#if CWS_GPU_FUSED_TYPE3
    const JoinTables *type3 = nullptr;  // set: structure 3 is joined during generation
#endif
};

// Device buffers, allocated once per run. Candidate-sized buffers hold a chunk;
// point and index buffers hold one slice of join survivors.
struct Workspace {
    std::uint64_t capacity = 0, slice = 0;
    DeviceCwsCandidate *candidates = nullptr, *survivors = nullptr, *accepted = nullptr, *deferred = nullptr;
    unsigned char *keep = nullptr;
    int *routes = nullptr, *audit_error = nullptr;
    std::uint64_t audit_count = 0;
    int *num_selected = nullptr;
    void *select_temp = nullptr;
    std::size_t select_temp_bytes = 0;
    long long *points = nullptr;
    int *np_out = nullptr, *valid_index = nullptr, *keys = nullptr, *perm = nullptr, *iota = nullptr;
    int *np_keys = nullptr, *keys_sorted = nullptr;
    DeviceCwsCandidate *sorted = nullptr;
    unsigned long long *counts = nullptr;  // [0] valid, [1] accepted, [2] deferred
    DeviceIpScratch *scratch = nullptr;
    DeviceIpStats *stats = nullptr;
    GJoinCensus *census = nullptr;
    void *sort_temp = nullptr;
    std::size_t sort_temp_bytes = 0;

    explicit Workspace(const PipelineOptions &options)
        : capacity(options.chunk_capacity), slice(std::min(options.chunk_capacity, options.slice_capacity)) {
        const std::uint64_t threads = static_cast<std::uint64_t>(options.blocks) * options.threads;
        for (DeviceCwsCandidate **buffer : {&candidates, &survivors, &accepted, &deferred})
            check_cuda(cudaMalloc(buffer, checked_mul(capacity, sizeof(DeviceCwsCandidate))), "cudaMalloc candidate buffer");
        check_cuda(cudaMalloc(&routes, checked_mul(capacity, sizeof(int))), "cudaMalloc ownership states");
        check_cuda(cudaMalloc(&audit_error, sizeof(int)), "cudaMalloc ownership error");
        check_cuda(cudaMalloc(&keep, capacity), "cudaMalloc keep");
        check_cuda(cudaMalloc(&num_selected, sizeof(int)), "cudaMalloc selected");
        check_cuda(cub::DeviceSelect::Flagged(nullptr, select_temp_bytes, candidates, keep, survivors, num_selected,
                                              static_cast<int>(capacity)), "cub select size");
        check_cuda(cudaMalloc(&select_temp, std::max<std::size_t>(select_temp_bytes, 1)), "cudaMalloc select temp");
        check_cuda(cudaMalloc(&points, checked_mul(checked_mul(slice, options.np_cap), 5ULL * sizeof(long long))), "cudaMalloc points");
        for (int **buffer : {&np_out, &valid_index, &keys, &perm, &iota, &np_keys, &keys_sorted})
            check_cuda(cudaMalloc(buffer, checked_mul(slice, sizeof(int))), "cudaMalloc index buffer");
        check_cuda(cudaMalloc(&sorted, checked_mul(slice, sizeof(DeviceCwsCandidate))), "cudaMalloc sorted");
        check_cuda(cudaMalloc(&counts, 3 * sizeof(unsigned long long)), "cudaMalloc counts");
        check_cuda(cudaMalloc(&scratch, checked_mul(threads, sizeof(DeviceIpScratch))), "cudaMalloc IP scratch");
        check_cuda(cudaMalloc(&stats, sizeof(DeviceIpStats)), "cudaMalloc stats");
        check_cuda(cudaMalloc(&census, sizeof(GJoinCensus)), "cudaMalloc census");
        std::size_t vol_bytes = 0, np_bytes = 0;
        const int items = static_cast<int>(slice);
        check_cuda(cub::DeviceRadixSort::SortPairs(nullptr, vol_bytes, keys, keys_sorted, iota, perm, items, 0,
                                                   kVolKeyBits), "cub sort size");
        check_cuda(cub::DeviceRadixSort::SortPairs(nullptr, np_bytes, np_keys, keys_sorted, iota, valid_index, items, 0,
                                                   np_key_bits(options.np_cap)), "cub sort size");
        sort_temp_bytes = std::max(vol_bytes, np_bytes);
        check_cuda(cudaMalloc(&sort_temp, sort_temp_bytes), "cudaMalloc sort temp");
    }
    ~Workspace() {
        for (void *p : {static_cast<void *>(sort_temp), static_cast<void *>(census), static_cast<void *>(stats),
                        static_cast<void *>(scratch), static_cast<void *>(counts), static_cast<void *>(sorted),
                        static_cast<void *>(keys_sorted), static_cast<void *>(np_keys), static_cast<void *>(iota),
                        static_cast<void *>(perm), static_cast<void *>(keys), static_cast<void *>(valid_index),
                        static_cast<void *>(np_out), static_cast<void *>(points), select_temp,
                        static_cast<void *>(num_selected), static_cast<void *>(routes), static_cast<void *>(audit_error), static_cast<void *>(keep), static_cast<void *>(deferred),
                        static_cast<void *>(accepted), static_cast<void *>(survivors), static_cast<void *>(candidates)})
            cudaFree(p);
    }
    Workspace(const Workspace &) = delete;
    Workspace &operator=(const Workspace &) = delete;
};

inline void require_identity_audit(Workspace &ws) {
    int error = 0;
    check_cuda(cudaMemcpy(&error, ws.audit_error, sizeof(error), cudaMemcpyDeviceToHost), "copy identity audit");
    if (error) throw std::runtime_error("candidate identity audit failed: duplicate, missing or changed record");
}

inline void copy_rows(const DeviceCwsCandidate *device_rows, unsigned long long count, std::ostream &out) {
    if (!count) return;
    std::vector<DeviceCwsCandidate> rows(static_cast<std::size_t>(count));
    check_cuda(cudaMemcpy(rows.data(), device_rows, count * sizeof(DeviceCwsCandidate), cudaMemcpyDeviceToHost),
               "cudaMemcpy rows");
    for (const DeviceCwsCandidate &row : rows) write_candidate_row(out, row);
    if (!out) throw std::runtime_error("failed to write output rows");
}

// Points, certificates and IP check for one slice of join survivors.
inline void classify_slice(DeviceCwsCandidate *candidates, std::uint64_t count, const PipelineOptions &options,
                           Workspace &ws) {
    const int blocks = options.blocks, threads = options.threads, items = static_cast<int>(count);
    iota_kernel<<<blocks, threads>>>(ws.iota, count);
    vol_key_kernel<<<blocks, threads>>>(candidates, count, ws.keys);
    std::size_t temp = ws.sort_temp_bytes;
    check_cuda(cub::DeviceRadixSort::SortPairs(ws.sort_temp, temp, ws.keys, ws.keys_sorted, ws.iota, ws.perm, items, 0,
                                               kVolKeyBits), "cub volume sort");
    gather_candidates_kernel<<<blocks, threads>>>(candidates, ws.perm, count, ws.sorted,
                                                    ws.routes, ws.audit_count, ws.audit_error);
    check_cuda(cudaGetLastError(), "gather audited candidates");
    require_identity_audit(ws);
    point_enum_kernel<<<blocks, threads>>>(ws.sorted, count, options.np_cap, ws.points, ws.np_out, ws.stats,
                                           ws.deferred, ws.counts + 2, ws.capacity, ws.routes, ws.audit_count, ws.audit_error);
    check_cuda(cudaGetLastError(), "point_enum_kernel launch");
    check_cuda(cudaMemset(ws.counts, 0, sizeof(unsigned long long)), "cudaMemset valid count");
    np_key_kernel<<<blocks, threads>>>(ws.np_out, count, ws.np_keys, ws.counts);
    temp = ws.sort_temp_bytes;
    check_cuda(cub::DeviceRadixSort::SortPairs(ws.sort_temp, temp, ws.np_keys, ws.keys_sorted, ws.iota, ws.valid_index,
                                               items, 0, np_key_bits(options.np_cap)), "cub np sort");
    unsigned long long valid = 0;
    check_cuda(cudaMemcpy(&valid, ws.counts, sizeof(valid), cudaMemcpyDeviceToHost), "cudaMemcpy valid count");
    if (valid > count) throw std::runtime_error("IP selection count exceeds slice size");
    if (valid) {
        ip_check_kernel<<<blocks, threads>>>(ws.sorted, ws.points, ws.np_out, options.np_cap,
                                             ws.valid_index + (count - valid), valid, ws.scratch, ws.stats,
                                             ws.accepted, ws.counts + 1, ws.capacity, ws.deferred, ws.counts + 2,
                                             ws.capacity, count, ws.routes, ws.audit_count, ws.audit_error);
        check_cuda(cudaGetLastError(), "ip_check_kernel launch");
    }
    check_cuda(cudaDeviceSynchronize(), "classify slice");
}

// Join, then points and IP check of the survivors, for one generated chunk.
inline HostIpResult classify_chunk(std::uint64_t count, bool joined, const PipelineOptions &options,
                                   const JoinContext &join, Workspace &ws, GJoinCensus *census_total,
                                   std::ostream &accepted_out, std::ostream &deferred_out) {
    HostIpResult result;
    result.candidates = count;
    if (count == 0) return result;
    if (count > ws.capacity) throw std::runtime_error("chunk exceeds allocated capacity");
    ws.audit_count = count;
    check_cuda(cudaMemset(ws.audit_error, 0, sizeof(int)), "clear identity audit");
    initialize_routes_kernel<<<options.blocks, options.threads>>>(ws.candidates, count, joined, ws.routes);
    check_cuda(cudaGetLastError(), "initialize candidate identities");
    auto start = std::chrono::steady_clock::now();
    check_cuda(cudaMemset(ws.stats, 0, sizeof(DeviceIpStats)), "cudaMemset stats");
    check_cuda(cudaMemset(ws.counts, 0, 3 * sizeof(unsigned long long)), "cudaMemset counts");
    if (options.census) check_cuda(cudaMemset(ws.census, 0, sizeof(GJoinCensus)), "cudaMemset census");
    // `joined`: the candidates are join survivors already (fused generation).
    DeviceCwsCandidate *survivor_rows = ws.candidates;
    std::uint64_t survivors = count;
    if (!joined) {
        join_filter_kernel<<<options.blocks, options.threads>>>(ws.candidates, count, join.entries, join.plan,
                                                                options.np_cap, ws.keep, ws.stats, ws.deferred,
                                                                ws.counts + 2, ws.capacity,
                                                                options.census ? ws.census : nullptr, ws.routes, count, ws.audit_error);
        check_cuda(cudaGetLastError(), "join_filter_kernel launch");
        std::size_t temp = ws.select_temp_bytes;
        check_cuda(cub::DeviceSelect::Flagged(ws.select_temp, temp, ws.candidates, ws.keep, ws.survivors,
                                              ws.num_selected, static_cast<int>(count)), "cub join compaction");
        int selected = 0;
        check_cuda(cudaMemcpy(&selected, ws.num_selected, sizeof(int), cudaMemcpyDeviceToHost), "cudaMemcpy survivors");
        if (selected < 0 || static_cast<std::uint64_t>(selected) > count)
            throw std::runtime_error("compacted candidate count exceeds input");
        survivor_rows = ws.survivors;
        survivors = static_cast<std::uint64_t>(selected);
        if (!options.census) {
            audit_selected_kernel<<<options.blocks, options.threads>>>(survivor_rows, survivors, ws.candidates,
                count, ws.keep, ws.routes, ws.audit_error);
            audit_compaction_kernel<<<options.blocks, options.threads>>>(ws.keep, ws.routes, count, ws.audit_error);
            check_cuda(cudaGetLastError(), "audit join compaction");
            require_identity_audit(ws);
        }
    }
    for (std::uint64_t first = 0; first < survivors; first += ws.slice)
        classify_slice(survivor_rows + first, std::min<std::uint64_t>(ws.slice, survivors - first), options, ws);
    check_cuda(cudaDeviceSynchronize(), "classify chunk");
    result.seconds = std::chrono::duration<double>(std::chrono::steady_clock::now() - start).count();

    unsigned long long counts[3] = {0, 0, 0};
    check_cuda(cudaMemcpy(counts, ws.counts, sizeof(counts), cudaMemcpyDeviceToHost), "cudaMemcpy counts");
    check_cuda(cudaMemcpy(&result.stats, ws.stats, sizeof(DeviceIpStats), cudaMemcpyDeviceToHost), "cudaMemcpy stats");
    if (options.census) {
        GJoinCensus c{};
        check_cuda(cudaMemcpy(&c, ws.census, sizeof(c), cudaMemcpyDeviceToHost), "cudaMemcpy census");
        census_total->degenerate += c.degenerate;
        census_total->certified += c.certified;
        census_total->kept += c.kept;
        census_total->kept_over_cap += c.kept_over_cap;
        census_total->max_np_all = std::max(census_total->max_np_all, c.max_np_all);
        census_total->max_np_kept = std::max(census_total->max_np_kept, c.max_np_kept);
        if (c.degenerate + c.certified + c.kept + result.stats.point_fail != count)
            throw std::runtime_error("census accounting failed");
        return result;
    }
    // Accounting: every candidate has exactly one outcome and every accepted or
    // deferred row fits its buffer, so nothing is lost silently.
    if (outcome_sum(result.stats) != count || counts[1] != result.stats.accepted || counts[2] != result.stats.deferred ||
        counts[1] > ws.capacity || counts[2] > ws.capacity)
        throw std::runtime_error("chunk accounting failed: outcomes do not add up to the candidate count");
    audit_output_kernel<<<options.blocks, options.threads>>>(ws.accepted, counts[1], ws.candidates, count,
        kRouteAccepted, kRouteAcceptedStored, ws.routes, ws.audit_error);
    audit_output_kernel<<<options.blocks, options.threads>>>(ws.deferred, counts[2], ws.candidates, count,
        kRouteDeferred, kRouteDeferredStored, ws.routes, ws.audit_error);
    audit_terminal_kernel<<<options.blocks, options.threads>>>(ws.routes, count, ws.audit_error);
    check_cuda(cudaGetLastError(), "audit terminal candidate identities");
    require_identity_audit(ws);
    copy_rows(ws.accepted, counts[1], accepted_out);
    copy_rows(ws.deferred, counts[2], deferred_out);
    return result;
}

// Generates and classifies a shard in chunks. A chunk that would not fit the
// buffer is halved and retried, so no candidate is truncated. Because the join
// removes most candidates before the point buffers, a chunk that stays under a
// quarter full doubles its span.
inline HostIpResult run_shard(const DeviceDescriptor &descriptor, const JoinContext &join, DeviceScanStats *device_stats,
                              std::uint64_t shard_start, std::uint64_t shard_size, const PipelineOptions &options,
                              std::ostream &accepted_out, std::ostream &deferred_out, std::uint64_t *generated,
                              GJoinCensus *census_total) {
    HostIpResult total;
    Workspace ws(options);
    std::uint64_t span = std::max<std::uint64_t>(1, options.chunk_capacity / max_prefix_variants_per_selection(descriptor));
    *generated = 0;
    const std::uint64_t shard_end = checked_add(shard_start, shard_size);
    for (std::uint64_t position = shard_start; position < shard_end;) {
        const std::uint64_t count = std::min(span, shard_end - position);
        bool joined = false;
#if CWS_GPU_FUSED_TYPE3
        joined = join.type3 != nullptr;
        const HostScanResult scan = joined
            ? scan_type3_fused(descriptor, join.entries, device_stats, ws.candidates, options.chunk_capacity,
                               options.blocks, options.threads, *join.type3, options.np_cap, position, count)
            : scan_descriptor_range(descriptor, join.entries, device_stats, ws.candidates, options.chunk_capacity,
                                    options.blocks, options.threads, position, count);
#else
        const HostScanResult scan = scan_descriptor_range(descriptor, join.entries, device_stats, ws.candidates,
                                                          options.chunk_capacity, options.blocks, options.threads,
                                                          position, count);
#endif
        if (scan.stored_candidate_count > options.chunk_capacity) {
            if (count == 1) throw std::runtime_error("one selection exceeds the chunk capacity; raise --chunk-capacity");
            span = std::max<std::uint64_t>(1, count / 2);
            continue;
        }
        if (scan.scanned_tuples != count) throw std::runtime_error("the scan visited a different number of tuples");
        const std::uint64_t decided = checked_add(scan.join_degenerate, scan.join_certified);
        if (checked_add(scan.stored_candidate_count, decided) != scan.prefix_candidates)
            throw std::runtime_error("generation stored a different number of candidates than it counted");
        *generated = checked_add(*generated, scan.prefix_candidates);
        HostIpResult chunk = classify_chunk(scan.stored_candidate_count, joined, options, join, ws, census_total,
                                            accepted_out, deferred_out);
        chunk.scanned_selections = scan.scanned_tuples;
        chunk.candidates = checked_add(chunk.candidates, decided);
        chunk.stats.degenerate = checked_add(chunk.stats.degenerate, scan.join_degenerate);
        chunk.stats.certified = checked_add(chunk.stats.certified, scan.join_certified);
        accumulate(&total, chunk);
        position += count;
        if (scan.stored_candidate_count * 4 < options.chunk_capacity) span = span > shard_size / 2 ? shard_size : span * 2;
    }
    if (total.scanned_selections != shard_size || total.candidates != *generated ||
        (!options.census && outcome_sum(total.stats) != *generated))
        throw std::runtime_error("shard completion accounting mismatch");
    return total;
}

}  // namespace cwsgpu
