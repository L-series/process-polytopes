// CUDA front end for the five-dimensional CWS descriptors (historical IDs 2..47).
//
//   build/cws-gpu.x --structure-id ID --accepted-output FILE --deferred-output FILE [options]
//   build/cws-gpu.x --structure-id ID --count-only [options]
//   build/cws-gpu.x --structure-id ID --census [options]
//
// Generates the candidates of one structure (the same pools and pruning as
// `build/cws.x -n5 -sID`), decides most of them with the join, and enumerates
// the lattice points of the rest and IP-checks them.
// Accepted IP CWS and deferred CWS (no GPU verdict, for the CPU) are written
// one per line in PALP CWS format.
#include "descriptor.cuh"
#include "pipeline.cuh"

#include <cstdlib>
#include <fstream>
#include <iostream>
#include <filesystem>
#include <limits>

namespace cwsgpu {
namespace {

struct Config {
    int structure_id = 0;
    std::string weights_dir = "docs/data/single-weight-systems";
    int shard_count = 1;
    int shard_index = 0;
    int cuda_device = 0;
    int blocks = 0;
    int threads = 128;
    bool count_only = false;
    bool census = false;
    int np_cap = 1024;
    std::uint64_t chunk_capacity = 4000000;
    std::uint64_t slice_capacity = 100000;
    std::string accepted_path, deferred_path;
};

long parse_long(const std::string &value) {
    std::size_t consumed = 0;
    long parsed = std::stol(value, &consumed, 0);
    if (consumed != value.size()) throw std::runtime_error("invalid integer: " + value);
    return parsed;
}

int parse_int(const std::string &value) {
    const long parsed = parse_long(value);
    if (parsed < std::numeric_limits<int>::min() || parsed > std::numeric_limits<int>::max())
        throw std::runtime_error("integer option exceeds signed 32-bit range");
    return static_cast<int>(parsed);
}

std::uint64_t parse_capacity(const std::string &value) {
    const long parsed = parse_long(value);
    if (parsed <= 0) throw std::runtime_error("capacity must be positive");
    return static_cast<std::uint64_t>(parsed);
}

void usage(const char *argv0) {
    std::cerr << "Usage: " << argv0 << " --structure-id ID [options]\n"
              << "  --weights-dir DIR   pool files 2d.txt 3d.txt 4d.txt (default docs/data/single-weight-systems)\n"
              << "  --shard-count N     split the selection space into N contiguous shards\n"
              << "  --shard-index I     zero-based shard to run\n"
              << "  --cuda-device N     CUDA device (default 0, i.e. the first visible device)\n"
              << "  --blocks N          CUDA blocks (default 16 per SM)\n"
              << "  --threads N         CUDA threads per block (default 128)\n"
              << "  --count-only        only count the generated candidates\n"
              << "  --np-cap N          largest point set checked on the GPU; larger ones are deferred (default 1024)\n"
              << "  --census            only join: exact largest point counts, nothing written\n"
              << "  --chunk-capacity N  candidates generated per chunk (default 4000000, ~1 KiB each)\n"
              << "  --slice-capacity N  join survivors walked at once (default 100000; ~ N * np-cap * 40 bytes)\n"
              << "  --accepted-output F write accepted IP CWS here\n"
              << "  --deferred-output F write CWS without a GPU verdict here (for the CPU)\n";
}

Config parse_args(int argc, char **argv) {
    Config config;
    for (int i = 1; i < argc; ++i) {
        const std::string arg = argv[i];
        auto value = [&]() -> std::string {
            if (i + 1 >= argc) throw std::runtime_error("missing value for " + arg);
            return argv[++i];
        };
        if (arg == "--structure-id") config.structure_id = parse_int(value());
        else if (arg == "--weights-dir") config.weights_dir = value();
        else if (arg == "--shard-count") config.shard_count = parse_int(value());
        else if (arg == "--shard-index") config.shard_index = parse_int(value());
        else if (arg == "--cuda-device") config.cuda_device = parse_int(value());
        else if (arg == "--blocks") config.blocks = parse_int(value());
        else if (arg == "--threads") config.threads = parse_int(value());
        else if (arg == "--count-only") config.count_only = true;
        else if (arg == "--np-cap") config.np_cap = parse_int(value());
        else if (arg == "--census") config.census = true;
        else if (arg == "--chunk-capacity") config.chunk_capacity = parse_capacity(value());
        else if (arg == "--slice-capacity") config.slice_capacity = parse_capacity(value());
        else if (arg == "--accepted-output") config.accepted_path = value();
        else if (arg == "--deferred-output") config.deferred_path = value();
        else if (arg == "-h" || arg == "--help") { usage(argv[0]); std::exit(0); }
        else throw std::runtime_error("unknown option: " + arg);
    }
    if (config.structure_id < 2 || config.structure_id > 47) throw std::runtime_error("--structure-id must be in 2..47");
    if (config.shard_count <= 0 || config.shard_index < 0 || config.shard_index >= config.shard_count)
        throw std::runtime_error("--shard-index must be in [0, --shard-count)");
    if (config.threads <= 0 || config.threads > 256 || (config.threads & (config.threads - 1)))
        throw std::runtime_error("--threads must be a power of two no larger than 256");
    if (!config.count_only && !config.census && (config.accepted_path.empty() || config.deferred_path.empty()))
        throw std::runtime_error("--accepted-output and --deferred-output are required (or --count-only / --census)");
    if (config.blocks < 0 || config.cuda_device < 0) throw std::runtime_error("blocks/device must be nonnegative");
    if (config.np_cap < 6 || config.chunk_capacity == 0 || config.chunk_capacity > 0x7FFFFFFF ||
        config.slice_capacity == 0)
        throw std::runtime_error("--np-cap must be >= 6, --chunk-capacity in 1..2^31-1, --slice-capacity >= 1");
    if (!config.count_only && !config.census) {
        const auto accepted = std::filesystem::weakly_canonical(config.accepted_path);
        const auto deferred = std::filesystem::weakly_canonical(config.deferred_path);
        if (accepted == deferred || (std::filesystem::exists(accepted) && std::filesystem::exists(deferred) &&
                                     std::filesystem::equivalent(accepted, deferred)))
            throw std::runtime_error("accepted and deferred outputs must be different files");
    }
    return config;
}

int run(const Config &config) {
    check_cuda(cudaSetDevice(config.cuda_device), "cudaSetDevice");
    // The prefix recursion in the generation kernels needs more than the default stack.
    check_cuda(cudaDeviceSetLimit(cudaLimitStackSize, 32u << 10), "cudaDeviceSetLimit stack");
    cudaDeviceProp properties{};
    check_cuda(cudaGetDeviceProperties(&properties, config.cuda_device), "cudaGetDeviceProperties");
    const int blocks = config.blocks ? config.blocks : properties.multiProcessorCount * 16;
    if (blocks <= 0 || checked_mul(static_cast<std::uint64_t>(blocks), config.threads) >
                           std::numeric_limits<unsigned int>::max())
        throw std::runtime_error("launch dimensions exceed the exact CUDA grid-stride arithmetic range");

    const GpuDescriptor catalogue = catalogue_descriptor(config.structure_id);
    const PoolArray pools = build_all_pools(config.weights_dir);
    const std::vector<SelectedEntry> flat = flatten_pools(pools);
    const DeviceDescriptor descriptor = make_device_descriptor(catalogue, pools);

    SelectedEntry *device_entries = nullptr;
    DeviceScanStats *device_stats = nullptr;
    check_cuda(cudaMalloc(&device_entries, flat.size() * sizeof(SelectedEntry)), "cudaMalloc entries");
    check_cuda(cudaMemcpy(device_entries, flat.data(), flat.size() * sizeof(SelectedEntry), cudaMemcpyHostToDevice),
               "cudaMemcpy entries");
    check_cuda(cudaMalloc(&device_stats, sizeof(DeviceScanStats)), "cudaMalloc stats");

    std::uint64_t shard_start = 0, shard_size = 0;
    descriptor_shard_range(descriptor, config.shard_count, config.shard_index, &shard_start, &shard_size);
    bool completed_classification = false;
    HostIpResult completion;
    std::uint64_t completion_generated = 0;
    if (config.count_only) {
        const HostScanResult result = scan_descriptor_range(descriptor, device_entries, device_stats, nullptr, 0,
                                                            blocks, config.threads, shard_start, shard_size);
        std::cout << "structure,selection_tuples,canonical_selection_tuples,candidates,seconds\n"
                  << result.structure_id << ',' << result.scanned_tuples << ',' << result.canonical_selection_tuples
                  << ',' << result.prefix_candidates << ',' << result.seconds << '\n';
    } else {
        std::array<HostGJoinTable, kPoolKeyStride * kPoolKeyStride> tables;
        JoinContext join;
        join.entries = device_entries;
#if CWS_GPU_FUSED_TYPE3
        HostJoinTables type3_tables;
        if (config.structure_id == 3 && !config.census) {
            // The fused kernel decodes pair indices, so the shard space must be the pair space.
            if (!type3_layout_supported(catalogue) || !use_pair_scan(descriptor)) throw std::runtime_error("structure 3 layout not supported by the fused join");
            const PoolInfo &pool = pools[pool_key(5, 3)];
            build_join_table(flat, device_entries, static_cast<int>(pool.device_offset),
                             static_cast<int>(pool.entries.size()), 5, 3, type3_tables);
            join.type3 = &type3_tables.view;
        } else {
            join.plan = build_gjoin_plan(catalogue, pools, tables);
        }
#else
        join.plan = build_gjoin_plan(catalogue, pools, tables);
#endif
        std::ofstream accepted_out, deferred_out;
        if (!config.census) {
            accepted_out.exceptions(std::ios::badbit | std::ios::failbit);
            deferred_out.exceptions(std::ios::badbit | std::ios::failbit);
            accepted_out.open(config.accepted_path);
            deferred_out.open(config.deferred_path);
            if (!accepted_out || !deferred_out) throw std::runtime_error("cannot open the output files");
        }
        PipelineOptions options;
        options.blocks = blocks;
        options.threads = config.threads;
        options.np_cap = config.np_cap;
        options.chunk_capacity = config.chunk_capacity;
        options.slice_capacity = config.slice_capacity;
        options.census = config.census;
        std::uint64_t generated = 0;
        GJoinCensus census{};
        const HostIpResult r = run_shard(descriptor, join, device_stats, shard_start, shard_size, options,
                                         accepted_out, deferred_out, &generated, &census);
        const DeviceIpStats &s = r.stats;
        if (config.census) {
            std::cout << "structure,candidates,degenerate,certified,kept,kept_over_np_cap,max_np_all,max_np_kept\n"
                      << descriptor.id << ',' << generated << ',' << census.degenerate << ',' << census.certified << ','
                      << census.kept << ',' << census.kept_over_cap << ',' << census.max_np_all << ','
                      << census.max_np_kept << '\n';
        } else {
            accepted_out.close();
            deferred_out.close();
            if (!accepted_out || !deferred_out) throw std::runtime_error("failed to write the output files");
            std::cout << "structure,candidates,accepted,deferred,rejected,certified,degenerate,rank_deficient,"
                         "inci_fail,point_fail,np_mismatch,seconds\n"
                      << descriptor.id << ',' << generated << ',' << s.accepted << ',' << s.deferred << ','
                      << s.rejected << ',' << s.certified << ',' << s.degenerate << ',' << s.rank_deficient << ','
                      << s.inci_fail << ',' << s.point_fail << ',' << s.np_mismatch << ',' << r.seconds << '\n';
            completed_classification = true;
            completion = r;
            completion_generated = generated;
            if (s.np_mismatch) std::cerr << "warning: " << s.np_mismatch << " walk/join point-count mismatches (deferred)\n";
        }
        for (HostGJoinTable &t : tables) {
            cudaFree(t.d_keys);
            cudaFree(t.d_offsets);
            cudaFree(t.d_covered);
            for (GJoinIdx *p : t.d_pidx) cudaFree(p);
        }
#if CWS_GPU_FUSED_TYPE3
        for (void *p : {static_cast<void *>(type3_tables.d_keys), static_cast<void *>(type3_tables.d_offsets),
                        static_cast<void *>(type3_tables.d_masks), static_cast<void *>(type3_tables.d_bloom),
                        static_cast<void *>(type3_tables.d_covered)})
            cudaFree(p);
#endif
    }

    check_cuda(cudaFree(device_stats), "free scan stats");
    check_cuda(cudaFree(device_entries), "free entries");
    if (completed_classification) {
        const auto &s = completion.stats;
        const auto rejected = outcome_sum(s) - s.accepted - s.deferred;
        std::cerr << "CWS_GPU_COMPLETION_V1 {\"structure_id\":" << config.structure_id
                  << ",\"shard_count\":" << config.shard_count << ",\"shard_index\":" << config.shard_index
                  << ",\"scan_space\":" << descriptor_scan_space(descriptor)
                  << ",\"range_start\":" << shard_start << ",\"range_count\":" << shard_size
                  << ",\"scanned_selections\":" << completion.scanned_selections
                  << ",\"generated\":" << completion_generated << ",\"processed\":" << completion.candidates
                  << ",\"accepted\":" << s.accepted << ",\"rejected\":" << rejected
                  << ",\"deferred\":" << s.deferred << ",\"identity_audited\":true}\n";
        if (!std::cerr) throw std::runtime_error("failed to write completion receipt");
    }
    return 0;
}

}  // namespace
}  // namespace cwsgpu

int main(int argc, char **argv) {
    try {
        return cwsgpu::run(cwsgpu::parse_args(argc, argv));
    } catch (const std::exception &error) {
        std::cerr << "error: " << error.what() << '\n';
        return 1;
    }
}
