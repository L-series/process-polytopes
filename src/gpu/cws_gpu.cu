// CUDA front end for the five-dimensional CWS descriptors (historical IDs 2..47).
//
//   build/cws-gpu.x --structure-id ID [--weights-dir DIR] [--shard-count N --shard-index I]
//
// Prints the number of candidates the generator produces for the selected
// shard, with the same pools and pruning as `build/cws.x -n5 -sID`.
#include "descriptor.cuh"
#include "generate.cuh"

#include <cstdlib>
#include <iostream>

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
};

long parse_long(const std::string &value) {
    std::size_t consumed = 0;
    long parsed = std::stol(value, &consumed, 0);
    if (consumed != value.size()) throw std::runtime_error("invalid integer: " + value);
    return parsed;
}

void usage(const char *argv0) {
    std::cerr << "Usage: " << argv0 << " --structure-id ID [options]\n"
              << "  --weights-dir DIR   pool files 2d.txt 3d.txt 4d.txt (default docs/data/single-weight-systems)\n"
              << "  --shard-count N     split the selection space into N contiguous shards\n"
              << "  --shard-index I     zero-based shard to run\n"
              << "  --cuda-device N     CUDA device (default 0, i.e. the first visible device)\n"
              << "  --blocks N          CUDA blocks (default 16 per SM)\n"
              << "  --threads N         CUDA threads per block (default 128)\n";
}

Config parse_args(int argc, char **argv) {
    Config config;
    for (int i = 1; i < argc; ++i) {
        const std::string arg = argv[i];
        auto value = [&]() -> std::string {
            if (i + 1 >= argc) throw std::runtime_error("missing value for " + arg);
            return argv[++i];
        };
        if (arg == "--structure-id") config.structure_id = static_cast<int>(parse_long(value()));
        else if (arg == "--weights-dir") config.weights_dir = value();
        else if (arg == "--shard-count") config.shard_count = static_cast<int>(parse_long(value()));
        else if (arg == "--shard-index") config.shard_index = static_cast<int>(parse_long(value()));
        else if (arg == "--cuda-device") config.cuda_device = static_cast<int>(parse_long(value()));
        else if (arg == "--blocks") config.blocks = static_cast<int>(parse_long(value()));
        else if (arg == "--threads") config.threads = static_cast<int>(parse_long(value()));
        else if (arg == "-h" || arg == "--help") { usage(argv[0]); std::exit(0); }
        else throw std::runtime_error("unknown option: " + arg);
    }
    if (config.structure_id < 2 || config.structure_id > 47) throw std::runtime_error("--structure-id must be in 2..47");
    if (config.shard_count <= 0 || config.shard_index < 0 || config.shard_index >= config.shard_count)
        throw std::runtime_error("--shard-index must be in [0, --shard-count)");
    if (config.threads <= 0 || config.threads > 1024 || (config.threads & (config.threads - 1)))
        throw std::runtime_error("--threads must be a power of two no larger than 1024");
    return config;
}

int run(const Config &config) {
    check_cuda(cudaSetDevice(config.cuda_device), "cudaSetDevice");
    // The prefix recursion in the generation kernels needs more than the default stack.
    check_cuda(cudaDeviceSetLimit(cudaLimitStackSize, 32u << 10), "cudaDeviceSetLimit stack");
    cudaDeviceProp properties{};
    check_cuda(cudaGetDeviceProperties(&properties, config.cuda_device), "cudaGetDeviceProperties");
    const int blocks = config.blocks ? config.blocks : properties.multiProcessorCount * 16;

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
    const HostScanResult result = scan_descriptor_range(descriptor, device_entries, device_stats, nullptr, 0, blocks,
                                                        config.threads, shard_start, shard_size);
    std::cout << "structure,selection_tuples,canonical_selection_tuples,candidates,seconds\n"
              << result.structure_id << ',' << result.scanned_tuples << ',' << result.canonical_selection_tuples << ','
              << result.prefix_candidates << ',' << result.seconds << '\n';

    cudaFree(device_stats);
    cudaFree(device_entries);
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
