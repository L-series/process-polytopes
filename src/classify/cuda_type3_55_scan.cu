#include <cuda_runtime.h>

#include <algorithm>
#include <array>
#include <chrono>
#include <cstdlib>
#include <cstdint>
#include <cstring>
#include <fstream>
#include <iomanip>
#include <iostream>
#include <limits>
#include <stdexcept>
#include <string>
#include <vector>

namespace {

// Return the CUDA device index allocated to this process by SLURM.
// When SLURM sets CUDA_VISIBLE_DEVICES the runtime remaps the physical
// GPU(s) to indices 0, 1, ..., so device 0 is always the right choice.
// When only SLURM_JOB_GPUS is present (no CUDA_VISIBLE_DEVICES remapping),
// use the first physical ordinal listed there.
static int slurm_default_cuda_device() {
    if (const char *cvd = std::getenv("CUDA_VISIBLE_DEVICES"))
        if (cvd[0] != '\0' && std::string(cvd) != "NoDevFiles")
            return 0;
    if (const char *sjg = std::getenv("SLURM_JOB_GPUS"))
        if (sjg[0] != '\0')
            try { return std::stoi(std::string(sjg)); } catch (...) {}
    return 0;
}

struct Weight5 {
    int degree = 0;
    int w[5] = {0, 0, 0, 0, 0};
};

struct Selected5 {
    int base_index = 0;
    int degree = 0;
    int w[5] = {0, 0, 0, 0, 0};
};

struct DeviceType3Candidate {
    int left_selected = 0;
    int right_selected = 0;
    int degree[2] = {0, 0};
    int weights[2][7] = {{0, 0, 0, 0, 0, 0, 0},
                         {0, 0, 0, 0, 0, 0, 0}};
};

struct DeviceScanStats {
    unsigned long long pair_count = 0;
    unsigned long long canonical_candidate_count = 0;
    unsigned long long stored_candidate_count = 0;
};

struct Config {
    std::string w5_path = "/tmp/process-polytopes-counts/w5.ip";
    int cuda_device = slurm_default_cuda_device();
    int blocks = 0;
    int threads = 256;
    unsigned long long start_pair = 0;
    unsigned long long pair_count = 1000000ULL;
    bool full = false;
    int shard_count = 1;
    int shard_index = 0;
    unsigned long long emit_capacity = 0;
    int print_candidates = 0;
    bool verify_cpu = false;
};

void check_cuda(cudaError_t status, const char *operation) {
    if (status != cudaSuccess) {
        throw std::runtime_error(std::string(operation) + ": " + cudaGetErrorString(status));
    }
}

unsigned long long checked_mul_div2(unsigned long long n) {
    if ((n & 1ULL) == 0) return (n / 2ULL) * (n + 1ULL);
    return n * ((n + 1ULL) / 2ULL);
}

void usage(const char *argv0) {
    std::cerr
        << "Usage: " << argv0 << " [options]\n"
        << "  --w5 <path>              W5 pool file, default /tmp/process-polytopes-counts/w5.ip\n"
        << "  --cuda-device <n>        CUDA device index, default 0\n"
        << "  --start-pair <n>         Upper-triangular selected-pair offset\n"
        << "  --pair-count <n>         Number of selected pairs to scan, default 1000000\n"
        << "  --full                   Scan the full selected-pair space\n"
        << "  --shard-count <n>        Split the full selected-pair space into n shards\n"
        << "  --shard-index <n>        Zero-based shard index\n"
        << "  --blocks <n>             CUDA block count, default SM count * 16\n"
        << "  --threads <n>            CUDA threads per block, default 256\n"
        << "  --emit-capacity <n>      Store up to n canonical CWS rows in device memory\n"
        << "  --print-candidates <n>   Print up to n stored candidate rows\n"
        << "  --verify-cpu             Verify the scanned range with the CPU counter\n";
}

unsigned long long parse_u64(const std::string &value) {
    std::size_t consumed = 0;
    unsigned long long parsed = std::stoull(value, &consumed, 0);
    if (consumed != value.size()) throw std::runtime_error("invalid integer: " + value);
    return parsed;
}

int parse_i32(const std::string &value) {
    std::size_t consumed = 0;
    long parsed = std::stol(value, &consumed, 0);
    if (consumed != value.size() || parsed < std::numeric_limits<int>::min() ||
        parsed > std::numeric_limits<int>::max()) {
        throw std::runtime_error("invalid integer: " + value);
    }
    return static_cast<int>(parsed);
}

Config parse_args(int argc, char **argv) {
    Config config;
    for (int index = 1; index < argc; ++index) {
        std::string arg = argv[index];
        auto require_value = [&](const char *name) -> std::string {
            if (index + 1 >= argc) throw std::runtime_error(std::string("missing value for ") + name);
            return argv[++index];
        };

        if (arg == "--w5") config.w5_path = require_value("--w5");
        else if (arg == "--cuda-device") config.cuda_device = parse_i32(require_value("--cuda-device"));
        else if (arg == "--start-pair") config.start_pair = parse_u64(require_value("--start-pair"));
        else if (arg == "--pair-count") config.pair_count = parse_u64(require_value("--pair-count"));
        else if (arg == "--full") config.full = true;
        else if (arg == "--shard-count") config.shard_count = parse_i32(require_value("--shard-count"));
        else if (arg == "--shard-index") config.shard_index = parse_i32(require_value("--shard-index"));
        else if (arg == "--blocks") config.blocks = parse_i32(require_value("--blocks"));
        else if (arg == "--threads") config.threads = parse_i32(require_value("--threads"));
        else if (arg == "--emit-capacity") config.emit_capacity = parse_u64(require_value("--emit-capacity"));
        else if (arg == "--print-candidates") config.print_candidates = parse_i32(require_value("--print-candidates"));
        else if (arg == "--verify-cpu") config.verify_cpu = true;
        else if (arg == "-h" || arg == "--help") {
            usage(argv[0]);
            std::exit(0);
        } else {
            throw std::runtime_error("unknown option: " + arg);
        }
    }

    if (config.shard_count <= 0) throw std::runtime_error("--shard-count must be positive");
    if (config.shard_index < 0 || config.shard_index >= config.shard_count) {
        throw std::runtime_error("--shard-index must be in [0, shard-count)");
    }
    if (config.threads <= 0 || config.threads > 1024) throw std::runtime_error("invalid --threads");
    if (config.blocks < 0) throw std::runtime_error("invalid --blocks");
    if (config.print_candidates < 0) throw std::runtime_error("invalid --print-candidates");
    return config;
}

std::vector<Weight5> load_w5_pool(const std::string &path) {
    std::ifstream input(path);
    if (!input) throw std::runtime_error("cannot open W5 pool: " + path);

    std::vector<Weight5> weights;
    Weight5 entry;
    while (input >> entry.degree >> entry.w[0] >> entry.w[1] >> entry.w[2] >> entry.w[3] >> entry.w[4]) {
        int sum = 0;
        for (int coord = 0; coord < 5; ++coord) {
            if (entry.w[coord] <= 0) throw std::runtime_error("W5 weights must be positive");
            if (coord > 0 && entry.w[coord - 1] > entry.w[coord]) {
                throw std::runtime_error("W5 weights must be sorted");
            }
            sum += entry.w[coord];
        }
        if (sum != entry.degree) throw std::runtime_error("W5 degree does not match row sum");
        weights.push_back(entry);
    }
    if (weights.empty()) throw std::runtime_error("empty W5 pool: " + path);
    return weights;
}

void append_selected_weight(const Weight5 &source,
                            int base_index,
                            const int selected_indices[3],
                            std::vector<Selected5> &selected) {
    Selected5 entry;
    entry.base_index = base_index;
    entry.degree = source.degree;

    int selected_position = 0;
    int output_position = 0;
    for (int index = 0; index < 3; ++index) {
        entry.w[output_position++] = source.w[selected_indices[index]];
    }
    for (int index = 0; index < 5; ++index) {
        if (selected_position < 3 && index == selected_indices[selected_position]) {
            ++selected_position;
            continue;
        }
        entry.w[output_position++] = source.w[index];
    }
    selected.push_back(entry);
}

void enumerate_dim5_selections(const Weight5 &source,
                               int base_index,
                               int selected_indices[3],
                               int selected_so_far,
                               std::vector<Selected5> &selected) {
    if (selected_so_far == 3) {
        append_selected_weight(source, base_index, selected_indices, selected);
        return;
    }

    int last_index = selected_indices[selected_so_far - 1];
    if (last_index == 4) return;

    if (source.w[last_index + 1] == source.w[last_index]) {
        selected_indices[selected_so_far] = last_index + 1;
        enumerate_dim5_selections(source, base_index, selected_indices,
                                  selected_so_far + 1, selected);
    }

    for (int index = last_index + 1; index < 5; ++index) {
        if (source.w[index] > source.w[index - 1]) {
            selected_indices[selected_so_far] = index;
            enumerate_dim5_selections(source, base_index, selected_indices,
                                      selected_so_far + 1, selected);
        }
    }
}

std::vector<Selected5> build_selected_u3_pool(const std::vector<Weight5> &base) {
    std::vector<Selected5> selected;
    selected.reserve(base.size() * 10ULL);
    int selected_indices[3] = {0, 0, 0};

    for (std::size_t base_index = 0; base_index < base.size(); ++base_index) {
        const Weight5 &source = base[base_index];
        selected_indices[0] = 0;
        enumerate_dim5_selections(source, static_cast<int>(base_index), selected_indices, 1, selected);
        for (int index = 1; index < 5; ++index) {
            if (source.w[index] > source.w[index - 1]) {
                selected_indices[0] = index;
                enumerate_dim5_selections(source, static_cast<int>(base_index), selected_indices, 1, selected);
            }
        }
    }
    return selected;
}

__device__ int device_prefix_is_canonical_for_anchor(const Selected5 &anchor,
                                                     const int prefix[3]) {
    int block_start = 0;
    while (block_start + 1 < 3) {
        int block_end = block_start + 1;
        while (block_end < 3 && anchor.w[block_end] == anchor.w[block_start]) {
            if (prefix[block_end - 1] > prefix[block_end]) return 0;
            ++block_end;
        }
        block_start = block_end;
    }
    return 1;
}

__device__ unsigned long long first_pair_for_left(unsigned long long left,
                                                  unsigned long long selected_count) {
    return left * selected_count - (left * (left - 1ULL)) / 2ULL;
}

__device__ unsigned long long pair_left_from_linear(unsigned long long pair_index,
                                                    unsigned long long selected_count) {
    unsigned long long low = 0;
    unsigned long long high = selected_count;
    while (low + 1ULL < high) {
        unsigned long long mid = low + (high - low) / 2ULL;
        if (first_pair_for_left(mid, selected_count) <= pair_index) low = mid;
        else high = mid;
    }
    return low;
}

__device__ int canonical_prefix_permutations(const Selected5 &left,
                                             const Selected5 &right,
                                             int output_prefixes[6][3]) {
    const int perms[6][3] = {
        {0, 1, 2}, {0, 2, 1}, {1, 0, 2},
        {1, 2, 0}, {2, 0, 1}, {2, 1, 0},
    };
    int count = 0;
    for (int permutation_index = 0; permutation_index < 6; ++permutation_index) {
        int prefix[3] = {
            right.w[perms[permutation_index][0]],
            right.w[perms[permutation_index][1]],
            right.w[perms[permutation_index][2]],
        };
        int duplicate = 0;
        for (int previous = 0; previous < permutation_index; ++previous) {
            int earlier[3] = {
                right.w[perms[previous][0]],
                right.w[perms[previous][1]],
                right.w[perms[previous][2]],
            };
            if (prefix[0] == earlier[0] && prefix[1] == earlier[1] && prefix[2] == earlier[2]) {
                duplicate = 1;
                break;
            }
        }
        if (duplicate) continue;
        if (!device_prefix_is_canonical_for_anchor(left, prefix)) continue;
        output_prefixes[count][0] = prefix[0];
        output_prefixes[count][1] = prefix[1];
        output_prefixes[count][2] = prefix[2];
        ++count;
    }
    return count;
}

__global__ void type3_scan_kernel(const Selected5 *selected,
                                  unsigned long long selected_count,
                                  unsigned long long start_pair,
                                  unsigned long long pair_count,
                                  DeviceScanStats *stats,
                                  DeviceType3Candidate *candidate_output,
                                  unsigned long long candidate_capacity) {
    unsigned long long local_pairs = 0;
    unsigned long long local_candidates = 0;
    unsigned long long global_thread = blockIdx.x * blockDim.x + threadIdx.x;
    unsigned long long stride = blockDim.x * gridDim.x;

    for (unsigned long long offset = global_thread; offset < pair_count; offset += stride) {
        unsigned long long pair_index = start_pair + offset;
        unsigned long long left_index = pair_left_from_linear(pair_index, selected_count);
        unsigned long long first_for_left = first_pair_for_left(left_index, selected_count);
        unsigned long long right_index = left_index + (pair_index - first_for_left);

        Selected5 left = selected[left_index];
        Selected5 right = selected[right_index];
        int prefixes[6][3];
        int candidate_count = canonical_prefix_permutations(left, right, prefixes);
        local_pairs += 1ULL;
        local_candidates += static_cast<unsigned long long>(candidate_count);

        if (candidate_output && candidate_capacity > 0ULL) {
            for (int prefix_index = 0; prefix_index < candidate_count; ++prefix_index) {
                unsigned long long output_index = atomicAdd(&stats->stored_candidate_count, 1ULL);
                if (output_index >= candidate_capacity) continue;

                DeviceType3Candidate out{};
                out.left_selected = static_cast<int>(left_index);
                out.right_selected = static_cast<int>(right_index);
                out.degree[0] = left.degree;
                out.degree[1] = right.degree;
                out.weights[0][0] = left.w[0];
                out.weights[0][1] = left.w[1];
                out.weights[0][2] = left.w[2];
                out.weights[0][3] = left.w[3];
                out.weights[0][4] = left.w[4];
                out.weights[1][0] = prefixes[prefix_index][0];
                out.weights[1][1] = prefixes[prefix_index][1];
                out.weights[1][2] = prefixes[prefix_index][2];
                out.weights[1][5] = right.w[3];
                out.weights[1][6] = right.w[4];
                candidate_output[output_index] = out;
            }
        }
    }

    if (local_pairs) atomicAdd(&stats->pair_count, local_pairs);
    if (local_candidates) atomicAdd(&stats->canonical_candidate_count, local_candidates);
}

int cpu_prefix_is_canonical_for_anchor(const Selected5 &anchor, const int prefix[3]) {
    int block_start = 0;
    while (block_start + 1 < 3) {
        int block_end = block_start + 1;
        while (block_end < 3 && anchor.w[block_end] == anchor.w[block_start]) {
            if (prefix[block_end - 1] > prefix[block_end]) return 0;
            ++block_end;
        }
        block_start = block_end;
    }
    return 1;
}

int cpu_canonical_prefix_count(const Selected5 &left, const Selected5 &right) {
    std::array<int, 3> prefix = {right.w[0], right.w[1], right.w[2]};
    std::sort(prefix.begin(), prefix.end());
    int count = 0;
    do {
        int candidate[3] = {prefix[0], prefix[1], prefix[2]};
        if (cpu_prefix_is_canonical_for_anchor(left, candidate)) ++count;
    } while (std::next_permutation(prefix.begin(), prefix.end()));
    return count;
}

unsigned long long cpu_count_range(const std::vector<Selected5> &selected,
                                   unsigned long long start_pair,
                                   unsigned long long pair_count) {
    unsigned long long selected_count = selected.size();
    unsigned long long total = 0;
    for (unsigned long long offset = 0; offset < pair_count; ++offset) {
        unsigned long long pair_index = start_pair + offset;
        unsigned long long low = 0;
        unsigned long long high = selected_count;
        while (low + 1ULL < high) {
            unsigned long long mid = low + (high - low) / 2ULL;
            unsigned long long first = mid * selected_count - (mid * (mid - 1ULL)) / 2ULL;
            if (first <= pair_index) low = mid;
            else high = mid;
        }
        unsigned long long left_index = low;
        unsigned long long first = left_index * selected_count - (left_index * (left_index - 1ULL)) / 2ULL;
        unsigned long long right_index = left_index + (pair_index - first);
        total += static_cast<unsigned long long>(
            cpu_canonical_prefix_count(selected[left_index], selected[right_index]));
    }
    return total;
}

void print_candidate(const DeviceType3Candidate &candidate) {
    std::cout << candidate.degree[0];
    for (int coord = 0; coord < 7; ++coord) std::cout << ' ' << candidate.weights[0][coord];
    std::cout << ' ' << candidate.degree[1];
    for (int coord = 0; coord < 7; ++coord) std::cout << ' ' << candidate.weights[1][coord];
    std::cout << " # selected=" << candidate.left_selected << ',' << candidate.right_selected << '\n';
}

}  // namespace

int main(int argc, char **argv) {
    try {
        Config config = parse_args(argc, argv);
        check_cuda(cudaSetDevice(config.cuda_device), "cudaSetDevice");

        cudaDeviceProp properties{};
        check_cuda(cudaGetDeviceProperties(&properties, config.cuda_device), "cudaGetDeviceProperties");
        if (config.blocks == 0) config.blocks = properties.multiProcessorCount * 16;

        auto load_start = std::chrono::steady_clock::now();
        std::vector<Weight5> base = load_w5_pool(config.w5_path);
        std::vector<Selected5> selected = build_selected_u3_pool(base);
        auto load_end = std::chrono::steady_clock::now();

        unsigned long long selected_count = static_cast<unsigned long long>(selected.size());
        unsigned long long total_pairs = checked_mul_div2(selected_count);

        if (config.full || config.shard_count > 1) {
            unsigned long long shard_start =
                (total_pairs * static_cast<unsigned long long>(config.shard_index)) /
                static_cast<unsigned long long>(config.shard_count);
            unsigned long long shard_end =
                (total_pairs * static_cast<unsigned long long>(config.shard_index + 1)) /
                static_cast<unsigned long long>(config.shard_count);
            config.start_pair = shard_start;
            config.pair_count = shard_end - shard_start;
        }

        if (config.start_pair > total_pairs) throw std::runtime_error("--start-pair exceeds total pair count");
        if (config.pair_count > total_pairs - config.start_pair) {
            config.pair_count = total_pairs - config.start_pair;
        }

        Selected5 *device_selected = nullptr;
        DeviceScanStats *device_stats = nullptr;
        DeviceType3Candidate *device_candidates = nullptr;
        check_cuda(cudaMalloc(&device_selected, selected.size() * sizeof(Selected5)), "cudaMalloc selected");
        check_cuda(cudaMemcpy(device_selected, selected.data(), selected.size() * sizeof(Selected5),
                              cudaMemcpyHostToDevice),
                   "cudaMemcpy selected");
        check_cuda(cudaMalloc(&device_stats, sizeof(DeviceScanStats)), "cudaMalloc stats");
        check_cuda(cudaMemset(device_stats, 0, sizeof(DeviceScanStats)), "cudaMemset stats");
        if (config.emit_capacity > 0) {
            check_cuda(cudaMalloc(&device_candidates,
                                  config.emit_capacity * sizeof(DeviceType3Candidate)),
                       "cudaMalloc candidates");
        }

        auto scan_start = std::chrono::steady_clock::now();
        type3_scan_kernel<<<config.blocks, config.threads>>>(
            device_selected, selected_count, config.start_pair, config.pair_count,
            device_stats, device_candidates, config.emit_capacity);
        check_cuda(cudaGetLastError(), "type3_scan_kernel launch");
        check_cuda(cudaDeviceSynchronize(), "cudaDeviceSynchronize scan");
        auto scan_end = std::chrono::steady_clock::now();

        DeviceScanStats stats{};
        check_cuda(cudaMemcpy(&stats, device_stats, sizeof(DeviceScanStats), cudaMemcpyDeviceToHost),
                   "cudaMemcpy stats");

        std::vector<DeviceType3Candidate> host_candidates;
        unsigned long long stored_to_copy = std::min(stats.stored_candidate_count, config.emit_capacity);
        if (device_candidates && stored_to_copy > 0) {
            host_candidates.resize(static_cast<std::size_t>(stored_to_copy));
            check_cuda(cudaMemcpy(host_candidates.data(), device_candidates,
                                  host_candidates.size() * sizeof(DeviceType3Candidate),
                                  cudaMemcpyDeviceToHost),
                       "cudaMemcpy candidates");
        }

        double load_seconds = std::chrono::duration<double>(load_end - load_start).count();
        double scan_seconds = std::chrono::duration<double>(scan_end - scan_start).count();
        double pairs_per_second = scan_seconds > 0.0 ? stats.pair_count / scan_seconds : 0.0;
        double candidates_per_second = scan_seconds > 0.0 ? stats.canonical_candidate_count / scan_seconds : 0.0;

        std::cerr << "type3_55_cuda_scan\n"
                  << "  device: " << properties.name << " sm_" << properties.major << properties.minor << '\n'
                  << "  base_w5: " << base.size() << '\n'
                  << "  selected_u3_pool: " << selected.size() << '\n'
                  << "  arity_only_base_pairs: " << checked_mul_div2(static_cast<unsigned long long>(base.size())) << '\n'
                  << "  selected_pair_space: " << total_pairs << '\n'
                  << "  scanned_start_pair: " << config.start_pair << '\n'
                  << "  scanned_pairs: " << stats.pair_count << '\n'
                  << "  canonical_prefix_candidates: " << stats.canonical_candidate_count << '\n'
                  << "  stored_candidates: " << stored_to_copy << " / " << stats.stored_candidate_count << '\n'
                  << "  blocks: " << config.blocks << '\n'
                  << "  threads_per_block: " << config.threads << '\n'
                  << "  pool_build_seconds: " << std::fixed << std::setprecision(3) << load_seconds << '\n'
                  << "  scan_seconds: " << std::fixed << std::setprecision(6) << scan_seconds << '\n'
                  << "  pairs_per_second: " << std::fixed << std::setprecision(1) << pairs_per_second << '\n'
                  << "  canonical_candidates_per_second: " << std::fixed << std::setprecision(1)
                  << candidates_per_second << '\n';

        if (config.verify_cpu) {
            auto verify_start = std::chrono::steady_clock::now();
            unsigned long long cpu_count = cpu_count_range(selected, config.start_pair, config.pair_count);
            auto verify_end = std::chrono::steady_clock::now();
            double verify_seconds = std::chrono::duration<double>(verify_end - verify_start).count();
            std::cerr << "  cpu_verify_candidates: " << cpu_count << '\n'
                      << "  cpu_verify_seconds: " << std::fixed << std::setprecision(3) << verify_seconds << '\n';
            if (cpu_count != stats.canonical_candidate_count) {
                throw std::runtime_error("CPU/GPU canonical prefix count mismatch");
            }
        }

        int to_print = std::min<int>(config.print_candidates, static_cast<int>(host_candidates.size()));
        for (int index = 0; index < to_print; ++index) print_candidate(host_candidates[index]);

        if (device_candidates) cudaFree(device_candidates);
        cudaFree(device_stats);
        cudaFree(device_selected);
        return 0;
    } catch (const std::exception &error) {
        std::cerr << "error: " << error.what() << '\n';
        return 1;
    }
}