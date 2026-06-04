#include <cuda_runtime.h>

#include <algorithm>
#include <array>
#include <chrono>
#include <cstdint>
#include <cstdlib>
#include <fstream>
#include <iomanip>
#include <iostream>
#include <limits>
#include <regex>
#include <sstream>
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

static constexpr int kMaxSlots = 5;
static constexpr int kMaxSize = 5;
static constexpr int kPoolKeyStride = 6;

struct Dim5StructureDescriptor {
    int id;
    int ambient_vertices;
    int simplex_count;
    int simplex_sizes[kMaxSlots];
    int shared_counts[kMaxSlots];
    int mappings[kMaxSlots][kMaxSize];
};

#include "dim5_structures.inc"

struct BaseWeight {
    int degree = 0;
    int size = 0;
    int w[kMaxSize] = {0, 0, 0, 0, 0};
};

struct SelectedEntry {
    int degree = 0;
    int size = 0;
    int w[kMaxSize] = {0, 0, 0, 0, 0};
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
    int mappings[kMaxSlots][kMaxSize] = {{0, 0, 0, 0, 0},
                                         {0, 0, 0, 0, 0},
                                         {0, 0, 0, 0, 0},
                                         {0, 0, 0, 0, 0},
                                         {0, 0, 0, 0, 0}};
    int family_groups[kMaxSlots] = {0, 0, 0, 0, 0};
    std::uint64_t pool_offsets[kMaxSlots] = {0, 0, 0, 0, 0};
    std::uint64_t pool_counts[kMaxSlots] = {0, 0, 0, 0, 0};
    std::uint64_t selection_product = 0;
};

struct DeviceScanStats {
    unsigned long long selection_tuples = 0;
    unsigned long long canonical_selection_tuples = 0;
    unsigned long long prefix_candidates = 0;
    unsigned long long stored_candidate_count = 0;
};

struct DeviceCwsCandidate {
    int structure_id = 0;
    int nw = 0;
    int ambient_vertices = 0;
    int degree[kMaxSlots] = {0, 0, 0, 0, 0};
    int weights[kMaxSlots][10] = {{0, 0, 0, 0, 0, 0, 0, 0, 0, 0},
                                  {0, 0, 0, 0, 0, 0, 0, 0, 0, 0},
                                  {0, 0, 0, 0, 0, 0, 0, 0, 0, 0},
                                  {0, 0, 0, 0, 0, 0, 0, 0, 0, 0},
                                  {0, 0, 0, 0, 0, 0, 0, 0, 0, 0}};
};

struct DeviceIpStats {
    unsigned long long processed = 0;
    unsigned long long precheck_fail = 0;
    unsigned long long point_overflow = 0;
    unsigned long long point_fail = 0;
    unsigned long long simplex_fail = 0;
    unsigned long long initial_inci_fail = 0;
    unsigned long long vertex_overflow = 0;
    unsigned long long ip_reject = 0;
    unsigned long long ip_count = 0;
    unsigned long long accepted_stored_count = 0;
};

struct DeviceIpStageStats {
    unsigned long long point_cycles = 0;
    unsigned long long ip_cycles = 0;
    unsigned long long glz_cycles = 0;
    unsigned long long initial_inci_cycles = 0;
    unsigned long long search_bad_eq_cycles = 0;
    unsigned long long search_new_vertex_cycles = 0;
    unsigned long long make_new_ceqs_cycles = 0;
    unsigned long long point_candidates = 0;
    unsigned long long ip_candidates = 0;
    unsigned long long search_bad_eq_calls = 0;
    unsigned long long search_new_vertex_calls = 0;
    unsigned long long make_new_ceqs_calls = 0;
    unsigned long long total_points = 0;
    unsigned long long max_points = 0;
    unsigned long long walk_nodes = 0;  // Exp D: tree-node count (FP would cut ~74x)
};

struct DeviceEquation5 {
    long long a[5] = {0, 0, 0, 0, 0};
    long long c = 0;
};

struct DeviceCEqList5 {
    int ne = 0;
    DeviceEquation5 e[64];
};

struct DeviceEqList5 {
    int ne = 0;
    DeviceEquation5 e[64];
};

struct DeviceIpScratch {
    int vertices[64];
    DeviceCEqList5 candidate_equations;
    DeviceEqList5 facets;
    DeviceCEqList5 bad_equations;
    unsigned long long ceq_inci[64];
    unsigned long long facet_inci[64];
    unsigned long long bad_inci[64];
};

struct HostScanResult {
    int structure_id = 0;
    std::uint64_t selection_product = 0;
    std::uint64_t scanned_tuples = 0;
    std::uint64_t canonical_selection_tuples = 0;
    std::uint64_t prefix_candidates = 0;
    std::uint64_t stored_candidate_count = 0;
    double seconds = 0.0;
};

struct HostIpResult {
    std::uint64_t candidate_count = 0;
    std::uint64_t processed = 0;
    std::uint64_t precheck_fail = 0;
    std::uint64_t point_overflow = 0;
    std::uint64_t point_fail = 0;
    std::uint64_t simplex_fail = 0;
    std::uint64_t initial_inci_fail = 0;
    std::uint64_t vertex_overflow = 0;
    std::uint64_t ip_reject = 0;
    std::uint64_t ip_count = 0;
    std::uint64_t accepted_stored_count = 0;
    DeviceIpStageStats stage{};
    double seconds = 0.0;
};

struct HostStreamIpResult {
    HostScanResult scan;
    HostIpResult ip;
    std::uint64_t chunks = 0;
    std::uint64_t retries = 0;
};

struct DeviceIpWorkspace {
    DeviceIpStats *stats = nullptr;
    DeviceIpStageStats *stage_stats = nullptr;
    DeviceCwsCandidate *accepted = nullptr;
    long long *points = nullptr;
    DeviceIpScratch *scratch = nullptr;
    std::uint64_t capacity = 0;
    std::uint64_t point_slots = 0;
    int max_points = 0;
};

struct Config {
    std::string w5_path = "results/cache/w5.ip";
    std::string palp_cws_path = "PALP/cws.c";
    int cuda_device = slurm_default_cuda_device();
    int structure_id = 0;
    int blocks = 0;
    int threads = 128;
    bool threads_explicit = false;
    int shard_count = 1;
    int shard_index = 0;
    std::uint64_t emit_capacity = 0;
    int print_candidates = 0;
    bool ip_check = false;
    bool stream_ip = false;
    bool block_ip = false;
    bool ip_stage_profile = false;
    // Per-candidate lattice-point buffer capacity. Defaults to PALP's POINT_Nmax
    // for POLY_Dmax==5 (see PALP/Global.h) so the GPU IP filter can hold every
    // lattice point any valid 5D CWS produces and never silently drops a
    // candidate. CORRECTNESS: a candidate that would exceed this is treated as a
    // hard error, not dropped. NOTE: at this size the device point workspace is
    // ~80 MB/slot, so the launch grid is capped to fit VRAM in main(); raising
    // throughput again is a separate (two-tier buffer / requeue) optimization.
    int ip_max_points = 2000000;  // == PALP POINT_Nmax (POLY_Dmax==5)
    std::string accepted_output_path;
    // ── Split bucketed IP pipeline (--ip-bucketed) ────────────────────────
    // Decouples the per-candidate point buffer from the launch grid: a lean
    // point-enumeration kernel writes each candidate's points into a COMPACT
    // np_cap-sized slot (memory scales with work, not the grid), so the grid
    // can cover every SM. ~98% of type-3 CWS have <=64 points; candidates that
    // exceed np_cap are NOT dropped -- they are written to --overflow-output for
    // the CPU (PALP) to finish, preserving dataset completeness. A second
    // (np-sorted) IP-check kernel then runs only over the valid candidates.
    bool ip_bucketed = false;
    bool vol_sort = false;  // Exp E: reorder candidates by box-volume proxy pre-enum
    int np_cap = 64;
    std::string overflow_output_path;
    bool all = false;
};

void check_cuda(cudaError_t status, const char *operation) {
    if (status != cudaSuccess) {
        throw std::runtime_error(std::string(operation) + ": " + cudaGetErrorString(status));
    }
}

std::uint64_t checked_mul(std::uint64_t left, std::uint64_t right) {
    if (right != 0 && left > std::numeric_limits<std::uint64_t>::max() / right) {
        throw std::runtime_error("uint64 overflow while computing selection product");
    }
    return left * right;
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

std::uint64_t parse_u64(const std::string &value) {
    std::size_t consumed = 0;
    unsigned long long parsed = std::stoull(value, &consumed, 0);
    if (consumed != value.size()) throw std::runtime_error("invalid integer: " + value);
    return static_cast<std::uint64_t>(parsed);
}

void usage(const char *argv0) {
    std::cerr
        << "Usage: " << argv0 << " [options]\n"
        << "  --all                    Scan all combined structure IDs 2..47\n"
        << "  --structure-id <id>      Scan one combined structure ID\n"
        << "  --w5 <path>              Published W5 pool, default results/cache/w5.ip\n"
        << "  --palp-cws <path>        PALP/cws.c path for builtin W4 parsing\n"
        << "  --cuda-device <n>        CUDA device index, default 0\n"
        << "  --shard-count <n>        Split each structure selection-product range\n"
        << "  --shard-index <n>        Zero-based shard index\n"
        << "  --blocks <n>             CUDA block count, default SM count * 16, or *256 with --block-ip\n"
        << "  --threads <n>            CUDA threads per block, default 128, or 32 with --block-ip\n"
        << "  --emit-capacity <n>      Store up to n generated CWS candidates on device\n"
        << "  --print-candidates <n>   Print up to n stored CWS candidates\n"
        << "  --ip-check               Run GPU IP check over the stored candidate buffer\n"
        << "  --stream-ip              Stream the full shard through chunked GPU IP filtering\n"
        << "  --block-ip               Use one CUDA block per active CWS for parallel points/equation scans\n"
        << "  --ip-stage-profile      Collect clock64 timing counters inside the GPU IP kernel\n"
        << "  --ip-max-points <n>      Per-candidate device point buffer, default 2000000 (== PALP POINT_Nmax); overflow is a hard error\n"
        << "  --accepted-output <path> Write accepted GPU-IP CWS rows to a text file\n"
        << "  --ip-bucketed            Split point-enum and IP-check into two kernels with a\n"
        << "                           compact np_cap point buffer (full-grid, low divergence)\n"
        << "  --np-cap <n>             Bucketed per-candidate point cap, default 64; candidates\n"
        << "                           exceeding it are shipped to --overflow-output for the CPU\n"
        << "  --overflow-output <path> Write np>np_cap CWS rows here for CPU (PALP) completion\n";
}

Config parse_args(int argc, char **argv) {
    Config config;
    for (int index = 1; index < argc; ++index) {
        std::string arg = argv[index];
        auto require_value = [&](const char *name) -> std::string {
            if (index + 1 >= argc) throw std::runtime_error(std::string("missing value for ") + name);
            return argv[++index];
        };

        if (arg == "--all") config.all = true;
        else if (arg == "--structure-id") config.structure_id = parse_i32(require_value("--structure-id"));
        else if (arg == "--w5") config.w5_path = require_value("--w5");
        else if (arg == "--palp-cws") config.palp_cws_path = require_value("--palp-cws");
        else if (arg == "--cuda-device") config.cuda_device = parse_i32(require_value("--cuda-device"));
        else if (arg == "--shard-count") config.shard_count = parse_i32(require_value("--shard-count"));
        else if (arg == "--shard-index") config.shard_index = parse_i32(require_value("--shard-index"));
        else if (arg == "--blocks") config.blocks = parse_i32(require_value("--blocks"));
        else if (arg == "--threads") {
            config.threads = parse_i32(require_value("--threads"));
            config.threads_explicit = true;
        }
        else if (arg == "--emit-capacity") config.emit_capacity = parse_u64(require_value("--emit-capacity"));
        else if (arg == "--print-candidates") config.print_candidates = parse_i32(require_value("--print-candidates"));
        else if (arg == "--ip-check") config.ip_check = true;
        else if (arg == "--stream-ip") config.stream_ip = true;
        else if (arg == "--block-ip") config.block_ip = true;
        else if (arg == "--ip-stage-profile") config.ip_stage_profile = true;
        else if (arg == "--ip-max-points") config.ip_max_points = parse_i32(require_value("--ip-max-points"));
        else if (arg == "--accepted-output") config.accepted_output_path = require_value("--accepted-output");
        else if (arg == "--ip-bucketed") config.ip_bucketed = true;
        else if (arg == "--vol-sort") config.vol_sort = true;
        else if (arg == "--np-cap") config.np_cap = parse_i32(require_value("--np-cap"));
        else if (arg == "--overflow-output") config.overflow_output_path = require_value("--overflow-output");
        else if (arg == "-h" || arg == "--help") {
            usage(argv[0]);
            std::exit(0);
        } else {
            throw std::runtime_error("unknown option: " + arg);
        }
    }

    if (!config.all && config.structure_id == 0) config.all = true;
    if (config.structure_id && (config.structure_id < 2 || config.structure_id > 47)) {
        throw std::runtime_error("--structure-id must be in 2..47");
    }
    if (config.shard_count <= 0) throw std::runtime_error("--shard-count must be positive");
    if (config.shard_index < 0 || config.shard_index >= config.shard_count) {
        throw std::runtime_error("--shard-index must be in [0, shard-count)");
    }
    if (config.threads <= 0 || config.threads > 1024 || (config.threads & (config.threads - 1)) != 0) {
        throw std::runtime_error("--threads must be a positive power of two no larger than 1024");
    }
    if (config.blocks < 0) throw std::runtime_error("invalid --blocks");
    if (config.print_candidates < 0) throw std::runtime_error("invalid --print-candidates");
    if (config.ip_max_points <= 0) throw std::runtime_error("invalid --ip-max-points");
    if (config.stream_ip) config.ip_check = true;
    if (config.ip_bucketed) {
        config.ip_check = true;  // bucketing IS the IP filter
        if (config.np_cap < 6) {
            throw std::runtime_error("--np-cap must be >= 6 (need >=6 points for a 5D simplex)");
        }
        if (config.stream_ip) {
            throw std::runtime_error("--ip-bucketed is incompatible with --stream-ip; use --ip-check");
        }
        // --block-ip + --ip-bucketed selects the block-cooperative point-enum
        // variant (Exp A): one block per candidate, shared basis, seed-split
        // frontier. The IP-check stage is unchanged.
    }
    if (config.ip_check && config.emit_capacity == 0) {
        throw std::runtime_error("--ip-check requires --emit-capacity > 0");
    }
    return config;
}

std::vector<BaseWeight> load_w5_pool(const std::string &path) {
    std::ifstream input(path);
    if (!input) throw std::runtime_error("cannot open W5 pool: " + path);

    std::vector<BaseWeight> weights;
    BaseWeight entry;
    entry.size = 5;
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

std::vector<BaseWeight> load_w4_pool_from_palp_cws(const std::string &path) {
    std::ifstream input(path);
    if (!input) throw std::runtime_error("cannot open PALP cws.c: " + path);
    std::stringstream buffer;
    buffer << input.rdbuf();
    std::string text = buffer.str();

    std::size_t begin = text.find("const wei4 W4[95]");
    std::size_t end = text.find("#include \"dim5_structures.inc\"", begin);
    if (begin == std::string::npos || end == std::string::npos || end <= begin) {
        throw std::runtime_error("could not locate W4 table in PALP cws.c");
    }
    std::string block = text.substr(begin, end - begin);
    std::regex entry_regex(R"(\{\s*(\d+)\s*,\s*\{\s*(\d+)\s*,\s*(\d+)\s*,\s*(\d+)\s*,\s*(\d+)\s*\}\s*\})");

    std::vector<BaseWeight> weights;
    for (std::sregex_iterator it(block.begin(), block.end(), entry_regex), last; it != last; ++it) {
        BaseWeight entry;
        entry.size = 4;
        entry.degree = std::stoi((*it)[1]);
        int sum = 0;
        for (int coord = 0; coord < 4; ++coord) {
            entry.w[coord] = std::stoi((*it)[coord + 2]);
            if (entry.w[coord] <= 0) throw std::runtime_error("W4 weights must be positive");
            if (coord > 0 && entry.w[coord - 1] > entry.w[coord]) {
                throw std::runtime_error("W4 weights must be sorted");
            }
            sum += entry.w[coord];
        }
        if (sum != entry.degree) throw std::runtime_error("W4 degree does not match row sum");
        weights.push_back(entry);
    }
    if (weights.size() != 95) {
        throw std::runtime_error("expected 95 W4 rows in PALP cws.c, found " + std::to_string(weights.size()));
    }
    return weights;
}

std::vector<BaseWeight> builtin_base_pool(int simplex_size,
                                          const std::string &palp_cws_path,
                                          const std::vector<BaseWeight> &w5_pool) {
    if (simplex_size == 2) {
        BaseWeight entry;
        entry.degree = 2;
        entry.size = 2;
        entry.w[0] = 1;
        entry.w[1] = 1;
        return {entry};
    }
    if (simplex_size == 3) {
        BaseWeight a;
        a.degree = 3;
        a.size = 3;
        a.w[0] = 1; a.w[1] = 1; a.w[2] = 1;
        BaseWeight b;
        b.degree = 4;
        b.size = 3;
        b.w[0] = 1; b.w[1] = 1; b.w[2] = 2;
        BaseWeight c;
        c.degree = 6;
        c.size = 3;
        c.w[0] = 1; c.w[1] = 2; c.w[2] = 3;
        return {a, b, c};
    }
    if (simplex_size == 4) return load_w4_pool_from_palp_cws(palp_cws_path);
    if (simplex_size == 5) return w5_pool;
    throw std::runtime_error("unsupported simplex size");
}

void append_selected_weight(const BaseWeight &source,
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

void enumerate_selections(const BaseWeight &source,
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

std::vector<SelectedEntry> build_selected_pool(const std::vector<BaseWeight> &base,
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

int pool_key(int simplex_size, int shared_count) {
    return simplex_size * kPoolKeyStride + shared_count;
}

std::array<PoolInfo, kPoolKeyStride * kPoolKeyStride> build_all_pools(
    const std::string &palp_cws_path,
    const std::vector<BaseWeight> &w5_pool) {
    std::array<PoolInfo, kPoolKeyStride * kPoolKeyStride> pools;
    for (int simplex_size = 2; simplex_size <= 5; ++simplex_size) {
        std::vector<BaseWeight> base = builtin_base_pool(simplex_size, palp_cws_path, w5_pool);
        for (int shared_count = 0; shared_count <= simplex_size; ++shared_count) {
            pools[pool_key(simplex_size, shared_count)].entries = build_selected_pool(base, shared_count);
        }
    }
    std::uint64_t offset = 0;
    for (PoolInfo &pool : pools) {
        pool.device_offset = offset;
        offset += static_cast<std::uint64_t>(pool.entries.size());
    }
    return pools;
}

std::vector<SelectedEntry> flatten_pools(const std::array<PoolInfo, kPoolKeyStride * kPoolKeyStride> &pools) {
    std::uint64_t total = 0;
    for (const PoolInfo &pool : pools) total += static_cast<std::uint64_t>(pool.entries.size());
    std::vector<SelectedEntry> flat;
    flat.reserve(static_cast<std::size_t>(total));
    for (const PoolInfo &pool : pools) {
        flat.insert(flat.end(), pool.entries.begin(), pool.entries.end());
    }
    return flat;
}

int is_descriptor_automorphism(const Dim5StructureDescriptor &descriptor,
                               const int permutation[kMaxSlots]) {
    int coordinate_map[11] = {0};
    int inverse_coordinate_map[11] = {0};

    for (int slot = 0; slot < descriptor.simplex_count; ++slot) {
        int target_slot = permutation[slot];
        int simplex_size = descriptor.simplex_sizes[slot];
        if (simplex_size != descriptor.simplex_sizes[target_slot]) return 0;
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

void enumerate_slot_automorphisms(const Dim5StructureDescriptor &descriptor,
                                  int slot,
                                  int permutation[kMaxSlots],
                                  int used[kMaxSlots],
                                  int reachable[kMaxSlots][kMaxSlots]) {
    if (slot >= descriptor.simplex_count) {
        if (is_descriptor_automorphism(descriptor, permutation)) {
            for (int index = 0; index < descriptor.simplex_count; ++index) {
                reachable[index][permutation[index]] = 1;
            }
        }
        return;
    }

    for (int target_slot = 0; target_slot < descriptor.simplex_count; ++target_slot) {
        if (used[target_slot]) continue;
        if (descriptor.simplex_sizes[slot] != descriptor.simplex_sizes[target_slot]) continue;
        if (descriptor.shared_counts[slot] != descriptor.shared_counts[target_slot]) continue;
        permutation[slot] = target_slot;
        used[target_slot] = 1;
        enumerate_slot_automorphisms(descriptor, slot + 1, permutation, used, reachable);
        used[target_slot] = 0;
    }
}

void compute_slot_orbit_groups(const Dim5StructureDescriptor &descriptor,
                               int orbit_groups[kMaxSlots]) {
    int permutation[kMaxSlots] = {0, 0, 0, 0, 0};
    int used[kMaxSlots] = {0, 0, 0, 0, 0};
    int reachable[kMaxSlots][kMaxSlots] = {{0, 0, 0, 0, 0},
                                           {0, 0, 0, 0, 0},
                                           {0, 0, 0, 0, 0},
                                           {0, 0, 0, 0, 0},
                                           {0, 0, 0, 0, 0}};
    for (int index = 0; index < descriptor.simplex_count; ++index) reachable[index][index] = 1;
    enumerate_slot_automorphisms(descriptor, 0, permutation, used, reachable);

    for (int index = 0; index < kMaxSlots; ++index) orbit_groups[index] = 0;
    int next_group = 1;
    for (int index = 0; index < descriptor.simplex_count; ++index) {
        if (orbit_groups[index]) continue;
        orbit_groups[index] = next_group;
        for (int other = index + 1; other < descriptor.simplex_count; ++other) {
            if (reachable[index][other]) orbit_groups[other] = next_group;
        }
        ++next_group;
    }
}

DeviceDescriptor make_device_descriptor(
    const Dim5StructureDescriptor &descriptor,
    const std::array<PoolInfo, kPoolKeyStride * kPoolKeyStride> &pools) {
    DeviceDescriptor device;
    device.id = descriptor.id;
    device.ambient_vertices = descriptor.ambient_vertices;
    device.simplex_count = descriptor.simplex_count;
    device.selection_product = 1;

    int orbit_groups[kMaxSlots] = {0, 0, 0, 0, 0};
    compute_slot_orbit_groups(descriptor, orbit_groups);

    for (int slot = 0; slot < kMaxSlots; ++slot) {
        device.simplex_sizes[slot] = descriptor.simplex_sizes[slot];
        device.shared_counts[slot] = descriptor.shared_counts[slot];
        device.family_groups[slot] = descriptor.simplex_sizes[slot] * (kMaxSlots + 1) + orbit_groups[slot];
        for (int position = 0; position < kMaxSize; ++position) {
            device.mappings[slot][position] = descriptor.mappings[slot][position];
        }
    }

    for (int slot = 0; slot < descriptor.simplex_count; ++slot) {
        int key = pool_key(descriptor.simplex_sizes[slot], descriptor.shared_counts[slot]);
        device.pool_offsets[slot] = pools[key].device_offset;
        device.pool_counts[slot] = static_cast<std::uint64_t>(pools[key].entries.size());
        device.selection_product = checked_mul(device.selection_product, device.pool_counts[slot]);
    }
    return device;
}

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

__device__ unsigned long long device_count_small_prefixes(const DeviceDescriptor &descriptor,
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

    for (int slot = 0; slot < descriptor.simplex_count; ++slot) {
        out->degree[slot] = entries[slot].degree;
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

__device__ void device_emit_small_prefixes(const DeviceDescriptor &descriptor,
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

std::uint64_t triangular_count(std::uint64_t count) {
    if ((count & 1ULL) == 0) return (count / 2ULL) * (count + 1ULL);
    return count * ((count + 1ULL) / 2ULL);
}

bool use_pair_scan(const DeviceDescriptor &descriptor) {
    return descriptor.simplex_count == 2 &&
           descriptor.family_groups[0] == descriptor.family_groups[1] &&
           descriptor.pool_offsets[0] == descriptor.pool_offsets[1] &&
           descriptor.pool_counts[0] == descriptor.pool_counts[1];
}

std::uint64_t descriptor_scan_space(const DeviceDescriptor &descriptor) {
    return use_pair_scan(descriptor)
        ? triangular_count(descriptor.pool_counts[0])
        : descriptor.selection_product;
}

std::uint64_t factorial_u64(int value) {
    std::uint64_t result = 1;
    for (int factor = 2; factor <= value; ++factor) result *= static_cast<std::uint64_t>(factor);
    return result;
}

std::uint64_t max_prefix_variants_per_selection(const DeviceDescriptor &descriptor) {
    std::uint64_t variants = 1;
    for (int slot = 1; slot < descriptor.simplex_count; ++slot) {
        variants = checked_mul(variants, factorial_u64(descriptor.shared_counts[slot]));
    }
    return std::max<std::uint64_t>(variants, 1);
}

void descriptor_shard_range(const DeviceDescriptor &descriptor,
                            const Config &config,
                            std::uint64_t *shard_start,
                            std::uint64_t *shard_count) {
    std::uint64_t scan_space = descriptor_scan_space(descriptor);
    std::uint64_t start =
        (scan_space * static_cast<std::uint64_t>(config.shard_index)) /
        static_cast<std::uint64_t>(config.shard_count);
    std::uint64_t end =
        (scan_space * static_cast<std::uint64_t>(config.shard_index + 1)) /
        static_cast<std::uint64_t>(config.shard_count);
    *shard_start = start;
    *shard_count = end - start;
}

__device__ int device_type3_prefix_is_canonical(const SelectedEntry &left,
                                                const int prefix[3]) {
    int block_start = 0;
    while (block_start + 1 < 3) {
        int block_end = block_start + 1;
        while (block_end < 3 && left.w[block_end] == left.w[block_start]) {
            if (prefix[block_end - 1] > prefix[block_end]) return 0;
            ++block_end;
        }
        block_start = block_end;
    }
    return 1;
}

__device__ int device_type3_prefix_count(const SelectedEntry &left,
                                         const SelectedEntry &right) {
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
        count += device_type3_prefix_is_canonical(left, prefix);
    }
    return count;
}

__device__ long long device_abs_ll(long long value) {
    return value < 0 ? -value : value;
}

__device__ long long device_nngcd(long long left, long long right) {
    left = device_abs_ll(left);
    right = device_abs_ll(right);
    if (!right) return left;
    while (left %= right) {
        if (!(right %= left)) return left;
    }
    return right;
}

__device__ long long device_egcd(long long a0, long long a1,
                                 long long *out0, long long *out1) {
    long long v0 = a0;
    long long v1 = a1;
    long long x0 = 1;
    long long x1 = 0;
    long long x2 = 0;
    long long a2 = 0;
    while ((a2 = a0 % a1)) {
        x2 = x0 - x1 * (a0 / a1);
        a0 = a1;
        a1 = a2;
        x0 = x1;
        x1 = x2;
    }
    *out0 = x1;
    *out1 = (a1 - v0 * x1) / v1;
    return a1;
}

__device__ long long device_round_q(long long numerator, long long denominator) {
    if (denominator < 0) {
        denominator = -denominator;
        numerator = -numerator;
    }
    long long floor_value = numerator / denominator;
    return floor_value + (2 * (numerator - floor_value * denominator)) / denominator;
}

__device__ long long device_pd_floor(long long numerator, long long denominator) {
    long long quotient = numerator / denominator;
    return quotient * denominator > numerator ? quotient - 1 : quotient;
}

// Exp B: 32-bit floored division for the point walk. For valid dim-5 CWS the
// coords, basis entries and bound sums fit well within int32 (max W5 degree
// 3486, weight 1743 => worst bound product ~3e7 << 2.1e9), so the walk can run
// entirely in 32-bit: half the per-thread state (the occupancy lever, per Exp C)
// and a cheaper emulated divide on sm_120 (no hardware integer divider).
__device__ int device_pd_floor32(int numerator, int denominator) {
    int quotient = numerator / denominator;
    return quotient * denominator > numerator ? quotient - 1 : quotient;
}

__device__ long long device_w_to_glz(long long *weights, int dim,
                                     long long glz[10][10]) {
    for (int row = 1; row < dim; ++row) {
        for (int col = 0; col < dim; ++col) glz[row][col] = 0;
    }
    long long *extended = glz[0];
    long long *base = glz[1];
    long long gcd = device_egcd(weights[0], weights[1], &extended[0], &extended[1]);
    base[0] = -weights[1] / gcd;
    base[1] = weights[0] / gcd;
    for (int row = 2; row < dim; ++row) {
        long long egcd_left = 0;
        long long egcd_right = 0;
        long long next_gcd = device_egcd(gcd, weights[row], &egcd_left, &egcd_right);
        base = glz[row];
        base[row] = gcd / next_gcd;
        long long scale = weights[row] / next_gcd;
        for (int col = 0; col < row; ++col) base[col] = -extended[col] * scale;
        for (int col = 0; col < row; ++col) extended[col] *= egcd_left;
        extended[row] = egcd_right;
        for (int improve = row - 1; improve > 0; --improve) {
            long long *candidate = glz[improve];
            long long round_base = device_round_q(base[improve], candidate[improve]);
            long long round_extended = device_round_q(extended[improve], candidate[improve]);
            for (int col = 0; col <= improve; ++col) {
                base[col] -= round_base * candidate[col];
                extended[col] -= round_extended * candidate[col];
            }
        }
        gcd = next_gcd;
    }
    return gcd;
}

__device__ int device_solve_next_weight_equation(const long long *next_weight,
                                                 int basis_coords,
                                                 int *out_dim,
                                                 long long out_basis[10][10]) {
    int positions[10];
    long long compact_weight[10];
    long long glz[10][10];
    int nonzero = 0;
    *out_dim = basis_coords - 1;
    for (int coord = 0; coord < basis_coords; ++coord) {
        for (int row = 0; row < *out_dim; ++row) out_basis[row][coord] = 0;
        if (next_weight[coord]) {
            positions[nonzero] = coord;
            compact_weight[nonzero] = next_weight[coord];
            ++nonzero;
        }
    }
    if (nonzero == 0) return 0;
    if (nonzero > 1) {
        device_w_to_glz(compact_weight, nonzero, glz);
    } else {
        for (int coord = 0; coord < positions[0]; ++coord) out_basis[coord][coord] = 1;
        for (int coord = positions[0] + 1; coord < basis_coords; ++coord) out_basis[coord - 1][coord] = 1;
        return 1;
    }
    for (int row = 1; row < nonzero; ++row) {
        if (glz[row][row] < 0) {
            for (int col = 0; col <= row; ++col) glz[row][col] = -glz[row][col];
        }
    }
    int scan = 0;
    for (; scan < positions[0]; ++scan) out_basis[scan][scan] = 1;
    while ((++scan) < positions[1]) out_basis[scan - 1][scan] = 1;
    out_basis[scan - 1][positions[0]] = glz[1][0];
    out_basis[scan - 1][positions[1]] = glz[1][1];
    int compact_row = 2;
    while (++scan < basis_coords) {
        if (next_weight[scan]) {
            long long *basis_row = out_basis[scan - 1];
            for (int col = 0; col <= compact_row; ++col) basis_row[positions[col]] = glz[compact_row][col];
            ++compact_row;
        } else {
            out_basis[scan - 1][scan] = 1;
        }
    }
    return 1;
}

__device__ int device_make_cws_basis(const DeviceCwsCandidate &candidate,
                                     int *basis_dim,
                                     long long basis[5][10]) {
    long long current[10][10];
    long long next_basis[10][10];
    long long composed[10][10];
    long long next_weight[10];
    int current_dim = 0;
    long long first_weight[10];
    for (int coord = 0; coord < candidate.ambient_vertices; ++coord) {
        first_weight[coord] = candidate.weights[0][coord];
    }
    if (!device_solve_next_weight_equation(first_weight, candidate.ambient_vertices, &current_dim, current)) {
        return 0;
    }
    for (int slot = 1; slot < candidate.nw; ++slot) {
        for (int row = 0; row < current_dim; ++row) {
            next_weight[row] = 0;
            for (int coord = 0; coord < candidate.ambient_vertices; ++coord) {
                next_weight[row] += static_cast<long long>(candidate.weights[slot][coord]) * current[row][coord];
            }
        }
        int reduced_dim = 0;
        if (!device_solve_next_weight_equation(next_weight, current_dim, &reduced_dim, next_basis)) return 0;
        for (int row = 0; row < reduced_dim; ++row) {
            for (int coord = 0; coord < candidate.ambient_vertices; ++coord) {
                composed[row][coord] = 0;
                for (int inner = 0; inner < current_dim; ++inner) {
                    composed[row][coord] += next_basis[row][inner] * current[inner][coord];
                }
            }
        }
        current_dim = reduced_dim;
        for (int row = 0; row < current_dim; ++row) {
            for (int coord = 0; coord < candidate.ambient_vertices; ++coord) current[row][coord] = composed[row][coord];
        }
    }
    if (current_dim != 5) return 0;
    *basis_dim = current_dim;
    for (int row = 0; row < 5; ++row) {
        for (int coord = 0; coord < candidate.ambient_vertices; ++coord) basis[row][coord] = current[row][coord];
    }
    return 1;
}

__device__ int device_candidate_basic_precheck(const DeviceCwsCandidate &candidate,
                                               long long x_upper[10]) {
    if (candidate.nw < 1 || candidate.nw > kMaxSlots) return 0;
    if (candidate.ambient_vertices < 1 || candidate.ambient_vertices > 10) return 0;
    if (candidate.ambient_vertices - candidate.nw != 5) return 0;

    for (int row = 0; row < candidate.nw; ++row) {
        long long sum = 0;
        for (int coord = 0; coord < candidate.ambient_vertices; ++coord) {
            int weight = candidate.weights[row][coord];
            if (weight < 0) return 0;
            sum += weight;
        }
        if (candidate.degree[row] <= 0 || sum != candidate.degree[row]) return 0;
    }

    unsigned long long point_upper_bound = 1;
    for (int coord = 0; coord < candidate.ambient_vertices; ++coord) {
        long long bound = 0;
        int support = 0;
        for (int row = 0; row < candidate.nw; ++row) {
            int weight = candidate.weights[row][coord];
            if (weight) {
                long long limit = candidate.degree[row] / weight;
                bound = support ? (bound < limit ? bound : limit) : limit;
                support = 1;
            }
        }
        if (!support) return 0;
        x_upper[coord] = bound;
        if (point_upper_bound < 6ULL) {
            unsigned long long choices = static_cast<unsigned long long>(bound + 1);
            point_upper_bound *= choices;
            if (point_upper_bound > 6ULL) point_upper_bound = 6ULL;
        }
    }
    return point_upper_bound >= 6ULL;
}

__device__ long long device_eval_eq(const DeviceEquation5 &equation,
                                    const long long *point) {
    return equation.c + equation.a[0] * point[0] + equation.a[1] * point[1] +
           equation.a[2] * point[2] + equation.a[3] * point[3] + equation.a[4] * point[4];
}

__device__ int device_vec_greater_than(const long long *left, const long long *right) {
    for (int coord = 4; coord >= 0; --coord) {
        if (left[coord] > right[coord]) return 1;
        if (left[coord] < right[coord]) return 0;
    }
    return 0;
}

__device__ int device_append_ip_point(const long long x[5], long long *points,
                                      int max_points, int *point_count) {
    int index = *point_count;
    if (index >= max_points) return 0;
    for (int coord = 0; coord < 5; ++coord) points[index * 5 + coord] = x[coord];
    *point_count = index + 1;
    return 1;
}

__device__ int device_make_points_serial(const DeviceCwsCandidate &candidate,
                                         long long *points,
                                         int max_points,
                                         int *point_count,
                                         unsigned long long *node_count = nullptr) {
    unsigned long long walk_nodes = 0;
    long long basis[5][10];
    int basis_dim = 0;
    long long x_upper64[10] = {0};
    if (!device_candidate_basic_precheck(candidate, x_upper64)) return 0;
    if (!device_make_cws_basis(candidate, &basis_dim, basis)) return 0;
    // Exp B: narrow basis + bounds to int32 for the walk. x0 is uniformly 1 here.
    const int nA = candidate.ambient_vertices;
    int b[5][10];
    int xu[10];
    for (int r = 0; r < basis_dim; ++r)
        for (int c = 0; c < nA; ++c) b[r][c] = static_cast<int>(basis[r][c]);
    for (int c = 0; c < nA; ++c) xu[c] = static_cast<int>(x_upper64[c]);
    int amin[6] = {0};
    int i = basis_dim;
    int j = nA;
    amin[0] = 0;
    amin[basis_dim] = nA;
    while (--i) {
        while (j > 0 && !b[i - 1][--j]) {}
        amin[i] = ++j;
    }
    int top_dim = basis_dim - 1;
    i = amin[top_dim + 1] - 1;
    int divisor = b[top_dim][i];
    int xmin[5] = {0};
    int xmax[5] = {0};
    int x[5] = {0};
    xmin[top_dim] = -device_pd_floor32(1, divisor);
    xmax[top_dim] = device_pd_floor32(xu[i] - 1, divisor);
    while ((i--) > amin[top_dim]) {
        int low = -1;
        int upper = low + xu[i];
        divisor = b[top_dim][i];
        if (divisor > 0) {
            int limit = device_pd_floor32(upper, divisor);
            if (xmax[top_dim] > limit) xmax[top_dim] = limit;
            limit = -device_pd_floor32(-low, divisor);
            if (xmin[top_dim] < limit) xmin[top_dim] = limit;
        } else {
            int limit = device_pd_floor32(-low, -divisor);
            if (xmax[top_dim] > limit) xmax[top_dim] = limit;
            limit = -device_pd_floor32(upper, -divisor);
            if (xmin[top_dim] < limit) xmin[top_dim] = limit;
        }
    }
    x[top_dim] = xmin[top_dim];
    int walk_dim = top_dim;
    *point_count = 0;
    while (walk_dim < basis_dim) {
        if (x[walk_dim] > xmax[walk_dim]) {
            ++walk_dim;
            if (basis_dim == walk_dim) break;
            ++x[walk_dim];
        } else {
            int source_coord = amin[walk_dim] - 1;
            --walk_dim;
            ++walk_nodes;
            int upper = xu[source_coord];
            int low = -1;
            int range_flag = 0;
            for (int k = walk_dim + 1; k < basis_dim; ++k) low -= x[k] * b[k][source_coord];
            upper += low;
            divisor = b[walk_dim][source_coord];
            xmin[walk_dim] = -device_pd_floor32(-low, divisor);
            xmax[walk_dim] = device_pd_floor32(upper, divisor);
            i = source_coord;
            while ((i--) > amin[walk_dim]) {
                divisor = b[walk_dim][i];
                if (divisor) {
                    low = -1;
                    upper = xu[i];
                    for (int k = walk_dim + 1; k < basis_dim; ++k) low -= x[k] * b[k][i];
                    upper += low;
                    if (divisor > 0) {
                        int limit = device_pd_floor32(upper, divisor);
                        if (xmax[walk_dim] > limit) xmax[walk_dim] = limit;
                        limit = -device_pd_floor32(-low, divisor);
                        if (xmin[walk_dim] < limit) xmin[walk_dim] = limit;
                    } else {
                        int limit = device_pd_floor32(-low, -divisor);
                        if (xmax[walk_dim] > limit) xmax[walk_dim] = limit;
                        limit = -device_pd_floor32(upper, -divisor);
                        if (xmin[walk_dim] < limit) xmin[walk_dim] = limit;
                    }
                } else {
                    int ambient = 1;
                    for (int k = walk_dim + 1; k < basis_dim; ++k) ambient += x[k] * b[k][i];
                    if (ambient < 0 || ambient > xu[i]) range_flag = 1;
                }
            }
            if (range_flag) ++x[++walk_dim];
            else x[walk_dim] = xmin[walk_dim];
            if (walk_dim == 0) {
                long long lp[5];
                while (x[0] <= xmax[0]) {
                    for (int c = 0; c < 5; ++c) lp[c] = x[c];
                    if (!device_append_ip_point(lp, points, max_points, point_count)) {
                        if (node_count) *node_count = walk_nodes;
                        return -1;
                    }
                    ++x[0];
                }
                walk_dim = 1;
                ++x[walk_dim];
            }
        }
    }
    if (node_count) *node_count = walk_nodes;
    return *point_count > 0 ? 1 : 0;
}

__device__ void device_append_ip_point_atomic(const long long x[5],
                                              long long *points,
                                              int max_points,
                                              int *point_count,
                                              int *overflow) {
    int index = atomicAdd(point_count, 1);
    if (index >= max_points) {
        atomicExch(overflow, 1);
        return;
    }
    for (int coord = 0; coord < 5; ++coord) points[index * 5 + coord] = x[coord];
}

__device__ void device_make_points_walk_seed(long long top_value,
                                             const long long basis[5][10],
                                             const long long x_upper[10],
                                             const int amin[6],
                                             long long *points,
                                             int max_points,
                                             int *point_count,
                                             int *overflow) {
    constexpr int basis_dim = 5;
    long long x0[10] = {0};
    for (int coord = 0; coord < 10; ++coord) x0[coord] = 1;

    long long xmin[5] = {0};
    long long xmax[5] = {0};
    long long x[5] = {0};
    int top_dim = basis_dim - 1;
    xmin[top_dim] = top_value;
    xmax[top_dim] = top_value;
    x[top_dim] = top_value;
    int walk_dim = top_dim;

    while (walk_dim < basis_dim && atomicAdd(overflow, 0) == 0) {
        if (x[walk_dim] > xmax[walk_dim]) {
            ++walk_dim;
            if (basis_dim == walk_dim) break;
            ++x[walk_dim];
        } else {
            int source_coord = amin[walk_dim] - 1;
            --walk_dim;
            long long upper = x_upper[source_coord];
            long long low = -x0[source_coord];
            int range_flag = 0;
            for (int k = walk_dim + 1; k < basis_dim; ++k) low -= x[k] * basis[k][source_coord];
            upper += low;
            long long divisor = basis[walk_dim][source_coord];
            xmin[walk_dim] = -device_pd_floor(-low, divisor);
            xmax[walk_dim] = device_pd_floor(upper, divisor);
            int i = source_coord;
            while ((i--) > amin[walk_dim]) {
                divisor = basis[walk_dim][i];
                if (divisor) {
                    low = -x0[i];
                    upper = x_upper[i];
                    for (int k = walk_dim + 1; k < basis_dim; ++k) low -= x[k] * basis[k][i];
                    upper += low;
                    if (divisor > 0) {
                        long long limit = device_pd_floor(upper, divisor);
                        if (xmax[walk_dim] > limit) xmax[walk_dim] = limit;
                        limit = -device_pd_floor(-low, divisor);
                        if (xmin[walk_dim] < limit) xmin[walk_dim] = limit;
                    } else {
                        long long limit = device_pd_floor(-low, -divisor);
                        if (xmax[walk_dim] > limit) xmax[walk_dim] = limit;
                        limit = -device_pd_floor(upper, -divisor);
                        if (xmin[walk_dim] < limit) xmin[walk_dim] = limit;
                    }
                } else {
                    long long ambient = 1;
                    for (int k = walk_dim + 1; k < basis_dim; ++k) ambient += x[k] * basis[k][i];
                    if (ambient < 0 || ambient > x_upper[i]) range_flag = 1;
                }
            }
            if (range_flag) ++x[++walk_dim];
            else x[walk_dim] = xmin[walk_dim];
            if (walk_dim == 0) {
                while (x[0] <= xmax[0] && atomicAdd(overflow, 0) == 0) {
                    device_append_ip_point_atomic(x, points, max_points, point_count, overflow);
                    ++x[0];
                }
                walk_dim = 1;
                ++x[walk_dim];
            }
        }
    }
}

__device__ int device_make_points_block(const DeviceCwsCandidate &candidate,
                                        long long *points,
                                        int max_points,
                                        int *out_point_count,
                                        int *precheck_failed) {
    __shared__ long long basis[5][10];
    __shared__ long long x_upper[10];
    __shared__ int amin[6];
    __shared__ long long top_min;
    __shared__ long long top_max;
    __shared__ int point_count;
    __shared__ int overflow;
    __shared__ int ok;

    if (threadIdx.x == 0) {
        point_count = 0;
        overflow = 0;
        ok = 1;
        *precheck_failed = 0;
        int basis_dim = 0;
        if (!device_candidate_basic_precheck(candidate, x_upper)) {
            ok = 0;
            *precheck_failed = 1;
        } else if (!device_make_cws_basis(candidate, &basis_dim, basis) || basis_dim != 5) {
            ok = 0;
        } else {
            int i = basis_dim;
            int j = candidate.ambient_vertices;
            amin[0] = 0;
            amin[basis_dim] = candidate.ambient_vertices;
            while (--i) {
                while (j > 0 && !basis[i - 1][--j]) {}
                amin[i] = ++j;
            }
            int top_dim = basis_dim - 1;
            i = amin[top_dim + 1] - 1;
            long long divisor = basis[top_dim][i];
            top_min = -device_pd_floor(1, divisor);
            top_max = device_pd_floor(x_upper[i] - 1, divisor);
            while ((i--) > amin[top_dim]) {
                long long low = -1;
                long long upper = low + x_upper[i];
                divisor = basis[top_dim][i];
                if (divisor > 0) {
                    long long limit = device_pd_floor(upper, divisor);
                    if (top_max > limit) top_max = limit;
                    limit = -device_pd_floor(-low, divisor);
                    if (top_min < limit) top_min = limit;
                } else {
                    long long limit = device_pd_floor(-low, -divisor);
                    if (top_max > limit) top_max = limit;
                    limit = -device_pd_floor(upper, -divisor);
                    if (top_min < limit) top_min = limit;
                }
            }
            if (top_max < top_min) ok = 0;
        }
    }
    __syncthreads();

    if (ok) {
        long long seed_count = top_max - top_min + 1;
        for (long long seed = threadIdx.x; seed < seed_count; seed += blockDim.x) {
            device_make_points_walk_seed(top_min + seed, basis, x_upper, amin,
                                         points, max_points, &point_count, &overflow);
        }
    }
    __syncthreads();

    if (threadIdx.x == 0) *out_point_count = point_count;
    __syncthreads();
    if (overflow) return -1;
    return ok && point_count > 0 ? 1 : 0;
}

__device__ int device_inci_abs(unsigned long long value) {
    return __popcll(value);
}

__device__ unsigned long long device_eq_to_inci(const DeviceEquation5 &equation,
                                                const long long *points,
                                                const int vertices[64],
                                                int vertex_count) {
    unsigned long long incidence = 0;
    for (int index = 0; index < vertex_count; ++index) {
        const long long *point = points + vertices[index] * 5;
        incidence = 2ULL * incidence + (device_eval_eq(equation, point) == 0 ? 1ULL : 0ULL);
    }
    return incidence;
}

__device__ long long device_vz_to_base(long long *vector, int dim,
                                       long long matrix[5][5]) {
    int positions[5];
    long long compact[5];
    long long *rows[5];
    long long glz[10][10];
    int nonzero = 0;
    long long gcd = 0;
    for (int row = 0; row < dim; ++row) {
        if (vector[row]) {
            compact[nonzero] = vector[row];
            positions[nonzero] = row;
            rows[nonzero] = matrix[row];
            ++nonzero;
        } else {
            for (int col = 0; col < dim; ++col) matrix[row][col] = (row == col);
        }
    }
    if (nonzero && positions[0]) {
        rows[0] = matrix[0];
        for (int col = 0; col < dim; ++col) matrix[positions[0]][col] = (col == 0);
    }
    if (nonzero > 1) {
        gcd = device_w_to_glz(compact, nonzero, glz);
        for (int row = 0; row < nonzero; ++row) {
            for (int col = 0; col < nonzero; ++col) rows[row][col] = glz[row][col];
        }
        for (int row = 0; row < nonzero; ++row) {
            int compact_col = nonzero;
            for (int col = dim - 1; col >= 0; --col) rows[row][col] = vector[col] ? rows[row][--compact_col] : 0;
        }
    } else if (nonzero) {
        gcd = compact[0];
        matrix[0][0] = 0;
        matrix[0][positions[0]] = 1;
    }
    return gcd;
}

__device__ int device_orthbase_red_by_v(long long *vector, int dim,
                                        long long a[5][5], int *rank,
                                        long long b[5][5]) {
    long long projected[5];
    long long glz_base[5][5];
    for (int row = 0; row < *rank; ++row) {
        projected[row] = 0;
        for (int col = 0; col < dim; ++col) projected[row] += a[row][col] * vector[col];
    }
    if (!device_vz_to_base(projected, *rank, glz_base)) return 0;
    for (int row = 0; row < *rank - 1; ++row) {
        for (int col = 0; col < dim; ++col) {
            b[row][col] = 0;
            for (int inner = 0; inner < *rank; ++inner) b[row][col] += glz_base[row + 1][inner] * a[inner][col];
        }
    }
    return (*rank)--;
}

__device__ int device_new_start_vertex(const long long *origin_vertex,
                                       long long *equation_a,
                                       const long long *points,
                                       int point_count,
                                       int *vertex_index) {
    DeviceEquation5 equation;
    for (int coord = 0; coord < 5; ++coord) equation.a[coord] = equation_a[coord];
    equation.c = -device_eval_eq(equation, origin_vertex);
    long long value = device_eval_eq(equation, points);
    long long positive = value > 0 ? value : 0;
    long long negative = value < 0 ? value : 0;
    int positive_index = 0;
    int negative_index = 0;
    for (int index = 1; index < point_count; ++index) {
        const long long *point = points + index * 5;
        value = device_eval_eq(equation, point);
        if (value == 0) continue;
        if (value == positive && device_vec_greater_than(point, points + positive_index * 5)) positive_index = index;
        if (value > positive) { positive = value; positive_index = index; }
        if (value == negative && device_vec_greater_than(point, points + negative_index * 5)) negative_index = index;
        if (value < negative) { negative = value; negative_index = index; }
    }
    if (positive) {
        if (negative) *vertex_index = (positive + negative > 0) ? negative_index : positive_index;
        else *vertex_index = positive_index;
    } else if (negative) {
        *vertex_index = negative_index;
    } else {
        return 0;
    }
    return 1;
}

__device__ int device_glz_start_simplex(const long long *points,
                                        int point_count,
                                        int vertices[64],
                                        int *vertex_count,
                                        DeviceCEqList5 *candidate_equations) {
    if (point_count < 2) return 1;
    int min_index = 0;
    int max_index = 0;
    for (int index = 1; index < point_count; ++index) {
        if (device_vec_greater_than(points + min_index * 5, points + index * 5)) min_index = index;
        if (device_vec_greater_than(points + index * 5, points + max_index * 5)) max_index = index;
    }
    if (min_index == max_index) return 1;
    long long min_norm = 0;
    long long max_norm = 0;
    for (int coord = 0; coord < 5; ++coord) {
        long long min_coord_norm = device_abs_ll(points[min_index * 5 + coord]);
        long long max_coord_norm = device_abs_ll(points[max_index * 5 + coord]);
        min_norm = min_norm > min_coord_norm ? min_norm : min_coord_norm;
        max_norm = max_norm > max_coord_norm ? max_norm : max_coord_norm;
    }
    if (max_norm < min_norm) { vertices[0] = max_index; vertices[1] = min_index; }
    else { vertices[0] = min_index; vertices[1] = max_index; }
    *vertex_count = 2;
    const long long *base_vertex = points + vertices[0] * 5;
    int b_offset[5];
    long long basis_store[15][5];
    long long work_vector[5];
    int rank = 5;
    for (int index = 0; index < 5; ++index) b_offset[index] = (index * (2 * 5 - index + 1)) / 2;
    for (int row = 0; row < 5; ++row) for (int col = 0; col < 5; ++col) basis_store[row][col] = (row == col);
    int y = vertices[1];
    int depth = 0;
    for (depth = 1; depth < 5; ++depth) {
        for (int coord = 0; coord < 5; ++coord) work_vector[coord] = points[y * 5 + coord] - base_vertex[coord];
        long long (*previous)[5] = &basis_store[b_offset[depth - 1]];
        long long (*next)[5] = &basis_store[b_offset[depth]];
        if (!device_orthbase_red_by_v(work_vector, 5, previous, &rank, next)) break;
        int found = 0;
        for (int row = 0; row < rank; ++row) {
            if (device_new_start_vertex(base_vertex, next[row], points, point_count, &y)) { found = 1; break; }
        }
        if (!found) break;
        vertices[(*vertex_count)++] = y;
    }
    if (depth < 5) {
        candidate_equations->ne = rank;
        for (int eq = 0; eq < rank; ++eq) {
            DeviceEquation5 *out = &candidate_equations->e[eq];
            out->c = 0;
            for (int coord = 0; coord < 5; ++coord) out->a[coord] = basis_store[b_offset[depth] + eq][coord];
            out->c = -device_eval_eq(*out, base_vertex);
        }
        return rank;
    }
    candidate_equations->ne = 6;
    long long *z = basis_store[b_offset[4]];
    DeviceEquation5 *equation = &candidate_equations->e[0];
    equation->c = 0;
    for (int coord = 0; coord < 5; ++coord) equation->a[coord] = z[coord];
    equation->c = -device_eval_eq(*equation, base_vertex);
    if (device_eval_eq(*equation, points + vertices[5] * 5) < 0) {
        for (int coord = 0; coord < 5; ++coord) equation->a[coord] = -equation->a[coord];
        equation->c = -equation->c;
    }
    const long long *opposite = points + vertices[5] * 5;
    rank = 5;
    for (int face = 1; face < 5; ++face) {
        const long long *other = points + vertices[face - 1] * 5;
        for (int coord = 0; coord < 5; ++coord) work_vector[coord] = opposite[coord] - other[coord];
        long long (*previous)[5] = &basis_store[b_offset[face - 1]];
        long long (*next)[5] = &basis_store[b_offset[face]];
        device_orthbase_red_by_v(work_vector, 5, previous, &rank, next);
    }
    equation = &candidate_equations->e[1];
    equation->c = 0;
    for (int coord = 0; coord < 5; ++coord) equation->a[coord] = z[coord];
    equation->c = -device_eval_eq(*equation, opposite);
    if (device_eval_eq(*equation, points + vertices[4] * 5) < 0) {
        for (int coord = 0; coord < 5; ++coord) equation->a[coord] = -equation->a[coord];
        equation->c = -equation->c;
    }
    int eq_count = 2;
    for (int omitted = 3; omitted >= 0; --omitted) {
        rank = 5 - omitted;
        for (int face = omitted + 1; face < 5; ++face) {
            const long long *other = points + vertices[face] * 5;
            for (int coord = 0; coord < 5; ++coord) work_vector[coord] = opposite[coord] - other[coord];
            long long (*previous)[5] = &basis_store[b_offset[face - 1]];
            long long (*next)[5] = &basis_store[b_offset[face]];
            device_orthbase_red_by_v(work_vector, 5, previous, &rank, next);
        }
        equation = &candidate_equations->e[eq_count++];
        equation->c = 0;
        for (int coord = 0; coord < 5; ++coord) equation->a[coord] = z[coord];
        equation->c = -device_eval_eq(*equation, opposite);
        if (device_eval_eq(*equation, points + vertices[omitted] * 5) < 0) {
            for (int coord = 0; coord < 5; ++coord) equation->a[coord] = -equation->a[coord];
            equation->c = -equation->c;
        }
    }
    return 0;
}

__device__ DeviceEquation5 device_eev_to_equation(const DeviceEquation5 &left,
                                                  const DeviceEquation5 &right,
                                                  const long long *vertex) {
    long long l = device_eval_eq(right, vertex);
    long long m = device_eval_eq(left, vertex);
    long long gcd = device_nngcd(l, m);
    DeviceEquation5 equation;
    if (!gcd) return equation;
    l /= gcd;
    m /= gcd;
    long long all_gcd = equation.c = l * left.c - m * right.c;
    for (int coord = 0; coord < 5; ++coord) {
        equation.a[coord] = l * left.a[coord] - m * right.a[coord];
        all_gcd = device_nngcd(all_gcd, equation.a[coord]);
    }
    if (all_gcd && all_gcd != 1) {
        equation.c /= all_gcd;
        for (int coord = 0; coord < 5; ++coord) equation.a[coord] /= all_gcd;
    }
    return equation;
}

__device__ int device_is_good_ceq(DeviceEquation5 *equation,
                                  const long long *points,
                                  const int vertices[64],
                                  int vertex_count) {
    int index = vertex_count;
    long long sign = 0;
    while (index > 0 && !(sign = device_eval_eq(*equation, points + vertices[--index] * 5))) {}
    if (sign < 0) {
        for (int coord = 0; coord < 5; ++coord) equation->a[coord] = -equation->a[coord];
        equation->c = -equation->c;
    }
    while (index > 0) {
        if (device_eval_eq(*equation, points + vertices[--index] * 5) < 0) return 0;
    }
    return 1;
}

__device__ int device_search_new_vertex(const DeviceEquation5 &equation,
                                        const long long *points,
                                        int point_count) {
    int vertex = 0;
    long long best = device_eval_eq(equation, points);
    for (int index = 1; index < point_count; ++index) {
        const long long *point = points + index * 5;
        long long value = device_eval_eq(equation, point);
        if (value > best) continue;
        if (value == best && device_vec_greater_than(points + vertex * 5, point)) continue;
        vertex = index;
        best = value;
    }
    return vertex;
}

__device__ void device_make_new_ceqs(const long long *points,
                                     const int vertices[64],
                                     int vertex_count,
                                     DeviceCEqList5 *candidate_equations,
                                     DeviceEqList5 *facets,
                                     unsigned long long ceq_inci[64],
                                     unsigned long long facet_inci[64],
                                     DeviceCEqList5 *bad,
                                     unsigned long long bad_inci[64]) {
    int old_count = candidate_equations->ne;
    candidate_equations->ne = 0;
    bad->ne = 0;
    for (int index = 0; index < old_count; ++index) {
        long long dist = device_eval_eq(candidate_equations->e[index], points + vertices[vertex_count - 1] * 5);
        ceq_inci[index] = 2ULL * ceq_inci[index] + (dist == 0 ? 1ULL : 0ULL);
        if (dist < 0) {
            bad->e[bad->ne] = candidate_equations->e[index];
            bad_inci[bad->ne++] = ceq_inci[index];
        } else {
            ceq_inci[candidate_equations->ne] = ceq_inci[index];
            candidate_equations->e[candidate_equations->ne++] = candidate_equations->e[index];
        }
    }
    old_count = candidate_equations->ne;
    for (int index = 0; index < facets->ne; ++index) {
        long long dist = device_eval_eq(facets->e[index], points + vertices[vertex_count - 1] * 5);
        facet_inci[index] = 2ULL * facet_inci[index] + (dist == 0 ? 1ULL : 0ULL);
    }
    for (int facet = 0; facet < facets->ne; ++facet) {
        if ((facet_inci[facet] & 1ULL) != 0) continue;
        for (int bad_index = 0; bad_index < bad->ne; ++bad_index) {
            unsigned long long new_face = bad_inci[bad_index] & facet_inci[facet];
            if (device_inci_abs(new_face) < 4) continue;
            int skip = 0;
            for (int k = 0; k < bad->ne; ++k) if (((new_face | bad_inci[k]) == bad_inci[k]) && k != bad_index) { skip = 1; break; }
            if (skip) continue;
            for (int k = 0; k < old_count; ++k) if ((new_face | ceq_inci[k]) == ceq_inci[k]) { skip = 1; break; }
            if (skip) continue;
            for (int k = 0; k < facets->ne; ++k) if (((new_face | facet_inci[k]) == facet_inci[k]) && k != facet) { skip = 1; break; }
            if (skip || candidate_equations->ne >= 64) continue;
            ceq_inci[candidate_equations->ne] = new_face | 1ULL;
            DeviceEquation5 equation = device_eev_to_equation(bad->e[bad_index], facets->e[facet], points + vertices[vertex_count - 1] * 5);
            if (device_is_good_ceq(&equation, points, vertices, vertex_count)) candidate_equations->e[candidate_equations->ne++] = equation;
        }
    }
    for (int old = 0; old < old_count; ++old) {
        if ((ceq_inci[old] & 1ULL) != 0) continue;
        for (int bad_index = bad->ne - 1; bad_index >= 0; --bad_index) {
            unsigned long long new_face = bad_inci[bad_index] & ceq_inci[old];
            if (device_inci_abs(new_face) < 4) continue;
            int skip = 0;
            for (int k = 0; k < bad->ne; ++k) if (((new_face | bad_inci[k]) == bad_inci[k]) && k != bad_index) { skip = 1; break; }
            if (skip) continue;
            for (int k = 0; k < old_count; ++k) if (((new_face | ceq_inci[k]) == ceq_inci[k]) && k != old) { skip = 1; break; }
            if (skip) continue;
            for (int k = 0; k < facets->ne; ++k) if ((new_face | facet_inci[k]) == facet_inci[k]) { skip = 1; break; }
            if (skip || candidate_equations->ne >= 64) continue;
            ceq_inci[candidate_equations->ne] = new_face | 1ULL;
            DeviceEquation5 equation = device_eev_to_equation(bad->e[bad_index], candidate_equations->e[old], points + vertices[vertex_count - 1] * 5);
            if (device_is_good_ceq(&equation, points, vertices, vertex_count)) candidate_equations->e[candidate_equations->ne++] = equation;
        }
    }
}

__device__ int device_ip_search_bad_eq(DeviceCEqList5 *candidate_equations,
                                       DeviceEqList5 *facets,
                                       unsigned long long ceq_inci[64],
                                       unsigned long long facet_inci[64],
                                       const long long *points,
                                       int point_count,
                                       int *ip) {
    while (candidate_equations->ne--) {
        int eq_index = candidate_equations->ne;
        int bad = 0;
        for (int point_index = 0; point_index < point_count; ++point_index) {
            if (device_eval_eq(candidate_equations->e[eq_index], points + point_index * 5) < 0) { bad = 1; break; }
        }
        if (bad) return ++candidate_equations->ne;
        if (candidate_equations->e[eq_index].c < 1) {
            *ip = 0;
            return 1;
        }
        if (facets->ne < 64) {
            facets->e[facets->ne] = candidate_equations->e[eq_index];
            facet_inci[facets->ne++] = ceq_inci[eq_index];
        }
    }
    return 0;
}

__device__ int device_block_equation_has_negative(const DeviceEquation5 &equation,
                                                  const long long *points,
                                                  int point_count) {
    __shared__ int has_negative;
    if (threadIdx.x == 0) has_negative = 0;
    __syncthreads();
    for (int point_index = threadIdx.x; point_index < point_count; point_index += blockDim.x) {
        if (device_eval_eq(equation, points + point_index * 5) < 0) atomicExch(&has_negative, 1);
    }
    __syncthreads();
    return has_negative;
}

__device__ int device_search_new_vertex_block(const DeviceEquation5 &equation,
                                              const long long *points,
                                              int point_count) {
    __shared__ long long shared_value[1024];
    __shared__ int shared_index[1024];
    int local_index = -1;
    long long local_value = 0;
    for (int point_index = threadIdx.x; point_index < point_count; point_index += blockDim.x) {
        const long long *point = points + point_index * 5;
        long long value = device_eval_eq(equation, point);
        if (local_index < 0 || value < local_value ||
            (value == local_value && device_vec_greater_than(point, points + local_index * 5))) {
            local_index = point_index;
            local_value = value;
        }
    }
    shared_value[threadIdx.x] = local_value;
    shared_index[threadIdx.x] = local_index;
    __syncthreads();
    for (int stride = blockDim.x / 2; stride > 0; stride >>= 1) {
        if (threadIdx.x < stride) {
            int other_index = shared_index[threadIdx.x + stride];
            if (other_index >= 0) {
                long long other_value = shared_value[threadIdx.x + stride];
                int current_index = shared_index[threadIdx.x];
                if (current_index < 0 || other_value < shared_value[threadIdx.x] ||
                    (other_value == shared_value[threadIdx.x] &&
                     device_vec_greater_than(points + other_index * 5, points + current_index * 5))) {
                    shared_value[threadIdx.x] = other_value;
                    shared_index[threadIdx.x] = other_index;
                }
            }
        }
        __syncthreads();
    }
    return shared_index[0];
}

__device__ int device_ip_search_bad_eq_block(DeviceCEqList5 *candidate_equations,
                                             DeviceEqList5 *facets,
                                             unsigned long long ceq_inci[64],
                                             unsigned long long facet_inci[64],
                                             const long long *points,
                                             int point_count,
                                             int *ip) {
    __shared__ int eq_index;
    __shared__ int done;
    __shared__ int result;
    while (true) {
        if (threadIdx.x == 0) {
            if (candidate_equations->ne <= 0) {
                candidate_equations->ne = -1;
                done = 1;
                result = 0;
                eq_index = -1;
            } else {
                eq_index = --candidate_equations->ne;
                done = 0;
                result = 0;
            }
        }
        __syncthreads();
        if (done) return result;

        int bad = device_block_equation_has_negative(candidate_equations->e[eq_index], points, point_count);
        if (threadIdx.x == 0) {
            if (bad) {
                result = ++candidate_equations->ne;
                done = 1;
            } else if (candidate_equations->e[eq_index].c < 1) {
                *ip = 0;
                result = 1;
                done = 1;
            } else {
                if (facets->ne < 64) {
                    facets->e[facets->ne] = candidate_equations->e[eq_index];
                    facet_inci[facets->ne++] = ceq_inci[eq_index];
                }
            }
        }
        __syncthreads();
        if (done) return result;
    }
}

__device__ int device_ip_check(const long long *points,
                               int point_count,
                               DeviceIpScratch *scratch,
                               int *reject_reason,
                               DeviceIpStageStats *stage_stats) {
    int *vertices = scratch->vertices;
    int vertex_count = 0;
    DeviceCEqList5 *candidate_equations = &scratch->candidate_equations;
    DeviceEqList5 *facets = &scratch->facets;
    unsigned long long *ceq_inci = scratch->ceq_inci;
    unsigned long long *facet_inci = scratch->facet_inci;
    unsigned long long tick = stage_stats ? clock64() : 0ULL;
    if (device_glz_start_simplex(points, point_count, vertices, &vertex_count, candidate_equations)) {
        if (stage_stats) atomicAdd(&stage_stats->glz_cycles, clock64() - tick);
        *reject_reason = 1;
        return 0;
    }
    if (stage_stats) {
        unsigned long long now = clock64();
        atomicAdd(&stage_stats->glz_cycles, now - tick);
        tick = now;
    }
    for (int index = 0; index < candidate_equations->ne; ++index) {
        ceq_inci[index] = device_eq_to_inci(candidate_equations->e[index], points, vertices, vertex_count);
        if (device_inci_abs(ceq_inci[index]) < 5) {
            if (stage_stats) atomicAdd(&stage_stats->initial_inci_cycles, clock64() - tick);
            *reject_reason = 2;
            return 0;
        }
    }
    if (stage_stats) {
        unsigned long long now = clock64();
        atomicAdd(&stage_stats->initial_inci_cycles, now - tick);
        tick = now;
    }
    facets->ne = 0;
    int ip = 1;
    while (candidate_equations->ne >= 0) {
        if (stage_stats) tick = clock64();
        if (device_ip_search_bad_eq(candidate_equations, facets, ceq_inci, facet_inci, points, point_count, &ip)) {
            if (stage_stats) {
                unsigned long long now = clock64();
                atomicAdd(&stage_stats->search_bad_eq_cycles, now - tick);
                atomicAdd(&stage_stats->search_bad_eq_calls, 1ULL);
                tick = now;
            }
            if (!ip) {
                *reject_reason = 4;
                return 0;
            }
            if (vertex_count >= 64) {
                *reject_reason = 3;
                return 0;
            }
            if (stage_stats) tick = clock64();
            vertices[vertex_count++] = device_search_new_vertex(candidate_equations->e[candidate_equations->ne - 1], points, point_count);
            if (stage_stats) {
                unsigned long long now = clock64();
                atomicAdd(&stage_stats->search_new_vertex_cycles, now - tick);
                atomicAdd(&stage_stats->search_new_vertex_calls, 1ULL);
                tick = now;
            }
            device_make_new_ceqs(points, vertices, vertex_count, candidate_equations,
                                 facets, ceq_inci, facet_inci,
                                 &scratch->bad_equations, scratch->bad_inci);
            if (stage_stats) {
                atomicAdd(&stage_stats->make_new_ceqs_cycles, clock64() - tick);
                atomicAdd(&stage_stats->make_new_ceqs_calls, 1ULL);
            }
        } else if (stage_stats) {
            atomicAdd(&stage_stats->search_bad_eq_cycles, clock64() - tick);
            atomicAdd(&stage_stats->search_bad_eq_calls, 1ULL);
        }
    }
    return 1;
}

__device__ int device_ip_check_block(const long long *points,
                                     int point_count,
                                     DeviceIpScratch *scratch,
                                     int *reject_reason) {
    __shared__ int vertex_count;
    __shared__ int done;
    __shared__ int result;
    __shared__ int shared_reject_reason;
    int *vertices = scratch->vertices;
    DeviceCEqList5 *candidate_equations = &scratch->candidate_equations;
    DeviceEqList5 *facets = &scratch->facets;
    unsigned long long *ceq_inci = scratch->ceq_inci;
    unsigned long long *facet_inci = scratch->facet_inci;

    if (threadIdx.x == 0) {
        vertex_count = 0;
        done = 0;
        result = 0;
        shared_reject_reason = 0;
        if (device_glz_start_simplex(points, point_count, vertices, &vertex_count, candidate_equations)) {
            shared_reject_reason = 1;
            done = 1;
        } else {
            for (int index = 0; index < candidate_equations->ne; ++index) {
                ceq_inci[index] = device_eq_to_inci(candidate_equations->e[index], points, vertices, vertex_count);
                if (device_inci_abs(ceq_inci[index]) < 5) {
                    shared_reject_reason = 2;
                    done = 1;
                    break;
                }
            }
            facets->ne = 0;
        }
    }
    __syncthreads();
    if (done) {
        if (threadIdx.x == 0) *reject_reason = shared_reject_reason;
        return 0;
    }

    __shared__ int ip;
    if (threadIdx.x == 0) ip = 1;
    __syncthreads();

    while (true) {
        if (threadIdx.x == 0) done = candidate_equations->ne < 0;
        __syncthreads();
        if (done) {
            if (threadIdx.x == 0) *reject_reason = 0;
            return 1;
        }

        int found_bad = device_ip_search_bad_eq_block(candidate_equations, facets, ceq_inci,
                                                      facet_inci, points, point_count, &ip);
        if (found_bad) {
            if (threadIdx.x == 0) {
                if (!ip) {
                    shared_reject_reason = 4;
                    done = 1;
                    result = 0;
                } else if (vertex_count >= 64) {
                    shared_reject_reason = 3;
                    done = 1;
                    result = 0;
                } else {
                    done = 0;
                }
            }
            __syncthreads();
            if (done) {
                if (threadIdx.x == 0) *reject_reason = shared_reject_reason;
                return result;
            }

            int new_vertex = device_search_new_vertex_block(candidate_equations->e[candidate_equations->ne - 1],
                                                            points, point_count);
            if (threadIdx.x == 0) {
                vertices[vertex_count++] = new_vertex;
                device_make_new_ceqs(points, vertices, vertex_count, candidate_equations,
                                     facets, ceq_inci, facet_inci,
                                     &scratch->bad_equations, scratch->bad_inci);
            }
            __syncthreads();
        }
    }
}

__global__ void cws_ip_filter_kernel(const DeviceCwsCandidate *candidates,
                                     unsigned long long candidate_count,
                                     int max_points_per_candidate,
                                     long long *point_workspace,
                                     DeviceIpScratch *scratch_workspace,
                                     DeviceIpStats *stats,
                                     DeviceIpStageStats *stage_stats,
                                     DeviceCwsCandidate *accepted_output,
                                     unsigned long long accepted_capacity) {
    unsigned long long index = blockIdx.x * blockDim.x + threadIdx.x;
    unsigned long long workspace_slot = index;
    unsigned long long stride = blockDim.x * gridDim.x;
    while (index < candidate_count) {
        const DeviceCwsCandidate &candidate = candidates[index];
        long long *points = point_workspace + workspace_slot * static_cast<unsigned long long>(max_points_per_candidate) * 5ULL;
        DeviceIpScratch *scratch = scratch_workspace + workspace_slot;
        int point_count = 0;
        unsigned long long tick = stage_stats ? clock64() : 0ULL;
        int point_status = device_make_points_serial(candidate, points, max_points_per_candidate, &point_count);
        if (stage_stats) {
            atomicAdd(&stage_stats->point_cycles, clock64() - tick);
            atomicAdd(&stage_stats->point_candidates, 1ULL);
            atomicAdd(&stage_stats->total_points, static_cast<unsigned long long>(point_count > 0 ? point_count : 0));
            if (point_count > 0) atomicMax(&stage_stats->max_points, static_cast<unsigned long long>(point_count));
        }
        atomicAdd(&stats->processed, 1ULL);
        if (point_status < 0) {
            atomicAdd(&stats->point_overflow, 1ULL);
        } else if (point_status == 0 || point_count <= 0) {
            atomicAdd(&stats->point_fail, 1ULL);
        } else if (point_count < 6) {
            atomicAdd(&stats->simplex_fail, 1ULL);
        } else {
            int reject_reason = 0;
            if (stage_stats) tick = clock64();
            int is_ip = device_ip_check(points, point_count, scratch, &reject_reason, stage_stats);
            if (stage_stats) {
                atomicAdd(&stage_stats->ip_cycles, clock64() - tick);
                atomicAdd(&stage_stats->ip_candidates, 1ULL);
            }
            if (!is_ip && reject_reason == 1) atomicAdd(&stats->simplex_fail, 1ULL);
            else if (!is_ip && reject_reason == 2) atomicAdd(&stats->initial_inci_fail, 1ULL);
            else if (!is_ip && reject_reason == 3) atomicAdd(&stats->vertex_overflow, 1ULL);
            else if (!is_ip) atomicAdd(&stats->ip_reject, 1ULL);
            if (is_ip) {
                unsigned long long accepted_index = atomicAdd(&stats->ip_count, 1ULL);
                if (accepted_index < accepted_capacity && accepted_output) {
                    accepted_output[accepted_index] = candidate;
                    atomicAdd(&stats->accepted_stored_count, 1ULL);
                }
            }
        }
        index += stride;
    }
}

__global__ void cws_ip_filter_block_kernel(const DeviceCwsCandidate *candidates,
                                           unsigned long long candidate_count,
                                           int max_points_per_candidate,
                                           long long *point_workspace,
                                           DeviceIpScratch *scratch_workspace,
                                           DeviceIpStats *stats,
                                           DeviceIpStageStats *stage_stats,
                                           DeviceCwsCandidate *accepted_output,
                                           unsigned long long accepted_capacity) {
    __shared__ int block_point_count;
    __shared__ int block_point_status;
    __shared__ int block_precheck_failed;
    unsigned long long workspace_slot = blockIdx.x;
    unsigned long long index = blockIdx.x;
    while (index < candidate_count) {
        const DeviceCwsCandidate &candidate = candidates[index];
        long long *points = point_workspace + workspace_slot * static_cast<unsigned long long>(max_points_per_candidate) * 5ULL;
        DeviceIpScratch *scratch = scratch_workspace + workspace_slot;
        int point_count = 0;
        int precheck_failed = 0;
        unsigned long long tick = (stage_stats && threadIdx.x == 0) ? clock64() : 0ULL;
        int point_status = device_make_points_block(candidate, points, max_points_per_candidate,
                                                    &point_count, &precheck_failed);
        if (threadIdx.x == 0) {
            block_point_count = point_count;
            block_point_status = point_status;
            block_precheck_failed = precheck_failed;
            if (stage_stats) {
                atomicAdd(&stage_stats->point_cycles, clock64() - tick);
                atomicAdd(&stage_stats->point_candidates, 1ULL);
                atomicAdd(&stage_stats->total_points, static_cast<unsigned long long>(point_count > 0 ? point_count : 0));
                if (point_count > 0) atomicMax(&stage_stats->max_points, static_cast<unsigned long long>(point_count));
            }
            atomicAdd(&stats->processed, 1ULL);
            if (block_precheck_failed) atomicAdd(&stats->precheck_fail, 1ULL);
            if (block_point_status < 0) atomicAdd(&stats->point_overflow, 1ULL);
            else if (block_point_status == 0 || block_point_count <= 0) atomicAdd(&stats->point_fail, 1ULL);
            else if (block_point_count < 6) atomicAdd(&stats->simplex_fail, 1ULL);
        }
        __syncthreads();

        if (block_point_status > 0 && block_point_count >= 6) {
            int reject_reason = 0;
            if (threadIdx.x == 0 && stage_stats) tick = clock64();
            int is_ip = device_ip_check_block(points, block_point_count, scratch, &reject_reason);
            if (threadIdx.x == 0) {
                if (stage_stats) {
                    atomicAdd(&stage_stats->ip_cycles, clock64() - tick);
                    atomicAdd(&stage_stats->ip_candidates, 1ULL);
                }
                if (!is_ip && reject_reason == 1) atomicAdd(&stats->simplex_fail, 1ULL);
                else if (!is_ip && reject_reason == 2) atomicAdd(&stats->initial_inci_fail, 1ULL);
                else if (!is_ip && reject_reason == 3) atomicAdd(&stats->vertex_overflow, 1ULL);
                else if (!is_ip) atomicAdd(&stats->ip_reject, 1ULL);
                if (is_ip) {
                    unsigned long long accepted_index = atomicAdd(&stats->ip_count, 1ULL);
                    if (accepted_index < accepted_capacity && accepted_output) {
                        accepted_output[accepted_index] = candidate;
                        atomicAdd(&stats->accepted_stored_count, 1ULL);
                    }
                }
            }
        }
        __syncthreads();
        index += gridDim.x;
    }
}

// ── Split bucketed IP pipeline ────────────────────────────────────────────
// np_out[] values written by point_enum_kernel:
//   >= 6           valid candidate: lattice-point count (6..np_cap), IP-checked
//   1..5           degenerate (simplex_fail): too few points for a 5D simplex
//   0              point_fail: precheck/basis failed or produced no points
//   kNpOverflow    candidate exceeds np_cap; shipped to the CPU for completeness
static constexpr int kNpOverflow = -1;

// Kernel 1: lean point enumeration. One thread per candidate (grid-stride).
// Each candidate's points go into a COMPACT slot (points + index*np_cap*5), so
// device memory scales with the candidate count rather than the launch grid --
// the grid can therefore cover every SM (no VRAM cap). The kernel carries no IP
// scratch, keeping its register footprint (and thus occupancy) low. With a small
// np_cap the lattice walk is also bounded to ~np_cap iterations, which removes
// most of the heavy-np-tail warp divergence for free. Candidates that exceed
// np_cap are recorded (and copied out) for CPU (PALP) completion -- never dropped.
__global__ void point_enum_kernel(const DeviceCwsCandidate *candidates,
                                  unsigned long long candidate_count,
                                  int np_cap,
                                  long long *points,
                                  int *np_out,
                                  DeviceIpStats *stats,
                                  DeviceIpStageStats *stage_stats,
                                  DeviceCwsCandidate *overflow_output,
                                  unsigned long long *overflow_count,
                                  unsigned long long overflow_capacity) {
    unsigned long long index = blockIdx.x * blockDim.x + threadIdx.x;
    unsigned long long stride = blockDim.x * gridDim.x;
    while (index < candidate_count) {
        const DeviceCwsCandidate &candidate = candidates[index];
        long long *slot = points + index * static_cast<unsigned long long>(np_cap) * 5ULL;
        int np = 0;
        unsigned long long walk_nodes = 0;
        unsigned long long tick = stage_stats ? clock64() : 0ULL;
        int status = device_make_points_serial(candidate, slot, np_cap, &np,
                                               stage_stats ? &walk_nodes : nullptr);
        if (stage_stats) {
            atomicAdd(&stage_stats->point_cycles, clock64() - tick);
            atomicAdd(&stage_stats->point_candidates, 1ULL);
            atomicAdd(&stage_stats->walk_nodes, walk_nodes);
            atomicAdd(&stage_stats->total_points,
                      static_cast<unsigned long long>(np > 0 ? np : 0));
            if (np > 0)
                atomicMax(&stage_stats->max_points, static_cast<unsigned long long>(np));
        }
        atomicAdd(&stats->processed, 1ULL);
        if (status < 0) {
            np_out[index] = kNpOverflow;
            atomicAdd(&stats->point_overflow, 1ULL);
            unsigned long long slot_index = atomicAdd(overflow_count, 1ULL);
            if (slot_index < overflow_capacity && overflow_output)
                overflow_output[slot_index] = candidate;
        } else if (status == 0) {
            np_out[index] = 0;
            atomicAdd(&stats->point_fail, 1ULL);
        } else {
            np_out[index] = np;
            if (np < 6) atomicAdd(&stats->simplex_fail, 1ULL);
        }
        index += stride;
    }
}

// Exp E: per-candidate box-volume proxy = Σ bit-length(x_upper[c]) ≈ log2(box
// volume). Cheap (precheck only, no walk). Sorting candidates by this key makes a
// warp's 32 lanes run near-equal-length walks → less intra-warp divergence.
__global__ void vol_key_kernel(const DeviceCwsCandidate *candidates,
                               unsigned long long candidate_count,
                               int *keys) {
    unsigned long long index = blockIdx.x * blockDim.x + threadIdx.x;
    unsigned long long stride = blockDim.x * gridDim.x;
    while (index < candidate_count) {
        long long x_upper[10] = {0};
        int key = 0;
        if (device_candidate_basic_precheck(candidates[index], x_upper)) {
            for (int c = 0; c < candidates[index].ambient_vertices; ++c) {
                long long v = x_upper[c];
                while (v > 0) { ++key; v >>= 1; }
            }
        } else {
            key = -1;  // invalid candidates cluster together (skipped fast)
        }
        keys[index] = key;
        index += stride;
    }
}

__global__ void gather_candidates_kernel(const DeviceCwsCandidate *src,
                                         const int *perm,
                                         unsigned long long count,
                                         DeviceCwsCandidate *dst) {
    unsigned long long index = blockIdx.x * blockDim.x + threadIdx.x;
    unsigned long long stride = blockDim.x * gridDim.x;
    while (index < count) {
        dst[index] = src[perm[index]];
        index += stride;
    }
}

// Kernel 1b (Exp A): block-cooperative point enumeration. One BLOCK per
// candidate (grid-stride by blockIdx); the block's threads split the wide
// top-coordinate seed frontier (device_make_points_block) with the basis in
// __shared__ memory built once per block -- removing the ~400 B/thread local-mem
// spill of the 1-thread/candidate kernel and the heavy-np intra-warp divergence
// (lanes now cooperate on ONE candidate instead of 32 unrelated np's). Same
// compact np_cap slot + overflow-to-CPU semantics as point_enum_kernel.
__global__ void point_enum_block_kernel(const DeviceCwsCandidate *candidates,
                                        unsigned long long candidate_count,
                                        int np_cap,
                                        long long *points,
                                        int *np_out,
                                        DeviceIpStats *stats,
                                        DeviceIpStageStats *stage_stats,
                                        DeviceCwsCandidate *overflow_output,
                                        unsigned long long *overflow_count,
                                        unsigned long long overflow_capacity) {
    __shared__ int s_np;
    __shared__ int s_precheck;
    for (unsigned long long index = blockIdx.x; index < candidate_count;
         index += gridDim.x) {
        const DeviceCwsCandidate &candidate = candidates[index];
        long long *slot = points + index * static_cast<unsigned long long>(np_cap) * 5ULL;
        unsigned long long tick =
            (stage_stats && threadIdx.x == 0) ? clock64() : 0ULL;
        // All threads must enter (internal __syncthreads in the block walk).
        int status = device_make_points_block(candidate, slot, np_cap,
                                              &s_np, &s_precheck);
        if (threadIdx.x == 0) {
            int np = s_np;
            if (stage_stats) {
                atomicAdd(&stage_stats->point_cycles, clock64() - tick);
                atomicAdd(&stage_stats->point_candidates, 1ULL);
                atomicAdd(&stage_stats->total_points,
                          static_cast<unsigned long long>(np > 0 ? np : 0));
                if (np > 0)
                    atomicMax(&stage_stats->max_points,
                              static_cast<unsigned long long>(np));
            }
            atomicAdd(&stats->processed, 1ULL);
            if (status < 0) {
                np_out[index] = kNpOverflow;
                atomicAdd(&stats->point_overflow, 1ULL);
                unsigned long long slot_index = atomicAdd(overflow_count, 1ULL);
                if (slot_index < overflow_capacity && overflow_output)
                    overflow_output[slot_index] = candidate;
            } else if (status == 0) {
                np_out[index] = 0;
                atomicAdd(&stats->point_fail, 1ULL);
            } else {
                np_out[index] = np;
                if (np < 6) atomicAdd(&stats->simplex_fail, 1ULL);
            }
        }
        __syncthreads();  // shared s_np/s_precheck reused next iteration
    }
}

// Kernel 2: IP check over the candidates that produced 6..np_cap points, given
// as a host-built index list (counting-sorted by np so a warp's 32 lanes run
// near-identical IP loops -> low divergence). One thread per candidate
// (grid-stride). Per-thread DeviceIpScratch lives in global memory sized to the
// launch (blocks*threads), independent of the candidate count.
__global__ void ip_check_bucketed_kernel(const DeviceCwsCandidate *candidates,
                                         const long long *points,
                                         const int *np_out,
                                         int np_cap,
                                         const int *valid_index,
                                         unsigned long long valid_count,
                                         DeviceIpScratch *scratch_workspace,
                                         DeviceIpStats *stats,
                                         DeviceIpStageStats *stage_stats,
                                         DeviceCwsCandidate *accepted_output,
                                         unsigned long long accepted_capacity) {
    unsigned long long tid = blockIdx.x * blockDim.x + threadIdx.x;
    unsigned long long stride = blockDim.x * gridDim.x;
    DeviceIpScratch *scratch = scratch_workspace + tid;
    for (unsigned long long j = tid; j < valid_count; j += stride) {
        unsigned long long index = static_cast<unsigned long long>(valid_index[j]);
        const long long *slot = points + index * static_cast<unsigned long long>(np_cap) * 5ULL;
        int np = np_out[index];
        int reject_reason = 0;
        unsigned long long tick = stage_stats ? clock64() : 0ULL;
        int is_ip = device_ip_check(slot, np, scratch, &reject_reason, stage_stats);
        if (stage_stats) {
            atomicAdd(&stage_stats->ip_cycles, clock64() - tick);
            atomicAdd(&stage_stats->ip_candidates, 1ULL);
        }
        if (!is_ip && reject_reason == 1) atomicAdd(&stats->simplex_fail, 1ULL);
        else if (!is_ip && reject_reason == 2) atomicAdd(&stats->initial_inci_fail, 1ULL);
        else if (!is_ip && reject_reason == 3) atomicAdd(&stats->vertex_overflow, 1ULL);
        else if (!is_ip) atomicAdd(&stats->ip_reject, 1ULL);
        if (is_ip) {
            unsigned long long accepted_index = atomicAdd(&stats->ip_count, 1ULL);
            if (accepted_index < accepted_capacity && accepted_output) {
                accepted_output[accepted_index] = candidates[index];
                atomicAdd(&stats->accepted_stored_count, 1ULL);
            }
        }
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
        unsigned long long linear = start_tuple + offset;
        unsigned long long remainder = linear;
        unsigned long long selected_indices[kMaxSlots] = {0, 0, 0, 0, 0};
        SelectedEntry tuple_entries[kMaxSlots];

        for (int slot = descriptor.simplex_count - 1; slot >= 0; --slot) {
            unsigned long long pool_count = descriptor.pool_counts[slot];
            selected_indices[slot] = remainder % pool_count;
            remainder /= pool_count;
        }

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
        if (descriptor.id == 3) {
            SelectedEntry left_entry = entries[descriptor.pool_offsets[0] + left];
            SelectedEntry right_entry = entries[descriptor.pool_offsets[1] + right];
            local_prefix += static_cast<unsigned long long>(device_type3_prefix_count(left_entry, right_entry));
            if (candidate_output && candidate_capacity > 0ULL) {
                SelectedEntry tuple_entries[kMaxSlots];
                tuple_entries[0] = left_entry;
                tuple_entries[1] = right_entry;
                device_emit_small_prefixes(descriptor, tuple_entries,
                                           candidate_output, candidate_capacity, stats);
            }
        } else {
            SelectedEntry tuple_entries[kMaxSlots];
            tuple_entries[0] = entries[descriptor.pool_offsets[0] + left];
            tuple_entries[1] = entries[descriptor.pool_offsets[1] + right];
            local_prefix += device_count_small_prefixes(descriptor, tuple_entries);
            if (candidate_output && candidate_capacity > 0ULL) {
                device_emit_small_prefixes(descriptor, tuple_entries,
                                           candidate_output, candidate_capacity, stats);
            }
        }
    }

    if (local_selection) {
        atomicAdd(&stats->selection_tuples, local_selection);
        atomicAdd(&stats->canonical_selection_tuples, local_selection);
    }
    if (local_prefix) atomicAdd(&stats->prefix_candidates, local_prefix);
}

HostScanResult scan_descriptor_range(const DeviceDescriptor &descriptor,
                                     const SelectedEntry *device_entries,
                                     DeviceScanStats *device_stats,
                                     DeviceCwsCandidate *candidate_output,
                                     std::uint64_t candidate_capacity,
                                     const Config &config,
                                     std::uint64_t range_start,
                                     std::uint64_t range_count) {
    check_cuda(cudaMemset(device_stats, 0, sizeof(DeviceScanStats)), "cudaMemset stats");
    auto start = std::chrono::steady_clock::now();
    if (use_pair_scan(descriptor)) {
        descriptor_pair_scan_kernel<<<config.blocks, config.threads>>>(
            device_entries, descriptor, range_start, range_count, device_stats,
            candidate_output, candidate_capacity);
    } else {
        descriptor_scan_kernel<<<config.blocks, config.threads>>>(
            device_entries, descriptor, range_start, range_count, device_stats,
            candidate_output, candidate_capacity);
    }
    check_cuda(cudaGetLastError(), "descriptor_scan_kernel launch");
    check_cuda(cudaDeviceSynchronize(), "cudaDeviceSynchronize descriptor scan");
    auto end = std::chrono::steady_clock::now();

    DeviceScanStats stats{};
    check_cuda(cudaMemcpy(&stats, device_stats, sizeof(DeviceScanStats), cudaMemcpyDeviceToHost),
               "cudaMemcpy stats");

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

HostScanResult scan_descriptor(const DeviceDescriptor &descriptor,
                               const SelectedEntry *device_entries,
                               DeviceScanStats *device_stats,
                               DeviceCwsCandidate *candidate_output,
                               std::uint64_t candidate_capacity,
                               const Config &config) {
    std::uint64_t shard_start = 0;
    std::uint64_t shard_count = 0;
    descriptor_shard_range(descriptor, config, &shard_start, &shard_count);
    return scan_descriptor_range(descriptor, device_entries, device_stats,
                                 candidate_output, candidate_capacity, config,
                                 shard_start, shard_count);
}

void write_candidate_row(std::ostream &out, const DeviceCwsCandidate &candidate) {
    for (int slot = 0; slot < candidate.nw; ++slot) {
        if (slot) out << ' ';
        out << candidate.degree[slot];
        for (int coordinate = 0; coordinate < candidate.ambient_vertices; ++coordinate) {
            out << ' ' << candidate.weights[slot][coordinate];
        }
    }
    out << '\n';
}

void allocate_ip_workspace(DeviceIpWorkspace *workspace,
                           std::uint64_t capacity,
                           int max_points_per_candidate,
                           std::uint64_t point_slots) {
    workspace->capacity = capacity;
    workspace->point_slots = std::max<std::uint64_t>(point_slots, 1);
    workspace->max_points = max_points_per_candidate;
    std::uint64_t point_values = workspace->point_slots * static_cast<std::uint64_t>(max_points_per_candidate) * 5ULL;
    check_cuda(cudaMalloc(&workspace->stats, sizeof(DeviceIpStats)), "cudaMalloc ip stats");
    check_cuda(cudaMalloc(&workspace->stage_stats, sizeof(DeviceIpStageStats)),
               "cudaMalloc ip stage stats");
    check_cuda(cudaMalloc(&workspace->accepted, capacity * sizeof(DeviceCwsCandidate)),
               "cudaMalloc accepted candidates");
    check_cuda(cudaMalloc(&workspace->points, point_values * sizeof(long long)),
               "cudaMalloc ip point workspace");
    check_cuda(cudaMalloc(&workspace->scratch, workspace->point_slots * sizeof(DeviceIpScratch)),
               "cudaMalloc ip scratch workspace");
}

void free_ip_workspace(DeviceIpWorkspace *workspace) {
    cudaFree(workspace->stage_stats);
    cudaFree(workspace->scratch);
    cudaFree(workspace->points);
    cudaFree(workspace->accepted);
    cudaFree(workspace->stats);
    *workspace = DeviceIpWorkspace{};
}

void accumulate_scan_result(HostScanResult *total, const HostScanResult &chunk) {
    total->selection_product = chunk.selection_product;
    total->scanned_tuples += chunk.scanned_tuples;
    total->canonical_selection_tuples += chunk.canonical_selection_tuples;
    total->prefix_candidates += chunk.prefix_candidates;
    total->stored_candidate_count += chunk.stored_candidate_count;
    total->seconds += chunk.seconds;
}

void accumulate_ip_result(HostIpResult *total, const HostIpResult &chunk) {
    total->candidate_count += chunk.candidate_count;
    total->processed += chunk.processed;
    total->precheck_fail += chunk.precheck_fail;
    total->point_overflow += chunk.point_overflow;
    total->point_fail += chunk.point_fail;
    total->simplex_fail += chunk.simplex_fail;
    total->initial_inci_fail += chunk.initial_inci_fail;
    total->vertex_overflow += chunk.vertex_overflow;
    total->ip_reject += chunk.ip_reject;
    total->ip_count += chunk.ip_count;
    total->accepted_stored_count += chunk.accepted_stored_count;
    total->stage.point_cycles += chunk.stage.point_cycles;
    total->stage.ip_cycles += chunk.stage.ip_cycles;
    total->stage.glz_cycles += chunk.stage.glz_cycles;
    total->stage.initial_inci_cycles += chunk.stage.initial_inci_cycles;
    total->stage.search_bad_eq_cycles += chunk.stage.search_bad_eq_cycles;
    total->stage.search_new_vertex_cycles += chunk.stage.search_new_vertex_cycles;
    total->stage.make_new_ceqs_cycles += chunk.stage.make_new_ceqs_cycles;
    total->stage.point_candidates += chunk.stage.point_candidates;
    total->stage.ip_candidates += chunk.stage.ip_candidates;
    total->stage.search_bad_eq_calls += chunk.stage.search_bad_eq_calls;
    total->stage.search_new_vertex_calls += chunk.stage.search_new_vertex_calls;
    total->stage.make_new_ceqs_calls += chunk.stage.make_new_ceqs_calls;
    total->stage.total_points += chunk.stage.total_points;
    total->stage.max_points = std::max(total->stage.max_points, chunk.stage.max_points);
    total->seconds += chunk.seconds;
}

std::uint64_t ip_workspace_slots(const Config &config, std::uint64_t candidate_count) {
    if (config.block_ip) {
        return std::min<std::uint64_t>(candidate_count,
                                       std::max<std::uint64_t>(static_cast<std::uint64_t>(config.blocks), 1));
    }
    std::uint64_t launch_threads =
        static_cast<std::uint64_t>(config.blocks) * static_cast<std::uint64_t>(config.threads);
    return std::min<std::uint64_t>(candidate_count, std::max<std::uint64_t>(launch_threads, 1));
}

HostIpResult run_ip_filter_with_workspace(DeviceCwsCandidate *device_candidates,
                                          std::uint64_t generated_count,
                                          const Config &config,
                                          std::ostream *accepted_output,
                                          DeviceIpWorkspace *workspace) {
    std::uint64_t candidate_count = std::min<std::uint64_t>(generated_count, workspace->capacity);
    HostIpResult result;
    result.candidate_count = candidate_count;
    if (candidate_count == 0) return result;

    check_cuda(cudaMemset(workspace->stats, 0, sizeof(DeviceIpStats)), "cudaMemset ip stats");
    if (config.ip_stage_profile) {
        check_cuda(cudaMemset(workspace->stage_stats, 0, sizeof(DeviceIpStageStats)),
                   "cudaMemset ip stage stats");
    }

    auto start = std::chrono::steady_clock::now();
    if (config.block_ip) {
        cws_ip_filter_block_kernel<<<config.blocks, config.threads>>>(
            device_candidates, candidate_count, config.ip_max_points, workspace->points,
            workspace->scratch, workspace->stats,
            config.ip_stage_profile ? workspace->stage_stats : nullptr,
            workspace->accepted, candidate_count);
        check_cuda(cudaGetLastError(), "cws_ip_filter_block_kernel launch");
    } else {
        cws_ip_filter_kernel<<<config.blocks, config.threads>>>(
            device_candidates, candidate_count, config.ip_max_points, workspace->points,
            workspace->scratch, workspace->stats,
            config.ip_stage_profile ? workspace->stage_stats : nullptr,
            workspace->accepted, candidate_count);
        check_cuda(cudaGetLastError(), "cws_ip_filter_kernel launch");
    }
    check_cuda(cudaDeviceSynchronize(), "cudaDeviceSynchronize ip filter");
    auto end = std::chrono::steady_clock::now();

    DeviceIpStats stats{};
    check_cuda(cudaMemcpy(&stats, workspace->stats, sizeof(DeviceIpStats), cudaMemcpyDeviceToHost),
               "cudaMemcpy ip stats");
    result.processed = stats.processed;
    result.precheck_fail = stats.precheck_fail;
    result.point_overflow = stats.point_overflow;
    result.point_fail = stats.point_fail;
    result.simplex_fail = stats.simplex_fail;
    result.initial_inci_fail = stats.initial_inci_fail;
    result.vertex_overflow = stats.vertex_overflow;
    result.ip_reject = stats.ip_reject;
    result.ip_count = stats.ip_count;
    result.accepted_stored_count = stats.accepted_stored_count;
    result.seconds = std::chrono::duration<double>(end - start).count();
    if (config.ip_stage_profile) {
        check_cuda(cudaMemcpy(&result.stage, workspace->stage_stats,
                              sizeof(DeviceIpStageStats), cudaMemcpyDeviceToHost),
                   "cudaMemcpy ip stage stats");
    }

    // CORRECTNESS: a point-buffer overflow means one or more candidates were not
    // IP-checked. We must never silently drop a candidate, so this is fatal.
    // Re-run with a larger --ip-max-points (PALP's reference ceiling is 2,000,000).
    if (result.point_overflow > 0) {
        throw std::runtime_error(
            "ip point-buffer overflow on " + std::to_string(result.point_overflow) +
            " candidate(s): they exceeded --ip-max-points (" +
            std::to_string(config.ip_max_points) +
            "); increase --ip-max-points to avoid dropping candidates");
    }

    if (accepted_output && result.accepted_stored_count > 0) {
        std::vector<DeviceCwsCandidate> accepted(static_cast<std::size_t>(result.accepted_stored_count));
        check_cuda(cudaMemcpy(accepted.data(), workspace->accepted,
                              result.accepted_stored_count * sizeof(DeviceCwsCandidate),
                              cudaMemcpyDeviceToHost),
                   "cudaMemcpy accepted candidates");
        for (const DeviceCwsCandidate &candidate : accepted) write_candidate_row(*accepted_output, candidate);
    }

    return result;
}

HostIpResult run_ip_filter(DeviceCwsCandidate *device_candidates,
                           std::uint64_t generated_count,
                           const Config &config,
                           std::ostream *accepted_output) {
    std::uint64_t candidate_count = std::min<std::uint64_t>(generated_count, config.emit_capacity);
    if (candidate_count == 0) return HostIpResult{};
    DeviceIpWorkspace workspace;
    allocate_ip_workspace(&workspace, candidate_count, config.ip_max_points,
                          ip_workspace_slots(config, candidate_count));
    HostIpResult result = run_ip_filter_with_workspace(device_candidates, generated_count,
                                                       config, accepted_output, &workspace);
    free_ip_workspace(&workspace);
    return result;
}

// Split + bucketed IP filter. Stage 1 enumerates points into a compact np_cap
// buffer over a full grid; the host classifies the np_out vector and counting-
// sorts the valid candidates by np; stage 2 IP-checks that sorted bucket. np>cap
// candidates are written to overflow_output for the CPU (never dropped).
HostIpResult run_ip_filter_bucketed(DeviceCwsCandidate *device_candidates,
                                    std::uint64_t generated_count,
                                    const Config &config,
                                    std::ostream *accepted_output,
                                    std::ostream *overflow_output) {
    std::uint64_t candidate_count = std::min<std::uint64_t>(generated_count, config.emit_capacity);
    HostIpResult result;
    result.candidate_count = candidate_count;
    if (candidate_count == 0) return result;

    const std::uint64_t np_cap = static_cast<std::uint64_t>(config.np_cap);
    const std::uint64_t point_values = candidate_count * np_cap * 5ULL;
    const std::uint64_t launch_threads = std::max<std::uint64_t>(
        static_cast<std::uint64_t>(config.blocks) * static_cast<std::uint64_t>(config.threads), 1);
    const bool profile = config.ip_stage_profile;

    long long *d_points = nullptr;
    int *d_np_out = nullptr;
    int *d_valid_index = nullptr;
    DeviceCwsCandidate *d_overflow = nullptr;
    unsigned long long *d_overflow_count = nullptr;
    DeviceCwsCandidate *d_accepted = nullptr;
    DeviceIpScratch *d_scratch = nullptr;
    DeviceIpStats *d_stats = nullptr;
    DeviceIpStageStats *d_stage = nullptr;

    check_cuda(cudaMalloc(&d_points, point_values * sizeof(long long)), "cudaMalloc bucket points");
    check_cuda(cudaMalloc(&d_np_out, candidate_count * sizeof(int)), "cudaMalloc np_out");
    check_cuda(cudaMalloc(&d_valid_index, candidate_count * sizeof(int)), "cudaMalloc valid_index");
    check_cuda(cudaMalloc(&d_overflow, candidate_count * sizeof(DeviceCwsCandidate)), "cudaMalloc overflow");
    check_cuda(cudaMalloc(&d_overflow_count, sizeof(unsigned long long)), "cudaMalloc overflow count");
    check_cuda(cudaMalloc(&d_accepted, candidate_count * sizeof(DeviceCwsCandidate)), "cudaMalloc accepted");
    check_cuda(cudaMalloc(&d_scratch, launch_threads * sizeof(DeviceIpScratch)), "cudaMalloc bucket scratch");
    check_cuda(cudaMalloc(&d_stats, sizeof(DeviceIpStats)), "cudaMalloc bucket stats");
    check_cuda(cudaMalloc(&d_stage, sizeof(DeviceIpStageStats)), "cudaMalloc bucket stage");

    check_cuda(cudaMemset(d_stats, 0, sizeof(DeviceIpStats)), "cudaMemset bucket stats");
    check_cuda(cudaMemset(d_overflow_count, 0, sizeof(unsigned long long)), "cudaMemset overflow count");
    if (profile) check_cuda(cudaMemset(d_stage, 0, sizeof(DeviceIpStageStats)), "cudaMemset bucket stage");

    auto free_all = [&]() {
        cudaFree(d_stage); cudaFree(d_stats); cudaFree(d_scratch); cudaFree(d_accepted);
        cudaFree(d_overflow_count); cudaFree(d_overflow); cudaFree(d_valid_index);
        cudaFree(d_np_out); cudaFree(d_points);
    };

    auto start = std::chrono::steady_clock::now();

    // Exp E: reorder candidates by box-volume proxy so enum warps are homogeneous.
    DeviceCwsCandidate *d_sorted = nullptr;
    if (config.vol_sort) {
        int *d_keys = nullptr;
        check_cuda(cudaMalloc(&d_keys, candidate_count * sizeof(int)), "cudaMalloc vol keys");
        check_cuda(cudaMalloc(&d_sorted, candidate_count * sizeof(DeviceCwsCandidate)), "cudaMalloc vol sorted");
        vol_key_kernel<<<config.blocks, config.threads>>>(device_candidates, candidate_count, d_keys);
        check_cuda(cudaDeviceSynchronize(), "vol_key_kernel");
        std::vector<int> keys(static_cast<std::size_t>(candidate_count));
        check_cuda(cudaMemcpy(keys.data(), d_keys, candidate_count * sizeof(int), cudaMemcpyDeviceToHost), "cudaMemcpy vol keys");
        std::vector<int> perm(static_cast<std::size_t>(candidate_count));
        for (std::uint64_t i = 0; i < candidate_count; ++i) perm[i] = static_cast<int>(i);
        std::sort(perm.begin(), perm.end(),
                  [&](int a, int b) { return keys[a] < keys[b]; });
        int *d_perm = nullptr;
        check_cuda(cudaMalloc(&d_perm, candidate_count * sizeof(int)), "cudaMalloc vol perm");
        check_cuda(cudaMemcpy(d_perm, perm.data(), candidate_count * sizeof(int), cudaMemcpyHostToDevice), "cudaMemcpy vol perm");
        gather_candidates_kernel<<<config.blocks, config.threads>>>(device_candidates, d_perm, candidate_count, d_sorted);
        check_cuda(cudaDeviceSynchronize(), "gather_candidates_kernel");
        cudaFree(d_perm); cudaFree(d_keys);
        device_candidates = d_sorted;  // enum + IP stages now read sorted order
    }

    // Stage 1: point enumeration (full grid, no VRAM cap). --block-ip selects
    // the block-cooperative variant (one block/candidate, shared basis).
    if (config.block_ip) {
        point_enum_block_kernel<<<config.blocks, config.threads>>>(
            device_candidates, candidate_count, config.np_cap, d_points, d_np_out,
            d_stats, profile ? d_stage : nullptr, d_overflow, d_overflow_count, candidate_count);
        check_cuda(cudaGetLastError(), "point_enum_block_kernel launch");
        check_cuda(cudaDeviceSynchronize(), "cudaDeviceSynchronize point_enum_block_kernel");
    } else {
        point_enum_kernel<<<config.blocks, config.threads>>>(
            device_candidates, candidate_count, config.np_cap, d_points, d_np_out,
            d_stats, profile ? d_stage : nullptr, d_overflow, d_overflow_count, candidate_count);
        check_cuda(cudaGetLastError(), "point_enum_kernel launch");
        check_cuda(cudaDeviceSynchronize(), "cudaDeviceSynchronize point_enum_kernel");
    }

    // Host: classify np_out and counting-sort the valid (6..np_cap) candidates
    // by np so each warp's lanes run near-identical IP loops.
    std::vector<int> np_host(static_cast<std::size_t>(candidate_count));
    check_cuda(cudaMemcpy(np_host.data(), d_np_out, candidate_count * sizeof(int),
                          cudaMemcpyDeviceToHost), "cudaMemcpy np_out");

    std::vector<std::uint64_t> bucket(static_cast<std::size_t>(np_cap) + 1, 0);
    std::uint64_t valid_total = 0;
    for (std::uint64_t i = 0; i < candidate_count; ++i) {
        int np = np_host[static_cast<std::size_t>(i)];
        if (np >= 6) { ++bucket[static_cast<std::size_t>(np)]; ++valid_total; }
    }
    std::uint64_t running = 0;
    for (std::size_t b = 0; b < bucket.size(); ++b) {
        std::uint64_t here = bucket[b];
        bucket[b] = running;  // bucket[b] now = start offset for np==b
        running += here;
    }
    std::vector<int> valid_index(static_cast<std::size_t>(valid_total));
    for (std::uint64_t i = 0; i < candidate_count; ++i) {
        int np = np_host[static_cast<std::size_t>(i)];
        if (np >= 6) valid_index[static_cast<std::size_t>(bucket[static_cast<std::size_t>(np)]++)] = static_cast<int>(i);
    }

    // Stage 2: IP-check the np-sorted valid bucket.
    if (valid_total > 0) {
        check_cuda(cudaMemcpy(d_valid_index, valid_index.data(), valid_total * sizeof(int),
                              cudaMemcpyHostToDevice), "cudaMemcpy valid_index");
        ip_check_bucketed_kernel<<<config.blocks, config.threads>>>(
            device_candidates, d_points, d_np_out, config.np_cap, d_valid_index,
            valid_total, d_scratch, d_stats, profile ? d_stage : nullptr,
            d_accepted, candidate_count);
        check_cuda(cudaGetLastError(), "ip_check_bucketed_kernel launch");
        check_cuda(cudaDeviceSynchronize(), "cudaDeviceSynchronize ip_check_bucketed_kernel");
    }
    auto end = std::chrono::steady_clock::now();

    DeviceIpStats stats{};
    check_cuda(cudaMemcpy(&stats, d_stats, sizeof(DeviceIpStats), cudaMemcpyDeviceToHost),
               "cudaMemcpy bucket stats");
    result.processed = stats.processed;
    result.precheck_fail = stats.precheck_fail;
    result.point_overflow = stats.point_overflow;
    result.point_fail = stats.point_fail;
    result.simplex_fail = stats.simplex_fail;
    result.initial_inci_fail = stats.initial_inci_fail;
    result.vertex_overflow = stats.vertex_overflow;
    result.ip_reject = stats.ip_reject;
    result.ip_count = stats.ip_count;
    result.accepted_stored_count = stats.accepted_stored_count;
    result.seconds = std::chrono::duration<double>(end - start).count();
    if (profile) {
        check_cuda(cudaMemcpy(&result.stage, d_stage, sizeof(DeviceIpStageStats),
                              cudaMemcpyDeviceToHost), "cudaMemcpy bucket stage");
    }

    // CORRECTNESS: ship np>np_cap candidates to the CPU; never drop them.
    unsigned long long overflow_count = 0;
    check_cuda(cudaMemcpy(&overflow_count, d_overflow_count, sizeof(unsigned long long),
                          cudaMemcpyDeviceToHost), "cudaMemcpy overflow count");
    if (overflow_count > 0) {
        if (!overflow_output) {
            free_all();
            if (d_sorted) cudaFree(d_sorted);
            throw std::runtime_error(
                std::to_string(overflow_count) + " candidate(s) exceeded --np-cap (" +
                std::to_string(config.np_cap) + "); pass --overflow-output to hand them to the "
                "CPU instead of dropping them (dataset completeness is required)");
        }
        std::vector<DeviceCwsCandidate> overflow(static_cast<std::size_t>(overflow_count));
        check_cuda(cudaMemcpy(overflow.data(), d_overflow,
                              overflow_count * sizeof(DeviceCwsCandidate),
                              cudaMemcpyDeviceToHost), "cudaMemcpy overflow candidates");
        for (const DeviceCwsCandidate &candidate : overflow) write_candidate_row(*overflow_output, candidate);
        overflow_output->flush();
        std::cerr << "  overflow_to_cpu structure " << config.structure_id
                  << " count: " << overflow_count
                  << " (np>" << config.np_cap << ", written to --overflow-output for CPU)\n";
    }

    if (accepted_output && stats.accepted_stored_count > 0) {
        std::vector<DeviceCwsCandidate> accepted(static_cast<std::size_t>(stats.accepted_stored_count));
        check_cuda(cudaMemcpy(accepted.data(), d_accepted,
                              stats.accepted_stored_count * sizeof(DeviceCwsCandidate),
                              cudaMemcpyDeviceToHost), "cudaMemcpy accepted candidates");
        for (const DeviceCwsCandidate &candidate : accepted) write_candidate_row(*accepted_output, candidate);
    }

    free_all();
    if (d_sorted) cudaFree(d_sorted);
    return result;
}

HostStreamIpResult stream_descriptor_ip(const DeviceDescriptor &descriptor,
                                        const SelectedEntry *device_entries,
                                        DeviceScanStats *device_stats,
                                        DeviceCwsCandidate *device_candidates,
                                        const Config &config,
                                        std::ostream *accepted_output) {
    HostStreamIpResult result;
    result.scan.structure_id = descriptor.id;
    result.scan.selection_product = descriptor.selection_product;
    result.ip.candidate_count = 0;

    std::uint64_t shard_start = 0;
    std::uint64_t shard_count = 0;
    descriptor_shard_range(descriptor, config, &shard_start, &shard_count);
    std::uint64_t shard_end = shard_start + shard_count;
    std::uint64_t position = shard_start;

    std::uint64_t max_variants = max_prefix_variants_per_selection(descriptor);
    std::uint64_t chunk_span = std::max<std::uint64_t>(1, config.emit_capacity / max_variants);
    DeviceIpWorkspace ip_workspace;
    allocate_ip_workspace(&ip_workspace, config.emit_capacity, config.ip_max_points,
                          ip_workspace_slots(config, config.emit_capacity));

    while (position < shard_end) {
        std::uint64_t range_count = std::min<std::uint64_t>(chunk_span, shard_end - position);
        HostScanResult scan_chunk = scan_descriptor_range(
            descriptor, device_entries, device_stats, device_candidates, config.emit_capacity,
            config, position, range_count);

        if (scan_chunk.stored_candidate_count > config.emit_capacity) {
            if (range_count == 1) {
                free_ip_workspace(&ip_workspace);
                throw std::runtime_error("single descriptor selection exceeds --emit-capacity; increase the chunk capacity");
            }
            chunk_span = std::max<std::uint64_t>(1, range_count / 2);
            ++result.retries;
            continue;
        }

        accumulate_scan_result(&result.scan, scan_chunk);
        HostIpResult ip_chunk = run_ip_filter_with_workspace(
            device_candidates, scan_chunk.stored_candidate_count,
            config, accepted_output, &ip_workspace);
        accumulate_ip_result(&result.ip, ip_chunk);
        ++result.chunks;
        position += range_count;
    }

    free_ip_workspace(&ip_workspace);
    return result;
}

void print_result_header() {
    std::cout << "structure_id,selection_product,scanned_selection_tuples,canonical_selection_tuples,prefix_candidates,seconds,scanned_tuples_per_second,prefix_candidates_per_second\n";
    std::cout.flush();
}

void print_result(const HostScanResult &result) {
    double tuple_rate = result.seconds > 0.0 ? result.scanned_tuples / result.seconds : 0.0;
    double prefix_rate = result.seconds > 0.0 ? result.prefix_candidates / result.seconds : 0.0;
    std::cout << result.structure_id << ','
              << result.selection_product << ','
              << result.scanned_tuples << ','
              << result.canonical_selection_tuples << ','
              << result.prefix_candidates << ','
              << std::fixed << std::setprecision(6) << result.seconds << ','
              << std::fixed << std::setprecision(1) << tuple_rate << ','
              << std::fixed << std::setprecision(1) << prefix_rate << '\n';
    std::cout.flush();
}

void print_ip_result(int structure_id, const HostIpResult &result) {
    double processed_rate = result.seconds > 0.0 ? result.processed / result.seconds : 0.0;
    std::cerr << "  gpu_ip structure " << structure_id
              << " candidates: " << result.candidate_count
              << " processed: " << result.processed
              << " ip: " << result.ip_count
              << " precheck_fail: " << result.precheck_fail
              << " point_overflow: " << result.point_overflow
              << " point_fail: " << result.point_fail
              << " simplex_fail: " << result.simplex_fail
              << " initial_inci_fail: " << result.initial_inci_fail
              << " vertex_overflow: " << result.vertex_overflow
              << " ip_reject: " << result.ip_reject
              << " accepted_stored: " << result.accepted_stored_count
              << " seconds: " << std::fixed << std::setprecision(6) << result.seconds
              << " candidates_per_second: " << std::fixed << std::setprecision(1) << processed_rate
              << '\n';
    if (result.stage.point_candidates > 0 || result.stage.ip_candidates > 0) {
        double top_total = static_cast<double>(result.stage.point_cycles + result.stage.ip_cycles);
        auto pct = [&](unsigned long long cycles, double denominator) {
            return denominator > 0.0 ? 100.0 * static_cast<double>(cycles) / denominator : 0.0;
        };
        double ip_total = static_cast<double>(result.stage.ip_cycles);
        double avg_points = result.stage.point_candidates > 0
            ? static_cast<double>(result.stage.total_points) / static_cast<double>(result.stage.point_candidates)
            : 0.0;
        std::cerr << "  ip_stage_profile structure " << structure_id
                  << " point_cycles: " << result.stage.point_cycles
                  << " (" << std::fixed << std::setprecision(1) << pct(result.stage.point_cycles, top_total) << "% top)"
                  << " ip_cycles: " << result.stage.ip_cycles
                  << " (" << std::fixed << std::setprecision(1) << pct(result.stage.ip_cycles, top_total) << "% top)"
                  << " point_candidates: " << result.stage.point_candidates
                  << " ip_candidates: " << result.stage.ip_candidates
                  << " avg_points: " << std::fixed << std::setprecision(1) << avg_points
                  << " max_points: " << result.stage.max_points
                  << " walk_nodes: " << result.stage.walk_nodes
                  << " nodes_per_cand: " << std::fixed << std::setprecision(1)
                  << (result.stage.point_candidates ? double(result.stage.walk_nodes) / double(result.stage.point_candidates) : 0.0)
                  << '\n'
                  << "    ip_substages glz: " << result.stage.glz_cycles
                  << " (" << std::fixed << std::setprecision(1) << pct(result.stage.glz_cycles, ip_total) << "% ip)"
                  << " initial_inci: " << result.stage.initial_inci_cycles
                  << " (" << std::fixed << std::setprecision(1) << pct(result.stage.initial_inci_cycles, ip_total) << "% ip)"
                  << " search_bad_eq: " << result.stage.search_bad_eq_cycles
                  << " (" << std::fixed << std::setprecision(1) << pct(result.stage.search_bad_eq_cycles, ip_total) << "% ip)"
                  << " search_new_vertex: " << result.stage.search_new_vertex_cycles
                  << " (" << std::fixed << std::setprecision(1) << pct(result.stage.search_new_vertex_cycles, ip_total) << "% ip)"
                  << " make_new_ceqs: " << result.stage.make_new_ceqs_cycles
                  << " (" << std::fixed << std::setprecision(1) << pct(result.stage.make_new_ceqs_cycles, ip_total) << "% ip)"
                  << '\n'
                  << "    ip_calls search_bad_eq: " << result.stage.search_bad_eq_calls
                  << " search_new_vertex: " << result.stage.search_new_vertex_calls
                  << " make_new_ceqs: " << result.stage.make_new_ceqs_calls
                  << '\n';
    }
}

void print_candidate(const DeviceCwsCandidate &candidate, int index) {
    std::cout << "candidate " << index
              << ": structure=" << candidate.structure_id
              << " nw=" << candidate.nw
              << " N=" << candidate.ambient_vertices;
    for (int slot = 0; slot < candidate.nw; ++slot) {
        std::cout << " d" << slot << '=' << candidate.degree[slot] << " w" << slot << '=';
        for (int coordinate = 0; coordinate < candidate.ambient_vertices; ++coordinate) {
            if (coordinate) std::cout << ' ';
            std::cout << candidate.weights[slot][coordinate];
        }
    }
    std::cout << '\n';
}

}  // namespace

int main(int argc, char **argv) {
    try {
        Config config = parse_args(argc, argv);
        check_cuda(cudaSetDevice(config.cuda_device), "cudaSetDevice");
        cudaDeviceSetLimit(cudaLimitStackSize, 1 << 20);
        cudaGetLastError();

        cudaDeviceProp properties{};
        check_cuda(cudaGetDeviceProperties(&properties, config.cuda_device), "cudaGetDeviceProperties");
        if (config.block_ip && !config.threads_explicit) config.threads = 32;
        if (config.blocks == 0) {
            config.blocks = properties.multiProcessorCount * (config.block_ip ? 256 : 16);
        }

        // The IP point workspace dominates VRAM: one slot per block (--block-ip)
        // or one slot per launched thread (serial), each holding
        // ip_max_points * 5 * 8 bytes plus a DeviceIpScratch. With ip_max_points
        // defaulting to PALP's POINT_Nmax (2,000,000 -> ~80 MB/slot) the default
        // grid cannot fit, so cap the launch grid to what free VRAM can hold. We
        // only ever reduce the grid: every candidate is still processed, just
        // fewer concurrently. (Restoring throughput at this buffer size is a
        // separate two-tier-buffer / overflow-requeue optimization.)
        //
        // --ip-bucketed is exactly that optimization: its point buffer is the
        // compact np_cap buffer (sized to the candidate count, not the grid), so
        // it never needs this cap and keeps the full grid.
        if (config.ip_check && !config.ip_bucketed) {
            std::size_t free_bytes = 0;
            std::size_t total_bytes = 0;
            check_cuda(cudaMemGetInfo(&free_bytes, &total_bytes), "cudaMemGetInfo");
            std::size_t budget = static_cast<std::size_t>(static_cast<double>(free_bytes) * 0.70);
            std::size_t bytes_per_slot =
                static_cast<std::size_t>(config.ip_max_points) * 5ULL * sizeof(long long) +
                sizeof(DeviceIpScratch);
            std::size_t affordable_slots =
                bytes_per_slot ? std::max<std::size_t>(budget / bytes_per_slot, 1) : 1;
            std::size_t affordable_blocks = config.block_ip
                ? affordable_slots
                : std::max<std::size_t>(affordable_slots / static_cast<std::size_t>(config.threads), 1);
            if (affordable_blocks < static_cast<std::size_t>(config.blocks)) {
                int capped = static_cast<int>(std::max<std::size_t>(affordable_blocks, 1));
                std::cerr << "  vram_cap: reducing blocks from " << config.blocks
                          << " to " << capped << " so the " << config.ip_max_points
                          << "-point workspace fits in VRAM (free "
                          << (free_bytes >> 20) << " MiB)\n";
                config.blocks = capped;
            }
        }

        auto pool_start = std::chrono::steady_clock::now();
        std::vector<BaseWeight> w5_pool = load_w5_pool(config.w5_path);
        auto pools = build_all_pools(config.palp_cws_path, w5_pool);
        std::vector<SelectedEntry> flat_entries = flatten_pools(pools);
        auto pool_end = std::chrono::steady_clock::now();

        SelectedEntry *device_entries = nullptr;
        DeviceScanStats *device_stats = nullptr;
        DeviceCwsCandidate *device_candidates = nullptr;
        check_cuda(cudaMalloc(&device_entries, flat_entries.size() * sizeof(SelectedEntry)),
                   "cudaMalloc selected entries");
        check_cuda(cudaMemcpy(device_entries, flat_entries.data(),
                              flat_entries.size() * sizeof(SelectedEntry),
                              cudaMemcpyHostToDevice),
                   "cudaMemcpy selected entries");
        check_cuda(cudaMalloc(&device_stats, sizeof(DeviceScanStats)), "cudaMalloc stats");
        if (config.emit_capacity > 0) {
            check_cuda(cudaMalloc(&device_candidates,
                                  config.emit_capacity * sizeof(DeviceCwsCandidate)),
                       "cudaMalloc candidates");
        }

        std::cerr << "dim5_cws_cuda_scan\n"
                  << "  device: " << properties.name << " sm_" << properties.major << properties.minor << '\n'
                  << "  blocks: " << config.blocks << '\n'
                  << "  threads_per_block: " << config.threads << '\n'
                  << "  w5_base_pool: " << w5_pool.size() << '\n'
                  << "  selected_entry_count: " << flat_entries.size() << '\n'
                  << "  emit_capacity: " << config.emit_capacity << '\n'
                  << "  ip_check: " << (config.ip_check ? "yes" : "no") << '\n'
                  << "  stream_ip: " << (config.stream_ip ? "yes" : "no") << '\n'
                  << "  block_ip: " << (config.block_ip ? "yes" : "no") << '\n'
                  << "  ip_bucketed: " << (config.ip_bucketed ? "yes" : "no") << '\n'
                  << "  np_cap: " << config.np_cap << '\n'
                  << "  ip_stage_profile: " << (config.ip_stage_profile ? "yes" : "no") << '\n'
                  << "  ip_max_points: " << config.ip_max_points << '\n'
                  << "  pool_build_seconds: " << std::fixed << std::setprecision(3)
                  << std::chrono::duration<double>(pool_end - pool_start).count() << '\n';

        std::ofstream accepted_output;
        if (!config.accepted_output_path.empty()) {
            accepted_output.open(config.accepted_output_path, std::ios::out | std::ios::trunc);
            if (!accepted_output) throw std::runtime_error("failed to open accepted output: " + config.accepted_output_path);
        }

        std::ofstream overflow_output;
        if (!config.overflow_output_path.empty()) {
            overflow_output.open(config.overflow_output_path, std::ios::out | std::ios::trunc);
            if (!overflow_output) throw std::runtime_error("failed to open overflow output: " + config.overflow_output_path);
        }

        print_result_header();
        std::uint64_t total_prefix = 0;
        std::uint64_t total_canonical_selection = 0;
        double total_seconds = 0.0;

        for (const Dim5StructureDescriptor &descriptor : kDim5Structures) {
            if (descriptor.id < 2 || descriptor.id > 47) continue;
            if (!config.all && config.structure_id != descriptor.id) continue;
            DeviceDescriptor device_descriptor = make_device_descriptor(descriptor, pools);
            if (config.stream_ip) {
                HostStreamIpResult stream_result = stream_descriptor_ip(
                    device_descriptor, device_entries, device_stats, device_candidates,
                    config, accepted_output ? &accepted_output : nullptr);
                print_result(stream_result.scan);
                std::cerr << "  stream_ip structure " << stream_result.scan.structure_id
                          << " chunks: " << stream_result.chunks
                          << " retries: " << stream_result.retries
                          << " stored_candidates_seen: " << stream_result.scan.stored_candidate_count
                          << " stored_capacity: " << config.emit_capacity << '\n';
                print_ip_result(stream_result.scan.structure_id, stream_result.ip);
                total_prefix += stream_result.scan.prefix_candidates;
                total_canonical_selection += stream_result.scan.canonical_selection_tuples;
                total_seconds += stream_result.scan.seconds;
                continue;
            }
            HostScanResult result = scan_descriptor(device_descriptor, device_entries, device_stats,
                                                    device_candidates, config.emit_capacity, config);
            print_result(result);
            if (config.emit_capacity > 0) {
                std::uint64_t printed = std::min<std::uint64_t>(
                    result.stored_candidate_count,
                    static_cast<std::uint64_t>(std::max(config.print_candidates, 0)));
                printed = std::min<std::uint64_t>(printed, config.emit_capacity);
                std::cerr << "  structure " << result.structure_id
                          << " generated_candidates_seen: " << result.stored_candidate_count
                          << " stored_capacity: " << config.emit_capacity << '\n';
                if (printed > 0) {
                    std::vector<DeviceCwsCandidate> host_candidates(static_cast<std::size_t>(printed));
                    check_cuda(cudaMemcpy(host_candidates.data(), device_candidates,
                                          printed * sizeof(DeviceCwsCandidate),
                                          cudaMemcpyDeviceToHost),
                               "cudaMemcpy candidates");
                    for (std::uint64_t index = 0; index < printed; ++index) {
                        print_candidate(host_candidates[static_cast<std::size_t>(index)],
                                        static_cast<int>(index));
                    }
                }
                if (config.ip_check) {
                    HostIpResult ip_result = config.ip_bucketed
                        ? run_ip_filter_bucketed(
                              device_candidates, result.stored_candidate_count, config,
                              accepted_output ? &accepted_output : nullptr,
                              overflow_output ? &overflow_output : nullptr)
                        : run_ip_filter(
                              device_candidates, result.stored_candidate_count, config,
                              accepted_output ? &accepted_output : nullptr);
                    print_ip_result(result.structure_id, ip_result);
                }
            }
            total_prefix += result.prefix_candidates;
            total_canonical_selection += result.canonical_selection_tuples;
            total_seconds += result.seconds;
        }

        std::cerr << "  total_canonical_selection_tuples: " << total_canonical_selection << '\n'
                  << "  total_prefix_candidates: " << total_prefix << '\n'
                  << "  total_scan_seconds: " << std::fixed << std::setprecision(6) << total_seconds << '\n';

        if (accepted_output) accepted_output.close();
        if (overflow_output) overflow_output.close();

        cudaFree(device_candidates);
        cudaFree(device_stats);
        cudaFree(device_entries);
        return 0;
    } catch (const std::exception &error) {
        std::cerr << "error: " << error.what() << '\n';
        return 1;
    }
}
