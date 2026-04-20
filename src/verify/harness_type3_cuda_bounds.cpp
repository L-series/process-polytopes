#include "type3_bounds_bench_common.h"

#include <chrono>
#include <cstring>
#include <fstream>
#include <iomanip>
#include <iostream>
#include <random>
#include <sstream>
#include <string>
#include <vector>

namespace {

enum class JobMode {
    SeedEmpty,
    TightenEmpty,
    Survive,
};

struct Options {
    std::string input_path;
    std::string output_path;
    uint32_t jobs = 500000;
    uint32_t max_constraints = 8;
    uint64_t seed = 12345;
    uint32_t iterations = 5;
    uint32_t seed_empty_pct = 74;
    uint32_t tighten_empty_pct = 6;
    uint32_t survive_pct = 20;
    bool generate_only = false;
};

struct Summary {
    uint64_t seed_empty = 0;
    uint64_t tighten_empty = 0;
    uint64_t zero_fail = 0;
    uint64_t survived = 0;
    uint64_t singleton = 0;
    uint64_t total_steps = 0;
    uint64_t checksum = 1469598103934665603ull;
};

struct ValidationSummary {
    uint64_t mismatches = 0;
    uint64_t first_mismatch_ordinal = 0;
    Type3BoundsResult expected = {0, 0, 0, 0};
    Type3BoundsResult actual = {0, 0, 0, 0};
};

void PrintUsage(const char *argv0) {
    std::cerr
        << "Usage: " << argv0 << " [options]\n"
        << "  --input PATH              load a previously generated binary dataset\n"
        << "  --output PATH             save generated jobs to PATH\n"
        << "  --jobs N                  number of synthetic jobs to generate\n"
        << "  --max-constraints N       max per-job tighten constraints (<= 16)\n"
        << "  --seed N                  RNG seed\n"
        << "  --iterations N            benchmark iterations\n"
        << "  --seed-empty-pct N        target percentage for seed-empty jobs\n"
        << "  --tighten-empty-pct N     target percentage for post-seed failures\n"
        << "  --survive-pct N           target percentage for survivors\n"
        << "  --generate-only           only write the dataset, skip CPU timing\n";
}

bool ParseUnsigned(const char *text, uint64_t *value) {
    char *end = nullptr;
    unsigned long long parsed = std::strtoull(text, &end, 10);

    if ((text == end) || (*end != '\0'))
        return false;
    *value = static_cast<uint64_t>(parsed);
    return true;
}

bool ParseArgs(int argc, char **argv, Options *options) {
    for (int i = 1; i < argc; ++i) {
        std::string arg(argv[i]);
        uint64_t value;

        if (arg == "--input" && (i + 1 < argc))
            options->input_path = argv[++i];
        else if (arg == "--output" && (i + 1 < argc))
            options->output_path = argv[++i];
        else if (arg == "--jobs" && (i + 1 < argc) &&
                 ParseUnsigned(argv[++i], &value))
            options->jobs = static_cast<uint32_t>(value);
        else if (arg == "--max-constraints" && (i + 1 < argc) &&
                 ParseUnsigned(argv[++i], &value))
            options->max_constraints = static_cast<uint32_t>(value);
        else if (arg == "--seed" && (i + 1 < argc) &&
                 ParseUnsigned(argv[++i], &value))
            options->seed = value;
        else if (arg == "--iterations" && (i + 1 < argc) &&
                 ParseUnsigned(argv[++i], &value))
            options->iterations = static_cast<uint32_t>(value);
        else if (arg == "--seed-empty-pct" && (i + 1 < argc) &&
                 ParseUnsigned(argv[++i], &value))
            options->seed_empty_pct = static_cast<uint32_t>(value);
        else if (arg == "--tighten-empty-pct" && (i + 1 < argc) &&
                 ParseUnsigned(argv[++i], &value))
            options->tighten_empty_pct = static_cast<uint32_t>(value);
        else if (arg == "--survive-pct" && (i + 1 < argc) &&
                 ParseUnsigned(argv[++i], &value))
            options->survive_pct = static_cast<uint32_t>(value);
        else if (arg == "--generate-only")
            options->generate_only = true;
        else {
            PrintUsage(argv[0]);
            return false;
        }
    }

    if ((options->seed_empty_pct + options->tighten_empty_pct +
         options->survive_pct) != 100) {
        std::cerr << "seed/tighten/survive percentages must sum to 100\n";
        return false;
    }
    if ((options->max_constraints == 0) ||
        (options->max_constraints > kType3BoundsMaxConstraints)) {
        std::cerr << "max constraints must be in [1, 16]\n";
        return false;
    }
    if (options->iterations == 0) {
        std::cerr << "iterations must be positive\n";
        return false;
    }
    if (options->input_path.empty() && (options->jobs == 0)) {
        std::cerr << "jobs must be positive when generating a dataset\n";
        return false;
    }
    return true;
}

uint32_t WeightedChoice(std::mt19937_64 *rng, const uint64_t *weights,
                        uint32_t count) {
    uint64_t total = 0;

    for (uint32_t i = 0; i < count; ++i)
        total += weights[i];

    std::uniform_int_distribution<uint64_t> dist(0, total - 1);
    uint64_t pick = dist(*rng);
    uint64_t prefix = 0;

    for (uint32_t i = 0; i < count; ++i) {
        prefix += weights[i];
        if (pick < prefix)
            return i;
    }
    return count - 1;
}

int32_t RandomInt(std::mt19937_64 *rng, int32_t lo, int32_t hi) {
    std::uniform_int_distribution<int32_t> dist(lo, hi);
    return dist(*rng);
}

int32_t PickSeedDivisor(std::mt19937_64 *rng) {
    static const uint64_t weights[] = {
        8136683477ull,
        467278090ull,
        314521923ull,
        228985662ull,
        32393047409ull,
    };
    uint32_t bucket = WeightedChoice(rng, weights, 5);

    if (bucket < 4)
        return static_cast<int32_t>(bucket + 1);
    return RandomInt(rng, 5, 31);
}

int32_t PickConstraintDivisor(std::mt19937_64 *rng) {
    static const uint64_t sign_weights[] = {
        894750ull,
        562581644ull,
        2287604483ull,
    };
    static const uint64_t positive_weights[] = {
        34181043ull,
        11635844ull,
        16614299ull,
        11030209ull,
        489120249ull,
    };
    static const uint64_t negative_weights[] = {
        3074495ull,
        19544407ull,
        48433529ull,
        23976082ull,
        2192575970ull,
    };
    uint32_t sign_bucket = WeightedChoice(rng, sign_weights, 3);

    if (sign_bucket == 0)
        return 0;

    if (sign_bucket == 1) {
        uint32_t bucket = WeightedChoice(rng, positive_weights, 5);
        if (bucket < 4)
            return static_cast<int32_t>(bucket + 1);
        return RandomInt(rng, 5, 31);
    }

    uint32_t bucket = WeightedChoice(rng, negative_weights, 5);
    if (bucket < 4)
        return -static_cast<int32_t>(bucket + 1);
    return -RandomInt(rng, 5, 31);
}

uint32_t PickConstraintCount(std::mt19937_64 *rng, uint32_t max_constraints,
                             JobMode mode) {
    uint32_t lo = (mode == JobMode::SeedEmpty) ? 1u : 4u;

    if (lo > max_constraints)
        lo = max_constraints;
    std::uniform_int_distribution<uint32_t> dist(lo, max_constraints);
    return dist(*rng);
}

void FillConstraint(Type3BoundsJob *job, uint32_t index, std::mt19937_64 *rng,
                    JobMode mode) {
    int32_t r = PickConstraintDivisor(rng);

    job->r[index] = r;
    if (r == 0) {
        if (mode == JobMode::TightenEmpty && index == 0) {
            job->zero_x[index] = RandomInt(rng, -8, -1);
            job->zero_xmax[index] = RandomInt(rng, 0, 4);
        } else {
            job->zero_x[index] = RandomInt(rng, 0, 6);
            job->zero_xmax[index] = job->zero_x[index] + RandomInt(rng, 0, 6);
        }
        job->low[index] = 0;
        job->upp[index] = 0;
        return;
    }

    if (mode == JobMode::TightenEmpty && index == 0) {
        job->r[index] = (RandomInt(rng, 0, 1) == 0) ? RandomInt(rng, 1, 2)
                                                     : -RandomInt(rng, 1, 2);
        job->low[index] = RandomInt(rng, 5, 14);
        job->upp[index] = RandomInt(rng, -4, 2);
        if (job->upp[index] >= job->low[index])
            job->upp[index] = job->low[index] - 1;
        job->zero_x[index] = 0;
        job->zero_xmax[index] = 0;
        return;
    }

    if (mode == JobMode::Survive) {
        job->low[index] = RandomInt(rng, -8, 2);
        job->upp[index] = RandomInt(rng, 2, 18);
    } else {
        job->low[index] = RandomInt(rng, -16, 16);
        job->upp[index] = RandomInt(rng, -8, 16);
    }

    if (job->low[index] > job->upp[index]) {
        int32_t tmp = job->low[index];
        job->low[index] = job->upp[index];
        job->upp[index] = tmp;
    }
    job->zero_x[index] = RandomInt(rng, -8, 8);
    job->zero_xmax[index] = RandomInt(rng, 0, 12);
}

void FillCandidate(Type3BoundsJob *job, std::mt19937_64 *rng,
                   uint32_t max_constraints, JobMode mode) {
    std::memset(job, 0, sizeof(*job));
    job->seed_r = PickSeedDivisor(rng);
    job->constraint_count = PickConstraintCount(rng, max_constraints, mode);

    if (mode == JobMode::SeedEmpty) {
        job->seed_low = RandomInt(rng, 2, 16);
        job->seed_upp = RandomInt(rng, -4, 2);
        if (job->seed_upp >= job->seed_low)
            job->seed_upp = job->seed_low - 1;
    } else if (mode == JobMode::TightenEmpty) {
        job->seed_low = RandomInt(rng, -12, -1);
        job->seed_upp = RandomInt(rng, 8, 20);
    } else {
        job->seed_low = RandomInt(rng, -16, 0);
        job->seed_upp = RandomInt(rng, 4, 20);
    }

    for (uint32_t i = 0; i < job->constraint_count; ++i)
        FillConstraint(job, i, rng, mode);
}

bool MatchesMode(const Type3BoundsResult &result, JobMode mode) {
    switch (mode) {
    case JobMode::SeedEmpty:
        return (result.flags & kType3BoundsFlagSeedEmpty) != 0;
    case JobMode::TightenEmpty:
        return (result.flags & (kType3BoundsFlagTightenEmpty |
                                kType3BoundsFlagZeroFail)) != 0;
    case JobMode::Survive:
        return (result.flags & kType3BoundsFlagSurvived) != 0;
    }
    return false;
}

void GenerateRecords(const Options &options, Type3BoundsFileHeader *header,
                     std::vector<Type3BoundsRecord> *records) {
    std::mt19937_64 rng(options.seed);
    std::vector<JobMode> modes;

    modes.reserve(options.jobs);
    for (uint32_t i = 0; i < options.jobs; ++i) {
        uint32_t pick = RandomInt(&rng, 0, 99);
        if (pick < options.seed_empty_pct)
            modes.push_back(JobMode::SeedEmpty);
        else if (pick < (options.seed_empty_pct + options.tighten_empty_pct))
            modes.push_back(JobMode::TightenEmpty);
        else
            modes.push_back(JobMode::Survive);
    }

    header->magic = kType3BoundsMagic;
    header->version = kType3BoundsVersion;
    header->seed = options.seed;
    header->dataset_kind = kType3BoundsDatasetSynthetic;
    header->reserved = 0;
    header->job_count = options.jobs;
    header->max_constraints = options.max_constraints;
    header->seed_empty_pct = options.seed_empty_pct;
    header->tighten_empty_pct = options.tighten_empty_pct;
    header->survive_pct = options.survive_pct;

    records->resize(options.jobs);
    for (uint32_t i = 0; i < options.jobs; ++i) {
        for (uint32_t attempt = 0; attempt < 2048; ++attempt) {
            Type3BoundsRecord *record = &(*records)[i];

            record->ordinal = static_cast<uint64_t>(i) + 1;
            FillCandidate(&record->job, &rng, options.max_constraints, modes[i]);
            record->expected = EvaluateType3BoundsJob(&record->job);
            if (MatchesMode(record->expected, modes[i]))
                break;
            if (attempt == 2047) {
                std::cerr << "failed to synthesize job " << i << "\n";
                std::exit(1);
            }
        }
    }
}

bool WriteRecords(const std::string &path, const Type3BoundsFileHeader &header,
                  const std::vector<Type3BoundsRecord> &records) {
    std::ofstream out(path, std::ios::binary);

    if (!out)
        return false;
    out.write(reinterpret_cast<const char *>(&header), sizeof(header));
    out.write(reinterpret_cast<const char *>(records.data()),
              static_cast<std::streamsize>(records.size() * sizeof(records[0])));
    return out.good();
}

bool ReadRecords(const std::string &path, Type3BoundsFileHeader *header,
                 std::vector<Type3BoundsRecord> *records) {
    std::ifstream in(path, std::ios::binary);

    if (!in)
        return false;
    in.read(reinterpret_cast<char *>(header), sizeof(*header));
    if (!in || (header->magic != kType3BoundsMagic) ||
        (header->version != kType3BoundsVersion) ||
        (header->max_constraints > kType3BoundsMaxConstraints)) {
        return false;
    }
    records->resize(header->job_count);
    in.read(reinterpret_cast<char *>(records->data()),
            static_cast<std::streamsize>(records->size() * sizeof((*records)[0])));
    return in.good();
}

void UpdateSummary(Summary *summary, const Type3BoundsResult &result) {
    summary->checksum = Type3BoundsChecksumUpdate(summary->checksum, &result);
    summary->total_steps += result.steps;
    if (result.flags & kType3BoundsFlagSeedEmpty)
        summary->seed_empty++;
    if (result.flags & kType3BoundsFlagTightenEmpty)
        summary->tighten_empty++;
    if (result.flags & kType3BoundsFlagZeroFail)
        summary->zero_fail++;
    if (result.flags & kType3BoundsFlagSurvived)
        summary->survived++;
    if (result.flags & kType3BoundsFlagSingleton)
        summary->singleton++;
}

Summary SummarizeExpected(const std::vector<Type3BoundsRecord> &records) {
    Summary summary;

    for (const Type3BoundsRecord &record : records)
        UpdateSummary(&summary, record.expected);
    return summary;
}

ValidationSummary ValidateRecords(const std::vector<Type3BoundsRecord> &records) {
    ValidationSummary summary;

    for (const Type3BoundsRecord &record : records) {
        Type3BoundsResult actual = EvaluateType3BoundsJob(&record.job);

        if (Type3BoundsResultsEqual(&actual, &record.expected))
            continue;
        if (summary.mismatches == 0) {
            summary.first_mismatch_ordinal = record.ordinal;
            summary.expected = record.expected;
            summary.actual = actual;
        }
        summary.mismatches++;
    }
    return summary;
}

double BenchmarkCpu(const std::vector<Type3BoundsRecord> &records,
                    uint32_t iterations,
                    uint64_t *checksum_out) {
    using clock = std::chrono::steady_clock;
    uint64_t checksum = 1469598103934665603ull;
    double total_seconds = 0.0;

    for (uint32_t iter = 0; iter < iterations; ++iter) {
        auto start = clock::now();

        for (const Type3BoundsRecord &record : records) {
            Type3BoundsResult result = EvaluateType3BoundsJob(&record.job);

            checksum = Type3BoundsChecksumUpdate(checksum, &result);
        }

        auto stop = clock::now();
        total_seconds +=
            std::chrono::duration_cast<std::chrono::duration<double>>(stop - start)
                .count();
    }

    *checksum_out = checksum;
    return total_seconds / iterations;
}

void PrintHeader(const Type3BoundsFileHeader &header,
                 const std::vector<Type3BoundsRecord> &records) {
    std::cout << "type3_bounds_dataset jobs=" << records.size()
              << " max_constraints=" << header.max_constraints
              << " seed=" << header.seed << '\n';
    if (header.dataset_kind == kType3BoundsDatasetSynthetic)
        std::cout << "target_mix seed_empty=" << header.seed_empty_pct
                  << "% tighten_empty=" << header.tighten_empty_pct
                  << "% survive=" << header.survive_pct << "%\n";
    else
        std::cout << "dataset_kind=" << header.dataset_kind << "\n";
}

void PrintSummary(const Summary &summary, size_t job_count) {
    double inv_job_count = job_count ? (1.0 / static_cast<double>(job_count)) : 0.0;

    std::cout << "observed_mix seed_empty=" << summary.seed_empty
              << " tighten_empty=" << summary.tighten_empty
              << " zero_fail=" << summary.zero_fail
              << " survived=" << summary.survived
              << " singleton=" << summary.singleton << '\n';
    std::cout << std::fixed << std::setprecision(3)
              << "observed_pct seed_empty="
              << (100.0 * summary.seed_empty * inv_job_count)
              << " tighten_empty="
              << (100.0 * summary.tighten_empty * inv_job_count)
              << " survived=" << (100.0 * summary.survived * inv_job_count)
              << " avg_steps="
              << (summary.total_steps * inv_job_count)
              << " checksum=" << summary.checksum << '\n';
}

}  // namespace

int main(int argc, char **argv) {
    Options options;
    Type3BoundsFileHeader header;
    std::vector<Type3BoundsRecord> records;
    Summary summary;
    ValidationSummary validation;
    uint64_t bench_checksum = 0;
    double mean_seconds;

    if (!ParseArgs(argc, argv, &options))
        return 1;

    if (!options.input_path.empty()) {
        if (!ReadRecords(options.input_path, &header, &records)) {
            std::cerr << "failed to read dataset: " << options.input_path << '\n';
            return 1;
        }
    } else {
        GenerateRecords(options, &header, &records);
    }

    if (!options.output_path.empty() &&
        !WriteRecords(options.output_path, header, records)) {
        std::cerr << "failed to write dataset: " << options.output_path << '\n';
        return 1;
    }

    PrintHeader(header, records);
    summary = SummarizeExpected(records);
    PrintSummary(summary, records.size());
    validation = ValidateRecords(records);
    std::cout << "cpu_validation mismatches=" << validation.mismatches;
    if (validation.mismatches) {
        std::cout << " first_ordinal=" << validation.first_mismatch_ordinal
                  << " expected_flags=" << validation.expected.flags
                  << " expected_xmin=" << validation.expected.xmin
                  << " expected_xmax=" << validation.expected.xmax
                  << " actual_flags=" << validation.actual.flags
                  << " actual_xmin=" << validation.actual.xmin
                  << " actual_xmax=" << validation.actual.xmax;
    }
    std::cout << '\n';
    if (validation.mismatches)
        return 1;

    if (options.generate_only)
        return 0;

    mean_seconds = BenchmarkCpu(records, options.iterations, &bench_checksum);
    std::cout << std::fixed << std::setprecision(6)
              << "cpu_mean_seconds=" << mean_seconds
              << " jobs_per_second=" << (records.size() / mean_seconds)
              << " iterations=" << options.iterations
              << " bench_checksum=" << bench_checksum << '\n';
    return 0;
}