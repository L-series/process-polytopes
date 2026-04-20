#include "type3_bounds_bench_common.h"

#include <cuda_runtime.h>

#include <cstdlib>
#include <fstream>
#include <iomanip>
#include <iostream>
#include <string>
#include <vector>

namespace {

struct Options {
    std::string input_path;
    uint32_t iterations = 20;
    uint32_t block_size = 256;
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

#define CUDA_CHECK(expr)                                                       \
    do {                                                                       \
        cudaError_t status__ = (expr);                                         \
        if (status__ != cudaSuccess) {                                         \
            std::cerr << "CUDA error: " << cudaGetErrorString(status__)      \
                      << " at " << __FILE__ << ':' << __LINE__ << '\n';     \
            std::exit(1);                                                      \
        }                                                                      \
    } while (0)

void PrintUsage(const char *argv0) {
    std::cerr << "Usage: " << argv0
              << " --input PATH [--iterations N] [--block-size N]\n";
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

        if ((arg == "--input") && (i + 1 < argc))
            options->input_path = argv[++i];
        else if ((arg == "--iterations") && (i + 1 < argc) &&
                 ParseUnsigned(argv[++i], &value))
            options->iterations = static_cast<uint32_t>(value);
        else if ((arg == "--block-size") && (i + 1 < argc) &&
                 ParseUnsigned(argv[++i], &value))
            options->block_size = static_cast<uint32_t>(value);
        else {
            PrintUsage(argv[0]);
            return false;
        }
    }

    if (options->input_path.empty()) {
        PrintUsage(argv[0]);
        return false;
    }
    if ((options->block_size == 0) || (options->iterations == 0)) {
        std::cerr << "iterations and block size must be positive\n";
        return false;
    }
    return true;
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

ValidationSummary ValidateResults(const std::vector<Type3BoundsRecord> &records,
                                  const std::vector<Type3BoundsResult> &results) {
    ValidationSummary summary;

    for (size_t i = 0; i < records.size(); ++i) {
        if (Type3BoundsResultsEqual(&records[i].expected, &results[i]))
            continue;
        if (summary.mismatches == 0) {
            summary.first_mismatch_ordinal = records[i].ordinal;
            summary.expected = records[i].expected;
            summary.actual = results[i];
        }
        summary.mismatches++;
    }
    return summary;
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
              << " avg_steps=" << (summary.total_steps * inv_job_count)
              << " checksum=" << summary.checksum << '\n';
}

__global__ void EvaluateKernel(const Type3BoundsJob *jobs,
                               Type3BoundsResult *results, uint32_t job_count) {
    uint32_t index = blockIdx.x * blockDim.x + threadIdx.x;

    if (index < job_count)
        results[index] = EvaluateType3BoundsJob(&jobs[index]);
}

}  // namespace

int main(int argc, char **argv) {
    Options options;
    Type3BoundsFileHeader header;
    std::vector<Type3BoundsRecord> records;
    std::vector<Type3BoundsResult> results;
    Type3BoundsJob *device_jobs = nullptr;
    Type3BoundsResult *device_results = nullptr;
    Summary summary;
    ValidationSummary validation;
    cudaEvent_t start_event, stop_event;
    float elapsed_ms = 0.0f;
    float total_ms = 0.0f;
    int device_count = 0;

    if (!ParseArgs(argc, argv, &options))
        return 1;
    if (!ReadRecords(options.input_path, &header, &records)) {
        std::cerr << "failed to read dataset: " << options.input_path << '\n';
        return 1;
    }
    CUDA_CHECK(cudaGetDeviceCount(&device_count));
    if (device_count == 0) {
        std::cerr << "no CUDA device visible\n";
        return 1;
    }

    std::vector<Type3BoundsJob> jobs(records.size());

    for (size_t i = 0; i < records.size(); ++i)
        jobs[i] = records[i].job;

    results.resize(jobs.size());
    CUDA_CHECK(cudaMalloc(&device_jobs, jobs.size() * sizeof(jobs[0])));
    CUDA_CHECK(cudaMalloc(&device_results, results.size() * sizeof(results[0])));
    CUDA_CHECK(cudaMemcpy(device_jobs, jobs.data(), jobs.size() * sizeof(jobs[0]),
                          cudaMemcpyHostToDevice));
    CUDA_CHECK(cudaEventCreate(&start_event));
    CUDA_CHECK(cudaEventCreate(&stop_event));

    uint32_t blocks = static_cast<uint32_t>((jobs.size() + options.block_size - 1) /
                                            options.block_size);

    EvaluateKernel<<<blocks, options.block_size>>>(device_jobs, device_results,
                                                   static_cast<uint32_t>(jobs.size()));
    CUDA_CHECK(cudaGetLastError());
    CUDA_CHECK(cudaDeviceSynchronize());

    for (uint32_t iter = 0; iter < options.iterations; ++iter) {
        CUDA_CHECK(cudaEventRecord(start_event));
        EvaluateKernel<<<blocks, options.block_size>>>(
            device_jobs, device_results, static_cast<uint32_t>(jobs.size()));
        CUDA_CHECK(cudaGetLastError());
        CUDA_CHECK(cudaEventRecord(stop_event));
        CUDA_CHECK(cudaEventSynchronize(stop_event));
        CUDA_CHECK(cudaEventElapsedTime(&elapsed_ms, start_event, stop_event));
        total_ms += elapsed_ms;
    }

    CUDA_CHECK(cudaMemcpy(results.data(), device_results,
                          results.size() * sizeof(results[0]),
                          cudaMemcpyDeviceToHost));

    for (const Type3BoundsResult &result : results)
        UpdateSummary(&summary, result);

    std::cout << "type3_bounds_dataset jobs=" << jobs.size()
              << " max_constraints=" << header.max_constraints
              << " seed=" << header.seed << '\n';
    PrintSummary(summary, jobs.size());
    validation = ValidateResults(records, results);
    std::cout << "gpu_validation mismatches=" << validation.mismatches;
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
    std::cout << std::fixed << std::setprecision(6)
              << "gpu_kernel_mean_seconds=" << ((total_ms / options.iterations) / 1000.0)
              << " jobs_per_second="
              << (jobs.size() / ((total_ms / options.iterations) / 1000.0))
              << " iterations=" << options.iterations
              << " block_size=" << options.block_size << '\n';

    CUDA_CHECK(cudaEventDestroy(start_event));
    CUDA_CHECK(cudaEventDestroy(stop_event));
    CUDA_CHECK(cudaFree(device_jobs));
    CUDA_CHECK(cudaFree(device_results));
    return 0;
}