#include "geometry_backend.h"

#include <cstring>
#include <iostream>
#include <memory>
#include <string>

int main() {
    std::string reason;
    if (!cuda_geometry_available(&reason)) {
        std::cout << "SKIP: CUDA backend unavailable: " << reason << "\n";
        return 0;
    }

    palp_init();

    PalpCWSInput input{};
    input.nw = 1;
    input.N = 6;
    input.index = 1;
    for (int coord = 0; coord < 6; ++coord) {
        input.weights[0][coord] = 1;
        input.degree[0] += 1;
    }

    PalpNFResult result{};
    std::unique_ptr<GeometryBackend> backend = make_geometry_backend(GeometryBackendKind::Cuda, 0);
    backend->compute_one(input, result);
    if (!result.ok) {
        std::cerr << "FAIL: CUDA backend smoke CWS did not produce a PALP normal form\n";
        return 1;
    }

    long long cuda_point_count = 0;
    if (!cuda_count_cws_points_for_testing(input, &cuda_point_count, &reason)) {
        std::cerr << "FAIL: CUDA point-count probe failed: " << reason << "\n";
        return 1;
    }
    if (cuda_point_count != result.np) {
        std::cerr << "FAIL: CUDA point count " << cuda_point_count
                  << " != PALP point count " << result.np << "\n";
        return 1;
    }

    std::cout << "PASS: " << backend->name()
              << " computed smoke NF with nv=" << result.nv
              << " ne=" << result.ne
              << " np=" << result.np
              << " cuda_point_count=" << cuda_point_count << "\n";
    return 0;
}