#ifndef GEOMETRY_BACKEND_H
#define GEOMETRY_BACKEND_H

#include <cstddef>
#include <memory>
#include <string>

#include "palp_api.h"

enum class GeometryBackendKind {
    Cpu,
    Cuda,
    Auto,
};

GeometryBackendKind parse_geometry_backend_kind(const std::string &name);
const char *geometry_backend_kind_name(GeometryBackendKind kind);

class GeometryBackend {
public:
    virtual ~GeometryBackend() = default;

    virtual const char *name() const = 0;
    virtual bool uses_cuda_device() const = 0;

    virtual void compute_batch(const PalpCWSInput *first_input,
                               std::size_t input_stride_bytes,
                               std::size_t count,
                               PalpNFResult *results) = 0;

    void compute_one(const PalpCWSInput &input, PalpNFResult &result) {
        compute_batch(&input, sizeof(PalpCWSInput), 1, &result);
    }
};

std::unique_ptr<GeometryBackend> make_geometry_backend(GeometryBackendKind kind,
                                                       int cuda_device);

bool cuda_geometry_available(std::string *reason);

#if CLASSIFIER_ENABLE_CUDA
bool cuda_count_cws_points_for_testing(const PalpCWSInput &input,
                                       long long *point_count,
                                       std::string *reason);
#endif

#endif