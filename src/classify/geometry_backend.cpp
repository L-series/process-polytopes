#include "geometry_backend.h"

#include <stdexcept>
#include <string>

#if CLASSIFIER_ENABLE_CUDA
std::unique_ptr<GeometryBackend> make_cuda_geometry_backend(int cuda_device);
#endif

namespace {

const PalpCWSInput &strided_input_at(const PalpCWSInput *first_input,
                                     std::size_t stride_bytes,
                                     std::size_t input_index) {
    const auto *base = reinterpret_cast<const unsigned char *>(first_input);
    return *reinterpret_cast<const PalpCWSInput *>(base + input_index * stride_bytes);
}

class CpuGeometryBackend final : public GeometryBackend {
public:
    CpuGeometryBackend() : workspace_(palp_workspace_alloc()) {
        if (!workspace_) {
            throw std::runtime_error("failed to allocate PALP CPU geometry workspace");
        }
    }

    ~CpuGeometryBackend() override {
        palp_workspace_free(workspace_);
    }

    const char *name() const override { return "cpu-palp"; }
    bool uses_cuda_device() const override { return false; }

    void compute_batch(const PalpCWSInput *first_input,
                       std::size_t input_stride_bytes,
                       std::size_t count,
                       PalpNFResult *results) override {
        for (std::size_t input_index = 0; input_index < count; ++input_index) {
            const PalpCWSInput &input = strided_input_at(first_input, input_stride_bytes, input_index);
            palp_compute_nf_from_cws(workspace_, &input, &results[input_index]);
        }
    }

private:
    PalpWorkspace *workspace_;
};

std::unique_ptr<GeometryBackend> make_cpu_geometry_backend() {
    return std::make_unique<CpuGeometryBackend>();
}

#if !CLASSIFIER_ENABLE_CUDA
std::unique_ptr<GeometryBackend> make_cuda_geometry_backend(int) {
    throw std::runtime_error("classifier was built without CUDA support; reconfigure with -DENABLE_CUDA=ON");
}
#endif

}  // namespace

GeometryBackendKind parse_geometry_backend_kind(const std::string &name) {
    if (name == "cpu" || name == "palp" || name == "cpu-palp") return GeometryBackendKind::Cpu;
    if (name == "cuda" || name == "gpu") return GeometryBackendKind::Cuda;
    if (name == "auto") return GeometryBackendKind::Auto;
    throw std::runtime_error("unknown geometry backend: " + name + " (expected cpu, cuda, or auto)");
}

const char *geometry_backend_kind_name(GeometryBackendKind kind) {
    switch (kind) {
        case GeometryBackendKind::Cpu: return "cpu";
        case GeometryBackendKind::Cuda: return "cuda";
        case GeometryBackendKind::Auto: return "auto";
    }
    return "unknown";
}

std::unique_ptr<GeometryBackend> make_geometry_backend(GeometryBackendKind kind,
                                                       int cuda_device) {
    if (kind == GeometryBackendKind::Cpu) {
        return make_cpu_geometry_backend();
    }

    if (kind == GeometryBackendKind::Auto) {
        std::string reason;
        if (cuda_geometry_available(&reason)) {
            return make_cuda_geometry_backend(cuda_device);
        }
        return make_cpu_geometry_backend();
    }

    return make_cuda_geometry_backend(cuda_device);
}

#if !CLASSIFIER_ENABLE_CUDA
bool cuda_geometry_available(std::string *reason) {
    if (reason) *reason = "classifier was built without CUDA support";
    return false;
}
#endif