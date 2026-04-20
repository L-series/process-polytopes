#include "type3_bounds_runtime.h"

#include <cuda_runtime.h>

#include <cstdio>
#include <cstdlib>
#include <cstring>

struct Type3BoundsCudaContext {
  Type3BoundsJob *device_jobs;
  Type3BoundsResult *device_results;
  uint32_t capacity;
  uint32_t block_size;
};

namespace {

void SetError(char *buffer, size_t buffer_size, const char *message) {
  if ((buffer == nullptr) || (buffer_size == 0)) {
    return;
  }
  std::snprintf(buffer, buffer_size, "%s", message);
}

void SetCudaError(char *buffer, size_t buffer_size, const char *message,
                  cudaError_t status) {
  if ((buffer == nullptr) || (buffer_size == 0)) {
    return;
  }
  std::snprintf(buffer, buffer_size, "%s: %s", message,
                cudaGetErrorString(status));
}

__global__ void EvaluateKernel(const Type3BoundsJob *jobs,
                               Type3BoundsResult *results,
                               uint32_t job_count) {
  uint32_t index = blockIdx.x * blockDim.x + threadIdx.x;

  if (index < job_count) {
    results[index] = EvaluateType3BoundsJob(&jobs[index]);
  }
}

}  // namespace

extern "C" int Type3BoundsCudaCreate(Type3BoundsCudaContext **context,
                                       uint32_t capacity,
                                       uint32_t block_size,
                                       char *error_buffer,
                                       size_t error_buffer_size) {
  Type3BoundsCudaContext *created = nullptr;
  int device_count = 0;
  cudaError_t status;

  if (context == nullptr) {
    SetError(error_buffer, error_buffer_size, "null context pointer");
    return 1;
  }
  *context = nullptr;
  if (capacity == 0) {
    capacity = 1;
  }
  if (block_size == 0) {
    block_size = 128;
  }

  status = cudaGetDeviceCount(&device_count);
  if (status != cudaSuccess) {
    SetCudaError(error_buffer, error_buffer_size,
                 "unable to query CUDA devices", status);
    return 1;
  }
  if (device_count == 0) {
    SetError(error_buffer, error_buffer_size, "no CUDA device visible");
    return 1;
  }

  created = static_cast<Type3BoundsCudaContext *>(
      std::calloc(1, sizeof(Type3BoundsCudaContext)));
  if (created == nullptr) {
    SetError(error_buffer, error_buffer_size, "unable to allocate context");
    return 1;
  }
  created->capacity = capacity;
  created->block_size = block_size;

  status = cudaMalloc(&created->device_jobs,
                      static_cast<size_t>(capacity) * sizeof(Type3BoundsJob));
  if (status != cudaSuccess) {
    SetCudaError(error_buffer, error_buffer_size,
                 "unable to allocate device jobs", status);
    std::free(created);
    return 1;
  }

  status = cudaMalloc(&created->device_results,
                      static_cast<size_t>(capacity) *
                          sizeof(Type3BoundsResult));
  if (status != cudaSuccess) {
    SetCudaError(error_buffer, error_buffer_size,
                 "unable to allocate device results", status);
    cudaFree(created->device_jobs);
    std::free(created);
    return 1;
  }

  *context = created;
  if ((error_buffer != nullptr) && (error_buffer_size > 0)) {
    error_buffer[0] = '\0';
  }
  return 0;
}

extern "C" int Type3BoundsCudaEvaluate(Type3BoundsCudaContext *context,
                                         const Type3BoundsJob *jobs,
                                         Type3BoundsResult *results,
                                         uint32_t job_count,
                                         char *error_buffer,
                                         size_t error_buffer_size) {
  cudaError_t status;
  uint32_t blocks;

  if ((context == nullptr) || (jobs == nullptr) || (results == nullptr)) {
    SetError(error_buffer, error_buffer_size, "invalid CUDA evaluate args");
    return 1;
  }
  if (job_count == 0) {
    if ((error_buffer != nullptr) && (error_buffer_size > 0)) {
      error_buffer[0] = '\0';
    }
    return 0;
  }
  if (job_count > context->capacity) {
    SetError(error_buffer, error_buffer_size,
             "job_count exceeds CUDA context capacity");
    return 1;
  }

  status = cudaMemcpy(context->device_jobs, jobs,
                      static_cast<size_t>(job_count) * sizeof(Type3BoundsJob),
                      cudaMemcpyHostToDevice);
  if (status != cudaSuccess) {
    SetCudaError(error_buffer, error_buffer_size,
                 "unable to upload jobs to device", status);
    return 1;
  }

  blocks = (job_count + context->block_size - 1) / context->block_size;
  EvaluateKernel<<<blocks, context->block_size>>>(
      context->device_jobs, context->device_results, job_count);
  status = cudaGetLastError();
  if (status != cudaSuccess) {
    SetCudaError(error_buffer, error_buffer_size,
                 "kernel launch failed", status);
    return 1;
  }

  status = cudaDeviceSynchronize();
  if (status != cudaSuccess) {
    SetCudaError(error_buffer, error_buffer_size,
                 "kernel execution failed", status);
    return 1;
  }

  status = cudaMemcpy(results, context->device_results,
                      static_cast<size_t>(job_count) *
                          sizeof(Type3BoundsResult),
                      cudaMemcpyDeviceToHost);
  if (status != cudaSuccess) {
    SetCudaError(error_buffer, error_buffer_size,
                 "unable to download results", status);
    return 1;
  }

  if ((error_buffer != nullptr) && (error_buffer_size > 0)) {
    error_buffer[0] = '\0';
  }
  return 0;
}

extern "C" void Type3BoundsCudaDestroy(Type3BoundsCudaContext *context) {
  if (context == nullptr) {
    return;
  }
  if (context->device_jobs != nullptr) {
    cudaFree(context->device_jobs);
  }
  if (context->device_results != nullptr) {
    cudaFree(context->device_results);
  }
  std::free(context);
}