#include "type3_bounds_runtime.h"

#include <cuda_runtime.h>

#include <cstdio>
#include <cstdlib>
#include <cstring>

struct Type3BoundsCudaContext {
  Type3BoundsJob *device_jobs;
  Type3BoundsResult *device_results;
  uint32_t capacity;
  uint32_t frontier_capacity;
  uint32_t point_capacity;
  uint32_t block_size;
  uint32_t *device_output_count;
  int *device_overflow_flag;
  Type3BoundsCudaStats *device_stats;
  Type3BoundsCudaProblem *device_problem;
  int64_t (*device_frontier_a)[kType3BoundsRuntimeMaxDimension];
  int64_t (*device_frontier_b)[kType3BoundsRuntimeMaxDimension];
  int64_t (*device_points)[kType3BoundsRuntimeMaxDimension];
};

namespace {

__device__ void BuildType3BoundsJob(
    const Type3BoundsCudaProblem *problem,
    const int64_t state[kType3BoundsRuntimeMaxDimension], int coord,
    Type3BoundsJob *job) {
  int i, k;
  int64_t low, upp, r;

  for (i = 0; i < (int)sizeof(*job); i++)
    ((unsigned char *)job)[i] = 0;

  i = problem->Amin[coord + 1] - 1;
  low = -problem->X0[i];
  for (k = coord + 1; k < (int)problem->n; k++)
    low -= state[k] * problem->basis[k][i];
  upp = problem->Xmax[i] + low;
  r = problem->basis[coord][i];

  job->seed_low = (int32_t)low;
  job->seed_upp = (int32_t)upp;
  job->seed_r = (int32_t)r;

  while ((i--) > problem->Amin[coord]) {
    uint32_t constraint_index = job->constraint_count;

    r = problem->basis[coord][i];
    if (r != 0) {
      low = -problem->X0[i];
      for (k = coord + 1; k < (int)problem->n; k++)
        low -= state[k] * problem->basis[k][i];
      upp = problem->Xmax[i] + low;
      job->low[constraint_index] = (int32_t)low;
      job->upp[constraint_index] = (int32_t)upp;
      job->r[constraint_index] = (int32_t)r;
    } else {
      int64_t x = 1;

      for (k = coord + 1; k < (int)problem->n; k++)
        x += state[k] * problem->basis[k][i];
      job->r[constraint_index] = 0;
      job->zero_x[constraint_index] = (int32_t)x;
      job->zero_xmax[constraint_index] = (int32_t)problem->Xmax[i];
    }
    job->constraint_count++;
  }
}

__device__ inline void AtomicAddU64(uint64_t *address,
                                    unsigned long long value) {
  atomicAdd(reinterpret_cast<unsigned long long *>(address), value);
}

__device__ void AccumulateType3BoundsStats(const Type3BoundsJob *job,
                                           const Type3BoundsResult *result,
                                           Type3BoundsCudaStats *stats) {
  uint32_t processed_constraints = result->steps > 0 ? (result->steps - 1) : 0;
  uint32_t remaining_constraints;
  uint32_t i;

  if (processed_constraints > job->constraint_count)
    processed_constraints = job->constraint_count;

  for (i = 0; i < processed_constraints; i++) {
    if (job->r[i] == 0)
      AtomicAddU64(&stats->zero_constraint_count, 1ULL);
    else
      AtomicAddU64(&stats->nonzero_constraint_count, 1ULL);
  }

  remaining_constraints = job->constraint_count - processed_constraints;
  if (remaining_constraints)
    AtomicAddU64(&stats->skipped_constraint_count,
                 (unsigned long long)remaining_constraints);

  if (result->flags & kType3BoundsFlagSeedEmpty) {
    AtomicAddU64(&stats->seed_interval_empty_count, 1ULL);
    return;
  }
  if (result->flags & kType3BoundsFlagTightenEmpty)
    AtomicAddU64(&stats->tighten_interval_empty_count, 1ULL);
  if (result->flags & kType3BoundsFlagZeroFail)
    AtomicAddU64(&stats->zero_range_fail_count, 1ULL);

  if ((result->flags & kType3BoundsFlagSingleton) && remaining_constraints) {
    AtomicAddU64(&stats->singleton_remaining_constraint_count,
                 (unsigned long long)remaining_constraints);
    if (processed_constraints == 0)
      AtomicAddU64(&stats->seed_singleton_count, 1ULL);
    else
      AtomicAddU64(&stats->tighten_singleton_count, 1ULL);
  }
}

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

__global__ void InitializeFrontierKernel(const Type3BoundsCudaProblem *problem,
                                         int64_t (*frontier)[kType3BoundsRuntimeMaxDimension],
                                         uint32_t state_count) {
  uint32_t index = blockIdx.x * blockDim.x + threadIdx.x;

  if (index >= state_count)
    return;

  for (uint32_t coord = 0; coord < problem->n; coord++)
    frontier[index][coord] = 0;
  frontier[index][problem->n - 1] =
      problem->initial_xmin + static_cast<int64_t>(index);
}

__global__ void ExpandFrontierKernel(
    const Type3BoundsCudaProblem *problem, int coord,
    const int64_t (*current)[kType3BoundsRuntimeMaxDimension],
    uint32_t current_count,
    int64_t (*output)[kType3BoundsRuntimeMaxDimension], uint32_t output_capacity,
    uint32_t *output_count, int *overflow_flag, Type3BoundsCudaStats *stats) {
  uint32_t index = blockIdx.x * blockDim.x + threadIdx.x;
  Type3BoundsJob job;
  Type3BoundsResult result;

  if (index >= current_count)
    return;

  BuildType3BoundsJob(problem, current[index], coord, &job);
  result = EvaluateType3BoundsJob(&job);
  AccumulateType3BoundsStats(&job, &result, stats);
  if (!(result.flags & kType3BoundsFlagSurvived))
    return;

  for (int64_t value = result.xmin; value <= result.xmax; value++) {
    uint32_t output_index = atomicAdd(output_count, 1u);

    if (output_index >= output_capacity) {
      atomicExch(overflow_flag, 1);
      continue;
    }

    for (uint32_t copy_coord = 0; copy_coord < problem->n; copy_coord++)
      output[output_index][copy_coord] = current[index][copy_coord];
    output[output_index][coord] = value;
  }
}

cudaError_t EnsureEnumerationCapacity(Type3BoundsCudaContext *context,
                                      uint32_t frontier_capacity,
                                      uint32_t point_capacity) {
  cudaError_t status;

  if (context->device_problem == nullptr) {
    status = cudaMalloc(&context->device_problem,
                        sizeof(Type3BoundsCudaProblem));
    if (status != cudaSuccess)
      return status;
  }
  if (context->device_output_count == nullptr) {
    status = cudaMalloc(&context->device_output_count, sizeof(uint32_t));
    if (status != cudaSuccess)
      return status;
  }
  if (context->device_overflow_flag == nullptr) {
    status = cudaMalloc(&context->device_overflow_flag, sizeof(int));
    if (status != cudaSuccess)
      return status;
  }
  if (context->device_stats == nullptr) {
    status = cudaMalloc(&context->device_stats, sizeof(Type3BoundsCudaStats));
    if (status != cudaSuccess)
      return status;
  }

  if (frontier_capacity > context->frontier_capacity) {
    if (context->device_frontier_a != nullptr)
      cudaFree(context->device_frontier_a);
    if (context->device_frontier_b != nullptr)
      cudaFree(context->device_frontier_b);
    status = cudaMalloc(&context->device_frontier_a,
                        static_cast<size_t>(frontier_capacity) *
                            kType3BoundsRuntimeMaxDimension * sizeof(int64_t));
    if (status != cudaSuccess)
      return status;
    status = cudaMalloc(&context->device_frontier_b,
                        static_cast<size_t>(frontier_capacity) *
                            kType3BoundsRuntimeMaxDimension * sizeof(int64_t));
    if (status != cudaSuccess)
      return status;
    context->frontier_capacity = frontier_capacity;
  }

  if (point_capacity > context->point_capacity) {
    if (context->device_points != nullptr)
      cudaFree(context->device_points);
    status = cudaMalloc(&context->device_points,
                        static_cast<size_t>(point_capacity) *
                            kType3BoundsRuntimeMaxDimension * sizeof(int64_t));
    if (status != cudaSuccess)
      return status;
    context->point_capacity = point_capacity;
  }

  return cudaSuccess;
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

extern "C" int Type3BoundsCudaEnumerate(Type3BoundsCudaContext *context,
                                         const Type3BoundsCudaProblem *problem,
                                         uint32_t point_capacity,
                                         int64_t *points,
                                         uint32_t *point_count,
                                         Type3BoundsCudaStats *stats,
                                         char *error_buffer,
                                         size_t error_buffer_size) {
  cudaError_t status;
  uint32_t state_count;
  uint32_t blocks;
  int overflow_flag = 0;
  Type3BoundsCudaStats local_stats = {0};
  int64_t (*current)[kType3BoundsRuntimeMaxDimension];
  int64_t (*next)[kType3BoundsRuntimeMaxDimension];

  if ((context == nullptr) || (problem == nullptr) || (points == nullptr) ||
      (point_count == nullptr)) {
    SetError(error_buffer, error_buffer_size,
             "invalid CUDA enumerate args");
    return 1;
  }
  if ((problem->n == 0) ||
      (problem->n > kType3BoundsRuntimeMaxDimension) ||
      (problem->ambient_count == 0) ||
      (problem->ambient_count > kType3BoundsRuntimeMaxAmbient)) {
    SetError(error_buffer, error_buffer_size,
             "problem dimensions exceed CUDA runtime limits");
    return 1;
  }
  if (problem->initial_xmax < problem->initial_xmin) {
    *point_count = 0;
    if (stats != nullptr)
      *stats = local_stats;
    if ((error_buffer != nullptr) && (error_buffer_size > 0))
      error_buffer[0] = '\0';
    return 0;
  }

  state_count = (uint32_t)(problem->initial_xmax - problem->initial_xmin + 1);
  if (state_count > point_capacity) {
    SetError(error_buffer, error_buffer_size,
             "initial frontier exceeds point capacity");
    return 1;
  }

  status = EnsureEnumerationCapacity(context, point_capacity, point_capacity);
  if (status != cudaSuccess) {
    SetCudaError(error_buffer, error_buffer_size,
                 "unable to reserve enumeration buffers", status);
    return 1;
  }

  status = cudaMemcpy(context->device_problem, problem, sizeof(*problem),
                      cudaMemcpyHostToDevice);
  if (status != cudaSuccess) {
    SetCudaError(error_buffer, error_buffer_size,
                 "unable to upload enumeration problem", status);
    return 1;
  }

  status = cudaMemset(context->device_stats, 0, sizeof(Type3BoundsCudaStats));
  if (status != cudaSuccess) {
    SetCudaError(error_buffer, error_buffer_size,
                 "unable to clear device stats", status);
    return 1;
  }

  blocks = (state_count + context->block_size - 1) / context->block_size;
  InitializeFrontierKernel<<<blocks, context->block_size>>>(
      context->device_problem, context->device_frontier_a, state_count);
  status = cudaGetLastError();
  if (status != cudaSuccess) {
    SetCudaError(error_buffer, error_buffer_size,
                 "initial frontier kernel launch failed", status);
    return 1;
  }
  status = cudaDeviceSynchronize();
  if (status != cudaSuccess) {
    SetCudaError(error_buffer, error_buffer_size,
                 "initial frontier kernel failed", status);
    return 1;
  }

  current = context->device_frontier_a;
  next = context->device_frontier_b;
  for (int coord = (int)problem->n - 2; coord >= 0; coord--) {
    status = cudaMemset(context->device_output_count, 0, sizeof(uint32_t));
    if (status != cudaSuccess) {
      SetCudaError(error_buffer, error_buffer_size,
                   "unable to reset output counter", status);
      return 1;
    }
    status = cudaMemset(context->device_overflow_flag, 0, sizeof(int));
    if (status != cudaSuccess) {
      SetCudaError(error_buffer, error_buffer_size,
                   "unable to reset overflow flag", status);
      return 1;
    }

    blocks = (state_count + context->block_size - 1) / context->block_size;
    ExpandFrontierKernel<<<blocks, context->block_size>>>(
        context->device_problem, coord, current, state_count,
        (coord == 0) ? context->device_points : next, point_capacity,
        context->device_output_count, context->device_overflow_flag,
        context->device_stats);
    status = cudaGetLastError();
    if (status != cudaSuccess) {
      SetCudaError(error_buffer, error_buffer_size,
                   "frontier expansion kernel launch failed", status);
      return 1;
    }
    status = cudaDeviceSynchronize();
    if (status != cudaSuccess) {
      SetCudaError(error_buffer, error_buffer_size,
                   "frontier expansion kernel failed", status);
      return 1;
    }

    status = cudaMemcpy(&state_count, context->device_output_count,
                        sizeof(state_count), cudaMemcpyDeviceToHost);
    if (status != cudaSuccess) {
      SetCudaError(error_buffer, error_buffer_size,
                   "unable to download output counter", status);
      return 1;
    }
    status = cudaMemcpy(&overflow_flag, context->device_overflow_flag,
                        sizeof(overflow_flag), cudaMemcpyDeviceToHost);
    if (status != cudaSuccess) {
      SetCudaError(error_buffer, error_buffer_size,
                   "unable to download overflow flag", status);
      return 1;
    }
    if (overflow_flag) {
      SetError(error_buffer, error_buffer_size,
               "device frontier exceeded output capacity");
      return 1;
    }
    if ((coord > 0) && (state_count > 0)) {
      int64_t (*swap)[kType3BoundsRuntimeMaxDimension] = current;

      current = next;
      next = swap;
    }
    if (state_count == 0)
      break;
  }

  *point_count = state_count;
  if (state_count > 0) {
    status = cudaMemcpy(points, context->device_points,
                        static_cast<size_t>(state_count) *
                            kType3BoundsRuntimeMaxDimension * sizeof(int64_t),
                        cudaMemcpyDeviceToHost);
    if (status != cudaSuccess) {
      SetCudaError(error_buffer, error_buffer_size,
                   "unable to download enumerated points", status);
      return 1;
    }
  }

  status = cudaMemcpy(&local_stats, context->device_stats, sizeof(local_stats),
                      cudaMemcpyDeviceToHost);
  if (status != cudaSuccess) {
    SetCudaError(error_buffer, error_buffer_size,
                 "unable to download frontier stats", status);
    return 1;
  }
  if (stats != nullptr)
    *stats = local_stats;
  if ((error_buffer != nullptr) && (error_buffer_size > 0))
    error_buffer[0] = '\0';
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
  if (context->device_output_count != nullptr) {
    cudaFree(context->device_output_count);
  }
  if (context->device_overflow_flag != nullptr) {
    cudaFree(context->device_overflow_flag);
  }
  if (context->device_stats != nullptr) {
    cudaFree(context->device_stats);
  }
  if (context->device_problem != nullptr) {
    cudaFree(context->device_problem);
  }
  if (context->device_frontier_a != nullptr) {
    cudaFree(context->device_frontier_a);
  }
  if (context->device_frontier_b != nullptr) {
    cudaFree(context->device_frontier_b);
  }
  if (context->device_points != nullptr) {
    cudaFree(context->device_points);
  }
  std::free(context);
}