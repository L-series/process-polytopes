#include "type3_bounds_runtime.h"

#include <cuda_runtime.h>

#include <atomic>
#include <condition_variable>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <exception>
#include <mutex>
#include <new>
#include <string>
#include <thread>
#include <vector>

struct Type3BoundsCudaEnumerationLane {
  cudaStream_t stream;
  uint32_t frontier_capacity;
  uint32_t host_point_capacity;
  uint32_t point_capacity;
  uint32_t *device_output_count;
  int *device_overflow_flag;
  int *device_prepare_status;
  Type3BoundsCudaStats *device_stats;
  Type3BoundsCudaCwsCandidate *device_candidate;
  Type3BoundsCudaProblem *device_problem;
  Type3BoundsCudaReduction *device_reduction;
  int64_t *host_point_staging;
  int64_t (*device_frontier_a)[kType3BoundsRuntimeMaxDimension];
  int64_t (*device_frontier_b)[kType3BoundsRuntimeMaxDimension];
  int64_t (*device_points)[kType3BoundsRuntimeMaxDimension];
  Type3BoundsCudaDevicePointBuffer *device_ip_point_buffer;
  int *device_ip_result;
  int *device_ip_error_flag;
};

struct Type3BoundsCudaBatchWorkers {
  std::mutex mutex;
  std::condition_variable cv;
  std::condition_variable done_cv;
  std::vector<std::thread> threads;
  std::atomic<uint32_t> next_index;
  std::atomic<int> failed;
  const Type3BoundsCudaProblem *problems;
  const Type3BoundsCudaCwsCandidate *candidates;
  const Type3BoundsCudaReduction *reductions;
  uint32_t problem_count;
  uint32_t point_capacity;
  int64_t **points;
  uint32_t *point_counts;
  Type3BoundsCudaPointBuffer *compact_outputs;
  Type3BoundsCudaDevicePointBuffer *device_outputs;
  int *ip_results;
  Type3BoundsCudaStats *stats;
  bool batch_active;
  bool stop;
  uint32_t pending;
  uint64_t generation;
  std::string first_error;

  Type3BoundsCudaBatchWorkers()
      : next_index(0),
        failed(0),
        problems(nullptr),
        candidates(nullptr),
        reductions(nullptr),
        problem_count(0),
        point_capacity(0),
        points(nullptr),
        point_counts(nullptr),
        compact_outputs(nullptr),
        device_outputs(nullptr),
        ip_results(nullptr),
        stats(nullptr),
        batch_active(false),
        stop(false),
        pending(0),
        generation(0) {}
};

struct Type3BoundsCudaContext {
  Type3BoundsJob *device_jobs;
  Type3BoundsResult *device_results;
  Type3BoundsCudaEquation *device_equation_batch;
  Type3BoundsCudaEquationTask *device_equation_tasks;
  Type3BoundsCudaIPState *device_ip_states;
  Type3BoundsCudaDevicePointBuffer *device_ip_point_buffers;
  int64_t *device_equation_points;
  uint8_t *device_equation_negative_flags;
  int *device_ip_results;
  int *device_ip_error_flag;
  Type3BoundsCudaCwsCandidate *device_candidate_batch;
  Type3BoundsCudaProblem *device_problem_batch;
  int *device_prepare_status_batch;
  Type3BoundsCudaWeightEntry *device_dim5_weights;
  Type3BoundsCudaDim5Structure3Candidate *device_dim5_structure3_candidates;
  uint32_t *device_dim5_structure3_candidate_counts;
  uint32_t candidate_batch_capacity;
  uint32_t equation_batch_capacity;
  uint32_t ip_state_capacity;
  uint32_t dim5_weight_capacity;
  uint32_t dim5_weight_count;
  uint32_t dim5_structure3_pair_capacity;
  uint32_t uploaded_equation_point_count;
  uint32_t uploaded_equation_point_stride;
  uint32_t capacity;
  uint32_t block_size;
  size_t equation_point_value_capacity;
  int64_t **device_output_pool;
  size_t *device_output_pool_value_capacities;
  uint32_t device_output_pool_capacity;
  int device_ordinal;
  uint32_t lane_count;
  Type3BoundsCudaEnumerationLane *lanes;
  Type3BoundsCudaBatchWorkers *batch_workers;
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

__device__ int PrefixIsCanonicalForStructure3(
    const Type3BoundsCudaWeightEntry *left, const int64_t prefix[3]) {
  int block_start = 0;

  while (block_start + 1 < 3) {
    int block_end = block_start + 1;

    while ((block_end < 3) &&
           (left->w[block_end] == left->w[block_start])) {
      if (prefix[block_end - 1] > prefix[block_end])
        return 0;
      block_end++;
    }
    block_start = block_end;
  }
  return 1;
}

__device__ int NextPermutation3(int64_t values[3]) {
  int left = 1;
  int right;
  int swap_index;
  int64_t tmp;

  while ((left >= 0) && (values[left] >= values[left + 1]))
    left--;
  if (left < 0)
    return 0;

  right = 2;
  while (values[left] >= values[right])
    right--;

  tmp = values[left];
  values[left] = values[right];
  values[right] = tmp;

  for (swap_index = left + 1, right = 2; swap_index < right;
       swap_index++, right--) {
    tmp = values[swap_index];
    values[swap_index] = values[right];
    values[right] = tmp;
  }
  return 1;
}

__device__ void DecodeUpperTriangularPair(uint64_t pair_index,
                                          uint32_t weight_count,
                                          uint32_t *left_index,
                                          uint32_t *right_index) {
  uint32_t low = 0;
  uint32_t high = weight_count;

  while (low < high) {
    uint32_t mid = low + (high - low) / 2;
    uint64_t row_start = static_cast<uint64_t>(mid) * weight_count -
                         (static_cast<uint64_t>(mid) * (mid - 1)) / 2;

    if (row_start <= pair_index)
      low = mid + 1;
    else
      high = mid;
  }

  *left_index = low - 1;
  {
    uint64_t row_start = static_cast<uint64_t>(*left_index) * weight_count -
                         (static_cast<uint64_t>(*left_index) *
                          (*left_index - 1)) /
                             2;
    *right_index = *left_index + (uint32_t)(pair_index - row_start);
  }
}

__device__ void StoreStructure3Candidate(
    const Type3BoundsCudaWeightEntry *left,
    const Type3BoundsCudaWeightEntry *right,
    const int64_t prefix[3],
    Type3BoundsCudaDim5Structure3Candidate *candidate) {
  candidate->left_d = left->d;
  candidate->right_d = right->d;
  for (int index = 0; index < 5; index++) {
    candidate->left_w[index] = left->w[index];
    candidate->right_w[index] = right->w[index];
  }
  for (int index = 0; index < 3; index++)
    candidate->right_w[index] = prefix[index];
}

enum {
  kType3PrepareOk = 0,
  kType3PrepareInvalidCandidate = 1,
  kType3PrepareNoX0 = 2,
  kType3PrepareConstraintOverflow = 3,
  kType3PrepareInitialFrontierOverflow = 4,
};

struct Type3BoundsDeviceBasis {
  int n;
  int N;
  int64_t x[kType3BoundsRuntimeMaxDimension][kType3BoundsRuntimeMaxAmbient];
};

__device__ int64_t GcdNonNegative(int64_t a, int64_t b) {
  a = (a < 0) ? -a : a;
  b = (b < 0) ? -b : b;
  if (b == 0)
    return a;
  while ((a %= b) != 0)
    if ((b %= a) == 0)
      return a;
  return b;
}

__device__ int64_t Egcd64(int64_t a0, int64_t a1,
                         int64_t *vout0, int64_t *vout1) {
  int64_t original0 = a0;
  int64_t original1 = a1;
  int64_t a2;
  int64_t x0 = 1;
  int64_t x1 = 0;
  int64_t x2 = 0;

  while ((a2 = a0 % a1) != 0) {
    x2 = x0 - x1 * (a0 / a1);
    a0 = a1;
    a1 = a2;
    x0 = x1;
    x1 = x2;
  }
  *vout0 = x1;
  *vout1 = (a1 - original0 * x1) / original1;
  return a1;
}

__device__ int64_t RoundQ64(int64_t n, int64_t d) {
  int64_t floor_value;

  if (d < 0) {
    d = -d;
    n = -n;
  }
  floor_value = n / d;
  return floor_value + (2 * (n - floor_value * d)) / d;
}

__device__ int64_t WToGLZDevice(
    int64_t *weights, int *dimension,
    int64_t glz[kType3BoundsRuntimeMaxAmbient][kType3BoundsRuntimeMaxAmbient]) {
  int i, j;
  int64_t gcd_value;
  int64_t *e = glz[0];
  int64_t *b = glz[1];

  for (i = 0; i < *dimension; i++) {
    if (weights[i] == 0)
      return 0;
  }
  for (i = 1; i < *dimension; i++)
    for (j = 0; j < *dimension; j++)
      glz[i][j] = 0;
  gcd_value = Egcd64(weights[0], weights[1], &e[0], &e[1]);
  b[0] = -weights[1] / gcd_value;
  b[1] = weights[0] / gcd_value;
  for (i = 2; i < *dimension; i++) {
    int64_t a;
    int64_t bb;
    int64_t g = Egcd64(gcd_value, weights[i], &a, &bb);

    b = glz[i];
    b[i] = gcd_value / g;
    gcd_value = weights[i] / g;
    for (j = 0; j < i; j++)
      b[j] = -e[j] * gcd_value;
    for (j = 0; j < i; j++)
      e[j] *= a;
    e[j] = bb;
    for (j = i - 1; j > 0; j--) {
      int n;
      int64_t *y = glz[j];
      int64_t round_b = RoundQ64(b[j], y[j]);
      int64_t round_e = RoundQ64(e[j], y[j]);

      for (n = 0; n <= j; n++) {
        b[n] -= round_b * y[n];
        e[n] -= round_e * y[n];
      }
    }
    gcd_value = g;
  }
  return gcd_value;
}

__device__ int SolveNextWeightEquationDevice(
    const int64_t *next_weight, int ambient_count,
    Type3BoundsDeviceBasis *basis) {
  int i, j, p_count = 0;
  int p[kType3BoundsRuntimeMaxAmbient];
  int64_t weights[kType3BoundsRuntimeMaxAmbient];
  int64_t *x_rows[kType3BoundsRuntimeMaxAmbient];
  int64_t glz[kType3BoundsRuntimeMaxAmbient][kType3BoundsRuntimeMaxAmbient];

  basis->n = ambient_count - 1;
  basis->N = ambient_count;
  for (i = 0; i < ambient_count; i++) {
    for (j = 0; j < basis->n; j++)
      basis->x[j][i] = 0;
    if (next_weight[i] != 0) {
      p[p_count] = i;
      x_rows[p_count] = glz[p_count];
      weights[p_count++] = next_weight[i];
    }
  }
  if (p_count <= 0)
    return 0;

  if (p_count > 1)
    WToGLZDevice(weights, &p_count, glz);
  else {
    for (i = 0; i < p[0]; i++)
      basis->x[i][i] = 1;
    while ((++i) < ambient_count)
      basis->x[i - 1][i] = 1;
    return 1;
  }

  for (i = 1; i < p_count; i++)
    if (x_rows[i][i] < 0)
      for (j = 0; j <= i; j++)
        x_rows[i][j] *= -1;
  for (i = 0; i < p[0]; i++)
    basis->x[i][i] = 1;
  while ((++i) < p[1])
    basis->x[i - 1][i] = 1;
  basis->x[i - 1][p[0]] = x_rows[1][0];
  basis->x[i - 1][p[1]] = x_rows[1][1];
  j = 2;
  while (++i < ambient_count) {
    if (next_weight[i] != 0) {
      int k;
      int64_t *row = basis->x[i - 1];

      for (k = 0; k <= j; k++)
        row[p[k]] = x_rows[j][k];
      j++;
    } else
      basis->x[i - 1][i] = 1;
  }
  return 1;
}

__device__ int MakeCwsBasisDevice(const Type3BoundsCudaCwsCandidate *candidate,
                                  Type3BoundsDeviceBasis *basis) {
  int i, j, k, l;
  int64_t projected_weight[kType3BoundsRuntimeMaxAmbient];
  Type3BoundsDeviceBasis next_basis, accum_basis;

  accum_basis.N = basis->N = (int)candidate->ambient_count;
  if (!SolveNextWeightEquationDevice(candidate->W[0], (int)candidate->ambient_count,
                                     basis))
    return 0;
  for (i = 1; i < (int)candidate->nw; i++) {
    for (j = 0; j < basis->n; j++) {
      projected_weight[j] = 0;
      for (k = 0; k < basis->N; k++)
        projected_weight[j] += candidate->W[i][k] * basis->x[j][k];
    }
    if (!SolveNextWeightEquationDevice(projected_weight, basis->n, &next_basis))
      return 0;
    accum_basis.n = next_basis.n;
    for (j = 0; j < accum_basis.N; j++)
      for (k = 0; k < accum_basis.n; k++) {
        accum_basis.x[k][j] = 0;
        for (l = 0; l < next_basis.N; l++)
          accum_basis.x[k][j] += next_basis.x[k][l] * basis->x[l][j];
      }
    *basis = accum_basis;
  }
  return 1;
}

__device__ int ComputeX0RecursiveDevice(
    int coord, const Type3BoundsCudaCwsCandidate *candidate,
    int64_t *work_degree, int64_t *x0) {
  int j;
  int64_t xmax = 0;

  if (coord == 0) {
    for (j = 0; j < (int)candidate->nw; j++)
      if (candidate->W[j][0] != 0) {
        int64_t value;

        if ((work_degree[j] % candidate->W[j][0]) != 0)
          return 0;
        value = work_degree[j] / candidate->W[j][0];
        if (xmax != 0) {
          if (xmax != value)
            return 0;
        } else
          xmax = value;
      } else if (work_degree[j] != 0)
        return 0;
    x0[0] = xmax;
    return 1;
  }

  for (j = 0; j < (int)candidate->nw; j++)
    if (candidate->W[j][coord] != 0) {
      int64_t value = work_degree[j] / candidate->W[j][coord];

      if (xmax != 0) {
        if (value < xmax)
          xmax = value;
      } else
        xmax = value;
    }

  for (x0[coord] = 0; x0[coord] <= xmax; x0[coord]++) {
    if (ComputeX0RecursiveDevice(coord - 1, candidate, work_degree, x0)) {
      for (j = 0; j < (int)candidate->nw; j++)
        work_degree[j] += x0[coord] * candidate->W[j][coord];
      return 1;
    }
    for (j = 0; j < (int)candidate->nw; j++)
      work_degree[j] -= candidate->W[j][coord];
  }
  for (j = 0; j < (int)candidate->nw; j++)
    work_degree[j] += (xmax + 1) * candidate->W[j][coord];
  return 0;
}

__device__ unsigned int MaxConstraintCountDevice(const int32_t *amin, int n) {
  unsigned int max_constraints = 0;

  for (int coord = 0; coord < (n - 1); coord++) {
    int segment_size = amin[coord + 1] - amin[coord];

    if ((segment_size > 1) &&
        ((unsigned int)(segment_size - 1) > max_constraints))
      max_constraints = (unsigned int)(segment_size - 1);
  }
  return max_constraints;
}

__device__ int PrepareProblemFromCandidateDevice(
    const Type3BoundsCudaCwsCandidate *candidate,
    Type3BoundsCudaProblem *problem) {
  Type3BoundsDeviceBasis basis;
  int64_t x0[kType3BoundsRuntimeMaxAmbient];
  int64_t xmax[kType3BoundsRuntimeMaxAmbient];
  int64_t work_degree[kType3BoundsRuntimeMaxWeightSystems];
  int i, j;
  int64_t low, upp, r, bound;

  if ((candidate->nw == 0) ||
      (candidate->nw > kType3BoundsRuntimeMaxWeightSystems) ||
      (candidate->ambient_count == 0) ||
      (candidate->ambient_count > kType3BoundsRuntimeMaxAmbient) ||
      (candidate->ambient_count <= candidate->nw) ||
      ((candidate->ambient_count - candidate->nw) >
       kType3BoundsRuntimeMaxDimension))
    return kType3PrepareInvalidCandidate;

  if (!MakeCwsBasisDevice(candidate, &basis))
    return kType3PrepareInvalidCandidate;

  for (i = 0; i < (int)candidate->ambient_count; i++)
    x0[i] = 0;
  if (candidate->index == 1) {
    for (i = 0; i < (int)candidate->ambient_count; i++)
      x0[i] = 1;
  } else {
    for (i = 0; i < (int)candidate->nw; i++)
      work_degree[i] = candidate->d[i];
    if (!ComputeX0RecursiveDevice((int)candidate->ambient_count - 1, candidate,
                                  work_degree, x0))
      return kType3PrepareNoX0;
  }

  for (i = 0; i < (int)candidate->ambient_count; i++) {
    xmax[i] = 0;
    for (j = 0; j < (int)candidate->nw; j++)
      if (candidate->W[j][i] != 0) {
        int64_t value = candidate->d[j] / candidate->W[j][i];

        if (xmax[i] != 0) {
          if (value < xmax[i])
            xmax[i] = value;
        } else
          xmax[i] = value;
      }
  }

  for (i = 0; i <= basis.n; i++)
    problem->Amin[i] = 0;
  i = basis.n;
  problem->n = (uint32_t)basis.n;
  problem->ambient_count = (uint32_t)basis.N;
  problem->Amin[0] = 0;
  problem->Amin[basis.n] = j = basis.N;
  while (--i) {
    while (!basis.x[i - 1][--j])
      ;
    problem->Amin[i] = ++j;
  }
  if (MaxConstraintCountDevice(problem->Amin, basis.n) >
      kType3BoundsMaxConstraints)
    return kType3PrepareConstraintOverflow;

  for (i = 0; i < basis.n; i++)
    for (j = 0; j < basis.N; j++)
      problem->basis[i][j] = basis.x[i][j];
  for (i = 0; i < basis.N; i++) {
    problem->X0[i] = x0[i];
    problem->Xmax[i] = xmax[i];
  }

  j = basis.n - 1;
  i = problem->Amin[j + 1] - 1;
  r = basis.x[j][i];
  if (r == 1) {
    problem->initial_xmin = -x0[i];
    problem->initial_xmax = xmax[i] - x0[i];
  } else {
    problem->initial_xmin = -Type3FloorDiv(x0[i], r);
    problem->initial_xmax = Type3FloorDiv(xmax[i] - x0[i], r);
  }
  while ((i--) > problem->Amin[j]) {
    low = -x0[i];
    upp = low + xmax[i];
    r = basis.x[basis.n - 1][i];
    if (r > 0) {
      if (r == 1) {
        if (problem->initial_xmax > upp)
          problem->initial_xmax = upp;
        if (problem->initial_xmin < low)
          problem->initial_xmin = low;
      } else {
        if (problem->initial_xmax >
            (bound = Type3FloorDiv(upp, r)))
          problem->initial_xmax = bound;
        if (problem->initial_xmin <
            (bound = -Type3FloorDiv(-low, r)))
          problem->initial_xmin = bound;
      }
    } else {
      if (r == -1) {
        if (problem->initial_xmax > (-low))
          problem->initial_xmax = -low;
        if (problem->initial_xmin < (-upp))
          problem->initial_xmin = -upp;
      } else {
        if (problem->initial_xmax >
            (bound = Type3FloorDiv(-low, -r)))
          problem->initial_xmax = bound;
        if (problem->initial_xmin <
            (bound = -Type3FloorDiv(upp, -r)))
          problem->initial_xmin = bound;
      }
    }
  }

  return kType3PrepareOk;
}

__global__ void PrepareProblemFromCwsKernel(
    const Type3BoundsCudaCwsCandidate *candidate,
    Type3BoundsCudaProblem *problem,
    int *prepare_status,
    uint32_t *state_count,
    int64_t (*frontier)[kType3BoundsRuntimeMaxDimension],
    uint32_t frontier_capacity) {
  __shared__ int shared_status;
  __shared__ uint32_t shared_state_count;

  if (blockIdx.x != 0)
    return;

  if (threadIdx.x == 0) {
    shared_status = PrepareProblemFromCandidateDevice(candidate, problem);
    shared_state_count = 0;
    if (shared_status == kType3PrepareOk) {
      if (problem->initial_xmax >= problem->initial_xmin) {
        uint64_t initial_state_total =
            (uint64_t)(problem->initial_xmax - problem->initial_xmin + 1);

        if (initial_state_total > frontier_capacity)
          shared_status = kType3PrepareInitialFrontierOverflow;
        else
          shared_state_count = (uint32_t)initial_state_total;
      }
    }
    *prepare_status = shared_status;
    *state_count = shared_state_count;
  }
  __syncthreads();

  if (shared_status != kType3PrepareOk)
    return;

  for (uint32_t index = threadIdx.x; index < shared_state_count;
       index += blockDim.x) {
    for (uint32_t coord = 0; coord < problem->n; coord++)
      frontier[index][coord] = 0;
    frontier[index][problem->n - 1] =
        problem->initial_xmin + static_cast<int64_t>(index);
  }
}

__global__ void PrepareProblemBatchFromCwsKernel(
    const Type3BoundsCudaCwsCandidate *candidates, uint32_t candidate_count,
    Type3BoundsCudaProblem *problems, int *prepare_statuses,
    uint32_t point_capacity) {
  uint32_t index = blockIdx.x * blockDim.x + threadIdx.x;
  int local_status;

  if (index >= candidate_count)
    return;

  local_status = PrepareProblemFromCandidateDevice(&candidates[index],
                                                   &problems[index]);
  if (local_status == kType3PrepareOk) {
    if (problems[index].initial_xmax >= problems[index].initial_xmin) {
      uint64_t initial_state_total =
          (uint64_t)(problems[index].initial_xmax -
                     problems[index].initial_xmin + 1);

      if (initial_state_total > point_capacity)
        local_status = kType3PrepareInitialFrontierOverflow;
    }
  }
  prepare_statuses[index] = local_status;
}

uint32_t ParseUnsignedEnv(const char *name, uint32_t default_value) {
  const char *value = std::getenv(name);
  char *end = nullptr;
  unsigned long parsed;

  if ((value == nullptr) || (*value == '\0'))
    return default_value;
  parsed = std::strtoul(value, &end, 10);
  if ((end == value) || (*end != '\0'))
    return default_value;
  if (parsed == 0)
    return 1;
  if (parsed > 32)
    return 32;
  return static_cast<uint32_t>(parsed);
}

void SetError(char *buffer, size_t buffer_size, const char *message) {
  if ((buffer == nullptr) || (buffer_size == 0))
    return;
  std::snprintf(buffer, buffer_size, "%s", message);
}

void SetCudaError(char *buffer, size_t buffer_size, const char *message,
                  cudaError_t status) {
  if ((buffer == nullptr) || (buffer_size == 0))
    return;
  std::snprintf(buffer, buffer_size, "%s: %s", message,
                cudaGetErrorString(status));
}

int EnsureCudaPreparationStack(char *error_buffer, size_t error_buffer_size) {
  cudaError_t status;
  size_t stack_limit = 0;

  status = cudaDeviceGetLimit(&stack_limit, cudaLimitStackSize);
  if (status != cudaSuccess) {
    SetCudaError(error_buffer, error_buffer_size,
                 "unable to query CUDA stack size limit", status);
    return 1;
  }
  if (stack_limit >= (64u * 1024u))
    return 0;

  status = cudaDeviceSetLimit(cudaLimitStackSize, 64u * 1024u);
  if (status != cudaSuccess) {
    SetCudaError(error_buffer, error_buffer_size,
                 "unable to raise CUDA stack size limit", status);
    return 1;
  }
  return 0;
}

int EnumerateOnLane(Type3BoundsCudaContext *context,
                    Type3BoundsCudaEnumerationLane *lane,
                    const Type3BoundsCudaProblem *problem,
          const Type3BoundsCudaReduction *reduction,
                    uint32_t point_capacity, int64_t *points,
                    uint32_t *point_count,
                    Type3BoundsCudaDevicePointBuffer *resident_output,
                    Type3BoundsCudaStats *stats,
                    char *error_buffer, size_t error_buffer_size);

int EnumerateCwsOnLane(Type3BoundsCudaContext *context,
                       Type3BoundsCudaEnumerationLane *lane,
                       const Type3BoundsCudaCwsCandidate *candidate,
                       const Type3BoundsCudaReduction *reduction,
                       uint32_t point_capacity, int64_t *points,
                       uint32_t *point_count,
                       Type3BoundsCudaDevicePointBuffer *resident_output,
                       Type3BoundsCudaStats *stats,
                       char *error_buffer, size_t error_buffer_size);

int EnumerateSynchronously(Type3BoundsCudaContext *context,
                           Type3BoundsCudaEnumerationLane *lane,
                           const Type3BoundsCudaProblem *problem,
            const Type3BoundsCudaReduction *reduction,
                           uint32_t point_capacity, int64_t *points,
                           uint32_t *point_count,
                           Type3BoundsCudaDevicePointBuffer *resident_output,
                           Type3BoundsCudaStats *stats,
                           char *error_buffer, size_t error_buffer_size);

int EnumerateCwsSynchronously(Type3BoundsCudaContext *context,
                              Type3BoundsCudaEnumerationLane *lane,
                              const Type3BoundsCudaCwsCandidate *candidate,
                              const Type3BoundsCudaReduction *reduction,
                              uint32_t point_capacity, int64_t *points,
                              uint32_t *point_count,
                              Type3BoundsCudaDevicePointBuffer *resident_output,
                              Type3BoundsCudaStats *stats,
                              char *error_buffer,
                              size_t error_buffer_size);

__global__ void RunIPCheckDeviceBatchKernel(
  const Type3BoundsCudaDevicePointBuffer *point_buffers,
  uint32_t state_count,
  int *results,
  int *error_flag);

void DestroyEnumerationLane(Type3BoundsCudaEnumerationLane *lane) {
  if (lane == nullptr)
    return;
  if (lane->device_output_count != nullptr)
    cudaFree(lane->device_output_count);
  if (lane->device_overflow_flag != nullptr)
    cudaFree(lane->device_overflow_flag);
  if (lane->device_prepare_status != nullptr)
    cudaFree(lane->device_prepare_status);
  if (lane->device_stats != nullptr)
    cudaFree(lane->device_stats);
  if (lane->device_candidate != nullptr)
    cudaFree(lane->device_candidate);
  if (lane->device_problem != nullptr)
    cudaFree(lane->device_problem);
  if (lane->device_reduction != nullptr)
    cudaFree(lane->device_reduction);
  if (lane->host_point_staging != nullptr)
    std::free(lane->host_point_staging);
  if (lane->device_frontier_a != nullptr)
    cudaFree(lane->device_frontier_a);
  if (lane->device_frontier_b != nullptr)
    cudaFree(lane->device_frontier_b);
  if (lane->device_points != nullptr)
    cudaFree(lane->device_points);
  if (lane->device_ip_point_buffer != nullptr)
    cudaFree(lane->device_ip_point_buffer);
  if (lane->device_ip_result != nullptr)
    cudaFree(lane->device_ip_result);
  if (lane->device_ip_error_flag != nullptr)
    cudaFree(lane->device_ip_error_flag);
  if (lane->stream != nullptr)
    cudaStreamDestroy(lane->stream);
  std::memset(lane, 0, sizeof(*lane));
}

void DestroyBatchWorkers(Type3BoundsCudaContext *context) {
  Type3BoundsCudaBatchWorkers *workers;

  if ((context == nullptr) || (context->batch_workers == nullptr))
    return;

  workers = context->batch_workers;
  {
    std::lock_guard<std::mutex> lock(workers->mutex);

    workers->stop = true;
  }
  workers->cv.notify_all();
  for (std::thread &thread : workers->threads)
    if (thread.joinable())
      thread.join();
  delete workers;
  context->batch_workers = nullptr;
}

void DestroyContext(Type3BoundsCudaContext *context) {
  if (context == nullptr)
    return;
  if (context->device_output_pool != nullptr) {
    for (uint32_t index = 0; index < context->device_output_pool_capacity;
         index++)
      if (context->device_output_pool[index] != nullptr)
        cudaFree(context->device_output_pool[index]);
    std::free(context->device_output_pool);
  }
  if (context->device_output_pool_value_capacities != nullptr)
    std::free(context->device_output_pool_value_capacities);
  if (context == nullptr)
    return;
  DestroyBatchWorkers(context);
  if (context->device_equation_batch != nullptr)
    cudaFree(context->device_equation_batch);
  if (context->device_equation_tasks != nullptr)
    cudaFree(context->device_equation_tasks);
  if (context->device_ip_states != nullptr)
    cudaFree(context->device_ip_states);
  if (context->device_ip_point_buffers != nullptr)
    cudaFree(context->device_ip_point_buffers);
  if (context->device_equation_points != nullptr)
    cudaFree(context->device_equation_points);
  if (context->device_equation_negative_flags != nullptr)
    cudaFree(context->device_equation_negative_flags);
  if (context->device_ip_results != nullptr)
    cudaFree(context->device_ip_results);
  if (context->device_ip_error_flag != nullptr)
    cudaFree(context->device_ip_error_flag);
  if (context->device_dim5_weights != nullptr)
    cudaFree(context->device_dim5_weights);
  if (context->device_dim5_structure3_candidates != nullptr)
    cudaFree(context->device_dim5_structure3_candidates);
  if (context->device_dim5_structure3_candidate_counts != nullptr)
    cudaFree(context->device_dim5_structure3_candidate_counts);
  if (context->device_candidate_batch != nullptr)
    cudaFree(context->device_candidate_batch);
  if (context->device_problem_batch != nullptr)
    cudaFree(context->device_problem_batch);
  if (context->device_prepare_status_batch != nullptr)
    cudaFree(context->device_prepare_status_batch);
  if (context->device_jobs != nullptr)
    cudaFree(context->device_jobs);
  if (context->device_results != nullptr)
    cudaFree(context->device_results);
  if (context->lanes != nullptr) {
    for (uint32_t index = 0; index < context->lane_count; index++)
      DestroyEnumerationLane(&context->lanes[index]);
    std::free(context->lanes);
  }
  std::free(context);
}

void RecordBatchFailure(Type3BoundsCudaBatchWorkers *workers,
                        const char *message) {
  std::lock_guard<std::mutex> lock(workers->mutex);

  if (!workers->failed.exchange(1))
    workers->first_error =
        (message != nullptr) ? message : "unknown batch failure";
}

int CopyCompactPointsFromLane(Type3BoundsCudaEnumerationLane *lane,
                              uint32_t point_count,
                              Type3BoundsCudaPointBuffer *output,
                              char *error_buffer,
                              size_t error_buffer_size) {
  size_t point_value_count;
  int64_t *copied_points;

  if (output == nullptr) {
    SetError(error_buffer, error_buffer_size,
             "invalid compact point output buffer");
    return 1;
  }

  output->points = nullptr;
  output->point_count = point_count;
  if (point_count == 0)
    return 0;

  point_value_count = static_cast<size_t>(point_count) *
                      kType3BoundsRuntimeMaxDimension;
  copied_points = static_cast<int64_t *>(
      std::malloc(point_value_count * sizeof(int64_t)));
  if (copied_points == nullptr) {
    SetError(error_buffer, error_buffer_size,
             "unable to allocate compact host point buffer");
    return 1;
  }

  std::memcpy(copied_points, lane->host_point_staging,
              point_value_count * sizeof(int64_t));
  output->points = copied_points;
  return 0;
}

void FreeCompactPointBuffers(Type3BoundsCudaPointBuffer *outputs,
                             uint32_t output_count) {
  if (outputs == nullptr)
    return;

  for (uint32_t index = 0; index < output_count; index++) {
    std::free(outputs[index].points);
    outputs[index].points = nullptr;
    outputs[index].point_count = 0;
  }
}

void FreeDevicePointBuffers(Type3BoundsCudaDevicePointBuffer *outputs,
                            uint32_t output_count) {
  if (outputs == nullptr)
    return;

  for (uint32_t index = 0; index < output_count; index++) {
    if (outputs[index].points != nullptr)
      cudaFree(outputs[index].points);
    outputs[index].points = nullptr;
    outputs[index].point_count = 0;
    outputs[index].point_dimension = 0;
    outputs[index].point_stride = 0;
  }
}

cudaError_t EnsureDeviceOutputPoolCapacity(Type3BoundsCudaContext *context,
                                           uint32_t output_count) {
  int64_t **new_pool;
  size_t *new_capacities;
  uint32_t index;

  if (output_count <= context->device_output_pool_capacity)
    return cudaSuccess;

  new_pool = static_cast<int64_t **>(std::realloc(
      context->device_output_pool,
      static_cast<size_t>(output_count) * sizeof(*new_pool)));
  if (new_pool == nullptr)
    return cudaErrorMemoryAllocation;
  context->device_output_pool = new_pool;

  new_capacities = static_cast<size_t *>(std::realloc(
      context->device_output_pool_value_capacities,
      static_cast<size_t>(output_count) * sizeof(*new_capacities)));
  if (new_capacities == nullptr)
    return cudaErrorMemoryAllocation;
  context->device_output_pool_value_capacities = new_capacities;

  for (index = context->device_output_pool_capacity; index < output_count;
       index++) {
    context->device_output_pool[index] = nullptr;
    context->device_output_pool_value_capacities[index] = 0;
  }
  context->device_output_pool_capacity = output_count;
  return cudaSuccess;
}

cudaError_t EnsureDeviceOutputBufferCapacity(Type3BoundsCudaContext *context,
                                             uint32_t output_index,
                                             size_t point_value_count) {
  cudaError_t status;
  int64_t *new_buffer;

  status = EnsureDeviceOutputPoolCapacity(context, output_index + 1);
  if (status != cudaSuccess)
    return status;
  if (point_value_count <=
      context->device_output_pool_value_capacities[output_index])
    return cudaSuccess;

  new_buffer = nullptr;
  status = cudaMalloc(&new_buffer, point_value_count * sizeof(int64_t));
  if (status != cudaSuccess)
    return status;
  if (context->device_output_pool[output_index] != nullptr)
    cudaFree(context->device_output_pool[output_index]);
  context->device_output_pool[output_index] = new_buffer;
  context->device_output_pool_value_capacities[output_index] = point_value_count;
  return cudaSuccess;
}

cudaError_t EnsureLaneIPCheckCapacity(Type3BoundsCudaEnumerationLane *lane) {
  cudaError_t status;

  if (lane->device_ip_point_buffer == nullptr) {
    status = cudaMalloc(&lane->device_ip_point_buffer,
                        sizeof(Type3BoundsCudaDevicePointBuffer));
    if (status != cudaSuccess)
      return status;
  }
  if (lane->device_ip_result == nullptr) {
    status = cudaMalloc(&lane->device_ip_result, sizeof(int));
    if (status != cudaSuccess)
      return status;
  }
  if (lane->device_ip_error_flag == nullptr) {
    status = cudaMalloc(&lane->device_ip_error_flag, sizeof(int));
    if (status != cudaSuccess)
      return status;
  }

  return cudaSuccess;
}

int PrepareDirectDeviceOutputBuffer(Type3BoundsCudaContext *context,
                                    uint32_t output_index,
                                    uint32_t point_capacity,
                                    uint32_t point_dimension,
                                    Type3BoundsCudaDevicePointBuffer *output,
                                    char *error_buffer,
                                    size_t error_buffer_size) {
  cudaError_t status;
  size_t point_value_count;

  if (output == nullptr) {
    SetError(error_buffer, error_buffer_size,
             "invalid direct device point output buffer");
    return 1;
  }
  if ((point_dimension == 0) ||
      (point_dimension > kType3BoundsRuntimeMaxDimension)) {
    SetError(error_buffer, error_buffer_size,
             "invalid point dimension for direct device output");
    return 1;
  }

  point_value_count = static_cast<size_t>(point_capacity) *
                      kType3BoundsRuntimeMaxDimension;
  status = EnsureDeviceOutputBufferCapacity(context, output_index,
                                            point_value_count);
  if (status != cudaSuccess) {
    SetCudaError(error_buffer, error_buffer_size,
                 "unable to reserve direct device point buffer", status);
    return 1;
  }

  output->points = context->device_output_pool[output_index];
  output->point_count = 0;
  output->point_dimension = point_dimension;
  output->point_stride = kType3BoundsRuntimeMaxDimension;
  return 0;
}

int CopyDevicePointsFromLane(Type3BoundsCudaContext *context,
                             Type3BoundsCudaEnumerationLane *lane,
                             uint32_t output_index,
                             uint32_t point_count,
                             uint32_t point_dimension,
                             Type3BoundsCudaDevicePointBuffer *output,
                             char *error_buffer,
                             size_t error_buffer_size) {
  cudaError_t status;
  size_t point_value_count;

  if (output == nullptr) {
    SetError(error_buffer, error_buffer_size,
             "invalid device point output buffer");
    return 1;
  }

  output->points = nullptr;
  output->point_count = point_count;
  output->point_dimension = point_dimension;
  output->point_stride = kType3BoundsRuntimeMaxDimension;
  if (point_count == 0)
    return 0;

  point_value_count = static_cast<size_t>(point_count) *
                      kType3BoundsRuntimeMaxDimension;
  status = EnsureDeviceOutputBufferCapacity(context, output_index,
                                            point_value_count);
  if (status != cudaSuccess) {
    SetCudaError(error_buffer, error_buffer_size,
                 "unable to reserve compact device point buffer", status);
    return 1;
  }
  status = cudaMemcpy(context->device_output_pool[output_index],
                      lane->device_points,
                      point_value_count * sizeof(int64_t),
                      cudaMemcpyDeviceToDevice);
  if (status != cudaSuccess) {
    SetCudaError(error_buffer, error_buffer_size,
                 "unable to copy compact device point buffer", status);
    return 1;
  }

  output->points = context->device_output_pool[output_index];
  return 0;
}

int RunResidentIPCheckOnLane(Type3BoundsCudaContext *context,
                             Type3BoundsCudaEnumerationLane *lane,
                             const Type3BoundsCudaDevicePointBuffer *point_buffer,
                             int *result,
                             char *error_buffer,
                             size_t error_buffer_size) {
  cudaError_t status;
  uint32_t block_size;
  int device_error = 0;

  if ((point_buffer == nullptr) || (result == nullptr)) {
    SetError(error_buffer, error_buffer_size,
             "invalid resident CUDA IP-check args");
    return 1;
  }
  if ((point_buffer->point_dimension == 0) ||
      (point_buffer->point_dimension > kType3BoundsRuntimeMaxDimension) ||
      (point_buffer->point_stride < point_buffer->point_dimension) ||
      ((point_buffer->point_count != 0) && (point_buffer->points == nullptr))) {
    SetError(error_buffer, error_buffer_size,
             "invalid resident CUDA IP-check payload");
    return 1;
  }

  status = EnsureLaneIPCheckCapacity(lane);
  if (status != cudaSuccess) {
    SetCudaError(error_buffer, error_buffer_size,
                 "unable to reserve resident CUDA IP-check buffers", status);
    return 1;
  }
  status = cudaMemcpyAsync(lane->device_ip_point_buffer, point_buffer,
                           sizeof(*point_buffer), cudaMemcpyHostToDevice,
                           lane->stream);
  if (status != cudaSuccess) {
    SetCudaError(error_buffer, error_buffer_size,
                 "unable to upload resident CUDA IP-check descriptor", status);
    return 1;
  }
  status = cudaMemsetAsync(lane->device_ip_error_flag, 0, sizeof(int),
                           lane->stream);
  if (status != cudaSuccess) {
    SetCudaError(error_buffer, error_buffer_size,
                 "unable to reset resident CUDA IP-check error flag", status);
    return 1;
  }

  block_size = context->block_size;
  if ((block_size == 0) || (block_size > 256))
    block_size = 256;
  RunIPCheckDeviceBatchKernel<<<1, block_size, 0, lane->stream>>>(
      lane->device_ip_point_buffer, 1, lane->device_ip_result,
      lane->device_ip_error_flag);
  status = cudaGetLastError();
  if (status != cudaSuccess) {
    SetCudaError(error_buffer, error_buffer_size,
                 "resident CUDA IP-check kernel launch failed", status);
    return 1;
  }
  status = cudaStreamSynchronize(lane->stream);
  if (status != cudaSuccess) {
    SetCudaError(error_buffer, error_buffer_size,
                 "resident CUDA IP-check kernel failed", status);
    return 1;
  }

  status = cudaMemcpy(&device_error, lane->device_ip_error_flag,
                      sizeof(device_error), cudaMemcpyDeviceToHost);
  if (status != cudaSuccess) {
    SetCudaError(error_buffer, error_buffer_size,
                 "unable to download resident CUDA IP-check error flag",
                 status);
    return 1;
  }
  if (device_error != 0) {
    SetError(error_buffer, error_buffer_size,
             "resident CUDA IP-check overflowed bounded geometry state");
    return 1;
  }
  status = cudaMemcpy(result, lane->device_ip_result, sizeof(*result),
                      cudaMemcpyDeviceToHost);
  if (status != cudaSuccess) {
    SetCudaError(error_buffer, error_buffer_size,
                 "unable to download resident CUDA IP-check result", status);
    return 1;
  }

  if ((error_buffer != nullptr) && (error_buffer_size > 0))
    error_buffer[0] = '\0';
  return 0;
}

void BatchWorkerLoop(Type3BoundsCudaContext *context, uint32_t worker_index) {
  Type3BoundsCudaBatchWorkers *workers = context->batch_workers;
  Type3BoundsCudaEnumerationLane *lane = &context->lanes[worker_index];
  uint64_t generation = 0;

  for (;;) {
    std::unique_lock<std::mutex> lock(workers->mutex);

    workers->cv.wait(lock, [workers, generation] {
      return workers->stop ||
             (workers->batch_active && (workers->generation != generation));
    });
    if (workers->stop)
      return;
    generation = workers->generation;
    lock.unlock();

    {
      char local_error[256] = {0};
      cudaError_t status = cudaSetDevice(context->device_ordinal);

      if (status != cudaSuccess) {
        std::string error = std::string("unable to select CUDA device: ") +
                            cudaGetErrorString(status);

        RecordBatchFailure(workers, error.c_str());
      } else {
        while (!workers->failed.load()) {
          uint32_t index = workers->next_index.fetch_add(1);
          int use_compact_output = workers->compact_outputs != nullptr;
          int use_device_output = workers->device_outputs != nullptr;
          int use_fused_ip = workers->ip_results != nullptr;
          uint32_t point_count = 0;
          uint32_t point_dimension = 0;
          Type3BoundsCudaDevicePointBuffer resident_output = {};

          if (index >= workers->problem_count)
            break;
          if (!use_compact_output && !use_device_output && !use_fused_ip &&
              (workers->points[index] == nullptr)) {
            RecordBatchFailure(workers, "null host point buffer");
            break;
          }
          if (use_device_output) {
            if (workers->candidates != nullptr)
              point_dimension = workers->candidates[index].ambient_count -
                                workers->candidates[index].nw;
            else
              point_dimension = workers->problems[index].n;
            if (PrepareDirectDeviceOutputBuffer(
                    context, index, workers->point_capacity, point_dimension,
                    &resident_output, local_error,
                    sizeof(local_error)) != 0) {
              RecordBatchFailure(workers,
                                 local_error[0] ? local_error
                                                : "unable to reserve direct device output buffer");
              break;
            }
          }
            if (((workers->candidates != nullptr) &&
               (EnumerateCwsOnLane(context, lane, &workers->candidates[index],
                         (workers->reductions != nullptr)
                           ? &workers->reductions[index]
                           : nullptr,
                         workers->point_capacity,
                         (use_compact_output || use_device_output || use_fused_ip)
                             ? nullptr
                             : workers->points[index],
                         (use_compact_output || use_device_output || use_fused_ip)
                           ? &point_count
                           : &workers->point_counts[index],
                         (use_fused_ip || use_device_output)
                             ? &resident_output
                             : nullptr,
                         (workers->stats != nullptr)
                           ? &workers->stats[index]
                           : nullptr,
                         local_error, sizeof(local_error)) != 0)) ||
              ((workers->candidates == nullptr) &&
               (EnumerateOnLane(context, lane, &workers->problems[index],
                      (workers->reductions != nullptr)
                        ? &workers->reductions[index]
                        : nullptr,
                      workers->point_capacity,
                      (use_compact_output || use_device_output || use_fused_ip)
                          ? nullptr
                          : workers->points[index],
                        (use_compact_output || use_device_output || use_fused_ip)
                          ? &point_count
                          : &workers->point_counts[index],
                      (use_fused_ip || use_device_output)
                          ? &resident_output
                          : nullptr,
                      (workers->stats != nullptr)
                        ? &workers->stats[index]
                        : nullptr,
                      local_error,
                      sizeof(local_error)) != 0))) {
            RecordBatchFailure(workers,
                               local_error[0] ? local_error
                                              : "unknown batch failure");
            break;
          }
          if (use_compact_output &&
              (CopyCompactPointsFromLane(lane, point_count,
                                         &workers->compact_outputs[index],
                                         local_error,
                                         sizeof(local_error)) != 0)) {
            RecordBatchFailure(workers,
                               local_error[0] ? local_error
                                              : "unable to build compact host point buffer");
            break;
          }
          if (use_device_output) {
            if ((point_dimension == 0) ||
                (point_dimension > kType3BoundsRuntimeMaxDimension)) {
              RecordBatchFailure(workers,
                                 "invalid point dimension for device output");
              break;
            }
            if ((resident_output.points == context->device_output_pool[index]) ||
                (point_count == 0)) {
              workers->device_outputs[index] = resident_output;
            } else if (CopyDevicePointsFromLane(context, lane, index,
                                                point_count, point_dimension,
                                                &workers->device_outputs[index],
                                                local_error,
                                                sizeof(local_error)) != 0) {
              RecordBatchFailure(workers,
                                 local_error[0] ? local_error
                                                : "unable to build compact device point buffer");
              break;
            }
          }
          if (use_fused_ip &&
              (RunResidentIPCheckOnLane(context, lane, &resident_output,
                                        &workers->ip_results[index],
                                        local_error,
                                        sizeof(local_error)) != 0)) {
            RecordBatchFailure(workers,
                               local_error[0] ? local_error
                                              : "unable to run resident CUDA IP-check");
            break;
          }
        }
      }
    }

    lock.lock();
    if (workers->pending > 0)
      workers->pending--;
    if (workers->pending == 0) {
      workers->batch_active = false;
      workers->done_cv.notify_one();
    }
  }
}

__global__ void EvaluateKernel(const Type3BoundsJob *jobs,
                               Type3BoundsResult *results,
                               uint32_t job_count) {
  uint32_t index = blockIdx.x * blockDim.x + threadIdx.x;

  if (index < job_count)
    results[index] = EvaluateType3BoundsJob(&jobs[index]);
}

__global__ void GenerateDim5Structure3CandidatesKernel(
    const Type3BoundsCudaWeightEntry *weights,
    uint32_t weight_count,
    uint64_t pair_start,
    uint32_t pair_count,
    Type3BoundsCudaDim5Structure3Candidate *candidates,
    uint32_t *candidate_counts) {
  uint32_t index = blockIdx.x * blockDim.x + threadIdx.x;

  if (index >= pair_count)
    return;

  {
    uint32_t left_index;
    uint32_t right_index;
    const Type3BoundsCudaWeightEntry *left;
    const Type3BoundsCudaWeightEntry *right;
    int64_t prefix[3];
    uint32_t output_count = 0;
    Type3BoundsCudaDim5Structure3Candidate *pair_output =
        &candidates[index * kType3BoundsRuntimeMaxStructure3PairOutputs];

    DecodeUpperTriangularPair(pair_start + index, weight_count, &left_index,
                              &right_index);
    left = &weights[left_index];
    right = &weights[right_index];

    prefix[0] = right->w[0];
    prefix[1] = right->w[1];
    prefix[2] = right->w[2];
    do {
      if (PrefixIsCanonicalForStructure3(left, prefix)) {
        StoreStructure3Candidate(left, right, prefix, &pair_output[output_count]);
        output_count++;
      }
    } while (NextPermutation3(prefix));

    candidate_counts[index] = output_count;
  }
}

__global__ void InitializeFrontierKernel(
    const Type3BoundsCudaProblem *problem,
    int64_t (*frontier)[kType3BoundsRuntimeMaxDimension], uint32_t state_count) {
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

__global__ void ReducePointsToSublatticeKernel(
  const Type3BoundsCudaReduction *reduction, uint32_t point_dimension,
    const int64_t (*input_points)[kType3BoundsRuntimeMaxDimension],
    uint32_t input_count,
    int64_t (*output_points)[kType3BoundsRuntimeMaxDimension],
    uint32_t output_capacity, uint32_t *output_count, int *overflow_flag) {
  uint32_t index = blockIdx.x * blockDim.x + threadIdx.x;
  int64_t reduced[kType3BoundsRuntimeMaxDimension];

  if (index >= input_count)
    return;

  for (uint32_t row = 0; row < point_dimension; row++) {
    int64_t value = 0;

    for (uint32_t column = 0; column < point_dimension; column++)
      value += reduction->matrix[row][column] * input_points[index][column];
    reduced[row] = value;
  }

  for (uint32_t row = 0; row < reduction->mod_count; row++) {
    int64_t mod = reduction->mod[row];

    if ((mod == 0) || ((reduced[row] % mod) != 0))
      return;
    reduced[row] /= mod;
  }

  {
    uint32_t output_index = atomicAdd(output_count, 1u);

    if (output_index >= output_capacity) {
      atomicExch(overflow_flag, 1);
      return;
    }

    for (uint32_t row = 0; row < point_dimension; row++)
      output_points[output_index][row] = reduced[row];
  }
}

__device__ int64_t EvalCudaEquationOnPoint(
    const Type3BoundsCudaEquation *equation, const int64_t *point,
    uint32_t point_dimension) {
  int64_t value = equation->c;

  for (uint32_t coord = 0; coord < point_dimension; coord++)
    value += equation->a[coord] * point[coord];
  return value;
}

__global__ void ClassifyEquationNegativityKernel(
    const Type3BoundsCudaEquation *equations, uint32_t equation_count,
  const int64_t *points, uint32_t point_count, uint32_t point_dimension,
  uint32_t point_stride,
    uint8_t *has_negative) {
  uint32_t equation_index = blockIdx.x;
  __shared__ int block_negative;

  if (equation_index >= equation_count)
    return;

  if (threadIdx.x == 0)
    block_negative = 0;
  __syncthreads();

  for (uint32_t point_index = threadIdx.x; point_index < point_count;
       point_index += blockDim.x) {
    const int64_t *point = points +
                    static_cast<size_t>(point_index) * point_stride;

    if (EvalCudaEquationOnPoint(&equations[equation_index], point,
                                point_dimension) < 0) {
      atomicExch(&block_negative, 1);
      break;
    }
  }

  __syncthreads();
  if ((threadIdx.x == 0) && block_negative)
    has_negative[equation_index] = 1;
}

__global__ void ClassifyEquationTaskBatchKernel(
    const Type3BoundsCudaEquation *equations,
    const Type3BoundsCudaEquationTask *tasks, uint32_t equation_count,
    const int64_t *points, uint32_t point_stride, uint8_t *has_negative) {
  uint32_t equation_index = blockIdx.x;
  __shared__ int block_negative;
  Type3BoundsCudaEquationTask task;

  if (equation_index >= equation_count)
    return;

  task = tasks[equation_index];
  if (threadIdx.x == 0)
    block_negative = 0;
  __syncthreads();

  for (uint32_t point_index = threadIdx.x; point_index < task.point_count;
       point_index += blockDim.x) {
    const int64_t *point =
        points + static_cast<size_t>(task.point_offset + point_index) *
                     point_stride;

    if (EvalCudaEquationOnPoint(&equations[equation_index], point,
                                task.point_dimension) < 0) {
      atomicExch(&block_negative, 1);
      break;
    }
  }

  __syncthreads();
  if ((threadIdx.x == 0) && block_negative)
    has_negative[equation_index] = 1;
}

__device__ uint64_t InciAppend(uint64_t incidence, int64_t value) {
  return 2ull * incidence + ((value == 0) ? 1ull : 0ull);
}

__device__ int InciAbs64(uint64_t incidence) {
  return __popcll(incidence);
}

__device__ int InciLe64(uint64_t left, uint64_t right) {
  return ((left | right) == right) ? 1 : 0;
}

__device__ int PointLexGreater(const int64_t *points, uint32_t point_stride,
                               uint32_t point_dimension, uint32_t left_index,
                               uint32_t right_index) {
  const int64_t *left =
      points + static_cast<size_t>(left_index) * point_stride;
  const int64_t *right =
      points + static_cast<size_t>(right_index) * point_stride;

  for (int coord = static_cast<int>(point_dimension) - 1; coord >= 0; coord--) {
    if (left[coord] > right[coord])
      return 1;
    if (left[coord] < right[coord])
      return 0;
  }
  return 0;
}

__device__ int PointLexGreaterPtr(const int64_t *left, const int64_t *right,
                                  uint32_t point_dimension) {
  for (int coord = static_cast<int>(point_dimension) - 1; coord >= 0;
       coord--) {
    if (left[coord] > right[coord])
      return 1;
    if (left[coord] < right[coord])
      return 0;
  }
  return 0;
}

__device__ int64_t WToGLZ64(int64_t *W, int d, int64_t **GLZ) {
  int i, j;
  int64_t G;
  int64_t *E = GLZ[0];
  int64_t *B = GLZ[1];

  for (i = 1; i < d; i++)
    for (j = 0; j < d; j++)
      GLZ[i][j] = 0;
  G = Egcd64(W[0], W[1], &E[0], &E[1]);
  B[0] = -W[1] / G;
  B[1] = W[0] / G;
  for (i = 2; i < d; i++) {
    int64_t a, b;
    int64_t g = Egcd64(G, W[i], &a, &b);

    B = GLZ[i];
    B[i] = G / g;
    G = W[i] / g;
    for (j = 0; j < i; j++)
      B[j] = -E[j] * G;
    for (j = 0; j < i; j++)
      E[j] *= a;
    E[j] = b;
    for (j = i - 1; j > 0; j--) {
      int n;
      int64_t *Y = GLZ[j];
      int64_t rB = RoundQ64(B[j], Y[j]);
      int64_t rE = RoundQ64(E[j], Y[j]);

      for (n = 0; n <= j; n++) {
        B[n] -= rB * Y[n];
        E[n] -= rE * Y[n];
      }
    }
    G = g;
  }
  return G;
}

__device__ int64_t VZToBase64(const int64_t *V, int d,
                              int64_t M[][kType3BoundsRuntimeMaxDimension]) {
  int p[kType3BoundsRuntimeMaxDimension];
  int i, j, J = 0;
  int64_t g = 0;
  int64_t W[kType3BoundsRuntimeMaxDimension];
  int64_t *G[kType3BoundsRuntimeMaxDimension];

  for (i = 0; i < d; i++) {
    if (V[i] != 0) {
      W[J] = V[i];
      G[J] = M[i];
      p[J++] = i;
    } else {
      for (j = 0; j < d; j++)
        M[i][j] = (i == j);
    }
  }
  if ((J != 0) && (p[0] != 0)) {
    G[0] = M[0];
    for (j = 0; j < d; j++)
      M[p[0]][j] = (j == 0);
  }
  if (J > 1)
    g = WToGLZ64(W, J, G);
  else if (J != 0) {
    g = W[0];
    M[0][0] = 0;
    M[0][p[0]] = 1;
  }
  if (J > 1)
    for (i = 0; i < J; i++) {
      int I = J;

      for (j = d - 1; j >= 0; j--)
        G[i][j] = (V[j] != 0) ? G[i][--I] : 0;
      if (I != 0)
        return 0;
    }
  return g;
}

__device__ int OrthBaseRedByV64(
    const int64_t *V, int d, int64_t A[][kType3BoundsRuntimeMaxDimension],
    int *r, int64_t B[][kType3BoundsRuntimeMaxDimension]) {
  int i, j, k;
  int64_t W[kType3BoundsRuntimeMaxDimension];
  int64_t G[kType3BoundsRuntimeMaxDimension][kType3BoundsRuntimeMaxDimension];

  for (i = 0; i < *r; i++) {
    W[i] = 0;
    for (j = 0; j < d; j++)
      W[i] += A[i][j] * V[j];
  }
  if (VZToBase64(W, *r, G) == 0)
    return 0;
  for (i = 0; i < *r - 1; i++)
    for (k = 0; k < d; k++) {
      B[i][k] = 0;
      for (j = 0; j < *r; j++)
        B[i][k] += G[i + 1][j] * A[j][k];
    }
  (*r)--;
  return 1;
}

__device__ int NewStartVertex64(const int64_t *V0, const int64_t *Ea,
                                const Type3BoundsCudaDevicePointBuffer *points,
                                uint32_t *vertex_index) {
  Type3BoundsCudaEquation equation = {};
  const int64_t *Xn;
  const int64_t *Xp;
  int n = 0;
  int p = 0;
  int64_t d;
  int64_t dn = 0;
  int64_t dp = 0;

  if ((points->point_count == 0) || (points->points == nullptr))
    return 0;
  for (uint32_t coord = 0; coord < points->point_dimension; coord++)
    equation.a[coord] = Ea[coord];
  equation.c = -EvalCudaEquationOnPoint(&equation, V0, points->point_dimension);
  Xn = points->points;
  Xp = Xn;
  d = EvalCudaEquationOnPoint(&equation, points->points, points->point_dimension);
  if (d > 0)
    dp = d;
  if (d < 0)
    dn = d;
  for (uint32_t index = 1; index < points->point_count; index++) {
    const int64_t *point = points->points +
        static_cast<size_t>(index) * points->point_stride;

    d = EvalCudaEquationOnPoint(&equation, point, points->point_dimension);
    if (d == 0)
      continue;
    if ((d == dp) && PointLexGreaterPtr(point, Xp, points->point_dimension)) {
      Xp = point;
      p = static_cast<int>(index);
    }
    if (d > dp) {
      dp = d;
      Xp = point;
      p = static_cast<int>(index);
    }
    if ((d == dn) && PointLexGreaterPtr(point, Xn, points->point_dimension)) {
      Xn = point;
      n = static_cast<int>(index);
    }
    if (d < dn) {
      dn = d;
      Xn = point;
      n = static_cast<int>(index);
    }
  }
  if (dp != 0) {
    if (dn != 0) {
      *vertex_index = static_cast<uint32_t>((dp + dn > 0) ? n : p);
      return 1;
    }
    *vertex_index = static_cast<uint32_t>(p);
    return 1;
  }
  if (dn != 0) {
    *vertex_index = static_cast<uint32_t>(n);
    return 1;
  }
  return 0;
}

__device__ int InitializeCudaIPStateFromPoints(
    const Type3BoundsCudaDevicePointBuffer *point_buffer,
    Type3BoundsCudaIPState *state) {
  int x = 0;
  int y = 0;
  int d = static_cast<int>(point_buffer->point_dimension);
  int r = d;
  int b[kType3BoundsRuntimeMaxDimension];
  int64_t XX = 0;
  int64_t YY = 0;
  int64_t B[(kType3BoundsRuntimeMaxDimension *
             (kType3BoundsRuntimeMaxDimension + 1)) /
            2][kType3BoundsRuntimeMaxDimension];
  int64_t W[kType3BoundsRuntimeMaxDimension];
  const int64_t *X;
  const int64_t *Y;

  state->point_offset = 0;
  state->point_count = point_buffer->point_count;
  state->point_dimension = point_buffer->point_dimension;
  state->vertex_count = 0;
  state->facet_count = 0;
  state->ceq_count = 0;
  if ((point_buffer->points == nullptr) ||
      (point_buffer->point_stride < point_buffer->point_dimension))
    return d;
  if (point_buffer->point_count < 2) {
    for (x = 0; x < d; x++) {
      for (y = 0; y < d; y++)
        state->ceqs[x].a[y] = (x == y);
      state->ceqs[x].c = -point_buffer->points[x];
    }
    state->ceq_count = static_cast<uint32_t>(d);
    return d;
  }

  X = point_buffer->points;
  Y = point_buffer->points;
  for (uint32_t index = 1; index < point_buffer->point_count; index++) {
    const int64_t *Z = point_buffer->points +
        static_cast<size_t>(index) * point_buffer->point_stride;

    if (PointLexGreaterPtr(X, Z, point_buffer->point_dimension)) {
      X = Z;
      x = static_cast<int>(index);
    }
    if (PointLexGreaterPtr(Z, Y, point_buffer->point_dimension)) {
      Y = Z;
      y = static_cast<int>(index);
    }
  }
  if (x == y)
    return d;
  for (int coord = 0; coord < d; coord++) {
    int64_t Xi = (X[coord] > 0) ? X[coord] : -X[coord];
    int64_t Yi = (Y[coord] > 0) ? Y[coord] : -Y[coord];

    if (Xi > XX)
      XX = Xi;
    if (Yi > YY)
      YY = Yi;
  }
  if (YY < XX) {
    state->vertices[0] = static_cast<uint32_t>(y);
    state->vertices[1] = static_cast<uint32_t>(x);
  } else {
    state->vertices[0] = static_cast<uint32_t>(x);
    state->vertices[1] = static_cast<uint32_t>(y);
  }
  state->vertex_count = 2;
  y = static_cast<int>(state->vertices[1]);
  X = point_buffer->points +
      static_cast<size_t>(state->vertices[0]) * point_buffer->point_stride;
  for (int coord = 0; coord < d; coord++)
    b[coord] = (coord * (2 * d - coord + 1)) / 2;
  for (x = 0; x < d; x++)
    for (int coord = 0; coord < d; coord++)
      B[x][coord] = (x == coord);
  for (x = 1; x < d; x++) {
    const int64_t *point_y = point_buffer->points +
        static_cast<size_t>(y) * point_buffer->point_stride;

    for (int coord = 0; coord < d; coord++)
      W[coord] = point_y[coord] - X[coord];
    if (!OrthBaseRedByV64(W, d, &B[b[x - 1]], &r, &B[b[x]]))
      return d;
    for (int index = 0; index < r; index++) {
      uint32_t new_vertex = 0;

      if (NewStartVertex64(X, B[b[x] + index], point_buffer, &new_vertex)) {
        y = static_cast<int>(new_vertex);
        break;
      }
      if (index == r - 1)
        goto simplex_done;
    }
    state->vertices[state->vertex_count++] = static_cast<uint32_t>(y);
  }

simplex_done:
  if (x < d) {
    for (y = 0; y < r; y++) {
      state->ceqs[y].c = 0;
      for (int coord = 0; coord < d; coord++)
        state->ceqs[y].a[coord] = B[b[x] + y][coord];
      state->ceqs[y].c = -EvalCudaEquationOnPoint(&state->ceqs[y], X,
                                                  point_buffer->point_dimension);
    }
    state->ceq_count = static_cast<uint32_t>(r);
    return r;
  }

  {
    Type3BoundsCudaEquation *equation = &state->ceqs[0];
    int64_t *Z = B[b[d - 1]];

    state->ceq_count = 2;
    equation->c = 0;
    for (int coord = 0; coord < d; coord++)
      equation->a[coord] = Z[coord];
    equation->c = -EvalCudaEquationOnPoint(equation, X,
                                           point_buffer->point_dimension);
    if (EvalCudaEquationOnPoint(
            equation,
            point_buffer->points +
                static_cast<size_t>(state->vertices[d]) * point_buffer->point_stride,
            point_buffer->point_dimension) < 0) {
      for (int coord = 0; coord < d; coord++)
        equation->a[coord] = -equation->a[coord];
      equation->c = -equation->c;
    }

    X = point_buffer->points +
        static_cast<size_t>(state->vertices[r = d]) * point_buffer->point_stride;
    for (x = 1; x < d; x++) {
      Y = point_buffer->points +
          static_cast<size_t>(state->vertices[x - 1]) * point_buffer->point_stride;
      for (int coord = 0; coord < d; coord++)
        W[coord] = X[coord] - Y[coord];
      if (!OrthBaseRedByV64(W, d, &B[b[x - 1]], &r, &B[b[x]]))
        return d;
    }
    equation = &state->ceqs[1];
    equation->c = 0;
    for (int coord = 0; coord < d; coord++)
      equation->a[coord] = Z[coord];
    equation->c = -EvalCudaEquationOnPoint(equation, X,
                                           point_buffer->point_dimension);
    XX = EvalCudaEquationOnPoint(
        equation,
        point_buffer->points +
            static_cast<size_t>(state->vertices[d - 1]) * point_buffer->point_stride,
        point_buffer->point_dimension);
    if (XX == 0)
      return d;
    if (XX < 0) {
      for (int coord = 0; coord < d; coord++)
        equation->a[coord] = -equation->a[coord];
      equation->c = -equation->c;
    }
    for (x = d - 2; x >= 0; x--) {
      r = d - x;
      for (y = x + 1; y < d; y++) {
        Y = point_buffer->points +
            static_cast<size_t>(state->vertices[y]) * point_buffer->point_stride;
        for (int coord = 0; coord < d; coord++)
          W[coord] = X[coord] - Y[coord];
        if (!OrthBaseRedByV64(W, d, &B[b[y - 1]], &r, &B[b[y]]))
          return d;
      }
      equation = &state->ceqs[state->ceq_count++];
      equation->c = 0;
      for (int coord = 0; coord < d; coord++)
        equation->a[coord] = Z[coord];
      equation->c = -EvalCudaEquationOnPoint(equation, X,
                                             point_buffer->point_dimension);
      XX = EvalCudaEquationOnPoint(
          equation,
          point_buffer->points +
              static_cast<size_t>(state->vertices[x]) * point_buffer->point_stride,
          point_buffer->point_dimension);
      if (XX == 0)
        return d;
      if (XX < 0) {
        for (int coord = 0; coord < d; coord++)
          equation->a[coord] = -equation->a[coord];
        equation->c = -equation->c;
      }
    }
  }

  return 0;
}

__device__ int InitializeCudaIPIncidences(
    Type3BoundsCudaIPState *state,
    const Type3BoundsCudaDevicePointBuffer *point_buffer) {
  for (uint32_t ceq_index = 0; ceq_index < state->ceq_count; ceq_index++) {
    uint64_t incidence = 0;

    for (uint32_t vertex_index = 0; vertex_index < state->vertex_count;
         vertex_index++) {
      const int64_t *point = point_buffer->points +
          static_cast<size_t>(state->vertices[vertex_index]) *
              point_buffer->point_stride;
      incidence = InciAppend(incidence,
                             EvalCudaEquationOnPoint(&state->ceqs[ceq_index],
                                                     point,
                                                     state->point_dimension));
    }
    if (InciAbs64(incidence) < static_cast<int>(state->point_dimension))
      return 1;
    state->ceq_incidences[ceq_index] = incidence;
  }
  return 0;
}

__device__ void NegateCudaEquation(Type3BoundsCudaEquation *equation,
                                   uint32_t point_dimension) {
  for (uint32_t coord = 0; coord < point_dimension; coord++)
    equation->a[coord] = -equation->a[coord];
  equation->c = -equation->c;
}

__device__ Type3BoundsCudaEquation EEVToCudaEquation(
    const Type3BoundsCudaEquation *left,
    const Type3BoundsCudaEquation *right,
    const int64_t *vertex,
    uint32_t point_dimension) {
  Type3BoundsCudaEquation equation = {{0}, 0};
  int64_t left_eval = EvalCudaEquationOnPoint(right, vertex, point_dimension);
  int64_t right_eval = EvalCudaEquationOnPoint(left, vertex, point_dimension);
  int64_t gcd = GcdNonNegative(left_eval, right_eval);

  left_eval /= gcd;
  right_eval /= gcd;
  for (uint32_t coord = 0; coord < point_dimension; coord++)
    equation.a[coord] = left_eval * left->a[coord] -
                        right_eval * right->a[coord];
  equation.c = left_eval * left->c - right_eval * right->c;

  gcd = GcdNonNegative(equation.c, equation.a[0]);
  for (uint32_t coord = 1; coord < point_dimension; coord++)
    gcd = GcdNonNegative(gcd, equation.a[coord]);
  if (gcd > 1) {
    equation.c /= gcd;
    for (uint32_t coord = 0; coord < point_dimension; coord++)
      equation.a[coord] /= gcd;
  }

  return equation;
}

__device__ int IsGoodCudaCEq(Type3BoundsCudaEquation *equation,
                             const Type3BoundsCudaIPState *state,
                             const int64_t *points,
                             uint32_t point_stride) {
  int vertex_index = static_cast<int>(state->vertex_count);
  int64_t sign = 0;

  while ((vertex_index > 0) && (sign == 0)) {
    uint32_t point_index = state->vertices[vertex_index - 1] + state->point_offset;

    sign = EvalCudaEquationOnPoint(
        equation, points + static_cast<size_t>(point_index) * point_stride,
        state->point_dimension);
    vertex_index--;
  }
  if (sign < 0)
    NegateCudaEquation(equation, state->point_dimension);

  while (vertex_index > 0) {
    uint32_t point_index = state->vertices[vertex_index - 1] + state->point_offset;

    if (EvalCudaEquationOnPoint(
            equation, points + static_cast<size_t>(point_index) * point_stride,
            state->point_dimension) < 0)
      return 0;
    vertex_index--;
  }
  return 1;
}

__device__ int SelectLexGreatestCEq(const Type3BoundsCudaIPState *state) {
  int selected = static_cast<int>(state->ceq_count) - 1;

  for (int index = 0; index < selected; index++)
    if (state->ceq_incidences[index] > state->ceq_incidences[selected])
      selected = index;
  return selected;
}

__device__ int MakeNewCudaCEqs(Type3BoundsCudaIPState *state,
                               const int64_t *points,
                               uint32_t point_stride) {
  Type3BoundsCudaEquation bad_equations[kType3BoundsRuntimeMaxIPEquations];
  uint64_t bad_incidences[kType3BoundsRuntimeMaxIPEquations];
  uint32_t bad_count = 0;
  uint32_t old_ceq_count = state->ceq_count;
  uint32_t keep_count = 0;
  const int64_t *new_vertex = points +
      static_cast<size_t>(state->point_offset +
                          state->vertices[state->vertex_count - 1]) *
          point_stride;

  for (uint32_t index = 0; index < old_ceq_count; index++) {
    int64_t distance =
        EvalCudaEquationOnPoint(&state->ceqs[index], new_vertex,
                                state->point_dimension);

    state->ceq_incidences[index] = InciAppend(state->ceq_incidences[index], distance);
    if (distance < 0) {
      bad_equations[bad_count] = state->ceqs[index];
      bad_incidences[bad_count] = state->ceq_incidences[index];
      bad_count++;
    } else {
      state->ceqs[keep_count] = state->ceqs[index];
      state->ceq_incidences[keep_count] = state->ceq_incidences[index];
      keep_count++;
    }
  }
  state->ceq_count = keep_count;

  for (uint32_t index = 0; index < state->facet_count; index++)
    state->facet_incidences[index] = InciAppend(
        state->facet_incidences[index],
        EvalCudaEquationOnPoint(&state->facets[index], new_vertex,
                                state->point_dimension));

  for (uint32_t facet_index = 0; facet_index < state->facet_count; facet_index++)
    if ((state->facet_incidences[facet_index] & 1ull) == 0)
      for (uint32_t bad_index = 0; bad_index < bad_count; bad_index++) {
        uint64_t new_face =
            bad_incidences[bad_index] & state->facet_incidences[facet_index];
        uint32_t check_index;

        if (InciAbs64(new_face) < static_cast<int>(state->point_dimension) - 1)
          continue;
        for (check_index = 0; check_index < bad_count; check_index++)
          if (InciLe64(new_face, bad_incidences[check_index]) &&
              (check_index != bad_index))
            break;
        if (check_index != bad_count)
          continue;
        for (check_index = 0; check_index < keep_count; check_index++)
          if (InciLe64(new_face, state->ceq_incidences[check_index]))
            break;
        if (check_index != keep_count)
          continue;
        for (check_index = 0; check_index < state->facet_count; check_index++)
          if (InciLe64(new_face, state->facet_incidences[check_index]) &&
              (check_index != facet_index))
            break;
        if (check_index != state->facet_count)
          continue;
        if (state->ceq_count >= kType3BoundsRuntimeMaxIPEquations)
          return 1;
        state->ceq_incidences[state->ceq_count] =
            InciAppend(new_face >> 1, 0);
        state->ceqs[state->ceq_count] = EEVToCudaEquation(
            &bad_equations[bad_index], &state->facets[facet_index],
            new_vertex, state->point_dimension);
        if (!IsGoodCudaCEq(&state->ceqs[state->ceq_count], state, points,
                           point_stride))
          return 1;
        state->ceq_count++;
      }

  for (uint32_t ceq_index = 0; ceq_index < keep_count; ceq_index++)
    if ((state->ceq_incidences[ceq_index] & 1ull) == 0)
      for (int bad_index = static_cast<int>(bad_count) - 1; bad_index >= 0;
           bad_index--) {
        uint64_t new_face = bad_incidences[bad_index] &
                            state->ceq_incidences[ceq_index];
        uint32_t check_index;

        if (InciAbs64(new_face) < static_cast<int>(state->point_dimension) - 1)
          continue;
        for (check_index = 0; check_index < bad_count; check_index++)
          if (InciLe64(new_face, bad_incidences[check_index]) &&
              (static_cast<int>(check_index) != bad_index))
            break;
        if (check_index != bad_count)
          continue;
        for (check_index = 0; check_index < keep_count; check_index++)
          if (InciLe64(new_face, state->ceq_incidences[check_index]) &&
              (check_index != ceq_index))
            break;
        if (check_index != keep_count)
          continue;
        for (check_index = 0; check_index < state->facet_count; check_index++)
          if (InciLe64(new_face, state->facet_incidences[check_index]))
            break;
        if (check_index != state->facet_count)
          continue;
        if (state->ceq_count >= kType3BoundsRuntimeMaxIPEquations)
          return 1;
        state->ceq_incidences[state->ceq_count] =
            InciAppend(new_face >> 1, 0);
        state->ceqs[state->ceq_count] = EEVToCudaEquation(
            &bad_equations[bad_index], &state->ceqs[ceq_index], new_vertex,
            state->point_dimension);
        if (!IsGoodCudaCEq(&state->ceqs[state->ceq_count], state, points,
                           point_stride))
          return 1;
        state->ceq_count++;
      }

  return 0;
}

__global__ void RunIPCheckBatchKernel(Type3BoundsCudaIPState *states,
                                      uint32_t state_count,
                                      const int64_t *points,
                                      uint32_t point_stride,
                                      int *results,
                                      int *error_flag) {
  enum { kMaxIPCheckBlockSize = 256 };
  uint32_t state_index = blockIdx.x;
  __shared__ Type3BoundsCudaIPState state;
  __shared__ int block_negative;
  __shared__ int finished;
  __shared__ int final_result;
  __shared__ int selected_eq_index;
  __shared__ uint32_t selected_vertex_index;
  __shared__ int64_t best_values[kMaxIPCheckBlockSize];
  __shared__ uint32_t best_indices[kMaxIPCheckBlockSize];
  __shared__ uint8_t has_best[kMaxIPCheckBlockSize];

  if (state_index >= state_count)
    return;

  if (threadIdx.x == 0) {
    state = states[state_index];
    finished = 0;
    final_result = 0;
  }
  __syncthreads();

  while (!finished) {
    const int64_t *state_points =
        points + static_cast<size_t>(state.point_offset) * point_stride;

    if (threadIdx.x == 0) {
      if ((state.point_dimension == 0) ||
          (state.point_dimension > kType3BoundsRuntimeMaxDimension) ||
          (state.vertex_count > kType3BoundsRuntimeMaxIPVertices) ||
          (state.facet_count > kType3BoundsRuntimeMaxIPEquations) ||
          (state.ceq_count > kType3BoundsRuntimeMaxIPEquations)) {
        atomicExch(error_flag, 1);
        finished = 1;
      } else if (state.ceq_count == 0) {
        final_result = 1;
        finished = 1;
      } else {
        selected_eq_index = SelectLexGreatestCEq(&state);
        block_negative = 0;
      }
    }
    __syncthreads();
    if (finished)
      break;

    has_best[threadIdx.x] = 0;
    for (uint32_t point_index = threadIdx.x; point_index < state.point_count;
         point_index += blockDim.x) {
      const int64_t *point = state_points +
          static_cast<size_t>(point_index) * point_stride;
      int64_t value = EvalCudaEquationOnPoint(
          &state.ceqs[selected_eq_index], point, state.point_dimension);

      if (value < 0)
        atomicExch(&block_negative, 1);
      if (!has_best[threadIdx.x] || (value < best_values[threadIdx.x]) ||
          ((value == best_values[threadIdx.x]) &&
           PointLexGreater(state_points, point_stride, state.point_dimension,
                           point_index, best_indices[threadIdx.x]))) {
        has_best[threadIdx.x] = 1;
        best_values[threadIdx.x] = value;
        best_indices[threadIdx.x] = point_index;
      }
    }
    __syncthreads();

    if (threadIdx.x == 0) {
      int best_thread = -1;

      for (uint32_t thread_index = 0; thread_index < blockDim.x;
           thread_index++) {
        if (!has_best[thread_index])
          continue;
        if ((best_thread < 0) ||
            (best_values[thread_index] < best_values[best_thread]) ||
            ((best_values[thread_index] == best_values[best_thread]) &&
             PointLexGreater(state_points, point_stride, state.point_dimension,
                             best_indices[thread_index],
                             best_indices[best_thread])))
          best_thread = static_cast<int>(thread_index);
      }
      if (best_thread < 0) {
        atomicExch(error_flag, 1);
        finished = 1;
      } else {
        selected_vertex_index = best_indices[best_thread];
        if (block_negative) {
          int last_index = static_cast<int>(state.ceq_count) - 1;

          if (selected_eq_index != last_index) {
            Type3BoundsCudaEquation selected_equation =
                state.ceqs[selected_eq_index];
            uint64_t selected_incidence = state.ceq_incidences[selected_eq_index];

            state.ceqs[selected_eq_index] = state.ceqs[last_index];
            state.ceq_incidences[selected_eq_index] =
                state.ceq_incidences[last_index];
            state.ceqs[last_index] = selected_equation;
            state.ceq_incidences[last_index] = selected_incidence;
          }
          if (state.vertex_count >= kType3BoundsRuntimeMaxIPVertices) {
            atomicExch(error_flag, 1);
            finished = 1;
          } else {
            state.vertices[state.vertex_count++] = selected_vertex_index;
            if (MakeNewCudaCEqs(&state, points, point_stride) != 0) {
              atomicExch(error_flag, 1);
              finished = 1;
            }
          }
        } else if (state.ceqs[selected_eq_index].c < 1) {
          final_result = 0;
          finished = 1;
        } else {
          uint32_t last_index = state.ceq_count - 1;

          if (state.facet_count >= kType3BoundsRuntimeMaxIPEquations) {
            atomicExch(error_flag, 1);
            finished = 1;
          } else {
            state.facets[state.facet_count] = state.ceqs[selected_eq_index];
            state.facet_incidences[state.facet_count] =
                state.ceq_incidences[selected_eq_index];
            state.facet_count++;
            if (selected_eq_index != static_cast<int>(last_index)) {
              state.ceqs[selected_eq_index] = state.ceqs[last_index];
              state.ceq_incidences[selected_eq_index] =
                  state.ceq_incidences[last_index];
            }
            state.ceq_count--;
            if (state.ceq_count == 0) {
              final_result = 1;
              finished = 1;
            }
          }
        }
      }
    }
    __syncthreads();
  }

  if (threadIdx.x == 0)
    results[state_index] = final_result;
}

__global__ void RunIPCheckDeviceBatchKernel(
    const Type3BoundsCudaDevicePointBuffer *point_buffers,
    uint32_t state_count,
    int *results,
    int *error_flag) {
  enum { kMaxIPCheckBlockSize = 256 };
  uint32_t state_index = blockIdx.x;
  __shared__ Type3BoundsCudaDevicePointBuffer point_buffer;
  __shared__ Type3BoundsCudaIPState state;
  __shared__ int block_negative;
  __shared__ int finished;
  __shared__ int final_result;
  __shared__ int selected_eq_index;
  __shared__ uint32_t selected_vertex_index;
  __shared__ int64_t best_values[kMaxIPCheckBlockSize];
  __shared__ uint32_t best_indices[kMaxIPCheckBlockSize];
  __shared__ uint8_t has_best[kMaxIPCheckBlockSize];

  if (state_index >= state_count)
    return;

  if (threadIdx.x == 0) {
    point_buffer = point_buffers[state_index];
    state = {};
    finished = 0;
    final_result = 0;
    if ((point_buffer.point_dimension == 0) ||
        (point_buffer.point_dimension > kType3BoundsRuntimeMaxDimension) ||
        (point_buffer.point_stride < point_buffer.point_dimension) ||
        ((point_buffer.point_count != 0) && (point_buffer.points == nullptr))) {
      atomicExch(error_flag, 1);
      finished = 1;
    } else if (InitializeCudaIPStateFromPoints(&point_buffer, &state) != 0) {
      finished = 1;
      final_result = 0;
    } else if (InitializeCudaIPIncidences(&state, &point_buffer) != 0) {
      atomicExch(error_flag, 1);
      finished = 1;
    }
  }
  __syncthreads();

  while (!finished) {
    const int64_t *state_points = point_buffer.points;

    if (threadIdx.x == 0) {
      if ((state.point_dimension == 0) ||
          (state.point_dimension > kType3BoundsRuntimeMaxDimension) ||
          (state.vertex_count > kType3BoundsRuntimeMaxIPVertices) ||
          (state.facet_count > kType3BoundsRuntimeMaxIPEquations) ||
          (state.ceq_count > kType3BoundsRuntimeMaxIPEquations)) {
        atomicExch(error_flag, 1);
        finished = 1;
      } else if (state.ceq_count == 0) {
        final_result = 1;
        finished = 1;
      } else {
        selected_eq_index = SelectLexGreatestCEq(&state);
        block_negative = 0;
      }
    }
    __syncthreads();
    if (finished)
      break;

    has_best[threadIdx.x] = 0;
    for (uint32_t point_index = threadIdx.x; point_index < state.point_count;
         point_index += blockDim.x) {
      const int64_t *point = state_points +
          static_cast<size_t>(point_index) * point_buffer.point_stride;
      int64_t value = EvalCudaEquationOnPoint(
          &state.ceqs[selected_eq_index], point, state.point_dimension);

      if (value < 0)
        atomicExch(&block_negative, 1);
      if (!has_best[threadIdx.x] || (value < best_values[threadIdx.x]) ||
          ((value == best_values[threadIdx.x]) &&
           PointLexGreater(state_points, point_buffer.point_stride,
                           state.point_dimension, point_index,
                           best_indices[threadIdx.x]))) {
        has_best[threadIdx.x] = 1;
        best_values[threadIdx.x] = value;
        best_indices[threadIdx.x] = point_index;
      }
    }
    __syncthreads();

    if (threadIdx.x == 0) {
      int best_thread = -1;

      for (uint32_t thread_index = 0; thread_index < blockDim.x;
           thread_index++) {
        if (!has_best[thread_index])
          continue;
        if ((best_thread < 0) ||
            (best_values[thread_index] < best_values[best_thread]) ||
            ((best_values[thread_index] == best_values[best_thread]) &&
             PointLexGreater(state_points, point_buffer.point_stride,
                             state.point_dimension,
                             best_indices[thread_index],
                             best_indices[best_thread])))
          best_thread = static_cast<int>(thread_index);
      }
      if (best_thread < 0) {
        atomicExch(error_flag, 1);
        finished = 1;
      } else {
        selected_vertex_index = best_indices[best_thread];
        if (block_negative) {
          int last_index = static_cast<int>(state.ceq_count) - 1;

          if (selected_eq_index != last_index) {
            Type3BoundsCudaEquation selected_equation =
                state.ceqs[selected_eq_index];
            uint64_t selected_incidence = state.ceq_incidences[selected_eq_index];

            state.ceqs[selected_eq_index] = state.ceqs[last_index];
            state.ceq_incidences[selected_eq_index] =
                state.ceq_incidences[last_index];
            state.ceqs[last_index] = selected_equation;
            state.ceq_incidences[last_index] = selected_incidence;
          }
          if (state.vertex_count >= kType3BoundsRuntimeMaxIPVertices) {
            atomicExch(error_flag, 1);
            finished = 1;
          } else {
            state.vertices[state.vertex_count++] = selected_vertex_index;
            if (MakeNewCudaCEqs(&state, point_buffer.points,
                                point_buffer.point_stride) != 0) {
              atomicExch(error_flag, 1);
              finished = 1;
            }
          }
        } else if (state.ceqs[selected_eq_index].c < 1) {
          final_result = 0;
          finished = 1;
        } else {
          uint32_t last_index = state.ceq_count - 1;

          if (state.facet_count >= kType3BoundsRuntimeMaxIPEquations) {
            atomicExch(error_flag, 1);
            finished = 1;
          } else {
            state.facets[state.facet_count] = state.ceqs[selected_eq_index];
            state.facet_incidences[state.facet_count] =
                state.ceq_incidences[selected_eq_index];
            state.facet_count++;
            if (selected_eq_index != static_cast<int>(last_index)) {
              state.ceqs[selected_eq_index] = state.ceqs[last_index];
              state.ceq_incidences[selected_eq_index] =
                  state.ceq_incidences[last_index];
            }
            state.ceq_count--;
            if (state.ceq_count == 0) {
              final_result = 1;
              finished = 1;
            }
          }
        }
      }
    }
    __syncthreads();
  }

  if (threadIdx.x == 0)
    results[state_index] = final_result;
}

cudaError_t EnsureEnumerationCapacity(Type3BoundsCudaEnumerationLane *lane,
                                      uint32_t frontier_capacity,
                                      uint32_t point_capacity) {
  cudaError_t status;

  if (lane->device_problem == nullptr) {
    status = cudaMalloc(&lane->device_problem, sizeof(Type3BoundsCudaProblem));
    if (status != cudaSuccess)
      return status;
  }
  if (lane->device_reduction == nullptr) {
    status = cudaMalloc(&lane->device_reduction,
                        sizeof(Type3BoundsCudaReduction));
    if (status != cudaSuccess)
      return status;
  }
  if (lane->device_output_count == nullptr) {
    status = cudaMalloc(&lane->device_output_count, sizeof(uint32_t));
    if (status != cudaSuccess)
      return status;
  }
  if (lane->device_overflow_flag == nullptr) {
    status = cudaMalloc(&lane->device_overflow_flag, sizeof(int));
    if (status != cudaSuccess)
      return status;
  }
  if (lane->device_prepare_status == nullptr) {
    status = cudaMalloc(&lane->device_prepare_status, sizeof(int));
    if (status != cudaSuccess)
      return status;
  }
  if (lane->device_stats == nullptr) {
    status = cudaMalloc(&lane->device_stats, sizeof(Type3BoundsCudaStats));
    if (status != cudaSuccess)
      return status;
  }
  if (lane->device_candidate == nullptr) {
    status = cudaMalloc(&lane->device_candidate,
                        sizeof(Type3BoundsCudaCwsCandidate));
    if (status != cudaSuccess)
      return status;
  }

  if (frontier_capacity > lane->frontier_capacity) {
    if (lane->device_frontier_a != nullptr)
      cudaFree(lane->device_frontier_a);
    if (lane->device_frontier_b != nullptr)
      cudaFree(lane->device_frontier_b);
    status = cudaMalloc(&lane->device_frontier_a,
                        static_cast<size_t>(frontier_capacity) *
                            kType3BoundsRuntimeMaxDimension * sizeof(int64_t));
    if (status != cudaSuccess)
      return status;
    status = cudaMalloc(&lane->device_frontier_b,
                        static_cast<size_t>(frontier_capacity) *
                            kType3BoundsRuntimeMaxDimension * sizeof(int64_t));
    if (status != cudaSuccess)
      return status;
    lane->frontier_capacity = frontier_capacity;
  }

  if (point_capacity > lane->point_capacity) {
    if (lane->device_points != nullptr)
      cudaFree(lane->device_points);
    status = cudaMalloc(&lane->device_points,
                        static_cast<size_t>(point_capacity) *
                            kType3BoundsRuntimeMaxDimension * sizeof(int64_t));
    if (status != cudaSuccess)
      return status;
    lane->point_capacity = point_capacity;
  }
  if (point_capacity > lane->host_point_capacity) {
    int64_t *host_points = static_cast<int64_t *>(
        std::realloc(lane->host_point_staging,
                     static_cast<size_t>(point_capacity) *
                         kType3BoundsRuntimeMaxDimension * sizeof(int64_t)));

    if (host_points == nullptr)
      return cudaErrorMemoryAllocation;
    lane->host_point_staging = host_points;
    lane->host_point_capacity = point_capacity;
  }

  return cudaSuccess;
}

cudaError_t EnsureCandidateBatchCapacity(Type3BoundsCudaContext *context,
                                         uint32_t candidate_count) {
  cudaError_t status;

  if (candidate_count <= context->candidate_batch_capacity)
    return cudaSuccess;

  if (context->device_candidate_batch != nullptr) {
    cudaFree(context->device_candidate_batch);
    context->device_candidate_batch = nullptr;
  }
  if (context->device_problem_batch != nullptr) {
    cudaFree(context->device_problem_batch);
    context->device_problem_batch = nullptr;
  }
  if (context->device_prepare_status_batch != nullptr) {
    cudaFree(context->device_prepare_status_batch);
    context->device_prepare_status_batch = nullptr;
  }

  status = cudaMalloc(&context->device_candidate_batch,
                      static_cast<size_t>(candidate_count) *
                          sizeof(Type3BoundsCudaCwsCandidate));
  if (status != cudaSuccess)
    return status;
  status = cudaMalloc(&context->device_problem_batch,
                      static_cast<size_t>(candidate_count) *
                          sizeof(Type3BoundsCudaProblem));
  if (status != cudaSuccess)
    return status;
  status = cudaMalloc(&context->device_prepare_status_batch,
                      static_cast<size_t>(candidate_count) * sizeof(int));
  if (status != cudaSuccess)
    return status;

  context->candidate_batch_capacity = candidate_count;
  return cudaSuccess;
}

cudaError_t EnsureDim5WeightPoolCapacity(Type3BoundsCudaContext *context,
                                         uint32_t weight_count) {
  cudaError_t status;

  if (weight_count <= context->dim5_weight_capacity)
    return cudaSuccess;

  if (context->device_dim5_weights != nullptr) {
    cudaFree(context->device_dim5_weights);
    context->device_dim5_weights = nullptr;
  }

  status = cudaMalloc(&context->device_dim5_weights,
                      static_cast<size_t>(weight_count) *
                          sizeof(Type3BoundsCudaWeightEntry));
  if (status != cudaSuccess)
    return status;

  context->dim5_weight_capacity = weight_count;
  return cudaSuccess;
}

cudaError_t EnsureDim5Structure3BatchCapacity(Type3BoundsCudaContext *context,
                                              uint32_t pair_count) {
  cudaError_t status;

  if (pair_count <= context->dim5_structure3_pair_capacity)
    return cudaSuccess;

  if (context->device_dim5_structure3_candidates != nullptr) {
    cudaFree(context->device_dim5_structure3_candidates);
    context->device_dim5_structure3_candidates = nullptr;
  }
  if (context->device_dim5_structure3_candidate_counts != nullptr) {
    cudaFree(context->device_dim5_structure3_candidate_counts);
    context->device_dim5_structure3_candidate_counts = nullptr;
  }

  status = cudaMalloc(&context->device_dim5_structure3_candidates,
                      static_cast<size_t>(pair_count) *
                          kType3BoundsRuntimeMaxStructure3PairOutputs *
                          sizeof(Type3BoundsCudaDim5Structure3Candidate));
  if (status != cudaSuccess)
    return status;
  status = cudaMalloc(&context->device_dim5_structure3_candidate_counts,
                      static_cast<size_t>(pair_count) * sizeof(uint32_t));
  if (status != cudaSuccess)
    return status;

  context->dim5_structure3_pair_capacity = pair_count;
  return cudaSuccess;
}

cudaError_t EnsureEquationClassificationCapacity(
    Type3BoundsCudaContext *context, uint32_t equation_count,
    size_t point_value_count) {
  cudaError_t status;

  if (equation_count > context->equation_batch_capacity) {
    if (context->device_equation_batch != nullptr) {
      cudaFree(context->device_equation_batch);
      context->device_equation_batch = nullptr;
    }
    if (context->device_equation_tasks != nullptr) {
      cudaFree(context->device_equation_tasks);
      context->device_equation_tasks = nullptr;
    }
    if (context->device_equation_negative_flags != nullptr) {
      cudaFree(context->device_equation_negative_flags);
      context->device_equation_negative_flags = nullptr;
    }

    status = cudaMalloc(&context->device_equation_batch,
                        static_cast<size_t>(equation_count) *
                            sizeof(Type3BoundsCudaEquation));
    if (status != cudaSuccess)
      return status;
    status = cudaMalloc(&context->device_equation_negative_flags,
                        static_cast<size_t>(equation_count) * sizeof(uint8_t));
    if (status != cudaSuccess)
      return status;
    status = cudaMalloc(&context->device_equation_tasks,
                        static_cast<size_t>(equation_count) *
                            sizeof(Type3BoundsCudaEquationTask));
    if (status != cudaSuccess)
      return status;

    context->equation_batch_capacity = equation_count;
  }

  if (point_value_count > context->equation_point_value_capacity) {
    if (context->device_equation_points != nullptr) {
      cudaFree(context->device_equation_points);
      context->device_equation_points = nullptr;
    }

    status = cudaMalloc(&context->device_equation_points,
                        point_value_count * sizeof(int64_t));
    if (status != cudaSuccess)
      return status;

    context->equation_point_value_capacity = point_value_count;
  }

  return cudaSuccess;
}

cudaError_t EnsureIPCheckBatchCapacity(Type3BoundsCudaContext *context,
                                      uint32_t state_count) {
  cudaError_t status;

  if (state_count <= context->ip_state_capacity)
    return cudaSuccess;

  if (context->device_ip_states != nullptr) {
    cudaFree(context->device_ip_states);
    context->device_ip_states = nullptr;
  }
  if (context->device_ip_point_buffers != nullptr) {
    cudaFree(context->device_ip_point_buffers);
    context->device_ip_point_buffers = nullptr;
  }
  if (context->device_ip_results != nullptr) {
    cudaFree(context->device_ip_results);
    context->device_ip_results = nullptr;
  }
  if (context->device_ip_error_flag != nullptr) {
    cudaFree(context->device_ip_error_flag);
    context->device_ip_error_flag = nullptr;
  }

  status = cudaMalloc(&context->device_ip_states,
                      static_cast<size_t>(state_count) *
                          sizeof(Type3BoundsCudaIPState));
  if (status != cudaSuccess)
    return status;
  status = cudaMalloc(&context->device_ip_point_buffers,
                      static_cast<size_t>(state_count) *
                          sizeof(Type3BoundsCudaDevicePointBuffer));
  if (status != cudaSuccess)
    return status;
  status = cudaMalloc(&context->device_ip_results,
                      static_cast<size_t>(state_count) * sizeof(int));
  if (status != cudaSuccess)
    return status;
  status = cudaMalloc(&context->device_ip_error_flag, sizeof(int));
  if (status != cudaSuccess)
    return status;

  context->ip_state_capacity = state_count;
  return cudaSuccess;
}

const char *PrepareStatusMessage(int prepare_status) {
  switch (prepare_status) {
    case kType3PrepareInvalidCandidate:
      return "invalid type-3 CWS candidate";
    case kType3PrepareNoX0:
      return "no X0 for type-3 CWS candidate";
    case kType3PrepareConstraintOverflow:
      return "type-3 candidate exceeded bounds constraint capacity";
    case kType3PrepareInitialFrontierOverflow:
      return "type-3 candidate initial frontier exceeded point capacity";
    default:
      return "unknown type-3 candidate preparation failure";
  }
}

int PrepareProblemBatchFromCandidates(Type3BoundsCudaContext *context,
                                      const Type3BoundsCudaCwsCandidate *candidates,
                                      uint32_t candidate_count,
                                      uint32_t point_capacity,
                                      Type3BoundsCudaProblem *prepared_problems,
                                      char *error_buffer,
                                      size_t error_buffer_size) {
  cudaError_t status;
  std::vector<int> prepare_statuses(candidate_count, 0);
  uint32_t blocks;

  status = cudaSetDevice(context->device_ordinal);
  if (status != cudaSuccess) {
    SetCudaError(error_buffer, error_buffer_size,
                 "unable to select CUDA device", status);
    return 1;
  }
  if (EnsureCudaPreparationStack(error_buffer, error_buffer_size) != 0)
    return 1;
  status = EnsureCandidateBatchCapacity(context, candidate_count);
  if (status != cudaSuccess) {
    SetCudaError(error_buffer, error_buffer_size,
                 "unable to reserve batched candidate preparation buffers",
                 status);
    return 1;
  }
  status = cudaMemcpy(context->device_candidate_batch, candidates,
                      static_cast<size_t>(candidate_count) *
                          sizeof(Type3BoundsCudaCwsCandidate),
                      cudaMemcpyHostToDevice);
  if (status != cudaSuccess) {
    SetCudaError(error_buffer, error_buffer_size,
                 "unable to upload raw CWS candidate batch", status);
    return 1;
  }

  blocks = (candidate_count + context->block_size - 1) / context->block_size;
  PrepareProblemBatchFromCwsKernel<<<blocks, context->block_size>>>(
      context->device_candidate_batch, candidate_count,
      context->device_problem_batch, context->device_prepare_status_batch,
      point_capacity);
  status = cudaGetLastError();
  if (status != cudaSuccess) {
    SetCudaError(error_buffer, error_buffer_size,
                 "candidate batch preparation kernel launch failed", status);
    return 1;
  }
  status = cudaDeviceSynchronize();
  if (status != cudaSuccess) {
    SetCudaError(error_buffer, error_buffer_size,
                 "candidate batch preparation kernel failed", status);
    return 1;
  }

  status = cudaMemcpy(prepare_statuses.data(),
                      context->device_prepare_status_batch,
                      static_cast<size_t>(candidate_count) * sizeof(int),
                      cudaMemcpyDeviceToHost);
  if (status != cudaSuccess) {
    SetCudaError(error_buffer, error_buffer_size,
                 "unable to download candidate preparation statuses", status);
    return 1;
  }
  for (uint32_t index = 0; index < candidate_count; index++)
    if (prepare_statuses[index] != kType3PrepareOk) {
      SetError(error_buffer, error_buffer_size,
               PrepareStatusMessage(prepare_statuses[index]));
      return 1;
    }

  status = cudaMemcpy(prepared_problems, context->device_problem_batch,
                      static_cast<size_t>(candidate_count) *
                          sizeof(Type3BoundsCudaProblem),
                      cudaMemcpyDeviceToHost);
  if (status != cudaSuccess) {
    SetCudaError(error_buffer, error_buffer_size,
                 "unable to download prepared problems", status);
    return 1;
  }
  return 0;
}

int PrepareProblemFromCandidateOnLane(
    Type3BoundsCudaContext *context, Type3BoundsCudaEnumerationLane *lane,
    const Type3BoundsCudaCwsCandidate *candidate, uint32_t point_capacity,
    Type3BoundsCudaProblem *prepared_problem, uint32_t *prepared_state_count,
    char *error_buffer,
    size_t error_buffer_size) {
  cudaError_t status;
  int prepare_status = 0;
  uint32_t state_count = 0;

  if (EnsureCudaPreparationStack(error_buffer, error_buffer_size) != 0)
    return 1;

  status = EnsureEnumerationCapacity(lane, point_capacity, point_capacity);
  if (status != cudaSuccess) {
    SetCudaError(error_buffer, error_buffer_size,
                 "unable to reserve candidate preparation buffers", status);
    return 1;
  }
  status = cudaMemcpyAsync(lane->device_candidate, candidate, sizeof(*candidate),
                           cudaMemcpyHostToDevice, lane->stream);
  if (status != cudaSuccess) {
    SetCudaError(error_buffer, error_buffer_size,
                 "unable to upload raw CWS candidate", status);
    return 1;
  }
  PrepareProblemFromCwsKernel<<<1, context->block_size, 0, lane->stream>>>(
      lane->device_candidate, lane->device_problem, lane->device_prepare_status,
      lane->device_output_count, lane->device_frontier_a, point_capacity);
  status = cudaGetLastError();
  if (status != cudaSuccess) {
    SetCudaError(error_buffer, error_buffer_size,
                 "candidate preparation kernel launch failed", status);
    return 1;
  }
  status = cudaMemcpyAsync(&prepare_status, lane->device_prepare_status,
                           sizeof(prepare_status), cudaMemcpyDeviceToHost,
                           lane->stream);
  if (status != cudaSuccess) {
    SetCudaError(error_buffer, error_buffer_size,
                 "unable to queue candidate preparation status download",
                 status);
    return 1;
  }
  status = cudaMemcpyAsync(&state_count, lane->device_output_count,
                           sizeof(state_count), cudaMemcpyDeviceToHost,
                           lane->stream);
  if (status != cudaSuccess) {
    SetCudaError(error_buffer, error_buffer_size,
                 "unable to queue initial frontier size download", status);
    return 1;
  }
  status = cudaMemcpyAsync(prepared_problem, lane->device_problem,
                           sizeof(*prepared_problem), cudaMemcpyDeviceToHost,
                           lane->stream);
  if (status != cudaSuccess) {
    SetCudaError(error_buffer, error_buffer_size,
                 "unable to queue prepared problem download", status);
    return 1;
  }
  status = cudaStreamSynchronize(lane->stream);
  if (status != cudaSuccess) {
    SetCudaError(error_buffer, error_buffer_size,
                 "candidate preparation kernel failed", status);
    return 1;
  }
  if (prepare_status != kType3PrepareOk) {
    SetError(error_buffer, error_buffer_size,
             PrepareStatusMessage(prepare_status));
    return 1;
  }
  if (prepared_state_count != nullptr)
    *prepared_state_count = state_count;
  return 0;
}

int EnumeratePreparedOnLane(Type3BoundsCudaContext *context,
                            Type3BoundsCudaEnumerationLane *lane,
                            const Type3BoundsCudaProblem *problem,
                            const Type3BoundsCudaReduction *reduction,
                            uint32_t point_capacity, uint32_t state_count,
                            int64_t *points, uint32_t *point_count,
                            Type3BoundsCudaDevicePointBuffer *resident_output,
                            Type3BoundsCudaStats *stats,
                            char *error_buffer,
                            size_t error_buffer_size) {
  cudaError_t status;
  uint32_t blocks;
  int overflow_flag = 0;
  Type3BoundsCudaStats local_stats = {0};
    int64_t (*device_point_output)[kType3BoundsRuntimeMaxDimension] =
      lane->device_points;
  int64_t (*current)[kType3BoundsRuntimeMaxDimension] = lane->device_frontier_a;
  int64_t (*next)[kType3BoundsRuntimeMaxDimension] = lane->device_frontier_b;
  const int64_t (*download_points)[kType3BoundsRuntimeMaxDimension] =
      lane->device_points;

  if ((problem == nullptr) || (point_count == nullptr)) {
    SetError(error_buffer, error_buffer_size, "invalid CUDA enumerate args");
    return 1;
  }
  if ((resident_output != nullptr) && (resident_output->points != nullptr)) {
    device_point_output = reinterpret_cast<
        int64_t (*)[kType3BoundsRuntimeMaxDimension]>(resident_output->points);
    download_points = device_point_output;
  }
  if (resident_output != nullptr) {
    resident_output->point_count = 0;
    resident_output->point_dimension = problem->n;
    resident_output->point_stride =
        (resident_output->points != nullptr) ? kType3BoundsRuntimeMaxDimension
                                             : 0;
  }
  if ((reduction != nullptr) && (reduction->mod_count > problem->n)) {
    SetError(error_buffer, error_buffer_size,
             "invalid sublattice reduction metadata");
    return 1;
  }
  if (reduction != nullptr)
    for (uint32_t index = 0; index < reduction->mod_count; index++) {
      if (reduction->mod[index] == 0) {
        SetError(error_buffer, error_buffer_size,
                 "invalid sublattice modulus");
        return 1;
      }
    }
  if (state_count == 0) {
    *point_count = 0;
    if (stats != nullptr)
      *stats = local_stats;
    if ((error_buffer != nullptr) && (error_buffer_size > 0))
      error_buffer[0] = '\0';
    return 0;
  }

  status = cudaMemsetAsync(lane->device_stats, 0, sizeof(Type3BoundsCudaStats),
                           lane->stream);
  if (status != cudaSuccess) {
    SetCudaError(error_buffer, error_buffer_size,
                 "unable to clear device stats", status);
    return 1;
  }
  if ((reduction != nullptr) && (reduction->mod_count > 0)) {
    status = cudaMemcpyAsync(lane->device_reduction, reduction,
                             sizeof(*reduction), cudaMemcpyHostToDevice,
                             lane->stream);
    if (status != cudaSuccess) {
      SetCudaError(error_buffer, error_buffer_size,
                   "unable to upload sublattice reduction metadata", status);
      return 1;
    }
  }

  for (int coord = (int)problem->n - 2; coord >= 0; coord--) {
    status = cudaMemsetAsync(lane->device_output_count, 0, sizeof(uint32_t),
                             lane->stream);
    if (status != cudaSuccess) {
      SetCudaError(error_buffer, error_buffer_size,
                   "unable to reset output counter", status);
      return 1;
    }
    status = cudaMemsetAsync(lane->device_overflow_flag, 0, sizeof(int),
                             lane->stream);
    if (status != cudaSuccess) {
      SetCudaError(error_buffer, error_buffer_size,
                   "unable to reset overflow flag", status);
      return 1;
    }

    blocks = (state_count + context->block_size - 1) / context->block_size;
    ExpandFrontierKernel<<<blocks, context->block_size, 0, lane->stream>>>(
        lane->device_problem, coord, current, state_count,
      (coord == 0) ? device_point_output : next, point_capacity,
        lane->device_output_count, lane->device_overflow_flag,
        lane->device_stats);
    status = cudaGetLastError();
    if (status != cudaSuccess) {
      SetCudaError(error_buffer, error_buffer_size,
                   "frontier expansion kernel launch failed", status);
      return 1;
    }

    status = cudaMemcpyAsync(&state_count, lane->device_output_count,
                             sizeof(state_count), cudaMemcpyDeviceToHost,
                             lane->stream);
    if (status != cudaSuccess) {
      SetCudaError(error_buffer, error_buffer_size,
                   "unable to queue output counter download", status);
      return 1;
    }
    status = cudaMemcpyAsync(&overflow_flag, lane->device_overflow_flag,
                             sizeof(overflow_flag), cudaMemcpyDeviceToHost,
                             lane->stream);
    if (status != cudaSuccess) {
      SetCudaError(error_buffer, error_buffer_size,
                   "unable to queue overflow flag download", status);
      return 1;
    }
    status = cudaStreamSynchronize(lane->stream);
    if (status != cudaSuccess) {
      SetCudaError(error_buffer, error_buffer_size,
                   "frontier expansion kernel failed", status);
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

  if ((state_count > 0) && (reduction != nullptr) && (reduction->mod_count > 0)) {
    status = cudaMemsetAsync(lane->device_output_count, 0, sizeof(uint32_t),
                             lane->stream);
    if (status != cudaSuccess) {
      SetCudaError(error_buffer, error_buffer_size,
                   "unable to reset reduction output counter", status);
      return 1;
    }
    status = cudaMemsetAsync(lane->device_overflow_flag, 0, sizeof(int),
                             lane->stream);
    if (status != cudaSuccess) {
      SetCudaError(error_buffer, error_buffer_size,
                   "unable to reset reduction overflow flag", status);
      return 1;
    }

    blocks = (state_count + context->block_size - 1) / context->block_size;
    ReducePointsToSublatticeKernel<<<blocks, context->block_size, 0,
                                     lane->stream>>>(
      lane->device_reduction, problem->n, device_point_output, state_count,
        lane->device_frontier_a, point_capacity, lane->device_output_count,
        lane->device_overflow_flag);
    status = cudaGetLastError();
    if (status != cudaSuccess) {
      SetCudaError(error_buffer, error_buffer_size,
                   "sublattice reduction kernel launch failed", status);
      return 1;
    }

    status = cudaMemcpyAsync(&state_count, lane->device_output_count,
                             sizeof(state_count), cudaMemcpyDeviceToHost,
                             lane->stream);
    if (status != cudaSuccess) {
      SetCudaError(error_buffer, error_buffer_size,
                   "unable to queue reduced point counter download", status);
      return 1;
    }
    status = cudaMemcpyAsync(&overflow_flag, lane->device_overflow_flag,
                             sizeof(overflow_flag), cudaMemcpyDeviceToHost,
                             lane->stream);
    if (status != cudaSuccess) {
      SetCudaError(error_buffer, error_buffer_size,
                   "unable to queue reduction overflow download", status);
      return 1;
    }
    status = cudaStreamSynchronize(lane->stream);
    if (status != cudaSuccess) {
      SetCudaError(error_buffer, error_buffer_size,
                   "sublattice reduction kernel failed", status);
      return 1;
    }
    if (overflow_flag) {
      SetError(error_buffer, error_buffer_size,
               "device reduction exceeded output capacity");
      return 1;
    }
    download_points = lane->device_frontier_a;
  }

  *point_count = state_count;
  if (resident_output != nullptr) {
    resident_output->points =
        (state_count == 0) ? nullptr : const_cast<int64_t *>(download_points[0]);
    resident_output->point_count = state_count;
    resident_output->point_dimension = problem->n;
    resident_output->point_stride = kType3BoundsRuntimeMaxDimension;
  }
  if ((state_count > 0) && ((points != nullptr) || (resident_output == nullptr))) {
    size_t download_bytes = static_cast<size_t>(state_count) *
                            kType3BoundsRuntimeMaxDimension * sizeof(int64_t);

    status = cudaMemcpy(lane->host_point_staging, download_points,
                        download_bytes, cudaMemcpyDeviceToHost);
    if (status != cudaSuccess) {
      SetCudaError(error_buffer, error_buffer_size,
                   "unable to download enumerated points", status);
      return 1;
    }
    if (points != nullptr)
      std::memcpy(points, lane->host_point_staging, download_bytes);
  }
  status = cudaMemcpy(&local_stats, lane->device_stats, sizeof(local_stats),
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

int PrepareProblemFromCandidateSynchronously(
    Type3BoundsCudaContext *context, Type3BoundsCudaEnumerationLane *lane,
    const Type3BoundsCudaCwsCandidate *candidate, uint32_t point_capacity,
    Type3BoundsCudaProblem *prepared_problem, char *error_buffer,
    size_t error_buffer_size) {
  cudaError_t status;
  int prepare_status = 0;

  if (EnsureCudaPreparationStack(error_buffer, error_buffer_size) != 0)
    return 1;

  status = EnsureEnumerationCapacity(lane, point_capacity, point_capacity);
  if (status != cudaSuccess) {
    SetCudaError(error_buffer, error_buffer_size,
                 "unable to reserve candidate preparation buffers", status);
    return 1;
  }
  status = cudaMemcpy(lane->device_candidate, candidate, sizeof(*candidate),
                      cudaMemcpyHostToDevice);
  if (status != cudaSuccess) {
    SetCudaError(error_buffer, error_buffer_size,
                 "unable to upload raw CWS candidate", status);
    return 1;
  }
  status = cudaMemset(lane->device_prepare_status, 0, sizeof(int));
  if (status != cudaSuccess) {
    SetCudaError(error_buffer, error_buffer_size,
                 "unable to clear candidate preparation status", status);
    return 1;
  }
  PrepareProblemFromCwsKernel<<<1, 1>>>(lane->device_candidate,
                                        lane->device_problem,
                                        lane->device_prepare_status,
                                        lane->device_output_count,
                                        lane->device_frontier_a,
                                        point_capacity);
  status = cudaGetLastError();
  if (status != cudaSuccess) {
    SetCudaError(error_buffer, error_buffer_size,
                 "candidate preparation kernel launch failed", status);
    return 1;
  }
  status = cudaDeviceSynchronize();
  if (status != cudaSuccess) {
    SetCudaError(error_buffer, error_buffer_size,
                 "candidate preparation kernel failed", status);
    return 1;
  }
  status = cudaMemcpy(&prepare_status, lane->device_prepare_status,
                      sizeof(prepare_status), cudaMemcpyDeviceToHost);
  if (status != cudaSuccess) {
    SetCudaError(error_buffer, error_buffer_size,
                 "unable to download candidate preparation status", status);
    return 1;
  }
  if (prepare_status != kType3PrepareOk) {
    SetError(error_buffer, error_buffer_size,
             PrepareStatusMessage(prepare_status));
    return 1;
  }
  status = cudaMemcpy(prepared_problem, lane->device_problem,
                      sizeof(*prepared_problem), cudaMemcpyDeviceToHost);
  if (status != cudaSuccess) {
    SetCudaError(error_buffer, error_buffer_size,
                 "unable to download prepared problem", status);
    return 1;
  }
  return 0;
}

int EnumerateCwsOnLane(Type3BoundsCudaContext *context,
                       Type3BoundsCudaEnumerationLane *lane,
                       const Type3BoundsCudaCwsCandidate *candidate,
                       const Type3BoundsCudaReduction *reduction,
                       uint32_t point_capacity, int64_t *points,
                       uint32_t *point_count,
                       Type3BoundsCudaDevicePointBuffer *resident_output,
                       Type3BoundsCudaStats *stats,
                       char *error_buffer, size_t error_buffer_size) {
  Type3BoundsCudaProblem prepared_problem;
  uint32_t state_count = 0;

  if (candidate == nullptr) {
    SetError(error_buffer, error_buffer_size, "invalid raw CWS candidate");
    return 1;
  }
  std::memset(&prepared_problem, 0, sizeof(prepared_problem));
  if (PrepareProblemFromCandidateOnLane(context, lane, candidate,
                                        point_capacity, &prepared_problem,
                                        &state_count,
                                        error_buffer, error_buffer_size) != 0)
    return 1;
  return EnumeratePreparedOnLane(context, lane, &prepared_problem, reduction,
                                 point_capacity, state_count, points,
                                 point_count, resident_output, stats,
                                 error_buffer,
                                 error_buffer_size);
}

int EnumerateCwsSynchronously(Type3BoundsCudaContext *context,
                              Type3BoundsCudaEnumerationLane *lane,
                              const Type3BoundsCudaCwsCandidate *candidate,
                              const Type3BoundsCudaReduction *reduction,
                              uint32_t point_capacity, int64_t *points,
                              uint32_t *point_count,
                              Type3BoundsCudaDevicePointBuffer *resident_output,
                              Type3BoundsCudaStats *stats,
                              char *error_buffer,
                              size_t error_buffer_size) {
  Type3BoundsCudaProblem prepared_problem;

  if (candidate == nullptr) {
    SetError(error_buffer, error_buffer_size, "invalid raw CWS candidate");
    return 1;
  }
  std::memset(&prepared_problem, 0, sizeof(prepared_problem));
  if (PrepareProblemFromCandidateSynchronously(
          context, lane, candidate, point_capacity, &prepared_problem,
          error_buffer, error_buffer_size) != 0)
    return 1;
  return EnumerateSynchronously(context, lane, &prepared_problem, reduction,
                                point_capacity, points, point_count,
                                resident_output, stats,
                                error_buffer, error_buffer_size);
}

int EnumerateOnLane(Type3BoundsCudaContext *context,
                    Type3BoundsCudaEnumerationLane *lane,
                    const Type3BoundsCudaProblem *problem,
                    const Type3BoundsCudaReduction *reduction,
                    uint32_t point_capacity, int64_t *points,
                    uint32_t *point_count,
                    Type3BoundsCudaDevicePointBuffer *resident_output,
                    Type3BoundsCudaStats *stats,
                    char *error_buffer, size_t error_buffer_size) {
  cudaError_t status;
  uint32_t state_count;
  uint32_t blocks;
  Type3BoundsCudaStats local_stats = {0};

  if ((problem == nullptr) || (point_count == nullptr)) {
    SetError(error_buffer, error_buffer_size, "invalid CUDA enumerate args");
    return 1;
  }
  if (point_capacity == 0) {
    SetError(error_buffer, error_buffer_size, "invalid point capacity");
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
  if ((reduction != nullptr) && (reduction->mod_count > problem->n)) {
    SetError(error_buffer, error_buffer_size,
             "invalid sublattice reduction metadata");
    return 1;
  }
  if (reduction != nullptr)
    for (uint32_t index = 0; index < reduction->mod_count; index++) {
      if (reduction->mod[index] == 0) {
        SetError(error_buffer, error_buffer_size,
                 "invalid sublattice modulus");
        return 1;
      }
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

  status = EnsureEnumerationCapacity(lane, point_capacity, point_capacity);
  if (status != cudaSuccess) {
    SetCudaError(error_buffer, error_buffer_size,
                 "unable to reserve enumeration buffers", status);
    return 1;
  }

  status = cudaMemcpyAsync(lane->device_problem, problem, sizeof(*problem),
                           cudaMemcpyHostToDevice, lane->stream);
  if (status != cudaSuccess) {
    SetCudaError(error_buffer, error_buffer_size,
                 "unable to upload enumeration problem", status);
    return 1;
  }

  blocks = (state_count + context->block_size - 1) / context->block_size;
  InitializeFrontierKernel<<<blocks, context->block_size, 0, lane->stream>>>(
      lane->device_problem, lane->device_frontier_a, state_count);
  status = cudaGetLastError();
  if (status != cudaSuccess) {
    SetCudaError(error_buffer, error_buffer_size,
                 "initial frontier kernel launch failed", status);
    return 1;
  }
  status = cudaStreamSynchronize(lane->stream);
  if (status != cudaSuccess) {
    SetCudaError(error_buffer, error_buffer_size,
                 "initial frontier kernel failed", status);
    return 1;
  }
  return EnumeratePreparedOnLane(context, lane, problem, reduction,
                                 point_capacity, state_count, points,
                                 point_count, resident_output, stats,
                                 error_buffer,
                                 error_buffer_size);
}

int EnumerateSynchronously(Type3BoundsCudaContext *context,
                           Type3BoundsCudaEnumerationLane *lane,
                           const Type3BoundsCudaProblem *problem,
                           const Type3BoundsCudaReduction *reduction,
                           uint32_t point_capacity, int64_t *points,
                           uint32_t *point_count,
                           Type3BoundsCudaDevicePointBuffer *resident_output,
                           Type3BoundsCudaStats *stats,
                           char *error_buffer, size_t error_buffer_size) {
  cudaError_t status;
  uint32_t state_count;
  uint32_t blocks;
  int overflow_flag = 0;
  Type3BoundsCudaStats local_stats = {0};
  int64_t (*device_point_output)[kType3BoundsRuntimeMaxDimension];
  int64_t (*current)[kType3BoundsRuntimeMaxDimension];
  int64_t (*next)[kType3BoundsRuntimeMaxDimension];
  const int64_t (*download_points)[kType3BoundsRuntimeMaxDimension];

  if ((problem == nullptr) || (point_count == nullptr)) {
    SetError(error_buffer, error_buffer_size, "invalid CUDA enumerate args");
    return 1;
  }
  device_point_output = lane->device_points;
  if ((resident_output != nullptr) && (resident_output->points != nullptr))
    device_point_output = reinterpret_cast<
        int64_t (*)[kType3BoundsRuntimeMaxDimension]>(resident_output->points);
  if (resident_output != nullptr) {
    resident_output->point_count = 0;
    resident_output->point_dimension = problem->n;
    resident_output->point_stride =
        (resident_output->points != nullptr) ? kType3BoundsRuntimeMaxDimension
                                             : 0;
  }
  if ((problem->n == 0) ||
      (problem->n > kType3BoundsRuntimeMaxDimension) ||
      (problem->ambient_count == 0) ||
      (problem->ambient_count > kType3BoundsRuntimeMaxAmbient)) {
    SetError(error_buffer, error_buffer_size,
             "problem dimensions exceed CUDA runtime limits");
    return 1;
  }
  if ((reduction != nullptr) && (reduction->mod_count > problem->n)) {
    SetError(error_buffer, error_buffer_size,
             "invalid sublattice reduction metadata");
    return 1;
  }
  if (reduction != nullptr)
    for (uint32_t index = 0; index < reduction->mod_count; index++) {
      if (reduction->mod[index] == 0) {
        SetError(error_buffer, error_buffer_size,
                 "invalid sublattice modulus");
        return 1;
      }
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

  status = EnsureEnumerationCapacity(lane, point_capacity, point_capacity);
  if (status != cudaSuccess) {
    SetCudaError(error_buffer, error_buffer_size,
                 "unable to reserve enumeration buffers", status);
    return 1;
  }
  download_points = device_point_output;

  status = cudaMemcpy(lane->device_problem, problem, sizeof(*problem),
                      cudaMemcpyHostToDevice);
  if (status != cudaSuccess) {
    SetCudaError(error_buffer, error_buffer_size,
                 "unable to upload enumeration problem", status);
    return 1;
  }
  if ((reduction != nullptr) && (reduction->mod_count > 0)) {
    status = cudaMemcpy(lane->device_reduction, reduction, sizeof(*reduction),
                        cudaMemcpyHostToDevice);
    if (status != cudaSuccess) {
      SetCudaError(error_buffer, error_buffer_size,
                   "unable to upload sublattice reduction metadata", status);
      return 1;
    }
  }

  status = cudaMemset(lane->device_stats, 0, sizeof(Type3BoundsCudaStats));
  if (status != cudaSuccess) {
    SetCudaError(error_buffer, error_buffer_size,
                 "unable to clear device stats", status);
    return 1;
  }

  blocks = (state_count + context->block_size - 1) / context->block_size;
  InitializeFrontierKernel<<<blocks, context->block_size>>>(
      lane->device_problem, lane->device_frontier_a, state_count);
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

  current = lane->device_frontier_a;
  next = lane->device_frontier_b;
  for (int coord = (int)problem->n - 2; coord >= 0; coord--) {
    status = cudaMemset(lane->device_output_count, 0, sizeof(uint32_t));
    if (status != cudaSuccess) {
      SetCudaError(error_buffer, error_buffer_size,
                   "unable to reset output counter", status);
      return 1;
    }
    status = cudaMemset(lane->device_overflow_flag, 0, sizeof(int));
    if (status != cudaSuccess) {
      SetCudaError(error_buffer, error_buffer_size,
                   "unable to reset overflow flag", status);
      return 1;
    }

    blocks = (state_count + context->block_size - 1) / context->block_size;
    ExpandFrontierKernel<<<blocks, context->block_size>>>(
        lane->device_problem, coord, current, state_count,
      (coord == 0) ? device_point_output : next, point_capacity,
        lane->device_output_count, lane->device_overflow_flag,
        lane->device_stats);
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

    status = cudaMemcpy(&state_count, lane->device_output_count,
                        sizeof(state_count), cudaMemcpyDeviceToHost);
    if (status != cudaSuccess) {
      SetCudaError(error_buffer, error_buffer_size,
                   "unable to download output counter", status);
      return 1;
    }
    status = cudaMemcpy(&overflow_flag, lane->device_overflow_flag,
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

  if ((state_count > 0) && (reduction != nullptr) && (reduction->mod_count > 0)) {
    status = cudaMemset(lane->device_output_count, 0, sizeof(uint32_t));
    if (status != cudaSuccess) {
      SetCudaError(error_buffer, error_buffer_size,
                   "unable to reset reduction output counter", status);
      return 1;
    }
    status = cudaMemset(lane->device_overflow_flag, 0, sizeof(int));
    if (status != cudaSuccess) {
      SetCudaError(error_buffer, error_buffer_size,
                   "unable to reset reduction overflow flag", status);
      return 1;
    }

    blocks = (state_count + context->block_size - 1) / context->block_size;
    ReducePointsToSublatticeKernel<<<blocks, context->block_size>>>(
      lane->device_reduction, problem->n, device_point_output, state_count,
        lane->device_frontier_a, point_capacity, lane->device_output_count,
        lane->device_overflow_flag);
    status = cudaGetLastError();
    if (status != cudaSuccess) {
      SetCudaError(error_buffer, error_buffer_size,
                   "sublattice reduction kernel launch failed", status);
      return 1;
    }
    status = cudaDeviceSynchronize();
    if (status != cudaSuccess) {
      SetCudaError(error_buffer, error_buffer_size,
                   "sublattice reduction kernel failed", status);
      return 1;
    }

    status = cudaMemcpy(&state_count, lane->device_output_count,
                        sizeof(state_count), cudaMemcpyDeviceToHost);
    if (status != cudaSuccess) {
      SetCudaError(error_buffer, error_buffer_size,
                   "unable to download reduced point counter", status);
      return 1;
    }
    status = cudaMemcpy(&overflow_flag, lane->device_overflow_flag,
                        sizeof(overflow_flag), cudaMemcpyDeviceToHost);
    if (status != cudaSuccess) {
      SetCudaError(error_buffer, error_buffer_size,
                   "unable to download reduction overflow flag", status);
      return 1;
    }
    if (overflow_flag) {
      SetError(error_buffer, error_buffer_size,
               "device reduction exceeded output capacity");
      return 1;
    }
    download_points = lane->device_frontier_a;
  }

  *point_count = state_count;
  if (resident_output != nullptr) {
    resident_output->points =
        (state_count == 0) ? nullptr : const_cast<int64_t *>(download_points[0]);
    resident_output->point_count = state_count;
    resident_output->point_dimension = problem->n;
    resident_output->point_stride = kType3BoundsRuntimeMaxDimension;
  }
  if ((state_count > 0) && ((points != nullptr) || (resident_output == nullptr))) {
    size_t download_bytes = static_cast<size_t>(state_count) *
                            kType3BoundsRuntimeMaxDimension * sizeof(int64_t);

    status = cudaMemcpy(lane->host_point_staging, download_points,
                        static_cast<size_t>(state_count) *
                            kType3BoundsRuntimeMaxDimension * sizeof(int64_t),
                        cudaMemcpyDeviceToHost);
    if (status != cudaSuccess) {
      SetCudaError(error_buffer, error_buffer_size,
                   "unable to download enumerated points", status);
      return 1;
    }
    if (points != nullptr)
      std::memcpy(points, lane->host_point_staging, download_bytes);
  }

  status = cudaMemcpy(&local_stats, lane->device_stats, sizeof(local_stats),
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
  if (capacity == 0)
    capacity = 1;
  if (block_size == 0)
    block_size = 128;

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

  status = cudaGetDevice(&created->device_ordinal);
  if (status != cudaSuccess) {
    SetCudaError(error_buffer, error_buffer_size,
                 "unable to query active CUDA device", status);
    DestroyContext(created);
    return 1;
  }

  status = cudaMalloc(&created->device_jobs,
                      static_cast<size_t>(capacity) * sizeof(Type3BoundsJob));
  if (status != cudaSuccess) {
    SetCudaError(error_buffer, error_buffer_size,
                 "unable to allocate device jobs", status);
    DestroyContext(created);
    return 1;
  }

  status = cudaMalloc(&created->device_results,
                      static_cast<size_t>(capacity) *
                          sizeof(Type3BoundsResult));
  if (status != cudaSuccess) {
    SetCudaError(error_buffer, error_buffer_size,
                 "unable to allocate device results", status);
    DestroyContext(created);
    return 1;
  }

  created->lane_count = ParseUnsignedEnv("PALP_TYPE3_CUDA_BATCH_LANES", 4);
  created->lanes = static_cast<Type3BoundsCudaEnumerationLane *>(
      std::calloc(created->lane_count, sizeof(Type3BoundsCudaEnumerationLane)));
  if (created->lanes == nullptr) {
    SetError(error_buffer, error_buffer_size,
             "unable to allocate enumeration lanes");
    DestroyContext(created);
    return 1;
  }

  for (uint32_t index = 0; index < created->lane_count; index++) {
    status = cudaStreamCreateWithFlags(&created->lanes[index].stream,
                                       cudaStreamNonBlocking);
    if (status != cudaSuccess) {
      SetCudaError(error_buffer, error_buffer_size,
                   "unable to create CUDA stream", status);
      DestroyContext(created);
      return 1;
    }
  }

  created->batch_workers = new (std::nothrow) Type3BoundsCudaBatchWorkers();
  if (created->batch_workers == nullptr) {
    SetError(error_buffer, error_buffer_size,
             "unable to allocate batch worker state");
    DestroyContext(created);
    return 1;
  }

  try {
    created->batch_workers->threads.reserve(created->lane_count);
    for (uint32_t index = 0; index < created->lane_count; index++)
      created->batch_workers->threads.emplace_back(BatchWorkerLoop, created,
                                                   index);
  } catch (const std::exception &error) {
    SetError(error_buffer, error_buffer_size, error.what());
    DestroyContext(created);
    return 1;
  } catch (...) {
    SetError(error_buffer, error_buffer_size,
             "unable to start batch worker threads");
    DestroyContext(created);
    return 1;
  }

  *context = created;
  if ((error_buffer != nullptr) && (error_buffer_size > 0))
    error_buffer[0] = '\0';
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
    if ((error_buffer != nullptr) && (error_buffer_size > 0))
      error_buffer[0] = '\0';
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

  if ((error_buffer != nullptr) && (error_buffer_size > 0))
    error_buffer[0] = '\0';
  return 0;
}

extern "C" int Type3BoundsCudaClassifyEquationBatch(
    Type3BoundsCudaContext *context,
    const Type3BoundsCudaEquation *equations,
    uint32_t equation_count,
    const int64_t *points,
    uint32_t point_count,
    uint32_t point_dimension,
  uint32_t point_stride,
    uint8_t *has_negative,
    char *error_buffer,
    size_t error_buffer_size) {
  cudaError_t status;
  size_t point_value_count;
  uint32_t blocks;

  if ((context == nullptr) || (equations == nullptr) ||
      (has_negative == nullptr)) {
    SetError(error_buffer, error_buffer_size,
             "invalid CUDA equation-classify args");
    return 1;
  }
  if (equation_count == 0) {
    if ((error_buffer != nullptr) && (error_buffer_size > 0))
      error_buffer[0] = '\0';
    return 0;
  }
  if ((point_dimension == 0) ||
      (point_dimension > kType3BoundsRuntimeMaxDimension)) {
    SetError(error_buffer, error_buffer_size,
             "invalid CUDA equation point dimension");
    return 1;
  }

  if (point_stride < point_dimension) {
    SetError(error_buffer, error_buffer_size,
             "invalid CUDA equation point stride");
    return 1;
  }

  std::memset(has_negative, 0,
              static_cast<size_t>(equation_count) * sizeof(uint8_t));
  if (point_count == 0) {
    if ((error_buffer != nullptr) && (error_buffer_size > 0))
      error_buffer[0] = '\0';
    return 0;
  }
  if (points == nullptr) {
    SetError(error_buffer, error_buffer_size,
             "null CUDA equation point buffer");
    return 1;
  }

  point_value_count = static_cast<size_t>(point_count) * point_stride;

  status = cudaSetDevice(context->device_ordinal);
  if (status != cudaSuccess) {
    SetCudaError(error_buffer, error_buffer_size,
                 "unable to select CUDA device", status);
    return 1;
  }
  status = EnsureEquationClassificationCapacity(context, equation_count,
                                                point_value_count);
  if (status != cudaSuccess) {
    SetCudaError(error_buffer, error_buffer_size,
                 "unable to reserve equation classifier buffers", status);
    return 1;
  }

  status = cudaMemcpy(context->device_equation_batch, equations,
                      static_cast<size_t>(equation_count) *
                          sizeof(Type3BoundsCudaEquation),
                      cudaMemcpyHostToDevice);
  if (status != cudaSuccess) {
    SetCudaError(error_buffer, error_buffer_size,
                 "unable to upload equations to device", status);
    return 1;
  }
  status = cudaMemcpy(context->device_equation_points, points,
                      point_value_count * sizeof(int64_t),
                      cudaMemcpyHostToDevice);
  if (status != cudaSuccess) {
    SetCudaError(error_buffer, error_buffer_size,
                 "unable to upload equation points to device", status);
    return 1;
  }
  status = cudaMemset(context->device_equation_negative_flags, 0,
                      static_cast<size_t>(equation_count) * sizeof(uint8_t));
  if (status != cudaSuccess) {
    SetCudaError(error_buffer, error_buffer_size,
                 "unable to reset equation classifier flags", status);
    return 1;
  }

  blocks = equation_count;
  ClassifyEquationNegativityKernel<<<blocks, context->block_size>>>(
      context->device_equation_batch, equation_count,
      context->device_equation_points, point_count, point_dimension,
      point_stride,
      context->device_equation_negative_flags);
  status = cudaGetLastError();
  if (status != cudaSuccess) {
    SetCudaError(error_buffer, error_buffer_size,
                 "equation classifier kernel launch failed", status);
    return 1;
  }
  status = cudaDeviceSynchronize();
  if (status != cudaSuccess) {
    SetCudaError(error_buffer, error_buffer_size,
                 "equation classifier kernel failed", status);
    return 1;
  }

  status = cudaMemcpy(has_negative, context->device_equation_negative_flags,
                      static_cast<size_t>(equation_count) * sizeof(uint8_t),
                      cudaMemcpyDeviceToHost);
  if (status != cudaSuccess) {
    SetCudaError(error_buffer, error_buffer_size,
                 "unable to download equation classifier flags", status);
    return 1;
  }

  if ((error_buffer != nullptr) && (error_buffer_size > 0))
    error_buffer[0] = '\0';
  return 0;
}

extern "C" int Type3BoundsCudaClassifyEquationTaskBatch(
    Type3BoundsCudaContext *context,
    const Type3BoundsCudaEquation *equations,
    const Type3BoundsCudaEquationTask *tasks,
    uint32_t equation_count,
    const int64_t *points,
    uint32_t point_count,
    uint32_t point_stride,
    uint8_t *has_negative,
    char *error_buffer,
    size_t error_buffer_size) {
  cudaError_t status;
  size_t point_value_count;
  uint32_t blocks;
  uint32_t uploaded_point_count;
  uint32_t uploaded_point_stride;

  if ((context == nullptr) || (equations == nullptr) || (tasks == nullptr) ||
      (has_negative == nullptr)) {
    SetError(error_buffer, error_buffer_size,
             "invalid CUDA equation-task-classify args");
    return 1;
  }
  if (equation_count == 0) {
    if ((error_buffer != nullptr) && (error_buffer_size > 0))
      error_buffer[0] = '\0';
    return 0;
  }

  std::memset(has_negative, 0,
              static_cast<size_t>(equation_count) * sizeof(uint8_t));
  if (points != nullptr) {
    if (point_stride < kType3BoundsRuntimeMaxDimension) {
      SetError(error_buffer, error_buffer_size,
               "invalid CUDA equation-task point stride");
      return 1;
    }
    uploaded_point_count = point_count;
    uploaded_point_stride = point_stride;
    point_value_count = static_cast<size_t>(uploaded_point_count) *
                        uploaded_point_stride;
  } else {
    uploaded_point_count = context->uploaded_equation_point_count;
    uploaded_point_stride = context->uploaded_equation_point_stride;
    if ((uploaded_point_count == 0) ||
        (uploaded_point_stride < kType3BoundsRuntimeMaxDimension)) {
      SetError(error_buffer, error_buffer_size,
               "no cached CUDA equation-task point buffer");
      return 1;
    }
    point_value_count = static_cast<size_t>(uploaded_point_count) *
                        uploaded_point_stride;
  }

  status = cudaSetDevice(context->device_ordinal);
  if (status != cudaSuccess) {
    SetCudaError(error_buffer, error_buffer_size,
                 "unable to select CUDA device", status);
    return 1;
  }
  status = EnsureEquationClassificationCapacity(context, equation_count,
                                                point_value_count);
  if (status != cudaSuccess) {
    SetCudaError(error_buffer, error_buffer_size,
                 "unable to reserve equation task classifier buffers", status);
    return 1;
  }

  status = cudaMemcpy(context->device_equation_batch, equations,
                      static_cast<size_t>(equation_count) *
                          sizeof(Type3BoundsCudaEquation),
                      cudaMemcpyHostToDevice);
  if (status != cudaSuccess) {
    SetCudaError(error_buffer, error_buffer_size,
                 "unable to upload equations to device", status);
    return 1;
  }
  status = cudaMemcpy(context->device_equation_tasks, tasks,
                      static_cast<size_t>(equation_count) *
                          sizeof(Type3BoundsCudaEquationTask),
                      cudaMemcpyHostToDevice);
  if (status != cudaSuccess) {
    SetCudaError(error_buffer, error_buffer_size,
                 "unable to upload equation task metadata", status);
    return 1;
  }
  if ((points != nullptr) && (point_value_count != 0)) {
    status = cudaMemcpy(context->device_equation_points, points,
                        point_value_count * sizeof(int64_t),
                        cudaMemcpyHostToDevice);
    if (status != cudaSuccess) {
      SetCudaError(error_buffer, error_buffer_size,
                   "unable to upload equation task points", status);
      return 1;
    }
    context->uploaded_equation_point_count = uploaded_point_count;
    context->uploaded_equation_point_stride = uploaded_point_stride;
  }
  status = cudaMemset(context->device_equation_negative_flags, 0,
                      static_cast<size_t>(equation_count) * sizeof(uint8_t));
  if (status != cudaSuccess) {
    SetCudaError(error_buffer, error_buffer_size,
                 "unable to reset equation task classifier flags", status);
    return 1;
  }

  blocks = equation_count;
  ClassifyEquationTaskBatchKernel<<<blocks, context->block_size>>>(
      context->device_equation_batch, context->device_equation_tasks,
      equation_count, context->device_equation_points, uploaded_point_stride,
      context->device_equation_negative_flags);
  status = cudaGetLastError();
  if (status != cudaSuccess) {
    SetCudaError(error_buffer, error_buffer_size,
                 "equation task classifier kernel launch failed", status);
    return 1;
  }
  status = cudaDeviceSynchronize();
  if (status != cudaSuccess) {
    SetCudaError(error_buffer, error_buffer_size,
                 "equation task classifier kernel failed", status);
    return 1;
  }

  status = cudaMemcpy(has_negative, context->device_equation_negative_flags,
                      static_cast<size_t>(equation_count) * sizeof(uint8_t),
                      cudaMemcpyDeviceToHost);
  if (status != cudaSuccess) {
    SetCudaError(error_buffer, error_buffer_size,
                 "unable to download equation task classifier flags", status);
    return 1;
  }

  if ((error_buffer != nullptr) && (error_buffer_size > 0))
    error_buffer[0] = '\0';
  return 0;
}

extern "C" int Type3BoundsCudaRunIPCheckBatch(
    Type3BoundsCudaContext *context,
    const Type3BoundsCudaIPState *states,
    uint32_t state_count,
    const Type3BoundsCudaPointBuffer *point_buffers,
    uint32_t point_stride,
    int *results,
    char *error_buffer,
    size_t error_buffer_size) {
  cudaError_t status;
  size_t point_value_count = 0;
  uint32_t point_count = 0;
  uint32_t block_size;
  int device_error = 0;

  if ((context == nullptr) || (states == nullptr) || (point_buffers == nullptr) ||
      (results == nullptr)) {
    SetError(error_buffer, error_buffer_size,
             "invalid CUDA IP-check batch args");
    return 1;
  }
  if (state_count == 0) {
    if ((error_buffer != nullptr) && (error_buffer_size > 0))
      error_buffer[0] = '\0';
    return 0;
  }
  if (point_stride == 0) {
    SetError(error_buffer, error_buffer_size,
             "invalid CUDA IP-check point stride");
    return 1;
  }

  for (uint32_t index = 0; index < state_count; index++) {
    if ((states[index].point_dimension == 0) ||
        (states[index].point_dimension > kType3BoundsRuntimeMaxDimension) ||
      (states[index].point_dimension > point_stride) ||
        (states[index].vertex_count > kType3BoundsRuntimeMaxIPVertices) ||
        (states[index].facet_count > kType3BoundsRuntimeMaxIPEquations) ||
        (states[index].ceq_count > kType3BoundsRuntimeMaxIPEquations) ||
        (point_buffers[index].point_count != states[index].point_count) ||
        ((point_buffers[index].point_count != 0) &&
         (point_buffers[index].points == nullptr))) {
      SetError(error_buffer, error_buffer_size,
               "invalid CUDA IP-check state payload");
      return 1;
    }
    if (states[index].point_offset + states[index].point_count > point_count)
      point_count = states[index].point_offset + states[index].point_count;
  }
  point_value_count = static_cast<size_t>(point_count) * point_stride;

  status = cudaSetDevice(context->device_ordinal);
  if (status != cudaSuccess) {
    SetCudaError(error_buffer, error_buffer_size,
                 "unable to select CUDA device", status);
    return 1;
  }
  status = EnsureEquationClassificationCapacity(
      context, 1, point_value_count == 0 ? 1 : point_value_count);
  if (status != cudaSuccess) {
    SetCudaError(error_buffer, error_buffer_size,
                 "unable to reserve CUDA IP-check point buffers", status);
    return 1;
  }
  status = EnsureIPCheckBatchCapacity(context, state_count);
  if (status != cudaSuccess) {
    SetCudaError(error_buffer, error_buffer_size,
                 "unable to reserve CUDA IP-check state buffers", status);
    return 1;
  }

  for (uint32_t index = 0; index < state_count; index++) {
    if (point_buffers[index].point_count == 0)
      continue;
    status = cudaMemcpy(
        context->device_equation_points +
            static_cast<size_t>(states[index].point_offset) * point_stride,
        point_buffers[index].points,
        static_cast<size_t>(point_buffers[index].point_count) * point_stride *
            sizeof(int64_t),
        cudaMemcpyHostToDevice);
    if (status != cudaSuccess) {
      SetCudaError(error_buffer, error_buffer_size,
                   "unable to upload CUDA IP-check points", status);
      return 1;
    }
  }
  context->uploaded_equation_point_count = point_count;
  context->uploaded_equation_point_stride = point_stride;

  status = cudaMemcpy(context->device_ip_states, states,
                      static_cast<size_t>(state_count) *
                          sizeof(Type3BoundsCudaIPState),
                      cudaMemcpyHostToDevice);
  if (status != cudaSuccess) {
    SetCudaError(error_buffer, error_buffer_size,
                 "unable to upload CUDA IP-check state", status);
    return 1;
  }
  status = cudaMemset(context->device_ip_error_flag, 0, sizeof(int));
  if (status != cudaSuccess) {
    SetCudaError(error_buffer, error_buffer_size,
                 "unable to reset CUDA IP-check error flag", status);
    return 1;
  }

  block_size = context->block_size;
  if ((block_size == 0) || (block_size > 256))
    block_size = 256;
  RunIPCheckBatchKernel<<<state_count, block_size>>>(
      context->device_ip_states, state_count, context->device_equation_points,
      point_stride, context->device_ip_results, context->device_ip_error_flag);
  status = cudaGetLastError();
  if (status != cudaSuccess) {
    SetCudaError(error_buffer, error_buffer_size,
                 "CUDA IP-check kernel launch failed", status);
    return 1;
  }
  status = cudaDeviceSynchronize();
  if (status != cudaSuccess) {
    SetCudaError(error_buffer, error_buffer_size,
                 "CUDA IP-check kernel failed", status);
    return 1;
  }

  status = cudaMemcpy(&device_error, context->device_ip_error_flag,
                      sizeof(device_error), cudaMemcpyDeviceToHost);
  if (status != cudaSuccess) {
    SetCudaError(error_buffer, error_buffer_size,
                 "unable to download CUDA IP-check error flag", status);
    return 1;
  }
  if (device_error != 0) {
    SetError(error_buffer, error_buffer_size,
             "CUDA IP-check state machine overflowed bounded geometry state");
    return 1;
  }

  status = cudaMemcpy(results, context->device_ip_results,
                      static_cast<size_t>(state_count) * sizeof(int),
                      cudaMemcpyDeviceToHost);
  if (status != cudaSuccess) {
    SetCudaError(error_buffer, error_buffer_size,
                 "unable to download CUDA IP-check results", status);
    return 1;
  }

  if ((error_buffer != nullptr) && (error_buffer_size > 0))
    error_buffer[0] = '\0';
  return 0;
}

extern "C" int Type3BoundsCudaRunIPCheckDeviceBatch(
    Type3BoundsCudaContext *context,
    const Type3BoundsCudaDevicePointBuffer *point_buffers,
    uint32_t state_count,
    int *results,
    char *error_buffer,
    size_t error_buffer_size) {
  cudaError_t status;
  uint32_t block_size;
  int device_error = 0;

  if ((context == nullptr) || (point_buffers == nullptr) ||
      (results == nullptr)) {
    SetError(error_buffer, error_buffer_size,
             "invalid CUDA device IP-check batch args");
    return 1;
  }
  if (state_count == 0) {
    if ((error_buffer != nullptr) && (error_buffer_size > 0))
      error_buffer[0] = '\0';
    return 0;
  }
  for (uint32_t index = 0; index < state_count; index++)
    if ((point_buffers[index].point_dimension == 0) ||
        (point_buffers[index].point_dimension > kType3BoundsRuntimeMaxDimension) ||
        (point_buffers[index].point_stride < point_buffers[index].point_dimension) ||
        ((point_buffers[index].point_count != 0) &&
         (point_buffers[index].points == nullptr))) {
      SetError(error_buffer, error_buffer_size,
               "invalid CUDA device IP-check point payload");
      return 1;
    }

  status = cudaSetDevice(context->device_ordinal);
  if (status != cudaSuccess) {
    SetCudaError(error_buffer, error_buffer_size,
                 "unable to select CUDA device", status);
    return 1;
  }
  status = EnsureIPCheckBatchCapacity(context, state_count);
  if (status != cudaSuccess) {
    SetCudaError(error_buffer, error_buffer_size,
                 "unable to reserve CUDA device IP-check buffers", status);
    return 1;
  }

  status = cudaMemcpy(context->device_ip_point_buffers, point_buffers,
                      static_cast<size_t>(state_count) *
                          sizeof(Type3BoundsCudaDevicePointBuffer),
                      cudaMemcpyHostToDevice);
  if (status != cudaSuccess) {
    SetCudaError(error_buffer, error_buffer_size,
                 "unable to upload CUDA device IP-check descriptors", status);
    return 1;
  }
  status = cudaMemset(context->device_ip_error_flag, 0, sizeof(int));
  if (status != cudaSuccess) {
    SetCudaError(error_buffer, error_buffer_size,
                 "unable to reset CUDA device IP-check error flag", status);
    return 1;
  }

  block_size = context->block_size;
  if ((block_size == 0) || (block_size > 256))
    block_size = 256;
  RunIPCheckDeviceBatchKernel<<<state_count, block_size>>>(
      context->device_ip_point_buffers, state_count,
      context->device_ip_results, context->device_ip_error_flag);
  status = cudaGetLastError();
  if (status != cudaSuccess) {
    SetCudaError(error_buffer, error_buffer_size,
                 "CUDA device IP-check kernel launch failed", status);
    return 1;
  }
  status = cudaDeviceSynchronize();
  if (status != cudaSuccess) {
    SetCudaError(error_buffer, error_buffer_size,
                 "CUDA device IP-check kernel failed", status);
    return 1;
  }
  status = cudaMemcpy(&device_error, context->device_ip_error_flag,
                      sizeof(device_error), cudaMemcpyDeviceToHost);
  if (status != cudaSuccess) {
    SetCudaError(error_buffer, error_buffer_size,
                 "unable to download CUDA device IP-check error flag", status);
    return 1;
  }
  if (device_error != 0) {
    SetError(error_buffer, error_buffer_size,
             "CUDA device IP-check overflowed bounded geometry state");
    return 1;
  }
  status = cudaMemcpy(results, context->device_ip_results,
                      static_cast<size_t>(state_count) * sizeof(int),
                      cudaMemcpyDeviceToHost);
  if (status != cudaSuccess) {
    SetCudaError(error_buffer, error_buffer_size,
                 "unable to download CUDA device IP-check results", status);
    return 1;
  }

  if ((error_buffer != nullptr) && (error_buffer_size > 0))
    error_buffer[0] = '\0';
  return 0;
}

extern "C" int Type3BoundsCudaEnumerate(Type3BoundsCudaContext *context,
                                         const Type3BoundsCudaProblem *problem,
                                         const Type3BoundsCudaReduction *reduction,
                                         uint32_t point_capacity,
                                         int64_t *points,
                                         uint32_t *point_count,
                                         Type3BoundsCudaStats *stats,
                                         char *error_buffer,
                                         size_t error_buffer_size) {
  if (context == nullptr) {
    SetError(error_buffer, error_buffer_size, "invalid CUDA enumerate args");
    return 1;
  }
  if ((context->lanes == nullptr) || (context->lane_count == 0)) {
    SetError(error_buffer, error_buffer_size,
             "CUDA enumeration lane state not initialized");
    return 1;
  }

  return EnumerateSynchronously(context, &context->lanes[0], problem,
                                reduction, point_capacity, points,
                                point_count, nullptr, stats, error_buffer,
                                error_buffer_size);
}

extern "C" int Type3BoundsCudaEnumerateCws(Type3BoundsCudaContext *context,
                                             const Type3BoundsCudaCwsCandidate *candidate,
                                             const Type3BoundsCudaReduction *reduction,
                                             uint32_t point_capacity,
                                             int64_t *points,
                                             uint32_t *point_count,
                                             Type3BoundsCudaStats *stats,
                                             char *error_buffer,
                                             size_t error_buffer_size) {
  if (context == nullptr) {
    SetError(error_buffer, error_buffer_size, "invalid CUDA enumerate args");
    return 1;
  }
  if ((context->lanes == nullptr) || (context->lane_count == 0)) {
    SetError(error_buffer, error_buffer_size,
             "CUDA enumeration lane state not initialized");
    return 1;
  }

  return EnumerateCwsSynchronously(context, &context->lanes[0], candidate,
                                   reduction, point_capacity, points,
                                   point_count, nullptr, stats, error_buffer,
                                   error_buffer_size);
}

extern "C" int Type3BoundsCudaEnumerateBatch(Type3BoundsCudaContext *context,
                                              const Type3BoundsCudaProblem *problems,
                                              const Type3BoundsCudaReduction *reductions,
                                              uint32_t problem_count,
                                              uint32_t point_capacity,
                                              int64_t **points,
                                              uint32_t *point_counts,
                                              Type3BoundsCudaStats *stats,
                                              char *error_buffer,
                                              size_t error_buffer_size) {
  Type3BoundsCudaBatchWorkers *workers;

  if ((context == nullptr) || (problems == nullptr) || (points == nullptr) ||
      (point_counts == nullptr)) {
    SetError(error_buffer, error_buffer_size,
             "invalid CUDA enumerate-batch args");
    return 1;
  }
  if (problem_count == 0) {
    if ((error_buffer != nullptr) && (error_buffer_size > 0))
      error_buffer[0] = '\0';
    return 0;
  }
  if ((context->lanes == nullptr) || (context->lane_count == 0)) {
    SetError(error_buffer, error_buffer_size,
             "CUDA enumeration lane state not initialized");
    return 1;
  }
  workers = context->batch_workers;
  if ((workers == nullptr) || workers->threads.empty()) {
    SetError(error_buffer, error_buffer_size,
             "CUDA batch worker state not initialized");
    return 1;
  }

  if (stats != nullptr)
    std::memset(stats, 0,
                static_cast<size_t>(problem_count) * sizeof(Type3BoundsCudaStats));
  std::memset(point_counts, 0,
              static_cast<size_t>(problem_count) * sizeof(uint32_t));

  {
    std::unique_lock<std::mutex> lock(workers->mutex);

    while (workers->batch_active)
      workers->done_cv.wait(lock);

    workers->problems = problems;
  workers->candidates = nullptr;
    workers->problem_count = problem_count;
    workers->reductions = reductions;
    workers->point_capacity = point_capacity;
    workers->points = points;
    workers->point_counts = point_counts;
    workers->compact_outputs = nullptr;
    workers->device_outputs = nullptr;
    workers->ip_results = nullptr;
    workers->stats = stats;
    workers->first_error.clear();
    workers->next_index.store(0);
    workers->failed.store(0);
    workers->pending = static_cast<uint32_t>(workers->threads.size());
    workers->generation++;
    workers->batch_active = true;

    workers->cv.notify_all();
    workers->done_cv.wait(lock, [workers] { return !workers->batch_active; });

    if (workers->failed.load()) {
      SetError(error_buffer, error_buffer_size,
               workers->first_error.empty() ? "unknown batch failure"
                                            : workers->first_error.c_str());
      return 1;
    }
  }

  if ((error_buffer != nullptr) && (error_buffer_size > 0))
    error_buffer[0] = '\0';
  return 0;
}

extern "C" int Type3BoundsCudaEnumerateBatchCompact(
    Type3BoundsCudaContext *context,
    const Type3BoundsCudaProblem *problems,
    const Type3BoundsCudaReduction *reductions,
    uint32_t problem_count,
    uint32_t point_capacity,
    Type3BoundsCudaPointBuffer *outputs,
    Type3BoundsCudaStats *stats,
    char *error_buffer,
    size_t error_buffer_size) {
  Type3BoundsCudaBatchWorkers *workers;

  if ((context == nullptr) || (problems == nullptr) || (outputs == nullptr)) {
    SetError(error_buffer, error_buffer_size,
             "invalid CUDA compact enumerate-batch args");
    return 1;
  }
  if (problem_count == 0) {
    if ((error_buffer != nullptr) && (error_buffer_size > 0))
      error_buffer[0] = '\0';
    return 0;
  }
  if ((context->lanes == nullptr) || (context->lane_count == 0)) {
    SetError(error_buffer, error_buffer_size,
             "CUDA enumeration lane state not initialized");
    return 1;
  }
  workers = context->batch_workers;
  if ((workers == nullptr) || workers->threads.empty()) {
    SetError(error_buffer, error_buffer_size,
             "CUDA batch worker state not initialized");
    return 1;
  }

  if (stats != nullptr)
    std::memset(stats, 0,
                static_cast<size_t>(problem_count) *
                    sizeof(Type3BoundsCudaStats));
  for (uint32_t index = 0; index < problem_count; index++) {
    outputs[index].points = nullptr;
    outputs[index].point_count = 0;
  }

  {
    std::unique_lock<std::mutex> lock(workers->mutex);

    while (workers->batch_active)
      workers->done_cv.wait(lock);

    workers->problems = problems;
    workers->candidates = nullptr;
    workers->problem_count = problem_count;
    workers->reductions = reductions;
    workers->point_capacity = point_capacity;
    workers->points = nullptr;
    workers->point_counts = nullptr;
    workers->compact_outputs = outputs;
    workers->device_outputs = nullptr;
    workers->ip_results = nullptr;
    workers->stats = stats;
    workers->first_error.clear();
    workers->next_index.store(0);
    workers->failed.store(0);
    workers->pending = static_cast<uint32_t>(workers->threads.size());
    workers->generation++;
    workers->batch_active = true;

    workers->cv.notify_all();
    workers->done_cv.wait(lock, [workers] { return !workers->batch_active; });

    if (workers->failed.load()) {
      FreeCompactPointBuffers(outputs, problem_count);
      SetError(error_buffer, error_buffer_size,
               workers->first_error.empty() ? "unknown batch failure"
                                            : workers->first_error.c_str());
      return 1;
    }
  }

  if ((error_buffer != nullptr) && (error_buffer_size > 0))
    error_buffer[0] = '\0';
  return 0;
}

extern "C" int Type3BoundsCudaEnumerateCwsBatch(Type3BoundsCudaContext *context,
                                                 const Type3BoundsCudaCwsCandidate *candidates,
                                                 const Type3BoundsCudaReduction *reductions,
                                                 uint32_t candidate_count,
                                                 uint32_t point_capacity,
                                                 int64_t **points,
                                                 uint32_t *point_counts,
                                                 Type3BoundsCudaStats *stats,
                                                 char *error_buffer,
                                                 size_t error_buffer_size) {
  std::vector<Type3BoundsCudaProblem> prepared_problems;

  if ((context == nullptr) || (candidates == nullptr) || (points == nullptr) ||
      (point_counts == nullptr)) {
    SetError(error_buffer, error_buffer_size,
             "invalid CUDA enumerate-batch args");
    return 1;
  }
  if (candidate_count == 0) {
    if ((error_buffer != nullptr) && (error_buffer_size > 0))
      error_buffer[0] = '\0';
    return 0;
  }
  if ((context->lanes == nullptr) || (context->lane_count == 0)) {
    SetError(error_buffer, error_buffer_size,
             "CUDA enumeration lane state not initialized");
    return 1;
  }
  try {
    prepared_problems.resize(candidate_count);
  } catch (const std::exception &error) {
    SetError(error_buffer, error_buffer_size, error.what());
    return 1;
  } catch (...) {
    SetError(error_buffer, error_buffer_size,
             "unable to allocate prepared problem batch");
    return 1;
  }

  if (PrepareProblemBatchFromCandidates(
          context, candidates, candidate_count, point_capacity,
          prepared_problems.data(), error_buffer, error_buffer_size) != 0)
    return 1;

  return Type3BoundsCudaEnumerateBatch(
      context, prepared_problems.data(), reductions, candidate_count,
      point_capacity, points, point_counts, stats, error_buffer,
      error_buffer_size);
}

extern "C" int Type3BoundsCudaEnumerateCwsBatchCompact(
    Type3BoundsCudaContext *context,
    const Type3BoundsCudaCwsCandidate *candidates,
    const Type3BoundsCudaReduction *reductions,
    uint32_t candidate_count,
    uint32_t point_capacity,
    Type3BoundsCudaPointBuffer *outputs,
    Type3BoundsCudaStats *stats,
    char *error_buffer,
    size_t error_buffer_size) {
  std::vector<Type3BoundsCudaProblem> prepared_problems;

  if ((context == nullptr) || (candidates == nullptr) || (outputs == nullptr)) {
    SetError(error_buffer, error_buffer_size,
             "invalid CUDA compact enumerate-batch args");
    return 1;
  }
  if (candidate_count == 0) {
    if ((error_buffer != nullptr) && (error_buffer_size > 0))
      error_buffer[0] = '\0';
    return 0;
  }
  if ((context->lanes == nullptr) || (context->lane_count == 0)) {
    SetError(error_buffer, error_buffer_size,
             "CUDA enumeration lane state not initialized");
    return 1;
  }
  try {
    prepared_problems.resize(candidate_count);
  } catch (const std::exception &error) {
    SetError(error_buffer, error_buffer_size, error.what());
    return 1;
  } catch (...) {
    SetError(error_buffer, error_buffer_size,
             "unable to allocate prepared problem batch");
    return 1;
  }

  if (PrepareProblemBatchFromCandidates(
          context, candidates, candidate_count, point_capacity,
          prepared_problems.data(), error_buffer, error_buffer_size) != 0)
    return 1;

  return Type3BoundsCudaEnumerateBatchCompact(
      context, prepared_problems.data(), reductions, candidate_count,
      point_capacity, outputs, stats, error_buffer, error_buffer_size);
}

extern "C" int Type3BoundsCudaEnumerateCwsBatchDeviceCompact(
    Type3BoundsCudaContext *context,
    const Type3BoundsCudaCwsCandidate *candidates,
    const Type3BoundsCudaReduction *reductions,
    uint32_t candidate_count,
    uint32_t point_capacity,
    Type3BoundsCudaDevicePointBuffer *outputs,
    Type3BoundsCudaStats *stats,
    char *error_buffer,
    size_t error_buffer_size) {
  std::vector<Type3BoundsCudaProblem> prepared_problems;
  Type3BoundsCudaBatchWorkers *workers;

  if ((context == nullptr) || (candidates == nullptr) || (outputs == nullptr)) {
    SetError(error_buffer, error_buffer_size,
             "invalid CUDA device enumerate-batch args");
    return 1;
  }
  if (candidate_count == 0) {
    if ((error_buffer != nullptr) && (error_buffer_size > 0))
      error_buffer[0] = '\0';
    return 0;
  }
  if ((context->lanes == nullptr) || (context->lane_count == 0)) {
    SetError(error_buffer, error_buffer_size,
             "CUDA enumeration lane state not initialized");
    return 1;
  }
  try {
    prepared_problems.resize(candidate_count);
  } catch (const std::exception &error) {
    SetError(error_buffer, error_buffer_size, error.what());
    return 1;
  } catch (...) {
    SetError(error_buffer, error_buffer_size,
             "unable to allocate prepared problem batch");
    return 1;
  }
  if (PrepareProblemBatchFromCandidates(
          context, candidates, candidate_count, point_capacity,
          prepared_problems.data(), error_buffer, error_buffer_size) != 0)
    return 1;

  workers = context->batch_workers;
  if ((workers == nullptr) || workers->threads.empty()) {
    SetError(error_buffer, error_buffer_size,
             "CUDA batch worker state not initialized");
    return 1;
  }

  if (stats != nullptr)
    std::memset(stats, 0,
                static_cast<size_t>(candidate_count) *
                    sizeof(Type3BoundsCudaStats));
  for (uint32_t index = 0; index < candidate_count; index++) {
    outputs[index].points = nullptr;
    outputs[index].point_count = 0;
    outputs[index].point_dimension = 0;
    outputs[index].point_stride = 0;
  }

  {
    std::unique_lock<std::mutex> lock(workers->mutex);

    while (workers->batch_active)
      workers->done_cv.wait(lock);

    workers->problems = prepared_problems.data();
    workers->candidates = nullptr;
    workers->problem_count = candidate_count;
    workers->reductions = reductions;
    workers->point_capacity = point_capacity;
    workers->points = nullptr;
    workers->point_counts = nullptr;
    workers->compact_outputs = nullptr;
    workers->device_outputs = outputs;
    workers->ip_results = nullptr;
    workers->stats = stats;
    workers->first_error.clear();
    workers->next_index.store(0);
    workers->failed.store(0);
    workers->pending = static_cast<uint32_t>(workers->threads.size());
    workers->generation++;
    workers->batch_active = true;

    workers->cv.notify_all();
    workers->done_cv.wait(lock, [workers] { return !workers->batch_active; });

    if (workers->failed.load()) {
      for (uint32_t index = 0; index < candidate_count; index++) {
        outputs[index].points = nullptr;
        outputs[index].point_count = 0;
        outputs[index].point_dimension = 0;
        outputs[index].point_stride = 0;
      }
      SetError(error_buffer, error_buffer_size,
               workers->first_error.empty() ? "unknown batch failure"
                                            : workers->first_error.c_str());
      return 1;
    }
  }

  if ((error_buffer != nullptr) && (error_buffer_size > 0))
    error_buffer[0] = '\0';
  return 0;
}

extern "C" int Type3BoundsCudaEnumerateCwsBatchIPResident(
    Type3BoundsCudaContext *context,
    const Type3BoundsCudaCwsCandidate *candidates,
    const Type3BoundsCudaReduction *reductions,
    uint32_t candidate_count,
    uint32_t point_capacity,
    int *results,
    Type3BoundsCudaStats *stats,
    char *error_buffer,
    size_t error_buffer_size) {
  std::vector<Type3BoundsCudaProblem> prepared_problems;
  Type3BoundsCudaBatchWorkers *workers;

  if ((context == nullptr) || (candidates == nullptr) || (results == nullptr)) {
    SetError(error_buffer, error_buffer_size,
             "invalid resident CUDA enumerate-batch args");
    return 1;
  }
  if (candidate_count == 0) {
    if ((error_buffer != nullptr) && (error_buffer_size > 0))
      error_buffer[0] = '\0';
    return 0;
  }
  if ((context->lanes == nullptr) || (context->lane_count == 0)) {
    SetError(error_buffer, error_buffer_size,
             "CUDA enumeration lane state not initialized");
    return 1;
  }
  try {
    prepared_problems.resize(candidate_count);
  } catch (const std::exception &error) {
    SetError(error_buffer, error_buffer_size, error.what());
    return 1;
  } catch (...) {
    SetError(error_buffer, error_buffer_size,
             "unable to allocate prepared problem batch");
    return 1;
  }
  if (PrepareProblemBatchFromCandidates(
          context, candidates, candidate_count, point_capacity,
          prepared_problems.data(), error_buffer, error_buffer_size) != 0)
    return 1;

  workers = context->batch_workers;
  if ((workers == nullptr) || workers->threads.empty()) {
    SetError(error_buffer, error_buffer_size,
             "CUDA batch worker state not initialized");
    return 1;
  }

  if (stats != nullptr)
    std::memset(stats, 0,
                static_cast<size_t>(candidate_count) *
                    sizeof(Type3BoundsCudaStats));
  std::memset(results, 0, static_cast<size_t>(candidate_count) * sizeof(int));

  {
    std::unique_lock<std::mutex> lock(workers->mutex);

    while (workers->batch_active)
      workers->done_cv.wait(lock);

    workers->problems = prepared_problems.data();
    workers->candidates = nullptr;
    workers->problem_count = candidate_count;
    workers->reductions = reductions;
    workers->point_capacity = point_capacity;
    workers->points = nullptr;
    workers->point_counts = nullptr;
    workers->compact_outputs = nullptr;
    workers->device_outputs = nullptr;
    workers->ip_results = results;
    workers->stats = stats;
    workers->first_error.clear();
    workers->next_index.store(0);
    workers->failed.store(0);
    workers->pending = static_cast<uint32_t>(workers->threads.size());
    workers->generation++;
    workers->batch_active = true;

    workers->cv.notify_all();
    workers->done_cv.wait(lock, [workers] { return !workers->batch_active; });

    if (workers->failed.load()) {
      SetError(error_buffer, error_buffer_size,
               workers->first_error.empty() ? "unknown batch failure"
                                            : workers->first_error.c_str());
      return 1;
    }
  }

  if ((error_buffer != nullptr) && (error_buffer_size > 0))
    error_buffer[0] = '\0';
  return 0;
}

extern "C" void Type3BoundsCudaFreeHostBuffer(void *buffer) {
  std::free(buffer);
}

extern "C" void Type3BoundsCudaFreeDeviceBuffer(void *buffer) {
  if (buffer != nullptr)
    cudaFree(buffer);
}

extern "C" int Type3BoundsCudaUploadDim5WeightPool(
    Type3BoundsCudaContext *context,
    const Type3BoundsCudaWeightEntry *weights,
    uint32_t weight_count,
    char *error_buffer,
    size_t error_buffer_size) {
  cudaError_t status;

  if ((context == nullptr) || (weights == nullptr) || (weight_count == 0)) {
    SetError(error_buffer, error_buffer_size,
             "invalid dim-5 weight-pool upload args");
    return 1;
  }

  status = cudaSetDevice(context->device_ordinal);
  if (status != cudaSuccess) {
    SetCudaError(error_buffer, error_buffer_size,
                 "unable to select CUDA device", status);
    return 1;
  }
  status = EnsureDim5WeightPoolCapacity(context, weight_count);
  if (status != cudaSuccess) {
    SetCudaError(error_buffer, error_buffer_size,
                 "unable to reserve dim-5 weight-pool buffers", status);
    return 1;
  }
  status = cudaMemcpy(context->device_dim5_weights, weights,
                      static_cast<size_t>(weight_count) *
                          sizeof(Type3BoundsCudaWeightEntry),
                      cudaMemcpyHostToDevice);
  if (status != cudaSuccess) {
    SetCudaError(error_buffer, error_buffer_size,
                 "unable to upload dim-5 weight pool", status);
    return 1;
  }

  context->dim5_weight_count = weight_count;
  if ((error_buffer != nullptr) && (error_buffer_size > 0))
    error_buffer[0] = '\0';
  return 0;
}

extern "C" int Type3BoundsCudaGenerateDim5Structure3Batch(
    Type3BoundsCudaContext *context,
    uint64_t pair_start,
    uint32_t pair_count,
    Type3BoundsCudaDim5Structure3Candidate *candidates,
    uint32_t *candidate_counts,
    char *error_buffer,
    size_t error_buffer_size) {
  cudaError_t status;
  uint64_t total_pairs;
  uint32_t blocks;

  if ((context == nullptr) || (candidates == nullptr) ||
      (candidate_counts == nullptr)) {
    SetError(error_buffer, error_buffer_size,
             "invalid structure-3 generation args");
    return 1;
  }
  if (pair_count == 0) {
    if ((error_buffer != nullptr) && (error_buffer_size > 0))
      error_buffer[0] = '\0';
    return 0;
  }
  if (context->dim5_weight_count == 0) {
    SetError(error_buffer, error_buffer_size,
             "no dim-5 weight pool uploaded");
    return 1;
  }

  total_pairs = (static_cast<uint64_t>(context->dim5_weight_count) *
                 (context->dim5_weight_count + 1)) /
                2;
  if ((pair_start >= total_pairs) || (pair_start + pair_count > total_pairs)) {
    SetError(error_buffer, error_buffer_size,
             "structure-3 pair batch exceeds uploaded weight pool");
    return 1;
  }

  status = cudaSetDevice(context->device_ordinal);
  if (status != cudaSuccess) {
    SetCudaError(error_buffer, error_buffer_size,
                 "unable to select CUDA device", status);
    return 1;
  }
  status = EnsureDim5Structure3BatchCapacity(context, pair_count);
  if (status != cudaSuccess) {
    SetCudaError(error_buffer, error_buffer_size,
                 "unable to reserve structure-3 generation buffers", status);
    return 1;
  }

  blocks = (pair_count + context->block_size - 1) / context->block_size;
  GenerateDim5Structure3CandidatesKernel<<<blocks, context->block_size>>>(
      context->device_dim5_weights, context->dim5_weight_count, pair_start,
      pair_count, context->device_dim5_structure3_candidates,
      context->device_dim5_structure3_candidate_counts);
  status = cudaGetLastError();
  if (status != cudaSuccess) {
    SetCudaError(error_buffer, error_buffer_size,
                 "structure-3 generation kernel launch failed", status);
    return 1;
  }
  status = cudaDeviceSynchronize();
  if (status != cudaSuccess) {
    SetCudaError(error_buffer, error_buffer_size,
                 "structure-3 generation kernel failed", status);
    return 1;
  }

  status = cudaMemcpy(candidate_counts,
                      context->device_dim5_structure3_candidate_counts,
                      static_cast<size_t>(pair_count) * sizeof(uint32_t),
                      cudaMemcpyDeviceToHost);
  if (status != cudaSuccess) {
    SetCudaError(error_buffer, error_buffer_size,
                 "unable to download structure-3 candidate counts", status);
    return 1;
  }
  status = cudaMemcpy(candidates, context->device_dim5_structure3_candidates,
                      static_cast<size_t>(pair_count) *
                          kType3BoundsRuntimeMaxStructure3PairOutputs *
                          sizeof(Type3BoundsCudaDim5Structure3Candidate),
                      cudaMemcpyDeviceToHost);
  if (status != cudaSuccess) {
    SetCudaError(error_buffer, error_buffer_size,
                 "unable to download structure-3 candidates", status);
    return 1;
  }

  if ((error_buffer != nullptr) && (error_buffer_size > 0))
    error_buffer[0] = '\0';
  return 0;
}

extern "C" void Type3BoundsCudaDestroy(Type3BoundsCudaContext *context) {
  if (context == nullptr)
    return;
  DestroyContext(context);
}