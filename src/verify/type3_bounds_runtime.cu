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
        stats(nullptr),
        batch_active(false),
        stop(false),
        pending(0),
        generation(0) {}
};

struct Type3BoundsCudaContext {
  Type3BoundsJob *device_jobs;
  Type3BoundsResult *device_results;
  Type3BoundsCudaCwsCandidate *device_candidate_batch;
  Type3BoundsCudaProblem *device_problem_batch;
  int *device_prepare_status_batch;
  Type3BoundsCudaWeightEntry *device_dim5_weights;
  Type3BoundsCudaDim5Structure3Candidate *device_dim5_structure3_candidates;
  uint32_t *device_dim5_structure3_candidate_counts;
  uint32_t candidate_batch_capacity;
  uint32_t dim5_weight_capacity;
  uint32_t dim5_weight_count;
  uint32_t dim5_structure3_pair_capacity;
  uint32_t capacity;
  uint32_t block_size;
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
                    uint32_t *point_count, Type3BoundsCudaStats *stats,
                    char *error_buffer, size_t error_buffer_size);

int EnumerateCwsOnLane(Type3BoundsCudaContext *context,
                       Type3BoundsCudaEnumerationLane *lane,
                       const Type3BoundsCudaCwsCandidate *candidate,
                       const Type3BoundsCudaReduction *reduction,
                       uint32_t point_capacity, int64_t *points,
                       uint32_t *point_count, Type3BoundsCudaStats *stats,
                       char *error_buffer, size_t error_buffer_size);

int EnumerateSynchronously(Type3BoundsCudaContext *context,
                           Type3BoundsCudaEnumerationLane *lane,
                           const Type3BoundsCudaProblem *problem,
            const Type3BoundsCudaReduction *reduction,
                           uint32_t point_capacity, int64_t *points,
                           uint32_t *point_count, Type3BoundsCudaStats *stats,
                           char *error_buffer, size_t error_buffer_size);

int EnumerateCwsSynchronously(Type3BoundsCudaContext *context,
                              Type3BoundsCudaEnumerationLane *lane,
                              const Type3BoundsCudaCwsCandidate *candidate,
                              const Type3BoundsCudaReduction *reduction,
                              uint32_t point_capacity, int64_t *points,
                              uint32_t *point_count,
                              Type3BoundsCudaStats *stats,
                              char *error_buffer,
                              size_t error_buffer_size);

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
  DestroyBatchWorkers(context);
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

          if (index >= workers->problem_count)
            break;
          if (workers->points[index] == nullptr) {
            RecordBatchFailure(workers, "null host point buffer");
            break;
          }
            if (((workers->candidates != nullptr) &&
               (EnumerateCwsOnLane(context, lane, &workers->candidates[index],
                         (workers->reductions != nullptr)
                           ? &workers->reductions[index]
                           : nullptr,
                         workers->point_capacity,
                         workers->points[index],
                         &workers->point_counts[index],
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
                      workers->points[index],
                      &workers->point_counts[index],
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
                            Type3BoundsCudaStats *stats,
                            char *error_buffer,
                            size_t error_buffer_size) {
  cudaError_t status;
  uint32_t blocks;
  int overflow_flag = 0;
  Type3BoundsCudaStats local_stats = {0};
  int64_t (*current)[kType3BoundsRuntimeMaxDimension] = lane->device_frontier_a;
  int64_t (*next)[kType3BoundsRuntimeMaxDimension] = lane->device_frontier_b;
  const int64_t (*download_points)[kType3BoundsRuntimeMaxDimension] =
      lane->device_points;

  if ((problem == nullptr) || (points == nullptr) || (point_count == nullptr)) {
    SetError(error_buffer, error_buffer_size, "invalid CUDA enumerate args");
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
        (coord == 0) ? lane->device_points : next, point_capacity,
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
        lane->device_reduction, problem->n, lane->device_points, state_count,
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
  if (state_count > 0) {
    size_t download_bytes = static_cast<size_t>(state_count) *
                            kType3BoundsRuntimeMaxDimension * sizeof(int64_t);

    status = cudaMemcpy(lane->host_point_staging, download_points,
                        download_bytes, cudaMemcpyDeviceToHost);
    if (status != cudaSuccess) {
      SetCudaError(error_buffer, error_buffer_size,
                   "unable to download enumerated points", status);
      return 1;
    }
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
                       uint32_t *point_count, Type3BoundsCudaStats *stats,
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
                                 point_count, stats, error_buffer,
                                 error_buffer_size);
}

int EnumerateCwsSynchronously(Type3BoundsCudaContext *context,
                              Type3BoundsCudaEnumerationLane *lane,
                              const Type3BoundsCudaCwsCandidate *candidate,
                              const Type3BoundsCudaReduction *reduction,
                              uint32_t point_capacity, int64_t *points,
                              uint32_t *point_count,
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
                                point_capacity, points, point_count, stats,
                                error_buffer, error_buffer_size);
}

int EnumerateOnLane(Type3BoundsCudaContext *context,
                    Type3BoundsCudaEnumerationLane *lane,
                    const Type3BoundsCudaProblem *problem,
                    const Type3BoundsCudaReduction *reduction,
                    uint32_t point_capacity, int64_t *points,
                    uint32_t *point_count, Type3BoundsCudaStats *stats,
                    char *error_buffer, size_t error_buffer_size) {
  cudaError_t status;
  uint32_t state_count;
  uint32_t blocks;
  Type3BoundsCudaStats local_stats = {0};

  if ((problem == nullptr) || (points == nullptr) || (point_count == nullptr)) {
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
                                 point_count, stats, error_buffer,
                                 error_buffer_size);
}

int EnumerateSynchronously(Type3BoundsCudaContext *context,
                           Type3BoundsCudaEnumerationLane *lane,
                           const Type3BoundsCudaProblem *problem,
                           const Type3BoundsCudaReduction *reduction,
                           uint32_t point_capacity, int64_t *points,
                           uint32_t *point_count, Type3BoundsCudaStats *stats,
                           char *error_buffer, size_t error_buffer_size) {
  cudaError_t status;
  uint32_t state_count;
  uint32_t blocks;
  int overflow_flag = 0;
  Type3BoundsCudaStats local_stats = {0};
  int64_t (*current)[kType3BoundsRuntimeMaxDimension];
  int64_t (*next)[kType3BoundsRuntimeMaxDimension];
  const int64_t (*download_points)[kType3BoundsRuntimeMaxDimension];

  if ((problem == nullptr) || (points == nullptr) || (point_count == nullptr)) {
    SetError(error_buffer, error_buffer_size, "invalid CUDA enumerate args");
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
  download_points = lane->device_points;

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
        (coord == 0) ? lane->device_points : next, point_capacity,
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
      lane->device_reduction, problem->n, lane->device_points, state_count,
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
  if (state_count > 0) {
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
                                point_count, stats, error_buffer,
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
                                   point_count, stats, error_buffer,
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