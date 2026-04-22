#ifndef TYPE3_BOUNDS_RUNTIME_H
#define TYPE3_BOUNDS_RUNTIME_H

#include <stddef.h>
#include <stdint.h>

#include "type3_bounds_bench_common.h"

#ifdef __cplusplus
extern "C" {
#endif

typedef struct Type3BoundsCudaContext Type3BoundsCudaContext;

enum {
    kType3BoundsRuntimeMaxDimension = 6,
    kType3BoundsRuntimeMaxAmbient = 32,
    kType3BoundsRuntimeMaxWeightSystems = 8,
    kType3BoundsRuntimeMaxSimplexWeights = 5,
    kType3BoundsRuntimeMaxStructure3PairOutputs = 6,
};

typedef struct {
    int64_t d;
    uint32_t count;
    int64_t w[kType3BoundsRuntimeMaxSimplexWeights];
} Type3BoundsCudaWeightEntry;

typedef struct {
    int64_t left_d;
    int64_t right_d;
    int64_t left_w[kType3BoundsRuntimeMaxSimplexWeights];
    int64_t right_w[kType3BoundsRuntimeMaxSimplexWeights];
} Type3BoundsCudaDim5Structure3Candidate;

typedef struct {
    uint32_t nw;
    uint32_t ambient_count;
    uint32_t nz;
    uint32_t index;
    int64_t W[kType3BoundsRuntimeMaxWeightSystems][kType3BoundsRuntimeMaxAmbient];
    int64_t d[kType3BoundsRuntimeMaxWeightSystems];
    int64_t z[kType3BoundsRuntimeMaxDimension][kType3BoundsRuntimeMaxAmbient];
    int64_t m[kType3BoundsRuntimeMaxDimension];
} Type3BoundsCudaCwsCandidate;

typedef struct {
    uint32_t n;
    uint32_t ambient_count;
    int32_t Amin[kType3BoundsRuntimeMaxDimension + 1];
    int64_t basis[kType3BoundsRuntimeMaxDimension][kType3BoundsRuntimeMaxAmbient];
    int64_t X0[kType3BoundsRuntimeMaxAmbient];
    int64_t Xmax[kType3BoundsRuntimeMaxAmbient];
    int64_t initial_xmin;
    int64_t initial_xmax;
} Type3BoundsCudaProblem;

typedef struct {
    uint32_t mod_count;
    int64_t mod[kType3BoundsRuntimeMaxDimension];
    int64_t matrix[kType3BoundsRuntimeMaxDimension][kType3BoundsRuntimeMaxDimension];
} Type3BoundsCudaReduction;

typedef struct {
    uint64_t seed_interval_empty_count;
    uint64_t tighten_interval_empty_count;
    uint64_t zero_range_fail_count;
    uint64_t seed_singleton_count;
    uint64_t tighten_singleton_count;
    uint64_t singleton_remaining_constraint_count;
    uint64_t zero_constraint_count;
    uint64_t nonzero_constraint_count;
    uint64_t skipped_constraint_count;
} Type3BoundsCudaStats;

int Type3BoundsCudaCreate(Type3BoundsCudaContext **context,
                          uint32_t capacity,
                          uint32_t block_size,
                          char *error_buffer,
                          size_t error_buffer_size);
int Type3BoundsCudaEvaluate(Type3BoundsCudaContext *context,
                            const Type3BoundsJob *jobs,
                            Type3BoundsResult *results,
                            uint32_t job_count,
                            char *error_buffer,
                            size_t error_buffer_size);
int Type3BoundsCudaEnumerate(Type3BoundsCudaContext *context,
                             const Type3BoundsCudaProblem *problem,
                        const Type3BoundsCudaReduction *reduction,
                             uint32_t point_capacity,
                             int64_t *points,
                             uint32_t *point_count,
                             Type3BoundsCudaStats *stats,
                             char *error_buffer,
                             size_t error_buffer_size);
int Type3BoundsCudaEnumerateBatch(Type3BoundsCudaContext *context,
                                  const Type3BoundsCudaProblem *problems,
                            const Type3BoundsCudaReduction *reductions,
                                  uint32_t problem_count,
                                  uint32_t point_capacity,
                                  int64_t **points,
                                  uint32_t *point_counts,
                                  Type3BoundsCudaStats *stats,
                                  char *error_buffer,
                                  size_t error_buffer_size);
int Type3BoundsCudaEnumerateCws(Type3BoundsCudaContext *context,
                                const Type3BoundsCudaCwsCandidate *candidate,
                                const Type3BoundsCudaReduction *reduction,
                                uint32_t point_capacity,
                                int64_t *points,
                                uint32_t *point_count,
                                Type3BoundsCudaStats *stats,
                                char *error_buffer,
                                size_t error_buffer_size);
int Type3BoundsCudaEnumerateCwsBatch(Type3BoundsCudaContext *context,
                                     const Type3BoundsCudaCwsCandidate *candidates,
                                     const Type3BoundsCudaReduction *reductions,
                                     uint32_t candidate_count,
                                     uint32_t point_capacity,
                                     int64_t **points,
                                     uint32_t *point_counts,
                                     Type3BoundsCudaStats *stats,
                                     char *error_buffer,
                                     size_t error_buffer_size);
int Type3BoundsCudaUploadDim5WeightPool(Type3BoundsCudaContext *context,
                                        const Type3BoundsCudaWeightEntry *weights,
                                        uint32_t weight_count,
                                        char *error_buffer,
                                        size_t error_buffer_size);
int Type3BoundsCudaGenerateDim5Structure3Batch(
    Type3BoundsCudaContext *context,
    uint64_t pair_start,
    uint32_t pair_count,
    Type3BoundsCudaDim5Structure3Candidate *candidates,
    uint32_t *candidate_counts,
    char *error_buffer,
    size_t error_buffer_size);
void Type3BoundsCudaDestroy(Type3BoundsCudaContext *context);

#ifdef __cplusplus
}
#endif

#endif