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
    kType3BoundsRuntimeMaxIPVertices = 64,
    kType3BoundsRuntimeMaxIPEquations = 64,
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

typedef struct {
    int64_t a[kType3BoundsRuntimeMaxDimension];
    int64_t c;
} Type3BoundsCudaEquation;

typedef struct {
    uint32_t point_offset;
    uint32_t point_count;
    uint32_t point_dimension;
} Type3BoundsCudaEquationTask;

typedef struct {
    int64_t *points;
    uint32_t point_count;
} Type3BoundsCudaPointBuffer;

typedef struct {
    int64_t *points;
    uint32_t point_count;
    uint32_t point_dimension;
    uint32_t point_stride;
} Type3BoundsCudaDevicePointBuffer;

typedef struct {
    uint32_t point_offset;
    uint32_t point_count;
    uint32_t point_dimension;
    uint32_t vertex_count;
    uint32_t facet_count;
    uint32_t ceq_count;
    uint32_t vertices[kType3BoundsRuntimeMaxIPVertices];
    uint64_t facet_incidences[kType3BoundsRuntimeMaxIPEquations];
    uint64_t ceq_incidences[kType3BoundsRuntimeMaxIPEquations];
    Type3BoundsCudaEquation facets[kType3BoundsRuntimeMaxIPEquations];
    Type3BoundsCudaEquation ceqs[kType3BoundsRuntimeMaxIPEquations];
} Type3BoundsCudaIPState;

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
int Type3BoundsCudaEnumerateBatchCompact(
    Type3BoundsCudaContext *context,
    const Type3BoundsCudaProblem *problems,
    const Type3BoundsCudaReduction *reductions,
    uint32_t problem_count,
    uint32_t point_capacity,
    Type3BoundsCudaPointBuffer *outputs,
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
int Type3BoundsCudaEnumerateCwsBatchCompact(
    Type3BoundsCudaContext *context,
    const Type3BoundsCudaCwsCandidate *candidates,
    const Type3BoundsCudaReduction *reductions,
    uint32_t candidate_count,
    uint32_t point_capacity,
    Type3BoundsCudaPointBuffer *outputs,
    Type3BoundsCudaStats *stats,
    char *error_buffer,
    size_t error_buffer_size);
int Type3BoundsCudaEnumerateCwsBatchDeviceCompact(
    Type3BoundsCudaContext *context,
    const Type3BoundsCudaCwsCandidate *candidates,
    const Type3BoundsCudaReduction *reductions,
    uint32_t candidate_count,
    uint32_t point_capacity,
    Type3BoundsCudaDevicePointBuffer *outputs,
    Type3BoundsCudaStats *stats,
    char *error_buffer,
    size_t error_buffer_size);
int Type3BoundsCudaEnumerateCwsBatchIPResident(
    Type3BoundsCudaContext *context,
    const Type3BoundsCudaCwsCandidate *candidates,
    const Type3BoundsCudaReduction *reductions,
    uint32_t candidate_count,
    uint32_t point_capacity,
    int *results,
    Type3BoundsCudaStats *stats,
    char *error_buffer,
    size_t error_buffer_size);
int Type3BoundsCudaClassifyEquationBatch(Type3BoundsCudaContext *context,
                                         const Type3BoundsCudaEquation *equations,
                                         uint32_t equation_count,
                                         const int64_t *points,
                                         uint32_t point_count,
                                         uint32_t point_dimension,
                                         uint32_t point_stride,
                                         uint8_t *has_negative,
                                         char *error_buffer,
                                         size_t error_buffer_size);
int Type3BoundsCudaClassifyEquationTaskBatch(
    Type3BoundsCudaContext *context,
    const Type3BoundsCudaEquation *equations,
    const Type3BoundsCudaEquationTask *tasks,
    uint32_t equation_count,
    const int64_t *points,
    uint32_t point_count,
    uint32_t point_stride,
    uint8_t *has_negative,
    char *error_buffer,
    size_t error_buffer_size);
int Type3BoundsCudaRunIPCheckBatch(
    Type3BoundsCudaContext *context,
    const Type3BoundsCudaIPState *states,
    uint32_t state_count,
    const Type3BoundsCudaPointBuffer *point_buffers,
    uint32_t point_stride,
    int *results,
    char *error_buffer,
    size_t error_buffer_size);
int Type3BoundsCudaRunIPCheckDeviceBatch(
    Type3BoundsCudaContext *context,
    const Type3BoundsCudaDevicePointBuffer *point_buffers,
    uint32_t state_count,
    int *results,
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
void Type3BoundsCudaFreeHostBuffer(void *buffer);
void Type3BoundsCudaFreeDeviceBuffer(void *buffer);
void Type3BoundsCudaDestroy(Type3BoundsCudaContext *context);

#ifdef __cplusplus
}
#endif

#endif