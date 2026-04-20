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
};

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
                             uint32_t point_capacity,
                             int64_t *points,
                             uint32_t *point_count,
                             Type3BoundsCudaStats *stats,
                             char *error_buffer,
                             size_t error_buffer_size);
void Type3BoundsCudaDestroy(Type3BoundsCudaContext *context);

#ifdef __cplusplus
}
#endif

#endif