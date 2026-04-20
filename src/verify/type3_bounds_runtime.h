#ifndef TYPE3_BOUNDS_RUNTIME_H
#define TYPE3_BOUNDS_RUNTIME_H

#include <stddef.h>
#include <stdint.h>

#include "type3_bounds_bench_common.h"

#ifdef __cplusplus
extern "C" {
#endif

typedef struct Type3BoundsCudaContext Type3BoundsCudaContext;

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
void Type3BoundsCudaDestroy(Type3BoundsCudaContext *context);

#ifdef __cplusplus
}
#endif

#endif