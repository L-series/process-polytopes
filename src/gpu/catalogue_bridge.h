/* Plain-C view of the project catalogue (src/cws/catalogue.c) for the CUDA
   front end, which does not include PALP's Global.h. */
#ifndef PROJECT_GPU_CATALOGUE_BRIDGE_H
#define PROJECT_GPU_CATALOGUE_BRIDGE_H
#ifdef __cplusplus
extern "C" {
#endif

enum { GPU_MAX_ROWS = 5, GPU_MAX_ROW_SIZE = 5, GPU_MAX_COORDINATES = 10 };

typedef struct {
  int id, coordinate_count, row_count;
  int row_sizes[GPU_MAX_ROWS], shared_counts[GPU_MAX_ROWS];
  /* One-based coordinates; shared positions precede private positions. */
  int mappings[GPU_MAX_ROWS][GPU_MAX_ROW_SIZE];
} GpuDescriptor;

/* Copies historical descriptor `id` (2..47); returns 0 if it does not exist or
   does not fit the GPU limits. */
int GpuCatalogueDescriptor(int id, GpuDescriptor *out);

#ifdef __cplusplus
}
#endif
#endif
