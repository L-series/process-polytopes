#include "Global.h"
#include "catalogue.h"
#include "catalogue_bridge.h"
#include <string.h>

int GpuCatalogueDescriptor(int id, GpuDescriptor *out) {
  CWSDescriptor d;
  int slot, position;
  if (!out || !ProjectCWSDescriptor(id, &d))
    return 0;
  if (d.row_count > GPU_MAX_ROWS || d.coordinate_count > GPU_MAX_COORDINATES)
    return 0;
  memset(out, 0, sizeof(*out));
  out->id = id;
  out->coordinate_count = d.coordinate_count;
  out->row_count = d.row_count;
  for (slot = 0; slot < d.row_count; slot++) {
    if (d.row_sizes[slot] > GPU_MAX_ROW_SIZE)
      return 0;
    out->row_sizes[slot] = d.row_sizes[slot];
    out->shared_counts[slot] = d.shared_counts[slot];
    for (position = 0; position < d.row_sizes[slot]; position++)
      out->mappings[slot][position] = d.mappings[slot][position];
  }
  return 1;
}
