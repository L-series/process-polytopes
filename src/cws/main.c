#include "Global.h"
#include "catalogue.h"
#include "cws_generate.h"
#include "weight_pool.h"
#include <errno.h>
#include <string.h>

extern void PRINT_CWS(CWS *);

static int EmitGeometry(CWS *candidate, void *user_data) {
  (void)user_data;
  PRINT_CWS(candidate);
  return 0;
}

static void ProjectGenerateCWS(int narg, char *fn[]) {
  CWSDescriptor descriptor;
  FILE *row_pools[CWS_MAX_ROWS] = {0};
  unsigned long long candidates = 0;
  FILE *pools[6] = {0};
  const char *directory = "docs/data/single-weight-systems";
  long id = 0;
  int i, j, slot;
  if (POLY_Dmax < 5)
    ProjectCWSError("requires a build with POLY_Dmax >= 5");
  for (i = 2; i < narg; i++) {
    if (!strncmp(fn[i], "-s", 2)) {
      char *end;
      const char *value = fn[i] + 2;
      if (id)
        ProjectCWSError("duplicate structure option");
      if (!*value && ++i < narg)
        value = fn[i];
      errno = 0;
      id = strtol(value, &end, 10);
      if (errno || end == value || *end || id < 2 || id > 47)
        ProjectCWSError("structure ID must be an integer from 2 to 47");
    } else if (!strcmp(fn[i], "--weights-dir") && i + 1 < narg)
      directory = fn[++i];
    else
      ProjectCWSError("usage: cws.x -n5 -sID [--weights-dir DIRECTORY]");
  }
  if (!ProjectCWSDescriptor((int)id, &descriptor))
    ProjectCWSError("select one structure with -sID (2..47)");
  /* Validate and load every project input before invoking the reusable engine.
   */
  for (slot = 0; slot < descriptor.row_count; slot++) {
    int size = descriptor.row_sizes[slot];
    if (!pools[size])
      pools[size] = ProjectReadWeightPool(directory, size);
    row_pools[slot] = pools[size];
  }
  if (CWSGenerate(&descriptor, row_pools, EmitGeometry, NULL, &candidates) != CWS_GENERATE_OK)
    ProjectCWSError("descriptor enumeration failed");
  for (j = 2; j <= 5; j++)
    if (pools[j])
      fclose(pools[j]);
  if (fflush(outFILE) || ferror(outFILE))
    ProjectCWSError("cannot write CWS output");
  fprintf(stderr, "CWS candidates: %llu\n", candidates);
}

int main(int argc, char **argv) {
  if (argc > 1 && !strcmp(argv[1], "-n5")) {
    inFILE = stdin;
    outFILE = stdout;
    ProjectGenerateCWS(argc, argv);
    return 0;
  }
  if (argc > 1 && !strcmp(argv[1], "-h")) {
    puts("Project extension: -n5 -sID [--weights-dir DIRECTORY] (historical "
         "IDs 2..47)");
    fflush(stdout);
  }
  return CWSMain(argc, argv);
}
