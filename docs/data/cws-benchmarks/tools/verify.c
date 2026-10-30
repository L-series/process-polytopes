/* Recount complete primal point sets and check IP using the pinned PALP code.
   Link with PALP (GPLv3); this is an implementation cross-check, not a proof. */
#include "Global.h"
#include <stdio.h>
#include <stdlib.h>

FILE *inFILE, *outFILE;

int main(void) {
  CWS *cws = calloc(1, sizeof(*cws));
  PolyPointList *points = malloc(sizeof(*points));
  VertexNumList *vertices = malloc(sizeof(*vertices));
  EqList *equations = malloc(sizeof(*equations));
  size_t row = 0;
  if (!cws || !points || !vertices || !equations) {
    fputs("allocation failure\n", stderr);
    return 1;
  }
  inFILE = NULL; /* PALP filter mode suppresses interactive prompts. */
  outFILE = stdout;
  while (Read_CWS_PP(cws, points)) {
    int ip, reflexive = 0;
    if (cws->nw < 2 || points->n != 5) {
      fputs("expected a five-dimensional combined weight system\n", stderr);
      return 1;
    }
    ip = IP_Check(points, vertices, equations);
    if (ip) {
      reflexive = 1;
      for (int i = 0; i < equations->ne; i++)
        if (equations->e[i].c != 1)
          reflexive = 0;
    }
    printf("%zu %d %d %d %d %d\n", ++row, points->n, points->np, ip, ip ? vertices->nv : 0,
           reflexive);
  }
  free(equations);
  free(vertices);
  free(points);
  free(cws);
  if (ferror(stdin) || fflush(stdout) || ferror(stdout))
    return 1;
  return 0;
}
