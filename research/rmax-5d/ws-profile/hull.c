#include "Global.h"
#include <stdlib.h>
#include <string.h>

FILE *inFILE, *outFILE;

int ws_hull(Long *input, int count, Long *vertices, int *nv, Long *facets, int *nf) {
  static PolyPointList *p;
  static VertexNumList *v;
  static EqList *e;
  if (!p) {
    p = malloc(sizeof(*p));
    v = malloc(sizeof(*v));
    e = malloc(sizeof(*e));
    if (!p || !v || !e)
      return -1;
    inFILE = stdin;
    outFILE = stdout;
  }
  if (count < 6)
    return 0;
  if (count > POINT_Nmax)
    return -2;
  p->n = 5;
  p->np = count;
  memcpy(p->x, input, (size_t)count * sizeof(p->x[0]));
  if (!IP_Check(p, v, e))
    return 0;
  *nv = v->nv;
  *nf = e->ne;
  for (int i = 0; i < v->nv; i++)
    memcpy(vertices + i * 5, p->x[v->v[i]], 5 * sizeof(Long));
  for (int i = 0; i < e->ne; i++) {
    memcpy(facets + i * 6, e->e[i].a, 5 * sizeof(Long));
    facets[i * 6 + 5] = e->e[i].c;
  }
  return 1;
}
