#define _POSIX_C_SOURCE 200809L
#include "Global.h"
#include <inttypes.h>
#include <stdint.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>

FILE *inFILE, *outFILE;
extern void Make_CWS_Points(CWS *, PolyPointList *);
extern int Make_Poly_NF(PolyPointList *, VertexNumList *, EqList *,
                        Long[POLY_Dmax][VERT_Nmax]);

static uint64_t now_ns(void) {
  struct timespec t;
  if (clock_gettime(CLOCK_MONOTONIC, &t))
    abort();
  return (uint64_t)t.tv_sec * 1000000000 + t.tv_nsec;
}

static void fail(const char *s) {
  fprintf(stderr, "%s\n", s);
  exit(1);
}

static void dump(const char *path, PolyPointList *p) {
  FILE *f = fopen(path, "wb");
  int64_t header[] = {p->np, p->n};
  if (!f || sizeof(Long) != 8 || p->n != POLY_Dmax ||
      fwrite(header, sizeof(int64_t), 2, f) != 2 ||
      fwrite(p->x, sizeof(p->x[0]), p->np, f) != (size_t)p->np ||
      fclose(f))
    fail("point dump failed");
}

int main(int argc, char **argv) {
  if (argc != 4)
    fail("usage: native POINT_DUMP DUAL_DUMP REPEATS");
  int np, nv, ne, reflexive, nd, repeats = atoi(argv[3]);
  long weights[6];
  if (repeats < 1 ||
      scanf("%d %d %d %d %d", &np, &nv, &ne, &reflexive, &nd) != 5)
    fail("invalid expectations");
  for (int i = 0; i < 6; i++)
    if (scanf("%ld", &weights[i]) != 1 || weights[i] < 1)
      fail("invalid weights");
  inFILE = stdin;
  outFILE = stdout;
  CWS *cw = calloc(1, sizeof(*cw));
  PolyPointList *p = malloc(sizeof(*p)), *vp = malloc(sizeof(*vp)),
                *dp = malloc(sizeof(*dp));
  VertexNumList *v = malloc(sizeof(*v)), *vv = malloc(sizeof(*vv));
  EqList *e = malloc(sizeof(*e)), *ve = malloc(sizeof(*ve));
  Long nf[POLY_Dmax][VERT_Nmax];
  if (!cw || !p || !vp || !dp || !v || !vv || !e || !ve)
    fail("allocation failed");
  printf("{\"runs\":[");
  for (int r = 0; r < repeats; r++) {
    cw->nw = 1;
    cw->N = 6;
    cw->nz = 0;
    cw->index = 1;
    cw->d[0] = 0;
    for (int j = 0; j < 6; j++) {
      cw->W[0][j] = weights[j];
      cw->d[0] += weights[j];
    }
    uint64_t t = now_ns();
    Make_CWS_Points(cw, p);
    uint64_t point_ns = now_ns() - t;
    if (p->n != 5 || p->np != np)
      fail("point-count mismatch");
    t = now_ns();
    int ip = IP_Check(p, v, e);
    uint64_t hull_ns = now_ns() - t;
    int ref = ip;
    for (int j = 0; j < e->ne; j++)
      if (e->e[j].c != 1)
        ref = 0;
    if (!ip || v->nv != nv || e->ne != ne || ref != reflexive)
      fail("IP/vertex/facet/reflexivity mismatch");
    vp->n = 5;
    vp->np = v->nv;
    for (int j = 0; j < v->nv; j++)
      memcpy(vp->x[j], p->x[v->v[j]], sizeof(p->x[0]));
    t = now_ns();
    int vip = IP_Check(vp, vv, ve);
    uint64_t vertex_hull_ns = now_ns() - t;
    if (!vip || vv->nv != nv || ve->ne != ne)
      fail("vertex-only hull mismatch");
    t = now_ns();
    int nfref = Make_Poly_NF(p, v, e, nf);
    uint64_t nf_ns = now_ns() - t;
    if (nfref != reflexive)
      fail("normal-form reflexivity mismatch");
    uint64_t dual_ns = 0;
    if (reflexive) {
      t = now_ns();
      Make_Dual_Poly(p, v, e, dp);
      dual_ns = now_ns() - t;
      if (dp->np != nd)
        fail("dual point-count mismatch");
    }
    if (r)
      putchar(',');
    printf("{\"point_ns\":%" PRIu64 ",\"hull_ns\":%" PRIu64
           ",\"vertex_hull_ns\":%" PRIu64 ",\"nf_ns\":%" PRIu64
           ",\"dual_ns\":%" PRIu64 "}",
           point_ns, hull_ns, vertex_hull_ns, nf_ns, dual_ns);
    if (!r) {
      dump(argv[1], p);
      if (reflexive)
        dump(argv[2], dp);
    }
  }
  printf("],\"vertices\":[");
  for (int j = 0; j < v->nv; j++) {
    if (j)
      putchar(',');
    printf("[");
    for (int k = 0; k < 5; k++)
      printf("%s%ld", k ? "," : "", p->x[v->v[j]][k]);
    printf("]");
  }
  printf("],\"facets\":[");
  for (int j = 0; j < e->ne; j++) {
    if (j)
      putchar(',');
    printf("[");
    for (int k = 0; k < 5; k++)
      printf("%s%ld", k ? "," : "", e->e[j].a[k]);
    printf(",%ld]", e->e[j].c);
  }
  puts("]}");
  free(cw);
  free(p);
  free(vp);
  free(dp);
  free(v);
  free(vv);
  free(e);
  free(ve);
  return 0;
}
