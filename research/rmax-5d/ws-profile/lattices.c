#define _POSIX_C_SOURCE 200809L
#include "Global.h"
#include <inttypes.h>
#include <limits.h>
#include <stdint.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>

FILE *inFILE, *outFILE;

static uint64_t now_ns(void) {
  struct timespec t;
  if (clock_gettime(CLOCK_MONOTONIC, &t))
    abort();
  return (uint64_t)t.tv_sec * 1000000000 + t.tv_nsec;
}

int main(int argc, char **argv) {
  if (argc != 2)
    return 2;
  inFILE = stdin;
  outFILE = stdout;
  PolyPointList *p = malloc(sizeof(*p)), *q = malloc(sizeof(*q));
  VertexNumList *v = malloc(sizeof(*v));
  EqList *e = malloc(sizeof(*e));
  FILE *f = fopen(argv[1], "rb");
  int64_t header[2];
  if (!p || !q || !v || !e || !f || sizeof(Long) != 8 ||
      fread(header, sizeof(int64_t), 2, f) != 2 || header[1] != 5 ||
      header[0] < 6 || header[0] > POINT_Nmax)
    return 3;
  p->n = q->n = 5;
  p->np = header[0];
  if (fread(p->x, sizeof(p->x[0]), p->np, f) != (size_t)p->np ||
      fgetc(f) != EOF || fclose(f))
    return 4;
  uint64_t start = now_ns();
  printf("{\"states\":[");
  int state = 0;
  for (int prime = 2; prime <= 3; prime++)
    for (int pivot = 0; pivot < 5; pivot++) {
      int count = 1;
      for (int j = pivot + 1; j < 5; j++)
        count *= prime;
      for (int code = 0; code < count; code++) {
        int coeff[5] = {0}, digits = code;
        for (int j = 4; j > pivot; j--) {
          coeff[j] = digits % prime;
          digits /= prime;
        }
        Long lo[5], hi[5];
        for (int j = 0; j < 5; j++) {
          lo[j] = LONG_MAX;
          hi[j] = LONG_MIN;
        }
        q->np = 0;
        uint64_t t = now_ns();
        for (int i = 0; i < p->np; i++) {
          Long value = p->x[i][pivot];
          for (int j = pivot + 1; j < 5; j++)
            value -= coeff[j] * p->x[i][j];
          if (value % prime)
            continue;
          for (int j = 0; j < 5; j++) {
            Long x = j == pivot ? value / prime : p->x[i][j];
            q->x[q->np][j] = x;
            if (x < lo[j])
              lo[j] = x;
            if (x > hi[j])
              hi[j] = x;
          }
          q->np++;
        }
        uint64_t filter_ns = now_ns() - t, hull_ns = 0;
        int cheap = q->np <= 5, ip = 0, ref = 0;
        for (int j = 0; j < 5; j++)
          if (lo[j] >= 0 || hi[j] <= 0)
            cheap = 1;
        if (!cheap) {
          t = now_ns();
          ip = IP_Check(q, v, e);
          hull_ns = now_ns() - t;
          ref = ip;
          if (ip)
            for (int j = 0; j < e->ne; j++)
              if (e->e[j].c != 1)
                ref = 0;
        }
        if (state++)
          putchar(',');
        printf("{\"prime\":%d,\"pivot\":%d,\"code\":%d,\"points\":%d,"
               "\"cheap_non_ip\":%d,\"ip\":%d,\"reflexive\":%d,"
               "\"vertices\":%d,\"facets\":%d,\"filter_ns\":%" PRIu64
               ",\"hull_ns\":%" PRIu64 "}",
               prime, pivot, code, q->np, cheap, ip, ref,
               ip ? v->nv : 0, ip ? e->ne : 0, filter_ns, hull_ns);
      }
    }
  printf("],\"kernel_ns\":%" PRIu64 "}\n", now_ns() - start);
  free(p);
  free(q);
  free(v);
  free(e);
  return 0;
}
