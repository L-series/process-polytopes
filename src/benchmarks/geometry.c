#define _GNU_SOURCE
#include "Global.h"
#include <errno.h>
#include <inttypes.h>
#include <limits.h>
#include <linux/perf_event.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/ioctl.h>
#include <sys/syscall.h>
#include <time.h>
#include <unistd.h>

FILE *inFILE, *outFILE;
extern void Make_CWS_Points(CWS *, PolyPointList *);
typedef struct {
  long values[80];
  int arity, points, vertices, reflexive;
} Input;
static uint64_t now_ns(void) {
  struct timespec t;
  if (clock_gettime(CLOCK_MONOTONIC, &t))
    abort();
  return (uint64_t)t.tv_sec * 1000000000 + t.tv_nsec;
}
static uint64_t instructions(int fd) {
  uint64_t value = 0;
  if (fd >= 0 && read(fd, &value, sizeof(value)) != sizeof(value))
    abort();
  return value;
}
static int open_counter(void) {
  struct perf_event_attr a = {0};
  a.type = PERF_TYPE_HARDWARE;
  a.size = sizeof(a);
  a.config = PERF_COUNT_HW_INSTRUCTIONS;
  a.disabled = 1;
  a.exclude_kernel = 1;
  a.exclude_hv = 1;
  return syscall(__NR_perf_event_open, &a, 0, -1, -1, 0);
}
static void fail(const char *message) {
  fprintf(stderr, "%s\n", message);
  exit(1);
}
/* Load compact inputs before timing; PALP's CWS contains large scratch arrays. */
static Input *load_inputs(const char *path, const char *labels, size_t *length) {
  FILE *f = fopen(path, "r"), *e = fopen(labels, "r");
  Input *rows = NULL;
  size_t n = 0, capacity = 0;
  char line[2048];
  if (!f || !e)
    fail("cannot open corpus or expectations");
  while (fgets(line, sizeof(line), f)) {
    Input row = {0};
    char *p = line, *end;
    int count = 0;
    while (1) {
      while (*p == ' ' || *p == '\t' || *p == '\n' || *p == '\r')
        p++;
      if (!*p)
        break;
      if (count == 80)
        fail("CWS too wide");
      errno = 0;
      long value = strtol(p, &end, 10);
      if (end == p || errno || value < 0 || value > INT_MAX)
        fail("invalid CWS integer");
      row.values[count++] = value;
      p = end;
    }
    for (int r = 2; r <= 5; r++)
      if (count == r * (6 + r))
        row.arity = r;
    if (!row.arity)
      fail("expected complete 5D CWS rows");
    for (int r = 0; r < row.arity; r++) {
      int width = 6 + row.arity;
      long sum = 0;
      for (int c = 1; c < width; c++)
        sum += row.values[r * width + c];
      if (sum <= 0 || sum != row.values[r * width])
        fail("degree mismatch");
    }
    if (fscanf(e, "%d %d %d", &row.points, &row.vertices, &row.reflexive) != 3)
      fail("missing corpus expectation");
    if (n == capacity) {
      capacity = capacity ? capacity * 2 : 1024;
      Input *next = realloc(rows, capacity * sizeof(*rows));
      if (!next)
        fail("input allocation failure");
      rows = next;
    }
    rows[n++] = row;
  }
  int extra;
  if (fscanf(e, "%d", &extra) != EOF || ferror(f) || ferror(e) || !n)
    fail("extra expectations or input read error");
  fclose(f);
  fclose(e);
  *length = n;
  return rows;
}

/* Separate symbol is also the Callgrind collection boundary. */
__attribute__((noinline)) static void run_geometry(Input *rows, size_t n, int fd) {
  CWS *cw = calloc(1, sizeof(*cw));
  PolyPointList *points = malloc(sizeof(*points));
  VertexNumList *vertices = malloc(sizeof(*vertices));
  EqList *equations = malloc(sizeof(*equations));
  uint64_t p_ns = 0, h_ns = 0, p_ir = 0, h_ir = 0, total_points = 0, reflexive = 0;
  if (!cw || !points || !vertices || !equations)
    fail("geometry allocation failure");
  uint64_t wall = now_ns();
  for (size_t i = 0; i < n; i++) {
    Input *row = &rows[i];
    int width = 6 + row->arity;
    cw->nw = row->arity;
    cw->N = 5 + row->arity;
    cw->nz = 0;
    cw->index = 1;
    for (int r = 0; r < cw->nw; r++) {
      cw->d[r] = row->values[r * width];
      for (int c = 0; c < cw->N; c++)
        cw->W[r][c] = row->values[r * width + 1 + c];
    }
    uint64_t a = instructions(fd), t = now_ns();
    Make_CWS_Points(cw, points);
    p_ns += now_ns() - t;
    p_ir += instructions(fd) - a;
    if (points->n != 5 || points->np != row->points)
      fail("dimension/point-count mismatch");
    a = instructions(fd);
    t = now_ns();
    int ip = IP_Check(points, vertices, equations);
    h_ns += now_ns() - t;
    h_ir += instructions(fd) - a;
    int r = ip;
    if (ip)
      for (int j = 0; j < equations->ne; j++)
        if (equations->e[j].c != 1)
          r = 0;
    if (!ip || vertices->nv != row->vertices || r != row->reflexive)
      fail("IP/vertex/reflexivity mismatch");
    total_points += points->np;
    reflexive += r;
  }
  wall = now_ns() - wall;
  printf("{\"rows\":%zu,\"points\":%" PRIu64 ",\"reflexive\":%" PRIu64 ",\"wall_ns\":%" PRIu64
         ",\"point_ns\":%" PRIu64 ",\"hull_ns\":%" PRIu64
         ",\"pmu\":%s,\"point_instructions\":%" PRIu64 ",\"hull_instructions\":%" PRIu64 "}\n",
         n, total_points, reflexive, wall, p_ns, h_ns, fd >= 0 ? "true" : "false", p_ir, h_ir);
  free(equations);
  free(vertices);
  free(points);
  free(cw);
}
int main(int argc, char **argv) {
  if (argc != 3 && argc != 4)
    fail("usage: geometry CORPUS EXPECTATIONS [--pmu]");
  inFILE = stdin;
  outFILE = stdout;
  size_t n;
  Input *rows = load_inputs(argv[1], argv[2], &n);
  int fd = -1;
  if (argc == 4) {
    if (strcmp(argv[3], "--pmu"))
      fail("unknown geometry option");
    fd = open_counter();
    if (fd < 0) {
      perror("hardware instructions unavailable");
      free(rows);
      return 77;
    }
    if (ioctl(fd, PERF_EVENT_IOC_RESET, 0) || ioctl(fd, PERF_EVENT_IOC_ENABLE, 0))
      abort();
  }
  run_geometry(rows, n, fd);
  if (fd >= 0)
    close(fd);
  free(rows);
  return fflush(stdout) ? 1 : 0;
}
