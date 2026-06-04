/* loop_parallel.c — test breadth-parallelism of PALP's dim-5 point walk.
 *
 * The 5 nested loops (Coord.c Make_CWS_Points fast path) form a DFS tree.
 * Question (user): is there dependent work in the 5-fold loop that can be
 * parallelized or precomputed?
 *
 * Key structural facts this program exploits/verifies:
 *  (1) The per-level "lev" accumulators are a loop-carried recurrence
 *      (lev4 += B4 each x4-step) but with a CLOSED FORM: after fixing x4,
 *      lev4[A] == x4 * B4[A].  So any outer-loop iteration can be STARTED
 *      independently from its index alone — the recurrence is an O(1)
 *      optimization, not a serialization barrier.
 *  (2) Sibling subtrees (distinct x4, or distinct x3 within an x4, ...) are
 *      INDEPENDENT: they share no mutable state except the output point
 *      buffer.  Hence the outer loop is embarrassingly parallel given
 *      per-thread output (here: a per-thread count; storage is negligible).
 *  (3) The only genuinely serial dependency is the depth-wise chain along a
 *      single root->leaf path (each level's bounds depend on the outer levels'
 *      chosen values).  That chain has length = dim = 5; it limits single-path
 *      ILP (and is why per-division speedups did not help, §4) but does NOT
 *      obstruct breadth parallelism.
 *
 * We measure: serial vs OpenMP-parallel-over-x4 wall time on the heaviest
 * candidates, verifying identical point counts.  Build: gcc -O3 -fopenmp.
 */
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#ifdef _OPENMP
#include <omp.h>
#endif

typedef long long Long;
#define NMAX 5
#define AMAX 32

typedef struct { int n, N; long np_ref, idx; Long b[NMAX][AMAX], X0[AMAX], Xmax[AMAX]; } Cand;

static inline Long PDF(Long N, Long D) { Long F = N / D; return (F * D > N) ? F - 1 : F; }
static int CLB(const Long *Bj, const Long *off, const Long *X0, const Long *Xmax,
               int Alo, int Ahi, Long *lo, Long *hi) {
  int A = Ahi - 1; Long R = Bj[A], Low = -X0[A] - off[A], Upp = Low + Xmax[A], L;
  *lo = -PDF(-Low, R); *hi = PDF(Upp, R);
  while (--A >= Alo) {
    if ((R = Bj[A])) {
      Low = -X0[A] - off[A]; Upp = Low + Xmax[A];
      if (R > 0) { if (*hi > (L = PDF(Upp, R))) *hi = L; if (*lo < (L = -PDF(-Low, R))) *lo = L; }
      else       { if (*hi > (L = PDF(-Low, -R))) *hi = L; if (*lo < (L = -PDF(Upp, -R))) *lo = L; }
    } else { Long X = X0[A] + off[A]; if (X < 0 || X > Xmax[A]) return 0; }
  }
  return 1;
}

/* Count points in the subtree below a fixed x4.  lev4 is passed in (= x4*B4,
 * the closed form), so this is fully independent of any other x4 value. */
static long subtree_x4(const Cand *c, const int *Amin, const Long *lev4_in) {
  const int A0 = 0, A1 = Amin[1], A2 = Amin[2], A3 = Amin[3], A4 = Amin[4];
  const Long *B0 = c->b[0], *B1 = c->b[1], *B2 = c->b[2], *B3 = c->b[3];
  const Long *X0 = c->X0, *Xmax = c->Xmax;
  Long lev3[AMAX], lev2[AMAX], lev1[AMAX];
  Long xmn3, xmx3, xmn2, xmx2, xmn1, xmx1, xmn0, xmx0;
  long cnt = 0; int A;
  if (!CLB(B3, lev4_in, X0, Xmax, A3, A4, &xmn3, &xmx3)) return 0;
  for (A = 0; A < A3; A++) lev3[A] = lev4_in[A] + (xmn3 - 1) * B3[A];
  for (Long x3 = xmn3; x3 <= xmx3; x3++) {
    for (A = 0; A < A3; A++) lev3[A] += B3[A];
    if (!CLB(B2, lev3, X0, Xmax, A2, A3, &xmn2, &xmx2)) continue;
    for (A = 0; A < A2; A++) lev2[A] = lev3[A] + (xmn2 - 1) * B2[A];
    for (Long x2 = xmn2; x2 <= xmx2; x2++) {
      for (A = 0; A < A2; A++) lev2[A] += B2[A];
      if (!CLB(B1, lev2, X0, Xmax, A1, A2, &xmn1, &xmx1)) continue;
      for (A = A0; A < A1; A++) lev1[A] = lev2[A] + (xmn1 - 1) * B1[A];
      for (Long x1 = xmn1; x1 <= xmx1; x1++) {
        for (A = A0; A < A1; A++) lev1[A] += B1[A];
        if (!CLB(B0, lev1, X0, Xmax, A0, A1, &xmn0, &xmx0)) continue;
        if (xmx0 >= xmn0) cnt += (long)(xmx0 - xmn0 + 1);
      }
    }
  }
  return cnt;
}

static int make_amin(const Cand *c, int *Amin) {
  int n = c->n, N = c->N;
  if (n != 5) return 0;
  Amin[0] = 0; Amin[n] = N;
  { int i = n, j = N; while (--i) { while (!c->b[i - 1][--j]) ; Amin[i] = ++j; } }
  return 1;
}

/* outer bounds for x4 */
static int x4_bounds(const Cand *c, const int *Amin, Long *xmn4, Long *xmx4) {
  Long zero[AMAX]; for (int A = 0; A < Amin[5]; A++) zero[A] = 0;
  return CLB(c->b[4], zero, c->X0, c->Xmax, Amin[4], Amin[5], xmn4, xmx4);
}

static long walk_serial(const Cand *c, const int *Amin) {
  Long xmn4, xmx4; if (!x4_bounds(c, Amin, &xmn4, &xmx4)) return 0;
  const Long *B4 = c->b[4]; int A4 = Amin[4];
  long cnt = 0;
  for (Long x4 = xmn4; x4 <= xmx4; x4++) {
    Long lev4[AMAX];
    for (int A = 0; A < A4; A++) lev4[A] = x4 * B4[A];   /* closed form */
    cnt += subtree_x4(c, Amin, lev4);
  }
  return cnt;
}

static long walk_parallel(const Cand *c, const int *Amin) {
  Long xmn4, xmx4; if (!x4_bounds(c, Amin, &xmn4, &xmx4)) return 0;
  const Long *B4 = c->b[4]; int A4 = Amin[4];
  long cnt = 0;
  long lo = (long)xmn4, hi = (long)xmx4;
#ifdef _OPENMP
  #pragma omp parallel for schedule(dynamic,1) reduction(+:cnt)
#endif
  for (long x4 = lo; x4 <= hi; x4++) {
    Long lev4[AMAX];
    for (int A = 0; A < A4; A++) lev4[A] = (Long)x4 * B4[A];  /* independent start */
    cnt += subtree_x4(c, Amin, lev4);
  }
  return cnt;
}

static int parse_line(char *line, Cand *c) {
  char *p = strstr(line, "BASIS"); if (!p) return 0;
  if (sscanf(p, "BASIS n=%d N=%d np=%ld idx=%ld", &c->n, &c->N, &c->np_ref, &c->idx) < 4) return 0;
  if (c->n != 5 || c->N < 1 || c->N > AMAX) return 0;
  char *s = p;
  for (int row = 0; row < c->n; row++) {
    s = strchr(s, '|'); if (!s) return 0; s++;
    for (int A = 0; A < c->N; A++) { long v; int k; if (sscanf(s, " %ld%n", &v, &k) != 1) return 0; c->b[row][A] = v; s += k; }
  }
  char *q0 = strstr(p, "|0"); if (!q0) return 0; q0 += 2;
  for (int A = 0; A < c->N; A++) { long v; int k; if (sscanf(q0, " %ld%n", &v, &k) != 1) return 0; c->X0[A] = v; q0 += k; }
  char *qx = strstr(p, "|X"); if (!qx) return 0; qx += 2;
  for (int A = 0; A < c->N; A++) { long v; int k; if (sscanf(qx, " %ld%n", &v, &k) != 1) return 0; c->Xmax[A] = v; qx += k; }
  return 1;
}

static double wall(void) { struct timespec t; clock_gettime(CLOCK_MONOTONIC, &t); return t.tv_sec + 1e-9 * t.tv_nsec; }

int main(int argc, char **argv) {
  int topN = 5, reps = 5;
  for (int i = 1; i < argc; i++) {
    if (!strcmp(argv[i], "--top") && i + 1 < argc) topN = atoi(argv[++i]);
    else if (!strcmp(argv[i], "--reps") && i + 1 < argc) reps = atoi(argv[++i]);
  }
  /* read all candidates, keep those with the most x4-range*np work (heaviest) */
  static Cand all[200000]; long n = 0;
  char line[1 << 16];
  while (n < (long)(sizeof all / sizeof all[0]) && fgets(line, sizeof line, stdin))
    if (parse_line(line, &all[n])) n++;
  fprintf(stderr, "loaded %ld candidates\n", n);
  if (!n) return 1;

  /* estimate weight = serial point count (one pass), pick heaviest */
  long *wt = malloc(n * sizeof(long));
  for (long i = 0; i < n; i++) {
    int Amin[NMAX + 1]; if (!make_amin(&all[i], Amin)) { wt[i] = -1; continue; }
    wt[i] = walk_serial(&all[i], Amin);
  }
  /* selection of topN heaviest indices */
  for (int t = 0; t < topN; t++) {
    long best = -1, bi = -1;
    for (long i = 0; i < n; i++) if (wt[i] > best) { best = wt[i]; bi = i; }
    if (bi < 0 || best <= 0) break;
    Cand *c = &all[bi]; int Amin[NMAX + 1]; make_amin(c, Amin);
    Long xmn4, xmx4; x4_bounds(c, Amin, &xmn4, &xmx4);

    /* time serial */
    long rs = 0; double bs = 1e30;
    for (int r = 0; r < reps; r++) { double a = wall(); rs = walk_serial(c, Amin); double e = wall() - a; if (e < bs) bs = e; }
    /* time parallel */
    long rp = 0; double bp = 1e30; int nth = 1;
#ifdef _OPENMP
    nth = omp_get_max_threads();
#endif
    for (int r = 0; r < reps; r++) { double a = wall(); rp = walk_parallel(c, Amin); double e = wall() - a; if (e < bp) bp = e; }

    printf("heavy#%d idx=%ld N=%d np=%ld x4range=%ld | serial np=%ld %.4fms | par(%dth) np=%ld %.4fms | speedup=%.2fx | match=%s\n",
           t, c->idx, c->N, c->np_ref, (long)(xmx4 - xmn4 + 1),
           rs, bs * 1e3, nth, rp, bp * 1e3, (bp > 0 ? bs / bp : 0),
           (rs == rp ? "YES" : "NO!"));
    wt[bi] = -1;  /* remove from pool */
  }
  free(wt);
  return 0;
}
