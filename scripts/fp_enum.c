/* fp_enum.c — LLL + Fincke-Pohst-style lattice-point enumerator prototype.
 *
 * Tests the §3 hypothesis of POINT_WALK_ALGORITHMS.md: PALP's triangular (HNF)
 * enumeration basis is pathologically skewed (orthogonality defect ~1e9), so a
 * near-orthogonal LLL-reduced basis with a general (Fincke-Pohst-style) walk
 * should visit far fewer tree nodes / divisions while enumerating the *same*
 * lattice points.  This program measures that directly and proves correctness
 * by comparing the enumerated point SETS.
 *
 * Input (stdin): the BASIS lines emitted by PALP's Coord.c built with
 * -DDUMP_BASIS, one candidate per line:
 *   BASIS n=<n> N=<N> np=<np> idx=<idx> | r0.. | r1.. ... | r{n-1}.. |0 X0.. |X Xmax..
 * where rows r_j are the triangular enumeration basis B.x[j][.], X0 is the
 * origin (all-ones at index 1), Xmax the per-coordinate box bound.
 *
 * The region enumerated is { X = X0 + sum_j x_j b_j : 0 <= X_A <= Xmax_A }.
 * (The weight-hyperplane constraints are automatically satisfied because the
 * b_j span the weight lattice; see POINT_WALK_ALGORITHMS.md §0.)
 *
 * Three enumerators, all collect the ambient point set X and count work:
 *   1. tri_enum  — exact byte-faithful replica of PALP's dim-5 fast path
 *                  (CLB + 5 nested loops on the triangular basis). REFERENCE.
 *   2. gen_enum(triangular) — the general box-propagation walk on the *same*
 *                  triangular basis (validates gen_enum; isolates algorithm).
 *   3. gen_enum(LLL)        — the same general walk on the LLL-reduced basis
 *                  (the experiment).
 * gen_enum derives a root search box from the box's circumscribed ellipsoid
 * (Gram + Cholesky, the Fincke-Pohst ingredient) and then enumerates with
 * integer box-constraint propagation (LP-free, exact-integer interval bounds).
 *
 * Correctness = the three point sets are identical (checked per candidate).
 * Work       = internal nodes visited + integer divisions performed.
 */
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <math.h>
#include <stdint.h>
#include <time.h>

typedef long long Long;

#define NMAX  5      /* lattice dimension (dim-5 fast path)            */
#define AMAX  32     /* ambient dimension bound (PALP AMBI_Dmax = 30)  */
#define PTMAX 8192   /* max points/candidate (type-3 np tops out ~2033)*/
#define NODE_CAP 20000000LL   /* safety cap against runaway enumeration */
#define GTRI_MAX_NP 256        /* run the gen(triangular) validation only here */

/* ----- exact floor / ceil division for any nonzero divisor ----- */
static inline Long fdiv_(Long a, Long b) {        /* floor(a/b) */
  Long q = a / b, r = a % b;
  if (r != 0 && ((a < 0) != (b < 0))) q--;
  return q;
}
static inline Long cdiv_(Long a, Long b) {        /* ceil(a/b) */
  Long q = a / b, r = a % b;
  if (r != 0 && ((a < 0) == (b < 0))) q++;
  return q;
}

/* ============================ candidate ============================ */
typedef struct {
  int  n, N;
  long np_ref, idx;
  Long b[NMAX][AMAX];   /* triangular enumeration basis (rows)         */
  Long X0[AMAX];
  Long Xmax[AMAX];
} Cand;

/* ============================ point set ============================ */
typedef struct {
  Long  p[PTMAX][AMAX];
  int   np;
  int   N;
  long  nodes, divs;
  int   overflow;       /* set if np would exceed PTMAX                 */
} PtSet;

static int g_N_cmp;
static int pt_cmp(const void *A, const void *B) {
  const Long *a = (const Long *)A, *b = (const Long *)B;
  for (int i = 0; i < g_N_cmp; i++)
    if (a[i] != b[i]) return a[i] < b[i] ? -1 : 1;
  return 0;
}
static void ps_sort(PtSet *s) {
  g_N_cmp = s->N;
  qsort(s->p, s->np, sizeof(Long) * AMAX, pt_cmp);
}
static int ps_equal(PtSet *a, PtSet *b) {
  if (a->overflow || b->overflow) return -1;   /* unknown */
  if (a->np != b->np) return 0;
  for (int i = 0; i < a->np; i++)
    for (int A = 0; A < a->N; A++)
      if (a->p[i][A] != b->p[i][A]) return 0;
  return 1;
}
static inline void ps_add(PtSet *s, const Long *X) {
  if (s->np >= PTMAX) { s->overflow = 1; return; }
  memcpy(s->p[s->np++], X, sizeof(Long) * AMAX);
}

/* ===================================================================
 *  1. tri_enum — exact replica of PALP's dim-5 fast path (Coord.c)
 * =================================================================== */
static long g_tri_divs;
static inline Long PDF(Long N, Long D) {          /* PALP PD_Floor, D>0 */
  g_tri_divs++;
  Long F = N / D;
  return (F * D > N) ? F - 1 : F;
}
static int CLB(const Long *Bj, const Long *off, const Long *X0,
               const Long *Xmax, int Alo, int Ahi, Long *lo, Long *hi,
               long *nodes) {
  (*nodes)++;
  int A = Ahi - 1;
  Long R = Bj[A], Low = -X0[A] - off[A], Upp = Low + Xmax[A], L;
  *lo = -PDF(-Low, R);
  *hi = PDF(Upp, R);
  while (--A >= Alo) {
    if ((R = Bj[A])) {
      Low = -X0[A] - off[A]; Upp = Low + Xmax[A];
      if (R > 0) {
        if (*hi > (L = PDF(Upp, R))) *hi = L;
        if (*lo < (L = -PDF(-Low, R))) *lo = L;
      } else {
        if (*hi > (L = PDF(-Low, -R))) *hi = L;
        if (*lo < (L = -PDF(Upp, -R))) *lo = L;
      }
    } else {
      Long X = X0[A] + off[A];
      if (X < 0 || X > Xmax[A]) return 0;
    }
  }
  return 1;
}
static void tri_enum(const Cand *c, PtSet *out) {
  int n = c->n, N = c->N, A;
  out->np = 0; out->N = N; out->overflow = 0; out->nodes = 0;
  g_tri_divs = 0;
  if (n != 5) return;                 /* dump only contains fast-path n=5 */

  int Amin[NMAX + 1];
  Amin[0] = 0; Amin[n] = N;
  { int i = n, j = N; while (--i) { while (!c->b[i - 1][--j]) ; Amin[i] = ++j; } }
  const int A0 = 0, A1 = Amin[1], A2 = Amin[2], A3 = Amin[3], A4 = Amin[4], A5 = Amin[5];
  const Long *B0 = c->b[0], *B1 = c->b[1], *B2 = c->b[2], *B3 = c->b[3], *B4 = c->b[4];
  const Long *X0 = c->X0, *Xmax = c->Xmax;

  Long lev4[AMAX], lev3[AMAX], lev2[AMAX], lev1[AMAX], zero[AMAX];
  Long xmn4, xmx4, xmn3, xmx3, xmn2, xmx2, xmn1, xmx1, xmn0, xmx0;

  for (A = 0; A < A5; A++) zero[A] = 0;
  if (!CLB(B4, zero, X0, Xmax, A4, A5, &xmn4, &xmx4, &out->nodes)) goto done;
  for (A = 0; A < A4; A++) lev4[A] = (xmn4 - 1) * B4[A];

  for (Long x4 = xmn4; x4 <= xmx4; x4++) {
    for (A = 0; A < A4; A++) lev4[A] += B4[A];
    if (!CLB(B3, lev4, X0, Xmax, A3, A4, &xmn3, &xmx3, &out->nodes)) continue;
    for (A = 0; A < A3; A++) lev3[A] = lev4[A] + (xmn3 - 1) * B3[A];

    for (Long x3 = xmn3; x3 <= xmx3; x3++) {
      for (A = 0; A < A3; A++) lev3[A] += B3[A];
      if (!CLB(B2, lev3, X0, Xmax, A2, A3, &xmn2, &xmx2, &out->nodes)) continue;
      for (A = 0; A < A2; A++) lev2[A] = lev3[A] + (xmn2 - 1) * B2[A];

      for (Long x2 = xmn2; x2 <= xmx2; x2++) {
        for (A = 0; A < A2; A++) lev2[A] += B2[A];
        if (!CLB(B1, lev2, X0, Xmax, A1, A2, &xmn1, &xmx1, &out->nodes)) continue;
        for (A = A0; A < A1; A++) lev1[A] = lev2[A] + (xmn1 - 1) * B1[A];

        for (Long x1 = xmn1; x1 <= xmx1; x1++) {
          for (A = A0; A < A1; A++) lev1[A] += B1[A];
          if (!CLB(B0, lev1, X0, Xmax, A0, A1, &xmn0, &xmx0, &out->nodes)) continue;

          for (Long x0 = xmn0; x0 <= xmx0; x0++) {
            Long X[AMAX];
            for (A = 0; A < N; A++)
              X[A] = X0[A] + x0 * B0[A] + x1 * B1[A] + x2 * B2[A]
                          + x3 * B3[A] + x4 * B4[A];
            ps_add(out, X);
          }
        }
      }
    }
  }
done:
  out->divs = g_tri_divs;
}

/* ===================================================================
 *  LLL reduction in the box-scaled metric  <u,v> = sum_A u_A v_A / r_A^2
 *  Reduces the integer basis rows; returns det(U) (should be +-1).
 * =================================================================== */
static void gso(Long b[NMAX][AMAX], int n, int N, const double *w /* 1/r^2 */,
                double mu[NMAX][NMAX], double *B2) {
  double bs[NMAX][AMAX];                /* Gram-Schmidt vectors (scaled space)*/
  for (int i = 0; i < n; i++) {
    for (int A = 0; A < N; A++) bs[i][A] = (double)b[i][A];
    for (int j = 0; j < i; j++) {
      double dot = 0, nj = B2[j];
      for (int A = 0; A < N; A++) dot += (double)b[i][A] * bs[j][A] * w[A];
      mu[i][j] = (nj > 0) ? dot / nj : 0.0;
      for (int A = 0; A < N; A++) bs[i][A] -= mu[i][j] * bs[j][A];
    }
    double nn = 0;
    for (int A = 0; A < N; A++) nn += bs[i][A] * bs[i][A] * w[A];
    B2[i] = nn;
  }
}
/* exact det of small integer matrix via fraction-free Bareiss */
static long long idet(long long M[NMAX][NMAX], int n) {
  long long prev = 1;
  for (int k = 0; k < n; k++) {
    if (M[k][k] == 0) {
      int s = -1;
      for (int i = k + 1; i < n; i++) if (M[i][k]) { s = i; break; }
      if (s < 0) return 0;
      for (int j = 0; j < n; j++) { long long t = M[k][j]; M[k][j] = M[s][j]; M[s][j] = t; }
      prev = -prev;                     /* row swap flips sign (folded below) */
    }
    for (int i = k + 1; i < n; i++)
      for (int j = k + 1; j < n; j++)
        M[i][j] = (M[i][j] * M[k][k] - M[i][k] * M[k][j]) / prev;
    prev = M[k][k];
  }
  return M[n - 1][n - 1];
}
static long long lll_reduce(Long b[NMAX][AMAX], int n, int N, const double *w,
                            double delta) {
  long long U[NMAX][NMAX];
  for (int i = 0; i < n; i++) for (int j = 0; j < n; j++) U[i][j] = (i == j);
  double mu[NMAX][NMAX], B2[NMAX];
  gso(b, n, N, w, mu, B2);
  int k = 1, guard = 0;
  while (k < n && guard++ < 100000) {
    for (int l = k - 1; l >= 0; l--) {
      if (fabs(mu[k][l]) > 0.5) {
        long long q = llroundl((long double)mu[k][l]);
        if (q) {
          for (int A = 0; A < N; A++) b[k][A] -= q * b[l][A];
          for (int j = 0; j < n; j++) U[k][j] -= q * U[l][j];
          gso(b, n, N, w, mu, B2);
        }
      }
    }
    if (B2[k] >= (delta - mu[k][k - 1] * mu[k][k - 1]) * B2[k - 1]) {
      k++;
    } else {
      for (int A = 0; A < N; A++) { Long t = b[k][A]; b[k][A] = b[k - 1][A]; b[k - 1][A] = t; }
      for (int j = 0; j < n; j++) { long long t = U[k][j]; U[k][j] = U[k - 1][j]; U[k - 1][j] = t; }
      gso(b, n, N, w, mu, B2);
      k = (k - 1 > 1) ? k - 1 : 1;
    }
  }
  long long Uc[NMAX][NMAX];
  for (int i = 0; i < n; i++) for (int j = 0; j < n; j++) Uc[i][j] = U[i][j];
  return idet(Uc, n);
}

/* ===================================================================
 *  3. gen_enum — general box-propagation walk on an arbitrary basis,
 *     root box from the circumscribed-ellipsoid (Fincke-Pohst ingredient).
 * =================================================================== */
typedef struct {
  int n, N;
  const Long (*b)[AMAX];
  const Long *X0, *Xmax;
  Long  Lo[NMAX], Hi[NMAX];          /* root search box (superset)        */
  int   ord[NMAX];                   /* enumeration order, outer..inner   */
  long  nodes, divs;
  PtSet *out;
  int   blow;
} Gen;

/* solve G x = rhs (n<=5) by Gauss-Jordan; also return full inverse diag */
static int spd_solve(double G[NMAX][NMAX], int n, double *rhs, double *x,
                     double *invdiag) {
  double A[NMAX][2 * NMAX];
  for (int i = 0; i < n; i++) {
    for (int j = 0; j < n; j++) { A[i][j] = G[i][j]; A[i][n + j] = (i == j); }
  }
  for (int c = 0; c < n; c++) {
    int piv = c; double best = fabs(A[c][c]);
    for (int r = c + 1; r < n; r++) if (fabs(A[r][c]) > best) { best = fabs(A[r][c]); piv = r; }
    if (best < 1e-12) return 0;
    if (piv != c) for (int j = 0; j < 2 * n; j++) { double t = A[c][j]; A[c][j] = A[piv][j]; A[piv][j] = t; }
    double d = A[c][c];
    for (int j = 0; j < 2 * n; j++) A[c][j] /= d;
    for (int r = 0; r < n; r++) if (r != c) {
      double f = A[r][c];
      for (int j = 0; j < 2 * n; j++) A[r][j] -= f * A[c][j];
    }
  }
  for (int i = 0; i < n; i++) {
    invdiag[i] = A[i][n + i];
    double s = 0; for (int j = 0; j < n; j++) s += A[i][n + j] * rhs[j];
    x[i] = s;
  }
  return 1;
}

static void gen_rec(Gen *g, int depth, const Long *accum) {
  if (g->blow) return;
  int n = g->n, N = g->N;
  if (depth == n) {                       /* leaf: not counted as a node */
    for (int A = 0; A < N; A++)
      if (accum[A] < 0 || accum[A] > g->Xmax[A]) return;   /* exact box test */
    ps_add(g->out, accum);
    if (g->out->overflow) g->blow = 1;
    return;
  }
  if (++g->nodes > NODE_CAP) { g->blow = 1; return; }   /* internal bounding node */
  int w = g->ord[depth];
  Long lo = g->Lo[w], hi = g->Hi[w];
  for (int A = 0; A < N && lo <= hi; A++) {
    Long c = g->b[w][A];
    /* interval of the not-yet-fixed deeper variables' contribution to coord A */
    Long sLo = 0, sHi = 0;
    for (int d = depth + 1; d < n; d++) {
      int j = g->ord[d]; Long bb = g->b[j][A];
      if (bb > 0) { sLo += bb * g->Lo[j]; sHi += bb * g->Hi[j]; }
      else        { sLo += bb * g->Hi[j]; sHi += bb * g->Lo[j]; }
    }
    Long base = accum[A];
    /* need 0 <= base + c*x_w + s <= Xmax[A] for some s in [sLo,sHi]:
     *   c*x_w >= -base - sHi   and   c*x_w <= Xmax[A] - base - sLo        */
    Long K1 = -base - sHi;                 /* c*x_w >= K1 */
    Long K2 = g->Xmax[A] - base - sLo;     /* c*x_w <= K2 */
    if (c > 0) {
      Long t = cdiv_(K1, c); if (t > lo) lo = t;
      t = fdiv_(K2, c); if (t < hi) hi = t;
      g->divs += 2;
    } else if (c < 0) {
      Long t = fdiv_(K1, c); if (t < hi) hi = t;   /* dividing by neg flips */
      t = cdiv_(K2, c); if (t > lo) lo = t;
      g->divs += 2;
    } else {                                /* c == 0: A cannot bound x_w */
      if (base + sHi < 0 || base + sLo > g->Xmax[A]) return;  /* prune node */
    }
  }
  Long child[AMAX];
  for (Long x = lo; x <= hi && !g->blow; x++) {
    for (int A = 0; A < N; A++) child[A] = accum[A] + x * g->b[w][A];
    gen_rec(g, depth + 1, child);
  }
}

static void gen_enum(const Cand *c, const Long basis[NMAX][AMAX], PtSet *out) {
  int n = c->n, N = c->N;
  out->np = 0; out->N = N; out->overflow = 0; out->nodes = 0; out->divs = 0;

  /* ellipsoid: box {0<=X_A<=Xmax_A} subset { sum_A ((X_A-cc_A)/r_A)^2 <= rho } */
  double cc[AMAX], r[AMAX], w[AMAX];
  double rho = 0;
  for (int A = 0; A < N; A++) {
    if (c->Xmax[A] > 0) { cc[A] = c->Xmax[A] / 2.0; r[A] = c->Xmax[A] / 2.0; rho += 1.0; }
    else                { cc[A] = 0.0;              r[A] = 0.5; }
    w[A] = 1.0 / (r[A] * r[A]);
  }
  /* Gram G = M^T M, g_vec = M^T t,  M[A][j]=basis[j][A]/r_A, t_A=(cc_A-X0_A)/r_A */
  double G[NMAX][NMAX], gv[NMAX], tt = 0;
  double t[AMAX];
  for (int A = 0; A < N; A++) t[A] = (cc[A] - c->X0[A]) / r[A];
  for (int A = 0; A < N; A++) tt += t[A] * t[A];
  for (int i = 0; i < n; i++) {
    gv[i] = 0;
    for (int A = 0; A < N; A++) gv[i] += (basis[i][A] / r[A]) * t[A];
    for (int j = 0; j < n; j++) {
      double s = 0;
      for (int A = 0; A < N; A++) s += (basis[i][A] / r[A]) * (basis[j][A] / r[A]);
      G[i][j] = s;
    }
  }
  double xhat[NMAX], invdiag[NMAX];
  if (!spd_solve(G, n, gv, xhat, invdiag)) {     /* singular: fall back wide */
    for (int j = 0; j < n; j++) { xhat[j] = 0; invdiag[j] = 0; }
  }
  double res = tt;
  for (int i = 0; i < n; i++) res -= gv[i] * xhat[i];   /* ||t - M xhat||^2 */
  double rho2 = rho - res + 1e-6;
  if (rho2 < 0) rho2 = 0;

  Gen g; g.n = n; g.N = N; g.b = basis; g.X0 = c->X0; g.Xmax = c->Xmax;
  g.nodes = 0; g.divs = 0; g.out = out; g.blow = 0;
  for (int j = 0; j < n; j++) {
    double hw = (invdiag[j] > 0) ? sqrt(rho2 * invdiag[j]) : 0.0;
    g.Lo[j] = (Long)floor(xhat[j] - hw) - 2;       /* +/-2 guard vs FP error */
    g.Hi[j] = (Long)ceil (xhat[j] + hw) + 2;
  }
  /* enumeration order: longest GSO vector (scaled metric) outermost */
  { double mu[NMAX][NMAX], B2[NMAX]; Long tmp[NMAX][AMAX];
    for (int i = 0; i < n; i++) for (int A = 0; A < N; A++) tmp[i][A] = basis[i][A];
    gso(tmp, n, N, w, mu, B2);
    int used[NMAX]; for (int j = 0; j < n; j++) used[j] = 0;
    for (int s = 0; s < n; s++) {
      int best = -1; double bv = -1;
      for (int j = 0; j < n; j++) if (!used[j] && B2[j] > bv) { bv = B2[j]; best = j; }
      used[best] = 1; g.ord[s] = best;
    }
  }
  Long accum0[AMAX];
  for (int A = 0; A < N; A++) accum0[A] = c->X0[A];
  gen_rec(&g, 0, accum0);
  out->nodes = g.nodes; out->divs = g.divs;
  if (g.blow) out->overflow = 1;
}

/* ============================== parse ============================== */
static int parse_line(char *line, Cand *c) {
  char *p = strstr(line, "BASIS");
  if (!p) return 0;
  if (sscanf(p, "BASIS n=%d N=%d np=%ld idx=%ld",
             &c->n, &c->N, &c->np_ref, &c->idx) < 4) return 0;
  if (c->n < 1 || c->n > NMAX || c->N < 1 || c->N > AMAX) return 0;
  char *s = p;
  for (int row = 0; row < c->n; row++) {
    s = strchr(s, '|'); if (!s) return 0; s++;          /* row pipe is "| .."*/
    for (int A = 0; A < c->N; A++) {
      long v; int k;
      if (sscanf(s, " %ld%n", &v, &k) != 1) return 0;
      c->b[row][A] = v; s += k;
    }
  }
  char *q0 = strstr(p, "|0"); if (!q0) return 0; q0 += 2;
  for (int A = 0; A < c->N; A++) { long v; int k; if (sscanf(q0, " %ld%n", &v, &k) != 1) return 0; c->X0[A] = v; q0 += k; }
  char *qx = strstr(p, "|X"); if (!qx) return 0; qx += 2;
  for (int A = 0; A < c->N; A++) { long v; int k; if (sscanf(qx, " %ld%n", &v, &k) != 1) return 0; c->Xmax[A] = v; qx += k; }
  return 1;
}

/* ============================== main ============================== */
int main(int argc, char **argv) {
  double delta = 0.99;
  int verbose = 0, csv = 0;
  for (int i = 1; i < argc; i++) {
    if (!strcmp(argv[i], "-v")) verbose = 1;
    else if (!strcmp(argv[i], "--csv")) csv = 1;
    else if (!strcmp(argv[i], "--delta") && i + 1 < argc) delta = atof(argv[++i]);
  }

  static Cand c;
  static PtSet tri, gtri, glll;
  char line[1 << 16];

  long  ncand = 0, n_np_ok = 0, n_gtri_ok = 0, n_gtri_tot = 0, n_glll_ok = 0;
  long  n_lll_bad = 0, n_skip = 0, n_blow_tri = 0, n_blow_lll = 0;
  long long tot_tri_nodes = 0, tot_tri_divs = 0;
  long long tot_gtri_nodes = 0, tot_gtri_divs = 0;
  long long tot_glll_nodes = 0, tot_glll_divs = 0;
  long long tot_np = 0;
  /* heavy-tail (np in top decile) aggregates */
  long long heavy_tri_nodes = 0, heavy_glll_nodes = 0, heavy_tri_divs = 0, heavy_glll_divs = 0;
  long  n_heavy = 0;
  double tri_secs = 0, glll_secs = 0, lll_secs = 0;

  if (csv) printf("idx,N,np,tri_nodes,tri_divs,gtri_nodes,gtri_divs,"
                  "glll_nodes,glll_divs,np_ok,gtri_ok,glll_ok,lll_det\n");

  while (fgets(line, sizeof line, stdin)) {
    if (!parse_line(line, &c)) continue;
    if (c.n != 5) continue;
    if (c.np_ref > PTMAX - 8) { n_skip++; continue; }
    ncand++;

    struct timespec t0, t1;
    clock_gettime(CLOCK_MONOTONIC, &t0);
    tri_enum(&c, &tri);
    clock_gettime(CLOCK_MONOTONIC, &t1);
    tri_secs += (t1.tv_sec - t0.tv_sec) + 1e-9 * (t1.tv_nsec - t0.tv_nsec);

    int do_gtri = (c.np_ref <= GTRI_MAX_NP);  /* gen(tri) blows up on skewed */
    if (do_gtri) gen_enum(&c, c.b, &gtri);    /* validation on tractable cands */

    Long lllb[NMAX][AMAX];
    memcpy(lllb, c.b, sizeof lllb);
    double w[AMAX];
    for (int A = 0; A < c.N; A++) { double rr = (c.Xmax[A] > 0) ? c.Xmax[A] / 2.0 : 0.5; w[A] = 1.0 / (rr * rr); }
    clock_gettime(CLOCK_MONOTONIC, &t0);
    long long det = lll_reduce(lllb, c.n, c.N, w, delta);
    clock_gettime(CLOCK_MONOTONIC, &t1);
    lll_secs += (t1.tv_sec - t0.tv_sec) + 1e-9 * (t1.tv_nsec - t0.tv_nsec);
    if (det != 1 && det != -1) n_lll_bad++;

    clock_gettime(CLOCK_MONOTONIC, &t0);
    gen_enum(&c, (const Long(*)[AMAX])lllb, &glll);
    clock_gettime(CLOCK_MONOTONIC, &t1);
    glll_secs += (t1.tv_sec - t0.tv_sec) + 1e-9 * (t1.tv_nsec - t0.tv_nsec);

    /* correctness: sort and compare the point sets */
    int np_ok = 1, gtri_ok = -1, glll_ok;
    if (!tri.overflow) {
      ps_sort(&tri);
      if ((long)tri.np != c.np_ref) np_ok = 0;  /* tri replica vs PALP's np */
    }
    if (do_gtri && !gtri.overflow) ps_sort(&gtri);
    if (!glll.overflow) ps_sort(&glll);
    if (do_gtri) gtri_ok = ps_equal(&tri, &gtri);
    glll_ok = ps_equal(&tri, &glll);

    if (np_ok) n_np_ok++;
    if (do_gtri) { n_gtri_tot++; if (gtri_ok == 1) n_gtri_ok++; if (gtri.overflow) n_blow_tri++; }
    if (glll_ok == 1) n_glll_ok++;
    if (glll.overflow) n_blow_lll++;

    tot_np += tri.np;
    tot_tri_nodes += tri.nodes; tot_tri_divs += tri.divs;
    if (do_gtri) { tot_gtri_nodes += gtri.nodes; tot_gtri_divs += gtri.divs; }
    tot_glll_nodes += glll.nodes; tot_glll_divs += glll.divs;

    if (verbose && (glll_ok != 1 || np_ok == 0)) {
      fprintf(stderr, "MISMATCH idx=%ld N=%d np_ref=%ld tri=%d gtri=%d(%d) glll=%d(%d) det=%lld\n",
              c.idx, c.N, c.np_ref, tri.np, gtri.np, gtri_ok, glll.np, glll_ok, det);
    }
    if (csv)
      printf("%ld,%d,%d,%ld,%ld,%ld,%ld,%ld,%ld,%d,%d,%d,%lld\n",
             c.idx, c.N, tri.np, tri.nodes, tri.divs, gtri.nodes, gtri.divs,
             glll.nodes, glll.divs, np_ok, gtri_ok, glll_ok, det);

    /* heavy tail: candidates with np >= 64 (the cost-dominating tail) */
    if (tri.np >= 64 && !tri.overflow && !glll.overflow) {
      n_heavy++;
      heavy_tri_nodes += tri.nodes; heavy_glll_nodes += glll.nodes;
      heavy_tri_divs  += tri.divs;  heavy_glll_divs  += glll.divs;
    }
  }

  fprintf(stderr,
    "\n==================== fp_enum summary ====================\n"
    "candidates processed     : %ld   (skipped np>%d: %ld)\n"
    "total points enumerated  : %lld\n"
    "--- correctness ---\n"
    "tri replica np == PALP np : %ld / %ld\n"
    "gen(triangular) set==tri  : %ld / %ld   (validation subset np<=%d, overflow %ld)\n"
    "gen(LLL)        set==tri  : %ld / %ld   (overflow %ld)\n"
    "LLL det(U) not +/-1       : %ld\n"
    "--- work (whole dataset) ---\n"
    "tri (PALP)  nodes=%lld  divs=%lld\n"
    "gen(tri)    nodes=%lld  divs=%lld   (validation subset only)\n"
    "gen(LLL)    nodes=%lld  divs=%lld\n"
    "LLL/tri(PALP) nodes ratio = %.4f   divs ratio = %.4f\n"
    "--- heavy tail (np>=64, %ld cand) ---\n"
    "tri nodes=%lld divs=%lld | LLL nodes=%lld divs=%lld\n"
    "heavy LLL/tri nodes=%.4f  divs=%.4f\n"
    "--- wall time ---\n"
    "tri_enum %.3fs   LLL-reduce %.3fs   gen(LLL) %.3fs   LLL+gen %.3fs\n"
    "=========================================================\n",
    ncand, PTMAX - 8, n_skip, tot_np,
    n_np_ok, ncand,
    n_gtri_ok, n_gtri_tot, GTRI_MAX_NP, n_blow_tri,
    n_glll_ok, ncand, n_blow_lll,
    n_lll_bad,
    tot_tri_nodes, tot_tri_divs,
    tot_gtri_nodes, tot_gtri_divs,
    tot_glll_nodes, tot_glll_divs,
    tot_tri_nodes ? (double)tot_glll_nodes / tot_tri_nodes : 0,
    tot_tri_divs ? (double)tot_glll_divs / tot_tri_divs : 0,
    n_heavy, heavy_tri_nodes, heavy_tri_divs, heavy_glll_nodes, heavy_glll_divs,
    heavy_tri_nodes ? (double)heavy_glll_nodes / heavy_tri_nodes : 0,
    heavy_tri_divs ? (double)heavy_glll_divs / heavy_tri_divs : 0,
    tri_secs, lll_secs, glll_secs, lll_secs + glll_secs);

  return 0;
}
