// Integer kernel basis of a CWS and its lattice points: the LLL +
// Fincke-Pohst walk (with an exact triangular fallback). Points are returned
// as 5-dimensional coordinates in the kernel basis, with (1,...,1) at the
// origin of the walk; the walk also records the coordinate and pair
// certificates that prove a complete point set is not IP.
#pragma once

#include "common.cuh"

namespace cwsgpu {

__device__ long long device_abs_ll(long long value) {
    return value < 0 ? -value : value;
}

__device__ long long device_nngcd(long long left, long long right) {
    left = device_abs_ll(left);
    right = device_abs_ll(right);
    if (!right) return left;
    while (left %= right) {
        if (!(right %= left)) return left;
    }
    return right;
}

__device__ long long device_egcd(long long a0, long long a1,
                                 long long *out0, long long *out1) {
    long long v0 = a0;
    long long v1 = a1;
    long long x0 = 1;
    long long x1 = 0;
    long long x2 = 0;
    long long a2 = 0;
    while ((a2 = a0 % a1)) {
        x2 = x0 - x1 * (a0 / a1);
        a0 = a1;
        a1 = a2;
        x0 = x1;
        x1 = x2;
    }
    *out0 = x1;
    *out1 = (a1 - v0 * x1) / v1;
    return a1;
}

__device__ long long device_round_q(long long numerator, long long denominator) {
    if (denominator < 0) {
        denominator = -denominator;
        numerator = -numerator;
    }
    long long floor_value = numerator / denominator;
    return floor_value + (2 * (numerator - floor_value * denominator)) / denominator;
}

// Signed floor/ceil division for the Fincke-Pohst box propagation (the divisor
// sign varies). The walk runs in int32: coordinate bounds are at most the
// largest degree (3486 over the pools) and LLL-reduced basis entries are small.
__device__ int device_fdiv32(int a, int b) {           // floor(a/b)
    int q = a / b, r = a % b;
    if (r != 0 && ((a < 0) != (b < 0))) --q;
    return q;
}
__device__ int device_cdiv32(int a, int b) {           // ceil(a/b)
    int q = a / b, r = a % b;
    if (r != 0 && ((a < 0) == (b < 0))) ++q;
    return q;
}

__device__ long long device_w_to_glz(long long *weights, int dim,
                                     long long glz[10][10]) {
    for (int row = 1; row < dim; ++row) {
        for (int col = 0; col < dim; ++col) glz[row][col] = 0;
    }
    long long *extended = glz[0];
    long long *base = glz[1];
    long long gcd = device_egcd(weights[0], weights[1], &extended[0], &extended[1]);
    base[0] = -weights[1] / gcd;
    base[1] = weights[0] / gcd;
    for (int row = 2; row < dim; ++row) {
        long long egcd_left = 0;
        long long egcd_right = 0;
        long long next_gcd = device_egcd(gcd, weights[row], &egcd_left, &egcd_right);
        base = glz[row];
        base[row] = gcd / next_gcd;
        long long scale = weights[row] / next_gcd;
        for (int col = 0; col < row; ++col) base[col] = -extended[col] * scale;
        for (int col = 0; col < row; ++col) extended[col] *= egcd_left;
        extended[row] = egcd_right;
        for (int improve = row - 1; improve > 0; --improve) {
            long long *candidate = glz[improve];
            long long round_base = device_round_q(base[improve], candidate[improve]);
            long long round_extended = device_round_q(extended[improve], candidate[improve]);
            for (int col = 0; col <= improve; ++col) {
                base[col] -= round_base * candidate[col];
                extended[col] -= round_extended * candidate[col];
            }
        }
        gcd = next_gcd;
    }
    return gcd;
}

__device__ int device_solve_next_weight_equation(const long long *next_weight,
                                                 int basis_coords,
                                                 int *out_dim,
                                                 long long out_basis[10][10]) {
    int positions[10];
    long long compact_weight[10];
    long long glz[10][10];
    int nonzero = 0;
    *out_dim = basis_coords - 1;
    for (int coord = 0; coord < basis_coords; ++coord) {
        for (int row = 0; row < *out_dim; ++row) out_basis[row][coord] = 0;
        if (next_weight[coord]) {
            positions[nonzero] = coord;
            compact_weight[nonzero] = next_weight[coord];
            ++nonzero;
        }
    }
    if (nonzero == 0) return 0;
    if (nonzero > 1) {
        device_w_to_glz(compact_weight, nonzero, glz);
    } else {
        for (int coord = 0; coord < positions[0]; ++coord) out_basis[coord][coord] = 1;
        for (int coord = positions[0] + 1; coord < basis_coords; ++coord) out_basis[coord - 1][coord] = 1;
        return 1;
    }
    for (int row = 1; row < nonzero; ++row) {
        if (glz[row][row] < 0) {
            for (int col = 0; col <= row; ++col) glz[row][col] = -glz[row][col];
        }
    }
    int scan = 0;
    for (; scan < positions[0]; ++scan) out_basis[scan][scan] = 1;
    while ((++scan) < positions[1]) out_basis[scan - 1][scan] = 1;
    out_basis[scan - 1][positions[0]] = glz[1][0];
    out_basis[scan - 1][positions[1]] = glz[1][1];
    int compact_row = 2;
    while (++scan < basis_coords) {
        if (next_weight[scan]) {
            long long *basis_row = out_basis[scan - 1];
            for (int col = 0; col <= compact_row; ++col) basis_row[positions[col]] = glz[compact_row][col];
            ++compact_row;
        } else {
            out_basis[scan - 1][scan] = 1;
        }
    }
    return 1;
}

__device__ int device_make_cws_basis(const DeviceCwsCandidate &candidate,
                                     int *basis_dim,
                                     long long basis[5][10]) {
    long long current[10][10];
    long long next_basis[10][10];
    long long composed[10][10];
    long long next_weight[10];
    int current_dim = 0;
    long long first_weight[10];
    for (int coord = 0; coord < candidate.ambient_vertices; ++coord) {
        first_weight[coord] = candidate.weights[0][coord];
    }
    if (!device_solve_next_weight_equation(first_weight, candidate.ambient_vertices, &current_dim, current)) {
        return 0;
    }
    for (int slot = 1; slot < candidate.nw; ++slot) {
        for (int row = 0; row < current_dim; ++row) {
            next_weight[row] = 0;
            for (int coord = 0; coord < candidate.ambient_vertices; ++coord) {
                next_weight[row] += static_cast<long long>(candidate.weights[slot][coord]) * current[row][coord];
            }
        }
        int reduced_dim = 0;
        if (!device_solve_next_weight_equation(next_weight, current_dim, &reduced_dim, next_basis)) return 0;
        for (int row = 0; row < reduced_dim; ++row) {
            for (int coord = 0; coord < candidate.ambient_vertices; ++coord) {
                composed[row][coord] = 0;
                for (int inner = 0; inner < current_dim; ++inner) {
                    composed[row][coord] += next_basis[row][inner] * current[inner][coord];
                }
            }
        }
        current_dim = reduced_dim;
        for (int row = 0; row < current_dim; ++row) {
            for (int coord = 0; coord < candidate.ambient_vertices; ++coord) current[row][coord] = composed[row][coord];
        }
    }
    if (current_dim != 5) return 0;
    *basis_dim = current_dim;
    for (int row = 0; row < 5; ++row) {
        for (int coord = 0; coord < candidate.ambient_vertices; ++coord) basis[row][coord] = current[row][coord];
    }
    return 1;
}

__device__ int device_candidate_basic_precheck(const DeviceCwsCandidate &candidate,
                                               long long x_upper[10]) {
    if (candidate.nw < 1 || candidate.nw > kMaxSlots) return 0;
    if (candidate.ambient_vertices < 1 || candidate.ambient_vertices > 10) return 0;
    if (candidate.ambient_vertices - candidate.nw != 5) return 0;

    for (int row = 0; row < candidate.nw; ++row) {
        long long sum = 0;
        for (int coord = 0; coord < candidate.ambient_vertices; ++coord) {
            int weight = candidate.weights[row][coord];
            if (weight < 0) return 0;
            sum += weight;
        }
        if (candidate.degree[row] <= 0 || sum != candidate.degree[row]) return 0;
    }

    unsigned long long point_upper_bound = 1;
    for (int coord = 0; coord < candidate.ambient_vertices; ++coord) {
        long long bound = 0;
        int support = 0;
        for (int row = 0; row < candidate.nw; ++row) {
            int weight = candidate.weights[row][coord];
            if (weight) {
                long long limit = candidate.degree[row] / weight;
                bound = support ? (bound < limit ? bound : limit) : limit;
                support = 1;
            }
        }
        if (!support) return 0;
        x_upper[coord] = bound;
        if (point_upper_bound < 6ULL) {
            unsigned long long choices = static_cast<unsigned long long>(bound + 1);
            point_upper_bound *= choices;
            if (point_upper_bound > 6ULL) point_upper_bound = 6ULL;
        }
    }
    return point_upper_bound >= 6ULL;
}


__device__ int device_vec_greater_than(const long long *left, const long long *right) {
    for (int coord = 4; coord >= 0; --coord) {
        if (left[coord] > right[coord]) return 1;
        if (left[coord] < right[coord]) return 0;
    }
    return 0;
}

__device__ int device_append_ip_point(const long long x[5], long long *points,
                                      int max_points, int *point_count) {
    int index = *point_count;
    if (index >= max_points) return 0;
    for (int coord = 0; coord < 5; ++coord) points[index * 5 + coord] = x[coord];
    *point_count = index + 1;
    return 1;
}


// LLL + Fincke-Pohst lattice-point walk. The kernel basis of a CWS is very
// skewed; an LLL-reduced basis and a box-propagation walk visit far fewer tree
// nodes. FP32 in the LLL only affects how well the basis is reduced: size
// reductions and swaps are integer row operations, so the lattice and the
// enumerated point set do not depend on rounding. The root box comes from an
// FP64 ellipsoid around the coordinate box, widened by an integer guard, and
// every leaf is an exact integer lattice point. Points are mapped back through
// the unimodular transform U to coordinates in the original kernel basis.
#define FP_NMAX 5
#define FP_GUARD 1            // integer slack on the FP64-derived root box
#define FP_NODE_CAP 4000000  // runaway guard -> ship to CPU (completeness-safe)

// Gram-Schmidt in the box-scaled metric <u,v> = sum_A u_A v_A * w_A (w_A=1/r_A^2).
__device__ void device_gso_f(const int b[FP_NMAX][10], int n, int N,
                             const float *w, float mu[FP_NMAX][FP_NMAX],
                             float *B2) {
    float bs[FP_NMAX][10];
    for (int i = 0; i < n; ++i) {
        for (int A = 0; A < N; ++A) bs[i][A] = static_cast<float>(b[i][A]);
        for (int j = 0; j < i; ++j) {
            float dot = 0.f, nj = B2[j];
            for (int A = 0; A < N; ++A)
                dot += static_cast<float>(b[i][A]) * bs[j][A] * w[A];
            mu[i][j] = (nj > 0.f) ? dot / nj : 0.f;
            for (int A = 0; A < N; ++A) bs[i][A] -= mu[i][j] * bs[j][A];
        }
        float nn = 0.f;
        for (int A = 0; A < N; ++A) nn += bs[i][A] * bs[i][A] * w[A];
        B2[i] = nn;
    }
}

// LLL reduce the integer basis rows in the box-scaled metric; track the
// unimodular transform U (reduced row i = sum_j U[i][j] * original row j). No
// determinant check: integer row ops + swaps are unimodular by construction.
__device__ void device_lll_f(int b[FP_NMAX][10], int n, int N, const float *w,
                             float delta, int U[FP_NMAX][FP_NMAX]) {
    for (int i = 0; i < n; ++i)
        for (int j = 0; j < n; ++j) U[i][j] = (i == j) ? 1 : 0;
    float mu[FP_NMAX][FP_NMAX], B2[FP_NMAX];
    device_gso_f(b, n, N, w, mu, B2);
    int k = 1, guard = 0;
    while (k < n && guard++ < 2000) {
        for (int l = k - 1; l >= 0; --l) {
            if (fabsf(mu[k][l]) > 0.5f) {
                int q = static_cast<int>(lrintf(mu[k][l]));
                if (q) {
                    for (int A = 0; A < N; ++A) b[k][A] -= q * b[l][A];
                    for (int j = 0; j < n; ++j) U[k][j] -= q * U[l][j];
                    device_gso_f(b, n, N, w, mu, B2);
                }
            }
        }
        if (B2[k] >= (delta - mu[k][k - 1] * mu[k][k - 1]) * B2[k - 1]) {
            ++k;
        } else {
            for (int A = 0; A < N; ++A) { int t = b[k][A]; b[k][A] = b[k - 1][A]; b[k - 1][A] = t; }
            for (int j = 0; j < n; ++j) { int t = U[k][j]; U[k][j] = U[k - 1][j]; U[k - 1][j] = t; }
            device_gso_f(b, n, N, w, mu, B2);
            k = (k - 1 > 1) ? k - 1 : 1;
        }
    }
}

// Gauss-Jordan solve G x = rhs (n<=5, SPD) in FP64, returning x and diag(G^-1);
// 0 if singular. FP64 keeps the root-box error (~1e-12) far inside the 1-unit
// integer guard.
__device__ int device_spd_solve_d(double G[FP_NMAX][FP_NMAX], int n,
                                  const double *rhs, double *x, double *invdiag) {
    double A[FP_NMAX][2 * FP_NMAX];
    for (int i = 0; i < n; ++i) {
        for (int j = 0; j < n; ++j) { A[i][j] = G[i][j]; A[i][n + j] = (i == j) ? 1.0 : 0.0; }
    }
    for (int c = 0; c < n; ++c) {
        int piv = c; double best = fabs(A[c][c]);
        for (int r = c + 1; r < n; ++r) if (fabs(A[r][c]) > best) { best = fabs(A[r][c]); piv = r; }
        if (best < 1e-9) return 0;
        if (piv != c) for (int j = 0; j < 2 * n; ++j) { double t = A[c][j]; A[c][j] = A[piv][j]; A[piv][j] = t; }
        double d = A[c][c];
        for (int j = 0; j < 2 * n; ++j) A[c][j] /= d;
        for (int r = 0; r < n; ++r) if (r != c) {
            double f = A[r][c];
            for (int j = 0; j < 2 * n; ++j) A[r][j] -= f * A[c][j];
        }
    }
    for (int i = 0; i < n; ++i) {
        invdiag[i] = A[i][n + i];
        double s = 0.0; for (int j = 0; j < n; ++j) s += A[i][n + j] * rhs[j];
        x[i] = s;
    }
    return 1;
}

// Tighten [lo,hi] for variable ord[depth] against every ambient box constraint
// 0 <= X_A <= xu[A], with deeper (not-yet-fixed) variables ranged over the root
// box [Lo,Hi]. Exact integer interval arithmetic. Returns 0 if the node is empty.
// cw[depth][A] = bred[ord[depth]][A] and the deeper-level range sums
// sLo/sHi[depth][A] depend only on the root box, so they are precomputed once
// per candidate instead of re-summed at every tree node.
__device__ int device_fp_bounds(int depth, const int *accum_d,
                                const int cw[FP_NMAX][10],
                                const int sLo_d[FP_NMAX][10], const int sHi_d[FP_NMAX][10],
                                int lo, int hi, const int *xu,
                                int N, int *out_lo, int *out_hi) {
    for (int A = 0; A < N && lo <= hi; ++A) {
        int c = cw[depth][A];
        int sLo = sLo_d[depth][A], sHi = sHi_d[depth][A];
        int base = accum_d[A];
        int K1 = -base - sHi;          // c*x >= K1
        int K2 = xu[A] - base - sLo;   // c*x <= K2
        if (c > 0) {
            int t = device_cdiv32(K1, c); if (t > lo) lo = t;
            t = device_fdiv32(K2, c); if (t < hi) hi = t;
        } else if (c < 0) {
            int t = device_fdiv32(K1, c); if (t < hi) hi = t;
            t = device_cdiv32(K2, c); if (t > lo) lo = t;
        } else {
            if (base + sHi < 0 || base + sLo > xu[A]) return 0;
        }
    }
    *out_lo = lo; *out_hi = hi;
    return 1;
}

// *ip_possible is cleared when the completed point set certifies that
// the candidate is not IP. Every lattice point has 0 <= X_A, and the candidate
// interior point X = (1,...,1) is itself a lattice point. Suppose X_A is not
// constant on the point set. If no point has X_A == 0 then all points lie in
// {X_A >= 1}; if no point has X_A >= 2 then all lie in {X_A <= 1}. Either way
// X_A = 1 is a supporting hyperplane of the hull that does not contain the
// whole hull, so (1,...,1) lies on a proper face and is not interior.
// A coordinate that is constant (== 1) on all points -- e.g. forced by a
// combination of weight rows -- proves nothing and is ignored.
// The flag stays set (no claim) on fallback paths and on overflow.
__device__ int device_make_points_fp(const DeviceCwsCandidate &candidate,
                                     long long *points, int max_points,
                                     int *point_count, int *ip_possible) {
    *ip_possible = 1;
    long long basis64[5][10];
    int basis_dim = 0;
    long long x_upper64[10] = {0};
    if (!device_candidate_basic_precheck(candidate, x_upper64)) return 0;
    const int nA = candidate.ambient_vertices;

    if (!device_make_cws_basis(candidate, &basis_dim, basis64)) return 0;
    const int n = basis_dim;                 // == 5 on the fast path
    int bred[FP_NMAX][10];
    int xu[10];
    for (int r = 0; r < n; ++r)
        for (int c = 0; c < nA; ++c) bred[r][c] = static_cast<int>(basis64[r][c]);
    for (int c = 0; c < nA; ++c) xu[c] = static_cast<int>(x_upper64[c]);

    // Box-scaled metric weights w_A = 1/r_A^2, r_A = Xmax_A/2 (origin X0 == 1).
    float w[10], r[10];
    for (int A = 0; A < nA; ++A) {
        r[A] = (xu[A] > 0) ? (xu[A] * 0.5f) : 0.5f;
        w[A] = 1.f / (r[A] * r[A]);
    }
    int U[FP_NMAX][FP_NMAX];
    device_lll_f(bred, n, nA, w, 0.75f, U);   // delta 0.75: faster than 0.99, same points

    // Circumscribed ellipsoid sum_A ((X_A-cc_A)/r_A)^2 <= rho ⊇ box.
    // Gram G=M^T M, gv=M^T t, M[A][j]=bred[j][A]/r_A, t_A=(cc_A-X0_A)/r_A, cc_A=r_A.
    // This root-box computation runs in FP64 (~200 flops/candidate),
    // which is what makes the 1-unit FP_GUARD safe; the LLL stays FP32.
    double G[FP_NMAX][FP_NMAX], gv[FP_NMAX], tt = 0.0;
    double rho = 0.0, t[10], rd[10];
    for (int A = 0; A < nA; ++A) {
        rd[A] = (xu[A] > 0) ? (xu[A] * 0.5) : 0.5;
        double cc = (xu[A] > 0) ? (xu[A] * 0.5) : 0.0;
        if (xu[A] > 0) rho += 1.0;
        t[A] = (cc - 1.0) / rd[A];         // X0_A == 1
        tt += t[A] * t[A];
    }
    for (int i = 0; i < n; ++i) {
        double gi = 0.0;
        for (int A = 0; A < nA; ++A) gi += (bred[i][A] / rd[A]) * t[A];
        gv[i] = gi;
        for (int j = 0; j < n; ++j) {
            double s = 0.0;
            for (int A = 0; A < nA; ++A) s += (bred[i][A] / rd[A]) * (bred[j][A] / rd[A]);
            G[i][j] = s;
        }
    }
    double xhat[FP_NMAX], invdiag[FP_NMAX];
    if (!device_spd_solve_d(G, n, gv, xhat, invdiag))   // numerically degenerate: defer to the CPU
        return -1;
    double res = tt;
    for (int i = 0; i < n; ++i) res -= gv[i] * xhat[i];
    double rho2 = rho - res + 1e-3;
    if (rho2 < 0.0) rho2 = 0.0;

    int Lo[FP_NMAX], Hi[FP_NMAX], ord[FP_NMAX];
    for (int j = 0; j < n; ++j) {
        double hw = (invdiag[j] > 0.0) ? sqrt(rho2 * invdiag[j]) : 0.0;
        Lo[j] = static_cast<int>(floor(xhat[j] - hw)) - FP_GUARD;
        Hi[j] = static_cast<int>(ceil(xhat[j] + hw)) + FP_GUARD;
    }
    // Enumeration order: longest GSO vector (scaled metric) outermost (FP convention).
    {
        int tmp[FP_NMAX][10];
        for (int i = 0; i < n; ++i) for (int A = 0; A < nA; ++A) tmp[i][A] = bred[i][A];
        float mu[FP_NMAX][FP_NMAX], B2[FP_NMAX];
        device_gso_f(tmp, n, nA, w, mu, B2);
        int used[FP_NMAX]; for (int j = 0; j < n; ++j) used[j] = 0;
        for (int s = 0; s < n; ++s) {
            int best = -1; float bv = -1.f;
            for (int j = 0; j < n; ++j) if (!used[j] && B2[j] > bv) { bv = B2[j]; best = j; }
            used[best] = 1; ord[s] = best;
        }
    }

    int cw[FP_NMAX][10], sLo[FP_NMAX][10], sHi[FP_NMAX][10], dLo[FP_NMAX], dHi[FP_NMAX];
    for (int d = 0; d < n; ++d) {
        dLo[d] = Lo[ord[d]];
        dHi[d] = Hi[ord[d]];
        for (int A = 0; A < nA; ++A) cw[d][A] = bred[ord[d]][A];
    }
    for (int A = 0; A < nA; ++A) {
        sLo[n - 1][A] = 0;
        sHi[n - 1][A] = 0;
        for (int d = n - 2; d >= 0; --d) {
            int j = ord[d + 1], bb = bred[j][A];
            sLo[d][A] = sLo[d + 1][A] + (bb > 0 ? bb * Lo[j] : bb * Hi[j]);
            sHi[d][A] = sHi[d + 1][A] + (bb > 0 ? bb * Hi[j] : bb * Lo[j]);
        }
    }

    // Iterative DFS box-propagation walk. accum[d] = X0 + contributions of the
    // d outer fixed levels; xprime[basis-row] = chosen reduced coefficient.
    // The last level's interval is exact (no deeper slack), so every leaf in it
    // is a lattice point: no leaf box test and no leaf accum update.
    *point_count = 0;
    int accum[FP_NMAX + 1][10];
    for (int A = 0; A < nA; ++A) accum[0][A] = 1;       // X0
    int lo_s[FP_NMAX], hi_s[FP_NMAX], cur[FP_NMAX], xprime[FP_NMAX] = {0};
    long long nodes = 0;
    int depth = 0;
    if (!device_fp_bounds(0, accum[0], cw, sLo, sHi, dLo[0], dHi[0], xu, nA, &lo_s[0], &hi_s[0]))
        return *point_count > 0 ? 1 : 0;
    cur[0] = lo_s[0];
    unsigned int touch_zero = 0, touch_two = 0;         // bit A: some point has X_A == 0 / X_A >= 2
    // The same certificate for the pair functionals X_A + X_B (value 2 at
    // (1,...,1)). pair_lo[A] bit B: some point has X_A == 0 and X_B <= 1 (sum <= 1);
    // pair_hi[A] bit B: some point has X_A >= 3, or X_A == 2 and X_B >= 1 (sum >= 3).
    unsigned int pair_lo[10] = {0}, pair_hi[10] = {0};
    while (depth >= 0) {
        if (cur[depth] > hi_s[depth]) {                 // exhausted -> ascend
            --depth;
            if (depth >= 0) ++cur[depth];
            continue;
        }
        int wv = ord[depth];
        xprime[wv] = cur[depth];
        if (depth + 1 == n) {                           // leaf: in-box by construction
            unsigned int m0 = 0, m1 = 0, m2 = 0, m3 = 0;   // X_A == 0 / 1 / 2 / >= 3
            for (int A = 0; A < nA; ++A) {
                int X = accum[depth][A] + cur[depth] * cw[depth][A];
                unsigned int bit = 1u << A;
                if (X == 0) m0 |= bit;
                else if (X == 1) m1 |= bit;
                else if (X == 2) m2 |= bit;
                else m3 |= bit;
            }
            touch_zero |= m0;
            touch_two |= m2 | m3;
            for (unsigned int s = m0; s; s &= s - 1) pair_lo[__ffs(s) - 1] |= m0 | m1;
            for (unsigned int s = m3; s; s &= s - 1) pair_hi[__ffs(s) - 1] = ~0u;
            for (unsigned int s = m2; s; s &= s - 1) pair_hi[__ffs(s) - 1] |= m1 | m2 | m3;
            long long lp[5];
            for (int j = 0; j < 5; ++j) {
                int sum = 0;
                for (int i = 0; i < n; ++i) sum += xprime[i] * U[i][j];
                lp[j] = sum;
            }
            if (!device_append_ip_point(lp, points, max_points, point_count)) return -1;
            ++cur[depth];
        } else {                                        // descend
            for (int A = 0; A < nA; ++A) accum[depth + 1][A] = accum[depth][A] + cur[depth] * cw[depth][A];
            if (++nodes > FP_NODE_CAP) return -1;       // runaway -> CPU (complete)
            if (device_fp_bounds(depth + 1, accum[depth + 1], cw, sLo, sHi, dLo[depth + 1], dHi[depth + 1],
                                 xu, nA, &lo_s[depth + 1], &hi_s[depth + 1])) {
                ++depth;
                cur[depth] = lo_s[depth];
            } else {
                ++cur[depth];                           // pruned node, next sibling
            }
        }
    }
    // X_A is a non-negative integer, so "not constant (== 1)" is touch_zero|touch_two;
    // "non-constant but one side missing" is exactly one of the two bits.
    if (touch_zero ^ touch_two) *ip_possible = 0;
    for (int A = 0; A < nA && *ip_possible; ++A) {
        for (int B = A + 1; B < nA; ++B) {
            unsigned int lo = ((pair_lo[A] >> B) | (pair_lo[B] >> A)) & 1u;
            unsigned int hi = ((pair_hi[A] >> B) | (pair_hi[B] >> A)) & 1u;
            if (lo ^ hi) { *ip_possible = 0; break; }
        }
    }
    return *point_count > 0 ? 1 : 0;
}

}  // namespace cwsgpu
