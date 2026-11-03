// Integer kernel basis of a CWS and its lattice points: the LLL +
// Fincke-Pohst walk (with an exact triangular fallback). Points are returned
// as 5-dimensional coordinates in the kernel basis, with (1,...,1) at the
// origin of the walk; the walk also records the coordinate and pair
// certificates that prove a complete point set is not IP.
#pragma once

#include "common.cuh"

namespace cwsgpu {

// Checked signed arithmetic used before the walk's bounded int32 phase.
// Failure propagates to an inconclusive/deferred candidate.
__device__ long long device_walk_add(long long a, long long b, int *ok) {
    const long long top = 9223372036854775807LL, bottom = -top - 1;
    if (!*ok) return 0;
    if ((b > 0 && a > top - b) || (b < 0 && a < bottom - b)) { *ok = 0; return 0; }
    return a + b;
}
__device__ long long device_walk_sub(long long a, long long b, int *ok) {
    const long long top = 9223372036854775807LL, bottom = -top - 1;
    if (!*ok) return 0;
    if ((b > 0 && a < bottom + b) || (b < 0 && a > top + b)) { *ok = 0; return 0; }
    return a - b;
}
__device__ long long device_walk_mul(long long a, long long b, int *ok) {
    if (!*ok) return 0;
    const bool negative = (a < 0) != (b < 0);
    const unsigned long long ua = a < 0 ? 0ULL - static_cast<unsigned long long>(a) : a;
    const unsigned long long ub = b < 0 ? 0ULL - static_cast<unsigned long long>(b) : b;
    const unsigned long long limit = negative ? (1ULL << 63) : 9223372036854775807ULL;
    if (ua && ub > limit / ua) { *ok = 0; return 0; }
    const unsigned long long product = ua * ub;
    if (!negative) return static_cast<long long>(product);
    return product == (1ULL << 63) ? (-9223372036854775807LL - 1) : -static_cast<long long>(product);
}
__device__ long long device_walk_div(long long a, long long b, int *ok) {
    if (!*ok) return 0;
    if (!b || (a == (-9223372036854775807LL - 1) && b == -1)) { *ok = 0; return 0; }
    return a / b;
}
__device__ long long device_walk_rem(long long a, long long b, int *ok) {
    if (!*ok) return 0;
    if (!b || (a == (-9223372036854775807LL - 1) && b == -1)) { *ok = 0; return 0; }
    return a % b;
}

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
                                 long long *out0, long long *out1, int *ok) {
    long long v0 = a0;
    long long v1 = a1;
    long long x0 = 1;
    long long x1 = 0;
    long long x2 = 0;
    long long a2 = 0;
    while ((a2 = device_walk_rem(a0, a1, ok))) {
        if (!*ok) return 0;
        x2 = device_walk_sub(x0, device_walk_mul(x1, device_walk_div(a0, a1, ok), ok), ok);
        if (!*ok) return 0;
        a0 = a1;
        a1 = a2;
        x0 = x1;
        x1 = x2;
    }
    *out0 = x1;
    *out1 = device_walk_div(device_walk_sub(a1, device_walk_mul(v0, x1, ok), ok), v1, ok);
    return a1;
}

__device__ long long device_round_q(long long numerator, long long denominator, int *ok) {
    if (denominator < 0) {
        denominator = device_walk_sub(0, denominator, ok);
        numerator = device_walk_sub(0, numerator, ok);
    }
    long long floor_value = device_walk_div(numerator, denominator, ok);
    long long remainder = device_walk_sub(numerator, device_walk_mul(floor_value, denominator, ok), ok);
    return device_walk_add(floor_value, device_walk_div(device_walk_mul(2, remainder, ok), denominator, ok), ok);
}

// Signed floor/ceil division for the Fincke-Pohst box propagation (the divisor
// sign varies). The int32 phase is entered only after device_walk_int32_box
// has established exact bounds for products, partial sums and divisions.
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
                                     long long glz[10][10], int *ok) {
    for (int row = 1; row < dim; ++row) {
        for (int col = 0; col < dim; ++col) glz[row][col] = 0;
    }
    long long *extended = glz[0];
    long long *base = glz[1];
    long long gcd = device_egcd(weights[0], weights[1], &extended[0], &extended[1], ok);
    if (!*ok || !gcd) return 0;
    base[0] = device_walk_div(device_walk_sub(0, weights[1], ok), gcd, ok);
    base[1] = device_walk_div(weights[0], gcd, ok);
    for (int row = 2; row < dim; ++row) {
        long long egcd_left = 0;
        long long egcd_right = 0;
        long long next_gcd = device_egcd(gcd, weights[row], &egcd_left, &egcd_right, ok);
        if (!*ok || !next_gcd) return 0;
        base = glz[row];
        base[row] = device_walk_div(gcd, next_gcd, ok);
        long long scale = device_walk_div(weights[row], next_gcd, ok);
        for (int col = 0; col < row; ++col) base[col] = device_walk_mul(device_walk_sub(0, extended[col], ok), scale, ok);
        for (int col = 0; col < row; ++col) extended[col] = device_walk_mul(extended[col], egcd_left, ok);
        extended[row] = egcd_right;
        for (int improve = row - 1; improve > 0; --improve) {
            long long *candidate = glz[improve];
            long long round_base = device_round_q(base[improve], candidate[improve], ok);
            long long round_extended = device_round_q(extended[improve], candidate[improve], ok);
            for (int col = 0; col <= improve; ++col) {
                base[col] = device_walk_sub(base[col], device_walk_mul(round_base, candidate[col], ok), ok);
                extended[col] = device_walk_sub(extended[col], device_walk_mul(round_extended, candidate[col], ok), ok);
            }
        }
        if (!*ok) return 0;
        gcd = next_gcd;
    }
    return gcd;
}

__device__ int device_solve_next_weight_equation(const long long *next_weight,
                                                 int basis_coords,
                                                 int *out_dim,
                                                 long long out_basis[10][10], int *ok) {
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
        device_w_to_glz(compact_weight, nonzero, glz, ok);
        if (!*ok) return 0;
    } else {
        for (int coord = 0; coord < positions[0]; ++coord) out_basis[coord][coord] = 1;
        for (int coord = positions[0] + 1; coord < basis_coords; ++coord) out_basis[coord - 1][coord] = 1;
        return 1;
    }
    for (int row = 1; row < nonzero; ++row) {
        if (glz[row][row] < 0) {
            for (int col = 0; col <= row; ++col) glz[row][col] = device_walk_sub(0, glz[row][col], ok);
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
    int arithmetic_ok = 1;
    long long current[10][10];
    long long next_basis[10][10];
    long long composed[10][10];
    long long next_weight[10];
    int current_dim = 0;
    long long first_weight[10];
    for (int coord = 0; coord < candidate.ambient_vertices; ++coord) {
        first_weight[coord] = candidate.weights[0][coord];
    }
    if (!device_solve_next_weight_equation(first_weight, candidate.ambient_vertices, &current_dim, current, &arithmetic_ok)) {
        return 0;
    }
    for (int slot = 1; slot < candidate.nw; ++slot) {
        for (int row = 0; row < current_dim; ++row) {
            next_weight[row] = 0;
            for (int coord = 0; coord < candidate.ambient_vertices; ++coord) {
                next_weight[row] = device_walk_add(next_weight[row], device_walk_mul(candidate.weights[slot][coord], current[row][coord], &arithmetic_ok), &arithmetic_ok);
            }
        }
        int reduced_dim = 0;
        if (!device_solve_next_weight_equation(next_weight, current_dim, &reduced_dim, next_basis, &arithmetic_ok)) return 0;
        for (int row = 0; row < reduced_dim; ++row) {
            for (int coord = 0; coord < candidate.ambient_vertices; ++coord) {
                composed[row][coord] = 0;
                for (int inner = 0; inner < current_dim; ++inner) {
                    composed[row][coord] = device_walk_add(composed[row][coord], device_walk_mul(next_basis[row][inner], current[inner][coord], &arithmetic_ok), &arithmetic_ok);
                }
            }
        }
        current_dim = reduced_dim;
        for (int row = 0; row < current_dim; ++row) {
            for (int coord = 0; coord < candidate.ambient_vertices; ++coord) current[row][coord] = composed[row][coord];
        }
    }
    if (!arithmetic_ok || current_dim != 5) return 0;
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
    for (int coord = 0; coord < 5; ++coord) points[static_cast<unsigned long long>(index) * 5ULL + coord] = x[coord];
    *point_count = index + 1;
    return 1;
}


// LLL + Fincke-Pohst lattice-point walk. The kernel basis of a CWS is very
// skewed; an LLL-reduced basis and a box-propagation walk visit far fewer tree
// nodes. FP32 in the LLL only affects how well the basis is reduced: size
// reductions and swaps are integer row operations, so the lattice and the
// enumerated point set do not depend on rounding. The root box comes from an
// FP64 ellipsoid around the coordinate box, widened by an integer guard, and
// each emitted point is validated exactly. Reduced coefficients are passed
// directly to the IP checker; a rank-five kernel embedding preserves IP.
#define FP_NMAX 5
#define FP_GUARD 1            // integer slack on the FP64-derived root box
#define FP_NODE_CAP 4000000  // runaway guard -> ship to CPU (completeness-safe)

// Gram-Schmidt in the box-scaled metric <u,v> = sum_A u_A v_A * w_A (w_A=1/r_A^2).
__device__ int device_gso_f(const int b[FP_NMAX][10], int n, int N,
                             const float *w, float mu[FP_NMAX][FP_NMAX],
                             float *B2) {
    float bs[FP_NMAX][10];
    for (int i = 0; i < n; ++i) {
        for (int A = 0; A < N; ++A) bs[i][A] = static_cast<float>(b[i][A]);
        for (int j = 0; j < i; ++j) {
            float dot = 0.f, nj = B2[j];
            for (int A = 0; A < N; ++A)
                dot += static_cast<float>(b[i][A]) * bs[j][A] * w[A];
            if (!(nj > 0.f) || !isfinite(nj)) return 0;
            mu[i][j] = dot / nj;
            if (!isfinite(mu[i][j])) return 0;
            for (int A = 0; A < N; ++A) bs[i][A] -= mu[i][j] * bs[j][A];
        }
        float nn = 0.f;
        for (int A = 0; A < N; ++A) nn += bs[i][A] * bs[i][A] * w[A];
        if (!(nn > 0.f) || !isfinite(nn)) return 0;
        B2[i] = nn;
    }
    return 1;
}

// Floating point only chooses integer row operations. Every update is checked
// before narrowing to int32; reduced coordinates are used directly, so there
// is no potentially overflowing transform matrix U.
__device__ int device_lll_f(int b[FP_NMAX][10], int n, int N, const float *w, float delta) {
    float mu[FP_NMAX][FP_NMAX], B2[FP_NMAX];
    if (!device_gso_f(b, n, N, w, mu, B2)) return 0;
    int k = 1, guard = 0;
    while (k < n && guard++ < 2000) {
        for (int l = k - 1; l >= 0; --l) {
            if (fabsf(mu[k][l]) > 0.5f) {
                // This strict float bound is safely inside INT_MAX, avoiding
                // undefined float-to-int conversion on NaN/overflow.
                if (!isfinite(mu[k][l]) || fabsf(mu[k][l]) > 2147483000.f) return 0;
                int q = static_cast<int>(lrintf(mu[k][l]));
                if (q) {
                    for (int A = 0; A < N; ++A) {
                        const long long next = static_cast<long long>(b[k][A]) - static_cast<long long>(q) * b[l][A];
                        if (next < (-2147483647LL - 1) || next > 2147483647LL) return 0;
                        b[k][A] = static_cast<int>(next);
                    }
                    if (!device_gso_f(b, n, N, w, mu, B2)) return 0;
                }
            }
        }
        if (B2[k] >= (delta - mu[k][k - 1] * mu[k][k - 1]) * B2[k - 1]) {
            ++k;
        } else {
            for (int A = 0; A < N; ++A) { int t = b[k][A]; b[k][A] = b[k - 1][A]; b[k - 1][A] = t; }
            if (!device_gso_f(b, n, N, w, mu, B2)) return 0;
            k = (k - 1 > 1) ? k - 1 : 1;
        }
    }
    return 1;
}

// Gauss-Jordan solve G x = rhs (n<=5, SPD) in FP64, returning x and diag(G^-1);
// 0 if numerically singular. The root box is heuristic; exact valid/distinct
// outputs and equality with the join count are required for completeness.
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

// An independently checked mod-2 left inverse certifies that these five
// integer rows are independent over Q/R. A singular reduction modulo 2 merely
// defers; it never asserts rank deficiency of the integer matrix.
__device__ int device_walk_basis_certificate(const DeviceCwsCandidate &candidate,
                                             const int basis[FP_NMAX][10]) {
    const int N = candidate.ambient_vertices;
    int ok = 1;
    for (int r = 0; r < 5; ++r)
        for (int s = 0; s < candidate.nw; ++s) {
            long long dot = 0;
            for (int a = 0; a < N; ++a)
                dot = device_walk_add(dot, device_walk_mul(basis[r][a], candidate.weights[s][a], &ok), &ok);
            if (!ok || dot != 0) return 0;
        }
    unsigned int rows[5] = {}, combination[5] = {};
    int pivot[5] = {};
    for (int r = 0; r < 5; ++r) {
        combination[r] = 1u << r;
        for (int a = 0; a < N; ++a)
            if (static_cast<unsigned int>(basis[r][a]) & 1u) rows[r] |= 1u << a;
    }
    int rank = 0;
    for (int a = 0; a < N && rank < 5; ++a) {
        int r = rank;
        while (r < 5 && !((rows[r] >> a) & 1u)) ++r;
        if (r == 5) continue;
        unsigned int temp = rows[r]; rows[r] = rows[rank]; rows[rank] = temp;
        temp = combination[r]; combination[r] = combination[rank]; combination[rank] = temp;
        for (int j = 0; j < 5; ++j) if (j != rank && ((rows[j] >> a) & 1u)) {
            rows[j] ^= rows[rank]; combination[j] ^= combination[rank];
        }
        pivot[rank++] = a;
    }
    if (rank != 5) return 0;
    // Verify the witness against the original rows, not the elimination state.
    for (int r = 0; r < 5; ++r) for (int c = 0; c < 5; ++c) {
        unsigned int value = 0;
        for (int i = 0; i < 5; ++i)
            value ^= ((combination[r] >> i) & 1u) & (static_cast<unsigned int>(basis[i][pivot[c]]) & 1u);
        if (value != static_cast<unsigned int>(r == c)) return 0;
    }
    return 1;
}

// A conservative sufficient bound for every int32 product, partial sum,
// interval numerator and increment in the exact walk. No floating-point bound
// is trusted for arithmetic safety. A box that fails this check is deferred.
__device__ int device_walk_int32_box(const int basis[FP_NMAX][10], int N,
                                     const int Lo[FP_NMAX], const int Hi[FP_NMAX],
                                     const int xu[10]) {
    const long long budget = 2147483647LL / 8 - 2;
    long long magnitude[5];
    for (int j = 0; j < 5; ++j) {
        if (Lo[j] > Hi[j] || Lo[j] < -2147483646 || Hi[j] > 2147483646) return 0;
        long long l = Lo[j], h = Hi[j];
        l = l < 0 ? -l : l; h = h < 0 ? -h : h;
        magnitude[j] = l > h ? l : h;
    }
    for (int a = 0; a < N; ++a) {
        if (xu[a] < 0 || xu[a] > 2147483647 / 4) return 0;
        long long remaining = budget;
        for (int j = 0; j < 5; ++j) {
            long long coefficient = basis[j][a];
            if (coefficient < 0) coefficient = -coefficient;
            if (magnitude[j] && coefficient > remaining / magnitude[j]) return 0;
            remaining -= coefficient * magnitude[j];
        }
    }
    return 1;
}

__device__ void device_walk_swap_point(long long *points, int a, int b) {
    for (int c = 0; c < 5; ++c) {
        const unsigned long long ia = static_cast<unsigned long long>(a) * 5ULL + c;
        const unsigned long long ib = static_cast<unsigned long long>(b) * 5ULL + c;
        const long long value = points[ia]; points[ia] = points[ib]; points[ib] = value;
    }
}
__device__ void device_walk_sift_points(long long *points, int count, int root) {
    while (static_cast<long long>(root) * 2 + 1 < count) {
        int child = static_cast<int>(static_cast<long long>(root) * 2 + 1);
        if (child + 1 < count && device_vec_greater_than(points + 5ULL * (child + 1), points + 5ULL * child)) ++child;
        if (!device_vec_greater_than(points + 5ULL * child, points + 5ULL * root)) return;
        device_walk_swap_point(points, root, child);
        root = child;
    }
}

// Sort coefficient tuples, reject duplicates, then independently revalidate
// every stored tuple after sorting. This makes count equality useful even if
// the walk misses a root interval or its traversal is imperfect.
__device__ int device_walk_validate_points(const DeviceCwsCandidate &candidate,
                                           const int basis[FP_NMAX][10], const int xu[10],
                                           long long *points, int count, int *ip_possible = nullptr) {
    if (count < 0) return 0;
    unsigned int touch_zero = 0, touch_two = 0, pair_lo[10] = {}, pair_hi[10] = {};
    for (int start = count / 2; start > 0; --start) device_walk_sift_points(points, count, start - 1);
    for (int end = count - 1; end > 0; --end) {
        device_walk_swap_point(points, 0, end); device_walk_sift_points(points, end, 0);
    }
    for (int p = 0; p < count; ++p) {
        const long long *point = points + 5ULL * static_cast<unsigned int>(p);
        if (p && !device_vec_greater_than(point, point - 5)) return 0;
        int X[10], ok = 1;
        for (int a = 0; a < candidate.ambient_vertices; ++a) {
            long long value = 1;
            for (int j = 0; j < 5; ++j)
                value = device_walk_add(value, device_walk_mul(point[j], basis[j][a], &ok), &ok);
            if (!ok || value < 0 || value > xu[a]) return 0;
            X[a] = static_cast<int>(value);
        }
        for (int r = 0; r < candidate.nw; ++r) {
            long long remaining = candidate.degree[r];
            for (int a = 0; a < candidate.ambient_vertices; ++a) {
                const int weight = candidate.weights[r][a];
                if (weight < 0 || (weight && X[a] > remaining / weight)) return 0;
                remaining -= static_cast<long long>(weight) * X[a];
            }
            if (remaining != 0) return 0;
        }
        unsigned int m0 = 0, m1 = 0, m2 = 0, m3 = 0;
        for (int a = 0; a < candidate.ambient_vertices; ++a) {
            const unsigned int bit = 1u << a;
            if (X[a] == 0) m0 |= bit;
            else if (X[a] == 1) m1 |= bit;
            else if (X[a] == 2) m2 |= bit;
            else m3 |= bit;
        }
        touch_zero |= m0; touch_two |= m2 | m3;
        for (unsigned int mask = m0; mask; mask &= mask - 1) pair_lo[__ffs(mask) - 1] |= m0 | m1;
        for (unsigned int mask = m3; mask; mask &= mask - 1) pair_hi[__ffs(mask) - 1] = ~0u;
        for (unsigned int mask = m2; mask; mask &= mask - 1) pair_hi[__ffs(mask) - 1] |= m1 | m2 | m3;
    }
    if (ip_possible) {
        *ip_possible = (touch_zero ^ touch_two) == 0;
        for (int a = 0; a < candidate.ambient_vertices && *ip_possible; ++a)
            for (int b = a + 1; b < candidate.ambient_vertices; ++b) {
                const unsigned int lo = ((pair_lo[a] >> b) | (pair_lo[b] >> a)) & 1u;
                const unsigned int hi = ((pair_hi[a] >> b) | (pair_hi[b] >> a)) & 1u;
                if (lo ^ hi) { *ip_possible = 0; break; }
            }
    }
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
// A positive status certifies valid, distinct reduced-coordinate points and
// an injective rank-five kernel embedding. It does NOT certify completeness:
// callers must compare point_count to the exact join count before using
// ip_possible or sending this buffer to the IP checker. Any failed certificate
// or representational bound returns -1 for exact CPU completion.
__device__ int device_make_points_fp(const DeviceCwsCandidate &candidate,
                                     long long *points, int max_points,
                                     int *point_count, int *ip_possible) {
    *ip_possible = 1;
    *point_count = 0;
    if (max_points < 1) return -1;
    long long basis64[5][10];
    int basis_dim = 0;
    long long x_upper64[10] = {0};
    if (!device_candidate_basic_precheck(candidate, x_upper64)) return -1;
    const int nA = candidate.ambient_vertices;

    if (!device_make_cws_basis(candidate, &basis_dim, basis64)) return -1;
    const int n = basis_dim;                 // == 5 on the fast path
    int bred[FP_NMAX][10];
    int xu[10];
    for (int r = 0; r < n; ++r)
        for (int c = 0; c < nA; ++c) {
            if (basis64[r][c] < (-2147483647LL - 1) || basis64[r][c] > 2147483647LL) return -1;
            bred[r][c] = static_cast<int>(basis64[r][c]);
        }
    for (int c = 0; c < nA; ++c) xu[c] = static_cast<int>(x_upper64[c]);

    // Box-scaled metric weights w_A = 1/r_A^2, r_A = Xmax_A/2 (origin X0 == 1).
    float w[10], r[10];
    for (int A = 0; A < nA; ++A) {
        r[A] = (xu[A] > 0) ? (xu[A] * 0.5f) : 0.5f;
        w[A] = 1.f / (r[A] * r[A]);
    }
    if (!device_lll_f(bred, n, nA, w, 0.75f)) return -1;
    if (!device_walk_basis_certificate(candidate, bred)) return -1;   // delta 0.75: faster than 0.99, same points

    // Circumscribed ellipsoid sum_A ((X_A-cc_A)/r_A)^2 <= rho ⊇ box.
    // Gram G=M^T M, gv=M^T t, M[A][j]=bred[j][A]/r_A, t_A=(cc_A-X0_A)/r_A, cc_A=r_A.
    // This root-box computation runs in FP64 (~200 flops/candidate),
    // but no error bound is trusted: missed points force a join-count mismatch.
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
        const double low = floor(xhat[j] - hw), high = ceil(xhat[j] + hw);
        if (!isfinite(low) || !isfinite(high) || low < -2147483644.0 || high > 2147483644.0) return -1;
        Lo[j] = static_cast<int>(low) - FP_GUARD;
        Hi[j] = static_cast<int>(high) + FP_GUARD;
    }
    if (!device_walk_int32_box(bred, nA, Lo, Hi, xu)) return -1;
    // Enumeration order: longest GSO vector (scaled metric) outermost (FP convention).
    {
        int tmp[FP_NMAX][10];
        for (int i = 0; i < n; ++i) for (int A = 0; A < nA; ++A) tmp[i][A] = bred[i][A];
        float mu[FP_NMAX][FP_NMAX], B2[FP_NMAX];
        if (!device_gso_f(tmp, n, nA, w, mu, B2)) return -1;
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
    while (depth >= 0) {
        if (cur[depth] > hi_s[depth]) {                 // exhausted -> ascend
            --depth;
            if (depth >= 0) ++cur[depth];
            continue;
        }
        int wv = ord[depth];
        xprime[wv] = cur[depth];
        if (depth + 1 == n) {                           // leaf: in-box by construction
            for (int A = 0; A < nA; ++A) {
                const int X = accum[depth][A] + cur[depth] * cw[depth][A];
                if (X < 0 || X > xu[A]) return -1;
            }
            long long lp[5];
            for (int j = 0; j < 5; ++j) lp[j] = xprime[j];
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
    // Recompute all certificate masks from the checked final point buffer,
    // so the certificate and the cardinality check concern the same family.
    if (!device_walk_validate_points(candidate, bred, xu, points, *point_count, ip_possible)) return -1;
    return *point_count > 0 ? 1 : 0;
}

}  // namespace cwsgpu
