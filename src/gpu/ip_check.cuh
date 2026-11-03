// IP check of a complete point set (port of PALP's IP_Check, Vertex.c):
// grows a simplex into the hull in the supplied origin-centered coordinates.
// Lists hold 64 vertices / equations; when one would overflow the result is
// "incomplete" (reason 3) and the caller hands the CWS to the CPU.
// Reasons: 1 rank deficient and 4 hull failure, each with an independently
// checked nonzero supporting inequality with constant <= 0 (the only
// rejections); 2 initial incidence failure and 3 incomplete defer.
#pragma once

#include "lattice.cuh"

namespace cwsgpu {

struct DeviceEquation5 {
    long long a[5] = {0, 0, 0, 0, 0};
    long long c = 0;
};

struct DeviceCEqList5 {
    int ne = 0;
    DeviceEquation5 e[64];
};

struct DeviceEqList5 {
    int ne = 0;
    DeviceEquation5 e[64];
};

struct DeviceIpScratch {
    int arithmetic_failed = 0;
    DeviceEquation5 rejecting_equation;
    int vertices[64];
    DeviceCEqList5 candidate_equations;
    DeviceEqList5 facets;
    DeviceCEqList5 bad_equations;
    unsigned long long ceq_inci[64];
    unsigned long long facet_inci[64];
    unsigned long long bad_inci[64];
};



__device__ __forceinline__ long long device_ip_add(long long a, long long b, int *failed) {
    if (failed && ((b > 0 && a > 9223372036854775807LL - b) ||
                   (b < 0 && a < (-9223372036854775807LL - 1) - b))) {
        *failed = 1; return 0;
    }
    return a + b;
}

__device__ __forceinline__ long long device_ip_sub(long long a, long long b, int *failed) {
    if (failed && ((b < 0 && a > 9223372036854775807LL + b) ||
                   (b > 0 && a < (-9223372036854775807LL - 1) + b))) {
        *failed = 1; return 0;
    }
    return a - b;
}

__device__ __forceinline__ long long device_ip_mul(long long a, long long b, int *failed) {
    // The common small-operand case needs no integer division to check it.
    if (failed && a >= -2147483648LL && a <= 2147483647LL &&
                  b >= -2147483648LL && b <= 2147483647LL) return a * b;
    if (failed && ((a > 0 && ((b > 0 && a > 9223372036854775807LL / b) ||
                             (b < 0 && b < (-9223372036854775807LL - 1) / a))) ||
                   (a < 0 && ((b > 0 && a < (-9223372036854775807LL - 1) / b) ||
                             (b < 0 && a < 9223372036854775807LL / b))))) {
        *failed = 1; return 0;
    }
    return a * b;
}

__device__ __forceinline__ long long device_ip_div(long long a, long long b, int *failed) {
    if (failed && (b == 0 || (a == (-9223372036854775807LL - 1) && b == -1))) {
        *failed = 1; return 0;
    }
    return a / b;
}

__device__ __forceinline__ long long device_ip_rem(long long a, long long b, int *failed) {
    if (failed && (b == 0 || (a == (-9223372036854775807LL - 1) && b == -1))) {
        *failed = 1; return 0;
    }
    return a % b;
}

__device__ long long device_ip_abs_ll(long long value, int *arithmetic_failed = nullptr) {
    return value < 0 ? device_ip_sub(0, value, arithmetic_failed) : value;
}

__device__ long long device_ip_nngcd(long long left, long long right, int *arithmetic_failed = nullptr) {
    left = device_ip_abs_ll(left, arithmetic_failed);
    right = device_ip_abs_ll(right, arithmetic_failed);
    if (!right) return left;
    while ((left = device_ip_rem(left, right, arithmetic_failed))) {
        if (!(right = device_ip_rem(right, left, arithmetic_failed))) return left;
    }
    return right;
}

__device__ long long device_ip_egcd(long long a0, long long a1,
                                 long long *out0, long long *out1, int *arithmetic_failed = nullptr) {
    long long v0 = a0;
    long long v1 = a1;
    long long x0 = 1;
    long long x1 = 0;
    long long x2 = 0;
    long long a2 = 0;
    while ((a2 = device_ip_rem(a0, a1, arithmetic_failed))) {
        x2 = device_ip_sub(x0, device_ip_mul(x1, device_ip_div(a0, a1, arithmetic_failed), arithmetic_failed), arithmetic_failed);
        a0 = a1;
        a1 = a2;
        x0 = x1;
        x1 = x2;
    }
    *out0 = x1;
    *out1 = device_ip_div(device_ip_sub(a1, device_ip_mul(v0, x1, arithmetic_failed), arithmetic_failed), v1, arithmetic_failed);
    return a1;
}

__device__ long long device_ip_round_q(long long numerator, long long denominator, int *arithmetic_failed = nullptr) {
    if (denominator < 0) {
        denominator = device_ip_sub(0, denominator, arithmetic_failed);
        numerator = device_ip_sub(0, numerator, arithmetic_failed);
    }
    long long floor_value = device_ip_div(numerator, denominator, arithmetic_failed);
    return device_ip_add(floor_value, device_ip_div(device_ip_mul(2, device_ip_sub(numerator, device_ip_mul(floor_value, denominator, arithmetic_failed), arithmetic_failed), arithmetic_failed), denominator, arithmetic_failed), arithmetic_failed);
}

__device__ long long device_ip_w_to_glz(long long *weights, int dim,
                                     long long glz[10][10], int *arithmetic_failed = nullptr) {
    for (int row = 1; row < dim; ++row) {
        for (int col = 0; col < dim; ++col) glz[row][col] = 0;
    }
    long long *extended = glz[0];
    long long *base = glz[1];
    long long gcd = device_ip_egcd(weights[0], weights[1], &extended[0], &extended[1], arithmetic_failed);
    base[0] = device_ip_div(device_ip_sub(0, weights[1], arithmetic_failed), gcd, arithmetic_failed);
    base[1] = device_ip_div(weights[0], gcd, arithmetic_failed);
    for (int row = 2; row < dim; ++row) {
        long long egcd_left = 0;
        long long egcd_right = 0;
        long long next_gcd = device_ip_egcd(gcd, weights[row], &egcd_left, &egcd_right, arithmetic_failed);
        base = glz[row];
        base[row] = device_ip_div(gcd, next_gcd, arithmetic_failed);
        long long scale = device_ip_div(weights[row], next_gcd, arithmetic_failed);
        for (int col = 0; col < row; ++col) base[col] = device_ip_mul(device_ip_sub(0, extended[col], arithmetic_failed), scale, arithmetic_failed);
        for (int col = 0; col < row; ++col) extended[col] = device_ip_mul(extended[col], egcd_left, arithmetic_failed);
        extended[row] = egcd_right;
        for (int improve = row - 1; improve > 0; --improve) {
            long long *candidate = glz[improve];
            long long round_base = device_ip_round_q(base[improve], candidate[improve], arithmetic_failed);
            long long round_extended = device_ip_round_q(extended[improve], candidate[improve], arithmetic_failed);
            for (int col = 0; col <= improve; ++col) {
                base[col] = device_ip_sub(base[col], device_ip_mul(round_base, candidate[col], arithmetic_failed), arithmetic_failed);
                extended[col] = device_ip_sub(extended[col], device_ip_mul(round_extended, candidate[col], arithmetic_failed), arithmetic_failed);
            }
        }
        gcd = next_gcd;
    }
    return gcd;
}

__device__ long long device_eval_eq(const DeviceEquation5 &equation,
                                    const long long *point) {
    return equation.c + equation.a[0] * point[0] + equation.a[1] * point[1] +
           equation.a[2] * point[2] + equation.a[3] * point[3] + equation.a[4] * point[4];
}

// |c| <= 2^61, |a_i| <= 2^44 and |x_i| <= 2^15 keep every product within 2^59 and
// every partial sum of device_eval_eq within 2^61 + 5 * 2^59 < 2^63, so the raw
// evaluation is exact there (CudaIPBounded.lean) and needs no per-operation check.
__device__ int device_eval_bounded(const DeviceEquation5 &equation, const long long *point) {
    return equation.c >= -2305843009213693952LL && equation.c <= 2305843009213693952LL &&
           equation.a[0] >= -17592186044416LL && equation.a[0] <= 17592186044416LL &&
           equation.a[1] >= -17592186044416LL && equation.a[1] <= 17592186044416LL &&
           equation.a[2] >= -17592186044416LL && equation.a[2] <= 17592186044416LL &&
           equation.a[3] >= -17592186044416LL && equation.a[3] <= 17592186044416LL &&
           equation.a[4] >= -17592186044416LL && equation.a[4] <= 17592186044416LL &&
           point[0] >= -32768LL && point[0] <= 32768LL && point[1] >= -32768LL && point[1] <= 32768LL &&
           point[2] >= -32768LL && point[2] <= 32768LL && point[3] >= -32768LL && point[3] <= 32768LL &&
           point[4] >= -32768LL && point[4] <= 32768LL;
}

__device__ long long device_ip_eval_eq(const DeviceEquation5 &equation,
                                       const long long *point, int *arithmetic_failed) {
    if (!arithmetic_failed) return device_eval_eq(equation, point);
    if (device_eval_bounded(equation, point)) return device_eval_eq(equation, point);
    long long value = equation.c;
    for (int i = 0; i < 5; ++i)
        value = device_ip_add(value, device_ip_mul(equation.a[i], point[i], arithmetic_failed), arithmetic_failed);
    return value;
}

__device__ int device_inci_abs(unsigned long long value) {
    return __popcll(value);
}

__device__ unsigned long long device_eq_to_inci(const DeviceEquation5 &equation,
                                                const long long *points,
                                                const int vertices[64],
                                                int vertex_count, int *arithmetic_failed = nullptr) {
    unsigned long long incidence = 0;
    for (int index = 0; index < vertex_count; ++index) {
        const long long *point = points + vertices[index] * 5;
        incidence = 2ULL * incidence + (device_ip_eval_eq(equation, point, arithmetic_failed) == 0 ? 1ULL : 0ULL);
    }
    return incidence;
}

__device__ long long device_vz_to_base(long long *vector, int dim,
                                       long long matrix[5][5], int *arithmetic_failed = nullptr) {
    int positions[5];
    long long compact[5];
    long long *rows[5];
    long long glz[10][10];
    int nonzero = 0;
    long long gcd = 0;
    for (int row = 0; row < dim; ++row) {
        if (vector[row]) {
            compact[nonzero] = vector[row];
            positions[nonzero] = row;
            rows[nonzero] = matrix[row];
            ++nonzero;
        } else {
            for (int col = 0; col < dim; ++col) matrix[row][col] = (row == col);
        }
    }
    if (nonzero && positions[0]) {
        rows[0] = matrix[0];
        for (int col = 0; col < dim; ++col) matrix[positions[0]][col] = (col == 0);
    }
    if (nonzero > 1) {
        gcd = device_ip_w_to_glz(compact, nonzero, glz, arithmetic_failed);
        for (int row = 0; row < nonzero; ++row) {
            for (int col = 0; col < nonzero; ++col) rows[row][col] = glz[row][col];
        }
        for (int row = 0; row < nonzero; ++row) {
            int compact_col = nonzero;
            for (int col = dim - 1; col >= 0; --col) rows[row][col] = vector[col] ? rows[row][--compact_col] : 0;
        }
    } else if (nonzero) {
        gcd = compact[0];
        matrix[0][0] = 0;
        matrix[0][positions[0]] = 1;
    }
    return gcd;
}

__device__ int device_orthbase_red_by_v(long long *vector, int dim,
                                        long long a[5][5], int *rank,
                                        long long b[5][5], int *arithmetic_failed = nullptr) {
    long long projected[5];
    long long glz_base[5][5];
    for (int row = 0; row < *rank; ++row) {
        projected[row] = 0;
        for (int col = 0; col < dim; ++col) projected[row] = device_ip_add(projected[row], device_ip_mul(a[row][col], vector[col], arithmetic_failed), arithmetic_failed);
    }
    if (!device_vz_to_base(projected, *rank, glz_base, arithmetic_failed)) return 0;
    for (int row = 0; row < *rank - 1; ++row) {
        for (int col = 0; col < dim; ++col) {
            b[row][col] = 0;
            for (int inner = 0; inner < *rank; ++inner) b[row][col] = device_ip_add(b[row][col], device_ip_mul(glz_base[row + 1][inner], a[inner][col], arithmetic_failed), arithmetic_failed);
        }
    }
    return (*rank)--;
}

__device__ int device_new_start_vertex(const long long *origin_vertex,
                                       long long *equation_a,
                                       const long long *points,
                                       int point_count,
                                       int *vertex_index, int *arithmetic_failed = nullptr) {
    DeviceEquation5 equation;
    for (int coord = 0; coord < 5; ++coord) equation.a[coord] = equation_a[coord];
    equation.c = device_ip_sub(0, device_ip_eval_eq(equation, origin_vertex, arithmetic_failed), arithmetic_failed);
    long long value = device_ip_eval_eq(equation, points, arithmetic_failed);
    long long positive = value > 0 ? value : 0;
    long long negative = value < 0 ? value : 0;
    int positive_index = 0;
    int negative_index = 0;
    for (int index = 1; index < point_count; ++index) {
        const long long *point = points + index * 5;
        value = device_ip_eval_eq(equation, point, arithmetic_failed);
        if (value == 0) continue;
        if (value == positive && device_vec_greater_than(point, points + positive_index * 5)) positive_index = index;
        if (value > positive) { positive = value; positive_index = index; }
        if (value == negative && device_vec_greater_than(point, points + negative_index * 5)) negative_index = index;
        if (value < negative) { negative = value; negative_index = index; }
    }
    if (positive) {
        if (negative) *vertex_index = (device_ip_add(positive, negative, arithmetic_failed) > 0) ? negative_index : positive_index;
        else *vertex_index = positive_index;
    } else if (negative) {
        *vertex_index = negative_index;
    } else {
        return 0;
    }
    return 1;
}

__device__ int device_glz_start_simplex(const long long *points,
                                        int point_count,
                                        int vertices[64],
                                        int *vertex_count,
                                        DeviceCEqList5 *candidate_equations, int *arithmetic_failed = nullptr) {
    if (point_count < 2) return 1;
    int min_index = 0;
    int max_index = 0;
    for (int index = 1; index < point_count; ++index) {
        if (device_vec_greater_than(points + min_index * 5, points + index * 5)) min_index = index;
        if (device_vec_greater_than(points + index * 5, points + max_index * 5)) max_index = index;
    }
    if (min_index == max_index) return 1;
    long long min_norm = 0;
    long long max_norm = 0;
    for (int coord = 0; coord < 5; ++coord) {
        long long min_coord_norm = device_ip_abs_ll(points[min_index * 5 + coord], arithmetic_failed);
        long long max_coord_norm = device_ip_abs_ll(points[max_index * 5 + coord], arithmetic_failed);
        min_norm = min_norm > min_coord_norm ? min_norm : min_coord_norm;
        max_norm = max_norm > max_coord_norm ? max_norm : max_coord_norm;
    }
    if (max_norm < min_norm) { vertices[0] = max_index; vertices[1] = min_index; }
    else { vertices[0] = min_index; vertices[1] = max_index; }
    *vertex_count = 2;
    const long long *base_vertex = points + vertices[0] * 5;
    int b_offset[5];
    long long basis_store[15][5];
    long long work_vector[5];
    int rank = 5;
    for (int index = 0; index < 5; ++index) b_offset[index] = (index * (2 * 5 - index + 1)) / 2;
    for (int row = 0; row < 5; ++row) for (int col = 0; col < 5; ++col) basis_store[row][col] = (row == col);
    int y = vertices[1];
    int depth = 0;
    for (depth = 1; depth < 5; ++depth) {
        for (int coord = 0; coord < 5; ++coord) work_vector[coord] = device_ip_sub(points[y * 5 + coord], base_vertex[coord], arithmetic_failed);
        long long (*previous)[5] = &basis_store[b_offset[depth - 1]];
        long long (*next)[5] = &basis_store[b_offset[depth]];
        if (!device_orthbase_red_by_v(work_vector, 5, previous, &rank, next, arithmetic_failed)) break;
        int found = 0;
        for (int row = 0; row < rank; ++row) {
            if (device_new_start_vertex(base_vertex, next[row], points, point_count, &y, arithmetic_failed)) { found = 1; break; }
        }
        if (!found) break;
        vertices[(*vertex_count)++] = y;
    }
    if (depth < 5) {
        candidate_equations->ne = rank;
        for (int eq = 0; eq < rank; ++eq) {
            DeviceEquation5 *out = &candidate_equations->e[eq];
            out->c = 0;
            for (int coord = 0; coord < 5; ++coord) out->a[coord] = basis_store[b_offset[depth] + eq][coord];
            out->c = device_ip_sub(0, device_ip_eval_eq(*out, base_vertex, arithmetic_failed), arithmetic_failed);
        }
        return rank;
    }
    candidate_equations->ne = 6;
    long long *z = basis_store[b_offset[4]];
    DeviceEquation5 *equation = &candidate_equations->e[0];
    equation->c = 0;
    for (int coord = 0; coord < 5; ++coord) equation->a[coord] = z[coord];
    equation->c = device_ip_sub(0, device_ip_eval_eq(*equation, base_vertex, arithmetic_failed), arithmetic_failed);
    if (device_ip_eval_eq(*equation, points + vertices[5] * 5, arithmetic_failed) < 0) {
        for (int coord = 0; coord < 5; ++coord) equation->a[coord] = device_ip_sub(0, equation->a[coord], arithmetic_failed);
        equation->c = device_ip_sub(0, equation->c, arithmetic_failed);
    }
    const long long *opposite = points + vertices[5] * 5;
    rank = 5;
    for (int face = 1; face < 5; ++face) {
        const long long *other = points + vertices[face - 1] * 5;
        for (int coord = 0; coord < 5; ++coord) work_vector[coord] = device_ip_sub(opposite[coord], other[coord], arithmetic_failed);
        long long (*previous)[5] = &basis_store[b_offset[face - 1]];
        long long (*next)[5] = &basis_store[b_offset[face]];
        device_orthbase_red_by_v(work_vector, 5, previous, &rank, next, arithmetic_failed);
    }
    equation = &candidate_equations->e[1];
    equation->c = 0;
    for (int coord = 0; coord < 5; ++coord) equation->a[coord] = z[coord];
    equation->c = device_ip_sub(0, device_ip_eval_eq(*equation, opposite, arithmetic_failed), arithmetic_failed);
    if (device_ip_eval_eq(*equation, points + vertices[4] * 5, arithmetic_failed) < 0) {
        for (int coord = 0; coord < 5; ++coord) equation->a[coord] = device_ip_sub(0, equation->a[coord], arithmetic_failed);
        equation->c = device_ip_sub(0, equation->c, arithmetic_failed);
    }
    int eq_count = 2;
    for (int omitted = 3; omitted >= 0; --omitted) {
        rank = 5 - omitted;
        for (int face = omitted + 1; face < 5; ++face) {
            const long long *other = points + vertices[face] * 5;
            for (int coord = 0; coord < 5; ++coord) work_vector[coord] = device_ip_sub(opposite[coord], other[coord], arithmetic_failed);
            long long (*previous)[5] = &basis_store[b_offset[face - 1]];
            long long (*next)[5] = &basis_store[b_offset[face]];
            device_orthbase_red_by_v(work_vector, 5, previous, &rank, next, arithmetic_failed);
        }
        equation = &candidate_equations->e[eq_count++];
        equation->c = 0;
        for (int coord = 0; coord < 5; ++coord) equation->a[coord] = z[coord];
        equation->c = device_ip_sub(0, device_ip_eval_eq(*equation, opposite, arithmetic_failed), arithmetic_failed);
        if (device_ip_eval_eq(*equation, points + vertices[omitted] * 5, arithmetic_failed) < 0) {
            for (int coord = 0; coord < 5; ++coord) equation->a[coord] = device_ip_sub(0, equation->a[coord], arithmetic_failed);
            equation->c = device_ip_sub(0, equation->c, arithmetic_failed);
        }
    }
    return 0;
}

__device__ DeviceEquation5 device_eev_to_equation(const DeviceEquation5 &left,
                                                  const DeviceEquation5 &right,
                                                  const long long *vertex, int *arithmetic_failed = nullptr) {
    long long l = device_ip_eval_eq(right, vertex, arithmetic_failed);
    long long m = device_ip_eval_eq(left, vertex, arithmetic_failed);
    long long gcd = device_ip_nngcd(l, m, arithmetic_failed);
    DeviceEquation5 equation;
    if (!gcd) return equation;
    l = device_ip_div(l, gcd, arithmetic_failed);
    m = device_ip_div(m, gcd, arithmetic_failed);
    long long all_gcd = equation.c = device_ip_sub(device_ip_mul(l, left.c, arithmetic_failed), device_ip_mul(m, right.c, arithmetic_failed), arithmetic_failed);
    for (int coord = 0; coord < 5; ++coord) {
        equation.a[coord] = device_ip_sub(device_ip_mul(l, left.a[coord], arithmetic_failed), device_ip_mul(m, right.a[coord], arithmetic_failed), arithmetic_failed);
        all_gcd = device_ip_nngcd(all_gcd, equation.a[coord], arithmetic_failed);
    }
    if (all_gcd && all_gcd != 1) {
        equation.c = device_ip_div(equation.c, all_gcd, arithmetic_failed);
        for (int coord = 0; coord < 5; ++coord) equation.a[coord] = device_ip_div(equation.a[coord], all_gcd, arithmetic_failed);
    }
    return equation;
}

__device__ int device_is_good_ceq(DeviceEquation5 *equation,
                                  const long long *points,
                                  const int vertices[64],
                                  int vertex_count, int *arithmetic_failed = nullptr) {
    int index = vertex_count;
    long long sign = 0;
    while (index > 0 && !(sign = device_ip_eval_eq(*equation, points + vertices[--index] * 5, arithmetic_failed))) {}
    if (sign < 0) {
        for (int coord = 0; coord < 5; ++coord) equation->a[coord] = device_ip_sub(0, equation->a[coord], arithmetic_failed);
        equation->c = device_ip_sub(0, equation->c, arithmetic_failed);
    }
    while (index > 0) {
        if (device_ip_eval_eq(*equation, points + vertices[--index] * 5, arithmetic_failed) < 0) return 0;
    }
    return 1;
}

__device__ int device_search_new_vertex(const DeviceEquation5 &equation,
                                        const long long *points,
                                        int point_count, int *arithmetic_failed = nullptr) {
    int vertex = 0;
    long long best = device_ip_eval_eq(equation, points, arithmetic_failed);
    for (int index = 1; index < point_count; ++index) {
        const long long *point = points + index * 5;
        long long value = device_ip_eval_eq(equation, point, arithmetic_failed);
        if (value > best) continue;
        if (value == best && device_vec_greater_than(points + vertex * 5, point)) continue;
        vertex = index;
        best = value;
    }
    return vertex;
}

__device__ void device_make_new_ceqs(const long long *points,
                                     const int vertices[64],
                                     int vertex_count,
                                     DeviceCEqList5 *candidate_equations,
                                     DeviceEqList5 *facets,
                                     unsigned long long ceq_inci[64],
                                     unsigned long long facet_inci[64],
                                     DeviceCEqList5 *bad,
                                     unsigned long long bad_inci[64],
                                     int *truncated = nullptr, int *arithmetic_failed = nullptr) {
    // Exp H26: the lists hold 64 equations; a full list used to drop candidate
    // equations silently. Report it so the caller can hand the CWS to the CPU.
    int old_count = candidate_equations->ne;
    candidate_equations->ne = 0;
    bad->ne = 0;
    for (int index = 0; index < old_count; ++index) {
        long long dist = device_ip_eval_eq(candidate_equations->e[index], points + vertices[vertex_count - 1] * 5, arithmetic_failed);
        ceq_inci[index] = 2ULL * ceq_inci[index] + (dist == 0 ? 1ULL : 0ULL);
        if (dist < 0) {
            bad->e[bad->ne] = candidate_equations->e[index];
            bad_inci[bad->ne++] = ceq_inci[index];
        } else {
            ceq_inci[candidate_equations->ne] = ceq_inci[index];
            candidate_equations->e[candidate_equations->ne++] = candidate_equations->e[index];
        }
    }
    old_count = candidate_equations->ne;
    for (int index = 0; index < facets->ne; ++index) {
        long long dist = device_ip_eval_eq(facets->e[index], points + vertices[vertex_count - 1] * 5, arithmetic_failed);
        facet_inci[index] = 2ULL * facet_inci[index] + (dist == 0 ? 1ULL : 0ULL);
    }
    for (int facet = 0; facet < facets->ne; ++facet) {
        if ((facet_inci[facet] & 1ULL) != 0) continue;
        for (int bad_index = 0; bad_index < bad->ne; ++bad_index) {
            unsigned long long new_face = bad_inci[bad_index] & facet_inci[facet];
            if (device_inci_abs(new_face) < 4) continue;
            int skip = 0;
            for (int k = 0; k < bad->ne; ++k) if (((new_face | bad_inci[k]) == bad_inci[k]) && k != bad_index) { skip = 1; break; }
            if (skip) continue;
            for (int k = 0; k < old_count; ++k) if ((new_face | ceq_inci[k]) == ceq_inci[k]) { skip = 1; break; }
            if (skip) continue;
            for (int k = 0; k < facets->ne; ++k) if (((new_face | facet_inci[k]) == facet_inci[k]) && k != facet) { skip = 1; break; }
            if (skip) continue;
            if (candidate_equations->ne >= 64) { if (truncated) *truncated = 1; continue; }
            ceq_inci[candidate_equations->ne] = new_face | 1ULL;
            DeviceEquation5 equation = device_eev_to_equation(bad->e[bad_index], facets->e[facet], points + vertices[vertex_count - 1] * 5, arithmetic_failed);
            if (device_is_good_ceq(&equation, points, vertices, vertex_count, arithmetic_failed)) candidate_equations->e[candidate_equations->ne++] = equation;
        }
    }
    for (int old = 0; old < old_count; ++old) {
        if ((ceq_inci[old] & 1ULL) != 0) continue;
        for (int bad_index = bad->ne - 1; bad_index >= 0; --bad_index) {
            unsigned long long new_face = bad_inci[bad_index] & ceq_inci[old];
            if (device_inci_abs(new_face) < 4) continue;
            int skip = 0;
            for (int k = 0; k < bad->ne; ++k) if (((new_face | bad_inci[k]) == bad_inci[k]) && k != bad_index) { skip = 1; break; }
            if (skip) continue;
            for (int k = 0; k < old_count; ++k) if (((new_face | ceq_inci[k]) == ceq_inci[k]) && k != old) { skip = 1; break; }
            if (skip) continue;
            for (int k = 0; k < facets->ne; ++k) if ((new_face | facet_inci[k]) == facet_inci[k]) { skip = 1; break; }
            if (skip) continue;
            if (candidate_equations->ne >= 64) { if (truncated) *truncated = 1; continue; }
            ceq_inci[candidate_equations->ne] = new_face | 1ULL;
            DeviceEquation5 equation = device_eev_to_equation(bad->e[bad_index], candidate_equations->e[old], points + vertices[vertex_count - 1] * 5, arithmetic_failed);
            if (device_is_good_ceq(&equation, points, vertices, vertex_count, arithmetic_failed)) candidate_equations->e[candidate_equations->ne++] = equation;
        }
    }
}

__device__ int device_equation_has_normal(const DeviceEquation5 &equation) {
    for (int i = 0; i < 5; ++i) if (equation.a[i] != 0) return 1;
    return 0;
}

__device__ int device_ip_search_bad_eq(DeviceCEqList5 *candidate_equations,
                                       DeviceEqList5 *facets,
                                       unsigned long long ceq_inci[64],
                                       unsigned long long facet_inci[64],
                                       const long long *points,
                                       int point_count,
                                       int *ip,
                                       int *truncated = nullptr, int *arithmetic_failed = nullptr) {
    while (candidate_equations->ne--) {
        int eq_index = candidate_equations->ne;
        int bad = 0;
        for (int point_index = 0; point_index < point_count; ++point_index) {
            if (device_ip_eval_eq(candidate_equations->e[eq_index], points + point_index * 5, arithmetic_failed) < 0) { bad = 1; break; }
        }
        if (bad) return ++candidate_equations->ne;
        if (candidate_equations->e[eq_index].c < 1) {
            // A zero normal is not a separating-hyperplane certificate.
            if (!device_equation_has_normal(candidate_equations->e[eq_index])) {
                if (truncated) *truncated = 1;
                return 0;
            }
            *ip = 0;
            return 1;
        }
        if (facets->ne < 64) {
            facets->e[facets->ne] = candidate_equations->e[eq_index];
            facet_inci[facets->ne++] = ceq_inci[eq_index];
        } else if (truncated) {
            *truncated = 1;
        }
    }
    return 0;
}

// Independent rejection gate: a hull-construction failure is never enough.
// The exact checked equation must have a nonzero normal, constant <= 0, and
// contain the entire supplied point set on its nonnegative side.
__device__ int device_ip_supporting_certificate(const DeviceEquation5 &equation,
                                                 const long long *points,
                                                 int point_count,
                                                 int *arithmetic_failed) {
    if (point_count < 1 || equation.c > 0 || !device_equation_has_normal(equation)) return 0;
    for (int i = 0; i < point_count; ++i)
        if (device_ip_eval_eq(equation, points + i * 5, arithmetic_failed) < 0) return 0;
    return !*arithmetic_failed;
}

__device__ int device_ip_check(const long long *points,
                               int point_count,
                               DeviceIpScratch *scratch,
                               int *reject_reason) {
    *reject_reason = 3;
    scratch->arithmetic_failed = 0;
    int *arithmetic_failed = &scratch->arithmetic_failed;
    if (point_count < 1 || point_count > 2147483647 / 5) return 0;
    int *vertices = scratch->vertices;
    int vertex_count = 0;
    DeviceCEqList5 *candidate_equations = &scratch->candidate_equations;
    DeviceEqList5 *facets = &scratch->facets;
    unsigned long long *ceq_inci = scratch->ceq_inci;
    unsigned long long *facet_inci = scratch->facet_inci;
    const int start_status = device_glz_start_simplex(points, point_count, vertices,
                                                    &vertex_count, candidate_equations, arithmetic_failed);
    if (*arithmetic_failed) return 0;
    if (start_status) {
        // Rank deficient: the points span a proper affine subspace, whose normals
        // are the proposed equations. One passing the independent gate (with the
        // origin among the points its constant is 0) rejects; otherwise defer.
        for (int index = 0; index < start_status && index < candidate_equations->ne && index < 64; ++index) {
            const DeviceEquation5 equation = candidate_equations->e[index];
            if (device_ip_supporting_certificate(equation, points, point_count, arithmetic_failed)) {
                scratch->rejecting_equation = equation;
                *reject_reason = 1;
                return 0;
            }
            if (*arithmetic_failed) return 0;
        }
        return 0;
    }
    for (int index = 0; index < candidate_equations->ne; ++index) {
        ceq_inci[index] = device_eq_to_inci(candidate_equations->e[index], points,
                                          vertices, vertex_count, arithmetic_failed);
        if (*arithmetic_failed) return 0;
        if (device_inci_abs(ceq_inci[index]) < 5) { *reject_reason = 2; return 0; }
    }
    facets->ne = 0;
    int ip = 1;
    int truncated = 0;
    while (candidate_equations->ne >= 0) {
        const int found_bad = device_ip_search_bad_eq(candidate_equations, facets,
            ceq_inci, facet_inci, points, point_count, &ip, &truncated, arithmetic_failed);
        if (truncated || *arithmetic_failed) return 0;
        if (found_bad) {
            if (!ip) {
                const int index = candidate_equations->ne;
                if (index < 0 || index >= 64) return 0;
                const DeviceEquation5 equation = candidate_equations->e[index];
                if (!device_ip_supporting_certificate(equation, points, point_count, arithmetic_failed)) return 0;
                scratch->rejecting_equation = equation;
                *reject_reason = 4;
                return 0;
            }
            if (vertex_count >= 64) return 0;
            vertices[vertex_count++] = device_search_new_vertex(
                candidate_equations->e[candidate_equations->ne - 1], points, point_count, arithmetic_failed);
            if (*arithmetic_failed) return 0;
            device_make_new_ceqs(points, vertices, vertex_count, candidate_equations,
                facets, ceq_inci, facet_inci, &scratch->bad_equations, scratch->bad_inci,
                &truncated, arithmetic_failed);
            if (truncated || *arithmetic_failed) return 0;
        }
    }
    if (truncated || *arithmetic_failed) return 0;
    return 1;
}

}  // namespace cwsgpu
