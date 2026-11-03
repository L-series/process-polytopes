// IP check of a complete point set (port of PALP's IP_Check, Vertex.c):
// grows a simplex into the hull and reports whether (1,...,1) is interior.
// Lists hold 64 vertices / equations; when one would overflow the result is
// "incomplete" (reason 3) and the caller hands the CWS to the CPU.
// Reasons: 1 rank deficient, 2 initial incidence failure, 3 incomplete,
// 4 a valid inequality with constant < 1 (not IP).
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
    int vertices[64];
    DeviceCEqList5 candidate_equations;
    DeviceEqList5 facets;
    DeviceCEqList5 bad_equations;
    unsigned long long ceq_inci[64];
    unsigned long long facet_inci[64];
    unsigned long long bad_inci[64];
};


__device__ long long device_eval_eq(const DeviceEquation5 &equation,
                                    const long long *point) {
    return equation.c + equation.a[0] * point[0] + equation.a[1] * point[1] +
           equation.a[2] * point[2] + equation.a[3] * point[3] + equation.a[4] * point[4];
}


__device__ int device_inci_abs(unsigned long long value) {
    return __popcll(value);
}

__device__ unsigned long long device_eq_to_inci(const DeviceEquation5 &equation,
                                                const long long *points,
                                                const int vertices[64],
                                                int vertex_count) {
    unsigned long long incidence = 0;
    for (int index = 0; index < vertex_count; ++index) {
        const long long *point = points + vertices[index] * 5;
        incidence = 2ULL * incidence + (device_eval_eq(equation, point) == 0 ? 1ULL : 0ULL);
    }
    return incidence;
}

__device__ long long device_vz_to_base(long long *vector, int dim,
                                       long long matrix[5][5]) {
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
        gcd = device_w_to_glz(compact, nonzero, glz);
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
                                        long long b[5][5]) {
    long long projected[5];
    long long glz_base[5][5];
    for (int row = 0; row < *rank; ++row) {
        projected[row] = 0;
        for (int col = 0; col < dim; ++col) projected[row] += a[row][col] * vector[col];
    }
    if (!device_vz_to_base(projected, *rank, glz_base)) return 0;
    for (int row = 0; row < *rank - 1; ++row) {
        for (int col = 0; col < dim; ++col) {
            b[row][col] = 0;
            for (int inner = 0; inner < *rank; ++inner) b[row][col] += glz_base[row + 1][inner] * a[inner][col];
        }
    }
    return (*rank)--;
}

__device__ int device_new_start_vertex(const long long *origin_vertex,
                                       long long *equation_a,
                                       const long long *points,
                                       int point_count,
                                       int *vertex_index) {
    DeviceEquation5 equation;
    for (int coord = 0; coord < 5; ++coord) equation.a[coord] = equation_a[coord];
    equation.c = -device_eval_eq(equation, origin_vertex);
    long long value = device_eval_eq(equation, points);
    long long positive = value > 0 ? value : 0;
    long long negative = value < 0 ? value : 0;
    int positive_index = 0;
    int negative_index = 0;
    for (int index = 1; index < point_count; ++index) {
        const long long *point = points + index * 5;
        value = device_eval_eq(equation, point);
        if (value == 0) continue;
        if (value == positive && device_vec_greater_than(point, points + positive_index * 5)) positive_index = index;
        if (value > positive) { positive = value; positive_index = index; }
        if (value == negative && device_vec_greater_than(point, points + negative_index * 5)) negative_index = index;
        if (value < negative) { negative = value; negative_index = index; }
    }
    if (positive) {
        if (negative) *vertex_index = (positive + negative > 0) ? negative_index : positive_index;
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
                                        DeviceCEqList5 *candidate_equations) {
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
        long long min_coord_norm = device_abs_ll(points[min_index * 5 + coord]);
        long long max_coord_norm = device_abs_ll(points[max_index * 5 + coord]);
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
        for (int coord = 0; coord < 5; ++coord) work_vector[coord] = points[y * 5 + coord] - base_vertex[coord];
        long long (*previous)[5] = &basis_store[b_offset[depth - 1]];
        long long (*next)[5] = &basis_store[b_offset[depth]];
        if (!device_orthbase_red_by_v(work_vector, 5, previous, &rank, next)) break;
        int found = 0;
        for (int row = 0; row < rank; ++row) {
            if (device_new_start_vertex(base_vertex, next[row], points, point_count, &y)) { found = 1; break; }
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
            out->c = -device_eval_eq(*out, base_vertex);
        }
        return rank;
    }
    candidate_equations->ne = 6;
    long long *z = basis_store[b_offset[4]];
    DeviceEquation5 *equation = &candidate_equations->e[0];
    equation->c = 0;
    for (int coord = 0; coord < 5; ++coord) equation->a[coord] = z[coord];
    equation->c = -device_eval_eq(*equation, base_vertex);
    if (device_eval_eq(*equation, points + vertices[5] * 5) < 0) {
        for (int coord = 0; coord < 5; ++coord) equation->a[coord] = -equation->a[coord];
        equation->c = -equation->c;
    }
    const long long *opposite = points + vertices[5] * 5;
    rank = 5;
    for (int face = 1; face < 5; ++face) {
        const long long *other = points + vertices[face - 1] * 5;
        for (int coord = 0; coord < 5; ++coord) work_vector[coord] = opposite[coord] - other[coord];
        long long (*previous)[5] = &basis_store[b_offset[face - 1]];
        long long (*next)[5] = &basis_store[b_offset[face]];
        device_orthbase_red_by_v(work_vector, 5, previous, &rank, next);
    }
    equation = &candidate_equations->e[1];
    equation->c = 0;
    for (int coord = 0; coord < 5; ++coord) equation->a[coord] = z[coord];
    equation->c = -device_eval_eq(*equation, opposite);
    if (device_eval_eq(*equation, points + vertices[4] * 5) < 0) {
        for (int coord = 0; coord < 5; ++coord) equation->a[coord] = -equation->a[coord];
        equation->c = -equation->c;
    }
    int eq_count = 2;
    for (int omitted = 3; omitted >= 0; --omitted) {
        rank = 5 - omitted;
        for (int face = omitted + 1; face < 5; ++face) {
            const long long *other = points + vertices[face] * 5;
            for (int coord = 0; coord < 5; ++coord) work_vector[coord] = opposite[coord] - other[coord];
            long long (*previous)[5] = &basis_store[b_offset[face - 1]];
            long long (*next)[5] = &basis_store[b_offset[face]];
            device_orthbase_red_by_v(work_vector, 5, previous, &rank, next);
        }
        equation = &candidate_equations->e[eq_count++];
        equation->c = 0;
        for (int coord = 0; coord < 5; ++coord) equation->a[coord] = z[coord];
        equation->c = -device_eval_eq(*equation, opposite);
        if (device_eval_eq(*equation, points + vertices[omitted] * 5) < 0) {
            for (int coord = 0; coord < 5; ++coord) equation->a[coord] = -equation->a[coord];
            equation->c = -equation->c;
        }
    }
    return 0;
}

__device__ DeviceEquation5 device_eev_to_equation(const DeviceEquation5 &left,
                                                  const DeviceEquation5 &right,
                                                  const long long *vertex) {
    long long l = device_eval_eq(right, vertex);
    long long m = device_eval_eq(left, vertex);
    long long gcd = device_nngcd(l, m);
    DeviceEquation5 equation;
    if (!gcd) return equation;
    l /= gcd;
    m /= gcd;
    long long all_gcd = equation.c = l * left.c - m * right.c;
    for (int coord = 0; coord < 5; ++coord) {
        equation.a[coord] = l * left.a[coord] - m * right.a[coord];
        all_gcd = device_nngcd(all_gcd, equation.a[coord]);
    }
    if (all_gcd && all_gcd != 1) {
        equation.c /= all_gcd;
        for (int coord = 0; coord < 5; ++coord) equation.a[coord] /= all_gcd;
    }
    return equation;
}

__device__ int device_is_good_ceq(DeviceEquation5 *equation,
                                  const long long *points,
                                  const int vertices[64],
                                  int vertex_count) {
    int index = vertex_count;
    long long sign = 0;
    while (index > 0 && !(sign = device_eval_eq(*equation, points + vertices[--index] * 5))) {}
    if (sign < 0) {
        for (int coord = 0; coord < 5; ++coord) equation->a[coord] = -equation->a[coord];
        equation->c = -equation->c;
    }
    while (index > 0) {
        if (device_eval_eq(*equation, points + vertices[--index] * 5) < 0) return 0;
    }
    return 1;
}

__device__ int device_search_new_vertex(const DeviceEquation5 &equation,
                                        const long long *points,
                                        int point_count) {
    int vertex = 0;
    long long best = device_eval_eq(equation, points);
    for (int index = 1; index < point_count; ++index) {
        const long long *point = points + index * 5;
        long long value = device_eval_eq(equation, point);
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
                                     int *truncated) {
    // The lists hold 64 equations; a full list reports truncation instead of
    // dropping candidate equations, so the caller can hand the CWS to the CPU.
    int old_count = candidate_equations->ne;
    candidate_equations->ne = 0;
    bad->ne = 0;
    for (int index = 0; index < old_count; ++index) {
        long long dist = device_eval_eq(candidate_equations->e[index], points + vertices[vertex_count - 1] * 5);
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
        long long dist = device_eval_eq(facets->e[index], points + vertices[vertex_count - 1] * 5);
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
            if (candidate_equations->ne >= 64) { *truncated = 1; continue; }
            ceq_inci[candidate_equations->ne] = new_face | 1ULL;
            DeviceEquation5 equation = device_eev_to_equation(bad->e[bad_index], facets->e[facet], points + vertices[vertex_count - 1] * 5);
            if (device_is_good_ceq(&equation, points, vertices, vertex_count)) candidate_equations->e[candidate_equations->ne++] = equation;
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
            if (candidate_equations->ne >= 64) { *truncated = 1; continue; }
            ceq_inci[candidate_equations->ne] = new_face | 1ULL;
            DeviceEquation5 equation = device_eev_to_equation(bad->e[bad_index], candidate_equations->e[old], points + vertices[vertex_count - 1] * 5);
            if (device_is_good_ceq(&equation, points, vertices, vertex_count)) candidate_equations->e[candidate_equations->ne++] = equation;
        }
    }
}

__device__ int device_ip_search_bad_eq(DeviceCEqList5 *candidate_equations,
                                       DeviceEqList5 *facets,
                                       unsigned long long ceq_inci[64],
                                       unsigned long long facet_inci[64],
                                       const long long *points,
                                       int point_count,
                                       int *ip,
                                       int *truncated) {
    while (candidate_equations->ne--) {
        int eq_index = candidate_equations->ne;
        int bad = 0;
        for (int point_index = 0; point_index < point_count; ++point_index) {
            if (device_eval_eq(candidate_equations->e[eq_index], points + point_index * 5) < 0) { bad = 1; break; }
        }
        if (bad) return ++candidate_equations->ne;
        if (candidate_equations->e[eq_index].c < 1) {
            *ip = 0;
            return 1;
        }
        if (facets->ne < 64) {
            facets->e[facets->ne] = candidate_equations->e[eq_index];
            facet_inci[facets->ne++] = ceq_inci[eq_index];
        } else {
            *truncated = 1;
        }
    }
    return 0;
}


__device__ int device_ip_check(const long long *points,
                               int point_count,
                               DeviceIpScratch *scratch,
                               int *reject_reason) {
    int *vertices = scratch->vertices;
    int vertex_count = 0;
    DeviceCEqList5 *candidate_equations = &scratch->candidate_equations;
    DeviceEqList5 *facets = &scratch->facets;
    unsigned long long *ceq_inci = scratch->ceq_inci;
    unsigned long long *facet_inci = scratch->facet_inci;
    if (device_glz_start_simplex(points, point_count, vertices, &vertex_count, candidate_equations)) {
        *reject_reason = 1;
        return 0;
    }
    for (int index = 0; index < candidate_equations->ne; ++index) {
        ceq_inci[index] = device_eq_to_inci(candidate_equations->e[index], points, vertices, vertex_count);
        if (device_inci_abs(ceq_inci[index]) < 5) {
            *reject_reason = 2;
            return 0;
        }
    }
    facets->ne = 0;
    int ip = 1;
    int truncated = 0;
    while (candidate_equations->ne >= 0) {
        if (device_ip_search_bad_eq(candidate_equations, facets, ceq_inci, facet_inci, points, point_count, &ip, &truncated)) {
            if (!ip) {
                *reject_reason = 4;
                return 0;
            }
            if (vertex_count >= 64) {
                *reject_reason = 3;
                return 0;
            }
            vertices[vertex_count++] = device_search_new_vertex(candidate_equations->e[candidate_equations->ne - 1], points, point_count);
            device_make_new_ceqs(points, vertices, vertex_count, candidate_equations,
                                 facets, ceq_inci, facet_inci,
                                 &scratch->bad_equations, scratch->bad_inci, &truncated);
            if (truncated) {
                *reject_reason = 3;
                return 0;
            }
        }
    }
    if (truncated) {
        *reject_reason = 3;
        return 0;
    }
    return 1;
}

}  // namespace cwsgpu
