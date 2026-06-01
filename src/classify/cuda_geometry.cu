#include "geometry_backend.h"

#include <cuda_runtime.h>

#include <algorithm>
#include <array>
#include <cstddef>
#include <cstdlib>
#include <stdexcept>
#include <string>
#include <vector>

namespace {

typedef struct {
    int ne;
    Equation e[CEQ_Nmax];
} CEqList;

typedef struct {
    int C[VERT_Nmax], L[VERT_Nmax], s;
} PERM;

struct DeviceVNF {
    int nv;
    int nf;
    int ns;
};

extern "C" {
int GLZ_Start_Simplex(PolyPointList *P, VertexNumList *V, CEqList *C);
INCI Eq_To_INCI(Equation *Eq, PolyPointList *P, VertexNumList *V);
int INCI_abs(INCI X);
int INCI_lex_GT(INCI *x, INCI *y);
void Make_New_CEqs(PolyPointList *P, VertexNumList *V, CEqList *C,
                   EqList *F, INCI *CEq_I, INCI *F_I);
}

struct DeviceCWSInput {
    int nw;
    int coords;
    int degree[PALP_API_MAX_CWS];
    int weights[PALP_API_MAX_CWS][PALP_API_MAX_COORDS];
};

struct DeviceCWSPrecheck {
    int valid;
    int computed_degree[PALP_API_MAX_CWS];
};

static void check_cuda(cudaError_t status, const char *operation) {
    if (status != cudaSuccess) {
        throw std::runtime_error(std::string(operation) + ": " + cudaGetErrorString(status));
    }
}

__device__ void device_swap_int(int *left, int *right) {
    int tmp = *left;
    *left = *right;
    *right = tmp;
}

__device__ long long device_gl_egcd(long long a0, long long a1,
                                    long long *vout0, long long *vout1) {
    long long v0 = a0;
    long long v1 = a1;
    long long a2;
    long long x0 = 1;
    long long x1 = 0;
    long long x2 = 0;
    while ((a2 = a0 % a1)) {
        x2 = x0 - x1 * (a0 / a1);
        a0 = a1;
        a1 = a2;
        x0 = x1;
        x1 = x2;
    }
    *vout0 = x1;
    *vout1 = (a1 - v0 * x1) / v1;
    return a1;
}

__device__ long long device_gl_round_q(long long numerator, long long denominator) {
    if (denominator < 0) {
        denominator = -denominator;
        numerator = -numerator;
    }
    long long floor_value = numerator / denominator;
    return floor_value + (2 * (numerator - floor_value * denominator)) / denominator;
}

__device__ long long device_gl_w_to_glz(long long *w, int dim,
                                        long long glz[POLY_Dmax][POLY_Dmax]) {
    for (int row = 1; row < dim; ++row) {
        for (int col = 0; col < dim; ++col) glz[row][col] = 0;
    }

    long long *extended = glz[0];
    long long *base = glz[1];
    long long gcd = device_gl_egcd(w[0], w[1], &extended[0], &extended[1]);
    base[0] = -w[1] / gcd;
    base[1] = w[0] / gcd;

    for (int row = 2; row < dim; ++row) {
        long long a;
        long long b;
        long long next_gcd = device_gl_egcd(gcd, w[row], &a, &b);
        base = glz[row];
        base[row] = gcd / next_gcd;
        gcd = w[row] / next_gcd;
        for (int col = 0; col < row; ++col) base[col] = -extended[col] * gcd;
        for (int col = 0; col < row; ++col) extended[col] *= a;
        extended[row] = b;
        for (int improve = row - 1; improve > 0; --improve) {
            long long *candidate = glz[improve];
            long long round_base = device_gl_round_q(base[improve], candidate[improve]);
            long long round_extended = device_gl_round_q(extended[improve], candidate[improve]);
            for (int col = 0; col <= improve; ++col) {
                base[col] -= round_base * candidate[col];
                extended[col] -= round_extended * candidate[col];
            }
        }
        gcd = next_gcd;
    }
    return gcd;
}

__device__ int device_glz_make_trian_nf(long long x[POLY_Dmax * VERT_Nmax],
                                        int dim,
                                        int vertex_count) {
    int current_column = -1;
    long long gcd;
    long long w[POLY_Dmax];
    long long normal_form[POLY_Dmax][VERT_Nmax];
    long long transform[POLY_Dmax][POLY_Dmax];
    long long next_transform[POLY_Dmax][POLY_Dmax];

    for (int row = 0; row < dim; ++row) {
        for (int col = 0; col < dim; ++col) transform[row][col] = (row == col);
    }
    for (int row = 0; row < dim; ++row) {
        for (int col = 0; col < vertex_count; ++col) normal_form[row][col] = 0;
    }

    for (int line = 0; line < dim; ++line) {
        int nonzero_count = 0;
        int positions[POLY_Dmax];
        while (nonzero_count == 0) {
            ++current_column;
            if (current_column >= vertex_count) return 0;
            for (int row = 0; row < dim; ++row) {
                for (int col = 0; col < dim; ++col) {
                    normal_form[row][current_column] +=
                        transform[row][col] * x[col * VERT_Nmax + current_column];
                }
            }
            for (int row = line; row < dim; ++row) {
                if (normal_form[row][current_column]) {
                    w[nonzero_count] = normal_form[row][current_column];
                    positions[nonzero_count++] = row;
                }
            }
        }

        if (nonzero_count == 1) {
            gcd = w[0];
            next_transform[0][0] = 1;
        } else {
            gcd = device_gl_w_to_glz(w, nonzero_count, next_transform);
        }
        if (gcd < 0) {
            gcd = -gcd;
            for (int row = 0; row < nonzero_count; ++row) next_transform[0][row] *= -1;
        }
        normal_form[line][current_column] = gcd;
        for (int row = line + 1; row < dim; ++row) normal_form[row][current_column] = 0;

        for (int transform_col = 0; transform_col < dim; ++transform_col) {
            long long copied[POLY_Dmax];
            for (int row = 0; row < nonzero_count; ++row) {
                copied[row] = transform[positions[row]][transform_col];
            }
            for (int row = 0; row < nonzero_count; ++row) {
                transform[positions[row]][transform_col] = 0;
                for (int col = 0; col < nonzero_count; ++col) {
                    transform[positions[row]][transform_col] += next_transform[row][col] * copied[col];
                }
            }
        }
        if (line != positions[0]) {
            for (int col = 0; col < dim; ++col) {
                long long tmp = transform[line][col];
                transform[line][col] = transform[positions[0]][col];
                transform[positions[0]][col] = tmp;
            }
        }
        for (int row = 0; row < line; ++row) {
            long long quotient = normal_form[row][current_column] / normal_form[line][current_column];
            if (normal_form[row][current_column] - quotient * normal_form[line][current_column] < 0) {
                --quotient;
            }
            normal_form[row][current_column] -= quotient * normal_form[line][current_column];
            for (int col = 0; col < dim; ++col) transform[row][col] -= quotient * transform[line][col];
        }
    }

    while (++current_column < vertex_count) {
        for (int row = 0; row < dim; ++row) {
            for (int col = 0; col < dim; ++col) {
                normal_form[row][current_column] +=
                    transform[row][col] * x[col * VERT_Nmax + current_column];
            }
        }
    }
    for (int row = 0; row < dim; ++row) {
        for (int col = 0; col < vertex_count; ++col) {
            x[row * VERT_Nmax + col] = normal_form[row][col];
        }
    }
    return 1;
}

__device__ int device_aux_x_lt_y(long long x[POLY_Dmax * VERT_Nmax],
                                 long long y[POLY_Dmax * VERT_Nmax],
                                 int dim,
                                 int vertex_count) {
    for (int row = 0; row < dim; ++row) {
        for (int col = 0; col < vertex_count; ++col) {
            long long diff = x[row * VERT_Nmax + col] - y[row * VERT_Nmax + col];
            if (diff) return diff < 0;
        }
    }
    return 0;
}

__device__ void device_aux_vnf_init(DeviceVNF *vpm_shape,
                                    long long *vpm,
                                    PERM *candidate_list,
                                    int *symmetry_blocks,
                                    int *candidate_count) {
    PERM permutation{};
    for (int row = 0; row < vpm_shape->nf; ++row) permutation.L[row] = row;
    for (int col = 0; col < vpm_shape->nv; ++col) permutation.C[col] = col;

    candidate_list[0] = permutation;
    PERM *best = &candidate_list[0];
    long long *best_row = vpm;

    for (int col = 1; col < vpm_shape->nv; ++col) {
        if (best_row[best->C[0]] < best_row[best->C[col]]) {
            device_swap_int(&best->C[0], &best->C[col]);
        }
    }
    for (int col = 1; col < vpm_shape->nv; ++col) {
        for (int scan = col + 1; scan < vpm_shape->nv; ++scan) {
            if (best_row[best->C[col]] < best_row[best->C[scan]]) {
                device_swap_int(&best->C[col], &best->C[scan]);
            }
        }
    }

    for (int next_row = 1; next_row < vpm_shape->nf; ++next_row) {
        long long diff;
        PERM *candidate = &candidate_list[*candidate_count];
        *candidate = permutation;
        long long *row_values = vpm + next_row * VERT_Nmax;

        int max_index = 0;
        for (int col = 1; col < vpm_shape->nv; ++col) {
            if (row_values[candidate->C[max_index]] < row_values[candidate->C[col]]) max_index = col;
        }
        if (max_index) device_swap_int(&candidate->C[0], &candidate->C[max_index]);

        diff = row_values[candidate->C[0]] - best_row[best->C[0]];
        if (diff < 0) continue;
        for (int col = 1; col < vpm_shape->nv; ++col) {
            int local_max = col;
            for (int scan = col + 1; scan < vpm_shape->nv; ++scan) {
                if (row_values[candidate->C[local_max]] < row_values[candidate->C[scan]]) {
                    local_max = scan;
                }
            }
            if (local_max > col) device_swap_int(&candidate->C[col], &candidate->C[local_max]);
            if (diff == 0) {
                diff = row_values[candidate->C[col]] - best_row[best->C[col]];
                if (diff < 0) break;
            }
        }
        if (diff < 0) continue;
        device_swap_int(&candidate->L[0], &candidate->L[next_row]);
        if (diff == 0) {
            ++(*candidate_count);
        } else {
            *best = *candidate;
            *candidate_count = 1;
            best_row = row_values;
        }
    }

    long long *row_values = vpm + candidate_list[0].L[0] * VERT_Nmax;
    symmetry_blocks[0] = 0;
    for (int col = 1; col < vpm_shape->nv; ++col) {
        if (row_values[candidate_list[0].C[col]] == row_values[candidate_list[0].C[col - 1]]) {
            ++symmetry_blocks[symmetry_blocks[col] = symmetry_blocks[col - 1]];
        } else {
            symmetry_blocks[col] = col;
        }
    }
}

__device__ void device_aux_vnf_line(int line,
                                    DeviceVNF *vpm_shape,
                                    long long *vpm,
                                    PERM *candidate_list,
                                    int *symmetry_blocks,
                                    int *candidate_count,
                                    int *status) {
    int current = *candidate_count;
    int compare_flag = 0;
    long long reference_line[VERT_Nmax];

    while (current--) {
        PERM next_permutations[VERT_Nmax];
        int col = 0;
        int candidate_row = line - 1;
        int next_count = 0;
        int *columns;
        int column_compare_flag = compare_flag;
        next_permutations[0] = candidate_list[current];

        while (++candidate_row < vpm_shape->nf) {
            int scan = 0;
            columns = next_permutations[next_count].C;
            long long *row_values = vpm + next_permutations[next_count].L[candidate_row] * VERT_Nmax;
            while (++scan <= symmetry_blocks[0]) {
                if (row_values[columns[col]] < row_values[columns[scan]]) {
                    device_swap_int(&columns[col], &columns[scan]);
                }
            }
            if (column_compare_flag) {
                long long diff = row_values[*columns] - reference_line[0];
                if (diff < 0) {
                } else if (diff) {
                    reference_line[0] = row_values[*columns];
                    compare_flag = 0;
                    next_permutations[0] = next_permutations[next_count];
                    next_count = 1;
                    *candidate_count = current + 1;
                    device_swap_int(&next_permutations[0].L[line],
                                    &next_permutations[0].L[candidate_row]);
                } else {
                    device_swap_int(&next_permutations[next_count].L[line],
                                    &next_permutations[next_count].L[candidate_row]);
                    ++next_count;
                    next_permutations[next_count] = candidate_list[current];
                }
            } else {
                reference_line[0] = row_values[*columns];
                device_swap_int(&next_permutations[next_count].L[line],
                                &next_permutations[next_count].L[candidate_row]);
                ++next_count;
                next_permutations[next_count] = candidate_list[current];
                column_compare_flag = 1;
            }
        }

        while (++col < vpm_shape->nv) {
            int block_end = symmetry_blocks[col];
            int scan_count = next_count;
            column_compare_flag = compare_flag;
            if (block_end < col) block_end = symmetry_blocks[block_end];
            while (scan_count--) {
                int scan = col;
                columns = next_permutations[scan_count].C;
                long long *row_values = vpm + next_permutations[scan_count].L[line] * VERT_Nmax;
                while (++scan <= block_end) {
                    if (row_values[columns[col]] < row_values[columns[scan]]) {
                        device_swap_int(&columns[col], &columns[scan]);
                    }
                }
                if (column_compare_flag) {
                    long long diff = row_values[columns[col]] - reference_line[col];
                    if (diff < 0) {
                        if (--next_count > scan_count) next_permutations[scan_count] = next_permutations[next_count];
                    } else if (diff) {
                        reference_line[col] = row_values[columns[col]];
                        compare_flag = 0;
                        next_count = scan_count + 1;
                        *candidate_count = current + 1;
                    }
                } else {
                    reference_line[col] = row_values[columns[col]];
                    column_compare_flag = 1;
                }
            }
        }
        compare_flag = 1;
        if (--(*candidate_count) > current) candidate_list[current] = candidate_list[*candidate_count];
        if (SYM_Nmax < (*candidate_count + next_count)) {
            *status = -1;
            return;
        }
        for (int index = 0; index < next_count; ++index) {
            candidate_list[(*candidate_count)++] = next_permutations[index];
        }
    }

    long long *row_values = vpm + candidate_list[0].L[line] * VERT_Nmax;
    int col = 0;
    int *columns = candidate_list[0].C;
    while (col < vpm_shape->nv) {
        int block_end = symmetry_blocks[col] + 1;
        symmetry_blocks[col] = col;
        while (++col < block_end) {
            if (row_values[columns[col]] == row_values[columns[col - 1]]) {
                ++symmetry_blocks[symmetry_blocks[col] = symmetry_blocks[col - 1]];
            } else {
                symmetry_blocks[col] = col;
            }
        }
    }
}

__device__ void device_make_vpm_nf(int vertex_count,
                                   int equation_count,
                                   long long *vpm,
                                   PERM *candidate_list,
                                   int *candidate_count,
                                   long long *vpm_nf,
                                   int *status) {
    int symmetry_blocks[VERT_Nmax];
    DeviceVNF shape{vertex_count, equation_count, 0};
    *candidate_count = 1;
    device_aux_vnf_init(&shape, vpm, candidate_list, symmetry_blocks, candidate_count);
    for (int line = 1; line < shape.nf - 1; ++line) {
        device_aux_vnf_line(line, &shape, vpm, candidate_list, symmetry_blocks, candidate_count, status);
        if (*status < 0) return;
    }
    shape.ns = *candidate_count;
    for (int col = 0; col < shape.nv; ++col) {
        for (int row = 0; row < shape.nf; ++row) {
            vpm_nf[row * VERT_Nmax + col] =
                vpm[candidate_list[0].L[row] * VERT_Nmax + candidate_list[0].C[col]];
        }
    }
}

__device__ void device_new_pnf_order(int vertex_count,
                                     int equation_count,
                                     PERM *candidate_list,
                                     int candidate_count,
                                     long long *vpm_nf) {
    int permutation[VERT_Nmax];
    int reordered[VERT_Nmax];
    long long max_pairing[VERT_Nmax];
    long long sum_pairing[VERT_Nmax];
    for (int col = 0; col < vertex_count; ++col) {
        permutation[col] = col;
        max_pairing[col] = 0;
        sum_pairing[col] = 0;
        for (int row = 0; row < equation_count; ++row) {
            long long value = vpm_nf[row * VERT_Nmax + col];
            sum_pairing[col] += value;
            if (value > max_pairing[col]) max_pairing[col] = value;
        }
    }
    for (int col = 0; col < vertex_count - 1; ++col) {
        int selected = col;
        for (int scan = col + 1; scan < vertex_count; ++scan) {
            if (max_pairing[scan] < max_pairing[selected]) {
                selected = scan;
            } else if (max_pairing[scan] == max_pairing[selected] &&
                       sum_pairing[scan] < sum_pairing[selected]) {
                selected = scan;
            }
        }
        if (selected != col) {
            long long max_tmp = max_pairing[col];
            max_pairing[col] = max_pairing[selected];
            max_pairing[selected] = max_tmp;
            long long sum_tmp = sum_pairing[col];
            sum_pairing[col] = sum_pairing[selected];
            sum_pairing[selected] = sum_tmp;
            int perm_tmp = permutation[col];
            permutation[col] = permutation[selected];
            permutation[selected] = perm_tmp;
        }
    }
    for (int candidate = 0; candidate < candidate_count; ++candidate) {
        int *columns = candidate_list[candidate].C;
        for (int col = 0; col < vertex_count; ++col) reordered[col] = columns[permutation[col]];
        for (int col = 0; col < vertex_count; ++col) columns[col] = reordered[col];
    }
}

__device__ void device_aux_make_triang(PERM *candidate_list,
                                       int candidate_count,
                                       long long *vertices,
                                       int dim,
                                       int vertex_count,
                                       int *status) {
    long long x[POLY_Dmax * VERT_Nmax];
    long long y[POLY_Dmax * VERT_Nmax];
    int use_y = 0;

    for (int row = 0; row < dim; ++row) {
        for (int col = 0; col < vertex_count; ++col) {
            x[row * VERT_Nmax + col] = vertices[row * VERT_Nmax + candidate_list[0].C[col]];
        }
    }
    if (!device_glz_make_trian_nf(x, dim, vertex_count)) {
        *status = -2;
        return;
    }

    for (int candidate = 1; candidate < candidate_count; ++candidate) {
        if (use_y) {
            for (int row = 0; row < dim; ++row) {
                for (int col = 0; col < vertex_count; ++col) {
                    x[row * VERT_Nmax + col] = vertices[row * VERT_Nmax + candidate_list[candidate].C[col]];
                }
            }
            if (!device_glz_make_trian_nf(x, dim, vertex_count)) {
                *status = -2;
                return;
            }
            if (device_aux_x_lt_y(x, y, dim, vertex_count)) use_y = 0;
        } else {
            for (int row = 0; row < dim; ++row) {
                for (int col = 0; col < vertex_count; ++col) {
                    y[row * VERT_Nmax + col] = vertices[row * VERT_Nmax + candidate_list[candidate].C[col]];
                }
            }
            if (!device_glz_make_trian_nf(y, dim, vertex_count)) {
                *status = -2;
                return;
            }
            if (device_aux_x_lt_y(y, x, dim, vertex_count)) use_y = 1;
        }
    }

    long long *selected = use_y ? y : x;
    for (int row = 0; row < dim; ++row) {
        for (int col = 0; col < vertex_count; ++col) {
            vertices[row * VERT_Nmax + col] = selected[row * VERT_Nmax + col];
        }
    }
}

__global__ void nf_canonical_kernel(const long long *vm,
                                    const long long *vpm,
                                    int dim,
                                    int vertex_count,
                                    int equation_count,
                                    PERM *candidate_list,
                                    long long *vpm_nf,
                                    long long *nf,
                                    int *status) {
    if (blockIdx.x != 0 || threadIdx.x != 0) return;
    *status = 0;

    int candidate_count = 0;
    long long working_vertices[POLY_Dmax * VERT_Nmax];
    for (int row = 0; row < dim; ++row) {
        for (int col = 0; col < vertex_count; ++col) {
            working_vertices[row * VERT_Nmax + col] = vm[row * VERT_Nmax + col];
        }
    }

    device_make_vpm_nf(vertex_count, equation_count, const_cast<long long *>(vpm),
                       candidate_list, &candidate_count, vpm_nf, status);
    if (*status < 0) return;
    device_new_pnf_order(vertex_count, equation_count, candidate_list, candidate_count, vpm_nf);
    device_aux_make_triang(candidate_list, candidate_count, working_vertices, dim, vertex_count, status);
    if (*status < 0) return;

    for (int row = 0; row < dim; ++row) {
        for (int col = 0; col < vertex_count; ++col) {
            nf[row * VERT_Nmax + col] = working_vertices[row * VERT_Nmax + col];
        }
    }
    *status = 1;
}

__global__ void cws_precheck_kernel(const DeviceCWSInput *inputs,
                                    DeviceCWSPrecheck *outputs,
                                    int count) {
    int row_index = blockIdx.x * blockDim.x + threadIdx.x;
    if (row_index >= count) return;

    const DeviceCWSInput &input = inputs[row_index];
    DeviceCWSPrecheck result{};
    result.valid = 1;

    if (input.nw < 1 || input.nw > PALP_API_MAX_CWS ||
        input.coords < 1 || input.coords > PALP_API_MAX_COORDS ||
        input.coords - input.nw != POLY_Dmax) {
        result.valid = 0;
        outputs[row_index] = result;
        return;
    }

    for (int row = 0; row < PALP_API_MAX_CWS; ++row) {
        int weight_sum = 0;
        for (int coord = 0; coord < PALP_API_MAX_COORDS; ++coord) {
            int weight = input.weights[row][coord];
            if (row < input.nw && coord < input.coords) {
                if (weight < 0) result.valid = 0;
                weight_sum += weight;
            } else if (weight != 0) {
                result.valid = 0;
            }
        }
        result.computed_degree[row] = weight_sum;
        if (row < input.nw) {
            int degree = input.degree[row] == 0 ? weight_sum : input.degree[row];
            if (degree <= 0 || degree != weight_sum) result.valid = 0;
        } else if (input.degree[row] != 0) {
            result.valid = 0;
        }
    }

    outputs[row_index] = result;
}

struct DevicePointCountInput {
    int dim;
    int coords;
    long long basis[POLY_Dmax][PALP_API_MAX_COORDS];
    long long xmax[PALP_API_MAX_COORDS];
    long long lower[POLY_Dmax];
    long long extent[POLY_Dmax];
    unsigned long long total_candidates;
};

__global__ void cws_point_count_kernel(DevicePointCountInput input,
                                       unsigned long long *point_count) {
    unsigned long long linear_index = blockIdx.x * blockDim.x + threadIdx.x;
    unsigned long long stride = blockDim.x * gridDim.x;

    while (linear_index < input.total_candidates) {
        unsigned long long remainder = linear_index;
        long long point[POLY_Dmax];
        for (int dim_index = 0; dim_index < input.dim; ++dim_index) {
            long long offset = static_cast<long long>(remainder % input.extent[dim_index]);
            remainder /= static_cast<unsigned long long>(input.extent[dim_index]);
            point[dim_index] = input.lower[dim_index] + offset;
        }

        int valid = 1;
        for (int coord = 0; coord < input.coords; ++coord) {
            long long ambient = 1;
            for (int dim_index = 0; dim_index < input.dim; ++dim_index) {
                ambient += point[dim_index] * input.basis[dim_index][coord];
            }
            if (ambient < 0 || ambient > input.xmax[coord]) {
                valid = 0;
                break;
            }
        }
        if (valid) atomicAdd(point_count, 1ULL);

        linear_index += stride;
    }
}

struct DeviceMakePointsInput {
    int dim;
    int coords;
    int nw;
    long long basis[POLY_Dmax][PALP_API_MAX_COORDS];
    long long weights[PALP_API_MAX_CWS][PALP_API_MAX_COORDS];
    long long degree[PALP_API_MAX_CWS];
    int max_points;
};

__device__ long long device_pd_floor(long long numerator, long long denominator) {
    long long quotient = numerator / denominator;
    return quotient * denominator > numerator ? quotient - 1 : quotient;
}

__device__ void append_device_point_atomic(const long long point[POLY_Dmax],
                                           int dim,
                                           long long *points,
                                           int max_points,
                                           int *point_count,
                                           int *status) {
    int index = atomicAdd(point_count, 1);
    if (index >= max_points) {
        atomicExch(status, 1);
        return;
    }
    for (int coord = 0; coord < dim; ++coord) {
        points[index * POLY_Dmax + coord] = point[coord];
    }
}

__global__ void cws_make_points_parallel_kernel(DeviceMakePointsInput input,
                                                long long *points,
                                                int *point_count,
                                                int *status) {
    long long x_upper[PALP_API_MAX_COORDS]{};
    long long x0[PALP_API_MAX_COORDS]{};
    int amin[POLY_Dmax + 1]{};

    for (int coord = 0; coord < input.coords; ++coord) x0[coord] = 1;

    int basis_dim = input.dim;
    int basis_coords = input.coords;
    int i = basis_dim;
    int j = basis_coords;
    amin[0] = 0;
    amin[basis_dim] = basis_coords;
    while (--i) {
        while (j > 0 && !input.basis[i - 1][--j]) {}
        amin[i] = ++j;
    }

    for (int coord = 0; coord < basis_coords; ++coord) {
        x_upper[coord] = 0;
        for (int row = 0; row < input.nw; ++row) {
            if (input.weights[row][coord]) {
                long long limit = input.degree[row] / input.weights[row][coord];
                x_upper[coord] = x_upper[coord] ? min(x_upper[coord], limit) : limit;
            }
        }
    }

    int top_dim = basis_dim - 1;
    i = amin[top_dim + 1] - 1;
    long long divisor = input.basis[top_dim][i];
    long long top_min = -device_pd_floor(x0[i], divisor);
    long long top_max = device_pd_floor(x_upper[i] - x0[i], divisor);

    while ((i--) > amin[top_dim]) {
        long long low = -x0[i];
        long long upper = low + x_upper[i];
        divisor = input.basis[top_dim][i];
        if (divisor > 0) {
            long long limit = device_pd_floor(upper, divisor);
            if (top_max > limit) top_max = limit;
            limit = -device_pd_floor(-low, divisor);
            if (top_min < limit) top_min = limit;
        } else {
            long long limit = device_pd_floor(-low, -divisor);
            if (top_max > limit) top_max = limit;
            limit = -device_pd_floor(upper, -divisor);
            if (top_min < limit) top_min = limit;
        }
    }

    long long seed_count = top_max >= top_min ? top_max - top_min + 1 : 0;
    long long stride = static_cast<long long>(blockDim.x) * gridDim.x;
    for (long long seed = static_cast<long long>(blockIdx.x) * blockDim.x + threadIdx.x;
         seed < seed_count && atomicAdd(status, 0) == 0;
         seed += stride) {
        long long x[POLY_Dmax]{};
        long long xmin[POLY_Dmax]{};
        long long xmax[POLY_Dmax]{};
        int walk_dim = top_dim;

        xmin[top_dim] = top_min + seed;
        xmax[top_dim] = top_min + seed;
        x[top_dim] = xmin[top_dim];

        while (walk_dim < basis_dim && atomicAdd(status, 0) == 0) {
            if (x[walk_dim] > xmax[walk_dim]) {
                ++walk_dim;
                if (basis_dim == walk_dim) break;
                ++x[walk_dim];
            } else {
                int source_coord = amin[walk_dim] - 1;
                --walk_dim;
                long long upper = x_upper[source_coord];
                long long low = -x0[source_coord];
                int range_flag = 0;
                for (int k = walk_dim + 1; k < basis_dim; ++k) {
                    low -= x[k] * input.basis[k][source_coord];
                }
                upper += low;
                divisor = input.basis[walk_dim][source_coord];
                xmin[walk_dim] = -device_pd_floor(-low, divisor);
                xmax[walk_dim] = device_pd_floor(upper, divisor);

                i = source_coord;
                while ((i--) > amin[walk_dim]) {
                    divisor = input.basis[walk_dim][i];
                    if (divisor) {
                        low = -x0[i];
                        upper = x_upper[i];
                        for (int k = walk_dim + 1; k < basis_dim; ++k) {
                            low -= x[k] * input.basis[k][i];
                        }
                        upper += low;
                        if (divisor > 0) {
                            long long limit = device_pd_floor(upper, divisor);
                            if (xmax[walk_dim] > limit) xmax[walk_dim] = limit;
                            limit = -device_pd_floor(-low, divisor);
                            if (xmin[walk_dim] < limit) xmin[walk_dim] = limit;
                        } else {
                            long long limit = device_pd_floor(-low, -divisor);
                            if (xmax[walk_dim] > limit) xmax[walk_dim] = limit;
                            limit = -device_pd_floor(upper, -divisor);
                            if (xmin[walk_dim] < limit) xmin[walk_dim] = limit;
                        }
                    } else {
                        long long ambient = 1;
                        for (int k = walk_dim + 1; k < basis_dim; ++k) {
                            ambient += x[k] * input.basis[k][i];
                        }
                        if (ambient < 0 || ambient > x_upper[i]) range_flag = 1;
                    }
                }

                if (range_flag) {
                    ++x[++walk_dim];
                } else {
                    x[walk_dim] = xmin[walk_dim];
                }

                if (walk_dim == 0) {
                    while (x[0] <= xmax[0] && atomicAdd(status, 0) == 0) {
                        append_device_point_atomic(x, basis_dim, points, input.max_points,
                                                   point_count, status);
                        ++x[0];
                    }
                    walk_dim = 1;
                    ++x[walk_dim];
                }
            }
        }
    }
}

struct DeviceEquationInput {
    int dim;
    long long a[POLY_Dmax];
    long long c;
};

struct DeviceVertexCandidate {
    long long value;
    int index;
    long long point[POLY_Dmax];
    int valid;
};

__device__ int device_point_lex_greater(const long long *left,
                                        const long long *right,
                                        int dim) {
    for (int coord = dim - 1; coord >= 0; --coord) {
        if (left[coord] > right[coord]) return 1;
        if (left[coord] < right[coord]) return 0;
    }
    return 0;
}

__device__ int device_candidate_better(const DeviceVertexCandidate &candidate,
                                       const DeviceVertexCandidate &best,
                                       int dim) {
    if (!candidate.valid) return 0;
    if (!best.valid) return 1;
    if (candidate.value < best.value) return 1;
    if (candidate.value > best.value) return 0;
    return device_point_lex_greater(candidate.point, best.point, dim);
}

__global__ void equation_min_vertex_kernel(const long long *points,
                                           int point_count,
                                           DeviceEquationInput equation,
                                           DeviceVertexCandidate *block_results) {
    __shared__ DeviceVertexCandidate shared[256];
    int tid = threadIdx.x;
    DeviceVertexCandidate best{};
    best.valid = 0;

    for (int point_index = blockIdx.x * blockDim.x + tid;
         point_index < point_count;
         point_index += blockDim.x * gridDim.x) {
        const long long *point = points + static_cast<std::size_t>(point_index) * POLY_Dmax;
        long long value = equation.c;
        for (int coord = 0; coord < equation.dim; ++coord) {
            value += equation.a[coord] * point[coord];
        }

        DeviceVertexCandidate candidate{};
        candidate.value = value;
        candidate.index = point_index;
        candidate.valid = 1;
        for (int coord = 0; coord < equation.dim; ++coord) {
            candidate.point[coord] = point[coord];
        }
        if (device_candidate_better(candidate, best, equation.dim)) best = candidate;
    }

    shared[tid] = best;
    __syncthreads();

    for (int stride = blockDim.x / 2; stride > 0; stride >>= 1) {
        if (tid < stride) {
            DeviceVertexCandidate other = shared[tid + stride];
            if (device_candidate_better(other, shared[tid], equation.dim)) shared[tid] = other;
        }
        __syncthreads();
    }

    if (tid == 0) block_results[blockIdx.x] = shared[0];
}

__global__ void equations_min_vertex_kernel(const long long *points,
                                    int point_count,
                                    const DeviceEquationInput *equations,
                                    int equation_count,
                                    int blocks_per_equation,
                                    DeviceVertexCandidate *block_results) {
    __shared__ DeviceVertexCandidate shared[256];
    int tid = threadIdx.x;
    int equation_index = blockIdx.x / blocks_per_equation;
    int equation_block = blockIdx.x - equation_index * blocks_per_equation;
    if (equation_index >= equation_count) return;

    const DeviceEquationInput &equation = equations[equation_index];
    DeviceVertexCandidate best{};
    best.valid = 0;

    for (int point_index = equation_block * blockDim.x + tid;
         point_index < point_count;
        point_index += blockDim.x * blocks_per_equation) {
        const long long *point = points + static_cast<std::size_t>(point_index) * POLY_Dmax;
        long long value = equation.c;
        for (int coord = 0; coord < equation.dim; ++coord) {
            value += equation.a[coord] * point[coord];
        }

        DeviceVertexCandidate candidate{};
        candidate.value = value;
        candidate.index = point_index;
        candidate.valid = 1;
        for (int coord = 0; coord < equation.dim; ++coord) {
            candidate.point[coord] = point[coord];
        }
        if (device_candidate_better(candidate, best, equation.dim)) best = candidate;
    }

    shared[tid] = best;
    __syncthreads();

    for (int stride = blockDim.x / 2; stride > 0; stride >>= 1) {
        if (tid < stride) {
            DeviceVertexCandidate other = shared[tid + stride];
            if (device_candidate_better(other, shared[tid], equation.dim)) shared[tid] = other;
        }
        __syncthreads();
    }

    if (tid == 0) block_results[equation_index * blocks_per_equation + equation_block] = shared[0];
}

__global__ void nf_vm_vpm_kernel(const long long *points,
                                 const int *vertices,
                                 const DeviceEquationInput *equations,
                                 int dim,
                                 int vertex_count,
                                 int equation_count,
                                 long long *vm,
                                 long long *vpm,
                                 int *reflexive) {
    int total_vm = dim * vertex_count;
    int total_vpm = equation_count * vertex_count;
    int task_count = total_vm + total_vpm;

    for (int task = blockIdx.x * blockDim.x + threadIdx.x;
         task < task_count;
         task += blockDim.x * gridDim.x) {
        if (task < total_vm) {
            int dim_index = task / vertex_count;
            int vertex_column = task % vertex_count;
            int point_index = vertices[vertex_column];
            vm[dim_index * VERT_Nmax + vertex_column] =
                points[static_cast<std::size_t>(point_index) * POLY_Dmax + dim_index];
            continue;
        }

        int vpm_task = task - total_vm;
        int equation_index = vpm_task / vertex_count;
        int vertex_column = vpm_task % vertex_count;
        int point_index = vertices[vertex_column];
        const long long *point = points + static_cast<std::size_t>(point_index) * POLY_Dmax;
        const DeviceEquationInput &equation = equations[equation_index];
        long long value = equation.c;
        for (int coord = 0; coord < dim; ++coord) value += equation.a[coord] * point[coord];
        vpm[equation_index * VERT_Nmax + vertex_column] = value;
        if (vertex_column == 0 && equation.c != 1) *reflexive = 0;
    }
}

const PalpCWSInput &strided_input_at(const PalpCWSInput *first_input,
                                     std::size_t stride_bytes,
                                     std::size_t input_index) {
    const auto *base = reinterpret_cast<const unsigned char *>(first_input);
    return *reinterpret_cast<const PalpCWSInput *>(base + input_index * stride_bytes);
}

DeviceCWSInput make_device_input(const PalpCWSInput &input) {
    DeviceCWSInput device_input{};
    device_input.nw = input.nw;
    device_input.coords = input.N;
    for (int row = 0; row < PALP_API_MAX_CWS; ++row) {
        device_input.degree[row] = input.degree[row];
        for (int coord = 0; coord < PALP_API_MAX_COORDS; ++coord) {
            device_input.weights[row][coord] = input.weights[row][coord];
        }
    }
    return device_input;
}

class CudaGeometryBackend final : public GeometryBackend {
public:
    explicit CudaGeometryBackend(int cuda_device)
        : cuda_device_(cuda_device), workspace_(palp_workspace_alloc()) {
        if (!workspace_) {
            throw std::runtime_error("failed to allocate PALP fallback workspace for CUDA backend");
        }
        check_cuda(cudaSetDevice(cuda_device_), "cudaSetDevice");
        cudaDeviceSetLimit(cudaLimitStackSize, 1 << 20);
        cudaGetLastError();
        check_cuda(cudaStreamCreateWithFlags(&stream_, cudaStreamNonBlocking), "cudaStreamCreateWithFlags");
    }

    ~CudaGeometryBackend() override {
        if (device_inputs_) cudaFree(device_inputs_);
        if (device_outputs_) cudaFree(device_outputs_);
        if (device_points_) cudaFree(device_points_);
        if (device_point_count_) cudaFree(device_point_count_);
        if (device_point_status_) cudaFree(device_point_status_);
        if (device_scan_equations_) cudaFree(device_scan_equations_);
        if (device_candidates_) cudaFree(device_candidates_);
        if (device_vertices_) cudaFree(device_vertices_);
        if (device_nf_equations_) cudaFree(device_nf_equations_);
        if (device_vm_) cudaFree(device_vm_);
        if (device_vpm_) cudaFree(device_vpm_);
        if (device_reflexive_) cudaFree(device_reflexive_);
        if (device_nf_candidate_list_) cudaFree(device_nf_candidate_list_);
        if (device_vpm_nf_) cudaFree(device_vpm_nf_);
        if (device_nf_result_) cudaFree(device_nf_result_);
        if (device_nf_status_) cudaFree(device_nf_status_);
        if (stream_) cudaStreamDestroy(stream_);
        palp_workspace_free(workspace_);
    }

    const char *name() const override { return "cuda-points+equation-scans+canonical-nf"; }
    bool uses_cuda_device() const override { return true; }

    void compute_batch(const PalpCWSInput *first_input,
                       std::size_t input_stride_bytes,
                       std::size_t count,
                       PalpNFResult *results) override {
        if (count == 0) return;
        ensure_capacity(count);

        host_inputs_.resize(count);
        host_outputs_.resize(count);
        for (std::size_t input_index = 0; input_index < count; ++input_index) {
            host_inputs_[input_index] = make_device_input(
                strided_input_at(first_input, input_stride_bytes, input_index));
        }

        check_cuda(cudaMemcpyAsync(device_inputs_, host_inputs_.data(),
                                   count * sizeof(DeviceCWSInput),
                                   cudaMemcpyHostToDevice, stream_),
                   "cudaMemcpyAsync inputs");
        int threads_per_block = 128;
        int block_count = static_cast<int>((count + threads_per_block - 1) / threads_per_block);
        cws_precheck_kernel<<<block_count, threads_per_block, 0, stream_>>>(
            device_inputs_, device_outputs_, static_cast<int>(count));
        check_cuda(cudaGetLastError(), "cws_precheck_kernel launch");
        check_cuda(cudaMemcpyAsync(host_outputs_.data(), device_outputs_,
                                   count * sizeof(DeviceCWSPrecheck),
                                   cudaMemcpyDeviceToHost, stream_),
                   "cudaMemcpyAsync outputs");
        check_cuda(cudaStreamSynchronize(stream_), "cudaStreamSynchronize");

        for (std::size_t input_index = 0; input_index < count; ++input_index) {
            results[input_index].ok = 0;
            if (!host_outputs_[input_index].valid) continue;
            const PalpCWSInput &input = strided_input_at(first_input, input_stride_bytes, input_index);
            if (!compute_with_cuda_points(input, results[input_index])) {
                palp_compute_nf_from_cws(workspace_, &input, &results[input_index]);
            }
        }
    }

private:
    bool compute_with_cuda_points(const PalpCWSInput &input, PalpNFResult &result) {
        result.ok = 0;
        if (!palp_prepare_cws_from_input(workspace_->CW, &input)) return false;
        if (workspace_->CW->index != 1 || workspace_->CW->nz != 0) return false;

        PalpCWLatticeBasis basis;
        Make_CWS_Basis(workspace_->CW, &basis);
        if (basis.n != POLY_Dmax) return false;

        DeviceMakePointsInput point_input{};
        point_input.dim = basis.n;
        point_input.coords = workspace_->CW->N;
        point_input.nw = workspace_->CW->nw;
        point_input.max_points = POINT_Nmax;
        for (int row = 0; row < point_input.nw; ++row) {
            point_input.degree[row] = workspace_->CW->d[row];
            for (int coord = 0; coord < point_input.coords; ++coord) {
                point_input.weights[row][coord] = workspace_->CW->W[row][coord];
            }
        }
        for (int dim_index = 0; dim_index < point_input.dim; ++dim_index) {
            for (int coord = 0; coord < point_input.coords; ++coord) {
                point_input.basis[dim_index][coord] = basis.x[dim_index][coord];
            }
        }

        ensure_point_capacity(POINT_Nmax);
        int zero = 0;
        check_cuda(cudaMemcpyAsync(device_point_count_, &zero, sizeof(int),
                                   cudaMemcpyHostToDevice, stream_),
                   "cudaMemcpyAsync point_count zero");
        check_cuda(cudaMemcpyAsync(device_point_status_, &zero, sizeof(int),
                                   cudaMemcpyHostToDevice, stream_),
                   "cudaMemcpyAsync point_status zero");

        cws_make_points_parallel_kernel<<<256, 128, 0, stream_>>>(
            point_input, device_points_, device_point_count_, device_point_status_);
        check_cuda(cudaGetLastError(), "cws_make_points_parallel_kernel launch");

        int point_count = 0;
        int status = 0;
        check_cuda(cudaMemcpyAsync(&point_count, device_point_count_, sizeof(int),
                                   cudaMemcpyDeviceToHost, stream_),
                   "cudaMemcpyAsync point_count");
        check_cuda(cudaMemcpyAsync(&status, device_point_status_, sizeof(int),
                                   cudaMemcpyDeviceToHost, stream_),
                   "cudaMemcpyAsync point_status");
        check_cuda(cudaStreamSynchronize(stream_), "cudaStreamSynchronize points");
        if (status != 0 || point_count <= 0 || point_count > POINT_Nmax) return false;

        host_points_.resize(static_cast<std::size_t>(point_count) * POLY_Dmax);
        check_cuda(cudaMemcpyAsync(host_points_.data(), device_points_,
                                   host_points_.size() * sizeof(long long),
                                   cudaMemcpyDeviceToHost, stream_),
                   "cudaMemcpyAsync points");
        check_cuda(cudaStreamSynchronize(stream_), "cudaStreamSynchronize point copy");

        workspace_->P->n = point_input.dim;
        workspace_->P->np = point_count;
        for (int point_index = 0; point_index < point_count; ++point_index) {
            for (int dim_index = 0; dim_index < point_input.dim; ++dim_index) {
                workspace_->P->x[point_index][dim_index] =
                    static_cast<Long>(host_points_[static_cast<std::size_t>(point_index) * POLY_Dmax + dim_index]);
            }
        }

        if (!run_nf_with_cuda_equation_scans(result)) {
            palp_run_nf_from_current_points(workspace_, &result);
        }
        return true;
    }

    bool run_nf_with_cuda_equation_scans(PalpNFResult &result) {
        result.ok = 0;
        PolyPointList *points = workspace_->P;
        EqList *equations = workspace_->E;
        VertexNumList vertices;
        int sym_num = 0;

        if (points->n == 0 || points->np <= 0) return true;
        int ip = find_equations_with_cuda_scans(points, &vertices, equations);
        if (!ip) return true;

        Sort_VL(&vertices);
        if (!compute_nf_with_cuda_vpm(points, &vertices, equations, result)) {
            Make_Poly_Sym_NF(points, &vertices, equations, &sym_num, workspace_->V_perm,
                             result.nf, 0, 0, 0);
            result.ok = 1;
            result.dim = points->n;
            result.nv = vertices.nv;
            result.ne = equations->ne;
            result.np = points->np;
            return true;
        }
        result.ok = 1;
        result.dim = points->n;
        result.nv = vertices.nv;
        result.ne = equations->ne;
        result.np = points->np;
        return true;
    }

    bool compute_nf_with_cuda_vpm(PolyPointList *points,
                                  VertexNumList *vertices,
                                  EqList *equations,
                                  PalpNFResult &result) {
        if (vertices->nv <= 0 || equations->ne <= 0) return false;
        ensure_nf_capacity(equations->ne);

        std::vector<int> vertex_indices(static_cast<std::size_t>(vertices->nv));
        for (int index = 0; index < vertices->nv; ++index) vertex_indices[index] = vertices->v[index];
        std::vector<DeviceEquationInput> host_equations(static_cast<std::size_t>(equations->ne));
        for (int equation_index = 0; equation_index < equations->ne; ++equation_index) {
            host_equations[equation_index].dim = points->n;
            host_equations[equation_index].c = equations->e[equation_index].c;
            for (int coord = 0; coord < points->n; ++coord) {
                host_equations[equation_index].a[coord] = equations->e[equation_index].a[coord];
            }
        }

        int reflexive = 1;
        check_cuda(cudaMemcpyAsync(device_vertices_, vertex_indices.data(),
                                   vertex_indices.size() * sizeof(int),
                                   cudaMemcpyHostToDevice, stream_),
                   "cudaMemcpyAsync NF vertices");
        check_cuda(cudaMemcpyAsync(device_nf_equations_, host_equations.data(),
                                   host_equations.size() * sizeof(DeviceEquationInput),
                                   cudaMemcpyHostToDevice, stream_),
                   "cudaMemcpyAsync NF equations");
        check_cuda(cudaMemcpyAsync(device_reflexive_, &reflexive, sizeof(int),
                                   cudaMemcpyHostToDevice, stream_),
                   "cudaMemcpyAsync reflexive flag");

        int tasks = points->n * vertices->nv + equations->ne * vertices->nv;
        int threads_per_block = 256;
        int block_count = std::min(256, std::max(1, (tasks + threads_per_block - 1) / threads_per_block));
        nf_vm_vpm_kernel<<<block_count, threads_per_block, 0, stream_>>>(
            device_points_, device_vertices_, device_nf_equations_, points->n,
            vertices->nv, equations->ne, device_vm_, device_vpm_, device_reflexive_);
        check_cuda(cudaGetLastError(), "nf_vm_vpm_kernel launch");

        nf_canonical_kernel<<<1, 1, 0, stream_>>>(
            device_vm_, device_vpm_, points->n, vertices->nv, equations->ne,
            device_nf_candidate_list_, device_vpm_nf_, device_nf_result_, device_nf_status_);
        check_cuda(cudaGetLastError(), "nf_canonical_kernel launch");

        int nf_status = 0;
        check_cuda(cudaMemcpyAsync(host_nf_.data(), device_nf_result_, host_nf_.size() * sizeof(long long),
                                   cudaMemcpyDeviceToHost, stream_),
                   "cudaMemcpyAsync CUDA NF");
        check_cuda(cudaMemcpyAsync(&nf_status, device_nf_status_, sizeof(int),
                                   cudaMemcpyDeviceToHost, stream_),
                   "cudaMemcpyAsync CUDA NF status");
        check_cuda(cudaMemcpyAsync(&reflexive, device_reflexive_, sizeof(int),
                                   cudaMemcpyDeviceToHost, stream_),
                   "cudaMemcpyAsync reflexive flag");
        check_cuda(cudaStreamSynchronize(stream_), "cudaStreamSynchronize CUDA NF");

        if (nf_status == 1) {
            for (int dim_index = 0; dim_index < points->n; ++dim_index) {
                for (int vertex_index = 0; vertex_index < vertices->nv; ++vertex_index) {
                    result.nf[dim_index][vertex_index] = static_cast<Long>(
                        host_nf_[static_cast<std::size_t>(dim_index) * VERT_Nmax + vertex_index]);
                }
            }
            (void)reflexive;
            return true;
        }
        throw std::runtime_error("CUDA canonical NF kernel failed with status " + std::to_string(nf_status));
    }

    int find_equations_with_cuda_scans(PolyPointList *points,
                                       VertexNumList *vertices,
                                       EqList *facets) {
        CEqList *candidate_equations = static_cast<CEqList *>(std::malloc(sizeof(CEqList)));
        INCI *candidate_incidences = static_cast<INCI *>(std::malloc(sizeof(INCI) * CEQ_Nmax));
        INCI *facet_incidences = static_cast<INCI *>(std::malloc(sizeof(INCI) * EQUA_Nmax));
        if (!candidate_equations || !candidate_incidences || !facet_incidences) {
            std::free(candidate_equations);
            std::free(candidate_incidences);
            std::free(facet_incidences);
            throw std::runtime_error("allocation failure in CUDA Find_Equations wrapper");
        }

        candidate_equations->ne = 0;
        if (GLZ_Start_Simplex(points, vertices, candidate_equations)) {
            facets->ne = candidate_equations->ne;
            for (int index = 0; index < facets->ne; ++index) facets->e[index] = candidate_equations->e[index];
            std::free(candidate_equations);
            std::free(candidate_incidences);
            std::free(facet_incidences);
            return 0;
        }

        facets->ne = 0;
        for (int index = 0; index < candidate_equations->ne; ++index) {
            candidate_incidences[index] = Eq_To_INCI(&candidate_equations->e[index], points, vertices);
            if (INCI_abs(candidate_incidences[index]) < points->n) {
                std::free(candidate_equations);
                std::free(candidate_incidences);
                std::free(facet_incidences);
                throw std::runtime_error("Bad CEq in CUDA Find_Equations wrapper");
            }
        }

        int ip = 1;
        while (0 <= candidate_equations->ne) {
            int new_vertex = -1;
            if (cuda_fe_search_bad_eq(candidate_equations, facets, candidate_incidences,
                                      facet_incidences, points, &ip, &new_vertex)) {
                if (vertices->nv >= VERT_Nmax) {
                    std::free(candidate_equations);
                    std::free(candidate_incidences);
                    std::free(facet_incidences);
                    throw std::runtime_error("VERT_Nmax exceeded in CUDA Find_Equations wrapper");
                }
                vertices->v[vertices->nv++] = new_vertex;
                Make_New_CEqs(points, vertices, candidate_equations, facets,
                              candidate_incidences, facet_incidences);
            }
        }

        std::free(candidate_equations);
        std::free(candidate_incidences);
        std::free(facet_incidences);
        return ip;
    }

    int cuda_fe_search_bad_eq(CEqList *candidate_equations,
                              EqList *facets,
                              INCI *candidate_incidences,
                              INCI *facet_incidences,
                              PolyPointList *points,
                              int *ip,
                              int *new_vertex) {
        if (points->np < 50000 || candidate_equations->ne < 16) {
            return cuda_fe_search_bad_eq_serial(candidate_equations, facets,
                                                candidate_incidences,
                                                facet_incidences, points,
                                                ip, new_vertex);
        }

        int equation_count = candidate_equations->ne;
        if (equation_count <= 0) {
            candidate_equations->ne = -1;
            return 0;
        }

        std::vector<DeviceVertexCandidate> equation_bests;
        find_min_vertices_on_device(candidate_equations, equation_count,
                                    points->np, points->n, equation_bests);

        std::vector<int> active(static_cast<std::size_t>(equation_count));
        for (int index = 0; index < equation_count; ++index) active[index] = index;

        while (!active.empty()) {
            int selected_position = static_cast<int>(active.size()) - 1;
            int selected = active[static_cast<std::size_t>(selected_position)];
            for (int position = 0; position < selected_position; ++position) {
                int candidate = active[static_cast<std::size_t>(position)];
                if (INCI_lex_GT(&candidate_incidences[candidate], &candidate_incidences[selected])) {
                    selected_position = position;
                    selected = candidate;
                }
            }

            const DeviceVertexCandidate &best = equation_bests[static_cast<std::size_t>(selected)];
            if (best.value < 0) {
                std::vector<Equation> original_equations(
                    candidate_equations->e, candidate_equations->e + equation_count);
                std::vector<INCI> original_incidences(
                    candidate_incidences, candidate_incidences + equation_count);
                int active_count = static_cast<int>(active.size());
                std::swap(active[static_cast<std::size_t>(selected_position)],
                          active[static_cast<std::size_t>(active_count - 1)]);
                for (int position = 0; position < active_count; ++position) {
                    int source_index = active[static_cast<std::size_t>(position)];
                    candidate_equations->e[position] = original_equations[static_cast<std::size_t>(source_index)];
                    candidate_incidences[position] = original_incidences[static_cast<std::size_t>(source_index)];
                }
                candidate_equations->ne = active_count;
                *new_vertex = best.index;
                return active_count;
            }

            if (candidate_equations->e[selected].c < 1) *ip = 0;
            if (facets->ne >= EQUA_Nmax) {
                throw std::runtime_error("EQUA_Nmax exceeded in CUDA Find_Equations wrapper");
            }
            facets->e[facets->ne] = candidate_equations->e[selected];
            facet_incidences[facets->ne++] = candidate_incidences[selected];
            active[static_cast<std::size_t>(selected_position)] = active.back();
            active.pop_back();
        }
        candidate_equations->ne = -1;
        return 0;
    }

    int cuda_fe_search_bad_eq_serial(CEqList *candidate_equations,
                                     EqList *facets,
                                     INCI *candidate_incidences,
                                     INCI *facet_incidences,
                                     PolyPointList *points,
                                     int *ip,
                                     int *new_vertex) {
        while (candidate_equations->ne--) {
            int selected = candidate_equations->ne;
            for (int index = 0; index < candidate_equations->ne; ++index) {
                if (INCI_lex_GT(&candidate_incidences[index], &candidate_incidences[selected])) {
                    selected = index;
                }
            }

            long long min_value = 0;
            int min_vertex = -1;
            find_min_vertex_on_device(candidate_equations->e[selected], points->np,
                                      points->n, &min_value, &min_vertex);
            if (min_value < 0) {
                INCI saved_incidence = candidate_incidences[selected];
                Equation saved_equation = candidate_equations->e[selected];
                candidate_incidences[selected] = candidate_incidences[candidate_equations->ne];
                candidate_equations->e[selected] = candidate_equations->e[candidate_equations->ne];
                candidate_incidences[candidate_equations->ne] = saved_incidence;
                candidate_equations->e[candidate_equations->ne] = saved_equation;
                *new_vertex = min_vertex;
                return ++candidate_equations->ne;
            }

            if (candidate_equations->e[selected].c < 1) *ip = 0;
            if (facets->ne >= EQUA_Nmax) {
                throw std::runtime_error("EQUA_Nmax exceeded in CUDA Find_Equations wrapper");
            }
            facets->e[facets->ne] = candidate_equations->e[selected];
            facet_incidences[facets->ne++] = candidate_incidences[selected];
            if (selected < candidate_equations->ne) {
                candidate_equations->e[selected] = candidate_equations->e[candidate_equations->ne];
                candidate_incidences[selected] = candidate_incidences[candidate_equations->ne];
            }
        }
        return 0;
    }

    void find_min_vertex_on_device(const Equation &equation,
                                   int point_count,
                                   int dim,
                                   long long *min_value,
                                   int *min_vertex) {
        ensure_candidate_capacity(256);

        DeviceEquationInput device_equation{};
        device_equation.dim = dim;
        device_equation.c = equation.c;
        for (int coord = 0; coord < dim; ++coord) device_equation.a[coord] = equation.a[coord];

        int threads_per_block = 256;
        int block_count = std::min(256, std::max(1, (point_count + threads_per_block - 1) / threads_per_block));
        equation_min_vertex_kernel<<<block_count, threads_per_block, 0, stream_>>>(
            device_points_, point_count, device_equation, device_candidates_);
        check_cuda(cudaGetLastError(), "equation_min_vertex_kernel launch");

        host_candidates_.resize(static_cast<std::size_t>(block_count));
        check_cuda(cudaMemcpyAsync(host_candidates_.data(), device_candidates_,
                                   host_candidates_.size() * sizeof(DeviceVertexCandidate),
                                   cudaMemcpyDeviceToHost, stream_),
                   "cudaMemcpyAsync equation candidates");
        check_cuda(cudaStreamSynchronize(stream_), "cudaStreamSynchronize equation candidates");

        DeviceVertexCandidate best{};
        best.valid = 0;
        for (const DeviceVertexCandidate &candidate : host_candidates_) {
            if (!candidate.valid) continue;
            bool better = !best.valid || candidate.value < best.value;
            if (!better && best.valid && candidate.value == best.value) {
                for (int coord = dim - 1; coord >= 0; --coord) {
                    if (candidate.point[coord] > best.point[coord]) { better = true; break; }
                    if (candidate.point[coord] < best.point[coord]) break;
                }
            }
            if (better) best = candidate;
        }
        if (!best.valid) throw std::runtime_error("CUDA equation scan produced no candidate");
        *min_value = best.value;
        *min_vertex = best.index;
    }

    void find_min_vertices_on_device(const CEqList *candidate_equations,
                                     int equation_count,
                                     int point_count,
                                     int dim,
                                     std::vector<DeviceVertexCandidate> &equation_bests) {
        int threads_per_block = 256;
        int blocks_per_equation = std::min(256, std::max(1, (point_count + threads_per_block - 1) / threads_per_block));
        int total_blocks = equation_count * blocks_per_equation;
        ensure_scan_capacity(static_cast<std::size_t>(equation_count),
                             static_cast<std::size_t>(total_blocks));

        host_scan_equations_.resize(static_cast<std::size_t>(equation_count));
        for (int equation_index = 0; equation_index < equation_count; ++equation_index) {
            host_scan_equations_[static_cast<std::size_t>(equation_index)].dim = dim;
            host_scan_equations_[static_cast<std::size_t>(equation_index)].c =
                candidate_equations->e[equation_index].c;
            for (int coord = 0; coord < dim; ++coord) {
                host_scan_equations_[static_cast<std::size_t>(equation_index)].a[coord] =
                    candidate_equations->e[equation_index].a[coord];
            }
        }

        check_cuda(cudaMemcpyAsync(device_scan_equations_, host_scan_equations_.data(),
                                   host_scan_equations_.size() * sizeof(DeviceEquationInput),
                                   cudaMemcpyHostToDevice, stream_),
                   "cudaMemcpyAsync scan equations");
        equations_min_vertex_kernel<<<total_blocks, threads_per_block, 0, stream_>>>(
            device_points_, point_count, device_scan_equations_, equation_count,
            blocks_per_equation, device_candidates_);
        check_cuda(cudaGetLastError(), "equations_min_vertex_kernel launch");

        host_candidates_.resize(static_cast<std::size_t>(total_blocks));
        check_cuda(cudaMemcpyAsync(host_candidates_.data(), device_candidates_,
                                   host_candidates_.size() * sizeof(DeviceVertexCandidate),
                                   cudaMemcpyDeviceToHost, stream_),
                   "cudaMemcpyAsync equation candidates");
        check_cuda(cudaStreamSynchronize(stream_), "cudaStreamSynchronize equation candidates");

        equation_bests.assign(static_cast<std::size_t>(equation_count), DeviceVertexCandidate{});
        for (int equation_index = 0; equation_index < equation_count; ++equation_index) {
            DeviceVertexCandidate best{};
            best.valid = 0;
            for (int block = 0; block < blocks_per_equation; ++block) {
                const DeviceVertexCandidate &candidate =
                    host_candidates_[static_cast<std::size_t>(equation_index * blocks_per_equation + block)];
                if (!candidate.valid) continue;
                bool better = !best.valid || candidate.value < best.value;
                if (!better && best.valid && candidate.value == best.value) {
                    for (int coord = dim - 1; coord >= 0; --coord) {
                        if (candidate.point[coord] > best.point[coord]) { better = true; break; }
                        if (candidate.point[coord] < best.point[coord]) break;
                    }
                }
                if (better) best = candidate;
            }
            if (!best.valid) throw std::runtime_error("CUDA equation scan produced no candidate");
            equation_bests[static_cast<std::size_t>(equation_index)] = best;
        }
    }

    void ensure_capacity(std::size_t required_count) {
        if (required_count <= capacity_) return;
        if (device_inputs_) {
            cudaFree(device_inputs_);
            device_inputs_ = nullptr;
        }
        if (device_outputs_) {
            cudaFree(device_outputs_);
            device_outputs_ = nullptr;
        }
        check_cuda(cudaMalloc(&device_inputs_, required_count * sizeof(DeviceCWSInput)),
                   "cudaMalloc inputs");
        check_cuda(cudaMalloc(&device_outputs_, required_count * sizeof(DeviceCWSPrecheck)),
                   "cudaMalloc outputs");
        capacity_ = required_count;
    }

    void ensure_point_capacity(std::size_t required_points) {
        if (required_points <= point_capacity_) return;
        if (device_points_) {
            cudaFree(device_points_);
            device_points_ = nullptr;
        }
        check_cuda(cudaMalloc(&device_points_, required_points * POLY_Dmax * sizeof(long long)),
                   "cudaMalloc points");
        point_capacity_ = required_points;
        if (!device_point_count_) {
            check_cuda(cudaMalloc(&device_point_count_, sizeof(int)), "cudaMalloc point_count");
        }
        if (!device_point_status_) {
            check_cuda(cudaMalloc(&device_point_status_, sizeof(int)), "cudaMalloc point_status");
        }
    }

    void ensure_candidate_capacity(std::size_t required_blocks) {
        if (required_blocks <= candidate_capacity_) return;
        if (device_candidates_) {
            cudaFree(device_candidates_);
            device_candidates_ = nullptr;
        }
        check_cuda(cudaMalloc(&device_candidates_, required_blocks * sizeof(DeviceVertexCandidate)),
                   "cudaMalloc equation candidates");
        candidate_capacity_ = required_blocks;
    }

    void ensure_scan_capacity(std::size_t required_equations,
                              std::size_t required_blocks) {
        if (required_equations > scan_equation_capacity_) {
            if (device_scan_equations_) {
                cudaFree(device_scan_equations_);
                device_scan_equations_ = nullptr;
            }
            check_cuda(cudaMalloc(&device_scan_equations_,
                                  required_equations * sizeof(DeviceEquationInput)),
                       "cudaMalloc scan equations");
            scan_equation_capacity_ = required_equations;
        }
        ensure_candidate_capacity(required_blocks);
    }

    void ensure_nf_capacity(std::size_t required_equations) {
        if (!device_vertices_) {
            check_cuda(cudaMalloc(&device_vertices_, VERT_Nmax * sizeof(int)),
                       "cudaMalloc NF vertices");
        }
        if (required_equations > nf_equation_capacity_) {
            if (device_nf_equations_) cudaFree(device_nf_equations_);
            check_cuda(cudaMalloc(&device_nf_equations_, required_equations * sizeof(DeviceEquationInput)),
                       "cudaMalloc NF equations");
            nf_equation_capacity_ = required_equations;
        }
        if (!device_vm_) {
            check_cuda(cudaMalloc(&device_vm_, POLY_Dmax * VERT_Nmax * sizeof(long long)),
                       "cudaMalloc VM");
            host_vm_.resize(POLY_Dmax * VERT_Nmax);
        }
        if (!device_vpm_) {
            check_cuda(cudaMalloc(&device_vpm_, VERT_Nmax * VERT_Nmax * sizeof(long long)),
                       "cudaMalloc VPM");
            host_vpm_.resize(VERT_Nmax * VERT_Nmax);
        }
        if (!device_reflexive_) {
            check_cuda(cudaMalloc(&device_reflexive_, sizeof(int)), "cudaMalloc reflexive flag");
        }
        if (!device_nf_candidate_list_) {
            check_cuda(cudaMalloc(&device_nf_candidate_list_, sizeof(PERM) * (SYM_Nmax + 1)),
                       "cudaMalloc NF candidate list");
        }
        if (!device_vpm_nf_) {
            check_cuda(cudaMalloc(&device_vpm_nf_, VERT_Nmax * VERT_Nmax * sizeof(long long)),
                       "cudaMalloc VPM NF");
        }
        if (!device_nf_result_) {
            check_cuda(cudaMalloc(&device_nf_result_, POLY_Dmax * VERT_Nmax * sizeof(long long)),
                       "cudaMalloc NF result");
            host_nf_.resize(POLY_Dmax * VERT_Nmax);
        }
        if (!device_nf_status_) {
            check_cuda(cudaMalloc(&device_nf_status_, sizeof(int)), "cudaMalloc NF status");
        }
    }

    int cuda_device_;
    cudaStream_t stream_{};
    std::size_t capacity_ = 0;
    DeviceCWSInput *device_inputs_ = nullptr;
    DeviceCWSPrecheck *device_outputs_ = nullptr;
    long long *device_points_ = nullptr;
    int *device_point_count_ = nullptr;
    int *device_point_status_ = nullptr;
    DeviceEquationInput *device_scan_equations_ = nullptr;
    DeviceVertexCandidate *device_candidates_ = nullptr;
    int *device_vertices_ = nullptr;
    DeviceEquationInput *device_nf_equations_ = nullptr;
    long long *device_vm_ = nullptr;
    long long *device_vpm_ = nullptr;
    int *device_reflexive_ = nullptr;
    PERM *device_nf_candidate_list_ = nullptr;
    long long *device_vpm_nf_ = nullptr;
    long long *device_nf_result_ = nullptr;
    int *device_nf_status_ = nullptr;
    std::size_t point_capacity_ = 0;
    std::size_t scan_equation_capacity_ = 0;
    std::size_t candidate_capacity_ = 0;
    std::size_t nf_equation_capacity_ = 0;
    std::vector<DeviceCWSInput> host_inputs_;
    std::vector<DeviceCWSPrecheck> host_outputs_;
    std::vector<long long> host_points_;
    std::vector<DeviceEquationInput> host_scan_equations_;
    std::vector<DeviceVertexCandidate> host_candidates_;
    std::vector<long long> host_vm_;
    std::vector<long long> host_vpm_;
    std::vector<long long> host_nf_;
    PalpWorkspace *workspace_;
};

}  // namespace

bool cuda_geometry_available(std::string *reason) {
    int device_count = 0;
    cudaError_t status = cudaGetDeviceCount(&device_count);
    if (status != cudaSuccess) {
        if (reason) *reason = cudaGetErrorString(status);
        return false;
    }
    if (device_count <= 0) {
        if (reason) *reason = "no CUDA devices visible";
        return false;
    }
    if (reason) *reason = "CUDA device visible";
    return true;
}

std::unique_ptr<GeometryBackend> make_cuda_geometry_backend(int cuda_device) {
    return std::make_unique<CudaGeometryBackend>(cuda_device);
}

bool cuda_count_cws_points_for_testing(const PalpCWSInput &input,
                                       long long *point_count,
                                       std::string *reason) {
    if (!point_count) {
        if (reason) *reason = "point_count output pointer is null";
        return false;
    }
    *point_count = 0;

    int device_count = 0;
    cudaError_t count_status = cudaGetDeviceCount(&device_count);
    if (count_status != cudaSuccess || device_count <= 0) {
        if (reason) {
            *reason = count_status == cudaSuccess ? "no CUDA devices visible"
                                                  : cudaGetErrorString(count_status);
        }
        return false;
    }

    CWS cws;
    if (!palp_prepare_cws_from_input(&cws, &input)) {
        if (reason) *reason = "invalid CWS input";
        return false;
    }
    if (cws.index != 1 || cws.nz != 0) {
        if (reason) *reason = "point-count probe only supports index-1 CWS without sublattice phases";
        return false;
    }

    PalpCWLatticeBasis basis;
    Make_CWS_Basis(&cws, &basis);
    if (basis.n != POLY_Dmax) {
        if (reason) *reason = "basis dimension is not POLY_Dmax";
        return false;
    }

    DevicePointCountInput device_input{};
    device_input.dim = basis.n;
    device_input.coords = cws.N;

    long long max_x = 0;
    for (int coord = 0; coord < cws.N; ++coord) {
        long long coord_max = 0;
        for (int row = 0; row < cws.nw; ++row) {
            if (cws.W[row][coord]) {
                long long row_max = cws.d[row] / cws.W[row][coord];
                coord_max = coord_max == 0 ? row_max : std::min(coord_max, row_max);
            }
        }
        device_input.xmax[coord] = coord_max;
        max_x = std::max(max_x, coord_max);
    }

    unsigned long long total_candidates = 1;
    long long broad_radius = max_x + 1;
    for (int dim_index = 0; dim_index < basis.n; ++dim_index) {
        device_input.lower[dim_index] = -broad_radius;
        device_input.extent[dim_index] = 2 * broad_radius + 1;
        if (device_input.extent[dim_index] <= 0) {
            if (reason) *reason = "invalid point-count extent";
            return false;
        }
        if (total_candidates > 100000000ULL / static_cast<unsigned long long>(device_input.extent[dim_index])) {
            if (reason) *reason = "point-count probe candidate box is too large for smoke testing";
            return false;
        }
        total_candidates *= static_cast<unsigned long long>(device_input.extent[dim_index]);
        for (int coord = 0; coord < cws.N; ++coord) {
            device_input.basis[dim_index][coord] = basis.x[dim_index][coord];
        }
    }
    device_input.total_candidates = total_candidates;

    unsigned long long *device_count_ptr = nullptr;
    check_cuda(cudaMalloc(&device_count_ptr, sizeof(unsigned long long)), "cudaMalloc point_count");
    check_cuda(cudaMemset(device_count_ptr, 0, sizeof(unsigned long long)), "cudaMemset point_count");

    int threads_per_block = 256;
    int block_count = static_cast<int>(std::min<unsigned long long>(
        65535ULL, (total_candidates + threads_per_block - 1) / threads_per_block));
    cws_point_count_kernel<<<block_count, threads_per_block>>>(device_input, device_count_ptr);
    cudaError_t launch_status = cudaGetLastError();
    if (launch_status != cudaSuccess) {
        cudaFree(device_count_ptr);
        if (reason) *reason = cudaGetErrorString(launch_status);
        return false;
    }

    unsigned long long host_count = 0;
    cudaError_t copy_status = cudaMemcpy(&host_count, device_count_ptr,
                                         sizeof(unsigned long long), cudaMemcpyDeviceToHost);
    cudaFree(device_count_ptr);
    if (copy_status != cudaSuccess) {
        if (reason) *reason = cudaGetErrorString(copy_status);
        return false;
    }

    *point_count = static_cast<long long>(host_count);
    if (reason) *reason = "ok";
    return true;
}