#include <assert.h>

#define DIM5_MAX_ARITY 5
#define DIM5_MAX_SIMPLEX_SIZE 5
#define DIM5_MAX_AMBIENT_VERTICES 10

typedef long Long;

typedef struct {
    int d;
    int N;
    Long w[DIM5_MAX_SIMPLEX_SIZE];
} Dim5WeightEntry;

typedef struct {
    int nw;
    int N;
    int nz;
    int d[DIM5_MAX_ARITY];
    Long W[DIM5_MAX_ARITY][DIM5_MAX_AMBIENT_VERTICES];
} CWS;

int nondet_int(void);
long nondet_long(void);
void __CPROVER_assume(int);

static void EmbedWeightInCWS(CWS *CW, const Dim5WeightEntry *W,
                             int ambient_vertices,
                             const int mapping[DIM5_MAX_SIMPLEX_SIZE]) {
    int i;

    CW->d[CW->nw] = W->d;
    CW->N = ambient_vertices;
    for (i = 0; i < ambient_vertices; i++)
        CW->W[CW->nw][i] = 0;
    for (i = 0; i < W->N; i++) {
        int coordinate = mapping[i] - 1;
        assert((coordinate >= 0) && (coordinate < ambient_vertices));
        CW->W[CW->nw][coordinate] = W->w[i];
    }
    CW->nw++;
    CW->nz = 0;
}

void harness(void) {
    CWS CW;
    Dim5WeightEntry weight;
    int mapping[DIM5_MAX_SIMPLEX_SIZE];
    int ambient_vertices = nondet_int();
    int original_nw = nondet_int();
    int coordinate;
    int i;

    __CPROVER_assume(ambient_vertices >= 1 &&
                     ambient_vertices <= DIM5_MAX_AMBIENT_VERTICES);
    __CPROVER_assume(original_nw >= 0 && original_nw < DIM5_MAX_ARITY);

    weight.d = nondet_int();
    weight.N = nondet_int();
    __CPROVER_assume(weight.N >= 1 && weight.N <= DIM5_MAX_SIMPLEX_SIZE);
    __CPROVER_assume(weight.N <= ambient_vertices);

    CW.nw = original_nw;
    CW.nz = 7;
    for (i = 0; i < DIM5_MAX_AMBIENT_VERTICES; i++)
        CW.W[original_nw][i] = 99;
    for (i = 0; i < weight.N; i++) {
        int j;

        weight.w[i] = nondet_long();
        mapping[i] = nondet_int();
        __CPROVER_assume(mapping[i] >= 1 && mapping[i] <= ambient_vertices);
        for (j = 0; j < i; j++)
            __CPROVER_assume(mapping[i] != mapping[j]);
    }

    EmbedWeightInCWS(&CW, &weight, ambient_vertices, mapping);

    assert(CW.d[original_nw] == weight.d);
    assert(CW.N == ambient_vertices);
    assert(CW.nw == original_nw + 1);
    assert(CW.nz == 0);

    for (coordinate = 0; coordinate < ambient_vertices; coordinate++) {
        int matched = -1;

        for (i = 0; i < weight.N; i++)
            if ((mapping[i] - 1) == coordinate)
                matched = i;

        if (matched >= 0)
            assert(CW.W[original_nw][coordinate] == weight.w[matched]);
        else
            assert(CW.W[original_nw][coordinate] == 0);
    }
}