/**
 * palp_api.h — Thin C API for calling PALP library functions from C++.
 *
 * Provides thread-safe wrappers around PALP's core polytope computation
 * routines.  All data lives on the caller's stack or heap; the only global
 * state (`inFILE` / `outFILE`) is set once at init and left as `/dev/null`.
 *
 * Compile PALP sources with:
 *   -DPOLY_Dmax=5 -DPALP_FAST_ASSERT -DPALP_THREADSAFE
 */
#ifndef PALP_API_H
#define PALP_API_H

#ifdef __cplusplus
extern "C" {
#endif

/* C library calls used by the inline wrapper implementation. */
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

/* ── Bring in the PALP type universe ────────────────────────────────────── */
#include "../../PALP/Global.h"

/* ── Forward declarations of PALP functions we call ─────────────────────── */
void  Make_CWS_Points(CWS *C, PolyPointList *P);
typedef struct {
    Long x[AMBI_Dmax][AMBI_Dmax];
    int n, N;
} PalpCWLatticeBasis;
void  Make_CWS_Basis(CWS *C, PalpCWLatticeBasis *B);
int   Find_Equations(PolyPointList *P, VertexNumList *V, EqList *E);
void  Sort_VL(VertexNumList *V);
void  Make_VEPM(PolyPointList *P, VertexNumList *V, EqList *E, PairMat PM);
int   Make_Poly_Sym_NF(PolyPointList *P, VertexNumList *VNL, EqList *EL,
                        int *SymNum, int V_perm[][VERT_Nmax],
                        Long NF[POLY_Dmax][VERT_Nmax], int t, int S, int N);

/* ── Result from a single CWS → NF computation ─────────────────────────── */
typedef struct {
    int  ok;                            /* 1 = success, 0 = non-IP / error   */
    int  dim;                           /* polytope dimension (should be 5)  */
    int  nv;                            /* number of vertices               */
    int  ne;                            /* number of facets / equations      */
    int  np;                            /* number of lattice points          */
    Long nf[POLY_Dmax][VERT_Nmax];     /* normal form vertex matrix         */
} PalpNFResult;

/* Maximum shape needed by the 5D minimal-polytope CWS profiles. */
#define PALP_API_MAX_CWS     5
#define PALP_API_MAX_COORDS 10

typedef struct {
    int nw;                                      /* number of weight systems */
    int N;                                       /* ambient homogeneous coords */
    int index;                                   /* PALP CWS index; default 1 */
    int degree[PALP_API_MAX_CWS];
    int weights[PALP_API_MAX_CWS][PALP_API_MAX_COORDS];
} PalpCWSInput;

/* ── Per-thread workspace ───────────────────────────────────────────────── */
typedef struct {
    PolyPointList *P;
    EqList        *E;
    CWS           *CW;
    int           (*V_perm)[VERT_Nmax]; /* SYM_Nmax × VERT_Nmax             */
    PairMat       *PM;
} PalpWorkspace;

/**
 * Allocate a per-thread workspace.  Must be called once per thread
 * before any `palp_compute_nf` calls.  Free with `palp_workspace_free`.
 */
static inline PalpWorkspace *palp_workspace_alloc(void) {
    PalpWorkspace *ws = (PalpWorkspace *)calloc(1, sizeof(PalpWorkspace));
    if (!ws) return NULL;
    ws->P  = (PolyPointList *)malloc(sizeof(PolyPointList));
    ws->E  = (EqList *)malloc(sizeof(EqList));
    ws->CW = (CWS *)malloc(sizeof(CWS));
    ws->PM = (PairMat *)malloc(sizeof(PairMat));
    ws->V_perm = (int (*)[VERT_Nmax])malloc(SYM_Nmax * sizeof(int[VERT_Nmax]));
    if (!ws->P || !ws->E || !ws->CW || !ws->PM || !ws->V_perm) {
        free(ws->P); free(ws->E); free(ws->CW); free(ws->PM);
        free(ws->V_perm); free(ws);
        return NULL;
    }
    return ws;
}

static inline void palp_workspace_free(PalpWorkspace *ws) {
    if (!ws) return;
    free(ws->P); free(ws->E); free(ws->CW); free(ws->PM);
    free(ws->V_perm); free(ws);
}

static inline void palp_run_nf_from_current_points(PalpWorkspace *ws,
                                                   PalpNFResult *result)
{
    PolyPointList *points = ws->P;
    EqList *equations = ws->E;
    VertexNumList vertices;
    int sym_num;

    result->ok = 0;
    if (points->n == 0) return;

    int ip = Find_Equations(points, &vertices, equations);
    if (!ip) return;

    Sort_VL(&vertices);

    Make_Poly_Sym_NF(points, &vertices, equations, &sym_num, ws->V_perm,
                     result->nf, 0, 0, 0);

    result->ok  = 1;
    result->dim = points->n;
    result->nv  = vertices.nv;
    result->ne  = equations->ne;
    result->np  = points->np;
}

static inline void palp_run_nf_pipeline(PalpWorkspace *ws,
                                        PalpNFResult *result)
{
    result->ok = 0;
    Make_CWS_Points(ws->CW, ws->P);
    palp_run_nf_from_current_points(ws, result);
}

static inline int palp_prepare_cws_from_input(CWS *cws,
                                              const PalpCWSInput *input)
{
    if (!cws || !input) return 0;
    if (input->nw < 1 || input->nw > PALP_API_MAX_CWS) return 0;
    if (input->N < 1 || input->N > PALP_API_MAX_COORDS) return 0;
    if (input->N - input->nw != POLY_Dmax) return 0;
    if (input->nw > AMBI_Dmax || input->N > AMBI_Dmax) return 0;

    memset(cws, 0, sizeof(CWS));
    cws->nw    = input->nw;
    cws->N     = input->N;
    cws->index = input->index > 0 ? input->index : 1;
    cws->nz    = 0;

    for (int row = 0; row < input->nw; row++) {
        int degree = input->degree[row];
        Long weight_sum = 0;
        for (int coord = 0; coord < input->N; coord++) {
            int weight = input->weights[row][coord];
            if (weight < 0) return 0;
            cws->W[row][coord] = weight;
            weight_sum += weight;
        }
        if (degree == 0) {
            degree = (int)weight_sum;
        } else if ((Long)degree != weight_sum) {
            return 0;
        }
        if (degree <= 0) return 0;
        cws->d[row] = degree;
    }

    return 1;
}

/**
 * Compute the normal form of a general combined weight system.
 *
 * The input arrays are deliberately fixed to the 5D classification envelope:
 * up to five weight-system rows and ten homogeneous coordinates.  Weight rows
 * may contain zeros for coordinates not used by that row.  If degree[row] is
 * zero, it is filled from the row sum; otherwise it must match that sum.
 */
static inline void palp_compute_nf_from_cws(PalpWorkspace *ws,
                                            const PalpCWSInput *input,
                                            PalpNFResult *result)
{
    if (!result) return;
    result->ok = 0;
    if (!ws || !input) return;
    if (!palp_prepare_cws_from_input(ws->CW, input)) return;

    palp_run_nf_pipeline(ws, result);
}

/**
 * Compute the normal form of the polytope defined by a single weight system.
 *
 * @param ws       Per-thread workspace (from `palp_workspace_alloc`).
 * @param weights  Array of 6 weights (w0 … w5).  Degree = sum.
 * @param result   Output: filled on success, result->ok == 0 on failure.
 */
static inline void palp_compute_nf(PalpWorkspace *ws,
                                   const int weights[6],
                                   PalpNFResult *result)
{
    PalpCWSInput input;
    memset(&input, 0, sizeof(input));
    input.nw = 1;
    input.N = 6;
    input.index = 1;
    for (int i = 0; i < 6; i++) {
        input.weights[0][i] = weights[i];
        input.degree[0] += weights[i];
    }
    palp_compute_nf_from_cws(ws, &input, result);
}

/**
 * One-time global initialization.  Call from the main thread before
 * spawning worker threads.  Sets the PALP I/O globals to /dev/null.
 */
static inline void palp_init(void) {
    extern FILE *inFILE, *outFILE;
    inFILE  = fopen("/dev/null", "r");
    outFILE = fopen("/dev/null", "w");
}

#ifdef __cplusplus
}
#endif
#endif /* PALP_API_H */
