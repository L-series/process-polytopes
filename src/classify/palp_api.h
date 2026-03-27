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

/* ── Bring in the PALP type universe ────────────────────────────────────── */
#include "../../PALP/Global.h"

/* ── Forward declarations of PALP functions we call ─────────────────────── */
void  Make_CWS_Points(CWS *C, PolyPointList *P);
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
    CWS *C           = ws->CW;
    PolyPointList *P  = ws->P;
    EqList *E         = ws->E;
    VertexNumList V;
    int SymNum;

    result->ok = 0;

    /* 1. Populate CWS struct */
    memset(C, 0, sizeof(CWS));
    C->nw    = 1;
    C->N     = 6;       /* 6 homogeneous coordinates */
    C->index = 1;       /* CY hypersurface           */
    C->nz    = 0;

    int degree = 0;
    for (int i = 0; i < 6; i++) {
        C->W[0][i] = weights[i];
        degree += weights[i];
    }
    C->d[0] = degree;

    /* 2. Generate lattice points */
    Make_CWS_Points(C, P);
    if (P->n == 0) return;  /* degenerate */

    /* 3. Find vertices and equations */
    int ip = Find_Equations(P, &V, E);
    if (!ip) return;  /* not interior point */

    /* 4. Sort vertex list */
    Sort_VL(&V);

    /* 5. Compute normal form (t=0, S=0, N=0 → no output) */
    Make_Poly_Sym_NF(P, &V, E, &SymNum, ws->V_perm,
                     result->nf, 0, 0, 0);

    result->ok  = 1;
    result->dim = P->n;
    result->nv  = V.nv;
    result->ne  = E->ne;
    result->np  = P->np;
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
