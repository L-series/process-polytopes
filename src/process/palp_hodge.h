/**
 * palp_hodge.h — Thin C wrapper around CLEAN PALP (v2.21) for the
 * normal-form + Hodge reprocessing pipeline.
 *
 * Per weight system it computes, with clean PALP:
 *   - the canonical normal-form vertex matrix  (Make_Poly_Sym_NF)
 *   - the Batyrev/Hodge data + M/N point counts (QuickAnalysis -> BaHo)
 *
 * Compile the PALP sources with:  -DPOLY_Dmax=5  (=> POINT_Nmax=2,000,000,
 * VERT_Nmax=64, EQUA_Nmax=1280, Long=32-bit).  Single process per core; no
 * thread-safety assumptions are made.
 *
 * All prototypes and the BaHo / FaceInfo / PolyPointList structs come straight
 * from the clean PALP Global.h (added to the include path by the build).
 */
#ifndef PALP_HODGE_H
#define PALP_HODGE_H

#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#ifdef __cplusplus
extern "C" {
#endif

#include "Global.h"   /* clean PALP-clean/Global.h via -I (C linkage) */

/* Make_CWS_Points is defined in Coord.c but not prototyped in Global.h. */
void Make_CWS_Points(CWS *C, PolyPointList *P);

/* ── Result of one weight-system reprocessing ───────────────────────────── */
typedef struct {
    int  ok;          /* 1 = success                                         */
    int  dim;         /* polytope dimension (expect 5)                       */
    int  nv;          /* number of normal-form vertices                      */
    Long nf[POLY_Dmax][VERT_Nmax];   /* canonical NF vertex matrix [dim][nv] */
    /* Hodge data (overflow-safe widths chosen by the caller) */
    int       h11, h12, h13, h22;
    long long chi;    /* Euler number = 48 + 6*(h11 - h12 + h13)             */
    /* M/N combinatorial counts straight from BaHo */
    int  bh_mp, bh_mv, bh_np, bh_nv;
} ProcResult;

/* ── Per-process workspace (allocate once, reuse for every row) ──────────── */
typedef struct {
    PolyPointList *P;       /* points consumed/mutated by QuickAnalysis      */
    PolyPointList *Pnf;     /* pristine copy used for the normal form        */
    EqList        *E;
    FaceInfo      *FI;
    BaHo          *BH;
    CWS           *CW;
    int          (*V_perm)[VERT_Nmax];   /* SYM_Nmax x VERT_Nmax            */
} ProcWorkspace;

static inline ProcWorkspace *proc_workspace_alloc(void) {
    ProcWorkspace *ws = (ProcWorkspace *)calloc(1, sizeof(ProcWorkspace));
    if (!ws) return NULL;
    ws->P      = (PolyPointList *)malloc(sizeof(PolyPointList));
    ws->Pnf    = (PolyPointList *)malloc(sizeof(PolyPointList));
    ws->E      = (EqList *)malloc(sizeof(EqList));
    ws->FI     = (FaceInfo *)malloc(sizeof(FaceInfo));
    ws->BH     = (BaHo *)malloc(sizeof(BaHo));
    ws->CW     = (CWS *)malloc(sizeof(CWS));
    ws->V_perm = (int (*)[VERT_Nmax])malloc(SYM_Nmax * sizeof(int[VERT_Nmax]));
    if (!ws->P || !ws->Pnf || !ws->E || !ws->FI || !ws->BH || !ws->CW ||
        !ws->V_perm) {
        free(ws->P); free(ws->Pnf); free(ws->E); free(ws->FI); free(ws->BH);
        free(ws->CW); free(ws->V_perm); free(ws);
        return NULL;
    }
    return ws;
}

static inline void proc_workspace_free(ProcWorkspace *ws) {
    if (!ws) return;
    free(ws->P); free(ws->Pnf); free(ws->E); free(ws->FI); free(ws->BH);
    free(ws->CW); free(ws->V_perm); free(ws);
}

/* One-time global init: route PALP I/O to /dev/null. */
static inline void palp_hodge_init(void) {
    extern FILE *inFILE, *outFILE;
    inFILE  = fopen("/dev/null", "r");
    outFILE = fopen("/dev/null", "w");
}

/**
 * Reprocess a single weight system w[0..5] (nw=1, N=6, degree = sum).
 *
 * Returns 1 on success (result->ok==1).  Returns 0 on any failure
 * (non-IP, non-reflexive) — the caller treats this as a hard error since
 * the input set is known reflexive and unique.
 */
static inline int proc_compute(ProcWorkspace *ws, const int weights[6],
                               ProcResult *result) {
    result->ok = 0;
    if (!ws || !result) return 0;

    /* Build CWS: single weight system, 6 homogeneous coords. */
    CWS *cws = ws->CW;
    memset(cws, 0, sizeof(CWS));
    cws->nw = 1;
    cws->N  = 6;
    cws->index = 1;
    cws->nz = 0;
    Long degree = 0;
    for (int i = 0; i < 6; i++) {
        if (weights[i] < 0) return 0;
        cws->W[0][i] = weights[i];
        degree += weights[i];
    }
    if (degree <= 0) return 0;
    cws->d[0] = degree;

    /* Build the point list once (the expensive step). */
    Make_CWS_Points(cws, ws->P);
    if (ws->P->n == 0 || ws->P->np == 0) return 0;

    /* Copy only the populated portion into the NF buffer so QuickAnalysis can
     * mutate ws->P freely without disturbing the normal-form computation. */
    ws->Pnf->n  = ws->P->n;
    ws->Pnf->np = ws->P->np;
    memcpy(ws->Pnf->x, ws->P->x,
           (size_t)ws->P->np * sizeof(ws->P->x[0]));

    /* ── Normal form on the pristine copy ──────────────────────────────── */
    VertexNumList V;
    int sym_num;
    if (!Find_Equations(ws->Pnf, &V, ws->E)) return 0;   /* non-IP */
    Sort_VL(&V);
    Make_Poly_Sym_NF(ws->Pnf, &V, ws->E, &sym_num, ws->V_perm,
                     result->nf, 0, 0, 0);
    result->dim = ws->Pnf->n;
    result->nv  = V.nv;

    /* ── Hodge + counts on the working buffer ──────────────────────────── */
    if (!QuickAnalysis(ws->P, ws->BH, ws->FI)) return 0; /* non-IP */
    BaHo *bh = ws->BH;
    if (bh->np == 0) return 0;                            /* non-reflexive */
    result->h11 = bh->h1[1];
    result->h12 = bh->h1[2];
    result->h13 = bh->h1[3];
    result->h22 = bh->h22;
    result->chi = 48LL + 6LL * ((long long)result->h11 - result->h12 +
                                result->h13);
    result->bh_mp = bh->mp;
    result->bh_mv = bh->mv;
    result->bh_np = bh->np;
    result->bh_nv = bh->nv;

    result->ok = 1;
    return 1;
}

#ifdef __cplusplus
}
#endif
#endif /* PALP_HODGE_H */
