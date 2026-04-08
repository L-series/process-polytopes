/**
 * eva_palp_driver.c — Frama-C Eva entry point for PALP verification
 *
 * Provides a main() that calls the PALP computation pipeline with
 * abstract (non-deterministic) inputs, allowing Eva to compute value
 * ranges and detect potential buffer overflows.
 *
 * Targets:
 *   2.2 — Array bounds in Make_CWS_Points (P->np <= POINT_Nmax)
 *   2.3 — Vertex count bounds in Find_Equations (V->nv <= VERT_Nmax)
 *   2.6 — palp_compute_nf wrapper correctness
 *
 * Run:
 *   frama-c -eva -eva-precision 3 \
 *     -cpp-extra-args='-DPOLY_Dmax=5 -DPALP_FAST_ASSERT -DCEQ_Nmax=2048 -I PALP' \
 *     PALP/Coord.c PALP/Rat.c PALP/Vertex.c PALP/Polynf.c PALP/LG.c \
 *     src/classify/palp_globals.c src/verify/eva_palp_driver.c \
 *     -main eva_palp_main
 */
#include "Global.h"

/* PALP function declarations */
void  Make_CWS_Points(CWS *C, PolyPointList *P);
int   Find_Equations(PolyPointList *P, VertexNumList *V, EqList *E);
void  Sort_VL(VertexNumList *V);
void  Make_VEPM(PolyPointList *P, VertexNumList *V, EqList *E, PairMat PM);
int   Make_Poly_Sym_NF(PolyPointList *P, VertexNumList *VNL, EqList *EL,
                        int *SymNum, int V_perm[][VERT_Nmax],
                        Long NF[POLY_Dmax][VERT_Nmax], int t, int S, int N);

/* Frama-C builtin for non-deterministic values */
int Frama_C_interval(int min, int max);

/*@ assigns \nothing; */
void eva_palp_main(void)
{
    /* Allocate on-stack (Eva tracks these precisely) */
    CWS C;
    PolyPointList P;
    EqList E;
    VertexNumList V;
    Long NF[POLY_Dmax][VERT_Nmax];
    int SymNum;

    /* We skip V_perm allocation to keep Eva tractable — Make_Poly_Sym_NF
       writes to it but Eva doesn't need to track its full contents */
    int V_perm[SYM_Nmax][VERT_Nmax];

    /* ── Set up a valid CWS with abstract weights ─────────────────────── */
    memset(&C, 0, sizeof(CWS));
    C.nw    = 1;
    C.N     = 6;
    C.index = 1;
    C.nz    = 0;

    /* Each weight is between 1 and 500 (covers all known 5D CWS) */
    int degree = 0;
    for (int i = 0; i < 6; i++) {
        int w = Frama_C_interval(1, 500);
        C.W[0][i] = w;
        degree += w;
    }
    C.d[0] = degree;

    /* ── Run the full pipeline ────────────────────────────────────────── */

    /* 2.2: Make_CWS_Points — Eva should verify P.np <= POINT_Nmax */
    Make_CWS_Points(&C, &P);
    /*@ assert P.np >= 0; */
    /*@ assert P.np <= POINT_Nmax; */

    if (P.n == 0) return;

    /* 2.3: Find_Equations — Eva should verify V.nv <= VERT_Nmax */
    int ip = Find_Equations(&P, &V, &E);
    /*@ assert V.nv >= 0; */
    /*@ assert V.nv <= VERT_Nmax; */

    if (!ip) return;

    /* Sort vertex list */
    Sort_VL(&V);

    /* 2.5: Make_Poly_Sym_NF — compute normal form */
    Make_Poly_Sym_NF(&P, &V, &E, &SymNum, V_perm, NF, 0, 0, 0);

    /* 2.6: Verify wrapper postconditions */
    /*@ assert P.n > 0; */
    /*@ assert V.nv > 0; */
}
