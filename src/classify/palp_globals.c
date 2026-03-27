/**
 * palp_globals.c — Definitions for PALP's global I/O symbols.
 *
 * When PALP is compiled as a library (no main() from poly.c etc.),
 * the `inFILE` and `outFILE` symbols have no definition.  This file
 * provides them.  They are set to valid file pointers by palp_init()
 * in palp_api.h.
 */
#include <stdio.h>

FILE *inFILE  = NULL;
FILE *outFILE = NULL;
