#ifndef PROJECT_CWS_CATALOGUE_H
#define PROJECT_CWS_CATALOGUE_H
#include "cws_generate.h"
#if POLY_Dmax < 5
#error "The historical five-dimensional catalogue requires POLY_Dmax >= 5"
#endif
int ProjectCWSDescriptor(int historical_id, CWSDescriptor *descriptor);
#endif
