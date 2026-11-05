#define _POSIX_C_SOURCE 200809L
#include "Global.h"
#include <stdint.h>
#include <time.h>

extern int Aided_IP_Check(PolyPointList *, VertexNumList *, EqList *, int, int);
static uint64_t ip, ref, aided, dual, complete;

void __cyg_profile_func_enter(void *fn, void *caller) {
  (void)caller;
  if (fn == (void *)IP_Check)
    ip++;
  else if (fn == (void *)Ref_Check)
    ref++;
  else if (fn == (void *)Aided_IP_Check)
    aided++;
  else if (fn == (void *)Make_Dual_Poly)
    dual++;
  else if (fn == (void *)Complete_Poly)
    complete++;
}

void __cyg_profile_func_exit(void *fn, void *caller) {
  (void)fn;
  (void)caller;
}

__attribute__((destructor)) static void report(void) {
  fprintf(stderr,
          "{\"IP_Check\":%llu,\"Ref_Check\":%llu,\"Aided_IP_Check\":%llu,"
          "\"Make_Dual_Poly\":%llu,\"Complete_Poly\":%llu}\n",
          (unsigned long long)ip, (unsigned long long)ref,
          (unsigned long long)aided, (unsigned long long)dual,
          (unsigned long long)complete);
}
