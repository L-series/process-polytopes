/* The verified CPU generation predicates (PALP cws.c, included unchanged), as a
   plain-C reference for the GPU equivalence queries. integration/cpu-math-verification
   proves these against the Lean model (S3 slot order, S4 shared prefix). */
#define PALP_CWS_NO_MAIN
#include "cws.c"
#include "catalogue.h"

/* PrefixIsCanonical for slot `slot` of historical descriptor `id` with the given
   row weights (positions in descriptor order, shared first). */
int cpu_prefix_is_canonical(int id, int slot, const long weights[CWS_MAX_ROWS][CWS_MAX_ROW_SIZE]) {
  static CWSDescriptor descriptor;
  static CWSEnumerationContext context;
  int t, p;
  if (!ProjectCWSDescriptor(id, &descriptor))
    return -1;
  context.descriptor = &descriptor;
  for (t = 0; t < descriptor.row_count; t++) {
    context.weights[t].N = descriptor.row_sizes[t];
    for (p = 0; p < descriptor.row_sizes[t]; p++)
      context.weights[t].w[p] = weights[t][p];
  }
  return PrefixIsCanonical(&context, slot);
}

/* CWSGenerate orders slots i < j by stream index exactly when they read the same
   pool stream and the transposition (i j) is a descriptor automorphism
   (cws.c, context.exchange). The project front end (src/cws/main.c) gives every
   slot of the same row size the same stream. */
int cpu_slot_exchange(int id, int i, int j) {
  CWSDescriptor descriptor;
  int permutation[CWS_MAX_ROWS], p;
  if (!ProjectCWSDescriptor(id, &descriptor))
    return -1;
  if (descriptor.row_sizes[i] != descriptor.row_sizes[j])
    return 0;
  for (p = 0; p < descriptor.row_count; p++)
    permutation[p] = p;
  permutation[i] = j;
  permutation[j] = i;
  return CWSDescriptorAutomorphism(&descriptor, permutation);
}
