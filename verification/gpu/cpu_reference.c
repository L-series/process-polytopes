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

/* One step of the CPU prefix-permutation iterator (static in cws.c). */
int cpu_next_prefix_permutation(long *values, int u) { return NextPrefixPermutation(values, u); }

/* Embeds the row weights as EmitDescriptorCWS does; out_degree[t] and
   out_weights[t][c] receive the CWS rows (the GPU's 10 coordinate columns). */
void cpu_embed(int id, const long weights[CWS_MAX_ROWS][CWS_MAX_ROW_SIZE], const long degrees[CWS_MAX_ROWS],
               long out_degree[CWS_MAX_ROWS], long out_weights[CWS_MAX_ROWS][10]) {
  static CWSDescriptor descriptor;
  static CWS cw;
  Weight w;
  int t, p, c;
  ProjectCWSDescriptor(id, &descriptor);
  cw.nw = 0;
  cw.nz = 0;
  for (t = 0; t < descriptor.row_count; t++) {
    w.N = descriptor.row_sizes[t];
    w.d = degrees[t];
    for (p = 0; p < w.N; p++)
      w.w[p] = weights[t][p];
    EmbedWeightInCWS(&cw, &w, descriptor.coordinate_count, descriptor.mappings[t]);
  }
  for (t = 0; t < descriptor.row_count; t++) {
    out_degree[t] = cw.d[t];
    for (c = 0; c < descriptor.coordinate_count; c++)
      out_weights[t][c] = cw.W[t][c];
  }
}

/* Reads one degree/weight row of a PALP stream (READ_Weight). */
int cpu_read_weight(FILE *in, int *d, int *n, long w[7]) {
  Weight weight;
  int p;
  if (!READ_Weight(&weight, in))
    return 0;
  *d = weight.d;
  *n = weight.N;
  for (p = 0; p < weight.N; p++)
    w[p] = weight.w[p];
  return 1;
}
