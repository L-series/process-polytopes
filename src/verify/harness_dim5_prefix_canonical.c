#include <assert.h>

#define DIM5_MAX_ARITY 5
#define DIM5_MAX_SIMPLEX_SIZE 5

typedef long Long;

typedef struct {
    int simplex_count;
    int shared_counts[DIM5_MAX_ARITY];
    int mappings[DIM5_MAX_ARITY][DIM5_MAX_SIMPLEX_SIZE];
} Dim5StructureDescriptor;

typedef struct {
    int d;
    int N;
    Long w[DIM5_MAX_SIMPLEX_SIZE];
} Dim5WeightEntry;

typedef struct {
    const Dim5StructureDescriptor *descriptor;
    Dim5WeightEntry weights[DIM5_MAX_ARITY];
} Dim5EnumerationContext;

static int Dim5WeightAtCoordinate(const Dim5WeightEntry *W,
                                  const int mapping[DIM5_MAX_SIMPLEX_SIZE],
                                  int coordinate) {
    int i;

    for (i = 0; i < W->N; i++)
        if ((mapping[i] - 1) == coordinate)
            return (int)W->w[i];
    return 0;
}

static int Dim5PrefixIsCanonical(const Dim5EnumerationContext *context,
                                 int slot) {
    int shared_count, i, j;
    const int *mapping = context->descriptor->mappings[slot];

    shared_count = context->descriptor->shared_counts[slot];
    if (shared_count < 2)
        return 1;

    for (i = 0; i < shared_count - 1; i++) {
        int left_coordinate = mapping[i] - 1;

        for (j = i + 1; j < shared_count; j++) {
            int equivalent = 1;
            int right_coordinate = mapping[j] - 1;
            int previous_slot;

            for (previous_slot = 0; previous_slot < slot; previous_slot++) {
                const int *previous_mapping =
                    context->descriptor->mappings[previous_slot];

                if (Dim5WeightAtCoordinate(&context->weights[previous_slot],
                                           previous_mapping,
                                           left_coordinate) !=
                    Dim5WeightAtCoordinate(&context->weights[previous_slot],
                                           previous_mapping,
                                           right_coordinate)) {
                    equivalent = 0;
                    break;
                }
            }

            if (equivalent &&
                (context->weights[slot].w[i] > context->weights[slot].w[j]))
                return 0;
        }
    }

    return 1;
}

static void InitContext(Dim5EnumerationContext *context,
                        Dim5StructureDescriptor *descriptor) {
    int i;

    context->descriptor = descriptor;
    descriptor->simplex_count = 2;

    for (i = 0; i < DIM5_MAX_ARITY; i++) {
        int j;

        descriptor->shared_counts[i] = 0;
        context->weights[i].d = 0;
        context->weights[i].N = 0;
        for (j = 0; j < DIM5_MAX_SIMPLEX_SIZE; j++) {
            descriptor->mappings[i][j] = j + 1;
            context->weights[i].w[j] = 0;
        }
    }
}

void harness(void) {
    Dim5StructureDescriptor descriptor;
    Dim5EnumerationContext context;

    InitContext(&context, &descriptor);

    descriptor.shared_counts[0] = 2;
    context.weights[0].N = 2;
    context.weights[0].w[0] = 5;
    context.weights[0].w[1] = 3;
    assert(Dim5PrefixIsCanonical(&context, 0) == 0);

    context.weights[0].w[0] = 3;
    context.weights[0].w[1] = 5;
    assert(Dim5PrefixIsCanonical(&context, 0) == 1);

    InitContext(&context, &descriptor);
    descriptor.shared_counts[1] = 2;
    context.weights[0].N = 2;
    context.weights[1].N = 2;
    context.weights[0].w[0] = 7;
    context.weights[0].w[1] = 7;
    context.weights[1].w[0] = 9;
    context.weights[1].w[1] = 4;
    assert(Dim5PrefixIsCanonical(&context, 1) == 0);

    context.weights[1].w[0] = 4;
    context.weights[1].w[1] = 9;
    assert(Dim5PrefixIsCanonical(&context, 1) == 1);

    InitContext(&context, &descriptor);
    descriptor.shared_counts[1] = 2;
    context.weights[0].N = 2;
    context.weights[1].N = 2;
    context.weights[0].w[0] = 8;
    context.weights[0].w[1] = 5;
    context.weights[1].w[0] = 9;
    context.weights[1].w[1] = 4;
    assert(Dim5PrefixIsCanonical(&context, 1) == 1);

    InitContext(&context, &descriptor);
    descriptor.shared_counts[1] = 1;
    context.weights[1].N = 2;
    context.weights[1].w[0] = 9;
    context.weights[1].w[1] = 4;
    assert(Dim5PrefixIsCanonical(&context, 1) == 1);
}