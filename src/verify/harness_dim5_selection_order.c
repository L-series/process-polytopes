#include <assert.h>

#define NFmax 10

typedef struct {
    int family_groups[NFmax];
    long indices[NFmax];
} Dim5EnumerationContext;

int nondet_int(void);
long nondet_long(void);
void __CPROVER_assume(int);

static int Dim5SelectionOrderIsCanonical(const Dim5EnumerationContext *context,
                                         int slot) {
    int i;

    for (i = 0; i < slot; i++)
        if ((context->family_groups[i] == context->family_groups[slot]) &&
            (context->indices[i] > context->indices[slot]))
            return 0;
    return 1;
}

void harness(void) {
    Dim5EnumerationContext context;
    int slot = nondet_int();
    int i;
    int has_blocking_predecessor = 0;

    __CPROVER_assume(slot >= 0 && slot < NFmax);

    for (i = 0; i < NFmax; i++) {
        context.family_groups[i] = nondet_int();
        context.indices[i] = nondet_long();
    }

    for (i = 0; i < slot; i++)
        if ((context.family_groups[i] == context.family_groups[slot]) &&
            (context.indices[i] > context.indices[slot]))
            has_blocking_predecessor = 1;

    assert(Dim5SelectionOrderIsCanonical(&context, slot) ==
           !has_blocking_predecessor);
}