// CBMC query: for U values (2..5) of any int, one step of the GPU prefix
// permutation iterator equals PALP's NextPrefixPermutation (same result, same
// array). Both loops start from the pool entry's prefix and step until it
// returns 0, so they visit the same sequence of arrangements.
#include "gpu_extract.h"

extern "C" int cpu_next_prefix_permutation(long *values, int u);
int nondet_int();

int main() {
    int gpu[5];
    long cpu[5];
    for (int i = 0; i < U; ++i) {
        gpu[i] = nondet_int();
        cpu[i] = gpu[i];
    }
    const int a = device_next_permutation(gpu, U);
    const int b = cpu_next_prefix_permutation(cpu, U);
    __CPROVER_assert(a == b, "same continuation");
    for (int i = 0; i < U; ++i) __CPROVER_assert(gpu[i] == cpu[i], "same next arrangement");
    return 0;
}
