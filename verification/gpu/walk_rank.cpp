// Full parity domain: all 5x10 binary matrices, with a symbolic nonzero row
// combination. Accepted rank certificates must have no vanishing combination.
extern "C" unsigned int nondet_uint();
extern "C" void __CPROVER_assume(bool);
extern "C" void __CPROVER_assert(bool, const char *);
int main() {
    DeviceCwsCandidate candidate;
    candidate.nw = 0; // isolate rank checker; kernel-dot validation is separate
    candidate.ambient_vertices = 10;
    int basis[5][10];
    for (int r = 0; r < 5; ++r) for (int a = 0; a < 10; ++a)
        basis[r][a] = nondet_uint() & 1u;
    const int accepted = device_walk_basis_certificate(candidate,basis);
    unsigned int combination = nondet_uint() & 31u;
    __CPROVER_assume(combination != 0);
    bool nonzero = false;
    for (int a = 0; a < 10; ++a) {
        unsigned int value = 0;
        for (int r = 0; r < 5; ++r)
            value ^= ((combination >> r) & 1u) & static_cast<unsigned int>(basis[r][a]);
        nonzero = nonzero || value != 0;
    }
    __CPROVER_assert(!accepted || nonzero, "accepted rank certificate implies independent rows modulo two");
#ifdef WALK_NONVACUITY
    __CPROVER_assert(!accepted, "nonvacuity: a full-rank certificate is reachable");
#endif
}
