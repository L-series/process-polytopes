# IP rejection verification on the integration branch

The no-omissions obligation of the IP stage is that a discarded candidate has
a valid non-IP certificate. It does not require the hull construction to find
such a certificate for every non-IP candidate. Inconclusive cases must reach
CPU completion.

## Concrete rejection gate

`device_ip_check` now sets reason 4 only after an independent call to
`device_ip_supporting_certificate` verifies the proposed equation against every
supplied point. The normal must be nonzero, its constant must be nonpositive,
and every equation evaluation must be nonnegative and complete without signed
arithmetic failure. The checked witness is stored in `scratch.rejecting_equation`.
A zero normal, truncated equation list, arithmetic failure or failed certificate
returns an inconclusive result. Rank and initial-incidence failures still have
reasons 1 and 2 for diagnostics; the pipeline must defer both, as well as all
other non-reason-4 failures.

All signed coefficient arithmetic reachable from the serial IP entry point uses
checked addition, subtraction, multiplication, division and remainder. This
includes the local IP copies of extended GCD and basis construction, eliminating
the former dependency on unchecked arithmetic in lattice helpers. Their failure
flag is sticky and is checked before any geometric rejection or acceptance.
The common 32-bit multiplication fast path is proved to fit in signed 64 bits.
The entry checks its point-count indexing bound before evaluating any point.

## Kernel-checked results

Seven `CudaIP*.lean` modules and 35 audited theorem closures establish:

* a nonzero supporting functional with constant at most zero excludes origin IP;
* exact semantics and definedness of the source-linked signed evaluator;
* correctness of the literal overflow guards and the small-multiply fast path;
* the finite equation-scan witness/soundness and sticky-truncation properties;
* exactness of the independent certificate scan, and rejection soundness for
  the supplied finite point family.

The three `generate_ip_*_model.py` scripts recognize restricted source fragments,
check their syntax/body and SHA256 and compare them with the Lean modules. They
reject changes outside the supported fragment. They are a trusted source-linking
frontend, not a verified general C++ translator. `audit_ip_arithmetic.py` uses
Clang's typed AST to reject unchecked signed payload arithmetic in the extracted
27-function IP call graph and missing failure-flag propagation.

The abstract geometric acceptance theorem requires an exact halfspace/hull
coverage invariant. The concrete hull construction has not been refined to that
invariant here. A returned IP acceptance is therefore not claimed to have a full
source-level proof. The independent rejection gate is sufficient for the
no-omissions direction, provided the walk's supplied point family is complete.
That completeness and its invertible coordinate map to the production CWS
subspace are separate pipeline obligations.

## Reproduction

```sh
python3 scripts/test_ip_proofs.py
python3 scripts/audit_ip_arithmetic.py
ASAN_OPTIONS=detect_leaks=0 python3 scripts/test_gpu_ip_source.py --sanitize
ASAN_OPTIONS=detect_leaks=0 python3 scripts/test_gpu_ip_source.py --sanitize --overflow-probe
```

The host harness compiles extracted production functions and tests 2,407 exact
geometric constructions, 50,081 signed arithmetic pairs against a 128-bit oracle,
capacity and zero-normal handling, direct certificate failures and signed-limit
inputs. Every reason-4 witness is independently rescanned in the harness. The
large-basis probe is an invertible shear of an IP cross polytope that forces
inconclusive arithmetic without evaluating overflowing signed operations.
These are host source tests; they are not GPU execution or compiler proofs.
