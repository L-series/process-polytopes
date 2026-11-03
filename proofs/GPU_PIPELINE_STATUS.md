# GPU correctness checkpoint

Branch: `integration/cws-5d-gpu`, continued from `73df0cb`.

**The full source-level no-omissions proof is not complete.** The integration
adds substantial proved models, checked runtime contracts and actual-source
verification. None of the finite tests below should be described as a proof of
all CUDA executions. No production GPU run was launched in this session.

## Implemented and verified

* Dimension-independent join rejection still uses the production `CWS.IsIP`.
  New Lean results cover arbitrary-length binary searches and exact packed
  ranges/lookups, explicit continuation and row-frame traversal, exact weighted
  enumeration, and depth-indexed stack decoding. The mutable matcher is checked
  against all supported arities/coordinate mappings and full machine payloads.
  See [JOIN_REJECTION.md](JOIN_REJECTION.md).
* Both join builders check row-point/fibre summaries, index permutations, plan
  masks and filter coverage. Unrepresentable shared values/counts/index tables
  mark the affected entry uncovered; candidates involving it remain inconclusive
  and reach CPU deferral. Resource/build failures stop the run visibly.
* The walk checks exact basis arithmetic, kernel membership, rank, arithmetic
  budgets, valid final points and distinct reduced tuples. It returns reduced
  coordinates directly. Lean connects complete valid distinct point families
  and invertible kernel coordinates to production IP. The source's mod-2 rank
  certificate passes CBMC for all 5x10 parity matrices. See
  [GPU_WALK_VERIFICATION.md](GPU_WALK_VERIFICATION.md).
* The IP call graph uses checked signed arithmetic. Only reason 4 can reject;
  an independent checked scan requires a nonzero normal and a valid one-sided
  equation on every supplied point. Rank/incidence/arithmetic/list failures
  defer. Source arithmetic and certificate models are checked against Lean;
  the typed arithmetic audit covers every reachable IP function. See
  [IP_VERIFICATION.md](IP_VERIFICATION.md).
* Chunk-local identity tags and atomic ownership transitions audit compaction,
  both sorts, terminal outcomes and output payloads. Dropped, duplicated,
  out-of-range or changed records prevent a completion receipt. Lean proves the
  corresponding output-audit model gives coverage and unique identities.
  Host shard boundaries use the proved nonoverflowing quotient/remainder formula;
  host allocation/counter arithmetic is checked and launch stride bounds enforced.
* The native CPU fallback uses exact PALP operations, assertions and UB traps.
  Every deferred row must receive a decision and a completion footer; inputs
  remain intact. Chunk receipts support CPU retries. Missing output, partial
  processing, changed inputs or missing shards prevent execution completion.
* Production orchestration covers exactly 53 jobs: all combined structures
  2..47, with structure 3 split into eight shards on eight exclusive devices.
  Every job includes CPU completion, and the final audit requires all receipts.
  Structure 1 was already completed according to the user. Instructions:
  [PRODUCTION_GPU_RUN.md](../docs/PRODUCTION_GPU_RUN.md).

## Validation performed

* Lean builds and foundational-axiom audits for join/search/stack, walk, IP and
  pipeline proofs; no proof placeholders or nonfoundational axioms in the
  audited theorem closures.
* Generation: 5,871,755 selected-pool entries, 149 canonical-prefix slots,
  12,528,582 emission order-type cases (108,732,951 candidates), and 17,424
  fused/general generation cases; the existing source-linked CBMC checks pass.
* Join: 1,203,052 exhaustive weighted table cases; independent point-oracle
  comparisons for all 46 descriptors; bounded actual traversal/range checks;
  full supported-arity matcher CBMC, nonvacuity and mutation checks.
* Walk: 250,405 checked arithmetic operations and 184 exact point-set/certificate
  cases across all 46 descriptors; address/undefined-behavior sanitizers.
* IP: 2,407 geometric cases, 50,081 arithmetic pairs, overflow probes and
  independently rescanned rejecting witnesses; sanitizers and typed AST audit.
* Routing: 2,574 source-kernel cases, including exhaustive small compactions and
  permutations, changed records, missing/duplicate identities and buffer bounds.
* Host bounds: 200,000 full-width arithmetic/shard cases checked against
  `unsigned __int128`, including maximum counts and key-width edges.
* CPU/job completion: 12 tests with the native driver; production scheduling:
  nine tests, including a 53-job end-to-end mock through the real runners/auditor.
* Both standard and fused CUDA configurations compile for `sm_75`. These are
  compile checks, not execution tests on the intended production GPUs.

Run `scripts/test-gpu-correctness.sh` with CBMC/goto-cc and a suitable CUDA compiler
on PATH to reproduce the complete regression suite. Set `NVCC`, `NVCCFLAGS` and
`CUDA_ARCH` for the installed toolkit/target. The combined command was added as
an entry point; individual checks were run during integration, rather than
claiming a separate completed invocation of the entire combined command.

## Remaining formal boundaries

1. The joint simulation from actual join packed-pointer iterators, chosen-key
   arrays and mutable values into the proved frame/slot model. Abstract stack,
   search and local mutation arguments are proved, but their complete concrete
   source-state coupling is still an obligation. The three-row/two-key symbolic
   traversal query timed out at 180 seconds; smaller queries and host exhaustion
   do not discharge an unbounded source proof.
2. Concrete host row enumeration/table uploads and walk validators/sorting must
   be formally refined to their Lean premises. Runtime validation removes
   reliance on floating-point enumeration completeness and basis saturation,
   but the implementation of those checks is not wholly kernel verified.
3. The finite generation order-type check has a reviewed comparison-only
   reduction to arbitrary positive weights; that reduction is not yet a Lean
   theorem about the complete source. CUDA atomics/compiler/memory semantics
   and the OS/filesystem remain implementation trust boundaries. CUB results
   are checked by identities rather than assumed correct.
4. Concrete hull acceptance is not formally refined. This completeness-focused
   work establishes sound rejection contracts; final GPU acceptance proof and
   a full implementation proof of the PALP CPU reference remain separate.
5. Actual production execution, performance, the supplied pool catalogue,
   structure-1 artifacts and all eight GPU/shard receipts must still be audited.
   The historical candidate-count table is a regression reference tied to its
   original inputs, not a mathematical completeness theorem or a newly measured
   baseline for different pools.
