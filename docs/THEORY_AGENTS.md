# Theory agents

Four project-scoped Codex agent definitions in [`.codex/agents/`](../.codex/agents/)
preserve the reusable prompts for the theory workstream. They use the standalone
TOML format in the
[official subagent documentation](https://learn.chatgpt.com/docs/agent-configuration/subagents).
Each file defines a name, description and developer instructions. Model and
reasoning settings inherit from the parent; these files do not override models,
permissions, tool servers or concurrency settings.

| Agent                   | Approved use                                                 | Owned output                                       |
| ----------------------- | ------------------------------------------------------------ | -------------------------------------------------- |
| `literature_researcher` | One topic in T-L1, T-L2 or T-L3                              | One assigned note in `docs/learning/`              |
| `nl_to_formal`          | T-A0; affected semantic revisions in an approved later chunk | One independent session report outside Git         |
| `formal_to_nl`          | T-A0; affected semantic revisions in an approved later chunk | One blind session report outside Git               |
| `proof_optimizer`       | T-O0, after T-P9 and T-C0                                    | Review findings or explicitly assigned proof edits |

Definitions are permanent instructions, not running agents or approval to start
a chunk. The integrator supplies approved scope and concrete inputs, owns shared
documentation and Git state, and stops after each chunk. Research uses one
instance per approved topic with disjoint note paths. The local, uncommitted
`plan.md` supplies topic briefs and sequencing; the definitions do not require
that planning artifact to be committed.

## Assignments

Give the researcher the chunk, exact topic brief, reviewed frozen source path,
relevant primary-source starting points and one owned note path. Its note must
include precise statements, verified references, Lean support and explicit gaps.
Include the [mathematical roadmap](learning/THEORY_ROADMAP.md) in its assignment:
the separate single-weight paper is a source of reference leads and cross-checks,
not the official manuscript or specification. Give this context to `nl_to_formal`
as well; keep it out of the blind checker's formal packet.

The researcher may also create PDF/TeX cache files in an assigned directory under
[`docs/learning/sources/`](learning/sources/README.md). Check matching cached
versions first, preserve original bytes/provenance and coordinate shared downloads
through the integrator. This is the explicit exception to owning only the topic
note; shared documentation and other agents' files remain integrator-owned.
Later research and prose-to-formal assignments can reuse verified cached papers;
the blind checker cannot access them.

Give `nl_to_formal` the important-declaration inventory, intended prose and
primary sources, frozen Lean source/build evidence and a separate report path.
Give `formal_to_nl` only a prepared packet of neutral declaration IDs, exact
types, binders and necessary definitions/instances, plus its own report path.
Remove informal prose, comments, proof bodies and intended claim labels from
the blind packet; verify it against the frozen source and retain the ID mapping
with the integrator. Neither checker receives the other's report or messages.

Spawn both checkers with fresh contexts (`fork_turns="none"` where supported).
Do not give the blind checker the parent conversation or ask it to read the
plan/learning notes. Freeze both initial reports and record their hashes before
revealing them for comparison. Preserve the initial reports and record
discrepancies, evidence, resolutions and remaining uncertainty in the audit
document. Agreement is evidence, not a correctness certificate; unexplained
disagreement can indicate misformalization, inaccurate prose or missing context.

These roles inherit parent permissions and share its workspace. Blindness comes
from input preparation and access instructions, not a separate filesystem
sandbox. Report accidental exposure and repeat affected blind readings in a
fresh context. If the runtime cannot supply a fresh context, postpone the blind
check rather than presenting an informed reconstruction as independent.

A task tool may automatically inject project intent or prior-audit metadata
even into a new conversation window. A refused/contaminated attempt is retained
as provenance, not counted as a completed blind audit. A supported alternative
is a new noninteractive CLI process in a neutral directory, with no resume/fork,
`--no-custom-instructions`, builtin MCPs disabled and no available/permitted
tools. Prompt-mode memory stays disabled. Supply only the sanitized formal packet
inline and capture the response verbatim; keep intent, ID mappings, sources and
the other report outside that directory. Verify and record the actual isolation
settings and disclosure before accepting the result.

Give `proof_optimizer` the final audited package, pinned dependencies, reconciled
statement inventory, assigned files and a review-only or bounded-edit brief.
The integrator verifies the clean full build and axiom guards before publication.

## Runtime use

In a client supporting project custom agents, request these roles by their
`name` fields. For example, after approval: “Use `literature_researcher` for the
T-L1 weights/WPS topic and its assigned note.” This does not authorize other
topics or chunks.

If the available spawn tool has no custom-role selector, the integrator reads
the corresponding TOML file and passes its complete `developer_instructions`
alongside the bounded assignment to a fresh generic agent. This is an explicit
prompt fallback, not automatic loading of the custom role. Do not start real
research or proof work merely to test configuration discovery.

The definitions have been checked for valid TOML and required fields. Automatic
custom-role discovery has not been verified in this CLI; its spawn interface has
no custom-role selector, so the explicit-prompt fallback above remains necessary.

The approved research batch has produced [seven literature notes and their
LaTeX compilation](learning/README.md). A pinned staging build and round-2
statement reports are archived outside Git; round 1 is superseded because blind
provenance was unconfirmed. The [tracked audit synthesis](proof-audit.md) is
complete, with all source/contract mappings and issue dispositions. Round 2's
attempted blind context received excluded repository metadata, so its report
is qualified historical evidence. The final new neutral CLI reading covers all
71 unchanged statements and closes the historical isolation debt. The accepted
reports were frozen before comparison; the earlier reports remain unchanged.
The foundation port now includes the complete faithful `Basic` and
`ReflexiveDuality` modules, with 26 additional donor theorem bodies imported
and covered by compiler-derived axiom auditing. The weight/box port adds complete
`WeightSystem` and `BoxReduction` modules with 30 further donor results, retaining
their audited scopes. These are faithful ports, not newly isolated semantic
checks.
The faithful `Algorithm`, `WeightSystemFinite` and `GeometricCWS` ports add 21
results and supporting definitions. Reachability remains abstract. The
normalized-IP finiteness proof uses positive-feasibility-guarded nodes and
classical selection; neither supplies executable search. Geometric simplex
IP/reachability retains the explicit affine independence, normalized positive
zero relation and dual
lattice-point witnesses, including lower-dimensional blocks. All three modules
are imported and compiler-audited; no KS obligation or routine is closed.

The complete faithful `CircuitGeometry`, `CircuitInterior` and `CircuitSpan`
ports add 24 theorem/lemma bodies, including the private boundary helper.
Positive simplex circuits span the entire real relation kernel under the
specified intrinsic-interior hypothesis. All three modules are imported and
compiler-audited. The subsequently implemented KS bridges prove primitive
integer normalization and the new circuit contract separately.

The faithful volume/finiteness and minimal-seed ports add `ReflexiveVolume`,
`Finiteness`, `MaxMin`, `Minimal` and `Classification`, with 35 source results.
The weight-finiteness → volume → box/finiteness dependency remains noncircular.
Bounds are existential and reflexive-only. Minimal structure gives one
step; the generic indexed $2d$ bound is now proved separately in `KS/SeedBound`.
Legacy simplex signatures still omit
seed-to-weight witness connections; stronger geometric witnesses are required
for the new contracts. All modules are imported and compiler-audited; no KS
obligation or routine is closed.

The complete faithful `CWSBasis`, `CWSContainer` and `CWSAssembly` ports add
20 source results. They provide independent complete real equations, coordinate
coverage, explicit reached/padded blocks and forward-integral containers from a
finite abstract catalogue. Container maps do not prove inverse integrality;
noncomputable catalogues do not implement primitive integer search or lattice
realizations. All units 3 and 4 are ported and compiler-audited; the new KS
implementation supplies the executable and lattice bridges.

Unit 5 now ports the remaining five donor modules: `DegreeBound`, `Sylvester`,
`PyramidLemma`, `BoxReductionAudit` and `CoverageAudit`. Their 24 source results
and 50 axiom guards are imported and built. Degree bounds remain existential;
no sharp numerical certificate or general pyramid normalization is claimed.
The applicable donor library is fully ported. The requester authorized completing
the remaining proof units as one batch, with bounded independent implementation
ownership and the integrator retaining shared files and publication.

All eighteen KS contracts, seven routines and the concrete endpoint are now
compiler-closed. Strict and conditional gates, native execution suites and exact
CPU regressions pass locally. The historical donor blind-reading debt was
resolved by a new tools-disabled neutral CLI context and explicit index
exports, reusing the unchanged frozen informed report. The new initial report was
frozen before comparison; two low-severity wording qualifications are recorded
in the audit ledger. Do not treat the older contaminated attempt as clean evidence.
Final proof optimization remains deferred to its approved prerequisites.
Use one dependency-aware integrator for coupled work, and delegate only
bounded independent mapping or review with disjoint ownership. The integrator
owns shared documentation, audit integration and publication.
