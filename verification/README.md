# Verified models and native kernels

These Dafny files contain recovery models and the sources of executable recovery, shared parsing, long-value descriptor, row-compaction, index-emission and usage-enumeration kernels, together with verification-only writer lifecycle and page-frame ownership specifications. Only their reviewed generated C# ships in JetDatabaseWriter; it adds no runtime support or package reference. The path-selected `formal` job in [ordinary CI](../.github/workflows/ci.yml) verifies the complete manifest, checks the trust policy and generated freshness, and retains `formal-verification` diagnostics. Proof-review freshness runs unconditionally in the independent `checks` job. Expensive lanes can inherit a validated main ancestor only when their inputs are unchanged. A failure in the other lane does not invalidate completed successful checks for those inputs; every job in the inherited lane and both common checks must pass. Manual and release runs select every lane. See [CI gating](../AGENTS.md#gating-a-commit-on-ci).

Read the [contract and trusted boundary](../docs/design/formal-verification.md) before interpreting a theorem. The [open plan](../PLAN.md#v--formal-verification-and-stronger-guarantees) tracks implementation refinement, actual storage qualification and later proof work.

The [proof and algorithm survey](../docs/design/proof-survey.md) records recent research, adoption decisions and concrete production targets. Its proof-search and coverage suggestions complement the pinned verifier, audit and negative controls below; they do not replace them.

## Layout

Proof directories mirror the owning subsystem under `src/JetDatabaseWriter`:

```text
verification/
  config/             Proof inventory, review records and pinned toolchain
  Encryption/         Replacement protocol
  LongValues/         Descriptor/visited-set kernel and traversal model
  Pages/              Row compaction and usage-map enumeration kernels
    Paging/           Undo journal, recovery kernel and page-frame ownership
  Indexes/            Index page emission and monotone relocation controls
  Transactions/       Generation/process ownership and writer lifecycle
  README.md
```

Place new proofs beside related models using the production directory names,
adding nested directories as those subsystems grow. Keep executable kernels and
verification-only specifications together by subsystem. `RecoveryKernel` remains
under `Pages/Paging`, its original journal/recovery owner, although its generated
primitives also serve shared parsing and other subsystems. Generated C# stays in
`src/JetDatabaseWriter/Verification`; manifests use repository-relative paths.

## Scope

`RecoveryKernel` also supplies complete bounded array operations for data-page
slot acquisition (`ReadDataRowSlot`), inline/reference usage-bitmap admission
and bit reads/updates, and index entry-boundary/sentinel scans
(`FindIndexEntryStart`, `CheckIndexLeafBitmap`). Their contracts establish exact
slot intervals, atomic bit refusal/preservation and strict scan progress. The
[page-operation boundary](../docs/design/formal-verification.md#page-parsing-and-bitmap-operations)
records production callers and the remaining containing-parser, ownership and
native-format obligations. These operations use the existing native projection
and introduce no additional executable module or shipping dependency.

The same module supplies Jet3 jump-table controls, intermediate-index seek
admission/selection and complete journal page-byte authorization scans. The
[expanded boundary](../docs/design/formal-verification.md#row-jumps-index-seek-control-and-journal-content-scans)
distinguishes generated execution from the span-load/store, vectorized comparison,
record-acquisition and I/O contracts retained by their production adapters.

| Model | Main checked properties | Deliberate boundary |
|---|---|---|
| [DataPageKernel.dfy](Pages/DataPageKernel.dfy) | Complete row-compaction admission, minimum/next boundary selection and exact directory/free-space rewriting with preservation outside written metadata. Modeled overlapping move/clear preserves surviving bytes. | Production retains optimized CLR bulk effects, stable exclusive page/layout identity and containing mutation/rollback contracts. Row-space compaction grants no page-release authority. |
| [IndexMutationKernel.dfy](Indexes/IndexMutationKernel.dfy) | Maximal shared-prefix scan, bounded entry capacity/progress, exact header/bookmark/child/bitmap emission and page finalization. Monotone relocation admission and binary-search controls preserve injectivity, order, exact membership and interval halving. | Key-suffix bulk copies, stable acquired entry lists, supplied layouts and containing tree/order/ownership/edit composition remain explicit contracts. |
| [UsageMapKernel.dfy](Pages/UsageMapKernel.dfy) | Allocation-free byte-mask scanning with exact members, bounded byte progress, tolerant skipping and strict in-byte prefix refusal. | Faithful ordered mask emission before terminal handling, reference pointer/page acquisition, cancellation/pool effects and complete parser/ownership/resource composition remain separate. |
| [UndoJournal.dfy](Pages/Paging/UndoJournal.dfy) | Logical and buffered journal traces preserve exact undo coverage, durable acknowledgement and terminating recovery under explicit persistence contracts. Aligned multi-page preparation derives padded original payloads and complete removed-tail coverage. Content admission composes recorded gap intents, torn writes and replay mixtures under the zero-exposure contract. Serialized inventory proofs derive exact map membership/source positions, consume exact saved ranges with the production replay rank and construct complete restoration. | Serialized acquisition, checksums and payload identity are explicit contracts. Successful modeled effects do not verify returned C# I/O or physical persistence. Unrestricted pre-marker reordering can preserve undo while requiring parser refusal; the abstract completion schedule and full production algorithm remain distinct. |
| [ReplacementProtocol.dfy](Encryption/ReplacementProtocol.dfy) | Bounded header/decision classification refines admission: incomplete decisions retain Prepared and complete invalid evidence refuses. Bounded source reads, torn writes, flush outcomes and retries refine staging traces. Successful-flush publication and held-generation authority compose selected copy recovery and finite crash/reacquisition traces. A separate production-order cleanup refinement retains the destination lease through native/receipt/intent deletion, preserves selection across restart and constructs a decreasing successful cleanup schedule. Its authority invariant constructs completion after any finite admitted cleanup/restart trace. | Byte/copy/authority composition is verification-only. Digest/path inputs, validated immutable copies, exclusive I/O and contiguous-prefix staging persistence are explicit contracts. Non-prefix files can refuse. Exact .NET/I/O and bootstrap correspondence, namespace operations and metadata remain separate; the abstract publication action is broader than the refined copy gate. |
| [GenerationOwnership.dfy](Transactions/GenerationOwnership.dfy) | Exact-generation deletion authority and finite acquisition/crash/cleanup traces preserve successor names. Staged native/receipt bootstrap refuses incomplete authority, retains failed preparation evidence, reconstructs absent native evidence from an exact receipt and constructs successful acquisition/cleanup schedules. | Fresh generation/object tokens, exact validated evidence, persistence fences and exclusive lease semantics are assumed. Receipt parsing, GUID collision resistance, actual OS locks, namespace durability and arbitrary namespace attackers are outside the model. Journal cleanup is a manual correspondence point rather than a separately modeled name. |
| [RecoveryKernel.dfy](Pages/Paging/RecoveryKernel.dfy) | Total native functions prove overflow-safe shared byte ranges, exact-read cursor progress, bounded journal framing/cursors, original-page and hash/payload slices, truncate-tail schedules, record classification, byte authorization and inventory-bound replay source selection. Generated array scans compare every replacement header/decision field and refine byte admission; generated length decoding and positive-read cursor controls preserve exact bounds. Outward numeric/signed-zero seek decisions preserve original matches under explicit comparison/order contracts. | Production invokes generated C# from `PersistentRollbackJournal`, `EncryptionReplacementIntent`, `BinaryBuffer`, shared `StreamRead`, pointer readers and compound-file/complex byte-range admission. Exact read effects, SHA-256/path computation, successful write/resize effects, ownership receipts and filesystem operations remain explicit contracts. Complete C# recovery loops and physical durability are not proved. |
| [WriterLifecycle.dfy](Transactions/WriterLifecycle.dfy) | Serialized writer/transaction/lease states preserve terminal scope identities, seven-level nesting/five-level writes, nested snapshots, statement rollback, bounded cache generations and idempotent disposal. A decreasing schedule releases ownership after resolving active scopes. | Verification-only specification under successful logical snapshot/restore effects and exclusive generation authority. Focused production traces supplement manual correspondence; OS locking, physical persistence, concurrent C# execution and cache pins remain separate. |
| [PageFrameOwnership.dfy](Pages/Paging/PageFrameOwnership.dfy) | Bounded frame references equal producer/cache ownership plus live lease tickets. Arbitrary admitted traces preserve immutable bytes, no premature pool return, one final return, idempotent disposal, overflow refusal and generation-checked producer-to-cache handoff. Retained and unretained production-shaped traces establish reachable completion. | Verification-only ownership ledger. Actual frame/array identity, CAS/cache-lock linearization, allocation and producer transfer, pooling, decoder discipline and full reader/lifecycle composition require manual correspondence and focused tests; see [page-frame ownership](../docs/design/page-frame-ownership.md). |
| [MultiProcessOwnership.dfy](Transactions/MultiProcessOwnership.dfy) | Arbitrarily many clients interleave queued acquisition, stale observation, staged bootstrap, recovery, cleanup, independent crash/restart and retries. Unbounded trace invariants retain current-ticket authority and exclusive cleanup; checked counterexamples expose early coordinator release and stale pathname deletion. | Atomic canonical coordination, exact validated evidence, fresh tickets/generations, successful selected-image fences and crash quiescence are explicit contracts. Missing-destination composite acquisition, ordinary held-handle disposal, OS exclusion, deadlock prevention and physical persistence remain separate. |
| [LongValueKernel.dfy](LongValues/LongValueKernel.dfy) | Complete 12-byte descriptor acquisition/serialization with exact field inverses, overflow-safe ranges, atomic refusal and preservation outside the descriptor. Generated payload-admission priority and 24-bit page/8-bit row-pointer codecs have exact refusal and inverse laws. Array lookup/idempotent add/rehash prove exact nonzero membership under the probe-chain contracts, occupied-slot progress and unchanged duplicate/full outcomes. `PrepareVisitedAdd` and `CompleteVisitedGrowth` compose count/growth decisions and array operations while preserving `TrackedVisited`'s exact count, distinct membership, half occupancy and capacity bound. Operation-specific native decisions have exact classification/payload laws and preserve the abstract transition through contextual retained-count projections; null initial storage and existing empty arrays are admitted. | Production uses bounded generated array methods and allocation-free span adapters. Reserved modes survive raw acquisition and refuse payload admission. CLR allocation, matching array/count retention and adapter sequencing remain explicit effects; containing traversal/resources, native pointer/token meaning and format compatibility remain separate. |
| [LongValueTraversal.dfy](LongValues/LongValueTraversal.dfy) | Selected external chained-payload traversal terminates with distinct visited identities, decreasing row/payload budgets, bounded output and aggregate geometric uint-table allocations. Bounded circular probes cover every slot. Array-backed traversal invokes the controllers and proves quadratic probe/source-iteration and peak old/new payload bounds. | Production invokes generated admission, count, capacity and probe decisions. Generated visited-set orchestration composes exact array membership with count/growth bookkeeping. Allocation/adapter effects and containing-parser composition remain manual obligations. Public C# resource correspondence/cancellation, locator/cache/pool, destination and CLR overhead, returned I/O effects and native ownership remain separate contracts. |

The production-order replay selector shares the conditional-marker, capture, resize, flush and decision schedule with both C# replay paths. Exact serialized-inventory preflight precedes replay mutation; generated source selection ties the decreasing control rank to admitted membership and the exact original payload prefix. Successful page effects have scoped restoration proofs, while actual I/O effects and cryptographic identity remain contracts. The replacement publication-fence refinement composes a successful source-copy flush with selected-destination flushing, copy removal and leased cleanup; generated field scans and focused refusal traces bind record admission before bootstrap without granting ownership from record validity alone.

The independent [LongValueKernel.dfy](LongValues/LongValueKernel.dfy) module verifies the complete bounded descriptor representation, including both opaque pointer/token words. Production array reads and writes execute generated bounds/access loops; span adapters use generated range and field functions. Invalid lengths/storage classes refuse before destination mutation, and raw reserved modes remain available for preservation while payload interpretation refuses them. The 24-bit page/8-bit row-pointer encoder refuses identities that would otherwise alias after narrowing. These codec properties do not establish native token interpretation, full chain/resource safety or Microsoft interoperability.

The proofs quantify over arbitrary finite images/traces; they are not an enumeration of selected database sizes. They also do not cover all possible actions of a real filesystem: each theorem applies only to its declared transition relation. Safety through arbitrary finite interruptions does not promise termination during infinitely many failures. Replacement's progress theorem constructs a successful abstract path; it does not bound production syscalls or prove a scheduler fair.

The undo model intentionally permits arbitrary changes within protected write intervals when proving recoverability. Separate byte-authorization lemmas constrain original/after-image mixtures and zero-tail replay. Authorization by itself does not establish correct undo coverage. Counterexample lemmas demonstrate why missing write/tail protection or missing recovery authorization matters.

The supplemental survey transfers execute through the existing kernels:
numeric range/signed-zero admission in `RecoveryKernel` and monotone relocation
admission/search in `IndexMutationKernel`. Their order, exact-membership and
progress proofs retain native conversion and immutable-stream contracts.
`ReplacementProtocol.InterruptedProductionCleanupAllowsCompletion` composes
finite interrupted cleanup traces with generation-bound reacquisition and a
successful continuation. The [formal boundary](../docs/design/formal-verification.md#numeric-seek-admission-and-relocation-ledgers)
names their production adapters, independent regression evidence and remaining
whole-path/native/platform obligations.

## Run locally

Use PowerShell 7 on Windows or Linux x64. The installer downloads an official pinned Dafny distribution, checks its recorded SHA-256 and retains its bundled Z3. Windows Arm64 may execute the x64 distribution through the host's supported emulation; no native Arm64 verifier is claimed. Verifier execution is a development requirement only.

Run from the checkout, choosing a fresh unique directory outside every repository/worktree:

```powershell
$proofRun = Join-Path ([IO.Path]::GetTempPath()) ('jdw-proof-' + [guid]::NewGuid().ToString('N'))
[IO.Directory]::CreateDirectory($proofRun) | Out-Null
pwsh -NoProfile -File scripts/install-dafny.ps1 -Destination (Join-Path $proofRun 'tool')
$executable = if ($IsWindows) { 'dafny/dafny.exe' } else { 'dafny/dafny' }
pwsh -NoProfile -File scripts/verify-proofs.ps1 `
    -DafnyPath (Join-Path $proofRun "tool/$executable") `
    -OutputDirectory (Join-Path $proofRun 'results')
```

Retain useful diagnostics before removing a directory you own. The verifier runs native child processes with an absolute external working directory and a timeout while preserving inherited Windows system/profile variables. It never executes the library test suite or a benchmark.

The entry point requires an exact manifest of every `.dfy` in the checkout, a matching pinned Dafny/Z3 version, a positive complete-inventory verification count with zero errors, and a clean Dafny audit. All manifest sources are normal verified inputs in one invocation, using at most four verifier cores; shared dependencies are checked once in that invocation. All native-kernel translations separately verify their declared normal-source dependencies. Its conservative source policy also rejects trust bypasses, includes and unreviewed attributes. The policy is a restricted pilot-language check, not a second Dafny parser. Dafny parsing/verification/auditing remain required.

Mandatory negative controls run on every verification invocation: an intentionally false theorem must fail verification, and an intentionally assumed theorem must produce an audit finding even when Dafny's audit process exits successfully. These control files and findings live only in the external result directory. They are expected failures and are never proof inputs admitted by the manifest.

`summary.json` records proof and generated-source hashes, tool/solver versions, verification summaries, zero audit findings and the negative-control outcome. Each proof retains its exact source hash and references the shared `inventory.verify.stdout.log`; `inventory.audit.txt` covers the complete inventory. Per-process `*.process.json` records invocations and outcomes. A genuine failed attempt retains `failure.json` and exact proof/dependency, tool-manifest and generated-source copies under `inputs/`; expected negative-control failures are distinguished from that gate failure. CI retains separate `formal-verification` and `proof-trust-review` artifacts for 30 days, including refusing review reports and failed verifier diagnostics. Per-process stdout/stderr and audit reports explain failures. A successful receipt establishes the checked models and exact generated-kernel freshness, not refinement of the remaining handwritten implementation.

[Proof and trust review records](../docs/design/proof-trust-review.md) bind mapped sources, conformance evidence, proof inputs, pins and gate tooling to reviewed content. The mandatory CI job checks their freshness and retains review refusals and failed proof inputs alongside verifier/auditor diagnostics. A fresh record does not certify reviewer judgment or qualify storage effects.

Ordinary inventory verification also retains `inventory.resources.csv` with
per-definition or assertion-batch solver resources and duration. Each proof's
receipt references that shared log. Compare resource costs with the same
toolchain and platform before choosing any additional resource budget; the
existing process and verification timeouts remain mandatory.

## Proof-dependency diagnostics

For a specification review, add `-AnalyzeProofCoverage` to the verification
command above, using a fresh external output directory. Prefer a focused
diagnostic source selection:

```powershell
$coverageRun = Join-Path ([IO.Path]::GetTempPath()) ('jdw-coverage-' + [guid]::NewGuid().ToString('N'))
pwsh -NoProfile -File scripts/verify-proofs.ps1 `
    -DafnyPath (Join-Path $proofRun "tool/$executable") `
    -OutputDirectory $coverageRun `
    -AnalyzeProofCoverage `
    -ProofCoverageSources verification/LongValues/LongValueKernel.dfy
```

The optional mode uses the exact pinned verifier and bundled solver after
mandatory verification, audit, negative controls and generated freshness over
the complete admitted manifest. `-ProofCoverageSources` accepts exact,
case-sensitive repository-relative manifest paths, includes every transitive
normal-source dependency in manifest order, and refuses unknown, duplicate or
escaping paths. A selection without `-AnalyzeProofCoverage` refuses. Omitting
the selection analyzes the complete inventory. Source selection affects only
supplemental diagnostics; it never selects a smaller acceptance gate or enables
diagnostics in CI.

Add `-SuggestProofRefactoring` with `-AnalyzeProofCoverage` to request Dafny's
experimental suggestions and CLI hints. These appear in the retained text and
process output; a run with no suggestions is valid. Suggestions are review
inputs, not permission to weaken a contract or discard a needed assumption.

For either diagnostic mode, `-ProofDiagnosticPosition 288-355` restricts the
supplemental pass to an inclusive line range. A single line (`288`) and open
ranges (`288-` or `-355`) are also accepted. A position requires exactly one
explicitly selected root source in each enabled diagnostic mode. The script
binds the range to that admitted source's normalized absolute path and keeps
its complete normal-source dependency closure. An empty selection refuses;
mandatory inventory verification is never filtered.

The extra invocation isolates assertions, requests contradictory/redundant
assumption warnings and writes dependency text plus HTML coverage. Retain
`proof-coverage.json`, `proof-coverage.dependencies.log`, the `proof-coverage/`
reports and process/stdout/stderr logs. The diagnostic receipt records exact
proof hashes, tool versions, warnings, exit/failure details and
`acceptanceEvidence: false`, the requested source selection and its complete
ordered dependency closure. A warning exit is accepted only with a complete
positive verification summary, zero errors and recognized warning output;
verification errors, timeouts and tool/report failures fail the requested
diagnostic run. Mandatory controls remain required on every invocation.

These experimental Dafny diagnostics can be noisy and expensive on large
proofs. Whole-inventory assertion isolation can produce hundreds of megabytes
of logs and exceed the process budget; retain those failure diagnostics and
use a focused dependency closure for routine review. Review findings against
reachable success/refusal cases and intended
contracts; unused assumptions alone do not establish vacuity, and coverage
does not establish specification adequacy. Keep executable logic and intended
contracts fixed during annotation search, and separately review changes to
either. Optional third-party ProofPulse analysis requires exact-pin and
dependency compatibility qualification before use; see the
[survey](../docs/design/proof-survey.md#specification-adequacy-and-proof-maintenance).

## Proof robustness diagnostics

Use the pinned verifier's seed mutations to find expensive or fragile proofs:

```powershell
$complexityRun = Join-Path ([IO.Path]::GetTempPath()) ('jdw-complexity-' + [guid]::NewGuid().ToString('N'))
pwsh -NoProfile -File scripts/verify-proofs.ps1 `
    -DafnyPath (Join-Path $proofRun "tool/$executable") `
    -OutputDirectory $complexityRun `
    -MeasureProofComplexity `
    -ProofComplexitySources verification/LongValues/LongValueKernel.dfy `
    -ProofComplexityMutations 3 `
    -ProofDiagnosticPosition 288-355
```

Complexity measurement requires explicit manifest sources and between two and
twenty mutations. Sources have the same exact-path and dependency-closure
rules as coverage diagnostics. Complete verification, audit, negative controls
and all generated freshness checks run first. The supplemental command uses
`measure-complexity --mutations`, assertion isolation and the pinned solver;
it never changes canonical source files or runs library benchmarks.

Retain `proof-complexity.json`, `proof-complexity.resources.csv` and the process
and output logs. The receipt records exact source hashes, versions, scope,
requested mutation count and `acceptanceEvidence: false`. Every requested seed
must report the same nonempty set of distinct successful obligations with valid
resource and duration values. Exit failure, missing/duplicate obligations,
empty filters, timeouts and source drift refuse and retain a failure receipt.
The complete mandatory source inventory is checked for drift, including sources
outside the diagnostic closure. Resource variation guides proof maintenance;
it does not establish specification adequacy or production execution costs.

## Executable kernel generation

The manifest's `dependencies` map lists earlier proof sources required by an importing file. They are supplied as normal inputs to verification, audit and translation, so their bodies are checked with the same options. No `--library` trust shortcut is used.

All executable-kernel translations use `--enforce-determinism`. Ghost witness
selection remains proof-only; executable nondeterministic statements and
incomplete initialization refuse. This check supplements verification and
exact generated-source comparison.

After reviewing a kernel source change, run the command above with `-UpdateGeneratedSource`. The default command and CI independently refuse missing or edited generated sources under `src/JetDatabaseWriter/Verification` for every registered kernel: `RecoveryKernel`, `LongValueKernel`, `DataPageKernel`, `IndexMutationKernel` and `UsageMapKernel`. The schema-version-2 receipt records exact source hashes in `proofs` and every generated-output hash in `executableKernels`. Generation first completes the proof/trust checks, then translates with the pinned compiler and solver into the external result directory. Raw C# is retained for review.

The projection accepts only those five reviewed module names, each with an exact source path. It keeps each final compiled namespace, internalizes and renames its generated class, and updates generated self references to that type. It retains method bodies, native arithmetic and bounded primitive-array operations. Unused compiler scaffolding, embedded source attributes, abstract-model code and native-type descriptors are omitted. The projection rejects an unexpected module shape or references to omitted runtime/model support. Only the reviewed `nativeType` representations `ulong`, `uint`, `byte`, `int` and `long` are allowed in addition to proof hints. Signed `int` carries the BCL lexicographic comparison result directly into verified index selection; signed `long` retains the existing usage-map page-number domain, including negative native bases and signed extrema, without narrowing. Both use native CLR arithmetic without a runtime helper. Unknown representations and attribute arguments still refuse.

After rejecting raw runtime references and preexisting inlining metadata, the projection adds one fixed, fully qualified BCL `MethodImplAttribute(AggressiveInlining)` to each of `LongValueKernel.PrepareVisitedAdd` and `CompleteVisitedGrowth`. Each complete declaration and opening brace must match its reviewed signature exactly once; missing, duplicate, overloaded or changed declarations refuse. Generated method bodies remain unchanged. The same exact-signature policy adds one fixed annotation to `UsageMapKernel.NextBitmapPage`, whose admitted byte-batch interface returns the cursor, member mask, status and already computed byte base through primitive output parameters without tuple allocation or per-member range revalidation. Other array loops and `RecoveryKernel` receive no added metadata. The hint requests compiler inlining; hosted measurements qualify its timing effect.

No Dafny runtime source is bundled and no Dafny license-bearing runtime material is redistributed. The development compiler is the upstream MIT-licensed [Dafny distribution](https://github.com/dafny-lang/dafny/tree/v4.11.0). The compiler, the projection including its exact-signature metadata insertion, the C# compiler and .NET JIT/AOT remain trusted; generation does not prove compiler correctness. The BCL metadata adds no shipping dependency. Focused production tests and the ordinary hosted framework/analyzer/trimming/NativeAOT lanes remain required.

Proof hints admit only the exact `trigger`, `induction` and `isolate` names in
addition to the reviewed native representations. Local isolation accepts only
`{:isolate}` and `{:isolate "paths"}`; other arguments and attribute spellings
refuse. Scoped `by` proofs and local `hide`/`reveal` can reduce irrelevant solver
context without changing contracts. Qualify each change with the pinned verifier
and generated freshness; neither opaque blocks nor global assertion isolation
are a default optimization for every proof.

## Review and updates

Follow the [continuing review policy](../docs/design/proof-trust-review.md#continuing-review-policy) whenever a change affects the [scope inventory](../docs/design/verification-scope.md) or its [evidence map](../docs/design/verification-evidence-map.md), including semantic dependencies outside the receipt inputs. Keep concrete unfinished work in the root plan; review maintenance continues under that policy.

- Keep all proof files listed exactly once in [proofs.json](config/proofs.json), with earlier normal-source dependencies declared explicitly. Do not add assumptions, axioms, bodyless trusted lemmas, disabled verification or unchecked external declarations. Do not relax the audit to accept a finding.
- Review mapped production changes and proof/tool pin changes under the [proof/trust review policy](../docs/design/proof-trust-review.md), updating concrete obligations and conformance evidence before recording new hashes. Source-name additions, removals or moves require boundary classification; the freshness gate does not attest reviewer judgment.
- Keep integrity pins, exact version output and solver paths together in [dafny-toolchain.json](config/dafny-toolchain.json). The archive hashes cover both Dafny and its distributed solver; downloads come from the upstream `dafny-lang/dafny` release.
- For a tool update, review upstream changes and release provenance, update the platform digests and exact tool/solver outputs together, reinstall into fresh external directories and run verification, audit and both negative controls. Verify the Linux CI distribution as well as any locally used distribution.
- Run the focused script regressions after changing tooling or CI: `pwsh -NoProfile -File scripts/tests/verify-proofs.tests.ps1` and `pwsh -NoProfile -File scripts/tests/formal-workflow.tests.ps1`. The ordinary library CI lane additionally parses every script and runs its regression checks.
- Review the contract, assumptions and source mapping whenever mapped recovery methods change. Kernel freshness detects generated-source drift; broader model-derived conformance and handwritten control-flow correspondence remain open work.
- Preserve ordinary Release/analyzer, framework, native interoperability and crash tests. Formal models strengthen a named layer of evidence; they do not implement missing Access formats or qualify hardware durability.
