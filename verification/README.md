# Verified models and native kernels

These Dafny files contain recovery models and the sources of small executable recovery, shared parsing and long-value descriptor kernels, together with verification-only writer lifecycle and page-frame ownership specifications. Only their reviewed generated C# ships in JetDatabaseWriter; it adds no runtime support or package reference. The path-selected `formal` job in [ordinary CI](../.github/workflows/ci.yml) verifies the complete manifest, checks the trust policy and generated freshness, and retains `formal-verification` diagnostics. Proof-review freshness runs unconditionally in the independent `checks` job. Expensive lanes can inherit a validated main ancestor only when their inputs are unchanged. A failure in the other lane does not invalidate completed successful checks for those inputs; every job in the inherited lane and both common checks must pass. Manual and release runs select every lane. See [CI gating](../AGENTS.md#gating-a-commit-on-ci).

Read the [contract and trusted boundary](../docs/design/formal-verification.md) before interpreting a theorem. The [open plan](../PLAN.md#v--formal-verification-and-stronger-guarantees) tracks implementation refinement, actual storage qualification and later proof work.

The [proof and algorithm survey](../docs/design/proof-survey.md) records recent research, adoption decisions and concrete production targets. Its proof-search and coverage suggestions complement the pinned verifier, audit and negative controls below; they do not replace them.

## Scope

| Model | Main checked properties | Deliberate boundary |
|---|---|---|
| [UndoJournal.dfy](UndoJournal.dfy) | Logical and buffered journal traces preserve exact undo coverage, durable acknowledgement and terminating recovery under explicit persistence contracts. Aligned multi-page preparation derives padded original payloads and complete removed-tail coverage. Content admission composes recorded gap intents, torn writes and replay mixtures under the zero-exposure contract. Serialized inventory proofs derive exact map membership/source positions, consume exact saved ranges with the production replay rank and construct complete restoration. | Serialized acquisition, checksums and payload identity are explicit contracts. Successful modeled effects do not verify returned C# I/O or physical persistence. Unrestricted pre-marker reordering can preserve undo while requiring parser refusal; the abstract completion schedule and full production algorithm remain distinct. |
| [ReplacementProtocol.dfy](ReplacementProtocol.dfy) | Bounded header/decision classification refines admission: incomplete decisions retain Prepared and complete invalid evidence refuses. Bounded source reads, torn writes, flush outcomes and retries refine staging traces. Successful-flush publication and held-generation authority compose selected copy recovery and finite crash/reacquisition traces. A separate production-order cleanup refinement retains the destination lease through native/receipt/intent deletion, preserves selection across restart and constructs a decreasing successful cleanup schedule. | Byte/copy/authority composition is verification-only. Digest/path inputs, validated immutable copies, exclusive I/O and contiguous-prefix staging persistence are explicit contracts. Non-prefix files can refuse. Exact .NET/I/O and bootstrap correspondence, namespace operations and metadata remain separate; the abstract publication action is broader than the refined copy gate. |
| [GenerationOwnership.dfy](GenerationOwnership.dfy) | Exact-generation deletion authority and finite acquisition/crash/cleanup traces preserve successor names. Staged native/receipt bootstrap refuses incomplete authority, retains failed preparation evidence, reconstructs absent native evidence from an exact receipt and constructs successful acquisition/cleanup schedules. | Fresh generation/object tokens, exact validated evidence, persistence fences and exclusive lease semantics are assumed. Receipt parsing, GUID collision resistance, actual OS locks, namespace durability and arbitrary namespace attackers are outside the model. Journal cleanup is a manual correspondence point rather than a separately modeled name. |
| [RecoveryKernel.dfy](RecoveryKernel.dfy) | Total native functions prove overflow-safe shared byte ranges, exact-read cursor progress, bounded journal framing/cursors, original-page and hash/payload slices, truncate-tail schedules, record classification, byte authorization and inventory-bound replay source selection. Generated array scans compare every replacement header/decision field and refine byte admission; generated length decoding and positive-read cursor controls preserve exact bounds. | Production invokes generated C# from `PersistentRollbackJournal`, `EncryptionReplacementIntent`, `BinaryBuffer`, shared `StreamRead`, pointer readers and compound-file/complex byte-range admission. Exact read effects, SHA-256/path computation, successful write/resize effects, ownership receipts and filesystem operations remain explicit contracts. Complete C# recovery loops and physical durability are not proved. |
| [WriterLifecycle.dfy](WriterLifecycle.dfy) | Serialized writer/transaction/lease states preserve terminal scope identities, seven-level nesting/five-level writes, nested snapshots, statement rollback, bounded cache generations and idempotent disposal. A decreasing schedule releases ownership after resolving active scopes. | Verification-only specification under successful logical snapshot/restore effects and exclusive generation authority. Focused production traces supplement manual correspondence; OS locking, physical persistence, concurrent C# execution and cache pins remain separate. |
| [PageFrameOwnership.dfy](PageFrameOwnership.dfy) | Bounded frame references equal producer/cache ownership plus live lease tickets. Arbitrary admitted traces preserve immutable bytes, no premature pool return, one final return, idempotent disposal, overflow refusal and generation-checked producer-to-cache handoff. Retained and unretained production-shaped traces establish reachable completion. | Verification-only ownership ledger. Actual frame/array identity, CAS/cache-lock linearization, allocation and producer transfer, pooling, decoder discipline and full reader/lifecycle composition require manual correspondence and focused tests; see [page-frame ownership](../docs/design/page-frame-ownership.md). |
| [MultiProcessOwnership.dfy](MultiProcessOwnership.dfy) | Arbitrarily many clients interleave queued acquisition, stale observation, staged bootstrap, recovery, cleanup, independent crash/restart and retries. Unbounded trace invariants retain current-ticket authority and exclusive cleanup; checked counterexamples expose early coordinator release and stale pathname deletion. | Atomic canonical coordination, exact validated evidence, fresh tickets/generations, successful selected-image fences and crash quiescence are explicit contracts. Missing-destination composite acquisition, ordinary held-handle disposal, OS exclusion, deadlock prevention and physical persistence remain separate. |
| [LongValueKernel.dfy](LongValueKernel.dfy) | Complete 12-byte descriptor acquisition/serialization with exact field inverses, overflow-safe ranges, atomic refusal and preservation outside the descriptor. Generated payload-admission priority and 24-bit page/8-bit row-pointer codecs have exact refusal and inverse laws. Array lookup/idempotent add/rehash prove exact nonzero membership under the probe-chain contracts, occupied-slot progress and unchanged duplicate/full outcomes. | Production uses bounded generated array methods and allocation-free span adapters. Reserved modes survive raw acquisition and refuse payload admission. The handwritten visited count/allocation wrapper must establish generated entry contracts; containing traversal/resources, native pointer/token meaning and format compatibility remain separate. |
| [LongValueTraversal.dfy](LongValueTraversal.dfy) | Selected external chained-payload traversal terminates with distinct visited identities, decreasing row/payload budgets, bounded output and aggregate geometric uint-table allocations. Bounded circular probes cover every slot. | Production invokes generated admission, count, capacity and probe decisions. Generated array lookup/insertion/rehash establish exact nonzero membership under the probe-chain invariant; count/allocation-wrapper and containing-parser composition remain manual obligations. Locator/cache/pool, destination and CLR overhead, returned I/O effects and native ownership remain separate contracts. |

The production-order replay selector shares the conditional-marker, capture, resize, flush and decision schedule with both C# replay paths. Exact serialized-inventory preflight precedes replay mutation; generated source selection ties the decreasing control rank to admitted membership and the exact original payload prefix. Successful page effects have scoped restoration proofs, while actual I/O effects and cryptographic identity remain contracts. The replacement publication-fence refinement composes a successful source-copy flush with selected-destination flushing, copy removal and leased cleanup; generated field scans and focused refusal traces bind record admission before bootstrap without granting ownership from record validity alone.

The independent [LongValueKernel.dfy](LongValueKernel.dfy) module verifies the complete bounded descriptor representation, including both opaque pointer/token words. Production array reads and writes execute generated bounds/access loops; span adapters use generated range and field functions. Invalid lengths/storage classes refuse before destination mutation, and raw reserved modes remain available for preservation while payload interpretation refuses them. The 24-bit page/8-bit row-pointer encoder refuses identities that would otherwise alias after narrowing. These codec properties do not establish native token interpretation, full chain/resource safety or Microsoft interoperability.

The proofs quantify over arbitrary finite images/traces; they are not an enumeration of selected database sizes. They also do not cover all possible actions of a real filesystem: each theorem applies only to its declared transition relation. Safety through arbitrary finite interruptions does not promise termination during infinitely many failures. Replacement's progress theorem constructs a successful abstract path; it does not bound production syscalls or prove a scheduler fair.

The undo model intentionally permits arbitrary changes within protected write intervals when proving recoverability. Separate byte-authorization lemmas constrain original/after-image mixtures and zero-tail replay. Authorization by itself does not establish correct undo coverage. Counterexample lemmas demonstrate why missing write/tail protection or missing recovery authorization matters.

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

The entry point requires an exact manifest of every `.dfy` in the checkout, a matching pinned Dafny/Z3 version, a positive complete-inventory verification count with zero errors, and a clean Dafny audit. All manifest sources are normal verified inputs in one invocation, using at most four verifier cores; shared dependencies are checked once in that invocation. Both native-kernel translations separately verify their declared normal-source dependencies. Its conservative source policy also rejects trust bypasses, includes and unreviewed attributes. The policy is a restricted pilot-language check, not a second Dafny parser. Dafny parsing/verification/auditing remain required.

Mandatory negative controls run on every verification invocation: an intentionally false theorem must fail verification, and an intentionally assumed theorem must produce an audit finding even when Dafny's audit process exits successfully. These control files and findings live only in the external result directory. They are expected failures and are never proof inputs admitted by the manifest.

`summary.json` records proof and generated-source hashes, tool/solver versions, verification summaries, zero audit findings and the negative-control outcome. Each proof retains its exact source hash and references the shared `inventory.verify.stdout.log`; `inventory.audit.txt` covers the complete inventory. Per-process `*.process.json` records invocations and outcomes. A genuine failed attempt retains `failure.json` and exact proof/dependency, tool-manifest and generated-source copies under `inputs/`; expected negative-control failures are distinguished from that gate failure. CI retains separate `formal-verification` and `proof-trust-review` artifacts for 30 days, including refusing review reports and failed verifier diagnostics. Per-process stdout/stderr and audit reports explain failures. A successful receipt establishes the checked models and exact generated-kernel freshness, not refinement of the remaining handwritten implementation.

[Proof and trust review records](../docs/design/proof-trust-review.md) bind mapped sources, conformance evidence, proof inputs, pins and gate tooling to reviewed content. The mandatory CI job checks their freshness and retains review refusals and failed proof inputs alongside verifier/auditor diagnostics. A fresh record does not certify reviewer judgment or qualify storage effects.

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
    -ProofCoverageSources verification/LongValueKernel.dfy
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

## Executable kernel generation

The manifest's `dependencies` map lists earlier proof sources required by an importing file. They are supplied as normal inputs to verification, audit and translation, so their bodies are checked with the same options. No `--library` trust shortcut is used.

After reviewing a kernel source change, run the command above with `-UpdateGeneratedSource`. The default command and CI independently refuse missing or edited `src/JetDatabaseWriter/Verification/RecoveryKernel.g.cs` and `LongValueKernel.g.cs`. The schema-version-2 receipt records exact source hashes in `proofs` and both generated-output hashes in `executableKernels`. Generation first completes the proof/trust checks, then translates with the pinned compiler and solver into the external result directory. Raw C# is retained for review.

The projection accepts only the reviewed `RecoveryKernel` and `LongValueKernel` module names. It keeps each final compiled namespace, internalizes and renames its generated class, and updates generated self references to that type. It retains method bodies, native arithmetic and bounded primitive-array operations. Unused compiler scaffolding, embedded source attributes, abstract-model code and native-type descriptors are omitted. The projection rejects an unexpected module shape or references to omitted runtime/model support. Only the reviewed `nativeType` representations `ulong`, `uint` and `byte` are allowed in addition to proof hints.

No Dafny runtime source is bundled and no Dafny license-bearing runtime material is redistributed. The development compiler is the upstream MIT-licensed [Dafny distribution](https://github.com/dafny-lang/dafny/tree/v4.11.0). The compiler, this packaging transformation, the C# compiler and .NET remain trusted; generation does not prove compiler correctness. Focused production tests and the ordinary hosted framework/analyzer/trimming/NativeAOT lanes remain required.

## Review and updates

Follow the [continuing review policy](../docs/design/proof-trust-review.md#continuing-review-policy) whenever a change affects the [scope inventory](../docs/design/verification-scope.md) or its [evidence map](../docs/design/verification-evidence-map.md), including semantic dependencies outside the receipt inputs. Keep concrete unfinished work in the root plan; review maintenance continues under that policy.

- Keep all proof files listed exactly once in [proofs.json](proofs.json), with earlier normal-source dependencies declared explicitly. Do not add assumptions, axioms, bodyless trusted lemmas, disabled verification or unchecked external declarations. Do not relax the audit to accept a finding.
- Review mapped production changes and proof/tool pin changes under the [proof/trust review policy](../docs/design/proof-trust-review.md), updating concrete obligations and conformance evidence before recording new hashes. Source-name additions, removals or moves require boundary classification; the freshness gate does not attest reviewer judgment.
- Keep integrity pins, exact version output and solver paths together in [dafny-toolchain.json](dafny-toolchain.json). The archive hashes cover both Dafny and its distributed solver; downloads come from the upstream `dafny-lang/dafny` release.
- For a tool update, review upstream changes and release provenance, update the platform digests and exact tool/solver outputs together, reinstall into fresh external directories and run verification, audit and both negative controls. Verify the Linux CI distribution as well as any locally used distribution.
- Run the focused script regressions after changing tooling or CI: `pwsh -NoProfile -File scripts/tests/verify-proofs.tests.ps1` and `pwsh -NoProfile -File scripts/tests/formal-workflow.tests.ps1`. The ordinary library CI lane additionally parses every script and runs its regression checks.
- Review the contract, assumptions and source mapping whenever mapped recovery methods change. Kernel freshness detects generated-source drift; broader model-derived conformance and handwritten control-flow correspondence remain open work.
- Preserve ordinary Release/analyzer, framework, native interoperability and crash tests. Formal models strengthen a named layer of evidence; they do not implement missing Access formats or qualify hardware durability.
