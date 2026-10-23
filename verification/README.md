# Recovery models and proof checks

These Dafny files contain recovery models and the source of a small executable recovery kernel. Only the kernel's reviewed generated C# ships in JetDatabaseWriter; it adds no runtime support or package reference. The required `formal` job in [ordinary CI](../.github/workflows/ci.yml) verifies the complete manifest, checks the trust policy and generated freshness, and retains `formal-verification` diagnostics.

Read the [contract and trusted boundary](../docs/design/formal-verification.md) before interpreting a theorem. The [open plan](../docs/formal-verification-todo.md) tracks implementation refinement, actual storage qualification and later proof work.

## Scope

| Model | Main checked properties | Deliberate boundary |
|---|---|---|
| [UndoJournal.dfy](UndoJournal.dfy) | Byte preservation lemmas cover torn writes, complete-tail truncation and repeated interrupted replay. The composed logical-record protocol derives exact captures and durable coverage, includes pending appends and uncertain flush/crash prefixes, and proves `CompletedProtocolSelectsBeforeOrCommitted` over arbitrary finite mixed traces. `AcknowledgedImageSurvivesMixedTrace` preserves successful decisions. `CompleteProtocolRecovery` constructs a terminating recovery schedule with a decreasing rank. | Database bytes represent the image surviving at each cut; delayed/reordered independent database buffers are not modeled. Logical record framing, page encoding, parsing/hashing, physical persistence and production-loop refinement remain separate. The abstract schedule always establishes a replay marker; production emits it conditionally for removed original tails. |
| [ReplacementProtocol.dfy](ReplacementProtocol.dfy) | `StepPreserves` and `TracePreserves` establish that each enabled staging/publication/decision/cleanup step preserves an authorized complete image. Finite traces preserve the invariant and committed decisions. `CompleteRecovery` uses a decreasing rank to construct an uninterrupted recovery path to the selected old/new destination. | Starts after valid Prepared evidence and exclusive generation authority. Immutable copies are modeled as validated identities. Prefix staging and publication are abstract actions; names, flushes, metadata and copy syscalls are not verified. |
| [GenerationOwnership.dfy](GenerationOwnership.dfy) | `DeletionRequiresGenerationAuthority`, `StaleOwnerCannotDelete` and `RepeatedDisposePreservesSuccessor` establish exact-generation deletion authority; `TracePreserves` establishes invariant preservation through finite acquisition/crash/cleanup traces, refusal of insufficient authority, and repeated disposal preserving successor names. | Fresh generation/object tokens and exclusive lease semantics are assumed. Receipt parsing, GUID collision resistance, actual OS locks, partial acquisition and arbitrary namespace attackers are outside the model. Journal cleanup is a manual correspondence point rather than a separately modeled name. |
| [RecoveryKernel.dfy](RecoveryKernel.dfy) | Total native-integer functions classify supported page sizes, bounded database lengths, page offsets and truncate lengths, and authorize original/replay-zero/after-image bytes. Proofs connect byte authorization to `UndoJournal.Allowed`. | The generated functions are called by `PersistentRollbackJournal`; signed-input guards, record iteration, hashing, I/O and evidence identity remain handwritten. |

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

The entry point requires an exact manifest of every `.dfy` in the checkout, a matching pinned Dafny/Z3 version, a positive verification count with zero errors for each model, and a clean Dafny audit. Its conservative source policy also rejects trust bypasses, includes and unreviewed attributes. The policy is a restricted pilot-language check, not a second Dafny parser. Dafny parsing/verification/auditing remain required.

Mandatory negative controls run on every verification invocation: an intentionally false theorem must fail verification, and an intentionally assumed theorem must produce an audit finding even when Dafny's audit process exits successfully. These control files and findings live only in the external result directory. They are expected failures and are never proof inputs admitted by the manifest.

`summary.json` records proof and generated-source hashes, tool/solver versions, verification summaries, zero audit findings and the negative-control outcome. Per-process stdout/stderr and audit reports explain failures. A successful receipt establishes the checked models and exact generated-kernel freshness, not refinement of the remaining handwritten implementation.

## Executable kernel generation

The manifest's `dependencies` map lists earlier proof sources required by an importing file. They are supplied as normal inputs to verification, audit and translation, so their bodies are checked with the same options. No `--library` trust shortcut is used.

After reviewing a kernel source change, run the command above with `-UpdateGeneratedSource`. The default command and CI refuse a missing or edited `src/JetDatabaseWriter/Verification/RecoveryKernel.g.cs`. Generation first completes the proof/trust checks, then translates with the pinned compiler and solver into the external result directory. Raw C# is retained for review.

The projection keeps the final compiled `RecoveryKernel` namespace, internalizes and renames its generated class, and updates generated self references to that type. It retains method bodies and native arithmetic. Unused compiler scaffolding, embedded source attributes, abstract-model code and native-type descriptors are omitted. The projection rejects an unexpected module shape or references to omitted runtime/model support. Only the reviewed `nativeType` representations `ulong`, `uint` and `byte` are allowed in addition to proof hints.

No Dafny runtime source is bundled and no Dafny license-bearing runtime material is redistributed. The development compiler is the upstream MIT-licensed [Dafny distribution](https://github.com/dafny-lang/dafny/tree/v4.11.0). The compiler, this packaging transformation, the C# compiler and .NET remain trusted; generation does not prove compiler correctness. Focused production tests and the ordinary hosted framework/analyzer/trimming/NativeAOT lanes remain required.

## Review and updates

- Keep all proof files listed exactly once in [proofs.json](proofs.json), with earlier normal-source dependencies declared explicitly. Do not add assumptions, axioms, bodyless trusted lemmas, disabled verification or unchecked external declarations. Do not relax the audit to accept a finding.
- Keep integrity pins, exact version output and solver paths together in [dafny-toolchain.json](dafny-toolchain.json). The archive hashes cover both Dafny and its distributed solver; downloads come from the upstream `dafny-lang/dafny` release.
- For a tool update, review upstream changes and release provenance, update the platform digests and exact tool/solver outputs together, reinstall into fresh external directories and run verification, audit and both negative controls. Verify the Linux CI distribution as well as any locally used distribution.
- Run the focused script regressions after changing tooling or CI: `pwsh -NoProfile -File scripts/tests/verify-proofs.tests.ps1` and `pwsh -NoProfile -File scripts/tests/formal-workflow.tests.ps1`. The ordinary library CI lane additionally parses every script and runs its regression checks.
- Review the contract, assumptions and source mapping whenever mapped recovery methods change. Kernel freshness detects generated-source drift; broader model-derived conformance and handwritten control-flow correspondence remain open work.
- Preserve ordinary Release/analyzer, framework, native interoperability and crash tests. Formal models strengthen a named layer of evidence; they do not implement missing Access formats or qualify hardware durability.
