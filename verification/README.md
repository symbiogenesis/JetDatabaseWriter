# Recovery models and proof checks

These Dafny files are verification-only models. They add no runtime dependency, generated source or package reference to JetDatabaseWriter. The required `formal` job in [ordinary CI](../.github/workflows/ci.yml) verifies the complete manifest, checks the trust policy and retains `formal-verification` diagnostics.

Read the [contract and trusted boundary](../docs/design/formal-verification.md) before interpreting a theorem. The [open plan](../docs/formal-verification-todo.md) tracks implementation refinement, actual storage qualification and later proof work.

## Scope

| Model | Main checked properties | Deliberate boundary |
|---|---|---|
| [UndoJournal.dfy](UndoJournal.dfy) | `ProtectedWritePreservesRecoverability`, `ProtectedTruncationPreservesRecoverability` and `RepeatedReplayPreservesRecoverability` establish that exact durable before-image coverage preserves recoverability through arbitrary protected torn byte writes, complete-tail truncation, zero-filled re-extension and finitely many interrupted replay steps. Completed restoration returns the exact original image. Commit ordering distinguishes success and uncertain decision outcomes. | Exact durable captures and authorized byte mutations are abstract premises. Page/record encoding, hash validation, parser correctness, OS persistence and C# control flow are not proved. The commit submodel is separate from the byte-replay submodel. |
| [ReplacementProtocol.dfy](ReplacementProtocol.dfy) | `StepPreserves` and `TracePreserves` establish that each enabled staging/publication/decision/cleanup step preserves an authorized complete image. Finite traces preserve the invariant and committed decisions. `CompleteRecovery` uses a decreasing rank to construct an uninterrupted recovery path to the selected old/new destination. | Starts after valid Prepared evidence and exclusive generation authority. Immutable copies are modeled as validated identities. Prefix staging and publication are abstract actions; names, flushes, metadata and copy syscalls are not verified. |
| [GenerationOwnership.dfy](GenerationOwnership.dfy) | `DeletionRequiresGenerationAuthority`, `StaleOwnerCannotDelete` and `RepeatedDisposePreservesSuccessor` establish exact-generation deletion authority; `TracePreserves` establishes invariant preservation through finite acquisition/crash/cleanup traces, refusal of insufficient authority, and repeated disposal preserving successor names. | Fresh generation/object tokens and exclusive lease semantics are assumed. Receipt parsing, GUID collision resistance, actual OS locks, partial acquisition and arbitrary namespace attackers are outside the model. Journal cleanup is a manual correspondence point rather than a separately modeled name. |

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

`summary.json` records model hashes, tool/solver versions, verification summaries, zero audit findings and the negative-control outcome. Per-process stdout/stderr and audit reports explain failures. A successful receipt records a model check; it is not an attestation that the C# implementation refines that model.

## Review and updates

- Keep all proof files listed exactly once in [proofs.json](proofs.json). Do not add assumptions, axioms, bodyless trusted lemmas, disabled verification or unchecked external declarations. Do not relax the audit to accept a finding.
- Keep integrity pins, exact version output and solver paths together in [dafny-toolchain.json](dafny-toolchain.json). The archive hashes cover both Dafny and its distributed solver; downloads come from the upstream `dafny-lang/dafny` release.
- For a tool update, review upstream changes and release provenance, update the platform digests and exact tool/solver outputs together, reinstall into fresh external directories and run verification, audit and both negative controls. Verify the Linux CI distribution as well as any locally used distribution.
- Run the focused script regressions after changing tooling or CI: `pwsh -NoProfile -File scripts/tests/verify-proofs.tests.ps1` and `pwsh -NoProfile -File scripts/tests/formal-workflow.tests.ps1`. The ordinary library CI lane additionally parses every script and runs its regression checks.
- Review the contract, assumptions and source mapping whenever mapped recovery methods change. Existing model checks cannot detect every C# drift; model-derived conformance traces and a verified executable boundary remain open work.
- Preserve ordinary Release/analyzer, framework, native interoperability and crash tests. Formal models strengthen a named layer of evidence; they do not implement missing Access formats or qualify hardware durability.
