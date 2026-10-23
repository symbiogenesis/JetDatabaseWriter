# Formal verification boundary

The verification work covers selected recovery algorithms and small executable recovery kernels in Dafny. Dafny and its solver are development tools, not runtime dependencies. The library targets, package references and managed execution requirements remain unchanged.

A verified model establishes only the properties stated by its definitions, preconditions and theorems. The handwritten C# implementation, record parser, hashing, file APIs and operating system do not become verified because their intended behavior is represented in a model. Implementation conformance and physical persistence qualification are separate obligations in the [open plan](../formal-verification-todo.md).

## Boundary expansion policy

Expand the executable verified boundary where it removes safety-critical handwritten decisions and supports useful contracts. Prioritize recovery authorization and bounds, then hostile-file parsing, page ownership and structural edits according to concrete risk. Measure progress by the production behaviors covered, their contracts and the assumptions removed. Source volume alone is not an acceptance criterion.

Keep effectful I/O, cryptographic implementations, native interoperability and platform persistence behind explicit contracts until their own evidence supports stronger claims. Moving an operation behind a trusted external declaration does not verify it. A larger abstract model also does not close the implementation gap by itself.

Each executable increment must have total entry points or checked caller preconditions, production callers, focused conformance and rejection tests, pinned verification and audit, reproducible generated source and a reviewed compilation boundary. Preserve supported targets, package dependencies, trimming and NativeAOT. Retain ordinary tests and native-format evidence as the boundary grows.

## Recovery contract

Let `O` be the exact raw database image and length before a transaction, and `C` the complete image selected by its durable commit decision. Images include encrypted bytes when the database is encrypted. Under validated, discoverable recovery evidence, exclusive cooperating access and the modeled storage contract, a completed recovery must return exactly `O` or `C`. A successfully acknowledged commit must select `C`. An interrupted commit whose decision may have persisted can legitimately have an uncertain outcome; the writer must retain evidence and must not perform a contradictory rollback.

This is a safety property of authorized results, not a promise that every damaged input can be recovered. Complete invalid records, conflicting images, malformed ownership, untrusted paths or insufficient bootstrap authority can require refusal while retaining evidence. Partial records are classified by the actual record parser before entering the model; their classification is not proved by an abstract decision datatype.

Recovery may itself be interrupted. Every allowed replay step must leave a recoverable state, including a partial overwrite, restored original prefix and zero-filled re-extension of a previously truncated tail. Safety must hold through arbitrarily many such interruptions. Completion requires a separate progress argument with a finite image and eventually successful exclusive I/O; infinite crashes do not permit a liveness guarantee.

The undo model's central invariant is:

```text
Exact(U, O) := every saved (offset, byte) in U equals O[offset]
Recoverable(O, D, U) := Exact(U, O) and
    for every i < length(O) absent from U:
        i < length(D) and D[i] = O[i]
```

Thus every original byte destroyed or removed from `D` has an exact saved value in `U`. The protected-write and protected-truncation lemmas preserve this invariant, and completed restoration selects `U[i]` where saved and `D[i]` otherwise. The theorem returns exactly `O`.

The composed logical-record protocol derives this byte invariant from journal state. A before-image append reads an as-yet-unsaved original position from the current database; the invariant establishes that its value is still original. Only the durable complete-record prefix supplies saved coverage and write/length intents that enable database mutation. Failed journal flushes and crashes may persist any extending complete prefix plus a trailing incomplete record. Failed flushes fault the writer; recovery classifies the surviving prefix before selecting undo or a committed image.

`ProtocolStepPreserves` and `ProtocolTracePreserves` retain those obligations through arbitrary finite mixed traces. `CompletedProtocolSelectsBeforeOrCommitted` establishes the authorized completed result; `AcknowledgedImageSurvivesMixedTrace` preserves the acknowledged image across later crashes and recovery. `CompleteProtocolRecovery` uses a decreasing rank for tail discard, marker publication, resize, per-byte replay, flush and completion under eventually successful exclusive I/O.

This protocol models the database image surviving at each cut, rather than separate volatile and durable database buffers. Delayed/reordered database writes remain open. Record framing and decisions are logical classifications, not byte-parser proofs. Page-aligned capture, partial final-page encoding and content-validation correspondence also remain open. The abstract schedule always establishes a replay marker, whereas production records it only for a removed original tail; its progress theorem does not refine the production loop or count its syscalls.

## Storage and integrity assumptions

| Boundary | Required assumption or separate obligation |
|---|---|
| Bytes and length | Reads report actual bytes; writes/truncation affect only their declared range/length. Torn writes can leave only changes allowed by the model. No atomic page write is assumed. |
| Write-ahead ordering | Durable original data and the relevant intent precede the authorized database mutation. A successful database flush precedes a commit decision that can authorize the new image. |
| Flush | A modeled durable step represents the promised persistence boundary. The proof does not establish that a runtime, filesystem, controller or device implements that boundary. Failed/uncertain flushes must retain all possible outcomes. |
| Names and discovery | Required journal/receipt/intent names survive and are discovered at the original canonical path. File-content flushing alone does not establish this after power loss. |
| Integrity | Exact validated images and decisions stand for successful parsing, length/path/generation checks and integrity validation. SHA-256 is not an injective mathematical function; accidental-collision resistance and absence of adversarial evidence rewriting remain assumptions. |
| Identity | Canonical path and validated contents provide the current identity contract. They are not OS file IDs, authenticated ownership or path-alias discovery. |
| Coordination | Only one cooperating owner mutates a generation. The host must implement the lease/lock semantics used by production code. Arbitrary independent Access/other-process writes are outside the model. |
| Environment | Trusted local directories and supported storage behavior. Unrelated media corruption, malicious namespace races and Byzantine devices require additional models. |
| Proof tools | Pinned Dafny, its bundled solver, verifier translation and the host executing them are trusted. A successful check is conditional on their soundness. No admitted proof or external contract is accepted by the pilot's trust policy. |
| Implementation | The source-to-model mapping below is a review aid. It is not a machine-checked refinement proof of C# or an `extern` contract assumed to be true. |

These boundaries retain E4/F6/I15. A persistently provisioned journal is a possible future way to reduce namespace changes; it would still need a proven provisioning/reuse/decision protocol and OS evidence. It is not the current implementation.

## Production correspondence

Paths are relative to the repository root. Method names identify the intended refinement boundary; the models deliberately omit page serialization and physical I/O.

| Modeled obligation | Production boundary | Existing focused evidence | Remaining correspondence gap |
|---|---|---|---|
| Prepare evidence before mutation | `Pages/Paging/PhysicalRollback.cs` `BeginWrite`; `PersistentRollbackJournal.cs` `Create`; all under `src/JetDatabaseWriter/` | `PersistentRollbackJournalTests`, `SelectiveRollbackJournalTests` | Logical captures now derive durable coverage, but preparation parsing, namespace promotion/discovery and actual flush ordering are handwritten. |
| Protect overwritten original bytes | `Pages/Paging/StreamPageStore.cs` `WriteAsync`; `PersistentRollbackJournal.cs` `RecordBeforeWrite`, `CaptureOriginalPage`, `AppendRecord` | `SelectiveRollbackJournalTests`, `FileRollbackTests` | Model bytes stand for aligned page records; checksums, offsets, repeated after-images and C# control flow require conformance. |
| Protect removed original tail | `Pages/Paging/StreamPageStore.cs` `SetLengthAsync`; `PersistentRollbackJournal.cs` `RecordBeforeTruncate` | `ShrinkCrashRecoveryTests`, `RecoveryReplayCrashTests` | Page rounding, partial last-page encoding, secure erase and actual file-length semantics are not extracted from the model. |
| Data before durable decision; uncertain outcome | `Pages/Paging/PhysicalRollback.cs` `CommitAsync`, `RestoreAsync`; `PersistentRollbackJournal.cs` `Complete` | `TransactionCrashRecoveryTests`, `PersistentRollbackJournalTests` | The composed protocol includes failed flush prefixes and faulted ownership; actual exceptions, buffer persistence and I/O traces still need correspondence. |
| Validate before replay; restart replay | `Pages/Paging/PersistentRollbackJournal.cs` `ValidateJournal`, `ValidateCurrentContent`, `RecordRecoveryStart`, `Recover`, `RestoreAsync` | `RecoveryReplayCrashTests`, `SelectiveRollbackJournalTests` | Pure value/byte decisions use the generated kernel. Manifest/record extraction, the conditional marker, actual replay cuts and production-loop progress still need correspondence to the composed protocol. |
| Bounded journal values and byte authorization | `Verification/RecoveryKernel.g.cs`, generated from `verification/RecoveryKernel.dfy`, called by `PersistentRollbackJournal` | `RecoveryKernelTests`, existing malformed-journal and replay tests; mandatory generated-source freshness | Total pure decisions are generated from verified source. Signed guards, evidence extraction, iteration and I/O remain handwritten; the compiler and packaging transformation are trusted. |
| Valid old/new decision and publication | `Encryption/EncryptionReplacementIntent.cs` `ReadState`, `ValidateState`, `ValidatePublication`, `Publish`, `Recover`, `Commit`, `Cleanup` | `ReplacementIntentTests`, `ReplacementCrashRecoveryTests`, `ReplacementNamespaceModelTests` | Byte-level decision parser, hash equality, partial prefix comparisons, missing-destination metadata and OS publication semantics remain handwritten. |
| Generation-bound cleanup | `Transactions/RecoverableWriterLock.cs` `RecoveryLease`; `Pages/Paging/PersistentRollbackJournal.cs` `CleanupCompleted` | `RecoveryLockFileTests`, `RecoveryReplayCrashTests`, completed-journal disposal regressions | GUID freshness and receipts/locks/path leases are abstractions; absence of live-owner races depends on the real coordination contract. |

The recovery-kernel tests exercise the generated boundary and its production callers. The other test classes provide independent implementation evidence; the full recovery algorithm does not yet have model-derived conformance traces. Process-kill tests exercise actual failure paths but do not establish power-loss behavior. Golden byte/native DAO checks remain necessary for Access compatibility.

## Evidence levels and public claims

Use the narrowest supported description:

1. **Mechanically verified abstract model:** the listed Dafny lemmas hold for their model and assumptions, including composed finite-trace safety and abstract recovery progress.
2. **Implementation correspondence:** named production operations have a justified connection to the model, with conformance traces or a verified executable kernel. A manual table alone does not establish this level.
3. **Qualified storage guarantee:** the required runtime/OS/filesystem/device persistence and coordination assumptions have independent evidence for the advertised platform.
4. **Native format compatibility:** Microsoft-authored specimens and native operations establish the supported JET/ACE behavior. Recovery proofs do not fill unimplemented format or expression semantics.

Do not advertise whole-library formal verification, unconditional crash safety, mathematical hash collision freedom or portable power-loss atomicity from this pilot. Update individual README claims only when their stated evidence is available.

## Verification approach

The undo model follows the principle of placing recoverability conditions before writes: protect every original byte that a write or truncation could destroy, preserve the invariant through torn writes and interrupted replay, and permit a commit decision only after the complete selected image is durable. This is inspired by [PoWER](https://www.usenix.org/conference/osdi25/presentation/leblanc) and [crash refinement](https://www.usenix.org/conference/osdi16/technical-sessions/presentation/sigurbjarnarson); it is a project-specific model, not a reuse of either project's verified implementation.

Dafny supplies both verification-only models and the small executable recovery kernel. The [generation policy](../../verification/README.md#executable-kernel-generation) records the exact source projection and trusted compiler boundary; generated primitive operations require no runtime support. Target compatibility, trimming/AOT and production invocation remain acceptance obligations. Dafny external declarations would merely trust existing C#, so they cannot close its proof gap. F* remains an option for a concrete future boundary, but no second verifier or extraction toolchain is introduced.
