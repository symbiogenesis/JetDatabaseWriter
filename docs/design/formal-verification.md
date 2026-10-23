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

The buffered refinement separates the readable database image from `durableDb` and a queue of issued byte/length effects. Each queued effect must already have durable original coverage derived from the logical journal. Any effect may persist repeatedly in any order; writes may tear by byte, and length changes may stop at any intermediate length with zero-filled extension. `BufferedStepPreserves` and `BufferedTracePreserves` prove recoverability of both database images. `CompletedBufferedProtocolSelectsBeforeOrCommitted` and `AcknowledgedBufferedImageSurvivesMixedTrace` establish the selected durable result. `CompleteBufferedRecovery` lifts the decreasing recovery schedule to this refinement.

A successful database flush synchronizes the exact readable bytes and length and fences every older effect. Crash discards the remaining queue and reloads the durable image after any finite permitted persistence sequence. These are explicit storage contracts: the model does not establish that process termination drains OS caches, or that an old in-flight operation cannot overwrite restarted replay. Those qualification obligations remain in V7.

`CaptureCurrentByteRange` constructs logical capture actions by reading uncaptured current bytes; the buffered invariant establishes their original values. `CaptureAlignedPage` constructs a complete aligned page payload, padding only the suffix beyond the original file length with zero. `PrepareSinglePageWrite` and `PrepareSinglePageTruncate` compose that capture, the corresponding intent and journal flush into durable coverage before mutation. Their preconditions require the affected original range to fit the captured page; truncation also requires the target length not to exceed the current readable length. Previously captured pages are skipped under the explicit whole-page coverage invariant. Arbitrary multi-page preparation and complete removed-tail composition remain open.

The page payloads expand into the existing logical byte records. Serialized tags, manifest/page hashes, actual record reads and content-validation correspondence remain open. The abstract schedule always establishes a replay marker, whereas production records it only for a removed original tail; its progress theorem does not refine the production loop or count its syscalls.

## Replacement evidence

`ReplacementProtocol.ReadRecordBytes` classifies the actual bounded record layout: a 256-byte header and zero to 96 appended decision bytes. It checks magic/version/header size, nonzero generation bytes, supplied canonical-path identity, both signed-compatible database length bounds and digest equality. A complete decision additionally binds its magic, generation, new-image hash and digest. Reserved bytes receive exactly the production parser's checksum-only treatment.

`BytesRefineEvidenceAdmission` connects this classifier to logical admission. `DecisionAppendCutsHaveExactSelection` proves that every incomplete cut retains Prepared and only a complete valid decision selects Committed. Complete invalid evidence and insufficient header bytes refuse. `AdmittedBytesSelectRecovery` and `AdmittedBytesSurviveInterruptedRecovery` compose that selection with the existing recovery invariant, conditional on validated complete image/copy evidence and generation authority.

This byte classifier is verification-only. Its digest parameter is an arbitrary pure function, with no collision-freedom premise; its path identity is supplied. Exact reads, .NET signed/GUID decoding, SHA-256, path canonicalization, ownership bootstrap and production control-flow correspondence remain separate obligations. Admission of record bytes alone grants no exclusive lease or physical publication guarantee.

The copy refinement retains a validated immutable source, a read cursor and buffer of at most 65,536 bytes, readable staging output and its durable prefix. `CopyStepRefinesStaging` and `CopyTraceRefinesStaging` derive the existing `Stage`/`Interrupt` actions from bounded source slices, full/torn writes, uncertain flushes, crashes and retries. A zero/failed read or write may abort; retry restarts the publication copy at byte zero. `CopyPublicationEnabled` requires a ready copy with exact durable output, and `CopyReadyTraceRequiresSuccessfulFlush` excludes readiness produced only by draining bytes or a failed flush. The constructed completion path uses this refined publication gate.

`CompleteCopySchedule` decreases a byte/phase rank to construct a successful bounded-copy schedule. `FiniteCopyFailuresAllowSelectedRecovery` composes any finite interruption prefix with a successful retry, gated publication and existing cleanup. Infinite failures do not imply progress. These theorems assume contiguous-prefix staging persistence, retained immutable copies and exclusive I/O. Retry retires every prior attempt's effects before recreating output; an earlier write cannot resurface afterwards. Reordered holes or conflicting bytes are outside that contract and can require refusal with retained evidence. `ConflictingPublicationCannotEnterCopyModel` makes that refusal boundary explicit. The existing abstract `Publish` action remains broader than the refined gate; neither the copy schedule nor its gate verifies production syscalls, hash implementations, physical rename/replace, destination flushing, metadata or namespace durability.

## Storage and integrity assumptions

| Boundary | Required assumption or separate obligation |
|---|---|
| Bytes and length | Reads report actual bytes; writes/truncation affect only their declared range/length. Torn writes can leave only changes allowed by the model. No atomic page write is assumed. |
| Write-ahead ordering | Durable original data and the relevant intent precede the authorized database mutation. A successful database flush precedes a commit decision that can authorize the new image. |
| Flush and restart | A successful modeled database flush synchronizes exact bytes and length and fences older queued effects. A modeled crash quiesces/discards remaining effects before replay. The proof does not establish these contracts for runtime/OS caches, controllers or devices. Failed flushes may retain any finite allowed reordered/torn persistence prefix. |
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
| Protect overwritten original bytes | `Pages/Paging/StreamPageStore.cs` `WriteAsync`; `PersistentRollbackJournal.cs` `RecordBeforeWrite`, `CaptureOriginalPage`, `AppendRecord` | `SelectiveRollbackJournalTests`, `FileRollbackTests`, `BufferedJournalConformanceTests` | Buffered effects derive protection from durable logical captures. Page-record extraction/checksums and C# control flow still require conformance. |
| Protect removed original tail | `Pages/Paging/StreamPageStore.cs` `SetLengthAsync`; `PersistentRollbackJournal.cs` `RecordBeforeTruncate` | `ShrinkCrashRecoveryTests`, `RecoveryReplayCrashTests`, `BufferedJournalConformanceTests` | The byte protocol requires full-tail coverage; constructive page preparation covers selected one-page tails. Arbitrary multi-page composition, serialized record/hash correspondence, secure erase and actual file-length semantics remain unproved. |
| Data before durable decision; uncertain outcome | `Pages/Paging/PhysicalRollback.cs` `CommitAsync`, `RestoreAsync`; `PersistentRollbackJournal.cs` `Complete` | `TransactionCrashRecoveryTests`, `PersistentRollbackJournalTests` | The composed protocol includes failed flush prefixes and faulted ownership; actual exceptions, buffer persistence and I/O traces still need correspondence. |
| Validate before replay; restart replay | `Pages/Paging/PersistentRollbackJournal.cs` `ValidateJournal`, `ValidateCurrentContent`, `RecordRecoveryStart`, `Recover`, `RestoreAsync` | `RecoveryReplayCrashTests`, `SelectiveRollbackJournalTests` | Pure value/byte decisions use the generated kernel. Manifest/record extraction, the conditional marker, actual replay cuts and production-loop progress still need correspondence to the composed protocol. |
| Bounded journal values, manifest/complete-prefix arithmetic, record tags/control state and byte authorization | `Verification/RecoveryKernel.g.cs`, generated from `verification/RecoveryKernel.dfy`, called by `PersistentRollbackJournal` | `RecoveryKernelTests`, existing malformed-journal and replay tests; mandatory generated-source freshness | Total pure decisions are generated from verified source. Raw byte acquisition/decoding, checksums, payload/coverage extraction, iteration and I/O remain handwritten; the compiler and packaging transformation are trusted. |
| Valid old/new decision and publication | `Encryption/EncryptionReplacementIntent.cs` `ReadState`, `ValidateState`, `ValidatePublication`, `Copy`, `Publish`, `Recover`, `Commit`, `Cleanup` | `ReplacementIntentTests` covers every incomplete decision cut, checksum-valid malformed fields, multi-buffer prefixes and repeated flushed-copy retries; `ReplacementCrashRecoveryTests`, `ReplacementNamespaceModelTests` | Verification-only byte/copy models specify checked fields, bounded staging and a publication gate. Exact reads, digest/path/GUID implementation, copy/prefix syscalls and control-flow correspondence remain unproved; missing-destination metadata and namespace semantics require qualification. |
| Generation-bound cleanup | `Transactions/RecoverableWriterLock.cs` `RecoveryLease`; `Pages/Paging/PersistentRollbackJournal.cs` `CleanupCompleted` | `RecoveryLockFileTests`, `RecoveryReplayCrashTests`, completed-journal disposal regressions | GUID freshness and receipts/locks/path leases are abstractions; absence of live-owner races depends on the real coordination contract. |

The recovery-kernel tests exercise the generated boundary and its production callers. `ClassifyJournalRecord` consumes the exact unsigned interpretation of the stored 64-bit tag after framing/checksum validation. Its total native-byte result admits only terminal commits, tail-authorized replay markers, pre-replay truncations, original aligned before-images and bounded after-images. `JournalRecordClassificationIsExact` specifies all result classes; `ReplayStateRefusesFurtherMutations` excludes write/truncate records after replay begins. The handwritten enum-to-result mapping is covered by production conformance tests.

`JournalRecordStart` computes the bounded rounded-up page-hash manifest end. `CompleteJournalEnd` selects exactly the maximal complete-record prefix and excludes the trailing fragment, under the 16 GiB journal bound. Both are total native functions; zero denotes invalid inputs, while every valid start/end is at least 128. Signed adapters reject that sentinel before narrowing. Production manifest validation, preparation-copy comparison and record append/replacement use these generated decisions. The framing proofs establish arithmetic bounds, page rounding and the exact incomplete suffix; actual byte reads and integrity validation remain handwritten.

`BufferedJournalConformanceTests` manually translates selected model transitions into production journal calls and surviving images: mixed bytes from repeated write intents, partial final-page captures, truncate/append tails, repeated torn page and length-restoration cuts, and durable-decision cleanup retries. These finite traces supplement the unbounded proof; they do not constitute an automated refinement proof or reproduce an OS cache. Broader transition/rejection coverage remains in V6. Process-kill tests exercise actual failure paths but do not establish power-loss behavior. Golden byte/native DAO checks remain necessary for Access compatibility.

Review mapped changes against this remaining manual refinement inventory:

| Unproved operation | Required review obligation |
|---|---|
| Record framing and payload extraction | Framing/checksums must precede tag classification. Signed decoding, original-page hashes, duplicate capture refusal and durable coverage must precede authorized mutation. |
| Replay loop and conditional marker | Marker publication must precede zero-tail authorization; record iteration, partial last-page counts, final length and flush/exception cuts must retain replay evidence. |
| Effect ordering | Journal flush must precede database mutation; exact database flush and its fence must precede a commit decision; uncertain decisions must prohibit contradictory in-place undo. |
| Replacement admission and ownership | Byte fields must bind the intended canonical path/generation and validated copies; record admission must not substitute for exclusive receipt/lease authority. |
| Cleanup | Durable selected content and exact-generation ownership must survive interrupted cleanup; stale disposal must preserve a successor's names. |

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
