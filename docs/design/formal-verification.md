# Formal verification boundary

The verification work covers selected recovery algorithms, writer lifecycles and executable recovery, shared parsing, long-value descriptor, row-compaction, index-emission and usage-enumeration kernels in Dafny. Dafny and its solver are development tools, not runtime dependencies. The library targets, package references and managed execution requirements remain unchanged.

A verified model establishes only the properties stated by its definitions, preconditions and theorems. The handwritten C# implementation, record parser, hashing, file APIs and operating system do not become verified because their intended behavior is represented in a model. Implementation conformance and physical persistence qualification are separate obligations in the [open plan](../../PLAN.md#v--formal-verification-and-stronger-guarantees).

## Boundary expansion policy

Expand the executable verified boundary where it removes safety-critical handwritten decisions and supports useful contracts. Prioritize recovery authorization and bounds, then hostile-file parsing, page ownership and structural edits according to concrete risk. Measure progress by the production behaviors covered, their contracts and the assumptions removed. Source volume alone is not an acceptance criterion.

Keep effectful I/O, cryptographic implementations, native interoperability and platform persistence behind explicit contracts until their own evidence supports stronger claims. Moving an operation behind a trusted external declaration does not verify it. A larger abstract model also does not close the implementation gap by itself.

Each executable increment must have total entry points or checked caller preconditions, production callers, focused conformance and rejection tests, pinned verification and audit, reproducible generated source and a reviewed compilation boundary. Preserve supported targets, package dependencies, trimming and NativeAOT. Retain ordinary tests and native-format evidence as the boundary grows.

The [critical-behavior inventory](verification-scope.md) defines the required surfaces, properties and fault/platform scope. Its [evidence map](verification-evidence-map.md) records proof and production coverage, contracts and remaining obligations for each behavior. The [continuing review policy](proof-trust-review.md#continuing-review-policy) applies to semantic changes affecting that coverage, including dependencies outside the mechanical receipt inputs.

The [proof and algorithm survey](proof-survey.md) maps recent research to these boundaries and the existing plan. Verifier-guided annotation search and proof-dependency diagnostics support contract review; their output does not establish specification adequacy or production composition. Keep intended contracts and independent success/refusal witnesses fixed while exploring annotations, and separately review every executable or contract change.

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

`CaptureCurrentByteRange` constructs logical capture actions by reading uncaptured current bytes; the buffered invariant establishes their original values. `CaptureAlignedPage` constructs a complete aligned page payload, padding only the suffix beyond the original file length with zero. `CaptureAlignedPageRange` constructs a decreasing finite schedule over arbitrary aligned page ranges, preserves readable/durable database images and pending effects, and emits exact padded payloads only for previously uncaptured original pages, in strictly increasing offset order. Its whole-page capture invariant justifies skipping existing captures.

`PreparePageWrite` composes this range schedule, a write intent and journal flush into durable coverage for an arbitrary finite payload at an aligned start. `PreparePageTruncate` covers the complete removed original tail before its length intent and flush, including a partial final original page. Truncation requires an aligned target no larger than the current readable length, matching production admission. Both preparations enable every corresponding modeled torn mutation. Production records one physical page per write intent; the arbitrary-payload theorem generalizes logical coverage and does not verify a serialized multi-page production call sequence. These are constructive logical-record schedules; their alignment, whole-page capture and buffered-state preconditions remain explicit.

The page payloads expand into the existing logical byte records. `RecordedIntents` derives per-offset after-image values from finite logical write records; `RecordFoldAdmitsExactlyRecordedValues` identifies the content loop's fold with `Allowed` using that derived map. `RecordedTornWritePreservesAdmission` preserves that admission through torn writes under explicit gap authorization. `RecordedGapAndWritePreserveAdmission` supplies a logical zero-gap intent before the requested write; `RecordedReplayTracePreservesBoth` composes record-derived admission with recoverability through finite replay mixtures and proves exact abstract `Restore` from the final state. It does not make an arbitrary supplied trace converge. The standalone fold takes supplied validated minimum-length and recovery-marker inputs; the logical protocol refinement below derives those controls from record order. Logical-record acquisition, serialized tags, manifest/page hashes and exact payload correspondence remain separate obligations. The abstract schedule always establishes a replay marker, whereas production records it only for a removed original tail; its progress theorem does not refine the production loop or count its syscalls.

The logical content refinement derives its minimum length and replay privilege from complete durable records. `RecordedMinimum` folds length intents; `OrderedTailRecords` requires original-tail captures before each length intent. `ContentProtocolStepPreserves` and `ContentProtocolTracePreserves` compose record-derived byte admission with exact undo recoverability across mutation, uncertain journal flush prefixes, crashes and replay. Sparse writes require explicit zero-gap authorization before mutation; ordinary length mutation only truncates. These restrictions constrain preparation, rather than assuming that an arbitrary resulting image passes validation.

`PrepareContentWrite` and `PrepareContentTruncation` construct finite capture/intent/flush paths that admit every corresponding modeled torn mutation. `CompleteContentRecovery` lifts the existing decreasing recovery schedule while preserving content admission and the selected decision. These are logical protocol proofs. The buffered content refinement below lifts admission under an explicit zero-exposure contract; serialized record identity and the production loop's conditional marker remain separate obligations. The abstract completion path still always publishes a marker.

`ContentEffect` derives pending write payload authorization and length floors from complete durable records. `ContentBufferedStepPreserves` and `ContentBufferedTracePreserves` preserve record-derived minimum/replay controls and content admission for both readable and durable images through torn, repeated and reordered effects, uncertain journal prefixes and crashes. `ZeroExposureAllowed` requires recorded authorization only for newly exposed zeros: the gap between an old length and a write start, or the extension of a length effect. It does not assume that the complete resulting image passes validation. `DurableReplayMarkerAdmitsReorderedPersistence` discharges this guard after a replay marker is durable. `ContentCrashQuiescesPendingEffects` retains admission when crash discards pending effects, and `CompleteContentBufferedRecovery` constructs a decreasing successful recovery schedule.

Before a replay marker, unrestricted reordering does not preserve parser admission. `ReorderedWriteCanLeaveRefusingCrashSurvivor` constructs an admitted prepared state where an older write persists after truncation, exposing an unrecorded zero gap. Exact undo coverage survives, but the resulting image must refuse content validation. The witness begins at that prepared invariant state; its setup is not a proved trace from the initial empty journal. This separates undo recoverability from recovery availability. Production conformance exercises the same refusal with unchanged evidence and no writes, and a marked counterpart that converges after repeated torn replay. Establishing the zero-exposure contract for actual storage, or changing the protocol to admit these survivors safely, remains open; the theorem does not qualify OS caches or make the unrestricted buffered relation admissible.

## Replacement evidence

`ReplacementProtocol.ReadRecordBytes` classifies the actual bounded record layout: a 256-byte header and zero to 96 appended decision bytes. It checks magic/version/header size, nonzero generation bytes, supplied canonical-path identity, both signed-compatible database length bounds and digest equality. A complete decision additionally binds its magic, generation, new-image hash and digest. Reserved bytes receive exactly the production parser's checksum-only treatment.

`BytesRefineEvidenceAdmission` connects this classifier to logical admission. `DecisionAppendCutsHaveExactSelection` proves that every incomplete cut retains Prepared and only a complete valid decision selects Committed. Complete invalid evidence and insufficient header bytes refuse. `AdmittedBytesSelectRecovery` and `AdmittedBytesSurviveInterruptedRecovery` compose that selection with the existing recovery invariant, conditional on validated complete image/copy evidence and generation authority.

`ReplacementCopyConformanceTests` exercises the shared production copy loop through unprepared `Begin`: empty and multibuffer inputs, positive short reads, bounded read requests, early EOF, read exceptions, cancellation and source-position restoration. Failure retains unrelated candidates and removes only owned unprepared evidence. Additional checks invoke the actual private copier with short reads, early EOF, read exceptions, partial writes across buffer boundaries and flush failures before or after a successful underlying flush. They retain the resulting Prepared staging prefixes and replay them through real missing-destination recovery, including repeated interruption after a complete copied/flushed image and eventual selected-image publication. A conflicting byte in a partial staging file causes repeated refusal with unchanged evidence.

`ReplacementPublicationConformanceTests` additionally interrupts the complete `Commit` and `Recover` callers around concrete publication creation, bounded reads and writes, source flush, rename and selected-destination flush. Prepared and Committed evidence, existing and missing destinations, empty selected images and repeated retries retain the selected decision and converge through production cleanup. Cleanup ordering assertions require a successful destination flush before copy deletion. These callback exceptions occur around real operations; they do not reproduce partial failed native syscalls or establish the complete recovery exception order. Disposal can persist buffered bytes, so these survivors are not distinct power-loss images. Actual syscall behavior, directory durability and missing-destination metadata remain separate obligations.

The sequence-based byte classifier supplies the specification for the generated production parser. `CheckReplacementHeader` and `CheckReplacementDecision` scan the actual bounded arrays in generated code, with proved prefix invariants binding every magic/version/size byte, nonempty generation, canonical-path digest, decision generation, selected-image hash and checksum byte to `ValidHeaderFields` and `ValidDecisionBytes`. `ReplacementHeaderChecksRefineBytes` and `ReplacementDecisionChecksRefineBytes` prove field equivalence, and `ReplacementScansRefineRecordAdmission` composes the scans with record admission; `DecodeReplacementLength`, `IsReplacementIntentLength`, `IsReplacementHeader` and `ReplacementAdmission` retain the bounded length and selection contracts. `ReplacementReadAdvance` proves exact bounded cursor progress for positive short reads. Production reads the complete header and any complete decision before validation, retains Prepared for an incomplete decision suffix, and refuses invalid evidence before generation bootstrap. `ReplacementFieldConformanceTests` checks semantic corruption with recomputed checksums, repeated refusal without callbacks or ownership provisioning, positive short reads, early EOF and real I/O errors. SHA-256, canonical-path computation, GUID construction, returned stream-read effects, receipt authority and filesystem operations remain explicit implementation contracts. Valid record bytes alone grant no exclusive lease or physical publication guarantee.

The copy refinement retains a validated immutable source, a read cursor and buffer of at most 65,536 bytes, readable staging output and its durable prefix. `CopyStepRefinesStaging` and `CopyTraceRefinesStaging` derive the existing `Stage`/`Interrupt` actions from bounded source slices, full/torn writes, uncertain flushes, crashes and retries. A zero/failed read or write may abort; retry restarts the publication copy at byte zero. `CopyPublicationEnabled` requires a ready copy with exact durable output, and `CopyReadyTraceRequiresSuccessfulFlush` excludes readiness produced only by draining bytes or a failed flush. The constructed completion path uses this refined publication gate.

`CompleteCopySchedule` decreases a byte/phase rank to construct a successful bounded-copy schedule. `FiniteCopyFailuresAllowSelectedRecovery` composes any finite interruption prefix with a successful retry, gated publication and existing cleanup. Infinite failures do not imply progress. These theorems assume contiguous-prefix staging persistence, retained immutable copies and exclusive I/O. Retry retires every prior attempt's effects before recreating output; an earlier write cannot resurface afterwards. Reordered holes or conflicting bytes are outside that contract and can require refusal with retained evidence. `ConflictingPublicationCannotEnterCopyModel` makes that refusal boundary explicit. The existing abstract `Publish` action remains broader than the refined gate; neither the copy schedule nor its gate verifies production syscalls, hash implementations, physical rename/replace, destination flushing, metadata or namespace durability.

`GenerationOwnership.AcquisitionTracePreserves` refines fresh/recovery acquisition into native-evidence and receipt preparation stages. `IncompleteBootstrapRefusesAuthority` and `FailedBootstrapRetainsEvidence` retain incomplete/native-only evidence while refusing ready authority. `AuthorityTraceRequiresCompleteReceipt` prevents readiness from appearing without receipt completion when the initial receipt is not exact. `FreshBootstrapCompletes` constructs successful fresh preparation; `RecordedBootstrapCompletes` reconstructs an absent native name from an exact receipt while refusing an existing partial native image. Exact evidence, fresh tokens, exclusive ownership and successful persistence fences are model premises, rather than proofs of receipt parsing, GUID uniqueness, OS handles or namespace durability.

`ReplacementProtocol.CoordinatedTracePreserves` composes replacement and exact-generation ownership transitions in an abstract schedule that removes replacement evidence before ownership names. `InterruptedCoordinatedRecoveryKeepsSelection` preserves the admitted decision through finite recovery/crash/reacquisition traces, and `OwnershipDeletionFollowsSelectedCleanup` requires replacement intent/copy cleanup before generation-name deletion. `BootstrapAuthorityAllowsSelectedRecovery` projects completed bootstrap into held authority and composes the bounded successful-copy schedule, selected publication, replacement cleanup and generation cleanup. Replacement-intent and ownership generations are distinct tokens; the proof does not assume that their independently generated GUIDs are equal. Production bootstrap tests supplement these abstractions but do not establish physical persistence or complete C# control-flow refinement.

`ProductionCleanupSnapshot` separately represents production recovery cleanup after selected-image validation, successful destination flushing and copy deletion. It retains the destination lease while native, receipt and finally replacement-intent names are removed. `ProductionCleanupTracePreserves` preserves the selected decision/image through finite cleanup/crash/restart traces, and `CompleteProductionCleanup` constructs a decreasing successful cleanup schedule. Crashes lose held leases. An exact remaining receipt permits reconstruction after native deletion; after both ownership names are absent, successful restart needs a fresh ownership generation. These restart actions abstract successful validated acquisition rather than verifying its syscalls. `CleanupSuccessorAdmission` states the cooperating-open contract; focused production tests exercise destination sharing refusal at each deletion boundary. The older `Coordinated` schedule does not refine this actual cleanup order, and neither model qualifies namespace persistence.

`PublicationFenceTracePreserves` separately retains the selected image and decision across finite destination acquisition, flush-failure, copy-deletion and interruption traces. `CompleteFencedSelectedProductionRecovery` composes the bounded source-copy flush gate, publication, a second successful flush of the held selected destination, ordered copy deletion and production-order leased cleanup. `UnfencedDestinationCannotDiscardCopies` and `FailedDestinationFlushRetainsEvidence` state the refusal/retention boundary. Successful validated acquisition and exact flush behavior remain contracts; this recovery-only composition does not refine the complete commit decision path or qualify physical namespace persistence.

## Long-value descriptor codec

`LongValueKernel.dfy` supplies the complete twelve-byte descriptor codec used by `LongValueDescriptor.TryRead` and `WriteTo`. Generated array methods acquire and serialize the length/mode word and both opaque pointer/token words with overflow-safe range checks, exact field postconditions, unchanged destinations on refusal and preservation outside the descriptor. Callers supply non-null CLR byte arrays whose lengths fit the signed array limit; signed adapters reject negative offsets, counts and lengths before narrowing. Allocation-free span adapters invoke the same generated range, scalar field and byte-extraction decisions. Reserved storage modes survive raw acquisition for preservation but refuse payload admission and encoding.

Inverse laws cover every pointer/token word and each supported 30-bit length/storage-mode encoding. Generated payload-admission decisions preserve malformed-data and configured-budget error priority; the row-pointer codec refuses pages beyond 24 bits or rows beyond 8 bits rather than wrapping into another identity. `LongValueDescriptorKernelTests` uses independent byte/range oracles, boundary encodings, untouched-destination checks and pointer-alias regressions; decoder controls retain reserved-mode refusal across exact, string, raw and streaming paths. Span glue, external-page acquisition, token meaning, chain traversal and aggregate resource budgets remain separate contracts. This verifies the named bounded descriptor representation, not a complete public long-value parser or native format compatibility.

## External long-value traversal

The selected [external chained-payload surface](long-value-traversal.md) is `LongValueStore.CopyPayloadAsync`, used by streaming decode and compact discovery. `LongValueTraversal.Traverse` proves finite traversal with distinct nonzero visited identities, a decreasing row budget and strict payload progress for each completed output chunk. Null, repeated, absent and no-progress rows refuse. For `n` attempted acquisitions, including a final malformed row, `n <= min(declaredLength, rowBudget)`; output is bounded by the declared length and logical page-prefix acquisition work by `pageSize * n`.

Production invokes generated row-admission/payload-count and visited capacity/probe decisions. The specialized visited table allocates lazily and doubles at half occupancy; the model bounds current capacity by `16 + 4*n` uint slots and cumulative allocations by `32 + 8*n` slots, including superseded arrays. Circular-probe lemmas establish decreasing probe work and complete bounded slot coverage. Generated `ContainsVisited`, `AddVisited` and `RehashVisited` array loops prove exact nonzero membership under the insertion-established probe-chain invariant, preservation during insertion/rehash and bounded termination. `TrackedVisited` couples those entry contracts to distinct membership, exact occupied count, half occupancy and the capacity bound. Generated `PrepareVisitedAdd` and `CompleteVisitedGrowth` compose count/growth policy with the array operations, preserving the single-scan insertion and duplicate-before-growth behavior. Preparation returns zero for unchanged state, a direct insertion count, a marked growth capacity or exhausted refusal. Exact classification, payload inverses and a constructive representation theorem preserve the abstract transition through ghost effective count/capacity projections. Null initial storage and the existing empty-array form are admitted. The C# adapter supplies CLR allocation and retains/publishes the array/count pair; its effect sequencing and composition with the public acquisition/write path remain manual obligations. Synchronous cancellation latency, CLR overhead, GC behavior, locator/cache/pool buffers, destination allocations, returned I/O effects and native pointer/ownership meaning remain separate contracts. These are named traversal bounds, not a verified complete public parser or a whole-library hostile-file claim.

The array-backed `LongValueTraversal.Traverse` calls the same generated
count/growth controllers and their concrete array operations. Ghost probe and
source-iteration meters derive `probes <= 20*n*n + 92*n + 48`,
`scannedSlots <= allocatedSlots <= 32 + 8*n` and
`peakSlots <= allocatedSlots` with `peakSlots <= 24 + 6*n`, including old/new
growth overlap and final refusal work. These are named table payload/work
bounds, not a total CPU, physical heap or .NET allocation-progress theorem.
Actual public parser effects and synchronous cancellation latency remain open.

## Page-frame ownership

`PageFrameOwnership.dfy` specifies the bounded reference ledger used to review
immutable pooled page frames. Its inductive transition proof covers cache and
producer ownership, fresh lease tickets, idempotent release, one final pool
return and generation-checked producer-to-cache transfer. Explicit retained and
unretained traces exercise successful acquisition, eviction and completion;
dead/full references and stale generations refuse their named operations.

The [ownership boundary](page-frame-ownership.md) maps this verification-only
model to `PageFrame`, `PageLease`, reader retention, pager replacement and scoped
scan/seek/overflow/LVAL callers. These remain handwritten implementations with
manual correspondence and regression evidence. Array immutability and abstract
atomic transitions do not prove C# synchronization, decoder lifetime discipline,
complete aggregate memory bounds or the containing public parser. Existing
generated kernels, runtime requirements and physical-storage claims are unchanged;
I7/S1/V8/V10 retain their remaining composition and qualification obligations.

## Shared parsing primitives

The shared primitive boundary covers range admission, slice composition and positive-read cursor progress. `RecoveryKernel.IsByteRange` admits only offsets and lengths whose sum fits both the containing range and the signed managed offset domain. `BufferReadAdvance` accepts only a positive returned count within the requested remainder; refusal cannot wrap or advance the cursor. `ReadMinimumSatisfied` distinguishes a sufficiently acquired prefix from exact completion.

Production buffer slices and exact-read adapters use these decisions on both library targets. The bounds and cursor decisions are total on their native input domains. Exact source-byte acquisition additionally depends on the stream's read-effect contract: a successful read supplies the reported next source prefix and does not mutate outside its requested destination. EOF, cancellation and exceptions remain effectful outcomes; a proof about successful chunks does not establish filesystem persistence or arbitrary custom-stream behavior. Parser traversal, aggregate allocation budgets and format interpretation remain separate obligations.

## Row read admission

The generated row-read boundary supplies `RowReadDisposition`, saturating
`NextOmittedRowCount` and widened `RowNullMaskStart`, `RowVariableCountStart` and
`RowVariableDataEnd` geometry. Strict refusal and lenient omission are distinct
outcomes; strict admission never selects omission. The native geometry functions
admit only complete bounded null-mask/count/offset-table intervals and refuse
underflow or overlapping metadata before a row is decoded. Production scans,
counts, direct/hybrid POCO and index materialization execute these decisions.
`RowReadAccounting` publishes the bounded counter atomically across a reader and
its authorized linked sources. Exact acquired bytes, complete C# call-site
coverage, diagnostic delivery and atomic publication remain adapter contracts;
these functions do not establish damaged-map membership or a complete parser.

## Complex cell admission

Complex row cells retain a bounded scalar item-ID/reference/location inventory
before admitting payloads. The inventory reuses generated `LongValueKernel`
nonzero integer membership/growth operations for flat-table-global identity
checks; `ComplexItemIdentityBudget` uses generated `RecoveryKernel.IsByteRange`
admission before adding inspected records. The operation quota is shared across
selected columns, nested calls and authorized linked readers. Grouped locations
avoid repeated fallback scans, and requested FK-index bookmarks must match the
independent scalar inventory before payload decoding. Typed item identity and
parent reference are checked again at acquired locations. These generated
collection/range decisions do not prove C# inventory construction, index
completeness, native field roles or whole-process memory; those remain explicit
composition and resource obligations. Payload materialization is bounded to the
consumed parent cell for row streams, while whole-column item-list APIs still
materialize their requested items.

## Compatible row patches

`DataPageKernel.PatchColumn` admits distinct source/destination arrays and
complete field/metadata bounds before mutation. Its generated loop copies the
exact payload, merges one selected null/Boolean bit and preserves every other
candidate byte and mask bit; refusal preserves the complete candidate.
`ReplaceRow` reacquires bounded directory identity, rejects flagged/aliased
slots and writes only the admitted live interval, preserving every page byte
outside it. Production re-resolves current slot geometry after prior batch
compaction, patches an unpublished row candidate, then publishes through the
transaction-aware pager. Index-directed snapshots check every live layout on
compatible-width selection, frozen explicit-assignment metadata, raw LVAL ownership and encoded-key equality remain
C# contracts. Index-directed writer discovery additionally relies on a correct
supported index for complete membership; bookmark checks do not reconstruct
missing index entries. Full mutation/rollback and native-format correspondence
retain their separate I2/I5/I6/V6/V9/V10 obligations.

## Writer lifecycle specification

[The writer lifecycle specification](writer-lifecycle.md) records the legal serialized writer, transaction and lease states, terminal transaction identities, nested savepoint behavior, disposal and cache invalidation. The checked model describes the protocol; it adds no shipping code or runtime dependency. Its successful effects and exclusive ownership are explicit premises, and focused production traces support only the named correspondence boundaries.

The independent [cooperating process ownership model](multi-process-ownership.md) covers arbitrary finite acquisition, recovery, cleanup, crash and restart interleavings for distinct clients of one canonical identity. `MultiProcessOwnership.StepPreserves` and `TracePreserves` retain exclusive current-ticket authority and phase/evidence consistency. Cached observations, including absent names during cleanup, grant no authority. Stale tickets cannot mutate or delete successor evidence. Checked counterexamples expose the consequences of releasing coordination before cleanup finishes or deleting by an old pathname observation. Constructive schedules complete exact-receipt acquisition and successful recovery cleanup.

That model assumes an atomic canonical coordinator, fresh tickets/generations, validated evidence, named successful flush effects and crash quiescence. Its cleanup schedule maps to `RecoveryLease.Cleanup` after a destination exists while the caller retains database coordination; ordinary owner disposal after database release retains its separate held-handle boundary. Missing-destination composite acquisition, actual cross-process OS exclusion, deadlock prevention and cache pin/copy-on-write correctness remain V10 refinement/qualification obligations. Neither lifecycle model proves arbitrary concurrent C# execution or physical durability.

## Storage and integrity assumptions

`GenerationOwnership.DirectoryAcquisition*` gates ready authority on an
acknowledged preparation-name fence. Its `DirectoryRetirement*` refinement
requires native-name removal and its acknowledged fence before receipt
retirement while the receipt lease remains held. `ReplacementProtocol.DirectoryCleanup*`
requires selected content and name acknowledgements before destroying retained
copies, then orders copy, ownership and intent retirement fences. Finite traces
preserve these named-effect ordering invariants and retain failed acknowledgements.
They are verification-only protocol refinements under explicit host contracts;
they do not model physical namespace loss or prove the C# syscall sequence.

Production supplies the corresponding directory adapter only in the native
.NET asset on Windows. Unsupported results preserve the process-only path and
never emit an acknowledged-directory-fence hook. Prepared copies and the intent
are fenced before publication; selected destination contents and its parent
name are fenced before copies are removed. Journal promotion fences before
physical writes. Ordinary ownership disposal retains the receipt lease until
native-name retirement finishes, and creation fences the published name without
deleting a completed destination on a later fence failure. The replacement
owner device-flushes its caller-written retained replacement itself. Runtime,
filesystem/device semantics and complete C# refinement remain separate, and no
power-loss qualification receipt or portable durability claim follows.

`PageDigestKernel` supplies the raw digest ledger's bounded shape/write/resize
admission and complete failure-atomic 32-byte slot replacement. The generated
loop proves exact replacement bytes and preservation outside the selected slot,
including aliasing and refused out-of-range input. `FilePageDigests` retains only
digests, never plaintext pages. One `StreamPageStore` acquires the original ledger
and tracks successful aligned overwrites, contiguous complete-page appends and truncations under exclusive writer
ownership; failed or untracked effects, sparse gaps and length extensions invalidate reuse. A partial-page shape
uses fresh commitment acquisition after mutation rather than guessing preserved
bytes. SHA-256, CLR allocation, faithful successful I/O and C# effect sequencing
remain adapter contracts. This is not a proof of the complete cache correspondence.
Length extensions and sparse gaps always invalidate reuse: the managed
[FileStream.SetLength contract](https://learn.microsoft.com/en-us/dotnet/api/system.io.filestream.setlength?view=net-10.0)
does not promise zero-filled expanded bytes on Windows. A fresh acquisition reads
the actual exposed bytes instead of committing an inferred zero image.

The version-three journal retains its complete original page manifest and
selective original images. Its commit commitment is SHA-256 over a sixteen-byte
length/page-size frame followed by the ordered SHA-256 page digests. Recovery
recomputes that commitment from the actual raw image with a bounded page buffer;
only a live exclusive store can reuse tracked digests. The frame includes an
eight-byte little-endian length, four-byte little-endian page size and four zero
bytes. Framing distinguishes image shapes, while collision resistance retains
the same cryptographic trust obligation. Manifest serialization and commitment
folding still scale with page count; neither the kernel nor I/O regression tests
establish end-to-end performance acceptance or physical durability.

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

These boundaries retain E4/F6/I15. The [storage qualification contract](recovery-storage-contract.md) records the managed flush/source audit, separate namespace and restart obligations and qualification receipt requirements. The [caller-stream contract](caller-stream-recovery.md) defines current live rollback, disposal and isolation requirements and the identity, journal, fence, reopen and lease capabilities required before persistent stream recovery can be implemented or advertised. No OS/filesystem/device combination has a power-loss qualification receipt. A persistently provisioned journal is a possible future way to reduce namespace changes; it would still need a proven provisioning/reuse/decision protocol and OS evidence. It is not the current implementation.

## Page parsing and bitmap operations

The shared executable kernel also covers three bounded page operations used by
production: data-page slot acquisition, usage-map bitmap access/update, and
index entry-boundary scans. These are array-level properties under the supplied
layout and immutable-input contracts. Their containing public parsers, page
identity, traversal budgets and mutation ownership retain separate obligations.

`ReadDataRowSlot` bounds the count by the physical directory, reads the
selected masked offset, refuses directory overlap and selects the least strictly
greater masked offset across all slots. Deleted slots remain valid boundary
contributors and may be selected as overflow targets. Duplicate offsets do not
produce a zero-sized row. The proof covers the selected interval. Complete row
decoding and overflow traversal retain separate obligations.

`InlineUsagePosition` and `ReferenceUsagePosition` admit the complete bitmap
window before selecting a byte and bit. `ReadUsageBit` acquires its state;
`WriteUsageBit` preserves unrelated bytes and bits. Refusal
leaves the array unchanged. Negative native base pages and invalid signed
adapter inputs refuse. A bitmap bit records membership; reclaiming a page still
requires the independent incoming-reference and ownership checks.

`FindIndexEntryStart` selects the first set boundary strictly after the current
entry and strictly before the payload end. `CheckIndexLeafBitmap` checks the implicit first
entry, exact nonempty end sentinel and clear tail. The sentinel is excluded from
entry selection. Each scan has a decreasing bounded cursor. Key reconstruction,
comparison, complete tree traversal and edit/reclamation composition remain
separate obligations.

## Row jumps, index seek control and journal content scans

The Jet3 jump-table boundary covers native entry sizing, dummy admission,
monotone boundary progress and offset high-part decisions. Production retains
allocation-free span adapters for row bytes and stack-allocated offsets. Their
loads and stores remain explicit effects; generated scalar controls supply the
format decisions without copying either span. Array-level specifications and
focused native-row controls support this boundary. Span-loop sequencing and
retention have manual correspondence; the complete generated array-scan proof
is not a mechanical refinement of those C# loops. Full row-trailer acquisition,
schema-derived offsets and complete public row decoding remain separate.

Intermediate index child selection uses generated entry admission and cursor
decisions, including shared-prefix bounds and canonical child-pointer bytes.
The adapter retains the existing vectorized span comparison and reports its
lexicographic sign to the controller. That BCL comparison, correct supplied
layout, stable page bytes and containing tree traversal remain explicit
contracts. The C# orchestration of the generated entry/advance operations retains
manual correspondence to the checked controller trace. A selected child pointer
grants neither ownership nor reclamation
authority; structural edits and native collation qualification remain separate.

Journal content authorization executes generated array scans for original or
replay-zero admission, accumulation of matching after-image bytes, final byte
admission and zero-filled extension checks. These scans preserve unrelated
scratch entries and input bytes, and retain the existing page-sized scratch
allocation and record iteration. Exact journal/database reads, admitted record
identity, checksum/hash validation and serialized inventory construction remain
contracts of the containing parser. Byte admission alone does not prove undo
coverage, exclusive authority, returned I/O effects or physical durability.

## Row compaction, index emission and usage enumeration

Three subsystem kernels extend the executing boundary beyond individual field
checks. They retain native primitive arrays and arithmetic, existing shipping
targets and dependencies, and the optimized bulk byte effects described below.
Their generated method bodies pass the same deterministic translation, audit,
negative controls and freshness checks as the recovery and long-value kernels.

`DataPageKernel` admits physical deleted-row reclamation before any mutation,
selects the minimum payload offset and next distinct row boundary, and updates
selected or relocated directory words plus the free-space header. Its contracts
preserve surviving slot flags and unrelated bytes. The exact overlapping `Buffer.BlockCopy` and optional
`Array.Clear` effects remain explicit adapter contracts; modeled byte-image
preservation does not verify the CLR implementation. Stable exclusive page
ownership, the supplied native layout and containing statement rollback remain
separate obligations. Reclaiming row space grants no authority to free a page.

`IndexMutationKernel` executes shared-prefix scanning, capacity/cursor decisions,
header emission, entry bookmarks and child pointers, bitmap boundaries, and
final sentinel/free-space emission for leaf and intermediate pages. Fixed-position
stores extract bytes directly; the general byte law remains ghost specification.
The adapter retains the existing list representation and bulk key-suffix copy. Exact copied
bytes, stable entry acquisition, correct layouts and caller-established
preconditions remain contracts. Ordered multisets, native collation, separators,
allocation, split/merge/root changes and full tree-edit rollback require their
own composition; an encoded page is not an ownership proof.

`UsageMapKernel` admits the complete array range once, then scans the actual
acquired bitmap using a native byte-mask result shared by INLINE and
REFERENCE enumeration. Zero-byte scanning advances a byte index; each nonempty
result admits up to eight ordered members and returns their computed byte base. The adapter appends the selected
mask bits before handling terminal status, including valid members before a
strict failure within that byte. The cursor makes bounded progress; tolerant
enumeration skips out-of-range members. Signed 64-bit page controls preserve the
adapter's native domain without narrowing. List append, stable array lifetime, reference
pointer/page acquisition, cancellation and pooled-page return remain explicit
effects. Exact bitmap membership does not establish complete database ownership
or authorize reclamation.

Focused conformance compares independently described byte images and scan
results, including supported success, refusal and retained-prefix behavior.
Hosted benchmarks compare the affected existing workloads against the published
starting revision and retain original-algorithm controls for new microbenchmarks.
Generated-source correctness supplies no timing theorem; actual allocation and
wall-clock preservation require those hosted measurements. Complete public
parser, tree and mutation proofs remain in V8/V9/V12.

## Numeric seek admission and relocation ledgers

`RecoveryKernel.IsOutwardSeekBound` admits exact numeric equality and range
bounds whose conversion moves away from matching values. Its order theorem
preserves every original match without changing strict/inclusive endpoints.
`IndexSeekFilter` executes this decision after checking lossless property and
operand domains and comparing nearest-even encoded values with the original
operand. Integral, Currency, declared-scale Decimal and finite Single rounding
may broaden a range; the original residual predicate still filters every row.
Overflow, NaN, unsafe inward rounding and nonexact numeric equality refuse.
The existing Date/Time policy uses the same range decision, retaining its
whole-millisecond equality exception.

`IsFloatingZeroSeekBound` separately accounts for the distinct index encodings
of CLR-equal negative and positive zero. Strict zero ranges remain admissible;
an inclusive lower bound must use negative zero and an inclusive upper bound
positive zero. Other zero comparisons scan. A rank-based theorem preserves both
zero observations. Numeric conversion, comparison signs and native key order
remain adapter contracts, checked by independent encoded-key vectors for
supported orders and both Decimal key forms, plus public indexed-versus-scan tests.
These are scalar admission proofs, not complete query-planner refinement or
new native-format evidence. Endpoint-rewriting optimizations remain I9.

Legacy descending Decimal encodings retain a sign-byte rule that does not
establish reversed cross-sign byte order. Inferred descending ranges remain
residual; planner regressions guard both complete refusal and retention of an
earlier equality prefix. Native fixture/comparison qualification remains F9/I9.

`IndexMutationKernel` also executes monotone relocation-pair admission and
overflow-safe binary-search pivot/interval decisions in `CompactRowRelocationMap`.
Ledger-extension proofs derive unique sources, injective destinations and
preserved bookmark order; search proofs retain exact membership and halve an
unsuccessful interval. Successful lookup preserves the stored key and returns
the destination belonging to that source position. The caller supplies an
immutable exclusively owned stream and exact read effects. Complete root and
incoming-reference enumeration, key/row correspondence, graph preservation,
safe reclamation and crash-safe compact publication remain I17/V9 obligations.

The replacement cleanup model composes `ProductionCleanupAuthority` through
arbitrary finite admitted cleanup/crash/restart traces. Both fenced entry
proofs establish it; `InterruptedProductionCleanupAllowsCompletion` constructs
a successful continuation with the same selected image and decision, recorded
generation reuse while a receipt survives, and fresh authority after name loss.
Repeated production interruption tests check these authority transfers and
final successor admission. Validated bootstrap, actual OS exclusion, successful
effects, namespace durability and eventual successful I/O remain explicit
contracts. This does not close complete recovery-controller refinement.

## Production correspondence

Paths are relative to the repository root. Method names identify the exact
executable or refinement boundary. Byte acquisition is covered only where
named; physical I/O and the remaining containing algorithms retain their
explicit contracts.

| Modeled obligation | Production boundary | Existing focused evidence | Remaining correspondence gap |
|---|---|---|---|
| Outward numeric range and signed-zero admission | `Indexes/IndexSeekFilter.cs`; generated `RecoveryKernel.IsOutwardSeekBound`, `IsFloatingZeroSeekBound` | `IndexSeekRoundingTests`, `IndexSeekFilterTests`, `TypedMappingCoercionTests` | CLR conversion/comparison and native key-order correspondence remain contracts; full planner/evaluation semantics and endpoint rewriting remain I9. |
| Monotone injective relocation and bounded exact ledger search | `Maintenance/CompactRowRelocationMap.cs`; generated `IndexMutationKernel` relocation controls | `CompactRowRelocationMapTests`, existing raw index assembly/validator tests | Immutable exclusive ledger/read effects remain contracts; complete rooted graph, native key/row correspondence and reclaim/publication composition remain I17/V9. |
| Complete row-reclamation admission and directory/free-space rewrite | `Pages/DataPageRows.cs` `ReclaimDeletedRowSpace`; generated `DataPageKernel` | `DataPageReclaimKernelTests`, existing deletion/overflow/reclamation tests | Exact overlapping copy/clear effects, exclusive page/layout identity, containing mutation and statement rollback remain contracts. |
| Leaf/intermediate index header, compressed entry metadata and page finalization | `Indexes/IndexPageCodec.cs` page builder and prefix scan; generated `IndexMutationKernel` | `IndexPageEmissionKernelTests`, strict leaf/intermediate and structural tests; existing hosted index-emission benchmarks | Exact suffix copy and stable list/layout inputs remain adapter contracts; ordered trees, separators, allocation and full edit paths remain open. |
| Ordered INLINE/REFERENCE bitmap member scan and strict prefix refusal | `Pages/UsageMap.cs` `TryEnumerateBitmapPages`; generated `UsageMapKernel` | `UsageMapEnumerationKernelTests`, existing bitmap enumeration and ownership tests | List append, reference-page acquisition/return, cancellation, map identity and complete public resource/ownership composition remain separate. |
| Jet3 jump sizing, monotone decoding and native dummy/encoding decisions | `Pages/Jet3JumpTable.cs`; generated `Jet3JumpCount`, `Jet3JumpReached` and encoder controls | `Jet3JumpTableKernelTests`, existing native long-row/jump controls | Span access and complete row acquisition remain adapter/containing-parser obligations. No row or offset array copy is introduced. |
| Compressed intermediate-entry admission, child selection and bounded progress | `Indexes/IndexPageCodec.cs` `SelectChildPage`; generated `ReadIndexChildEntry`, `AdvanceIndexChildSearch` | Focused child-selection, prefix and malformed-entry controls; public seek regressions | Vectorized comparison sign, layout/page identity and containing tree traversal remain explicit contracts. |
| Journal admission-state transitions and final recorded length authority | `Pages/Paging/PersistentRollbackJournal.cs` `ValidateJournal`; generated `AdvanceJournalAdmission`, `CompleteJournalAdmission` | `JournalAdmissionKernelTests`, `SelectiveRollbackJournalTests`, `BufferedJournalConformanceTests` | Failure-atomic scalar state and prefix controls are verified under exact capture/hash/tail outcomes. Actual record acquisition and map/hash effects remain explicit contracts. |
| Exact journal page-byte authorization and zero-extension admission | `Pages/Paging/PersistentRollbackJournal.cs` `ValidateCurrentContent`; generated authorization array scans | `JournalContentKernelTests`, `SelectiveRollbackJournalTests`, `BufferedJournalConformanceTests` | Exact acquired record identity, hashes/maps, I/O effects and complete recovery composition remain separate. |
| Exact bounded data-row slot acquisition and next-distinct offset | `Pages/DataPageRows.cs` `TryGetSlotBound`; generated `ReadDataRowSlot` | `DataRowSlotKernelTests`, `RowDirectoryTests`, overflow/reclamation regressions | The complete slot scan is generated. Other directory builders, row decoding, page identity and overflow traversal remain separate. |
| Bounded inline/reference bitmap admission and exact bit access/update | `Pages/UsageMap.cs` bitmap state readers/writers; generated usage-position and bit methods | `UsageMapBitmapKernelTests`, `UsageMapBitmapEnumerationTests`, usage-map maintenance regressions | Generated updates preserve unrelated bytes/bits and refuse atomically. Map traversal, alias isolation, page ownership and reclamation authority remain separate. |
| Index next-entry selection and exact leaf sentinel/tail admission | `Indexes/IndexPageCodec.cs` `NextEntryStart`, `HasExactLeafPayloadBoundary`; generated `FindIndexEntryStart`, `CheckIndexLeafBitmap` | `IndexEntryBoundaryKernelTests`, `IndexPageCodecStrictLeafTests`, `IndexPageCodecLeafPageTests` | Both complete bounded scans are generated. Key reconstruction/comparison, complete tree traversal and structural edit/ownership composition remain separate. |
| Prepare evidence before mutation | `Pages/Paging/PhysicalRollback.cs` `BeginWrite`; `PersistentRollbackJournal.cs` `Create`; all under `src/JetDatabaseWriter/` | `PersistentRollbackJournalTests`, `SelectiveRollbackJournalTests` | Logical captures now derive durable coverage, but preparation parsing, namespace promotion/discovery and actual flush ordering are handwritten. |
| Protect overwritten original bytes | `Pages/Paging/StreamPageStore.cs` `WriteAsync`; `PersistentRollbackJournal.cs` `RecordBeforeWrite`, `CaptureOriginalPage`, `AppendRecord` | `SelectiveRollbackJournalTests`, `FileRollbackTests`, `BufferedJournalConformanceTests` | Buffered effects derive protection from durable logical captures. Page-record extraction/checksums and C# control flow still require conformance. |
| Protect removed original tail | `Pages/Paging/StreamPageStore.cs` `SetLengthAsync`; `PersistentRollbackJournal.cs` `RecordBeforeTruncate` | `ShrinkCrashRecoveryTests`, `RecoveryReplayCrashTests`, `BufferedJournalConformanceTests` | Constructive preparation establishes logical original-tail coverage; generated page selection is shared by production preparation and parser admission. Exact validated map-to-record/hash correspondence, secure erase and actual file-length semantics remain unproved. |
| Data before durable decision; uncertain outcome | `Pages/Paging/PhysicalRollback.cs` `CommitAsync`, `RestoreAsync`; `PersistentRollbackJournal.cs` `Complete` | `TransactionCrashRecoveryTests`, `PersistentRollbackJournalTests` | The composed protocol includes failed flush prefixes and faulted ownership; actual exceptions, buffer persistence and I/O traces still need correspondence. |
| Validate before replay; restart replay | `Pages/Paging/PersistentRollbackJournal.cs` `ValidateJournal`, `ValidateReplayInventory`, `NextReplayPage`, `ValidateCurrentContent`, `RecordRecoveryStart`, `Recover`, `RestoreAsync` | `JournalReplayInventoryTests`, `RecoveryReplayCrashTests`, `SelectiveRollbackJournalTests` | Exact inventory preflight and generated serialized-source selection bind admitted captures to replay membership, payload prefixes and the decreasing control rank under explicit payload/effect contracts. Hash implementations, effectful I/O and full buffered-content composition remain separate. |
| Bounded journal values, framing/cursors, record classification, inventory-bound replay selection, original-page sources, truncate-tail schedules, payload/hash slices and byte authorization | `Verification/RecoveryKernel.g.cs`, generated from `verification/Pages/Paging/RecoveryKernel.dfy`, called by `PersistentRollbackJournal` | `RecoveryKernelTests`, `JournalReplayInventoryTests`, malformed-journal and replay tests; mandatory generated-source freshness | Pure decisions are generated; exact acquisition, checksums and cryptographic identity remain implementation contracts. Serialized inventory and successful replay effects have scoped refinement proofs; the complete effectful C# algorithm is not verified. |
| Valid old/new decision and publication | `Encryption/EncryptionReplacementIntent.cs` `ReadState`, `ValidateState`, `ValidatePublication`, `Copy`, `Publish`, `Recover`, `Commit`, `Cleanup` | `ReplacementKernelTests`, `ReplacementFieldConformanceTests`, `ReplacementIntentTests`, `ReplacementCrashRecoveryTests`, `ReplacementNamespaceModelTests`, `ReplacementCleanupConformanceTests`, `ReplacementCopyConformanceTests`, `ReplacementPublicationConformanceTests` | Generated bounded array scans verify exact header/decision field comparisons and refine byte admission. Focused production controls bind short reads, refusal and validation-before-bootstrap; SHA/path primitives, copy syscalls, full recovery control flow, metadata and namespace persistence remain explicit contracts. |
| Complete long-value descriptor codec, bounded acquisition/serialization, payload admission and row-pointer packing | `LongValues/Models/LongValueDescriptor.cs` `TryRead`, `WriteTo`; `LongValues/LongValueStore.cs` pointer helpers and `ValidatePayload`; `Verification/LongValueKernel.g.cs` | `LongValueDescriptorKernelTests`, `LongValueStoreTests`, malformed long-value and compact-audit tests | Generated array methods prove complete 12-byte acquisition/serialization, exact field inverses, bounds, atomic refusal and preservation outside the descriptor. Span adapters use generated range/field decisions. Payload admission and 24-bit page/8-bit row packing are generated. External-chain traversal, aggregate resource bounds, token meaning and native compatibility remain separate. |
| Bounded external chained-payload traversal | `LongValues/LongValueStore.cs` `CopyPayloadAsync`; `LongValueVisitedSet.cs`; `LongValueKernel.g.cs` | `LongValueTraversalTests`, `LongValueStreamingTests`, `CompactRawDataLongValueValidatorTests` | Generated array loops establish exact lookup, single-scan idempotent add and rehash membership under their contracts. `PrepareVisitedAdd` and `CompleteVisitedGrowth` compose those operations with exact count/growth policy under `TrackedVisited`; zero/duplicate handling, the half-load threshold and bounded growth requests execute in generated code. Operation-specific native decisions preserve the abstract transition through exact classification/payload laws and contextual retained-count projections. Present/full array outcomes remain unchanged, and rehash accepts duplicate source identities while producing unique entries and the exact distinct-source occupied count. Actual allocation, retention/publication of matching array/count state, public C# traversal-schedule correspondence, correspondence of the modeled aggregate probe/overlap bounds, cancellation latency, locator/pool/cache and destination resources, I/O effects and native ownership retain explicit manual contracts. |
| Generation bootstrap and cleanup | `Transactions/RecoverableWriterLock.cs` `RecoveryLease`; `Pages/Paging/PersistentRollbackJournal.cs` `CleanupCompleted` | `RecoverableWriterLockTests`, `RecoveryLockFileTests`, `RecoveryReplayCrashTests`, completed-journal disposal regressions | Staged bootstrap, the abstract coordinated schedule and production-order leased cleanup are modeled; selected incomplete-evidence/refusal/reconstruction/deletion traces have production tests. GUID freshness, receipt parsing and actual lock/fence behavior remain separate obligations. |

The shared range/read boundary is exercised by `SharedParsingKernelTests`: wide-arithmetic boundary oracles, overflowing native pointer offsets, every short-read partition of an eight-byte request, EOF, cancellation, exceptions and invalid returned counts. `ByteRangeIsExactAndOverflowSafe` and `NestedByteSlicesCompose` prove bounded slicing; `BufferReadMakesExactProgress` and `ReadTraceConsumesBoundedPrefix` prove exact cursor progress and bound the successful read count by the initial remainder. `ExactReadTraceAcquiresRequestedPrefix` composes arbitrary finite chunks into the exact requested source prefix under the named stream effects. Production `BinaryBuffer`, pointer readers and exact-read adapters invoke these decisions on both targets; containing parser loops and aggregate budgets remain unproved.

`WriterLifecycle.StepPreserves` and `TracePreserves` preserve legal scope/lease states and terminal transaction identities through arbitrary finite modeled traces. Named rollback, statement, stale-generation and disposal lemmas refine the specification under its explicit successful-effect and bounded-counter contracts. `WriterLifecycleConformanceTests` supplements existing transaction, pager and ownership tests with composed released-child/failed-statement/rollback, maximum-depth disposal and faulted current-generation read/disposal traces. See the [lifecycle mapping](writer-lifecycle.md) for exact operations, assumptions and remaining correspondence gaps.

The recovery-kernel tests exercise the generated boundary and its production callers. `ClassifyJournalRecord` consumes the exact unsigned interpretation of the stored 64-bit tag after framing/checksum validation. Its total native-byte result admits only terminal commits, tail-authorized replay markers, pre-replay truncations, original aligned before-images and bounded after-images. `JournalRecordClassificationIsExact` specifies all result classes; `ReplayStateRefusesFurtherMutations` excludes write/truncate records after replay begins. The handwritten enum-to-result mapping is covered by production conformance tests.

`AdvanceJournalAdmission` owns the journal parser's admission-state transitions:
duplicate capture refusal, capture-before-overwrite and removed-tail requirements,
cumulative minimum and historical maximum lengths, observed-length authorization,
replay restrictions and terminal hash-matched commit selection. Refusal returns
the unchanged input state. `CompleteJournalAdmission` admits an undecided image
only within its recorded bounds and with write/truncate length authority or an
admitted replay marker; a validated commit selects the current image independently
of those undecided bounds. Both entry points refuse invalid native control states.
`ValidateJournal` reads and checks records, supplies exact dictionary membership,
complete tail-coverage and hash-comparison results, and adds a capture only after
the controller accepts it. It retains the sparse dictionary and bounded record
buffer. These effects and their association with the acquired record remain
explicit contracts, alongside checksum/manifest acquisition and exclusive image
identity. `JournalAdmissionTracePreservesPrefix` connects the executing transition
to ordered logical length/replay controls, captured tails, exact historical maximum
and observed-length authority. `CompleteJournalAdmissionHasRecordedLengthAuthority`
composes that invariant with the final gate;
`JournalAdmissionAcceptsUnchangedEmptyJournal` establishes a reachable empty-record
success case. These proofs do not verify the C# read/hash/map adapters or the
complete recovery path.
`JournalAdmissionKernelTests` uses independent state-fold oracles and composed
traces; production duplicate and missing-capture controls require repeated refusal
with unchanged database and journal evidence.

`JournalRecordStart` computes the bounded rounded-up page-hash manifest end. `CompleteJournalEnd` selects exactly the maximal complete-record prefix and excludes the trailing fragment, under the 16 GiB journal bound. Both are total native functions; zero denotes invalid inputs, while every valid start/end is at least 128. Signed adapters reject that sentinel before narrowing. Production manifest validation, preparation-copy comparison and record append/replacement use these generated decisions. The framing proofs establish arithmetic bounds, page rounding and the exact incomplete suffix; actual byte reads and integrity validation remain handwritten.

`NextJournalRecord` authorizes only complete aligned records and advances exactly one bounded record. `JournalRecordCursorMakesBoundedProgress` decreases the remaining-record count, while `IsFinalJournalRecord` includes trailing fragments when deciding terminal commit admission. Production record acquisition and content/parser scans use these decisions. This establishes cursor arithmetic, not progress of dictionary replay, flushes or the complete C# algorithm.

`JournalReplayAction` supplies the production replay control decisions for both static `Recover` and live `RestoreAsync`: conditionally establish the durable removed-tail marker, consume each admitted before-image, restore the original length, flush the database, then publish the terminal decision. Counters and phase advance only after the selected operation returns successfully. `JournalReplayActionRefinesProductionOrder` connects this total native selector to `UndoJournal.PageReplayAction`; `CompletePageReplayControl` constructs an unbounded successful control path whose rank decreases by exactly one for each action. Restart control resets to the complete admitted capture set and retains only a marker that actually survived validation.

The rank verifies control scheduling under successful effects and counts abstract actions, not syscalls; live `Complete` can perform an additional underlying database flush. `SerializedInventoryPreflightEstablishesEquality` derives exact dictionary membership and source positions from complete serialized captures plus cardinality. `ReplaySelectionRefinesExactInventoryPayload` binds the generated `JournalReplayPageOffset` decision to that inventory and the exact original-page prefix. `SerializedPageSelectionConsumesInventory` ties one successful page effect to suffix membership and the decreasing production-control rank; `CompleteSerializedPageReplay` constructs exact restoration under the named payload and successful-effect contracts. Both production replay paths preflight the complete inventory before any replay mutation, then select captures in serialized order rather than dictionary enumeration order. `JournalReplayInventoryTests` mutates internal admitted maps to exercise missing, extra and redirected entries, requires refusal before writes, and checks serialized write order independently of dictionary order. These internal-state controls do not demonstrate an external-file exploit. Exact record acquisition, checksum/hash implementations, returned write/resize effects, flush fences and terminal-decision I/O remain separate contracts; generated selection alone does not establish recovery availability after arbitrary storage reordering.

`OriginalPageLength` computes the exact original prefix of an aligned page, excluding padded bytes beyond original EOF. `OriginalPageHashOffset` locates its bounded 32-byte manifest hash. `JournalRecordPayloadOffset` admits only complete aligned records within the validated complete prefix and locates their page-sized payload after the 8-byte tag. Each total native function returns zero for invalid inputs, which production adapters reject before reading. `OriginalPageSliceIsExact`, `OriginalPageHashSliceIsBounded` and `JournalPayloadSliceIsExactAndBounded` prove these slice decisions. `OriginalPayloadExtractionRefinesCapturedPage` connects the copied original prefix to an exact padded logical page under explicit payload identity. Production capture, validation and both replay paths use these decisions. `OriginalPageRecordPayloadOffset` additionally binds a requested page to the exact before-image tag of a complete record. `ReadOriginalPage` reacquires the full bounded record, validates its checksum and original manifest hash, then copies only the admitted original prefix. `AcquiredOriginalPayloadRefinesCapturedPage` connects that tag association to logical page extraction under explicit exact-payload identity. Raw reads, digest implementations, dictionary construction and exclusive I/O remain handwritten contracts.

`TruncateOriginalPageCount` and `TruncateOriginalPageOffset` select exactly the aligned original pages intersecting a truncate intent's removed tail, including a partial final original page. Count is bounded by 1,048,576; selectors are strictly ordered, and the remaining count decreases. Invalid counts are zero, so production must validate the intent bounds first. The offset selector instead returns the 2 GiB refusal sentinel outside every original page; the signed adapter rejects it before narrowing. Both producer capacity/capture passes and parser coverage admission use this schedule.

`TruncateCoverageAdmissionRefinesSaved` establishes complete removed-tail coverage and exact saved values under explicit exact-record and validated-map-entry-to-`Saved` correspondence. The arithmetic schedule does not establish payload identity by itself. Production admits a map entry only after complete-record framing, checksum/tag classification, duplicate refusal and original-page manifest-hash validation. Focused production controls remove each required tail capture, retain the same rejected evidence across retries, and check complete recovery with prior captures and a partial final page. Dictionary construction, serialized hashes and the full capture/replay loops remain manual refinement obligations.

`BufferedJournalConformanceTests` manually translates selected model transitions into production journal calls and surviving images: mixed bytes from repeated write intents, partial final-page captures, truncate/append tails, repeated torn page and length-restoration cuts, and durable-decision cleanup retries. Failed database flushes before a commit decision retain undecided evidence whether the injected flush persists bytes first or throws first. Repeated replay flush failures retain stable evidence and publish exactly the required conditional tail marker. Sparse re-extension preparation records exact zero-gap after-images before mutation, preserving any existing partial-page prefix, and preflights required capture/intent capacity. `IsAfterImageLength` admits only a nonempty bounded prefix of a valid aligned write page; the parser combines it with its existing minimum/maximum length checks and validated truncation coverage. This admits completed or torn regrowth below original EOF, including a length cut inside an explicit gap intent. These decisions retain the original/replay-zero/after-image classifier; they do not grant arbitrary tail zeros without evidence. Model-derived controls remove required replay markers or tamper with retained prefixes and sparse gaps, then assert refusal without writes and stable evidence across retries. Disposal can later persist buffered bytes; these injected exception traces do not establish distinct power-loss images. Live `RestoreAsync` conformance additionally interrupts partial-final-page replay, subsequent torn writes, resize before/after mutation and the store flush, repeats each failure with retained undecided evidence, and converges through static recovery and cleanup retries. It checks both conditional-marker outcomes and a single terminal decision. The journal/replacement kernel and buffered conformance classes also run in the hosted Linux/macOS focused lanes. These finite traces supplement the unbounded proof; they do not constitute an automated refinement proof or reproduce an OS cache. Broader transition/rejection coverage remains in V6. Process-kill tests exercise actual failure paths but do not establish power-loss behavior. Golden byte/native DAO checks remain necessary for Access compatibility.

Review mapped changes against this remaining manual refinement inventory:

| Unproved operation | Required review obligation |
|---|---|
| Record acquisition and payload identity | Generated framing/slice bounds must precede record acquisition. Record checksums must precede semantic tag/source admission and payload copying; original-page hash identity must precede use of before-image bytes. Signed decoding, duplicate capture refusal and durable coverage must precede authorized mutation. |
| Replay loop and conditional marker | Marker publication must precede zero-tail authorization; record iteration, partial last-page counts, final length and flush/exception cuts must retain replay evidence. |
| Effect ordering | Journal flush must precede database mutation; exact database flush and its fence must precede a commit decision; uncertain decisions must prohibit contradictory in-place undo. |
| Replacement admission and ownership | Byte fields must bind the intended canonical path/generation and validated copies; record admission must not substitute for exclusive receipt/lease authority. |
| Cleanup | Durable selected content and exact-generation ownership must survive interrupted cleanup; stale disposal must preserve a successor's names. |

Mapped source, conformance evidence, tool pins and proof-policy changes require a fresh [proof/trust review record](proof-trust-review.md). CI checks content freshness before verification and retains review refusals, failed inputs, expected-negative-control sources and process diagnostics. This gate records review obligations; it does not establish reviewer judgment or remove the manual refinement inventory above.

## Evidence levels and public claims

Use the narrowest supported description:

1. **Mechanically verified abstract model:** the listed Dafny lemmas hold for their model and assumptions, including composed finite-trace safety and abstract recovery progress.
2. **Implementation correspondence:** named production operations have a justified connection to the model, with conformance traces or a verified executable kernel. A manual table alone does not establish this level.
3. **Qualified storage guarantee:** the required runtime/OS/filesystem/device persistence and coordination assumptions have independent evidence for the advertised platform.
4. **Native format compatibility:** Microsoft-authored specimens and native operations establish the supported JET/ACE behavior. Recovery proofs do not fill unimplemented format or expression semantics.

Do not advertise whole-library formal verification, unconditional crash safety, mathematical hash collision freedom or portable power-loss atomicity from this pilot. Update individual README claims only when their stated evidence is available.

## Verification approach

The undo model follows the principle of placing recoverability conditions before writes: protect every original byte that a write or truncation could destroy, preserve the invariant through torn writes and interrupted replay, and permit a commit decision only after the complete selected image is durable. This is inspired by [PoWER](https://www.usenix.org/conference/osdi25/presentation/leblanc) and [crash refinement](https://www.usenix.org/conference/osdi16/technical-sessions/presentation/sigurbjarnarson); it is a project-specific model, not a reuse of either project's verified implementation.

Dafny supplies both verification-only models and the small executable native kernels. The [generation policy](../../verification/README.md#executable-kernel-generation) records the exact source projection and trusted compiler boundary; generated primitive operations require no runtime support. Its three fixed inlining annotations (two long-value controllers and the byte-batch usage-map controller) require exact unique signatures after raw-runtime rejection and preserve generated bodies. This BCL metadata insertion and the C# compiler/JIT/AOT remain trusted; an advisory inlining hint supplies no performance theorem. Target compatibility, trimming/AOT, production invocation and hosted timing remain acceptance obligations. Dafny external declarations would merely trust existing C#, so they cannot close its proof gap. F* remains an option for a concrete future boundary, but no second verifier or extraction toolchain is introduced.
