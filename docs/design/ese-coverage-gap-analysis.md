# ESE-Inspired Coverage Gap Analysis

**Status:** Updated after DAO complex/LVAL validation, 2026-05-21.
**Reference:** [microsoft/Extensible-Storage-Engine](https://github.com/microsoft/Extensible-Storage-Engine).

This note compares JetDatabaseWriter's current reader/writer test surface with themes from Microsoft's Extensible Storage Engine (ESE) repository. ESE is a full database engine, not an Access MDB/ACCDB file-format oracle, so the goal is not feature parity. The useful signal is where ESE's long-lived engine tests expose categories of risk that also matter to a direct JET/ACE page writer.

## Scope

JetDatabaseWriter is a managed reader/writer over Access JET/ACE files. It does not implement the ESE runtime, the Windows JET API, SQL execution, ODBC execution, backup, or multi-instance engine services. See [README.md](../../README.md) and [library-structure.md](library-structure.md) for the local architecture.

The ESE repo is still useful as a coverage guide because it heavily tests:

- page mutation invariants and corruption checks;
- checksum and logged-data stability;
- delete/replace scrubbing behavior;
- cache/resource-manager trace replay;
- transaction, logging, and recovery flows;
- feature/API matrices for supported engine behavior.

## High-Value Gaps

### 1. Crash Recovery and Commit Durability (DONE)

ESE has write-ahead logging, log preread, recovery, checkpoint, revert snapshot, and recovery-cleanup test surfaces. JetDatabaseWriter has page-buffered transactions via [PageJournal.cs](../../JetDatabaseWriter/Pages/PageJournal.cs), [JetTransaction.cs](../../JetDatabaseWriter/JetTransaction.cs), and [TransactionLifecycle.cs](../../JetDatabaseWriter/Transactions/TransactionLifecycle.cs), but no durable redo/undo log or recovery pass after process loss.

Local coverage is strong for logical transaction behavior in [JetTransactionTests.cs](../../JetDatabaseWriter.Tests/Writer/JetTransactionTests.cs): begin, commit, rollback, dispose rollback, read-your-writes, page-budget failure, commit-lock byte update, `UseTransactionalWrites`, commit replay write failure, commit-lock byte failure/cancellation, and durable-flush failure. The remaining gap is true WAL-style redo/undo recovery after process loss.

Implemented coverage (2026-05-20):

- Added a fault-injecting test stream that throws after N page writes during `CommitAsync`.
- Pinned current partial-commit behavior: once commit replay starts, successful page writes remain on disk even when the transaction object is marked rolled back after a later failure.
- Added cancellation/failure coverage around replay, the page-0 commit-lock byte update, and the final durable flush.
- Documented WAL-style recovery as out of scope for the current file-format writer; it would be a future feature requiring a durable redo/undo log and open-time recovery pass.

### 2. Page Integrity and Structural Validation

ESE has explicit checksum/page validation tests such as checksum set/fix/fail cases, bad page validation callbacks, CPAGE insert/replace/delete tests, tag-boundary corruption, and logged-data checksum stability.

JetDatabaseWriter has many targeted byte-format tests, especially for TDEFs, index pages, catalog rows, and fuzz/corruption robustness. It does not appear to have a reusable page-invariant checker that validates every emitted page shape after writes.

Suggested work:

- Add page invariant helpers for data, TDEF, LVAL, usage-map, leaf-index, and intermediate-index pages.
- Validate slot offsets are in range, sorted into coherent row bounds, non-overlapping, and do not point through deleted/overflow markers incorrectly.
- Validate free-space hints and row counts against decoded live rows where the format exposes them.
- Run the invariant helper after representative create/insert/update/delete/index-maintenance operations.

### 3. Deleted Data Scrubbing and Data Remanence

ESE tests `DeleteScrubsData`, `ReplaceScrubsData`, `ReorganizePageScrubsData`, and overwrite-unused-space behavior. JetDatabaseWriter currently marks a deleted row by setting the high bit in the row-offset slot in [AccessWriter.cs](../../JetDatabaseWriter/AccessWriter.cs); it does not overwrite the old row payload.

Local tests assert deleted rows are no longer readable through public APIs, but do not assert whether the old bytes remain on disk. This is probably format-compatible behavior, but it is worth making explicit.

Suggested work:

- Add a byte-level test proving current delete/update behavior either preserves or removes old row payload bytes.
- Document the data-remanence behavior in README if preservation is intentional.
- Consider an opt-in scrub mode that overwrites deleted row payloads and freed row gaps.
- Include LVAL chains in the decision: updated/deleted long values leave old LVAL pages for Access Compact and Repair to reclaim.

### 4. Public Index Seek and Cursor Navigation

ESE's public model includes indexed and sequential cursor navigation. JetDatabaseWriter now has substantial index writing, maintenance, and internal seek support through [IndexBTreeSeeker.cs](../../JetDatabaseWriter/Indexes/IndexBTreeSeeker.cs), but [index-and-relationship-format-notes.md](index-and-relationship-format-notes.md) still calls out public `SeekRowsAsync`-style access as unshipped.

The local reader still primarily enumerates rows through data-page scans. Internal index seeks support referential-integrity paths, but users cannot yet query rows through an index.

Suggested work:

- Add a narrow public or internal-experimental seek API over an index name and key tuple.
- Test unique and non-unique indexes, composite keys, sibling-leaf walks, tail-page fall-through, missing keys, and row materialization.
- Add tests proving seek results match a full table scan for supported key types.
- Keep range scans separate unless there is a clear API design.

### 5. Access Compact and Repair Validation Automation (DONE)

ESE's test culture validates engine behavior through real persisted state. JetDatabaseWriter already has DAO/Access validation hooks, but the design notes still contained standing warnings that several writer phases needed Microsoft Access Compact and Repair validation. Examples included [index-and-relationship-format-notes.md](index-and-relationship-format-notes.md) and [complex-columns-format-notes.md](complex-columns-format-notes.md).

Implemented coverage (2026-05-21):

- Added a single validation matrix for writer-emitted disk-format features: [writer-disk-format-validation-matrix.md](writer-disk-format-validation-matrix.md).
- Added a Northwind-hosted DAO CompactDatabase test for writer-created attachment and multi-value complex columns, including wrapper-encoded attachment `FileData`, Access-style LVAL pages, a chained-LVAL payload, flat-table index maintenance, and an `AddColumnAsync` rewrite that preserves the complex artifacts.
- Clarified that the strongest DAO compact tests mutate Access-authored fixtures such as Northwind, so the writer-created bytes under test are isolated from fresh-database bootstrap trust.
- Updated stale blanket warnings in the index/relationship and complex-column notes to point at the matrix and the remaining phase-specific gaps.
- Cleaned the README and public XML comments that still described encryption, attachment fields, index maintenance, or relationship enforcement using older caveats.

Remaining matrix rows still marked reader-round-trip only should be promoted into DAO-driven tests when they become high-risk release blockers and a reliable Access-authored fixture can host the mutation.

### 6. Cache and Resource-Manager Behavior

ESE has trace-driven resource-manager tests for LRU/LRU-K, supercold pages, no-touch traces, DB-scan replay, dirty/write stats, and abrupt cycles. JetDatabaseWriter has focused LRU unit coverage in [LruCacheTests.cs](../../JetDatabaseWriter.Tests/Infrastructure/LruCacheTests.cs), plus small reader cache allocation tests in [AccessReaderCacheTests.cs](../../JetDatabaseWriter.Tests/Reader/AccessReaderCacheTests.cs).

The remaining local risk is integration behavior rather than the basic LRU data structure.

Suggested work:

- Add reader integration tests that exercise page-cache eviction during large table scans.
- Cover interleaved reads across multiple tables and repeated calls that reuse catalog and row-bounds caches.
- Verify uncached readers do not allocate page caches and still return equivalent data.
- Verify active transaction journals override cached page bytes for read-your-writes paths.

### 7. Relationship Mutation on Multi-Page TDEFs

ESE emphasizes wide tables and rich schema evolution. JetDatabaseWriter can emit multi-page TDEFs for wide schemas, but parts of relationship mutation still require single-page TDEF mutation and throw `NotSupportedException` when the TDEF cannot be mutated in place. This is called out in [index-and-relationship-format-notes.md](index-and-relationship-format-notes.md) and implemented in [RelationshipManager.cs](../../JetDatabaseWriter/Relationships/RelationshipManager.cs).

Suggested work:

- Add pinning tests for the current multi-page TDEF relationship limitation.
- Add design notes for lifting it using the same logical-buffer/page-chain split used by TDEF creation.
- Cover create, drop, and rename relationship paths for wide tables once implemented.

### 8. Complex Columns and LVAL Reclamation

ESE has long-value, cleanup, and space-management machinery. JetDatabaseWriter supports complex columns and Access-style LVAL chains. DAO CompactDatabase coverage now includes a Northwind-hosted writer-created attachment/multi-value table with wrapper-encoded attachment `FileData`, a chained-LVAL attachment payload, flat-table indexes, and complex-column schema-evolution preservation. Remaining caveats are narrower: no old LVAL page reuse on update/delete, fresh writer-created complex system-table scaffolding is still reader-round-trip only, and broader complex-column mutation coverage should be added when a new mutation becomes release-critical. See [complex-columns-format-notes.md](complex-columns-format-notes.md) and [writer-disk-format-validation-matrix.md](writer-disk-format-validation-matrix.md).

Suggested work:

- Expand DAO Compact and Repair tests beyond the current Northwind-hosted attachment/multi-value/LVAL representative case when new complex-column mutations become release blockers.
- Add byte-level tests documenting old LVAL page retention after update/delete.
- Decide whether page reuse is out of scope or a future space-management feature.

## Documentation Drift Found During Triage (RESOLVED)

The comparison also surfaced comments that were older than the current implementation:

- [AccessReader.cs](../../JetDatabaseWriter/AccessReader.cs) described encrypted databases and attachment fields as unsupported in the class XML comment, while README and tests described encryption and complex-column support.
- [IAccessSchema.cs](../../JetDatabaseWriter/Interfaces/IAccessSchema.cs) had relationship comments that implied runtime referential integrity is handled only by Microsoft Access after Compact and Repair, while README described library-side enforcement.
- Several complex-column comments used `ConceptualTableID` for the per-row flat-table join value without distinguishing it from `MSysComplexColumns.ConceptualTableID`, which now refers to the parent table object/TDEF id in the writer path.

These were documentation/comment drift rather than missing functionality and were cleaned up with the complex/LVAL validation work.

## Likely Out of Scope

The following ESE areas do not appear to map directly to this project unless the library's charter expands from file-format writer to embedded database engine:

- SQL parser/query optimizer and general query execution;
- ODBC linked-source execution;
- online backup/restore, incremental backup, and VSS integration;
- database shrink/repair utilities;
- multi-instance engine lifecycle and Windows JET API compatibility;
- snapshot isolation/version-store semantics;
- ESE-specific page sizes, page hydration/dehydration, and block-cache internals.

## Suggested Next Test Work

1. Add a shared emitted-page invariant checker and run it from representative writer tests.
2. Add byte-level data-remanence tests for delete/update and LVAL update/delete.
3. Promote the internal index seeker into a tested public or internal-experimental row seek surface.
4. Promote remaining reader-only rows from the writer disk-format validation matrix into DAO tests as risk warrants.
5. Add integration cache tests around large scans, interleaved table reads, and transaction-local page reads.
