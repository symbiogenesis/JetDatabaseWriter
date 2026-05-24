# ESE-Inspired Coverage Gap Analysis

**Status:** Updated after page allocator, secure-erase, and tail-shrink implementation, 2026-05-21.
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

### 2. Page Integrity and Structural Validation (DONE)

ESE has explicit checksum/page validation tests such as checksum set/fix/fail cases, bad page validation callbacks, CPAGE insert/replace/delete tests, tag-boundary corruption, and logged-data checksum stability.

JetDatabaseWriter has many targeted byte-format tests, especially for TDEFs, index pages, catalog rows, and fuzz/corruption robustness. It now also has a reusable emitted-page invariant checker for writer-created streams.

Implemented coverage (2026-05-21):

- Added `EmittedPageInvariantAssert` in [EmittedPageInvariantAssert.cs](../../JetDatabaseWriter.Tests/Infrastructure/EmittedPageInvariantAssert.cs) for data, TDEF, LVAL, usage-map, leaf-index, and intermediate-index pages.
- Validates data-page row slot directories: slot offsets are in range, unique after sorting into row bounds, non-overlapping, and marked deleted/overflow rows still point at coherent payload bounds.
- Validates free-space hints for data, usage-map, LVAL, TDEF, leaf-index, and intermediate-index pages, including the index entry-start sentinel bit.
- Aggregates decoded live data rows by parent TDEF and compares them with each TDEF `row_count`; also keeps the per-real-idx `num_idx_rows` counters in the same sweep.
- Added [EmittedPageInvariantTests.cs](../../JetDatabaseWriter.Tests/Pages/EmittedPageInvariantTests.cs), which runs the helper after representative create, insert, update, delete, leaf-index maintenance, multi-level intermediate-index maintenance, usage-map emission, and chained-LVAL writes across Jet3/Jet4/ACE where applicable.

This is structural validation, not a checksum oracle or byte-for-byte DAO comparison. DAO CompactDatabase coverage remains the stronger compatibility check for high-risk disk-format changes.

### 3. Deleted Data Scrubbing and Data Remanence (DONE)

ESE tests `DeleteScrubsData`, `ReplaceScrubsData`, `ReorganizePageScrubsData`, and overwrite-unused-space behavior. JetDatabaseWriter still defaults to normal JET logical delete semantics, but the writer now also has an opt-in secure-erase mode that overwrites deleted row bodies and old MEMO/OLE LVAL chains before returning their pages to the Access global free list.

Local tests already asserted deleted rows are no longer readable through public APIs. The byte-level behavior is now explicit for both modes: default delete/update preserve old row payload bytes and old LVAL pages until reuse, while secure erase removes those bytes before free-list reclamation.

Implemented coverage (2026-05-21):

- Added [DataRemanenceTests.cs](../../JetDatabaseWriter.Tests/Writer/DataRemanenceTests.cs) byte-level coverage for Jet3, Jet4, and ACE inline row payloads: `DeleteRowsAsync` flips the normal deleted-slot bit (`0x8000`) without overwriting the row body, and `UpdateRowsAsync` leaves the old slot deleted with its original bytes intact while inserting a replacement row. The separate `0x4000` row-offset flag is treated as non-live/overflow, not as the deletion marker the writer should set.
- Added Jet4/ACE coverage for oversized OLE values stored on chained LVAL pages: update appends a fresh chain and leaves the old LVAL pages present; delete leaves both old and current LVAL pages present after the live row is removed.
- Added secure-erase coverage showing `SecureEraseMode.DeletedRowsAndFreedPages` removes inline row payload markers and old LVAL markers from the file before reclamation.
- Documented the behavior in [README.md](../../README.md): update/delete are logical mutations by default, while secure erase is an explicit writer option with storage-level caveats.

### 4. Public Index Seek and Cursor Navigation (DONE)

ESE's public model includes indexed and sequential cursor navigation. JetDatabaseWriter now has substantial index writing, maintenance, internal seek support through [IndexBTreeSeeker.cs](../../JetDatabaseWriter/Indexes/IndexBTreeSeeker.cs), and a narrow public exact-seek reader surface.

Implemented coverage (2026-05-21):

- Added `IAccessReader.SeekRowsAsync(tableName, indexName, keyValues, CT)` and [AccessReader.cs](../../JetDatabaseWriter/AccessReader.cs) plumbing for exact Jet4/ACE index seeks over an index name and key tuple.
- Reuses `IndexBTreeSeeker.FindRowLocationsAsync` for root descent, prefix-compressed leaf decoding, non-unique sibling-leaf walks, and tail-page fall-through, then materialises rows through the same typed row decoder used by `Rows(...)`.
- Added [AccessReaderIndexSeekTests.cs](../../JetDatabaseWriter.Tests/Reader/AccessReaderIndexSeekTests.cs) for unique and non-unique indexes, composite keys, missing keys, sibling-leaf walks, tail-page append fall-through, Jet3 rejection, and seek results matching full table scans for supported key types.

Range scans remain separate until there is a clear API design.

### 5. Access Compact and Repair Validation Automation (DONE)

ESE's test culture validates engine behavior through real persisted state. JetDatabaseWriter already has DAO/Access validation hooks; earlier design notes had standing warnings that several writer phases needed Microsoft Access Compact and Repair validation. Those blanket warnings now point at the active matrix or a feature-specific residual gap. Examples included [index-and-relationship-format-notes.md](index-and-relationship-format-notes.md) and [complex-columns-format-notes.md](complex-columns-format-notes.md).

Implemented coverage (2026-05-21):

- Added a cross-feature validation checklist for writer-emitted disk-format features: [writer-disk-format-validation-matrix.md](writer-disk-format-validation-matrix.md), with durable DAO validation rules split into [dao-validation-strategy.md](dao-validation-strategy.md).
- Added a Northwind-hosted DAO CompactDatabase test for writer-created attachment and multi-value complex columns, including wrapper-encoded attachment `FileData`, Access-style LVAL pages, a chained-LVAL payload, flat-table index maintenance, and an `AddColumnAsync` rewrite that preserves the complex artifacts.
- Clarified that the strongest DAO compact tests mutate Access-authored fixtures such as Northwind, so the writer-created bytes under test are isolated from fresh-database bootstrap trust.
- Updated stale blanket warnings in the index/relationship and complex-column notes to point at the matrix and the remaining phase-specific gaps.
- Cleaned the README and public XML comments that still described encryption, attachment fields, index maintenance, or relationship enforcement using older caveats.

Remaining checklist items should be promoted into DAO-driven tests when they become high-risk release blockers and a reliable Access-authored fixture can host the mutation.

### 6. Cache and Resource-Manager Behavior (DONE)

ESE has trace-driven resource-manager tests for LRU/LRU-K, supercold pages, no-touch traces, DB-scan replay, dirty/write stats, and abrupt cycles. JetDatabaseWriter has focused LRU unit coverage in [LruCacheTests.cs](../../JetDatabaseWriter.Tests/Infrastructure/LruCacheTests.cs), plus small reader cache allocation tests in [AccessReaderCacheTests.cs](../../JetDatabaseWriter.Tests/Reader/AccessReaderCacheTests.cs).

Implemented coverage (2026-05-21):

- Added reader integration tests in [AccessReaderCacheTests.cs](../../JetDatabaseWriter.Tests/Reader/AccessReaderCacheTests.cs) that force page-cache and row-bounds-cache eviction during a large synthetic ACE table scan with a tiny cache.
- Covered interleaved reads across multiple tables, including repeated `ListTablesAsync` calls that reuse the catalog cache and repeated table scans that hit row-bounds/page caches without new row-bounds misses.
- Expanded uncached-reader coverage to compare cached and uncached readers over the same multi-table stream while verifying `OpenUncachedAsync` does not allocate page or row-bounds caches.
- Pinned the transaction-local edge case where an active `PageJournal` must override already-cached reader page bytes; `ReadPageCachedAsync` and row-bounds lookup now bypass reader caches while a journal is active.

This is deterministic integration coverage rather than ESE-style trace replay. The remaining ESE resource-manager scenarios around supercold/no-touch pages and dirty/write stats do not currently map to JetDatabaseWriter's direct page-reader charter.

### 7. Relationship Mutation on Multi-Page TDEFs (DONE)

ESE emphasizes wide tables and rich schema evolution. JetDatabaseWriter can emit multi-page TDEFs for wide schemas, and relationship mutation now uses the same stitched-logical-buffer model instead of requiring the endpoint TDEF to fit on one physical page. This is called out in [index-and-relationship-format-notes.md](index-and-relationship-format-notes.md) and implemented in [RelationshipManager.cs](../../JetDatabaseWriter/Relationships/RelationshipManager.cs).

Implemented coverage (2026-05-21):

- Added a logical TDEF-chain mutation layer in [RelationshipManager.cs](../../JetDatabaseWriter/Relationships/RelationshipManager.cs): read the full TDEF chain into a stitched buffer, parse and shift FK sections in logical offsets, then materialise the resized buffer back into physical TDEF pages, appending continuation pages when needed.
- Promoted `RelationshipWriterTests.CreateRelationshipAsync_MultiPageEndpointTDef_EmitsFkLogicalIdxEntries` to a successful round trip for both parent-wide and child-wide endpoints.
- Added wide-endpoint drop and rename coverage in [RelationshipMutationTests.cs](../../JetDatabaseWriter.Tests/Relationships/RelationshipMutationTests.cs), verifying FK logical-idx entries are removed and renamed through multi-page TDEF chains.
- Logical TDEF rewrites now deallocate old continuation pages that are no longer reachable after a shrink, so relationship drop/rename does not leave shortened-chain pages behind for Compact and Repair.

### 8. Complex Columns and LVAL Reclamation (DONE)

ESE has long-value, cleanup, and space-management machinery. JetDatabaseWriter now has Access-aware page reuse, tail shrinking, and opt-in secure erase for the writer path, while still avoiding a misleading promise of full Microsoft Access Compact & Repair page renumbering. The implementation follows the online Jackcess model for page 1 as the global usage map (`PAGE_GLOBAL_USAGE_MAP = 1`, inline/reference map rows, and allocation removing pages from the free map) plus local DAO probes that identify Access's type-`0x09` freed-page sentinel. Secure erase follows the same intent as ESE scrub coverage: overwrite deleted row bodies and freed-page payloads before those bytes become reusable.

Implemented coverage and scope decision (2026-05-21):

- DAO CompactDatabase coverage includes a Northwind-hosted writer-created attachment/multi-value table with wrapper-encoded attachment `FileData`, a chained-LVAL attachment payload, flat-table indexes, and complex-column schema-evolution preservation. It also includes a fresh writer-created ACCDB complex payload table in `FreshWriterCreatedComplexColumns_SurviveCompactAndRepair`.
- [PageAllocator.cs](../../JetDatabaseWriter/Pages/PageAllocator.cs) owns the page-1 global free-list allocator. It reuses pages only when both the global map and the physical page header agree the page is free/invalid, which avoids over-trusting stale Access-authored map bits. It supports inline global maps, reference-map backing pages, contiguous reservations for TDEF/index rebuilds, and Access-style deallocation through a type-`0x09` free-page header.
- [AccessWriter.ShrinkDatabaseAsync](../../JetDatabaseWriter/AccessWriter.cs) truncates trailing globally-free pages without renumbering live pages. This is a tail shrinker, not a full Compact & Repair clone.
- `AccessWriterOptions.SecureEraseMode = SecureEraseMode.DeletedRowsAndFreedPages` overwrites deleted row bodies and old MEMO/OLE LVAL chains before freeing their pages. The default remains `None`, preserving normal JET logical-delete behavior and backward-compatible remanence.
- [DataRemanenceTests.cs](../../JetDatabaseWriter.Tests/Writer/DataRemanenceTests.cs) byte-pins both behaviors: default update/delete leave old inline row bytes and old LVAL pages on disk, while secure erase removes the markers from deleted row bodies and LVAL pages.
- [PageAllocatorTests.cs](../../JetDatabaseWriter.Tests/Pages/PageAllocatorTests.cs) verifies fresh page-1 map initialization, free-page reuse, tail shrinking, and the boundary that interior free pages are not compacted past a live tail across Jet3, Jet4, and ACE formats.
- [DaoStorageMaintenanceTests.cs](../../JetDatabaseWriter.Tests/RoundTrip/DaoStorageMaintenanceTests.cs) runs Access DAO CompactDatabase against Northwind-hosted storage-maintenance mutations: secure-erased deleted rows inside otherwise-live pages, secure-erased old OLE/LVAL chains, ordinary user-table full index rebuilds that replace old index pages, relationship TDEF rewrites that shorten continuation chains, and relationship rename on a multi-page child TDEF. Relationship drop/rename now rewrites `MSysRelationships` as live-only rows; Type=8 relationship `MSysObjects` rows are emitted on create but are not manually renamed or deleted during mutation, because Compact & Repair normalizes them from `MSysRelationships`.
- Fresh writer-created complex payload tables now have representative DAO CompactDatabase coverage for attachment and multi-value row APIs plus flat-table index metadata. The Northwind-hosted test remains the strongest Access-authored-host coverage for schema evolution and the chained-LVAL payload path.
- Broader DAO Compact and Repair coverage should be added when a new complex-column mutation becomes release-critical and a reliable Access-authored or fresh bootstrap fixture can host it. Remaining cleanup gaps are full Access-style live-page compaction/renumbering, byte-scrubbing of arbitrary unused free-space gaps that were not created by secure delete/update, and conservative non-reclamation of replaced index pages for Access system tables and generated complex flat child tables.
- Future storage-mutating features should add matching DAO CompactDatabase scrub/reuse coverage as part of the feature work rather than keeping a separate standing backlog item.

## Documentation Drift Found During Triage (RESOLVED)

The comparison also surfaced comments that were older than the current implementation:

- [AccessReader.cs](../../JetDatabaseWriter/AccessReader.cs) described encrypted databases and attachment fields as unsupported in the class XML comment, while README and tests described encryption and complex-column support.
- [IAccessSchema.cs](../../JetDatabaseWriter/Interfaces/IAccessSchema.cs) had relationship comments that implied runtime referential integrity is handled only by Microsoft Access after Compact and Repair, while README described library-side enforcement.
- Several complex-column comments used `ConceptualTableID` for the per-row flat-table join value without distinguishing it from `MSysComplexColumns.ConceptualTableID`, which now refers to the parent table object/TDEF id in the writer path.
- The 2026-05-23 relationship cleanup also refreshed comments that still described relationship rename/drop as tombstone-plus-reinsert operations, or said fresh `CreateDatabaseAsync` ACCDB outputs lacked `MSysRelationships`. Full-catalog ACCDB bootstrap now scaffolds the core relationship catalog, while Jet/MDB and slim-catalog outputs may still lack it.

These were documentation/comment drift rather than missing functionality and were cleaned up with the complex/LVAL validation work.

## Likely Out of Scope

The following ESE areas do not appear to map directly to this project unless the library's charter expands from file-format writer to embedded database engine:

- SQL parser/query optimizer and general query execution;
- ODBC linked-source execution;
- online backup/restore, incremental backup, and VSS integration;
- full Access-style shrink/repair utilities beyond the implemented free-page tail shrinker;
- multi-instance engine lifecycle and Windows JET API compatibility;
- snapshot isolation/version-store semantics;
- ESE-specific page sizes, page hydration/dehydration, and block-cache internals.

## Next Work Ownership

No standalone ESE-inspired backlog remains in this note. Residual validation work is owned by the checklist in [writer-disk-format-validation-matrix.md](writer-disk-format-validation-matrix.md); keep future DAO promotions there unless a new ESE-derived risk category appears.
