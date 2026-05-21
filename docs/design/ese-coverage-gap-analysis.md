# ESE-Inspired Coverage Gap Analysis

**Status:** Initial triage, 2026-05-20.
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

### 1. Crash Recovery and Commit Durability

ESE has write-ahead logging, log preread, recovery, checkpoint, revert snapshot, and recovery-cleanup test surfaces. JetDatabaseWriter has page-buffered transactions via [PageJournal.cs](../../JetDatabaseWriter/Pages/PageJournal.cs), [JetTransaction.cs](../../JetDatabaseWriter/JetTransaction.cs), and [TransactionLifecycle.cs](../../JetDatabaseWriter/Transactions/TransactionLifecycle.cs), but no durable redo/undo log or recovery pass after process loss.

Local coverage is strong for logical transaction behavior in [JetTransactionTests.cs](../../JetDatabaseWriter.Tests/Writer/JetTransactionTests.cs): begin, commit, rollback, dispose rollback, read-your-writes, page-budget failure, commit-lock byte update, and `UseTransactionalWrites`. The remaining gap is failure injection during commit replay and flush.

Suggested work:

- Add a test stream or file adapter that throws after N page writes during `CommitAsync`.
- Assert the observed partial-commit behavior is documented and stable.
- Add cancellation/failure tests between page replay, commit-lock byte update, and durable flush.
- Decide whether WAL-style recovery is explicitly out of scope or a future feature.

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

### 5. Access Compact and Repair Validation Automation

ESE's test culture validates engine behavior through real persisted state. JetDatabaseWriter already has DAO/Access validation hooks, but the design notes still contain standing warnings that several writer phases need Microsoft Access Compact and Repair validation. Examples include [index-and-relationship-format-notes.md](index-and-relationship-format-notes.md) and [complex-columns-format-notes.md](complex-columns-format-notes.md).

Local DAO validation exists in [DaoValidationTests.cs](../../JetDatabaseWriter.Tests/RoundTrip/DaoValidationTests.cs), [DaoValidationFixture.cs](../../JetDatabaseWriter.Tests/RoundTrip/DaoValidationFixture.cs), and FormatProbe modes such as [DaoBaselineProbe.cs](../../JetDatabaseWriter.FormatProbe/DaoBaselineProbe.cs). The gap is consistency: design notes, test names, and automated coverage should agree on which disk-format phases are validated.

Suggested work:

- Create a single validation matrix for writer-emitted disk-format features.
- Mark each feature as reader-round-trip only, DAO OpenRecordset, DAO CompactDatabase, or manually Access-verified.
- Convert high-risk manual validation items into DAO-driven tests when the host supports DAO.
- Update stale design warnings once coverage exists.

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

ESE has long-value, cleanup, and space-management machinery. JetDatabaseWriter supports complex columns and LVAL chains, but design notes still identify caveats: no old LVAL page reuse on update/delete, Access Compact and Repair validation gaps for some complex-column paths, and flat-table/index artifacts that Access may repair later. See [complex-columns-format-notes.md](complex-columns-format-notes.md).

Suggested work:

- Expand DAO Compact and Repair tests for attachment and multi-value columns with large LVAL payloads.
- Add byte-level tests documenting old LVAL page retention after update/delete.
- Decide whether page reuse is out of scope or a future space-management feature.

## Documentation Drift Found During Triage

The comparison also surfaced comments that appear older than the current implementation:

- [AccessReader.cs](../../JetDatabaseWriter/AccessReader.cs) still describes encrypted databases and attachment fields as unsupported in the class XML comment, while README and tests describe encryption and complex-column support.
- [IAccessSchema.cs](../../JetDatabaseWriter/Interfaces/IAccessSchema.cs) has relationship comments that imply runtime referential integrity is handled by Microsoft Access after Compact and Repair, while README describes library-side enforcement.

These are not missing functionality, but stale public comments can mislead API consumers and should be cleaned up separately.

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

1. Add failure-injection tests for transaction commit replay and flush boundaries.
2. Add a shared emitted-page invariant checker and run it from representative writer tests.
3. Add byte-level data-remanence tests for delete/update and LVAL update/delete.
4. Promote the internal index seeker into a tested public or internal-experimental row seek surface.
5. Build a DAO Compact and Repair validation matrix and use it to close stale design warnings.
6. Add integration cache tests around large scans, interleaved table reads, and transaction-local page reads.
