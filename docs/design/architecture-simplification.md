# Architecture Simplification

Status: active scouting note plus completed decision record
Date: 2026-05-30

This note keeps the simplification roadmap in one place. The active candidates
below are opportunities that still look large enough to matter. The completed
outcomes record the high-impact backlog that already closed, so future work does
not reopen settled architecture threads or mistake historical implementation
notes for active backlog.

The goal is not formatting, file splitting, or abstraction for its own sake. A
candidate belongs here only if it can plausibly delete or consolidate meaningful
code while preserving performance, readability, Access/DAO compatibility, and
the current public API shape.

For similar future work, treat each item as architecture work: start with
characterization tests and benchmarks, then refactor behind existing public APIs.

## Selection criteria

- Delete or collapse real algorithms, data structures, or parallel code paths.
- Preserve or improve performance by reusing existing fast paths and caches.
- Keep compatibility-sensitive logic byte-for-byte equivalent unless tests or
  fixtures prove the behavioral change is intentional.
- Avoid broad rewrites whose only benefit is naming, formatting, or moving code
  between files.
- Prefer helpers that clarify ownership boundaries already present in the
  codebase.

## Active candidates

The strongest remaining pattern is repeated writer-side table-row scanning code
that could reuse the same owned-page and live-row infrastructure already used by
reader table scans.

### 1. Shared table row walker and owned-page locator

Primary files:

- [../../JetDatabaseWriter/AccessReader.cs](../../JetDatabaseWriter/AccessReader.cs)
- [../../JetDatabaseWriter/AccessWriter.cs](../../JetDatabaseWriter/AccessWriter.cs)
- [../../JetDatabaseWriter/Catalog/CatalogWriter.cs](../../JetDatabaseWriter/Catalog/CatalogWriter.cs)
- [../../JetDatabaseWriter/Relationships/RelationshipCatalogStore.cs](../../JetDatabaseWriter/Relationships/RelationshipCatalogStore.cs)
- [../../JetDatabaseWriter/ComplexColumns/ComplexColumnManager.cs](../../JetDatabaseWriter/ComplexColumns/ComplexColumnManager.cs)
- [../../JetDatabaseWriter/Pages/UsageMap.cs](../../JetDatabaseWriter/Pages/UsageMap.cs)

Why this looks like the best next pure win: reader-side scans already have a
usage-map-aware owned-page discovery path in `AccessReader.GetOwnedDataPagesAsync`.
That path reads the table's owned-page usage map, validates data-page ownership,
caches stable results when no journal is active, and falls back to a whole-file
owner index when the map is unfamiliar or corrupt.

Several writer-side catalog and mutation helpers still repeat the lower-level
pattern directly:

- Iterate every physical page from page 3 to EOF.
- Read the page.
- Skip non-data pages.
- Check `DataPage.TDefOff` against the target TDEF page.
- Enumerate live rows with `EnumerateLiveRowLocations`.
- Decode simple columns from each live row.
- Return the page in a `finally` block.

That code appears in catalog reads, non-table object-id allocation, relationship
catalog discovery, complex-column ID and flat-table lookups, cascade cleanup,
ACE cleanup, and writer `GetLiveRowLocationsAsync`.

Target shape:

- Introduce a shared internal row-walk surface, such as `TableRowScanner` or
  `OwnedDataPageLocator`, usable by both `AccessReader` and `AccessWriter`.
- Expose an internal method that returns candidate data pages for a TDEF page,
  using the owned-page usage map when possible and the current full-file owner
  index fallback otherwise.
- Expose row iteration as `(pageNumber, page, RowLocation)` or a narrow visitor
  callback so callers can decode only the columns they need without materializing
  full typed rows.
- Preserve the existing cached reader table-scan path and avoid adding async
  enumerable overhead to hot writer paths where a visitor-style method is
  clearer and cheaper.
- Route writer `GetLiveRowLocationsAsync` through the shared page locator.
- Convert repeated catalog, relationship, and complex-column scans one at a
  time after the shared primitive is covered by tests.

Likely payoff:

- High deletion potential across `AccessWriter`, `CatalogWriter`,
  `RelationshipCatalogStore`, and `ComplexColumnManager`.
- Likely performance improvement for Access-authored or writer-authored tables
  with recognized owned-page maps, because many writer helpers can stop walking
  every physical page.
- Better consistency in page-return and cancellation behavior.

Risks and guardrails:

- Preserve whole-file fallback for corrupt, unfamiliar, or missing owned-page
  maps.
- Preserve journal semantics: cached owned-page lists are safe only when the
  active journal cannot make page ownership stale.
- Keep writer mutation paths careful around pages that are read for inspection
  and then later modified.
- Do not force full typed row decoding where callers only need one or two
  scalar catalog columns.

Proof plan:

- Characterize the shared locator with inline usage maps, reference usage maps,
  missing maps, corrupt maps, and empty tables.
- Run catalog, relationship, complex-column, index, and schema-evolution tests.
- Run DAO CompactDatabase validation for fresh databases, relationships,
  complex columns, linked tables, and catalog mutations.
- Benchmark or at least time representative writer operations on databases with
  many pages but small target system tables, where avoiding full physical scans
  should show up clearly.

### 2. Delete the stale index leaf facade

Primary files:

- [../../JetDatabaseWriter/Indexes/IndexLeafIncremental.cs](../../JetDatabaseWriter/Indexes/IndexLeafIncremental.cs)
- [../../JetDatabaseWriter/Indexes/IndexPageCodec.cs](../../JetDatabaseWriter/Indexes/IndexPageCodec.cs)
- [../../JetDatabaseWriter/Indexes/IndexBTreeEditor.cs](../../JetDatabaseWriter/Indexes/IndexBTreeEditor.cs)
- [../../JetDatabaseWriter/Indexes/IndexMaintainer.cs](../../JetDatabaseWriter/Indexes/IndexMaintainer.cs)
- [../../JetDatabaseWriter.Tests/Indexes/IndexLeafIncrementalTests.cs](../../JetDatabaseWriter.Tests/Indexes/IndexLeafIncrementalTests.cs)

Why this is interesting: the earlier index simplification work moved most
layout-aware read/write behavior into `IndexPageCodec`, and mutation planning
now lives in `IndexBTreeEditor`. `IndexLeafIncremental` remains as a historical
facade with many pass-through methods to `IndexPageCodec`, plus one meaningful
entry-list splice helper.

Target shape:

- Move callers of page-header reads, entry decoding, child-pointer reads, and
  leaf rebuilds directly to `IndexPageCodec` or `IndexLeafPageBuilder`.
- Preserve the splice algorithm as a small focused helper, such as
  `IndexEntrySplicer` or `IndexEntrySet.Splice`.
- Delete `IndexLeafIncremental` once it no longer owns a distinct concept.
- Rename or retarget tests from `IndexLeafIncrementalTests` to codec/splicer
  tests instead of keeping a test suite for a deleted facade.

Likely payoff:

- Moderate direct deletion.
- Clearer index ownership: codec for page format, cursor for navigation, editor
  for mutation planning, splicer for in-memory entry-list edits.
- Less confusion around the term `incremental`, since multi-leaf and multi-level
  mutation now lives elsewhere.

Risks and guardrails:

- Keep Jet3 and Jet4/ACE layout selection explicit through
  `IndexLeafPageBuilder.LeafPageLayout` or a successor descriptor.
- Preserve all existing single-leaf, multi-leaf, and intermediate-page tests.
- Do not change `Splice` ordering semantics: duplicate keys currently retain a
  deterministic tie order by tagging the original entry order.

Proof plan:

- Run the full `Indexes` test namespace.
- Run index relationship and DAO compact tests that exercise maintained B-trees.
- Diff emitted page invariants before and after for representative index pages.

### 3. Unify insert batch mutation flow

Primary files:

- [../../JetDatabaseWriter/AccessWriter.cs](../../JetDatabaseWriter/AccessWriter.cs)
- [../../JetDatabaseWriter/Indexes/UniqueIndexChecker.cs](../../JetDatabaseWriter/Indexes/UniqueIndexChecker.cs)
- [../../JetDatabaseWriter/Relationships/RelationshipEnforcer.cs](../../JetDatabaseWriter/Relationships/RelationshipEnforcer.cs)
- [../../JetDatabaseWriter/Schema/ConstraintRegistry.cs](../../JetDatabaseWriter/Schema/ConstraintRegistry.cs)
- [../../JetDatabaseWriter/ValueDecoding/RowMapper.cs](../../JetDatabaseWriter/ValueDecoding/RowMapper.cs)

Why this is interesting: the public row insertion paths differ mostly in how
they convert caller input into `object[]` rows. After mapping, they run the same
pipeline:

- Apply column constraints and collect AutoNumber checkpoints.
- Materialize pending rows so unique checks can see the whole batch.
- Pre-check unique indexes.
- Enforce foreign keys.
- Insert row bytes and record row locations.
- Augment in-memory relationship sets.
- Maintain indexes incrementally or fall back to rebuild.
- Update AutoNumber high-water values.
- Roll back inserted rows and restore AutoNumber counters on failure.

Both `InsertRowsAsync(string, IEnumerable<object?[]>)` and
`InsertRowsAsync<T>(string, IEnumerable<T>)` carry this structure separately,
and the single-row path contains a smaller version of the same mutation and
rollback protocol.

Target shape:

- Introduce an internal prepared-insert-batch helper that accepts a row mapper or
  already-normalized `object[]` rows.
- Keep public overload validation at the overload boundary, but share the
  mutation pipeline after rows are normalized.
- Keep rollback behavior explicit and testable: inserted row rollback, TDEF
  row-count adjustment, and AutoNumber restoration should remain one cohesive
  unit.
- Avoid hiding the ordering of constraint, FK, unique-index, row-write, and
  index-maintenance phases behind overly generic callbacks.

Likely payoff:

- Moderate to high deletion in `AccessWriter`.
- Better reliability: one rollback path to audit for duplicate-key, FK, and
  index-maintenance failures.
- Easier future work on bulk insert performance because there is one main insert
  pipeline.

Risks and guardrails:

- Preserve AutoNumber assignment timing before unique checks.
- Preserve FK parent-set augmentation after each successful insert.
- Preserve current exception behavior and best-effort rollback behavior.
- Do not allocate extra row copies in hot insert paths.

Proof plan:

- Run writer insert, bulk insert, unique-index, AutoNumber, FK, transaction, and
  index-maintenance tests.
- Add focused tests that make each phase fail and assert rows/counters/indexes
  are left in the expected state.
- Benchmark bulk insert with and without indexes to verify allocation and
  throughput stay neutral or better.

### 4. Make logical TDEF chains a small shared data structure

Primary files:

- [../../JetDatabaseWriter/AccessBase.cs](../../JetDatabaseWriter/AccessBase.cs)
- [../../JetDatabaseWriter/AccessWriter.cs](../../JetDatabaseWriter/AccessWriter.cs)
- [../../JetDatabaseWriter/Relationships/RelationshipManager.cs](../../JetDatabaseWriter/Relationships/RelationshipManager.cs)
- [../../JetDatabaseWriter/Schema/TDefPageBuilder.cs](../../JetDatabaseWriter/Schema/TDefPageBuilder.cs)

Why this is interesting: `AccessBase.ReadTDefBytesAsync` already stitches a TDEF
page chain into a logical byte buffer for readers. Relationship mutation needs
the same logical buffer plus the physical page numbers so it can write changes
back, so `RelationshipManager` carries a parallel `LogicalTDefChain`,
logical-capacity math, page materialization, and write-back helpers.

Target shape:

- Introduce a schema-level `LogicalTDefChain` helper that can read and stitch a
  TDEF chain, retain physical page numbers when requested, compute logical
  capacity/page counts, ensure logical buffer capacity, materialize logical bytes
  back into physical TDEF pages, and write the chain while allocating or
  deallocating continuation pages as needed.
- Route `AccessBase.ReadTDefBytesAsync` through the read-only side of that
  helper.
- Route relationship TDEF mutation write-back through the mutable side.

Likely payoff:

- Moderate deletion in `RelationshipManager` and `AccessBase`.
- Fewer independent copies of page-chain math.
- More obvious ownership for future schema mutations that need multi-page TDEF
  write-back.

Risks and guardrails:

- Preserve the logical layout exactly: first page contributes the full page;
  continuation pages contribute bytes after offset 8 only.
- Preserve next-page pointers and first-page free-space/tdef-length fields.
- Preserve deallocation behavior for no-longer-needed continuation pages.
- Keep malformed-chain handling compatible with current `ReadTableDefAsync` and
  relationship mutation behavior.

Proof plan:

- Run schema, relationship, index, and catalog tests covering multi-page TDEFs.
- Add focused tests for growing and shrinking a multi-page TDEF chain.
- Run DAO CompactDatabase validation for relationship create, rename, and drop.

### 5. Promote Numeric fixed-point payload encoding into `NumericEncoder`

Primary files:

- [../../JetDatabaseWriter/ValueEncoding/NumericEncoder.cs](../../JetDatabaseWriter/ValueEncoding/NumericEncoder.cs)
- [../../JetDatabaseWriter/ValueEncoding/RowEncoder.cs](../../JetDatabaseWriter/ValueEncoding/RowEncoder.cs)
- [../../JetDatabaseWriter/Indexes/IndexKeyEncoder.cs](../../JetDatabaseWriter/Indexes/IndexKeyEncoder.cs)

Why this is interesting: `NumericEncoder` already owns decimal decomposition,
but row storage and index-key encoding both still perform similar work around
natural scale, declared scale, `BigInteger` rescaling, and 16-byte unsigned
mantissa shaping. The output frames differ, but the middle algorithm can be
shared.

Target shape:

- Add a helper that converts a decimal value and target scale into sign, natural
  scale, and a 16-byte big-endian unsigned magnitude.
- Let `RowEncoder.EncodeNumericValue` use it for JET 17-byte column storage.
- Let `IndexKeyEncoder.EncodeNumericEntry` use it before applying
  Access/Jackcess index-key twiddling rules.
- Keep precision checks and exception types compatible with existing callers.

Likely payoff:

- Smaller deletion than the row-walker or insert-flow work, but clean and
  bounded.
- Reduces the chance that future Numeric fixes land in one path and not the
  other.

Risks and guardrails:

- Numeric precision, rounding, and scale behavior is compatibility-sensitive.
- Preserve current differences between row storage and index-key output frames.
- Preserve legacy Jet4 MDB vs newer ACCDB numeric index twiddling rules.

Proof plan:

- Add edge-case tests for zero, negative values, max precision, scale rounding,
  overflow past 16-byte mantissa, and declared-scale index keys.
- Run numeric value-encoding, index seek-key, unique-index, FK, and writer
  round-trip tests.
- Benchmark numeric-heavy row writes and index maintenance for neutral results.

### Lower-payoff candidates

These may still be worth doing opportunistically, but they do not meet the
current large meaningful simplification bar on their own.

- **Binary slice helpers:** `DirectRowDecoderBuilder`, `RowDecodePlan`,
  complex-column attachment decoding, and some tests all contain tiny byte-slice
  or data-URI parsing helpers. A shared helper could remove a few lines and
  standardize edge behavior, but the payoff is small.
- **LVAL row location wrapper:** `LongValueDecoder` mostly delegates
  row-location math to `LongValueStore`. There may be a small cleanup around
  passing cached row bounds directly, but this is not large enough to treat as
  architecture work unless another LVAL refactor is already underway.
- **Text linked-table enumeration:** `LinkedTableManager` has separate surfaces
  for counting rows, streaming rows, metadata, and materializing a `DataTable`.
  Some common source opening and row normalization is already shared. Further
  consolidation might be useful, but the likely deletion is modest.

### Not recommended as pure wins right now

- **Text index encoder strategy rewrite:** collation encoding is byte-level
  compatibility work with a history of fixture-driven edge cases. A strategy
  simplification may exist, but it should begin with byte-for-byte
  characterization over Access-authored fixtures and DAO-generated samples.
- **Generic page builder abstraction:** TDEF pages, data pages, index leaves,
  index intermediates, LVAL pages, and CFB sectors have different headers,
  trailers, ownership rules, and validation contracts. A generic builder would
  likely add indirection without deleting much real code.
- **Compound File dependency replacement:** the local CFB layer was already
  reassessed and kept. Its runtime surface is narrow, internal, and tailored to
  Office Crypto streams. Adding a dependency would trade local code for
  supply-chain, package-size, and attack-surface risk.
- **Row decode plan redux:** the major row-reader consolidation already landed.
  Further changes should be motivated by specific performance data or bug fixes,
  not another broad pass over row decoding.

Suggested order:

1. Shared table row walker and owned-page locator.
2. Delete or shrink `IndexLeafIncremental` into a focused splicer helper.
3. Unify insert batch mutation flow.
4. Extract logical TDEF chain read/write helpers.
5. Promote Numeric fixed-point rescaling and magnitude shaping into
   `NumericEncoder`.

## Completed outcomes

### 1. Shared index cursor and B-tree editor

Status: completed 2026-05-30.

Primary files:

- [../../JetDatabaseWriter/Indexes/IndexMaintainer.cs](../../JetDatabaseWriter/Indexes/IndexMaintainer.cs)
- [../../JetDatabaseWriter/Indexes/IndexLeafIncremental.cs](../../JetDatabaseWriter/Indexes/IndexLeafIncremental.cs)
- [../../JetDatabaseWriter/Indexes/IndexBTreeBuilder.cs](../../JetDatabaseWriter/Indexes/IndexBTreeBuilder.cs)
- [../../JetDatabaseWriter/Indexes/IndexBTreeEditor.cs](../../JetDatabaseWriter/Indexes/IndexBTreeEditor.cs)
- [../../JetDatabaseWriter/Indexes/IndexCursor.cs](../../JetDatabaseWriter/Indexes/IndexCursor.cs)
- [../../JetDatabaseWriter/Indexes/IndexPageCodec.cs](../../JetDatabaseWriter/Indexes/IndexPageCodec.cs)
- [../../JetDatabaseWriter/Indexes/UniqueIndexChecker.cs](../../JetDatabaseWriter/Indexes/UniqueIndexChecker.cs)
- [../../JetDatabaseWriter/Relationships/RelationshipChildRowLocator.cs](../../JetDatabaseWriter/Relationships/RelationshipChildRowLocator.cs)
- [../../JetDatabaseWriter/Relationships/RelationshipEnforcer.cs](../../JetDatabaseWriter/Relationships/RelationshipEnforcer.cs)

Index logic had parallel implementations for full rebuild, single-leaf splice,
append-tail maintenance, same-leaf surgery, cross-leaf surgery, catalog
splicing, relationship seeks, and uniqueness checks. The shared cursor/editor
work routed read-only seeks through `IndexCursor`, write-side mutation planning
through `IndexBTreeEditor`, page byte construction and sibling-pointer patches
through `IndexPageCodec`, and unique pre-insert checks through the same encoded
key and cursor infrastructure. `IndexBTreeSeeker` was deleted.

Evidence at closeout: focused pre-write/cursor tests passed, the full index
namespace passed, `dotnet build --no-restore` passed, and the non-fuzz suite
passed with environment skips only.

Preserve these guardrails: keep Jet3 and Jet4/ACE layout selection explicit,
keep MSysObjects on the byte-preserving catalog path, and validate future index
page changes against index, relationship, catalog, DAO CompactDatabase, and
round-trip tests documented in [index-and-relationship-format-notes.md](index-and-relationship-format-notes.md),
[catalog-index-maintenance-notes.md](catalog-index-maintenance-notes.md), and
[writer-disk-format-validation-matrix.md](writer-disk-format-validation-matrix.md).

### 2. Row decode plan

Status: completed 2026-05-30.

Primary files:

- [../../JetDatabaseWriter/AccessReader.cs](../../JetDatabaseWriter/AccessReader.cs)
- [../../JetDatabaseWriter/AccessBase.cs](../../JetDatabaseWriter/AccessBase.cs)
- [../../JetDatabaseWriter/ValueDecoding/DirectRowDecoderBuilder.cs](../../JetDatabaseWriter/ValueDecoding/DirectRowDecoderBuilder.cs)
- [../../JetDatabaseWriter/ValueDecoding/RowMapper.cs](../../JetDatabaseWriter/ValueDecoding/RowMapper.cs)
- [../../JetDatabaseWriter/ValueDecoding/TypedRowFallbackPolicy.cs](../../JetDatabaseWriter/ValueDecoding/TypedRowFallbackPolicy.cs)
- [../../JetDatabaseWriter/AccessWriter.cs](../../JetDatabaseWriter/AccessWriter.cs)

String row cracking, typed object-array cracking, pooled-buffer cracking, direct
POCO decoding, materialized POCO reads, and writer partial key-column reads now
share `RowDecodePlan` for row-layout parsing and column-slice resolution.
`AccessReader` remains responsible for asynchronous long-value resolution and
scan-level complex-column/hyperlink post-processing.

Evidence at closeout: `dotnet build --no-restore` passed, the non-fuzz suite
passed with environment skips only, and focused BenchmarkDotNet ShortRun results
for affected hot paths stayed neutral or better. Treat [read-performance-bottlenecks.md](read-performance-bottlenecks.md)
as the current performance baseline.

Preserve malformed-row fallback behavior, calculated-column handling, hyperlink
wrapping, complex-column post-processing, and `RowsAsStrings()` empty-string
semantics.

### 3. Usage map and page ownership model

Status: completed 2026-05-26.

Primary files:

- [../../JetDatabaseWriter/AccessReader.cs](../../JetDatabaseWriter/AccessReader.cs)
- [../../JetDatabaseWriter/AccessWriter.cs](../../JetDatabaseWriter/AccessWriter.cs)
- [../../JetDatabaseWriter/Pages/DataPageInserter.cs](../../JetDatabaseWriter/Pages/DataPageInserter.cs)
- [../../JetDatabaseWriter/Pages/PageAllocator.cs](../../JetDatabaseWriter/Pages/PageAllocator.cs)
- [../../JetDatabaseWriter/Indexes/IndexMaintainer.cs](../../JetDatabaseWriter/Indexes/IndexMaintainer.cs)
- [../../JetDatabaseWriter/Constants.cs](../../JetDatabaseWriter/Constants.cs)

INLINE and REFERENCE usage-map bit math now routes through `Pages/UsageMap.cs`.
Reader owned-page discovery, data-page insertion, global free-map allocation,
index usage-map emission, table-drop cleanup, and index-maintenance refresh use
the shared codec.

Preserve recognized per-table usage-map fast path performance, whole-file
fallback for corrupt or unfamiliar owned-page maps, Jet3 behavior, and coverage
for INLINE base windows, out-of-window pages, REFERENCE maps, global free maps,
table owned maps, and index-page maps.

### 4. Declarative catalog artifact planning

Status: completed 2026-05-30.

Primary files:

- [../../JetDatabaseWriter/AccessWriter.cs](../../JetDatabaseWriter/AccessWriter.cs)
- [../../JetDatabaseWriter/Catalog/CatalogWriter.cs](../../JetDatabaseWriter/Catalog/CatalogWriter.cs)
- [../../JetDatabaseWriter/ComplexColumns/ComplexColumnManager.cs](../../JetDatabaseWriter/ComplexColumns/ComplexColumnManager.cs)
- [../../JetDatabaseWriter/Relationships/RelationshipManager.cs](../../JetDatabaseWriter/Relationships/RelationshipManager.cs)
- [../../JetDatabaseWriter/Schema/TDefPageBuilder.cs](../../JetDatabaseWriter/Schema/TDefPageBuilder.cs)

Table, system, complex-table, relationship, linked-table, replacement, and
deletion catalog work now flows through artifact plans and shared executor
primitives instead of parallel row-mutation paths. `CreateTableInternalAsync` is
a thin facade over the plan executor. Fresh ACCDB core system tables, complex
type-template tables, hidden complex flat tables, relationship catalog objects,
linked-table catalog rows, and schema-rewrite replacement/deletion primitives all
share the same planning surface.

Evidence at closeout: Debug library build passed, focused catalog/linked-table/
relationship/complex schema-evolution/general schema-evolution/relationship
mutation tests passed, and the full non-fuzz suite passed with environment skips
only.

Preserve bootstrap ordering for fresh full-catalog ACCDB files, MSysObjects
incremental-only requirements, complex-column parent identity rules, and DAO
compact/open-recordset validation for complex columns, relationships, linked
tables, and fresh database creation.

### 5. Compound File dependency decision

Status: completed 2026-05-30; no dependency introduced.

Primary files:

- [../../JetDatabaseWriter/CompoundFile/CompoundFileReader.cs](../../JetDatabaseWriter/CompoundFile/CompoundFileReader.cs)
- [../../JetDatabaseWriter/CompoundFile/CompoundFileWriter.cs](../../JetDatabaseWriter/CompoundFile/CompoundFileWriter.cs)
- [../../JetDatabaseWriter/Encryption/EncryptionConverter.cs](../../JetDatabaseWriter/Encryption/EncryptionConverter.cs)
- [../../JetDatabaseWriter/Encryption/EncryptionManager.cs](../../JetDatabaseWriter/Encryption/EncryptionManager.cs)

The local CFB implementation remains. The deletion upside from adding a runtime
CFB package was outweighed by supply-chain review, attack surface,
target-framework/package-size constraints, and hot-path risk for code limited to
`EncryptionInfo` / `EncryptedPackage` stream extraction and emission. OpenMcdf
remains a test-fixture source only, not a runtime dependency.

Preserve Agile, Agile CFB, Standard, and legacy AES CFB-wrapped behavior;
defensive bounds against crafted CFB inputs; and explicit dependency, target
framework, package size, and license review if this decision is reopened.

## Completed backlog order

1. IndexPageCodec plus read-only IndexCursor: completed 2026-05-26.
2. IndexBTreeEditor mutation planner: completed 2026-05-26.
3. UsageMap ownership model: completed 2026-05-26.
4. RowDecodePlan, gated by BenchmarkDotNet evidence: completed 2026-05-30.
5. Declarative catalog artifact planning: completed 2026-05-30.
6. CFB dependency decision: completed 2026-05-30; no dependency introduced.

## Non-goals

- Do not remove measured read-path optimizations unless benchmarks show the
  replacement is neutral or better.
- Do not bulk-rebuild MSysObjects from decoded rows.
- Do not trade DAO CompactDatabase compatibility for smaller code.
- Do not turn simplification into a formatting or file-splitting exercise; the
  goal is fewer concepts, fewer duplicate algorithms, and fewer parallel code
  paths.
