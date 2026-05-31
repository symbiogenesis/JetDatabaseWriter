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

The numeric payload encoding, LVAL row-location, and text linked-table
enumeration candidates are now recorded under completed outcomes. No high-payoff
or lower-payoff active simplification candidate is currently open.

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

Suggested order: treat any future lower-payoff candidates as opportunistic
follow-ups when related work is already in progress.

## Completed outcomes

### Completed linked-text row source reader

Status: completed 2026-05-31.

Primary files:

- [../../JetDatabaseWriter/Relationships/LinkedTableManager.cs](../../JetDatabaseWriter/Relationships/LinkedTableManager.cs)
- [../../JetDatabaseWriter/AccessReader.cs](../../JetDatabaseWriter/AccessReader.cs)
- [../../JetDatabaseWriter.Tests/Relationships/LinkedTextTableTests.cs](../../JetDatabaseWriter.Tests/Relationships/LinkedTextTableTests.cs)

`LinkedTableManager` now owns linked-text source opening, header discovery,
headerless first-row buffering, normalized row reads, metadata shaping, and
materialized-row-limit enforcement through a shared row reader. Count-only reads
still use the non-materializing `DelimitedTextReader.CountRecordsAsync` path,
while string/object streaming, typed streaming, typed materialization, metadata,
and `DataTable` materialization reuse the same normalized row source instead of
reopening the file for separate metadata and data passes. `AccessReader` keeps
the public linked-table orchestration surface and delegates linked-text mapped
rows to the shared manager path.

Evidence at closeout: the focused
`JetDatabaseWriter.Tests.Relationships.LinkedTextTableTests` class passed with
36 succeeded and 1 environment skip for directory symlink availability; the full
`JetDatabaseWriter.Tests.Relationships` namespace passed with 163 succeeded and
1 environment skip; and `dotnet build JetDatabaseWriter.slnx --no-restore
--configuration Debug` passed across all projects and target frameworks.

Preserve these guardrails: keep count-only linked-text reads non-materializing,
keep no-header files treating the first record as both generated-column evidence
and the first data row, keep ragged rows normalized to discovered column width,
keep streaming APIs outside `LinkedTextMaxMaterializedRows`, and enforce the
materialized-row limit before adding the extra row to a `DataTable` or typed
list.

### Completed LVAL row-location handoff cleanup

Status: completed 2026-05-31.

Primary files:

- [../../JetDatabaseWriter/ValueDecoding/LongValueDecoder.cs](../../JetDatabaseWriter/ValueDecoding/LongValueDecoder.cs)
- [../../JetDatabaseWriter/LongValues/LongValueStore.cs](../../JetDatabaseWriter/LongValues/LongValueStore.cs)
- [../../JetDatabaseWriter.Tests/ValueEncoding/LongValueStoreTests.cs](../../JetDatabaseWriter.Tests/ValueEncoding/LongValueStoreTests.cs)

`LongValueDecoder` now resolves the LVAL page and row index once, obtains the
reader-owned cached row bounds when the target page is available, and passes the
page/row indexes plus those cached bounds directly to `LongValueStore`.
`LongValueStore.LocateRow` keeps the data-page validation, row-count check,
live-row matching, and row-bound error reporting without recomputing row-pointer
pieces from the packed `lvalDp` value.

Evidence at closeout: focused `LongValueStoreTests` passed with 7 succeeded; the
LVAL-adjacent `LongValueStoreTests`, `CompressedMemoLvalTests`,
`OverflowMemoReadTests`, and `DirectRowDecoderBuilderTests` slice passed with 21
succeeded; and `dotnet build JetDatabaseWriter.slnx --no-restore
--configuration Debug` passed across all projects and target frameworks.

Preserve these guardrails: keep `AccessReader.GetLiveRowBoundsCached` as the
row-directory source for reader-side LVAL page lookup, keep `LongValueStore`
responsible for row-location validation against caller-provided live-row bounds,
and keep chained LVAL reads resolving each chunk through the decoder-provided
page/cache lookup.

### Completed binary slice and data-URI helpers

Status: completed 2026-05-31.

Primary files:

- [../../JetDatabaseWriter/Infrastructure/BinaryBuffer.cs](../../JetDatabaseWriter/Infrastructure/BinaryBuffer.cs)
- [../../JetDatabaseWriter/Infrastructure/BinaryStringParser.cs](../../JetDatabaseWriter/Infrastructure/BinaryStringParser.cs)
- [../../JetDatabaseWriter/ValueDecoding/DirectRowDecoderBuilder.cs](../../JetDatabaseWriter/ValueDecoding/DirectRowDecoderBuilder.cs)
- [../../JetDatabaseWriter/ValueDecoding/RowDecodePlan.cs](../../JetDatabaseWriter/ValueDecoding/RowDecodePlan.cs)
- [../../JetDatabaseWriter/ValueDecoding/LongValueDecoder.cs](../../JetDatabaseWriter/ValueDecoding/LongValueDecoder.cs)
- [../../JetDatabaseWriter/ComplexColumns/ComplexColumnReader.cs](../../JetDatabaseWriter/ComplexColumns/ComplexColumnReader.cs)
- [../../JetDatabaseWriter.Tests/Infrastructure/BinaryStringParserTests.cs](../../JetDatabaseWriter.Tests/Infrastructure/BinaryStringParserTests.cs)
- [../../JetDatabaseWriter.Tests/Infrastructure/BinaryBufferTests.cs](../../JetDatabaseWriter.Tests/Infrastructure/BinaryBufferTests.cs)

`BinaryBuffer` now owns the tiny byte-slice copy convention used by direct row
decode, planned row decode, raw long-value extraction, OLE payload extraction,
and complex-column attachment fallback paths. `BinaryStringParser` now owns
base64 data-URI payload discovery, optional MIME-type filtering, and decode
dispatch, so typed value parsing, complex-column OLE/attachment normalization,
catalog `LvProp` parsing, and related tests no longer carry local comma/prefix/
base64 parsing.

Evidence at closeout: focused helper/parser classes passed with 44 succeeded;
the complex-column OLE object class passed with 6 succeeded; the writer OLE LVAL
round-trip method passed with 1 succeeded; direct-row-decoder and LVAL-focused
classes passed with 13 succeeded; and `dotnet build JetDatabaseWriter.slnx
--no-restore --configuration Debug` passed across all projects and target
frameworks.

Preserve these guardrails: keep `BinaryBuffer.CopySlice` exact about requested
byte ranges while normalizing non-positive lengths to an empty array; keep
base64 data-URI detection ordinal and explicitly gated on `;base64`; and keep
catalog `LvProp` decoding restricted to `application/octet-stream` data URIs.

### Completed Numeric fixed-point payload helper

Status: completed 2026-05-31.

Primary files:

- [../../JetDatabaseWriter/ValueEncoding/NumericEncoder.cs](../../JetDatabaseWriter/ValueEncoding/NumericEncoder.cs)
- [../../JetDatabaseWriter/ValueEncoding/RowEncoder.cs](../../JetDatabaseWriter/ValueEncoding/RowEncoder.cs)
- [../../JetDatabaseWriter/Indexes/IndexKeyEncoder.cs](../../JetDatabaseWriter/Indexes/IndexKeyEncoder.cs)
- [../../JetDatabaseWriter.Tests/ValueEncoding/NumericEncoderTests.cs](../../JetDatabaseWriter.Tests/ValueEncoding/NumericEncoderTests.cs)
- [../../JetDatabaseWriter.Tests/Writer/NumericRowEncodingTests.cs](../../JetDatabaseWriter.Tests/Writer/NumericRowEncodingTests.cs)

`NumericEncoder.TryEncodeFixedPointPayload` now owns the shared NUMERIC
fixed-point middle algorithm: decimal sign detection, natural-scale capture,
target-scale rescaling, digit counting, 16-byte mantissa fit detection, and
16-byte big-endian unsigned magnitude shaping. `RowEncoder.EncodeNumericValue`
keeps row-storage ownership by rounding to declared scale, enforcing declared
precision with `JetLimitationException`, applying the row sign byte, and running
JET row-storage byte-order correction. `IndexKeyEncoder.EncodeNumericEntry`
uses the same payload helper before applying Access/Jackcess legacy MDB versus
new-style ACCDB index-key twiddling rules, preserving its `ArgumentException`
target-scale validation and `NotSupportedException` mantissa-overflow contract.

Evidence at closeout: focused helper/index/seek/FK/writer numeric classes passed
with 127 succeeded; the existing
`AccessWriterTests.InsertRow_NumericPrecisionAndScaleBoundaries_RoundTripsLosslessly`
theory passed with 13 succeeded; and `dotnet build JetDatabaseWriter.slnx
--no-restore --configuration Debug` passed across all projects and target
frameworks.

Preserve these guardrails: keep fixed-point rescaling and 16-byte unsigned
magnitude shaping centralized in `NumericEncoder`; keep row precision and
mantissa exceptions as `JetLimitationException`; keep raw index target-scale
errors as `ArgumentException` and 16-byte index mantissa overflow as
`NotSupportedException`; keep declared-scale index wrappers rounding
half-to-even before encoding; and keep row storage byte-order correction and
index-key twiddling at their respective call sites.

### Completed logical TDEF chain helper

Status: completed 2026-05-31.

Primary files:

- [../../JetDatabaseWriter/AccessBase.cs](../../JetDatabaseWriter/AccessBase.cs)
- [../../JetDatabaseWriter/Relationships/RelationshipManager.cs](../../JetDatabaseWriter/Relationships/RelationshipManager.cs)
- [../../JetDatabaseWriter/Schema/LogicalTDefChain.cs](../../JetDatabaseWriter/Schema/LogicalTDefChain.cs)
- [../../JetDatabaseWriter/Schema/TDefPageBuilder.cs](../../JetDatabaseWriter/Schema/TDefPageBuilder.cs)
- [../../JetDatabaseWriter.Tests/Schema/LogicalTDefChainTests.cs](../../JetDatabaseWriter.Tests/Schema/LogicalTDefChainTests.cs)

`LogicalTDefChain` now owns logical TDEF chain reads, optional physical page
retention, page-count/capacity math, logical-offset mapping, physical page
materialization, and mutable write-back with continuation-page allocation and
deallocation. `AccessBase.ReadTDefBytesAsync` uses the read-only path,
`TDefPageBuilder` uses the shared logical-to-physical and materialization
helpers, and `RelationshipManager` keeps only FK-specific layout mutation plus
thin read/write wrappers over the shared chain.

The refactor preserves the existing logical layout: the first physical page
contributes the full page, each continuation contributes bytes after offset 8,
write-back stamps next-page pointers from the retained or newly allocated page
numbers, and shrinking a logical TDEF deallocates continuation pages that are no
longer needed.

Evidence at closeout: focused `LogicalTDefChainTests` passed with 2 succeeded;
`JetDatabaseWriter.Tests.Relationships` passed with 163 succeeded and 1
environment skip; `JetDatabaseWriter.Tests.Schema` passed with 223 succeeded;
`JetDatabaseWriter.Tests.Indexes` passed with 447 succeeded;
`JetDatabaseWriter.Tests.Catalog` passed with 111 succeeded; targeted DAO
CompactDatabase validation for relationship create/rename/drop passed with 2
succeeded; and `dotnet build JetDatabaseWriter.slnx --no-restore
--configuration Debug` passed.

Preserve these guardrails: keep first-page versus continuation logical layout
byte-for-byte compatible, keep next-page pointer stamping tied to physical page
numbers, keep first-page free-space and `tdef_len` updates centralized, keep
shrink deallocation for unused continuation pages, and route future schema
mutations that need multi-page TDEF write-back through `LogicalTDefChain` rather
than rebuilding page-chain math locally.

### Completed insert batch mutation flow

Status: completed 2026-05-31.

Primary files:

- [../../JetDatabaseWriter/AccessWriter.cs](../../JetDatabaseWriter/AccessWriter.cs)
- [../../JetDatabaseWriter/Schema/ConstraintRegistry.cs](../../JetDatabaseWriter/Schema/ConstraintRegistry.cs)
- [../../JetDatabaseWriter.Tests/Indexes/IndexPreWriteUniqueEnforcementTests.cs](../../JetDatabaseWriter.Tests/Indexes/IndexPreWriteUniqueEnforcementTests.cs)

Object-array single inserts, object-array bulk inserts, typed single inserts,
and typed bulk inserts now share one mapped-row preparation path and one
prepared-batch mutation path. `AccessWriter` keeps overload validation at the
public boundary, maps caller input to `object[]` rows through
`InsertMappedRowsAfterValidationAsync`, applies constraints and AutoNumber
assignment in `PrepareInsertBatchAsync`, then runs the single mutation protocol
in `InsertPreparedBatchAsync`: pre-write unique-index checks, FK checks,
data-row writes, FK parent-set augmentation, incremental index maintenance with
rebuild fallback, AutoNumber TDEF high-water updates, inserted-row rollback,
TDEF row-count adjustment, and AutoNumber checkpoint restoration.

The refactor also fixed batch AutoNumber restoration. `ConstraintRegistry` now
restores checkpoints in reverse order so a rejected multi-row batch that
advanced the same AutoNumber counter several times returns to the earliest
checkpoint. A focused typed-batch duplicate-key test covers that path.

Evidence at closeout: the focused
`JetDatabaseWriter.Tests.Indexes.IndexPreWriteUniqueEnforcementTests` class
passed with 10 succeeded, the insert-adjacent writer/constraint/AutoNumber/FK/
transaction/index-maintenance slice passed with 375 succeeded, and the full
non-fuzz suite passed with 3,562 succeeded and 2 environment skips. A focused
BenchmarkDotNet ShortRun for indexed `AccessWriterBenchmarks.InsertRows_Batch`
reported 338.7 ms / 45.69 MB allocated for 10 rows and 319.0 ms / 46.33 MB
allocated for 100 rows.

Preserve these guardrails: keep AutoNumber assignment before unique-index
checks, keep FK parent-set augmentation after each successful row write, keep
single and bulk overloads on the shared prepared-batch mutation path, restore
AutoNumber checkpoints in reverse order, and avoid adding extra row copies in
the hot insert paths.


### Completed facade / adapter cleanup

Status: completed 2026-05-31.

The lower-risk follow-ups from the 2026-05-31 facade scan are closed. Future
work touching the same ownership boundaries should preserve the outcomes below
rather than treating this as an active cleanup queue.

- [x] Delete the stale index leaf facade. `IndexLeafIncremental` is gone; page
  reads now route to `IndexPageCodec`, null-on-overflow leaf rebuilds live on
  `IndexPageCodec.TryBuildLeafPage`, and stable entry-list edits live in
  `IndexEntrySplicer.Splice`.
- [x] Collapse `AccessWriter` TDEF-builder compatibility forwarders where call
  sites can depend on `TDefPageBuilder` directly. Completed 2026-05-31: the
  static `AccessWriter.BuildTableDefinition` helper and the writer-owned
  `BuildTDefPageWithIndexOffsets` / `BuildTDefPagesWithIndexOffsets` wrappers
  were removed; production and test callers now use `TDefPageBuilder` directly
  for pure TDEF construction while writer-owned page-number and usage-map
  patching remains in `AccessWriter`.
- [x] Reassess `IndexLeafPageBuilder` after the codec split. Completed
  2026-05-31: the builder had become a facade, so its remaining ownership moved
  to clearer homes. `IndexPageLayout` now owns Jet3 / Jet4 page-layout selection,
  `IndexPageCodec` owns leaf page build / try-build semantics next to the
  intermediate page codec, and `IndexLeafPageBuilder` was deleted.
- [x] Move relationship fallback string-key ownership out of `IndexHelpers` or
  make the dependency explicit. Completed 2026-05-31:
  `RelationshipKeyBuilder` now owns canonical snapshot/fallback key
  normalization directly, and `IndexHelpers` keeps only encoded seek-key work.
- [x] Normalize the `AccessBase` row decode-plan adapters. Completed
  2026-05-31: `TryParseRowLayout` and `ResolveColumnSlice` are now the
  shared internal row-layout API for `RowDecodePlan` and writer-side row
  helpers, so the decode-plan-only `TryParseRowLayoutForDecodePlan` and
  `ResolveColumnSliceForDecodePlan` forwarders were deleted.
- [x] Inline pure long-value one-liners. Completed 2026-05-31:
  `RowEncoder` now calls `LongValueStore.WrapInlineLongValue` directly, and
  `CatalogWriter` receives the writer-owned `LongValueEncoder` collaborator for
  linked-table memo LVAL encoding. The obsolete
  `LongValueEncoder.WrapInlineLongValue` and
  `AccessWriter.ForceEncodeMemoAsLvalAsync` bridges were removed.
- [x] Leave intentional public facades in place. Completed 2026-05-31: the
  remaining `AccessWriter` relationship, complex-column row API, transaction,
  and encryption/password forwarders were checked against the public interfaces,
  README examples, and focused test usage. They are externally documented writer
  orchestration entry points, so the implementation should stay delegated to
  `RelationshipManager`, `ComplexColumnManager`, transaction lifecycle, and
  encryption services rather than exposing those collaborators or deleting the
  public API surface.

### 1. Shared index cursor and B-tree editor

Status: completed 2026-05-30.

Primary files:

- [../../JetDatabaseWriter/Indexes/IndexMaintainer.cs](../../JetDatabaseWriter/Indexes/IndexMaintainer.cs)
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

### 2. Stale index leaf facade deletion

Status: completed 2026-05-31.

Primary files:

- [../../JetDatabaseWriter/Indexes/IndexPageCodec.cs](../../JetDatabaseWriter/Indexes/IndexPageCodec.cs)
- [../../JetDatabaseWriter/Indexes/IndexPageLayout.cs](../../JetDatabaseWriter/Indexes/IndexPageLayout.cs)
- [../../JetDatabaseWriter/Indexes/IndexEntrySplicer.cs](../../JetDatabaseWriter/Indexes/IndexEntrySplicer.cs)
- [../../JetDatabaseWriter/Indexes/IndexBTreeEditor.cs](../../JetDatabaseWriter/Indexes/IndexBTreeEditor.cs)
- [../../JetDatabaseWriter/Indexes/IndexMaintainer.cs](../../JetDatabaseWriter/Indexes/IndexMaintainer.cs)
- [../../JetDatabaseWriter.Tests/Indexes/IndexPageCodecAndEntrySplicerTests.cs](../../JetDatabaseWriter.Tests/Indexes/IndexPageCodecAndEntrySplicerTests.cs)
- [../../JetDatabaseWriter.Tests/Indexes/IndexPageCodecLeafPageTests.cs](../../JetDatabaseWriter.Tests/Indexes/IndexPageCodecLeafPageTests.cs)

`IndexLeafIncremental` was deleted. Its page-header reads, leaf and
intermediate entry decoding, child-pointer reads, single-root-leaf detection,
and intermediate-page checks now call `IndexPageCodec` directly.
Null-on-overflow leaf rebuild wrappers live on `IndexPageCodec` as
`TryBuildLeafPage` overloads, beside the intermediate-page try-build API. The
per-format Jet3 / Jet4 index page offsets live in `IndexPageLayout`.

The one non-pass-through algorithm, stable entry-list splicing for incremental
adds/removes, moved into `IndexEntrySplicer.Splice`. Production callers in
`IndexBTreeEditor`, `IndexMaintainer`, and FormatProbe now use the codec,
layout, and splicer owners directly. The facade test suite was renamed and
retargeted to codec/splicer/leaf-page behavior instead of keeping tests for a
deleted concept.

Closeout shape: the historical facade file and the follow-up leaf builder were
removed, one focused splicer helper was added, `IndexPageLayout` was extracted,
and `IndexPageCodec` gained small null-on-overflow leaf overloads for existing
surgical rewrite call sites.

Evidence at closeout: `dotnet build JetDatabaseWriter.slnx --no-restore
--configuration Debug` passed, the retargeted codec/splicer test file passed
with 26 succeeded, the full `JetDatabaseWriter.Tests.Indexes` namespace passed
with 446 succeeded, and the non-fuzz suite passed with 3,561 succeeded and 2
environment skips.

Follow-up evidence after deleting `IndexLeafPageBuilder`: the full
`JetDatabaseWriter.Tests.Indexes` namespace passed with 446 succeeded,
`JetDatabaseWriter.Tests.Relationships` passed with 163 succeeded and 1
environment skip, and `dotnet build JetDatabaseWriter.slnx --no-restore
--configuration Debug` passed.

Preserve these guardrails: keep Jet3 and Jet4/ACE layout selection explicit in
`IndexPageLayout`, keep the splicer duplicate-key tie ordering stable, keep leaf
rebuild overflow reported as `null` for surgical paths, and keep page-format
reads and writes in `IndexPageCodec` rather than recreating facade methods.

### 3. Row decode plan

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

### 4. Usage map and page ownership model

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

### 5. Declarative catalog artifact planning

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

### 6. Shared table row walker and owned-page locator

Status: completed 2026-05-30.

Primary files:

- [../../JetDatabaseWriter/AccessBase.cs](../../JetDatabaseWriter/AccessBase.cs)
- [../../JetDatabaseWriter/AccessReader.cs](../../JetDatabaseWriter/AccessReader.cs)
- [../../JetDatabaseWriter/AccessWriter.cs](../../JetDatabaseWriter/AccessWriter.cs)
- [../../JetDatabaseWriter/Catalog/CatalogWriter.cs](../../JetDatabaseWriter/Catalog/CatalogWriter.cs)
- [../../JetDatabaseWriter/Relationships/RelationshipCatalogStore.cs](../../JetDatabaseWriter/Relationships/RelationshipCatalogStore.cs)
- [../../JetDatabaseWriter/ComplexColumns/ComplexColumnManager.cs](../../JetDatabaseWriter/ComplexColumns/ComplexColumnManager.cs)
- [../../JetDatabaseWriter/Pages/DataPageInserter.cs](../../JetDatabaseWriter/Pages/DataPageInserter.cs)

The reader-owned table-page locator moved into `AccessBase` as a shared
internal primitive. It now exposes `GetOwnedDataPagesAsync`,
`ForEachOwnedDataPageAsync`, and `ForEachLiveTableRowAsync`, so reader and
writer code use the same usage-map-aware page discovery and whole-file owner
index fallback. Reader instances still cache stable owned-page results when no
journal is active; writer instances deliberately avoid cross-call owned-page
caching so table ownership mutations cannot leave cached page lists stale.

Writer `GetLiveRowLocationsAsync`, catalog row reads, non-table object-id
allocation, ACE cleanup, relationship catalog discovery, complex-column ID and
flat-table lookups, cascade cleanup, rename/drop/update helpers, table-drop
storage reclamation, system-table rewrites, and system-table insert target
selection now route through the shared locator or visitor surface. The local
`DataPageInserter` usage-map reader and the duplicate reader-only owned-page
locator were deleted.

Closeout diff: 7 files changed with 585 insertions and 888 deletions, for a net
reduction of 303 lines while keeping the public API shape unchanged.

Evidence at closeout: `dotnet build JetDatabaseWriter.slnx --no-restore
--configuration Debug` passed, focused reader/relationship/catalog/complex-column
tests passed with 66 succeeded, and the non-fuzz suite passed with 3,561
succeeded and 2 environment skips.

Preserve these guardrails: keep the whole-file fallback for corrupt,
unfamiliar, or missing owned-page maps; keep writer owned-page lists uncached
across calls unless a future mutation-invalidation story exists; do not force
typed row materialization where callers only need scalar catalog columns; and
keep page-return ownership inside the shared visitor methods.

### 7. Compound File dependency decision

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
7. Shared table row walker and owned-page locator: completed 2026-05-30.
8. Stale index leaf facade deletion: completed 2026-05-31.
9. Index leaf builder reassessment and deletion: completed 2026-05-31.
10. Insert batch mutation flow: completed 2026-05-31.

## Non-goals

- Do not remove measured read-path optimizations unless benchmarks show the
  replacement is neutral or better.
- Do not bulk-rebuild MSysObjects from decoded rows.
- Do not trade DAO CompactDatabase compatibility for smaller code.
- Do not turn simplification into a formatting or file-splitting exercise; the
  goal is fewer concepts, fewer duplicate algorithms, and fewer parallel code
  paths.
