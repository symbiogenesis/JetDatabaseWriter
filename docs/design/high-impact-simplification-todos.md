# High-Impact Simplification TODOs

Status: candidate backlog
Date: 2026-05-26

This note captures only the remaining large simplification opportunities that
look likely to delete or consolidate meaningful code while retaining features
and performance. It intentionally excludes completed refactors, formatting,
comments, small helper extractions, and local tidy-ups.

Each item should be treated as architecture work: start with characterization
tests and benchmarks, then refactor behind existing public APIs.

## 1. Build a Shared Index Cursor and B-tree Editor

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

Why this is exciting: index logic is the largest remaining cluster of parallel
implementations. Full rebuild, single-leaf splice, append-tail maintenance,
same-leaf surgery, cross-leaf surgery, catalog splicing, relationship seeks,
and uniqueness checks all understand overlapping pieces of the same B-tree
format. A real cursor/editor abstraction could remove substantial code rather
than merely move it.

Target shape:

- [x] Extract an `IndexPageCodec` that owns read-side bitmask scanning, prefix
      handling, entry decoding, sibling pointer reads, child selection, and
      encoded-key comparison.
- [ ] Move sibling pointer writes and leaf/intermediate page emission behind
      the codec or a successor descriptor.
- [x] Extract an `IndexCursor` that performs layout-aware descent, leaf-chain
      walks, tail-page fall-through, and exact-key row-location collection.
- [x] Extract a mutation planner, such as `IndexBTreeEditor`, that can model
      insert/delete changes against one or more leaves and decide between
      in-place rewrite, split, ancestor propagation, or full-tree rebuild.
- [x] Remove `IndexBTreeSeeker` and route exact-key seeks through the cursor.
- [x] Route `RelationshipChildRowLocator` and parent/child FK seek logic
      through the cursor rather than direct seeker calls.
- [ ] Route unique-index fast checks through the same encoded-key and cursor
      infrastructure where possible.
- [x] Collapse duplicated `NextEntryStart`, canonical-key compare, prefix
      reconstruction, and child-selection logic.

Progress 2026-05-26: read-only index seeking now flows through
`Indexes/IndexCursor.cs`, backed by `Indexes/IndexPageCodec.cs`. The legacy
`IndexBTreeSeeker` facade was deleted; public reader seeks, relationship FK
enforcement, and child-row location call the cursor directly. Mutation editing
now flows through `Indexes/IndexBTreeEditor.cs`, leaving `IndexMaintainer` as
the TDEF/catalog orchestration surface.

Guardrails:

- [ ] Preserve Jet3 and Jet4/ACE layout selection via
      `IndexLeafPageBuilder.LeafPageLayout` or a successor descriptor.
- [ ] Preserve MSysObjects special handling; do not reintroduce unsafe
      full-row re-encoding for catalog rows.
- [ ] Validate with index, relationship, catalog, DAO CompactDatabase, and
      round-trip tests documented in
      [index-and-relationship-format-notes.md](index-and-relationship-format-notes.md),
      [catalog-index-maintenance-notes.md](catalog-index-maintenance-notes.md), and
      [writer-disk-format-validation-matrix.md](writer-disk-format-validation-matrix.md).
- [ ] Add focused tests for multi-level seek, duplicate-key leaf spans,
      tail-page append fall-through, same-leaf mutation, cross-leaf mutation,
      leaf underflow, and root split.

Likely payoff: very high deletion and maintainability upside, with high
compatibility risk. This is the top candidate.

## 2. Replace Parallel Row Readers with a Row Decode Plan

Status: completed 2026-05-30

Primary files:

- [../../JetDatabaseWriter/AccessReader.cs](../../JetDatabaseWriter/AccessReader.cs)
- [../../JetDatabaseWriter/AccessBase.cs](../../JetDatabaseWriter/AccessBase.cs)
- [../../JetDatabaseWriter/ValueDecoding/DirectRowDecoderBuilder.cs](../../JetDatabaseWriter/ValueDecoding/DirectRowDecoderBuilder.cs)
- [../../JetDatabaseWriter/ValueDecoding/RowMapper.cs](../../JetDatabaseWriter/ValueDecoding/RowMapper.cs)
- [../../JetDatabaseWriter/ValueDecoding/TypedRowFallbackPolicy.cs](../../JetDatabaseWriter/ValueDecoding/TypedRowFallbackPolicy.cs)
- [../../JetDatabaseWriter/AccessWriter.cs](../../JetDatabaseWriter/AccessWriter.cs)

Why this is exciting: the repo currently has string row cracking, typed
object-array cracking, pooled-buffer cracking, direct POCO decoding, and
writer-side partial column reads. All depend on the same row layout parser and
column slicing rules, but each has its own switch/fallback surface.

Target shape:

- [x] Introduce a `RowDecodePlan` built from `TableDef`, projected columns,
      strictness, and caller requirements.
- [x] Let the plan parse row layout once and feed specialized sinks:
      string row sink, object-buffer sink, pooled object-buffer sink, direct
      POCO sink, and partial key-column sink.
- [x] Move variable-slice typed decode decisions out of `AccessReader` and
      writer helper code into shared plan components.
- [x] Preserve the current async LVAL sentinel model so rows without external
      long values still complete synchronously.
- [x] Make the writer's key-column reader use the same plan instead of its
      private `TryDecodeColumnSlice` path.
- [x] Keep `RowsAsStrings()` compatibility semantics intact unless an explicit
      compatibility review says otherwise.

Progress 2026-05-27: first slice landed in
`ValueDecoding/RowDecodePlan.cs`. The plan now owns row-layout preflight,
projection masks, typed fixed/variable decode decisions, calculated-column
payload decode, async LVAL sentinels, pooled object-buffer fills, and the
writer's partial key-column reader. `AccessReader` still owns async LVAL
resolution and post-processing for complex columns/hyperlinks. `RowsAsStrings()`
and the direct POCO expression-tree decoder remain separate sinks for future
slices.

Completion 2026-05-30: `RowDecodePlan` now also owns `RowsAsStrings()` row
materialization, feeds `DirectRowDecoderBuilder` with plan-owned row-layout
preflight and column-slice resolution, and backs the materialized
`ReadTable<T>` full/projection paths. `AccessReader` remains responsible for
async LVAL sentinel resolution plus complex-column and Hyperlink
post-processing at scan boundaries.

Evidence 2026-05-27: full test suite passed (`dotnet test --project
JetDatabaseWriter.Tests`: 3,537 passed, 3 expected skips). BenchmarkDotNet
ShortRun was run against a clean `HEAD` worktree and then against the current
branch using the same command:
`dotnet run --project JetDatabaseWriter.Benchmarks -c Release -- --filter
*AccessReaderRowDecodeBenchmarks* --job short`. The latest current run showed
unchanged allocation profiles and no hot-path regression versus the same-machine
baseline: numeric untyped 10.05 ms vs 10.20 ms, numeric typed 11.28 ms vs
11.31 ms, text untyped 25.45 ms vs 25.68 ms, text typed 25.98 ms vs 27.50 ms,
text as strings 26.75 ms vs 26.73 ms, and wide narrow projection 11.01 ms vs
23.10 ms. MEMO/OLE submode results were faster in that ShortRun but noisy, so
treat them as non-regression evidence rather than a claimed optimization.

Evidence 2026-05-30: `dotnet build --no-restore` passed. The non-fuzz test
task passed (`dotnet test --project JetDatabaseWriter.Tests --filter-not-trait
Category=Fuzz --stop-on-fail on`: 3,558 passed, 2 environment skips).
BenchmarkDotNet ShortRun was rerun with
`dotnet run --project JetDatabaseWriter.Benchmarks -c Release --no-restore --
--filter *AccessReaderRowDecodeBenchmarks* --job short`; affected hot-path
means stayed neutral or better in the short run: numeric untyped 7.56 ms,
numeric typed/direct 9.33 ms, numeric as strings 12.01 ms, text untyped
19.63 ms, text typed/direct 21.08 ms, text as strings 20.52 ms, and wide narrow
projection 11.39 ms. MEMO/OLE long-value submodes remain noisy in ShortRun and
are not claimed as optimized.

Guardrails:

- [x] Treat [read-performance-bottlenecks.md](read-performance-bottlenecks.md)
      as the current performance baseline.
- [x] Benchmark numeric, text-heavy, wide projection, memo, DataTable, and
      direct POCO paths before and after.
- [x] Preserve malformed-row fallback behavior, calculated-column handling,
      hyperlink wrapping, complex-column post-processing, and `RowsAsStrings()`
      empty-string semantics.

Outcome: row-layout parsing and column-slice resolution for string rows,
typed rows, pooled buffers, direct POCO decoding, materialized POCO reads, and
writer partial key reads now flow through `RowDecodePlan`. The remaining reader
logic handles asynchronous long-value resolution and scan-level post-processing
where those concerns belong.

## 3. Centralize Usage Map and Page Ownership Logic

Status: completed 2026-05-26

Primary files:

- [../../JetDatabaseWriter/AccessReader.cs](../../JetDatabaseWriter/AccessReader.cs)
- [../../JetDatabaseWriter/AccessWriter.cs](../../JetDatabaseWriter/AccessWriter.cs)
- [../../JetDatabaseWriter/Pages/DataPageInserter.cs](../../JetDatabaseWriter/Pages/DataPageInserter.cs)
- [../../JetDatabaseWriter/Pages/PageAllocator.cs](../../JetDatabaseWriter/Pages/PageAllocator.cs)
- [../../JetDatabaseWriter/Indexes/IndexMaintainer.cs](../../JetDatabaseWriter/Indexes/IndexMaintainer.cs)
- [../../JetDatabaseWriter/Constants.cs](../../JetDatabaseWriter/Constants.cs)

Why this is exciting: INLINE and REFERENCE usage-map bit math appears in reader
owned-page discovery, data-page insertion, global free-page allocation,
index-page usage-map refresh, table drop cleanup, and page reclamation. A
single model could remove duplicate parsing and make future REFERENCE-map
support less scattered.

Target shape:

- [x] Introduce a `UsageMap` reader/writer abstraction for INLINE and
      REFERENCE forms.
- [x] Provide common operations: enumerate pages, contains page, mark page,
      clear page, serialize row, and read/write map pointers.
- [x] Route `AccessReader` owned-page discovery through the abstraction.
- [x] Route `DataPageInserter.MarkPageInOwnedMapAsync` through the abstraction.
- [x] Route `PageAllocator` global free-map enumeration and mutation through
      the abstraction.
- [x] Route `AccessWriter` index usage-map row emission and drop cleanup
      through the abstraction.
- [x] Route `IndexMaintainer` usage-map refresh through the abstraction.

Guardrails:

- [x] Preserve recognized per-table usage-map fast path performance.
- [x] Preserve whole-file fallback for corrupt or unfamiliar owned-page maps.
- [x] Preserve Jet3 behavior, where table usage maps are not maintained in the
      same way as Jet4/ACE.
- [x] Add tests for INLINE base-page windows, out-of-window pages,
      REFERENCE maps, global free maps, table owned maps, and index-page maps.

Outcome: centralized usage-map pointer handling, row lookup, INLINE/REFERENCE
enumeration, bit checks/mutation, and inline row serialization in
`Pages/UsageMap.cs`. Reader owned-page discovery, data-page insertion,
global free-map allocation, index usage-map emission, table-drop cleanup, and
index-maintenance refresh now route through the shared codec.

## 4. Make System Catalog and Hidden Artifact Creation Declarative

Primary files:

- [../../JetDatabaseWriter/AccessWriter.cs](../../JetDatabaseWriter/AccessWriter.cs)
- [../../JetDatabaseWriter/Catalog/CatalogWriter.cs](../../JetDatabaseWriter/Catalog/CatalogWriter.cs)
- [../../JetDatabaseWriter/ComplexColumns/ComplexColumnManager.cs](../../JetDatabaseWriter/ComplexColumns/ComplexColumnManager.cs)
- [../../JetDatabaseWriter/Relationships/RelationshipManager.cs](../../JetDatabaseWriter/Relationships/RelationshipManager.cs)
- [../../JetDatabaseWriter/Schema/TDefPageBuilder.cs](../../JetDatabaseWriter/Schema/TDefPageBuilder.cs)

Why this is exciting: table creation, system-table scaffolding, complex flat
tables, catalog rows, ACE rows, relationship objects, and LvProp emission are
currently choreographed by bespoke sequences. The behavior is format-sensitive,
but a declarative artifact model could remove a lot of orchestration code while
making hidden dependencies explicit.

Target shape:

- [ ] Define a `CatalogArtifactPlan` or similar object that describes table
      TDEF pages, catalog rows, ACE rows, LvProp blobs, indexes, owned maps,
      and post-create maintenance requirements.
- [ ] Express core ACCDB system tables, `MSysComplexColumns`,
      `MSysComplexType_*`, hidden complex flat tables, relationship catalog
      objects, and linked-table catalog rows as artifact plans.
- [ ] Use one executor for plan steps: reserve pages, write TDEF pages, emit
      usage maps, insert catalog rows, insert ACE rows, maintain system-table
      indexes, and invalidate caches.
- [ ] Keep `CreateTableInternalAsync` as a thin public/internal facade over the
      plan executor.
- [ ] Make schema rewrite transplant/copy-swap paths use the same catalog-row
      replacement/deletion primitives.

Guardrails:

- [ ] Preserve bootstrap ordering for fresh full-catalog ACCDB files.
- [ ] Preserve MSysObjects incremental-only requirements for catalog indexes.
- [ ] Preserve complex-column parent identity rules described in
      [complex-columns-format-notes.md](complex-columns-format-notes.md).
- [ ] Run DAO compact/open-recordset validation for complex columns,
      relationships, linked tables, and fresh database creation.

Likely payoff: medium to high deletion upside, especially in writer
orchestration. Risk is mostly compatibility ordering rather than hot-path
performance.

## 5. Reassess the Hand-Rolled Compound File Layer

Primary files:

- [../../JetDatabaseWriter/CompoundFile/CompoundFileReader.cs](../../JetDatabaseWriter/CompoundFile/CompoundFileReader.cs)
- [../../JetDatabaseWriter/CompoundFile/CompoundFileWriter.cs](../../JetDatabaseWriter/CompoundFile/CompoundFileWriter.cs)
- [../../JetDatabaseWriter/Encryption/EncryptionConverter.cs](../../JetDatabaseWriter/Encryption/EncryptionConverter.cs)
- [../../JetDatabaseWriter/Encryption/EncryptionManager.cs](../../JetDatabaseWriter/Encryption/EncryptionManager.cs)

Why this is exciting: the CFB reader/writer is an entire self-contained file
format implementation used for Office Crypto wrappers. If an acceptable,
well-tested dependency can cover the exact needs, a whole subsystem could go
away.

Target shape:

- [ ] Survey candidate CFB libraries for .NET target support, licensing,
      maintenance, stream extraction, stream writing, v3/v4 sectors, FAT,
      DIFAT, mini-FAT, and malicious-file bounds.
- [ ] Verify Office Crypto compatibility for `EncryptionInfo` and
      `EncryptedPackage` streams, including regular-FAT output requirements.
- [ ] If a dependency is acceptable, replace the local reader/writer behind a
      small adapter and delete the local implementation.
- [ ] If no dependency is acceptable, narrow the local implementation's public
      surface to exactly the Office Crypto use case and remove any unsupported
      generality.

Guardrails:

- [ ] Preserve Agile, Agile CFB, Standard, and legacy AES CFB-wrapped behavior.
- [ ] Preserve current defensive bounds against crafted CFB inputs.
- [ ] Keep dependency policy, target frameworks, package size, and license
      compatibility explicit in the decision record.

Likely payoff: potentially high deletion upside, but dependent on external
package acceptability. If no dependency qualifies, leave this mostly alone.

## Suggested Order

1. IndexPageCodec plus read-only IndexCursor: completed 2026-05-26.
2. IndexBTreeEditor mutation planner: completed 2026-05-26.
3. RowDecodePlan, gated by BenchmarkDotNet evidence: completed 2026-05-30.
4. Declarative catalog artifact planning, after index/system-table maintenance
   surfaces are cleaner.
5. CFB dependency decision, when dependency policy is worth revisiting.

## Non-goals

- Do not remove measured read-path optimizations unless benchmarks show the
  replacement is neutral or better.
- Do not bulk-rebuild MSysObjects from decoded rows.
- Do not trade DAO CompactDatabase compatibility for smaller code.
- Do not turn this into a formatting or file-splitting exercise; the goal is
  fewer concepts, fewer duplicate algorithms, and fewer parallel code paths.
