# High-Impact Simplification TODOs

Status: active backlog with completed outcomes retained
Date: 2026-05-30

This note tracks the remaining large simplification opportunities that look
likely to delete or consolidate meaningful code while retaining features and
performance. Completed outcomes stay here as waypoints so future work does not
reopen settled architecture threads. The note intentionally excludes formatting,
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
- [x] Move sibling pointer writes and leaf/intermediate page emission behind
      the codec or a successor descriptor.
- [x] Extract an `IndexCursor` that performs layout-aware descent, leaf-chain
      walks, tail-page fall-through, and exact-key row-location collection.
- [x] Extract a mutation planner, such as `IndexBTreeEditor`, that can model
      insert/delete changes against one or more leaves and decide between
      in-place rewrite, split, ancestor propagation, or full-tree rebuild.
- [x] Remove `IndexBTreeSeeker` and route exact-key seeks through the cursor.
- [x] Route `RelationshipChildRowLocator` and parent/child FK seek logic
      through the cursor rather than direct seeker calls.
- [x] Route unique-index fast checks through the same encoded-key and cursor
      infrastructure where possible.
- [x] Collapse duplicated `NextEntryStart`, canonical-key compare, prefix
      reconstruction, and child-selection logic.

Progress 2026-05-26: read-only index seeking now flows through
`Indexes/IndexCursor.cs`, backed by `Indexes/IndexPageCodec.cs`. The legacy
`IndexBTreeSeeker` facade was deleted; public reader seeks, relationship FK
enforcement, and child-row location call the cursor directly. Mutation editing
now flows through `Indexes/IndexBTreeEditor.cs`, leaving `IndexMaintainer` as
the TDEF/catalog orchestration surface.

Progress 2026-05-30: write-side index page emission now routes through
`Indexes/IndexPageCodec.cs`: leaf and intermediate builders delegate page-byte
construction to the codec, and sibling-pointer patches use codec write helpers.
The unique-index pre-insert fast path now encodes pending keys with the same
index-key encoder and probes existing B-trees through `IndexCursor`, while
keeping the snapshot fallback for Numeric/Memo cases. Evidence: focused
pre-write/cursor tests passed (17/17), the full index namespace passed
(446/446), `dotnet build --no-restore` passed, and the non-fuzz suite passed
(3,559 succeeded, 2 environment skips).

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
and the direct POCO expression-tree decoder were still separate sinks at this
checkpoint; the 2026-05-30 completion note below supersedes that interim state.

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

Status: completed 2026-05-30

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

- [x] Define a `CatalogArtifactPlan` or similar object that describes table
      TDEF pages, catalog rows, ACE rows, LvProp blobs, indexes, owned maps,
      and post-create maintenance requirements.
- [x] Express core ACCDB system tables, `MSysComplexColumns`,
      `MSysComplexType_*`, and hidden complex flat tables as artifact plans.
- [x] Express relationship catalog objects and linked-table catalog rows as
      artifact plans.
- [x] Use one executor for table/system/complex-table plan steps: reserve
      pages, write TDEF pages, emit usage maps, insert catalog rows, insert ACE
      rows, register constraints, and invalidate caches.
- [x] Extend the same executor surface to remaining catalog-only artifacts and
      shared replacement/deletion primitives.
- [x] Keep `CreateTableInternalAsync` as a thin public/internal facade over the
      plan executor.
- [x] Make schema rewrite transplant/copy-swap paths use the same catalog-row
      replacement/deletion primitives.

Progress 2026-05-30: first planner slice landed in
`Catalog/Models/CatalogArtifactPlan.cs` and related artifact models.
`CreateTableInternalAsync` is now a thin facade over a shared artifact executor
that reserves/writes TDEF pages, emits index leaves and usage-map rows, inserts
catalog rows, applies declarative ACE-row policy, registers constraints, and
invalidates caches. Fresh ACCDB core system tables plus their fixed container
catalog rows are expressed as one plan, complex type-template tables use the
same executor with usage maps disabled to preserve their prior TDEF shape, and
hidden complex flat tables declare their forced ACE-row emission through the
table artifact instead of a separate follow-up call. As of this snapshot,
relationship catalog objects and linked-table catalog rows also flow through
catalog-object artifacts with declarative non-table object-id, ACE, linked-field,
and rollback policy. `CatalogArtifactPlan` now carries catalog-row replacement
and deletion artifacts, and the schema-rewrite transplant path uses them through
the plan executor. Copy/swap table rewrites, table drops, and catalog renames
share the same lower-level replacement/deletion primitives. No parallel
catalog-row mutation path remains for this item.

Evidence 2026-05-30: the Debug library build passed, focused catalog,
linked-table, relationship, complex schema-evolution, general schema-evolution,
and relationship-mutation tests passed (71/71), and the full non-fuzz suite
passed (3,561 succeeded, 2 environment skips).

Guardrails:

- [x] Preserve bootstrap ordering for fresh full-catalog ACCDB files.
- [x] Preserve MSysObjects incremental-only requirements for catalog indexes.
- [x] Preserve complex-column parent identity rules described in
      [complex-columns-format-notes.md](complex-columns-format-notes.md).
- [x] Run DAO compact/open-recordset validation for complex columns,
      relationships, linked tables, and fresh database creation.

Likely payoff: medium to high deletion upside, especially in writer
orchestration. Risk is mostly compatibility ordering rather than hot-path
performance.

## 5. Reassess the Hand-Rolled Compound File Layer

Status: completed 2026-05-30 (closed by dependency decision)

Primary files:

- [../../JetDatabaseWriter/CompoundFile/CompoundFileReader.cs](../../JetDatabaseWriter/CompoundFile/CompoundFileReader.cs)
- [../../JetDatabaseWriter/CompoundFile/CompoundFileWriter.cs](../../JetDatabaseWriter/CompoundFile/CompoundFileWriter.cs)
- [../../JetDatabaseWriter/Encryption/EncryptionConverter.cs](../../JetDatabaseWriter/Encryption/EncryptionConverter.cs)
- [../../JetDatabaseWriter/Encryption/EncryptionManager.cs](../../JetDatabaseWriter/Encryption/EncryptionManager.cs)

Why this was considered: the CFB reader/writer is a self-contained file-format
implementation used for Office Crypto wrappers. In theory, an acceptable,
well-tested dependency could have deleted the subsystem.

Target shape:

- [x] Reassess the dependency route against supply-chain, attack-surface,
      package-size, and performance policy; decision: do not add a CFB runtime
      dependency for this narrow use case.
- [x] Verify the local CFB surface is already constrained to top-level stream
      extraction and Office Crypto wrapper output behind internal APIs.
- [x] Verify Office Crypto compatibility for `EncryptionInfo` and
      `EncryptedPackage` streams, including regular-FAT output requirements.
- [x] Keep the local implementation rather than replacing it with a dependency.
- [x] Leave the implementation mostly alone: the reader/writer are internal,
      small, fixture-backed, and already shaped around the Office Crypto use
      case rather than a general-purpose public CFB API.

Decision 2026-05-30: no CFB package will be introduced. The current package
has no CFB runtime dependency; adding one would increase supply-chain review,
attack surface, target-framework/package-size constraints, and hot-path risk
for code that is currently limited to `EncryptionInfo` / `EncryptedPackage`
stream extraction and emission. OpenMcdf remains a test-fixture source only,
not a runtime dependency.

Evidence 2026-05-30: `CompoundFileReader` handles v3/v4 sector sizes, FAT,
DIFAT extensions, mini-FAT streams, exact logical stream sizing, contiguous-run
read coalescing, and crafted-file bounds/cycle checks. `CompoundFileWriter`
emits the Office Crypto CFB shape with v4 sectors and regular-FAT streams, and
rejects sub-cutoff Office Crypto streams that would otherwise require mini-FAT
output. CFB tests cover OpenMcdf fixture boundaries, real Office/LibreOffice
compound samples, corrupt headers, FAT/DIFAT loops, oversized FAT/DIFAT counts,
v3/v4 writer output, and Office Crypto v4 shape. Encryption tests cover Agile,
Agile CFB, Standard, legacy AES CFB-wrapped, and encryption mutation round
trips over the same CFB layer.

Guardrails:

- [x] Preserve Agile, Agile CFB, Standard, and legacy AES CFB-wrapped behavior.
- [x] Preserve current defensive bounds against crafted CFB inputs.
- [x] Keep dependency policy, target frameworks, package size, and license
      compatibility explicit in the decision record.

Outcome: closed. The deletion upside is outweighed by dependency risk, and the
local layer is already narrow enough that further simplification is unlikely to
pay for itself without weakening coverage or compatibility.

## Suggested Order

1. IndexPageCodec plus read-only IndexCursor: completed 2026-05-26.
2. IndexBTreeEditor mutation planner: completed 2026-05-26.
3. RowDecodePlan, gated by BenchmarkDotNet evidence: completed 2026-05-30.
4. Declarative catalog artifact planning: completed 2026-05-30.
5. CFB dependency decision: completed 2026-05-30; no dependency introduced.

## Non-goals

- Do not remove measured read-path optimizations unless benchmarks show the
  replacement is neutral or better.
- Do not bulk-rebuild MSysObjects from decoded rows.
- Do not trade DAO CompactDatabase compatibility for smaller code.
- Do not turn this into a formatting or file-splitting exercise; the goal is
  fewer concepts, fewer duplicate algorithms, and fewer parallel code paths.
