# Read performance

Status: closed; retained as archived baseline and caller guidance
Date: 2026-05-20
Closed: 2026-05-31
Last updated: 2026-05-31

This note is closed. It records the read-performance baseline for
`AccessReader`, the caller guidance that falls out of the measurements, and the
future API ideas that would need fresh workload evidence before becoming active
work. The previous implementation phases are complete, and there are no open
action items or active implementation candidates tracked here. Future
read-performance work should start from fresh profiling, release-quality
BenchmarkDotNet results, or a concrete workload report, not from reopening
already-settled optimization threads. The evidence-gated ideas near the end are
not a backlog.

## Closeout state

Closed on 2026-05-31 after implementation, focused measurement, and the
`RowDecodePlan` consolidation closeout from the completed architecture
simplification sweep (working log closed and removed from the repository
2026-06-30; see git history for full detail). Keep this file
as the durable evidence record for the completed read-performance pass. If a
future workload feels slow, use the measurement commands and workload-shape
guidance below to produce new evidence before changing the core reader.

## Evidence sources

- Row decode results: `BenchmarkDotNet.Artifacts/results/JetDatabaseWriter.Benchmarks.Reader.AccessReaderRowDecodeBenchmarks-report-github.md`
- Open-floor results: `BenchmarkDotNet.Artifacts/results/JetDatabaseWriter.Benchmarks.AccessReaderOpenBenchmarks-report-github.md`
- DataTable strategy benchmarks: `JetDatabaseWriter.Benchmarks/DataTableMaterializationBenchmarks.cs`
- Owned-page discovery benchmarks: `JetDatabaseWriter.Benchmarks/AccessReaderOwnedPageDiscoveryBenchmarks.cs`
- Table-scan read-ahead benchmarks: `JetDatabaseWriter.Benchmarks/AccessReaderTableScanReadAheadBenchmarks.cs`
- Benchmark fixture sizes: `JetDatabaseWriter.Benchmarks/SyntheticDatabases.cs`
- Main read path: `JetDatabaseWriter/AccessReader.cs`
- Shared page, row, and text decode helpers: `JetDatabaseWriter/AccessBase.cs`
- Long-value decode path: `JetDatabaseWriter/ValueDecoding/LongValueDecoder.cs` plus shared LVAL chain traversal in `JetDatabaseWriter/LongValues/LongValueStore.cs`

## Current architecture

Treat this as the stable read-path architecture unless new profiling or
release-quality benchmark results justify reopening a specific area.

- `Rows()`, `Rows<T>()`, and `ReadDataTableAsync()` decode through the typed
  crack path. `CrackRowTypedAsync` calls `TryCrackRowSync` /
  `TryCrackRowSyncIntoBuffer`, which delegate layout preflight and typed
  fixed/variable slice decoding to `RowDecodePlan`, fill `object?[]` buffers
  directly, and return a sync-completed `ValueTask` on rows that do not require
  long-value resolution.
- `Memo` and `Ole` slots emit `RowDecodePlan.LongValueRef` during sync
  cracking. The async wrapper resolves only the rows and cells that actually
  contain long values.
- `RowsAsStrings()` keeps its compatibility semantics but now uses
  `RowDecodePlan.CreateStrings` / `TryDecodeStringRowAsync` for row-layout
  preflight and per-column string materialization instead of a separate row
  parser.
- `RowMapper<T>.Build(headers, sourceTypes?)` builds an expression-tree
  `Func<object?[], T>`. `BuildIndex` and `Map` remain for fallback paths and
  tests, type mismatches flow through `CoerceToTarget`, and the `ToRow` /
  `Accessor` API remains available for writer-side mapping.
- `DirectRowDecoderBuilder.TryBuild<T>` can emit a direct page-to-POCO delegate
  for primitive projections. The compiled delegate still asks `RowDecodePlan`
  to parse row layout and resolve column slices, so direct and fallback decode
  paths share the same row-layout rules. It falls back to the typed crack path
  for calculated columns, `Memo` / `Ole` LVAL columns, `Binary`, `Complex` /
  `Attachment`, hyperlinks, type-mismatched targets, or tables with complex
  columns. `Numeric` direct-decodes for decimal targets using `NumericScale`.
- Synthetic benchmark databases are generated under `%TEMP%\JetBench\` by
  `SyntheticDatabases.cs`: `Numeric` has 25K rows / 9 columns, `TextHeavy` has
  25K rows / 6 columns, `Wide` has 10K rows / 40 columns, and `Memos` has 5K
  rows with an integer plus MEMO payload.
- The `OpenAsync` floor is settled at roughly 1.1 ms / 41 KB. Do not spend
  optimization time on lazy catalog loading or catalog span rewrites without new
  measurements that contradict that floor.

## Latest focused measurements

Focused BenchmarkDotNet ShortRun jobs were run after the implementation slices
on Windows 11, .NET SDK 10.0.300, .NET 10.0.8, BenchmarkDotNet 0.15.8, Intel
Core Ultra 7 268V. Treat these as engineering closeout measurements rather than
release-quality full-run numbers. A later 2026-05-30 ShortRun after the
`RowsAsStrings()` and direct-decoder `RowDecodePlan` consolidation stayed
neutral or better on the affected hot paths; the detailed ShortRun closeout
numbers lived in the now-removed architecture simplification working log (see
git history) and are not reproduced here.

| Area | ShortRun result | Decision |
|---|---|---|
| LVAL/MEMO decode | `Decode_Memo_Untyped` is 99.4 ms / 31.2 MB; `Decode_Memo_Typed` is 130.3 ms / 31.1 MB; `Decode_Memo_DataTable` is 179.1 ms / 31.7 MB. | Allocation is materially lower than the historical 146-147 MB MEMO rows. No more LVAL allocation work is justified without a new profile. |
| Text decode | `Decode_Text_Untyped` is 14.3 ms / 4.6 MB, `Decode_Text_Typed` is 17.7 ms / 4.3 MB, and `Decode_Text_AsStrings` is 12.8 ms / 4.8 MB. | The `string.Create` and Latin-1 changes achieved the intended allocation reduction. No further text decode change is pending. |
| DataTable strategies | Public numeric `ReadDataTableAsync` is 21.9 ms / 10.9 MB; `Rows.Add(object?[])` and `LoadDataRow` are about 21.4 ms but allocate 13.2 MB. Text alternatives are close: public 15.7 ms, `Rows.Add(object?[])` 13.7 ms, `LoadDataRow` 14.9 ms. | Keep production on the current `NewRow` path with `BeginLoadData` and `MinimumCapacity`; alternatives are not enough better to trade away conservative semantics. |
| Owned-page discovery | Recognized per-table usage maps are about 2.3 ms for cold first-row/full-scan; forced whole-file fallback is about 15.7-15.8 ms on the same large-file shape. | Recognized maps avoid the O(total file pages) cold-start path. Keep the whole-file scan as a safety fallback for unfamiliar or invalid maps. |
| Table-scan read-ahead | Warm full scans improve when page-read optimization is enabled: numeric 10.5 ms to 8.7 ms, text 8.8 ms to 6.9 ms, wide 19.0 ms to 16.8 ms. Cold first-row latency does not improve. | Keep the one-page read-ahead as an automatic but narrowly guarded throughput benefit with opt-out; do not add tunable depth or LVAL-heavy read-ahead now. |

## Historical baseline

The saved baseline artifact was run on Windows 11, .NET SDK 10.0.203,
BenchmarkDotNet 0.15.8, Intel Core Ultra 7 268V. It remains useful as a rough
before/after comparison, but the focused refresh above is the current decision
point.

| Benchmark | Fixture | Mean | Allocated | Notes |
|---|---:|---:|---:|---|
| `Decode_Memo_DataTable` | 5,000 rows | 178.610 ms | 147.47 MB | Slowest measured baseline path. |
| `Decode_Memo_Untyped` | 5,000 rows | 159.447 ms | 146.66 MB | Streaming was dominated by LVAL payload work. |
| `Decode_Memo_Typed` | 5,000 rows | 157.329 ms | 146.63 MB | POCO mapping was not the bottleneck for MEMO rows. |
| `Decode_Text_DataTable` | 25,000 rows | 56.684 ms | 21.18 MB | DataTable materialization roughly doubled text streaming time. |
| `Decode_Numeric_DataTable` | 25,000 rows | 27.468 ms | 13.20 MB | DataTable overhead was visible even on fixed-width rows. |
| `Decode_Text_Untyped` | 25,000 rows | 22.954 ms | 16.35 MB | Text allocation dominated ordinary text scans. |
| `Decode_Text_Typed` | 25,000 rows | 24.778 ms | 15.63 MB | Similar to untyped because strings still had to be allocated. |
| `Decode_Wide_Untyped` | 10,000 rows | 20.177 ms | 23.08 MB | Decoded all 40 columns. |
| `Decode_Wide_Typed_NarrowProjection` | 10,000 rows | 12.917 ms | 1.75 MB | Projection optimization was already paying off. |
| `Decode_Numeric_Untyped` | 25,000 rows | 9.886 ms | 8.26 MB | Fixed-width streaming baseline. |
| `Decode_Numeric_Typed` | 25,000 rows | 12.028 ms | 3.54 MB | Lower allocation, modestly higher mean. |

`OpenAsync` is not the main large-read bottleneck in the current data:
`Open_Northwind` is 1.254 ms / 40.81 KB, and synthetic open benchmarks are in
the same range.

## Current bottleneck guidance

### 1. MEMO/OLE LVAL decode

MEMO/OLE remains the largest special-case read cost, but the avoidable
allocation work has already been reduced. The current implementation fills the
declared chained payload buffer directly for valid chains, uses inline cycle
detection before allocating a `HashSet<uint>`, reuses cached live-row bounds when
locating LVAL rows, and avoids async slow-path setup on cached LVAL page hits.

Remaining cost is mostly inherent: non-inline values require additional page
reads, text MEMO values must allocate the returned `string`, and OLE callers
must receive a stable `byte[]`. Reopen this area only with a new profile that
points to an avoidable LVAL-specific cost.

Primary code path:

- `LongValueDecoder.ReadLongValueAsync`
- `LongValueDecoder.ReadLongValueRawBytesAsync`
- `LongValueDecoder.ReadOleValueBytesAsync`
- `LongValueDecoder.ReadLvalChainAsync`
- `LongValueDecoder.LocateLvalRowAsync`
- `LongValueDecoder.DecodeLongValue`
- `LongValueStore.ReadChainedPayloadAsync`

### 2. DataTable materialization

`DataTable` remains materially more expensive than streaming APIs because it
forces full materialization, allocates `DataRow` instances, and assigns cells
through the general `DataRow` machinery. The public path now uses
`BeginLoadData()` / `EndLoadData()`, sets `MinimumCapacity` when bounded by table
row count or `maxRows`, and tracks loaded row count directly.

Keep the current `NewRow` path. Benchmark alternatives such as
`Rows.Add(object?[])` and `LoadDataRow` are not enough better to justify changing
row-state and null-handling semantics.

Primary code path:

- `AccessReader.ReadDataTableAsync`
- `DataTable.NewRow()`
- Per-cell assignment through `DataRow`
- `DataTable.Rows.Add(newRow)`

### 3. Text-heavy row allocation

Text-heavy scans still allocate final `string` instances, but transient decode
allocation is no longer the obvious target. Byte-array-backed Jet4 compressed
text decode uses `string.Create` for decompression paths, and modern target
frameworks use `Encoding.Latin1.GetString` for the all-compressed fast path.

Reopen this area only if a new profile shows an avoidable cost beyond required
string materialization.

Primary code path:

- `AccessBase.DecodeJet4Text`
- `AccessBase.DecompressJet4`
- `AccessBase.CreateFromCompressed`
- `AccessBase.DecompressJet4Slow`
- `JetTypeInfo.DecodeUtf16LE`

### 4. Unprojected wide-row decode

Wide untyped scans still decode every column into a fresh `object?[]` per row.
The current optimization is to use `Rows<T>()` when callers can express a narrow
primitive projection; `DirectRowDecoderBuilder.TryBuild<T>` can then bypass much
of the object-array work for supported shapes.

Object-array consumers should still expect to pay for all requested columns.
There is no pending change here without a new API shape or fresh profiling.

Primary code path:

- `AccessReader.EnumerateTypedRowsAsync`
- `AccessReader.TryCrackRowSyncIntoBuffer`
- `AccessReader.ResolveColumnSliceForDirectDecode`
- `DirectRowDecoderBuilder.TryBuild`

### 5. Cold owned-page discovery on large files

`GetOwnedDataPagesAsync` now attempts to use recognized per-table INLINE and
REFERENCE owned-page maps before falling back to the whole-file owner index.
Recognized maps scale with table pages instead of total database pages. The
fallback remains intentional for corrupt or unfamiliar usage-map shapes.

The full-file fallback uses uncached page reads and returns pooled pages
immediately, so its classification pass does not churn the normal reader LRU
before the actual table scan.

Primary code path:

- `AccessReader.GetOwnedDataPagesAsync`
- `AccessReader.BuildOwnedDataPageIndexAsync`
- `AccessReader.ReadPageCachedAsync`

### 6. Table-scan read-ahead

`PageReadOptimizationMode` controls whether path-opened databases use random-
access file options and whether eligible simple table scans use a conservative
one-page read-ahead path. The default `Auto` mode enables random-access reads
for path-opened readers, but table-scan read-ahead is more selective than
explicit `Enabled`: it requires a file-backed stream, no active transaction
journal, and enough table pages to benefit after the first page. `Disabled`
preserves the seek/read path and suppresses table-scan read-ahead. The read-
ahead path preserves row order and reuses the normal page cache.

Eligibility is intentionally narrow: page cache enabled, cache size of at least
three pages, more than one data page for explicit `Enabled` or at least three
data pages for `Auto`, and no MEMO/OLE/complex/attachment columns. Those
exclusions avoid cache re-entrancy and pooled-buffer ownership risks while
long-value or complex-column resolution may read additional pages for the
current row. In `Auto`, the first page is yielded before prefetch begins to
avoid adding speculative I/O to first-row latency.

Keep this as automatic with opt-out. Do not add tunable depth or LVAL-heavy
read-ahead without a page-buffer lease model and a fresh profile.

Primary code path:

- `AccessReaderOptions.PageReadOptimizationMode`
- `AccessReader.CreateStream`
- `AccessBase.EnableRandomAccessPageReadsIfSupported`
- `AccessBase.ReadPageAsync`
- `AccessReader.EnumerateTableScanPagesAsync`
- Table scan loops in `Rows()`, `Rows<T>()`, `RowsAsStrings`,
  `ReadDataTableAsync`, `ReadFirstTableAsStringsAsync`, and list materialization paths

## When read performance still feels slow

Start by checking the workload shape before changing the core reader:

- Confirm the slow path is not `OpenAsync`. Current measurements put the open
  floor around 1.1 ms / 41 KB, so large-read work should usually focus on row
  scan shape, materialization, projection, long values, or filtering.
- Separate cold first-row latency from full-scan throughput. The read-ahead path
  helps warm full scans more than first-row latency.
- Compare the real workload against the existing synthetic shapes: fixed-width
  numeric, text-heavy, wide rows, MEMO/OLE long values, `DataTable`
  materialization, and owned-page discovery.

## High-leverage caller choices

### Prefer narrow `Rows<T>()` projections

Use `Rows<T>()` with a DTO that binds only the columns needed by the caller.
The reader can emit a direct page-to-POCO decoder for primitive projections,
which avoids per-row `object?[]` allocation and primitive boxing. This is the
best available path for wide tables when the caller does not need every column.

The direct path applies when bound properties match the source CLR types and no
bound column requires calculated-column, MEMO/OLE, Binary, Complex/Attachment,
or Hyperlink handling. When the direct path cannot apply, the fallback still
uses the projection-aware typed crack path for non-complex tables.

### Avoid `DataTable` in hot paths

`ReadDataTableAsync`, `ReadAllTablesAsync`, and string-typed `DataTable` APIs
are convenience and compatibility APIs. They fully materialize rows, allocate
`DataRow` instances, and assign every cell through `DataRow` machinery. Keep
them for UI binding, previews, exports, and compatibility layers; use streaming
for bulk processing.

### Use count and seek APIs when they match the question

Use `GetRealRowCountAsync` for accurate row counts instead of
`Rows(...).CountAsync()` when cell values are irrelevant. It still scans data
pages, but it skips full row decode and long-value resolution.

Use `SeekRowsAsync` for exact indexed lookups instead of `Rows(...).Where(...)`
when the predicate matches an available Jet4/ACE index. LINQ filters run after
rows are decoded; index seek starts from the B-tree. Current seek support is
exact-match only and returns full `object[]` rows.

### Treat MEMO/OLE as a two-phase read when possible

For tables with expensive long values, first scan only key, filter, and status
columns through a narrow DTO. Resolve MEMO/OLE payloads only for the rows that
survive the first pass. Public full-row APIs must return stable `string` and
`byte[]` values, so they cannot make those payloads lazy without a new API
shape.

### Tune cache and read-ahead for repeat scans

For repeated full scans over simple tables, try:

```csharp
var options = new AccessReaderOptions
{
    PageCacheSize = 2048,
    PageReadOptimizationMode = PageReadOptimizationMode.Enabled,
};
```

The default `PageReadOptimizationMode.Auto` enables the guarded read-ahead path
for file-backed scans with at least three data pages. Use `Enabled` only when a
caller wants to force the less conservative path after previously disabling it.
The path requires page caching and no MEMO/OLE/complex/attachment columns. It
is most useful for warm full-scan throughput, not cold first-row latency.

## Evidence-gated future ideas

These are not active work items. They are possible API or benchmark directions
only if a future workload provides evidence that the existing caller guidance is
insufficient.

### Public object-array projection API

A public object-array projection API such as `Rows(tableName, columns)` or a
similar column-selection surface could expose projection benefits to callers
that cannot or do not want to define DTOs. Internally, `RowDecodePlan` already
supports a projection mask; today the public projection benefit is mainly
exposed through `Rows<T>()`.

This would likely be the best next library feature if wide untyped scans become
the remaining measured pain point.

### Projected or typed index seek

A `SeekRowsAsync<T>` or projected seek API could let exact indexed lookups avoid
materializing full `object[]` rows. The current `SeekRowsAsync` narrows page
discovery through the index but then decodes complete rows for each hit.

This may be worthwhile if measured workloads perform many indexed point lookups
and only consume a few columns from each match.

### Lazy long-value access

A new opt-in API could expose MEMO/OLE payloads through a lazy reader or handle
instead of immediately returning `string` / `byte[]`. This would require a
careful lifetime and page-buffer ownership model. It should not be mixed into the
existing row APIs because those APIs currently return stable values independent
of the reader's internal page buffers.

### Workload-specific benchmarks

Future read-performance work should begin with a benchmark that matches the slow
real workload before changing the core decoder again. Useful comparisons:

- Full `Rows(...)` versus narrow `Rows<T>()`.
- `Rows(...).Where(...)` versus `SeekRowsAsync` for exact indexed predicates.
- `Rows<T>()` two-phase MEMO/OLE filtering versus full-row long-value decode.
- Default options versus larger `PageCacheSize` plus explicit
  `PageReadOptimizationMode.Enabled`.
- Streaming APIs versus `ReadDataTableAsync` only when full materialization is
  truly required.

## Non-goals without new evidence

- Reopening text decode micro-optimizations without a fresh profile showing
  avoidable transient allocation beyond required final `string` creation.
- Swapping the production `DataTable` insertion strategy based only on the prior
  `Rows.Add(object?[])` or `LoadDataRow` benchmark results.
- Adding tunable read-ahead depth or enabling read-ahead for long-value-heavy
  scans before there is a page-buffer lease model and a profile showing it will
  pay for its complexity.
- Spending more time on lazy catalog loading unless new release-quality data
  contradicts the current `OpenAsync` floor.

## Measurement commands

These are the commands used for the focused refresh; omit `--job short` for a
release-quality full BenchmarkDotNet run.

```powershell
dotnet run --project JetDatabaseWriter.Benchmarks -c Release -- --filter *AccessReaderRowDecodeBenchmarks* --job short
dotnet run --project JetDatabaseWriter.Benchmarks -c Release -- --filter *DataTableMaterializationBenchmarks* --job short
dotnet run --project JetDatabaseWriter.Benchmarks -c Release -- --filter *AccessReaderOwnedPageDiscoveryBenchmarks* --job short
dotnet run --project JetDatabaseWriter.Benchmarks -c Release -- --filter *AccessReaderTableScanReadAheadBenchmarks* --job short
```

Summary decisions from the refresh:

- LVAL and text benchmark deltas confirm the completed allocation work is enough
  for this pass; more decode-path work needs a new profile.
- DataTable materialization keeps the current production path; alternatives do
  not justify semantic risk.
- Owned-page discovery results validate the recognized usage-map path.
- Read-ahead stays as one-page opt-in lookahead with no tunable depth yet.
