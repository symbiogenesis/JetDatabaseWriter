# Read performance

Status: closed; retained as archived baseline and caller guidance
Date: 2026-05-20
Closed: 2026-05-31
Last updated: 2026-10-03

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
- DataTable strategy benchmarks: `JetDatabaseWriter.Benchmarks/Reader/DataTableMaterializationBenchmarks.cs`
- Owned-page discovery benchmarks: `JetDatabaseWriter.Benchmarks/Reader/AccessReaderOwnedPageDiscoveryBenchmarks.cs`
- Table-scan read-ahead benchmarks: `JetDatabaseWriter.Benchmarks/Reader/AccessReaderTableScanReadAheadBenchmarks.cs`
- Read-ahead eligibility benchmarks (long-value tables, small or disabled caches): `JetDatabaseWriter.Benchmarks/Reader/AccessReaderReadAheadEligibilityBenchmarks.cs`
- Long values longer than the page cache: `JetDatabaseWriter.Benchmarks/Reader/AccessReaderLargeLongValueBenchmarks.cs`
- Concurrent scans on one shared reader versus one reader per scan, plain and AES-encrypted, in `Disabled` (page reads through the reader's I/O gate) and `Auto`: `JetDatabaseWriter.Benchmarks/Reader/AccessReaderConcurrentScanBenchmarks.cs`
- `Query<T>().Include(...)` over a related customer/order pair: `JetDatabaseWriter.Benchmarks/Queries/QueryIncludeBenchmarks.cs`
- The per-call cost of typed reads and `Query<T>()` (materializer and predicate compiles, translation, the in-memory tail, index-ordered reads, inferred range seeks and aggregates): `JetDatabaseWriter.Benchmarks/Queries/QueryBenchmarks.cs`
- Attachment reads (`GetAttachmentsAsync`, `Rows()`, `Rows<T>()` with `ComplexCellValue.ReadAttachments`), writer-authored and Access-authored: `JetDatabaseWriter.Benchmarks/Reader/ComplexColumnReadBenchmarks.cs`
- Public seek APIs (`SeekRowsAsync`, `FromIndex`, inferred `Rows<T>(predicate)`) against a client-side scan: `JetDatabaseWriter.Benchmarks/Indexes/PublicSeekBenchmarks.cs`
- Opening and scanning a file the OS has not cached (Windows only): `JetDatabaseWriter.Benchmarks/Reader/AccessReaderColdScanBenchmarks.cs`
- Benchmark fixture sizes: `JetDatabaseWriter.Benchmarks/Infrastructure/SyntheticDatabases.cs`
- Main read path: `JetDatabaseWriter/Tables/TableReader.cs` (table scans), `JetDatabaseWriter/ValueDecoding/RowDecoder.cs` (row decode), and `JetDatabaseWriter/Pages/ReaderPageCache.cs` (page and row-bound caches)
- Page I/O: `JetDatabaseWriter/Pages/Paging/PageFile.cs` (the writer's `Pager.cs` beside it); owned pages and row directories: `JetDatabaseWriter/Pages/OwnedDataPages.cs` and `DataPageRows.cs`; text decode helpers: `JetDatabaseWriter/Schema/JetTypeInfo.cs`
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
  `Func<object?[], T>` that typed reads use when no direct page-to-POCO decoder
  applies (and that index and linked-table reads always use); type mismatches flow through
  `Mapping/ValueCoercer`, which `Include`'s `RuntimeRowMapper` shares, and the
  `ToRow` / `Accessor` API remains available for writer-side mapping.
- `DirectRowDecoderBuilder.TryBuild<T>` can emit a direct page-to-POCO delegate
  for primitive projections. The compiled delegate still asks `RowDecodePlan`
  to parse row layout and resolve column slices, so direct and fallback decode
  paths share the same row-layout rules. It falls back to the typed crack path
  for bound calculated columns, `Memo` / `Ole` LVAL columns, `Binary`,
  `Complex` / `Attachment`, hyperlinks and type-mismatched targets. Complex
  columns the type does not bind do not stop it, and neither path reads their
  flat tables. `Numeric` direct-decodes for decimal targets using `NumericScale`.
- Synthetic benchmark databases are generated under `%TEMP%\JetBench\` by
  `SyntheticDatabases.cs`: `Numeric` has 25K rows / 9 columns, `TextHeavy` has
  25K rows / 6 columns, `Wide` has 10K rows / 40 columns, and `Memos` has 5K
  rows with an integer plus MEMO payload. `LargeLongValues` has 30 rows whose
  three 1.5M-character MEMOs (about 370 LVAL pages each) and three 2 MB OLE
  values (about 490 pages each) are each longer than the default 256-page cache;
  every other long-value fixture fits in the cache, which is how a cache-eviction
  bug that dropped the rows after a large MEMO went unmeasured. The numeric
  database also has an `AccdbAesCfbWrapped`-encrypted copy. The relational
  database has 1,000 `Customers` and 10,000 `Orders` (primary keys, a non-unique
  `OrderDate` index, and the `FK_Orders_Customers` relationship), the query
  database has a 6-row `QuerySmall` table and a 25K-row `QueryLarge` table
  (integer primary key and a non-unique `Score` index), and the attachment
  database has 150 `Documents` rows with one 16 KB attachment each. The
  relational and attachment databases are smaller than planned because of a
  writer limit: a full index rebuild that spreads a table's index pages over
  more than one inline usage-map bitmap throws `NotSupportedException`
  ("REFERENCE usage maps for index pages are not yet supported"). Creating the
  relationship over 20,000 orders that already have two indexes hits it, and so
  does `AddAttachmentAsync`, which rebuilds the hidden flat table's indexes on
  every call: on an 800-row table the 338th call threw.
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
| Read-ahead eligibility (2026-10-02, Arm64, .NET 10.0.12, in-process ShortRun, two interleaved before/after runs) | Warm `Auto` scans of the 25K-row numeric table: page cache disabled 9.9-10.3 ms before, 8.6-8.7 ms after read-ahead was allowed; 2-page cache 10.4-10.5 ms before, 8.7-8.9 ms after; 256-page cache unchanged at 8.8-9.0 ms. A 5,000-row MEMO table (62-73 ms) and a 2,000-row single-page OLE table (22.5-26 ms) moved by less than the run-to-run noise when read-ahead was allowed, at every cache size. | Allow read-ahead at any page-cache size, including none. Keep MEMO, OLE, complex and attachment tables sequential: the cache-ownership hazard behind that exclusion is gone, but it measured no gain. |
| Long values longer than the cache (2026-10-03, Arm64, .NET 10.0.12, in-process ShortRun on a shared machine) | `AccessReaderLargeLongValueBenchmarks`: a warm `Rows()` scan of the 30 `LargeLongValues` rows took 45-54 ms and allocated 30 MB at page-cache sizes 0, 8 and 256, in both `Disabled` and `Auto`. `Rows<T>()` was within the noise of `Rows()`, and opening a fresh reader for the scan added 22-30 ms. Every case returned all 30 rows with every value at full length. | No change. Setup checks the row count and value lengths, so a return of the evicted-page bug shows as an NA row rather than a fast result. `Auto` and `Disabled` were within this run's noise of each other on these values. |
| Include (same run) | `QueryIncludeBenchmarks` over 1,000 customers and 10,000 orders: the collection include took 52.7 ms and 18 MB, the same include filtered, ordered and capped at five orders per customer 51.9 ms, and the reference include (each order's customer) 9.3 ms and 4 MB. | The collection include costs 5-6 times the reference include over the same rows; filtering per parent runs after the load, so it saves nothing. Profile the collection path before changing it. |
| Attachment reads (same run) | `ComplexColumnReadBenchmarks`: 150 writer-authored 16 KB attachments took 14.7 ms through `GetAttachmentsAsync`, 18.0 ms through a `Rows()` scan and 21.0 ms through `Rows<T>()` plus `ComplexCellValue.ReadAttachments`. The 16 Access-authored Northwind category images took 8.4, 10.5 and 10.7 ms. | No change. Reading attachments through the parent row costs 23-43% more than reading the flat table directly. |
| Public seeks (same run) | `PublicSeekBenchmarks` over the 10,000 orders: 1,000 primary-key lookups took 38.7 ms (15 MB) through `SeekRowsAsync` and 31.9 ms through `FromIndex(...).WhereEquals`, against 2.9 ms for one `Rows<T>()` scan that keeps the same 1,000 keys. A 250-row `WhereBetween` on the `OrderDate` index took 0.47 ms. The same 1,000 lookups as `Rows<T>(o => o.OrderId == key)` took 525 ms and 58 MB; each call compiles its predicate (`Expression.Compile`) and lists the table's indexes before it seeks. | A seek costs about 32-39 µs and 15 KB, so one full scan of this table beats about 75 single-key seeks. The inferred `Rows<T>(predicate)` seek costs about 0.5 ms more per call than an explicit one; profile its per-call setup before recommending it in loops. |
| Page I/O handle (2026-10-03, Arm64, .NET 10.0.12, A/B of Release builds in interleaved processes on a shared machine) | Opening path-opened readers with a synchronous handle and no access hint, instead of an overlapped handle with the `RandomAccess` or `SequentialScan` hint, took a warm page read from 15-19 µs to 8-10 µs, warm MEMO scans from 97-124 ms to 67-76 ms and OLE scans from 42-58 ms to 36-46 ms, and the open and first scan of an uncached copy of the OLE table from 2.3-3.2 s to 0.18-0.66 s. | Keep `FileOptions.None` for path-opened readers in every mode; see "Page I/O handle" below. Re-measure on x64 and slower storage before changing it. |
| Inline page reads (same day and method) | Reading pages on the calling pool thread instead of handing each read to another pool thread took a warm page read from 6-10 µs to 2.6-4.5 µs, warm MEMO scans from 76-90 ms to 43-58 ms and OLE scans from 40-43 ms to 20-30 ms, and was faster on the numeric table in every run. | Keep inline reads for path-opened readers on pool threads without a synchronization context; see "Inline reads on thread-pool threads". |
| Concurrent scans (2026-10-03, after the two changes above; Arm64, .NET 10.0.12, in-process ShortRun on a shared machine) | `AccessReaderConcurrentScanBenchmarks`: N simultaneous `Rows()` scans of the 25K-row numeric table, whose 405 data pages are more than the default 256-page cache holds, plain and AES-encrypted. On one shared reader in `Auto`, whose `RandomAccess` page reads bypass the reader's I/O gate: 7.0-7.5 ms at N=1, 8.9-10.2 ms at 2, 10.0-12.9 ms at 4 and 20.2-20.6 ms at 8. In `Disabled`, where every page read the cache misses waits for the gate: 7.6-7.7, 8.7-9.7, 13.6-15.3 and 23.0-24.7 ms. One reader per scan, in either mode: 6.9-7.8, 10.1-12.3, 19.4-24.9 and 34.2-37.4 ms, allocating up to 25% more. The AES-encrypted copy was within this run's noise of the plain file. An earlier run, before the two changes above, used `Auto` only, so none of its page reads took the gate. | A shared reader scales better on this shape in both modes: scans running in near lockstep hit pages another scan has just cached, while separate readers each read and decode every page. On a shared reader `Disabled` was 12-36% slower than `Auto` at 4 and 8 scans, against 3-9% at one scan, so the I/O gate costs something under contention, but a shared reader in `Disabled` still beat separate readers in either mode. The AES transform lock did not show. |

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

- `JetTypeInfo.DecodeJet4Text`
- `JetTypeInfo.DecompressJet4`
- `JetTypeInfo.CreateFromCompressed`
- `JetTypeInfo.DecompressJet4Slow`
- `JetTypeInfo.DecodeUtf16LE`

### 4. Unprojected wide-row decode

Wide untyped scans still decode every column into a fresh `object?[]` per row.
The current optimization is to use `Rows<T>()` when callers can express a narrow
primitive projection; `DirectRowDecoderBuilder.TryBuild<T>` can then bypass much
of the object-array work for supported shapes.

Object-array consumers should still expect to pay for all requested columns.
There is no pending change here without a new API shape or fresh profiling.

Primary code path:

- `TableReader.EnumerateTypedRowsAsync`
- `RowDecoder.TryCrackRowSyncIntoBuffer`
- `RowDecodePlan.ResolveColumnSliceForDirectDecode`
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

- `OwnedDataPages.GetOwnedDataPagesAsync`
- `OwnedDataPages.BuildOwnedDataPageIndexAsync`
- `ReaderPageCache.ReadPageAsync`

### 6. Table-scan read-ahead

`PageReadOptimizationMode` controls whether path-opened readers of the net10.0
build read pages with positional `RandomAccess` reads and whether eligible
simple table scans use a conservative one-page read-ahead path. It does not
change how the file is opened (see "Page I/O handle" below). The default `Auto`
mode enables random-access reads for path-opened readers, but table-scan
read-ahead is more selective than
explicit `Enabled`: it requires a file-backed stream, no active transaction
journal, and enough table pages to benefit after the first page. `Disabled`
preserves the seek/read path and suppresses table-scan read-ahead. The read-
ahead path preserves row order and reuses the normal page cache.

Eligibility: no attached transaction journal, more than one data page for
explicit `Enabled` or at least three data pages for `Auto`, and no
MEMO/OLE/complex/attachment columns. Any page-cache size qualifies, including a
disabled cache. Caches under three pages and disabled caches used to be
excluded because the cache returned evicted buffers to the shared pool while a
scan still read them. `ReaderPageCache` now leaves evicted buffers to the GC,
so the exclusion was removed after the 2026-10-02 eligibility benchmark showed
a 12-17% gain on small or disabled caches. The long-value exclusion is no longer
a safety rule for the same reason, but the same benchmark showed no gain on
MEMO or OLE tables, whose long-value page reads dwarf the one data page the
prefetch overlaps, so those scans stay sequential. In `Auto`, the first page is
yielded before prefetch begins to avoid adding speculative I/O to first-row
latency.

The two page reads in flight can complete, and decrypt, on two threads at once.
Jet3 XOR and Jet4 RC4 decryption keep no state between pages; the cached
AES-ECB transforms in `PageDecryptionKeys` are built and used under a private
lock (see `concurrency-and-lock-ordering.md`). When a path-opened reader's scan
runs on a thread-pool thread, its prefetch completes inline instead; see
"Inline reads on thread-pool threads" below.

The same benchmark showed a separate cost: on the MEMO and OLE tables, `Auto`,
whose path-opened readers use positionless `RandomAccess` page reads, was about
20-30% slower than `Disabled`, which reads through the buffered `FileStream`,
with or without read-ahead. Its investigation (2026-10-03, same machine) found
that `RandomAccess` itself was not slower.
`ReadPageRandomAccessAsync` (then on `DatabaseFile`, now on `PageFile`) read `FileStream.SafeFileHandle` for
every page, and that getter is not a field read: it flushes the stream's buffer
and seeks the OS file pointer to the stream's position (a `SetFilePointerEx`
call) before it returns the handle, which cost 1.7-2.0 µs per call in
isolation. A long-value scan reads about one page per row (2,021 reads for the
2,000 OLE rows), so the getter showed there and not on the numeric table's 405
reads, which read-ahead overlaps with decode. The handle is now read once, by
`EnableRandomAccessPageReadsIfSupported`, and every page read reuses it. In
that investigation's 15-21 interleaved rounds of warm scans, `Auto` went from
111-138% of `Disabled` on the MEMO and OLE tables to 91-108%. A later replay of
the OLE scan's 2,021 page reads, on the same machine while other builds ran,
could not separate the builds before and after this change: 15-19 µs per read
either way, in both modes. Both modes paid a larger cost, the file handle's
type; see "Page I/O handle" below.

Keep this as automatic with opt-out. Do not add tunable depth or LVAL-heavy
read-ahead without a fresh profile that shows a gain.

Primary code path:

- `AccessReaderOptions.PageReadOptimizationMode`
- `AccessReader.CreateStream`
- `PageFile.EnableRandomAccessPageReadsIfSupported`
- `PageFile.ReadPageAsync`
- `TableReader.EnumerateTableScanPagesAsync`
- Every table scan in `TableReader`: `Rows()`, `Rows<T>()`, `RowsAsStrings`,
  `ReadDataTableAsync`, `ReadTableAsync<T>`, `ReadTableAsStringsAsync`,
  `ReadFirstTableAsStringsAsync`, and `GetRealRowCountAsync`

### 7. Page I/O handle

A path-opened reader opens its file with `FileOptions.None` in every
`PageReadOptimizationMode`: a synchronous handle, without
`FILE_FLAG_OVERLAPPED`, and with no `RandomAccess` or `SequentialScan` hint.
Before 2026-10-03 it opened an overlapped (`FileOptions.Asynchronous`) handle
with the `RandomAccess` hint in `Auto` and `Enabled` and the `SequentialScan`
hint in `Disabled`. Page reads stay asynchronous to the caller:
`RandomAccess.ReadAsync` (net10.0 build, `Auto` and `Enabled`) and
`FileStream.ReadAsync` (`Disabled`, and every mode of the netstandard2.1
build) run a synchronous read on a thread-pool thread, which is how .NET reads
every file on Unix.

On Windows an overlapped read completes through the I/O completion port and a
thread-pool callback even when the OS cache already holds the page; .NET does
not skip the completion port for reads that succeed at once, and 0-1% of the
overlapped reads were complete when awaited. The 2026-10-03 investigation
replayed a warm scan's 4 KB page reads at 14.8-19.1 µs each on the overlapped
handle, 3.8-4.9 µs as `RandomAccess.ReadAsync` on a synchronous one and
2.8-3.6 µs as a blocking `RandomAccess.Read`. On files the OS had not cached,
the hints and the overlapped handle also defeated the OS read-ahead: 9,800
sequential 4 KB reads took 965-1,040 ms on the old handles, 654 ms on a
synchronous handle with `SequentialScan`, 584 ms with `RandomAccess` and 47 ms
with no hint. Why `SequentialScan` was slow there is not understood.

A/B of Release builds before and after the change (2026-10-03, Arm64, NVMe,
.NET 10.0.12, the benchmark databases, path-opened readers with default
options except where noted). Ranges are the medians of four interleaved
processes on a machine shared with other builds, and for the uncached copies
every run of three processes, `Auto` and `Disabled` together unless split:

| Case | Overlapped handle with a hint | Synchronous handle, no hint |
|---|---|---|
| The OLE scan's 2,021 page reads replayed through `DatabaseFile.ReadPageAsync`, no page cache | 15.0-19.0 µs per read | 7.9-9.8 µs |
| Warm `Rows()` of the 2,000-row OLE table | 42-58 ms | 36-46 ms |
| Warm `Rows()` of the 5,000-row MEMO table | 97-124 ms | 67-76 ms |
| Warm `Rows()` of the 25,000-row numeric table, `Auto` | 10.2-19.9 ms | 8.9-14.0 ms |
| Open and first `Rows()` of an uncached copy, OLE table (includes the 25,228-page whole-file owned-page pass) | 2.3-3.2 s | 0.18-0.66 s |
| Open and first `Rows()` of an uncached copy, numeric table | 64-404 ms | 54-176 ms (within the noise) |

Each page read now holds a pool thread while the OS reads, as `FileStream`
does by default; an overlapped read held none while the device worked. On
Windows the I/O manager serializes I/O on a synchronous file object, so the
read-ahead pair and concurrent scans on one reader queue their reads in the
kernel instead of overlapping them; they still overlap reads with decode. All
of this was measured on one Arm64 machine with local NVMe storage. Re-run
`AccessReaderColdScanBenchmarks` on x64, a hard disk or a network share before
changing the options again; with many concurrent scans on slow storage, open
more readers. The writer still opens an overlapped handle with the
`RandomAccess` hint, which a separate, measured change would revisit.

A caller that opens the `FileStream` itself for
`AccessReader.OpenAsync(Stream)` gets the same benefit by opening it without
`FileOptions.Asynchronous` and without an access hint.

#### Inline reads on thread-pool threads

A path-opened reader also reads a page on the calling thread when that thread
is a thread-pool thread with no `SynchronizationContext` and the default
`TaskScheduler` (`PageFile.ReadsInlineOnThreadPool`). That is where most
reads start: the library awaits with `ConfigureAwait(false)`, and console,
ASP.NET Core and worker-service callers have no context. Handing a read that
takes a few microseconds to another pool thread, and waiting for it, cost more
than the read. A caller with a context, such as a UI thread, still has every
read offloaded and never blocks on the disk. Readers opened on a caller's
stream, and the writer, keep offloaded reads too, because their handles may be
overlapped, where a blocking read measured slower.

The trade-offs:

- An inline read cannot be cancelled once it has started. The token is checked
  before every page read, so a cancelled scan stops at its next page, but a
  read that hangs on a network share holds its pool thread until the OS gives
  up.
- On such threads the read-ahead prefetch completes inline, before the current
  page is yielded, so it no longer overlaps decode. The scans were faster
  anyway, including the numeric table that read-ahead was built for.
- The pool thread blocks for the read, the same thread time as the offloaded
  read, which also blocked a pool thread.

A/B of Release builds with offloaded reads and with inline reads, by the same
method as above (medians of four interleaved processes, `Auto` and `Disabled`
together):

| Case | Offloaded reads | Inline on pool threads |
|---|---|---|
| The OLE scan's 2,021 page reads replayed through `DatabaseFile.ReadPageAsync`, no page cache | 6.1-9.8 µs per read | 2.6-4.5 µs |
| Warm `Rows()` of the 2,000-row OLE table | 40-43 ms | 20-30 ms |
| Warm `Rows()` of the 5,000-row MEMO table | 76-90 ms | 43-58 ms |
| Warm `Rows()` of the 25,000-row numeric table | 12.9-21.8 ms | 9.1-18.4 ms, faster in every run |
| Open and first `Rows()` of an uncached copy, OLE table (every run of three processes) | 0.18-1.1 s | 0.17-0.53 s |

The overlapped handle with a hint, in the same runs: 48-94 ms on the OLE
table, 121-280 ms on the MEMO table, 14-74 ms on the numeric table and
2.2-27 s for the uncached OLE copy.

Primary code path:

- `AccessReader.CreateStream`
- `AccessReader.OpenAsync(string, ...)`, which sets `ReadsInlineOnThreadPool`
- `PageFile.ReadPageAsync`
- `PageFile.ReadPageRandomAccessAsync` and `ReadPageRandomAccess`
- `PageFile.ReadPageFromStream`

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
`ReadTableAsync<T>()` materializes the same scan into a list, so it picks the
same decoder. The reader can emit a direct page-to-POCO decoder for primitive projections,
which avoids per-row `object?[]` allocation and primitive boxing. This is the
best available path for wide tables when the caller does not need every column.

The direct path applies when bound properties match the source CLR types and no
bound column requires calculated-column, MEMO/OLE, Binary, Complex/Attachment,
or Hyperlink handling. When the direct path cannot apply, the fallback still
uses the projection-aware typed crack path. On a table with Attachment or
multi-value columns, both paths leave the complex columns the DTO does not bind
alone: their flat tables and attachment data are neither loaded nor decoded, so
a DTO of scalar columns reads only the table's own data pages. Index seeks and
`Rows<T>(predicate)` read through an index do the same.

### Avoid `DataTable` in hot paths

`ReadDataTableAsync`, `ReadAllTablesAsync`, and string-typed `DataTable` APIs
are convenience and compatibility APIs. They fully materialize rows, allocate
`DataRow` instances, and assign every cell through `DataRow` machinery. Keep
them for UI binding, previews, exports, and compatibility layers; use streaming
for bulk processing.

### Use count and seek APIs when they match the question

Use `GetRealRowCountAsync` for accurate row counts instead of
`Rows(...).CountAsync()` when cell values are irrelevant. It still scans data
pages and checks each row's layout, so it counts the same rows the reads
return, but it skips full row decode and long-value resolution.

Use `SeekRowsAsync` or `FromIndex(...)` (`WhereEquals`, `WhereKeyPrefix`,
`WhereBetween`, `WhereRange`) for indexed lookups instead of
`Rows(...).Where(...)` when the predicate matches an available Jet4/ACE index.
LINQ filters run after rows are decoded; an index seek starts from the B-tree.
Each seek has a fixed cost, though: on the 10,000-row `PublicSeekBenchmarks`
table, 1,000 single-key seeks took 32-39 ms while one full scan that kept the
same keys took 2.9 ms. For more than a few dozen keys, scan once and filter
against a `HashSet`. `Rows<T>(predicate)` infers the index but recompiles and
replans on every call (about 0.5 ms each), so prefer the explicit APIs in loops.

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
The path works at any page-cache size and skips tables with MEMO/OLE/complex/
attachment columns. It is most useful for warm full-scan throughput, not cold
first-row latency.

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
- Adding tunable read-ahead depth without a profile showing it will pay for its
  complexity, or enabling read-ahead for long-value-heavy scans, which measured
  no gain on 2026-10-02.
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
dotnet run --project JetDatabaseWriter.Benchmarks -c Release -- --filter *AccessReaderReadAheadEligibilityBenchmarks* --job short
dotnet run --project JetDatabaseWriter.Benchmarks -c Release -- --filter *AccessReaderLargeLongValueBenchmarks* --job short
dotnet run --project JetDatabaseWriter.Benchmarks -c Release -- --filter *AccessReaderConcurrentScanBenchmarks* --job short
dotnet run --project JetDatabaseWriter.Benchmarks -c Release -- --filter *QueryIncludeBenchmarks* --job short
dotnet run --project JetDatabaseWriter.Benchmarks -c Release -- --filter *ComplexColumnReadBenchmarks* --job short
dotnet run --project JetDatabaseWriter.Benchmarks -c Release -- --filter *PublicSeekBenchmarks* --job short
dotnet run --project JetDatabaseWriter.Benchmarks -c Release -- --filter *AccessReaderColdScanBenchmarks* --job short
```

Summary decisions from the refresh:

- LVAL and text benchmark deltas confirm the completed allocation work is enough
  for this pass; more decode-path work needs a new profile.
- DataTable materialization keeps the current production path; alternatives do
  not justify semantic risk.
- Owned-page discovery results validate the recognized usage-map path.
- Read-ahead stays as one-page opt-in lookahead with no tunable depth yet.
