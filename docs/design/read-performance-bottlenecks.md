# Read performance bottlenecks

Status: analysis, implementation slices, and measurement plan
Date: 2026-05-20
Last updated: 2026-05-24

This note summarizes the slowest known bottlenecks when reading large amounts
of data through `AccessReader`, based on the checked-in BenchmarkDotNet results
and the current read-path implementation. It is meant to guide optimization
work without re-opening the already-settled `OpenAsync` floor investigation.

## Evidence sources

- Benchmark results: `BenchmarkDotNet.Artifacts/results/JetDatabaseWriter.Benchmarks.AccessReaderRowDecodeBenchmarks-report-github.md`
- Open-floor results: `BenchmarkDotNet.Artifacts/results/JetDatabaseWriter.Benchmarks.AccessReaderOpenBenchmarks-report-github.md`
- Benchmark fixture sizes: `JetDatabaseWriter.Benchmarks/SyntheticDatabases.cs`
- Main read path: `JetDatabaseWriter/AccessReader.cs`
- Shared page, row, and text decode helpers: `JetDatabaseWriter/AccessBase.cs`
- Long-value decode path: `JetDatabaseWriter/ValueDecoding/LongValueDecoder.cs`

The existing repository memory records the stable typed-row architecture:
`Rows()`, `Rows<T>()`, and `ReadDataTableAsync()` use the typed crack path;
`Rows<T>()` can use a direct page-to-POCO decoder; and `RowsAsStrings()` remains
the legacy string compatibility path.

## Current benchmark snapshot

Environment from the saved artifacts: Windows 11, .NET SDK 10.0.203,
BenchmarkDotNet 0.15.8, Intel Core Ultra 7 268V.

| Benchmark | Fixture | Mean | Allocated | Notes |
|---|---:|---:|---:|---|
| `Decode_Memo_DataTable` | 5,000 rows | 178.610 ms | 147.47 MB | Slowest measured read path. |
| `Decode_Memo_Untyped` | 5,000 rows | 159.447 ms | 146.66 MB | Streaming still dominated by LVAL payload work. |
| `Decode_Memo_Typed` | 5,000 rows | 157.329 ms | 146.63 MB | POCO mapping is not the bottleneck for MEMO rows. |
| `Decode_Text_DataTable` | 25,000 rows | 56.684 ms | 21.18 MB | DataTable materialization roughly doubles text streaming time. |
| `Decode_Numeric_DataTable` | 25,000 rows | 27.468 ms | 13.20 MB | DataTable overhead is visible even on fixed-width rows. |
| `Decode_Text_Untyped` | 25,000 rows | 22.954 ms | 16.35 MB | Text allocation dominates ordinary text scans. |
| `Decode_Text_Typed` | 25,000 rows | 24.778 ms | 15.63 MB | Similar to untyped because strings must still be allocated. |
| `Decode_Wide_Untyped` | 10,000 rows | 20.177 ms | 23.08 MB | Decodes all 40 columns. |
| `Decode_Wide_Typed_NarrowProjection` | 10,000 rows | 12.917 ms | 1.75 MB | Projection optimization is already paying off. |
| `Decode_Numeric_Untyped` | 25,000 rows | 9.886 ms | 8.26 MB | Fixed-width streaming baseline. |
| `Decode_Numeric_Typed` | 25,000 rows | 12.028 ms | 3.54 MB | Lower allocation, modestly higher mean. |

`OpenAsync` is not the main large-read bottleneck in the current data:
`Open_Northwind` is 1.254 ms / 40.81 KB, and synthetic open benchmarks are in
the same range. Do not spend optimization time here unless new profiling data
contradicts the current floor.

## Ranked bottlenecks

### 1. MEMO/OLE LVAL decode

This is the largest measured hotspot. The MEMO fixture has only 5,000 rows, but
all MEMO variants are around 157-179 ms and allocate about 146-147 MB. That is
an order of magnitude slower than fixed-width row scans.

Status: local implementation addressed, pending benchmark refresh. Avoidable
per-value overhead in chained assembly, cycle detection, row-bound parsing,
cached-page continuation setup, and chained OLE fallback copying has been
reduced. Remaining cold page I/O and final value materialization are inherent to
the storage format or tracked by the later text/allocation and prefetch sections.

Primary code path:

- `LongValueDecoder.ReadLongValueAsync`
- `LongValueDecoder.ReadLongValueRawBytesAsync`
- `LongValueDecoder.ReadOleValueBytesAsync`
- `LongValueDecoder.ReadLvalChainAsync`
- `LongValueDecoder.LocateLvalRowAsync`
- `LongValueDecoder.DecodeLongValue`

Completed LVAL work:

- `ReadLvalChainAsync` now fills the final declared payload buffer directly for
  valid chains; corrupt or short chains still trim to the actual byte count.
- Short chained values now use inline cycle detection; a `HashSet<uint>` is
  allocated only after the inline visited-page capacity is exceeded.
- LVAL row location now reuses the reader's cached live-row bounds instead of
  re-parsing the row-offset trailer for every located LVAL row.
- Cached LVAL page hits now return a completed `ValueTask` from
  `LocateLvalRowAsync` without entering the async slow path.
- Chained OLE byte fallback can reuse the owned chain buffer when no package
  extraction or payload slicing is needed; page-backed inline and single-page
  values still return copied `byte[]` instances.

Closed or handed-off cost centers:

- Chained LVAL values still read one or more additional pages per cold cell; this
  is inherent to non-inline storage. Broader cold-scan prefetch remains tracked
  under `ParallelPageReadsEnabled` / table-scan read-ahead.
- Text MEMO values must allocate the returned `string`, and OLE callers must
  receive a stable `byte[]`. Transient text decode allocation is tracked in the
  text-heavy row allocation section.

### 2. `DataTable` materialization

`DataTable` is consistently much slower than streaming APIs. For text rows it is
56.684 ms versus roughly 23-25 ms for streaming. For numeric rows it is 27.468 ms
versus roughly 10-13 ms for streaming.

Primary code path:

- `AccessReader.ReadDataTableAsync`
- `DataTable.NewRow()`
- Per-cell assignment through `DataRow`
- `DataTable.Rows.Add(newRow)`

Likely cost centers:

- `DataTable` forces full materialization instead of lazy row streaming.
- Every row allocates a `DataRow`.
- Every cell is assigned through the general `DataRow` machinery.
- Column constraints, indexes, and change tracking may run unless bulk-load mode
  is enabled.

This API will always be higher memory than `Rows()` / `Rows<T>()`; optimization
should reduce avoidable overhead, not try to make it equivalent to streaming.

### 3. Text-heavy row allocation

Text streaming is much slower and more allocation-heavy than fixed-width
streaming because every string value has to allocate. The benchmark shows
roughly 16 MB allocated for 25,000 text-heavy rows.

Primary code path:

- `AccessBase.DecodeJet4Text`
- `AccessBase.DecompressJet4`
- `AccessBase.CreateFromCompressed`
- `AccessBase.DecompressJet4Slow`
- `JetTypeInfo.DecodeUtf16LE`

Likely cost centers:

- The common compressed Jet4 fast path currently creates a `char[]`, fills it,
  then constructs a `string`, doubling transient character storage.
- Slow decompression also counts and fills into a `char[]` before constructing
  the final `string`.
- Text-heavy POCO mapping cannot avoid the string allocation when the caller
  actually binds string properties.

### 4. Unprojected wide-row decode

The wide table benchmark shows a clear split: untyped decode of all 40 columns is
20.177 ms and 23.08 MB, while a typed narrow projection is 12.917 ms and 1.75 MB.
The current `Rows<T>()` projection optimization is working, but object-array
consumers still pay for every column.

Primary code path:

- `AccessReader.EnumerateTypedRowsAsync`
- `AccessReader.TryCrackRowSyncIntoBuffer`
- `AccessReader.ResolveColumnSliceForDirectDecode`
- `DirectRowDecoderBuilder.TryBuild`

Likely cost centers:

- `Rows()` has to decode all columns into a fresh `object?[]` per row.
- Wide tables multiply boxing and string allocation costs.
- The direct page-to-POCO path only applies to `Rows<T>()` shapes that bind
  directly decodable columns.

### 5. Cold owned-page discovery on large files

The current benchmarks isolate warmed row decode, but a real first table scan can
pay for owned-page discovery before yielding rows. `GetOwnedDataPagesAsync` now
uses recognized per-table INLINE and REFERENCE owned-page maps when possible,
but unfamiliar or corrupt usage-map shapes still fall back to
`_ownedDataPageIndex`, which builds by scanning every page from page 3 to EOF and
checking data-page ownership.

Primary code path:

- `AccessReader.GetOwnedDataPagesAsync`
- `AccessReader.BuildOwnedDataPageIndexAsync`
- `AccessReader.ReadPageCachedAsync`

Remaining cost centers:

- Unfamiliar or corrupt usage maps still use the whole-file fallback, so their
  first table read is O(total file pages), not O(table pages).
- Usage-map discovery validates mapped data pages before scanning rows, so it
  scales with table pages but still adds a cold verification pass.
- This cost is not represented by the `OpenAsync` benchmark because the index is
  lazily initialized.

### 6. `ParallelPageReadsEnabled` only prefetches eligible simple table scans

The option switches the file stream access pattern for path-opened databases and
now adds a one-page read-ahead pipeline for eligible table scans. The read-ahead
path preserves row order and reuses the normal page cache, but it is deliberately
gated to simple table shapes: page cache enabled, cache size of at least three
pages, more than one data page, and no MEMO/OLE/complex/attachment columns.
Those exclusions avoid cache re-entrancy and pooled-buffer eviction risks while
LVAL or complex-column resolution may read additional pages for the current row.

Primary code path:

- `AccessReaderOptions.ParallelPageReadsEnabled`
- `AccessReader.CreateStream`
- `AccessBase.EnableRandomAccessPageReadsIfSupported`
- `AccessBase.ReadPageAsync`
- `AccessReader.EnumerateTableScanPagesAsync`
- Table scan loops in `Rows()`, `Rows<T>()`, `RowsAsStrings`,
  `ReadDataTableAsync`, `ReadFirstTableAsStringsAsync`, and list materialization paths

## Implementation plan

### Phase 0: improve measurement coverage

Add focused benchmarks before changing behavior:

Status: benchmark definitions implemented, pending refreshed measurements. LVAL
submode benchmarks, OLE submode benchmarks, cold-open first-scan coverage,
DataTable insertion-strategy benchmarks including `NewRow`,
`Rows.Add(object?[])`, and `LoadDataRow`, numeric cold-scan variants for
disabled/large page caches, and a simple table-scan read-ahead matrix for
numeric/text/wide tables with cold/warm and `ParallelPageReadsEnabled` on/off
have been added.

- Cold first table enumeration versus warm repeat enumeration.
- LVAL inline, single-page, and chained MEMO separately.
- OLE byte payloads separately from MEMO text payloads.
- `DataTable` alternatives: current path, `BeginLoadData` / `EndLoadData`,
  `MinimumCapacity`, `Rows.Add(object?[])`, and `LoadDataRow`.
- Page-cache sizes for large scans: default, disabled, and enough to hold the
  whole synthetic table.
- `ParallelPageReadsEnabled` on/off for large cold and warm scans.

Acceptance criteria:

- The new benchmarks isolate first-row latency from steady-state row decode.
- LVAL benchmarks report inline, single-page, and chained costs separately.
- Results make allocation deltas visible, not just elapsed time.

### Phase 1: reduce LVAL allocation and per-value overhead

Start with the top measured bottleneck.

Completed changes:

- Chained LVAL assembly allocates the final payload buffer once when
  `memoLen` is known, then fills it directly from chunks. This removes the second
  copy from rented buffer to final array on valid chains.
- Short, well-formed chains use inline cycle detection and allocate a
  `HashSet<uint>` only after the inline visited-page capacity is exceeded.
- `LocateLvalRowAsync` has an explicit sync fast path returning a completed
  `ValueTask` when the LVAL page is already cached.
- LVAL row location reuses cached live-row bounds by page number, matching normal
  data-page row scans.
- Chained OLE byte fallback reuses the owned chain buffer when no package
  extraction or payload slicing is needed.

Remaining measurement:

- Refresh LVAL submode benchmarks to quantify allocation and elapsed-time deltas
  after the implementation changes.

Risks and constraints:

- Preserve cycle detection and corrupt-chain safety from the CVE hardening work.
- Preserve the 24-bit LVAL size bound.
- Do not return pooled buffers to public callers.

Acceptance criteria:

- `Decode_Memo_Untyped` and `Decode_Memo_Typed` reduce allocated bytes
  materially, ideally without trading off correctness or malformed-file safety.
- Chained LVAL microbenchmarks show fewer Gen0 collections and fewer copied
  bytes per payload.

### Phase 2: reduce text decode transient allocation

Text is the broadest ordinary-row cost after LVAL.

Completed changes:

- Byte-array-backed Jet4 compressed text decode now uses `string.Create` for
  the all-compressed fast path and the mode-switching slow path, removing the
  intermediate `char[]` on normal reader/writer hot paths.
- On modern target frameworks, the byte-array-backed all-compressed fast path
  now delegates to `Encoding.Latin1.GetString`, matching Jet4's one-byte
  U+0001..U+00FF compressed text semantics while keeping the `string.Create`
  fallback for `netstandard2.1`.
- The existing span-backed decoder remains as a compatibility fallback for any
  future span-only caller.

Remaining measurement:

- Refresh text-heavy benchmarks to quantify the `string.Create` and Latin-1
  decoder changes together.

Risks and constraints:

- Keep `netstandard2.1` compatibility. If needed, use conditional compilation
  and leave the existing implementation on older targets.
- Preserve Jet4 compressed-Unicode mode-switch semantics.

Acceptance criteria:

- `Decode_Text_Untyped`, `Decode_Text_Typed`, and `Decode_Text_AsStrings` reduce
  allocation and do not regress elapsed time.
- Existing text encoding and non-Western text tests continue to pass.

### Phase 3: reduce `DataTable` materialization overhead

Treat this as API-specific tuning, not a replacement for streaming.

Completed changes:

- `ReadDataTableAsync` now wraps row insertion in `BeginLoadData()` /
  `EndLoadData()`.
- `ReadDataTableAsync` sets `MinimumCapacity` when the table row count and/or
  `maxRows` provide a bounded capacity.
- The loader tracks the number of loaded rows directly for progress and
  `maxRows` instead of polling `DataTable.Rows.Count` after every row.
- README and public reader XML docs now nudge bulk consumers toward `Rows()` /
  `Rows<T>()` when `DataTable` materialization is not required.

Remaining measurement:

- Run the new DataTable materialization benchmarks to compare the public path,
  `NewRow` variants, `Rows.Add(object?[])`, and `LoadDataRow` with bulk-load
  mode and minimum capacity.
- If `Rows.Add(object?[])` wins materially, decide whether production can use it
  without adding per-row exact-array allocation or weakening null handling.

Risks and constraints:

- `DataTable` semantics can be surprisingly stateful. Verify row state,
  column types, `DBNull.Value`, and early `maxRows` returns.

Acceptance criteria:

- `Decode_Text_DataTable` and `Decode_Numeric_DataTable` improve without changing
  public return shape.
- DataTable tests cover nulls, strings, fixed primitives, MEMO, and `maxRows`.

### Phase 4: avoid whole-file owned-page scans when possible

This targets cold reads of large databases.

Completed changes:

- The full-file owner-index fallback now uses uncached page reads and returns
  each pooled page immediately, so the classification pass no longer fills or
  churns the normal reader LRU before the actual table scan begins.
- `GetOwnedDataPagesAsync` now attempts to read the TDEF `owned_pages` pointer
  first, parses type-0 INLINE usage-map rows, validates mapped data pages against
  their TDEF back-pointer, and caches successful per-table results without
  initializing the whole-file owner index.
- REFERENCE-form owned-page maps are also parsed by following type-1 map-page
  pointers to page-type `0x05` bitmap pages, with the same data-page back-pointer
  validation and fallback behavior.

Remaining measurement:

- Refresh cold first-scan benchmarks to quantify the difference between
  recognized usage maps and the whole-file fallback on large files.
- Keep the current whole-file owner scan available as a safety fallback for
  unusual or corrupt databases.

Risks and constraints:

- Usage-map parsing must be format-compatible across Jet3, Jet4, and ACE shapes
  before replacing the full scan broadly.
- Corrupt or unfamiliar usage maps must safely fall back.

Acceptance criteria:

- Cold first table scan time scales with table pages, not total database pages,
  for databases with recognized INLINE or REFERENCE usage maps.
- Full-file fallback behavior remains available for unfamiliar or invalid maps.

### Phase 5: make parallel page reads visible to table scans

The option now has a conservative implementation for simple table scans; this
phase tracks whether and how to extend it beyond that shape.

Status: partially implemented. Simple table scans now use a bounded one-page
read-ahead path when `ParallelPageReadsEnabled` is set and the page cache can
hold previous/current/prefetched pages. MEMO/OLE/complex/attachment tables still
use the sequential path until page-buffer ownership is made explicit enough for
LVAL-heavy scans.

Completed changes:

- Added a bounded table-page enumerator that starts the next eligible data-page
  read before the current page's rows are decoded.
- Preserved row order by awaiting prefetched pages in original page-number order.
- Kept the normal sequential path for cache-disabled, tiny-cache, single-page,
  and LVAL/complex scan shapes.
- Updated `ParallelPageReadsEnabled` API docs to describe read-ahead eligibility
  and random-access page reads rather than unconditional parallel processing.

Remaining work:

- Run the table-scan read-ahead benchmark matrix and compare cold/warm simple
  scans with `ParallelPageReadsEnabled` on/off.
- Revisit LVAL-heavy read-ahead only if page-buffer leases or a similar
  ownership model are introduced.
- Consider a tunable read-ahead depth only after one-page lookahead shows a
  measurable benefit and no cache contention regressions.

Risks and constraints:

- A single `AccessReader` should not become an unbounded parallel worker.
- Preserve cancellation and disposal behavior.
- Do not break transaction/journal semantics for shared base-class page I/O.

Acceptance criteria:

- Large cold scan benchmarks show a clear throughput or latency improvement when
  enabled, or documentation is corrected to match actual behavior.

## Recommended first slice

Start with Phase 0 plus the lowest-risk parts of Phases 1-3:

1. Add the missing benchmarks for LVAL submodes, DataTable insertion variants,
   and cold versus warm table scans.
2. Optimize chained LVAL assembly to fill the final buffer directly.
3. Replace Jet4 compressed text `char[]` construction with `string.Create` where
   target frameworks allow it.
4. Test `DataTable.BeginLoadData` / `EndLoadData` and `MinimumCapacity`.

This first slice attacks the top two measured slow paths and a broad allocation
hotspot without changing table-discovery semantics or introducing read-ahead
concurrency.
