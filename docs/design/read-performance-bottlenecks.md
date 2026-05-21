# Read performance bottlenecks

Status: analysis and implementation plan  
Date: 2026-05-20

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
pay for whole-file owner indexing before yielding rows. `GetOwnedDataPagesAsync`
uses `_ownedDataPageIndex`, which currently builds by scanning every page from
page 3 to EOF and checking data-page ownership.

Primary code path:

- `AccessReader.GetOwnedDataPagesAsync`
- `AccessReader.BuildOwnedDataPageIndexAsync`
- `AccessReader.ReadPageCachedAsync`

Likely cost centers:

- The first table read is O(total file pages), not O(table pages).
- The index builder uses the normal page cache, so a large cold scan can churn
  the cache before actual row enumeration begins.
- This cost is not represented by the `OpenAsync` benchmark because the index is
  lazily initialized.

### 6. `ParallelPageReadsEnabled` does not yet prefetch table scans

The option currently switches the file stream access pattern and enables
`RandomAccess`-based page reads, but table enumeration still awaits pages in
sequence. This may help random page reads, but it does not implement a bounded
read-ahead pipeline for sequential table scans.

Primary code path:

- `AccessReaderOptions.ParallelPageReadsEnabled`
- `AccessReader.CreateStream`
- `AccessBase.EnableRandomAccessPageReadsIfSupported`
- `AccessBase.ReadPageAsync`
- Sequential loops in `EnumerateTypedRowsAsync`, `EnumerateDirectRowsAsync`,
  `RowsAsStrings`, and `ReadDataTableAsync`

## Implementation plan

### Phase 0: improve measurement coverage

Add focused benchmarks before changing behavior:

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

Candidate changes:

- Replace `CreateFromCompressed`'s `char[]` plus `new string(chars)` path with
  `string.Create` on target frameworks that support it.
- Replace `DecompressJet4Slow`'s intermediate `char[]` with `string.Create` after
  the existing counting pass.
- Investigate whether the all-compressed fast path can use a Latin-1 decoding
  helper on newer target frameworks without changing semantics.

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

Candidate changes:

- Wrap row insertion with `dt.BeginLoadData()` / `dt.EndLoadData()`.
- Set `dt.MinimumCapacity` when a trustworthy row count or `maxRows` is known.
- Benchmark `Rows.Add(object?[])` or `LoadDataRow` against `NewRow` plus per-cell
  assignment, while preserving `DBNull.Value` handling and typed columns.
- Add documentation nudging bulk consumers toward `Rows()` / `Rows<T>()` when a
  `DataTable` is not required.

Risks and constraints:

- `DataTable` semantics can be surprisingly stateful. Verify row state,
  column types, `DBNull.Value`, and early `maxRows` returns.

Acceptance criteria:

- `Decode_Text_DataTable` and `Decode_Numeric_DataTable` improve without changing
  public return shape.
- DataTable tests cover nulls, strings, fixed primitives, MEMO, and `maxRows`.

### Phase 4: avoid whole-file owned-page scans when possible

This targets cold reads of large databases.

Candidate changes:

- Implement per-table usage-map parsing for owned data pages and use it in
  `GetOwnedDataPagesAsync` when the map shape is recognized.
- Keep the current whole-file owner scan as a fallback for unusual or corrupt
  databases.
- When falling back, consider bypassing the normal page LRU or using a dedicated
  uncached read path so the classification pass does not evict pages needed by
  the actual table scan.

Risks and constraints:

- Usage-map parsing must be format-compatible across Jet3, Jet4, and ACE shapes
  before replacing the full scan broadly.
- Corrupt or unfamiliar usage maps must safely fall back.

Acceptance criteria:

- Cold first table scan time scales with table pages, not total database pages,
  for databases with recognized usage maps.
- Full-file fallback behavior remains available and tested.

### Phase 5: make parallel page reads visible to table scans

The option name implies more than the current table enumeration loops perform.

Candidate changes:

- Add a bounded read-ahead pipeline for table page numbers, preserving row order.
- Use `RandomAccess.ReadAsync` when enabled and supported.
- Keep default behavior conservative until benchmarks show a clear benefit.
- If read-ahead is not worth the complexity, clarify docs so the option describes
  random-access page I/O rather than parallel table scan processing.

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
