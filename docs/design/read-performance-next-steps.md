# Read performance next steps

Status: candidate guidance for future work
Date: 2026-05-30

This note is a companion to
[read-performance-bottlenecks.md](read-performance-bottlenecks.md), which
records completed implementation and measurement work. Use this file when read
performance still feels slow and the next move is to reshape the caller
workload, add a targeted API, or gather new workload-specific evidence.

## First checks

- Confirm the slow path is not `OpenAsync`. Current measurements put the open
  floor around 1.1 ms / 41 KB, so large-read work should usually focus on row
  scan shape, materialization, projection, long values, or filtering.
- Separate cold first-row latency from full-scan throughput. The read-ahead path
  helps warm full scans more than first-row latency.
- Compare the real workload against the existing synthetic shapes: fixed-width
  numeric, text-heavy, wide rows, MEMO/OLE long values, DataTable
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

`ReadDataTableAsync`, `ReadAllTablesAsync`, and string-typed DataTable APIs are
convenience and compatibility APIs. They fully materialize rows, allocate
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

## Candidate library work

### Public object-array projection API

Add an object-array projection API such as `Rows(tableName, columns)` or a
similar column-selection surface. Internally, `RowDecodePlan` already supports a
projection mask; today the public projection benefit is mainly exposed through
`Rows<T>()`. A public projection API would help callers that cannot or do not
want to define DTOs but still want to avoid decoding every column in wide rows.

This is likely the best next library feature if wide untyped scans are the
remaining pain point.

### Projected or typed index seek

Add `SeekRowsAsync<T>` or a projected seek API so exact indexed lookups can
avoid materializing full `object[]` rows. The current `SeekRowsAsync` narrows
page discovery through the index but then decodes complete rows for each hit.

This is likely worthwhile when workloads perform many indexed point lookups and
only consume a few columns from each match.

### Lazy long-value access

Consider a new opt-in API that exposes MEMO/OLE payloads through a lazy reader or
handle instead of immediately returning `string` / `byte[]`. This would require
a careful lifetime and page-buffer ownership model. It should not be mixed into
the existing row APIs because those APIs currently return stable values
independent of the reader's internal page buffers.

### Workload-specific benchmarks

Before changing the core decoder again, add a benchmark that matches the slow
real workload. Useful comparisons:

- Full `Rows(...)` versus narrow `Rows<T>()`.
- `Rows(...).Where(...)` versus `SeekRowsAsync` for exact indexed predicates.
- `Rows<T>()` two-phase MEMO/OLE filtering versus full-row long-value decode.
- Default options versus larger `PageCacheSize` plus explicit `PageReadOptimizationMode.Enabled`.
- Streaming APIs versus `ReadDataTableAsync` only when full materialization is
  truly required.

## Non-goals without new evidence

- Reopening text decode micro-optimizations without a fresh profile showing
  avoidable transient allocation beyond required final `string` creation.
- Swapping the production DataTable insertion strategy based only on the prior
  `Rows.Add(object?[])` or `LoadDataRow` benchmark results.
- Adding tunable read-ahead depth or enabling read-ahead for long-value-heavy
  scans before there is a page-buffer lease model and a profile showing it will
  pay for its complexity.
- Spending more time on lazy catalog loading unless new release-quality data
  contradicts the current `OpenAsync` floor.