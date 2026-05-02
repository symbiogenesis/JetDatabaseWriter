# Typed-row read performance plan

Goal: speed up the typed-row read path (`AccessReader.Rows()`, `Rows<T>()`,
`ReadDataTableAsync()`) by removing the per-column string round-trip and the
per-row async overhead.

## Problem summary

Today every typed row flows through:

```
page bytes → CrackRowAsync → List<string> → ConvertRowToTyped → object[] → (RowMapper) → T
```

Hottest costs, in order:

1. Fixed-width primitives are formatted to invariant-culture strings in
   `AccessBase.ReadFixedString` and re-parsed by
   `TypedValueParser.ParseValue` — pure waste. Worst offender: `T_DATETIME`
   (bytes → `double` → `DateTime` → `"yyyy-MM-dd HH:mm:ss"` → `DateTime.Parse`
   → boxed `DateTime`).
2. Per-row allocations: `List<string>(N)` + N `string` objects + N boxed
   primitives.
3. `async`/`ValueTask` state machine on `CrackRowAsync` / `ReadVarAsync` even
   when no async I/O happens (fixed-only rows).
4. `cancellationToken.ThrowIfCancellationRequested()` per column.
5. `ResolveClrType(col)` + complex-id marker scan per column per row in
   `ConvertRowToTyped`.
6. `RowMapper<T>.Map` does `acc.TargetType != value.GetType()` on every
   column and falls back to `Convert.ChangeType` for primitive widening
   (e.g. `short` → `int`).
7. `await foreach` / `IAsyncEnumerator` allocation per page.

## Plan

### Phase 0 — Baseline measurement
- [x] Add/extend BenchmarkDotNet cases in
  [AccessReaderBenchmarks.cs](JetDatabaseWriter.Benchmarks/AccessReaderBenchmarks.cs)
  to cover `StreamRows_All`, `StreamRows<T>_All`, and a numeric/date-heavy
  table (not just first table).
- [x] Capture baseline numbers (mean ns/row, allocations/row) and record in
  this doc before any change.

#### Baseline (2026-05-02, .NET 10.0.7, Intel Core Ultra 7 268V)

Run: `dotnet run --project JetDatabaseWriter.Benchmarks -c Release -- --filter "*AccessReaderBenchmarks.StreamRows*" --warmupCount 3 --iterationCount 5 --invocationCount 1 --unrollFactor 1`

Database: `NorthwindTraders.accdb` (copied from `JetDatabaseWriter.Tests/Databases/`).
- First table = `Catalog_TableOfContents` (16 rows, 2 columns).
- Numeric/date-heavy table = `OrderDetails` (130 rows, 11 columns: 5×int, 1×short, 1×currency, 1×single, 2×datetime, 2×short text).

Each `op` includes one `AccessReader.OpenAsync` + a full table scan (the
existing `Setup` does not pre-open the reader). Open cost is ~constant
per op and dominates the 16-row first-table case; the 130-row OrderDetails
case is a better Phase 1 signal.

| Method                        | Mean / op  | Alloc / op | Rows | µs / row | Alloc / row |
| ----------------------------- | ---------: | ---------: | ---: | -------: | ----------: |
| `StreamRows_All` (catalog)    |  49.59 ms  |  3.27 MB   |   16 |  3 100   |   209 KB    |
| `StreamRowsAsStrings_All`     |  51.67 ms  |  3.26 MB   |   16 |  3 230   |   208 KB    |
| `StreamRows_All_Numeric`      |  65.85 ms  |  3.37 MB   |  130 |    506   |    26 KB    |
| `StreamRowsTyped_All_Numeric` |  65.72 ms  |  4.22 MB   |  130 |    506   |    33 KB    |

Notes:
- The ~3.3 MB floor per op is `OpenAsync` (catalog + system table parsing).
  Phase 1/2 progress should be tracked via the `_Numeric` rows above and
  via the **alloc / row** delta, not the absolute `op` mean.
- `StreamRowsTyped_All_Numeric` adds ~7 KB/row over the untyped path —
  that is the `RowMapper<T>` boxing/widening overhead Phase 5 targets.
- The untyped string round-trip (`StreamRows_All_Numeric`) currently
  matches the typed path on time because both are dominated by per-column
  string formatting + parsing in `ReadFixedString` / `ParseValue`. Phase 1
  is expected to drop both numbers materially.
- `iterationCount=5` was chosen for fast turnaround; some `Error` margins
  are wide (>10%). Re-run with default BDN settings (longer iterations)
  for the post-Phase 1 comparison.

### Phase 1 — Typed fixed-width decode (biggest win)
- [ ] Add `internal static object ReadFixedTyped(byte[] row, int start, byte type, int size, bool strictNumeric)`
  in [AccessBase.cs](JetDatabaseWriter/Core/AccessBase.cs) returning the
  boxed primitive directly: `byte`, `short`, `int`, `float`, `double`,
  `DateTime`, `decimal` (money + numeric), `Guid`, complex-id marker
  sentinel.
- [ ] Unit-test `ReadFixedTyped` parity with `ReadFixedString` →
  `TypedValueParser.ParseValue` for every JET type (incl. T_NUMERIC strict
  overflow, T_DATETIME OADate edges, T_MONEY scale=4).

### Phase 2 — Typed row cracker
- [ ] Add `CrackRowTyped` that fills an `object?[]` directly (no
  intermediate `List<string>`). Variable-width text still decodes to
  `string`; `T_MEMO`/`T_OLE` keeps its async branch.
- [ ] Make the synchronous portion truly sync: split into
  `TryCrackRowSync(... out object?[] row, out bool needsLongValue)` plus a
  fallback async path that only runs when long-value chains are present.
- [ ] Move `cancellationToken.ThrowIfCancellationRequested()` from
  per-column to per-row (or per-page).
- [ ] Hoist `ResolveClrType(col)` results into `TableDef`/`ColumnInfo` once
  per table (cache `Type[] ClrTypes`).
- [ ] Hoist the "has any var-cols" / "has any complex cols" flags onto
  `TableDef`.

### Phase 3 — Wire new path into public API
- [ ] Change `AccessReader.Rows(string)` to use `CrackRowTyped` + the new
  per-table complex-data resolver (no `ConvertRowToTyped` round-trip).
- [ ] Change `ReadDataTableAsync` to use the typed path.
- [ ] Keep `RowsAsStrings` on the existing string path (it's the only
  consumer that actually wants strings).
- [ ] Delete or relegate `ConvertRowToTyped` to a fallback used only by the
  string→typed conversion API (if any external caller still needs it).

### Phase 4 — Per-page enumerator (kill async-per-row overhead)
- [ ] Replace per-row `IAsyncEnumerable<object?[]>` with per-page
  `ValueTask<PageRows>` returning a small struct holding the decoded
  `object?[][]` (or an `ArrayPool`-backed buffer) for that page; expose to
  callers as `IAsyncEnumerable<object?[]>` via a thin wrapper.
- [ ] Pool the per-row `object?[]` via `ArrayPool<object?>` for internal
  consumers (e.g., `DataTable` builder, `Rows<T>()` mapper) where the array
  doesn't escape the loop iteration.

### Phase 5 — RowMapper<T> fast path
- [ ] In `RowMapper<T>.BuildIndex`, accept `Type[] sourceTypes` (the per-
  column CLR types from Phase 2) and pre-compile a single
  `Action<object?[], T>` that:
  - skips null/DBNull,
  - emits direct assignment when source==target,
  - emits inlined widening conversions (e.g. `(int)(short)v`) instead of
    `Convert.ChangeType`,
  - keeps the Hyperlink ↔ string interop branch.
- [ ] Replace `Map(row, index)` callsites with the compiled delegate.

### Phase 6 — Micro-cleanups
- [ ] In `Rows<T>()`, avoid double `await foreach` (today it iterates
  `Rows()` which is itself async-iterated). Inline against the page-level
  enumerator from Phase 4.
- [ ] Audit `ColumnSlice`/`RowLayout` for per-row struct copies; pass by
  `in` / `ref readonly` where it removes copies.
- [ ] Fast-path complex-id marker resolution: store the resolved
  `int complexId` directly in the typed slot (boxed `int` sentinel or
  internal struct) instead of a `"__CX:N__"` string.

### Phase 7 — Verify & document
- [ ] Re-run the Phase 0 benchmarks; record results in this doc.
- [ ] Run full `dotnet test` (xUnit v3 / MTP; args go directly to
  `dotnet test`).
- [ ] Update README perf section if numbers warrant it.

## Expected impact

- Phases 1–3 alone: **~3–6× throughput** on numeric/date-heavy tables and a
  large drop in GC pressure (no per-column string allocation).
- Phase 4 saves the async state-machine overhead on fixed-only rows.
- Phase 5 mostly helps `Rows<T>()` workloads with primitive-widening
  properties.

## Out of scope

- Page I/O, decryption, and the LRU page cache — already covered by
  `ReadPageCachedAsync`. Not the bottleneck for typed reads.
- Index/seek paths — separate work item.
- Long-value (`T_MEMO`/`T_OLE`) decode — keep the existing async chain
  walker; only fixed/var-text decode is on the hot path here.
