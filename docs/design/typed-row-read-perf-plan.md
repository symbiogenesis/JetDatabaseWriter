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
5. `JetTypeInfo.ResolveClrType(col)` + complex-id marker scan per column per row in
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
- [x] Add `internal static object ReadFixedTyped(byte[] row, int start, byte type, int size, bool strictNumeric)`
  in [AccessBase.cs](JetDatabaseWriter/Core/AccessBase.cs) returning the
  boxed primitive directly: `byte`, `short`, `int`, `float`, `double`,
  `DateTime`, `decimal` (money + numeric), `Guid`, complex-id marker
  sentinel.
- [x] Unit-test `ReadFixedTyped` parity with `ReadFixedString` →
  `TypedValueParser.ParseValue` for every JET type (incl. T_NUMERIC strict
  overflow, T_DATETIME OADate edges, T_MONEY scale=4).

#### Notes
- Tests live in
  [ReadFixedTypedTests.cs](JetDatabaseWriter.Tests/Core/ReadFixedTypedTests.cs)
  (46 cases, all green).
- The typed path **fixes three latent correctness bugs** in the legacy
  round-trip that are masked today by the `string.Empty → DBNull` fallback:
  - `T_INT` negative shorts: legacy `(short)Ru16(...)` throws under
    `<CheckForOverflowUnderflow>true` and collapses to `DBNull`. Typed
    path uses `BinaryPrimitives.ReadInt16LittleEndian` and returns the
    correct value.
  - `T_NUMERIC` mantissas with the high bit set (e.g. `decimal.MaxValue`):
    legacy `(int)uint` cast throws and collapses to `DBNull`. Typed path
    uses `unchecked((int)lo/mid/hi)` to preserve the bit pattern.
  - `T_DATETIME` sub-second precision: legacy formats with
    `"yyyy-MM-dd HH:mm:ss"` and re-parses, dropping milliseconds. Typed
    path returns the un-truncated `DateTime` straight from
    `DateTime.FromOADate(...)`.
- These divergences are pinned by named tests
  (`Int_Negative_TypedKeepsValue_RoundTripDropsToDBNull`,
  `Numeric_DecimalMaxValue_TypedKeepsValue_RoundTripDropsToDBNull`,
  `DateTime_SubSecondPrecision_TypedKeepsItRoundTripDoesNot`) so the
  contract is documented, not accidental.
- `ReadFixedString` is left unchanged — `RowsAsStrings` and the diagnostics
  paths still go through it, so the legacy bugs remain to be fixed there
  in a separate work item if desired.

### Phase 2 — Typed row cracker
- [x] Add `CrackRowTyped` that fills an `object?[]` directly (no
  intermediate `List<string>`). Variable-width text still decodes to
  `string`; `T_MEMO`/`T_OLE` keeps its async branch.
- [x] Make the synchronous portion truly sync: split into
  `TryCrackRowSync(... out object?[] row, out bool needsLongValue)` plus a
  fallback async path that only runs when long-value chains are present.
- [x] Move `cancellationToken.ThrowIfCancellationRequested()` from
  per-column to per-row (or per-page).
- [x] Hoist `JetTypeInfo.ResolveClrType(col)` results into `TableDef`/`ColumnInfo` once
  per table (cache `Type[] ClrTypes`).
- [x] Hoist the "has any var-cols" / "has any complex cols" flags onto
  `TableDef`.

#### Notes
- New typed cracker lives in
  [AccessReader.cs](JetDatabaseWriter/Core/AccessReader.cs) as
  `CrackRowTypedAsync` + `TryCrackRowSync` + `ReadVarTypedSync`. Sync
  fast-path emits a `LongValueRef` sentinel in any `T_MEMO`/`T_OLE` slot
  whose 12-byte header bitmask is NOT `0x80` (inline); the async wrapper
  walks the LVAL chain only for those sentinels and leaves all other
  slots untouched. Pure-fixed and inline-only rows therefore never enter
  the async state machine even though the entry point is `async`.
- `JetTypeInfo.ResolveClrType` (and its hyperlink-detection helper `JetTypeInfo.IsHyperlinkColumn`)
  moved to [JetTypeInfo.cs](JetDatabaseWriter/Internal/JetTypeInfo.cs)
  so both `AccessReader` and `TableDef.InitializeColumnMetadata` share
  the projection. The reader's local `JetTypeInfo.ResolveClrType`/`JetTypeInfo.IsHyperlinkColumn`
  now delegate to it (kept for backwards-compat with existing call sites).
- `TableDef` gained `Type[] ClrTypes`, `bool HasVarColumns`,
  `bool HasComplexColumns`, populated once by
  `InitializeColumnMetadata()`. Called from `AccessBase.ReadTableDefAsync`
  (read path) and `AccessWriter.BuildTableDefinition` (write path) so the
  cache is always present whenever a `TableDef` is observed by the
  cracker.
- Cancellation moved from per-column to per-row (per-page in the typed
  enumerator). This drops 11× `ThrowIfCancellationRequested` calls per
  row on the OrderDetails benchmark table.
- Wiring into the public `Rows()` / `ReadDataTableAsync()` API is
  Phase 3 — Phase 2 leaves the typed cracker reachable only via the
  internal `AccessReader.EnumerateRowsTypedNewPathAsync` test/benchmark
  hook. Parity with the legacy `ConvertRowToTyped` path is pinned by
  [CrackRowTypedParityTests.cs](JetDatabaseWriter.Tests/Core/CrackRowTypedParityTests.cs)
  on four NorthwindTraders tables (Order Details, Customers, Products,
  Employees), all green.

### Phase 3 — Wire new path into public API
- [x] Change `AccessReader.Rows(string)` to use `CrackRowTyped` + the new
  per-table complex-data resolver (no `ConvertRowToTyped` round-trip).
- [x] Change `ReadDataTableAsync` to use the typed path.
- [x] Keep `RowsAsStrings` on the existing string path (it's the only
  consumer that actually wants strings).
- [x] Delete or relegate `ConvertRowToTyped` to a fallback used only by the
  string→typed conversion API (if any external caller still needs it).

#### Notes
- `Rows(string)` and `ReadDataTableAsync` now decode rows via
  `CrackRowTypedAsync` directly — no `EnumerateRowsAsync` →
  `List<string>` → `ConvertRowToTyped` round-trip. Both methods skip
  the `BuildComplexColumnDataAsync` prefetch when
  `td.HasComplexColumns == false`.
- New post-processing helpers in
  [AccessReader.cs](JetDatabaseWriter/Core/AccessReader.cs):
  - `ResolveComplexColumns(typedRow, columns, complexData)` replaces
    the `"__CX:N__"` marker emitted by
    `JetTypeInfo.ReadFixedTyped`/`ReadVarTypedSync` with the joined
    attachment bytes (or `DBNull` when no child data resolves). Falls
    back to the parent row's first `T_LONG` column when the marker is
    missing — a typed equivalent of the legacy `ExtractParentId`.
  - `WrapHyperlinkColumns(typedRow, td.ClrTypes)` upgrades
    Hyperlink-flagged text payloads into `Hyperlink` instances, matching
    the projection `JetTypeInfo.ResolveClrType` exposes via the public
    API. Both helpers are gated by new
    `TableDef.HasComplexColumns` / `TableDef.HasHyperlinkColumns`
    flags so non-affected tables pay zero per-row cost.
- `ConvertRowToTyped`, `ExtractComplexId` (now `static`), and
  `ExtractParentId` (deleted) are no longer reached from any public path.
  `ExtractParentId` is replaced by `ExtractParentIdTyped` (operates on
  `object?[]`); `ConvertRowToTyped` was removed.
- Removed [CrackRowTypedParityTests.cs](JetDatabaseWriter.Tests/Core/CrackRowTypedParityTests.cs) —
  after Phase 3 the legacy and new paths are the same path, so the
  parity test devolved into a self-comparison. Coverage of the typed
  path is preserved by the existing Northwind round-trip and DataTable
  read tests (see `AccessReaderReadTests`, `HyperlinkTests`,
  `CfbAesDecryptionTests`).
- `RowsAsStrings` is unchanged (still uses `EnumerateRowsAsync` →
  `List<string>`).
- Test result after wiring: 2548/2550 pass. The two failures
  (`AccessRoundTripTests.SinglePk_AndSingleColumnFk_SurviveCompactAndRepair`
  and `…CompositePk_AndMultiColumnFk_SurviveCompactAndRepair`) are the
  pre-existing DAO Compact baseline failures documented in
  `/memories/repo/round-trip-tests.md` — unrelated to typed-row
  decoding.

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
